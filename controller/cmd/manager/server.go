package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/go-logr/logr"

	"github.com/jackfrancis/agent-execution-interface/control"
	"github.com/jackfrancis/agent-execution-interface/controller/internal/dispatch"
)

// defaultDispatchPort is the dispatch listener's port when -dispatch-addr is not set.
const defaultDispatchPort = "8081"

// resolveDispatchAddr picks the dispatch listener's address. An explicit
// -dispatch-addr is used as given. Without one, dispatch listens on the ABI
// listener's host at defaultDispatchPort, so an installation that binds the ABI to
// one interface (say loopback) keeps dispatch on that interface.
//
// Either way the two listeners must use different ports, whatever their hosts: a
// NetworkPolicy admits traffic by port, so dispatch on the ABI's port (on another
// address of the pod, say its IPv6 one) would be open to everything the ABI is,
// runtime pods included.
func resolveDispatchAddr(abiAddr, dispatchAddr string) (string, error) {
	abiHost, abiPort, err := splitPort("-abi-addr", abiAddr)
	if err != nil {
		return "", err
	}
	if dispatchAddr == "" {
		dispatchAddr = net.JoinHostPort(abiHost, defaultDispatchPort)
	}
	_, dispatchPort, err := splitPort("-dispatch-addr", dispatchAddr)
	if err != nil {
		return "", err
	}
	if dispatchPort == abiPort {
		return "", fmt.Errorf("-dispatch-addr %q and -abi-addr %q use the same port %d; dispatch needs a port of its own (set -dispatch-addr)",
			dispatchAddr, abiAddr, abiPort)
	}
	return dispatchAddr, nil
}

// splitPort splits a listen address and resolves its port, which may be a
// service name such as "http", to a number.
func splitPort(flagName, addr string) (host string, port int, err error) {
	host, p, err := net.SplitHostPort(addr)
	if err != nil {
		return "", 0, fmt.Errorf("%s %q: %w", flagName, addr, err)
	}
	port, err = net.LookupPort("tcp", p)
	if err != nil {
		return "", 0, fmt.Errorf("%s %q: %w", flagName, addr, err)
	}
	return host, port, nil
}

// newMuxes builds the handlers for the two listeners. The ABI listener carries the
// routes runtime pods call (/complete, /redeem, /vend, /authority) and /metrics for
// Prometheus; the dispatch listener carries only /dispatch and /runs/{id}, which
// apps call. Neither serves the other's routes, so a NetworkPolicy on the dispatch
// port keeps runtime pods away from dispatch.
func newMuxes(plane *control.Plane, disp *dispatch.Handler, metrics http.Handler) (abi, dispatchMux *http.ServeMux) {
	abi = http.NewServeMux()
	plane.RegisterRoutes(abi)
	abi.Handle("GET /metrics", metrics)
	dispatchMux = http.NewServeMux()
	disp.RegisterRoutes(dispatchMux)
	return abi, dispatchMux
}

// serverTimeouts bounds how long one connection may hold the listener. Runtime pods
// reach this listener by design, so a stalled or hostile client must not be able to
// keep a connection, or a goroutine, open indefinitely.
type serverTimeouts struct {
	ReadHeader time.Duration // request line and headers
	Read       time.Duration // whole request, body included
	Write      time.Duration // from the end of the request headers to the end of the response
	Handler    time.Duration // request context deadline; shorter than Write so a handler that checks it can still answer
	Idle       time.Duration // keep-alive wait for the next request
	Shutdown   time.Duration // drain in-flight requests after SIGTERM
}

// defaultServerTimeouts suits the AEI routes, which all answer promptly: none
// streams or long-polls. Handler leaves 15s of Write for a handler that stops at
// its deadline to send its response: WriteTimeout only sets a socket deadline, so
// without it a slow /dispatch could launch a run whose 202 never reaches the
// caller, and the caller's retry would launch a second. Shutdown stays under the
// pod's default 30s termination grace period and controller-runtime's default 30s
// runnable shutdown timeout.
var defaultServerTimeouts = serverTimeouts{
	ReadHeader: 5 * time.Second,
	Read:       30 * time.Second,
	Write:      60 * time.Second,
	Handler:    45 * time.Second,
	Idle:       120 * time.Second,
	Shutdown:   20 * time.Second,
}

// httpServer is one HTTP listener run by the controller manager. The manager starts
// it with the controllers and, on SIGTERM, cancels its context: the listener stops
// accepting and in-flight requests get up to Shutdown to finish. A runtime whose
// /complete call is refused while the pod drains retries against the next replica
// (aeiruntime.Complete). The manager runs one for the ABI and one for dispatch.
type httpServer struct {
	name     string
	addr     string
	handler  http.Handler
	timeouts serverTimeouts
	log      logr.Logger
}

// Start implements manager.Runnable.
func (s *httpServer) Start(ctx context.Context) error {
	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		return err
	}
	return s.serve(ctx, ln)
}

// NeedLeaderElection implements manager.LeaderElectionRunnable: every replica
// serves, since a runtime callback may land on any of them.
func (s *httpServer) NeedLeaderElection() bool { return false }

// withDeadline gives every request's context a deadline d from arrival. Handlers
// that pass r.Context() to their lookups stop at it, and a handler about to take
// an irreversible step checks it first.
func withDeadline(h http.Handler, d time.Duration) http.Handler {
	if d <= 0 {
		return h
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), d)
		defer cancel()
		h.ServeHTTP(w, r.WithContext(ctx))
	})
}

// serve runs the server on ln until ctx ends, then shuts it down gracefully.
func (s *httpServer) serve(ctx context.Context, ln net.Listener) error {
	srv := &http.Server{
		Handler:           withDeadline(s.handler, s.timeouts.Handler),
		ReadHeaderTimeout: s.timeouts.ReadHeader,
		ReadTimeout:       s.timeouts.Read,
		WriteTimeout:      s.timeouts.Write,
		IdleTimeout:       s.timeouts.Idle,
	}
	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.Serve(ln) }()
	s.log.Info("serving", "server", s.name, "addr", ln.Addr().String())

	select {
	case err := <-serveErr:
		return err // Serve never returns nil; without a Shutdown this is a real failure
	case <-ctx.Done():
	}
	s.log.Info("shutting down; draining in-flight requests", "server", s.name, "timeout", s.timeouts.Shutdown)
	shutdownCtx, cancel := context.WithTimeout(context.Background(), s.timeouts.Shutdown)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		_ = srv.Close()
		return err
	}
	if err := <-serveErr; !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}
