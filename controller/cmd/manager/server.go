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

// dispatchOnABIListener keeps the dispatch API on the ABI listener too, where apps
// called it before it had a listener of its own. It is transitional: once apps
// call the dispatch listener, it goes, and a NetworkPolicy can then keep runtime
// pods away from dispatch.
const dispatchOnABIListener = true

// defaultDispatchPort is the dispatch listener's port when -dispatch-addr is not set.
const defaultDispatchPort = "8081"

// resolveDispatchAddr picks the dispatch listener's address and reports whether it
// gets a listener of its own. An explicit -dispatch-addr is used as given, and must
// differ from -abi-addr. Without one, dispatch listens on the ABI listener's host
// at defaultDispatchPort, so an installation that binds the ABI to one interface
// (say loopback) does not start serving dispatch on every interface after an
// upgrade. An installation whose ABI already uses that port keeps dispatch on the
// ABI listener alone, as before, rather than failing to bind it twice.
func resolveDispatchAddr(abiAddr, dispatchAddr string) (addr string, separate bool, err error) {
	if dispatchAddr != "" {
		if dispatchAddr == abiAddr {
			return "", false, fmt.Errorf("-dispatch-addr %q must differ from -abi-addr", dispatchAddr)
		}
		return dispatchAddr, true, nil
	}
	host, port, err := net.SplitHostPort(abiAddr)
	if err != nil {
		return "", false, fmt.Errorf("-abi-addr %q: %w", abiAddr, err)
	}
	if port == defaultDispatchPort {
		if !dispatchOnABIListener {
			return "", false, fmt.Errorf("-abi-addr %q uses the default dispatch port; set -dispatch-addr", abiAddr)
		}
		return abiAddr, false, nil
	}
	return net.JoinHostPort(host, defaultDispatchPort), true, nil
}

// newMuxes builds the handlers for the two listeners. The ABI listener carries the
// routes runtime pods call (/complete, /redeem, /vend, /authority) and /metrics for
// Prometheus; the dispatch listener carries only /dispatch and /runs/{id}, which
// apps call. withDispatch also mounts the dispatch routes on the ABI listener.
func newMuxes(plane *control.Plane, disp *dispatch.Handler, metrics http.Handler, withDispatch bool) (abi, dispatchMux *http.ServeMux) {
	abi = http.NewServeMux()
	plane.RegisterRoutes(abi)
	abi.Handle("GET /metrics", metrics)
	if withDispatch {
		disp.RegisterRoutes(abi)
	}
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
