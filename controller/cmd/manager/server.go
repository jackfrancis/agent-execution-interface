package main

import (
	"context"
	"errors"
	"net"
	"net/http"
	"time"

	"github.com/go-logr/logr"
)

// serverTimeouts bounds how long one connection may hold the listener. Runtime pods
// reach this listener by design, so a stalled or hostile client must not be able to
// keep a connection, or a goroutine, open indefinitely.
type serverTimeouts struct {
	ReadHeader time.Duration // request line and headers
	Read       time.Duration // whole request, body included
	Write      time.Duration // from the end of the request headers to the end of the response
	Idle       time.Duration // keep-alive wait for the next request
	Shutdown   time.Duration // drain in-flight requests after SIGTERM
}

// defaultServerTimeouts suits the AEI routes, which all answer promptly: none
// streams or long-polls. Shutdown stays under the pod's default 30s termination
// grace period and controller-runtime's default 30s runnable shutdown timeout.
var defaultServerTimeouts = serverTimeouts{
	ReadHeader: 5 * time.Second,
	Read:       30 * time.Second,
	Write:      60 * time.Second,
	Idle:       120 * time.Second,
	Shutdown:   20 * time.Second,
}

// httpServer is one HTTP listener run by the controller manager. The manager starts
// it with the controllers and, on SIGTERM, cancels its context: the listener stops
// accepting and in-flight requests get up to Shutdown to finish. A runtime whose
// /complete call is refused while the pod drains retries against the next replica
// (aeiruntime.Complete). To serve another listener (for example a separate
// dispatch port), add another httpServer with its own address and handler.
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

// serve runs the server on ln until ctx ends, then shuts it down gracefully.
func (s *httpServer) serve(ctx context.Context, ln net.Listener) error {
	srv := &http.Server{
		Handler:           s.handler,
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
