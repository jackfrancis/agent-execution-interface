package main

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"testing"
	"time"

	"github.com/go-logr/logr"
)

var testTimeouts = serverTimeouts{
	ReadHeader: 100 * time.Millisecond,
	Read:       300 * time.Millisecond,
	Write:      time.Second,
	Handler:    500 * time.Millisecond,
	Idle:       time.Second,
	Shutdown:   5 * time.Second,
}

// startServer serves h on a loopback port until ctx ends; the returned channel
// yields serve's result.
func startServer(t *testing.T, ctx context.Context, h http.Handler) (string, <-chan error) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	s := &httpServer{name: "test", handler: h, timeouts: testTimeouts, log: logr.Discard()}
	done := make(chan error, 1)
	go func() { done <- s.serve(ctx, ln) }()
	return ln.Addr().String(), done
}

// TestDefaultServerTimeoutsAreSet: the production listener bounds every phase of
// a connection; a zero value would mean no limit.
func TestDefaultServerTimeoutsAreSet(t *testing.T) {
	d := defaultServerTimeouts
	for name, v := range map[string]time.Duration{
		"ReadHeader": d.ReadHeader, "Read": d.Read, "Write": d.Write, "Handler": d.Handler, "Idle": d.Idle, "Shutdown": d.Shutdown,
	} {
		if v <= 0 {
			t.Errorf("%s timeout = %v, want > 0", name, v)
		}
	}
	if d.Handler >= d.Write {
		t.Errorf("Handler %v must be shorter than Write %v so a handler that stops at its deadline can still answer", d.Handler, d.Write)
	}
}

// TestRequestContextHasHandlerDeadline: every request's context carries the
// Handler deadline, which handlers check before an irreversible step.
func TestRequestContextHasHandlerDeadline(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	remaining := make(chan time.Duration, 1)
	addr, _ := startServer(t, ctx, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		dl, ok := r.Context().Deadline()
		if !ok {
			remaining <- 0
			return
		}
		remaining <- time.Until(dl)
	}))
	resp, err := http.Get("http://" + addr + "/")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	got := <-remaining
	if got <= 0 || got > testTimeouts.Handler {
		t.Errorf("request deadline in %v; want within the %v Handler timeout", got, testTimeouts.Handler)
	}
}

// TestStalledClientIsDisconnected: a client that stops sending partway through its
// headers or its body loses the connection once the read timeouts pass.
func TestStalledClientIsDisconnected(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	addr, _ := startServer(t, ctx, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, err := io.ReadAll(r.Body); err != nil {
			http.Error(w, "bad body", http.StatusBadRequest)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))

	stalls := map[string]string{
		"partial headers": "POST /aei/v1alpha1/complete HTTP/1.1\r\nHost: aei\r\n",
		"partial body":    "POST /aei/v1alpha1/complete HTTP/1.1\r\nHost: aei\r\nContent-Length: 100\r\n\r\n{\"run_id\":",
	}
	for name, sent := range stalls {
		t.Run(name, func(t *testing.T) {
			conn, err := net.Dial("tcp", addr)
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			if _, err := io.WriteString(conn, sent); err != nil {
				t.Fatal(err)
			}
			// Then send nothing more. The server must close the connection; the
			// 5s client deadline only stops the test from hanging if it does not.
			start := time.Now()
			_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
			_, err = io.Copy(io.Discard, conn) // nil at EOF
			var ne net.Error
			if errors.As(err, &ne) && ne.Timeout() {
				t.Fatalf("server held the stalled connection open for %v", time.Since(start))
			}
			if elapsed := time.Since(start); elapsed > 2*time.Second {
				t.Errorf("connection closed after %v; want about the read timeout", elapsed)
			}
		})
	}
}

// TestSIGTERMFinishesInFlightRequest: a request that is being handled when the
// process receives SIGTERM completes normally, and the listener then closes.
func TestSIGTERMFinishesInFlightRequest(t *testing.T) {
	// The same wiring as ctrl.SetupSignalHandler, which may be called only once
	// per process: SIGTERM cancels the context the manager hands its runnables.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM)
	defer stop()

	entered := make(chan struct{})
	addr, served := startServer(t, ctx, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		close(entered)
		time.Sleep(500 * time.Millisecond) // still working when SIGTERM arrives
		_, _ = io.WriteString(w, "done")
	}))

	type result struct {
		status int
		body   string
		err    error
	}
	got := make(chan result, 1)
	go func() {
		resp, err := http.Post("http://"+addr+"/aei/v1alpha1/complete", "application/json", nil)
		if err != nil {
			got <- result{err: err}
			return
		}
		defer resp.Body.Close()
		b, err := io.ReadAll(resp.Body)
		got <- result{status: resp.StatusCode, body: string(b), err: err}
	}()

	<-entered
	if err := syscall.Kill(os.Getpid(), syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}

	r := <-got
	if r.err != nil || r.status != http.StatusOK || r.body != "done" {
		t.Fatalf("in-flight request = %d %q (err %v), want 200 done", r.status, r.body, r.err)
	}
	select {
	case err := <-served:
		if err != nil {
			t.Fatalf("serve returned %v, want nil after a graceful shutdown", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("server did not stop after SIGTERM")
	}
	if conn, err := net.DialTimeout("tcp", addr, time.Second); err == nil {
		conn.Close()
		t.Error("listener still accepting after shutdown")
	}
}
