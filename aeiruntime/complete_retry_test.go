package aeiruntime_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	rt "github.com/jackfrancis/agent-execution-interface/aeiruntime"
)

// completeServer answers /complete with respond(attempt), counting attempts from 1.
func completeServer(t *testing.T, respond func(attempt int32, w http.ResponseWriter)) (*rt.Runtime, *atomic.Int32) {
	t.Helper()
	var attempts atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/complete" {
			http.NotFound(w, r)
			return
		}
		respond(attempts.Add(1), w)
	}))
	t.Cleanup(srv.Close)
	r, err := rt.Load(context.Background(), envFunc(map[string]string{
		rt.EnvRunID:      "run-retry",
		rt.EnvCredential: "push-tok",
		rt.EnvControlURL: srv.URL,
	}), nil)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	return r, &attempts
}

// dropConnection closes the connection without a response, as a controller that
// restarts mid-request does.
func dropConnection(w http.ResponseWriter) {
	conn, _, err := http.NewResponseController(w).Hijack()
	if err == nil {
		_ = conn.Close()
	}
}

// TestCompleteRetriesTransientFailure: a completion call that fails once — a 5xx,
// a 429, or a dropped connection — is retried and then delivered.
func TestCompleteRetriesTransientFailure(t *testing.T) {
	failures := map[string]func(http.ResponseWriter){
		"503":     func(w http.ResponseWriter) { w.WriteHeader(http.StatusServiceUnavailable) },
		"429":     func(w http.ResponseWriter) { w.WriteHeader(http.StatusTooManyRequests) },
		"dropped": dropConnection,
	}
	for name, fail := range failures {
		t.Run(name, func(t *testing.T) {
			r, attempts := completeServer(t, func(n int32, w http.ResponseWriter) {
				if n == 1 {
					fail(w)
					return
				}
				w.WriteHeader(http.StatusAccepted)
			})
			if err := r.Complete(context.Background(), nil, rt.Timing{}, ""); err != nil {
				t.Fatalf("Complete: %v", err)
			}
			if got := attempts.Load(); got != 2 {
				t.Errorf("attempts = %d, want 2", got)
			}
		})
	}
}

// TestCompleteDoesNotRetryClientError: a 4xx such as 401 (expired credential) or
// 403 (foreign run) cannot succeed on a repeat, so it fails after one attempt.
func TestCompleteDoesNotRetryClientError(t *testing.T) {
	for _, code := range []int{http.StatusUnauthorized, http.StatusForbidden} {
		r, attempts := completeServer(t, func(_ int32, w http.ResponseWriter) { w.WriteHeader(code) })
		if err := r.Complete(context.Background(), nil, rt.Timing{}, ""); err == nil {
			t.Errorf("status %d: want an error", code)
		}
		if got := attempts.Load(); got != 1 {
			t.Errorf("status %d: attempts = %d, want 1", code, got)
		}
	}
}

// TestCompleteStopsWhenContextEnds: against a control plane that never recovers,
// Complete keeps retrying only until the caller's context ends.
func TestCompleteStopsWhenContextEnds(t *testing.T) {
	r, attempts := completeServer(t, func(_ int32, w http.ResponseWriter) {
		w.WriteHeader(http.StatusBadGateway)
	})
	ctx, cancel := context.WithTimeout(context.Background(), 800*time.Millisecond)
	defer cancel()
	start := time.Now()
	if err := r.Complete(ctx, nil, rt.Timing{}, ""); err == nil {
		t.Fatal("want an error once the context ends")
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Errorf("Complete returned after %v; want it bounded by the 800ms context", elapsed)
	}
	if got := attempts.Load(); got < 2 {
		t.Errorf("attempts = %d, want at least 2", got)
	}
}
