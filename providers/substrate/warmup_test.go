package substrate

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackfrancis/agent-execution-interface/aei"
)

// fastWarmup returns a Launcher whose warmup backoff is tiny, pointed at srvURL. The
// literal ActorHost makes ensure() a no-op (the actor is assumed pre-provisioned), so
// these tests exercise only the dispatch retry.
func fastWarmup(srvURL string) *Launcher {
	l := New(&http.Client{}, Options{RouterURL: srvURL, ActorHost: "gofer.aei.actors"})
	l.backoffStart, l.backoffMax = time.Millisecond, time.Millisecond
	return l
}

// acceptedTask writes a minimal accepted A2A task (a state other than "failed").
func acceptedTask(w http.ResponseWriter, id string) {
	_ = json.NewEncoder(w).Encode(map[string]any{
		"result": map[string]any{"id": id, "status": map[string]any{"state": "submitted"}},
	})
}

// TestDispatchRetriesWarmup: a transient 503 (actor warming up during its golden-
// snapshot cold start) is retried until the actor accepts the task, so the app never
// sees the durable-only transient.
func TestDispatchRetriesWarmup(t *testing.T) {
	var attempts int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if atomic.AddInt32(&attempts, 1) <= 2 {
			http.Error(w, `actor "gofer" unavailable: no free workers available`, http.StatusServiceUnavailable)
			return
		}
		acceptedTask(w, "task-1")
	}))
	defer srv.Close()

	h, err := fastWarmup(srv.URL).Dispatch(context.Background(), aei.RunSpec{ID: "run-1"}, aei.Credential{})
	if err != nil {
		t.Fatalf("Dispatch after warmup: %v", err)
	}
	if h.Ref != "task-1" {
		t.Errorf("handle ref = %q, want task-1", h.Ref)
	}
	if got := atomic.LoadInt32(&attempts); got != 3 {
		t.Errorf("attempts = %d, want 3 (2×503 then 200)", got)
	}
}

// TestDispatchWarmupDeadline: if the actor never becomes ready, the retry is bounded
// by the run deadline and returns an error rather than looping forever.
func TestDispatchWarmupDeadline(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "no free workers available", http.StatusServiceUnavailable)
	}))
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	if _, err := fastWarmup(srv.URL).Dispatch(ctx, aei.RunSpec{ID: "run-1"}, aei.Credential{}); err == nil {
		t.Fatal("want a deadline error when the actor never becomes ready")
	}
}

// TestDispatchDoesNotRetryPermanent: only the transient 503 is retried — a rejected
// request (any other status) returns immediately.
func TestDispatchDoesNotRetryPermanent(t *testing.T) {
	var attempts int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		atomic.AddInt32(&attempts, 1)
		http.Error(w, "bad request", http.StatusBadRequest)
	}))
	defer srv.Close()

	if _, err := fastWarmup(srv.URL).Dispatch(context.Background(), aei.RunSpec{ID: "run-1"}, aei.Credential{}); err == nil {
		t.Fatal("want an error for a 400")
	}
	if got := atomic.LoadInt32(&attempts); got != 1 {
		t.Errorf("attempts = %d, want 1 (a 400 is not retried)", got)
	}
}
