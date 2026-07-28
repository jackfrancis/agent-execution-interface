package aeiruntime

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

// fakeControl is a minimal broker+control plane: it redeems a ticket and records
// the /complete call, signalling completion on a channel so a detached run can be
// awaited. It is a white-box helper (the durable serve loop is unexported), so it
// is deliberately separate from the black-box fakeCP in aeiruntime_test.go.
type fakeControl struct {
	srv      *httptest.Server
	mu       sync.Mutex
	redeemed bool
	redeems  int
	complete chan map[string]any
}

func newFakeControl(t *testing.T) *fakeControl {
	t.Helper()
	f := &fakeControl{complete: make(chan map[string]any, 1)}
	f.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		switch r.URL.Path {
		case "/redeem":
			f.mu.Lock()
			f.redeemed = true
			f.redeems++
			f.mu.Unlock()
			writeTestJSON(w, map[string]any{"credential": "redeemed-tok", "expires_in": 600})
		case "/complete":
			f.complete <- body
			w.WriteHeader(http.StatusAccepted)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(f.srv.Close)
	return f
}

func writeTestJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

// postSendTask posts a JSON-RPC message/send with the given AEI_* metadata to the
// actor, mirroring the agent-substrate provider's dispatch, and returns the decoded
// ack. It fails the test on a non-200 (the provider requires 200 for both result
// and error).
func postSendTask(t *testing.T, url string, metadata map[string]string) rpcResponse {
	t.Helper()
	body, _ := json.Marshal(map[string]any{
		"jsonrpc": "2.0",
		"id":      "req-1",
		"method":  "message/send",
		"params": map[string]any{
			"message": map[string]any{
				"role":     "user",
				"kind":     "message",
				"parts":    []map[string]string{{"kind": "text", "text": "Run AEI agent"}},
				"metadata": metadata,
			},
			"configuration": map[string]any{"blocking": false},
		},
	})
	resp, err := http.Post(url, "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("status %d: %s", resp.StatusCode, b)
	}
	var out rpcResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	return out
}

// TestServeAcceptsTaskRedeemsAndCompletes: a well-formed message/send on the pull
// path is acked as accepted, the ticket is redeemed, fn runs with the decoded spec,
// and completion is reported out-of-band — the durable mirror of the one-shot Main.
func TestServeAcceptsTaskRedeemsAndCompletes(t *testing.T) {
	cp := newFakeControl(t)
	ran := make(chan RunSpec, 1)
	fn := func(_ context.Context, r *Runtime) error {
		ran <- r.Spec()
		return nil
	}
	srv := httptest.NewServer(actorMux(func(string) string { return "" }, fn, nil))
	defer srv.Close()

	resp := postSendTask(t, srv.URL, map[string]string{
		EnvRunID:      "run-durable-1",
		EnvControlURL: cp.srv.URL,
		EnvTicket:     "ticket-xyz",
		EnvParams:     `{"kind":"converse"}`,
	})

	if resp.Result == nil || resp.Result.Status.State != stateSubmitted {
		t.Fatalf("ack = %+v, want state %q", resp.Result, stateSubmitted)
	}
	if resp.Result.ID != "run-durable-1" {
		t.Fatalf("task id = %q, want run-durable-1", resp.Result.ID)
	}

	select {
	case spec := <-ran:
		if spec.RunID != "run-durable-1" || spec.Parameters["kind"] != "converse" {
			t.Fatalf("fn spec = %+v", spec)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("fn did not run")
	}

	select {
	case body := <-cp.complete:
		if body["run_id"] != "run-durable-1" {
			t.Fatalf("complete run_id = %v, want run-durable-1", body["run_id"])
		}
	case <-time.After(2 * time.Second):
		t.Fatal("completion did not land")
	}

	cp.mu.Lock()
	defer cp.mu.Unlock()
	if !cp.redeemed {
		t.Fatal("pull path: ticket was not redeemed")
	}
}

// TestServeRejectsMalformedTask: a request whose metadata cannot Load (here, no
// control URL) is rejected synchronously with stateFailed and a message, and fn
// never runs — so no late/duplicate completion for a task we never accepted.
func TestServeRejectsMalformedTask(t *testing.T) {
	var ran bool
	fn := func(context.Context, *Runtime) error { ran = true; return nil }
	srv := httptest.NewServer(actorMux(func(string) string { return "" }, fn, nil))
	defer srv.Close()

	resp := postSendTask(t, srv.URL, map[string]string{EnvRunID: "run-x"}) // missing AEI_CONTROL_URL
	if resp.Result == nil || resp.Result.Status.State != stateFailed {
		t.Fatalf("ack = %+v, want state %q", resp.Result, stateFailed)
	}
	if resp.Result.Status.Message == nil || len(resp.Result.Status.Message.Parts) == 0 {
		t.Fatal("rejection should carry a message the provider can surface")
	}
	if ran {
		t.Fatal("fn must not run for a rejected task")
	}
}

// TestServeUnknownMethod: a non-message/send method is a JSON-RPC method-not-found
// error (still HTTP 200 so the provider decodes the error body).
func TestServeUnknownMethod(t *testing.T) {
	srv := httptest.NewServer(actorMux(func(string) string { return "" }, func(context.Context, *Runtime) error { return nil }, nil))
	defer srv.Close()

	body, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": "1", "method": "tasks/cancel", "params": map[string]any{}})
	resp, err := http.Post(srv.URL, "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status %d, want 200", resp.StatusCode)
	}
	var out rpcResponse
	_ = json.NewDecoder(resp.Body).Decode(&out)
	if out.Error == nil || out.Error.Code != -32601 {
		t.Fatalf("want method-not-found (-32601), got %+v", out)
	}
}

// TestServeHealthProbe: a GET answers 200 so the router can detect a resumed actor.
func TestServeHealthProbe(t *testing.T) {
	srv := httptest.NewServer(actorMux(func(string) string { return "" }, func(context.Context, *Runtime) error { return nil }, nil))
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("health GET = %d, want 200", resp.StatusCode)
	}
}

// TestServeShedsAtCapacity: with a concurrency cap of 1, a first run fills the slot
// and blocks; a second task is shed with a retryable 503 and — crucially — its
// single-use ticket is never redeemed, so the provider's retry can still redeem it.
func TestServeShedsAtCapacity(t *testing.T) {
	cp := newFakeControl(t)
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	fn := func(_ context.Context, _ *Runtime) error {
		started <- struct{}{}
		<-release // hold the only slot until released
		return nil
	}
	srv := httptest.NewServer(actorMux(func(string) string { return "" }, fn, newSemaphore(1)))
	defer srv.Close()

	// First task takes the only slot and blocks in fn.
	resp := postSendTask(t, srv.URL, map[string]string{
		EnvRunID:      "run-1",
		EnvControlURL: cp.srv.URL,
		EnvTicket:     "ticket-1",
	})
	if resp.Result == nil || resp.Result.Status.State != stateSubmitted {
		t.Fatalf("first ack = %+v, want submitted", resp.Result)
	}
	<-started // fn is running, holding the slot

	// Second task is shed: a plain HTTP 503 (not a JSON-RPC ack), so the provider
	// treats it as retryable — the same signal as a cold-start "no free workers".
	body, _ := json.Marshal(map[string]any{
		"jsonrpc": "2.0", "id": "req-2", "method": "message/send",
		"params": map[string]any{"message": map[string]any{
			"metadata": map[string]string{EnvRunID: "run-2", EnvControlURL: cp.srv.URL, EnvTicket: "ticket-2"},
		}},
	})
	r2, err := http.Post(srv.URL, "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("post 2: %v", err)
	}
	r2.Body.Close()
	if r2.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("shed task status = %d, want 503", r2.StatusCode)
	}

	close(release) // let the first run finish and free the slot
	select {
	case <-cp.complete:
	case <-time.After(2 * time.Second):
		t.Fatal("first run never completed")
	}

	cp.mu.Lock()
	defer cp.mu.Unlock()
	if cp.redeems != 1 {
		t.Fatalf("redeems = %d, want 1 — the shed task must not spend its ticket", cp.redeems)
	}
}

// TestMaxConcurrentRuns: the cap parses from AEI_MAX_CONCURRENT_RUNS, with unset,
// non-numeric, and negative all meaning unbounded (0).
func TestMaxConcurrentRuns(t *testing.T) {
	cases := map[string]int{"": 0, "abc": 0, "-3": 0, "0": 0, "5": 5}
	for in, want := range cases {
		if got := maxConcurrentRuns(func(string) string { return in }); got != want {
			t.Errorf("maxConcurrentRuns(%q) = %d, want %d", in, got, want)
		}
	}
}
