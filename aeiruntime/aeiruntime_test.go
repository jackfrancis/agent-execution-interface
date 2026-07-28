package aeiruntime_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	rt "github.com/jackfrancis/agent-execution-interface/aeiruntime"
)

// envFunc turns a map into a getenv function.
func envFunc(m map[string]string) func(string) string {
	return func(k string) string { return m[k] }
}

// fakeCP is a fake control plane + broker: it records the auth header and JSON
// body seen on /redeem, /vend, and /complete, and returns canned responses.
type fakeCP struct {
	srv *httptest.Server

	mu           sync.Mutex
	redeemAuth   string
	redeemBody   map[string]any
	vendAuth     string
	vendBody     map[string]any
	completeAuth string
	completeBody map[string]any
}

func newFakeCP(t *testing.T) *fakeCP {
	t.Helper()
	f := &fakeCP{}
	f.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body := map[string]any{}
		_ = json.NewDecoder(r.Body).Decode(&body)
		f.mu.Lock()
		defer f.mu.Unlock()
		switch r.URL.Path {
		case "/redeem":
			f.redeemAuth, f.redeemBody = r.Header.Get("Authorization"), body
			writeJSON(w, map[string]any{"credential": "redeemed-tok", "expires_in": 600})
		case "/vend":
			f.vendAuth, f.vendBody = r.Header.Get("Authorization"), body
			writeJSON(w, map[string]any{"credential": "vended-tok", "token_type": "Bearer", "expires_in": 300})
		case "/complete":
			f.completeAuth, f.completeBody = r.Header.Get("Authorization"), body
			w.WriteHeader(http.StatusAccepted)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(f.srv.Close)
	return f
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

// TestLoadDecodesPushEnv: env transport, pushed credential. The spec decodes and
// the broker defaults to the control URL.
func TestLoadDecodesPushEnv(t *testing.T) {
	cp := newFakeCP(t)
	env := envFunc(map[string]string{
		rt.EnvRunID:        "run-abc",
		rt.EnvTaskRef:      "worklist://item/pr-4127",
		rt.EnvParams:       `{"kind":"converse","itemId":"pr-4127"}`,
		rt.EnvDispatchedAt: "1789000000000",
		rt.EnvCredential:   "push-tok",
		rt.EnvControlURL:   cp.srv.URL,
	})

	r, err := rt.Load(context.Background(), env, nil)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	spec := r.Spec()
	if spec.RunID != "run-abc" || spec.TaskRef != "worklist://item/pr-4127" {
		t.Fatalf("spec ids = %+v", spec)
	}
	if spec.Parameters["kind"] != "converse" || spec.Parameters["itemId"] != "pr-4127" {
		t.Fatalf("params = %v", spec.Parameters)
	}
	if spec.DispatchedAt.UnixMilli() != 1789000000000 {
		t.Fatalf("dispatchedAt = %v", spec.DispatchedAt)
	}
	// A push credential must not trigger a redeem.
	cp.mu.Lock()
	defer cp.mu.Unlock()
	if cp.redeemBody != nil {
		t.Fatal("push path must not call /redeem")
	}
}

// TestDecoderMetadataFirst: a key present in metadata wins over env; an empty
// metadata value falls back to env (does not shadow it).
func TestDecoderMetadataFirst(t *testing.T) {
	cp := newFakeCP(t)
	env := envFunc(map[string]string{
		rt.EnvRunID:      "env-run",
		rt.EnvTaskRef:    "env-task",
		rt.EnvCredential: "tok",
		rt.EnvControlURL: cp.srv.URL,
	})
	metadata := map[string]string{
		rt.EnvRunID:   "meta-run", // wins over env
		rt.EnvTaskRef: "",         // empty ⇒ does not shadow env
	}

	r, err := rt.Load(context.Background(), env, metadata)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if got := r.Spec().RunID; got != "meta-run" {
		t.Errorf("RunID = %q, want meta-run (metadata wins)", got)
	}
	if got := r.Spec().TaskRef; got != "env-task" {
		t.Errorf("TaskRef = %q, want env-task (empty metadata falls back)", got)
	}
}

// TestLoadRedeemsTicket: pull transport. Load redeems the ticket (no bearer, body
// carries the ticket) and uses the redeemed credential for later calls.
func TestLoadRedeemsTicket(t *testing.T) {
	cp := newFakeCP(t)
	env := envFunc(map[string]string{
		rt.EnvRunID:      "run-pull",
		rt.EnvTicket:     "tkt-once",
		rt.EnvControlURL: cp.srv.URL,
	})

	r, err := rt.Load(context.Background(), env, nil)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	cp.mu.Lock()
	if cp.redeemBody["ticket"] != "tkt-once" {
		t.Errorf("redeem body = %v, want ticket tkt-once", cp.redeemBody)
	}
	if cp.redeemAuth != "" {
		t.Errorf("redeem must carry no bearer, got %q", cp.redeemAuth)
	}
	cp.mu.Unlock()

	// The redeemed credential is what Complete now presents.
	if err := r.Complete(context.Background(), nil, rt.Timing{}, ""); err != nil {
		t.Fatalf("Complete: %v", err)
	}
	cp.mu.Lock()
	defer cp.mu.Unlock()
	if cp.completeAuth != "Bearer redeemed-tok" {
		t.Errorf("complete auth = %q, want Bearer redeemed-tok", cp.completeAuth)
	}
}

// TestCompleteSendsOutcome: run id, error detail, result_ref, and timing ride the
// completion body; the run credential is the bearer.
func TestCompleteSendsOutcome(t *testing.T) {
	cp := newFakeCP(t)
	env := envFunc(map[string]string{
		rt.EnvRunID:      "run-c",
		rt.EnvCredential: "push-tok",
		rt.EnvControlURL: cp.srv.URL,
	})
	r, err := rt.Load(context.Background(), env, nil)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	timing := rt.Timing{ProvisioningSeconds: 0.6, RuntimeSeconds: 64.6, ModelCalls: 3}
	if err := r.Complete(context.Background(), errors.New("boom"), timing, "worklist://item/pr-4127"); err != nil {
		t.Fatalf("Complete: %v", err)
	}

	cp.mu.Lock()
	defer cp.mu.Unlock()
	if cp.completeAuth != "Bearer push-tok" {
		t.Errorf("auth = %q", cp.completeAuth)
	}
	if cp.completeBody["run_id"] != "run-c" {
		t.Errorf("run_id = %v", cp.completeBody["run_id"])
	}
	if cp.completeBody["error"] != "boom" {
		t.Errorf("error = %v, want boom", cp.completeBody["error"])
	}
	if cp.completeBody["result_ref"] != "worklist://item/pr-4127" {
		t.Errorf("result_ref = %v", cp.completeBody["result_ref"])
	}
	tm, ok := cp.completeBody["timing"].(map[string]any)
	if !ok || tm["provisioning_seconds"].(float64) != 0.6 {
		t.Errorf("timing = %v", cp.completeBody["timing"])
	}
}

// TestVendSendsProviderWithBearer: /vend carries the provider and the run
// credential, and returns the vended credential.
func TestVendSendsProviderWithBearer(t *testing.T) {
	cp := newFakeCP(t)
	env := envFunc(map[string]string{
		rt.EnvRunID:      "run-v",
		rt.EnvCredential: "push-tok",
		rt.EnvControlURL: cp.srv.URL,
	})
	r, err := rt.Load(context.Background(), env, nil)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	cred, err := r.Vend(context.Background(), "github")
	if err != nil {
		t.Fatalf("Vend: %v", err)
	}
	if cred.Value != "vended-tok" || cred.ExpiresIn != 300 {
		t.Errorf("vended = %+v", cred)
	}
	cp.mu.Lock()
	defer cp.mu.Unlock()
	if cp.vendAuth != "Bearer push-tok" {
		t.Errorf("vend auth = %q", cp.vendAuth)
	}
	if cp.vendBody["provider"] != "github" {
		t.Errorf("vend provider = %v", cp.vendBody["provider"])
	}
}

// TestBrokerSplit: with AEI_BROKER_URL set, /redeem and /vend hit the broker while
// /complete hits the control plane (the B-with-default split).
func TestBrokerSplit(t *testing.T) {
	control := newFakeCP(t)
	broker := newFakeCP(t)
	env := envFunc(map[string]string{
		rt.EnvRunID:      "run-split",
		rt.EnvTicket:     "tkt-once",
		rt.EnvControlURL: control.srv.URL,
		rt.EnvBrokerURL:  broker.srv.URL,
	})

	r, err := rt.Load(context.Background(), env, nil) // redeems at the broker
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if _, err := r.Vend(context.Background(), "github"); err != nil {
		t.Fatalf("Vend: %v", err)
	}
	if err := r.Complete(context.Background(), nil, rt.Timing{}, ""); err != nil {
		t.Fatalf("Complete: %v", err)
	}

	broker.mu.Lock()
	defer broker.mu.Unlock()
	control.mu.Lock()
	defer control.mu.Unlock()
	if broker.redeemBody == nil || broker.vendBody == nil {
		t.Error("redeem + vend must hit the broker")
	}
	if control.redeemBody != nil || control.vendBody != nil {
		t.Error("redeem + vend must NOT hit the control plane")
	}
	if control.completeBody == nil {
		t.Error("complete must hit the control plane")
	}
	if broker.completeBody != nil {
		t.Error("complete must NOT hit the broker")
	}
}

// TestLoadRequiresRunIDAndControlURL: structural keys are mandatory.
func TestLoadRequiresRunIDAndControlURL(t *testing.T) {
	cp := newFakeCP(t)
	if _, err := rt.Load(context.Background(), envFunc(map[string]string{rt.EnvControlURL: cp.srv.URL}), nil); err == nil {
		t.Error("missing AEI_RUN_ID must fail")
	}
	if _, err := rt.Load(context.Background(), envFunc(map[string]string{rt.EnvRunID: "x"}), nil); err == nil {
		t.Error("missing AEI_CONTROL_URL must fail")
	}
}
