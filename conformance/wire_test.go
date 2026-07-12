package conformance

import (
	"context"
	"encoding/json"
	"maps"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/jackfrancis/agent-execution-interface/aei"
	"github.com/jackfrancis/agent-execution-interface/aeiruntime"
)

// perRunKeys are the AEI_* keys a durable (A2A) provider forwards in the message
// metadata; everything else is config a standing runtime reads from its own
// (frozen) environment (docs/runtime-abi.md §1.2). Used to model the metadata
// transport faithfully — per-run in metadata, config in env.
var perRunKeys = map[string]bool{
	aei.EnvRunID:        true,
	aei.EnvTaskRef:      true,
	aei.EnvParams:       true,
	aei.EnvDispatchedAt: true,
	aei.EnvDeadline:     true,
	aei.EnvCredential:   true,
	aei.EnvTicket:       true,
}

func getenvFrom(m map[string]string) func(string) string {
	return func(k string) string { return m[k] }
}

// split partitions an injection map into the per-run subset (rides A2A metadata)
// and the config subset (the durable runtime's env).
func split(m map[string]string) (perRun, config map[string]string) {
	perRun, config = map[string]string{}, map[string]string{}
	for k, v := range m {
		if perRunKeys[k] {
			perRun[k] = v
		} else {
			config[k] = v
		}
	}
	return perRun, config
}

// recorder is a stub control plane + broker: it serves the three runtime ABI
// routes and records what the runtime sent, so a test can observe the otherwise
// in-memory-only run credential via the bearer it presents.
type recorder struct {
	mu            sync.Mutex
	completeAuth  string
	completeRunID string
	redeemTicket  string
	redeemAuth    string
	vendAuth      string
}

type calls struct {
	completeAuth, completeRunID string
	redeemTicket, redeemAuth    string
	vendAuth                    string
}

func (rec *recorder) calls() calls {
	rec.mu.Lock()
	defer rec.mu.Unlock()
	return calls{rec.completeAuth, rec.completeRunID, rec.redeemTicket, rec.redeemAuth, rec.vendAuth}
}

func (rec *recorder) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/complete", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			RunID string `json:"run_id"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		rec.mu.Lock()
		rec.completeAuth = r.Header.Get("Authorization")
		rec.completeRunID = body.RunID
		rec.mu.Unlock()
		w.WriteHeader(http.StatusAccepted)
	})
	mux.HandleFunc("/redeem", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Ticket string `json:"ticket"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		rec.mu.Lock()
		rec.redeemTicket = body.Ticket
		rec.redeemAuth = r.Header.Get("Authorization")
		rec.mu.Unlock()
		writeJSON(w, map[string]any{"credential": "redeemed-" + body.Ticket, "expires_in": 600})
	})
	mux.HandleFunc("/vend", func(w http.ResponseWriter, r *http.Request) {
		rec.mu.Lock()
		rec.vendAuth = r.Header.Get("Authorization")
		rec.mu.Unlock()
		writeJSON(w, map[string]any{"credential": "vended", "token_type": "Bearer", "expires_in": 300})
	})
	return mux
}

// TestInjectionDecodeRoundTrip pins the wire: a RunSpec encoded by the dispatch
// side (aei.Injection) decodes to the identical RunSpec on the runtime side
// (aeiruntime.Load), over BOTH transports — env (workload provider) and message
// metadata (durable provider) — so the same runtime binary cannot tell which
// substrate launched it (the portability invariant, runtime-abi.md §1.3). The
// pushed run credential is read back via the bearer on /complete, which is the
// only externally observable proof it resolved.
func TestInjectionDecodeRoundTrip(t *testing.T) {
	ctx := context.Background()
	dispatchedAt := time.UnixMilli(1_789_000_000_000) // ms-aligned so the round trip is exact
	spec := aei.RunSpec{
		ID:         "run-abc",
		TaskRef:    "worklist://item/pr-4127",
		Parameters: map[string]string{"kind": "converse", "itemId": "pr-4127", "provider": "github"},
		Identity:   aei.IdentityRequest{Audience: "team-a-agent"},
		Deadline:   15 * time.Minute,
	}
	cred := aei.Credential{Value: "run-token-xyz", Kind: aei.Token}

	transports := []struct {
		name string
		// route splits an injection map into (getenv, metadata) for the decoder.
		route func(m map[string]string) (func(string) string, map[string]string)
	}{
		{"env transport (workload provider)", func(m map[string]string) (func(string) string, map[string]string) {
			return getenvFrom(m), nil // a workload reads everything from env; no metadata
		}},
		{"metadata transport (durable provider)", func(m map[string]string) (func(string) string, map[string]string) {
			perRun, config := split(m) // per-run rides A2A metadata; config from the actor's env
			return getenvFrom(config), perRun
		}},
	}

	for _, tc := range transports {
		t.Run(tc.name, func(t *testing.T) {
			rec := &recorder{}
			srv := httptest.NewServer(rec.handler())
			defer srv.Close()

			// Encode on the dispatch side; control URL points at the stub so the
			// decoded runtime's /complete lands where we can read its bearer.
			m := aei.Injection(spec, cred, srv.URL, "", dispatchedAt)

			// The push/pull credential channel is exclusive on the wire (§1.1).
			if m[aei.EnvCredential] == "" || m[aei.EnvTicket] != "" {
				t.Fatalf("push injection: want %s set and %s empty; got cred=%q ticket=%q",
					aei.EnvCredential, aei.EnvTicket, m[aei.EnvCredential], m[aei.EnvTicket])
			}

			// Decode on the runtime side, through this transport.
			getenv, metadata := tc.route(m)
			rt, err := aeiruntime.Load(ctx, getenv, metadata)
			if err != nil {
				t.Fatalf("Load: %v", err)
			}

			got := rt.Spec()
			if got.RunID != spec.ID || got.TaskRef != spec.TaskRef {
				t.Fatalf("ids round trip: got RunID=%q TaskRef=%q, want %q / %q",
					got.RunID, got.TaskRef, spec.ID, spec.TaskRef)
			}
			if !maps.Equal(got.Parameters, spec.Parameters) {
				t.Fatalf("params round trip: got %v, want %v", got.Parameters, spec.Parameters)
			}
			if !got.DispatchedAt.Equal(dispatchedAt) {
				t.Fatalf("dispatchedAt round trip: got %v, want %v", got.DispatchedAt, dispatchedAt)
			}
			if want := dispatchedAt.Add(spec.Deadline); !got.Deadline.Equal(want) {
				t.Fatalf("deadline round trip: got %v, want %v", got.Deadline, want)
			}

			// The credential resolved on the runtime side must be the one dispatch
			// pushed — observed via the bearer on /complete (it is otherwise private).
			if err := rt.Complete(ctx, nil, aeiruntime.Timing{RuntimeSeconds: 1}, "done"); err != nil {
				t.Fatalf("Complete: %v", err)
			}
			c := rec.calls()
			if want := "Bearer " + cred.Value; c.completeAuth != want {
				t.Fatalf("complete bearer: got %q, want %q (the pushed credential)", c.completeAuth, want)
			}
			if c.completeRunID != spec.ID {
				t.Fatalf("complete run_id: got %q, want %q", c.completeRunID, spec.ID)
			}
		})
	}
}

// TestTicketPullRedeemRoundTrip covers the pull path: a Ticket credential is
// injected as AEI_RUN_TICKET (never AEI_RUN_CREDENTIAL), and Load redeems it at
// {broker}/redeem — carrying NO bearer, since the ticket is itself the
// authorization — before returning, so the runtime authorizes /complete with the
// redeemed credential, not the ticket (runtime-abi.md §2.1).
func TestTicketPullRedeemRoundTrip(t *testing.T) {
	ctx := context.Background()
	dispatchedAt := time.UnixMilli(1_789_000_000_000)
	spec := aei.RunSpec{ID: "run-pull", TaskRef: "worklist://x", Parameters: map[string]string{"k": "v"}, Deadline: time.Minute}
	cred := aei.Credential{Value: "tkt-once", Kind: aei.Ticket}

	rec := &recorder{}
	srv := httptest.NewServer(rec.handler())
	defer srv.Close()

	m := aei.Injection(spec, cred, srv.URL, "", dispatchedAt)
	if m[aei.EnvTicket] == "" || m[aei.EnvCredential] != "" {
		t.Fatalf("pull injection: want %s set and %s empty; got ticket=%q cred=%q",
			aei.EnvTicket, aei.EnvCredential, m[aei.EnvTicket], m[aei.EnvCredential])
	}

	// Load must redeem the ticket before returning.
	rt, err := aeiruntime.Load(ctx, getenvFrom(m), nil)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	c := rec.calls()
	if c.redeemTicket != cred.Value {
		t.Fatalf("redeem ticket: got %q, want %q", c.redeemTicket, cred.Value)
	}
	if c.redeemAuth != "" {
		t.Fatalf("redeem must carry no bearer (the ticket is the auth); got %q", c.redeemAuth)
	}

	// The redeemed credential — not the ticket — authorizes the callback.
	if err := rt.Complete(ctx, nil, aeiruntime.Timing{RuntimeSeconds: 1}, ""); err != nil {
		t.Fatalf("Complete: %v", err)
	}
	c = rec.calls()
	if want := "Bearer redeemed-" + cred.Value; c.completeAuth != want {
		t.Fatalf("complete bearer: got %q, want %q (redeemed credential, not the ticket)", c.completeAuth, want)
	}
}

// TestBrokerSplitRouting covers B-with-default: when the broker URL differs from
// the control URL, Injection emits AEI_BROKER_URL and the runtime sends the
// identity routes (/redeem, /vend) to the broker while /complete stays on the
// control plane — the wire mirror of the Contract 1 / Contract 3 boundary
// (runtime-abi.md §2).
func TestBrokerSplitRouting(t *testing.T) {
	ctx := context.Background()
	dispatchedAt := time.UnixMilli(1_789_000_000_000)
	spec := aei.RunSpec{ID: "run-split", Deadline: time.Minute}
	cred := aei.Credential{Value: "tkt-split", Kind: aei.Ticket}

	controlRec, brokerRec := &recorder{}, &recorder{}
	control := httptest.NewServer(controlRec.handler())
	defer control.Close()
	broker := httptest.NewServer(brokerRec.handler())
	defer broker.Close()

	m := aei.Injection(spec, cred, control.URL, broker.URL, dispatchedAt)
	if m[aei.EnvBrokerURL] != broker.URL {
		t.Fatalf("broker url on the wire: got %q, want %q", m[aei.EnvBrokerURL], broker.URL)
	}

	// Load redeems the ticket — which must hit the broker, not the control plane.
	rt, err := aeiruntime.Load(ctx, getenvFrom(m), nil)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if got := brokerRec.calls().redeemTicket; got != cred.Value {
		t.Fatalf("redeem should hit the broker; broker saw ticket=%q, want %q", got, cred.Value)
	}
	if got := controlRec.calls().redeemTicket; got != "" {
		t.Fatalf("redeem must not hit the control plane; it saw ticket=%q", got)
	}

	// Vend hits the broker; Complete hits the control plane — both with the
	// redeemed credential.
	if _, err := rt.Vend(ctx, "github"); err != nil {
		t.Fatalf("Vend: %v", err)
	}
	if err := rt.Complete(ctx, nil, aeiruntime.Timing{RuntimeSeconds: 1}, ""); err != nil {
		t.Fatalf("Complete: %v", err)
	}
	want := "Bearer redeemed-" + cred.Value
	if got := brokerRec.calls().vendAuth; got != want {
		t.Fatalf("vend should hit the broker with the redeemed credential; got %q, want %q", got, want)
	}
	if got := controlRec.calls().completeAuth; got != want {
		t.Fatalf("complete should hit the control plane with the redeemed credential; got %q, want %q", got, want)
	}
	if got := brokerRec.calls().completeAuth; got != "" {
		t.Fatalf("complete must not hit the broker; it saw bearer=%q", got)
	}
}
