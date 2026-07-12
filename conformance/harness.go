package conformance

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"sync"
	"time"

	"github.com/jackfrancis/agent-execution-interface/aei"
)

// RuntimeCmd is an external runtime to drive through the AEI wire: any executable
// — a Go binary, a shell script, a Python program — that reads the AEI_*
// environment and speaks the /redeem · /complete · /vend JSON routes. Argv[0] is
// the program and the rest are its arguments; the harness appends the AEI_* env
// (the env transport, docs/runtime-abi.md §1.2). It carries no Go dependency on
// the runtime, so a runtime in ANY language is drivable — that is the point.
type RuntimeCmd struct {
	Argv []string // e.g. {"sh", "runtime.sh"} or {"./mybin"} or {"python3", "runtime.py"}
	Env  []string // extra static env (the runtime's "frozen" config); AEI_* are added
	Dir  string   // working directory (optional)
}

// Observed is what the stub control plane + broker saw the runtime do — the only
// externally visible evidence of conformance, since the run credential is
// otherwise private to the runtime. Verify checks it against the standard
// invariants.
type Observed struct {
	Completed        bool
	CompleteAuth     string // Authorization header on /complete
	CompleteRunID    string
	CompleteError    string
	Redeemed         bool
	RedeemTicket     string
	RedeemAuth       string // MUST be empty: the ticket is the authorization
	MintedCredential string // what /redeem returned (pull path), for bearer comparison
	Vended           bool
	VendAuth         string
	ExitErr          error // non-nil if the process failed to run or exited non-zero
	Stdout, Stderr   string
}

// Drive injects a RunSpec + credential onto the AEI wire as environment, launches
// the runtime, waits for it to report completion (bounded by ctx), and returns
// what the stub control plane observed. It is the language-agnostic conformance
// primitive: it asks nothing of the runtime but that it read AEI_* and speak the
// wire. The broker collapses onto the control URL here (B-with-default); split
// broker routing is covered by the Go-side wire test.
func Drive(ctx context.Context, cmd RuntimeCmd, spec aei.RunSpec, cred aei.Credential) (Observed, error) {
	if len(cmd.Argv) == 0 {
		return Observed{}, fmt.Errorf("conformance: RuntimeCmd.Argv is empty")
	}
	rec := &wireRecorder{}
	srv := httptest.NewServer(rec.handler())
	defer srv.Close()

	// Encode on the dispatch side; the control URL points at the stub so the
	// runtime's callbacks land where we can observe them.
	env := aei.Injection(spec, cred, srv.URL, "", time.Now())

	c := exec.CommandContext(ctx, cmd.Argv[0], cmd.Argv[1:]...)
	c.Dir = cmd.Dir
	c.Env = append(append(os.Environ(), cmd.Env...), asEnv(env)...)
	var stdout, stderr bytes.Buffer
	c.Stdout, c.Stderr = &stdout, &stderr

	runErr := c.Run()

	o := rec.snapshot()
	o.ExitErr = runErr
	o.Stdout, o.Stderr = stdout.String(), stderr.String()
	return o, nil
}

// Verify checks the standard conformance invariants for a run driven with cred:
// the runtime exited cleanly and completed with the correct run id; on the pull
// path it redeemed the ticket carrying NO bearer (the ticket is the auth) and
// then authorized completion with the redeemed credential; on the push path it
// authorized completion with the injected token and redeemed nothing. Any AEI
// runtime, in any language, must satisfy this.
func (o Observed) Verify(spec aei.RunSpec, cred aei.Credential) error {
	if o.ExitErr != nil {
		return fmt.Errorf("runtime failed to run: %v (stderr: %q)", o.ExitErr, o.Stderr)
	}
	if !o.Completed {
		return fmt.Errorf("runtime did not POST /complete (stderr: %q)", o.Stderr)
	}
	if o.CompleteRunID != spec.ID {
		return fmt.Errorf("/complete run_id = %q, want %q", o.CompleteRunID, spec.ID)
	}
	wantBearer := "Bearer " + cred.Value
	switch cred.Kind {
	case aei.Ticket:
		if !o.Redeemed {
			return fmt.Errorf("pull path: runtime did not redeem the ticket at /redeem")
		}
		if o.RedeemTicket != cred.Value {
			return fmt.Errorf("/redeem ticket = %q, want %q", o.RedeemTicket, cred.Value)
		}
		if o.RedeemAuth != "" {
			return fmt.Errorf("/redeem carried a bearer %q, but the ticket is the authorization", o.RedeemAuth)
		}
		wantBearer = "Bearer " + o.MintedCredential
	default:
		if o.Redeemed {
			return fmt.Errorf("push path: runtime redeemed a ticket it was never given")
		}
	}
	if o.CompleteAuth != wantBearer {
		return fmt.Errorf("/complete Authorization = %q, want %q", o.CompleteAuth, wantBearer)
	}
	return nil
}

// wireRecorder is a stub control plane + broker: it serves the three runtime ABI
// routes and records what the runtime sent, minting a fresh credential on /redeem
// so the pull path's completion bearer can be checked against it.
type wireRecorder struct {
	mu sync.Mutex
	o  Observed
}

func (rec *wireRecorder) snapshot() Observed {
	rec.mu.Lock()
	defer rec.mu.Unlock()
	return rec.o
}

func (rec *wireRecorder) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/complete", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			RunID string `json:"run_id"`
			Error string `json:"error"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		rec.mu.Lock()
		rec.o.Completed = true
		rec.o.CompleteAuth = r.Header.Get("Authorization")
		rec.o.CompleteRunID = body.RunID
		rec.o.CompleteError = body.Error
		rec.mu.Unlock()
		w.WriteHeader(http.StatusAccepted)
	})
	mux.HandleFunc("/redeem", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Ticket string `json:"ticket"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		minted := "run-cred-" + randHex()
		rec.mu.Lock()
		rec.o.Redeemed = true
		rec.o.RedeemTicket = body.Ticket
		rec.o.RedeemAuth = r.Header.Get("Authorization")
		rec.o.MintedCredential = minted
		rec.mu.Unlock()
		writeJSON(w, map[string]any{"credential": minted, "expires_in": 600})
	})
	mux.HandleFunc("/vend", func(w http.ResponseWriter, r *http.Request) {
		rec.mu.Lock()
		rec.o.Vended = true
		rec.o.VendAuth = r.Header.Get("Authorization")
		rec.mu.Unlock()
		writeJSON(w, map[string]any{"credential": "vended-" + randHex(), "token_type": "Bearer", "expires_in": 300})
	})
	return mux
}

func asEnv(m map[string]string) []string {
	out := make([]string, 0, len(m))
	for k, v := range m {
		out = append(out, k+"="+v)
	}
	return out
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

func randHex() string {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}
