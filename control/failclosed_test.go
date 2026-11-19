package control

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/jackfrancis/agent-execution-interface/aei"
)

func mustToken(t *testing.T, p *Plane, c Claims) string {
	t.Helper()
	tok, err := p.mintToken(c)
	if err != nil {
		t.Fatalf("mintToken: %v", err)
	}
	return tok
}

func mustMint(t *testing.T, p *Plane, l aei.Launcher, spec aei.RunSpec) aei.Credential {
	t.Helper()
	cred, err := p.mint(l, spec)
	if err != nil {
		t.Fatalf("mint: %v", err)
	}
	return cred
}

// failingAuthority cannot mint (e.g. a broken or unreachable signing key).
type failingAuthority struct{ empty bool }

func (a failingAuthority) Mint(Claims) (string, error) {
	if a.empty {
		return "", nil // a buggy authority: no error, but no token either
	}
	return "", errors.New("signing key unavailable")
}
func (failingAuthority) Verify(string) (Claims, bool) { return Claims{}, false }

// countingLauncher records whether the plane dispatched at all.
type countingLauncher struct{ launched int }

func (l *countingLauncher) Launch(context.Context, aei.RunSpec, aei.Credential) (aei.Handle, error) {
	l.launched++
	return aei.Handle{}, nil
}

// An unkeyed HMAC authority must not mint (an empty key is forgeable) nor verify.
func TestHMACAuthorityWithoutSecretFailsClosed(t *testing.T) {
	a := hmacAuthority{now: time.Now}
	if tok, err := a.Mint(Claims{RunID: "r", Exp: time.Now().Add(time.Hour).Unix()}); err == nil {
		t.Fatalf("unkeyed HMAC minted %q; want an error", tok)
	}
	other := hmacAuthority{secret: []byte("k"), now: time.Now}
	tok, _ := other.Mint(Claims{RunID: "r", Exp: time.Now().Add(time.Hour).Unix()})
	if _, ok := a.Verify(tok); ok {
		t.Fatal("unkeyed HMAC verified a token")
	}
}

// Run (the AgentRun path): a mint error fails the run and never dispatches it.
func TestRunFailsWhenMintFails(t *testing.T) {
	for _, tc := range []struct {
		name string
		cfg  Config
	}{
		{"authority error", Config{Authority: failingAuthority{}}},
		{"empty token", Config{Authority: failingAuthority{empty: true}}},
		{"no secret", Config{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := New(tc.cfg)
			l := &countingLauncher{}
			res, err := p.Run(context.Background(), l, aei.RunSpec{ID: "run-m"})
			if err == nil || res.Err == nil {
				t.Fatalf("Run = (%+v, %v); want a mint error", res, err)
			}
			if !strings.Contains(res.Err.Error(), "mint run credential") {
				t.Errorf("error %q should name the mint failure", res.Err)
			}
			if l.launched != 0 {
				t.Errorf("launched %d times; a run without a credential must not dispatch", l.launched)
			}
		})
	}
}

// recordingObserver captures terminal phases.
type recordingObserver struct{ phases []string }

func (o *recordingObserver) ObserveRun(_ string, phase string, _ aei.Timing) {
	o.phases = append(o.phases, phase)
}

// Submit (the data-plane path): a mint error records the run Failed, visible at
// GET /runs/{id}, and never dispatches it.
func TestSubmitMarksRunFailedWhenMintFails(t *testing.T) {
	obs := &recordingObserver{}
	p := New(Config{Authority: failingAuthority{}, Observer: obs})
	l := &countingLauncher{}
	id, err := p.Submit(context.Background(), l, aei.RunSpec{ID: "run-s"})
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}
	st, ok := p.Status(id)
	if !ok || st.Phase != phaseFailed {
		t.Fatalf("status = %+v (known=%v); want Failed", st, ok)
	}
	if !strings.Contains(st.Err, "signing key unavailable") {
		t.Errorf("status error %q should carry the mint failure", st.Err)
	}
	if l.launched != 0 {
		t.Errorf("launched %d times; a run without a credential must not dispatch", l.launched)
	}
	if len(obs.phases) != 1 || obs.phases[0] != phaseFailed {
		t.Errorf("observed phases %v; want [Failed]", obs.phases)
	}
}

// Redeem (the pull path): a mint error at redemption returns 500 — never a 200
// with an empty credential — and fails the run.
func TestRedeemFailsRunWhenMintFails(t *testing.T) {
	p := New(Config{Authority: failingAuthority{}})
	// A ticket is issued without minting, so the mint error surfaces at redeem.
	cred := mustMint(t, p, &fakePull{}, aei.RunSpec{ID: "run-p"})
	if cred.Kind != aei.Ticket {
		t.Fatalf("want a ticket, got %v", cred.Kind)
	}
	p.store.SetRunning("run-p")
	done := p.arm("run-p")
	defer p.disarm("run-p")

	srv := httptest.NewServer(p.Handler())
	defer srv.Close()
	resp := post(t, srv.URL+"/aei/v1alpha1/redeem", "", map[string]string{"ticket": cred.Value})
	resp.Body.Close()
	if resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("redeem status = %d; want 500", resp.StatusCode)
	}
	if st, _ := p.Status("run-p"); st.Phase != phaseFailed {
		t.Errorf("run phase = %q; want Failed", st.Phase)
	}
	select {
	case r := <-done:
		if r.Err == nil {
			t.Error("waiter woke with no error")
		}
	default:
		t.Error("a waiting Run/Submit was not woken with the failure")
	}
}
