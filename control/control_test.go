package control

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/jackfrancis/agent-execution-interface/aei"
)

// fakeAsync is an AsyncLauncher whose Await blocks until the deadline, so
// completion must arrive via the /complete callback. It captures the credential
// the plane pushed, so the test can act as the runtime.
type fakeAsync struct{ dispatched chan aei.Credential }

func (f *fakeAsync) Launch(ctx context.Context, spec aei.RunSpec, cred aei.Credential) (aei.Handle, error) {
	h, _ := f.Dispatch(ctx, spec, cred)
	r, _ := f.Await(ctx, h)
	return h, r.Err
}
func (f *fakeAsync) Dispatch(_ context.Context, spec aei.RunSpec, cred aei.Credential) (aei.Handle, error) {
	f.dispatched <- cred
	return aei.Handle{Provider: "fake", Ref: spec.ID}, nil
}
func (f *fakeAsync) Await(ctx context.Context, _ aei.Handle) (aei.Result, error) {
	<-ctx.Done()
	return aei.Result{Err: ctx.Err()}, nil
}

// fakeWatch is an AsyncLauncher whose Await returns a definite result at once, so
// completion arrives via the watch backstop (no callback).
type fakeWatch struct{ result aei.Result }

func (f *fakeWatch) Launch(ctx context.Context, spec aei.RunSpec, cred aei.Credential) (aei.Handle, error) {
	return aei.Handle{Ref: spec.ID}, f.result.Err
}
func (f *fakeWatch) Dispatch(_ context.Context, spec aei.RunSpec, _ aei.Credential) (aei.Handle, error) {
	return aei.Handle{Ref: spec.ID}, nil
}
func (f *fakeWatch) Await(context.Context, aei.Handle) (aei.Result, error) { return f.result, nil }

// fakePull adds the pull-credential capability.
type fakePull struct{ fakeWatch }

func (fakePull) PullsCredential() bool { return true }

func post(t *testing.T, url, bearer string, body any) *http.Response {
	t.Helper()
	b, _ := json.Marshal(body)
	req, _ := http.NewRequest(http.MethodPost, url, bytes.NewReader(b))
	if bearer != "" {
		req.Header.Set("Authorization", "Bearer "+bearer)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST %s: %v", url, err)
	}
	return resp
}

func TestTokenRoundTripTamperExpire(t *testing.T) {
	p := New(Config{Secret: []byte("k")})
	tok := p.mintToken(Claims{RunID: "r", Exp: p.now().Add(time.Hour).Unix()})
	if c, ok := p.verifyToken(tok); !ok || c.RunID != "r" {
		t.Fatalf("valid token should verify, got ok=%v", ok)
	}
	if _, ok := p.verifyToken(tok + "x"); ok {
		t.Error("tampered signature must fail")
	}
	expired := p.mintToken(Claims{RunID: "r", Exp: p.now().Add(-time.Hour).Unix()})
	if _, ok := p.verifyToken(expired); ok {
		t.Error("expired token must fail")
	}
}

// The asymmetric authority: the plane mints with the private key, and a
// verify-only tier holding ONLY the public key verifies the same wire format
// (base64url(json).base64url(ed25519-sig)) without the plane and without a shared
// secret. This is the interop an app's verify-only tier relies on (e.g. gofer's
// web tier, ADR 0002).
func TestEd25519AuthorityPublicVerify(t *testing.T) {
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	auth := NewEd25519Authority(priv)

	tok, err := auth.Mint(Claims{RunID: "r", Subject: "u", Scopes: []string{"signals:read"}, Audience: "gofer-agent", Exp: time.Now().Add(time.Hour).Unix()})
	if err != nil {
		t.Fatal(err)
	}
	if c, ok := auth.Verify(tok); !ok || c.RunID != "r" || c.Audience != "gofer-agent" {
		t.Fatalf("valid token should verify, ok=%v claims=%+v", ok, c)
	}
	// A holder of only the public key verifies the signature independently.
	b, sigPart, _ := strings.Cut(tok, ".")
	sig, _ := base64.RawURLEncoding.DecodeString(sigPart)
	if !ed25519.Verify(pub, []byte(b), sig) {
		t.Fatal("public key must verify the signature")
	}
	if _, ok := auth.Verify(tok + "x"); ok {
		t.Error("tampered token must fail")
	}
	expired, _ := auth.Mint(Claims{RunID: "r", Exp: time.Now().Add(-time.Hour).Unix()})
	if _, ok := auth.Verify(expired); ok {
		t.Error("expired token must fail")
	}
}

// stubAuthority is a trivial TokenAuthority (token == "stub.<runID>") that counts
// its calls, so a test can prove the plane mints/verifies through the injected
// authority rather than the HMAC default.
type stubAuthority struct{ minted, verified int }

func (s *stubAuthority) Mint(c Claims) (string, error) { s.minted++; return "stub." + c.RunID, nil }
func (s *stubAuthority) Verify(tok string) (Claims, bool) {
	s.verified++
	if id, ok := strings.CutPrefix(tok, "stub."); ok {
		return Claims{RunID: id, Exp: 1 << 62}, true
	}
	return Claims{}, false
}

// A supplied Config.Authority replaces the HMAC default for both mint and verify
// — the pluggable identity seam (an asymmetric, audience-bound authority plugs in
// the same way).
func TestConfigAuthorityOverridesDefault(t *testing.T) {
	sa := &stubAuthority{}
	p := New(Config{Authority: sa})
	tok := p.mintToken(Claims{RunID: "r7"})
	if tok != "stub.r7" {
		t.Fatalf("plane should mint via the injected authority, got %q", tok)
	}
	c, ok := p.verifyToken(tok)
	if !ok || c.RunID != "r7" {
		t.Fatalf("plane should verify via the injected authority, got ok=%v claims=%+v", ok, c)
	}
	if sa.minted == 0 || sa.verified == 0 {
		t.Fatal("injected authority was not used")
	}
	// The HMAC default must reject a stub token, confirming the two are distinct.
	if _, ok := New(Config{Secret: []byte("k")}).verifyToken(tok); ok {
		t.Fatal("the HMAC default must not verify a stub-authority token")
	}
}

// TestRunCompletesViaCallback: the runtime's /complete callback finalizes the run
// (the fast path), ahead of the deadline-bound Await.
func TestRunCompletesViaCallback(t *testing.T) {
	fp := &fakeAsync{dispatched: make(chan aei.Credential, 1)}
	p := New(Config{Secret: []byte("k")})
	srv := httptest.NewServer(p.Handler())
	defer srv.Close()

	resCh := make(chan aei.Result, 1)
	go func() {
		r, _ := p.Run(context.Background(), fp, aei.RunSpec{ID: "run-1", Deadline: 2 * time.Second})
		resCh <- r
	}()

	cred := <-fp.dispatched // the pushed run token
	resp := post(t, srv.URL+"/aei/v1alpha1/complete", cred.Value,
		map[string]any{"run_id": "run-1", "result_ref": "res://1"})
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("complete status = %d, want 202", resp.StatusCode)
	}

	select {
	case r := <-resCh:
		if r.Err != nil {
			t.Errorf("want success, got %v", r.Err)
		}
		if r.ResultRef != "res://1" {
			t.Errorf("resultRef = %q, want res://1", r.ResultRef)
		}
	case <-time.After(time.Second):
		t.Fatal("Run did not return after the callback")
	}
}

// TestRunCompletesViaWatch: with no callback, the provider's Await (the backstop)
// finalizes the run.
func TestRunCompletesViaWatch(t *testing.T) {
	fw := &fakeWatch{result: aei.Result{ResultRef: "watched"}}
	p := New(Config{Secret: []byte("k")})
	r, err := p.Run(context.Background(), fw, aei.RunSpec{ID: "run-2", Deadline: time.Second})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if r.ResultRef != "watched" {
		t.Errorf("resultRef = %q, want watched", r.ResultRef)
	}
}

// TestPullProviderIssuesTicketAndRedeems: a pull provider is handed a ticket, and
// redeeming it (single-use) yields a token bound to the same run.
func TestPullProviderIssuesTicketAndRedeems(t *testing.T) {
	p := New(Config{Secret: []byte("k")})
	cred := p.mint(&fakePull{}, aei.RunSpec{ID: "run-3", Identity: aei.IdentityRequest{Subject: "u1"}})
	if cred.Kind != aei.Ticket {
		t.Fatalf("pull provider must get a ticket, got kind %v", cred.Kind)
	}

	srv := httptest.NewServer(p.Handler())
	defer srv.Close()

	resp := post(t, srv.URL+"/aei/v1alpha1/redeem", "", map[string]string{"ticket": cred.Value})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("redeem status = %d, want 200", resp.StatusCode)
	}
	var out struct {
		Credential string `json:"credential"`
		ExpiresIn  int    `json:"expires_in"`
	}
	_ = json.NewDecoder(resp.Body).Decode(&out)
	claims, ok := p.verifyToken(out.Credential)
	if !ok || claims.RunID != "run-3" || claims.Subject != "u1" {
		t.Fatalf("redeemed token claims = %+v ok=%v", claims, ok)
	}

	// Single-use: a second redemption of the same ticket fails.
	resp2 := post(t, srv.URL+"/aei/v1alpha1/redeem", "", map[string]string{"ticket": cred.Value})
	if resp2.StatusCode != http.StatusUnauthorized {
		t.Errorf("second redeem status = %d, want 401 (single-use)", resp2.StatusCode)
	}
}

// TestCompleteRejectsForeignRun: a runtime cannot complete another run — the body
// run id must match the credential's bound run.
func TestCompleteRejectsForeignRun(t *testing.T) {
	p := New(Config{Secret: []byte("k")})
	cred := p.mint(&fakeWatch{}, aei.RunSpec{ID: "run-A"})

	srv := httptest.NewServer(p.Handler())
	defer srv.Close()

	done := p.arm("run-A")
	defer p.disarm("run-A")

	resp := post(t, srv.URL+"/aei/v1alpha1/complete", cred.Value, map[string]any{"run_id": "run-B"})
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("status = %d, want 403", resp.StatusCode)
	}
	select {
	case <-done:
		t.Fatal("a foreign completion must not fire the run's waiter")
	default:
	}
}

type fakeCreds struct{}

func (fakeCreds) Vend(_ context.Context, _, subject, provider string) (VendedCredential, error) {
	if provider != "github" {
		return VendedCredential{}, ErrNoCredential
	}
	return VendedCredential{Value: "gh-" + subject, ExpiresIn: 300}, nil
}

// TestVendUsesSource: /vend authorizes with the run credential and returns the
// app's downstream credential; an unknown provider is 404.
func TestVendUsesSource(t *testing.T) {
	p := New(Config{Secret: []byte("k"), Credentials: fakeCreds{}})
	cred := p.mint(&fakeWatch{}, aei.RunSpec{ID: "run-v", Identity: aei.IdentityRequest{Subject: "u1"}})

	srv := httptest.NewServer(p.Handler())
	defer srv.Close()

	resp := post(t, srv.URL+"/aei/v1alpha1/vend", cred.Value, map[string]string{"provider": "github"})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("vend status = %d, want 200", resp.StatusCode)
	}
	var out struct {
		Credential string `json:"credential"`
		ExpiresIn  int    `json:"expires_in"`
	}
	_ = json.NewDecoder(resp.Body).Decode(&out)
	if out.Credential != "gh-u1" || out.ExpiresIn != 300 {
		t.Errorf("vended = %+v, want gh-u1/300", out)
	}

	if resp := post(t, srv.URL+"/aei/v1alpha1/vend", cred.Value, map[string]string{"provider": "gitlab"}); resp.StatusCode != http.StatusNotFound {
		t.Errorf("unknown provider status = %d, want 404", resp.StatusCode)
	}
	// Unauthorized without a bearer.
	if resp := post(t, srv.URL+"/aei/v1alpha1/vend", "", map[string]string{"provider": "github"}); resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("no-bearer status = %d, want 401", resp.StatusCode)
	}
}
