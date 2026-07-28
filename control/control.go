// Package control is the AEI reference control plane: the platform side of the
// three contracts, so an application (or a CRD controller) gets "everything within
// AEI's scope" without reimplementing it. It:
//
//   - mints a per-run, scope-limited, short-TTL credential and hands it to the
//     provider — a pushed token, or a single-use ticket for a pull provider
//     (Contract 3);
//   - dispatches through any aei.Launcher and drives completion by racing the
//     runtime's /complete callback against the provider's Await and the deadline
//     (Contract 1, the Completion section);
//   - serves the runtime ABI routes /complete, /redeem, /vend (Contract 2, the
//     platform side of docs/runtime-abi.md).
//
// It imports only the aei core and the standard library — no provider, no
// client-go — so an app that embeds it stays lightweight (design tenet #5).
//
// This is reference-grade by default: in-memory ticket and waiter stores, a
// single process, and a symmetric HMAC token. Identity is a seam — supply
// Config.Authority to mint/verify with an asymmetric, audience-bound credential
// (Ed25519, a SPIFFE/SVID issuer, a transaction-token service); a production
// profile also hardens the stores and mints against SPIFFE/SPIRE (roadmap
// Phase 6). The seam does not change.
package control

import (
	"context"
	"crypto/ed25519"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/jackfrancis/agent-execution-interface/aei"
)

// CredentialSource vends a downstream provider credential for a run, serving
// POST /vend. The application implements it; the control plane handles the HTTP,
// the run-scoped authorization, and the "no standing secret on the runtime"
// guarantee. Returning ErrNoCredential signals an unconfigured provider (404).
type CredentialSource interface {
	Vend(ctx context.Context, runID, subject, provider string) (VendedCredential, error)
}

// VendedCredential is a short-lived downstream credential returned by a
// CredentialSource.
type VendedCredential struct {
	Value     string
	ExpiresIn int // seconds
}

// ErrNoCredential is returned by a CredentialSource when it has no credential for
// the requested provider (surfaced to the runtime as 404).
var ErrNoCredential = errors.New("control: no credential for provider")

// TokenAuthority mints and verifies the claims of a run credential. It is the
// pluggable identity seam — the mint/verify analog of CredentialSource for /vend
// (docs/design.md, Contract 3). The reference implementation is a symmetric HMAC
// token; a platform can supply an asymmetric, audience-bound authority (Ed25519,
// a SPIFFE/SVID issuer, a transaction-token service) so the sole-minter tier holds
// a private key and a verify-only tier holds only the public half — without
// changing the plane. Set it via Config.Authority; unset uses the built-in HMAC
// default, so existing callers are unaffected.
type TokenAuthority interface {
	// Mint returns a signed credential encoding claims.
	Mint(Claims) (string, error)
	// Verify checks a credential and returns its claims, or false if it is
	// invalid, tampered, or expired.
	Verify(token string) (Claims, bool)
}

// PublicKeyAuthority is an optional capability a TokenAuthority implements when it
// has a public verification half worth publishing (an asymmetric authority such as
// Ed25519). The control plane serves it at GET /aei/v1alpha1/authority so a
// verify-only app discovers the key and verifies minted run credentials itself —
// no shared secret, no minting capability. A symmetric (HMAC) authority does not
// implement it, so the endpoint 404s.
type PublicKeyAuthority interface {
	// PublicKey returns the raw public verification key (Ed25519: 32 bytes).
	PublicKey() []byte
}

// Observer records a run's terminal outcome for metrics. It is the pluggable
// observability seam — the metrics analog of CredentialSource (/vend) and
// TokenAuthority (mint/verify). The control plane calls ObserveRun exactly once
// per run on the data-plane (Submit) path, with the run's taskRef, terminal phase
// ("Succeeded" or "Failed"), and the provider-reported timing breakdown. The
// reference default is a no-op; a platform supplies a Prometheus implementation
// (exposed at /metrics) without changing the plane. Implementations MUST be safe
// for concurrent use.
type Observer interface {
	ObserveRun(taskRef, phase string, timing aei.Timing)
}

// nopObserver is the default Observer: it records nothing, so an unconfigured
// plane (the reference profile) emits no metrics.
type nopObserver struct{}

func (nopObserver) ObserveRun(string, string, aei.Timing) {}

// Config configures a control plane.
type Config struct {
	// Secret is the HMAC key for the default (reference-grade) TokenAuthority. It
	// is ignored when Authority is set.
	Secret []byte
	// Authority mints and verifies run credentials. Optional: nil uses a built-in
	// HMAC authority keyed by Secret. Supply one to replace the reference token
	// with an asymmetric, audience-bound credential (the mint/verify seam).
	Authority   TokenAuthority
	Credentials CredentialSource // optional: serves POST /vend
	TokenTTL    time.Duration    // run credential lifetime; default 15m
	TicketTTL   time.Duration    // redemption ticket lifetime; default 5m
	// Logger records run outcomes: a failed run is logged at Warn. Optional — nil
	// uses slog.Default(), so a dispatch failure is never silent even unconfigured
	// (the data-plane path keeps no per-run object, only GET /runs/{id}).
	Logger *slog.Logger
	// Store holds the plane's persistable state — redemption tickets and run status.
	// Optional: nil uses an in-memory, single-process store (reference-grade). Supply
	// a shared implementation to run the control plane with replicas > 1 (the HA
	// seam; roadmap Phase 7).
	Store Store
	// Observer records each run's terminal outcome for metrics: its taskRef, terminal
	// phase, and timing breakdown (the metrics seam, the /metrics analog of
	// CredentialSource for /vend). Optional: nil records nothing (reference-grade). A
	// platform supplies a Prometheus implementation and serves it at /metrics, without
	// changing the plane.
	Observer Observer
}

// Run phases: a submitted run is phaseRunning until a terminal signal — the runtime
// callback, the provider watch, or the deadline — resolves it.
const (
	phaseRunning   = "Running"
	phaseSucceeded = "Succeeded"
	phaseFailed    = "Failed"
)

// Plane is a running control plane. Construct it with New; serve Handler() and
// dispatch with Run.
type Plane struct {
	auth      TokenAuthority
	creds     CredentialSource
	tokenTTL  time.Duration
	ticketTTL time.Duration
	now       func() time.Time
	log       *slog.Logger
	store     Store    // persistable state (tickets + run status); the HA seam
	obs       Observer // run-outcome metrics sink; the observability seam

	mu      sync.Mutex
	waiters map[string]chan aei.Result // runID -> completion channel (same-replica wake)
}

// New builds a control plane from cfg, applying defaults.
func New(cfg Config) *Plane {
	now := time.Now
	auth := cfg.Authority
	if auth == nil {
		// Reference-grade default: a symmetric HMAC token keyed by cfg.Secret.
		auth = hmacAuthority{secret: cfg.Secret, now: now}
	}
	log := cfg.Logger
	if log == nil {
		log = slog.Default()
	}
	store := cfg.Store
	if store == nil {
		store = newMemStore() // reference-grade: in-memory, single process
	}
	obs := cfg.Observer
	if obs == nil {
		obs = nopObserver{} // reference-grade: record nothing
	}
	return &Plane{
		auth:      auth,
		creds:     cfg.Credentials,
		tokenTTL:  orDur(cfg.TokenTTL, 15*time.Minute),
		ticketTTL: orDur(cfg.TicketTTL, 5*time.Minute),
		now:       now,
		log:       log,
		store:     store,
		obs:       obs,
		waiters:   map[string]chan aei.Result{},
	}
}

// Run dispatches spec through launcher and returns its terminal Result. It mints
// the run credential, dispatches, and races the runtime's /complete callback
// against the provider's Await and the run deadline — the first terminal signal
// wins (docs/design.md, Completion). A missing spec.ID is generated. Taking the
// launcher per call (rather than binding one) lets a single plane — one broker,
// one completion registry, one ABI server — front many providers, which is what
// the CRD controller needs (a provider per AgentProviderClass).
func (p *Plane) Run(ctx context.Context, launcher aei.Launcher, spec aei.RunSpec) (aei.Result, error) {
	if launcher == nil {
		return aei.Result{}, errors.New("control: no launcher")
	}
	if spec.ID == "" {
		spec.ID = randID()
	}
	cred := p.mint(launcher, spec)

	if spec.Deadline > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, spec.Deadline)
		defer cancel()
	}

	done := p.arm(spec.ID)
	defer p.disarm(spec.ID)

	// The async capability lets us dispatch without blocking a worker and observe
	// completion by watch; a plain provider blocks in Launch. Either way the
	// runtime's callback (delivered to `done` by handleComplete) races the result.
	if async, ok := launcher.(aei.AsyncLauncher); ok {
		h, err := async.Dispatch(ctx, spec, cred)
		if err != nil {
			return aei.Result{Err: err}, err
		}
		watch := make(chan aei.Result, 1)
		go func() {
			r, _ := async.Await(ctx, h)
			watch <- r
		}()
		select {
		case r := <-done:
			return r, nil
		case r := <-watch:
			return r, nil
		}
	}

	launch := make(chan aei.Result, 1)
	go func() {
		_, err := launcher.Launch(ctx, spec, cred)
		launch <- aei.Result{Err: err}
	}()
	select {
	case r := <-done:
		return r, nil
	case r := <-launch:
		return r, nil
	}
}

// RunState is the observable state of a submitted run, held in the Store so the
// data-plane status query needs no per-run Kubernetes object
// (docs/dispatch-plane.md). Phase is phaseRunning until a terminal signal — the
// runtime callback, the provider watch, or the deadline — resolves it.
type RunState struct {
	Phase     string // Running | Succeeded | Failed
	Err       string
	ResultRef string
	Timing    aei.Timing
}

// Submit dispatches spec through launcher without blocking and returns the run
// id. Unlike Run it does not wait: the terminal outcome (the runtime's /complete
// callback raced against the provider's Await and the deadline) is recorded into
// the in-memory run store and read back with Status. This is the scalable app
// path — one call per run, no per-run object (docs/dispatch-plane.md).
func (p *Plane) Submit(_ context.Context, launcher aei.Launcher, spec aei.RunSpec) (string, error) {
	if launcher == nil {
		return "", errors.New("control: no launcher")
	}
	if spec.ID == "" {
		spec.ID = randID()
	}
	cred := p.mint(launcher, spec)
	p.store.SetRunning(spec.ID)
	done := p.arm(spec.ID)

	go func() {
		defer p.disarm(spec.ID)
		// The run outlives the submit request, so it runs on its own context
		// bounded only by the run deadline (the universal backstop).
		ctx := context.Background()
		if spec.Deadline > 0 {
			var cancel context.CancelFunc
			ctx, cancel = context.WithTimeout(ctx, spec.Deadline)
			defer cancel()
		}
		res := p.race(ctx, launcher, spec, cred, done)
		p.observe(spec, res)
		p.finishRun(spec.ID, res)
	}()
	return spec.ID, nil
}

// observe reports a run's terminal outcome to the metrics Observer (the metrics
// seam). It fires once per Submit — the data-plane app path — with the run's
// taskRef, terminal phase, and the provider-reported timing breakdown. The default
// Observer is a no-op, so an unconfigured plane records nothing. The phase mirrors
// finishRun's: a run with an error is Failed, otherwise Succeeded.
func (p *Plane) observe(spec aei.RunSpec, res aei.Result) {
	phase := phaseSucceeded
	if res.Err != nil {
		phase = phaseFailed
	}
	p.obs.ObserveRun(spec.TaskRef, phase, res.Timing)
}

// Status returns the current state of a submitted run, and whether it is known.
func (p *Plane) Status(runID string) (RunState, bool) {
	return p.store.Run(runID)
}

// finishRun records a run's terminal outcome through the store, first-writer-wins
// (R4): the callback and the watch may both fire; whichever wins the race writes the
// terminal state and logs a failure once, and the loser is a no-op. The durable write
// is also what makes the outcome visible when a callback lands on a different replica
// than the one that dispatched (the HA seam).
func (p *Plane) finishRun(runID string, res aei.Result) {
	st := RunState{Phase: phaseSucceeded, ResultRef: res.ResultRef, Timing: res.Timing}
	if res.Err != nil {
		st.Phase, st.Err = phaseFailed, res.Err.Error()
	}
	if !p.store.FinishRun(runID, st) {
		return // a terminal outcome was already recorded
	}
	if res.Err != nil {
		// Otherwise only visible via GET /runs/{id} (the data-plane path keeps no
		// per-run object), so surface it: a dispatch error — e.g. the substrate or
		// its CRD is missing — reads clearly in the message here.
		p.log.Warn("run failed", "run_id", runID, "error", res.Err.Error())
	}
}

// race dispatches spec and returns the first terminal signal: the runtime's
// /complete callback (delivered on done), the provider's Await, or the deadline
// (ctx). It is the shared completion core of Run (blocking) and Submit
// (non-blocking).
func (p *Plane) race(ctx context.Context, launcher aei.Launcher, spec aei.RunSpec, cred aei.Credential, done chan aei.Result) aei.Result {
	if async, ok := launcher.(aei.AsyncLauncher); ok {
		h, err := async.Dispatch(ctx, spec, cred)
		if err != nil {
			return aei.Result{Err: err}
		}
		watch := make(chan aei.Result, 1)
		go func() {
			r, _ := async.Await(ctx, h)
			watch <- r
		}()
		select {
		case r := <-done:
			return r
		case r := <-watch:
			return r
		}
	}
	launch := make(chan aei.Result, 1)
	go func() {
		_, err := launcher.Launch(ctx, spec, cred)
		launch <- aei.Result{Err: err}
	}()
	select {
	case r := <-done:
		return r
	case r := <-launch:
		return r
	}
}

// --- completion registry (the callback/watch race) ---

func (p *Plane) arm(runID string) chan aei.Result {
	ch := make(chan aei.Result, 1)
	p.mu.Lock()
	p.waiters[runID] = ch
	p.mu.Unlock()
	return ch
}

func (p *Plane) disarm(runID string) {
	p.mu.Lock()
	delete(p.waiters, runID)
	p.mu.Unlock()
}

// fire delivers a run's terminal outcome to its armed waiter, exactly once. An
// unknown or already-delivered runID is a harmless no-op (idempotent finish, R4).
func (p *Plane) fire(runID string, r aei.Result) {
	p.mu.Lock()
	ch, ok := p.waiters[runID]
	p.mu.Unlock()
	if !ok {
		return
	}
	select {
	case ch <- r:
	default:
	}
}

// --- identity broker (Contract 3) ---

// Claims are the run credential's payload: the run it is bound to, the acting
// principal, the scopes it may exercise, and its expiry.
type Claims struct {
	RunID    string   `json:"run"`
	Subject  string   `json:"sub"`
	Scopes   []string `json:"scp,omitempty"`
	Audience string   `json:"aud,omitempty"`
	Exp      int64    `json:"exp"` // unix seconds
}

// mint produces the credential handed to the provider: a pushed token, or (for a
// PullCredentialLauncher) a single-use ticket the runtime redeems for the token.
func (p *Plane) mint(launcher aei.Launcher, spec aei.RunSpec) aei.Credential {
	claims := Claims{
		RunID:    spec.ID,
		Subject:  spec.Identity.Subject,
		Scopes:   spec.Identity.Scopes,
		Audience: spec.Identity.Audience,
		Exp:      p.now().Add(p.tokenTTL).Unix(),
	}
	if pl, ok := launcher.(aei.PullCredentialLauncher); ok && pl.PullsCredential() {
		return aei.Credential{Value: p.issueTicket(claims), Kind: aei.Ticket}
	}
	return aei.Credential{Value: p.mintToken(claims), Kind: aei.Token}
}

func (p *Plane) issueTicket(c Claims) string {
	id := randID()
	p.store.PutTicket(id, c, p.now().Add(p.ticketTTL))
	return id
}

// redeem consumes a single-use ticket and mints the run token it was issued for,
// preserving the run id so completion still correlates.
func (p *Plane) redeem(id string) (string, int, bool) {
	c, ok := p.store.RedeemTicket(id, p.now())
	if !ok {
		return "", 0, false
	}
	ttl := int(time.Unix(c.Exp, 0).Sub(p.now()).Seconds())
	return p.mintToken(c), ttl, true
}

// mintToken produces a run credential via the configured TokenAuthority. It keeps
// a single-value signature (a mint error collapses to an empty token) so the
// internal mint/redeem paths stay unchanged.
func (p *Plane) mintToken(c Claims) string {
	tok, _ := p.auth.Mint(c)
	return tok
}

// verifyToken authenticates a run credential via the configured TokenAuthority.
func (p *Plane) verifyToken(tok string) (Claims, bool) {
	return p.auth.Verify(tok)
}

// hmacAuthority is the reference-grade TokenAuthority: base64url(json).base64url(
// hmac-sha256). It is symmetric — the same secret mints and verifies — which is
// why a production deployment supplies an asymmetric Authority instead (Config).
type hmacAuthority struct {
	secret []byte
	now    func() time.Time
}

func (a hmacAuthority) Mint(c Claims) (string, error) {
	payload, err := json.Marshal(c)
	if err != nil {
		return "", err
	}
	b := base64.RawURLEncoding.EncodeToString(payload)
	return b + "." + a.sign(b), nil
}

func (a hmacAuthority) Verify(tok string) (Claims, bool) {
	b, sig, ok := strings.Cut(tok, ".")
	if !ok || !hmac.Equal([]byte(sig), []byte(a.sign(b))) {
		return Claims{}, false
	}
	payload, err := base64.RawURLEncoding.DecodeString(b)
	if err != nil {
		return Claims{}, false
	}
	var c Claims
	if err := json.Unmarshal(payload, &c); err != nil {
		return Claims{}, false
	}
	if a.now().Unix() > c.Exp {
		return Claims{}, false
	}
	return c, true
}

func (a hmacAuthority) sign(b string) string {
	m := hmac.New(sha256.New, a.secret)
	m.Write([]byte(b))
	return base64.RawURLEncoding.EncodeToString(m.Sum(nil))
}

// ed25519Authority is the asymmetric TokenAuthority: the control plane holds the
// private key and is the sole minter, while a verify-only tier (e.g. an app's web
// tier) holds only the public key. The wire format matches hmacAuthority —
// base64url(json(claims)).base64url(sig) — with an Ed25519 signature over the
// encoded payload, so any holder of the public key verifies a token independently,
// without the plane and without a shared secret. Supply it via Config.Authority.
type ed25519Authority struct {
	priv ed25519.PrivateKey
	pub  ed25519.PublicKey
	now  func() time.Time
}

// NewEd25519Authority builds an asymmetric TokenAuthority from an Ed25519 private
// key. The control plane mints run credentials with it; a verify-only tier (which
// holds only priv.Public(), distributed out of band) verifies the same tokens and
// can never mint — the stronger identity profile the reference HMAC authority is
// deliberately not, plugged in without changing the plane.
func NewEd25519Authority(priv ed25519.PrivateKey) TokenAuthority {
	return ed25519Authority{priv: priv, pub: priv.Public().(ed25519.PublicKey), now: time.Now}
}

// PublicKey publishes the verification half (the PublicKeyAuthority capability) so
// an app can verify minted tokens independently of the plane.
func (a ed25519Authority) PublicKey() []byte { return a.pub }

func (a ed25519Authority) Mint(c Claims) (string, error) {
	payload, err := json.Marshal(c)
	if err != nil {
		return "", err
	}
	b := base64.RawURLEncoding.EncodeToString(payload)
	sig := ed25519.Sign(a.priv, []byte(b))
	return b + "." + base64.RawURLEncoding.EncodeToString(sig), nil
}

func (a ed25519Authority) Verify(tok string) (Claims, bool) {
	b, sigPart, ok := strings.Cut(tok, ".")
	if !ok {
		return Claims{}, false
	}
	sig, err := base64.RawURLEncoding.DecodeString(sigPart)
	if err != nil || !ed25519.Verify(a.pub, []byte(b), sig) {
		return Claims{}, false
	}
	payload, err := base64.RawURLEncoding.DecodeString(b)
	if err != nil {
		return Claims{}, false
	}
	var c Claims
	if err := json.Unmarshal(payload, &c); err != nil {
		return Claims{}, false
	}
	if a.now().Unix() > c.Exp {
		return Claims{}, false
	}
	return c, true
}

// --- runtime ABI server (Contract 2, platform side) ---

// RegisterRoutes mounts the runtime-facing ABI routes on mux, so a caller can
// serve them alongside other routes (e.g. the data-plane dispatch API) on one
// server.
func (p *Plane) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("POST /aei/v1alpha1/complete", p.handleComplete)
	mux.HandleFunc("POST /aei/v1alpha1/redeem", p.handleRedeem)
	mux.HandleFunc("POST /aei/v1alpha1/vend", p.handleVend)
	mux.HandleFunc("GET /aei/v1alpha1/authority", p.handleAuthority)
}

// Handler returns the runtime-facing ABI routes under /aei/v1alpha1: POST
// /complete, /redeem, /vend (docs/runtime-abi.md §2). Mount it where a run's
// AEI_CONTROL_URL / AEI_BROKER_URL point.
func (p *Plane) Handler() http.Handler {
	mux := http.NewServeMux()
	p.RegisterRoutes(mux)
	return mux
}

// handleAuthority publishes the run-credential verification key so a verify-only
// app can validate minted credentials itself (JWKS-like discovery), holding no
// secret and unable to mint. It returns the base64 Ed25519 public key when the
// configured authority is asymmetric, or 404 for a symmetric (HMAC) authority,
// which has no public half to publish.
func (p *Plane) handleAuthority(w http.ResponseWriter, _ *http.Request) {
	pk, ok := p.auth.(PublicKeyAuthority)
	if !ok {
		http.Error(w, "no public authority (symmetric)", http.StatusNotFound)
		return
	}
	writeJSON(w, map[string]any{
		"alg":        "Ed25519",
		"public_key": base64.StdEncoding.EncodeToString(pk.PublicKey()),
	})
}

// wireTiming is the /complete timing body (docs/runtime-abi.md §3), whose snake_case
// JSON is the wire contract; it maps onto the domain aei.Timing.
type wireTiming struct {
	ProvisioningSeconds float64 `json:"provisioning_seconds"`
	RuntimeSeconds      float64 `json:"runtime_seconds"`
	ModelSeconds        float64 `json:"model_seconds"`
	ModelCalls          int     `json:"model_calls"`
	ToolCalls           int     `json:"tool_calls"`
}

func (t wireTiming) toAEI() aei.Timing {
	return aei.Timing{
		ProvisioningSeconds: t.ProvisioningSeconds,
		RuntimeSeconds:      t.RuntimeSeconds,
		ModelSeconds:        t.ModelSeconds,
		ModelCalls:          t.ModelCalls,
		ToolCalls:           t.ToolCalls,
	}
}

func (p *Plane) handleComplete(w http.ResponseWriter, r *http.Request) {
	claims, ok := p.bearer(r)
	if !ok {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	var body struct {
		RunID     string     `json:"run_id"`
		Error     string     `json:"error"`
		ResultRef string     `json:"result_ref"`
		Timing    wireTiming `json:"timing"`
	}
	_ = json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&body)
	// A runtime may only complete its own run: the body's run id must match the
	// run the credential is bound to.
	if body.RunID != claims.RunID {
		http.Error(w, "run mismatch", http.StatusForbidden)
		return
	}
	var runErr error
	if body.Error != "" {
		runErr = errors.New(body.Error)
	}
	res := aei.Result{Err: runErr, ResultRef: body.ResultRef, Timing: body.Timing.toAEI()}
	// Record the outcome durably first, so a callback landing on a different replica
	// than the one that dispatched still finalizes the run (the HA seam); then wake a
	// same-replica waiter for a prompt Run()/Submit() return. Both go through the
	// store's first-writer-wins, so the watch/deadline backstop cannot double-count.
	p.finishRun(claims.RunID, res)
	p.fire(claims.RunID, res)
	w.WriteHeader(http.StatusAccepted)
}

func (p *Plane) handleRedeem(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Ticket string `json:"ticket"`
	}
	if json.NewDecoder(io.LimitReader(r.Body, 1<<16)).Decode(&body) != nil || body.Ticket == "" {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	tok, ttl, ok := p.redeem(body.Ticket)
	if !ok {
		http.Error(w, "invalid ticket", http.StatusUnauthorized) // coarse: unknown/spent/expired alike
		return
	}
	writeJSON(w, map[string]any{"credential": tok, "expires_in": ttl})
}

func (p *Plane) handleVend(w http.ResponseWriter, r *http.Request) {
	claims, ok := p.bearer(r)
	if !ok {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	if p.creds == nil {
		http.Error(w, "vending not configured", http.StatusNotImplemented)
		return
	}
	var body struct {
		Provider string `json:"provider"`
	}
	if json.NewDecoder(io.LimitReader(r.Body, 1<<16)).Decode(&body) != nil || body.Provider == "" {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	cred, err := p.creds.Vend(r.Context(), claims.RunID, claims.Subject, body.Provider)
	if err != nil {
		if errors.Is(err, ErrNoCredential) {
			http.Error(w, "no credential", http.StatusNotFound)
			return
		}
		http.Error(w, "vend failed", http.StatusBadGateway)
		return
	}
	writeJSON(w, map[string]any{"credential": cred.Value, "token_type": "Bearer", "expires_in": cred.ExpiresIn})
}

func (p *Plane) bearer(r *http.Request) (Claims, bool) {
	tok, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
	if !ok {
		return Claims{}, false
	}
	return p.verifyToken(tok)
}

// --- helpers ---

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

func orDur(v, def time.Duration) time.Duration {
	if v <= 0 {
		return def
	}
	return v
}

func randID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	return base64.RawURLEncoding.EncodeToString(b[:])
}
