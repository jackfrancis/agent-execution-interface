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
	"fmt"
	"io"
	"log/slog"
	"maps"
	"math"
	"net"
	"net/http"
	"os"
	"reflect"
	"runtime/debug"
	"slices"
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
// per run on the data-plane (Submit) path, with a RunObservation. The reference
// default is a no-op; a platform supplies a Prometheus implementation (exposed at
// /metrics) without changing the plane. Implementations MUST be safe for
// concurrent use.
type Observer interface {
	ObserveRun(RunObservation)
}

// RunObservation is one run's terminal outcome as the control plane saw it.
type RunObservation struct {
	TaskRef string // the app's task, e.g. "github-converse"; may be empty
	Phase   string // terminal phase: "Succeeded" or "Failed"
	// Provider is the provider that placed the run, from its Handle (e.g. "k8s-job"),
	// or else the one the caller named with WithProvider. Empty when neither did.
	Provider string
	// Duration is the run's wall-clock as the controller measured it: from the start
	// of dispatch to the terminal signal (the runtime's callback, the provider's
	// watch, or the deadline). It is set for every run, including one the runtime
	// never reported on.
	Duration time.Duration
	// Timing is the breakdown the terminal signal carried: the runtime's /complete
	// report or the provider's Await. It is the zero value when the run ended without
	// one, e.g. at the deadline.
	Timing aei.Timing
}

// nopObserver is the default Observer: it records nothing, so an unconfigured
// plane (the reference profile) emits no metrics.
type nopObserver struct{}

func (nopObserver) ObserveRun(RunObservation) {}

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
	// phase, provider, measured duration, and timing breakdown (the metrics seam, the /metrics analog of
	// CredentialSource for /vend). Optional: nil records nothing (reference-grade). A
	// platform supplies a Prometheus implementation and serves it at /metrics, without
	// changing the plane.
	Observer Observer
	// DefaultDeadline is the wall-clock budget applied to a run whose spec carries
	// no Deadline (zero or negative). Optional: zero uses 15m. Every run gets a
	// deadline, so a dispatch without a timeout still ends.
	DefaultDeadline time.Duration
	// MaxDeadline caps a run's Deadline: a longer one is reduced to it. Optional:
	// zero uses 1h. A DefaultDeadline above MaxDeadline is reduced to MaxDeadline.
	MaxDeadline time.Duration
	// MaxConcurrentCreates bounds how many provider creates (an async provider's
	// Dispatch, or a plain provider's blocking Launch) run at once, across every
	// provider the plane fronts. Awaiting completion does not hold a slot, so it
	// bounds the create burst, not the number of runs in flight. Optional: <= 0
	// uses DefaultMaxConcurrentCreates.
	MaxConcurrentCreates int
	// MaxQueued bounds how many submitted runs may wait for a create slot. A Submit
	// beyond it is refused at once with ErrOverloaded, so a burst gets a predictable
	// answer instead of an unbounded queue. Optional: <= 0 uses DefaultMaxQueued.
	MaxQueued int
	// CreateTimeout bounds how long one provider create may hold a create slot, so
	// a provider that hangs (e.g. retrying an unavailable substrate) cannot stall
	// dispatch for every other provider. It covers minting the run credential and
	// an async provider's Dispatch, which is cancelled at the timeout; the run then
	// fails. A plain provider's blocking Launch is the whole run, so it holds its
	// slot until it returns, bounded by the run deadline. Optional: <= 0 uses
	// DefaultCreateTimeout.
	CreateTimeout time.Duration
}

// Deadline defaults (Config.DefaultDeadline, Config.MaxDeadline).
const (
	defaultRunDeadline = 15 * time.Minute
	defaultMaxDeadline = time.Hour
)

// ErrDeadlineExceeded is the error a run records when the control plane ends it
// at its deadline because neither the runtime's callback nor the provider
// reported a terminal outcome in time (errors.Is matches the recorded error).
var ErrDeadlineExceeded = errors.New("control: run deadline exceeded")

// Dispatch bounds, matching ZZ's orchestrator (two dispatch workers, a 128-deep
// enqueue buffer).
const (
	DefaultMaxConcurrentCreates = 2
	DefaultMaxQueued            = 128
	DefaultCreateTimeout        = 2 * time.Minute
)

// ErrOverloaded is returned by Submit when MaxQueued runs are already waiting for a
// create slot (on either path). Nothing about the run is recorded; the caller may
// retry later. The dispatch API maps it to 429 Too Many Requests with Retry-After.
var ErrOverloaded = errors.New("control: dispatch queue full")

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

	// creates holds one token per provider create in progress (cap
	// MaxConcurrentCreates); admit holds one per run, on either path, that has not
	// finished its create (cap MaxConcurrentCreates+MaxQueued), so it bounds the
	// queue. createTimeout bounds how long one create holds its slot.
	creates       chan struct{}
	admit         chan struct{}
	createTimeout time.Duration
	// Permits for credential calls in flight, one pool per backend, each counting
	// calls a run abandoned at its create timeout until they return (cap
	// 2*MaxConcurrentCreates, leaving room for the next creates beside calls stuck
	// in a slow backend).
	capabilityCalls map[any]*permitPool // a provider's PullsCredential, per launcher (guarded by mu)
	ticketCalls     chan struct{}       // Store.PutTicket (pull credential)
	tokenCalls      chan struct{}       // TokenAuthority.Mint (pushed credential)

	defaultDeadline time.Duration // applied to a run with no Deadline
	maxDeadline     time.Duration // cap on a run's Deadline

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
	creates, queued := cfg.MaxConcurrentCreates, cfg.MaxQueued
	if creates <= 0 {
		creates = DefaultMaxConcurrentCreates
	}
	if queued <= 0 {
		queued = DefaultMaxQueued
	}
	maxDeadline := orDur(cfg.MaxDeadline, defaultMaxDeadline)
	defaultDeadline := min(orDur(cfg.DefaultDeadline, defaultRunDeadline), maxDeadline)
	return &Plane{
		auth:            auth,
		creds:           cfg.Credentials,
		tokenTTL:        orDur(cfg.TokenTTL, 15*time.Minute),
		ticketTTL:       orDur(cfg.TicketTTL, 5*time.Minute),
		now:             now,
		log:             log,
		store:           store,
		obs:             obs,
		creates:         make(chan struct{}, creates),
		admit:           make(chan struct{}, creates+queued),
		createTimeout:   orDur(cfg.CreateTimeout, DefaultCreateTimeout),
		capabilityCalls: map[any]*permitPool{},
		ticketCalls:     make(chan struct{}, 2*creates),
		tokenCalls:      make(chan struct{}, 2*creates),
		waiters:         map[string]chan aei.Result{},

		defaultDeadline: defaultDeadline,
		maxDeadline:     maxDeadline,
	}
}

// applyDeadline sets spec's Deadline to its effective value and DeadlineAt to the
// absolute cutoff the plane enforces, measured from now — before the credential is
// minted and the provider is called, so minting and provisioning time are spent
// from the budget, and the runtime's AEI_DEADLINE is the instant the plane ends
// the run.
func (p *Plane) applyDeadline(spec aei.RunSpec) aei.RunSpec {
	spec.Deadline = p.effectiveDeadline(spec)
	spec.DeadlineAt = time.Now().Add(spec.Deadline)
	return spec
}

// ownSpec returns spec with its own copies of the caller-owned slice and maps.
// Minting and the provider run after Submit returns (and can outlive a timed-out
// Run), so they must not read a slice or map the caller may reuse for its next
// run.
func ownSpec(spec aei.RunSpec) aei.RunSpec {
	spec.Identity.Scopes = slices.Clone(spec.Identity.Scopes)
	spec.Parameters = maps.Clone(spec.Parameters)
	spec.Metadata = maps.Clone(spec.Metadata)
	return spec
}

// DeadlineFromSeconds converts a caller's timeoutSeconds to a run Deadline without
// overflowing time.Duration: a value too large to represent saturates (and is then
// reduced to the plane's maximum), and zero or a negative value is 0 (the plane's
// default). The dispatch API and the AgentRun controller both convert through it.
func DeadlineFromSeconds(n int64) time.Duration {
	switch {
	case n <= 0:
		return 0
	case n > int64(math.MaxInt64/time.Second):
		return time.Duration(math.MaxInt64)
	default:
		return time.Duration(n) * time.Second
	}
}

// effectiveDeadline returns the deadline a run is held to: the plane default when
// the caller set none, the plane maximum when the caller asked for more, and the
// caller's value otherwise.
func (p *Plane) effectiveDeadline(spec aei.RunSpec) time.Duration {
	switch d := spec.Deadline; {
	case d <= 0:
		return p.defaultDeadline
	case d > p.maxDeadline:
		p.log.Info("run deadline reduced to the maximum", "run_id", spec.ID,
			"requested", d.String(), "max", p.maxDeadline.String())
		return p.maxDeadline
	default:
		return d
	}
}

// Run dispatches spec through launcher and returns its terminal Result. It mints
// the run credential, dispatches, and races the runtime's /complete callback
// against the provider's Await and the run deadline — the first terminal signal
// wins (docs/design.md, Completion). A missing spec.ID is generated. Taking the
// launcher per call (rather than binding one) lets a single plane — one broker,
// one completion registry, one ABI server — front many providers, which is what
// the CRD controller needs (a provider per AgentProviderClass).
//
// Run shares the plane's create bound and queue with Submit, but where Submit is
// refused at the queue limit Run waits for room, bounded by ctx and the deadline.
// A provider panic or watch error comes back as a failed Result; the error is
// non-nil only when the run could not be dispatched. Run accepts the same options
// as Submit; WithProviderClass isolates the provider's credential checks.
func (p *Plane) Run(ctx context.Context, launcher aei.Launcher, spec aei.RunSpec, opts ...SubmitOption) (res aei.Result, err error) {
	if launcher == nil {
		return aei.Result{}, errors.New("control: no launcher")
	}
	if spec.ID == "" {
		spec.ID = randID()
	}
	var o submitOptions
	for _, opt := range opts {
		opt(&o)
	}
	// The caller (the AgentRun reconciler) runs this on its own goroutine, so a
	// panic that escapes here would end the controller. Provider calls are guarded
	// individually in race; this is the backstop for anything else.
	release := func() {}
	defer func() {
		if rec := recover(); rec != nil {
			release()
			res = aei.Result{Err: p.panicked(spec.ID, "run", rec)}
			err = res.Err
		}
	}()

	spec = p.applyDeadline(ownSpec(spec))
	ctx, cancel := withRunDeadline(ctx, spec)
	defer cancel()
	spec = withContextCutoff(ctx, spec) // a caller deadline sooner than the run's wins

	// Run takes an admission slot like Submit, so a run here waiting for or holding
	// a create slot counts against the queue Submit enforces. It waits for the slot
	// rather than being refused: the CRD path has no caller to answer 429 to.
	select {
	case p.admit <- struct{}{}:
	case <-ctx.Done():
		err := fmt.Errorf("control: no dispatch slot before the deadline: %w", context.Cause(ctx))
		return aei.Result{Err: err}, err
	}
	release = sync.OnceFunc(func() { <-p.admit })

	done := p.arm(spec.ID)
	defer p.disarm(spec.ID)

	// race releases admission when the create gives its slot back, which for a
	// blocking Launch can be after Run returns.
	res, _, err = p.race(ctx, launcher, spec, done, release, capabilityKey(launcher, o.class))
	return res, err
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
//
// Submit never blocks on the provider: when MaxQueued runs are already waiting for
// a create slot it returns ErrOverloaded and records nothing. A provider panic or
// watch error is recorded as a Failed run.
func (p *Plane) Submit(_ context.Context, launcher aei.Launcher, spec aei.RunSpec, opts ...SubmitOption) (string, error) {
	if launcher == nil {
		return "", errors.New("control: no launcher")
	}
	var o submitOptions
	for _, opt := range opts {
		opt(&o)
	}
	// Admission: a run holds an admission slot from here until its provider create
	// returns, so at most MaxConcurrentCreates runs are creating and at most
	// MaxQueued are waiting to, across Submit and Run. Beyond that the submission
	// is refused at once with ErrOverloaded rather than queued without bound (ZZ's
	// non-blocking enqueue).
	select {
	case p.admit <- struct{}{}:
	default:
		return "", ErrOverloaded
	}
	release := sync.OnceFunc(func() { <-p.admit })

	if spec.ID == "" {
		spec.ID = randID()
	}
	// The measured duration starts no later than the deadline's clock, so a run
	// the deadline ends is never recorded as shorter than its deadline.
	start := time.Now()
	spec = p.applyDeadline(ownSpec(spec))
	p.store.SetRunning(spec.ID)
	done := p.arm(spec.ID)

	go func() {
		defer p.disarm(spec.ID)
		// Provider calls are guarded in race, which also owns releasing admission;
		// this is the backstop for anything else on this goroutine, which no HTTP
		// server recovers.
		defer func() {
			if rec := recover(); rec != nil {
				release()
				p.finishRun(spec.ID, aei.Result{Err: p.panicked(spec.ID, "run", rec)})
			}
		}()
		// The run outlives the submit request, so it runs on its own context
		// bounded only by the run deadline (the universal backstop).
		ctx, cancel := withRunDeadline(context.Background(), spec)
		defer cancel()
		res, h, _ := p.race(ctx, launcher, spec, done, release, capabilityKey(launcher, o.class))
		if h.Provider == "" {
			h.Provider = o.provider
		}
		// Commit first, then observe what the store holds: a callback that wrote
		// before this run's own outcome is the outcome, and metrics must agree
		// with GET /runs/{id}.
		p.observe(spec, h, p.finishRun(spec.ID, res), time.Since(start))
	}()
	return spec.ID, nil
}

// SubmitOption configures one Submit call.
type SubmitOption func(*submitOptions)

type submitOptions struct {
	provider string
	class    string
}

// WithProvider names the provider the caller resolved for the run (e.g. the
// provider class's driver, "k8s-job"). It attributes the run's metrics when the
// provider's Handle does not name it — notably when a synchronous Launch has not
// returned yet because the runtime's /complete callback won the race.
func WithProvider(name string) SubmitOption {
	return func(o *submitOptions) { o.provider = name }
}

// WithProviderClass names the provider class (or other stable identity) of the
// configured launcher the run uses. The plane keeps a provider's PullsCredential
// calls in their own bounded pool per class, so a provider stuck there cannot stop
// another's runs. Without it the pool is keyed by the launcher value when that is
// comparable (a pointer, as every in-tree provider is), and otherwise by its Go
// type, which separately configured launchers of that type then share.
func WithProviderClass(name string) SubmitOption {
	return func(o *submitOptions) { o.class = name }
}

// observe reports a run's terminal outcome to the metrics Observer (the metrics
// seam). It fires once per Submit — the data-plane app path — with the run's
// taskRef, the terminal state the store recorded (whichever signal won the first
// write, so metrics never disagree with Status), the provider from its handle, and
// the duration the controller measured. The default Observer is a no-op, so an
// unconfigured plane records nothing.
func (p *Plane) observe(spec aei.RunSpec, h aei.Handle, st RunState, d time.Duration) {
	p.obs.ObserveRun(RunObservation{
		TaskRef:  spec.TaskRef,
		Phase:    st.Phase,
		Provider: h.Provider,
		Duration: d,
		Timing:   st.Timing,
	})
}

// Status returns the current state of a submitted run, and whether it is known.
func (p *Plane) Status(runID string) (RunState, bool) {
	return p.store.Run(runID)
}

// finishRun records a run's terminal outcome through the store, first-writer-wins
// (R4): the callback and the watch may both fire; whichever wins the race writes the
// terminal state and logs a failure once, and the loser is a no-op. The durable write
// is also what makes the outcome visible when a callback lands on a different replica
// than the one that dispatched (the HA seam). It returns the state the store holds
// afterwards: res when this call won, else the earlier winner.
func (p *Plane) finishRun(runID string, res aei.Result) RunState {
	st := RunState{Phase: phaseSucceeded, ResultRef: res.ResultRef, Timing: res.Timing}
	if res.Err != nil {
		st.Phase, st.Err = phaseFailed, res.Err.Error()
	}
	if !p.store.FinishRun(runID, st) {
		// A terminal outcome was already recorded; report that one.
		if won, ok := p.store.Run(runID); ok && won.Phase != phaseRunning {
			return won
		}
		return st
	}
	if res.Err != nil {
		// Otherwise only visible via GET /runs/{id} (the data-plane path keeps no
		// per-run object), so surface it: a dispatch error — e.g. the substrate or
		// its CRD is missing — reads clearly in the message here.
		p.log.Warn("run failed", "run_id", runID, "error", res.Err.Error())
	}
	return st
}

// race dispatches spec and returns the first terminal signal: the runtime's
// /complete callback (delivered on done), the provider's result (Await, or Launch
// returning for a provider without the async capability), or the deadline (ctx).
// It is the shared completion core of Run (blocking) and Submit (non-blocking).
// It also returns the provider's Handle, as far as it is known when the race
// ends, so the run can be attributed to its provider. The returned error is
// non-nil only when an async provider's Dispatch did not accept the run: Dispatch
// failed, or ctx ended before Dispatch returned.
//
// The deadline is enforced here, not delegated to the provider: when ctx ends,
// race returns at once with the context's cause (ErrDeadlineExceeded for the run
// deadline), whether or not the provider honors cancellation. A provider call
// that ignores cancellation is left to return on its own; its result goes to a
// buffered channel nobody reads, so it cannot block or overwrite the outcome.
//
// The provider create (Dispatch, or a plain provider's blocking Launch) waits for
// one of the plane's MaxConcurrentCreates slots, bounded by ctx. Once it has a
// slot, race owns the run's admission too: created is called, exactly once, when
// the create gives its slot back, even if the run's outcome arrived first.
// Minting (only after the slot is held, so no credential is spent in the queue)
// and an async Dispatch together get CreateTimeout; capKey selects the provider's
// PullsCredential permits. Every provider call is guarded: a panic becomes a
// failed Result instead of ending the process, and an error from Await fails the
// run instead of being dropped.
func (p *Plane) race(ctx context.Context, launcher aei.Launcher, spec aei.RunSpec, done chan aei.Result, created func(), capKey any) (aei.Result, aei.Handle, error) {
	type outcome struct {
		res         aei.Result
		dispatchErr error
	}
	provider := make(chan outcome, 1)
	// handle holds the provider's Handle once Dispatch or Launch has returned it;
	// a callback can win before a blocking Launch returns, when it is not known.
	var (
		hmu    sync.Mutex
		handle aei.Handle
	)
	setHandle := func(h aei.Handle) { hmu.Lock(); handle = h; hmu.Unlock() }
	getHandle := func() aei.Handle { hmu.Lock(); defer hmu.Unlock(); return handle }
	// accepted closes once an async provider's Dispatch returns without error, so
	// a deadline or cancellation that lands first is still reported as a failed
	// dispatch rather than as an accepted run that ended.
	accepted := make(chan struct{})
	async, isAsync := launcher.(aei.AsyncLauncher)
	if !isAsync {
		close(accepted) // a plain provider has no separate dispatch step
	}
	go func() {
		// Provider calls are guarded where they are made; this is the backstop for
		// anything else on this goroutine (the plane's own credential bookkeeping),
		// which nothing above would recover: the run fails and gives back what it
		// holds instead of ending the controller.
		release := created
		defer func() {
			if rec := recover(); rec != nil {
				release()
				err := p.panicked(spec.ID, "run", rec)
				select {
				case provider <- outcome{res: aei.Result{Err: err}, dispatchErr: err}:
				default:
				}
			}
		}()
		// Wait for a create slot. A run still queued when its deadline passes was
		// never handed to the provider.
		select {
		case p.creates <- struct{}{}:
		case <-ctx.Done():
			created()
			err := fmt.Errorf("control: no dispatch slot before the deadline: %w", context.Cause(ctx))
			provider <- outcome{res: aei.Result{Err: err}, dispatchErr: err}
			return
		}
		endCreate := sync.OnceFunc(func() { <-p.creates; created() })
		release = endCreate
		notDispatched := func(err error) {
			o := outcome{res: aei.Result{Err: err}}
			if isAsync {
				o.dispatchErr = err
			}
			provider <- o
		}

		// One create budget, started now that the slot is held, covers preparing
		// the credential and the provider's Dispatch, so neither a stuck
		// credential backend nor a stuck provider can hold the slot past
		// CreateTimeout.
		createCtx, cancelCreate := context.WithTimeout(ctx, p.createTimeout)
		defer cancelCreate()
		createErr := func(err error) error {
			if errors.Is(createCtx.Err(), context.DeadlineExceeded) && ctx.Err() == nil {
				return fmt.Errorf("control: provider create did not finish within %s: %w", p.createTimeout, err)
			}
			return err
		}
		// Minting runs under the deadline too: a slow credential issuer cannot
		// hold the run open, and a run whose deadline passed while its credential
		// was minted is never handed to the provider.
		cred, err := p.mintWithin(createCtx, launcher, spec, capKey)
		if ctx.Err() != nil {
			err = context.Cause(ctx)
		} else if err != nil {
			err = createErr(err)
		}
		if err != nil {
			endCreate()
			notDispatched(err)
			return
		}
		// The async capability lets us dispatch without blocking a worker and
		// observe completion by watch; a plain provider blocks in Launch.
		if isAsync {
			h, err := p.guardCreate(spec.ID, "Dispatch", func() (aei.Handle, error) { return async.Dispatch(createCtx, spec, cred) })
			setHandle(h)
			cancelCreate()
			endCreate()
			if err != nil {
				// Only a failed Dispatch is attributed to the timeout. A provider
				// that accepted the workload as the timer fired returns a handle
				// and no error; that run goes on to be watched, since a retry
				// would start a second workload beside it.
				err = createErr(err)
				provider <- outcome{res: aei.Result{Err: err}, dispatchErr: err}
				return
			}
			close(accepted)
			provider <- outcome{res: p.await(ctx, async, spec.ID, h)}
			return
		}
		// A plain provider's Launch is the whole run: it cannot be cut off at the
		// create timeout without killing the run, and there is no signal for when
		// its workload exists. So it holds its create slot (and admission) until
		// it returns, bounded by the run deadline, even if race returned first.
		cancelCreate()
		h, err := p.guardCreate(spec.ID, "Launch", func() (aei.Handle, error) { return launcher.Launch(ctx, spec, cred) })
		setHandle(h)
		endCreate()
		provider <- outcome{res: aei.Result{Err: err}}
	}()

	select {
	case r := <-done:
		return r, getHandle(), nil
	case o := <-provider:
		r, err := providerOutcome(ctx, done, o.res, o.dispatchErr)
		return r, getHandle(), err
	case <-ctx.Done():
		// A runtime callback that landed together with the deadline still wins.
		select {
		case r := <-done:
			return r, getHandle(), nil
		default:
		}
		cause := context.Cause(ctx)
		select {
		case <-accepted:
			return aei.Result{Err: cause}, getHandle(), nil
		default:
			return aei.Result{Err: cause}, getHandle(), cause // Dispatch never accepted the run
		}
	}
}

// withRunDeadline bounds ctx by the run's absolute cutoff (spec.DeadlineAt, set by
// applyDeadline). When it passes, the context's cause is an error matching both
// ErrDeadlineExceeded and context.DeadlineExceeded, which race records as the
// run's error.
func withRunDeadline(ctx context.Context, spec aei.RunSpec) (context.Context, context.CancelFunc) {
	return context.WithDeadlineCause(ctx, spec.DeadlineAt,
		fmt.Errorf("%w after %s: %w", ErrDeadlineExceeded, spec.Deadline, context.DeadlineExceeded))
}

// providerOutcome settles a run whose provider reported first. A provider that
// honors cancellation reports the context's bare error when the deadline fires;
// the plane records its own cause instead, so a timed-out run reads the same
// whichever signal won — unless a runtime callback is queued alongside the
// cancellation, which still wins.
func providerOutcome(ctx context.Context, done chan aei.Result, res aei.Result, dispatchErr error) (aei.Result, error) {
	if ctx.Err() == nil || !(errors.Is(res.Err, context.DeadlineExceeded) || errors.Is(res.Err, context.Canceled)) {
		return res, dispatchErr
	}
	select {
	case r := <-done:
		return r, nil
	default:
	}
	res.Err = context.Cause(ctx)
	if dispatchErr != nil {
		dispatchErr = res.Err
	}
	return res, dispatchErr
}

// withContextCutoff lowers spec.DeadlineAt to ctx's deadline when that is sooner
// (a Run caller's own deadline), so the provider and the runtime stop when the
// plane stops waiting.
func withContextCutoff(ctx context.Context, spec aei.RunSpec) aei.RunSpec {
	if dl, ok := ctx.Deadline(); ok && dl.Before(spec.DeadlineAt) {
		spec.DeadlineAt = dl
	}
	return spec
}

// mintWithin mints the run credential, giving up when ctx ends. Minting takes no
// context, so each step runs through bounded: a call still blocked when ctx ends
// is abandoned (its credential, if ever produced, is never handed out and expires
// unused) but keeps its permit until it returns. Each credential backend has its
// own permits (2*MaxConcurrentCreates), so outstanding calls stay bounded and an
// outage in one cannot stall the others: a stalled ticket store does not block
// runs whose token the authority signs, and vice versa. A provider's
// PullsCredential, which decides between them, has permits of its own per
// provider (capKey, from capabilityKey), so one provider stuck there does not
// stall another.
//
// The claims, including their expiry, are computed inside each step once it holds
// its permit, so waiting for capacity does not shorten the credential's life.
func (p *Plane) mintWithin(ctx context.Context, launcher aei.Launcher, spec aei.RunSpec, capKey any) (aei.Credential, error) {
	pulls := false
	if pl, ok := launcher.(aei.PullCredentialLauncher); ok {
		permits, done := p.capabilityPermits(capKey)
		if err := p.bounded(ctx, permits, done, spec.ID, "PullsCredential", func() { pulls = pl.PullsCredential() }); err != nil {
			return aei.Credential{}, err
		}
	}
	if pulls {
		var id string
		if err := p.bounded(ctx, p.ticketCalls, nil, spec.ID, "redemption ticket", func() { id = p.issueTicket(p.claims(spec)) }); err != nil {
			return aei.Credential{}, err
		}
		return aei.Credential{Value: id, Kind: aei.Ticket}, nil
	}
	var tok string
	if err := p.bounded(ctx, p.tokenCalls, nil, spec.ID, "run token", func() { tok = p.mintToken(p.claims(spec)) }); err != nil {
		return aei.Credential{}, err
	}
	return aei.Credential{Value: tok, Kind: aei.Token}, nil
}

// capabilityPermits returns the PullsCredential permits for key (cap
// 2*MaxConcurrentCreates) and a func to call once the caller is finished with
// them. An entry lives only while some run is waiting for, or holding, one of its
// permits, so the map stays as small as the set of providers with calls in
// flight, however many the plane sees over time. key must be hashable, which
// capabilityKey guarantees; the lock is released however the lookup ends.
func (p *Plane) capabilityPermits(key any) (chan struct{}, func()) {
	p.mu.Lock()
	defer p.mu.Unlock()
	e, ok := p.capabilityCalls[key]
	if !ok {
		e = &permitPool{permits: make(chan struct{}, cap(p.ticketCalls))}
		p.capabilityCalls[key] = e
	}
	e.users++
	return e.permits, func() {
		p.mu.Lock()
		defer p.mu.Unlock()
		if e.users--; e.users == 0 {
			delete(p.capabilityCalls, key)
		}
	}
}

// permitPool is one provider's PullsCredential permits and how many runs are
// waiting for or holding them.
type permitPool struct {
	permits chan struct{}
	users   int
}

// classKey keys capability permits by a caller-named provider class.
type classKey string

// capabilityKey identifies the provider a run's PullsCredential calls belong to.
// A provider class named with WithProviderClass is the stable identity of a
// configured launcher. Without one, a launcher whose value is comparable (checked
// on the value itself, including what any interface fields hold) is its own key;
// any other is keyed by its Go type, since a value with no comparable identity
// cannot be told apart from another configured the same way.
func capabilityKey(l aei.Launcher, class string) any {
	if class != "" {
		return classKey(class)
	}
	if reflect.ValueOf(l).Comparable() {
		return l
	}
	return reflect.TypeOf(l)
}

// bounded runs f, which takes no context, holding one of permits until f returns,
// and waits for it only until ctx ends. A panic in f is returned as an error.
// Results f writes are safe to read once bounded returns nil (the channel send
// orders them); after a timeout the caller must not read them. done, if not nil,
// is called once the permit is given back, or at once if none was acquired.
func (p *Plane) bounded(ctx context.Context, permits chan struct{}, done func(), runID, what string, f func()) error {
	if done == nil {
		done = func() {}
	}
	select {
	case permits <- struct{}{}:
	case <-ctx.Done():
		done()
		return fmt.Errorf("control: %s not ready (earlier calls still outstanding): %w", what, ctx.Err())
	}
	ch := make(chan error, 1)
	go func() {
		defer done()
		defer func() { <-permits }()
		defer func() {
			if rec := recover(); rec != nil {
				ch <- p.panicked(runID, what, rec)
			}
		}()
		f()
		ch <- nil
	}()
	select {
	case err := <-ch:
		return err
	case <-ctx.Done():
		return fmt.Errorf("control: %s not ready: %w", what, ctx.Err())
	}
}

// guardCreate calls a provider's Dispatch or Launch, converting a panic into an
// error. Provider code runs off any request goroutine, where nothing else would
// recover it, so without this one provider bug ends the controller.
func (p *Plane) guardCreate(runID, op string, f func() (aei.Handle, error)) (h aei.Handle, err error) {
	defer func() {
		if rec := recover(); rec != nil {
			err = p.panicked(runID, op, rec)
		}
	}()
	return f()
}

// await calls the provider's Await and folds its outcome into one Result: a panic
// or a returned error is a failed run. An Await error means the watch itself broke
// (it lost the workload, or the substrate API failed), so the run's success is
// unknown and must not be recorded as success.
func (p *Plane) await(ctx context.Context, async aei.AsyncLauncher, runID string, h aei.Handle) (res aei.Result) {
	defer func() {
		if rec := recover(); rec != nil {
			res = aei.Result{Err: p.panicked(runID, "Await", rec)}
		}
	}()
	r, err := async.Await(ctx, h)
	if err != nil && r.Err == nil {
		r.Err = fmt.Errorf("control: provider watch failed: %w", err)
	}
	return r
}

// panicked logs a recovered panic with its stack and returns the error recorded
// as the run's failure.
func (p *Plane) panicked(runID, op string, rec any) error {
	p.log.Error("provider panicked", "run_id", runID, "op", op, "panic", fmt.Sprint(rec), "stack", string(debug.Stack()))
	return fmt.Errorf("control: provider panicked in %s: %v", op, rec)
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
	claims := p.claims(spec)
	if pl, ok := launcher.(aei.PullCredentialLauncher); ok && pl.PullsCredential() {
		return aei.Credential{Value: p.issueTicket(claims), Kind: aei.Ticket}
	}
	return aei.Credential{Value: p.mintToken(claims), Kind: aei.Token}
}

// claims are the run credential's payload for spec.
func (p *Plane) claims(spec aei.RunSpec) Claims {
	return Claims{
		RunID:    spec.ID,
		Subject:  spec.Identity.Subject,
		Scopes:   spec.Identity.Scopes,
		Audience: spec.Identity.Audience,
		Exp:      p.now().Add(p.tokenTTL).Unix(),
	}
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
// JSON is the wire contract; it maps onto the domain aei.Timing. The durations are
// pointers so an explicit 0 is kept apart from an omitted field (aei.Timing.Measured).
type wireTiming struct {
	ProvisioningSeconds *float64 `json:"provisioning_seconds"`
	RuntimeSeconds      *float64 `json:"runtime_seconds"`
	ModelSeconds        *float64 `json:"model_seconds"`
	ModelCalls          int      `json:"model_calls"`
	ToolCalls           int      `json:"tool_calls"`
}

func (t wireTiming) toAEI() aei.Timing {
	out := aei.Timing{ModelCalls: t.ModelCalls, ToolCalls: t.ToolCalls}
	set := func(v *float64, dst *float64, stage aei.TimingStages) {
		if v != nil {
			*dst = *v
			out.Measured |= stage
		}
	}
	set(t.ProvisioningSeconds, &out.ProvisioningSeconds, aei.StageProvisioning)
	set(t.RuntimeSeconds, &out.RuntimeSeconds, aei.StageRuntime)
	set(t.ModelSeconds, &out.ModelSeconds, aei.StageModel)
	return out
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
	if err := json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&body); err != nil {
		// A body cut off by the listener's read timeout is retryable: answer 408,
		// which the runtime SDK retries, rather than falling through to a run-id
		// mismatch (403), which it treats as final and so would drop the outcome.
		if isTimeout(err) {
			http.Error(w, "request body timed out", http.StatusRequestTimeout)
			return
		}
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
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

// isTimeout reports whether err is a read deadline expiring, as when a client
// stalls past the server's read timeout.
func isTimeout(err error) bool {
	var ne net.Error
	return errors.Is(err, os.ErrDeadlineExceeded) || (errors.As(err, &ne) && ne.Timeout())
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
