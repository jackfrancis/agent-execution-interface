// Package aeiruntime is the runtime-author half of the Agent Execution Interface:
// the "little boilerplate" a launched workload uses to learn its RunSpec, redeem a
// pull-ticket, vend downstream credentials, and report completion. It implements
// the runtime ABI (docs/runtime-abi.md) of the agent-execution-interface design.
//
// It is a deliberately SEPARATE module with no dependency on the aei core package
// or any provider: the two halves meet at the wire (the AEI_* injection keys and
// the /redeem · /complete · /vend JSON routes), not in shared Go types. So an app
// runtime that imports this SDK inherits zero provider dependencies (client-go and
// friends never enter its module graph) — design tenet #5.
//
// The SDK encodes the runtime-authoring rules R1–R4: it reads the RunSpec through
// one decoder (metadata-first, env-fallback), holds the run credential and any
// vended credential in memory only, and reports completion idempotently.
package aeiruntime

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"
)

// ABIVersion is the runtime ABI revision this SDK speaks (docs/runtime-abi.md).
const ABIVersion = "v1alpha1"

// The reserved AEI_* injection keys. A launcher fills these; the runtime reads
// them through the one decoder. Per-run keys MUST ride the dispatch (rule R1);
// config keys MAY come from the process environment.
const (
	EnvRunID         = "AEI_RUN_ID"              // unique run id; completion-correlation key
	EnvTaskRef       = "AEI_TASK_REF"            // opaque handle to the work
	EnvParams        = "AEI_PARAMS"              // opaque per-run params, a JSON object string
	EnvDispatchedAt  = "AEI_DISPATCHED_AT"       // dispatch-start, unix millis
	EnvCredential    = "AEI_RUN_CREDENTIAL"      // push path: the run credential (bearer)
	EnvTicket        = "AEI_RUN_TICKET"          // pull path: single-use redemption ticket
	EnvDeadline      = "AEI_DEADLINE"            // run wall-clock budget end, unix millis (optional)
	EnvControlURL    = "AEI_CONTROL_URL"         // base URL for /complete
	EnvBrokerURL     = "AEI_BROKER_URL"          // base URL for /redeem + /vend; absent ⇒ control URL
	EnvAudience      = "AEI_AUDIENCE"            // audience the run credential is bound to (optional)
	EnvABIVersion    = "AEI_ABI_VERSION"         // ABI version the launcher speaks (optional)
	EnvServeAddr     = "AEI_SERVE_ADDR"          // durable serve mode: bind address (e.g. ":8000"). Its presence runs the runtime as a standing actor (the serve loop) instead of one shot. A config key set by the durable launcher/ActorTemplate, never per-run.
	EnvMaxConcurrent = "AEI_MAX_CONCURRENT_RUNS" // durable serve mode: max runs a standing actor executes at once. At capacity the actor sheds new tasks with a retryable 503 (the provider's warm-up retry absorbs it) rather than over-committing. A config key from the ActorTemplate; unset/<=0 ⇒ unbounded.
	EnvSession       = "AEI_SESSION"             // durable session/conversation key (optional; the runtime keys its warm working-set cache by it, and a warm provider gives the session actor affinity).
)

// RunSpec is the runtime's view of a run — a subset of the dispatch-side RunSpec.
// Isolation and the identity request are dispatch-side and never reach the
// runtime; the credential they produced arrives out-of-band (injected or redeemed).
type RunSpec struct {
	RunID        string
	TaskRef      string
	Parameters   map[string]string // decoded from AEI_PARAMS; app-defined
	DispatchedAt time.Time         // zero if AEI_DISPATCHED_AT absent
	Deadline     time.Time         // zero if AEI_DEADLINE absent
	Session      string            // AEI_SESSION; "" ⇒ a one-shot run (no working-set caching)
}

// Timing is the launcher-symmetric run-timing breakdown reported on /complete
// (docs/runtime-abi.md §3). A duration is sent when it is positive or its stage is
// in Measured, and omitted otherwise, so an empty Timing reports no durations and a
// measured zero (e.g. a run that made no model calls) survives encoding.
type Timing struct {
	ProvisioningSeconds float64
	RuntimeSeconds      float64
	ModelSeconds        float64
	ModelCalls          int
	ToolCalls           int

	// Measured marks durations the runtime measured even when they are zero. A
	// positive duration is sent whether or not its stage is set.
	Measured Stages
}

// Stages is a set of Timing duration fields.
type Stages uint8

// The Timing duration fields.
const (
	StageProvisioning Stages = 1 << iota
	StageRuntime
	StageModel
)

// timingWire is the /complete timing object's JSON shape. The snake_case keys are
// the wire contract; the durations are pointers so presence is explicit.
type timingWire struct {
	ProvisioningSeconds *float64 `json:"provisioning_seconds,omitempty"`
	RuntimeSeconds      *float64 `json:"runtime_seconds,omitempty"`
	ModelSeconds        *float64 `json:"model_seconds,omitempty"`
	ModelCalls          int      `json:"model_calls,omitempty"`
	ToolCalls           int      `json:"tool_calls,omitempty"`
}

// MarshalJSON encodes the /complete timing object; a duration is present only when
// it is positive or measured (see Timing).
func (t Timing) MarshalJSON() ([]byte, error) {
	dur := func(v float64, s Stages) *float64 {
		if v > 0 || t.Measured&s != 0 {
			return &v
		}
		return nil
	}
	return json.Marshal(timingWire{
		ProvisioningSeconds: dur(t.ProvisioningSeconds, StageProvisioning),
		RuntimeSeconds:      dur(t.RuntimeSeconds, StageRuntime),
		ModelSeconds:        dur(t.ModelSeconds, StageModel),
		ModelCalls:          t.ModelCalls,
		ToolCalls:           t.ToolCalls,
	})
}

// UnmarshalJSON decodes the /complete timing object, marking each duration present
// on the wire as measured, so a decoded report re-encodes unchanged.
func (t *Timing) UnmarshalJSON(b []byte) error {
	var w timingWire
	if err := json.Unmarshal(b, &w); err != nil {
		return err
	}
	*t = Timing{ModelCalls: w.ModelCalls, ToolCalls: w.ToolCalls}
	set := func(v *float64, dst *float64, s Stages) {
		if v != nil {
			*dst = *v
			t.Measured |= s
		}
	}
	set(w.ProvisioningSeconds, &t.ProvisioningSeconds, StageProvisioning)
	set(w.RuntimeSeconds, &t.RuntimeSeconds, StageRuntime)
	set(w.ModelSeconds, &t.ModelSeconds, StageModel)
	return nil
}

// Credential is a short-lived bearer credential the runtime holds in memory only
// (rule R2): the run credential (redeemed or injected) or a vended downstream one.
type Credential struct {
	Value     string
	ExpiresIn int // seconds; 0 if the issuer did not say
}

// Runtime is a loaded run: its decoded RunSpec, the control/broker endpoints, and
// the run credential held ephemerally. Build one with Load.
type Runtime struct {
	spec       RunSpec
	controlURL string
	brokerURL  string
	cred       string // the run credential; in memory only, never logged (R2)
	http       *http.Client
	// oneShot marks a run on a workload substrate, where the process must exit
	// for the provider's watch to see the outcome; Complete then bounds its
	// retries by the run deadline (oneShotReportPlan).
	oneShot bool
}

// Load runs the one decoder (metadata-first, env-fallback), then resolves the run
// credential: a pushed AEI_RUN_CREDENTIAL is used directly, while an
// AEI_RUN_TICKET is redeemed at {broker}/redeem before Load returns — so the
// caller always holds a live credential and never sees the pull/push distinction.
// metadata is nil for a workload substrate (env transport) and the A2A message
// metadata for a durable one.
func Load(ctx context.Context, getenv func(string) string, metadata map[string]string) (*Runtime, error) {
	get := func(key string) string {
		if metadata != nil {
			if v, ok := metadata[key]; ok && v != "" {
				return v // metadata-first; an empty value does not shadow env
			}
		}
		return getenv(key)
	}

	runID := get(EnvRunID)
	if runID == "" {
		return nil, fmt.Errorf("aeiruntime: %s is required", EnvRunID)
	}
	controlURL := strings.TrimRight(get(EnvControlURL), "/")
	if controlURL == "" {
		return nil, fmt.Errorf("aeiruntime: %s is required", EnvControlURL)
	}
	brokerURL := strings.TrimRight(get(EnvBrokerURL), "/")
	if brokerURL == "" {
		brokerURL = controlURL // B-with-default: broker collapses onto control
	}
	// Reject an endpoint no request can be sent to now, rather than letting
	// Complete retry a failure that can never clear.
	for key, u := range map[string]string{EnvControlURL: controlURL, EnvBrokerURL: brokerURL} {
		if err := checkEndpoint(u); err != nil {
			return nil, fmt.Errorf("aeiruntime: %s: %w", key, err)
		}
	}

	params, err := decodeParams(get(EnvParams))
	if err != nil {
		return nil, fmt.Errorf("aeiruntime: decode %s: %w", EnvParams, err)
	}

	rt := &Runtime{
		spec: RunSpec{
			RunID:        runID,
			TaskRef:      get(EnvTaskRef),
			Parameters:   params,
			DispatchedAt: parseUnixMillis(get(EnvDispatchedAt)),
			Deadline:     parseUnixMillis(get(EnvDeadline)),
			Session:      get(EnvSession),
		},
		controlURL: controlURL,
		brokerURL:  brokerURL,
		http:       &http.Client{Timeout: requestTimeout},
		// No message metadata means the env transport of a workload substrate
		// (see Load's doc); the durable serve loop clears this for its runs.
		oneShot: metadata == nil,
	}

	// Exactly one of credential/ticket is present (docs/runtime-abi.md §1.1). A
	// pushed credential is used as-is; a ticket is redeemed for one now.
	switch cred, ticket := get(EnvCredential), get(EnvTicket); {
	case cred != "":
		rt.cred = cred
	case ticket != "":
		c, err := rt.redeem(ctx, ticket)
		if err != nil {
			return nil, fmt.Errorf("aeiruntime: redeem ticket: %w", err)
		}
		rt.cred = c.Value
	}
	return rt, nil
}

// Spec returns the decoded run description.
func (r *Runtime) Spec() RunSpec { return r.spec }

// Credential returns the run credential the runtime holds (injected on the push
// path, or redeemed on the pull path), so a runtime can authenticate calls to its
// own application backend with the same audience-bound credential it uses for the
// ABI. Hold it in memory only; never log or persist it (rule R2).
func (r *Runtime) Credential() string { return r.cred }

// Vend brokers a short-lived downstream credential (a model key, a user's Git
// token) at {broker}/vend, so the runtime never holds a standing secret. The
// result is held in memory for the run only; never log or persist it (R2).
func (r *Runtime) Vend(ctx context.Context, provider string) (Credential, error) {
	var out struct {
		Credential string `json:"credential"`
		TokenType  string `json:"token_type"`
		ExpiresIn  int    `json:"expires_in"`
	}
	if err := r.post(ctx, r.brokerURL+"/vend", r.cred, map[string]string{"provider": provider}, &out); err != nil {
		return Credential{}, err
	}
	return Credential{Value: out.Credential, ExpiresIn: out.ExpiresIn}, nil
}

// completeRequest is the POST /complete body (docs/runtime-abi.md §2.2).
type completeRequest struct {
	RunID     string `json:"run_id"`
	Error     string `json:"error,omitempty"`
	ResultRef string `json:"result_ref,omitempty"`
	Timing    Timing `json:"timing"`
}

// Complete reports the terminal outcome at {control}/complete. runErr nil is
// success; resultRef is an optional opaque pointer the platform stores but never
// parses. The platform records the first outcome for a run and ignores the rest
// (rule R4), so Complete retries a failed delivery until ctx ends: on a substrate
// with no watch (a durable actor) this call is the only success signal, and one
// dropped request would otherwise turn a finished run into a timeout. Network
// errors and 408/429/5xx responses are retried with jittered exponential backoff;
// any other 4xx (an expired or foreign credential) is final, and so is a request
// that cannot be built (an unencodable Timing such as NaN). Bound the total time
// with ctx. On a workload substrate Complete also stops in time for the process to
// exit before the run deadline (oneShotReportPlan), whether the SDK or the app
// calls it.
func (r *Runtime) Complete(ctx context.Context, runErr error, t Timing, resultRef string) error {
	req := completeRequest{RunID: r.spec.RunID, ResultRef: resultRef, Timing: t}
	if runErr != nil {
		req.Error = runErr.Error()
	}
	body, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("aeiruntime: encode completion: %w", err)
	}
	retry := true
	if r.oneShot {
		budget, ok := oneShotReportPlan(r.spec.Deadline, time.Now())
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, budget)
		defer cancel()
		retry = ok
	}
	backoff := completeBackoffMin
	for {
		attemptCtx, cancel := context.WithTimeout(ctx, completeAttemptTimeout)
		err := r.send(attemptCtx, r.controlURL+"/complete", r.cred, body, nil)
		cancel()
		if err == nil || !retry || !retryable(err) {
			return err
		}
		// Full jitter in [backoff/2, backoff) keeps many runtimes finishing at once
		// (a controller restart) from retrying in lockstep.
		wait := backoff/2 + rand.N(backoff/2)
		select {
		case <-ctx.Done():
			return fmt.Errorf("%w (gave up: %w)", err, context.Cause(ctx))
		case <-time.After(wait):
		}
		backoff = min(2*backoff, completeBackoffMax)
	}
}

// Completion delivery bounds. Each attempt gets completeAttemptTimeout so a hung
// controller connection cannot consume the caller's whole budget; between attempts
// the delay doubles from completeBackoffMin to completeBackoffMax.
const (
	completeAttemptTimeout = 10 * time.Second
	completeBackoffMin     = 200 * time.Millisecond
	completeBackoffMax     = 5 * time.Second
	// requestTimeout caps any single runtime ABI request (redeem, vend, complete).
	requestTimeout = 30 * time.Second
	// completionBudget is how long a durable actor keeps retrying a run's
	// completion report. Nothing else can report the run's success there, and the
	// actor keeps serving other tasks while it retries.
	completionBudget = 2 * time.Minute
	// A one-shot workload must exit for its provider's watch to see the outcome,
	// so it retries for at most oneShotCompletionBudget, and stops
	// oneShotExitMargin before the run deadline: time for the process to exit, the
	// pod status to update, and the provider's watch (a 2s poll) to observe it
	// before the controller's own deadline. Inside that margin it makes one
	// attempt of at most oneShotFinalAttempt and no retries.
	oneShotCompletionBudget = 30 * time.Second
	oneShotExitMargin       = 10 * time.Second
	oneShotFinalAttempt     = 500 * time.Millisecond
)

// oneShotReportPlan is how long a one-shot workload that finishes reporting at
// now may spend on it, and whether it may retry. AEI_DEADLINE is the controller's
// own cutoff once the plane stamps it (RunSpec.DeadlineAt); before that it is
// the provider's dispatch time plus the full budget, slightly later.
func oneShotReportPlan(deadline, now time.Time) (budget time.Duration, retry bool) {
	if deadline.IsZero() {
		return oneShotCompletionBudget, true
	}
	if left := deadline.Sub(now) - oneShotExitMargin; left > 0 {
		return min(oneShotCompletionBudget, left), true
	}
	return oneShotFinalAttempt, false
}

// checkEndpoint reports whether raw is an absolute http(s) URL with a host.
func checkEndpoint(raw string) error {
	u, err := url.Parse(raw)
	if err != nil {
		return err
	}
	if (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return fmt.Errorf("want an http:// or https:// URL with a host, got %q", raw)
	}
	return nil
}

// statusError is a non-2xx response from the control plane or broker.
type statusError struct {
	url  string
	code int
}

func (e *statusError) Error() string { return fmt.Sprintf("POST %s: status %d", e.url, e.code) }

// transportError is a request that was sent, or attempted, but got no response:
// refused, reset, or timed out.
type transportError struct{ err error }

func (e *transportError) Error() string { return e.err.Error() }
func (e *transportError) Unwrap() error { return e.err }

// retryable reports whether a failed completion attempt may succeed if repeated:
// a transport failure or a 408/429/5xx response. Anything else, such as a request
// that could not be built, fails the same way every time.
func retryable(err error) bool {
	var se *statusError
	if errors.As(err, &se) {
		return se.code == http.StatusRequestTimeout || se.code == http.StatusTooManyRequests || se.code >= 500
	}
	var te *transportError
	return errors.As(err, &te)
}

// redeem exchanges a single-use ticket for the run credential at {broker}/redeem.
// The ticket is the authorization (the platform issues exactly one per run), so
// the request carries no bearer.
func (r *Runtime) redeem(ctx context.Context, ticket string) (Credential, error) {
	var out struct {
		Credential string `json:"credential"`
		ExpiresIn  int    `json:"expires_in"`
	}
	if err := r.post(ctx, r.brokerURL+"/redeem", "", map[string]string{"ticket": ticket}, &out); err != nil {
		return Credential{}, err
	}
	return Credential{Value: out.Credential, ExpiresIn: out.ExpiresIn}, nil
}

// post sends a JSON body to url, optionally bearer-authed, and decodes a JSON
// response into out (nil to discard). A non-2xx status is an error.
func (r *Runtime) post(ctx context.Context, url, bearer string, in, out any) error {
	b, err := json.Marshal(in)
	if err != nil {
		return err
	}
	return r.send(ctx, url, bearer, b, out)
}

// send posts an encoded JSON body. A failure to get any response is a
// *transportError and a non-2xx response a *statusError.
func (r *Runtime) send(ctx context.Context, url, bearer string, body []byte, out any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if bearer != "" {
		req.Header.Set("Authorization", "Bearer "+bearer)
	}
	resp, err := r.http.Do(req)
	if err != nil {
		return &transportError{err: err}
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<16))
		return &statusError{url: url, code: resp.StatusCode}
	}
	if out == nil {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<16))
		return nil
	}
	return json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(out)
}

// Main is the one runtime entrypoint, and it is substrate-agnostic: a workload's
// main() is one line — aeiruntime.Main(os.Getenv, nil, myWork) — and the same
// binary runs unchanged on either substrate. On a workload substrate it runs one
// shot: Load → run fn under the deadline → report completion with launcher-
// symmetric provisioning + runtime timing, then exit. On a durable substrate the
// launcher sets AEI_SERVE_ADDR, so Main instead serves the long-lived actor loop,
// running fn once per received task. The app never branches on the substrate — the
// platform selects the lifecycle through injected config, so the substrate is an
// operational choice, not conditional application code.
//
// An app that needs model/tool timing or a result_ref should call Complete itself
// (a duplicate is a harmless no-op, R4). On a one-shot failure it exits non-zero
// after reporting.
func Main(getenv func(string) string, metadata map[string]string, fn func(context.Context, *Runtime) error) {
	// Durable substrate: a standing actor sets AEI_SERVE_ADDR, so run the long-lived
	// serve loop instead of one shot. main() and fn are identical either way.
	if addr := getenv(EnvServeAddr); addr != "" {
		ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		defer stop()
		if err := serve(ctx, addr, getenv, fn); err != nil {
			fmt.Fprintln(os.Stderr, "aeiruntime:", err)
			os.Exit(2)
		}
		return
	}

	// Workload substrate: one shot.
	if code := runOneShot(getenv, metadata, fn); code != 0 {
		os.Exit(code)
	}
}

// runOneShot is Main on a workload substrate: Load, run fn under the deadline,
// report completion, and return the process exit code. Complete bounds the
// report by oneShotReportPlan, so a callback outage cannot keep the process alive
// past the point where the provider's watch can still observe a successful exit.
func runOneShot(getenv func(string) string, metadata map[string]string, fn func(context.Context, *Runtime) error) int {
	rt, err := Load(context.Background(), getenv, metadata)
	if err != nil {
		fmt.Fprintln(os.Stderr, "aeiruntime:", err)
		return 2
	}
	timing, runErr := runTask(context.Background(), rt, fn)
	reportCompletion(rt, runErr, timing, oneShotCompletionBudget)
	if runErr != nil {
		fmt.Fprintln(os.Stderr, "aeiruntime: run failed:", runErr)
		return 1
	}
	return 0
}

// runTask runs fn once under the run's deadline (AEI_DEADLINE) and returns the
// outcome with launcher-symmetric provisioning + runtime timing. It is the single
// run bracket shared by the one-shot Main and the durable serve loop, so a run is
// driven identically on either substrate.
func runTask(ctx context.Context, rt *Runtime, fn func(context.Context, *Runtime) error) (Timing, error) {
	start := time.Now()
	t := Timing{Measured: StageRuntime}
	if !rt.spec.DispatchedAt.IsZero() {
		t.ProvisioningSeconds = start.Sub(rt.spec.DispatchedAt).Seconds()
		t.Measured |= StageProvisioning
	}
	runCtx := ctx
	var cancel context.CancelFunc
	if !rt.spec.Deadline.IsZero() {
		runCtx, cancel = context.WithDeadline(ctx, rt.spec.Deadline)
	} else {
		runCtx, cancel = context.WithCancel(ctx)
	}
	defer cancel()
	runErr := fn(runCtx, rt)
	t.RuntimeSeconds = time.Since(start).Seconds()
	return t, runErr
}

// reportCompletion posts the terminal outcome on a fresh context so it still lands
// after a deadline cancel (rule R4), retrying for up to budget. A final failure is
// logged, not fatal: on a workload substrate the provider's watch still reports
// the outcome, and on a durable one the deadline backstop ends the run.
func reportCompletion(rt *Runtime, runErr error, t Timing, budget time.Duration) {
	reportCtx, cancel := context.WithTimeout(context.Background(), budget)
	defer cancel()
	if cerr := rt.Complete(reportCtx, runErr, t, ""); cerr != nil {
		fmt.Fprintln(os.Stderr, "aeiruntime: completion report failed:", cerr)
	}
}

// decodeParams parses AEI_PARAMS (a JSON object of string→string). Empty is an
// empty map, not an error.
func decodeParams(s string) (map[string]string, error) {
	if strings.TrimSpace(s) == "" {
		return map[string]string{}, nil
	}
	m := map[string]string{}
	if err := json.Unmarshal([]byte(s), &m); err != nil {
		return nil, err
	}
	return m, nil
}

// parseUnixMillis parses a unix-millis timestamp, returning the zero time for an
// empty or malformed value (both optional fields).
func parseUnixMillis(s string) time.Time {
	if s == "" {
		return time.Time{}
	}
	ms, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return time.Time{}
	}
	return time.UnixMilli(ms)
}
