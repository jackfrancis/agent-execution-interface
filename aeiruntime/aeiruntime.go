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
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

// ABIVersion is the runtime ABI revision this SDK speaks (docs/runtime-abi.md).
const ABIVersion = "v1alpha1"

// The reserved AEI_* injection keys. A launcher fills these; the runtime reads
// them through the one decoder. Per-run keys MUST ride the dispatch (rule R1);
// config keys MAY come from the process environment.
const (
	EnvRunID        = "AEI_RUN_ID"         // unique run id; completion-correlation key
	EnvTaskRef      = "AEI_TASK_REF"       // opaque handle to the work
	EnvParams       = "AEI_PARAMS"         // opaque per-run params, a JSON object string
	EnvDispatchedAt = "AEI_DISPATCHED_AT"  // dispatch-start, unix millis
	EnvCredential   = "AEI_RUN_CREDENTIAL" // push path: the run credential (bearer)
	EnvTicket       = "AEI_RUN_TICKET"     // pull path: single-use redemption ticket
	EnvDeadline     = "AEI_DEADLINE"       // run wall-clock budget end, unix millis (optional)
	EnvControlURL   = "AEI_CONTROL_URL"    // base URL for /complete
	EnvBrokerURL    = "AEI_BROKER_URL"     // base URL for /redeem + /vend; absent ⇒ control URL
	EnvAudience     = "AEI_AUDIENCE"       // audience the run credential is bound to (optional)
	EnvABIVersion   = "AEI_ABI_VERSION"    // ABI version the launcher speaks (optional)
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
}

// Timing is the launcher-symmetric run-timing breakdown reported on /complete. The
// JSON tags are the wire contract (docs/runtime-abi.md §3).
type Timing struct {
	ProvisioningSeconds float64 `json:"provisioning_seconds"`
	RuntimeSeconds      float64 `json:"runtime_seconds"`
	ModelSeconds        float64 `json:"model_seconds,omitempty"`
	ModelCalls          int     `json:"model_calls,omitempty"`
	ToolCalls           int     `json:"tool_calls,omitempty"`
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
		},
		controlURL: controlURL,
		brokerURL:  brokerURL,
		http:       http.DefaultClient,
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
// parses. Safe to call once — the platform dedupes it against the substrate watch
// and the deadline backstop (rule R4).
func (r *Runtime) Complete(ctx context.Context, runErr error, t Timing, resultRef string) error {
	body := completeRequest{RunID: r.spec.RunID, ResultRef: resultRef, Timing: t}
	if runErr != nil {
		body.Error = runErr.Error()
	}
	return r.post(ctx, r.controlURL+"/complete", r.cred, body, nil)
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
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(b))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if bearer != "" {
		req.Header.Set("Authorization", "Bearer "+bearer)
	}
	resp, err := r.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<16))
		return fmt.Errorf("POST %s: status %d", url, resp.StatusCode)
	}
	if out == nil {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<16))
		return nil
	}
	return json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(out)
}

// Main is the runtime entrypoint helper: Load → run fn under the deadline → report
// completion with launcher-symmetric provisioning + runtime timing. A workload's
// main() is one line: aeiruntime.Main(os.Getenv, nil, myWork). An app that needs
// model/tool timing or a result_ref should call Complete itself (a duplicate is a
// harmless no-op, R4). On failure it exits non-zero after reporting.
func Main(getenv func(string) string, metadata map[string]string, fn func(context.Context, *Runtime) error) {
	rt, err := Load(context.Background(), getenv, metadata)
	if err != nil {
		fmt.Fprintln(os.Stderr, "aeiruntime:", err)
		os.Exit(2)
	}

	start := time.Now()
	var provisioning float64
	if !rt.spec.DispatchedAt.IsZero() {
		provisioning = start.Sub(rt.spec.DispatchedAt).Seconds()
	}

	ctx := context.Background()
	var cancel context.CancelFunc
	if !rt.spec.Deadline.IsZero() {
		ctx, cancel = context.WithDeadline(ctx, rt.spec.Deadline)
	} else {
		ctx, cancel = context.WithCancel(ctx)
	}
	defer cancel()

	runErr := fn(ctx, rt)

	// Report on a fresh context so completion still lands after a deadline cancel.
	reportCtx, rcancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer rcancel()
	timing := Timing{ProvisioningSeconds: provisioning, RuntimeSeconds: time.Since(start).Seconds()}
	if cerr := rt.Complete(reportCtx, runErr, timing, ""); cerr != nil {
		fmt.Fprintln(os.Stderr, "aeiruntime: completion report failed:", cerr)
	}
	if runErr != nil {
		fmt.Fprintln(os.Stderr, "aeiruntime: run failed:", runErr)
		os.Exit(1)
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
