// Package aei is the Agent Execution Interface: a substrate-neutral contract for
// dispatching an agent workload, handing it a scoped short-lived identity, and
// observing its completion — independent of whether the run lands on a Kubernetes
// Job, an isolated Sandbox, a remote sandbox control plane, or a durable A2A
// agent.
//
// This file is Contract 1 of the design (docs/design.md): the dispatch interface
// plus the optional capability interfaces a provider feature-detects with a type
// assertion. The driver registry lives in registry.go. The runtime side of the
// boundary — how a launched workload learns its RunSpec and reports back — is the
// runtime ABI (docs/runtime-abi.md), implemented by a separate runtime SDK.
//
// Design tenet #1: the required core is one method (Launch); everything else is a
// capability a provider adds by implementing an extra interface, never a fat
// interface every provider must stub.
package aei

import (
	"context"
	"time"
)

// RunSpec is the substrate-neutral description of one unit of agent work. The
// payload is deliberately opaque: AEI standardizes dispatch, not authoring, so
// TaskRef and Parameters are handles the app defines and AEI never parses.
type RunSpec struct {
	// ID is the unique run id and the completion-correlation key. A provider
	// injects it as AEI_RUN_ID; the runtime echoes it on POST /complete
	// (docs/runtime-abi.md §2.2).
	ID string

	// TaskRef is an opaque handle to the work — a queue item, a prompt-store key.
	// AEI carries it (AEI_TASK_REF) and never interprets it.
	TaskRef string

	// Parameters are per-run values that MUST ride the dispatch (rule R1): a
	// durable runtime reuses one process and a snapshotted runtime freezes its
	// env, so per-run inputs can never be image-baked. Carried opaquely as
	// AEI_PARAMS and decoded by the app's runtime.
	Parameters map[string]string

	// Isolation names the IsolationClass a provider must satisfy ("", "gvisor",
	// "kata", "sandbox", …). A provider that cannot satisfy a required class fails
	// admission (docs/design.md, the capability model).
	//
	// Providers MUST honor this or fail: a pod-shaped provider resolves it to the
	// pod's RuntimeClassName and verifies that RuntimeClass exists; a provider whose
	// isolation is fixed at provisioning (agent-substrate) accepts only the class it
	// was provisioned with. Ignoring an isolation class it cannot satisfy is the one
	// unacceptable behavior — running unisolated is fine when that is what was
	// asked for, but running unisolated under a class that claims isolation is a
	// silent security downgrade.
	Isolation string

	// Image is the runtime (agent) container image the app wants to run, supplied
	// by the app at dispatch rather than baked into the provider class — the class
	// is a generic substrate binding shared across apps, not an app-specific
	// object. A workload provider (k8s-job, agent-sandbox) runs this image; a
	// durable provider targeting a standing actor ignores it. Empty falls back to a
	// provider-class default image where one is configured (back-compat).
	Image string

	// Identity describes the per-run credential the platform mints and injects.
	Identity IdentityRequest

	// Deadline is the run's wall-clock budget and the universal completion
	// backstop: a provider that can be neither watched nor guaranteed to call back
	// still terminates because Deadline bounds Await.
	Deadline time.Duration

	// Metadata is labels/annotations for observability. It is never semantic —
	// nothing in AEI's behavior may depend on it.
	Metadata map[string]string

	// App is the name of the AgentApp this run was dispatched as (empty on the
	// declarative AgentRun path, which has no app object). Providers use it to make
	// the run's workload attributable to its origin app — an "<app>-" name prefix and
	// a part-of label — so runs from different apps that share one runtime namespace
	// stay distinguishable.
	App string

	// AppNamespace is the namespace that owns the run: the app's namespace (data
	// plane) or the AgentRun's namespace (declarative). It is the tenant boundary a
	// provider MAY place the run's workload in; it is carried here so per-app runtime
	// placement is a provider-only change, not a re-plumbing of the dispatch paths.
	AppNamespace string

	// Session, when non-empty, binds this run to a durable conversation/session. It
	// is an advisory affinity hint, never a correctness requirement: a warm provider
	// (agent-substrate, SessionWarmable) routes every run that shares a Session to the
	// same standing actor so the runtime's per-session working set stays warm across
	// turns; a stateless provider (k8s-job, agent-sandbox) ignores it — each run gets a
	// fresh workload that reconstructs the working set from the app's durable seed, so
	// correctness never depends on it, only latency. Empty ⇒ an ordinary one-shot run.
	// Carried as AEI_SESSION so the runtime keys its working-set cache by it.
	Session string
}

// Handle records where a run was placed, for observability and Await. For a
// synchronous Launcher the terminal outcome is the Launch error; the Handle adds
// the workload's identity and location.
type Handle struct {
	Provider  string // the provider that placed the run, e.g. "k8s-job"
	Ref       string // provider-specific id: a Job/Pod/Sandbox name, a task id
	Namespace string // the namespace the workload was placed in (async Await reads it)
}

// Timing is the launcher-symmetric run-timing breakdown (docs/design.md,
// Observability). ProvisioningSeconds is measured the same way for an
// asynchronous pod cold-start and a synchronous durable-actor resume, so
// launchers are comparable.
type Timing struct {
	ProvisioningSeconds float64 // dispatch-start → runtime actually working
	RuntimeSeconds      float64 // the runtime's own work
	ModelSeconds        float64 // the model share (optional)
	ModelCalls          int     // optional
	ToolCalls           int     // optional
}

// Result is the terminal outcome of a run. The result *payload* is out of scope
// (app-defined); AEI carries only success/failure and observability. ResultRef is
// an opaque pointer to where the result lives (a worklist item, an object-store
// key) — AEI stores it but never parses it (docs/runtime-abi.md §2.2).
type Result struct {
	Err       error
	ResultRef string
	Timing    Timing
}

// IdentityRequest is what the platform mints for a run: a per-run,
// capability-scoped, short-TTL, audience-bound credential whose authority is
// platform authority ∩ delegated consent ∩ provider-class policy
// (docs/design.md, Contract 3).
type IdentityRequest struct {
	Subject  string   // the acting principal (e.g. a user) — delegated authorization
	Scopes   []string // capability scopes the run may exercise
	Audience string   // the plane/audience the minted credential is bound to
}

// CredentialKind distinguishes a pushed bearer token from a pull redemption
// ticket (docs/design.md Contract 3; docs/runtime-abi.md §2.1).
type CredentialKind int

const (
	// Token is a bearer credential injected at dispatch — the push path.
	Token CredentialKind = iota
	// Ticket is a single-use redemption ticket the runtime exchanges for a
	// credential before doing any work — the pull path. Required where a control
	// plane persists task history or snapshots runtime memory, so a live
	// credential never rides the dispatch (rule R2).
	Ticket
)

// Credential is what a provider injects into a run. Its Value is opaque to the
// provider: a bearer token (push) or a redemption ticket (pull). The provider
// chooses delivery; the platform chooses what to mint.
type Credential struct {
	Value string
	Kind  CredentialKind
}

// Launcher is the one required method a provider must satisfy: dispatch a run and
// report where it landed. This is the whole mandatory contract; async completion
// and pull-credential identity are optional capabilities below.
type Launcher interface {
	Launch(ctx context.Context, spec RunSpec, cred Credential) (Handle, error)
}

// AsyncLauncher is the optional capability that separates "create/accept" from
// "await completion", so a long-running or un-watchable runtime never pins a
// dispatch worker and completion can arrive by callback OR by watching the
// substrate (docs/design.md, Completion). An orchestrator prefers it when present:
// Dispatch runs on the bounded worker pool, Await on its own goroutine. Await must
// derive everything it needs from the Handle, so it can run with no shared state.
type AsyncLauncher interface {
	Launcher
	Dispatch(ctx context.Context, spec RunSpec, cred Credential) (Handle, error)
	Await(ctx context.Context, h Handle) (Result, error)
}

// PullCredentialLauncher is the optional capability declaring that the runtime
// pulls its credential by redeeming a single-use ticket, instead of receiving it
// at dispatch. When a provider reports PullsCredential, the platform hands it a
// Credential of Kind Ticket; otherwise it is handed a Token (docs/design.md
// Contract 3; docs/runtime-abi.md §2.1).
type PullCredentialLauncher interface {
	Launcher
	PullsCredential() bool
}

// Warmer is the optional capability of a provider that materializes long-lived,
// per-app infrastructure (a durable actor and its golden snapshot) rather than a
// per-run workload. The platform calls Warm when an app binds the provider's class,
// so that infrastructure is provisioned ahead of the first run and the first dispatch
// does not pay the cold start. Providers that create a fresh workload per run (a Job,
// a Sandbox) need no warming and do not implement it. Warm must be idempotent — it
// runs on every reconcile of the app — and a no-op when the app cannot be warmed (no
// image, or the provider is not configured to provision).
type Warmer interface {
	Launcher
	Warm(ctx context.Context, spec RunSpec) error
}

// SessionWarmable is the optional capability of a provider that can keep a session's
// runtime warm across runs — routing every run that shares a RunSpec.Session to the
// same standing, stateful instance, so the runtime's per-session working set survives
// between turns. It is advisory, never required: the app always supplies a
// reconstructible seed and works correctly on a stateless provider that rebuilds the
// working set per run. A UI or admission may feature-detect it (render "warm" vs
// "rebuilding", or place a latency-sensitive session on a warm class). A durable
// provider (agent-substrate) reports true; per-run providers (k8s-job, agent-sandbox)
// do not implement it.
type SessionWarmable interface {
	Launcher
	WarmsSession() bool
}
