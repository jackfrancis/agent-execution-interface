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
	Isolation string

	// Identity describes the per-run credential the platform mints and injects.
	Identity IdentityRequest

	// Deadline is the run's wall-clock budget and the universal completion
	// backstop: a provider that can be neither watched nor guaranteed to call back
	// still terminates because Deadline bounds Await.
	Deadline time.Duration

	// Metadata is labels/annotations for observability. It is never semantic —
	// nothing in AEI's behavior may depend on it.
	Metadata map[string]string
}

// Handle records where a run was placed, for observability and Await. For a
// synchronous Launcher the terminal outcome is the Launch error; the Handle adds
// the workload's identity and location.
type Handle struct {
	Provider string // the provider that placed the run, e.g. "k8s-job"
	Ref      string // provider-specific id: a Job/Pod/Sandbox name, a task id
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
