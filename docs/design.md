# AEI design

*This is the plan. It specifies the three contracts AEI standardizes, the Go
interface and the declarative CRDs, the capability model, and how AEI composes
with the ecosystem. The problem it solves is in [motivation.md](motivation.md);
the build sequence is in [roadmap.md](roadmap.md).*

Everything here is a **draft for comment**. Names and shapes will change; the
tenets are what we are trying to hold fixed.

---

## Design tenets

1. **Thin required core, optional capabilities.** One required method (dispatch);
   everything else — async completion, pull-credential identity, isolation,
   streaming, snapshotting — is a capability a provider adds by implementing an
   extra interface. Never a fat interface every provider must stub.
2. **Separate dispatch from the runtime ABI.** The launcher standardizes
   *where/how/when*; a separate, thinner contract standardizes *what the running
   agent does and how it reports back*. The runtime ABI is the real portability
   boundary.
3. **Decouple "created" from "done."** Completion must be expressible as a
   callback *or* a watch, because durable and remote runtimes cannot be
   watched-to-done. Race them.
4. **Identity is a seam, short-lived by construction, and more than attestation.**
   The platform mints a per-run, capability-scoped, short-TTL credential;
   providers choose push (inject at dispatch) or pull (redeem a single-use
   ticket). Downstream provider credentials are *brokered on demand*, never
   standing on the runtime.
5. **Config-selected drivers; the caller depends on no provider.** A registry
   keyed by name (à la `database/sql`, CSI `StorageClass`, CRI `RuntimeClass`);
   provider client libraries never enter the application's dependency graph.
6. **Selection is an admin-owned class, not an app field.** The app references a
   class *name* (or a cluster default); the admin binds that class to a provider
   and its policy. This is what makes the same `AgentRun` portable across
   clusters.
7. **Isolation is a selector, not a fork.** Express isolation as a named class
   (mirroring `RuntimeClass`) resolved by the provider — gVisor, Kata, a
   `Sandbox` CR, or an off-cluster micro-VM.
8. **Portability constraints are first-class rules, not provider trivia.** Durable
   and snapshotted substrates impose obligations on the *runtime author* (params
   must ride the dispatch; no credential at rest). AEI states these as rules and
   ships an SDK that encodes them, rather than hiding them and letting apps break
   on the next substrate.

---

## Three personas

AEI serves three developers, and keeping them distinct keeps the interface small:

| Persona | Wants | AEI surface |
| --- | --- | --- |
| **App developer** | dispatch a run and get a result, without importing a substrate SDK | the **dispatch API** (`AgentApp` + a submit call) or the `AgentRun` CRD |
| **Provider author** | plug a substrate in without editing the core | one interface + `Register()` from `init()` |
| **Runtime author** | write agent code that runs on any substrate | the **runtime SDK** (encodes the portability rules) |

---

## The three contracts

A reusable standard is not one interface. The launcher is the anchor, but it
drags two more contracts with it — and those two are where the ecosystem's real
pain (credential injection, completion, portability) actually lives.

### Contract 1 — Dispatch (the `Launcher`)

The dispatch interface is a thin required core plus optional capability
interfaces the orchestrator feature-detects with a type assertion.

```go
package aei // Agent Execution Interface

// RunSpec is the substrate-neutral description of one unit of agent work. The
// payload is deliberately opaque: AEI standardizes dispatch, not authoring.
type RunSpec struct {
    ID         string            // unique run id; the completion-correlation key
    TaskRef    string            // opaque handle to the work (a queue item, a prompt-store key, …)
    Parameters map[string]string // per-run values that MUST ride the dispatch (see Contract 2, R1)
    Isolation  string            // IsolationClass name: "", "gvisor", "kata", "sandbox", …
    Identity   IdentityRequest   // what to mint/inject (subject, scopes, audience)
    Deadline   time.Duration     // the run's wall-clock budget; also the completion backstop
    Metadata   map[string]string // labels/annotations for observability
}

// Handle records where a run was placed, for observability and Await.
type Handle struct{ Provider, Ref string }

// Result is the terminal outcome. The result *payload* is out of scope (A2A /
// app-defined); AEI carries only success/failure and observability.
type Result struct {
    Err     error
    Timing  Timing // provisioning / runtime / model breakdown (optional, see Observability)
}

// (1) REQUIRED — the whole contract a provider must satisfy.
type Launcher interface {
    Launch(ctx context.Context, spec RunSpec, cred Credential) (Handle, error)
}

// (2) OPTIONAL — separate "create/accept" from "await completion", so a
// long-running or un-watchable runtime never pins a dispatch worker and
// completion can arrive by callback OR by watching the substrate.
type AsyncLauncher interface {
    Launcher
    Dispatch(ctx context.Context, spec RunSpec, cred Credential) (Handle, error)
    Await(ctx context.Context, h Handle) (Result, error)
}

// (3) OPTIONAL — the runtime pulls its credential by redeeming a single-use
// ticket, instead of receiving it at dispatch. Required for control planes that
// persist task history or snapshot runtime memory.
type PullCredentialLauncher interface {
    Launcher
    PullsCredential() bool
}
```

**Registry** (the `database/sql` pattern). Providers self-register under a name;
the app builds the one named by configuration. The registry depends *only* on the
`Launcher` interface, so provider client libraries stay out of the caller's
dependency graph.

```go
func Register(name string, f Factory)                 // from each provider's init()
func Build(name string, cfg Config) (Launcher, error) // build the one selected
func Names() []string                                 // diagnostics / "unknown provider"
```

A provider activates via a **blank import** and is chosen by class (see the CRD)
or, for embedded use, a config value. Adding a provider touches **no core wiring
and no selection switch** — a new package plus one blank import.

### Contract 2 — the runtime ABI (the real portability boundary)

The runtime is *anything that holds an AEI-minted run credential and speaks a
small control ABI back to the platform.* Because the runtime only ever speaks
this ABI plus a fixed injection convention, **the same runtime binary runs on
every substrate**, and swapping substrates never ports business logic.

> The concrete wire contract — the generic `AEI_*` injection keys, the one
> decoder, and the `/redeem` · `/complete` · `/vend` routes — is specified in
> [runtime-abi.md](runtime-abi.md). This section is the rationale; that sub-spec
> is what a runtime author implements against.

AEI standardizes exactly three things in the runtime ABI, and nothing more:

1. **Injection convention.** How a runtime learns its `RunSpec`. Two transports,
   one decoder: environment variables for workload-creating providers; message
   metadata (A2A) for durable providers. A conformant runtime reads both through
   a single decoder so it cannot tell which substrate it is on.
2. **Ticket redemption** (pull path). `POST /redeem {ticket}` → the run
   credential, for providers that declared `PullsCredential()`.
3. **Completion callback.** `POST /complete {runID, error?, timing?}` — the fast
   path the orchestrator races against the substrate watch.

Everything else a runtime does — reading its input data, writing its results — is
**app-defined and out of AEI's scope.** AEI defines *lifecycle*, not *payload*.

**Credential brokering** is an *optional* identity module (see Contract 3): a
runtime that needs a downstream provider credential (e.g., a user's GitHub token)
calls `POST /vend {provider}` and receives a short-lived credential the platform
mints on demand — so the runtime never holds a standing secret.

#### Runtime-authoring rules

These are consequences of the durable/snapshotted shapes, and they are **rules,
not provider trivia** — a runtime that violates them is not portable:

- **R1 — Per-run values ride the dispatch.** A runtime MUST take its per-run
  inputs from `RunSpec` (env or message metadata), never from image-baked
  configuration, because a durable runtime reuses one process across runs and a
  snapshotted runtime freezes its environment in a golden image.
- **R2 — No credential at rest.** A runtime MUST hold credentials in memory for
  the duration of a run only, and MUST NOT log or persist them, because task
  history and RAM snapshots persist at rest. Where the control plane persists,
  the provider MUST use the pull-ticket so the live credential never rides the
  dispatch at all.
- **R3 — Multiplex-safe or declare otherwise.** A runtime targeting a durable
  *multiplexed* provider MUST NOT keep cross-run global state (no bleed between
  runs); otherwise it MUST declare `runtimeLifecycle: perRun` so admission
  rejects a multiplexed provider (see the capability model).
- **R4 — Idempotent completion.** A runtime MUST report completion and tolerate
  the deadline backstop firing as well (both may occur; the outcome is recorded
  once).

The **runtime SDK** (one per language, Go first) encodes R1–R4: it reads the
`RunSpec` uniformly, redeems a ticket if present, holds vended credentials
ephemerally, and reports completion. This is how the *runtime author* gets the
same "little boilerplate" the app developer does.

### Contract 3 — identity and credentials (the novel part)

This is the contribution the ecosystem genuinely lacks, and it is **not just
"SPIFFE for agents."** It is two seams that are easy to conflate:

- **Workload attestation** — *who is this runtime?* This is what SPIFFE/SPIRE
  already does well, and AEI should mint against it where available.
- **Delegated authorization + credential brokering** — *what is this run allowed
  to do, on whose behalf, and how does it obtain downstream credentials without
  holding standing secrets?* This is the missing piece.

AEI standardizes the second seam as a **token-exchange + broker profile** (RFC
8693 in shape):

```go
type IdentityRequest struct {
    Subject  string   // the acting principal (e.g., a user) — delegated authorization
    Scopes   []string // capability scopes the run may exercise
    Audience string   // the plane/audience the minted credential is bound to
}

type CredentialKind int
const ( Token CredentialKind = iota; Ticket )

type Credential struct {
    Value string         // opaque to the provider: a bearer token (push) or a redemption ticket (pull)
    Kind  CredentialKind
}
```

The platform mints a **per-run credential** whose authority is
`platform authority ∩ delegated consent ∩ provider-class policy`, short-TTL and
audience-bound. Delivery is the provider's choice, feature-detected:

- **Push** (default): the token rides the dispatch and is injected (env / message
  metadata).
- **Pull** (`PullsCredential()`): the dispatch carries a **single-use, run-bound
  ticket**; the runtime redeems it for the token before doing any work. This is
  the difference between *a live token at rest in a third party's store* and *no
  token at rest at all* — mandatory for persisting or snapshotting control planes,
  and it belongs in the standard, not as an afterthought.

Downstream provider credentials (a model key, a user's Git token) are **brokered
on demand** via `POST /vend`, never injected as standing secrets. The model gateway
pattern (an in-cluster proxy that holds the model key so the runtime never carries
it) composes here: `vend` and the gateway are two points on the same
"credentials-scoped-to-a-run" axis.

---

## Completion

One pattern lets a single orchestrator drive both a `Job` it watches to
completion **and** a durable agent that can only report out-of-band: **race the
runtime callback against the substrate watch (or the deadline).**

```go
if al, ok := launcher.(AsyncLauncher); ok {
    handle, _ := al.Dispatch(ctx, spec, cred)   // create/accept (bounded worker)
    completion := registerCompletion(spec.ID)   // the runtime's POST /complete lands here
    go func() {
        watch := make(chan Result, 1)
        go func() { r, _ := al.Await(ctx, handle); watch <- r }() // watch / deadline backstop
        select {
        case r := <-completion: finish(spec, handle, r) // fast path: runtime callback
        case r := <-watch:      finish(spec, handle, r) // backstop: watch or timeout
        }
    }()
}
```

Requirements this imposes on the standard:

- **Idempotent finish.** Both the callback and the watch may fire; the outcome is
  recorded exactly once.
- **The deadline is the universal backstop.** A provider that can be neither
  watched nor guaranteed to call back still terminates, because `RunSpec.Deadline`
  bounds `Await`.
- **"Done" is per-run, not per-workload.** For a durable runtime that keeps
  running, completion is a property of the *run*, not the termination of a
  workload — the callback carries the `runID`.

---

## The capability model: providers declare, apps require

CSI has access modes (`RWO`/`RWX`) because *the caller must be able to require a
semantic the backend may or may not offer.* Agent runtimes have the same need,
and it is a **multi-tenant safety** issue, not mere ergonomics: an app written
assuming a fresh, isolated runtime per run is **unsafe** on a durable multiplexed
actor unless its runtime is written stateless.

So capabilities flow both ways:

**Providers *declare* capabilities** (via the optional interfaces + class
metadata):

| Capability | Meaning |
| --- | --- |
| `ASYNC` | implements `Dispatch`/`Await` (completion decoupled from dispatch) |
| `PULL_CREDENTIAL` | delivers identity via redemption ticket |
| `ISOLATION:<class>` | can satisfy the named `IsolationClass` |
| `DURABLE` | dispatches to a standing runtime; no per-run workload |
| `MULTIPLEXED` | shares one runtime process across runs |
| `SNAPSHOT` | checkpoints/restores runtime state (implies R1/R2 obligations) |
| `STREAMING` | can emit partial results |

**Apps *require* semantics** (on `AgentRun`), and **admission matches** the
requirement against the resolved provider class, rejecting an incompatible bind:

| Requirement | Meaning |
| --- | --- |
| `runtimeLifecycle: perRun \| any` | forbid (or allow) a multiplexed/durable runtime |
| `isolation: required \| preferred \| none` | require an isolating provider |
| `statePersistence: none` | forbid providers that snapshot/persist run state |

This is the piece that keeps "swap by class" *safe* rather than merely possible.

---

## The declarative surface

Two CRDs, mirroring `PersistentVolumeClaim` + `StorageClass` (and
`Pod` + `RuntimeClass`). An app can equally program to the `aei.Launcher` Go
interface; a controller implements the CRD by calling providers through that same
interface.

The `AgentRun` CRD is the **declarative projection** — one object per run, for the
human/GitOps persona and low-to-moderate volume. It is *not* the high-volume app
path: an object per run is an etcd cost that does not scale to thousands of agents
(the Tekton/Argo `TaskRun` lesson). Apps dispatching at volume use the **data
plane** — a single per-app `AgentApp` plus a submit call, no per-run object — over
the same engine (see [dispatch-plane.md](dispatch-plane.md)). Both are adapters
over one `control.Plane`; the CRD is the declarative option, not a second engine.

### `AgentRun` — the app's intent (namespaced)

```yaml
apiVersion: agents.x-k8s.io/v1alpha1   # SIG-Apps is the natural home; see roadmap
kind: AgentRun
metadata: { name: review-pr-4127, namespace: team-a }
spec:
  providerClassName: standard          # a NAME, not a substrate; omit → cluster default
  taskRef: worklist://item/pr-4127     # opaque; AEI does not parse the payload
  parameters:                          # per-run values (ride the dispatch; see R1)
    kind: review
  isolationClass: gvisor               # optional; may default from the provider class
  identity:
    subject: user-123                  # delegated authorization
    scopes: [repo:read, metadata:write]
    audience: team-a-agent
  requirements:                        # app-declared semantics; admission enforces
    runtimeLifecycle: any
    isolation: preferred
  timeoutSeconds: 900
status:
  phase: Succeeded                     # Pending | Dispatched | Running | Succeeded | Failed
  handle: { provider: kagent, ref: task-abc }
  resultRef: worklist://item/pr-4127   # opaque; a pointer, not the payload
  timing: { provisioningSeconds: 0.6, runtimeSeconds: 64.6, modelSeconds: 60.7 }
  conditions: [ { type: Finished, status: "True", reason: CallbackReceived } ]
```

### `AgentProviderClass` — the admin's binding (cluster-scoped)

This is the indirection that makes an `AgentRun` portable across clusters — the
lesson CSI/`RuntimeClass` teach. The app references a class *name*; the admin owns
what it maps to.

```yaml
apiVersion: agents.x-k8s.io/v1alpha1
kind: AgentProviderClass
metadata:
  name: standard
  annotations: { agents.x-k8s.io/is-default-class: "true" }
spec:
  provider: kagent                     # the driver/plugin that backs this class
  parameters:                          # driver-specific knobs (endpoint, pool, image, …)
    endpoint: http://kagent.kagent-system.svc
  defaultIsolationClass: gvisor        # applied when the AgentRun omits one
  identityIssuer: spiffe://cluster/aei # where per-run credentials are minted
  queueName: agents                    # Kueue admission/quota (see composition)
  capabilities: [ ASYNC, PULL_CREDENTIAL, DURABLE ]   # advertised; conformance-verified
```

Swap `standard`'s `provider` from `kagent` to `ray` and every `AgentRun` in the
cluster moves substrates — with **no app manifest change.** That is the whole
value proposition, and it is why provider selection must be a class, not a field.

---

## Dispatch at scale — the data plane

An object per run does not scale to thousands of agents (etcd write amplification,
watch fan-out, the object-size cap). So the app-facing dispatch surface is a
**data plane**, not the CRD: one long-lived per-app `AgentApp` registration, and
each run is a **submit call** (`POST /dispatch` → a run id), not a Kubernetes
object. It is a second ingress over the *same* `control.Plane` the reconciler
uses, with the same mint / complete / provider path — so the runtime never
notices. Caller identity is the app's projected ServiceAccount token (TokenReview,
no shared secret), bounded by the `AgentApp` policy. The full wire, objects, and
identity model are in **[dispatch-plane.md](dispatch-plane.md)**; the `AgentRun`
CRD above remains the optional declarative adapter for humans, GitOps, and audit.

---

## Isolation

Isolation is a **named selector resolved by the provider**, mirroring
`RuntimeClass`/`StorageClass`. `isolationClass: gvisor` on an in-cluster provider
becomes a `runtimeClassName` in the pod spec; on `agent-sandbox` it selects the
`Sandbox`'s runtime; on an off-cluster provider it maps to a micro-VM tier. A
provider that cannot satisfy a required isolation class fails admission (the
capability model), so security posture is a **dispatch-time policy, not a
rewrite.**

---

## Conformance

"Swap by `providerClassName`" is only real if a provider's behavior is
*verifiable*. AEI ships an **`aei-sanity`** suite (the analog of `csi-sanity` /
`cri-tools`): a fixed battery of runs a candidate provider must drive to
identical terminal outcomes — dispatch, callback completion, watch-backstop
completion, ticket redemption, deadline enforcement, and the injection round-trip.

This suite is a direct extraction of the reference implementation's existing
non-regression check ("identical `ingest → enrich → rank` output across
providers"). Without conformance, fragmentation simply re-emerges *inside* the
standard.

---

## Observability

Because completion flows through one path, AEI standardizes a small run-timing
contract in `Result.Timing` / `AgentRun.status.timing`, so launcher overhead is
comparable *across* substrates:

- `provisioningSeconds` — dispatch-start → runtime actually working (a pod's
  cold-start **or** a durable actor's resume, measured the same way);
- `runtimeSeconds` — the runtime's own work;
- `modelSeconds` / `modelCalls` / `toolCalls` — the model share;
- optional `cost` (tokens).

The key requirement is that provisioning is **launcher-symmetric**: a pod
cold-start (asynchronous, after dispatch returns) and an actor resume
(synchronous, inside dispatch) must land in one comparable number, or launchers
cannot be compared. (The reference implementation measures this by stamping a
dispatch timestamp the runtime reports back.)

---

## How AEI composes with what exists

AEI sits **above** the converging standards and fills the gap between them; it
does not compete with them.

| Layer | Project(s) | Relationship to AEI |
| --- | --- | --- |
| **Wire** (agent↔agent, agent↔tool) | A2A, MCP | AEI's durable-dispatch transport *is* A2A; MCP is how the launched agent reaches tools. AEI standardizes the dispatch/lifecycle/identity these deliberately omit. |
| **Isolation primitive** | kubernetes-sigs/agent-sandbox | one provider (`isolationClass: sandbox`); AEI is the dispatch+completion+identity layer *above* the `Sandbox` CRD |
| **Egress / key management** | agentgateway, Envoy AI Gateway | the credential data plane; referenced by identity/broker policy so the runtime never holds the model key |
| **Workload identity** | SPIFFE/SPIRE | the attestation substrate `identityIssuer` mints against; AEI adds the delegated-authorization + brokering layer on top |
| **Admission / quota / scheduling** | Kueue | `AgentProviderClass.queueName`; `AgentRun`s become Kueue workloads. AEI defers scheduling — it does not reinvent it. Agent runs are bursty and expensive; this composition is first-order. |
| **Pool autoscaling** | Knative, KEDA | scale-to-zero for durable provider pools, orthogonal to dispatch |

---

## Non-goals

AEI deliberately does **not**:

- unify how you *author* an agent (ADK, LangGraph, a hand loop) — it is
  launch-and-lifecycle, not authoring;
- standardize the **payload** — the task input, tool-call schema, and result
  schema stay opaque (`taskRef` / `parameters` / `resultRef`); that is A2A/MCP and
  app-defined territory;
- implement **scheduling, quota, or gang admission** — that composes with Kueue;
- provide the **isolation runtime** itself — gVisor/Kata/`Sandbox` are the
  primitives AEI *selects*, not ones it builds.

Resisting a fat interface is the single most important discipline here. Every
precedent that succeeded (CSI, CRI) kept its mandatory core tiny; every ambition
to standardize the payload belongs one layer up, in A2A.
