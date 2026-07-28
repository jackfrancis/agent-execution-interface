# AEI provider extension model (v1alpha1)

*A sub-spec of [design.md](design.md), resolving **Decision A** in
[roadmap.md](roadmap.md). Where [runtime-abi.md](runtime-abi.md) makes the
**runtime** persona language-agnostic (a wire, not a Go SDK), this makes the
**provider** persona language-agnostic too — so no persona is locked to Go.*

**Status:** draft for comment; resolves the roadmap's Decision A. The recommended
model is implemented in part (the in-tree registry) and in part proposed (the
`controllerName` hand-off and the deferred gRPC plugin), noted inline.

---

## 1. The question

A substrate author wants to make their runtime (agent-sandbox, kagent, Ray, a
remote sandbox server, an in-house thing) selectable as an AEI `providerClass`.
Two sub-questions, and the second is the sharp one:

1. **What interface do they implement** to place a run and observe its completion?
2. **In what language**, and **in what process** — must it be Go, compiled into
   the AEI controller? CSI drivers can be *any* language because CSI is gRPC; is
   AEI's provider boundary similarly open, or Go-only?

This is orthogonal to the runtime ABI. The *runtime* (the agent code) is already
any-language over the wire ([runtime-abi.md](runtime-abi.md), proven by the
`conformance` shell+curl runtime). Decision A is only about the *dispatch* side.

## 2. Where we are today

The dispatch boundary is the Go interface + registry in `aei`:

- `aei.Launcher` (required `Launch`) + optional `AsyncLauncher` / `PullCredentialLauncher`;
- `aei.Register(name, Factory)` from a provider's `init()`, `aei.Build(name, cfg)` to construct it — the `database/sql` pattern;
- the reference controller resolves an `AgentProviderClass` and calls
  `aei.Build(class.Spec.Provider, aei.Config{Params: class.Spec.Parameters})`, then
  drives the run through the shared `control.Plane`. **The CRD is a thin
  projection over the same registry, not a second engine.**

So today a provider is a **Go package**, blank-imported into the controller (or an
embedding app). That is the smallest possible thing and the right *default* — but
on its own it would make the provider persona Go-only, the one axis where AEI
would be less open than CSI. It is not, for the two reasons below.

## 3. The three extension models

| Model | Mechanism | Language of the *provider* | New machinery | Best for |
| --- | --- | --- | --- | --- |
| **In-tree Go Factory** | implement `aei.Launcher`, `Register` from `init()` | Go | none | the reference providers; Go substrate authors |
| **Delegating Go adapter** | a Go Factory whose `Launch` *creates a substrate CR / calls the substrate's HTTP API / sends A2A* | Go adapter over an **any-language substrate** | none | substrates that already expose a wire (kagent A2A, a REST sandbox server, a CR) |
| **Controller-per-class** | the substrate ships **its own controller** watching `AgentRun` by `providerClassName` | any | none (the CRD *is* the wire) | substrates that already ship a controller (kagent, Ray/KubeRay, agent-sandbox) |
| **Out-of-process gRPC plugin** | provider is a separate process speaking a gRPC `Launcher` service; the controller is the client | any | a proto + socket + versioning | a *single core controller* hosting *non-Go* plugins, when neither of the above fits |

## 4. The decision — a layered hybrid

Adopt all three of the *no-new-machinery* models now, in layers, and defer gRPC
until a concrete out-of-tree consumer needs it.

### 4.1 The in-tree Go Factory stays the default and the reference

It is the smallest surface and what the reference providers use. Nothing below
replaces it; the others are additive.

### 4.2 "Written in Go" ≠ "the substrate is Go" — the delegating adapter

The crucial observation: a Go `Launcher` says nothing about *how* it dispatches.
Its `Launch`/`Dispatch` may **create a Kubernetes CR, call a remote HTTP API, or
send an A2A `message/send`** — so a ~40-line Go adapter can front an
**any-language** substrate over that substrate's *existing* wire. The reference
implementation already proves this: its `kagent` provider is a Go adapter that
JSON-RPCs a Go-agnostic A2A service, and its `opensandbox` provider is a Go
adapter over a REST control plane. **Most non-Go substrates need no AEI-specific
protocol at all — just a thin adapter over the wire they already speak.** This is
the same shape as a CSI driver that shells out to a vendor CLI.

### 4.3 Controller-per-class is first-class — the Gateway API hand-off

A substrate that already ships a Kubernetes controller should not write a Go
adapter at all. It should **watch `AgentRun` filtered by its `providerClassName`
and reconcile it directly**, exactly as an `IngressClass`/`GatewayClass`
controller does. This needs one addition, borrowed verbatim from Gateway API:

> **Proposed:** `AgentProviderClass.spec.controllerName` (optional). The AEI
> reference controller reconciles only classes whose `controllerName` is empty or
> equal to its own identity; a class naming a **foreign** controller is ignored by
> the core and owned by the substrate's controller. The `status`/completion
> contract (phases, the `Finished` condition, `resultRef`, `timing`) is the shared
> obligation both honor — that is what keeps `AgentRun` uniform across owners.

With this, a substrate with an existing controller integrates with **zero** Go
AEI code: it claims a class and fills the same `status`. The reference control
plane's `/complete` · `/redeem` · `/vend` routes remain available for such a
controller to reuse, or it may satisfy the contracts itself.

### 4.4 The gRPC plugin is deferred, and drops into the same registry

A single-core-controller-with-non-Go-plugins model (true CSI parity) is real but
premature: it costs a proto, a socket lifecycle, and a versioning story, and
§4.2/§4.3 already give non-Go substrates a path. **Build it only when a second,
external author needs it** and cannot use a delegating adapter or a
controller-per-class. When built, it is **just another `Factory` kind** — a
gRPC-client `Launcher` registered under a name — so it lands in the existing
registry with **no core change**. The service mirrors the Go interface:

```proto
service Launcher {                     // v1alpha1, illustrative
  rpc GetInfo(GetInfoRequest) returns (Info);              // name, version, CAPABILITIES
  rpc Dispatch(DispatchRequest) returns (Handle);          // create/accept a run
  rpc Await(AwaitRequest) returns (stream RunStatus);      // optional: ASYNC capability
  rpc PullsCredential(google.protobuf.Empty) returns (Bool); // optional capability bit
}
```

The `RunSpec`, `Handle`, `Credential`, and `Timing` messages are the protobuf
mirror of the Go types in `aei/aei.go`; the capability interfaces become
capability bits in `Info`. Because the runtime ABI is already a wire, a gRPC
provider needs nothing new on the runtime side.

## 5. Language-agnosticism scorecard

| Persona | Contract | Any language? | How |
| --- | --- | --- | --- |
| **Runtime author** (the agent code) | the runtime ABI wire ([runtime-abi.md](runtime-abi.md)) | **yes, today** | read `AEI_*`, speak `/redeem`·`/complete`·`/vend`; proven by the `conformance` shell+curl runtime |
| **App developer** (dispatches a run) | `AgentRun` CRD, or the Go `aei`/`control` API | **yes, today** | the CRD is language-agnostic by construction; Go is one embedding |
| **Provider author** (plugs in a substrate) | §4.2 delegating adapter · §4.3 controller-per-class · §4.4 gRPC (later) | **yes** | a thin Go adapter over the substrate's own any-language wire, **or** a controller-per-class in any language, with gRPC as a deferred third path |

No persona is Go-locked. The provider persona reaches other languages by fronting
the substrate's *existing* wire (adapter) or by owning the CRD directly
(controller-per-class) — the same two escape hatches the Kubernetes storage and
gateway ecosystems use — with an in-process gRPC plugin held in reserve.

## 6. When to use which (author's guide)

- **You have a Go substrate, or a simple API/CR to call** → in-tree Go Factory (§4.1), possibly a delegating adapter (§4.2). Smallest.
- **Your substrate already speaks A2A / REST / a CR, in any language** → a delegating Go adapter (§4.2). No new protocol.
- **You already ship a Kubernetes controller** → controller-per-class (§4.3). Claim a `providerClassName`, honor the `status` contract, write no Go AEI code.
- **You need one core controller to host a non-Go plugin, and can't do the above** → wait for, or drive, the gRPC plugin (§4.4).

## 7. Resolved decisions

- **Provider selection stays a name on an admin-owned class** (`AgentProviderClass.spec.provider`), resolved by the core controller via `aei.Build`, mirroring `StorageClass`→provisioner. Unchanged.
- **Non-Go providers need no gRPC to start.** The delegating adapter and controller-per-class paths make the provider persona language-agnostic with zero new machinery; gRPC is an optimization for a specific (single-core-controller + non-Go plugin) case, not a prerequisite.
- **gRPC, when built, is additive.** It is a `Factory` kind in the existing registry, not a core rewrite — so deferring it forecloses nothing.

## 8. Open questions

- **`controllerName` semantics.** Adopt Gateway API's exact matching rules (a
  class is owned by exactly one controller; unowned classes are inert)? And does
  the core controller *validate* an unknown `controllerName`, or silently ignore
  it (the Gateway API choice, which tolerates a not-yet-installed controller)?
  Deferred to the Phase-4 CRD work.
- **Capability trust.** A controller-per-class or gRPC provider *advertises*
  `capabilities` on its class; conformance (`aei-sanity`) must *verify* them, or an
  app's `requirements` match is only as honest as the provider. The wire/harness
  work (`conformance`) is the seed; extending it to drive a *provider* (not just a
  runtime) to prove its advertised capabilities is the follow-up.
- **Completion ownership in controller-per-class.** Does a foreign controller
  reuse the AEI control plane's `/complete` endpoint, or record completion itself?
  Both satisfy the `status` contract; the reference reuses the plane. Pin the
  recommendation with the first real controller-per-class provider.
