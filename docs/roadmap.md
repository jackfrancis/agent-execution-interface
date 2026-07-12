# Roadmap

*How to get from this design to a usable standard, and the decisions still open.
The guiding principle is the one the reference implementation validated:
**interface first, providers accreted behind it** — prove the boundary is right
before standardizing a CRD and controller.*

> **Status (2026): past paper.** A reference implementation now lives in this repo
> — the `aei` core + registry, the `control` plane, the CRDs + controller + the
> **data-plane dispatch API**, the `k8s-job` / `agent-sandbox` / `agent-substrate`
> providers, Go and Python **app** SDKs, a Go **runtime** SDK, and a conformance
> harness. The phases below are the original plan; most of Phases 1–5 now have
> working code, and refinement continues.

---

## Phasing

### Phase 0 — RFC (this repository)

The design, the motivation, and the open decisions below, as a request for
comment. **Deliverable:** consensus on the three contracts and the tenets — the
goal was to be wrong on paper cheaply before writing code. **Done**; the phases
below have since accreted the reference implementation (see the status note above).

### Phase 1 — the Go module + two reference providers

A tiny module (`Launcher` + optional `AsyncLauncher` / `PullCredentialLauncher` +
the registry) with **two** reference providers that bracket the design:
`k8s-job` (in-cluster, watch, push-token) and `agent-sandbox` (in-cluster,
isolated, callback, push-token). This is immediately useful to the people feeling
the lock-in most — application developers who embed dispatch — and it proves the
capability-negotiation core without a CRD.

**Deliverable:** `import "…/aei"`, dispatch a run, swap the two providers by
config. **Exit criterion:** the two providers pass an early `aei-sanity`.

### Phase 2 — the runtime SDK

The runtime-author half of "little boilerplate": a Go library that reads a
`RunSpec` uniformly (env *and* message metadata), redeems a pull-ticket, holds
vended credentials ephemerally, and reports completion — i.e., encodes
authoring-rules **R1–R4**. Without this, every runtime author re-derives the
snapshot/durability rules by hitting the bugs.

**Deliverable:** a runtime binary built on the SDK runs unmodified on both
Phase-1 providers.

### Phase 3 — conformance (`aei-sanity`)

Extract the reference implementation's cross-provider equivalence test into a
portable suite a candidate provider runs to claim compliance: dispatch, callback
completion, watch-backstop completion, ticket redemption, deadline enforcement,
injection round-trip. This is what makes "provider" a verifiable claim.

### Phase 4 — the declarative surface + the dispatch plane

`AgentRun` + `AgentProviderClass` CRDs and a reference controller that implements
them by calling providers through the Phase-1 interface — the GitOps-native,
admin-owned, portable API. At scale the app-facing surface is the **data-plane
dispatch API** (a per-app `AgentApp` + a submit call, no per-run object; see
[dispatch-plane.md](dispatch-plane.md)); the CRD is the declarative adapter over
the same engine. **Decision A** and **Decision E** are resolved here.

### Phase 5 — the provider ecosystem

Reference providers for the CNCF-adjacent durable/remote shapes — `kagent`
(durable, pull-token, A2A), `ray`/KubeRay (watch/poll), `dapr` (workflow
callback) — so "swap by class" is real on day one and the durable + pull-token +
snapshot paths are exercised by more than the reference app.

### Phase 6 — identity/broker hardening

Promote the identity/credential-broker profile (Contract 3) from
reference-implementation shape to a SPIFFE/SPIRE-backed production profile:
attestation via SVID, delegated authorization + `vend` brokering on top,
push/pull delivery. This is the highest-value, least-standardized piece — worth
its own focused design pass and probably its own sub-spec.

---

## Open decisions

These are the forks the design leaves deliberately unresolved; each wants an RFC
issue.

### Decision A — how a provider plugs in — RESOLVED

**Resolved in [provider-extension.md](provider-extension.md).** A layered hybrid:
the in-tree Go `Factory` registry stays the default and reference; non-Go
substrates plug in **today, with no new machinery**, via a delegating Go adapter
over the substrate's own wire (A2A/REST/CR — as the reference `kagent` and
`opensandbox` providers already do) *or* controller-per-class (Gateway API style,
via a proposed `AgentProviderClass.spec.controllerName`). An out-of-process
**gRPC plugin** (true CSI parity) is deferred until a second, external author
needs it, and lands in the same registry as another `Factory` kind — so deferring
it forecloses nothing. Net: the provider persona is language-agnostic without a
gRPC prerequisite.

### Decision B — where it lives

- **kubernetes-sigs / SIG-Apps**, alongside agent-sandbox — the only vendor-neutral
  body already shipping an agent-workload CRD, and adjacent to the isolation
  primitive AEI builds on. **Leading candidate.**
- **CNCF sandbox** — closer to kagent/KubeRay, further from the CRD lineage.
- **Standalone** — fastest to move, weakest neutrality signal.

Anchoring in SIG-Apps also lets AEI extend agent-sandbox's own "decouple API from
runtime" work *upward* from isolation into dispatch, rather than duplicating it.

### Decision C — the payload-opacity boundary

The design keeps `taskRef` / `parameters` / `resultRef` opaque and leaves prompt,
tool, and result schemas to A2A/MCP. The open question is exactly where the line
sits: are `tools` (MCP/A2A refs) part of `RunSpec`, or entirely out? The tension
to resolve: enough structure to be useful, not so much that AEI drifts into agent
authoring. Bias toward *less*.

### Decision D — naming

`AgentRun` vs `AgentJob` vs `AgentTask`; `AgentProviderClass` vs
`AgentRuntimeClass`; `Launcher` vs `Executor`. The repository name commits to
**"Agent Execution Interface"** as the umbrella; the resource and interface names
are still up for community bikeshedding and should be settled before Phase 4
freezes the CRD group.

### Decision E — dispatch at scale — RESOLVED

**Resolved in [dispatch-plane.md](dispatch-plane.md).** An `AgentRun` object per
run is the declarative projection, not the high-volume app path — one etcd object
per invocation does not scale to thousands of agents (the Tekton/Argo `TaskRun`
lesson). The app-facing surface is a **data plane** over the same `control.Plane`:
a single per-app `AgentApp` registration plus a non-blocking submit call
(`POST /dispatch` → a run id; `GET /runs/{id}` for lifecycle), with **no per-run
object**. Caller identity is the app's projected ServiceAccount token
(TokenReview, no shared secret) bounded by `AgentApp` policy; results stay off-API
in the app's store. The `AgentRun` CRD remains the optional declarative adapter
(GitOps, audit, low volume). The runtime ABI is unchanged — the runtime cannot
tell which ingress dispatched it.

---

## Non-goals

Restated from [design.md](design.md#non-goals) because scope discipline is the
main way standards like this fail:

- not an agent-authoring framework;
- not a payload/result schema (that is A2A/MCP);
- not a scheduler or quota system (that is Kueue);
- not an isolation runtime (that is gVisor/Kata/`Sandbox`).

The smallest interface that answers *submit · observe completion · inject a
scoped identity · select isolation* is the whole ambition. Every precedent that
succeeded kept its mandatory core tiny; every one that sprawled did not ship.

---

## How to help

The most useful contributions at Phase 0 are **adversarial**: a real use case a
current tenet would fail, a substrate whose shape the capability model can't
express, or a security hole in the identity/broker profile. Open an issue. The
identity/credential-broker profile (Contract 3) and the `AgentRun` /
`AgentProviderClass` split are where feedback changes the design the most.
