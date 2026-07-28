# AEI dispatch plane (v1alpha1)

*A sub-spec of [design.md](design.md). It concretizes how an app dispatches at
scale — thousands of runs — without one Kubernetes object per run. It sits beside
[runtime-abi.md](runtime-abi.md): that spec is the runtime→platform wire, this one
is the app→platform wire.*

**Status:** draft for comment. The shape is what we are trying to hold fixed.

---

## The problem

The `AgentRun` CRD ([design.md](design.md), the declarative surface) is one
object per run, stored in etcd. That is the right surface for a *human* declaring
a run (GitOps, `kubectl`, audit) and for low/moderate volume. It does **not**
scale to thousands of runs per app: etcd is a consistent config store, not a
job queue, and an object-per-invocation means write amplification, watch fan-out
to every controller, the 1.5 MB object cap, and a permanent TTL/archival tax.
This is the lesson Tekton/Argo learned with a `TaskRun`/`Workflow` per run.

## The model: one engine, two ingresses

There is exactly **one** dispatch engine — `control.Plane` ([../control](../control)).
The CRD is not a second engine; the reconciler is a *watch adapter* that turns an
`AgentRun` into a `Plane` call. The **data plane** is a second, request-driven
adapter over the same engine, and it is the app surface at scale:

| Ingress | Persona | Per-run object | Use |
| --- | --- | --- | --- |
| `AgentRun` CRD (watch adapter) | human / GitOps | one etcd object | declarative, low volume, audit |
| **dispatch plane** (this spec) | app SDK | **none** | programmatic, high volume |

The app SDK targets the data plane. The CRD stays as an optional adapter; both
reach the same providers, so nothing about the runtime, the credential mint, or
completion differs between them (the portability invariant holds one level up).

## Objects: one per app, not one per run

The only durable object is a per-app registration — **`AgentApp`** (namespaced,
[../controller/api/v1alpha1/agentapp_types.go](../controller/api/v1alpha1/agentapp_types.go)):

```yaml
apiVersion: agents.x-k8s.io/v1alpha1
kind: AgentApp
metadata: { name: pyfer, namespace: team-a }
spec:
  providerClassName: standard          # which class runs dispatch to; empty → default
  dispatchers: [pyfer]                  # ServiceAccounts allowed to dispatch as this app
  identity:
    audience: pyfer-agent               # the app fixes the minted-credential audience
    maxScopes: [signals:read, metadata:write]   # the ceiling; a run may narrow, never widen
```

One `AgentApp` fronts every run the app dispatches. Runs themselves are **not**
objects — they exist only as in-memory state on the control plane, addressable by
the id `/dispatch` returns.

## The wire

Two routes, alongside the runtime ABI, under the same `/aei/v1alpha1` base.

### `POST /aei/v1alpha1/dispatch` — submit a run

```http
POST /aei/v1alpha1/dispatch
Authorization: Bearer <caller's projected ServiceAccount token>
Content-Type: application/json

{
  "app": "pyfer",
  "taskRef": "item://org/repo#7",
  "parameters": { "kind": "converse", "provider": "github" },
  "identity": { "subject": "github:1", "scopes": ["signals:read"] },
  "timeoutSeconds": 900
}
```

```jsonc
// 202 Accepted
{ "runId": "b3f1c9e0…" }     // the correlation key; there is no object to GET in k8s
```

The body is the `AgentRunSpec` fields (camelCase — the same CRD schema the app SDK
already models) plus `app`. `providerClassName` is ignored: the `AgentApp` fixes
the class. Dispatch is **non-blocking** — the run outlives the request; its
outcome lands off-API (below).

### `GET /aei/v1alpha1/runs/{id}` — read lifecycle

```http
GET /aei/v1alpha1/runs/b3f1c9e0…
Authorization: Bearer <caller's ServiceAccount token>
```

```jsonc
// 200 OK — read from the plane's in-memory run store, not etcd
{ "phase": "Succeeded", "message": "", "resultRef": "worklist://item/7",
  "timing": { "runtimeSeconds": 64.6, "modelSeconds": 60.7, "modelCalls": 12 } }
```

`phase` is `Running | Succeeded | Failed`. This is **lifecycle only** — the run's
*result* is written by the runtime to the app's own store (`resultRef` is an
opaque pointer AEI never parses), exactly as on the CRD path. An app that only
cares that work landed in its store can ignore this route entirely.

## Identity: no shared secret

The caller authenticates with its **projected ServiceAccount token**; the endpoint
validates it via the Kubernetes **TokenReview** API and extracts the SA identity
(`system:serviceaccount:<ns>:<name>`). There is no shared bearer. Authorization is
then the `AgentApp` policy:

1. the caller SA MUST be in `spec.dispatchers`;
2. the requested `scopes` MUST be a subset of `spec.identity.maxScopes` (least
   privilege — a run may narrow the grant, never widen it);
3. the minted run credential's `audience` is fixed by `spec.identity.audience`.

The minted per-run credential is otherwise identical to the CRD path
(`platform authority ∩ delegated consent ∩ class policy`, short-TTL,
push or pull per the provider). Reading a run's status is gated the same way: only
a dispatcher of the owning app may read its runs.

## Completion

`Plane.Submit` dispatches and returns immediately; a background goroutine races
the runtime's `/complete` callback against the provider's `Await` and the deadline
(the same race as `Plane.Run`), and records the first terminal signal into the
in-memory run store. `GET /runs/{id}` reads that store. Idempotent finish (R4)
holds: callback and watch may both fire; the first wins.

## Tradeoffs (stated, not hidden)

- **No per-run `kubectl get` / GitOps / etcd audit.** Regained, if needed, via the
  app's own store + metrics/tracing, or by keeping the CRD adapter for the runs
  that warrant it.
- **The run store is in-memory** (reference-grade, `replicas: 1`, like the rest of
  the control plane): a control-plane restart forgets in-flight run *status*
  (results already persisted by the runtime survive). Durability + HA of the run
  store, and an eviction policy for terminal entries, land with the shared store
  (design.md, composition) — the seam does not change.

## What does *not* change

The runtime ABI ([runtime-abi.md](runtime-abi.md)) is untouched — a runtime cannot
tell whether it was dispatched from a CRD or the data plane. The provider
interface, the credential mint, and completion are identical. This is a new
*ingress*, not a new engine.
