# Motivation: the fragmentation problem

*This document states the problem AEI exists to solve, surveys the 2025–2026
landscape, and shows that Kubernetes has solved this exact shape of problem
before. The proposed solution is in [design.md](design.md).*

---

## 1. The developer's problem, stated precisely

You have an application. Somewhere in it you want to run an agent: *given this
task and these tools, execute an LLM-driven agent and hand me a result.* On
Kubernetes today you must independently answer four orthogonal questions, and
every answer locks you deeper into one stack.

### (a) Where and how does the workload run?

Each option is a different API, a different client library, a different failure
model:

| You could… | …but that means |
| --- | --- |
| submit a Kubernetes **`Job`** | one-shot; no agent semantics; you build pre-warming, retries, and result plumbing yourself |
| create a **`Pod`** and read logs | no lifecycle, no result contract; fragile |
| create a **`kagent.dev` `Agent`** + call its A2A endpoint | requires kagent installed; A2A-specific |
| create an **`agents.x-k8s.io` `Sandbox`** + dial its FQDN | agent-sandbox CRD; isolation-focused, no "dispatch-and-get-result" |
| use **Dapr Agents** `DurableAgent` + Dapr Workflow | requires the Dapr sidecar/runtime everywhere |
| submit a **`RayJob`** + poll the Ray dashboard | requires KubeRay; Ray-specific |
| `POST /runs` to **LangGraph Platform** | requires the licensed server |
| spawn an **E2B/Modal** sandbox via SDK | SaaS; no Kubernetes involvement at all |

Switching from any one of these to any other is a **rewrite of the dispatch
layer**, not a configuration change.

### (b) How is a credential/identity injected?

kagent references a Secret from a `ModelConfig`; LangGraph reads a key from a
Helm-valued `secretRef`; Dapr uses its `secretstore` component; Ray mounts
Secrets into worker specs; E2B/Modal take a client-side env var. There is **no
common contract** for "hand this agent run a short-lived, scoped token for
exactly the model and data it needs." SPIFFE/SPIRE solves east-west workload
identity for mesh mTLS, but there is **no SPIFFE-equivalent for AI-provider
credentials scoped to a single agent run**.

### (c) How is completion observed?

Watch a `Job` condition; poll `GET /runs/{id}`; subscribe to an SSE stream; query
a Dapr Workflow instance; watch a `Sandbox` `Finished` condition; poll the Ray
dashboard. **Five-plus completion models, no common "await result."**

### (d) What isolation is selected?

Plain container (namespace only); gVisor via `RuntimeClass: gvisor`; Kata via
`RuntimeClass: kata-containers`; a Firecracker micro-VM through E2B/Modal (which
exits the Kubernetes API surface); or an agent-sandbox `Sandbox`. **No framework
lets you select isolation as a dimension of the dispatch call.**

---

## 2. The landscape, surveyed (2025–2026)

A compressed map of what an application developer must choose among. Each cell is
a distinct API and operational model.

**Agent runtimes / frameworks targeting Kubernetes:** kagent (K8s-native, agents
as CRDs over A2A; CNCF Sandbox), Google ADK + Vertex AI Agent Engine, Dapr Agents
(durable-actor on the Dapr runtime), LangGraph Platform (graph orchestration + a
licensed server), Ray Serve / KubeRay (`RayCluster`/`RayJob`; CNCF Sandbox),
Kubeflow / KServe, Llama Stack, and the in-process libraries (CrewAI, AutoGen/AG2,
Semantic Kernel) that ship no operator, CRD, or completion contract at all.

**Sandboxing / secure code execution:** kubernetes-sigs/agent-sandbox (`Sandbox`
CRD, any `RuntimeClass`; the most K8s-native), Alibaba OpenSandbox (a control-plane
server with a K8s runtime), and the SaaS micro-VM products E2B (Firecracker),
Modal (gVisor), Daytona, microsandbox, Northflank, Cloudflare. Underlying
isolation: **gVisor** and **Kata** are Kubernetes-native via `RuntimeClass`;
**Firecracker** and **V8 isolates** require leaving the Kubernetes API.

**Model / LLM gateways** (egress, key management, routing): agentgateway
(Solo.io → Linux Foundation), Envoy AI Gateway (CNCF), kgateway, LiteLLM,
Portkey, Kong AI Gateway, plus vLLM + KServe for serving.

**Interop protocols — the part that *is* converging:**

| Protocol | Standardizes | Status |
| --- | --- | --- |
| **MCP** | agent ↔ tool (data/context) | de facto standard; thousands of servers |
| **A2A** | agent ↔ agent (task delegation) | Linux Foundation; rapidly converging |
| **AGNTCY / SLIM** | discovery, identity, secure messaging | early infra |
| **ACP** | agent ↔ agent (REST) | merged into A2A (2025) |

---

## 3. What is standardized vs. what is not

The crucial observation: **the wire is converging; the dispatch model is not.**

- **How agents *communicate*** is standardizing fast — MCP for tools, A2A for
  agent-to-agent — both under the Linux Foundation, both JSON-RPC/HTTP.
- **How you *launch* an agent workload on Kubernetes** — the resource model, the
  credential injection, the completion signal, the isolation selector — has **no
  standard at all.** MCP and A2A are protocols *between running agents*; they say
  nothing about how an application asks a cluster to *start* one and *tell it
  when it's done*.

That is the gap AEI is about.

---

## 4. Kubernetes has solved this shape before

The ecosystem keeps re-implementing point solutions where Kubernetes history says
the answer is a **thin interface + swappable providers**:

| Interface | Abstracts | Mechanism |
| --- | --- | --- |
| **CNI** | pod networking | kubelet invokes a plugin with JSON (`ADD`/`DEL`/`CHECK`) |
| **CSI** | storage | `PVC` → `StorageClass` → driver gRPC (`CreateVolume`, `NodePublishVolume`) |
| **CRI** | container execution | kubelet gRPC (`RunPodSandbox`, `CreateContainer`); `RuntimeClass` selects the impl |
| **Open Service Broker** | service provisioning | `ServiceInstance`/`ServiceBinding` → broker REST (`provision`/`bind`) |
| **`database/sql`** | SQL access | `db.Query(...)` independent of `pgx`/`sqlite`; the driver implements one interface |

Every one of these lets you **swap the implementation without rewriting the
caller.** Agent dispatch has no such row. You cannot yet write

```go
run := agents.Dispatch(ctx, RunSpec{Task: t, Isolation: "gvisor"})
result := agents.Await(ctx, run)   // works on kagent, or Ray, or a Sandbox CR…
```

…and swap the backend by configuration. That missing row is what
[design.md](design.md) specifies.

The two most important lessons those precedents carry into AEI:

1. **A thin required core beats a fat interface.** CSI's mandatory surface is
   small; almost everything is a negotiated *capability*. A provider that can
   only do the simplest thing implements only the simplest thing.
2. **Selection is a named class the *admin* owns, not a field the app hardcodes.**
   A `PersistentVolumeClaim` says `storageClassName: standard`; the admin binds
   `standard` to a provisioner. That indirection is exactly what makes the same
   manifest portable across clusters — and AEI applies it to *provider* selection
   (`AgentProviderClass`), not just isolation.

---

## 5. Existence proof: one app, eight substrates, one env var

AEI is not a greenfield guess. Its shape is distilled from a working reference
implementation, [Zumble-Zay](https://github.com/jackfrancis/zumble-zay), a small
security-focused Go backend that dispatches its agent jobs through a two-part
seam — a `Launcher` interface (dispatch + optional capabilities), selected by a
`database/sql`-style driver registry, and a substrate-neutral **runtime ABI**
every runtime speaks back over plain HTTP.

Behind that seam it runs **eight** providers spanning **four** operational
shapes, and the identical pipeline runs on any of them by setting `LAUNCHER=`:

| Provider | Shape | Starts the runtime | Completion | Identity | App RBAC |
| --- | --- | --- | --- | --- | --- |
| `inprocess` | in-process | goroutine | return | direct | none |
| `k8s-job` | in-cluster one-shot | `batch/v1` Job | watch | push token | create Jobs |
| `k8s-pod` | in-cluster one-shot | bare Pod | watch | push token | create Pods |
| `k8s-pod-detached` | in-cluster one-shot | bare Pod | **callback** | push token | create Pods |
| `agent-sandbox` | in-cluster **isolated** | `Sandbox` CR (gVisor/Kata) | **callback** | push token | create sandboxes |
| `opensandbox` | **remote** one-shot | HTTP create + `exec` | **callback** | push token | none (API key) |
| `kagent` | **remote durable** | A2A `message/send` to a standing agent | **callback** | **pull ticket** | none (A2A) |
| `substrate` | **remote durable + multiplexed + snapshotted** | A2A to a warm, gVisor-checkpointed actor | **callback** | **pull ticket** | none |

Three of these are *shape boundaries the interface had to absorb*, and they are
why AEI's core is structured the way it is:

- **In-cluster vs. remote control plane.** The `k8s-*` and `agent-sandbox`
  providers create Kubernetes objects (and need RBAC); `opensandbox`, `kagent`,
  and `substrate` are *clients of a control plane* and create nothing in
  Kubernetes.
- **One-shot vs. durable.** Every provider except the last two creates a fresh
  workload per run. `kagent` and `substrate` dispatch to a **standing** runtime;
  there is no per-run workload at all. The interface absorbs this because dispatch
  says nothing about *whether* a workload is created — only that a run was
  accepted and will report completion.
- **Push-token vs. pull-token identity.** Most providers receive the token at
  dispatch. `kagent` and `substrate` receive a **single-use ticket** the runtime
  redeems for the real token — because their control planes persist task history
  (`kagent`) or snapshot runtime memory to object storage (`substrate`), and a
  live bearer token must never sit at rest there.

The application code that produced all eight runs is *identical*. The difference
is one environment variable and a provider package. That is the entire point —
and the argument that the missing standard is small enough to build.

> **Honest caveat.** Zumble-Zay is a single-tenant, dev-focused proof of a
> *shape*, not a drop-in standard: `replicas: 1`, in-memory job/ticket stores,
> and an app-specific payload. A community standard must generalize the payload
> (opaque task + A2A result) and harden the stores. Its value here is as an
> existence proof, not a blueprint to copy verbatim. See
> [design.md](design.md) for the generalized design and
> [roadmap.md](roadmap.md#non-goals) for what AEI deliberately leaves out.
