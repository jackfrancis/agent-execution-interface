# Agent Execution Interface (AEI)

> A vendor-neutral, Kubernetes-native standard for **dispatching** an agent
> workload, handing it a **scoped, short-lived identity**, and getting its
> **result** back — independent of the substrate that runs it (a `Job`, a gVisor
> `Sandbox`, a remote sandbox control plane, a durable A2A agent, a snapshotted
> micro-VM). **The "CRI/CSI for agent dispatch."**

The 2025–2026 ecosystem standardized how agents *talk* (MCP for tools, A2A for
agent-to-agent) but not how you *launch* one on Kubernetes: where the workload
runs, how its credential is injected, how completion is observed, and what
isolation is selected are four independent choices that each marry an app to a
stack it can't leave — so switching substrates becomes a rewrite of the dispatch
layer. Kubernetes has solved this shape before — CNI, CSI, CRI, `database/sql` —
with a thin, stable interface and swappable providers behind it; AEI is that
interface for agent dispatch. The full problem is in
[docs/motivation.md](docs/motivation.md).

AEI follows the storage/compute extensibility playbook. An **app developer**
dispatches a run through a small API — a per-app `AgentApp` plus a submit call
([docs/dispatch-plane.md](docs/dispatch-plane.md)), or the declarative `AgentRun`
CRD for GitOps — and never imports a substrate SDK. An **admin** binds a named
`AgentProviderClass` to a provider, so moving every app to a different substrate
is one field. A **provider author** plugs a substrate in behind one Go interface
and a `Register()` ([docs/provider-extension.md](docs/provider-extension.md)); a
**runtime author** writes agent code against a small runtime SDK over a stable
wire ([docs/runtime-abi.md](docs/runtime-abi.md)). Three contracts make it
reusable — dispatch, the runtime ABI, and a per-run identity/credential broker —
all detailed in [docs/design.md](docs/design.md).

The seam is small and already demonstrated: the reference app
[Zumble-Zay](https://github.com/jackfrancis/zumble-zay) runs one pipeline
unchanged across eight substrates — a `Job`, a bare `Pod`, a gVisor `Sandbox`, a
remote OpenSandbox plane, a durable kagent A2A agent, and a snapshotted Agent
Substrate actor — by changing one environment variable. So this repo holds the
design **and** a reference implementation: the `aei` core + registry, the
`control` plane, the CRDs + controller + data-plane dispatch, the `k8s-job` /
`agent-sandbox` / `agent-substrate` providers, Go and Python **app** SDKs, and a
Go **runtime** SDK. **[docs/hello-world.md](docs/hello-world.md) is what
this looks like in practice** — "Hello, World!" as an agentic run in both Go and
Python, operationalized over agent-sandbox and Agent Substrate.

## Install

`make help` lists every target. There are two ways in.

**kind (the dev loop).** `make dev-up` creates or reuses a kind cluster, builds and
loads the controller image, installs the CRDs and controller, and provisions the
signing authority. Add `PROVIDERS=agent-sandbox` (and `make install-agent-sandbox`,
`make install-gvisor`) for the isolated class.

**Any other cluster (AKS, GKE, …).** AEI does not create or manage the cluster; give
it a kubeconfig file. This is what a CI job would run:

```sh
# 1. Build for the nodes' platform and push to a registry the cluster can pull from.
make push IMG=<registry>/aei-manager:<tag> PLATFORM=linux/amd64

# 2. Install the CRDs + controller, provision the signing authority, wire a class.
make deploy KUBECONFIG=/path/to/kubeconfig IMG=<registry>/aei-manager:<tag> PROVIDERS=k8s-job

# 3. The public key an app verifies run credentials with.
make public-key KUBECONFIG=/path/to/kubeconfig
```

- `KUBECONFIG` must be passed on the make command line. One that is only exported
  in your shell is ignored, so the dev loop keeps targeting kind. The file's
  current context is used unless `KUBE_CONTEXT=<name>` picks another.
- Use a new image tag per build: the controller pod re-pulls only when the image
  reference changes.
- The same flag works on the other cluster-agnostic targets (`status`, `logs`,
  `smoke`, `provider`, `install-agent-sandbox`, `agent-sandbox-enable`,
  `register-app`, `undeploy`). The kind-only ones (`dev-up`, `load`, `kind-up`,
  `install-gvisor`, `install-agent-substrate`, `agent-substrate-enable`) refuse it.
- `k8s-job` needs no extra substrate. The `isolated` class (`PROVIDERS=agent-sandbox`,
  after `make install-agent-sandbox`) puts an isolation RuntimeClass on every run's
  pod. It defaults to `gvisor`, which `install-gvisor` provides on kind only. On a
  cluster with another sandbox runtime, name its RuntimeClass with
  `ISOLATION_CLASS`, e.g. `ISOLATION_CLASS=kata-vm-isolation` for AKS Pod Sandboxing
  (Kata), which needs a node pool created with `--workload-runtime KataVmIsolation`.

## Docs

- [docs/hello-world.md](docs/hello-world.md) — **start here**: a Go + Python "Hello, World!" agent, end to end
- [docs/motivation.md](docs/motivation.md) — the fragmentation problem and the ecosystem survey
- [docs/design.md](docs/design.md) — the three contracts, the CRDs, the capability model, isolation, conformance
- [docs/dispatch-plane.md](docs/dispatch-plane.md) — the app→platform wire (the scalable, object-free dispatch path)
- [docs/runtime-abi.md](docs/runtime-abi.md) — the runtime→platform wire (`AEI_*` + `/complete` · `/redeem` · `/vend`)
- [docs/provider-extension.md](docs/provider-extension.md) — how a substrate plugs in, in any language
- [docs/roadmap.md](docs/roadmap.md) — the phased plan and the open decisions

## Scope

**AEI standardizes** submitting a run, observing completion, injecting a scoped
short-lived identity (and brokering downstream credentials), and selecting
isolation. **It does not standardize** how you author an agent, the
prompt/tool/result payload (that is A2A/MCP, app-defined), or scheduling and quota
(defer to Kueue). See [docs/design.md](docs/design.md).

## License

[Apache License 2.0](LICENSE). AEI composes with — not competes with — MCP and A2A
as the wire, kubernetes-sigs/agent-sandbox for isolation, model gateways for
egress, SPIFFE/SPIRE for attestation, and Kueue for admission.
