# AEI SDKs

Per-language **app-developer** SDKs for the Agent Execution Interface. An AEI app
dispatches agent work by submitting runs to the AEI **dispatch API** and reading
their status; an SDK is the thin, idiomatic veneer for that in one language, so an
app writes **no wire plumbing and no substrate code**, and creates **no per-run
Kubernetes object** (docs/dispatch-plane.md).

Every SDK is the same two pieces:

1. **typed models** of the run spec/status, *generated* from the one published
   `AgentRun` CRD schema (`controller/config/crd/agents.x-k8s.io_agentruns.yaml`) —
   the schema is reused as the dispatch **body** contract, so it stays the single
   source of truth even though dispatch is a call, not an object;
2. **a thin client** of the dispatch API — a small, stable **HTTP/JSON** contract
   (`POST /dispatch` → a run id; `GET /runs/{id}` for lifecycle).

Nothing here is a bespoke protocol: the transport is plain HTTP, the schema is the
contract. A change to the schema matriculates to every language by regenerating the
models against it.

| SDK | Status | Uses |
| --- | --- | --- |
| [`python/`](python) | reference | `pydantic` (models) + `requests` (wire) |
| `go/` | generatable | the controller's own API types + `net/http` |
| `typescript/` | generatable | a quicktype/OpenAPI model + `fetch` |
| _any other_ | generatable | generated model + that language's HTTP client |

## Scaling across languages — generate, don't author

Hand-authoring an SDK per language would not scale. It does not have to, because
**both pieces are trivial or generated:**

- **The wire is plain HTTP/JSON.** Dispatch is `POST /aei/v1alpha1/dispatch` with a
  bearer token; status is a `GET`. Every language already has an HTTP client — this
  is *more* universal than a Kubernetes client, and the floor is `curl` (the
  [conformance](../conformance) suite proves the sibling *runtime* wire works from
  POSIX-sh + curl; the app wire is the same shape).
- **The types are generated from one schema.** The `AgentRun` CRD's
  `openAPIV3Schema` is the single source of truth, and it is itself generated from
  the controller's Go structs (`make -C controller generate`, a pinned
  `controller-gen`). Feed it to any schema code generator to emit typed models.
  [`generate.sh`](generate.sh) does exactly this:

  ```sh
  ./generate.sh go        agentrun.go       # quicktype
  ./generate.sh typescript agentrun.ts      # quicktype
  ./generate.sh rust      agentrun.rs       # quicktype (also swift, csharp, kotlin, …)
  ./generate.sh python                      # writes python/aei/_agentrun.py; needs pip install -e 'python[gen]'
  ```

  One schema in, typed models out — descriptions become doc comments, enums and
  camelCase are preserved. Adding a language is a generator invocation plus a
  ~50-line HTTP veneer.

So AEI maintains **one schema + one generation pipeline + a conformance test**, not
N SDKs. Each generated SDK is validated against the published schema in CI (the
drift test in [`python/tests`](python/tests) generalizes), so none can drift from
the Go controller.

## Python

```python
from aei import AeiClient, AgentRunSpec, Identity

# endpoint + app default from AEI_DISPATCH_URL / AEI_APP; the bearer defaults to
# the pod's projected ServiceAccount token.
client = AeiClient(endpoint="http://aei-abi.aei-system.svc:8080", app="pyfer")
spec = AgentRunSpec(
    parameters={"kind": "converse", "itemId": "org/repo#7", "provider": "github"},
    task_ref="item://org/repo#7",
    identity=Identity(subject="github:1", scopes=["signals:read", "metadata:write"]),
    timeout_seconds=900,
)
run_id = client.dispatch(spec)          # POST /dispatch — no Kubernetes object
result = client.wait(run_id)            # GET /runs/{id} — lifecycle only
print(result.phase, result.result_ref)  # the result itself lands in the app's store
```

The reference SDK dogfoods the generation pipeline — it is a generated mirror plus
a thin veneer:

| File | Written by | What it is |
| --- | --- | --- |
| [`aei/_agentrun.py`](python/aei/_agentrun.py) | **generated** (`./generate.sh python`) | the `AgentRun` type mirror; never hand-edited |
| [`aei/_base.py`](python/aei/_base.py) | hand | the base the generated models inherit (2 pydantic settings) |
| [`aei/models.py`](python/aei/models.py) | hand (veneer) | `AgentRunSpec.to_spec()` (the dispatch body), the `Result` view |
| [`aei/client.py`](python/aei/client.py) | hand (veneer) | `dispatch`/`get`/`wait` over the dispatch API (HTTP) |

Regenerate the mirror after a schema change with `./generate.sh python`; the veneer
and the drift test in [`python/tests`](python/tests) stay put. Enums are enforced
twice over — the generated model rejects a bad value at construction, and the
published schema rejects it independently (a non-vacuous check).

See [../docs/dispatch-plane.md](../docs/dispatch-plane.md) for the app→platform
wire and [jackfrancis/pyfer](https://github.com/jackfrancis/pyfer) for a full app
built on this SDK.
