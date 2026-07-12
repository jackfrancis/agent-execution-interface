# AEI runtime ABI (v1alpha1)

*A sub-spec of [design.md](design.md). This concretizes **Contract 2** — the
runtime ABI — from prose into a wire contract a runtime author (and a second app,
e.g. `github-gofer`) can implement against today. It is the **generic
replacement for a reference implementation's app-shaped injection contract**
(zumble-zay's `ZZ_*` env): the same three seams, with every app noun removed.*

**Status:** draft for comment. Names will change; the *shape* is what we are
trying to hold fixed. This is the **pacing item** for the zumble-zay →
`github-gofer` + AEI extraction, because it is the seam the extraction cuts
along. See [roadmap.md](roadmap.md) Phase 2.

---

## Scope

The runtime ABI standardizes exactly three things, plus one optional module, and
**nothing about the payload**:

| # | Seam | Direction | Required? |
| --- | --- | --- | --- |
| 1 | **Injection** — how a runtime learns its `RunSpec` | platform → runtime | required |
| 2 | **Ticket redemption** — pull-path credential | runtime → control plane | required *iff* the provider declared `PullsCredential()` |
| 3 | **Completion callback** — terminal outcome + timing | runtime → control plane | required |
| 4 | **Credential vending** — downstream secrets on demand | runtime → broker | optional |

**Non-goals (unchanged from the design):** the *task input*, the *tool-call
schema*, and the *result schema* are opaque (`taskRef` + `parameters` +
app-defined I/O). AEI defines **lifecycle, not payload**. A runtime that needs
AEI to understand its payload is a design smell (see [design.md](design.md)
non-goals and roadmap Decision C).

**The portability invariant:** a conformant runtime reads seams 1–4 through a
single decoder and a fixed set of routes, so **the same runtime binary runs on
every substrate** and cannot tell which one it is on.

---

## 0. Conventions

- **Reserved namespace.** Environment variables and metadata keys prefixed
  `AEI_` are reserved by this spec. An app carries its own per-run values inside
  `AEI_PARAMS` (see below) and MAY use its own non-`AEI_` env for *static*
  config.
- **Versioning.** This is `v1alpha1`. `AEI_CONTROL_URL` SHOULD include the
  version segment (e.g. `https://host/aei/v1alpha1`); routes are relative to it.
  `AEI_ABI_VERSION` MAY be injected for defensive checks; a mismatch the runtime
  cannot satisfy is a fatal start-up error.
- **Encoding.** All request/response bodies are JSON. Timestamps that ride the
  injection channel are **unix milliseconds** (compact, timezone-free); JSON
  bodies use RFC 3339 where a timestamp appears.
- **Keywords.** MUST / SHOULD / MAY per RFC 2119.

---

## 1. Injection convention

### 1.1 The keys

A launcher fills these; the runtime reads them through the one decoder (§1.3).
The **per-run** set MUST ride the dispatch (rule **R1**) — it can never be baked
into an image, because a durable runtime reuses one process and a snapshotted
runtime freezes its env in a golden image.

| Key | Meaning | Class | Notes |
| --- | --- | --- | --- |
| `AEI_RUN_ID` | unique run id; the completion-correlation key | per-run | echoed on `/complete`; not secret |
| `AEI_TASK_REF` | opaque handle to the work | per-run | AEI never parses it |
| `AEI_PARAMS` | opaque per-run parameters, JSON object encoded as a string | per-run | the app's per-run channel; app decodes |
| `AEI_DISPATCHED_AT` | dispatch-start, unix millis | per-run | drives launcher-symmetric provisioning timing |
| `AEI_RUN_CREDENTIAL` | the run credential (bearer) — **push** path | per-run | present **xor** `AEI_RUN_TICKET` |
| `AEI_RUN_TICKET` | single-use redemption ticket — **pull** path | per-run | present **xor** `AEI_RUN_CREDENTIAL` |
| `AEI_DEADLINE` | run wall-clock budget end, unix millis | per-run | optional; runtime MAY self-bound |
| `AEI_CONTROL_URL` | base URL for `/complete` (and the broker routes when `AEI_BROKER_URL` is unset) | config | typically stable; MUST be per-run-injectable for durable substrates |
| `AEI_BROKER_URL` | base URL for the identity broker — `/redeem` + `/vend` | config | optional; **absent ⇒ `AEI_CONTROL_URL`** |
| `AEI_AUDIENCE` | audience the run credential is bound to | config | optional |
| `AEI_ABI_VERSION` | ABI version the launcher speaks | config | optional; default `v1alpha1` |

**Exactly one** of `AEI_RUN_CREDENTIAL` / `AEI_RUN_TICKET` is present, and which
one is the provider's push/pull choice (Contract 1 capability). A provider whose
control plane **persists or snapshots** MUST use the ticket, so a live credential
never rides the dispatch at all (rule **R2**).

There is deliberately **no** `AEI_*` key for a downstream secret (a model key, a
user's Git token). Those are **vended** (§4), never injected. This is the one
place the generic contract is *stricter* than a typical reference implementation,
and on purpose.

### 1.2 The two transports

The keys above travel by one of two transports; the provider picks based on how
it starts the runtime:

- **Environment variables** — for **workload-creating** providers (`k8s-job`,
  `k8s-pod`, `agent-sandbox`, a remote sandbox `exec`). Every key is an env var.
- **Message metadata** — for **durable** providers (A2A `message/send`). The
  **per-run** keys ride the message metadata map (HTTP headers are stripped by
  A2A intermediaries, so metadata is the only reliable carrier); **config** keys
  come from the standing runtime's own environment.

### 1.3 The one decoder (precedence)

A conformant runtime resolves each key **metadata-first, then process env**:

```
value(key) = metadata[key]  if present, else  getenv(key)
```

This single rule is correct for both transports: a workload reads everything from
env (there is no metadata); a durable actor reads per-run values from metadata
and static config from its frozen env. The runtime therefore **cannot tell which
substrate launched it** — the portability invariant.

---

## 2. Control-plane routes

Routes live on **two bases**, and the split mirrors the contract boundary:
`/complete` is a **dispatch** concern (Contract 1) and lives on `AEI_CONTROL_URL`;
`/redeem` and `/vend` are **identity** concerns (Contract 3) and live on
`AEI_BROKER_URL`. `AEI_BROKER_URL` is **optional and defaults to
`AEI_CONTROL_URL`**, so a co-located deployment injects one URL and the runtime
never notices, while a deployment with a shared or separately-hardened broker
injects both — same ABI, no version bump. Splitting the base also lets an operator
write least-privilege egress ("runtimes may reach the broker") independently of
the control plane. Behind either base the fan-out is invisible to the runtime.

### 2.1 `POST {broker}/redeem` — pull-path credential (seam 2)

Called **only** when `AEI_RUN_TICKET` is set and no `AEI_RUN_CREDENTIAL`. The
ticket **is** the authorization (single-use; the platform issues exactly one per
dispatched run), so this route carries **no** bearer — the runtime has no
credential yet.

```http
POST {broker}/redeem
Content-Type: application/json

{ "ticket": "<AEI_RUN_TICKET>" }
```

```jsonc
// 200 OK
{ "credential": "<bearer>", "expires_in": 600 }
```

- The ticket rides the **body**, never a header, and is **consumed on first
  use**. A second presentation fails.
- Failure is a **coarse `401`**: unknown, spent, and expired are indistinguishable
  and reveal nothing.
- The minted credential's authority is bound to `AEI_RUN_ID` (so completion still
  correlates) and is short-TTL. The runtime holds it **in memory only** (**R2**).

### 2.2 `POST {control}/complete` — terminal outcome (seam 3)

The fast path the orchestrator races against the substrate watch and the deadline
backstop.

```http
POST {control}/complete
Authorization: Bearer <run credential>
Content-Type: application/json

{
  "run_id":     "<AEI_RUN_ID>",
  "error":      "",          // empty string = success; non-empty = failure detail
  "timing":     { /* §3 */ },
  "result_ref": ""           // optional opaque pointer to the result; AEI never parses it
}
```

```http
202 Accepted        // always, best-effort
```

- **`run_id` is echoed explicitly**, not inferred from the credential. This keeps
  the ABI uniform across push/pull and env/metadata: the runtime always has
  `AEI_RUN_ID` from the decoder and never has to parse an opaque credential.
- The control plane **MUST authorize**: the `run_id` must match the run the
  credential is bound to. A mismatch is rejected, so a runtime can only complete
  **its own** run.
- **Idempotent (R4).** The callback and the watch/deadline may both fire; the
  outcome is recorded **exactly once**. A duplicate or unknown `run_id` is a
  harmless no-op that still returns `202`.
- **`result_ref` is an opaque pointer, not payload.** When set, AEI stores it on
  `AgentRun.status.resultRef` for observability and **never parses it** — where the
  actual result lives (a worklist item, an object-store key) is app-defined.

### 2.3 `POST {broker}/vend` — downstream credential (seam 4, optional)

For a runtime that needs a downstream provider credential (a model key, a user's
GitHub token) without holding a standing secret.

```http
POST {broker}/vend
Authorization: Bearer <run credential>
Content-Type: application/json

{ "provider": "github", "audience": "…", "scopes": ["repo:read"] }
```

```jsonc
// 200 OK
{ "credential": "<bearer>", "token_type": "Bearer", "expires_in": 300 }
```

- The broker mints/fetches a credential whose authority is
  `run authority ∩ provider-class policy`, short-TTL and scoped to **this run**.
- Any long-lived secret (an OAuth **refresh** token, a root key) stays
  **server-side**; only a short-lived access credential crosses the wire.
- The runtime holds it in memory for the run only and MUST NOT log or persist it
  (**R2**). `vend` and an egress model-gateway are two points on the same
  "credentials-scoped-to-a-run" axis; either satisfies this seam.

---

## 3. Timing schema

Carried on `/complete.timing` and surfaced as `AgentRun.status.timing`. The point
is **launcher-symmetry**: a pod cold-start and a durable-actor resume must land in
one comparable number.

```jsonc
{
  "provisioning_seconds": 0.6,   // dispatch-start → runtime actually working
  "runtime_seconds":     64.6,   // the runtime's own work
  "model_seconds":       60.7,   // the model share (optional)
  "model_calls":         12,     // optional
  "tool_calls":          8,      // optional
  "cost":                { "tokens": 4200 }   // optional
}
```

`provisioning_seconds` is computed **by the runtime** as
`runtime_start − AEI_DISPATCHED_AT`, so an asynchronous pod cold-start (which the
launcher cannot see) and a synchronous actor resume are measured the same way.

---

## 4. Runtime-authoring rules → ABI mapping

The rules from [design.md](design.md) are consequences of this contract, not
extra burdens:

| Rule | How this ABI enforces / expresses it |
| --- | --- |
| **R1** per-run values ride the dispatch | the *per-run* keys in §1.1; decoder reads metadata-first so durable substrates get them out-of-band, never baked |
| **R2** no credential at rest | only a short-TTL credential **or** single-use ticket is injected; downstream secrets are vended (§2.3), never injected; persisting/snapshotting providers MUST use the ticket |
| **R3** multiplex-safe or declare | the decoder yields a fresh `RunSpec` per run; the runtime MUST NOT retain cross-run globals, else declare `runtimeLifecycle: perRun` |
| **R4** idempotent completion | `/complete` is best-effort `202` + deadline backstop; server records once |

---

## 5. Runtime SDK surface (Go, illustrative)

The SDK (one per language, Go first) encodes §1–§4 so the runtime author writes
business logic, not boilerplate. This is the generic replacement for a reference
implementation's `RunParams` / `ParamsFromEnv` / `ReportCompletion`.

```go
package aeiruntime // github.com/…/aei-runtime-go

// RunSpec is the runtime's view — a subset of the dispatch RunSpec. Isolation and
// the identity request are dispatch-side and never reach the runtime; the
// credential they produced arrives out-of-band (redeemed or injected).
type RunSpec struct {
    RunID        string
    TaskRef      string
    Parameters   map[string]string // decoded from AEI_PARAMS; app-defined
    DispatchedAt time.Time
    Deadline     time.Time // zero if AEI_DEADLINE absent
}

// Load runs the one decoder (metadata-first, env-fallback), redeems AEI_RUN_TICKET
// if present, and returns a Runtime that holds the run credential ephemerally.
// metadata is nil for a workload substrate; non-nil for a durable/A2A one.
func Load(ctx context.Context, getenv func(string) string, metadata map[string]string) (*Runtime, error)

type Runtime struct { /* control + broker URLs, run credential (in memory only), runID */ }

func (r *Runtime) Spec() RunSpec

// Vend brokers a short-lived downstream credential (seam 4). The result is held
// in memory for the run only; never log or persist it.
func (r *Runtime) Vend(ctx context.Context, provider string) (Credential, error)

// Complete reports the terminal outcome (seam 3), optionally with an opaque
// result_ref the platform stores on status.resultRef but never parses. Safe to
// call once; the platform dedupes against the watch/deadline backstop.
func (r *Runtime) Complete(ctx context.Context, runErr error, t Timing, resultRef string) error

// Main is the "little boilerplate": Load → run(fn) under the deadline → Complete.
// A workload's entrypoint is one line: aeiruntime.Main(os.Getenv, nil, myWork).
func Main(getenv func(string) string, metadata map[string]string,
    fn func(context.Context, *Runtime) error)
```

A durable (A2A) entrypoint passes the message metadata instead of `nil`; the same
`fn` runs unchanged. That single difference — `nil` vs. a metadata map — is the
*only* place the substrate leaks, and it is confined to the entrypoint, never the
business logic.

---

## 6. Worked example — one run, two substrates, one decoder

The same run (say, a `github-gofer` "converse" turn on PR 4127), dispatched two
ways. **Nothing in the runtime's business logic differs.**

**Push, via a Sandbox (env transport):**

```
AEI_RUN_ID=run-abc
AEI_TASK_REF=worklist://item/pr-4127
AEI_PARAMS={"kind":"converse","itemId":"pr-4127","provider":"github"}
AEI_RUN_CREDENTIAL=<bearer>          # push: token rides the dispatch
AEI_DISPATCHED_AT=1789000000000
AEI_CONTROL_URL=https://gofer.team-a.svc/aei/v1alpha1
# AEI_BROKER_URL unset ⇒ /redeem + /vend also use AEI_CONTROL_URL (co-located)
```

**Pull, via a durable A2A actor (metadata transport):**

```jsonc
// message metadata (per-run); AEI_CONTROL_URL comes from the actor's frozen env
{
  "AEI_RUN_ID":       "run-abc",
  "AEI_TASK_REF":     "worklist://item/pr-4127",
  "AEI_PARAMS":       "{\"kind\":\"converse\",\"itemId\":\"pr-4127\",\"provider\":\"github\"}",
  "AEI_RUN_TICKET":   "tkt-once",     // pull: a single-use ticket, not the token
  "AEI_DISPATCHED_AT":"1789000000000"
}
```

In both cases the runtime calls `Load(...)`, gets an identical `RunSpec`, `Vend`s
the `github` credential when it needs live PR context, does the model turn, writes
its result through its **own** app API, and calls `Complete`. The pull case adds
exactly one hidden hop — `Load` redeems `tkt-once` at `{broker}/redeem` before
returning — and the business logic never sees it.

---

## 7. Migration map — reference `ZZ_*` → generic `AEI_*`

For the extraction, the translation is mechanical. App nouns collapse into the
opaque `AEI_PARAMS`; a downstream secret becomes a `vend` call.

| zumble-zay `ZZ_*` | AEI generic | Note |
| --- | --- | --- |
| `ZZ_JOB_TYPE` | `AEI_PARAMS.kind` | app payload — opaque to AEI |
| `ZZ_ITEM_ID` | `AEI_PARAMS.itemId` | app payload |
| `ZZ_PROVIDER` | `AEI_PARAMS.provider` | app payload; also the `vend` argument |
| `ZZ_BASE_URL` | `AEI_CONTROL_URL` | now versioned; hosts `/complete` (and `/redeem` `/vend` unless `AEI_BROKER_URL` splits them out) |
| `ZZ_JOB_TOKEN` | `AEI_RUN_CREDENTIAL` | push path |
| `ZZ_JOB_TICKET` | `AEI_RUN_TICKET` | pull path |
| `ZZ_DISPATCHED_AT` | `AEI_DISPATCHED_AT` | unchanged in spirit |
| *(run id in token `jid`)* | `AEI_RUN_ID` | **now explicit**, not inferred from the credential |
| `ZZ_AI_ENDPOINT` / `ZZ_AI_MODEL` | `AEI_PARAMS.*` or app static env | app config, not AEI's concern |
| `ZZ_AI_TOKEN` (secret, injected) | **removed → `POST /vend {provider:"model"}`** | no downstream secret at rest |
| `POST /agent/complete` | `POST {control}/complete` | `run_id` explicit in body |
| `POST /agent/token` (redeem) | `POST {broker}/redeem` | same single-use semantics |
| `POST /agent/credentials/{p}` | `POST {broker}/vend` | provider in the body |

The two substantive changes the extraction introduces — **explicit `run_id`** and
**vend-instead-of-inject for downstream secrets** — are the two places the generic
ABI is deliberately cleaner than the app-shaped original, and both fall out of the
tenets rather than being invented here.

---

## Resolved decisions

Settled after review; recorded so the rationale survives.

- **`AEI_PARAMS` shape — one JSON string.** A single JSON object encoded as a
  string keeps AEI's reserved surface minimal and gives the app exactly one decode
  site. (Considered a nested metadata object; rejected as more surface for no gain.)
- **Deadline — absolute unix-millis (`AEI_DEADLINE`).** Robust to dispatch
  latency and symmetric with `AEI_DISPATCHED_AT`. (Considered a seconds budget;
  rejected as ambiguous once dispatch latency is non-trivial.)
- **Result pointer — an opaque `result_ref` on `/complete`.** Carried for
  `status.resultRef` observability and **never parsed**; where the result lives is
  app-defined, so a pointer does not breach payload opacity.
- **Broker addressing — split, defaulting to collapsed (B-with-default).**
  `/redeem` + `/vend` live on `AEI_BROKER_URL`, optional and defaulting to
  `AEI_CONTROL_URL`. Co-located deployments inject one URL; a shared or
  separately-hardened broker injects both with no ABI change. The wire mirrors the
  Contract 1 / Contract 3 boundary and matches the class model, where
  `AgentProviderClass.identityIssuer` is already distinct from the provider
  endpoint; it also enables least-privilege egress separation for the broker.

## Remaining open questions

- **Broker audience.** With a *separately-hardened* broker (its own trust domain),
  should the run credential carry a distinct `AEI_BROKER_AUDIENCE`, or is a single
  `AEI_AUDIENCE` valid at both planes sufficient? Co-located needs one; a split
  trust domain may want two. Deferred to the Contract 3 sub-spec (Phase 6).
