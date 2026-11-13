# AEI dev scaffolding, organized around the two real operational personas.
#
# CLUSTER ADMIN — stand up an AEI-functional cluster, generically (no app knowledge):
#     make dev-up          create/reuse a kind cluster; install the CRDs + controller;
#                          and provision the platform Ed25519 signing authority (all
#                          idempotent). This alone is a fully AEI-functional cluster.
#                          Add PROVIDERS=agent-sandbox,agent-substrate to also wire a
#                          default (trait-named) AgentProviderClass per substrate.
#     make agent-sandbox-enable / agent-substrate-enable
#                          enable the providers the cluster's substrates support (each
#                          substrate is installed with its OWN tooling; these apply
#                          only the AEI-side delta). The default k8s-job provider needs
#                          no external substrate, so `make e2e` works with zero prereqs.
#     make install-gvisor  install runsc + the 'gvisor' RuntimeClass on the nodes. A
#                          class with defaultIsolationClass: gvisor resolves to a pod
#                          runtimeClassName, so without this the pod-shaped providers
#                          REFUSE to dispatch (they never downgrade to the default
#                          runtime silently). Needed by the 'isolated' sample class.
#
# APP DEVELOPER — target an AEI-functional cluster with an app. The app brings only
# its own runtime image and CONSUMES AEI's public verify key; it feeds the platform
# nothing secret:
#     make public-key            read AEI's verify key to configure the app
#     make register-app APP_NS=<ns>   apply the app's AgentApp (its image + scopes)
#     make smoke                 dispatch a sample run and watch it Succeed
#
# DURABLE SUBSTRATE (agent-substrate) — the one substrate AEI must help provision
# (its actors are ate-api objects, not per-run pods). The whole prep is in `make`,
# in order, so it is reproducible:
#     make dev-up KIND_CLUSTER=<c>                    cluster + AEI (above)
#     make install-agent-substrate KIND_CLUSTER=<c>   clone/build/install the ate
#                          platform from source + stand up the local image registry.
#     make agent-substrate-enable KIND_CLUSTER=<c>    grant RBAC, create the actor
#                          namespace, point the controller at the in-cluster registry,
#                          build the ateom runtime image, and apply a CONFIGURED
#                          'durable' class (snapshotLocation + ateomImage). After this an
#                          app bound to the durable class dispatches with AEI
#                          materializing the WorkerPool + ActorTemplate + actor on the
#                          first run.
# SUBSTRATE_REGISTRY (localhost:5001) + SNAPSHOT_LOCATION (gs://ate-snapshots/) are
# the knobs. In a real (non-kind) deployment you set ateomImage + snapshotLocation on
# the class directly — no build step — see the sample class.
#
# KIND_CLUSTER selects the target cluster (env var or make arg); it defaults to your
# current kind context, else "aei".
#
# ANY OTHER CLUSTER (AKS, GKE, ... — what a CI job would run). Pass a kubeconfig file
# on the command line and the same targets act on that cluster instead of kind:
#     make push   IMG=<registry>/aei-manager:<tag> PLATFORM=linux/amd64
#     make deploy KUBECONFIG=<file> IMG=<registry>/aei-manager:<tag> PROVIDERS=k8s-job
#     make public-key KUBECONFIG=<file>          the verify key an app is configured with
# `deploy` installs the CRDs + controller, provisions the signing authority, and wires
# the PROVIDERS classes. AEI does not create, size or manage the cluster. The file's
# current context is used unless KUBE_CONTEXT names another. The kind-only targets
# (kind-up/down, load, dev-up, install-gvisor, install-agent-substrate,
# agent-substrate-enable) refuse to run against a kubeconfig.
#
# AN EXAMPLE OF SUCH A CLUSTER: AKS. Not part of AEI or of the standard; a
# convenience kept wholly in hack/aks/, as hack/kind/ is for the dev loop:
#     make create-aks      create/reuse a resource group, an AKS cluster (a system
#                          pool + a Kata pool for the 'isolated' class's
#                          kata-vm-isolation RuntimeClass) and an attached container
#                          registry; write a kubeconfig FILE; print the push/deploy
#                          commands above with the values filled in.
#     make delete-aks      delete what create-aks made (it asks for
#                          AKS_CONFIRM_DELETE=<resource group> first).
# AKS_* variables size and name everything, and AKS_DRY_RUN=1 prints the Azure
# commands without running them; see hack/aks/aks.mk.

IMG           ?= localhost/aei-manager:dev
# Container engine for building the image: prefer docker, fall back to podman when
# docker is not installed (kind then uses its podman provider for load too).
ENGINE        ?= $(shell command -v docker >/dev/null 2>&1 && echo docker || echo podman)
KIND          ?= kind
# kind must drive the SAME engine we build and load with. Its podman support is
# opt-in via an env var, so derive it from ENGINE instead of relying on the caller's
# shell — otherwise kind defaults to docker and every cluster command fails with
# "docker: executable file not found" on a podman-only machine.
ifeq ($(ENGINE),podman)
export KIND_EXPERIMENTAL_PROVIDER := podman
endif
# Target cluster. Defaults to the kind cluster of your CURRENT kube context (so
# `make dev-up` installs into the cluster you are already on — e.g. kind-gofer),
# falling back to "aei". Override explicitly with KIND_CLUSTER=<name>.
CURRENT_KIND_CLUSTER := $(shell kubectl config current-context 2>/dev/null | sed -n 's/^kind-//p')
KIND_CLUSTER  ?= $(if $(CURRENT_KIND_CLUSTER),$(CURRENT_KIND_CLUSTER),aei)
# A kubeconfig FILE passed on the make command line (KUBECONFIG=<file>) retargets every
# kubectl command at that cluster instead of kind. Only the command line counts: a
# KUBECONFIG merely exported in your shell leaves the kind default alone, so the dev
# loop cannot wander onto another cluster. KUBE_CONTEXT picks a context within the
# file; empty means the file's current context.
KUBECONFIG_GIVEN := $(filter command line,$(origin KUBECONFIG))
KUBE_CONTEXT  ?= $(if $(KUBECONFIG_GIVEN),,kind-$(KIND_CLUSTER))
# Every kubectl command is pinned to that one cluster; check-cluster (a dep
# of the install targets) validates it exists and echoes the resolved target.
KUBECTL       ?= kubectl $(if $(KUBECONFIG_GIVEN),--kubeconfig $(abspath $(KUBECONFIG))) $(if $(KUBE_CONTEXT),--context $(KUBE_CONTEXT))
# How messages name the target, and the argument that selects it again.
TARGET        = $(if $(KUBECONFIG_GIVEN),the cluster in $(KUBECONFIG),kind cluster '$(KIND_CLUSTER)')
TARGET_ARG    = $(if $(KUBECONFIG_GIVEN),KUBECONFIG=$(KUBECONFIG),KIND_CLUSTER=$(KIND_CLUSTER))
# The platform `make image` builds for (e.g. linux/amd64 for a cloud cluster built from
# an arm64 laptop). Empty builds for the engine's own platform, which is what kind runs.
PLATFORM      ?=
# Render the full install with the manager image set to IMG, and apply it.
APPLY_INSTALL = $(KUBECTL) kustomize controller/config/default | sed 's|image: localhost/aei-manager:dev$$|image: $(IMG)|' | $(KUBECTL) apply -f -

# The sample run the smoke test (re)creates.
SAMPLE_NS     ?= default
SAMPLE_RUN    ?= hello
# Per-provider smoke runs.
SANDBOX_RUN   ?= hello-sandbox
SANDBOX_NS    ?= aei-runtime     # where the (app-less) agent-sandbox smoke lands: the {app}-runtime pool
SUBSTRATE_RUN ?= hello-substrate
# The namespace the AgentApp is installed into (set to your app's namespace, e.g.
# APP_NS=gofer, to register it alongside an existing app).
APP_NS        ?= default
SAMPLES       := controller/config/samples
# PROVIDERS = comma-separated LITERAL provider (sandbox platform) names to wire a
# default AgentProviderClass for (e.g. agent-sandbox,agent-substrate). The class
# RESOURCE names read as traits (isolated, durable) so an app references a
# capability, not a platform. `make dev-up` applies these when PROVIDERS is set; it
# does NOT install the upstream substrates themselves.
PROVIDERS     ?=
# The agent-sandbox release that `make install-agent-sandbox` applies (upstream's
# Standard Install: core + extensions). Bump it as new releases land, or override
# per run: make install-agent-sandbox AGENT_SANDBOX_VERSION=v0.6.0
AGENT_SANDBOX_VERSION ?= v0.5.3
# The agent-substrate checkout `make install-agent-substrate` builds from. Substrate
# has no published install manifest — it builds from source — so this tracks a git
# ref (default main; substrate is very early, so expect breakage). Overridable.
AGENT_SUBSTRATE_REF ?= main
# The gVisor release `make install-gvisor` puts on the nodes, as it appears under
# storage.googleapis.com/gvisor/releases/ — "release/latest", or a dated pin like
# release/20260101.
GVISOR_RELEASE ?= release/latest
# The isolation class the 'isolated' class applies to every run. It is a RuntimeClass
# name: the pod-shaped providers put it on the runtime pod, so the cluster must have
# a RuntimeClass by this name. The default, gvisor, is what install-gvisor provides on
# kind. On a cluster with a different sandbox runtime, name its RuntimeClass instead,
# e.g. ISOLATION_CLASS=kata-vm-isolation for AKS Pod Sandboxing (Kata).
#
# When it is not given, a cluster that already has the 'isolated' class keeps that
# class's isolation, and only a cluster without one gets gvisor. Several targets
# re-apply the class (agent-sandbox-enable, agent-sandbox-smoke, provider), and
# without this a later run of one of them would quietly reset a Kata cluster to gvisor.
ISOLATION_CLASS ?= $(or $(shell $(KUBECTL) get agentproviderclass isolated --request-timeout=5s -o jsonpath='{.spec.defaultIsolationClass}' 2>/dev/null),gvisor)
# Apply the 'isolated' (agent-sandbox) class with ISOLATION_CLASS as both its default
# isolation and its advertised ISOLATION: capability, so the two cannot drift.
APPLY_ISOLATED_CLASS = sed -e 's|^  defaultIsolationClass: gvisor$$|  defaultIsolationClass: $(ISOLATION_CLASS)|' \
	-e 's|"ISOLATION:gvisor"|"ISOLATION:$(ISOLATION_CLASS)"|' $(SAMPLES)/agentproviderclass-agent-sandbox.yaml | $(KUBECTL) apply -f -
# The local registry the durable substrate pulls actor images from (stood up by
# install-agent-substrate). agent-substrate-enable builds the ateom-gvisor image here
# and pins the durable class's ateomImage to the pushed digest.
SUBSTRATE_REGISTRY ?= localhost:5001
# The in-cluster address of that registry, for the aei-controller to READ image configs
# (it resolves each durable app image's entrypoint for the ActorTemplate, since ate does
# not derive the process command from the image). agent-substrate-enable sets it as the
# controller's AEI_REGISTRY_REWRITE target and install-agent-substrate publishes the
# matching Service. In a real cluster this is unused — the controller reads the image
# ref's own registry directly.
SUBSTRATE_REGISTRY_SERVICE ?= kind-registry.default.svc.cluster.local:5000
# The golden-snapshot storage PREFIX for durable actors. The durable class carries it
# and AEI derives a per-app path (<prefix><actor>). On kind, gs:// routes to the
# in-cluster rustfs, whose bucket the ate install names "ate-snapshots".
SNAPSHOT_LOCATION ?= gs://ate-snapshots/
# KIND_CONFIG is the kind cluster config `kind-up`/`dev-up` builds with. It defaults
# to a config carrying the feature gates Agent Substrate needs (see the file), so ONE
# cluster runs every provider — the gates are inert for the others. It only affects
# CREATION (gates can't be added to a running cluster), so after changing it recreate:
# make down && make dev-up. Set KIND_CONFIG= (empty) to build a plain cluster.
KIND_CONFIG   ?= hack/kind/cluster.yaml

.DEFAULT_GOAL := help

.PHONY: help
help: ## list targets
	@grep -hE '^[a-zA-Z0-9_-]+:.*?## ' $(MAKEFILE_LIST) | sort | \
		awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-22s\033[0m %s\n", $$1, $$2}'

## ── cluster lifecycle ─────────────────────────────────────────────────────────

# Targets that only make sense on kind (they create the cluster, load images into its
# nodes, or exec into them) depend on this, so they fail clearly against a kubeconfig.
.PHONY: require-kind
require-kind:
	@[ -z "$(KUBECONFIG_GIVEN)" ] || { \
		echo "ERROR: '$(MAKECMDGOALS)' includes a kind-only step, but KUBECONFIG=$(KUBECONFIG) targets another cluster."; \
		echo "       On a non-kind cluster: push the image (make push IMG=...), then make deploy KUBECONFIG=... IMG=..."; \
		exit 1; }

.PHONY: kind-up
kind-up: require-kind ## create the kind cluster if missing (reuses an existing one); KIND_CONFIG=<file> customizes creation
	@if $(KIND) get clusters 2>/dev/null | grep -qx $(KIND_CLUSTER); then \
		echo "reusing existing kind cluster '$(KIND_CLUSTER)'"; \
	else \
		$(KIND) create cluster --name $(KIND_CLUSTER) $(if $(KIND_CONFIG),--config $(KIND_CONFIG)); \
	fi
	@# Per-node runtime tuning. Applied on every kind-up (create OR reuse), idempotent
	@# and best-effort, so a cluster is never left half-tuned:
	@#   * proxy ARP — gVisor pod-to-pod loopback (Agent Substrate and agent-sandbox
	@#     both use gVisor); inert on a non-gVisor cluster.
	@#   * unlimited pids — podman caps each node container at 2048 tasks (docker does
	@#     not). One gVisor pod costs a runsc sandbox process, a gofer process, and the
	@#     agent's own threads, so a fan-out of a few dozen isolated runs exhausts the
	@#     node's task budget and runsc dies with "failed to create new OS thread
	@#     ... errno=11". That is a dev-cluster ceiling, not a workload limit, so lift it.
	@for n in $$($(KIND) get nodes --name $(KIND_CLUSTER) 2>/dev/null); do \
		$(ENGINE) exec "$$n" sysctl -w net.ipv4.conf.all.proxy_arp=1 >/dev/null 2>&1 || true; \
		$(ENGINE) update --pids-limit=-1 "$$n" >/dev/null 2>&1 || true; \
	done
	@$(KUBECTL) cluster-info >/dev/null && echo "kind cluster '$(KIND_CLUSTER)' ready"

.PHONY: kind-down
kind-down: require-kind ## delete the kind cluster
	$(KIND) delete cluster --name $(KIND_CLUSTER)

.PHONY: check-cluster
check-cluster: ## verify the target cluster is reachable (echoes the resolved target)
ifneq ($(KUBECONFIG_GIVEN),)
	@[ -f "$(abspath $(KUBECONFIG))" ] || { echo "ERROR: kubeconfig not found: $(abspath $(KUBECONFIG))"; exit 1; }
	@$(KUBECTL) get --raw /version >/dev/null 2>&1 || { \
		echo "ERROR: cannot reach the cluster in $(KUBECONFIG)$(if $(KUBE_CONTEXT), (context $(KUBE_CONTEXT)))."; \
		echo "contexts in that file:"; $(KUBECTL) config get-contexts -o name 2>/dev/null | sed 's/^/  - /'; \
		exit 1; }
	@echo "→ targeting context '$$($(KUBECTL) config view --minify -o jsonpath='{.current-context}')' at $$($(KUBECTL) config view --minify -o jsonpath='{.clusters[0].cluster.server}') ($(KUBECONFIG))"
else
	@kubectl config get-contexts -o name 2>/dev/null | grep -qx kind-$(KIND_CLUSTER) || { \
		echo "ERROR: kube context 'kind-$(KIND_CLUSTER)' not found — AEI would install nowhere."; \
		kubectl config current-context >/dev/null 2>&1 || \
			echo "(your kube current-context is not set, so KIND_CLUSTER fell back to '$(KIND_CLUSTER)'.)"; \
		echo "kind contexts in your kubeconfig:"; \
		kubectl config get-contexts -o name 2>/dev/null | sed -n 's/^kind-/  - /p' | grep . || \
			echo "  (none — create one with: make up KIND_CLUSTER=<name>)"; \
		echo "Then: make <target> KIND_CLUSTER=<name>  (or first: kubectl config use-context kind-<name>)"; \
		echo "For a non-kind cluster: make <target> KUBECONFIG=<file>"; \
		exit 1; }
	@echo "→ targeting kind cluster '$(KIND_CLUSTER)' (context kind-$(KIND_CLUSTER))"
endif

# An example non-kind cluster (create-aks, delete-aks). Everything Azure-specific is
# in hack/aks/.
include hack/aks/aks.mk

## ── build & deploy ────────────────────────────────────────────────────────────

.PHONY: image
image: ## build the manager image (context = repo root); PLATFORM=linux/amd64 cross-builds
	$(MAKE) -C controller docker-build IMG=$(IMG) ENGINE=$(ENGINE) PLATFORM=$(PLATFORM)

# A non-kind cluster pulls the manager from a registry, so the image needs a registry
# name and the nodes' platform. The dev default (localhost/...) is not pushable.
.PHONY: push
push: ## build + push the manager image: IMG=<registry>/aei-manager:<tag> [PLATFORM=linux/amd64]
	@case "$(IMG)" in localhost/*) \
		echo "ERROR: IMG=$(IMG) is the kind dev tag. Set IMG=<registry>/aei-manager:<tag>."; exit 1;; esac
	$(MAKE) image IMG=$(IMG) PLATFORM=$(PLATFORM)
	$(ENGINE) push $(IMG)

.PHONY: load
load: require-kind check-cluster ## load the manager image into the kind cluster
	$(MAKE) -C controller kind-load IMG=$(IMG) ENGINE=$(ENGINE) KIND=$(KIND) KIND_CLUSTER=$(KIND_CLUSTER)

.PHONY: install
install: check-cluster ## install just the CRDs
	$(KUBECTL) apply -k controller/config/crd

# deploy brings AEI up on a cluster that already exists: it applies the CRDs, RBAC and
# the manager (with its image set to IMG), provisions the signing authority, and wires
# the PROVIDERS classes. It does NOT build, push or load the image: on kind it must
# already be loaded (make load / make dev-up); on any other cluster IMG must be pullable
# from a registry (make push). It is the whole install for a non-kind cluster:
#     make deploy KUBECONFIG=<file> IMG=<registry>/aei-manager:<tag> PROVIDERS=k8s-job
# Use a new tag per build: the pod only re-pulls when the image reference changes.
.PHONY: deploy
deploy: check-cluster ## install CRDs + RBAC + the manager at IMG, provision the authority, wire PROVIDERS
	$(APPLY_INSTALL)
	@$(KUBECTL) -n aei-system rollout status deploy/aei-controller --timeout=180s || { \
		echo; echo "controller not ready — check: $(KUBECTL) -n aei-system get pods"; \
		echo "If its pod is ImagePullBackOff, the image '$(IMG)' is not available to $(TARGET):"; \
		echo "  kind: make load $(TARGET_ARG)      other clusters: make push IMG=<registry>/aei-manager:<tag>"; \
		exit 1; }
	$(MAKE) authority
	@[ -z "$(PROVIDERS)" ] || $(MAKE) providers PROVIDERS="$(PROVIDERS)"
	@echo "AEI is up on $(TARGET). App devs: 'make public-key $(TARGET_ARG)', then register + smoke."

.PHONY: undeploy
undeploy: ## remove the operator (leaves the cluster running)
	$(MAKE) -C controller undeploy KUBECTL="$(KUBECTL)"

## ── one-shot orchestration ────────────────────────────────────────────────────

# dev-up brings AEI up on the cluster named by KIND_CLUSTER (default: your current
# kind context, else "aei"), CREATING that kind cluster if it does not exist, then
# building the single controller image (it embeds every in-process provider),
# loading it, (re)deploying, and rolling the pod so a same-tag :dev rebuild is
# picked up. Override the target with KIND_CLUSTER=<name> (env var or make arg) —
# e.g. when you have multiple clusters. dev-down removes AEI (keeps the cluster);
# down deletes the whole cluster.

.PHONY: dev-up
dev-up: require-kind kind-up image load ## create the cluster if needed, build+load+deploy AEI, and provision the signing authority
	$(APPLY_INSTALL)
	@echo "rolling the controller to pick up the freshly loaded image..."
	@$(KUBECTL) -n aei-system rollout restart deploy/aei-controller
	@$(KUBECTL) -n aei-system rollout status deploy/aei-controller --timeout=120s || { \
		echo; echo "controller not ready — check: $(KUBECTL) -n aei-system get pods"; exit 1; }
	$(MAKE) authority KIND_CLUSTER="$(KIND_CLUSTER)"
	@[ -z "$(PROVIDERS)" ] || $(MAKE) providers KIND_CLUSTER="$(KIND_CLUSTER)" PROVIDERS="$(PROVIDERS)"
	@echo "AEI is up on kind cluster '$(KIND_CLUSTER)'. App devs: 'make public-key', then register + smoke."

.PHONY: dev-down
dev-down: undeploy ## remove AEI from the cluster (leaves the cluster running)

.PHONY: up
up: dev-up ## alias for dev-up (create/reuse the cluster, then install AEI)

.PHONY: down
down: kind-down ## delete the whole kind cluster

.PHONY: e2e
e2e: up smoke ## full loop: stand AEI up, then drive a run to Succeeded

# Run-credential signing authority (docs/design.md, Contract 3). AEI is the sole
# minter: `authority` provisions a PLATFORM Ed25519 keypair, stores it, and rolls
# the controller onto it; apps consume only the public key (`public-key`) and feed
# the platform nothing secret. Idempotent — it never rotates an existing key. Until
# provisioned the controller uses the reference HMAC secret (fine for the built-in
# smoke; asymmetric verify-only apps need this).
.PHONY: authority
authority: check-cluster ## provision the platform Ed25519 signing keypair (idempotent) and roll the controller
	@if $(KUBECTL) -n aei-system get secret aei-signing -o jsonpath='{.data.AEI_ED25519_PRIVATE_KEY}' 2>/dev/null | grep -q .; then \
		echo "platform Ed25519 authority already provisioned (make public-key to read it)"; \
	else \
		echo "generating a platform Ed25519 keypair..."; \
		keys=$$(go run ./hack/genkey) || exit 1; \
		priv=$$(printf '%s\n' "$$keys" | sed -n 's/^AEI_ED25519_PRIVATE_KEY=//p'); \
		pub=$$(printf '%s\n' "$$keys" | sed -n 's/^AEI_ED25519_PUBLIC_KEY=//p'); \
		$(KUBECTL) -n aei-system patch secret aei-signing --type=merge \
			-p '{"stringData":{"AEI_ED25519_PRIVATE_KEY":"'"$$priv"'","AEI_ED25519_PUBLIC_KEY":"'"$$pub"'"}}'; \
		$(KUBECTL) -n aei-system rollout restart deploy/aei-controller; \
		$(KUBECTL) -n aei-system rollout status deploy/aei-controller --timeout=120s; \
		echo "provisioned. app verify key (also at GET /aei/v1alpha1/authority):"; echo "  $$pub"; \
	fi

.PHONY: public-key
public-key: check-cluster ## print AEI's run-credential verification (public) key for app developers
	@pub=$$($(KUBECTL) -n aei-system get secret aei-signing -o jsonpath='{.data.AEI_ED25519_PUBLIC_KEY}' 2>/dev/null | base64 -d 2>/dev/null); \
		test -n "$$pub" || { echo "no Ed25519 authority provisioned — run: make authority"; exit 1; }; \
		echo "$$pub"

## ── samples & smoke test (k8s-job: no external substrate) ─────────────────────

.PHONY: samples
samples: ## apply the default (generic) k8s-job provider class — an admin substrate binding
	$(KUBECTL) apply -f $(SAMPLES)/agentproviderclass-k8s-job.yaml

.PHONY: register-app
register-app: install samples ## register an app: apply its AgentApp (its own runtime image + scopes)
	$(KUBECTL) -n $(APP_NS) apply -f $(SAMPLES)/agentapp.yaml
	@echo "registered app in namespace '$(APP_NS)' on $(TARGET)."
	@echo "Serve dispatch with a running control plane (make dev-up); for a verify-only"
	@echo "app, provision the authority (make authority) and share it (make public-key)."

.PHONY: unregister-app
unregister-app: ## remove the AgentApp (leaves the generic class + CRDs)
	-$(KUBECTL) -n $(APP_NS) delete -f $(SAMPLES)/agentapp.yaml --ignore-not-found

.PHONY: smoke
smoke: check-cluster samples ## dispatch a sample AgentRun and wait for it to Succeed
	@$(KUBECTL) -n $(SAMPLE_NS) delete agentrun $(SAMPLE_RUN) --ignore-not-found >/dev/null 2>&1 || true
	$(KUBECTL) apply -f $(SAMPLES)/agentrun.yaml
	@echo "waiting for AgentRun/$(SAMPLE_RUN) to finish..."
	@for i in $$(seq 1 90); do \
		phase=$$($(KUBECTL) -n $(SAMPLE_NS) get agentrun $(SAMPLE_RUN) -o jsonpath='{.status.phase}' 2>/dev/null); \
		case "$$phase" in \
			Succeeded) echo "  AgentRun/$(SAMPLE_RUN) Succeeded"; $(KUBECTL) -n $(SAMPLE_NS) get agentrun $(SAMPLE_RUN) -o wide; exit 0;; \
			Failed)    echo "  AgentRun/$(SAMPLE_RUN) Failed"; $(KUBECTL) -n $(SAMPLE_NS) get agentrun $(SAMPLE_RUN) -o yaml; exit 1;; \
			*)         printf '  phase=%s\r' "$${phase:-<pending>}";; \
		esac; \
		sleep 2; \
	done; \
	echo; echo "timed out waiting for AgentRun/$(SAMPLE_RUN); recent controller logs:"; \
	$(KUBECTL) -n aei-system logs deploy/aei-controller --tail=40; exit 1

.PHONY: k8s-job-smoke
k8s-job-smoke: smoke ## alias for smoke (the k8s-job round-trip to Succeeded)

.PHONY: samples-delete
samples-delete: ## remove the sample run + k8s-job class
	-$(KUBECTL) -n $(SAMPLE_NS) delete agentrun $(SAMPLE_RUN) --ignore-not-found
	-$(KUBECTL) delete -f $(SAMPLES)/agentproviderclass-k8s-job.yaml --ignore-not-found

## ── provider prerequisites (delegated to each substrate's own tooling) ────────
# AEI does NOT own these substrates. Install each with its own operator/CLI, then
# run the matching *-enable target to apply only the AEI-side delta. As a dev
# convenience, `make install-agent-sandbox` wraps upstream agent-sandbox's own
# published install manifest (still that project's asset, pinned by version).

# providers wires a default (trait-named) AgentProviderClass — plus the AEI-side
# RBAC where a provider needs it — for each LITERAL provider name in PROVIDERS. It
# does NOT preflight or install the upstream substrate (bring that with its own
# tooling); a class can exist before its substrate does. Named classes: k8s-job
# → k8s-job, agent-sandbox → isolated, agent-substrate → durable.
.PHONY: providers
providers: check-cluster ## wire default AgentProviderClasses for PROVIDERS=<name,…> (literal provider names)
	@for p in $$(echo "$(PROVIDERS)" | tr ',' ' '); do \
		echo "wiring provider class for '$$p'"; \
		case "$$p" in \
			k8s-job) $(KUBECTL) apply -f $(SAMPLES)/agentproviderclass-k8s-job.yaml;; \
			agent-sandbox) $(KUBECTL) apply -f controller/config/rbac/agent-sandbox.yaml; \
				$(APPLY_ISOLATED_CLASS); \
				$(KUBECTL) get runtimeclass $(ISOLATION_CLASS) >/dev/null 2>&1 || \
					echo "  WARNING: no '$(ISOLATION_CLASS)' RuntimeClass yet — isolated runs fail at dispatch until: make install-gvisor KIND_CLUSTER=$(KIND_CLUSTER) (kind only; elsewhere install a '$(ISOLATION_CLASS)' RuntimeClass yourself)";; \
			agent-substrate) $(KUBECTL) apply -f controller/config/rbac/agent-substrate.yaml; \
				$(KUBECTL) apply -f $(SAMPLES)/agentproviderclass-agent-substrate.yaml; \
				$(MAKE) durable-class-hints KIND_CLUSTER=$(KIND_CLUSTER);; \
			"") ;; \
			*) echo "  unknown provider '$$p' (known: k8s-job, agent-sandbox, agent-substrate)"; exit 1;; \
		esac; \
	done

# provider is the singular convenience over `providers`: install ONE provider's
# default (trait-named) class on the target cluster. Provider classes are
# cluster-scoped, so install several on one cluster to A/B-swap at runtime — just
# repoint an app's spec.providerClassName (e.g. gofer: AGENT_PROVIDER_CLASS=isolated
# make dev-up), no redeploy of AEI needed. `make status` lists what is installed.
#     make provider PROVIDER=k8s-job
#     make provider PROVIDER=agent-sandbox   # then: make install-agent-sandbox for the substrate
.PHONY: provider
provider: ## install ONE provider's default class: PROVIDER=<k8s-job|agent-sandbox|agent-substrate>
	@test -n "$(PROVIDER)" || { echo "usage: make provider PROVIDER=<k8s-job|agent-sandbox|agent-substrate>"; exit 1; }
	@$(MAKE) providers PROVIDERS="$(PROVIDER)" KIND_CLUSTER="$(KIND_CLUSTER)"

.PHONY: install-agent-sandbox
install-agent-sandbox: check-cluster ## install upstream agent-sandbox (core + extensions) at AGENT_SANDBOX_VERSION
	@echo "installing agent-sandbox $(AGENT_SANDBOX_VERSION) (core + extensions) on $(TARGET)..."
	$(KUBECTL) apply -f https://github.com/kubernetes-sigs/agent-sandbox/releases/download/$(AGENT_SANDBOX_VERSION)/sandbox-with-extensions.yaml
	@echo "waiting for the Sandbox CRD to be established..."
	$(KUBECTL) wait --for=condition=established --timeout=60s crd/sandboxes.agents.x-k8s.io
	@echo "agent-sandbox $(AGENT_SANDBOX_VERSION) installed on $(TARGET). Enable the AEI-side delta with:"
	@echo "  make agent-sandbox-enable $(TARGET_ARG)   (or wire a class via PROVIDERS=agent-sandbox on make dev-up / make deploy)"

# install-agent-substrate installs the UPSTREAM agent-substrate ("ate") FROM SOURCE
# (it has no published manifest): it clones the repo, stands up the local image registry
# its ko build pushes to, and runs substrate's own install-ate-kind.sh against this
# cluster (which dev-up already built substrate-compatible). Heavy + tracks main; see
# hack/install-agent-substrate.sh. Then `make agent-substrate-enable` applies the
# AEI-side 'durable' class.
.PHONY: install-agent-substrate
install-agent-substrate: require-kind check-cluster ## clone+build+install upstream agent-substrate (ate) from source (git+go+engine; AGENT_SUBSTRATE_REF=main)
	AGENT_SUBSTRATE_REF="$(AGENT_SUBSTRATE_REF)" KIND_CLUSTER="$(KIND_CLUSTER)" KIND="$(KIND)" ENGINE="$(ENGINE)" hack/install-agent-substrate.sh

# install-gvisor makes `defaultIsolationClass: gvisor` real on kind. AEI resolves a
# run's isolation class to a pod runtimeClassName, so the cluster needs runsc on each
# node plus a matching RuntimeClass — a stock kind node has neither, and the
# pod-shaped providers then refuse to dispatch rather than run unisolated. Works on a
# running cluster (no recreation) and is idempotent. Agent Substrate's own gVisor does
# NOT satisfy this: it is not registered as a containerd runtime handler.
.PHONY: install-gvisor
install-gvisor: require-kind check-cluster ## install runsc on every node + the gvisor RuntimeClass (needed by defaultIsolationClass: gvisor)
	KIND_CLUSTER="$(KIND_CLUSTER)" KIND="$(KIND)" ENGINE="$(ENGINE)" GVISOR_RELEASE="$(GVISOR_RELEASE)" hack/install-gvisor.sh

.PHONY: agent-sandbox-enable
agent-sandbox-enable: ## grant the controller Sandbox RBAC + a sample class (needs upstream agent-sandbox)
	@$(KUBECTL) get crd sandboxes.agents.x-k8s.io >/dev/null 2>&1 || { \
		echo "ERROR: the Sandbox CRD is not installed."; \
		echo "Install it with 'make install-agent-sandbox' (upstream core + extensions),"; \
		echo "  or with agent-sandbox's own tooling (kubernetes-sigs/agent-sandbox),"; \
		echo "  then re-run this target."; \
		exit 1; }
	$(KUBECTL) apply -f controller/config/rbac/agent-sandbox.yaml
	$(APPLY_ISOLATED_CLASS)
	@# The class asks for ISOLATION_CLASS isolation, which resolves to a pod RuntimeClass. If
	@# the cluster has no such handler the provider fails every dispatch (by design —
	@# it will not run unisolated under an isolated class), so say so up front.
	@$(KUBECTL) get runtimeclass $(ISOLATION_CLASS) >/dev/null 2>&1 || { \
		echo "WARNING: this class sets defaultIsolationClass: $(ISOLATION_CLASS), but no such"; \
		echo "  RuntimeClass exists — runs will FAIL at dispatch until you install it:"; \
		echo "    make install-gvisor KIND_CLUSTER=$(KIND_CLUSTER)"; }
	@echo "agent-sandbox enabled. RBAC is evaluated server-side per request, so a"
	@echo "running controller picks up the new Sandbox permission with no restart."

.PHONY: agent-substrate-enable
agent-substrate-enable: require-kind ## make durable ready: RBAC, namespace, registry route, ateom image + a CONFIGURED durable class (needs upstream ate)
	@$(KUBECTL) get svc -n ate-system atenet-router >/dev/null 2>&1 || { \
		echo "ERROR: atenet-router not found in namespace ate-system."; \
		echo "Install the substrate first: make install-agent-substrate KIND_CLUSTER=$(KIND_CLUSTER)"; \
		exit 1; }
	$(KUBECTL) apply -f controller/config/rbac/agent-substrate.yaml
	$(KUBECTL) apply -f $(SAMPLES)/agentproviderclass-agent-substrate.yaml
	@# AEI materializes the actor's ActorTemplate + WorkerPool in the class's
	@# templateNamespace (default aei-actors) but does not create that namespace itself
	@# (it is the admin's placement namespace for durable actor pods), so ensure it here.
	@tns=$$($(KUBECTL) get agentproviderclass durable -o jsonpath='{.spec.parameters.templateNamespace}' 2>/dev/null); \
	[ -n "$$tns" ] || tns=aei-actors; \
	echo "  durable actor namespace: $$tns"; \
	$(KUBECTL) create namespace "$$tns" --dry-run=client -o yaml | $(KUBECTL) apply -f - >/dev/null
	@# Point the controller at an in-cluster route to the app-image registry so it can read
	@# each image's entrypoint for the durable ActorTemplate (ate needs an explicit command;
	@# the controller pod cannot reach the nodes' localhost:5001). A no-op in a real cluster,
	@# where the image ref's own registry is reachable from the controller.
	@$(KUBECTL) -n aei-system set env deployment/aei-controller -c manager \
		AEI_REGISTRY_REWRITE="$(SUBSTRATE_REGISTRY)=http://$(SUBSTRATE_REGISTRY_SERVICE)" \
		|| echo "  WARNING: could not set AEI_REGISTRY_REWRITE on aei-controller (deployed? run 'make dev-up')."
	@# Build the substrate ateom-gvisor image so AEI can materialize the WorkerPool,
	@# then configure the durable class with the pushed digest + snapshot prefix. If the
	@# build is unavailable, fall back to snapshotLocation only (admin supplies a pool).
	@ateom=$$(SUBSTRATE_REGISTRY="$(SUBSTRATE_REGISTRY)" hack/build-ateom-image.sh || true); \
	if [ -n "$$ateom" ]; then \
		echo "  ateom-gvisor image: $$ateom"; \
		$(KUBECTL) patch agentproviderclass durable --type=merge \
			-p "{\"spec\":{\"parameters\":{\"snapshotLocation\":\"$(SNAPSHOT_LOCATION)\",\"ateomImage\":\"$$ateom\"}}}"; \
	else \
		echo "  WARNING: ateom-gvisor build unavailable; configuring snapshotLocation only."; \
		echo "           AEI will not create the WorkerPool - provide one labeled aei.dev/worker-pool=aei."; \
		$(KUBECTL) patch agentproviderclass durable --type=merge \
			-p "{\"spec\":{\"parameters\":{\"snapshotLocation\":\"$(SNAPSHOT_LOCATION)\"}}}"; \
	fi
	@# The controller builds and caches the provider launcher per class at first use,
	@# so the class patch above only reaches a fresh process. Restart it so an already
	@# deployed app (its AgentApp bound to this class) picks up the configured
	@# snapshotLocation/ateomImage and provisions its actor on the next warm/dispatch.
	@$(KUBECTL) -n aei-system rollout restart deploy/aei-controller 2>/dev/null \
		|| echo "  (aei-controller not deployed; it will read the configured class on next start)"
	@$(KUBECTL) -n aei-system rollout status deploy/aei-controller --timeout=120s 2>/dev/null || true
	@echo "agent-substrate enabled (durable class configured)."
	@$(MAKE) durable-class-hints KIND_CLUSTER="$(KIND_CLUSTER)"

# durable-class-hints warns when the 'durable' class lacks the ate infra params AEI
# needs to auto-provision an app's actor (the sample ships them empty). Shared by
# agent-substrate-enable and the `providers` wiring so both paths surface it.
.PHONY: durable-class-hints
durable-class-hints: ## warn if the durable class lacks snapshotLocation/ateomImage
	@snap=$$($(KUBECTL) get agentproviderclass durable -o jsonpath='{.spec.parameters.snapshotLocation}' 2>/dev/null); \
	ateom=$$($(KUBECTL) get agentproviderclass durable -o jsonpath='{.spec.parameters.ateomImage}' 2>/dev/null); \
	if [ -n "$$snap" ]; then \
		echo "  durable: snapshotLocation=$$snap -> AEI auto-creates the ActorTemplate from each app's image."; \
	else \
		echo; \
		echo "  durable WARNING: no snapshotLocation on the 'durable' class, so AEI will NOT auto-create the"; \
		echo "                   ActorTemplate; a durable run then needs a pre-provisioned actor. Set it with:"; \
		echo "    $(KUBECTL) patch agentproviderclass durable --type=merge \\"; \
		echo "      -p '{\"spec\":{\"parameters\":{\"snapshotLocation\":\"gs://<bucket>/aei/\"}}}'"; \
	fi; \
	if [ -z "$$ateom" ]; then \
		echo "  durable NOTE: no ateomImage -> AEI will not create the WorkerPool; provide one yourself"; \
		echo "                (an ate.dev WorkerPool running the substrate ateom-gvisor image, labeled"; \
		echo "                aei.dev/worker-pool=aei so the ActorTemplate selects it)."; \
	fi

# A provider-scoped smoke proves DISPATCH: it asserts the artifact the provider
# created, which is the fastest provider-specific signal. agent-substrate's Await is
# detached (an A2A actor leaves no cluster object to watch), so only a real runtime
# calling /complete finishes one of its runs. agent-sandbox and k8s-job both watch
# their workload, so their runs also finalize on their own once the pod terminates.

.PHONY: agent-sandbox-smoke
agent-sandbox-smoke: agent-sandbox-enable ## dispatch against the isolated class and verify the provider creates a Sandbox
	@$(KUBECTL) -n $(SANDBOX_NS) delete sandboxes.agents.x-k8s.io -l app.kubernetes.io/managed-by=aei --ignore-not-found >/dev/null 2>&1 || true
	@$(KUBECTL) -n $(SAMPLE_NS) delete agentrun $(SANDBOX_RUN) --ignore-not-found >/dev/null 2>&1 || true
	$(KUBECTL) apply -f $(SAMPLES)/agentrun-agent-sandbox.yaml
	@echo "waiting for the agent-sandbox provider to create a Sandbox (dispatch proof)..."
	@for i in $$(seq 1 45); do \
		if [ -n "$$($(KUBECTL) -n $(SANDBOX_NS) get sandboxes.agents.x-k8s.io -l app.kubernetes.io/managed-by=aei -o name 2>/dev/null)" ]; then \
			echo "  Sandbox created — the provider dispatched:"; \
			$(KUBECTL) -n $(SANDBOX_NS) get sandboxes.agents.x-k8s.io -l app.kubernetes.io/managed-by=aei; \
			echo "  NOTE: the run finalizes when the runtime calls /complete, or when"; \
			echo "        the Sandbox's pod terminates (agent-sandbox Await watches it)."; \
			exit 0; \
		fi; \
		if [ "$$($(KUBECTL) -n $(SAMPLE_NS) get agentrun $(SANDBOX_RUN) -o jsonpath='{.status.phase}' 2>/dev/null)" = Failed ]; then \
			echo "  AgentRun/$(SANDBOX_RUN) Failed before a Sandbox appeared:"; \
			$(KUBECTL) -n $(SAMPLE_NS) get agentrun $(SANDBOX_RUN) -o yaml; \
			$(KUBECTL) -n aei-system logs deploy/aei-controller --tail=30; exit 1; \
		fi; \
		sleep 2; \
	done; \
	echo "timed out; is the controller running (make dev-up)? recent logs:"; \
	$(KUBECTL) -n aei-system logs deploy/aei-controller --tail=30; exit 1

.PHONY: agent-substrate-smoke
agent-substrate-smoke: agent-substrate-enable ## dispatch against the durable class and verify the router accepts it (needs full ate)
	@$(KUBECTL) -n $(SAMPLE_NS) delete agentrun $(SUBSTRATE_RUN) --ignore-not-found >/dev/null 2>&1 || true
	$(KUBECTL) apply -f $(SAMPLES)/agentrun-agent-substrate.yaml
	@echo "checking the agent-substrate dispatch is accepted (run stays Running, not Failed)..."
	@ok=; for i in $$(seq 1 10); do \
		phase=$$($(KUBECTL) -n $(SAMPLE_NS) get agentrun $(SUBSTRATE_RUN) -o jsonpath='{.status.phase}' 2>/dev/null); \
		if [ "$$phase" = Failed ]; then \
			echo "  AgentRun/$(SUBSTRATE_RUN) Failed — the router rejected dispatch:"; \
			$(KUBECTL) -n $(SAMPLE_NS) get agentrun $(SUBSTRATE_RUN) -o yaml; exit 1; \
		fi; \
		[ "$$phase" = Running ] && ok=1; \
		sleep 2; \
	done; \
	if [ -n "$$ok" ]; then \
		echo "  dispatch accepted — AgentRun/$(SUBSTRATE_RUN) is Running (A2A message sent to the router)."; \
		echo "  NOTE: completion needs a standing actor + a runtime that calls /complete (pull-ticket path)."; \
	else \
		echo "  AgentRun/$(SUBSTRATE_RUN) never reached Running; is the controller up (make dev-up)?"; \
		$(KUBECTL) -n aei-system logs deploy/aei-controller --tail=30; exit 1; \
	fi

## ── diagnostics ───────────────────────────────────────────────────────────────

.PHONY: status
status: ## show the controller + AEI objects
	-$(KUBECTL) -n aei-system get pods
	-$(KUBECTL) get agentproviderclass
	-$(KUBECTL) get agentrun -A

.PHONY: logs
logs: ## follow the controller logs
	$(KUBECTL) -n aei-system logs deploy/aei-controller -f
