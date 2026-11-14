# Minimal build/deploy for the AEI reference operator. The image builds from the
# repo root (the controller module's replace directives point at ../ and
# ../providers); everything else is kubectl + kustomize.
#
# KUBECTL/KIND are overridable so the repo-root Makefile can pin every command to
# a specific cluster/context (e.g. KUBECTL="kubectl --context kind-aei").
IMG ?= localhost/aei-manager:dev
# Build engine: docker, or podman when docker is absent.
ENGINE ?= $(shell command -v docker >/dev/null 2>&1 && echo docker || echo podman)
KIND_CLUSTER ?= aei
KIND ?= kind
KUBECTL ?= kubectl
# Temp tarball for the engine-agnostic image load (see kind-load).
IMAGE_ARCHIVE ?= /tmp/aei-manager-$(KIND_CLUSTER).tar

.PHONY: help docker-build kind-load deploy undeploy manifests generate verify-generate

help: ## list targets
	@grep -hE '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*?## "}{printf "  %-16s %s\n", $$1, $$2}'

# PLATFORM (e.g. linux/amd64) cross-builds for a cluster whose nodes differ from this
# machine; empty builds for the engine's own platform.
PLATFORM ?=

docker-build: ## build the manager image (context = repo root)
	cd .. && $(ENGINE) build $(if $(PLATFORM),--platform $(PLATFORM)) -f controller/Dockerfile -t $(IMG) .

# `kind load docker-image` is unreliable with the podman provider on macOS (the
# podman machine is a remote VM, so the image kind looks for is "not present
# locally"). Load via an explicit archive instead: `<engine> save` to a tar, then
# `kind load image-archive`. This path works for both docker and podman.
kind-load: ## load the image into the kind cluster (engine-agnostic archive load)
	$(ENGINE) save $(IMG) -o $(IMAGE_ARCHIVE)
	$(KIND) load image-archive $(IMAGE_ARCHIVE) --name $(KIND_CLUSTER)
	@rm -f $(IMAGE_ARCHIVE)

deploy: ## install CRDs + RBAC + manager (Deployment/Service/Secret)
	$(KUBECTL) apply -k config/default

undeploy: ## remove the operator
	$(KUBECTL) delete -k config/default --ignore-not-found

manifests: ## render the full install to stdout
	$(KUBECTL) kustomize config/default

# --- Code generation ---------------------------------------------------------
# controller-gen is pinned here and run with "go run", so nothing is installed and
# the generator version is part of the repository. controller-tools releases track
# the k8s.io/* minor (v0.23 is built against k8s.io v0.37), so move this pin with
# the k8s.io/* requirement in go.mod.
CONTROLLER_GEN_VERSION ?= v0.23.0
CONTROLLER_GEN ?= go run sigs.k8s.io/controller-tools/cmd/controller-gen@$(CONTROLLER_GEN_VERSION)
# Two choices are deliberate:
#   * crd:allowDangerousTypes=true: AgentRun.status.timing carries float64 seconds,
#     and controller-gen refuses to emit a CRD with floats without it.
#   * no rbac generator: config/rbac/role.yaml is hand-maintained. Its batch/jobs and
#     node.k8s.io/runtimeclasses rules have no +kubebuilder:rbac marker, so generating
#     it would drop permissions the in-process providers need.
CRD_GEN = crd:allowDangerousTypes=true paths=./api/...

# The generated CRDs are removed first: controller-gen only writes files, so a CRD
# whose kind was removed or renamed would otherwise stay behind.
generate: ## regenerate api/v1alpha1/zz_generated.deepcopy.go and config/crd from the Go types
	$(CONTROLLER_GEN) object paths=./api/...
	rm -f config/crd/agents.x-k8s.io_*.yaml
	$(CONTROLLER_GEN) $(CRD_GEN) output:crd:dir=config/crd

# Generates into a temporary directory and compares, so it changes nothing and gives
# the same answer in a dirty tree, a clean one, or an exported copy without .git.
verify-generate: ## fail if the committed deepcopy code or CRDs differ from what the Go types generate
	@tmp=$$(mktemp -d) && trap 'rm -rf "$${tmp:?}"' EXIT && \
	$(CONTROLLER_GEN) object paths=./api/... output:object:dir=$$tmp/object && \
	$(CONTROLLER_GEN) $(CRD_GEN) output:crd:dir=$$tmp/crd && \
	stale=0 && \
	{ diff -u api/v1alpha1/zz_generated.deepcopy.go $$tmp/object/zz_generated.deepcopy.go || stale=1; } && \
	for f in $$tmp/crd/*.yaml; do diff -u config/crd/$$(basename $$f) $$f || stale=1; done && \
	for f in config/crd/agents.x-k8s.io_*.yaml; do [ -f $$tmp/crd/$$(basename $$f) ] || { echo "no longer generated: $$f"; stale=1; }; done && \
	if [ $$stale = 0 ]; then echo "generated files are up to date"; \
	else echo "generated files are stale: run 'make -C controller generate' and commit the result"; exit 1; fi
