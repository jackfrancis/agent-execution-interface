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

.PHONY: help docker-build kind-load deploy undeploy manifests

help: ## list targets
	@grep -hE '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*?## "}{printf "  %-16s %s\n", $$1, $$2}'

docker-build: ## build the manager image (context = repo root)
	cd .. && $(ENGINE) build -f controller/Dockerfile -t $(IMG) .

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
