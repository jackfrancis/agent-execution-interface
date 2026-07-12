# Minimal build/deploy for the AEI reference operator. The image builds from the
# repo root (the controller module's replace directives point at ../ and
# ../providers); everything else is kubectl + kustomize.
IMG ?= localhost/aei-manager:dev
ENGINE ?= docker            # podman works too: make ENGINE=podman docker-build
KIND_CLUSTER ?= aei

.PHONY: help docker-build kind-load deploy undeploy manifests

help: ## list targets
	@grep -hE '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*?## "}{printf "  %-16s %s\n", $$1, $$2}'

docker-build: ## build the manager image (context = repo root)
	cd .. && $(ENGINE) build -f controller/Dockerfile -t $(IMG) .

kind-load: ## load the image into a kind cluster
	kind load docker-image $(IMG) --name $(KIND_CLUSTER)

deploy: ## install CRDs + RBAC + manager (Deployment/Service/Secret)
	kubectl apply -k config/default

undeploy: ## remove the operator
	kubectl delete -k config/default --ignore-not-found

manifests: ## render the full install to stdout
	kubectl kustomize config/default
