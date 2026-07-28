#!/usr/bin/env bash
# Install upstream Agent Substrate (github.com/agent-substrate/substrate, "ate") on
# the current AEI kind cluster, so the agent-substrate provider's "durable" class has
# a substrate to dispatch to.
#
# Unlike agent-sandbox (a single published manifest), substrate installs FROM SOURCE:
# this clones the repo, stands up the local image registry its ko-based build pushes
# to (the registry bits of substrate's own create-kind-cluster.sh, minus recreating
# the cluster — ours is already substrate-compatible), then runs their
# hack/install-ate-kind.sh against our cluster.
#
# Heavy + upstream-tracking (AGENT_SUBSTRATE_REF defaults to main). Substrate is very
# early and its scripts assume docker; on a podman host, expect breakage. Requires:
# git, go, an engine (docker/podman), kind, kubectl.
set -o errexit -o nounset -o pipefail

REPO="${AGENT_SUBSTRATE_REPO:-https://github.com/agent-substrate/substrate}"
REF="${AGENT_SUBSTRATE_REF:-main}"
KIND_CLUSTER="${KIND_CLUSTER:-aei}"
KIND="${KIND:-kind}"
ENGINE="${ENGINE:-$(command -v docker >/dev/null 2>&1 && echo docker || echo podman)}"
CACHE_DIR="${CACHE_DIR:-hack/.cache/substrate}"
REG_NAME="${REG_NAME:-kind-registry}"
REG_PORT="${REG_PORT:-5001}"
CTX="kind-${KIND_CLUSTER}"

echo "==> agent-substrate: ref=${REF} cluster=${KIND_CLUSTER} engine=${ENGINE}"

# 0. tool preflight
for t in git go "${ENGINE}" "${KIND}" kubectl; do
	command -v "${t}" >/dev/null 2>&1 || { echo "ERROR: required tool not found: ${t}"; exit 1; }
done

# 1. clone/update the substrate repo (cached under hack/.cache, gitignored).
if [ -d "${CACHE_DIR}/.git" ]; then
	echo "==> updating ${CACHE_DIR} to ${REF}"
	git -C "${CACHE_DIR}" fetch --quiet origin
else
	echo "==> cloning ${REPO} into ${CACHE_DIR}"
	git clone --quiet "${REPO}" "${CACHE_DIR}"
fi
git -C "${CACHE_DIR}" checkout --quiet "${REF}"
git -C "${CACHE_DIR}" pull --quiet --ff-only origin "${REF}" 2>/dev/null || true

# 2. Ensure the local image registry substrate's ko build pushes to (localhost:PORT),
#    wired into the kind cluster. Engine-agnostic (their script is docker-only). We do
#    NOT recreate the cluster — ours already has substrate's feature gates. This block
#    is idempotent and non-destructive: a re-run never deletes the registry (it holds
#    the ko-built ate fleet + ateom + gofer-runtime that the nodes pull by digest and
#    that survive cluster loss), it only creates/starts it as needed.
echo "==> ensuring local registry '${REG_NAME}' on :${REG_PORT}"
if ! ${ENGINE} container inspect "${REG_NAME}" >/dev/null 2>&1; then
	# Absent → create. The named volume keeps pushed images across container recreation
	# (and cluster loss), so a re-run never rebuilds them; --restart=always keeps it up.
	${ENGINE} run -d --restart=always -p "127.0.0.1:${REG_PORT}:5000" \
		-v "${REG_NAME}-data:/var/lib/registry" \
		--name "${REG_NAME}" docker.io/library/registry:3
elif [ "$(${ENGINE} inspect -f '{{.State.Running}}' "${REG_NAME}")" != "true" ]; then
	# Exists but stopped → start it (never `run` a new one; the name would conflict).
	${ENGINE} start "${REG_NAME}"
fi
${ENGINE} network connect kind "${REG_NAME}" 2>/dev/null || true

echo "==> wiring kind nodes' containerd to the registry"
REG_DIR="/etc/containerd/certs.d/localhost:${REG_PORT}"
for node in $(${KIND} get nodes --name "${KIND_CLUSTER}"); do
	${ENGINE} exec "${node}" mkdir -p "${REG_DIR}"
	printf '[host."http://%s:5000"]\n' "${REG_NAME}" | ${ENGINE} exec -i "${node}" cp /dev/stdin "${REG_DIR}/hosts.toml"
done

echo "==> documenting the registry (local-registry-hosting ConfigMap)"
kubectl --context "${CTX}" apply -f - <<EOF
apiVersion: v1
kind: ConfigMap
metadata:
  name: local-registry-hosting
  namespace: kube-public
data:
  localRegistryHosting.v1: |
    host: "localhost:${REG_PORT}"
    help: "https://kind.sigs.k8s.io/docs/user/local-registry/"
EOF

# 2b. Expose the registry in-cluster so the aei-controller POD can read an app image's
#     entrypoint when provisioning a durable actor. The kind NODES reach the registry
#     via containerd host rewriting, but a pod cannot; a Service backed by the registry
#     container's kind-network IP gives the controller a stable in-cluster address. It
#     is paired with the aei-controller's AEI_REGISTRY_REWRITE (set by
#     `make agent-substrate-enable`). Not needed in a real cluster, where the image
#     ref's own registry is reachable from the controller.
echo "==> exposing '${REG_NAME}' in-cluster (Service/Endpoints for the aei-controller)"
REG_IP="$(${ENGINE} inspect -f '{{(index .NetworkSettings.Networks "kind").IPAddress}}' "${REG_NAME}" 2>/dev/null || true)"
if [ -n "${REG_IP}" ]; then
	kubectl --context "${CTX}" apply -f - <<EOF
apiVersion: v1
kind: Service
metadata:
  name: ${REG_NAME}
  namespace: default
spec:
  ports:
  - name: registry
    port: 5000
    targetPort: 5000
---
apiVersion: v1
kind: Endpoints
metadata:
  name: ${REG_NAME}
  namespace: default
subsets:
- addresses:
  - ip: ${REG_IP}
  ports:
  - name: registry
    port: 5000
EOF
else
	echo "  WARNING: could not read ${REG_NAME}'s kind-network IP; the aei-controller cannot"
	echo "           read image entrypoints for durable actors. If durable provisioning fails"
	echo "           on image-config reads, set AEI_REGISTRY_REWRITE on deploy/aei-controller."
fi

# 3. Run substrate's own installer against OUR cluster. Its scripts use bare `kubectl`,
#    so hand them an isolated kubeconfig pinned to our cluster (never mutate the user's
#    current-context).
echo "==> building + deploying ate-system (substrate hack/install-ate-kind.sh --deploy-ate-system)"
KCFG="$(mktemp)"
trap 'rm -f "${KCFG}"' EXIT
${KIND} get kubeconfig --name "${KIND_CLUSTER}" >"${KCFG}"
( cd "${CACHE_DIR}" && KUBECONFIG="${KCFG}" hack/install-ate-kind.sh --deploy-ate-system )

# 4. The kubectl-ate CLI (a kubectl plugin).
echo "==> installing kubectl-ate CLI (go install ./cmd/kubectl-ate)"
( cd "${CACHE_DIR}" && go install ./cmd/kubectl-ate )

# Tailor the "next steps": only recommend applying the 'durable' class if it isn't
# already installed (the class can be applied before or after the substrate).
CTX="kind-${KIND_CLUSTER}"
if kubectl --context "${CTX}" get agentproviderclass durable >/dev/null 2>&1; then
	class_line="  # 'durable' provider class already installed — verify its params:  make durable-class-hints"
else
	class_line="  make agent-substrate-enable            # apply the AEI-side 'durable' provider class"
fi

cat <<MSG

agent-substrate installed on '${KIND_CLUSTER}'. Next:
${class_line}
  kubectl --context ${CTX} -n ate-system get pods    # watch ate-system come up

Optional (manual smoke, like the counter demo) — reach an actor directly from your laptop.
The gofer/AEI dispatch path does NOT need this: the aei-controller reaches the router in-cluster.
  kubectl --context ${CTX} port-forward -n ate-system svc/atenet-router 8000:80
  # then: curl -X POST -H 'Host: <actor>.<atespace>.actors.resources.substrate.ate.dev' http://localhost:8000

Put your Go bin dir (go env GOBIN, else \$(go env GOPATH)/bin) on PATH for 'kubectl ate'.
MSG
