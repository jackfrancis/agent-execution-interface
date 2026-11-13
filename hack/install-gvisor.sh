#!/usr/bin/env bash
# Install gVisor (runsc) on an AEI kind cluster and register it as a RuntimeClass,
# so a provider class with `defaultIsolationClass: gvisor` has something real to
# resolve to.
#
# WHY this is needed: AEI resolves a run's isolationClass to a pod runtimeClassName
# (docs/design.md, Isolation). Kubernetes then routes that pod to the matching
# containerd runtime handler. A stock kind node ships neither runsc nor a gvisor
# RuntimeClass, so without this the pod-shaped providers refuse to dispatch — by
# design, because the alternative is running unisolated while claiming isolation.
#
# NOTE Agent Substrate does NOT provide this. It ships its own runsc and invokes it
# from its ateom worker; it is not registered as a containerd runtime handler, so it
# does nothing for the agent-sandbox or k8s-job providers.
#
# Per node it: installs runsc + containerd-shim-runsc-v1, appends a runsc runtime to
# containerd's config (matching that node's config schema version), and restarts
# containerd. Then it applies the RuntimeClass. Idempotent — safe to re-run, and it
# works on an already-running cluster (no recreation needed).
#
# Requires: curl, an engine (docker/podman), kind, kubectl. Downloads binaries from
# storage.googleapis.com/gvisor and verifies their published SHA-512.
set -o errexit -o nounset -o pipefail

KIND_CLUSTER="${KIND_CLUSTER:-aei}"
KIND="${KIND:-kind}"
ENGINE="${ENGINE:-$(command -v docker >/dev/null 2>&1 && echo docker || echo podman)}"
# A gVisor release point, as it appears under storage.googleapis.com/gvisor/releases/:
# "release/latest", or a dated pin like "release/20260101".
GVISOR_RELEASE="${GVISOR_RELEASE:-release/latest}"
RUNTIME_CLASS="${RUNTIME_CLASS:-gvisor}"
CTX="kind-${KIND_CLUSTER}"

echo "==> gvisor: release=${GVISOR_RELEASE} cluster=${KIND_CLUSTER} engine=${ENGINE}"

for t in curl "${ENGINE}" "${KIND}" kubectl; do
	command -v "${t}" >/dev/null 2>&1 || { echo "ERROR: required tool not found: ${t}"; exit 1; }
done

NODES="$(${KIND} get nodes --name "${KIND_CLUSTER}")"
[ -n "${NODES}" ] || { echo "ERROR: no nodes for kind cluster '${KIND_CLUSTER}'"; exit 1; }

# gVisor publishes per-architecture builds; match the nodes, not the host (on a
# macOS podman/docker VM those differ from the shell's uname).
NODE_ARCH="$(${ENGINE} exec "$(echo "${NODES}" | head -1)" uname -m)"
case "${NODE_ARCH}" in
	x86_64|aarch64) ;;
	*) echo "ERROR: unsupported node architecture for gVisor: ${NODE_ARCH}"; exit 1 ;;
esac

WORK="$(mktemp -d)"
trap 'rm -rf "${WORK}"' EXIT
URL="https://storage.googleapis.com/gvisor/releases/${GVISOR_RELEASE}/${NODE_ARCH}"

echo "==> downloading runsc + containerd-shim-runsc-v1 (${NODE_ARCH})"
(
	cd "${WORK}"
	curl -fsSL -O "${URL}/runsc" -O "${URL}/runsc.sha512" \
		-O "${URL}/containerd-shim-runsc-v1" -O "${URL}/containerd-shim-runsc-v1.sha512"
	# Verify the publisher's checksums before anything is installed on a node.
	if command -v sha512sum >/dev/null 2>&1; then
		sha512sum -c runsc.sha512 containerd-shim-runsc-v1.sha512
	else
		shasum -a 512 -c runsc.sha512 containerd-shim-runsc-v1.sha512
	fi
	chmod 0755 runsc containerd-shim-runsc-v1
)

for node in ${NODES}; do
	echo "==> installing runsc on ${node}"
	${ENGINE} cp "${WORK}/runsc" "${node}:/usr/local/bin/runsc"
	${ENGINE} cp "${WORK}/containerd-shim-runsc-v1" "${node}:/usr/local/bin/containerd-shim-runsc-v1"

	# Register the handler with containerd. Two ways this goes wrong quietly, so
	# both are checked rather than assumed:
	#   * the plugin path differs by config schema version (v3 renamed the CRI
	#     plugin), and a section under the wrong name is parsed as an unknown plugin
	#     and ignored — leaving pods on the default runtime;
	#   * `exec` needs -i or the heredoc never reaches bash, which then exits 0
	#     having done nothing at all.
	# So the node script verifies containerd's own loaded config afterwards and
	# prints a sentinel, and the sentinel is required here.
	out="$(${ENGINE} exec -i "${node}" bash -s -- "${RUNTIME_CLASS}" <<'NODE'
set -o errexit -o nounset -o pipefail
handler="runsc"
conf=/etc/containerd/config.toml
if grep -q "runtimes\.${handler}\]" "${conf}"; then
	echo "containerd already has a ${handler} runtime"
else
	if grep -qE '^version[[:space:]]*=[[:space:]]*3' "${conf}"; then
		plugin='io.containerd.cri.v1.runtime'
	else
		plugin='io.containerd.grpc.v1.cri'
	fi
	cat >>"${conf}" <<EOF

[plugins."${plugin}".containerd.runtimes.${handler}]
  runtime_type = "io.containerd.runsc.v1"
EOF
	echo "added ${handler} runtime under ${plugin}"
fi
systemctl restart containerd
# Ask containerd what it actually loaded; the file being right is not proof.
# Capture before matching: `... | grep -q` under `pipefail` reports FAILURE on a
# match, because grep exits first and the producer dies of SIGPIPE.
for i in $(seq 1 30); do
	dump="$(containerd config dump 2>/dev/null || true)"
	case "${dump}" in
		(*"runtimes.${handler}]"*)
			echo "runsc-configured"
			exit 0
			;;
	esac
	sleep 1
done
echo "ERROR: containerd did not load a ${handler} runtime after restart" >&2
exit 1
NODE
)" || { echo "${out}"; echo "ERROR: configuring containerd on ${node} failed"; exit 1; }
	echo "${out}" | sed 's/^/    /'
	case "${out}" in
		*runsc-configured*) ;;
		*) echo "ERROR: containerd on ${node} was not configured (the node script did not run)"; exit 1 ;;
	esac
done

echo "==> registering RuntimeClass '${RUNTIME_CLASS}'"
# Restarting containerd blips each kubelet's CRI connection, so let the nodes settle
# before driving the API server.
kubectl --context "${CTX}" wait --for=condition=Ready nodes --all --timeout=180s
kubectl --context "${CTX}" apply -f - <<EOF
apiVersion: node.k8s.io/v1
kind: RuntimeClass
metadata:
  name: ${RUNTIME_CLASS}
handler: runsc
EOF

echo "==> verifying with a smoke pod"
kubectl --context "${CTX}" delete pod gvisor-smoke --ignore-not-found >/dev/null 2>&1 || true
kubectl --context "${CTX}" apply -f - <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: gvisor-smoke
spec:
  runtimeClassName: ${RUNTIME_CLASS}
  restartPolicy: Never
  containers:
    - name: smoke
      image: busybox:1.37
      command: ["dmesg"]
EOF
if kubectl --context "${CTX}" wait --for=jsonpath='{.status.phase}'=Succeeded pod/gvisor-smoke --timeout=120s >/dev/null 2>&1; then
	# gVisor announces itself in the guest kernel banner ("Starting gVisor..."); the
	# default runtime shows the host's real dmesg. Capture the log and match it as a
	# string — piping into `grep -q` under `pipefail` would report failure on a match.
	SMOKE_LOG="$(kubectl --context "${CTX}" logs gvisor-smoke 2>/dev/null || true)"
	case "${SMOKE_LOG}" in
		*gVisor*|*gvisor*)
			echo "==> gVisor is serving pods (RuntimeClass ${RUNTIME_CLASS} -> runsc)"
			;;
		*)
			echo "WARNING: the smoke pod ran but did not report a gVisor kernel; check the containerd config"
			echo "${SMOKE_LOG}" | head -5
			;;
	esac
else
	echo "ERROR: smoke pod did not run under ${RUNTIME_CLASS}; inspect: kubectl --context ${CTX} describe pod gvisor-smoke"
	exit 1
fi
kubectl --context "${CTX}" delete pod gvisor-smoke --ignore-not-found >/dev/null 2>&1 || true
