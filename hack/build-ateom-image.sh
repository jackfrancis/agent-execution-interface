#!/usr/bin/env bash
# Build and push the substrate "ateom-gvisor" runtime image (the image an ate
# WorkerPool runs) using the substrate's own ko toolchain, and print its pushed
# digest reference (e.g. localhost:5001/ateom-gvisor-<hash>@sha256:...) to stdout.
#
# It exists so the durable-substrate prep is reproducible from `make`
# (agent-substrate-enable calls this), rather than a manual ko invocation: AEI puts
# the printed digest on the durable AgentProviderClass's `ateomImage` parameter so
# the controller can materialize each app's WorkerPool.
#
# Progress/logs go to STDERR; ONLY the image ref goes to STDOUT so a caller can
# capture it with `ateom=$(hack/build-ateom-image.sh)`.
#
# Requires the substrate clone from `make install-agent-substrate`
# (hack/.cache/substrate) and a working Go toolchain. In a real deployment you would
# instead set `ateomImage` on the durable class to your platform's own ateom image.
set -o errexit -o nounset -o pipefail

SUBSTRATE_REGISTRY="${SUBSTRATE_REGISTRY:-localhost:5001}"
CACHE_DIR="${CACHE_DIR:-hack/.cache/substrate}"

if [ ! -d "${CACHE_DIR}" ]; then
	echo "ERROR: substrate clone not found at ${CACHE_DIR}." >&2
	echo "       Run 'make install-agent-substrate' first (it clones + builds substrate)." >&2
	exit 1
fi

cd "${CACHE_DIR}"

export KO_DOCKER_REPO="${SUBSTRATE_REGISTRY}"
export KO_DEFAULTPLATFORMS="linux/$(go env GOARCH)"

echo "==> building ateom-gvisor via ko -> ${KO_DOCKER_REPO} (${KO_DEFAULTPLATFORMS})" >&2

# Mirror substrate's run_ko: pass the version ldflags from `make ldflags`.
ldflags_args=()
while IFS= read -r f; do
	[ -n "${f}" ] && ldflags_args+=("--ldflags=${f}")
done < <(make ldflags 2>/dev/null || true)

# ko writes logs to stderr and the pushed ref to stdout. Filter for the registry-
# qualified digest ref defensively, in case a log line leaks onto stdout.
ref="$(./hack/run-tool.sh ko build ./cmd/ateom-gvisor "${ldflags_args[@]}" \
	| grep -E "^${SUBSTRATE_REGISTRY}/.*@sha256:" | tail -1)"

if [ -z "${ref}" ]; then
	echo "ERROR: could not determine the pushed ateom-gvisor image ref." >&2
	exit 1
fi

echo "${ref}"
