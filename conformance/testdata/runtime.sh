#!/bin/sh
# A reference AEI-conformant runtime in POSIX sh + curl — no SDK, no language
# binding, only the wire (docs/runtime-abi.md). If the conformance harness drives
# this to a green result, the ABI is genuinely language-agnostic: any executable
# that reads the AEI_* environment and speaks the /redeem and /complete JSON
# routes is a conformant runtime, with or without a Go SDK.
set -eu

control="${AEI_CONTROL_URL:?AEI_CONTROL_URL required}"
broker="${AEI_BROKER_URL:-$control}"   # B-with-default: broker collapses onto control
run_id="${AEI_RUN_ID:?AEI_RUN_ID required}"

# Resolve the run credential: a pushed AEI_RUN_CREDENTIAL is used as-is; otherwise
# an AEI_RUN_TICKET is redeemed for one (the pull path). Exactly one is present.
cred="${AEI_RUN_CREDENTIAL:-}"
if [ -z "$cred" ] && [ -n "${AEI_RUN_TICKET:-}" ]; then
	# The ticket is the authorization, so this request carries no bearer.
	resp=$(curl -sS -X POST "$broker/redeem" \
		-H 'Content-Type: application/json' \
		-d "{\"ticket\":\"${AEI_RUN_TICKET}\"}")
	cred=$(printf '%s' "$resp" | sed -n 's/.*"credential"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p')
fi

# Report terminal completion (best-effort 202), authorized by the run credential.
# run_id is echoed explicitly; the platform dedupes against its watch/deadline.
curl -sS -X POST "$control/complete" \
	-H 'Content-Type: application/json' \
	-H "Authorization: Bearer ${cred}" \
	-d "{\"run_id\":\"${run_id}\",\"timing\":{\"runtime_seconds\":0.01}}" \
	>/dev/null
