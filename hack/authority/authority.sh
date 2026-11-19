#!/usr/bin/env bash
# authority.sh provisions AEI's platform Ed25519 run-credential signing keypair in
# the aei-signing secret, or reuses the one already there (`make authority` runs it;
# docs/design.md, Contract 3). The controller refuses to start without a key, so
# `make deploy` and `make dev-up` run this before they apply the controller.
#
# It never replaces a key. Verify-only apps (e.g. gofer's web tier) hold the public
# half from their own deploy, so a silently swapped key would make them reject every
# new run credential. Hence:
#   * Only a NotFound read means "no secret". Any other read failure stops.
#   * A secret without a key is filled by a merge patch carrying the resourceVersion
#     it was read at, so a concurrent writer turns the patch into a Conflict instead
#     of being overwritten; on Conflict (or AlreadyExists for a create) it re-reads,
#     and a key someone else just wrote is reused.
#   * When it creates a key for a controller that is already running, it restarts
#     the controller and waits for the rollout before calling the key active.
#
# Env: KUBECTL (command plus flags, word-split; default "kubectl"), GENKEY (default
# "go run ./hack/genkey"), ROLLOUT_TIMEOUT (default 180s).
set -uo pipefail

KUBECTL=${KUBECTL:-kubectl}
GENKEY=${GENKEY:-go run ./hack/genkey}
ROLLOUT_TIMEOUT=${ROLLOUT_TIMEOUT:-180s}
NS=aei-system
NAME=aei-signing
DEPLOY=deploy/aei-controller
ATTEMPTS=5

# KUBECTL is deliberately unquoted: it carries --context/--kubeconfig flags.
k() { $KUBECTL -n "$NS" "$@"; }
die() { echo "authority: $*" >&2; exit 1; }

errf=$(mktemp) || die "mktemp failed"
trap 'rm -f -- "$errf"' EXIT
lasterr() { tr '\n' ' ' <"$errf"; }

priv=""
pub=""
gen() {
	local keys
	keys=$($GENKEY) || die "key generation failed"
	priv=$(printf '%s\n' "$keys" | sed -n 's/^AEI_ED25519_PRIVATE_KEY=//p')
	pub=$(printf '%s\n' "$keys" | sed -n 's/^AEI_ED25519_PUBLIC_KEY=//p')
	[ -n "$priv" ] && [ -n "$pub" ] || die "key generation printed no keypair"
}

provisioned=""
for attempt in $(seq 1 "$ATTEMPTS"); do
	if out=$(k get secret "$NAME" -o 'jsonpath={.metadata.resourceVersion} {.data.AEI_ED25519_PRIVATE_KEY}' 2>"$errf"); then
		rv=${out%% *}
		have=${out#* }
		if [ -n "$have" ]; then
			echo "platform Ed25519 authority already provisioned; reusing it (make public-key to read it)"
			exit 0
		fi
		[ -n "$rv" ] || die "secret $NAME has no resourceVersion; refusing an unconditional write"
		gen
		# The private key reaches kubectl on stdin (printf is a builtin), never argv.
		if printf '{"metadata":{"resourceVersion":"%s"},"stringData":{"AEI_ED25519_PRIVATE_KEY":"%s","AEI_ED25519_PUBLIC_KEY":"%s"}}' \
			"$rv" "$priv" "$pub" | k patch secret "$NAME" --type=merge --patch-file=/dev/stdin >/dev/null 2>"$errf"; then
			provisioned=1
			break
		fi
		grep -q '(Conflict)\|the object has been modified' "$errf" ||
			die "cannot add the key to secret $NAME: $(lasterr)"
		echo "secret $NAME changed while provisioning (attempt $attempt); re-reading"
	else
		grep -q '(NotFound)' "$errf" ||
			die "cannot read secret $NAME, so not treating it as missing: $(lasterr)"
		gen
		if printf 'AEI_ED25519_PRIVATE_KEY=%s\nAEI_ED25519_PUBLIC_KEY=%s\n' "$priv" "$pub" |
			k create secret generic "$NAME" --from-env-file=/dev/stdin >/dev/null 2>"$errf"; then
			provisioned=1
			break
		fi
		grep -q '(AlreadyExists)' "$errf" ||
			die "cannot create secret $NAME: $(lasterr)"
		echo "secret $NAME appeared while provisioning (attempt $attempt); re-reading"
	fi
done
[ -n "$provisioned" ] || die "secret $NAME kept changing; gave up after $ATTEMPTS attempts"

if k get "$DEPLOY" >/dev/null 2>"$errf"; then
	echo "rolling the controller onto the new key..."
	k rollout restart "$DEPLOY" >/dev/null 2>"$errf" ||
		die "key provisioned but NOT active: restarting the controller failed: $(lasterr)"
	k rollout status "$DEPLOY" --timeout="$ROLLOUT_TIMEOUT" ||
		die "key provisioned but NOT active: the controller did not become ready on it (check: kubectl -n $NS get pods)"
	echo "provisioned and active. app verify key (also at GET /aei/v1alpha1/authority):"
else
	grep -q '(NotFound)' "$errf" ||
		die "key provisioned, but cannot read $DEPLOY to roll it: $(lasterr)"
	echo "provisioned; the controller starts with it. app verify key (also at GET /aei/v1alpha1/authority once it is up):"
fi
echo "  $pub"
