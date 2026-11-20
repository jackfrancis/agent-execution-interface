#!/usr/bin/env bash
# authority.sh manages AEI's platform Ed25519 run-credential signing key (docs/
# design.md, Contract 3). `make authority` runs it; `make deploy` and `make dev-up`
# run its two steps around applying the controller:
#
#   authority.sh provision   create the key in the aei-signing secret, or reuse it
#   authority.sh activate    roll the controller onto that key and wait for it
#   authority.sh             both (the default)
#   authority.sh public-key  print the verification key, read-only (make public-key)
#
# provision never replaces a key. Verify-only apps (e.g. gofer's web tier) hold the
# public half from their own deploy, so a silently swapped key would make them
# reject every new run credential. Hence:
#   * Only a NotFound read means "no secret". Any other read failure stops.
#   * A secret without a key is filled by a merge patch carrying the resourceVersion
#     it was read at, so a concurrent writer turns the patch into a Conflict instead
#     of being overwritten. On Conflict (or AlreadyExists for a create) it re-reads,
#     and a key someone else just wrote is reused.
#
# activate first settles which public key to advertise: it DERIVES it from the
# secret's private key (the key the controller signs with), so an install whose
# secret holds only AEI_ED25519_PRIVATE_KEY (which the old Deployment documented)
# gets the field backfilled by a conditional patch, and a stale
# AEI_ED25519_PUBLIC_KEY that would not verify the controller's credentials is
# rejected, never announced. An immutable secret cannot take the backfill, so its
# derived key is used as is. `public-key` runs the same derivation and check
# without writing anything. activate then records that key on the pod template (annotation
# agents.x-k8s.io/signing-public-key). A template without that exact key is patched,
# which rolls the controller; either way it waits for the rollout to finish before
# calling the key active. So a run interrupted after provisioning (a failed patch,
# a rollout that timed out) finishes activation on the next run instead of
# reporting success while the old pod still signs with something else. With no
# controller yet, there is nothing to roll: it starts with the key.
#
# Env: KUBECTL (command plus flags, word-split; default "kubectl"), GENKEY (default
# "go run ./hack/genkey"), DERIVE (reads a base64 private key on stdin and prints its
# public key; default "go run ./hack/genkey -public-from-private"), ROLLOUT_TIMEOUT
# (default 180s).
set -uo pipefail

KUBECTL=${KUBECTL:-kubectl}
GENKEY=${GENKEY:-go run ./hack/genkey}
DERIVE=${DERIVE:-go run ./hack/genkey -public-from-private}
ROLLOUT_TIMEOUT=${ROLLOUT_TIMEOUT:-180s}
NS=aei-system
NAME=aei-signing
DEPLOY=deploy/aei-controller
ANNOTATION=agents.x-k8s.io/signing-public-key
ANNOTATION_JSONPATH='{.spec.template.metadata.annotations.agents\.x-k8s\.io/signing-public-key}'
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

provision() {
	local attempt out rv have
	for attempt in $(seq 1 "$ATTEMPTS"); do
		if out=$(k get secret "$NAME" -o 'jsonpath={.metadata.resourceVersion} {.data.AEI_ED25519_PRIVATE_KEY}' 2>"$errf"); then
			rv=${out%% *}
			have=${out#* }
			if [ -n "$have" ]; then
				echo "platform Ed25519 authority already provisioned; reusing it"
				return 0
			fi
			[ -n "$rv" ] || die "secret $NAME has no resourceVersion; refusing an unconditional write"
			gen
			# The private key reaches kubectl on stdin (printf is a builtin), never argv.
			if printf '{"metadata":{"resourceVersion":"%s"},"stringData":{"AEI_ED25519_PRIVATE_KEY":"%s","AEI_ED25519_PUBLIC_KEY":"%s"}}' \
				"$rv" "$priv" "$pub" | k patch secret "$NAME" --type=merge --patch-file=/dev/stdin >/dev/null 2>"$errf"; then
				echo "generated a platform Ed25519 keypair into secret $NAME"
				return 0
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
				echo "generated a platform Ed25519 keypair into new secret $NAME"
				return 0
			fi
			# The API's AlreadyExists reaches us as kubectl's client message:
			#   error: failed to create secret secrets "aei-signing" already exists
			grep -q '(AlreadyExists)\|already exists' "$errf" ||
				die "cannot create secret $NAME: $(lasterr)"
			echo "secret $NAME appeared while provisioning (attempt $attempt); re-reading"
		fi
	done
	die "secret $NAME kept changing; gave up after $ATTEMPTS attempts"
}

# verified_key sets $key to the public key that verifies the controller's
# credentials: derived from the secret's private key and checked against a present
# AEI_ED25519_PUBLIC_KEY. With mode "backfill" a missing field is written into a
# mutable secret; with mode "readonly", or for an immutable secret, nothing is
# written. Progress goes to stderr, so `public-key` prints only the key.
key=""
verified_key() {
	local mode=$1 attempt out rv immutable privb64 pubb64 derived stored
	for attempt in $(seq 1 "$ATTEMPTS"); do
		# '|' separates the fields (base64 never contains it), so an empty field
		# cannot shift the others.
		if ! out=$(k get secret "$NAME" -o 'jsonpath={.metadata.resourceVersion}|{.immutable}|{.data.AEI_ED25519_PRIVATE_KEY}|{.data.AEI_ED25519_PUBLIC_KEY}' 2>"$errf"); then
			grep -q '(NotFound)' "$errf" && die "no Ed25519 authority provisioned — run: make authority"
			die "cannot read secret $NAME: $(lasterr)"
		fi
		IFS='|' read -r rv immutable privb64 pubb64 <<<"$out"
		[ -n "$privb64" ] || die "no Ed25519 authority provisioned (secret $NAME has no AEI_ED25519_PRIVATE_KEY) — run: make authority"
		# The private key travels only through pipes, never argv.
		derived=$(printf '%s' "$privb64" | base64 -d | $DERIVE 2>"$errf") && [ -n "$derived" ] ||
			die "AEI_ED25519_PRIVATE_KEY in secret $NAME is unusable, and the controller will refuse it too: $(lasterr)"
		stored=$(printf '%s' "$pubb64" | base64 -d 2>/dev/null)
		if [ -n "$stored" ]; then
			[ "$stored" = "$derived" ] ||
				die "AEI_ED25519_PUBLIC_KEY in secret $NAME does not match its private key, so it cannot verify the controller's credentials; refusing to advertise it. Remove that field (recreate the secret if it is immutable) and re-run make authority to derive it."
			key=$derived
			return 0
		fi
		if [ "$mode" = readonly ]; then
			key=$derived
			return 0
		fi
		if [ "$immutable" = true ]; then
			echo "secret $NAME is immutable and has no AEI_ED25519_PUBLIC_KEY; using the key derived from its private key without saving it" >&2
			key=$derived
			return 0
		fi
		echo "secret $NAME has no AEI_ED25519_PUBLIC_KEY; deriving it from the private key" >&2
		[ -n "$rv" ] || die "secret $NAME has no resourceVersion; refusing an unconditional write"
		if printf '{"metadata":{"resourceVersion":"%s"},"stringData":{"AEI_ED25519_PUBLIC_KEY":"%s"}}' "$rv" "$derived" |
			k patch secret "$NAME" --type=merge --patch-file=/dev/stdin >/dev/null 2>"$errf"; then
			key=$derived
			return 0
		fi
		grep -q '(Conflict)\|the object has been modified' "$errf" ||
			die "cannot add the public key to secret $NAME: $(lasterr)"
		echo "secret $NAME changed while backfilling (attempt $attempt); re-reading" >&2
	done
	die "secret $NAME kept changing; gave up after $ATTEMPTS attempts"
}

activate() {
	local cur
	verified_key backfill
	if ! cur=$(k get "$DEPLOY" -o "jsonpath=$ANNOTATION_JSONPATH" 2>"$errf"); then
		grep -q '(NotFound)' "$errf" ||
			die "cannot read $DEPLOY, so the key is NOT known to be active: $(lasterr)"
		echo "no controller yet; it starts with this key. app verify key (also at GET /aei/v1alpha1/authority once it is up):"
		echo "  $key"
		return 0
	fi
	if [ "$cur" != "$key" ]; then
		echo "rolling the controller onto the signing key..."
		printf '{"spec":{"template":{"metadata":{"annotations":{"%s":"%s"}}}}}' "$ANNOTATION" "$key" |
			k patch "$DEPLOY" --type=merge --patch-file=/dev/stdin >/dev/null 2>"$errf" ||
			die "key NOT active: cannot roll the controller onto it: $(lasterr)"
	fi
	k rollout status "$DEPLOY" --timeout="$ROLLOUT_TIMEOUT" ||
		die "key NOT active: the controller did not become ready on it (check: kubectl -n $NS get pods)"
	echo "signing key active. app verify key (also at GET /aei/v1alpha1/authority):"
	echo "  $key"
}

case "${1:-all}" in
provision) provision ;;
activate) activate ;;
public-key)
	verified_key readonly
	echo "$key"
	;;
all) provision && activate ;;
*) die "usage: $0 [provision|activate|all|public-key]" ;;
esac
