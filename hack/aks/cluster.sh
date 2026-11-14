#!/usr/bin/env bash
# Create (or delete) an AKS cluster that satisfies AEI. AKS is ONE example of such a
# cluster, the way kind is for the dev loop: AEI itself needs only a kubeconfig, and
# nothing in the standard depends on Azure.
#
#   cluster.sh create   resource group + AKS cluster with two node pools + (optional)
#                       container registry; writes a kubeconfig FILE and prints the
#                       `make push / deploy` commands to run next. Idempotent: every
#                       step is skipped when its resource already exists.
#   cluster.sh delete   remove what `create` made. Prints the plan and stops unless
#                       AKS_CONFIRM_DELETE=<resource group> is set.
#
# WHAT "satisfies AEI" means here: a cluster the controller can run on, a registry
# its nodes can pull from, and a RuntimeClass for the 'isolated' provider class. On
# AKS that RuntimeClass is 'kata-vm-isolation' (Pod Sandboxing: each pod in its own
# Kata VM). AKS ships the RuntimeClass on every cluster, with a node selector of its
# own, so its pods run only on nodes of a pool created with --workload-runtime
# KataVmIsolation (and stay Pending if there is none). Such a pool needs the Azure
# Linux OS SKU and a Generation 2 VM size with nested virtualization
# (https://learn.microsoft.com/azure/aks/use-pod-sandboxing; its examples use the
# Dsv3 series, the default here is the current Dsv7 series, which has both).
#
# The cluster keeps itself current, with the two channels AKS Automatic presets, on
# an ordinary (Standard) cluster: Kubernetes upgrades on the 'stable' channel and
# node OS images on the 'NodeImage' channel. The AKS Automatic SKU itself is not
# used: it manages the node pools for you, which rules out choosing a Kata pool.
#
# So the cluster has two pools: a small system pool (controller, add-ons), and a
# Kata pool for agent runs.
#
# Every setting is an environment variable (make passes its command-line variables
# through), with the defaults below. AKS_DRY_RUN=1 prints the Azure commands instead
# of running them and assumes nothing exists yet, so it shows the full sequence.
#
# Requires: az 2.80.0 or later (for --workload-runtime) and kubectl. Works under
# macOS's bash 3.2.
set -o errexit -o nounset -o pipefail

AKS_RESOURCE_GROUP="${AKS_RESOURCE_GROUP:-aei}"
AKS_CLUSTER="${AKS_CLUSTER:-aei}"
AKS_LOCATION="${AKS_LOCATION:-westus2}"
# Subscription name or id. Empty means the az CLI's current default; either way the
# resolved id is shown first and pinned on every command.
AKS_SUBSCRIPTION="${AKS_SUBSCRIPTION:-}"
# The version to CREATE with; empty leaves it to AKS's default. Pod Sandboxing needs
# 1.27 or later. It is a starting point, not a pin, unless the channel below is none.
AKS_KUBERNETES_VERSION="${AKS_KUBERNETES_VERSION:-}"
# Automatic upgrades, applied at creation and re-applied on every run.
#   Kubernetes: stable (latest patch of minor N-1) | patch | rapid | node-image | none
#   Node OS:    NodeImage (a fresh node image, about weekly) | SecurityPatch | Unmanaged | None
# 'none' / 'None' turn them off. Upgrades reimage nodes, which ends any run on them;
# an AKS planned-maintenance window (not set here) controls when.
AKS_AUTO_UPGRADE_CHANNEL="${AKS_AUTO_UPGRADE_CHANNEL:-stable}"
AKS_NODE_OS_UPGRADE_CHANNEL="${AKS_NODE_OS_UPGRADE_CHANNEL:-NodeImage}"

# System pool: the AEI controller and cluster add-ons. No agent run needs to land here.
AKS_SYSTEM_POOL="${AKS_SYSTEM_POOL:-system}"
AKS_SYSTEM_NODE_COUNT="${AKS_SYSTEM_NODE_COUNT:-1}"
AKS_SYSTEM_VM_SIZE="${AKS_SYSTEM_VM_SIZE:-Standard_D2s_v7}"

# Kata pool: where isolated runs execute. The VM size must be Generation 2 with
# nested virtualization. Pool names are at most 12 lowercase alphanumerics.
AKS_KATA_POOL="${AKS_KATA_POOL:-kata}"
# The pool starts at AKS_KATA_NODE_COUNT nodes and the cluster autoscaler then keeps
# it between AKS_KATA_MIN_COUNT and AKS_KATA_MAX_COUNT: it adds a node when a run's
# pod cannot be scheduled, and removes one that has stayed underused (it does not
# evict a running Sandbox pod to do so). AKS_KATA_AUTOSCALE=false makes it a fixed
# pool of AKS_KATA_NODE_COUNT.
#
# AKS_KATA_MIN_COUNT=0 lets the pool empty out when nothing runs: a node comes back
# for the next Kata pod in about two minutes. It only reaches zero when nothing but
# agent runs and DaemonSets is on it. The AKS_KATA_TAINT below arranges that by
# keeping every pod without the toleration (the AEI controller, an app) on the
# system pool; with AKS_KATA_TAINT= (no taint) such pods can hold a node. The default
# stays at 1, so a run does not wait for a node to start.
#
# What "cannot be scheduled" means for AEI: its runtime pods set no resource
# requests, so the scheduler counts only the RuntimeClass's fixed 600Mi of memory
# per pod. A Standard_D4s_v7 node therefore takes about 18 runs before the next one
# waits for a new node, and CPU is not counted at all.
#
# These apply when the pool is CREATED. An existing pool is reused as it is.
AKS_KATA_NODE_COUNT="${AKS_KATA_NODE_COUNT:-1}"
AKS_KATA_AUTOSCALE="${AKS_KATA_AUTOSCALE:-true}"
AKS_KATA_MIN_COUNT="${AKS_KATA_MIN_COUNT:-1}"
AKS_KATA_MAX_COUNT="${AKS_KATA_MAX_COUNT:-3}"
AKS_KATA_VM_SIZE="${AKS_KATA_VM_SIZE:-Standard_D4s_v7}"
# The taint put on the Kata pool's nodes when the pool is created, reserving them
# for agent runs: pods without a matching toleration (the AEI controller, add-ons,
# anything else) stay on the system pool, so the Kata pool holds only runs. The
# printed `make deploy` command passes it as RUNTIME_TOLERATIONS, which makes the
# 'isolated' class's runs tolerate it. Format: key=value:Effect, as for
# `az aks nodepool add --node-taints` (the kubernetes.azure.com/ prefix is reserved
# by AKS). Empty means no taint: any pod may then land on the Kata pool.
# Applied at creation only; an existing pool keeps the taints it has.
AKS_KATA_TAINT="${AKS_KATA_TAINT-aei.dev/agent-runs=true:NoSchedule}"
# The RuntimeClass AKS provides for Pod Sandboxing; passed to AEI as ISOLATION_CLASS.
AKS_RUNTIME_CLASS="${AKS_RUNTIME_CLASS:-kata-vm-isolation}"

# Where the cluster's credentials go. Always a file of its own, replaced whole on
# every run; never merged into ~/.kube/config.
AKS_KUBECONFIG="${AKS_KUBECONFIG:-${HOME}/.kube/aei-aks-${AKS_CLUSTER}.kubeconfig}"

# Registry. By default an Azure Container Registry is created in the resource group
# and attached to the cluster (the kubelet identity gets AcrPull, so no pull secrets).
#   AKS_CREATE_ACR=false   no registry: bring your own and make it pullable yourself.
#   AKS_ATTACH_ACR=<name or resource id>   attach an existing registry instead.
#   AKS_ACR_NAME=<name>    name for the created registry (globally unique, 5-50
#                          lowercase alphanumerics). Default: derived, see below.
AKS_CREATE_ACR="${AKS_CREATE_ACR:-true}"
AKS_ATTACH_ACR="${AKS_ATTACH_ACR:-}"
AKS_ACR_NAME="${AKS_ACR_NAME:-}"
AKS_ACR_SKU="${AKS_ACR_SKU:-Basic}"

AKS_DRY_RUN="${AKS_DRY_RUN:-}"
AKS_CONFIRM_DELETE="${AKS_CONFIRM_DELETE:-}"

# Ownership marker. `create` puts it on each resource IT creates and on nothing it
# merely reuses; `delete` removes only resources that carry it.
OWNER_TAG_KEY="aei-created-by"
OWNER_TAG_VALUE="make-create-aks"
OWNER_TAG="${OWNER_TAG_KEY}=${OWNER_TAG_VALUE}"

die() { echo "ERROR: $*" >&2; exit 1; }
say() { echo "==> $*"; }
dry() { [ -n "${AKS_DRY_RUN}" ]; }

# run executes a command that CHANGES something, echoing it first. Under
# AKS_DRY_RUN it only echoes. Read-only queries call az directly.
run() {
	printf '+'
	printf ' %q' "$@"
	printf '\n'
	dry || "$@"
}

SUB=""
SUB_NAME=""

# preflight: the tools exist, az is logged in, and the subscription is resolved and
# shown before anything is created or deleted.
preflight() {
	command -v az >/dev/null 2>&1 || die "the Azure CLI (az) is not installed: https://learn.microsoft.com/cli/azure/install-azure-cli"
	command -v kubectl >/dev/null 2>&1 || die "kubectl is not installed"

	local acct
	# ${arr[@]+...} keeps an empty array legal under nounset on bash 3.2.
	local sub_arg=()
	[ -z "${AKS_SUBSCRIPTION}" ] || sub_arg=(--subscription "${AKS_SUBSCRIPTION}")
	if acct="$(az account show ${sub_arg[@]+"${sub_arg[@]}"} --query '[id,name,tenantId,user.name]' -o tsv 2>/dev/null)" && [ -n "${acct}" ]; then
		SUB="$(printf '%s\n' "${acct}" | sed -n 1p)"
		SUB_NAME="$(printf '%s\n' "${acct}" | sed -n 2p)"
		say "subscription: ${SUB_NAME} (${SUB})"
		echo "    tenant: $(printf '%s\n' "${acct}" | sed -n 3p)   signed in as: $(printf '%s\n' "${acct}" | sed -n 4p)"
	elif dry; then
		SUB="<subscription-id>"
		say "subscription: not resolved (az is not logged in, or cannot see AKS_SUBSCRIPTION); dry run continues with a placeholder"
	elif [ -n "${AKS_SUBSCRIPTION}" ]; then
		die "az cannot use subscription '${AKS_SUBSCRIPTION}'. Run 'az login', then 'az account list -o table' to see what is available."
	else
		die "az is not logged in. Run 'az login' (and 'az account set --subscription <id>' or pass AKS_SUBSCRIPTION=<id> to choose a subscription)."
	fi
}

# require_kata_flag: --workload-runtime moved from the aks-preview extension into
# the core CLI in 2.80.0. The check is for the flag, not a version number. The
# advice is to upgrade, not to add the extension: the extension tracks the newest
# core CLI, and on an old one `az aks create` crashes inside it.
require_kata_flag() {
	local help
	help="$(az aks nodepool add --help 2>/dev/null || true)"
	case "${help}" in
		*--workload-runtime*) return 0 ;;
	esac
	local msg="this az CLI has no 'az aks nodepool add --workload-runtime' (needed for the Kata pool). Upgrade the Azure CLI to 2.80.0 or later ('az upgrade', or your package manager)."
	if dry; then echo "WARNING: ${msg}"; else die "${msg}"; fi
}

# require_provider: a subscription must have a resource provider registered before
# anything of that type can be created in it, and a subscription that has never held
# a registry (or a cluster) has not. Registering is a one-time change to the
# subscription, so it is left to its owner: this only checks, before anything is
# created, and says what to run.
require_provider() {
	dry && return 0
	local state
	state="$(az provider show --subscription "${SUB}" --namespace "$1" --query registrationState -o tsv 2>/dev/null || true)"
	[ "${state}" = Registered ] || die "subscription ${SUB} has not registered the $1 resource provider (state: ${state:-unknown}), which $2 needs. Register it once, then re-run:
  az provider register --subscription ${SUB} --namespace $1 --wait${3:+
$3}"
}

# default_acr_name: registry names are global across Azure, so derive one that is
# stable for this subscription + group + cluster (re-runs find the same registry)
# and unlikely to collide with anyone else's.
default_acr_name() {
	local sum
	if command -v sha256sum >/dev/null 2>&1; then
		sum="$(printf '%s' "${SUB}/${AKS_RESOURCE_GROUP}/${AKS_CLUSTER}" | sha256sum)"
	else
		sum="$(printf '%s' "${SUB}/${AKS_RESOURCE_GROUP}/${AKS_CLUSTER}" | shasum -a 256)"
	fi
	echo "aei${sum:0:12}"
}

# resource_tag prints the ownership tag's value on a resource, or nothing.
group_tag()   { az group show --subscription "${SUB}" -n "${AKS_RESOURCE_GROUP}" --query "tags.\"${OWNER_TAG_KEY}\"" -o tsv 2>/dev/null || true; }
cluster_tag() { az aks show --subscription "${SUB}" -g "${AKS_RESOURCE_GROUP}" -n "${AKS_CLUSTER}" --query "tags.\"${OWNER_TAG_KEY}\"" -o tsv 2>/dev/null || true; }
acr_tag()     { az acr show --subscription "${SUB}" -g "${AKS_RESOURCE_GROUP}" -n "$1" --query "tags.\"${OWNER_TAG_KEY}\"" -o tsv 2>/dev/null || true; }

group_exists()   { ! dry && [ "$(az group exists --subscription "${SUB}" -n "${AKS_RESOURCE_GROUP}" 2>/dev/null)" = true ]; }
cluster_exists() { ! dry && az aks show --subscription "${SUB}" -g "${AKS_RESOURCE_GROUP}" -n "${AKS_CLUSTER}" -o none 2>/dev/null; }
acr_exists()     { ! dry && az acr show --subscription "${SUB}" -g "${AKS_RESOURCE_GROUP}" -n "$1" -o none 2>/dev/null; }

kc() { kubectl --kubeconfig "${AKS_KUBECONFIG}" "$@"; }

# The taints on the Kata pool as it really is: AKS_KATA_TAINT for a pool this run
# creates, the pool's own for one it reuses. Comma-separated key=value:Effect.
POOL_TAINTS=""

# tolerations_yaml prints pod-spec tolerations matching POOL_TAINTS, indented for
# the smoke pod, or nothing when the pool has no taints.
tolerations_yaml() {
	[ -n "${POOL_TAINTS}" ] || return 0
	echo "  tolerations:"
	local t kv effect
	local IFS=,
	for t in ${POOL_TAINTS}; do
		kv="${t%%:*}"
		effect=""
		case "${t}" in *:*) effect="${t##*:}" ;; esac
		case "${kv}" in
			*=*) printf '    - {key: "%s", operator: Equal, value: "%s"' "${kv%%=*}" "${kv#*=}" ;;
			*)   printf '    - {key: "%s", operator: Exists' "${kv}" ;;
		esac
		if [ -n "${effect}" ]; then printf ', effect: %s' "${effect}"; fi
		printf '}\n'
	done
}

create() {
	preflight
	require_kata_flag
	case "${AKS_KUBECONFIG}" in
		"${HOME}/.kube/config") die "AKS_KUBECONFIG is your default kubeconfig; this file is replaced whole, so choose another path" ;;
	esac

	local acr="" acr_create=""
	if [ -n "${AKS_ATTACH_ACR}" ]; then
		acr="${AKS_ATTACH_ACR}"
	elif [ "${AKS_CREATE_ACR}" = true ]; then
		acr="${AKS_ACR_NAME:-$(default_acr_name)}"
		acr_create=1
	fi

	if [ "${AKS_KATA_AUTOSCALE}" = true ]; then
		[ "${AKS_KATA_MIN_COUNT}" -ge 0 ] 2>/dev/null || die "AKS_KATA_MIN_COUNT must be a number, 0 or more (got '${AKS_KATA_MIN_COUNT}')"
		[ "${AKS_KATA_MAX_COUNT}" -ge "${AKS_KATA_MIN_COUNT}" ] 2>/dev/null || die "AKS_KATA_MAX_COUNT (${AKS_KATA_MAX_COUNT}) must be at least AKS_KATA_MIN_COUNT (${AKS_KATA_MIN_COUNT})"
		{ [ "${AKS_KATA_NODE_COUNT}" -ge "${AKS_KATA_MIN_COUNT}" ] && [ "${AKS_KATA_NODE_COUNT}" -le "${AKS_KATA_MAX_COUNT}" ]; } 2>/dev/null \
			|| die "AKS_KATA_NODE_COUNT (${AKS_KATA_NODE_COUNT}) must be between AKS_KATA_MIN_COUNT (${AKS_KATA_MIN_COUNT}) and AKS_KATA_MAX_COUNT (${AKS_KATA_MAX_COUNT})"
	fi

	require_provider Microsoft.ContainerService "an AKS cluster"
	[ -z "${acr_create}" ] || require_provider Microsoft.ContainerRegistry "the container registry" \
		"Or skip the registry (AKS_CREATE_ACR=false), or attach one you already have (AKS_ATTACH_ACR=<registry>)."

	say "resource group '${AKS_RESOURCE_GROUP}' (${AKS_LOCATION})"
	if group_exists; then
		if [ "$(group_tag)" = "${OWNER_TAG_VALUE}" ]; then
			echo "    exists (created by create-aks); reusing"
		else
			echo "    exists; reusing. create-aks did not create it, so delete-aks will keep it."
		fi
	else
		run az group create --subscription "${SUB}" --name "${AKS_RESOURCE_GROUP}" --location "${AKS_LOCATION}" \
			--tags "${OWNER_TAG}" --output none
	fi

	if [ -n "${acr_create}" ]; then
		say "container registry '${acr}' (${AKS_ACR_SKU})"
		if acr_exists "${acr}"; then
			echo "    exists; reusing"
		else
			run az acr create --subscription "${SUB}" --resource-group "${AKS_RESOURCE_GROUP}" --name "${acr}" \
				--location "${AKS_LOCATION}" --sku "${AKS_ACR_SKU}" --tags "${OWNER_TAG}" --output none
		fi
	fi

	say "AKS cluster '${AKS_CLUSTER}': system pool '${AKS_SYSTEM_POOL}' = ${AKS_SYSTEM_NODE_COUNT} x ${AKS_SYSTEM_VM_SIZE}"
	if cluster_exists; then
		echo "    exists; reusing"
	else
		local version=()
		[ -z "${AKS_KUBERNETES_VERSION}" ] || version=(--kubernetes-version "${AKS_KUBERNETES_VERSION}")
		# --no-ssh-key: nothing here needs node SSH, and it keeps az from writing a
		# key pair into ~/.ssh.
		run az aks create --subscription "${SUB}" --resource-group "${AKS_RESOURCE_GROUP}" --name "${AKS_CLUSTER}" \
			--location "${AKS_LOCATION}" ${version[@]+"${version[@]}"} \
			--nodepool-name "${AKS_SYSTEM_POOL}" --node-count "${AKS_SYSTEM_NODE_COUNT}" \
			--node-vm-size "${AKS_SYSTEM_VM_SIZE}" --os-sku AzureLinux \
			--auto-upgrade-channel "${AKS_AUTO_UPGRADE_CHANNEL}" \
			--node-os-upgrade-channel "${AKS_NODE_OS_UPGRADE_CHANNEL}" \
			--enable-managed-identity --no-ssh-key --tags "${OWNER_TAG}" --output none
	fi

	local scaling=() sizing="${AKS_KATA_NODE_COUNT} x ${AKS_KATA_VM_SIZE}, fixed size"
	if [ "${AKS_KATA_AUTOSCALE}" = true ]; then
		scaling=(--enable-cluster-autoscaler --min-count "${AKS_KATA_MIN_COUNT}" --max-count "${AKS_KATA_MAX_COUNT}")
		sizing="${AKS_KATA_NODE_COUNT} x ${AKS_KATA_VM_SIZE}, autoscaling ${AKS_KATA_MIN_COUNT}-${AKS_KATA_MAX_COUNT}"
	fi
	local taint=()
	[ -z "${AKS_KATA_TAINT}" ] || taint=(--node-taints "${AKS_KATA_TAINT}")
	say "Kata pool '${AKS_KATA_POOL}' = ${sizing} (KataVmIsolation, AzureLinux), taint '${AKS_KATA_TAINT:-none}'"
	local pool_runtime=""
	dry || pool_runtime="$(az aks nodepool show --subscription "${SUB}" -g "${AKS_RESOURCE_GROUP}" --cluster-name "${AKS_CLUSTER}" \
		-n "${AKS_KATA_POOL}" --query workloadRuntime -o tsv 2>/dev/null || true)"
	case "${pool_runtime}" in
		KataVmIsolation|KataMshvVmIsolation)
			# Reused as it is: resizing, re-scaling or re-tainting a pool that may be
			# running agents is not something a re-run should do on its own. The pool
			# keeps its own taints, and everything after this point (the smoke pod,
			# the printed deploy command) follows what the pool actually has.
			POOL_TAINTS="$(az aks nodepool show --subscription "${SUB}" -g "${AKS_RESOURCE_GROUP}" --cluster-name "${AKS_CLUSTER}" \
				-n "${AKS_KATA_POOL}" --query "join(',', nodeTaints || \`[]\`)" -o tsv 2>/dev/null || true)"
			echo "    exists; reusing as it is: $(az aks nodepool show --subscription "${SUB}" -g "${AKS_RESOURCE_GROUP}" \
				--cluster-name "${AKS_CLUSTER}" -n "${AKS_KATA_POOL}" -o tsv \
				--query "join('', ['count=', to_string(count), ', autoscaling=', to_string(enableAutoScaling), ', min=', to_string(minCount), ', max=', to_string(maxCount)])" 2>/dev/null || true), taints=${POOL_TAINTS:-none}" ;;
		"")
			run az aks nodepool add --subscription "${SUB}" --resource-group "${AKS_RESOURCE_GROUP}" \
				--cluster-name "${AKS_CLUSTER}" --name "${AKS_KATA_POOL}" --mode User \
				--node-count "${AKS_KATA_NODE_COUNT}" ${scaling[@]+"${scaling[@]}"} \
				--node-vm-size "${AKS_KATA_VM_SIZE}" \
				--os-sku AzureLinux --workload-runtime KataVmIsolation \
				${taint[@]+"${taint[@]}"} --output none
			POOL_TAINTS="${AKS_KATA_TAINT}" ;;
		*)
			die "node pool '${AKS_KATA_POOL}' exists with workload runtime '${pool_runtime}', not KataVmIsolation. Pick another AKS_KATA_POOL." ;;
	esac

	# The AKS docs call for an `az aks update` after adding a Kata pool, so the
	# cluster reconciles and installs the RuntimeClass. Setting the upgrade channels
	# (so a reused cluster gets them too) and attaching the registry are `az aks
	# update` as well, so one call does all three. --attach-acr creates a role
	# assignment, which needs Owner (or User Access Administrator) on the registry.
	local attach=()
	if [ -n "${acr}" ]; then
		attach=(--attach-acr "${acr}")
		say "reconciling the cluster: upgrade channels ${AKS_AUTO_UPGRADE_CHANNEL}/${AKS_NODE_OS_UPGRADE_CHANNEL}, registry '${acr}'"
	else
		say "reconciling the cluster: upgrade channels ${AKS_AUTO_UPGRADE_CHANNEL}/${AKS_NODE_OS_UPGRADE_CHANNEL} (no registry: AKS_CREATE_ACR=${AKS_CREATE_ACR})"
	fi
	run az aks update --subscription "${SUB}" --resource-group "${AKS_RESOURCE_GROUP}" --name "${AKS_CLUSTER}" \
		--auto-upgrade-channel "${AKS_AUTO_UPGRADE_CHANNEL}" \
		--node-os-upgrade-channel "${AKS_NODE_OS_UPGRADE_CHANNEL}" \
		${attach[@]+"${attach[@]}"} --output none

	# Credentials go to a fresh temporary file that then replaces the target, so a
	# stale or unrelated file at that path is never merged into.
	say "writing kubeconfig to ${AKS_KUBECONFIG}"
	local tmp="${AKS_KUBECONFIG}.tmp.$$"
	run mkdir -p "$(dirname "${AKS_KUBECONFIG}")"
	run rm -f "${tmp}"
	run az aks get-credentials --subscription "${SUB}" --resource-group "${AKS_RESOURCE_GROUP}" --name "${AKS_CLUSTER}" \
		--file "${tmp}" --only-show-errors --output none
	run chmod 600 "${tmp}"
	run mv -f "${tmp}" "${AKS_KUBECONFIG}"

	local login_server="<registry>"
	if [ -n "${acr}" ]; then
		if dry; then
			login_server="${acr##*/}.azurecr.io"
		else
			login_server="$(az acr show --subscription "${SUB}" --name "${acr}" --query loginServer -o tsv 2>/dev/null || true)"
			[ -n "${login_server}" ] || login_server="${acr##*/}.azurecr.io"
		fi
	fi

	dry || verify
	next_steps "${acr}" "${login_server}"
}

# verify checks what AEI's isolated class depends on: the RuntimeClass exists, and
# a pod shaped like AEI's runtime pods (the RuntimeClass, the pool's tolerations,
# and nothing else) runs on a Kata node. It also reports whether the RuntimeClass
# steers pods there itself.
verify() {
	say "waiting for nodes"
	kc wait --for=condition=Ready nodes --all --timeout=300s

	say "checking RuntimeClass '${AKS_RUNTIME_CLASS}'"
	local i
	for i in $(seq 1 60); do
		kc get runtimeclass "${AKS_RUNTIME_CLASS}" >/dev/null 2>&1 && break
		[ "${i}" != 60 ] || die "RuntimeClass '${AKS_RUNTIME_CLASS}' did not appear. Present: $(kc get runtimeclass -o name | tr '\n' ' ')"
		sleep 5
	done
	local selector
	selector="$(kc get runtimeclass "${AKS_RUNTIME_CLASS}" -o jsonpath='{.scheduling.nodeSelector}')"
	if [ -n "${selector}" ]; then
		echo "    scheduling.nodeSelector: ${selector}"
		echo "    The RuntimeClass places its pods on Kata nodes; AEI runs need no node selector."
	else
		echo "    WARNING: the RuntimeClass has no scheduling.nodeSelector. AEI's runtime pods carry"
		echo "    none either, so an isolated run may be scheduled onto the '${AKS_SYSTEM_POOL}' pool and fail to start."
	fi

	say "smoke: a pod with only runtimeClassName: ${AKS_RUNTIME_CLASS}${POOL_TAINTS:+ and a toleration for ${POOL_TAINTS}}"
	kc -n default delete pod aei-kata-smoke --ignore-not-found >/dev/null 2>&1 || true
	kc -n default apply -f - <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: aei-kata-smoke
spec:
  runtimeClassName: ${AKS_RUNTIME_CLASS}
$(tolerations_yaml)
  restartPolicy: Never
  containers:
    - name: smoke
      image: mcr.microsoft.com/aks/fundamental/base-ubuntu:v0.0.11
      command: ["uname", "-r"]
EOF
	if kc -n default wait --for=jsonpath='{.status.phase}'=Succeeded pod/aei-kata-smoke --timeout=300s >/dev/null 2>&1; then
		local node pool
		node="$(kc -n default get pod aei-kata-smoke -o jsonpath='{.spec.nodeName}')"
		pool="$(kc get node "${node}" -o jsonpath='{.metadata.labels.kubernetes\.azure\.com/agentpool}')"
		echo "    ran on node ${node} (pool '${pool}'); guest kernel $(kc -n default logs aei-kata-smoke 2>/dev/null || true), node kernel $(kc get node "${node}" -o jsonpath='{.status.nodeInfo.kernelVersion}')"
		[ "${pool}" = "${AKS_KATA_POOL}" ] || echo "    WARNING: that is not the Kata pool '${AKS_KATA_POOL}'."
		kc -n default delete pod aei-kata-smoke --ignore-not-found >/dev/null 2>&1 || true
	else
		echo "ERROR: the smoke pod did not run under ${AKS_RUNTIME_CLASS}. Inspect, then delete it:" >&2
		echo "  kubectl --kubeconfig ${AKS_KUBECONFIG} -n default describe pod aei-kata-smoke" >&2
		exit 1
	fi
}

next_steps() {
	local acr="$1" login_server="$2"
	local img="${login_server}/aei-manager:\$(git rev-parse --short HEAD)"
	echo
	say "AKS cluster '${AKS_CLUSTER}' is ready${AKS_DRY_RUN:+ (dry run: nothing was created)}. Kubeconfig: ${AKS_KUBECONFIG}"
	echo
	echo "Next, install AEI on it (the same commands as for any cluster):"
	echo
	if [ -n "${acr}" ]; then
		echo "  az acr login --name ${acr##*/}"
	else
		echo "  # no registry was attached: push to one this cluster's nodes can pull from"
	fi
	echo "  make push IMG=${img} PLATFORM=linux/amd64"
	echo "  make install-agent-sandbox KUBECONFIG=${AKS_KUBECONFIG}"
	echo "  make deploy KUBECONFIG=${AKS_KUBECONFIG} IMG=${img} PROVIDERS=agent-sandbox ISOLATION_CLASS=${AKS_RUNTIME_CLASS}${POOL_TAINTS:+ RUNTIME_TOLERATIONS=${POOL_TAINTS}}"
	echo "  make public-key KUBECONFIG=${AKS_KUBECONFIG}"
	echo
	echo "The cluster bills while it exists. Remove it with:"
	echo "  make delete-aks AKS_SUBSCRIPTION=${SUB} AKS_RESOURCE_GROUP=${AKS_RESOURCE_GROUP} AKS_CLUSTER=${AKS_CLUSTER} AKS_CONFIRM_DELETE=${AKS_RESOURCE_GROUP}"
}

# delete removes only what carries the ownership tag. If `create` made the resource
# group, the whole group goes (and everything in it is listed first). If the group
# was there already, only the tagged cluster and registry in it are deleted and the
# group is left alone. Nothing happens without AKS_CONFIRM_DELETE=<resource group>.
delete() {
	preflight
	dry || [ "$(az group exists --subscription "${SUB}" -n "${AKS_RESOURCE_GROUP}" 2>/dev/null)" = true ] || {
		echo "resource group '${AKS_RESOURCE_GROUP}' does not exist in this subscription; nothing to delete"
		return 0
	}

	local acr="${AKS_ACR_NAME:-$(default_acr_name)}"
	local own_group="" own_cluster="" own_acr=""
	if dry; then
		own_group=1
	else
		[ "$(group_tag)" != "${OWNER_TAG_VALUE}" ] || own_group=1
		[ "$(cluster_tag)" != "${OWNER_TAG_VALUE}" ] || own_cluster=1
		[ "$(acr_tag "${acr}")" != "${OWNER_TAG_VALUE}" ] || own_acr=1
	fi

	say "delete plan for resource group '${AKS_RESOURCE_GROUP}'"
	if [ -n "${own_group}" ]; then
		echo "    ${AKS_DRY_RUN:+(dry run: assuming) }The group was created by create-aks: the WHOLE GROUP is deleted, with everything in it:"
		dry || az resource list --subscription "${SUB}" -g "${AKS_RESOURCE_GROUP}" --query '[].[type,name]' -o tsv 2>/dev/null | sed 's/^/      - /' || true
		echo "    (and the cluster's managed node resource group, which Azure removes with the cluster)"
	else
		echo "    The group was NOT created by create-aks (no ${OWNER_TAG} tag): it is kept."
		if [ -n "${own_cluster}" ]; then echo "      - delete AKS cluster '${AKS_CLUSTER}'"; else echo "      - keep AKS cluster '${AKS_CLUSTER}' (absent, or not created by create-aks)"; fi
		if [ -n "${own_acr}" ]; then echo "      - delete container registry '${acr}'"; else echo "      - keep container registry '${acr}' (absent, or not created by create-aks)"; fi
	fi
	local kubeconfig_ours=""
	if [ -f "${AKS_KUBECONFIG}" ] && grep -q "^current-context: ${AKS_CLUSTER}\$" "${AKS_KUBECONFIG}"; then
		kubeconfig_ours=1
		echo "    The kubeconfig file ${AKS_KUBECONFIG} is removed."
	fi

	if [ -z "${own_group}${own_cluster}${own_acr}${kubeconfig_ours}" ]; then
		echo "nothing here was created by create-aks; nothing to delete"
		return 0
	fi
	if ! dry && [ "${AKS_CONFIRM_DELETE}" != "${AKS_RESOURCE_GROUP}" ]; then
		echo
		echo "Nothing deleted. To carry out this plan, repeat the command with AKS_CONFIRM_DELETE=${AKS_RESOURCE_GROUP}"
		exit 1
	fi

	if [ -n "${own_group}" ]; then
		run az group delete --subscription "${SUB}" --name "${AKS_RESOURCE_GROUP}" --yes
	else
		[ -z "${own_cluster}" ] || run az aks delete --subscription "${SUB}" --resource-group "${AKS_RESOURCE_GROUP}" --name "${AKS_CLUSTER}" --yes
		[ -z "${own_acr}" ] || run az acr delete --subscription "${SUB}" --resource-group "${AKS_RESOURCE_GROUP}" --name "${acr}" --yes
	fi
	if [ -n "${kubeconfig_ours}" ] || dry; then
		run rm -f "${AKS_KUBECONFIG}"
	fi
	say "deleted${AKS_DRY_RUN:+ (dry run: nothing was deleted)}"
}

case "${1:-}" in
	create) create ;;
	delete) delete ;;
	*) echo "usage: $0 create|delete   (settings are AKS_* environment variables; see the header)" >&2; exit 2 ;;
esac
