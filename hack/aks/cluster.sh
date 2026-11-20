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
# Microsoft Entra ID for the cluster's API: people sign in with Entra identities, and
# Azure RBAC authorizes their Kubernetes requests. Many organizations' Azure Policy
# refuses an AKS cluster without it (RequestDisallowedByPolicy, citing
# Azure_KubernetesService_AuthN_Enabled_Microsoft_Entra_ID).
#   AKS_ENTRA_ID=true (the default) | false
#   AKS_ENTRA_ADMIN_GROUPS=<group object id>[,<id>...]   Entra groups made cluster
#                          admins. Empty: none; the admin kubeconfig (below) needs
#                          no group, and people can be given an Azure RBAC role.
# Applied when the cluster is CREATED. A reused cluster keeps what it has: turning
# Entra ID on cannot be undone, so create-aks never does it on its own. Local
# accounts stay enabled, so the admin kubeconfig (below) works either way.
AKS_ENTRA_ID="${AKS_ENTRA_ID:-true}"
AKS_ENTRA_ADMIN_GROUPS="${AKS_ENTRA_ADMIN_GROUPS:-}"

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
# every run; never merged into ~/.kube/config. It holds the cluster's local admin
# certificate, which needs no kubelogin and no role on the cluster, with or without
# Entra ID, as long as the cluster's local accounts are enabled (a reused cluster
# with them disabled is refused before anything changes). For a kubeconfig with
# your Entra identity instead, see "Entra ID user credentials" in hack/aks/README.md.
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

# A static public IP for an app on the cluster (optional; AEI itself needs none). An
# app that must be reachable at an address that does not change, one you can give to
# a DNS record, an OAuth app's callback or a webhook, puts it on a Service of type
# LoadBalancer; `create` prints the two annotations that do that.
#   AKS_PUBLIC_IP=<name>           create (or reuse) a Standard, static IPv4 address
#                                  with this name. Empty (the default): none.
#   AKS_PUBLIC_IP_DNS_LABEL=<label>  also give it the DNS name
#                                  <label>.<location>.cloudapp.azure.com, known before
#                                  the address is (the label must be unique in the
#                                  region). Applied at creation only.
# The address lives in AKS_RESOURCE_GROUP, not in the cluster's managed node resource
# group, so it is not tied to any one Service: deleting and re-creating the Service
# (a redeploy) keeps it. delete-aks removes it with the rest. The cluster's identity
# is given Network Contributor on that one address, which the cloud provider needs to
# use an address outside the node resource group.
AKS_PUBLIC_IP="${AKS_PUBLIC_IP:-}"
AKS_PUBLIC_IP_DNS_LABEL="${AKS_PUBLIC_IP_DNS_LABEL:-}"

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

# run_when_idle is run for a change to an existing cluster. AKS takes one change at a
# time per cluster and refuses the next while another is in progress, as
# OperationNotAllowed or EtagMismatch ("Another operation is in progress"); the other
# one need not be ours: a subscription's Azure Policy
# (deployIfNotExists) can install an add-on or extension on a new cluster a few
# minutes after it is created. So on that error it waits and tries again, for up to
# 20 minutes; any other error fails at once.
run_when_idle() {
	printf '+'
	printf ' %q' "$@"
	printf '\n'
	dry && return 0
	local attempt out lower
	for attempt in $(seq 1 40); do
		if out="$("$@" 2>&1)"; then
			[ -z "${out}" ] || printf '%s\n' "${out}"
			return 0
		fi
		# Matched in lower case: AKS words the same refusal several ways.
		lower="$(printf '%s' "${out}" | tr '[:upper:]' '[:lower:]')"
		case "${lower}" in
			*operationnotallowed*|*etagmismatch*|*"another operation"*|*"in progress"*|*"in-progress"*)
				echo "    the cluster is busy with another operation; retrying in 30s (${attempt}/40):"
				printf '%s\n' "${out}" | grep -m1 -E "Message:|ERROR:" | cut -c1-200 | sed 's/^/      /'
				sleep 30 ;;
			*)
				printf '%s\n' "${out}" >&2
				return 1 ;;
		esac
	done
	printf '%s\n' "${out}" >&2
	die "the cluster stayed busy for 20 minutes; re-run create-aks once its other operation has finished"
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
ip_tag()      { az network public-ip show --subscription "${SUB}" -g "${AKS_RESOURCE_GROUP}" -n "$1" --query "tags.\"${OWNER_TAG_KEY}\"" -o tsv 2>/dev/null || true; }

# azure_has runs an `az ... show` and answers only what Azure confirms: the resource
# is there (exit 0), or Azure reports it missing (exit 3: ResourceNotFound, or
# ResourceGroupNotFound). Any other failure (throttling, a 5xx, the network) stops
# the run instead of being taken as "absent", which would create, or keep, on a
# wrong answer. Under AKS_DRY_RUN nothing is asked and everything is absent.
azure_has() {
	dry && return 1
	local out rc=0
	out="$("$@" -o none 2>&1)" || rc=$?
	case "${rc}" in
		0) return 0 ;;
		3) return 1 ;;
	esac
	printf '%s\n' "${out}" >&2
	die "could not tell whether it exists ('$*' failed with exit ${rc}, not 'not found'); stopping here. Re-run once Azure answers."
}
group_exists()   { azure_has az group show --subscription "${SUB}" -n "${AKS_RESOURCE_GROUP}"; }
cluster_exists() { azure_has az aks show --subscription "${SUB}" -g "${AKS_RESOURCE_GROUP}" -n "${AKS_CLUSTER}"; }
acr_exists()     { azure_has az acr show --subscription "${SUB}" -g "${AKS_RESOURCE_GROUP}" -n "$1"; }
ip_exists()      { azure_has az network public-ip show --subscription "${SUB}" -g "${AKS_RESOURCE_GROUP}" -n "$1"; }

# public_ip creates (or reuses) AKS_PUBLIC_IP and lets the cluster use it. The role
# goes to the cluster's own (control plane) identity, which is what the cloud
# provider's load balancer code acts as, and is scoped to the one address.
public_ip() {
	[ -n "${AKS_PUBLIC_IP}" ] || return 0
	say "static public IP '${AKS_PUBLIC_IP}'${AKS_PUBLIC_IP_DNS_LABEL:+ (DNS label '${AKS_PUBLIC_IP_DNS_LABEL}')}"
	if ip_exists "${AKS_PUBLIC_IP}"; then
		echo "    exists; reusing"
	else
		local dns=()
		[ -z "${AKS_PUBLIC_IP_DNS_LABEL}" ] || dns=(--dns-name "${AKS_PUBLIC_IP_DNS_LABEL}")
		run az network public-ip create --subscription "${SUB}" --resource-group "${AKS_RESOURCE_GROUP}" \
			--name "${AKS_PUBLIC_IP}" --location "${AKS_LOCATION}" --sku Standard --version IPv4 \
			--allocation-method Static ${dns[@]+"${dns[@]}"} --tags "${OWNER_TAG}" --output none
	fi
	if dry; then
		run az role assignment create --assignee-object-id "<cluster identity>" --assignee-principal-type ServicePrincipal \
			--role "Network Contributor" --scope "<public IP id>" --output none
		return 0
	fi
	local principal ip_id
	principal="$(az aks show --subscription "${SUB}" -g "${AKS_RESOURCE_GROUP}" -n "${AKS_CLUSTER}" --query identity.principalId -o tsv)"
	ip_id="$(az network public-ip show --subscription "${SUB}" -g "${AKS_RESOURCE_GROUP}" -n "${AKS_PUBLIC_IP}" --query id -o tsv)"
	[ -n "${principal}" ] || die "cluster '${AKS_CLUSTER}' has no system-assigned identity to give the public IP to"
	if [ -n "$(az role assignment list --subscription "${SUB}" --assignee "${principal}" --scope "${ip_id}" \
		--role "Network Contributor" --query '[].id' -o tsv 2>/dev/null)" ]; then
		echo "    the cluster identity already has Network Contributor on it"
	else
		# A new cluster identity can take a minute to be visible to role assignment.
		local i
		for i in 1 2 3 4 5 6; do
			if run az role assignment create --assignee-object-id "${principal}" --assignee-principal-type ServicePrincipal \
				--role "Network Contributor" --scope "${ip_id}" --output none; then
				break
			fi
			[ "${i}" != 6 ] || die "could not give the cluster identity Network Contributor on ${ip_id} (assigning roles needs Owner or User Access Administrator)"
			echo "    retrying in 20s"
			sleep 20
		done
	fi
}

# public_ip_field prints one field of AKS_PUBLIC_IP: ipAddress or dnsSettings.fqdn.
public_ip_field() {
	if dry; then
		case "$1" in
			ipAddress) echo "<address>" ;;
			*) [ -z "${AKS_PUBLIC_IP_DNS_LABEL}" ] || echo "${AKS_PUBLIC_IP_DNS_LABEL}.${AKS_LOCATION}.cloudapp.azure.com" ;;
		esac
		return 0
	fi
	az network public-ip show --subscription "${SUB}" -g "${AKS_RESOURCE_GROUP}" -n "${AKS_PUBLIC_IP}" \
		--query "$1" -o tsv 2>/dev/null || true
}

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
	case "${AKS_ENTRA_ID}" in true|false) ;; *) die "AKS_ENTRA_ID must be true or false (got '${AKS_ENTRA_ID}')" ;; esac
	# For an existing cluster, read up front, before anything changes: whether it has
	# Entra ID (aadProfile is present for either integration, AKS-managed or the
	# retired legacy one), and whether its local accounts are disabled, in which case
	# the admin kubeconfig this writes cannot be fetched and the run could not finish.
	# A cluster create makes keeps local accounts enabled.
	local exists="" cluster_entra="${AKS_ENTRA_ID}" state=""
	if cluster_exists; then
		exists=1
		state="$(az aks show --subscription "${SUB}" -g "${AKS_RESOURCE_GROUP}" -n "${AKS_CLUSTER}" \
			--query "join(' ', [to_string(aadProfile != null), to_string(disableLocalAccounts == \`true\`)])" -o tsv)" \
			|| die "could not read the settings of cluster '${AKS_CLUSTER}' (az aks show failed above); nothing was changed"
		case "${state}" in
			"true true"|"true false"|"false true"|"false false") ;;
			*) die "unexpected answer reading the settings of cluster '${AKS_CLUSTER}': '${state}'; nothing was changed" ;;
		esac
		cluster_entra="${state%% *}"
		if [ "${state#* }" = true ]; then
			die "cluster '${AKS_CLUSTER}' has local accounts disabled, so create-aks cannot fetch the admin kubeconfig it writes. Nothing was changed. Get a kubeconfig with your Entra identity yourself (see \"Entra ID user credentials\" in hack/aks/README.md)."
		fi
	fi

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

	local entra=() entra_desc=", Entra ID off"
	if [ "${AKS_ENTRA_ID}" = true ]; then
		entra=(--enable-aad --enable-azure-rbac)
		entra_desc=", Entra ID with Azure RBAC"
		if [ -n "${AKS_ENTRA_ADMIN_GROUPS}" ]; then
			entra=(--enable-aad --enable-azure-rbac --aad-admin-group-object-ids "${AKS_ENTRA_ADMIN_GROUPS}")
			entra_desc="${entra_desc} (admin groups ${AKS_ENTRA_ADMIN_GROUPS})"
		fi
	fi
	say "AKS cluster '${AKS_CLUSTER}': system pool '${AKS_SYSTEM_POOL}' = ${AKS_SYSTEM_NODE_COUNT} x ${AKS_SYSTEM_VM_SIZE}${entra_desc}"
	if [ -n "${exists}" ]; then
		if [ "${cluster_entra}" = true ]; then
			echo "    exists (Entra ID on); reusing"
		else
			echo "    exists (Entra ID off); reusing as it is"
			if [ "${AKS_ENTRA_ID}" = true ]; then
				echo "    NOTE: create-aks does not turn Entra ID on for an existing cluster: it cannot be undone. To do it"
				echo "    yourself: az aks update --subscription ${SUB} -g ${AKS_RESOURCE_GROUP} -n ${AKS_CLUSTER} --enable-aad --enable-azure-rbac"
				echo "    If an Azure Policy here requires Entra ID, the 'az aks update' below may be refused until you do."
			fi
		fi
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
			--enable-managed-identity --no-ssh-key ${entra[@]+"${entra[@]}"} --tags "${OWNER_TAG}" --output none
	fi

	local scaling=() sizing="${AKS_KATA_NODE_COUNT} x ${AKS_KATA_VM_SIZE}, fixed size"
	if [ "${AKS_KATA_AUTOSCALE}" = true ]; then
		scaling=(--enable-cluster-autoscaler --min-count "${AKS_KATA_MIN_COUNT}" --max-count "${AKS_KATA_MAX_COUNT}")
		sizing="${AKS_KATA_NODE_COUNT} x ${AKS_KATA_VM_SIZE}, autoscaling ${AKS_KATA_MIN_COUNT}-${AKS_KATA_MAX_COUNT}"
	fi
	local taint=()
	[ -z "${AKS_KATA_TAINT}" ] || taint=(--node-taints "${AKS_KATA_TAINT}")
	say "Kata pool '${AKS_KATA_POOL}'"
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
			# The requested settings are shown only here: a reused pool keeps its own.
			echo "    creating: ${sizing} (KataVmIsolation, AzureLinux), taint '${AKS_KATA_TAINT:-none}'"
			run_when_idle az aks nodepool add --subscription "${SUB}" --resource-group "${AKS_RESOURCE_GROUP}" \
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
	run_when_idle az aks update --subscription "${SUB}" --resource-group "${AKS_RESOURCE_GROUP}" --name "${AKS_CLUSTER}" \
		--auto-upgrade-channel "${AKS_AUTO_UPGRADE_CHANNEL}" \
		--node-os-upgrade-channel "${AKS_NODE_OS_UPGRADE_CHANNEL}" \
		${attach[@]+"${attach[@]}"} --output none

	public_ip

	# Credentials go to a fresh temporary file that then replaces the target, so a
	# stale or unrelated file at that path is never merged into.
	say "writing kubeconfig to ${AKS_KUBECONFIG} (the cluster's local admin certificate)"
	local tmp="${AKS_KUBECONFIG}.tmp.$$"
	case "${tmp}" in /*) ;; *) tmp="${PWD}/${tmp}" ;; esac
	run mkdir -p "$(dirname "${tmp}")"
	run rm -f "${tmp}"
	# KUBECONFIG names the temporary file too, not only --file: Azure CLI (2.93) runs
	# `kubelogin convert-kubeconfig` on its own, with no --kubeconfig, when what it
	# fetched signs in through kubelogin, and kubelogin then converts whatever KUBECONFIG
	# names. An admin kubeconfig does not, but nothing here should reach the caller's
	# own kubeconfig either way.
	run env KUBECONFIG="${tmp}" az aks get-credentials --subscription "${SUB}" --resource-group "${AKS_RESOURCE_GROUP}" \
		--name "${AKS_CLUSTER}" --admin --file "${tmp}" --only-show-errors --output none
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

	# Not something AEI uses; reported for apps that keep data on a volume. AKS ships
	# Azure Disk StorageClasses: 'default' (the default; Standard SSD, like
	# managed-csi) and managed-csi-premium. A disk attaches to one node at a time, so
	# a claim on it is ReadWriteOnce.
	say "default StorageClass (for apps' PersistentVolumeClaims)"
	kc get storageclass -o jsonpath='{range .items[?(@.metadata.annotations.storageclass\.kubernetes\.io/is-default-class=="true")]}    {.metadata.name}: {.provisioner}, {.parameters.skuname}, reclaim {.reclaimPolicy}, binding {.volumeBindingMode}{"\n"}{end}'

	# AEI ships a NetworkPolicy that keeps runtime pods off its dispatch port
	# (controller/config/manager/networkpolicy.yaml). AKS enforces NetworkPolicy only
	# when the cluster has a policy engine, and az aks create leaves it at none unless
	# told otherwise, which is what this script does. Report it rather than change a
	# cluster's networking: the engine is a creation-time choice.
	say "network policy engine (enforces AEI's dispatch NetworkPolicy)"
	local policy=""
	dry || policy="$(az aks show --subscription "${SUB}" -g "${AKS_RESOURCE_GROUP}" -n "${AKS_CLUSTER}" \
		--query networkProfile.networkPolicy -o tsv 2>/dev/null || true)"
	case "${policy}" in
		""|none)
			echo "    ${policy:-unknown}. AEI's NetworkPolicy is accepted but NOT enforced here: runtime pods can"
			echo "    connect to the dispatch port (8081), where only the TokenReview for the aei-dispatch"
			echo "    audience refuses them. For the network-layer restriction too, create the cluster with"
			echo "    a policy engine (for Linux pools AKS recommends Azure CNI powered by Cilium), and check"
			echo "    that it applies to Kata pods: https://learn.microsoft.com/azure/aks/use-network-policies" ;;
		*)
			echo "    ${policy}" ;;
	esac
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
	if [ -n "${AKS_PUBLIC_IP}" ]; then
		local addr fqdn
		addr="$(public_ip_field ipAddress)"
		fqdn="$(public_ip_field dnsSettings.fqdn)"
		echo
		echo "Static public IP '${AKS_PUBLIC_IP}': ${addr}${fqdn:+ (${fqdn})}. A Service of type LoadBalancer"
		echo "gets it with these two annotations:"
		echo "  service.beta.kubernetes.io/azure-load-balancer-resource-group: ${AKS_RESOURCE_GROUP}"
		echo "  service.beta.kubernetes.io/azure-pip-name: ${AKS_PUBLIC_IP}"
		echo "To limit who can connect, add service.beta.kubernetes.io/azure-allowed-ip-ranges (CIDRs)"
		echo "and/or .../azure-allowed-service-tags (service tags). Do not combine a service tag with"
		echo "spec.loadBalancerSourceRanges: kube-proxy enforces that list too and drops the tagged sources."
	fi
	echo
	echo "The cluster bills while it exists. Remove it with:"
	echo "  make delete-aks AKS_SUBSCRIPTION=${SUB} AKS_RESOURCE_GROUP=${AKS_RESOURCE_GROUP} AKS_CLUSTER=${AKS_CLUSTER}${AKS_PUBLIC_IP:+ AKS_PUBLIC_IP=${AKS_PUBLIC_IP}} AKS_CONFIRM_DELETE=${AKS_RESOURCE_GROUP}"
}

# delete removes only what carries the ownership tag. If `create` made the resource
# group, the whole group goes (and everything in it is listed first). If the group
# was there already, only the tagged cluster and registry in it are deleted and the
# group is left alone. Nothing happens without AKS_CONFIRM_DELETE=<resource group>.
delete() {
	preflight
	dry || group_exists || {
		echo "resource group '${AKS_RESOURCE_GROUP}' does not exist in this subscription; nothing to delete"
		return 0
	}

	local acr="${AKS_ACR_NAME:-$(default_acr_name)}"
	local own_group="" own_cluster="" own_acr="" own_ip=""
	if dry; then
		own_group=1
	else
		[ "$(group_tag)" != "${OWNER_TAG_VALUE}" ] || own_group=1
		[ "$(cluster_tag)" != "${OWNER_TAG_VALUE}" ] || own_cluster=1
		[ "$(acr_tag "${acr}")" != "${OWNER_TAG_VALUE}" ] || own_acr=1
		[ -z "${AKS_PUBLIC_IP}" ] || [ "$(ip_tag "${AKS_PUBLIC_IP}")" != "${OWNER_TAG_VALUE}" ] || own_ip=1
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
		if [ -n "${own_ip}" ]; then echo "      - delete public IP '${AKS_PUBLIC_IP}'"
		elif [ -n "${AKS_PUBLIC_IP}" ]; then echo "      - keep public IP '${AKS_PUBLIC_IP}' (absent, or not created by create-aks)"; fi
	fi
	local kubeconfig_ours="" kc_user="" server
	# Ours only when it is what create writes for THIS cluster and holds nothing else:
	# one cluster, one context and one user entry; the user named as az names this
	# group's and cluster's credentials (clusterAdmin_<group>_<cluster>, or clusterUser_
	# in a file written before create-aks fetched admin credentials); the context using
	# that user; the current context the cluster's (<cluster>-admin, or <cluster>); and
	# the API server this cluster's. Names alone do not pin it: the same group and
	# cluster names in another subscription give an identical file (default path
	# included) but for the server. The server is read here, before anything is
	# deleted; if the cluster is already gone it cannot be checked, and the file is
	# kept. A file with more in it is someone's merged kubeconfig, and is kept.
	if [ -f "${AKS_KUBECONFIG}" ] \
		&& [ "$(grep -c '^- cluster:' "${AKS_KUBECONFIG}")" = 1 ] \
		&& [ "$(grep -c '^- context:' "${AKS_KUBECONFIG}")" = 1 ] \
		&& [ "$(grep -c '^- name:' "${AKS_KUBECONFIG}")" = 1 ]; then
		kc_user="$(sed -n 's/^- name: //p' "${AKS_KUBECONFIG}")"
		case "${kc_user}" in
			"clusterAdmin_${AKS_RESOURCE_GROUP}_${AKS_CLUSTER}"|"clusterUser_${AKS_RESOURCE_GROUP}_${AKS_CLUSTER}")
				if grep -qxF "    user: ${kc_user}" "${AKS_KUBECONFIG}" \
					&& grep -qxF -e "current-context: ${AKS_CLUSTER}-admin" -e "current-context: ${AKS_CLUSTER}" "${AKS_KUBECONFIG}"; then
					if dry; then
						kubeconfig_ours=1
					else
						# Public and private API server names; tsv prints an absent one as None.
						for server in $(az aks show --subscription "${SUB}" -g "${AKS_RESOURCE_GROUP}" -n "${AKS_CLUSTER}" \
							--query '[fqdn, privateFqdn]' -o tsv 2>/dev/null || true); do
							[ "${server}" != None ] || continue
							! grep -qxF "    server: https://${server}:443" "${AKS_KUBECONFIG}" || kubeconfig_ours=1
						done
					fi
				fi ;;
		esac
	fi
	if [ -n "${kubeconfig_ours}" ]; then
		echo "    The kubeconfig file ${AKS_KUBECONFIG} is removed${AKS_DRY_RUN:+ (dry run: its API server is not checked)}."
	elif [ -f "${AKS_KUBECONFIG}" ]; then
		echo "    The kubeconfig file ${AKS_KUBECONFIG} is kept: it is not one create-aks wrote for this cluster alone (or the cluster is gone, so its API server cannot be checked)."
	fi

	if [ -z "${own_group}${own_cluster}${own_acr}${own_ip}${kubeconfig_ours}" ]; then
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
		# After the cluster: its load balancer holds the address until the cluster is gone.
		[ -z "${own_ip}" ] || run az network public-ip delete --subscription "${SUB}" --resource-group "${AKS_RESOURCE_GROUP}" --name "${AKS_PUBLIC_IP}"
	fi
	[ -z "${kubeconfig_ours}" ] || run rm -f "${AKS_KUBECONFIG}"
	say "deleted${AKS_DRY_RUN:+ (dry run: nothing was deleted)}"
}

case "${1:-}" in
	create) create ;;
	delete) delete ;;
	*) echo "usage: $0 create|delete   (settings are AKS_* environment variables; see the header)" >&2; exit 2 ;;
esac
