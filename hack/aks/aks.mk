# AKS as an example cluster for AEI. Included by the root Makefile; all the logic is
# in hack/aks/cluster.sh. AEI needs only a kubeconfig (see `deploy`), so nothing
# outside this directory knows about Azure.
#
# Settings are AKS_* variables, given on the make command line or in the environment.
# The script holds their defaults and documents each one. The common ones:
#     AKS_RESOURCE_GROUP=aei  AKS_CLUSTER=aei  AKS_LOCATION=westus2  AKS_SUBSCRIPTION=
#     AKS_SYSTEM_NODE_COUNT=1 AKS_SYSTEM_VM_SIZE=Standard_D2s_v7
#     AKS_KATA_NODE_COUNT=1   AKS_KATA_VM_SIZE=Standard_D4s_v7
#     AKS_KATA_MIN_COUNT=1    AKS_KATA_MAX_COUNT=3   the Kata pool's autoscaling range
#     AKS_KATA_AUTOSCALE=true           (false: a fixed pool of AKS_KATA_NODE_COUNT)
#     AKS_KATA_TAINT=aei.dev/agent-runs=true:NoSchedule   reserves the Kata pool for
#                                       agent runs (empty: no taint)
#     AKS_KUBERNETES_VERSION=           (empty: the AKS default; a starting point)
#     AKS_AUTO_UPGRADE_CHANNEL=stable   AKS_NODE_OS_UPGRADE_CHANNEL=NodeImage
#                                       automatic Kubernetes / node OS image upgrades
#     AKS_KUBECONFIG=~/.kube/aei-aks-<cluster>.kubeconfig
#     AKS_CREATE_ACR=true | AKS_ATTACH_ACR=<existing registry> | AKS_ACR_NAME=<name>
#     AKS_DRY_RUN=1                     print the az commands, run nothing
# Use AKS_DRY_RUN=1, not `make -n`, to preview.

# Variables set on the make command line reach the script through the environment
# (make exports them); nothing is redeclared here, so the defaults live in one place.
.PHONY: create-aks
create-aks: ## create/reuse an AKS cluster that satisfies AEI (Kata pool + registry); writes a kubeconfig file. AKS_DRY_RUN=1 previews
	@hack/aks/cluster.sh create

.PHONY: delete-aks
delete-aks: ## delete what create-aks made; prints the plan and stops unless AKS_CONFIRM_DELETE=<resource group>
	@hack/aks/cluster.sh delete
