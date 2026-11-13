# AKS as an example cluster for AEI. Included by the root Makefile; all the logic is
# in hack/aks/cluster.sh. AEI needs only a kubeconfig (see `deploy`), so nothing
# outside this directory knows about Azure.
#
# Settings are AKS_* variables, given on the make command line or in the environment.
# The script holds their defaults and documents each one. The common ones:
#     AKS_RESOURCE_GROUP=aei  AKS_CLUSTER=aei  AKS_LOCATION=westus2  AKS_SUBSCRIPTION=
#     AKS_SYSTEM_NODE_COUNT=1 AKS_SYSTEM_VM_SIZE=Standard_D2s_v3
#     AKS_KATA_NODE_COUNT=2   AKS_KATA_VM_SIZE=Standard_D4s_v3
#     AKS_KUBERNETES_VERSION=           (empty: the AKS default)
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
