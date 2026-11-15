# hack/aks: an AKS cluster for AEI

AKS is one example of a cluster that satisfies AEI; nothing outside this directory
knows about Azure. `make create-aks` / `make delete-aks` ([aks.mk](aks.mk),
[cluster.sh](cluster.sh)) are the supported path; the top-level
[README](../../README.md#example-an-aks-cluster) describes them.

## Bicep instead of the script (prototype)

[main.bicep](main.bicep) declares the same Azure resources `create-aks` creates, with
the same defaults:

- the AKS cluster: system pool (`Standard_D2s_v7`, Azure Linux) and Kata pool
  (`workloadRuntime: KataVmIsolation`, `Standard_D4s_v7`, autoscaling 1-3, taint
  `aei.dev/agent-runs=true:NoSchedule`), upgrade channels `stable` / `NodeImage`,
  system-assigned identity, no SSH key;
- a Basic container registry, and AcrPull on it for the kubelet identity (what
  `--attach-acr` does);
- only when `publicIpName` is set: a Standard static IPv4 public IP (with
  `publicIpDnsLabel`, a DNS name), and Network Contributor on it for the cluster
  identity.

Each resource carries the `aei-created-by=make-create-aks` tag, so `make delete-aks`
removes them as it does the script's. Parameters are named after the `AKS_*`
variables (see the `@description` of each). The file names no subscription, tenant
or secret; pass real values with `-p` on the command line, and do not commit a
`.bicepparam` file that holds them.

```sh
az group create -n aei -l westus2 --tags aei-created-by=make-create-aks
az deployment group what-if -g aei -f hack/aks/main.bicep -p publicIpName=aei-ip   # preview
az deployment group create  -g aei -f hack/aks/main.bicep -p publicIpName=aei-ip   # bills
az aks get-credentials -g aei -n aei --file ~/.kube/aei-aks-aei.kubeconfig
```

How the two compare:

| | `make create-aks` (script) | `main.bicep` |
|---|---|---|
| Resource group | creates it, tagged | must exist; create it yourself (tagged, so `delete-aks` removes the whole group) |
| Preview | `AKS_DRY_RUN=1` prints az commands | `what-if` asks Azure for the real diff |
| Re-run on an existing cluster | reuses an existing pool as it is (counts, taints) | re-applies every declared setting: a redeploy with other pool settings changes the live pools, and can be refused while the autoscaler or another operation is busy |
| Order of operations | create cluster, add Kata pool, `az aks update` (attach registry, reconcile), retry while the cluster is busy | one deployment; Azure orders the resources |
| Registry name default | `aei` + sha256 of subscription/group/cluster | `aei` + `uniqueString(group, cluster)`; pass `acrName` to match, or `AKS_ACR_NAME` to `delete-aks` when it keeps the group |
| Existing registry (`AKS_ATTACH_ACR`) | yes | not modelled (`createAcr=false` and assign AcrPull yourself) |
| Preflight (az version, provider registration, subscription shown) | yes | no; a missing provider fails the deployment |
| kubeconfig, Kata smoke pod, StorageClass report, next-step commands | yes | no; run them after the deployment |

Either way, the role assignments need Owner or User Access Administrator on the
resource group.

What has been checked: `az bicep build` and `az bicep lint` pass, and a `what-if`
(at subscription scope, with a throwaway wrapper that declares the resource group, so
nothing was created) shows the resource group plus registry, AcrPull assignment and
cluster, and with `publicIpName` also the public IP and its Network Contributor
assignment, with the pool settings and tags above. It has NOT been deployed. Still
open: whether the `kata-vm-isolation` RuntimeClass appears when the Kata pool is part
of the cluster's creation (the script follows the AKS docs and runs `az aks update`
after adding the pool), and the effect of redeploying onto a running cluster.

Why keep the script: the steps Bicep cannot do (preflight, kubeconfig, the smoke pod,
the printed `make deploy` command) are most of `create-aks`, and its reuse-as-is
behavior is safer for a cluster that may have agents running. Bicep is a good fit if
the cluster should be managed declaratively, for example from CI with `what-if` on
every change; `cluster.sh` could then deploy `main.bicep` and keep only its
before-and-after steps.
