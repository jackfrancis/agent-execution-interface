# hack/aks: an AKS cluster for AEI

AKS is one example of a cluster that satisfies AEI; nothing outside this directory
knows about Azure. `make create-aks` / `make delete-aks` ([aks.mk](aks.mk),
[cluster.sh](cluster.sh)) are the supported path; the top-level
[README](../../README.md#example-an-aks-cluster) describes them.

## Entra ID user credentials

`create-aks` writes the cluster's local admin certificate. For a kubeconfig that signs
in with your own Entra identity instead (for example on a cluster whose local accounts
are disabled, where `create-aks` stops before changing anything), you need:

- kubelogin (`az aks install-cli`, or your package manager);
- permission to download the user kubeconfig, an Azure role on the cluster resource
  such as "Azure Kubernetes Service Cluster User Role" (Owner and Contributor include
  it); without it, `az aks get-credentials` fails with AuthorizationFailed;
- access inside the cluster, which is separate: membership in one of its admin groups
  (`AKS_ENTRA_ADMIN_GROUPS`, `entraAdminGroups`), or an Azure RBAC role on it such as
  "Azure Kubernetes Service RBAC Cluster Admin".

```sh
KC=~/.kube/aei-aks-aei-user.kubeconfig
KUBECONFIG=$KC az aks get-credentials --subscription <subscription> -g aei -n aei --file $KC
KUBECONFIG=$KC kubelogin convert-kubeconfig --login azurecli --kubeconfig $KC
```

Set `KUBECONFIG` on both: after fetching such a kubeconfig, Azure CLI runs
`kubelogin convert-kubeconfig` itself, without `--kubeconfig` (so the second command
may find nothing left to do), and kubelogin converts whatever `KUBECONFIG` names,
which would otherwise be your own kubeconfig. The `azurecli` login signs kubectl in as
az's active account, and works only with AKS-managed Entra ID, not the retired legacy
integration. `create-aks` does not run these commands, and they have not been tested
here.

## Bicep instead of the script (prototype)

[main.bicep](main.bicep) declares the same Azure resources `create-aks` creates, with
the same defaults:

- the AKS cluster: system pool (`Standard_D2s_v7`, Azure Linux) and Kata pool
  (`workloadRuntime: KataVmIsolation`, `Standard_D4s_v7`, autoscaling 1-3, taint
  `aei.dev/agent-runs=true:NoSchedule`), upgrade channels `stable` / `NodeImage`,
  system-assigned identity, no SSH key, Microsoft Entra ID with Azure RBAC
  (`entraId`, `entraAdminGroups`; added after the check below, and not deployed from
  this file since);
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

**Redeploying a cluster made before `entraId` existed:** the template now turns
Microsoft Entra ID on by default, and on an existing cluster that cannot be undone.
For a cluster without Entra ID that should stay that way, pass `-p entraId=false` on
every deployment; `what-if` shows the change (an `aadProfile` added) if you forget.

```sh
az group create -n aei -l westus2 --tags aei-created-by=make-create-aks
az deployment group what-if -g aei -f hack/aks/main.bicep -p publicIpName=aei-ip   # preview
az deployment group create  -g aei -f hack/aks/main.bicep -p publicIpName=aei-ip   # bills
az aks get-credentials -g aei -n aei --admin --file ~/.kube/aei-aks-aei.kubeconfig
```

`--admin` is the cluster's local admin certificate, which `create-aks` writes too.
Without it, on a cluster with Entra ID (`entraId`, the default), you get a kubeconfig
for your Entra identity instead: see [Entra ID user credentials](#entra-id-user-credentials)
for what that needs, and set `KUBECONFIG` on that command as shown there.

How the two compare:

| | `make create-aks` (script) | `main.bicep` |
|---|---|---|
| Resource group | creates it, tagged | must exist; create it yourself (tagged, so `delete-aks` removes the whole group) |
| Preview | `AKS_DRY_RUN=1` prints az commands | `what-if` asks Azure for the diff; exact for a new group, but on a live cluster it lists every server-side default the template omits as a "Delete" (a redeploy does not actually remove them) |
| Re-run on an existing cluster | reuses an existing pool as it is (counts, taints) | re-applies every declared setting. With the same parameters it changes nothing, and an autoscaled pool keeps its current count; a redeploy with other pool settings changes the live pools (not tested) |
| Busy cluster | waits and retries for up to 20 minutes | a deployment (and `what-if`) is refused at preflight with OperationNotAllowed; re-run it once the other operation ends |
| Order of operations | create cluster, add Kata pool, `az aks update` (attach registry, reconcile), retry while the cluster is busy | one deployment; Azure orders the resources |
| Registry name default | `aei` + sha256 of subscription/group/cluster | `aei` + `uniqueString(group, cluster)`; pass `acrName` to match, or `AKS_ACR_NAME` to `delete-aks` when it keeps the group |
| Existing registry (`AKS_ATTACH_ACR`) | yes | not modelled (`createAcr=false` and assign AcrPull yourself) |
| Preflight (az version, provider registration, subscription shown) | yes | no; a missing provider fails the deployment |
| kubeconfig, Kata smoke pod, StorageClass report, next-step commands | yes | no; run them after the deployment |

Either way, the role assignments need Owner or User Access Administrator on the
resource group.

What has been checked (2026-11-15, westus2, defaults plus `publicIpName` and
`publicIpDnsLabel`):

- `az bicep build` and `az bicep lint` pass; `what-if` against a group that did not
  exist yet listed exactly the resources above.
- The deployment succeeded first time, in one step. The cluster has both pools as
  declared (the Kata pool with `KataVmIsolation`, autoscaling 1-3 and its taint) and
  the `stable` / `NodeImage` channels.
- The `kata-vm-isolation` RuntimeClass was there without the `az aks update` the
  script runs after adding the pool. A pod with only that RuntimeClass and the
  toleration ran on the Kata pool in a Kata VM (guest kernel differs from the node's),
  from an image in the new registry, pulled with no pull secret (AcrPull works).
- A LoadBalancer Service with the two annotations got the static address (the
  Network Contributor assignment works).
- Minutes after creation, the subscription's Azure Policy installed the Defender
  extension; while it did, `what-if` and a redeploy were refused (the busy-cluster
  row above).
- A redeploy with the same parameters succeeded in about two minutes with no change
  and no node replaced. After the autoscaler had grown the Kata pool to two nodes
  for 25 Kata pods, another redeploy (the template says `count: 1`) left both nodes
  and every pod as they were.

Not tested: redeploying with different pool settings (VM size, taints), which AKS
may refuse on an existing pool or apply by replacing nodes.

Why keep the script: the steps Bicep cannot do (preflight, kubeconfig, the smoke pod,
the printed `make deploy` command) are most of `create-aks`, and its reuse-as-is
behavior is safer for a cluster that may have agents running. Bicep is a good fit if
the cluster should be managed declaratively, for example from CI with `what-if` on
every change; `cluster.sh` could then deploy `main.bicep` and keep only its
before-and-after steps.
