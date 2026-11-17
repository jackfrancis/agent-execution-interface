// The Azure resources `make create-aks` (hack/aks/cluster.sh) creates, declared as
// Bicep: an AKS cluster with a system pool and a Kata (Pod Sandboxing) pool, a
// container registry its kubelet can pull from, and optionally a static public IP the
// cluster may put on a LoadBalancer Service. A PROTOTYPE alongside the script, not a
// replacement: see "Bicep instead of the script" in hack/aks/README.md.
//
// Deployed into an existing resource group (Bicep does not create it here):
//
//   az group create -n aei -l westus2 --tags aei-created-by=make-create-aks
//   az deployment group what-if -g aei -f hack/aks/main.bicep
//   az deployment group create  -g aei -f hack/aks/main.bicep [-p name=value ...]
//
// Everything is a parameter with the same default as the script's AKS_* variable of
// the same meaning. Nothing here names a subscription, a tenant or a secret; keep it
// that way, and keep real parameter values out of the repository.
//
// Every resource carries the tag aei-created-by=make-create-aks, the marker
// `make delete-aks` looks for: it deletes only what has it.

targetScope = 'resourceGroup'

// ---- cluster ----------------------------------------------------------------

@description('AKS cluster name (AKS_CLUSTER).')
param clusterName string = 'aei'

@description('Region for every resource (AKS_LOCATION). Default: the resource group\'s.')
param location string = resourceGroup().location

@description('Kubernetes version to create with (AKS_KUBERNETES_VERSION). Empty: the AKS default. A starting point, not a pin, unless autoUpgradeChannel is none.')
param kubernetesVersion string = ''

@description('Automatic Kubernetes upgrades (AKS_AUTO_UPGRADE_CHANNEL).')
@allowed(['stable', 'patch', 'rapid', 'node-image', 'none'])
param autoUpgradeChannel string = 'stable'

@description('Automatic node OS image upgrades (AKS_NODE_OS_UPGRADE_CHANNEL).')
@allowed(['NodeImage', 'SecurityPatch', 'Unmanaged', 'None'])
param nodeOSUpgradeChannel string = 'NodeImage'

// ---- system pool: the AEI controller and cluster add-ons ----------------------

@description('System pool name (AKS_SYSTEM_POOL). At most 12 lowercase alphanumerics.')
@maxLength(12)
param systemPoolName string = 'system'

@description('System pool node count (AKS_SYSTEM_NODE_COUNT).')
@minValue(1)
param systemNodeCount int = 1

@description('System pool VM size (AKS_SYSTEM_VM_SIZE).')
param systemVmSize string = 'Standard_D2s_v7'

// ---- Kata pool: where isolated agent runs execute -----------------------------

@description('Kata pool name (AKS_KATA_POOL). At most 12 lowercase alphanumerics.')
@maxLength(12)
param kataPoolName string = 'kata'

@description('Kata pool VM size (AKS_KATA_VM_SIZE): Generation 2 with nested virtualization.')
param kataVmSize string = 'Standard_D4s_v7'

@description('Kata pool starting node count (AKS_KATA_NODE_COUNT).')
@minValue(0)
param kataNodeCount int = 1

@description('Let the cluster autoscaler size the Kata pool between kataMinCount and kataMaxCount (AKS_KATA_AUTOSCALE).')
param kataAutoscale bool = true

@description('Kata pool autoscaling minimum (AKS_KATA_MIN_COUNT).')
@minValue(0)
param kataMinCount int = 1

@description('Kata pool autoscaling maximum (AKS_KATA_MAX_COUNT).')
@minValue(1)
param kataMaxCount int = 3

@description('Taint on the Kata pool\'s nodes, key=value:Effect (AKS_KATA_TAINT). Empty: none. Pass the same value to `make deploy` as RUNTIME_TOLERATIONS.')
param kataTaint string = 'aei.dev/agent-runs=true:NoSchedule'

// ---- registry -----------------------------------------------------------------

@description('Create a container registry and give the kubelet identity AcrPull on it (AKS_CREATE_ACR).')
param createAcr bool = true

@description('Registry name (AKS_ACR_NAME): globally unique, 5-50 lowercase alphanumerics. The default is derived from the resource group and cluster; it is NOT the name cluster.sh derives, so pass AKS_ACR_NAME=<this> to `make delete-aks` when its resource group is kept.')
@minLength(5)
@maxLength(50)
param acrName string = 'aei${uniqueString(resourceGroup().id, clusterName)}'

@description('Registry SKU (AKS_ACR_SKU).')
@allowed(['Basic', 'Standard', 'Premium'])
param acrSku string = 'Basic'

// ---- optional static public IP for an app's LoadBalancer Service ------------------

@description('Name of a Standard, static IPv4 public IP to create (AKS_PUBLIC_IP). Empty: none.')
param publicIpName string = ''

@description('DNS label: the IP also gets <label>.<location>.cloudapp.azure.com (AKS_PUBLIC_IP_DNS_LABEL). Empty: none.')
param publicIpDnsLabel string = ''

@description('Microsoft Entra ID sign-in with Azure RBAC for Kubernetes authorization (AKS_ENTRA_ID). Some subscriptions\' Azure Policy requires it. Turning it on later cannot be undone.')
param entraId bool = true

@description('Entra group object ids made cluster admins (AKS_ENTRA_ADMIN_GROUPS). Empty: none.')
param entraAdminGroups array = []

// -------------------------------------------------------------------------------

// The ownership marker cluster.sh puts on what it creates; delete-aks keys on it.
var tags = {
  'aei-created-by': 'make-create-aks'
}

// key=value:Effect, the format AKS takes in nodeTaints (as `--node-taints` does).
var kataTaints = empty(kataTaint) ? [] : [kataTaint]

var createPublicIp = !empty(publicIpName)

// Built-in role definition ids (the same in every subscription).
var acrPullRoleId = '7f951dda-4ed3-4680-a7ca-43fe172d538d'
var networkContributorRoleId = '4d97b98b-1d4f-4787-a291-c67834d212e7'

resource acr 'Microsoft.ContainerRegistry/registries@2025-11-01' = if (createAcr) {
  name: acrName
  location: location
  tags: tags
  sku: {
    name: acrSku
  }
  properties: {
    // Nodes pull with their kubelet identity (AcrPull below); no admin user.
    adminUserEnabled: false
  }
}

resource cluster 'Microsoft.ContainerService/managedClusters@2026-08-01' = {
  name: clusterName
  location: location
  tags: tags
  identity: {
    type: 'SystemAssigned'
  }
  properties: {
    kubernetesVersion: empty(kubernetesVersion) ? null : kubernetesVersion
    dnsPrefix: '${take(clusterName, 10)}-${take(uniqueString(resourceGroup().id), 6)}'
    autoUpgradeProfile: {
      upgradeChannel: autoUpgradeChannel
      nodeOSUpgradeChannel: nodeOSUpgradeChannel
    }
    // No linuxProfile: nodes get no SSH key (cluster.sh passes --no-ssh-key).
    // Local accounts stay enabled, so `az aks get-credentials --admin` keeps working.
    aadProfile: entraId ? {
      managed: true
      enableAzureRBAC: true
      adminGroupObjectIDs: entraAdminGroups
    } : null
    agentPoolProfiles: [
      {
        name: systemPoolName
        mode: 'System'
        count: systemNodeCount
        vmSize: systemVmSize
        osType: 'Linux'
        osSKU: 'AzureLinux'
        type: 'VirtualMachineScaleSets'
      }
      {
        // Pod Sandboxing: AKS's 'kata-vm-isolation' RuntimeClass schedules only
        // onto pools with this workload runtime.
        name: kataPoolName
        mode: 'User'
        count: kataNodeCount
        enableAutoScaling: kataAutoscale
        minCount: kataAutoscale ? kataMinCount : null
        maxCount: kataAutoscale ? kataMaxCount : null
        vmSize: kataVmSize
        osType: 'Linux'
        osSKU: 'AzureLinux'
        workloadRuntime: 'KataVmIsolation'
        nodeTaints: kataTaints
        type: 'VirtualMachineScaleSets'
      }
    ]
  }
}

// What `az aks update --attach-acr` does: AcrPull for the kubelet identity, scoped to
// the one registry, so nodes pull without pull secrets.
resource acrPull 'Microsoft.Authorization/roleAssignments@2022-04-01' = if (createAcr) {
  name: guid(acr.id, cluster.id, acrPullRoleId)
  scope: acr
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', acrPullRoleId)
    principalId: cluster.properties.identityProfile.kubeletidentity.objectId
    principalType: 'ServicePrincipal'
  }
}

// In this resource group, not the node resource group, so it outlives any one
// Service (and the cluster).
resource publicIp 'Microsoft.Network/publicIPAddresses@2026-03-01' = if (createPublicIp) {
  name: createPublicIp ? publicIpName : 'unused'
  location: location
  tags: tags
  sku: {
    name: 'Standard'
  }
  properties: {
    publicIPAddressVersion: 'IPv4'
    publicIPAllocationMethod: 'Static'
    dnsSettings: empty(publicIpDnsLabel) ? null : {
      domainNameLabel: publicIpDnsLabel
    }
  }
}

// The cloud provider acts as the cluster's own identity when it puts an address from
// outside the node resource group on a load balancer; it needs Network Contributor
// on that address.
resource publicIpNetworkContributor 'Microsoft.Authorization/roleAssignments@2022-04-01' = if (createPublicIp) {
  name: guid(publicIp.id, cluster.id, networkContributorRoleId)
  scope: publicIp
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', networkContributorRoleId)
    principalId: cluster.identity.principalId
    principalType: 'ServicePrincipal'
  }
}

output clusterName string = cluster.name
output acrName string = createAcr ? acr.name : ''
output acrLoginServer string = createAcr ? acr!.properties.loginServer : ''
output kataTaint string = kataTaint
output publicIpAddress string = createPublicIp ? publicIp!.properties.ipAddress : ''
output publicIpFqdn string = createPublicIp && !empty(publicIpDnsLabel) ? publicIp!.properties.dnsSettings.fqdn : ''
