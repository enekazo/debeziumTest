# Debezium Fabric Deployment Guide

## Overview

The `deploy-azure-services.ps1` script automates the deployment of the Debezium CDC pipeline to Azure, including:
- Resource Group and Azure Container Registry (ACR)
- Storage Account with persistent Azure File Share for Debezium state
- Oracle Container Instance (ACI)
- Debezium Sink Container Instance with volume mount for persistent state management

## Key Features

### Persistent State Management
The script now creates an Azure File Share that is mounted to the Debezium container at `/debezium/data`. This ensures:
- Debezium offset files (`offsets.dat`) survive container restarts
- Schema history (`schemahistory.dat`) is preserved
- Prevents unintended re-snapshots when containers are restarted
- Enables clean recovery scenarios

## Prerequisites

1. Azure CLI installed and authenticated
2. PowerShell 5.1 or higher
3. Access to Azure subscription with permissions to create:
   - Resource Groups
   - Container Registries (ACR)
   - Storage Accounts
   - Container Instances (ACI)

## Usage

### Full Deployment (Recommended for Initial Setup)

```powershell
.\deploy-azure-services.ps1 `
    -ResourceGroup "debezium-aci-rg-se" `
    -Location "swedencentral" `
    -AcrName "debezium2gun5tse" `
    -SinkImageTag "azure-v7" `
    -OracleImageTag "azure-v3" `
    -OraclePassword "YourOraclePassword" `
    -AzureClientSecret "YourServicePrincipalSecret" `
    -FabricTenantId "8d66ab52-dbaf-4bdc-8178-ce448361b104" `
    -FabricClientId "80586b13-4e01-4f9c-8649-d74b26828f64" `
    -FabricBaseUri "abfss://workspace-id@onelake.dfs.fabric.microsoft.com/item-id/Files/LandingZone" `
    -CreateInfra `
    -BuildImages `
    -DeployOracle `
    -DeployDebezium `
    -Verify
```

### Step-by-Step Deployment

#### Step 1: Create Infrastructure
```powershell
.\deploy-azure-services.ps1 `
    -CreateInfra `
    -Verify
```

This will:
- Create Resource Group
- Create Azure Container Registry (ACR)
- Create Storage Account (with auto-generated name or your specified `StorageAccountName`)
- Create Azure File Share `debezium-offsets` with 20 GB quota

#### Step 2: Build Images
```powershell
.\deploy-azure-services.ps1 `
    -BuildImages `
    -SinkImageTag "azure-v7" `
    -OracleImageTag "azure-v3"
```

#### Step 3: Deploy Oracle
```powershell
.\deploy-azure-services.ps1 `
    -DeployOracle `
    -OraclePassword "YourOraclePassword" `
    -Verify
```

#### Step 4: Deploy Debezium
```powershell
.\deploy-azure-services.ps1 `
    -DeployDebezium `
    -OraclePassword "YourOraclePassword" `
    -AzureClientSecret "YourServicePrincipalSecret" `
    -FabricTenantId "YourTenantId" `
    -FabricClientId "YourClientId" `
    -FabricBaseUri "YourOneLakeUri" `
    -Verify
```

## Parameters

### Common Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `ResourceGroup` | string | debezium-aci-rg-se | Azure Resource Group name |
| `Location` | string | swedencentral | Azure region |
| `AcrName` | string | debezium2gun5tse | Azure Container Registry name |
| `StorageAccountName` | string | *auto-generated* | Storage account name (auto-generated if empty) |

### Storage & State Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `DebeziumOffsetsShareName` | string | debezium-offsets | Azure File Share name for Debezium offsets |
| `DebeziumOffsetsShareQuota` | string | 20 | File share quota in GB |

### Container Configuration

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `SinkImageTag` | string | azure-v1 | Docker image tag for Debezium sink |
| `OracleImageTag` | string | azure-v3 | Docker image tag for Oracle |
| `OracleContainerGroup` | string | oracle-db-aci-se | Oracle ACI container group name |
| `DebeziumContainerGroup` | string | debezium-fabric-aci-se | Debezium ACI container group name |

### Required for Debezium Deployment

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `OraclePassword` | string | Yes | Oracle database password |
| `AzureClientSecret` | string | Yes | Service Principal client secret |
| `FabricTenantId` | string | Yes | Entra ID tenant ID |
| `FabricClientId` | string | Yes | Service Principal client ID |
| `FabricBaseUri` | string | Yes | OneLake landing zone base URI |

### Execution Flags

| Flag | Description |
|------|-------------|
| `-CreateInfra` | Create resource group, ACR, storage account, and file share |
| `-BuildImages` | Build Docker images and push to ACR |
| `-DeployOracle` | Deploy Oracle container instance |
| `-DeployDebezium` | Deploy Debezium container instance |
| `-Verify` | Display logs and verify deployments |

## Persistent State Features

### What Gets Persisted

The Azure File Share mounted at `/debezium/data` in the Debezium container persists:

- **offsets.dat**: CDC offset tracking (prevents re-snapshot on restart)
- **schemahistory.dat**: Schema version history
- Any other Debezium state files

### Benefits

1. **Prevents Re-snapshots**: Container restart doesn't trigger full data reload
2. **Clean Recovery**: Can reset state by deleting files from the persistent share
3. **Operational Stability**: Offsets survive Azure infrastructure operations

### Resetting State

To perform a clean reload:

```powershell
# 1. Stop the container
az container stop -g debezium-aci-rg-se -n debezium-fabric-aci-se

# 2. Delete persistent offset files
az storage file delete --share-name debezium-offsets --path offsets.dat
az storage file delete --share-name debezium-offsets --path schemahistory.dat

# 3. Restart for fresh snapshot
az container start -g debezium-aci-rg-se -n debezium-fabric-aci-se
```

## Examples

### Example 1: Initial Setup in Sweden Central

```powershell
.\deploy-azure-services.ps1 `
    -ResourceGroup "mycompany-dbz-rg" `
    -Location "swedencentral" `
    -AcrName "mycompanydbz" `
    -SinkImageTag "azure-v7" `
    -OracleImageTag "azure-v3" `
    -OraclePassword "MySecureOraclePass" `
    -AzureClientSecret "my-sp-secret" `
    -FabricTenantId "8d66ab52-dbaf-4bdc-8178-ce448361b104" `
    -FabricClientId "80586b13-4e01-4f9c-8649-d74b26828f64" `
    -FabricBaseUri "abfss://b5e0c504-3815-4509-bd2b-e025cb9342e7@onelake.dfs.fabric.microsoft.com/d0d63a31-e0c6-49ee-92c1-5df7d53d26af/Files/LandingZone"
```

### Example 2: Rebuild and Redeploy Only

```powershell
.\deploy-azure-services.ps1 `
    -BuildImages `
    -DeployDebezium `
    -SinkImageTag "azure-v8" `
    -OraclePassword "MySecureOraclePass" `
    -AzureClientSecret "my-sp-secret" `
    -FabricTenantId "8d66ab52-dbaf-4bdc-8178-ce448361b104" `
    -FabricClientId "80586b13-4e01-4f9c-8649-d74b26828f64" `
    -FabricBaseUri "abfss://b5e0c504-3815-4509-bd2b-e025cb9342e7@onelake.dfs.fabric.microsoft.com/d0d63a31-e0c6-49ee-92c1-5df7d53d26af/Files/LandingZone"
```

### Example 3: Infrastructure Only (DevOps Step)

```powershell
.\deploy-azure-services.ps1 `
    -CreateInfra `
    -Verify
```

## Troubleshooting

### Storage Account Not Found

If you see "Storage account not found" error when deploying Debezium:

1. Manually specify the storage account:
```powershell
.\deploy-azure-services.ps1 `
    -DeployDebezium `
    -StorageAccountName "your-existing-storage-account" `
    ...
```

Or run `-CreateInfra` first to create it.

### File Share Mount Failures

Check container events:
```powershell
az container show -g debezium-aci-rg-se -n debezium-fabric-aci-se --query "containers[0].instanceView.events" -o table
```

Look for "Successfully mounted Azure File Volume" message.

### Persistent State Not Working

Verify the mount:
```powershell
az container exec -g debezium-aci-rg-se -n debezium-fabric-aci-se -c debezium-fabric-aci-se --exec-command "ls -la /debezium/data"
```

You should see `offsets.dat` and `schemahistory.dat` after Debezium runs.

## Output Example

```
=== Done ===
Resource Group: debezium-aci-rg-se
ACR: debezium2gun5tse
Oracle Image: debezium2gun5tse.azurecr.io/oracle-free-custom:azure-v3
Debezium Image: debezium2gun5tse.azurecr.io/fabric-sink:azure-v7
Storage Account: dbzstate12345678
Debezium Offsets Share: debezium-offsets (Quota: 20 GB)
Persistent mount point: /debezium/data
```

## References

- [Azure Container Instances Documentation](https://learn.microsoft.com/en-us/azure/container-instances/)
- [Azure Files Documentation](https://learn.microsoft.com/en-us/azure/storage/files/)
- [Debezium Oracle Connector](https://debezium.io/documentation/reference/stable/connectors/oracle.html)
