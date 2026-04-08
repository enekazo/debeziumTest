param(
    [string]$ResourceGroup = "debezium-aci-rg-se",
    [string]$Location = "swedencentral",
    [string]$AcrName = "debezium2gun5tse",
    [string]$StorageAccountName = "",
    [string]$SinkImageTag = "azure-v1",
    [string]$OracleImageTag = "azure-v3",
    [string]$OracleContainerGroup = "oracle-db-aci-se",
    [string]$DebeziumContainerGroup = "debezium-fabric-aci-se",
    [string]$DebeziumOffsetsShareName = "debezium-offsets",
    [string]$DebeziumOffsetsShareQuota = "20",
    [string]$OraclePassword = "",
    [string]$AzureClientSecret = "",
    [string]$FabricTenantId = "",
    [string]$FabricClientId = "",
    [string]$FabricBaseUri = "",
    [switch]$CreateInfra,
    [switch]$BuildImages,
    [switch]$DeployOracle,
    [switch]$DeployDebezium,
    [switch]$Verify
)

$ErrorActionPreference = "Stop"

function Require-Value {
    param(
        [string]$Name,
        [string]$Value
    )
    if ([string]::IsNullOrWhiteSpace($Value)) {
        throw "Missing required value: $Name"
    }
}

function Show-Step {
    param([string]$Text)
    Write-Host "\n=== $Text ===" -ForegroundColor Cyan
}

# If no switches are provided, run full flow.
if (-not ($CreateInfra -or $BuildImages -or $DeployOracle -or $DeployDebezium -or $Verify)) {
    $CreateInfra = $true
    $BuildImages = $true
    $DeployOracle = $true
    $DeployDebezium = $true
    $Verify = $true
}

$repoRoot = Resolve-Path (Join-Path $PSScriptRoot "..")
$oraclePath = Join-Path $repoRoot "oracle"
$loginServer = "$AcrName.azurecr.io"
$sinkImage = "$loginServer/fabric-sink:$SinkImageTag"
$oracleImage = "$loginServer/oracle-free-custom:$OracleImageTag"

Show-Step "Azure account"
az account show --query "{name:name,id:id,tenantId:tenantId}" -o table | Out-Host

if ($CreateInfra) {
    Show-Step "Create resource group"
    az group create --name $ResourceGroup --location $Location -o none

    Show-Step "Create ACR"
    az acr create --resource-group $ResourceGroup --name $AcrName --sku Basic --admin-enabled true --location $Location -o none

    Show-Step "Create Storage Account for Debezium persistent state"
    if ([string]::IsNullOrWhiteSpace($StorageAccountName)) {
        # Generate a unique storage account name if not provided
        $randomSuffix = -join ((48..57) + (97..122) | Get-Random -Count 8 | % {[char]$_})
        $StorageAccountName = "dbzstate$randomSuffix"
    }
    az storage account create `
        --resource-group $ResourceGroup `
        --name $StorageAccountName `
        --location $Location `
        --sku Standard_LRS `
        --kind StorageV2 `
        -o none

    Show-Step "Create Azure File Share for Debezium offsets"
    az storage share-rm create `
        --resource-group $ResourceGroup `
        --storage-account $StorageAccountName `
        --name $DebeziumOffsetsShareName `
        --quota $DebeziumOffsetsShareQuota `
        -o table | Out-Host

    Write-Host "Storage account created: $StorageAccountName" -ForegroundColor Green
}

if ($BuildImages) {
    Show-Step "Build Debezium sink image in ACR"
    Push-Location $repoRoot
    az acr build --registry $AcrName --image "fabric-sink:$SinkImageTag" .
    Pop-Location

    Show-Step "Build Oracle custom image in ACR"
    Push-Location $oraclePath
    az acr build --registry $AcrName --image "oracle-free-custom:$OracleImageTag" .
    Pop-Location
}

$acrUser = az acr credential show -n $AcrName --query username -o tsv
$acrPass = az acr credential show -n $AcrName --query "passwords[0].value" -o tsv

if ($DeployOracle) {
    Require-Value -Name "OraclePassword" -Value $OraclePassword

    Show-Step "Redeploy Oracle ACI"
    az container delete --resource-group $ResourceGroup --name $OracleContainerGroup --yes 2>$null

    az container create \
        --resource-group $ResourceGroup \
        --name $OracleContainerGroup \
        --location $Location \
        --os-type Linux \
        --image $oracleImage \
        --registry-login-server $loginServer \
        --registry-username $acrUser \
        --registry-password $acrPass \
        --cpu 2 \
        --memory 4 \
        --ports 1521 \
        --ip-address Public \
        --restart-policy OnFailure \
        --environment-variables ORACLE_DATABASE=ORCLPDB1 ORACLE_PASSWORD=$OraclePassword \
        -o table | Out-Host
}

if ($DeployDebezium) {
    Require-Value -Name "OraclePassword" -Value $OraclePassword
    Require-Value -Name "AzureClientSecret" -Value $AzureClientSecret
    Require-Value -Name "FabricTenantId" -Value $FabricTenantId
    Require-Value -Name "FabricClientId" -Value $FabricClientId
    Require-Value -Name "FabricBaseUri" -Value $FabricBaseUri

    # If storage account name not provided, find the existing one
    if ([string]::IsNullOrWhiteSpace($StorageAccountName)) {
        $StorageAccountName = az storage account list -g $ResourceGroup --query "[0].name" -o tsv
        if ([string]::IsNullOrWhiteSpace($StorageAccountName)) {
            throw "Storage account not found. Please ensure CreateInfra ran successfully or provide StorageAccountName parameter."
        }
        Write-Host "Using existing storage account: $StorageAccountName" -ForegroundColor Cyan
    }

    $oracleIp = az container show -g $ResourceGroup -n $OracleContainerGroup --query ipAddress.ip -o tsv
    Require-Value -Name "OracleContainer IP" -Value $oracleIp

    Show-Step "Redeploy Debezium ACI with persistent state volume"
    az container delete --resource-group $ResourceGroup --name $DebeziumContainerGroup --yes 2>$null

    az container create `
        --resource-group $ResourceGroup `
        --name $DebeziumContainerGroup `
        --location $Location `
        --os-type Linux `
        --image $sinkImage `
        --registry-login-server $loginServer `
        --registry-username $acrUser `
        --registry-password $acrPass `
        --cpu 2 `
        --memory 4 `
        --ports 8080 `
        --ip-address Public `
        --restart-policy OnFailure `
        --azure-file-volume-account-name $StorageAccountName `
        --azure-file-volume-share-name $DebeziumOffsetsShareName `
        --azure-file-volume-mount-path /debezium/data `
        --environment-variables `
            AZURE_CLIENT_SECRET=$AzureClientSecret `
            ORACLE_PASSWORD=$OraclePassword `
            FABRIC_SP_TENANTID=$FabricTenantId `
            FABRIC_SP_CLIENTID=$FabricClientId `
            FABRIC_LANDING_BASEURI=$FabricBaseUri `
            DEBEZIUM_SOURCE_DATABASE_HOSTNAME=$oracleIp `
            DEBEZIUM_SOURCE_DATABASE_PORT=1521 `
            DEBEZIUM_SOURCE_DATABASE_DBNAME=FREE `
            DEBEZIUM_SOURCE_DATABASE_PDB_NAME=ORCLPDB1 `
            DEBEZIUM_SOURCE_DATABASE_USER=c##dbzuser `
            DEBEZIUM_SINK_TYPE=fabric `
        -o table | Out-Host

    Write-Host "Debezium container deployed with persistent state mounted at /debezium/data" -ForegroundColor Green
}

if ($Verify) {
    Show-Step "Current deployments"
    az container list -g $ResourceGroup --query "[].{name:name,image:containers[0].image,state:containers[0].instanceView.currentState.state,ip:ipAddress.ip}" -o table | Out-Host

    Show-Step "Oracle logs"
    az container logs -g $ResourceGroup -n $OracleContainerGroup --container-name $OracleContainerGroup | Out-Host

    Show-Step "Debezium logs"
    az container logs -g $ResourceGroup -n $DebeziumContainerGroup --container-name $DebeziumContainerGroup | Out-Host
}

Show-Step "Done"
Write-Host "Resource Group: $ResourceGroup"
Write-Host "ACR: $AcrName"
Write-Host "Oracle Image: $oracleImage"
Write-Host "Debezium Image: $sinkImage"
if (-not [string]::IsNullOrWhiteSpace($StorageAccountName)) {
    Write-Host "Storage Account: $StorageAccountName"
    Write-Host "Debezium Offsets Share: $DebeziumOffsetsShareName (Quota: $($DebeziumOffsetsShareQuota) GB)"
    Write-Host "Persistent mount point: /debezium/data"
}
