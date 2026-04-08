# Oracle + Debezium on Azure ACI Runbook

This runbook covers a full Azure deployment of Oracle and Debezium containers for Oracle CDC to Fabric, including:
- exact scripts and commands to run
- Oracle ARCHIVELOG and supplemental logging requirements
- required Oracle permissions for Debezium
- test data generation and CDC validation

## 1. Prerequisites

- Azure CLI installed and authenticated
- Access to Azure subscription and resource group creation rights
- Oracle SQLcl installed locally
- This repo available locally
- Fabric service principal values ready

Repo paths used in this runbook:
- `scripts/deploy-azure-services.ps1`
- `oracle/init/`
- `config/application.properties`

## 2. Set deployment variables (PowerShell)

Run in PowerShell from repo root:

```powershell
$ResourceGroup = "debezium-aci-rg-se"
$Location = "swedencentral"
$AcrName = "debezium2gun5tse"

$OracleContainerGroup = "oracle-db-aci-se"
$DebeziumContainerGroup = "debezium-fabric-aci-se"

$OraclePassword = "<oracle-password>"
$FabricTenantId = "<tenant-id>"
$FabricClientId = "<client-id>"
$AzureClientSecret = "<client-secret>"
$FabricBaseUri = "abfss://<workspaceId>@onelake.dfs.fabric.microsoft.com/<itemId>/Files/LandingZone"
```

## 3. Build and deploy with script

Important: pass image tags explicitly so Oracle uses the ARCHIVELOG-fixed image.

```powershell
pwsh ./scripts/deploy-azure-services.ps1 `
  -ResourceGroup $ResourceGroup `
  -Location $Location `
  -AcrName $AcrName `
  -OracleContainerGroup $OracleContainerGroup `
  -DebeziumContainerGroup $DebeziumContainerGroup `
  -OracleImageTag "azure-v4" `
  -SinkImageTag "azure-v4" `
  -OraclePassword $OraclePassword `
  -FabricTenantId $FabricTenantId `
  -FabricClientId $FabricClientId `
  -AzureClientSecret $AzureClientSecret `
  -FabricBaseUri $FabricBaseUri `
  -CreateInfra -BuildImages -DeployOracle -DeployDebezium -Verify
```

If you only need redeploy steps later, run only required switches:
- Oracle only: `-DeployOracle`
- Debezium only: `-DeployDebezium`
- Status/logs only: `-Verify`

## 4. Verify Oracle is configured correctly

### 4.1 Oracle container status

```powershell
az container show -g $ResourceGroup -n $OracleContainerGroup --query "{state:containers[0].instanceView.currentState.state,ip:ipAddress.ip}" -o json
```

### 4.2 Oracle logs must show ARCHIVELOG setup step

```powershell
az container logs -g $ResourceGroup -n $OracleContainerGroup
```

Expected marker in logs:
- [01] ARCHIVELOG mode and supplemental logging enabled.

### 4.3 Database checks from SQLcl

Get Oracle IP:

```powershell
$OracleIp = az container show -g $ResourceGroup -n $OracleContainerGroup --query ipAddress.ip -o tsv
```

Check log mode and supplemental logging:

```powershell
$sql = @'
SELECT LOG_MODE, SUPPLEMENTAL_LOG_DATA_MIN, SUPPLEMENTAL_LOG_DATA_ALL FROM V$DATABASE;
EXIT;
'@
Set-Content -Path .\oracle_check.sql -Value $sql -Encoding ascii
& sqlcl "system/$OraclePassword@//$OracleIp`:1521/ORCLPDB1" "@./oracle_check.sql"
```

Expected:
- LOG_MODE = ARCHIVELOG
- SUPPLEMENTAL_LOG_DATA_MIN = YES
- SUPPLEMENTAL_LOG_DATA_ALL = YES

## 5. Ensure Debezium user permissions are complete

These permissions are required for stable snapshot and streaming:
- CREATE SESSION
- SET CONTAINER
- LOGMINING
- SELECT ANY TRANSACTION
- SELECT ANY DICTIONARY
- SELECT ANY TABLE
- LOCK ANY TABLE
- EXECUTE_CATALOG_ROLE
- SELECT on V$ views used by LogMiner
- CREATE TABLE and CREATE SEQUENCE (for flush strategy objects)
- EXECUTE on DBMS_METADATA
- QUOTA on USERS

Run this as SYSDBA once after Oracle deploy:

```powershell
$sql = @'
ALTER SESSION SET CONTAINER = ORCLPDB1;
GRANT SELECT_CATALOG_ROLE TO c##dbzuser;
GRANT FLASHBACK ANY TABLE TO c##dbzuser;

ALTER SESSION SET CONTAINER = CDB$ROOT;
GRANT CREATE TABLE TO c##dbzuser CONTAINER=ALL;
GRANT CREATE SEQUENCE TO c##dbzuser CONTAINER=ALL;
GRANT EXECUTE ON DBMS_METADATA TO c##dbzuser CONTAINER=ALL;
ALTER USER c##dbzuser QUOTA UNLIMITED ON USERS CONTAINER=ALL;
EXIT;
'@
Set-Content -Path .\oracle_permissions_fix.sql -Value $sql -Encoding ascii
& sqlcl "sys/$OraclePassword@//$OracleIp`:1521/FREE as sysdba" "@./oracle_permissions_fix.sql"
```

## 6. Verify Debezium container health

```powershell
az container show -g $ResourceGroup -n $DebeziumContainerGroup --query "{state:containers[0].instanceView.currentState.state,restarts:containers[0].instanceView.restartCount,ip:ipAddress.ip}" -o json
```

Expected:
- state = Running
- restarts stays stable (not increasing repeatedly)

If it is crash-looping, redeploy Debezium after permission fix:

```powershell
pwsh ./scripts/deploy-azure-services.ps1 `
  -ResourceGroup $ResourceGroup `
  -Location $Location `
  -AcrName $AcrName `
  -OracleContainerGroup $OracleContainerGroup `
  -DebeziumContainerGroup $DebeziumContainerGroup `
  -SinkImageTag "azure-v4" `
  -OraclePassword $OraclePassword `
  -FabricTenantId $FabricTenantId `
  -FabricClientId $FabricClientId `
  -AzureClientSecret $AzureClientSecret `
  -FabricBaseUri $FabricBaseUri `
  -DeployDebezium -Verify
```

## 7. Add test data (CDC trigger)

Run from SQLcl against ORCLPDB1:

```powershell
$sql = @'
INSERT INTO HR.EMPLOYEES (EMPLOYEE_ID, FIRST_NAME, LAST_NAME, EMAIL, PHONE_NUMBER, HIRE_DATE, JOB_ID, SALARY, COMMISSION_PCT, MANAGER_ID, DEPARTMENT_ID)
VALUES (999, 'CDC', 'VALIDATION', 'CDCVALID', '515.123.4567', SYSDATE, 'IT_PROG', 9000, NULL, 103, 60);
COMMIT;

UPDATE HR.EMPLOYEES SET SALARY = 9116 WHERE EMPLOYEE_ID = 999;
COMMIT;

SELECT EMPLOYEE_ID, SALARY, ORA_ROWSCN FROM HR.EMPLOYEES WHERE EMPLOYEE_ID = 999;
EXIT;
'@
Set-Content -Path .\oracle_test_data.sql -Value $sql -Encoding ascii
& sqlcl "system/$OraclePassword@//$OracleIp`:1521/ORCLPDB1" "@./oracle_test_data.sql"
```

## 8. Validate CDC test

### 8.1 Debezium still running

```powershell
az container show -g $ResourceGroup -n $DebeziumContainerGroup --query "{state:containers[0].instanceView.currentState.state,restarts:containers[0].instanceView.restartCount}" -o json
```

### 8.2 Offset file exists and advances

```powershell
az container exec -g $ResourceGroup -n $DebeziumContainerGroup --exec-command "cat /debezium/data/offsets.dat"
```

You should see JSON-like fields including:
- scn
- commit_scn
- snapshot_scn

After additional test updates, scn and commit_scn should increase.

### 8.3 OneLake landing files exist

In Fabric portal, confirm under LandingZone:
- _partnerEvents.json
- HR.schema/EMPLOYEES/ parquet files
- HR.schema/DEPARTMENTS/ parquet files

## 9. Troubleshooting quick map

- ORA-01325 or startup validation errors:
  - Re-check ARCHIVELOG mode and supplemental logging.
- ORA-31603 during snapshot metadata:
  - Ensure EXECUTE on DBMS_METADATA and catalog-related grants are applied.
- ORA-01031 with "Failed to create flush table":
  - Ensure CREATE TABLE, CREATE SEQUENCE, and quota on USERS for c##dbzuser.
- Debezium starts then exits quickly:
  - Check restarts and logs with az container show and az container logs.

## 10. Optional cleanup

```powershell
az container delete -g $ResourceGroup -n $DebeziumContainerGroup --yes
az container delete -g $ResourceGroup -n $OracleContainerGroup --yes
```

To keep images and infra for reuse, do not delete ACR or resource group.
