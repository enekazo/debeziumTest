# Configuration Files

All Debezium configuration templates and examples are stored here for easy reference.

## Files

- **application.properties.template** - Main configuration template with all options
- **application-local.properties.example** - Local filesystem backend (development)
- **application-adls.properties.example** - ADLS Gen2 backend (standalone Azure storage)
- **application-fabric.properties.example** - Microsoft Fabric Open Mirroring backend (production)

## Setup

### For Local Docker Compose Development

```bash
cp application-local.properties.example application.properties
export ORACLE_PASSWORD="your_password"
docker compose up -d
```

### For Azure ADLS Gen2

```bash
cp application-adls.properties.example application.properties
# Edit with your storage account, container, and service principal details
export ORACLE_PASSWORD="your_password"
export AZURE_CLIENT_SECRET="your_service_principal_secret"
docker compose up -d
```

### For Fabric Open Mirroring (Azure CI Deployment)

```bash
cp application-template.properties application.properties
# Edit with your OneLake workspace and item IDs
./scripts/deploy-azure-services.ps1 ...
```

## Configuration Reference

### Required Properties

| Property | Example | Description |
|----------|---------|-------------|
| `fabric.landing.baseUri` | `abfss://workspace@onelake.dfs.fabric.microsoft.com/item/Files/LandingZone` | OneLake/ADLS landing zone path |
| `fabric.sp.tenantId` | `8d66ab52-dbaf-4bdc...` | Entra ID tenant ID |
| `fabric.sp.clientId` | `80586b13-4e01-4f9c...` | Service principal client ID |
| `fabric.sp.clientSecret` | (env var) | Service principal secret (use `${AZURE_CLIENT_SECRET}` env var) |
| `fabric.oracle.jdbcUrl` | `jdbc:oracle:thin:@//host:1521/ORCLPDB1` | Oracle JDBC connection string |
| `fabric.oracle.username` | `c##dbzuser` | Oracle CDC user |
| `fabric.oracle.password` | (env var) | Oracle password (use `${ORACLE_PASSWORD}` env var) |

### Optional Properties

| Property | Default | Description |
|----------|---------|-------------|
| `fabric.flush.maxRecords` | `200000` | Flush after N rows buffered |
| `fabric.flush.intervalMs` | `10000` | Flush every N milliseconds |
| `fabric.parquet.compression` | `SNAPPY` | Compression codec (SNAPPY, GZIP, LZ4, ZSTD) |
| `fabric.rowMarker.column` | `__rowMarker__` | Operation marker column name |
| `debezium.source.table.include.list` | — | Comma-separated table list to sync |

### Environment Variables (Never Commit Raw Values)

```bash
# Oracle password
export ORACLE_PASSWORD="your_oracle_password"

# Service principal secret for Azure authentication
export AZURE_CLIENT_SECRET="your_azure_secret"
```

## Storage Backends

The correct backend is selected automatically based on `fabric.landing.baseUri`:

| URI Pattern | Backend | Use Case |
|-------------|---------|----------|
| `abfss://*.dfs.fabric.microsoft.com/*` | OneLake (Fabric) | Production - Fabric mirroring |
| `abfss://container@account.dfs.core.windows.net/*` | ADLS Gen2 | Azure Storage - standalone parquet storage |
| Any other path (e.g., `file:///`, `/local/path`) | Local Filesystem | Development - testing, smoke tests |

All three backends write identical Parquet files and `_metadata.json` format.

## Troubleshooting

### "No credentials found" error

- Ensure `AZURE_CLIENT_SECRET` environment variable is set
- Verify `fabric.sp.tenantId` and `fabric.sp.clientId` are correct

### "Cannot connect to Oracle"

- Check `fabric.oracle.jdbcUrl` points to reachable Oracle instance
- Verify `fabric.oracle.password=${ORACLE_PASSWORD}` and env var is set
- Ensure Debezium user has required permissions (SELECT, LOGMINING, etc.)

### Unable to write to landing zone

- Verify service principal has "Storage Blob Data Contributor" role on the container
- Check `fabric.landing.baseUri` format is correct
- Confirm path exists or service principal has permissions to create folders

## References

See documentation in `../docs/`:
- [RUNBOOK.md](../docs/RUNBOOK.md) - Local Docker deployment
- [RUNBOOK_AZURE_ACI.md](../docs/RUNBOOK_AZURE_ACI.md) - Azure ACI deployment
- [DEPLOYMENT_GUIDE.md](../scripts/DEPLOYMENT_GUIDE.md) - Deployment script reference
