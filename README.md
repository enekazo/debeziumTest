# Debezium Server → Microsoft Fabric Open Mirroring Sink

A custom [Debezium Server](https://debezium.io/documentation/reference/stable/operations/debezium-server.html) sink connector that streams Oracle CDC events into a **Microsoft Fabric Open Mirroring** landing zone (ADLS Gen2 / OneLake) as Parquet files, enabling near-real-time mirroring of Oracle tables into Microsoft Fabric without Kafka.

## Overview

```
Oracle DB  ──(LogMiner)──►  Debezium Server  ──►  FabricMirroringSink  ──►  OneLake (ABFS)
                             (Quarkus/CDI)              (this project)          Parquet files
```

Debezium Server reads Oracle redo logs via LogMiner and passes change events to this custom sink. The sink buffers rows in memory per table, converts them to Parquet using Avro schema derived from Oracle column metadata, uploads numbered Parquet files to OneLake, and writes metadata files for Fabric to process.

Fabric continuously reads the landing zone and mirrors the data into a Lakehouse/Warehouse using the `__rowMarker__` column (0=INSERT, 1=UPDATE, 2=DELETE) for upsert operations.

---

## 🚀 Quick Start

### Local Docker Development

```bash
# Prerequisites: Docker Desktop, environment variables set
export ORACLE_PASSWORD="your_oracle_password"
export AZURE_CLIENT_SECRET="your_azure_secret"

# Start both Oracle and Debezium
docker compose -f ./infra/docker/docker-compose.yml up -d

# Verify health
curl http://localhost:8080/q/health
```

👉 **See [docs/RUNBOOK.md](docs/RUNBOOK.md) for full local setup guide**

### Azure Deployment (ACI)

```powershell
./scripts/deploy-azure-services.ps1 `
    -OraclePassword "your_password" `
    -AzureClientSecret "your_secret" `
    -FabricTenantId "your_tenant_id" `
    -FabricClientId "your_client_id" `
    -FabricBaseUri "your_onelake_uri"
```

👉 **See [docs/RUNBOOK_AZURE_ACI.md](docs/RUNBOOK_AZURE_ACI.md) for full Azure setup guide**

---

## 📖 Documentation

All documentation is organized in the `docs/` directory:

| Document | Purpose |
|----------|---------|
| **[docs/README.md](docs/README.md)** | Project overview and architecture |
| **[docs/RUNBOOK.md](docs/RUNBOOK.md)** | Local Docker Compose setup |
| **[docs/RUNBOOK_AZURE_ACI.md](docs/RUNBOOK_AZURE_ACI.md)** | Azure Container Instances deployment |
| **[PROJECT_STRUCTURE.md](PROJECT_STRUCTURE.md)** | Complete directory structure guide |

---

## ⚙️ Configuration

Configuration files are in the `config/` directory:

| File | Purpose |
|------|---------|
| **[config/README.md](config/README.md)** | Configuration reference and setup guide |
| **[config/application.properties.template](config/application.properties.template)** | Main config template (all options) |
| **[config/application-local.properties.example](config/application-local.properties.example)** | Local filesystem backend (dev) |
| **[config/application-adls.properties.example](config/application-adls.properties.example)** | ADLS Gen2 backend (standalone storage) |

**Active configuration:** `src/main/resources/application.properties` (not committed; use template as base)


## 🐳 Infrastructure

### Docker
- **[infra/docker/Dockerfile](infra/docker/Dockerfile)** - Multi-stage Debezium application build
- **[infra/docker/docker-compose.yml](infra/docker/docker-compose.yml)** - Local Oracle + Debezium environment
- **[infra/docker/README.md](infra/docker/README.md)** - Docker setup guide

### Azure Deployment
- **[scripts/deploy-azure-services.ps1](scripts/deploy-azure-services.ps1)** - Automated Azure ACI deployment
- **[scripts/DEPLOYMENT_GUIDE.md](scripts/DEPLOYMENT_GUIDE.md)** - Complete deployment script reference

### Planned IaC
- **[infra/bicep/](infra/bicep/)** - Bicep templates (future)
- **[infra/terraform/](infra/terraform/)** - Terraform modules (future)

---

## 🗄️ Source Code

```
src/
├── main/java/io/debezium/server/fabric/
│   ├── FabricMirroringSink.java          # Main Debezium sink (CDI @Named("fabric"))
│   ├── config/FabricSinkConfig.java       # Configuration loader
│   ├── metadata/
│   │   ├── ColumnMetadata.java
│   │   ├── TableMetadata.java
│   │   └── OracleMetadataLoader.java      # Oracle JDBC metadata + row fetch
│   ├── parquet/
│   │   ├── AvroSchemaBuilder.java         # Oracle→Avro type mapping
│   │   └── ParquetFileWriter.java         # Avro Parquet writer
│   └── storage/
│       ├── StorageBackend.java            # Interface
│       ├── LocalStorageBackend.java       # Local filesystem (dev/test)
│       ├── AbfsStorageBackend.java        # Azure ADLS Gen2 / OneLake
│       ├── AdlsServicePrincipalStorageBackend.java  # Service principal auth
│       ├── SequenceManager.java
│       └── MetadataManager.java
└── test/java/io/debezium/server/fabric/
    └── LocalSmokeTest.java                # Local unit test (no Oracle/Azure)
```

---

## 🧪 Building & Testing

### Build

**Prerequisites:** Java 17+, Maven 3.8+

```bash
# Build fat JAR
mvn clean package -DskipTests

# Build + run unit tests (no Oracle/Azure required)
mvn clean package
```

Produces: `target/debezium-server-sink-fabric-1.0.0-SNAPSHOT-all.jar`

### Local Smoke Test

Tests row marker serialization to Parquet without Oracle or Azure:

```bash
mvn test
```

---

## 📊 Features

### Row Marker Column
Every Parquet row includes `__rowMarker__` (INT32) for Fabric upsert logic:
- `0` = INSERT
- `1` = UPDATE
- `2` = DELETE

### Parquet Sequencing
Files named as 20-digit zero-padded integers (e.g., `00000000000000000001.parquet`).
Sequence counter persists in `_sequence.txt` and survives container restarts via persistent Azure Files mount.

### Persistent State
- **Offsets:** CDC position saved to `/debezium/data/offsets.dat`
- **Schema History:** Table metadata cached in `/debezium/data/schemahistory.dat`
- **Storage:** Azure File Share (mounted to ACI) or Docker volume

Prevents unintended re-snapshots when containers restart.

### Type Mapping
Comprehensive Oracle → Avro/Parquet type mappings including:
- `DATE` → `INT32 DATE`
- `TIMESTAMP` → `INT64 TIMESTAMP_MILLIS`
- `NUMBER`, `FLOAT`, `BINARY_FLOAT` → floating-point types
- `VARCHAR2`, `CLOB` → UTF8 strings
- `RAW`, `BLOB` → binary

---

## 🔧 SQL Validation Scripts

Run Oracle validation queries from `sql/validation/`:

```bash
sqlcl system/<password>@//host:1521/ORCLPDB1 @./sql/validation/verify_dbzuser.sql
sqlcl system/<password>@//host:1521/ORCLPDB1 @./sql/validation/cdc_validate_current_scn.sql
```

See **[sql/README.md](sql/README.md)** for complete script list and usage.

---

## 🔐 Security Notes

**Never commit secrets:**
- `AZURE_CLIENT_SECRET` — Use environment variables
- `ORACLE_PASSWORD` — Use environment variables
- `application.properties` — Use template as base, populate locally

**Best practices:**
- Rotate service principal secrets regularly
- Use managed identities where possible
- Store secrets in Azure Key Vault
- Never log credentials (sink implementation filters them)

---

## 🐛 Troubleshooting

### Local Docker Issues
👉 **[docs/RUNBOOK.md](docs/RUNBOOK.md#troubleshooting)**

### Azure ACI Issues
👉 **[docs/RUNBOOK_AZURE_ACI.md](docs/RUNBOOK_AZURE_ACI.md#9-troubleshooting-quick-map)**

### Configuration Issues
👉 **[config/README.md](config/README.md#troubleshooting)**

---

## 📚 Additional Resources

- [Project Structure Guide](PROJECT_STRUCTURE.md) — Complete directory organization
- [Debezium Server Documentation](https://debezium.io/documentation/reference/stable/operations/debezium-server.html)
- [Microsoft Fabric Open Mirroring](https://learn.microsoft.com/en-us/fabric/database/mirroring/overview)
- [Azure Container Instances](https://learn.microsoft.com/en-us/azure/container-instances/)

---

## 🤝 Contributing

This is a custom Debezium sink for Fabric mirroring. For issues:

1. Check the relevant runbook for your deployment type
2. Verify configuration and environment variables
3. Check Debezium logs for errors
4. Validate Oracle permissions and CDC setup
5. Confirm file uploads to landing zone

---

## 🔗 Quick Links

| Purpose | Link |
|---------|------|
| Getting Started | [docs/README.md](docs/README.md) |
| Local Setup | [docs/RUNBOOK.md](docs/RUNBOOK.md) |
| Azure Setup | [docs/RUNBOOK_AZURE_ACI.md](docs/RUNBOOK_AZURE_ACI.md) |
| Configuration | [config/README.md](config/README.md) |
| Deployment Scripts | [scripts/DEPLOYMENT_GUIDE.md](scripts/DEPLOYMENT_GUIDE.md) |
| Project Structure | [PROJECT_STRUCTURE.md](PROJECT_STRUCTURE.md) |
| SQL Validation | [sql/README.md](sql/README.md) |
