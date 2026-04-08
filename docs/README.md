# Debezium Server → Microsoft Fabric Open Mirroring Sink

A custom [Debezium Server](https://debezium.io/documentation/reference/stable/operations/debezium-server.html) sink connector that streams Oracle CDC events into a **Microsoft Fabric Open Mirroring** landing zone (ADLS Gen2 / OneLake) as Parquet files, enabling near-real-time mirroring of Oracle tables into Microsoft Fabric without Kafka.

---

## Overview

```
Oracle DB  ──(LogMiner)──►  Debezium Server  ──►  FabricMirroringSink  ──►  OneLake (ABFS)
                             (Quarkus/CDI)              (this project)          Parquet files
```

Debezium Server reads Oracle redo logs via LogMiner and passes change events to this custom sink. The sink:

1. Buffers rows in memory per table
2. Converts them to Parquet using Avro schema derived from Oracle column metadata
3. Uploads numbered Parquet files to the landing zone folder
4. Writes `_metadata.json` (key columns) and `_sequence.txt` (file counter) per table

Fabric continuously reads the landing zone and mirrors the data into a Lakehouse/Warehouse.

---

## Project Structure

```
├── docs/                    ← This directory: all documentation
├── config/                  ← Configuration templates and examples
├── infra/                   ← Infrastructure & deployment
│   ├── docker/
│   ├── bicep/
│   └── terraform/
├── scripts/                 ← Deployment and utility scripts
├── sql/                     ← Oracle setup and validation scripts
├── oracle/                  ← Oracle container init scripts
├── src/                     ← Java source code
│   ├── main/
│   │   ├── java/io/debezium/server/fabric/
│   │   │   ├── FabricMirroringSink.java
│   │   │   ├── config/
│   │   │   ├── metadata/
│   │   │   ├── parquet/
│   │   │   └── storage/
│   │   └── resources/
│   └── test/
├── pom.xml                  ← Maven configuration
└── README.md                ← This file

```

---

## Documentation Index

- **[RUNBOOK.md](./RUNBOOK.md)** - Local Docker Compose setup guide
- **[RUNBOOK_AZURE_ACI.md](./RUNBOOK_AZURE_ACI.md)** - Azure Container Instances deployment
- **[DEPLOYMENT_GUIDE.md](./DEPLOYMENT_GUIDE.md)** - Complete deployment script reference
- **[../config/README.md](../config/README.md)** - Configuration file documentation

---

## Quick Start

### Local Development (Docker Compose)

```bash
export ORACLE_PASSWORD="your_password"
export AZURE_CLIENT_SECRET="your_secret"
docker compose up -d
```

See [RUNBOOK.md](./RUNBOOK.md) for full steps.

### Azure Deployment (ACI)

```powershell
./scripts/deploy-azure-services.ps1 `
    -OraclePassword "your_password" `
    -AzureClientSecret "your_secret" `
    -FabricTenantId "your_tenant_id" `
    -FabricClientId "your_client_id" `
    -FabricBaseUri "your_onelake_uri"
```

See [RUNBOOK_AZURE_ACI.md](./RUNBOOK_AZURE_ACI.md) for full steps.

---

## Building

**Prerequisites:** Java 17+, Maven 3.8+

```bash
mvn clean package -DskipTests
```

---

## Configuration

All properties are in `config/application.properties` (or environment variables).

Key properties:
- `fabric.landing.baseUri` - OneLake or local path
- `fabric.oracle.jdbcUrl` - Oracle connection
- `debezium.source.table.include.list` - Tables to sync

See [config/README.md](../config/README.md) for complete reference.

---

## Architecture

- **Source:** Oracle LogMiner (via Debezium)
- **Format:** Parquet + Avro schema
- **Storage:** OneLake (ABFS) or local filesystem
- **Row Marker:** `__rowMarker__` (0=INSERT, 1=UPDATE, 2=DELETE)

---

## Troubleshooting

See individual runbooks:
- Local issues → [RUNBOOK.md](./RUNBOOK.md)
- Azure issues → [RUNBOOK_AZURE_ACI.md](./RUNBOOK_AZURE_ACI.md)

---

## References

- [Debezium Server Documentation](https://debezium.io/documentation/reference/stable/operations/debezium-server.html)
- [Microsoft Fabric Open Mirroring](https://learn.microsoft.com/en-us/fabric/database/mirroring/overview)
- [Azure Container Instances](https://learn.microsoft.com/en-us/azure/container-instances/)
