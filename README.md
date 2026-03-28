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
├── pom.xml
├── Dockerfile
├── docker-compose.yml
├── src/
│   ├── main/
│   │   ├── java/io/debezium/server/fabric/
│   │   │   ├── FabricMirroringSink.java          # Main sink (CDI @Named("fabric"))
│   │   │   ├── config/FabricSinkConfig.java       # Config loader
│   │   │   ├── metadata/
│   │   │   │   ├── ColumnMetadata.java
│   │   │   │   ├── TableMetadata.java
│   │   │   │   └── OracleMetadataLoader.java      # Oracle JDBC metadata + row fetch
│   │   │   ├── parquet/
│   │   │   │   ├── AvroSchemaBuilder.java          # Oracle→Avro type mapping
│   │   │   │   └── ParquetFileWriter.java          # Avro Parquet writer
│   │   │   └── storage/
│   │   │       ├── StorageBackend.java             # Interface
│   │   │       ├── LocalStorageBackend.java        # Local filesystem (dev/test)
│   │   │       ├── AbfsStorageBackend.java         # Azure ADLS Gen2 / OneLake
│   │   │       ├── SequenceManager.java            # 20-digit file sequencing
│   │   │       └── MetadataManager.java            # _metadata.json writer
│   │   └── resources/application.properties
│   └── test/java/io/debezium/server/fabric/
│       └── LocalSmokeTest.java
```

---

## Building

**Prerequisites:** Java 17+, Maven 3.8+

```bash
mvn clean package -DskipTests
```

This produces `target/debezium-server-sink-fabric-1.0.0-SNAPSHOT-all.jar` — a fat JAR containing Parquet, Avro, Azure SDK, and Oracle JDBC but **not** the provided-scope deps (Jackson, SLF4J, CDI API, MicroProfile Config, Debezium itself), which are already present in the Debezium Server runtime.

To also run the smoke test (no Oracle or Azure required):

```bash
mvn clean package
```

---

## Configuration

All properties go in `application.properties` (or Debezium Server's environment/config).

### Required

| Property | Description |
|---|---|
| `fabric.landing.baseUri` | Storage URI: `abfss://<fs>@<account>.dfs.fabric.microsoft.com/<path>` or `file:///path` |
| `ONELAKE_SAS` (env) | SAS token for OneLake authentication (or `fabric.landing.sasToken`) |
| `debezium.source.topic.prefix` | Debezium connector topic prefix (e.g. `oracle`) — used to derive table folder names |

### Oracle JDBC Metadata (optional but recommended)

When configured, the sink loads column definitions and PK from Oracle so Parquet schema is accurate. Without this, the sink cannot write Parquet (no schema available).

| Property | Description | Default |
|---|---|---|
| `fabric.oracle.jdbcUrl` | JDBC URL, e.g. `jdbc:oracle:thin:@//host:1521/SVC` | — |
| `fabric.oracle.username` | Oracle user | — |
| `fabric.oracle.password` or `ORACLE_PASSWORD` env | Oracle password | — |
| `fabric.oracle.metadata.refreshIntervalMs` | Metadata cache TTL | `300000` (5 min) |
| `fabric.oracle.fetchRowOnUpdate` | Re-fetch full row from Oracle on UPDATE when Debezium's `after` is null | `true` |

### Flush / Buffer Settings

| Property | Description | Default |
|---|---|---|
| `fabric.flush.maxRecords` | Flush after this many buffered rows | `200000` |
| `fabric.flush.maxBytes` | Reserved for future byte-threshold flush | `1073741824` (1 GB) |
| `fabric.flush.intervalMs` | Background flush interval | `10000` (10 s) |

### Parquet Settings

| Property | Description | Default |
|---|---|---|
| `fabric.parquet.compression` | Compression codec: `SNAPPY`, `GZIP`, `LZ4`, `ZSTD`, `UNCOMPRESSED` | `SNAPPY` |
| `fabric.rowMarker.column` | Column name for the row operation marker | `__rowMarker__` |

### Sequence / State

| Property | Description | Default |
|---|---|---|
| `fabric.sequence.stateFileName` | Per-table file storing current sequence number | `_sequence.txt` |

---

## Landing Zone Layout

For a topic `oracle.HR.EMPLOYEES`:

```
<baseUri>/
└── HR_EMPLOYEES/
    ├── _metadata.json                       ← Key columns (created on startup)
    ├── _sequence.txt                        ← Current max sequence number
    ├── 00000000000000000001.parquet
    ├── 00000000000000000002.parquet
    └── ...
```

### `_metadata.json` Format

Generated automatically from Oracle `ALL_CONSTRAINTS` (PK columns). Fabric uses this to identify the primary key for merge/upsert.

```json
{ "KeyColumns": ["EMPLOYEE_ID"] }
```

### Parquet File Naming (Sequencing)

Files are named as **20-digit zero-padded integers** (e.g. `00000000000000000001.parquet`). On startup the sink reads `_sequence.txt` to restore the counter. If `_sequence.txt` is absent (e.g. first run or accident), it scans `.parquet` files in the folder and picks the highest number.

---

## Oracle DATE Handling

With `debezium.source.time.precision.mode=connect` (recommended), Debezium represents `DATE` columns as **integer days since Unix epoch** (1970-01-01). The sink maps these directly to **Parquet `INT32 DATE`** (Avro `int` with `logicalType=date`).

Without `connect` mode, Debezium sends DATE as milliseconds since epoch (a `long`). The sink detects this and divides by `86400000` to convert to days.

**Always use** `debezium.source.time.precision.mode=connect` for predictable DATE handling.

---

## Oracle Type → Avro/Parquet Mapping

| Oracle Type | Avro Type | Parquet Type |
|---|---|---|
| `DATE` | `int` (logicalType=`date`) | `INT32 DATE` |
| `TIMESTAMP`, `TIMESTAMP(n)` | `long` (logicalType=`timestamp-millis`) | `INT64 TIMESTAMP_MILLIS` |
| `TIMESTAMP WITH TIME ZONE` | `string` | `BINARY UTF8` |
| `TIMESTAMP WITH LOCAL TIME ZONE` | `string` | `BINARY UTF8` |
| `NUMBER` (no precision) | `double` | `DOUBLE` |
| `NUMBER(p,0)` p≤9 | `int` | `INT32` |
| `NUMBER(p,0)` p≤18 | `long` | `INT64` |
| `NUMBER(p,s)` s>0 | `double` | `DOUBLE` |
| `FLOAT`, `BINARY_FLOAT` | `float` | `FLOAT` |
| `BINARY_DOUBLE` | `double` | `DOUBLE` |
| `VARCHAR2`, `CHAR`, `NVARCHAR2`, `NCHAR`, `VARCHAR` | `string` | `BINARY UTF8` |
| `CLOB`, `NCLOB`, `LONG` | `string` | `BINARY UTF8` |
| `RAW`, `BLOB`, `LONG RAW` | `bytes` | `BINARY` |

Nullable columns are wrapped in `["null", <type>]` Avro union with default `null`.

---

## Row Marker Column

Every Parquet row includes `__rowMarker__` (INT32):

| Value | Operation |
|---|---|
| `0` | INSERT (op `c` or snapshot `r`) |
| `1` | UPDATE (op `u`) |
| `2` | DELETE (op `d` or tombstone) |

Fabric Open Mirroring uses this to apply inserts/updates/deletes during the next sync cycle.

---

## Docker Deployment

### Build the image

```bash
mvn clean package -DskipTests
docker build -t fabric-sink:latest .
```

### Deploy on Azure VM

1. Copy `src/main/resources/application.properties` to your VM, edit it with your Oracle and OneLake details.
2. Set environment variables:

```bash
export ONELAKE_SAS="sv=2023-...&sig=..."
export ORACLE_PASSWORD="secret"
```

3. Run with docker-compose:

```bash
docker-compose up -d
```

Or run directly:

```bash
docker run -d \
  -e ONELAKE_SAS="$ONELAKE_SAS" \
  -e ORACLE_PASSWORD="$ORACLE_PASSWORD" \
  -v "$(pwd)/application.properties:/debezium/conf/application.properties:ro" \
  -v debezium-data:/debezium/data \
  -p 8080:8080 \
  fabric-sink:latest
```

### Health check

```bash
curl http://localhost:8080/q/health/ready
```

---

## Local Smoke Test

No Oracle or Azure connection required. Uses `LocalStorageBackend` with a temp directory.

```bash
mvn test
```

The `LocalSmokeTest` test:
1. Creates a temp directory
2. Builds `TableMetadata` with columns: `EMPLOYEE_ID NUMBER(10) PK`, `FIRST_NAME VARCHAR2`, `LAST_NAME VARCHAR2`, `HIRE_DATE DATE`, `SALARY NUMBER(8,2)`
3. Writes 3 rows (INSERT, UPDATE, DELETE) to a Parquet file
4. Reads the file back using `parquet-avro`
5. Asserts row count and `__rowMarker__` values

To test with a local landing zone, set in `application.properties`:

```properties
fabric.landing.baseUri=file:///absolute/path/to/landing
```

---

## Schema Evolution

Oracle `ALL_TAB_COLUMNS` is re-queried every `fabric.oracle.metadata.refreshIntervalMs` milliseconds. When a column is added in Oracle:

1. The next metadata refresh picks up the new column
2. Subsequent Parquet files include the new column
3. Older Parquet files simply don't have that column (Fabric handles missing columns gracefully with nulls)

**Column removal** requires manual intervention — remove from Fabric mirroring schema and restart the sink.

---

## Security Notes

- **Never** store `ONELAKE_SAS` or `ORACLE_PASSWORD` in `application.properties`. Use environment variables.
- The SAS token is never logged (the `AbfsStorageBackend` does not log it).
- Rotate the SAS token and restart the container as needed.

---

## Troubleshooting

| Issue | Resolution |
|---|---|
| `No metadata for HR_EMPLOYEES, cannot write Parquet` | Check Oracle JDBC config and connectivity |
| `401 Unauthorized` from Azure | SAS token expired or incorrect; regenerate and restart |
| Sink jar not discovered by Debezium Server | Ensure the jar is in `/debezium/lib/` and `debezium.sink.type=fabric` is set |
| Duplicate rows after restart | Normal; Fabric deduplicates using `__rowMarker__` and key columns |
| `_sequence.txt` missing after crash | Sink will scan parquet files on next start to recover sequence |
