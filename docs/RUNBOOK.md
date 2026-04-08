# Oracle → Fabric Open Mirroring — Runbook

## Prerequisites

| Requirement | Notes |
|---|---|
| Docker Desktop running | macOS |
| `ORACLE_PASSWORD` env var set | Password for Oracle `SYS`/`SYSTEM` and `c##dbzuser` |
| `AZURE_CLIENT_SECRET` env var set | Entra ID service principal secret for OneLake |

---

## 1. Set environment variables

```bash
export ORACLE_PASSWORD="*******"
export AZURE_CLIENT_SECRET="<your-client-secret>"
```

> These must be set in every new shell session before running `docker compose`.

---

## 2. Build the Debezium image

Only needed on first run or after code changes:

```bash
docker compose build debezium-server
```

---

## 3. Start both containers (clean state)

```bash
docker compose up -d
```

Oracle takes **3–5 minutes** on first boot to initialise the CDB/PDB and run the init scripts.
Debezium will not start until Oracle passes its healthcheck (`service_healthy`).

Monitor startup:

```bash
# Watch both containers reach healthy state
docker compose ps

# Oracle init logs
docker logs oracle-db --follow

# Debezium logs (once it starts)
docker logs debezium-fabric-sink --follow
```

---

## 4. Confirm health

```bash
curl -s http://localhost:8080/q/health | python3 -m json.tool
```

Expected:

```json
{
    "status": "UP",
    "checks": [
        { "name": "debezium", "status": "UP" }
    ]
}
```

---

## 5. Confirm Debezium is streaming

```bash
docker logs debezium-fabric-sink 2>&1 | grep -E "Flushed|partnerEvents|snapshot completed|Starting streaming"
```

Look for:
- `Wrote _partnerEvents.json to landing zone root`
- `Snapshot completed`
- `Starting streaming`
- `Flushed 21 rows to HR.schema/EMPLOYEES/00000000000000000001.parquet`
- `Flushed 13 rows to HR.schema/DEPARTMENTS/00000000000000000001.parquet`

---

## 6. Generate test CDC data

Run this block to produce INSERTs, UPDATEs and a DELETE across both tables:

```bash
docker exec -i oracle-db sqlplus -s hr/hr@//localhost:1521/ORCLPDB1 <<'SQL'
-- Insert new employees
INSERT INTO employees (employee_id, first_name, last_name, email, hire_date, job_id, salary, department_id)
  VALUES (300, 'Alice', 'TestUser', 'ALICETEST', SYSDATE, 'IT_PROG', 75000, 60);
INSERT INTO employees (employee_id, first_name, last_name, email, hire_date, job_id, salary, department_id)
  VALUES (301, 'Bob', 'TestUser', 'BOBTEST', SYSDATE, 'SA_REP', 62000, 80);
COMMIT;

-- Insert a new department
INSERT INTO departments (department_id, department_name, manager_id, location_id)
  VALUES (280, 'Data Engineering', NULL, 1700);
COMMIT;

-- Update salaries
UPDATE employees SET salary = 99000 WHERE employee_id = 300;
UPDATE employees SET salary = 70000 WHERE employee_id = 301;
COMMIT;

-- Delete one record
DELETE FROM employees WHERE employee_id = 301;
COMMIT;

-- Verify row counts
SELECT COUNT(*) AS total_employees FROM employees;
SELECT COUNT(*) AS total_departments FROM departments;
EXIT;
SQL
```

---

## 7. Confirm CDC events were flushed

Wait ~15 seconds for the flush interval, then:

```bash
sleep 15 && docker logs debezium-fabric-sink --since 30s 2>&1 | \
  grep -E "Flushed|ERROR|Wrote.*metadata"
```

Expected output (sequence numbers may vary):

```
Wrote _metadata.json for table HR.schema/EMPLOYEES: {"keyColumns":["EMPLOYEE_ID"]}
Flushed 5 rows to HR.schema/EMPLOYEES/00000000000000000002.parquet
Wrote _metadata.json for table HR.schema/DEPARTMENTS: {"keyColumns":["DEPARTMENT_ID"]}
Flushed 1 rows to HR.schema/DEPARTMENTS/00000000000000000002.parquet
```

No `ERROR` lines should appear.

---

## 8. Verify files in OneLake

Go to [Microsoft Fabric portal](https://app.fabric.microsoft.com) → your workspace → the Mirrored Database item → **Files → LandingZone**.

You should see:

```
LandingZone/
├── _partnerEvents.json
├── HR.schema/
│   ├── EMPLOYEES/
│   │   ├── _metadata.json          ← {"keyColumns":["EMPLOYEE_ID"]}
│   │   ├── 00000000000000000001.parquet  ← snapshot
│   │   └── 00000000000000000002.parquet  ← CDC events
│   └── DEPARTMENTS/
│       ├── _metadata.json          ← {"keyColumns":["DEPARTMENT_ID"]}
│       ├── 00000000000000000001.parquet  ← snapshot
│       └── 00000000000000000002.parquet  ← CDC events
```

After a few minutes Fabric will process these and create Delta tables `HR.EMPLOYEES` and `HR.DEPARTMENTS` under the mirrored database.

---

## Restart / Recovery procedures

### Normal restart (preserves CDC position)

```bash
docker compose restart debezium-server
```

### Force fresh snapshot (discard all offsets)

```bash
docker exec debezium-fabric-sink rm -f /debezium/data/offsets.dat /debezium/data/schemahistory.dat
docker restart debezium-fabric-sink
```

> **Warning:** This re-snapshots all rows and produces duplicate Parquet files in OneLake. Use only when you need to reset the pipeline.

### Fix `ORA-00308` (archived log missing after long downtime)

Oracle purges archived redo logs. If Debezium saved an offset pointing to a purged log, do a fresh snapshot:

```bash
docker exec debezium-fabric-sink rm -f /debezium/data/offsets.dat /debezium/data/schemahistory.dat
docker restart debezium-fabric-sink
```

### Full tear-down and restart

```bash
docker compose down -v   # WARNING: deletes oracle-data and debezium-offsets volumes
docker compose up -d
```

---

## Troubleshooting

| Symptom | Fix |
|---|---|
| `ORA-01017` authentication failure | `export ORACLE_PASSWORD="secret"` was not set; run `ALTER USER c##dbzuser IDENTIFIED BY secret CONTAINER=ALL` via sysdba |
| `ORA-00308` archived log missing | Delete offsets and restart (see above) |
| Debezium container exits immediately | Check `docker logs debezium-fabric-sink` — usually a missing env var or Oracle not yet healthy |
| Files uploaded but Delta tables not appearing | Wait 5–10 min; verify `_metadata.json` contains `"keyColumns"` (lowercase) and folder is `HR.schema/EMPLOYEES` |
| `PathNotFound 404` on first run | Normal — Debezium creates the directory on first write |

---

## Adding a new table to the pipeline

New tables are **not** synced automatically. Debezium uses an explicit allowlist in
`config/application.properties`. You must add the table there, rebuild, and
restart with fresh offsets so Debezium snapshots it.

### Step 1 — Create the table in Oracle (if it does not exist yet)

```bash
docker exec -i oracle-db sqlplus -s hr/hr@//localhost:1521/ORCLPDB1 <<'SQL'
-- Example: a new PROJECTS table in the HR schema
CREATE TABLE hr.projects (
    project_id   NUMBER(6)     PRIMARY KEY,
    project_name VARCHAR2(100) NOT NULL,
    budget       NUMBER(12,2),
    start_date   DATE,
    department_id NUMBER(4) REFERENCES hr.departments(department_id)
);

-- Enable supplemental logging for the new table so LogMiner captures all columns
ALTER TABLE hr.projects ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;

-- Seed some data
INSERT INTO projects VALUES (1, 'Data Platform', 500000, SYSDATE, 60);
INSERT INTO projects VALUES (2, 'Cloud Migration', 750000, SYSDATE, 90);
COMMIT;
EXIT;
SQL
```

> The `ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS` step is required — without it LogMiner only
> captures the primary key in UPDATE/DELETE records, not the changed column values.

### Step 2 — Add the table to the include list

Edit `config/application.properties`:

```properties
# Before
debezium.source.table.include.list=HR.EMPLOYEES,HR.DEPARTMENTS

# After (append the new table)
debezium.source.table.include.list=HR.EMPLOYEES,HR.DEPARTMENTS,HR.PROJECTS
```

### Step 3 — Rebuild the Debezium image

```bash
docker compose build debezium-server
```

### Step 4 — Restart with fresh offsets

A fresh snapshot is required so Debezium reads the new table's current rows and registers its schema.

```bash
docker compose up -d debezium-server
sleep 3
docker exec debezium-fabric-sink rm -f /debezium/data/offsets.dat /debezium/data/schemahistory.dat
docker restart debezium-fabric-sink
```

### Step 5 — Confirm the new table is snapshotted

```bash
sleep 30 && docker logs debezium-fabric-sink --since 60s 2>&1 | \
  grep -E "Flushed|Wrote.*metadata|PROJECTS"
```

Expected:

```
Wrote _metadata.json for table HR.schema/PROJECTS: {"keyColumns":["PROJECT_ID"]}
Flushed 2 rows to HR.schema/PROJECTS/00000000000000000001.parquet
```

---

## Storage backends

The sink supports multiple storage options. The backend is selected automatically based on
the value of `fabric.landing.baseUri` in `config/application.properties`.

| `fabric.landing.baseUri` value | Backend selected |
|---|---|
| Starts with `abfss://` | Azure ADLS Gen2 / OneLake |
| Anything else | Local filesystem |

See the configuration examples in `config/` for each option.
