#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# 01_enable_supplemental_logging.sh
#
# Connects to CDB as SYSDBA and:
#   1. Enables ARCHIVELOG mode (required for LogMiner / Debezium)
#   2. Enables supplemental logging at the database level
#
# NOTE: gvenzl/oracle-free:23-slim does NOT enable ARCHIVELOG by default.
#       We shutdown/mount/alter/open to enable it, then set supplemental logging.
# ---------------------------------------------------------------------------
set -euo pipefail

echo "==> [01] Enabling ARCHIVELOG mode and supplemental logging in CDB..."

sqlplus -s / as sysdba << 'SQLEOF'
  -- Step 1: Enable ARCHIVELOG mode (requires mount state)
  SHUTDOWN IMMEDIATE;
  STARTUP MOUNT;
  ALTER DATABASE ARCHIVELOG;
  ALTER DATABASE OPEN;

  -- Step 2: Supplemental logging - minimal
  ALTER DATABASE ADD SUPPLEMENTAL LOG DATA;

  -- All-column supplemental logging (full before/after images for Debezium)
  ALTER DATABASE ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;

  -- Step 3: Build LogMiner dictionary into redo logs
  EXECUTE DBMS_LOGMNR_D.BUILD(OPTIONS => DBMS_LOGMNR_D.STORE_IN_REDO_LOGS);

  -- Confirm
  SELECT LOG_MODE,
         SUPPLEMENTAL_LOG_DATA_MIN AS "SUPP_MIN",
         SUPPLEMENTAL_LOG_DATA_ALL AS "SUPP_ALL"
  FROM   V$DATABASE;

  EXIT;
SQLEOF

echo "==> [01] ARCHIVELOG mode and supplemental logging enabled."