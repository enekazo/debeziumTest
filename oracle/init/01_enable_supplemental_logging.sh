#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# 01_enable_supplemental_logging.sh
#
# Run as the oracle OS user inside the gvenzl/oracle-free container.
# Connects to the CDB as SYSDBA and enables supplemental logging at the
# database level — required by Debezium / Oracle LogMiner.
#
# gvenzl/oracle-free ships with ARCHIVELOG mode already ON, so we only
# need to turn on supplemental logging here.
# ---------------------------------------------------------------------------
set -euo pipefail

echo "==> [01] Enabling supplemental logging in CDB…"

sqlplus -s / as sysdba << 'SQLEOF'
  -- Minimal supplemental logging (tracks row changes)
  ALTER DATABASE ADD SUPPLEMENTAL LOG DATA;

  -- All-column supplemental logging (gives Debezium full before/after images)
  ALTER DATABASE ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;

  -- Build the LogMiner dictionary into the redo logs (used by Debezium)
  EXECUTE DBMS_LOGMNR_D.BUILD(OPTIONS => DBMS_LOGMNR_D.STORE_IN_REDO_LOGS);

  -- Confirm
  SELECT SUPPLEMENTAL_LOG_DATA_MIN  AS "MIN",
         SUPPLEMENTAL_LOG_DATA_ALL  AS "ALL"
  FROM   V$DATABASE;

  EXIT;
SQLEOF

echo "==> [01] Supplemental logging enabled."
