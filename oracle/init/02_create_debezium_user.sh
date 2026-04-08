#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# 02_create_debezium_user.sh
#
# Creates the common CDB user c##dbzuser that Debezium uses to run LogMiner
# queries.  The user is created in CDB$ROOT with CONTAINER=ALL so it is
# visible from every PDB.
#
# Password is taken from the ORACLE_PASSWORD environment variable (the same
# value used for SYS/SYSTEM in this container).
# ---------------------------------------------------------------------------
set -euo pipefail

echo "==> [02] Creating Debezium LogMiner user c##dbzuserâ€¦"

sqlplus -s / as sysdba << SQLEOF
  -- Switch to the root container
  ALTER SESSION SET CONTAINER = CDB\$ROOT;

  -- Drop user if it exists (idempotent re-runs)
  DECLARE
    v_count NUMBER;
  BEGIN
    SELECT COUNT(*) INTO v_count
    FROM   cdb_users
    WHERE  username = 'C##DBZUSER';
    IF v_count > 0 THEN
      EXECUTE IMMEDIATE 'DROP USER c##dbzuser CASCADE';
    END IF;
  END;
  /

  CREATE USER c##dbzuser
    IDENTIFIED BY "${ORACLE_PASSWORD}"
    DEFAULT TABLESPACE   USERS
    TEMPORARY TABLESPACE TEMP
    QUOTA UNLIMITED ON   USERS
    CONTAINER = ALL;

  -- Session / container access
  GRANT CREATE SESSION   TO c##dbzuser CONTAINER = ALL;
  GRANT SET CONTAINER    TO c##dbzuser CONTAINER = ALL;

  -- LogMiner privilege (Oracle 12.2+)
  GRANT LOGMINING        TO c##dbzuser CONTAINER = ALL;

  -- Dictionary / transaction access
  GRANT SELECT ANY TRANSACTION  TO c##dbzuser CONTAINER = ALL;
  GRANT SELECT ANY DICTIONARY   TO c##dbzuser CONTAINER = ALL;
  GRANT SELECT ANY TABLE        TO c##dbzuser CONTAINER = ALL;
  GRANT EXECUTE_CATALOG_ROLE    TO c##dbzuser CONTAINER = ALL;

  -- Required for consistent snapshot (Debezium locks tables briefly during initial snapshot)
  GRANT LOCK ANY TABLE          TO c##dbzuser CONTAINER = ALL;

  -- Dynamic performance views needed by Debezium
  GRANT SELECT ON V_\$DATABASE            TO c##dbzuser;
  GRANT SELECT ON V_\$ARCHIVED_LOG        TO c##dbzuser;
  GRANT SELECT ON V_\$LOG                 TO c##dbzuser;
  GRANT SELECT ON V_\$LOGFILE             TO c##dbzuser;
  GRANT SELECT ON V_\$INSTANCE           TO c##dbzuser;
  GRANT SELECT ON V_\$TRANSACTION        TO c##dbzuser;
  GRANT SELECT ON V_\$LOGMNR_LOGS        TO c##dbzuser;
  GRANT SELECT ON V_\$LOGMNR_CONTENTS    TO c##dbzuser;
  GRANT SELECT ON V_\$LOGMNR_PARAMETERS  TO c##dbzuser;

  EXIT;
SQLEOF

echo "==> [02] c##dbzuser created and privileges granted."
