# SQL Validation Scripts

Oracle validation and troubleshooting queries.

## Quick Reference

### Check Debezium User Permissions

```sql
SELECT * FROM DBA_ROLE_PRIVS WHERE GRANTEE = 'C##DBZUSER';
SELECT * FROM DBA_SYS_PRIVS WHERE GRANTEE = 'C##DBZUSER';
```

### Current SCN (System Change Number)

```sql
SELECT DBMS_FLASHBACK.GET_SYSTEM_CHANGE_NUMBER FROM DUAL;
```

### Verify Supplemental Logging

```sql
SELECT LOG_MODE, SUPPLEMENTAL_LOG_DATA_MIN, SUPPLEMENTAL_LOG_DATA_ALL FROM V$DATABASE;
```

### Check ARCHIVELOG Mode

```sql
SELECT ARCHIVE_DESTINATION FROM V$PARAMETER WHERE NAME = 'log_archive_dest_1';
```

## Script Files

Each .sql file in this directory can be executed via SQL*Plus or SQLcl:

```bash
sqlcl system/password@//host:1521/ORCLPDB1 @./script_name.sql
```
