# Debezium User Permissions Verification

Checks that the c##dbzuser has all required permissions for CDC operations.

REQUIRED:
- SELECT ANY TABLE
- LOCK ANY TABLE  
- SELECT ANY TRANSACTION
- SELECT ANY DICTIONARY
- LOGMINING (system privilege or role)
- CREATE TABLE
- CREATE SEQUENCE
- FLASHBACK ANY TABLE
- EXECUTE ON DBMS_METADATA
- Unlimited quota on USERS tablespace

RUN AS SYSDBA:
  sqlcl "sys/password@//host:1521/FREE as sysdba" @./verify_dbzuser.sql
