# SQL Scripts

This directory contains SQL scripts for Oracle setup and validation.

## Structure

- **setup/** - Oracle initialization and configuration scripts
- **validation/** - CDC and data validation queries

## Validation Scripts

Use these scripts to verify the CDC pipeline is working correctly.

### Key Validation Queries

- `cdc_rowmarker_verify.sql` - Check __rowMarker__ column in parquet files
- `cdc_validate_insert.sql` - Validate INSERT operations
- `cdc_validate_update.sql` - Validate UPDATE operations
- `cdc_validate_current_scn.sql` - Check current System Change Number (SCN)
- `verify_dbzuser.sql` - Verify Debezium user permissions

## Usage

### Run Oracle validation checks

```bash
sqlcl system/<password>@//host:1521/ORCLPDB1 @./sql/validation/verify_dbzuser.sql
```

### Monitor CDC position

```bash
sqlcl system/<password>@//host:1521/ORCLPDB1 @./sql/validation/cdc_validate_current_scn.sql
```

## References

See Oracle init scripts in `../oracle/init/` for:
- Supplemental logging setup
- Debezium user creation
- HR schema initialization
