# UPDATE Operation Validation

Validates that UPDATE operations are being captured correctly to CDC.

Test steps:
1. Update an existing row
2. Verify in parquet file with __rowMarker__ = 1
