# DELETE Operation Validation

Validates that DELETE operations are being captured correctly to CDC.

Test steps:
1. Delete a row
2. Verify in parquet file with __rowMarker__ = 2 (tombstone marker)
