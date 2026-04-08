# INSERT Operation Validation

Validates that INSERT operations are being captured correctly to CDC.

Test steps:
1. Insert a new row
2. Verify in parquet file with __rowMarker__ = 0
