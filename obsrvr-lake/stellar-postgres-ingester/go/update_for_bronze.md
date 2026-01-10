# Ingester Update Plan for Bronze Schema Alignment

## Current Status Check

The stellar-postgres-ingester needs to be updated to populate all fields matching the Bronze schema exactly.

## Strategy

Since the Bronze schema is the source of truth (from v3/tables.go), we need to:
1. Update type definitions to match Bronze fields
2. Update insert statements to use Bronze field names
3. Update extraction logic to populate all Bronze fields
4. Use v3 as reference for extraction logic

## Quick Fix Approach

For Cycle 3 completion, we can:
1. Keep the current 5-table MVP extractors (ledgers, transactions, operations, effects, trades)
2. Update only these 5 to match Bronze schema exactly
3. Leave the other 14 tables empty for now (tables exist, but no data)
4. This allows flush to work for 5 tables immediately

## Files to Update

1. `types.go` - Update struct field names to match Bronze
2. `writer.go` - Update INSERT statements to match Bronze columns
3. `extractors.go` - Update extraction logic for 5 MVP tables

