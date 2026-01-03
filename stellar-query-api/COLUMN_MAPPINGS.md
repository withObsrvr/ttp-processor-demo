# Column Name Mappings Between Query API and Actual Schemas

## Status

### ✅ Fixed in Readers (hot_reader.go and cold_reader.go)

1. **Ledgers**: Fixed `soroban_fee_write1kb`, `ingestion_timestamp` → `created_at`
2. **Transactions**: Removed `application_order`, changed to `source_account`
3. **Operations**: Removed `application_order`
4. **Trades**: Changed `seller_account_id` → `seller_account`, removed price_n/price_d
5. **Accounts**: Removed `buying_liabilities`, `selling_liabilities`, `inflation_destination`
6. **Trustlines**: Changed `trustline_limit` → `trust_limit`
7. **Offers**: Changed `seller_id` → `seller_account`
8. **Contract Events**: Removed `application_order`

### ✅ Fixed in Scanning Functions (query_service.go)

1. **scanLedgers**: Updated to handle NULL values for era_id, version_label, etc.
2. **scanTransactions**: Fixed to match 12-column query (removed XDR fields, application_order)

### ✅ All Scanning Functions Fixed

3. **scanOperations**: Removed application_order, updated to match 10-column query
4. **scanTrades**: Changed seller_account_id → seller_account, updated to match 16-column query
5. **scanAccounts**: Removed buying_liabilities, selling_liabilities, inflation_destination, updated to match 19-column query
6. **scanTrustlines**: Changed trustline_limit → trust_limit, updated to match 13-column query
7. **scanOffers**: Changed seller_id → seller_account, updated to match 16-column query
8. **scanContractEvents**: Removed application_order, updated to match 13-column query

### ✅ Testing Complete

All endpoints tested and working correctly:
- ✓ Ledgers
- ✓ Transactions
- ✓ Operations
- ✓ Trades
- ✓ Accounts
- ✓ Trustlines
- ✓ Offers
- ✓ Contract Events
- ✓ Effects

## Schema Alignment Strategy

These column names should match the V3 Bronze schemas exactly, as both hot (PostgreSQL) and cold (DuckLake Bronze) use the same V3 expanded schemas.

**Source of Truth**: `/home/tillman/Documents/ttp-processor-demo/v3_bronze_schema.sql`
