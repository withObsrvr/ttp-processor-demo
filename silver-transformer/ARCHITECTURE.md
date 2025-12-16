# Silver Transformer Architecture

## Overview

The silver-transformer is a Stellar Hubble medallion architecture component that transforms bronze layer data into silver layer data.

## Correct Hubble Layer Architecture

Based on [Stellar Hubble Documentation](https://developers.stellar.org/docs/data/analytics/hubble/data-catalog):

### Bronze Layer (Raw/Decoded)
Bronze tables contain decoded XDR data but minimal transformation:
- `history_ledgers` - Ledger metadata (sequence, timestamps, transaction counts)
- `history_transactions` - Transaction details with envelope and result XDR
- `history_operations` - Individual operation records
- Other entity tables: accounts, trustlines, offers, liquidity_pools, etc.

### Silver Layer (Transformed/Enriched)
Silver tables contain joined, enriched, and transformed data:
- `enriched_history_operations` - Operations joined with transaction and ledger context
- `enriched_history_operations_soroban` - Soroban-specific operations
- `*_current` tables - Point-in-time snapshots (accounts_current, trustlines_current, etc.)
- `*_snapshot` tables - SCD Type 2 with historical tracking
- `token_transfers_raw` - Extracted token transfer events

## Architectural Fix Applied

### What Was Wrong
The initial implementation incorrectly used bronze table names (`history_ledgers`, `history_transactions`) as if they were silver tables. This violated the Hubble architecture.

**Incorrect schema.go (before fix):**
```go
// Created history_ledgers table - THIS IS BRONZE!
// Created history_transactions table - THIS IS BRONZE!
// Created enriched_history_operations table - ✓ This one was correct
```

### What Was Fixed

1. **schema.go:9-22** - Removed bronze table creation functions
   - Deleted `createHistoryLedgersTable()`
   - Deleted `createHistoryTransactionsTable()`
   - Kept only `createEnrichedHistoryOperationsTable()` (correct silver table)

2. **transformer.go:119-168** - Updated transformation logic
   - Added documentation explaining the requirement for operations/transactions/ledgers data
   - Changed from writing to history_ledgers (bronze) to preparing for enriched_history_operations (silver)
   - Added TODOs for full implementation

### Current State

**What Works:**
- ✅ Service compiles and runs
- ✅ Query API client connects to bronze catalog (http://localhost:8081)
- ✅ Checkpoint management tracks progress
- ✅ Health and metrics endpoints operational
- ✅ Correct silver schema (enriched_history_operations only)

**What's Limited:**
- ⚠️  Transformation is stubbed - logs progress but doesn't write data
- ⚠️  Only tracks ledger sequences for checkpoint management
- ⚠️  Cannot populate enriched_history_operations without operations and transactions data

## Full Implementation Requirements

### Bronze Layer Prerequisites

For the silver-transformer to function fully, the bronze layer must provide:

1. **history_operations** - Raw operation records
   ```sql
   SELECT id, transaction_id, application_order, type, type_string,
          source_account, details
   FROM bronze.history_operations
   WHERE ledger_sequence > last_checkpoint
   ```

2. **history_transactions** - Transaction context
   ```sql
   SELECT id, transaction_hash, ledger_sequence, application_order,
          account, account_sequence, successful, fee_charged,
          created_at, memo_type, memo, time_bounds
   FROM bronze.history_transactions
   WHERE ledger_sequence > last_checkpoint
   ```

3. **history_ledgers** - Ledger metadata
   ```sql
   SELECT sequence, closed_at, successful_transaction_count,
          failed_transaction_count, operation_count
   FROM bronze.history_ledgers
   WHERE sequence > last_checkpoint
   ```

### Silver Transformation Logic

The transformation must:

1. **Join the three bronze tables:**
   ```
   operations
   JOIN transactions ON operations.transaction_id = transactions.id
   JOIN ledgers ON transactions.ledger_sequence = ledgers.sequence
   ```

2. **Denormalize transaction fields into operations:**
   - `tx_successful`, `tx_application_order`, `tx_account`, etc.

3. **Denormalize ledger fields into operations:**
   - `ledger_closed_at`, `ledger_successful_transaction_count`, etc.

4. **Mark payment operations:**
   - Set `is_payment = true` for payment operation types

5. **Add metadata:**
   - `batch_id`, `batch_run_date`, `batch_insert_ts`
   - Generate `closed_at_date` from timestamp

### Query API Extensions Needed

The bronze Query API must support querying all three tables:

```go
// Add to query_client.go:
func (c *BronzeQueryClient) QueryOperations(schema string, lastSeq int64, limit int) ([]BronzeOperation, error)
func (c *BronzeQueryClient) QueryTransactions(schema string, lastSeq int64, limit int) ([]BronzeTransaction, error)
// QueryLedgers already exists
```

## Data Flow

### Current Flow (Limited)
```
Bronze Catalog (DuckDB file)
  ↓ (Query API on :8081)
Bronze Query Client (HTTP)
  ↓ (Query ledgers_row_v2)
Silver Transformer
  ↓ (Log progress, update checkpoint)
Silver Catalog
  └─ enriched_history_operations (empty - waiting for operations data)
```

### Target Flow (Full Implementation)
```
Bronze Catalog (DuckDB file)
  ├─ history_ledgers
  ├─ history_transactions
  └─ history_operations
    ↓ (Query API on :8081)
Bronze Query Client (HTTP)
  ↓ (Query all three bronze tables)
Silver Transformer
  ↓ (Join + enrich + denormalize)
Silver Catalog
  └─ enriched_history_operations (populated with enriched data)
```

## Configuration

Environment variables:
```bash
BRONZE_QUERY_URL=http://localhost:8081  # Bronze Query API endpoint
SILVER_PATH=catalogs/silver.duckdb      # Silver catalog path (read-write)
BRONZE_SCHEMA=testnet                   # Bronze schema name
POLL_INTERVAL=1s                        # Polling frequency
BATCH_SIZE=100                          # Records per batch
HEALTH_PORT=8082                        # Health check port
METRICS_PORT=9092                       # Prometheus metrics port
```

## Testing

### Current Testing (Limited)
```bash
# Start bronze ingestion with Query API
cd ../ducklake-ingestion-obsrvr-v3
./bin/ducklake-ingestion-obsrvr-v3 -config config/query-api-test.yaml --multi-network --query-port :8081

# Start silver transformer (will log progress but not write data)
cd ../silver-transformer
./silver-transformer
```

### Full Testing (When Bronze Layer Complete)
```bash
# 1. Start bronze ingestion with Query API
cd ../ducklake-ingestion-obsrvr-v3
./bin/ducklake-ingestion-obsrvr-v3 \
  -config config/query-api-test.yaml \
  --multi-network \
  --query-port :8081

# 2. Verify bronze has all three tables
curl -X POST http://localhost:8081/query \
  -H "Content-Type: application/json" \
  -d '{"sql":"SELECT name FROM testnet.information_schema.tables WHERE table_schema='\''testnet'\'' AND name LIKE '\''history_%'\''"}' | jq

# Expected output should include:
# - history_ledgers
# - history_transactions
# - history_operations

# 3. Start silver transformer
cd ../silver-transformer
rm -f catalogs/silver.duckdb*
./silver-transformer

# 4. Monitor transformation progress
curl http://localhost:8082/health | jq
curl http://localhost:9092/metrics | grep silver_transformer

# 5. Verify enriched_history_operations is populated
curl -X POST http://localhost:8081/query \
  -H "Content-Type: application/json" \
  -d '{"sql":"SELECT COUNT(*) FROM silver.enriched_history_operations"}' | jq
```

## Next Steps

1. **Expand bronze ingestion** to include operations and transactions XDR decoding
2. **Add Query API methods** for querying operations and transactions
3. **Implement full transformation** in transformer.go:processNewLedgers()
4. **Add integration tests** with mock bronze data
5. **Performance optimization** for high-throughput scenarios
6. **Add other silver tables** (accounts_current, trustlines_current, etc.) as needed

## References

- [Stellar Hubble Bronze Data Dictionary](https://developers.stellar.org/docs/data/analytics/hubble/data-catalog/data-dictionary/bronze)
- [Stellar Hubble Silver Data Dictionary](https://developers.stellar.org/docs/data/analytics/hubble/data-catalog/data-dictionary/silver)
- [Medallion Architecture Overview](https://www.databricks.com/glossary/medallion-architecture)
