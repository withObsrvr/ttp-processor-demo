# Silver Layer Table Mapping & Implementation Plan

## Overview

This document maps Stellar Hubble's 18 silver layer tables to our bronze layer capabilities and provides an implementation roadmap for the `silver-transformer` service.

**Silver Layer Purpose**: Transformed, enriched data optimized for analytics and consumption. Silver tables join bronze tables, denormalize relationships, track historical changes (SCD Type 2), and filter for specific use cases.

## Hubble Silver Tables (18 Total)

### 1. Current State Tables (Entity Snapshots)

These tables represent the **current state** of entities, typically derived from bronze snapshot tables with the latest ledger sequence.

| Silver Table | Bronze Source | Status | Implementation Notes |
|--------------|---------------|--------|---------------------|
| `accounts_current` | `accounts_snapshot_v1` | ✅ Ready | Filter latest ledger sequence |
| `account_signers_current` | `account_signers_snapshot_v1` | ✅ Ready | Filter latest ledger sequence |
| `trustlines_current` | `trustlines_snapshot_v1` | ✅ Ready | Filter latest ledger sequence |
| `offers_current` | `offers_snapshot_v1` | ✅ Ready | Filter latest ledger sequence |
| `claimable_balances_current` | `claimable_balances_snapshot_v1` | ✅ Ready | Filter latest ledger sequence |
| `liquidity_pools_current` | `liquidity_pools_snapshot_v1` | ✅ Ready | Filter latest ledger sequence |
| `contract_data_current` | `contract_data_snapshot_v1` | ✅ Ready | Filter latest ledger sequence (Soroban) |
| `contract_code_current` | `contract_code_snapshot_v1` | ✅ Ready | Filter latest ledger sequence (Soroban) |
| `config_settings_current` | `config_settings_snapshot_v1` | ✅ Ready | Filter latest ledger sequence (Protocol 20+) |
| `ttl_current` | `ttl_snapshot_v1` | ✅ Ready | Filter latest ledger sequence (Soroban TTL) |

**Implementation**: Simple `WHERE closed_at = (SELECT MAX(closed_at) FROM ledgers_row_v2)` filter.

**Coverage**: 10 of 10 current state tables ✅

### 2. Historical State Tables (SCD Type 2)

These tables track **historical changes** using Slowly Changing Dimension Type 2 with `valid_from`/`valid_to` timestamps.

| Silver Table | Bronze Source | Status | Implementation Notes |
|--------------|---------------|--------|---------------------|
| `accounts_snapshot` | `accounts_snapshot_v1` | ✅ Ready | Add SCD Type 2 tracking (valid_from, valid_to) |
| `trustlines_snapshot` | `trustlines_snapshot_v1` | ✅ Ready | Add SCD Type 2 tracking |
| `liquidity_pools_snapshot` | `liquidity_pools_snapshot_v1` | ✅ Ready | Add SCD Type 2 tracking |
| `contract_data_snapshot` | `contract_data_snapshot_v1` | ✅ Ready | Add SCD Type 2 tracking (Soroban) |
| `evicted_keys_snapshot` | `evicted_keys_state_v1` | ✅ Ready | Add SCD Type 2 tracking (Soroban) |

**Implementation**:
```sql
-- Add valid_from (current closed_at) and valid_to (next change or NULL for current)
-- Track state changes across ledgers
SELECT
  *,
  closed_at AS valid_from,
  LEAD(closed_at) OVER (PARTITION BY <entity_id> ORDER BY closed_at) AS valid_to
FROM bronze.<snapshot_table>
```

**Coverage**: 5 of 5 snapshot tables ✅

### 3. Enriched History Tables

These tables **denormalize and enrich** history tables by joining operations/transactions/ledgers.

| Silver Table | Bronze Sources | Status | Implementation Notes |
|--------------|----------------|--------|---------------------|
| `enriched_history_operations` | `operations_row_v2` + `transactions_row_v2` + `ledgers_row_v2` | ⚠️ Partially Ready | **Currently stubbed** - needs full join implementation |
| `enriched_history_operations_soroban` | `operations_row_v2` + `transactions_row_v2` + `ledgers_row_v2` + `contract_events_stream_v1` | ⚠️ Partially Ready | Filter for Soroban operations (invoke_host_function, extend_footprint_ttl, restore_footprint) |

**Implementation**:
```sql
-- enriched_history_operations
SELECT
  o.*,
  -- Transaction fields (denormalized)
  t.transaction_hash,
  t.successful AS tx_successful,
  t.account AS tx_account,
  t.fee_charged AS tx_fee_charged,
  t.memo_type AS tx_memo_type,
  t.memo AS tx_memo,
  -- Ledger fields (denormalized)
  l.closed_at AS ledger_closed_at,
  l.successful_transaction_count AS ledger_successful_transaction_count,
  l.operation_count AS ledger_operation_count,
  -- Derived fields
  CASE WHEN o.type IN (1, 2, 13) THEN true ELSE false END AS is_payment
FROM bronze.operations_row_v2 o
JOIN bronze.transactions_row_v2 t ON o.transaction_id = t.id
JOIN bronze.ledgers_row_v2 l ON t.ledger_sequence = l.sequence
```

**Status**:
- ✅ Schema exists in silver-transformer/schema.go:31-73
- ⚠️ Transformation stubbed in silver-transformer/transformer.go:119-168
- 📋 Documented in silver-transformer/ARCHITECTURE.md

**Coverage**: 2 of 2 enriched tables (with implementation pending)

### 4. Derived Analytics Tables

These tables **extract and transform** specific business logic.

| Silver Table | Bronze Sources | Status | Implementation Notes |
|--------------|----------------|--------|---------------------|
| `token_transfers_raw` | `operations_row_v2` + `transactions_row_v2` + `contract_events_stream_v1` | ❓ Complex | Extract transfer events from operations (payment, path_payment_strict_send/receive) + Soroban contract events |

**Implementation Requirements**:
1. Parse payment operations (type 1, 2, 13)
2. Extract SEP-41 contract events from `contract_events_stream_v1`
3. Normalize into unified token transfer schema
4. Handle both classic Stellar assets and Soroban tokens

**Coverage**: 1 of 1 analytics table (not yet implemented)

## Implementation Readiness Matrix

### ✅ Can Implement Immediately (15 tables)

| Category | Count | Tables |
|----------|-------|--------|
| **Current State** | 10 | All `*_current` tables |
| **Historical Snapshots** | 5 | All `*_snapshot` tables |

**Why Ready**: Our bronze layer has all required snapshot tables. Implementation is straightforward SQL filtering/windowing.

### ⚠️ Partially Implemented (2 tables)

| Category | Count | Tables | Blocker |
|----------|-------|--------|---------|
| **Enriched History** | 2 | `enriched_history_operations`, `enriched_history_operations_soroban` | Schema exists but transformation logic stubbed |

**Why Partially Ready**:
- ✅ Schema defined correctly
- ✅ Bronze tables exist (operations_row_v2, transactions_row_v2, ledgers_row_v2)
- ⚠️ Transformation logic not fully implemented (see silver-transformer/transformer.go:119-168)

### ❓ Requires Analysis (1 table)

| Category | Count | Tables | Blocker |
|----------|-------|--------|---------|
| **Analytics** | 1 | `token_transfers_raw` | Complex extraction logic needed |

**Why Requires Analysis**:
- Complex business logic spanning multiple operation types
- SEP-41 contract event parsing
- Unified schema design across classic/Soroban assets

## Priority Implementation Plan

### Phase 1: Current State Tables (Immediate - 1 day)

**Goal**: Create 10 `*_current` tables from bronze snapshots.

**Effort**: Low - simple SQL filters

**Tables**:
1. accounts_current
2. account_signers_current
3. trustlines_current
4. offers_current
5. claimable_balances_current
6. liquidity_pools_current
7. contract_data_current
8. contract_code_current
9. config_settings_current
10. ttl_current

**Implementation Pattern**:
```go
func createAccountsCurrentTable(db *sql.DB) error {
    _, err := db.Exec(`
        CREATE TABLE IF NOT EXISTS accounts_current AS
        SELECT a.*
        FROM bronze.accounts_snapshot_v1 a
        WHERE a.closed_at = (
            SELECT MAX(l.closed_at)
            FROM bronze.ledgers_row_v2 l
        )
    `)
    return err
}
```

### Phase 2: Historical Snapshot Tables (Near-term - 2 days)

**Goal**: Create 5 `*_snapshot` tables with SCD Type 2 tracking.

**Effort**: Medium - windowing functions

**Tables**:
1. accounts_snapshot
2. trustlines_snapshot
3. liquidity_pools_snapshot
4. contract_data_snapshot
5. evicted_keys_snapshot

**Implementation Pattern**:
```go
func createAccountsSnapshotTable(db *sql.DB) error {
    _, err := db.Exec(`
        CREATE TABLE IF NOT EXISTS accounts_snapshot AS
        SELECT
            *,
            closed_at AS valid_from,
            LEAD(closed_at) OVER (
                PARTITION BY account_id
                ORDER BY closed_at
            ) AS valid_to
        FROM bronze.accounts_snapshot_v1
    `)
    return err
}
```

### Phase 3: Enriched History Operations (Current - 3 days)

**Goal**: Complete `enriched_history_operations` transformation.

**Effort**: Medium - joins implemented, transformation logic exists

**Status**:
- ✅ Schema created (schema.go:24-73)
- ⚠️ Transformation stubbed (transformer.go:119-168)
- ✅ Bronze tables available

**Implementation**:
```go
func (st *SilverTransformer) transformEnrichedOperations(lastSeq int64) error {
    // 1. Query operations, transactions, ledgers from bronze via Query API
    operations := st.bronzeClient.QueryOperations(st.config.BronzeSchema, lastSeq, st.config.BatchSize)
    transactions := st.bronzeClient.QueryTransactions(st.config.BronzeSchema, lastSeq, st.config.BatchSize)
    ledgers := st.bronzeClient.QueryLedgers(st.config.BronzeSchema, lastSeq, st.config.BatchSize)

    // 2. Join in memory (or use DuckDB JOIN if querying silver catalog)
    enriched := joinOperationsTransactionsLedgers(operations, transactions, ledgers)

    // 3. Insert into silver.enriched_history_operations
    st.insertEnrichedOperations(enriched)
}
```

**Dependency**: Need to add Query API methods for operations and transactions (currently only ledgers implemented).

### Phase 4: Enriched Soroban Operations (Follow-up - 2 days)

**Goal**: Filter enriched operations for Soroban-specific ops.

**Effort**: Low - reuse Phase 3 with additional filtering

**Implementation**:
```sql
CREATE TABLE enriched_history_operations_soroban AS
SELECT *
FROM enriched_history_operations
WHERE type_string IN (
    'invoke_host_function',
    'extend_footprint_ttl',
    'restore_footprint'
)
```

### Phase 5: Token Transfers (Future - 5 days)

**Goal**: Extract unified token transfer events.

**Effort**: High - complex extraction and parsing

**Requirements**:
1. Research SEP-41 event schema
2. Parse classic payment operations (type 1, 2, 13)
3. Parse Soroban contract events for token transfers
4. Design unified schema
5. Implement extraction logic

**Deferred**: This is complex and can be implemented after core silver tables are stable.

## Implementation Dependencies

### Bronze Layer Requirements

**All Required Bronze Tables**: ✅ Available

| Requirement | Status | Notes |
|-------------|--------|-------|
| operations_row_v2 | ✅ | Available |
| transactions_row_v2 | ✅ | Available |
| ledgers_row_v2 | ✅ | Available |
| All snapshot tables | ✅ | 11 snapshot tables available |
| contract_events_stream_v1 | ✅ | Available for Soroban |

### Query API Requirements

**Current Status**: Partial

| Method | Status | Priority |
|--------|--------|----------|
| QueryLedgers | ✅ Implemented | Done |
| QueryOperations | ❌ Missing | **High** - Needed for Phase 3 |
| QueryTransactions | ❌ Missing | **High** - Needed for Phase 3 |

**Action Required**:
1. Add `QueryOperations()` to bronze Query API (ducklake-ingestion-obsrvr-v3)
2. Add `QueryTransactions()` to bronze Query API
3. Add client methods in silver-transformer/query_client.go

### Silver Transformer Service

**Current Status**: Operational but limited

| Component | Status | Notes |
|-----------|--------|-------|
| Service framework | ✅ | Polling, health, metrics working |
| Checkpoint management | ✅ | Tracking progress |
| Query API client | ✅ | HTTP client working |
| Silver schema | ⚠️ | Only enriched_history_operations |
| Transformation logic | ⚠️ | Stubbed |

**Action Required**:
1. Expand schema.go to include all 18 silver tables
2. Implement transformation for each table category
3. Add batch processing for enriched tables

## Schema Comparison: Our Bronze → Hubble Silver

### Naming Alignment

**Our Bronze → Hubble Silver Mapping**:
```
accounts_snapshot_v1 → accounts_current, accounts_snapshot
trustlines_snapshot_v1 → trustlines_current, trustlines_snapshot
offers_snapshot_v1 → offers_current
liquidity_pools_snapshot_v1 → liquidity_pools_current, liquidity_pools_snapshot
contract_data_snapshot_v1 → contract_data_current, contract_data_snapshot
operations_row_v2 + transactions_row_v2 + ledgers_row_v2 → enriched_history_operations
```

### What We Have That Hubble Doesn't

| Our Table | Purpose | Include in Silver? |
|-----------|---------|-------------------|
| `native_balances_snapshot_v1` | XLM balance tracking | ✅ Yes - useful analytics |
| `evicted_keys_state_v1` | Soroban state archival | ✅ Yes - Hubble has evicted_keys_snapshot |
| `restored_keys_state_v1` | Soroban state restoration | ⚠️ Maybe - not in Hubble standard |
| `_meta_*` tables (5) | Lineage, quality, eras | ❌ No - operational metadata |

## Recommended Implementation Order

### Week 1: Foundation
1. ✅ **Phase 1**: Implement all 10 `*_current` tables (1 day)
2. ✅ **Phase 2**: Implement 5 `*_snapshot` SCD Type 2 tables (2 days)
3. 🧪 **Testing**: Verify current/snapshot tables populate correctly (2 days)

### Week 2: Enriched Operations
4. 🔧 **Bronze Extension**: Add QueryOperations/QueryTransactions to Query API (2 days)
5. ✅ **Phase 3**: Complete enriched_history_operations transformation (3 days)

### Week 3: Soroban & Polish
6. ✅ **Phase 4**: Implement enriched_history_operations_soroban (2 days)
7. 🧪 **Integration Testing**: End-to-end with bronze ingestion (2 days)
8. 📖 **Documentation**: Update README with all silver tables (1 day)

### Future: Advanced Analytics
9. 🔬 **Phase 5**: Design and implement token_transfers_raw (5 days)
10. 🚀 **Optimization**: Batch processing, indexing, performance tuning

## Success Criteria

### Phase 1-2 Success (Current/Snapshot Tables)
- [ ] All 10 `*_current` tables created and populated
- [ ] All 5 `*_snapshot` tables with valid_from/valid_to timestamps
- [ ] Query validation: `SELECT COUNT(*) FROM accounts_current` returns expected count
- [ ] SCD Type 2 validation: Historical changes tracked correctly

### Phase 3-4 Success (Enriched Operations)
- [ ] `enriched_history_operations` fully populated
- [ ] Transaction and ledger fields denormalized into operations
- [ ] `is_payment` flag correctly set
- [ ] `enriched_history_operations_soroban` filtered correctly
- [ ] Query performance < 1 second for recent operations

### Overall Success (All 18 Tables)
- [ ] 18 Hubble-standard silver tables implemented
- [ ] All tables update in real-time as bronze ingests new data
- [ ] Checkpoint resume works across all tables
- [ ] Health endpoint shows all table statistics
- [ ] Documentation complete with field mappings

## References

- [Stellar Hubble Silver Data Dictionary](https://developers.stellar.org/docs/data/analytics/hubble/data-catalog/data-dictionary/silver)
- [Bronze Layer Mapping](./BRONZE_LAYER_MAPPING.md)
- [Silver Transformer Architecture](./silver-transformer/ARCHITECTURE.md)
- Implementation: `/home/tillman/Documents/ttp-processor-demo/silver-transformer/`

## Next Steps

1. **Immediate**: Implement Phase 1 (`*_current` tables) - expand silver-transformer/schema.go
2. **This Week**: Implement Phase 2 (`*_snapshot` tables with SCD Type 2)
3. **Next Week**: Add QueryOperations/QueryTransactions to bronze Query API
4. **Next Week**: Complete Phase 3 (enriched_history_operations transformation)
