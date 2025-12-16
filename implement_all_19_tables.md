# Complete Implementation Plan: All 19 Tables

## Scope
Implement full extraction and ingestion for all 19 Hubble Bronze tables.

## Current State
- ✅ PostgreSQL schema: All 19 tables created
- ✅ DuckLake Bronze schema: All 19 tables created
- ⚠️  Ingester: Only 5 tables have working extractors
- ❌ Missing: 14 table extractors

## Implementation Approach

### Phase 1: Copy V3 Reference Code (30 min)
Extract v3's table extraction patterns into reference docs for each missing table.

###  Phase 2: Implement 14 Missing Extractors (3-4 hours)
Using v3 patterns, implement extractors for:
- accounts_snapshot_v1
- trustlines_snapshot_v1
- account_signers_snapshot_v1
- native_balances_snapshot_v1
- offers_snapshot_v1
- liquidity_pools_snapshot_v1
- claimable_balances_snapshot_v1
- contract_data_snapshot_v1
- contract_code_snapshot_v1
- restored_keys_state_v1
- ttl_snapshot_v1
- evicted_keys_state_v1
- config_settings_snapshot_v1
- (contract_events already works!)

### Phase 3: Update 5 Existing Extractors (30 min)
Update ledgers, transactions, operations, effects, trades to match Bronze fields exactly.

### Phase 4: Integration Test (30 min)
- Re-ingest 1000 ledgers
- Verify all 19 tables populated
- Test flush to DuckLake

## Time Estimate
**Total: 5-6 hours of focused development**

## Decision Point
Given this is 5-6 hours of work, we have options:
A) Implement all 19 tables now (complete Option B from earlier)
B) Ship Cycle 3 with 5 tables, document remaining work as Cycle 2 Extension backlog
C) Implement in stages: 5 tables → test flush → add remaining 14

Which approach would you prefer?
