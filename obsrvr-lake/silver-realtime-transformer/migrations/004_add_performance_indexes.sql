-- Phase 4: Performance indexes for bronze hot and silver hot
-- Run these on the respective databases to improve transformer read performance.
-- All use CONCURRENTLY to avoid blocking writes.

-- =============================================================================
-- BRONZE HOT (stellar_hot) — Run on bronze hot database
-- =============================================================================
-- These indexes support the WHERE ledger_sequence BETWEEN $1 AND $2 range scans
-- that every transform function uses to read from bronze.

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_operations_ledger_seq
    ON operations_row_v2 (ledger_sequence);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_transactions_ledger_seq
    ON transactions_row_v2 (ledger_sequence);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_contract_events_ledger_seq
    ON contract_events_stream_v1 (ledger_sequence);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_accounts_snapshot_ledger_seq
    ON accounts_snapshot_v1 (ledger_sequence);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_trustlines_snapshot_ledger_seq
    ON trustlines_snapshot_v1 (ledger_sequence);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_effects_ledger_seq
    ON effects_row_v1 (ledger_sequence);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_contract_data_ledger_seq
    ON contract_data_snapshot_v1 (ledger_sequence);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_contract_creations_ledger
    ON contract_creations_v1 (created_ledger);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_offers_snapshot_ledger_seq
    ON offers_snapshot_v1 (ledger_sequence);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_account_signers_ledger_seq
    ON account_signers_snapshot_v1 (ledger_sequence);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_contract_code_ledger_seq
    ON contract_code_snapshot_v1 (ledger_sequence);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_ttl_snapshot_ledger_seq
    ON ttl_snapshot_v1 (ledger_sequence);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_liquidity_pools_ledger_seq
    ON liquidity_pools_snapshot_v1 (ledger_sequence);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_claimable_balances_ledger_seq
    ON claimable_balances_snapshot_v1 (ledger_sequence);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_config_settings_ledger_seq
    ON config_settings_snapshot_v1 (ledger_sequence);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_trades_ledger_seq
    ON trades_row_v1 (ledger_sequence);

-- =============================================================================
-- SILVER HOT (silver_hot) — Run on silver hot database
-- =============================================================================
-- Supports the semantic_entities_contracts Phase 3 fix (incremental upsert)
-- and general semantic transform performance.

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_contract_invocations_contract_id
    ON contract_invocations_raw (contract_id);
