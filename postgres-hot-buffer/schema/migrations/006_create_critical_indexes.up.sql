-- Migration 006: Create critical indexes for flush performance
-- Total: 38 indexes (2 per table × 19 tables)
-- Purpose: Enable fast high-watermark queries for flush process
--
-- Index 1: ledger_range - For high-watermark flush queries (WHERE ledger_range <= watermark)
-- Index 2: sequence/ledger_sequence - For ordered iteration
--
-- These are the ONLY indexes in hot buffer (staging layer, not analytics layer)

-- ============================================================================
-- CORE TRANSACTION TABLES (3 tables × 2 indexes = 6 indexes)
-- ============================================================================

-- Ledgers
CREATE INDEX idx_ledgers_range ON ledgers_row_v2 (ledger_range);
CREATE INDEX idx_ledgers_sequence ON ledgers_row_v2 (sequence);

-- Transactions
CREATE INDEX idx_transactions_range ON transactions_row_v2 (ledger_range);
CREATE INDEX idx_transactions_ledger_seq ON transactions_row_v2 (ledger_sequence);

-- Operations
CREATE INDEX idx_operations_range ON operations_row_v2 (ledger_range);
CREATE INDEX idx_operations_ledger_seq ON operations_row_v2 (ledger_sequence);

-- ============================================================================
-- EFFECTS & TRADES (2 tables × 2 indexes = 4 indexes)
-- ============================================================================

-- Effects
CREATE INDEX idx_effects_range ON effects_row_v1 (ledger_range);
CREATE INDEX idx_effects_ledger_seq ON effects_row_v1 (ledger_sequence);

-- Trades
CREATE INDEX idx_trades_range ON trades_row_v1 (ledger_range);
CREATE INDEX idx_trades_ledger_seq ON trades_row_v1 (ledger_sequence);

-- ============================================================================
-- SNAPSHOT TABLES (7 tables × 2 indexes = 14 indexes)
-- ============================================================================

-- Accounts
CREATE INDEX idx_accounts_range ON accounts_snapshot_v1 (ledger_range);
CREATE INDEX idx_accounts_ledger_seq ON accounts_snapshot_v1 (ledger_sequence);

-- Trustlines
CREATE INDEX idx_trustlines_range ON trustlines_snapshot_v1 (ledger_range);
CREATE INDEX idx_trustlines_ledger_seq ON trustlines_snapshot_v1 (ledger_sequence);

-- Native Balances
CREATE INDEX idx_native_balances_range ON native_balances_snapshot_v1 (ledger_range);
CREATE INDEX idx_native_balances_ledger_seq ON native_balances_snapshot_v1 (ledger_sequence);

-- Account Signers
CREATE INDEX idx_account_signers_range ON account_signers_snapshot_v1 (ledger_range);
CREATE INDEX idx_account_signers_ledger_seq ON account_signers_snapshot_v1 (ledger_sequence);

-- Offers
CREATE INDEX idx_offers_range ON offers_snapshot_v1 (ledger_range);
CREATE INDEX idx_offers_ledger_seq ON offers_snapshot_v1 (ledger_sequence);

-- Claimable Balances
CREATE INDEX idx_claimable_balances_range ON claimable_balances_snapshot_v1 (ledger_range);
CREATE INDEX idx_claimable_balances_ledger_seq ON claimable_balances_snapshot_v1 (ledger_sequence);

-- Liquidity Pools
CREATE INDEX idx_liquidity_pools_range ON liquidity_pools_snapshot_v1 (ledger_range);
CREATE INDEX idx_liquidity_pools_ledger_seq ON liquidity_pools_snapshot_v1 (ledger_sequence);

-- ============================================================================
-- SOROBAN TABLES (7 tables × 2 indexes = 14 indexes)
-- ============================================================================

-- Contract Events
CREATE INDEX idx_contract_events_range ON contract_events_stream_v1 (ledger_range);
CREATE INDEX idx_contract_events_ledger_seq ON contract_events_stream_v1 (ledger_sequence);

-- Contract Data
CREATE INDEX idx_contract_data_range ON contract_data_snapshot_v1 (ledger_range);
CREATE INDEX idx_contract_data_ledger_seq ON contract_data_snapshot_v1 (ledger_sequence);

-- Contract Code
CREATE INDEX idx_contract_code_range ON contract_code_snapshot_v1 (ledger_range);
CREATE INDEX idx_contract_code_ledger_seq ON contract_code_snapshot_v1 (ledger_sequence);

-- Config Settings
CREATE INDEX idx_config_settings_range ON config_settings_snapshot_v1 (ledger_range);
CREATE INDEX idx_config_settings_ledger_seq ON config_settings_snapshot_v1 (ledger_sequence);

-- TTL
CREATE INDEX idx_ttl_range ON ttl_snapshot_v1 (ledger_range);
CREATE INDEX idx_ttl_ledger_seq ON ttl_snapshot_v1 (ledger_sequence);

-- Evicted Keys
CREATE INDEX idx_evicted_keys_range ON evicted_keys_state_v1 (ledger_range);
CREATE INDEX idx_evicted_keys_ledger_seq ON evicted_keys_state_v1 (ledger_sequence);

-- Restored Keys
CREATE INDEX idx_restored_keys_range ON restored_keys_state_v1 (ledger_range);
CREATE INDEX idx_restored_keys_ledger_seq ON restored_keys_state_v1 (ledger_sequence);

-- ============================================================================
-- Summary: 38 indexes total
-- ============================================================================
-- Core (3): 6 indexes
-- Effects/Trades (2): 4 indexes
-- Snapshots (7): 14 indexes
-- Soroban (7): 14 indexes
-- Total: 38 indexes
--
-- Metadata tables (_meta_*) do NOT get these indexes because:
-- - They're not flushed (they stay in hot buffer permanently)
-- - They're low volume (metadata only)
-- - They're queried differently (not by ledger_range)
