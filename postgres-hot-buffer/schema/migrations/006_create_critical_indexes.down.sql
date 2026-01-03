-- Rollback migration 006: Drop all critical indexes

-- Soroban indexes (14)
DROP INDEX IF EXISTS idx_restored_keys_ledger_seq;
DROP INDEX IF EXISTS idx_restored_keys_range;
DROP INDEX IF EXISTS idx_evicted_keys_ledger_seq;
DROP INDEX IF EXISTS idx_evicted_keys_range;
DROP INDEX IF EXISTS idx_ttl_ledger_seq;
DROP INDEX IF EXISTS idx_ttl_range;
DROP INDEX IF EXISTS idx_config_settings_ledger_seq;
DROP INDEX IF EXISTS idx_config_settings_range;
DROP INDEX IF EXISTS idx_contract_code_ledger_seq;
DROP INDEX IF EXISTS idx_contract_code_range;
DROP INDEX IF EXISTS idx_contract_data_ledger_seq;
DROP INDEX IF EXISTS idx_contract_data_range;
DROP INDEX IF EXISTS idx_contract_events_ledger_seq;
DROP INDEX IF EXISTS idx_contract_events_range;

-- Snapshot indexes (14)
DROP INDEX IF EXISTS idx_liquidity_pools_ledger_seq;
DROP INDEX IF EXISTS idx_liquidity_pools_range;
DROP INDEX IF EXISTS idx_claimable_balances_ledger_seq;
DROP INDEX IF EXISTS idx_claimable_balances_range;
DROP INDEX IF EXISTS idx_offers_ledger_seq;
DROP INDEX IF EXISTS idx_offers_range;
DROP INDEX IF EXISTS idx_account_signers_ledger_seq;
DROP INDEX IF EXISTS idx_account_signers_range;
DROP INDEX IF EXISTS idx_native_balances_ledger_seq;
DROP INDEX IF EXISTS idx_native_balances_range;
DROP INDEX IF EXISTS idx_trustlines_ledger_seq;
DROP INDEX IF EXISTS idx_trustlines_range;
DROP INDEX IF EXISTS idx_accounts_ledger_seq;
DROP INDEX IF EXISTS idx_accounts_range;

-- Effects/Trades indexes (4)
DROP INDEX IF EXISTS idx_trades_ledger_seq;
DROP INDEX IF EXISTS idx_trades_range;
DROP INDEX IF EXISTS idx_effects_ledger_seq;
DROP INDEX IF EXISTS idx_effects_range;

-- Core indexes (6)
DROP INDEX IF EXISTS idx_operations_ledger_seq;
DROP INDEX IF EXISTS idx_operations_range;
DROP INDEX IF EXISTS idx_transactions_ledger_seq;
DROP INDEX IF EXISTS idx_transactions_range;
DROP INDEX IF EXISTS idx_ledgers_sequence;
DROP INDEX IF EXISTS idx_ledgers_range;
