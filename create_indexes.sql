-- Create indexes for all 19 tables (ledger_range + sequence/ledger_sequence)

-- account_signers_snapshot_v1
CREATE INDEX IF NOT EXISTS idx_account_signers_range ON account_signers_snapshot_v1(ledger_range);
CREATE INDEX IF NOT EXISTS idx_account_signers_sequence ON account_signers_snapshot_v1(ledger_sequence);

-- accounts_snapshot_v1
CREATE INDEX IF NOT EXISTS idx_accounts_range ON accounts_snapshot_v1(ledger_range);
CREATE INDEX IF NOT EXISTS idx_accounts_sequence ON accounts_snapshot_v1(ledger_sequence);

-- claimable_balances_snapshot_v1
CREATE INDEX IF NOT EXISTS idx_claimable_balances_range ON claimable_balances_snapshot_v1(ledger_range);
CREATE INDEX IF NOT EXISTS idx_claimable_balances_sequence ON claimable_balances_snapshot_v1(ledger_sequence);

-- config_settings_snapshot_v1
CREATE INDEX IF NOT EXISTS idx_config_settings_range ON config_settings_snapshot_v1(ledger_range);
CREATE INDEX IF NOT EXISTS idx_config_settings_sequence ON config_settings_snapshot_v1(ledger_sequence);

-- contract_code_snapshot_v1
CREATE INDEX IF NOT EXISTS idx_contract_code_range ON contract_code_snapshot_v1(ledger_range);
CREATE INDEX IF NOT EXISTS idx_contract_code_sequence ON contract_code_snapshot_v1(ledger_sequence);

-- contract_data_snapshot_v1
CREATE INDEX IF NOT EXISTS idx_contract_data_range ON contract_data_snapshot_v1(ledger_range);
CREATE INDEX IF NOT EXISTS idx_contract_data_sequence ON contract_data_snapshot_v1(ledger_sequence);

-- contract_events_stream_v1
CREATE INDEX IF NOT EXISTS idx_contract_events_range ON contract_events_stream_v1(ledger_range);
CREATE INDEX IF NOT EXISTS idx_contract_events_sequence ON contract_events_stream_v1(ledger_sequence);

-- effects_row_v1
CREATE INDEX IF NOT EXISTS idx_effects_range ON effects_row_v1(ledger_range);
CREATE INDEX IF NOT EXISTS idx_effects_sequence ON effects_row_v1(ledger_sequence);

-- evicted_keys_state_v1
CREATE INDEX IF NOT EXISTS idx_evicted_keys_range ON evicted_keys_state_v1(ledger_range);
CREATE INDEX IF NOT EXISTS idx_evicted_keys_sequence ON evicted_keys_state_v1(ledger_sequence);

-- ledgers_row_v2
CREATE INDEX IF NOT EXISTS idx_ledgers_range ON ledgers_row_v2(ledger_range);
CREATE INDEX IF NOT EXISTS idx_ledgers_sequence ON ledgers_row_v2(sequence);

-- liquidity_pools_snapshot_v1
CREATE INDEX IF NOT EXISTS idx_liquidity_pools_range ON liquidity_pools_snapshot_v1(ledger_range);
CREATE INDEX IF NOT EXISTS idx_liquidity_pools_sequence ON liquidity_pools_snapshot_v1(ledger_sequence);

-- native_balances_snapshot_v1
CREATE INDEX IF NOT EXISTS idx_native_balances_range ON native_balances_snapshot_v1(ledger_range);
CREATE INDEX IF NOT EXISTS idx_native_balances_sequence ON native_balances_snapshot_v1(ledger_sequence);

-- offers_snapshot_v1
CREATE INDEX IF NOT EXISTS idx_offers_range ON offers_snapshot_v1(ledger_range);
CREATE INDEX IF NOT EXISTS idx_offers_sequence ON offers_snapshot_v1(ledger_sequence);

-- operations_row_v2
CREATE INDEX IF NOT EXISTS idx_operations_range ON operations_row_v2(ledger_range);
CREATE INDEX IF NOT EXISTS idx_operations_sequence ON operations_row_v2(ledger_sequence);

-- restored_keys_state_v1
CREATE INDEX IF NOT EXISTS idx_restored_keys_range ON restored_keys_state_v1(ledger_range);
CREATE INDEX IF NOT EXISTS idx_restored_keys_sequence ON restored_keys_state_v1(ledger_sequence);

-- trades_row_v1
CREATE INDEX IF NOT EXISTS idx_trades_range ON trades_row_v1(ledger_range);
CREATE INDEX IF NOT EXISTS idx_trades_sequence ON trades_row_v1(ledger_sequence);

-- transactions_row_v2
CREATE INDEX IF NOT EXISTS idx_transactions_range ON transactions_row_v2(ledger_range);
CREATE INDEX IF NOT EXISTS idx_transactions_sequence ON transactions_row_v2(ledger_sequence);

-- trustlines_snapshot_v1
CREATE INDEX IF NOT EXISTS idx_trustlines_range ON trustlines_snapshot_v1(ledger_range);
CREATE INDEX IF NOT EXISTS idx_trustlines_sequence ON trustlines_snapshot_v1(ledger_sequence);

-- ttl_snapshot_v1
CREATE INDEX IF NOT EXISTS idx_ttl_range ON ttl_snapshot_v1(ledger_range);
CREATE INDEX IF NOT EXISTS idx_ttl_sequence ON ttl_snapshot_v1(ledger_sequence);
