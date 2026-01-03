-- Add PRIMARY KEY constraints to all tables for ON CONFLICT support
-- This enables upsert operations in the stellar-postgres-ingester

-- Core ledger and transaction tables
ALTER TABLE ledgers_row_v2 ADD PRIMARY KEY (sequence);
ALTER TABLE transactions_row_v2 ADD PRIMARY KEY (ledger_sequence, transaction_hash);
ALTER TABLE operations_row_v2 ADD PRIMARY KEY (transaction_hash, operation_index);
ALTER TABLE effects_row_v1 ADD PRIMARY KEY (ledger_sequence, transaction_hash, operation_index, effect_index);
ALTER TABLE trades_row_v1 ADD PRIMARY KEY (ledger_sequence, transaction_hash, operation_index, trade_index);

-- Contract events
ALTER TABLE contract_events_stream_v1 ADD PRIMARY KEY (event_id);

-- Snapshot tables (entity + ledger_sequence)
ALTER TABLE accounts_snapshot_v1 ADD PRIMARY KEY (account_id, ledger_sequence);
ALTER TABLE account_signers_snapshot_v1 ADD PRIMARY KEY (account_id, signer, ledger_sequence);
ALTER TABLE trustlines_snapshot_v1 ADD PRIMARY KEY (account_id, asset_code, asset_issuer, asset_type, ledger_sequence);
ALTER TABLE native_balances_snapshot_v1 ADD PRIMARY KEY (account_id, ledger_sequence);
ALTER TABLE offers_snapshot_v1 ADD PRIMARY KEY (offer_id, ledger_sequence);
ALTER TABLE liquidity_pools_snapshot_v1 ADD PRIMARY KEY (liquidity_pool_id, ledger_sequence);
ALTER TABLE claimable_balances_snapshot_v1 ADD PRIMARY KEY (balance_id, ledger_sequence);

-- Contract data tables
ALTER TABLE contract_data_snapshot_v1 ADD PRIMARY KEY (ledger_key_hash, ledger_sequence);
ALTER TABLE contract_code_snapshot_v1 ADD PRIMARY KEY (contract_code_hash, ledger_sequence);

-- Config and state tables
ALTER TABLE config_settings_snapshot_v1 ADD PRIMARY KEY (config_setting_id, ledger_sequence);
ALTER TABLE ttl_snapshot_v1 ADD PRIMARY KEY (key_hash, ledger_sequence);
ALTER TABLE evicted_keys_state_v1 ADD PRIMARY KEY (key_hash, ledger_sequence);
ALTER TABLE restored_keys_state_v1 ADD PRIMARY KEY (key_hash, ledger_sequence);
