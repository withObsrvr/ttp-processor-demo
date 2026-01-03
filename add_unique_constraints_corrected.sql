-- Add PRIMARY KEY constraints to all tables for ON CONFLICT support
-- Based on actual ON CONFLICT clauses in stellar-postgres-ingester/go/writer.go

-- Drop any existing incorrect constraints first (if needed)
-- ALTER TABLE operations_row_v2 DROP CONSTRAINT IF EXISTS operations_row_v2_pkey;
-- ALTER TABLE trustlines_snapshot_v1 DROP CONSTRAINT IF EXISTS trustlines_snapshot_v1_pkey;
-- ALTER TABLE contract_events_stream_v1 DROP CONSTRAINT IF EXISTS contract_events_stream_v1_pkey;
-- ALTER TABLE contract_data_snapshot_v1 DROP CONSTRAINT IF EXISTS contract_data_snapshot_v1_pkey;

-- Core ledger and transaction tables
-- writer.go:629 - ON CONFLICT (sequence)
ALTER TABLE ledgers_row_v2 ADD PRIMARY KEY (sequence);

-- writer.go:684 - ON CONFLICT (ledger_sequence, transaction_hash)
ALTER TABLE transactions_row_v2 ADD PRIMARY KEY (ledger_sequence, transaction_hash);

-- writer.go:730 - ON CONFLICT (ledger_sequence, transaction_hash, operation_index)
ALTER TABLE operations_row_v2 ADD PRIMARY KEY (ledger_sequence, transaction_hash, operation_index);

-- writer.go:777 - ON CONFLICT (ledger_sequence, transaction_hash, operation_index, effect_index)
ALTER TABLE effects_row_v1 ADD PRIMARY KEY (ledger_sequence, transaction_hash, operation_index, effect_index);

-- writer.go:829 - ON CONFLICT (ledger_sequence, transaction_hash, operation_index, trade_index)
ALTER TABLE trades_row_v1 ADD PRIMARY KEY (ledger_sequence, transaction_hash, operation_index, trade_index);

-- writer.go:1334 - ON CONFLICT (ledger_sequence, transaction_hash, event_index)
ALTER TABLE contract_events_stream_v1 ADD PRIMARY KEY (ledger_sequence, transaction_hash, event_index);

-- Snapshot tables (entity + ledger_sequence)
-- writer.go:880 - ON CONFLICT (account_id, ledger_sequence)
ALTER TABLE accounts_snapshot_v1 ADD PRIMARY KEY (account_id, ledger_sequence);

-- writer.go:1036 - ON CONFLICT (account_id, signer, ledger_sequence)
ALTER TABLE account_signers_snapshot_v1 ADD PRIMARY KEY (account_id, signer, ledger_sequence);

-- writer.go:987 - ON CONFLICT (account_id, asset_code, asset_issuer, asset_type, ledger_sequence)
ALTER TABLE trustlines_snapshot_v1 ADD PRIMARY KEY (account_id, asset_code, asset_issuer, asset_type, ledger_sequence);

-- writer.go:1511 - ON CONFLICT (account_id, ledger_sequence)
ALTER TABLE native_balances_snapshot_v1 ADD PRIMARY KEY (account_id, ledger_sequence);

-- writer.go:939 - ON CONFLICT (offer_id, ledger_sequence)
ALTER TABLE offers_snapshot_v1 ADD PRIMARY KEY (offer_id, ledger_sequence);

-- writer.go:1135 - ON CONFLICT (liquidity_pool_id, ledger_sequence)
ALTER TABLE liquidity_pools_snapshot_v1 ADD PRIMARY KEY (liquidity_pool_id, ledger_sequence);

-- writer.go:1086 - ON CONFLICT (balance_id, ledger_sequence)
ALTER TABLE claimable_balances_snapshot_v1 ADD PRIMARY KEY (balance_id, ledger_sequence);

-- Contract data tables
-- writer.go:1389 - ON CONFLICT (contract_id, ledger_key_hash, ledger_sequence)
ALTER TABLE contract_data_snapshot_v1 ADD PRIMARY KEY (contract_id, ledger_key_hash, ledger_sequence);

-- writer.go:1448 - ON CONFLICT (contract_code_hash, ledger_sequence)
ALTER TABLE contract_code_snapshot_v1 ADD PRIMARY KEY (contract_code_hash, ledger_sequence);

-- Config and state tables
-- writer.go:1188 - ON CONFLICT (config_setting_id, ledger_sequence)
ALTER TABLE config_settings_snapshot_v1 ADD PRIMARY KEY (config_setting_id, ledger_sequence);

-- writer.go:1558 - ON CONFLICT (key_hash, ledger_sequence)
ALTER TABLE ttl_snapshot_v1 ADD PRIMARY KEY (key_hash, ledger_sequence);

-- writer.go:1252 - ON CONFLICT (key_hash, ledger_sequence)
ALTER TABLE evicted_keys_state_v1 ADD PRIMARY KEY (key_hash, ledger_sequence);

-- writer.go:1294 - ON CONFLICT (key_hash, ledger_sequence)
ALTER TABLE restored_keys_state_v1 ADD PRIMARY KEY (key_hash, ledger_sequence);
