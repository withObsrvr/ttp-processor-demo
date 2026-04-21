-- Migration 007: add incremental-versioning metadata to bronze hot tables
-- Safe to run repeatedly.

ALTER TABLE ledgers_row_v2 ADD COLUMN IF NOT EXISTS era_id TEXT;
ALTER TABLE ledgers_row_v2 ADD COLUMN IF NOT EXISTS version_label TEXT;

ALTER TABLE transactions_row_v2 ADD COLUMN IF NOT EXISTS era_id TEXT;
ALTER TABLE transactions_row_v2 ADD COLUMN IF NOT EXISTS version_label TEXT;

ALTER TABLE operations_row_v2 ADD COLUMN IF NOT EXISTS era_id TEXT;
ALTER TABLE operations_row_v2 ADD COLUMN IF NOT EXISTS version_label TEXT;

ALTER TABLE effects_row_v1 ADD COLUMN IF NOT EXISTS era_id TEXT;
ALTER TABLE effects_row_v1 ADD COLUMN IF NOT EXISTS version_label TEXT;

ALTER TABLE trades_row_v1 ADD COLUMN IF NOT EXISTS era_id TEXT;
ALTER TABLE trades_row_v1 ADD COLUMN IF NOT EXISTS version_label TEXT;

ALTER TABLE accounts_snapshot_v1 ADD COLUMN IF NOT EXISTS era_id TEXT;
ALTER TABLE accounts_snapshot_v1 ADD COLUMN IF NOT EXISTS version_label TEXT;

ALTER TABLE offers_snapshot_v1 ADD COLUMN IF NOT EXISTS era_id TEXT;
ALTER TABLE offers_snapshot_v1 ADD COLUMN IF NOT EXISTS version_label TEXT;

ALTER TABLE trustlines_snapshot_v1 ADD COLUMN IF NOT EXISTS era_id TEXT;
ALTER TABLE trustlines_snapshot_v1 ADD COLUMN IF NOT EXISTS version_label TEXT;

ALTER TABLE account_signers_snapshot_v1 ADD COLUMN IF NOT EXISTS era_id TEXT;
ALTER TABLE account_signers_snapshot_v1 ADD COLUMN IF NOT EXISTS version_label TEXT;

ALTER TABLE claimable_balances_snapshot_v1 ADD COLUMN IF NOT EXISTS era_id TEXT;
ALTER TABLE claimable_balances_snapshot_v1 ADD COLUMN IF NOT EXISTS version_label TEXT;

ALTER TABLE liquidity_pools_snapshot_v1 ADD COLUMN IF NOT EXISTS era_id TEXT;
ALTER TABLE liquidity_pools_snapshot_v1 ADD COLUMN IF NOT EXISTS version_label TEXT;

ALTER TABLE config_settings_snapshot_v1 ADD COLUMN IF NOT EXISTS era_id TEXT;
ALTER TABLE config_settings_snapshot_v1 ADD COLUMN IF NOT EXISTS version_label TEXT;

ALTER TABLE ttl_snapshot_v1 ADD COLUMN IF NOT EXISTS era_id TEXT;
ALTER TABLE ttl_snapshot_v1 ADD COLUMN IF NOT EXISTS version_label TEXT;

ALTER TABLE evicted_keys_state_v1 ADD COLUMN IF NOT EXISTS era_id TEXT;
ALTER TABLE evicted_keys_state_v1 ADD COLUMN IF NOT EXISTS version_label TEXT;

ALTER TABLE contract_events_stream_v1 ADD COLUMN IF NOT EXISTS era_id TEXT;
ALTER TABLE contract_events_stream_v1 ADD COLUMN IF NOT EXISTS version_label TEXT;

ALTER TABLE contract_data_snapshot_v1 ADD COLUMN IF NOT EXISTS era_id TEXT;
ALTER TABLE contract_data_snapshot_v1 ADD COLUMN IF NOT EXISTS version_label TEXT;

ALTER TABLE contract_code_snapshot_v1 ADD COLUMN IF NOT EXISTS era_id TEXT;
ALTER TABLE contract_code_snapshot_v1 ADD COLUMN IF NOT EXISTS version_label TEXT;

ALTER TABLE native_balances_snapshot_v1 ADD COLUMN IF NOT EXISTS era_id TEXT;
ALTER TABLE native_balances_snapshot_v1 ADD COLUMN IF NOT EXISTS version_label TEXT;

ALTER TABLE restored_keys_state_v1 ADD COLUMN IF NOT EXISTS era_id TEXT;
ALTER TABLE restored_keys_state_v1 ADD COLUMN IF NOT EXISTS version_label TEXT;

ALTER TABLE contract_creations_v1 ADD COLUMN IF NOT EXISTS era_id TEXT;
ALTER TABLE contract_creations_v1 ADD COLUMN IF NOT EXISTS version_label TEXT;

ALTER TABLE token_transfers_stream_v1 ADD COLUMN IF NOT EXISTS era_id TEXT;
ALTER TABLE token_transfers_stream_v1 ADD COLUMN IF NOT EXISTS version_label TEXT;
