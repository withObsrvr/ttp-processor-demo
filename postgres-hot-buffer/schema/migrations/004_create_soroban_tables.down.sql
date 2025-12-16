-- Rollback migration 004: Drop Soroban tables

DROP TABLE IF EXISTS restored_keys_state_v1;
DROP TABLE IF EXISTS evicted_keys_state_v1;
DROP TABLE IF EXISTS ttl_snapshot_v1;
DROP TABLE IF EXISTS config_settings_snapshot_v1;
DROP TABLE IF EXISTS contract_code_snapshot_v1;
DROP TABLE IF EXISTS contract_data_snapshot_v1;
DROP TABLE IF EXISTS contract_events_stream_v1;
