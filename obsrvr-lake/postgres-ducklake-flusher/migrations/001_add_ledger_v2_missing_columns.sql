-- Migration: Align ledgers_row_v2 with PostgreSQL hot buffer schema
-- Run via DuckDB CLI after ATTACHing the production DuckLake catalog
--
-- Existing cold data will have NULLs for the new columns — this is expected.

-- Rename existing columns to match PostgreSQL
ALTER TABLE bronze.ledgers_row_v2 RENAME COLUMN soroban_fee_write_1kb TO soroban_fee_write1kb;
ALTER TABLE bronze.ledgers_row_v2 RENAME COLUMN created_at TO ingestion_timestamp;

-- Add missing columns
ALTER TABLE bronze.ledgers_row_v2 ADD COLUMN node_id TEXT;
ALTER TABLE bronze.ledgers_row_v2 ADD COLUMN signature TEXT;
ALTER TABLE bronze.ledgers_row_v2 ADD COLUMN soroban_op_count INTEGER;
ALTER TABLE bronze.ledgers_row_v2 ADD COLUMN total_fee_charged BIGINT;
ALTER TABLE bronze.ledgers_row_v2 ADD COLUMN contract_events_count INTEGER;
ALTER TABLE bronze.ledgers_row_v2 ADD COLUMN bucket_list_size BIGINT;
ALTER TABLE bronze.ledgers_row_v2 ADD COLUMN live_soroban_state_size BIGINT;
ALTER TABLE bronze.ledgers_row_v2 ADD COLUMN evicted_keys_count INTEGER;
