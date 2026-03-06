-- Migration: Add Soroban aggregate columns to ledgers_row_v2
-- These columns enable per-ledger Soroban statistics without JOINs

-- Bronze hot (PostgreSQL)
ALTER TABLE ledgers_row_v2
  ADD COLUMN IF NOT EXISTS soroban_op_count INTEGER,
  ADD COLUMN IF NOT EXISTS total_fee_charged BIGINT,
  ADD COLUMN IF NOT EXISTS contract_events_count INTEGER;

-- For DuckLake cold storage, these columns need to be added to the DuckLake catalog
-- via INSERT into ducklake_column (see DuckLake schema management docs)
