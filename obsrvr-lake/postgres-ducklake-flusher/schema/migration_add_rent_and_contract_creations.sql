-- Migration: Add rent_fee_charged to transactions and contract_creations_v1 table
-- C13: Rent tracking per transaction
-- C11: Contract creation tracking

-- Add rent_fee_charged column to transactions_row_v2
ALTER TABLE transactions_row_v2
  ADD COLUMN IF NOT EXISTS rent_fee_charged BIGINT;

-- Contract creations table (bronze) for tracking contract deployers
CREATE TABLE IF NOT EXISTS contract_creations_v1 (
  contract_id TEXT PRIMARY KEY,
  creator_address TEXT NOT NULL,
  wasm_hash TEXT,
  created_ledger BIGINT NOT NULL,
  created_at TIMESTAMP NOT NULL,
  ledger_range BIGINT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_contract_creations_creator
  ON contract_creations_v1 (creator_address);

CREATE INDEX IF NOT EXISTS idx_contract_creations_wasm_hash
  ON contract_creations_v1 (wasm_hash) WHERE wasm_hash IS NOT NULL;
