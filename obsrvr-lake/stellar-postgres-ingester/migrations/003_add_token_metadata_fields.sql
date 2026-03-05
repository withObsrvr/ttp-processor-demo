-- Migration: Add token metadata fields to contract_data_snapshot_v1
-- Purpose: Store decoded token name/symbol/decimals from Soroban METADATA key
-- Date: 2026-03-05

BEGIN;

-- Add token_name column (from METADATA.name in contract instance storage)
ALTER TABLE contract_data_snapshot_v1
ADD COLUMN IF NOT EXISTS token_name TEXT;

-- Add token_symbol column (from METADATA.symbol in contract instance storage)
ALTER TABLE contract_data_snapshot_v1
ADD COLUMN IF NOT EXISTS token_symbol TEXT;

-- Add token_decimals column (from METADATA.decimal in contract instance storage)
ALTER TABLE contract_data_snapshot_v1
ADD COLUMN IF NOT EXISTS token_decimals INTEGER;

-- Create index for querying contract instances with token metadata
CREATE INDEX IF NOT EXISTS idx_contract_data_token_name
ON contract_data_snapshot_v1 (token_name)
WHERE token_name IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_contract_data_token_symbol
ON contract_data_snapshot_v1 (token_symbol)
WHERE token_symbol IS NOT NULL;

-- Verify columns exist
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'contract_data_snapshot_v1'
        AND column_name = 'token_name'
    ) THEN
        RAISE EXCEPTION 'Failed to add token_name column';
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'contract_data_snapshot_v1'
        AND column_name = 'token_symbol'
    ) THEN
        RAISE EXCEPTION 'Failed to add token_symbol column';
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'contract_data_snapshot_v1'
        AND column_name = 'token_decimals'
    ) THEN
        RAISE EXCEPTION 'Failed to add token_decimals column';
    END IF;

    RAISE NOTICE 'Successfully added token metadata fields to contract_data_snapshot_v1';
END $$;

COMMIT;
