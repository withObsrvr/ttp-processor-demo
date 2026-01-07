-- Migration: Add contract invocation fields to operations_row_v2
-- Purpose: Enable extraction of contract invocation arguments and TOID generation
-- Date: 2026-01-03

BEGIN;

-- Add transaction_index column (for TOID generation)
ALTER TABLE operations_row_v2
ADD COLUMN IF NOT EXISTS transaction_index INT;

-- Add soroban_arguments_json column (for contract invocation arguments)
ALTER TABLE operations_row_v2
ADD COLUMN IF NOT EXISTS soroban_arguments_json TEXT;

-- Verify columns exist
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'operations_row_v2'
        AND column_name = 'transaction_index'
    ) THEN
        RAISE EXCEPTION 'Failed to add transaction_index column';
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'operations_row_v2'
        AND column_name = 'soroban_arguments_json'
    ) THEN
        RAISE EXCEPTION 'Failed to add soroban_arguments_json column';
    END IF;

    RAISE NOTICE 'Successfully added contract invocation fields to operations_row_v2';
END $$;

COMMIT;
