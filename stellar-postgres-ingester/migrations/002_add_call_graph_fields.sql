-- Migration: Add call graph fields to operations_row_v2
-- Purpose: Enable tracking of cross-contract call hierarchies for Freighter
-- Date: 2026-01-04

BEGIN;

-- Add contract_calls_json column (JSONB array of call relationships)
-- Structure: [{"from": "CXXX", "to": "CYYY", "function": "swap", "depth": 1, "order": 0}, ...]
ALTER TABLE operations_row_v2
ADD COLUMN IF NOT EXISTS contract_calls_json JSONB;

-- Add contracts_involved column (array of all contract IDs in the call chain)
ALTER TABLE operations_row_v2
ADD COLUMN IF NOT EXISTS contracts_involved TEXT[];

-- Add max_call_depth column (maximum depth of nested calls, 0 = no sub-calls)
ALTER TABLE operations_row_v2
ADD COLUMN IF NOT EXISTS max_call_depth INT;

-- Create index on contracts_involved for efficient lookups
-- Using GIN index for array containment queries
CREATE INDEX IF NOT EXISTS idx_operations_contracts_involved
ON operations_row_v2 USING GIN (contracts_involved)
WHERE contracts_involved IS NOT NULL;

-- Create index on contract_calls_json for JSONB queries
CREATE INDEX IF NOT EXISTS idx_operations_contract_calls
ON operations_row_v2 USING GIN (contract_calls_json)
WHERE contract_calls_json IS NOT NULL;

-- Create index on max_call_depth for filtering operations with cross-contract calls
CREATE INDEX IF NOT EXISTS idx_operations_call_depth
ON operations_row_v2 (max_call_depth)
WHERE max_call_depth IS NOT NULL AND max_call_depth > 0;

-- Verify columns exist
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'operations_row_v2'
        AND column_name = 'contract_calls_json'
    ) THEN
        RAISE EXCEPTION 'Failed to add contract_calls_json column';
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'operations_row_v2'
        AND column_name = 'contracts_involved'
    ) THEN
        RAISE EXCEPTION 'Failed to add contracts_involved column';
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'operations_row_v2'
        AND column_name = 'max_call_depth'
    ) THEN
        RAISE EXCEPTION 'Failed to add max_call_depth column';
    END IF;

    RAISE NOTICE 'Successfully added call graph fields to operations_row_v2';
END $$;

COMMIT;
