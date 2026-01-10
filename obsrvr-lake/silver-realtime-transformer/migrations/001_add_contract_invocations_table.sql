-- Migration: Create contract_invocations_raw table
-- Purpose: Store transformed contract invocations with TOID support
-- Date: 2026-01-03

BEGIN;

-- Create contract_invocations_raw table
CREATE TABLE IF NOT EXISTS contract_invocations_raw (
    -- TOID components (enables TOID computation)
    ledger_sequence BIGINT NOT NULL,
    transaction_index INTEGER NOT NULL,
    operation_index INTEGER NOT NULL,

    -- Transaction context
    transaction_hash VARCHAR(64) NOT NULL,
    source_account VARCHAR(56) NOT NULL,

    -- Contract invocation details
    contract_id VARCHAR(100) NOT NULL,
    function_name VARCHAR(100) NOT NULL,
    arguments_json TEXT NOT NULL,

    -- Execution context
    successful BOOLEAN NOT NULL,
    closed_at TIMESTAMP NOT NULL,

    -- Partitioning
    ledger_range BIGINT NOT NULL,

    -- Metadata
    inserted_at TIMESTAMP DEFAULT NOW(),

    -- Primary key (enables TOID computation: ledger_sequence << 32 | transaction_index << 12 | operation_index)
    PRIMARY KEY (ledger_sequence, transaction_index, operation_index)
);

-- Create indexes for contract invocation queries
CREATE INDEX IF NOT EXISTS idx_contract_invocations_contract_id ON contract_invocations_raw(contract_id);
CREATE INDEX IF NOT EXISTS idx_contract_invocations_function ON contract_invocations_raw(function_name);
CREATE INDEX IF NOT EXISTS idx_contract_invocations_ledger_range ON contract_invocations_raw(ledger_range);
CREATE INDEX IF NOT EXISTS idx_contract_invocations_closed_at ON contract_invocations_raw(closed_at DESC);
CREATE INDEX IF NOT EXISTS idx_contract_invocations_contract_function ON contract_invocations_raw(contract_id, function_name);

-- Verify table exists
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.tables
        WHERE table_name = 'contract_invocations_raw'
    ) THEN
        RAISE EXCEPTION 'Failed to create contract_invocations_raw table';
    END IF;

    RAISE NOTICE 'Successfully created contract_invocations_raw table with indexes';
END $$;

COMMIT;
