-- Migration: Add contract call tables for cross-contract call tracking
-- Purpose: Silver layer tables for Freighter "Contracts Involved" feature
-- Date: 2026-01-04

BEGIN;

-- ============================================================================
-- Table: contract_invocation_calls
-- Flattened cross-contract call relationships from Bronze call graphs
-- ============================================================================

CREATE TABLE IF NOT EXISTS contract_invocation_calls (
    -- Identity (auto-generated)
    call_id BIGSERIAL PRIMARY KEY,

    -- TOID components (for joining back to operations)
    ledger_sequence BIGINT NOT NULL,
    transaction_index INT NOT NULL,
    operation_index INT NOT NULL,
    transaction_hash VARCHAR(64) NOT NULL,

    -- Call relationship
    from_contract VARCHAR(100) NOT NULL,
    to_contract VARCHAR(100) NOT NULL,
    function_name VARCHAR(100),
    call_depth INT NOT NULL,
    execution_order INT NOT NULL,

    -- Status
    successful BOOLEAN NOT NULL,
    closed_at TIMESTAMP NOT NULL,

    -- Partitioning
    ledger_range BIGINT NOT NULL,

    -- Timestamps
    created_at TIMESTAMP DEFAULT NOW()
);

-- Indexes for common query patterns

-- Find all calls in a transaction
CREATE INDEX IF NOT EXISTS idx_contract_calls_tx_hash
ON contract_invocation_calls(transaction_hash);

-- Find all calls where a contract is the caller
CREATE INDEX IF NOT EXISTS idx_contract_calls_from_contract
ON contract_invocation_calls(from_contract);

-- Find all calls where a contract is the callee
CREATE INDEX IF NOT EXISTS idx_contract_calls_to_contract
ON contract_invocation_calls(to_contract);

-- Find calls by function name (for pattern analysis)
CREATE INDEX IF NOT EXISTS idx_contract_calls_function
ON contract_invocation_calls(function_name)
WHERE function_name IS NOT NULL;

-- Time-based queries
CREATE INDEX IF NOT EXISTS idx_contract_calls_closed_at
ON contract_invocation_calls(closed_at DESC);

-- Partition key for range queries
CREATE INDEX IF NOT EXISTS idx_contract_calls_ledger_range
ON contract_invocation_calls(ledger_range);

-- Unique constraint on call within operation
CREATE UNIQUE INDEX IF NOT EXISTS idx_contract_calls_unique
ON contract_invocation_calls(ledger_sequence, transaction_hash, operation_index, execution_order);

-- ============================================================================
-- Table: contract_invocation_hierarchy
-- Pre-computed ancestry chains for efficient contract relationship queries
-- ============================================================================

CREATE TABLE IF NOT EXISTS contract_invocation_hierarchy (
    -- Identity
    transaction_hash VARCHAR(64) NOT NULL,
    root_contract VARCHAR(100) NOT NULL,
    child_contract VARCHAR(100) NOT NULL,

    -- Hierarchy
    path_depth INT NOT NULL,
    full_path TEXT[] NOT NULL,

    -- Partitioning
    ledger_range BIGINT NOT NULL,

    -- Timestamps
    created_at TIMESTAMP DEFAULT NOW(),

    -- Primary key
    PRIMARY KEY (transaction_hash, root_contract, child_contract)
);

-- Indexes for hierarchy queries

-- Find all children of a root contract
CREATE INDEX IF NOT EXISTS idx_hierarchy_root
ON contract_invocation_hierarchy(root_contract);

-- Find all roots that include a child contract
CREATE INDEX IF NOT EXISTS idx_hierarchy_child
ON contract_invocation_hierarchy(child_contract);

-- GIN index for path containment queries
CREATE INDEX IF NOT EXISTS idx_hierarchy_full_path
ON contract_invocation_hierarchy USING GIN (full_path);

-- Partition key
CREATE INDEX IF NOT EXISTS idx_hierarchy_ledger_range
ON contract_invocation_hierarchy(ledger_range);

-- ============================================================================
-- View: v_contract_call_summary
-- Aggregated view of contract call relationships
-- ============================================================================

CREATE OR REPLACE VIEW v_contract_call_summary AS
SELECT
    from_contract,
    to_contract,
    function_name,
    COUNT(*) as call_count,
    COUNT(DISTINCT transaction_hash) as unique_transactions,
    MIN(closed_at) as first_seen,
    MAX(closed_at) as last_seen,
    AVG(call_depth) as avg_depth,
    COUNT(*) FILTER (WHERE successful = true) as successful_count,
    COUNT(*) FILTER (WHERE successful = false) as failed_count
FROM contract_invocation_calls
GROUP BY from_contract, to_contract, function_name;

-- ============================================================================
-- View: v_contract_callers
-- Contracts that call a specific contract
-- ============================================================================

CREATE OR REPLACE VIEW v_contract_callers AS
SELECT
    to_contract as contract_id,
    from_contract as caller_contract,
    array_agg(DISTINCT function_name) as functions_called,
    COUNT(*) as call_count,
    MAX(closed_at) as last_call
FROM contract_invocation_calls
GROUP BY to_contract, from_contract;

-- ============================================================================
-- View: v_contract_callees
-- Contracts that are called by a specific contract
-- ============================================================================

CREATE OR REPLACE VIEW v_contract_callees AS
SELECT
    from_contract as contract_id,
    to_contract as callee_contract,
    array_agg(DISTINCT function_name) as functions_invoked,
    COUNT(*) as call_count,
    MAX(closed_at) as last_call
FROM contract_invocation_calls
GROUP BY from_contract, to_contract;

-- ============================================================================
-- Verification
-- ============================================================================

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.tables
        WHERE table_name = 'contract_invocation_calls'
    ) THEN
        RAISE EXCEPTION 'Failed to create contract_invocation_calls table';
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM information_schema.tables
        WHERE table_name = 'contract_invocation_hierarchy'
    ) THEN
        RAISE EXCEPTION 'Failed to create contract_invocation_hierarchy table';
    END IF;

    RAISE NOTICE 'Successfully created Silver layer contract call tables';
END $$;

COMMIT;
