-- PostgreSQL schema for Stellar contract data
-- This schema is optimized for both real-time inserts and analytical queries

-- Create schema if it doesn't exist
CREATE SCHEMA IF NOT EXISTS contract_data;

-- Main contract data table
CREATE TABLE IF NOT EXISTS contract_data.entries (
    -- Primary identification
    id BIGSERIAL PRIMARY KEY,
    contract_id VARCHAR(56) NOT NULL,
    ledger_sequence INTEGER NOT NULL,
    
    -- Contract metadata
    contract_key_type VARCHAR(50) NOT NULL,
    contract_durability VARCHAR(20) NOT NULL,
    
    -- Asset information (nullable for non-asset contracts)
    asset_code VARCHAR(12),
    asset_issuer VARCHAR(56),
    asset_type VARCHAR(50),
    
    -- Balance information (nullable)
    balance_holder VARCHAR(56),
    balance BIGINT, -- Store as integer to avoid precision issues
    
    -- Ledger metadata
    last_modified_ledger INTEGER NOT NULL,
    deleted BOOLEAN NOT NULL DEFAULT FALSE,
    closed_at TIMESTAMP WITH TIME ZONE NOT NULL,
    
    -- Additional data
    ledger_key_hash VARCHAR(64),
    key_xdr TEXT,
    val_xdr TEXT,
    contract_instance_type VARCHAR(50),
    contract_instance_wasm_hash VARCHAR(64),
    expiration_ledger_seq INTEGER,
    
    -- Ingestion metadata
    ingested_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    batch_id UUID,
    
    -- Constraints
    CONSTRAINT unique_contract_ledger UNIQUE (contract_id, ledger_sequence)
);

-- Indexes for query performance
CREATE INDEX idx_contract_id ON contract_data.entries (contract_id);
CREATE INDEX idx_ledger_sequence ON contract_data.entries (ledger_sequence);
CREATE INDEX idx_asset_code ON contract_data.entries (asset_code) WHERE asset_code IS NOT NULL;
CREATE INDEX idx_asset_issuer ON contract_data.entries (asset_issuer) WHERE asset_issuer IS NOT NULL;
CREATE INDEX idx_balance_holder ON contract_data.entries (balance_holder) WHERE balance_holder IS NOT NULL;
CREATE INDEX idx_closed_at ON contract_data.entries (closed_at);
CREATE INDEX idx_deleted ON contract_data.entries (deleted) WHERE deleted = true;
CREATE INDEX idx_last_modified ON contract_data.entries (last_modified_ledger);

-- Composite indexes for common query patterns
CREATE INDEX idx_asset_balance ON contract_data.entries (asset_code, balance) 
    WHERE asset_code IS NOT NULL AND balance IS NOT NULL;
CREATE INDEX idx_holder_asset ON contract_data.entries (balance_holder, asset_code) 
    WHERE balance_holder IS NOT NULL AND asset_code IS NOT NULL;

-- Materialized view for current contract states (latest non-deleted entry per contract)
CREATE MATERIALIZED VIEW contract_data.current_states AS
SELECT DISTINCT ON (contract_id) 
    contract_id,
    contract_key_type,
    contract_durability,
    asset_code,
    asset_issuer,
    asset_type,
    balance_holder,
    balance,
    last_modified_ledger,
    deleted,
    closed_at,
    ledger_key_hash,
    contract_instance_type,
    contract_instance_wasm_hash,
    expiration_ledger_seq
FROM contract_data.entries
WHERE NOT deleted
ORDER BY contract_id, ledger_sequence DESC;

-- Index on materialized view
CREATE UNIQUE INDEX idx_current_states_contract ON contract_data.current_states (contract_id);
CREATE INDEX idx_current_states_asset ON contract_data.current_states (asset_code) WHERE asset_code IS NOT NULL;
CREATE INDEX idx_current_states_holder ON contract_data.current_states (balance_holder) WHERE balance_holder IS NOT NULL;

-- Function to refresh current states
CREATE OR REPLACE FUNCTION contract_data.refresh_current_states()
RETURNS void AS $$
BEGIN
    REFRESH MATERIALIZED VIEW CONCURRENTLY contract_data.current_states;
END;
$$ LANGUAGE plpgsql;

-- Ingestion tracking table
CREATE TABLE IF NOT EXISTS contract_data.ingestion_progress (
    id SERIAL PRIMARY KEY,
    batch_id UUID NOT NULL,
    start_ledger INTEGER NOT NULL,
    end_ledger INTEGER NOT NULL,
    entries_processed BIGINT NOT NULL,
    started_at TIMESTAMP WITH TIME ZONE NOT NULL,
    completed_at TIMESTAMP WITH TIME ZONE,
    status VARCHAR(20) NOT NULL DEFAULT 'processing',
    error_message TEXT,
    CONSTRAINT valid_status CHECK (status IN ('processing', 'completed', 'failed'))
);

-- Analytics helper views

-- Asset summary view
CREATE VIEW contract_data.asset_summary AS
SELECT 
    asset_code,
    asset_issuer,
    COUNT(DISTINCT balance_holder) as holder_count,
    SUM(balance) as total_supply,
    MAX(last_modified_ledger) as last_activity
FROM contract_data.current_states
WHERE asset_code IS NOT NULL 
    AND balance IS NOT NULL
    AND NOT deleted
GROUP BY asset_code, asset_issuer;

-- Contract activity view (last 24 hours)
CREATE VIEW contract_data.recent_activity AS
SELECT 
    contract_id,
    COUNT(*) as change_count,
    MIN(closed_at) as first_change,
    MAX(closed_at) as last_change,
    ARRAY_AGG(DISTINCT contract_key_type) as key_types
FROM contract_data.entries
WHERE closed_at > NOW() - INTERVAL '24 hours'
GROUP BY contract_id
ORDER BY change_count DESC;

-- Helper functions

-- Get contract balance history
CREATE OR REPLACE FUNCTION contract_data.get_balance_history(
    p_contract_id VARCHAR(56),
    p_limit INTEGER DEFAULT 100
)
RETURNS TABLE (
    ledger_sequence INTEGER,
    balance BIGINT,
    closed_at TIMESTAMP WITH TIME ZONE,
    deleted BOOLEAN
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        e.ledger_sequence,
        e.balance,
        e.closed_at,
        e.deleted
    FROM contract_data.entries e
    WHERE e.contract_id = p_contract_id
        AND e.balance IS NOT NULL
    ORDER BY e.ledger_sequence DESC
    LIMIT p_limit;
END;
$$ LANGUAGE plpgsql;

-- Get top holders for an asset
CREATE OR REPLACE FUNCTION contract_data.get_top_holders(
    p_asset_code VARCHAR(12),
    p_asset_issuer VARCHAR(56),
    p_limit INTEGER DEFAULT 100
)
RETURNS TABLE (
    balance_holder VARCHAR(56),
    balance BIGINT,
    last_modified_ledger INTEGER
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        cs.balance_holder,
        cs.balance,
        cs.last_modified_ledger
    FROM contract_data.current_states cs
    WHERE cs.asset_code = p_asset_code
        AND cs.asset_issuer = p_asset_issuer
        AND cs.balance IS NOT NULL
        AND cs.balance > 0
        AND NOT cs.deleted
    ORDER BY cs.balance DESC
    LIMIT p_limit;
END;
$$ LANGUAGE plpgsql;

-- Partitioning setup for high-volume deployments (optional)
-- Uncomment and adjust based on your data volume

-- -- Convert to partitioned table by ledger sequence
-- CREATE TABLE contract_data.entries_partitioned (
--     LIKE contract_data.entries INCLUDING ALL
-- ) PARTITION BY RANGE (ledger_sequence);

-- -- Create monthly partitions
-- CREATE TABLE contract_data.entries_y2024m01 PARTITION OF contract_data.entries_partitioned
--     FOR VALUES FROM (1000000) TO (2000000);

-- Performance hints
COMMENT ON TABLE contract_data.entries IS 'Main table for Stellar contract data. Optimized for time-series inserts and analytical queries.';
COMMENT ON COLUMN contract_data.entries.balance IS 'Balance stored as BIGINT. Divide by 10^7 for actual Stellar amount.';
COMMENT ON INDEX idx_asset_balance IS 'Composite index for asset balance queries - useful for rich list generation.';