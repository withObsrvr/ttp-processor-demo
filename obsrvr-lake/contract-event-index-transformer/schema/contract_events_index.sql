-- Contract Event Index Schema
--
-- Purpose: Indexes contract_id → ledger_sequence mappings for fast lookup
-- Pattern: Follows tx_hash_index architecture (hybrid hot/cold, DuckLake)
-- Partitioning: ledger_range = ledger_sequence / 100000
--
-- Usage:
--   psql $CATALOG_DB < contract_events_index.sql
--
-- This script is idempotent - can be run multiple times safely.

-- Create index schema (if not exists)
CREATE SCHEMA IF NOT EXISTS index;

-- Main index table
-- Maps contract_id → ledgers containing events from that contract
CREATE TABLE IF NOT EXISTS index.contract_events_index (
    -- Contract address (e.g., CABC123...)
    contract_id VARCHAR NOT NULL,

    -- Ledger sequence number containing events from this contract
    ledger_sequence BIGINT NOT NULL,

    -- Number of events from this contract in this ledger
    -- (useful for quick stats without querying Bronze layer)
    event_count INTEGER NOT NULL,

    -- When this pair was first indexed (for debugging)
    first_seen_at TIMESTAMP NOT NULL,

    -- Partition key: ledger_sequence / 100000
    -- Groups ~100K ledgers per partition (~3-4 months of data)
    -- Enables efficient range queries via partition pruning
    ledger_range BIGINT NOT NULL,

    -- When this row was created
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    -- Primary key: each contract appears once per ledger
    -- Prevents duplicates even if transformer runs twice
    PRIMARY KEY (contract_id, ledger_sequence)
);

-- Metadata table: tracks DuckLake tables in catalog
-- Required by DuckLake extension for catalog management
CREATE TABLE IF NOT EXISTS index.tables (
    table_name VARCHAR PRIMARY KEY,
    description TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_updated TIMESTAMP
);

-- Maintenance log: audit trail for operations
-- Tracks CHECKPOINT, merge, expire, vacuum operations
CREATE TABLE IF NOT EXISTS index.maintenance_log (
    operation VARCHAR,           -- 'checkpoint', 'merge', 'expire', 'vacuum'
    table_name VARCHAR,          -- Target table
    details JSONB,               -- Operation-specific metadata
    executed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Register contract_events_index table in catalog
-- ON CONFLICT ensures idempotency (can run script multiple times)
INSERT INTO index.tables (table_name, description, last_updated)
VALUES (
    'contract_events_index',
    'Maps contract_id to ledgers containing events from that contract. Enables fast lookup: which ledgers have events from contract X?',
    CURRENT_TIMESTAMP
)
ON CONFLICT (table_name)
DO UPDATE SET
    last_updated = CURRENT_TIMESTAMP,
    description = EXCLUDED.description;

-- Schema notes:
--
-- 1. Minimal fields: Just contract_id, ledger_sequence, and count
--    - NO topics_decoded (too much data, query Bronze if needed)
--    - NO data_decoded (too much data, query Bronze if needed)
--    - NO event_type (index ALL events, filter at query time)
--
-- 2. Partitioning strategy:
--    - ledger_range = ledger_sequence / 100000
--    - Same as tx_hash_index for consistency
--    - Example: ledger 276,592 → range 2, ledger 300,000 → range 3
--    - Storage layout: s3://bucket/index/contract_events_index/ledger_range=2/part-0001.parquet
--
-- 3. Query optimization:
--    - Primary key enables fast lookups on (contract_id, ledger_sequence)
--    - WHERE ledger_sequence >= ? AND <= ? can use partition pruning
--    - DuckDB may auto-create additional indexes
--
-- 4. Data source:
--    - Transformer queries: bronze.contract_events_stream_v1
--    - Query: SELECT contract_id, ledger_sequence, COUNT(*) ... GROUP BY ...
--    - Multiple events from same contract in same ledger → single index row
--
-- 5. Use cases:
--    - "Which ledgers have events from contract X?" → O(1) lookup
--    - Cursor-based continuous auditing (convergence/Stellarcarbon)
--    - Public access without API keys (Cycle 4)
