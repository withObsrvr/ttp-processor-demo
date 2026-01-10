-- DuckLake Index Plane Schema
-- DuckDB schema for Index Plane tables stored in Parquet format on B2
-- Compatible with Parquet storage - no PRIMARY KEY/UNIQUE/NOT NULL/DEFAULT constraints

-- Note: This SQL should be executed via DuckDB after attaching the DuckLake catalog
-- The tables will be stored in Parquet format on S3/B2 storage
-- Catalog: testnet_catalog (or mainnet_catalog)
-- Schema: index
-- Storage: s3://obsrvr-prod-testnet-index/

-- ============================================================================
-- TX_HASH_INDEX TABLE
-- ============================================================================

-- Table: tx_hash_index
-- Fast lookup index from transaction hash to ledger sequence
-- Enables <500ms transaction queries at mainnet scale (60M+ ledgers)

CREATE TABLE IF NOT EXISTS testnet_catalog.index.tx_hash_index (
    -- Primary lookup fields
    tx_hash VARCHAR,                    -- Transaction hash (64 chars)
    ledger_sequence BIGINT,             -- Ledger where transaction appears
    application_order INTEGER,          -- Order within ledger

    -- Transaction metadata
    operation_count INTEGER,            -- Number of operations in transaction
    successful BOOLEAN,                 -- Whether transaction was successful
    closed_at TIMESTAMP,                -- When ledger closed

    -- Partition pruning (for efficient queries)
    ledger_range BIGINT,                -- Partition key (ledger_sequence / 100000)

    -- Metadata
    created_at TIMESTAMP                -- When index entry was created
);

COMMENT ON TABLE testnet_catalog.index.tx_hash_index IS 'Transaction hash to ledger sequence index for fast explorer queries';

-- ============================================================================
-- NOTES
-- ============================================================================

-- Partition Strategy:
--   - Files partitioned by ledger_range (100,000 ledgers per partition)
--   - Example: ledger_range=1 contains ledgers 1-99,999
--   - Example: ledger_range=2 contains ledgers 100,000-199,999
--   - Partition pruning eliminates 99%+ of files for point lookups

-- Query Pattern:
--   SELECT ledger_sequence, application_order, operation_count, successful, closed_at
--   FROM testnet_catalog.index.tx_hash_index
--   WHERE tx_hash = '<hash>'
--
--   Expected performance: <500ms p95 for 60M+ ledgers

-- Storage Layout:
--   s3://obsrvr-prod-testnet-index/tx_hash_index/
--     ledger_range=1/
--       part_001.parquet
--       part_002.parquet
--     ledger_range=2/
--       part_001.parquet
--       part_002.parquet
--     ...

-- File Size Guidance:
--   - Target: 100-200 MB per Parquet file
--   - ~10,000-20,000 transactions per file
--   - Adjust batch size in transformer if files too large/small

-- ============================================================================
-- SAMPLE QUERIES
-- ============================================================================

-- Count transactions in index
-- SELECT COUNT(*) FROM testnet_catalog.index.tx_hash_index;

-- Check ledger range coverage
-- SELECT
--     MIN(ledger_sequence) as min_ledger,
--     MAX(ledger_sequence) as max_ledger,
--     COUNT(*) as tx_count
-- FROM testnet_catalog.index.tx_hash_index;

-- Count transactions per ledger_range
-- SELECT
--     ledger_range,
--     COUNT(*) as tx_count,
--     MIN(ledger_sequence) as min_ledger,
--     MAX(ledger_sequence) as max_ledger
-- FROM testnet_catalog.index.tx_hash_index
-- GROUP BY ledger_range
-- ORDER BY ledger_range;

-- Lookup specific transaction (fast!)
-- SELECT
--     ledger_sequence,
--     application_order,
--     operation_count,
--     successful,
--     closed_at
-- FROM testnet_catalog.index.tx_hash_index
-- WHERE tx_hash = 'abc123...';
