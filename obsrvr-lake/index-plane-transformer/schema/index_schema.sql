-- Index Plane Schema
-- PostgreSQL Catalog Database Schema
-- Tracks Index Plane metadata (tables, files, checkpoints)
-- Database: obsrvr_lake_catalog_prod (or catalog database)

-- ============================================================================
-- CREATE INDEX SCHEMA
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS index;

-- ============================================================================
-- METADATA TABLES (Catalog tracking)
-- ============================================================================

-- Table: index.tables
-- Tracks Index Plane tables and their metadata
CREATE TABLE IF NOT EXISTS index.tables (
    table_name TEXT PRIMARY KEY,
    total_files BIGINT DEFAULT 0,
    total_rows BIGINT DEFAULT 0,
    total_size_bytes BIGINT DEFAULT 0,
    min_ledger_sequence BIGINT,
    max_ledger_sequence BIGINT,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    description TEXT
);

COMMENT ON TABLE index.tables IS 'Index Plane table metadata tracking';

-- Table: index.files
-- Tracks individual Parquet files in Index Plane
CREATE TABLE IF NOT EXISTS index.files (
    file_id SERIAL PRIMARY KEY,
    table_name TEXT NOT NULL,
    file_path TEXT NOT NULL,
    ledger_range BIGINT NOT NULL,
    min_ledger_sequence BIGINT NOT NULL,
    max_ledger_sequence BIGINT NOT NULL,
    row_count BIGINT NOT NULL,
    file_size_bytes BIGINT NOT NULL,
    created_at TIMESTAMP DEFAULT NOW(),
    checksum TEXT,

    FOREIGN KEY (table_name) REFERENCES index.tables(table_name) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_index_files_table ON index.files(table_name);
CREATE INDEX IF NOT EXISTS idx_index_files_ledger_range ON index.files(ledger_range);
CREATE INDEX IF NOT EXISTS idx_index_files_path ON index.files(file_path);

COMMENT ON TABLE index.files IS 'Index Plane Parquet file tracking';

-- ============================================================================
-- CHECKPOINT TABLE
-- ============================================================================

-- Table: index.transformer_checkpoint
-- Tracks last processed ledger for index-plane-transformer
CREATE TABLE IF NOT EXISTS index.transformer_checkpoint (
    id INTEGER PRIMARY KEY DEFAULT 1,
    last_ledger_sequence BIGINT NOT NULL,
    last_processed_at TIMESTAMP NOT NULL,
    transformer_version VARCHAR(50),

    -- Ensure only one checkpoint row
    CONSTRAINT single_checkpoint CHECK (id = 1)
);

COMMENT ON TABLE index.transformer_checkpoint IS 'Index Plane transformer checkpoint tracking';

-- Insert initial checkpoint row
INSERT INTO index.transformer_checkpoint (id, last_ledger_sequence, last_processed_at, transformer_version)
VALUES (1, 0, NOW(), 'v1.0.0')
ON CONFLICT (id) DO NOTHING;

-- ============================================================================
-- INITIAL METADATA
-- ============================================================================

-- Register tx_hash_index table
INSERT INTO index.tables (table_name, description)
VALUES ('tx_hash_index', 'Transaction hash to ledger sequence index for fast lookups')
ON CONFLICT (table_name) DO NOTHING;

-- ============================================================================
-- GRANTS
-- ============================================================================

-- Grant permissions to application user
-- Note: Adjust user name as needed for your environment
GRANT USAGE ON SCHEMA index TO stellar;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA index TO stellar;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA index TO stellar;

-- ============================================================================
-- VERIFICATION QUERIES
-- ============================================================================

-- Verify schema created
-- SELECT schema_name FROM information_schema.schemata WHERE schema_name = 'index';

-- Check tables
-- SELECT table_name FROM information_schema.tables WHERE table_schema = 'index';

-- Check checkpoint
-- SELECT * FROM index.transformer_checkpoint;

-- Check registered tables
-- SELECT * FROM index.tables;
