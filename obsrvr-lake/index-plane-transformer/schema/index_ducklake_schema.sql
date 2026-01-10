-- DuckLake Index Plane Schema
-- Transaction hash index for fast lookups
-- Compatible with Parquet storage - no PRIMARY KEY/UNIQUE/NOT NULL/DEFAULT constraints

CREATE TABLE IF NOT EXISTS testnet_catalog.index.tx_hash_index (
    tx_hash VARCHAR,
    ledger_sequence BIGINT,
    operation_count INTEGER,
    successful BOOLEAN,
    closed_at TIMESTAMP,
    ledger_range BIGINT,
    created_at TIMESTAMP
);

COMMENT ON TABLE testnet_catalog.index.tx_hash_index IS 'Transaction hash index for O(1) lookups (Index Plane)';
