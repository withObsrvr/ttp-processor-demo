-- Migration 005: Create Obsrvr metadata tables
-- Tables: _meta_datasets, _meta_lineage, _meta_quality, _meta_changes, _meta_eras
-- These tables track dataset registry, lineage, quality, schema evolution, and era management

-- ============================================================================
-- _META_DATASETS (Dataset Registry) (14 fields)
-- ============================================================================
CREATE UNLOGGED TABLE _meta_datasets (
    -- Core metadata (10 fields)
    dataset TEXT NOT NULL,
    tier TEXT NOT NULL,
    domain TEXT NOT NULL,
    major_version INTEGER NOT NULL,
    current_minor_version INTEGER NOT NULL,
    owner TEXT,
    purpose TEXT,
    grain TEXT,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,

    -- Era-aware meta tables (4 fields)
    era_id TEXT,
    version_label TEXT,
    schema_hash TEXT,
    compatibility TEXT,

    PRIMARY KEY (dataset, major_version)
);

COMMENT ON TABLE _meta_datasets IS 'Dataset registry - tracks all datasets, versions, ownership, purpose';
COMMENT ON COLUMN _meta_datasets.grain IS 'Data granularity: row, snapshot, aggregate, etc.';
COMMENT ON COLUMN _meta_datasets.tier IS 'Data tier: bronze (raw), silver (cleaned), gold (analytics)';

-- ============================================================================
-- _META_LINEAGE (Processing Provenance) (15 fields)
-- ============================================================================
CREATE UNLOGGED TABLE _meta_lineage (
    -- Core lineage (10 fields)
    id INTEGER NOT NULL PRIMARY KEY,
    dataset TEXT NOT NULL,
    partition TEXT,
    source_ledger_start INTEGER NOT NULL,
    source_ledger_end INTEGER NOT NULL,
    pipeline_version TEXT NOT NULL,
    processor_name TEXT NOT NULL,
    checksum TEXT,
    row_count INTEGER,
    created_at TIMESTAMPTZ NOT NULL,

    -- Era-aware meta tables (2 fields)
    era_id TEXT,
    version_label TEXT,

    -- PAS (Provenance Audit System) linkage (3 fields)
    pas_event_id TEXT,
    pas_event_hash TEXT,
    pas_verified BOOLEAN
);

COMMENT ON TABLE _meta_lineage IS 'Processing provenance - who processed what, when, with which version';
COMMENT ON COLUMN _meta_lineage.pas_event_id IS 'Link to Provenance Audit System event';
COMMENT ON COLUMN _meta_lineage.checksum IS 'Data checksum for integrity verification';

-- ============================================================================
-- _META_QUALITY (Data Quality Tracking) (10 fields)
-- ============================================================================
CREATE UNLOGGED TABLE _meta_quality (
    -- Core quality (10 fields)
    id BIGINT NOT NULL PRIMARY KEY,
    dataset TEXT NOT NULL,
    partition TEXT,
    check_name TEXT NOT NULL,
    check_type TEXT NOT NULL,
    passed BOOLEAN NOT NULL,
    details TEXT,
    row_count INTEGER,
    null_anomalies INTEGER,
    created_at TIMESTAMPTZ NOT NULL
);

COMMENT ON TABLE _meta_quality IS 'Data quality tracking - automated quality checks and results';
COMMENT ON COLUMN _meta_quality.check_type IS 'Type: sequence_monotonicity, hash_format, balance_range, etc.';
COMMENT ON COLUMN _meta_quality.passed IS 'TRUE if check passed, FALSE if failed';

-- ============================================================================
-- _META_CHANGES (Schema Evolution Log) (8 fields)
-- ============================================================================
CREATE UNLOGGED TABLE _meta_changes (
    -- Schema evolution (8 fields)
    id INTEGER NOT NULL PRIMARY KEY,
    dataset TEXT NOT NULL,
    from_version TEXT,
    to_version TEXT NOT NULL,
    change_type TEXT NOT NULL,
    summary TEXT,
    migration_sql TEXT,
    applied_at TIMESTAMPTZ NOT NULL
);

COMMENT ON TABLE _meta_changes IS 'Schema evolution log - tracks all schema changes over time';
COMMENT ON COLUMN _meta_changes.change_type IS 'Type: add_column, drop_column, rename_table, etc.';
COMMENT ON COLUMN _meta_changes.migration_sql IS 'SQL commands to apply this migration';

-- ============================================================================
-- _META_ERAS (Era Registry) (12 fields)
-- ============================================================================
CREATE UNLOGGED TABLE _meta_eras (
    -- Era definition (12 fields)
    era_id TEXT NOT NULL PRIMARY KEY,
    network TEXT NOT NULL,
    version_label TEXT NOT NULL,
    ledger_start BIGINT NOT NULL,
    ledger_end BIGINT,
    protocol_min INTEGER,
    protocol_max INTEGER,
    status TEXT NOT NULL,
    schema_epoch TEXT,
    pas_chain_head TEXT,
    created_at TIMESTAMPTZ,
    frozen_at TIMESTAMPTZ
);

COMMENT ON TABLE _meta_eras IS 'Era registry - tracks protocol eras for multi-era catalog support';
COMMENT ON COLUMN _meta_eras.status IS 'Status: active, frozen, archived';
COMMENT ON COLUMN _meta_eras.pas_chain_head IS 'Latest PAS event in this era (blockchain-like provenance)';
COMMENT ON COLUMN _meta_eras.schema_epoch IS 'Schema version epoch for this era';
