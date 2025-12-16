-- Rollback migration 005: Drop metadata tables

DROP TABLE IF EXISTS _meta_eras;
DROP TABLE IF EXISTS _meta_changes;
DROP TABLE IF EXISTS _meta_quality;
DROP TABLE IF EXISTS _meta_lineage;
DROP TABLE IF EXISTS _meta_datasets;
