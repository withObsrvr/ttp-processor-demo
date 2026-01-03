-- Rollback migration 007: Drop flush helper functions

DROP FUNCTION IF EXISTS cleanup_flushed_data(BIGINT);
DROP FUNCTION IF EXISTS get_flush_candidates(INTEGER);
DROP FUNCTION IF EXISTS get_high_watermark();
