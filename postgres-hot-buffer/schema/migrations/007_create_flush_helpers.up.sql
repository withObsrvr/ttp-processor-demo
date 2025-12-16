-- Migration 007: Create flush helper functions
-- Functions: get_high_watermark(), get_flush_candidates(), cleanup_flushed_data()
-- Purpose: Orchestrate the flush process (Postgres → DuckLake Bronze)

-- ============================================================================
-- FUNCTION 1: get_high_watermark()
-- Returns the current high watermark (max ledger_sequence) across all tables
-- Used to determine the safe flush boundary
-- ============================================================================
CREATE OR REPLACE FUNCTION get_high_watermark()
RETURNS TABLE (
    table_name TEXT,
    max_ledger_sequence BIGINT,
    max_ledger_range BIGINT,
    row_count BIGINT
) AS $$
BEGIN
    RETURN QUERY
    -- Core tables
    SELECT 'ledgers_row_v2'::TEXT,
           MAX(sequence),
           MAX(ledger_range),
           COUNT(*)::BIGINT
    FROM ledgers_row_v2
    UNION ALL
    SELECT 'transactions_row_v2'::TEXT,
           MAX(ledger_sequence),
           MAX(ledger_range),
           COUNT(*)::BIGINT
    FROM transactions_row_v2
    UNION ALL
    SELECT 'operations_row_v2'::TEXT,
           MAX(ledger_sequence),
           MAX(ledger_range),
           COUNT(*)::BIGINT
    FROM operations_row_v2

    -- Effects & Trades
    UNION ALL
    SELECT 'effects_row_v1'::TEXT,
           MAX(ledger_sequence),
           MAX(ledger_range),
           COUNT(*)::BIGINT
    FROM effects_row_v1
    UNION ALL
    SELECT 'trades_row_v1'::TEXT,
           MAX(ledger_sequence),
           MAX(ledger_range),
           COUNT(*)::BIGINT
    FROM trades_row_v1

    -- Snapshots
    UNION ALL
    SELECT 'accounts_snapshot_v1'::TEXT,
           MAX(ledger_sequence),
           MAX(ledger_range),
           COUNT(*)::BIGINT
    FROM accounts_snapshot_v1
    UNION ALL
    SELECT 'trustlines_snapshot_v1'::TEXT,
           MAX(ledger_sequence),
           MAX(ledger_range),
           COUNT(*)::BIGINT
    FROM trustlines_snapshot_v1
    UNION ALL
    SELECT 'native_balances_snapshot_v1'::TEXT,
           MAX(ledger_sequence),
           MAX(ledger_range),
           COUNT(*)::BIGINT
    FROM native_balances_snapshot_v1
    UNION ALL
    SELECT 'account_signers_snapshot_v1'::TEXT,
           MAX(ledger_sequence),
           MAX(ledger_range),
           COUNT(*)::BIGINT
    FROM account_signers_snapshot_v1
    UNION ALL
    SELECT 'offers_snapshot_v1'::TEXT,
           MAX(ledger_sequence),
           MAX(ledger_range),
           COUNT(*)::BIGINT
    FROM offers_snapshot_v1
    UNION ALL
    SELECT 'claimable_balances_snapshot_v1'::TEXT,
           MAX(ledger_sequence),
           MAX(ledger_range),
           COUNT(*)::BIGINT
    FROM claimable_balances_snapshot_v1
    UNION ALL
    SELECT 'liquidity_pools_snapshot_v1'::TEXT,
           MAX(ledger_sequence),
           MAX(ledger_range),
           COUNT(*)::BIGINT
    FROM liquidity_pools_snapshot_v1

    -- Soroban
    UNION ALL
    SELECT 'contract_events_stream_v1'::TEXT,
           MAX(ledger_sequence),
           MAX(ledger_range),
           COUNT(*)::BIGINT
    FROM contract_events_stream_v1
    UNION ALL
    SELECT 'contract_data_snapshot_v1'::TEXT,
           MAX(ledger_sequence),
           MAX(ledger_range),
           COUNT(*)::BIGINT
    FROM contract_data_snapshot_v1
    UNION ALL
    SELECT 'contract_code_snapshot_v1'::TEXT,
           MAX(ledger_sequence),
           MAX(ledger_range),
           COUNT(*)::BIGINT
    FROM contract_code_snapshot_v1
    UNION ALL
    SELECT 'config_settings_snapshot_v1'::TEXT,
           MAX(ledger_sequence),
           MAX(ledger_range),
           COUNT(*)::BIGINT
    FROM config_settings_snapshot_v1
    UNION ALL
    SELECT 'ttl_snapshot_v1'::TEXT,
           MAX(ledger_sequence),
           MAX(ledger_range),
           COUNT(*)::BIGINT
    FROM ttl_snapshot_v1
    UNION ALL
    SELECT 'evicted_keys_state_v1'::TEXT,
           MAX(ledger_sequence),
           MAX(ledger_range),
           COUNT(*)::BIGINT
    FROM evicted_keys_state_v1
    UNION ALL
    SELECT 'restored_keys_state_v1'::TEXT,
           MAX(ledger_sequence),
           MAX(ledger_range),
           COUNT(*)::BIGINT
    FROM restored_keys_state_v1;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION get_high_watermark() IS 'Returns max ledger_sequence across all 19 tables for flush watermark determination';

-- ============================================================================
-- FUNCTION 2: get_flush_candidates()
-- Returns ledger_ranges that are ready to be flushed (older than N minutes)
-- Default: 5 minutes (can be adjusted)
-- ============================================================================
CREATE OR REPLACE FUNCTION get_flush_candidates(
    min_age_minutes INTEGER DEFAULT 5
)
RETURNS TABLE (
    ledger_range BIGINT,
    min_timestamp TIMESTAMPTZ,
    max_timestamp TIMESTAMPTZ,
    ledger_count BIGINT
) AS $$
BEGIN
    RETURN QUERY
    SELECT
        l.ledger_range,
        MIN(l.closed_at),
        MAX(l.closed_at),
        COUNT(*)::BIGINT
    FROM ledgers_row_v2 l
    GROUP BY l.ledger_range
    HAVING MAX(l.closed_at) < NOW() - (min_age_minutes || ' minutes')::INTERVAL
    ORDER BY l.ledger_range;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION get_flush_candidates(INTEGER) IS 'Returns ledger_ranges older than N minutes, ready for flush to cold storage';

-- ============================================================================
-- FUNCTION 3: cleanup_flushed_data()
-- Deletes data that has been successfully flushed to cold storage
-- Takes ledger_range as parameter (from high-watermark pattern)
-- Returns count of deleted rows per table
-- ============================================================================
CREATE OR REPLACE FUNCTION cleanup_flushed_data(
    p_ledger_range BIGINT
)
RETURNS TABLE (table_name TEXT, deleted_rows BIGINT) AS $$
DECLARE
    v_deleted_count BIGINT;
BEGIN
    -- Core tables
    DELETE FROM ledgers_row_v2 WHERE ledger_range <= p_ledger_range;
    GET DIAGNOSTICS v_deleted_count = ROW_COUNT;
    RETURN QUERY SELECT 'ledgers_row_v2'::TEXT, v_deleted_count;

    DELETE FROM transactions_row_v2 WHERE ledger_range <= p_ledger_range;
    GET DIAGNOSTICS v_deleted_count = ROW_COUNT;
    RETURN QUERY SELECT 'transactions_row_v2'::TEXT, v_deleted_count;

    DELETE FROM operations_row_v2 WHERE ledger_range <= p_ledger_range;
    GET DIAGNOSTICS v_deleted_count = ROW_COUNT;
    RETURN QUERY SELECT 'operations_row_v2'::TEXT, v_deleted_count;

    -- Effects & Trades
    DELETE FROM effects_row_v1 WHERE ledger_range <= p_ledger_range;
    GET DIAGNOSTICS v_deleted_count = ROW_COUNT;
    RETURN QUERY SELECT 'effects_row_v1'::TEXT, v_deleted_count;

    DELETE FROM trades_row_v1 WHERE ledger_range <= p_ledger_range;
    GET DIAGNOSTICS v_deleted_count = ROW_COUNT;
    RETURN QUERY SELECT 'trades_row_v1'::TEXT, v_deleted_count;

    -- Snapshots
    DELETE FROM accounts_snapshot_v1 WHERE ledger_range <= p_ledger_range;
    GET DIAGNOSTICS v_deleted_count = ROW_COUNT;
    RETURN QUERY SELECT 'accounts_snapshot_v1'::TEXT, v_deleted_count;

    DELETE FROM trustlines_snapshot_v1 WHERE ledger_range <= p_ledger_range;
    GET DIAGNOSTICS v_deleted_count = ROW_COUNT;
    RETURN QUERY SELECT 'trustlines_snapshot_v1'::TEXT, v_deleted_count;

    DELETE FROM native_balances_snapshot_v1 WHERE ledger_range <= p_ledger_range;
    GET DIAGNOSTICS v_deleted_count = ROW_COUNT;
    RETURN QUERY SELECT 'native_balances_snapshot_v1'::TEXT, v_deleted_count;

    DELETE FROM account_signers_snapshot_v1 WHERE ledger_range <= p_ledger_range;
    GET DIAGNOSTICS v_deleted_count = ROW_COUNT;
    RETURN QUERY SELECT 'account_signers_snapshot_v1'::TEXT, v_deleted_count;

    DELETE FROM offers_snapshot_v1 WHERE ledger_range <= p_ledger_range;
    GET DIAGNOSTICS v_deleted_count = ROW_COUNT;
    RETURN QUERY SELECT 'offers_snapshot_v1'::TEXT, v_deleted_count;

    DELETE FROM claimable_balances_snapshot_v1 WHERE ledger_range <= p_ledger_range;
    GET DIAGNOSTICS v_deleted_count = ROW_COUNT;
    RETURN QUERY SELECT 'claimable_balances_snapshot_v1'::TEXT, v_deleted_count;

    DELETE FROM liquidity_pools_snapshot_v1 WHERE ledger_range <= p_ledger_range;
    GET DIAGNOSTICS v_deleted_count = ROW_COUNT;
    RETURN QUERY SELECT 'liquidity_pools_snapshot_v1'::TEXT, v_deleted_count;

    -- Soroban
    DELETE FROM contract_events_stream_v1 WHERE ledger_range <= p_ledger_range;
    GET DIAGNOSTICS v_deleted_count = ROW_COUNT;
    RETURN QUERY SELECT 'contract_events_stream_v1'::TEXT, v_deleted_count;

    DELETE FROM contract_data_snapshot_v1 WHERE ledger_range <= p_ledger_range;
    GET DIAGNOSTICS v_deleted_count = ROW_COUNT;
    RETURN QUERY SELECT 'contract_data_snapshot_v1'::TEXT, v_deleted_count;

    DELETE FROM contract_code_snapshot_v1 WHERE ledger_range <= p_ledger_range;
    GET DIAGNOSTICS v_deleted_count = ROW_COUNT;
    RETURN QUERY SELECT 'contract_code_snapshot_v1'::TEXT, v_deleted_count;

    DELETE FROM config_settings_snapshot_v1 WHERE ledger_range <= p_ledger_range;
    GET DIAGNOSTICS v_deleted_count = ROW_COUNT;
    RETURN QUERY SELECT 'config_settings_snapshot_v1'::TEXT, v_deleted_count;

    DELETE FROM ttl_snapshot_v1 WHERE ledger_range <= p_ledger_range;
    GET DIAGNOSTICS v_deleted_count = ROW_COUNT;
    RETURN QUERY SELECT 'ttl_snapshot_v1'::TEXT, v_deleted_count;

    DELETE FROM evicted_keys_state_v1 WHERE ledger_range <= p_ledger_range;
    GET DIAGNOSTICS v_deleted_count = ROW_COUNT;
    RETURN QUERY SELECT 'evicted_keys_state_v1'::TEXT, v_deleted_count;

    DELETE FROM restored_keys_state_v1 WHERE ledger_range <= p_ledger_range;
    GET DIAGNOSTICS v_deleted_count = ROW_COUNT;
    RETURN QUERY SELECT 'restored_keys_state_v1'::TEXT, v_deleted_count;

    RETURN;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION cleanup_flushed_data(BIGINT) IS 'Deletes flushed data from all 19 tables - safe to call after successful flush to cold storage';

-- ============================================================================
-- Usage Examples
-- ============================================================================
--
-- 1. Check current state:
--    SELECT * FROM get_high_watermark();
--
-- 2. Find flush candidates (>5 mins old):
--    SELECT * FROM get_flush_candidates(5);
--
-- 3. Cleanup after successful flush:
--    SELECT * FROM cleanup_flushed_data(1000000);
--
-- 4. High-watermark flush pattern:
--    -- Step 1: Get watermark
--    SELECT MIN(max_ledger_range) FROM get_high_watermark();
--    -- Step 2: Flush to DuckLake (external)
--    -- Step 3: Cleanup
--    SELECT * FROM cleanup_flushed_data(<watermark>);
