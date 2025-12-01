-- Example Queries for Contract Events
-- Demonstrates hybrid schema: indexed columns + JSONB flexibility

-- ============================================================================
-- Basic Queries
-- ============================================================================

-- 1. Show recent events (uses ledger_sequence index)
SELECT
    ledger_sequence,
    ledger_closed_at,
    contract_id,
    event_type,
    tx_hash
FROM contract_events
ORDER BY ledger_sequence DESC
LIMIT 10;

-- 2. Count events by type (uses event_type index)
SELECT
    event_type,
    COUNT(*) as count
FROM contract_events
GROUP BY event_type
ORDER BY count DESC;

-- 3. Events from specific contract (uses contract_id index)
SELECT *
FROM contract_events
WHERE contract_id = 'CABC123...'
ORDER BY ledger_sequence DESC
LIMIT 20;

-- ============================================================================
-- JSONB Queries (The Magic!)
-- ============================================================================

-- 4. Find transfers with amount > 1000 (queries JSONB with GIN index)
SELECT
    ledger_sequence,
    contract_id,
    event_type,
    topics->0 as event_name,
    topics->1 as from_address,
    topics->2 as to_address,
    event_data
FROM contract_events
WHERE event_type = 'transfer'
  AND (event_data->>'amount')::numeric > 1000
ORDER BY ledger_sequence DESC;

-- 5. Find all events involving a specific address (in topics)
SELECT
    ledger_sequence,
    contract_id,
    event_type,
    topics,
    event_data
FROM contract_events
WHERE topics::text LIKE '%GABC123...%'
ORDER BY ledger_sequence DESC
LIMIT 50;

-- 6. Extract specific field from event_data
SELECT
    ledger_sequence,
    contract_id,
    event_data->>'from' as from_account,
    event_data->>'to' as to_account,
    event_data->>'amount' as amount
FROM contract_events
WHERE event_type = 'transfer'
  AND event_data IS NOT NULL
LIMIT 20;

-- ============================================================================
-- Analytics Queries
-- ============================================================================

-- 7. Daily event volume by type
SELECT
    DATE(ledger_closed_at) as date,
    event_type,
    COUNT(*) as event_count
FROM contract_events
GROUP BY date, event_type
ORDER BY date DESC, event_count DESC;

-- 8. Most active contracts
SELECT
    contract_id,
    COUNT(*) as event_count,
    COUNT(DISTINCT event_type) as unique_event_types,
    MAX(ledger_sequence) as last_ledger
FROM contract_events
GROUP BY contract_id
ORDER BY event_count DESC
LIMIT 10;

-- 9. Hourly event rate
SELECT
    DATE_TRUNC('hour', ledger_closed_at) as hour,
    COUNT(*) as events,
    COUNT(DISTINCT contract_id) as active_contracts
FROM contract_events
GROUP BY hour
ORDER BY hour DESC
LIMIT 24;

-- ============================================================================
-- Advanced JSONB Queries
-- ============================================================================

-- 10. Complex nested JSONB query (asset transfers)
SELECT
    ledger_sequence,
    contract_id,
    event_data->'asset'->>'code' as asset_code,
    event_data->'asset'->>'issuer' as asset_issuer,
    (event_data->>'amount')::numeric as amount
FROM contract_events
WHERE event_type = 'transfer'
  AND event_data->'asset' IS NOT NULL
  AND event_data->'asset'->>'code' = 'USDC'
ORDER BY ledger_sequence DESC;

-- 11. Aggregate by nested JSONB field
SELECT
    event_data->'asset'->>'code' as asset,
    COUNT(*) as transfer_count,
    SUM((event_data->>'amount')::numeric) as total_volume,
    AVG((event_data->>'amount')::numeric) as avg_amount
FROM contract_events
WHERE event_type = 'transfer'
  AND event_data->'asset' IS NOT NULL
GROUP BY asset
ORDER BY total_volume DESC;

-- ============================================================================
-- Time-Series Queries
-- ============================================================================

-- 12. Events in specific time range (uses closed_at index)
SELECT *
FROM contract_events
WHERE ledger_closed_at BETWEEN '2025-01-01' AND '2025-01-31'
ORDER BY ledger_closed_at DESC;

-- 13. Latest event per contract
SELECT DISTINCT ON (contract_id)
    contract_id,
    ledger_sequence,
    event_type,
    ledger_closed_at
FROM contract_events
ORDER BY contract_id, ledger_sequence DESC;

-- ============================================================================
-- Success/Failure Analysis
-- ============================================================================

-- 14. Failed vs successful transactions (uses tx_successful index)
SELECT
    tx_successful,
    COUNT(*) as count,
    COUNT(DISTINCT contract_id) as unique_contracts
FROM contract_events
GROUP BY tx_successful;

-- 15. Failure rate by contract
SELECT
    contract_id,
    COUNT(*) as total_events,
    SUM(CASE WHEN tx_successful THEN 0 ELSE 1 END) as failed_events,
    ROUND(
        100.0 * SUM(CASE WHEN tx_successful THEN 0 ELSE 1 END) / COUNT(*),
        2
    ) as failure_rate_pct
FROM contract_events
GROUP BY contract_id
HAVING COUNT(*) > 10
ORDER BY failure_rate_pct DESC;

-- ============================================================================
-- Performance Views (Optional Materialized Views)
-- ============================================================================

-- 16. Create materialized view for daily stats (faster dashboard queries)
CREATE MATERIALIZED VIEW IF NOT EXISTS daily_contract_stats AS
SELECT
    DATE(ledger_closed_at) as date,
    contract_id,
    event_type,
    COUNT(*) as event_count,
    COUNT(DISTINCT tx_hash) as unique_transactions,
    MIN(ledger_sequence) as first_ledger,
    MAX(ledger_sequence) as last_ledger
FROM contract_events
GROUP BY date, contract_id, event_type;

-- Create index on materialized view
CREATE INDEX IF NOT EXISTS idx_daily_stats_date
    ON daily_contract_stats(date DESC);

-- Refresh materialized view (run periodically)
-- REFRESH MATERIALIZED VIEW daily_contract_stats;

-- ============================================================================
-- XDR Verification Queries
-- ============================================================================

-- 17. Get raw XDR for verification/debugging
SELECT
    ledger_sequence,
    contract_id,
    event_type,
    topics_xdr,
    data_xdr
FROM contract_events
WHERE ledger_sequence = 1234
  AND event_index = 0;

-- ============================================================================
-- Combined Index Usage (Most Efficient)
-- ============================================================================

-- 18. Use composite index (contract_id, event_type, ledger_closed_at)
SELECT *
FROM contract_events
WHERE contract_id = 'CABC123...'
  AND event_type = 'transfer'
  AND ledger_closed_at >= NOW() - INTERVAL '7 days'
ORDER BY ledger_closed_at DESC;

-- 19. Transaction lookup (uses tx_hash index)
SELECT *
FROM contract_events
WHERE tx_hash = 'abc123...'
ORDER BY event_index;

-- ============================================================================
-- Data Export Examples
-- ============================================================================

-- 20. Export to CSV
\copy (SELECT ledger_sequence, contract_id, event_type, ledger_closed_at FROM contract_events WHERE ledger_sequence BETWEEN 1000 AND 2000) TO '/tmp/events.csv' CSV HEADER;

-- 21. Export to JSON
SELECT json_agg(row_to_json(t))
FROM (
    SELECT
        ledger_sequence,
        contract_id,
        event_type,
        topics,
        event_data
    FROM contract_events
    WHERE ledger_sequence BETWEEN 1000 AND 1010
) t;
