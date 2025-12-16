-- Test Queries for DuckLake Mainnet Backfill
-- Run these while ingestion is happening to monitor progress and validate data
--
-- Usage:
--   duckdb catalogs/mainnet-backfill-test.duckdb < test-queries.sql
--   # Or interactively:
--   duckdb catalogs/mainnet-backfill-test.duckdb

-- ═══════════════════════════════════════════════════════════
-- 1. INGESTION PROGRESS MONITORING
-- ═══════════════════════════════════════════════════════════

.print ───────────────────────────────────────────────────────────
.print 📊 Ingestion Progress
.print ───────────────────────────────────────────────────────────

-- Total ledgers ingested
SELECT
  COUNT(*) as total_ledgers,
  MIN(sequence) as first_ledger,
  MAX(sequence) as last_ledger,
  MAX(sequence) - MIN(sequence) + 1 as expected_ledgers,
  COUNT(*) = (MAX(sequence) - MIN(sequence) + 1) as no_gaps
FROM mainnet.ledgers_row_v2;

-- Ledgers per hour (ingestion rate)
SELECT
  DATE_TRUNC('hour', closed_at) as hour,
  COUNT(*) as ledgers,
  SUM(transaction_count) as transactions,
  SUM(operation_count) as operations
FROM mainnet.ledgers_row_v2
GROUP BY DATE_TRUNC('hour', closed_at)
ORDER BY hour DESC
LIMIT 10;

-- ═══════════════════════════════════════════════════════════
-- 2. DATA QUALITY CHECKS
-- ═══════════════════════════════════════════════════════════

.print
.print ───────────────────────────────────────────────────────────
.print 🔍 Data Quality Checks
.print ───────────────────────────────────────────────────────────

-- Check for gaps in ledger sequence
WITH ledger_gaps AS (
  SELECT
    sequence,
    sequence - LAG(sequence) OVER (ORDER BY sequence) - 1 AS gap_size
  FROM mainnet.ledgers_row_v2
)
SELECT
  COUNT(*) as gap_count,
  COALESCE(SUM(gap_size), 0) as total_missing_ledgers
FROM ledger_gaps
WHERE gap_size > 0;

-- Verify transaction counts match
SELECT
  'ledgers_row_v2' as table_name,
  SUM(transaction_count) as tx_count_from_ledgers,
  (SELECT COUNT(*) FROM mainnet.transactions_row_v2) as actual_tx_count,
  SUM(transaction_count) = (SELECT COUNT(*) FROM mainnet.transactions_row_v2) as counts_match
FROM mainnet.ledgers_row_v2;

-- Verify operation counts match
SELECT
  'ledgers_row_v2' as table_name,
  SUM(operation_count) as op_count_from_ledgers,
  (SELECT COUNT(*) FROM mainnet.operations_row_v2) as actual_op_count,
  SUM(operation_count) = (SELECT COUNT(*) FROM mainnet.operations_row_v2) as counts_match
FROM mainnet.ledgers_row_v2;

-- ═══════════════════════════════════════════════════════════
-- 3. TABLE SIZES & ROW COUNTS
-- ═══════════════════════════════════════════════════════════

.print
.print ───────────────────────────────────────────────────────────
.print 📈 Table Statistics
.print ───────────────────────────────────────────────────────────

-- Row counts for all tables
SELECT 'ledgers_row_v2' as table_name, COUNT(*) as row_count FROM mainnet.ledgers_row_v2
UNION ALL
SELECT 'transactions_row_v2', COUNT(*) FROM mainnet.transactions_row_v2
UNION ALL
SELECT 'operations_row_v2', COUNT(*) FROM mainnet.operations_row_v2
UNION ALL
SELECT 'balances_row_v2', COUNT(*) FROM mainnet.balances_row_v2
UNION ALL
SELECT 'effects_row_v2', COUNT(*) FROM mainnet.effects_row_v2
UNION ALL
SELECT 'trades_row_v2', COUNT(*) FROM mainnet.trades_row_v2
UNION ALL
SELECT 'accounts_row_v2', COUNT(*) FROM mainnet.accounts_row_v2
UNION ALL
SELECT 'trustlines_row_v2', COUNT(*) FROM mainnet.trustlines_row_v2
UNION ALL
SELECT 'offers_row_v2', COUNT(*) FROM mainnet.offers_row_v2
UNION ALL
SELECT 'claimable_balances_row_v2', COUNT(*) FROM mainnet.claimable_balances_row_v2
UNION ALL
SELECT 'liquidity_pools_row_v2', COUNT(*) FROM mainnet.liquidity_pools_row_v2
UNION ALL
SELECT 'contract_events_row_v2', COUNT(*) FROM mainnet.contract_events_row_v2
UNION ALL
SELECT 'contract_data_row_v2', COUNT(*) FROM mainnet.contract_data_row_v2
UNION ALL
SELECT 'contract_code_row_v2', COUNT(*) FROM mainnet.contract_code_row_v2
UNION ALL
SELECT 'config_settings_row_v2', COUNT(*) FROM mainnet.config_settings_row_v2
UNION ALL
SELECT 'ttl_row_v2', COUNT(*) FROM mainnet.ttl_row_v2
ORDER BY row_count DESC;

-- ═══════════════════════════════════════════════════════════
-- 4. BLOCKCHAIN ANALYTICS
-- ═══════════════════════════════════════════════════════════

.print
.print ───────────────────────────────────────────────────────────
.print 🌟 Blockchain Analytics
.print ───────────────────────────────────────────────────────────

-- Top 10 most active ledgers (by transaction count)
SELECT
  sequence,
  closed_at,
  transaction_count,
  operation_count,
  successful_tx_count,
  failed_tx_count
FROM mainnet.ledgers_row_v2
ORDER BY transaction_count DESC
LIMIT 10;

-- Transaction success rate
SELECT
  COUNT(*) as total_transactions,
  SUM(CASE WHEN successful THEN 1 ELSE 0 END) as successful,
  SUM(CASE WHEN NOT successful THEN 1 ELSE 0 END) as failed,
  ROUND(100.0 * SUM(CASE WHEN successful THEN 1 ELSE 0 END) / COUNT(*), 2) as success_rate_pct
FROM mainnet.transactions_row_v2;

-- Most common operation types
SELECT
  operation_type,
  COUNT(*) as count,
  ROUND(100.0 * COUNT(*) / (SELECT COUNT(*) FROM mainnet.operations_row_v2), 2) as percentage
FROM mainnet.operations_row_v2
GROUP BY operation_type
ORDER BY count DESC
LIMIT 10;

-- Fee statistics
SELECT
  MIN(max_fee) as min_max_fee,
  AVG(max_fee) as avg_max_fee,
  MAX(max_fee) as max_max_fee,
  PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY max_fee) as median_max_fee,
  PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY max_fee) as p95_max_fee
FROM mainnet.transactions_row_v2;

-- ═══════════════════════════════════════════════════════════
-- 5. ACCOUNT ANALYTICS
-- ═══════════════════════════════════════════════════════════

.print
.print ───────────────────────────────────────────────────────────
.print 👤 Account Analytics
.print ───────────────────────────────────────────────────────────

-- Total unique accounts
SELECT
  COUNT(DISTINCT account_id) as total_accounts
FROM mainnet.accounts_row_v2;

-- Top 10 richest accounts (by native balance)
SELECT
  account_id,
  native_balance / 10000000.0 as xlm_balance,
  num_subentries,
  num_sponsoring,
  num_sponsored
FROM mainnet.accounts_row_v2
ORDER BY native_balance DESC
LIMIT 10;

-- Accounts by number of trustlines
SELECT
  CASE
    WHEN trustline_count = 0 THEN '0 trustlines'
    WHEN trustline_count BETWEEN 1 AND 5 THEN '1-5 trustlines'
    WHEN trustline_count BETWEEN 6 AND 10 THEN '6-10 trustlines'
    WHEN trustline_count BETWEEN 11 AND 50 THEN '11-50 trustlines'
    ELSE '50+ trustlines'
  END as trustline_bucket,
  COUNT(*) as account_count
FROM (
  SELECT
    account_id,
    COUNT(*) as trustline_count
  FROM mainnet.trustlines_row_v2
  GROUP BY account_id
) trustline_counts
GROUP BY trustline_bucket
ORDER BY
  CASE trustline_bucket
    WHEN '0 trustlines' THEN 1
    WHEN '1-5 trustlines' THEN 2
    WHEN '6-10 trustlines' THEN 3
    WHEN '11-50 trustlines' THEN 4
    ELSE 5
  END;

-- ═══════════════════════════════════════════════════════════
-- 6. ASSET & TRADING ANALYTICS
-- ═══════════════════════════════════════════════════════════

.print
.print ───────────────────────────────────────────────────────────
.print 💱 Asset & Trading Analytics
.print ───────────────────────────────────────────────────────────

-- Most popular assets by trustline count
SELECT
  asset_code,
  asset_issuer,
  COUNT(*) as trustline_count
FROM mainnet.trustlines_row_v2
WHERE asset_type != 'native'
GROUP BY asset_code, asset_issuer
ORDER BY trustline_count DESC
LIMIT 10;

-- Total trades and volume
SELECT
  COUNT(*) as total_trades,
  COUNT(DISTINCT base_asset_code || ':' || base_asset_issuer) as unique_base_assets,
  COUNT(DISTINCT counter_asset_code || ':' || counter_asset_issuer) as unique_counter_assets
FROM mainnet.trades_row_v2;

-- Trading activity over time
SELECT
  DATE_TRUNC('day', ledger_closed_at) as day,
  COUNT(*) as trade_count,
  COUNT(DISTINCT base_account) as unique_traders
FROM mainnet.trades_row_v2
GROUP BY DATE_TRUNC('day', ledger_closed_at)
ORDER BY day DESC
LIMIT 10;

-- ═══════════════════════════════════════════════════════════
-- 7. SOROBAN (SMART CONTRACT) ANALYTICS
-- ═══════════════════════════════════════════════════════════

.print
.print ───────────────────────────────────────────────────────────
.print 🤖 Soroban Smart Contract Analytics
.print ───────────────────────────────────────────────────────────

-- Contract events summary
SELECT
  COUNT(*) as total_events,
  COUNT(DISTINCT contract_id) as unique_contracts
FROM mainnet.contract_events_row_v2;

-- Most active contracts (by event count)
SELECT
  contract_id,
  COUNT(*) as event_count
FROM mainnet.contract_events_row_v2
GROUP BY contract_id
ORDER BY event_count DESC
LIMIT 10;

-- Contract data entries
SELECT
  COUNT(*) as total_entries,
  COUNT(DISTINCT contract_id) as unique_contracts_with_data
FROM mainnet.contract_data_row_v2;

-- Contract code uploads
SELECT
  COUNT(*) as total_code_uploads,
  COUNT(DISTINCT hash) as unique_code_hashes
FROM mainnet.contract_code_row_v2;

-- ═══════════════════════════════════════════════════════════
-- 8. LEDGER TIMELINE & PATTERNS
-- ═══════════════════════════════════════════════════════════

.print
.print ───────────────────────────────────────────────────────────
.print ⏰ Ledger Timeline & Patterns
.print ───────────────────────────────────────────────────────────

-- Ledger close time distribution (should be ~5 seconds)
SELECT
  ROUND(AVG(ledger_close_time_seconds), 2) as avg_close_time_seconds,
  MIN(ledger_close_time_seconds) as min_close_time,
  MAX(ledger_close_time_seconds) as max_close_time,
  PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY ledger_close_time_seconds) as median_close_time
FROM (
  SELECT
    sequence,
    EXTRACT(EPOCH FROM (closed_at - LAG(closed_at) OVER (ORDER BY sequence))) as ledger_close_time_seconds
  FROM mainnet.ledgers_row_v2
) close_times
WHERE ledger_close_time_seconds IS NOT NULL;

-- Activity patterns by hour of day
SELECT
  EXTRACT(HOUR FROM closed_at) as hour_of_day,
  COUNT(*) as ledger_count,
  AVG(transaction_count) as avg_transactions_per_ledger,
  SUM(transaction_count) as total_transactions
FROM mainnet.ledgers_row_v2
GROUP BY EXTRACT(HOUR FROM closed_at)
ORDER BY hour_of_day;

-- ═══════════════════════════════════════════════════════════
-- 9. QUALITY VALIDATION QUERIES
-- ═══════════════════════════════════════════════════════════

.print
.print ───────────────────────────────────────────────────────────
.print ✅ Data Quality Validation
.print ───────────────────────────────────────────────────────────

-- Orphaned transactions (transactions without corresponding ledger)
SELECT
  COUNT(*) as orphaned_transaction_count
FROM mainnet.transactions_row_v2 t
WHERE NOT EXISTS (
  SELECT 1 FROM mainnet.ledgers_row_v2 l
  WHERE l.sequence = t.ledger_sequence
);

-- Orphaned operations (operations without corresponding transaction)
SELECT
  COUNT(*) as orphaned_operation_count
FROM mainnet.operations_row_v2 o
WHERE NOT EXISTS (
  SELECT 1 FROM mainnet.transactions_row_v2 t
  WHERE t.transaction_hash = o.transaction_hash
);

-- Check for duplicate ledgers
SELECT
  sequence,
  COUNT(*) as duplicate_count
FROM mainnet.ledgers_row_v2
GROUP BY sequence
HAVING COUNT(*) > 1;

.print
.print ═══════════════════════════════════════════════════════════
.print ✅ Query execution complete!
.print ═══════════════════════════════════════════════════════════
