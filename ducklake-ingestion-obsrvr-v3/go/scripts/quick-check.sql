-- Quick Check - Run this for a fast progress snapshot
-- Usage: duckdb catalogs/mainnet-backfill-test.duckdb < quick-check.sql

.print ═══════════════════════════════════════════════════════════
.print 🚀 Quick Ingestion Status Check
.print ═══════════════════════════════════════════════════════════
.print

-- Overall progress
SELECT
  '📊 Progress' as metric,
  COUNT(*) || ' ledgers' as value
FROM mainnet.ledgers_row_v2
UNION ALL
SELECT
  '📝 Ledger Range',
  CAST(MIN(sequence) as VARCHAR) || ' → ' || CAST(MAX(sequence) as VARCHAR)
FROM mainnet.ledgers_row_v2
UNION ALL
SELECT
  '💳 Transactions',
  CAST(COUNT(*) as VARCHAR)
FROM mainnet.transactions_row_v2
UNION ALL
SELECT
  '⚙️  Operations',
  CAST(COUNT(*) as VARCHAR)
FROM mainnet.operations_row_v2
UNION ALL
SELECT
  '✅ Success Rate',
  CAST(ROUND(100.0 * SUM(CASE WHEN successful THEN 1 ELSE 0 END) / COUNT(*), 2) as VARCHAR) || '%'
FROM mainnet.transactions_row_v2
UNION ALL
SELECT
  '🔍 Data Quality',
  CASE
    WHEN COUNT(*) = (MAX(sequence) - MIN(sequence) + 1) THEN '✅ No gaps'
    ELSE '⚠️  ' || CAST((MAX(sequence) - MIN(sequence) + 1 - COUNT(*)) as VARCHAR) || ' gaps found'
  END
FROM mainnet.ledgers_row_v2;

.print
.print ═══════════════════════════════════════════════════════════
