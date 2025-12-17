-- Example SQL queries for DuckLake Silver layer
-- Connect with: duckdb and attach catalog

-- Setup (run first)
INSTALL ducklake;
LOAD ducklake;
INSTALL httpfs;
LOAD httpfs;

CREATE SECRET (
    TYPE S3,
    KEY_ID 'YOUR_KEY',
    SECRET 'YOUR_SECRET',
    REGION 'us-west-004',
    ENDPOINT 's3.us-west-004.backblazeb2.com',
    URL_STYLE 'path'
);

ATTACH 'ducklake:postgres:postgresql://user:pass@host:port/catalog?sslmode=require'
  AS catalog
  (DATA_PATH 's3://bucket/network_silver/', METADATA_SCHEMA 'bronze_meta');

-- ============================================
-- CURRENT STATE QUERIES (Point-in-time)
-- ============================================

-- Top accounts by balance
SELECT
    account_id,
    balance,
    num_subentries,
    ledger_sequence
FROM catalog.silver.accounts_current
ORDER BY CAST(balance AS BIGINT) DESC
LIMIT 20;

-- Active trustlines for a specific asset
SELECT
    account_id,
    asset_code,
    asset_issuer,
    balance,
    trust_limit
FROM catalog.silver.trustlines_current
WHERE asset_code = 'USDC'
ORDER BY CAST(balance AS BIGINT) DESC;

-- Current liquidity pools
SELECT
    liquidity_pool_id,
    asset_a_code,
    asset_b_code,
    asset_a_amount,
    asset_b_amount,
    total_trustlines,
    total_shares
FROM catalog.silver.liquidity_pools_current;

-- Active Soroban contract data
SELECT
    contract_id,
    contract_key_type,
    ledger_sequence
FROM catalog.silver.contract_data_current
LIMIT 100;

-- ============================================
-- HISTORICAL SNAPSHOT QUERIES (SCD Type 2)
-- ============================================

-- Account balance history for specific account
SELECT
    account_id,
    balance,
    ledger_sequence,
    valid_from,
    valid_to,
    CASE
        WHEN valid_to IS NULL THEN true
        ELSE false
    END AS is_current
FROM catalog.silver.accounts_snapshot
WHERE account_id = 'GXXXXXXXXXXXXXX'
ORDER BY ledger_sequence DESC;

-- Trustline changes over time
SELECT
    account_id,
    asset_code,
    balance,
    ledger_sequence,
    valid_from,
    valid_to
FROM catalog.silver.trustlines_snapshot
WHERE account_id = 'GXXXXXXXXXXXXXX'
    AND asset_code = 'USDC'
ORDER BY ledger_sequence DESC;

-- Find accounts that existed in a specific time range
SELECT
    account_id,
    balance,
    valid_from,
    valid_to
FROM catalog.silver.accounts_snapshot
WHERE valid_from <= '2024-01-01 12:00:00'
    AND (valid_to IS NULL OR valid_to >= '2024-01-01 12:00:00');

-- ============================================
-- ENRICHED OPERATIONS QUERIES
-- ============================================

-- Recent successful payment operations with transaction context
SELECT
    o.transaction_hash,
    o.ledger_sequence,
    o.ledger_closed_at,
    o.source_account,
    o.destination,
    o.asset_code,
    o.amount,
    o.tx_successful,
    o.tx_fee_charged,
    o.tx_memo_type,
    o.ledger_transaction_count
FROM catalog.silver.enriched_history_operations o
WHERE o.is_payment_op = true
    AND o.tx_successful = true
ORDER BY o.ledger_sequence DESC
LIMIT 100;

-- Failed transactions analysis
SELECT
    ledger_sequence,
    COUNT(*) as failed_ops_count,
    AVG(tx_fee_charged) as avg_fee
FROM catalog.silver.enriched_history_operations
WHERE tx_successful = false
GROUP BY ledger_sequence
ORDER BY failed_ops_count DESC
LIMIT 20;

-- Soroban contract invocations
SELECT
    transaction_hash,
    ledger_sequence,
    ledger_closed_at,
    source_account,
    tx_successful,
    tx_fee_charged
FROM catalog.silver.enriched_history_operations_soroban
ORDER BY ledger_sequence DESC
LIMIT 100;

-- ============================================
-- TOKEN TRANSFERS ANALYTICS
-- ============================================

-- All token transfers (classic + Soroban unified)
SELECT
    timestamp,
    source_type,
    from_account,
    to_account,
    asset_code,
    amount,
    transaction_successful
FROM catalog.silver.token_transfers_raw
WHERE transaction_successful = true
ORDER BY timestamp DESC
LIMIT 100;

-- Transfer volume by asset (last 24 hours)
SELECT
    asset_code,
    source_type,
    COUNT(*) as transfer_count,
    SUM(CAST(amount AS DOUBLE)) as total_volume,
    AVG(CAST(amount AS DOUBLE)) as avg_transfer
FROM catalog.silver.token_transfers_raw
WHERE transaction_successful = true
    AND timestamp >= NOW() - INTERVAL '24 hours'
    AND amount IS NOT NULL
GROUP BY asset_code, source_type
ORDER BY total_volume DESC;

-- Classic vs Soroban transfer comparison
SELECT
    source_type,
    COUNT(*) as count,
    COUNT(DISTINCT from_account) as unique_senders,
    COUNT(DISTINCT to_account) as unique_receivers
FROM catalog.silver.token_transfers_raw
WHERE transaction_successful = true
    AND timestamp >= NOW() - INTERVAL '7 days'
GROUP BY source_type;

-- ============================================
-- CROSS-TABLE ANALYTICS
-- ============================================

-- Account activity: current state + recent transfers
SELECT
    a.account_id,
    a.balance as current_balance,
    COUNT(t.transaction_hash) as recent_transfers,
    SUM(CAST(t.amount AS DOUBLE)) as total_sent
FROM catalog.silver.accounts_current a
LEFT JOIN catalog.silver.token_transfers_raw t
    ON a.account_id = t.from_account
    AND t.timestamp >= NOW() - INTERVAL '24 hours'
GROUP BY a.account_id, a.balance
HAVING COUNT(t.transaction_hash) > 0
ORDER BY recent_transfers DESC
LIMIT 50;

-- Liquidity pool operations with enriched context
SELECT
    e.ledger_sequence,
    e.ledger_closed_at,
    e.source_account,
    e.type,
    e.tx_successful,
    l.liquidity_pool_id,
    l.asset_a_code,
    l.asset_b_code,
    l.total_shares
FROM catalog.silver.enriched_history_operations e
JOIN catalog.silver.liquidity_pools_current l
    ON e.liquidity_pool_id = l.liquidity_pool_id
WHERE e.type IN (22, 23)  -- LIQUIDITY_POOL_DEPOSIT, LIQUIDITY_POOL_WITHDRAW
ORDER BY e.ledger_sequence DESC
LIMIT 50;

-- ============================================
-- TIME-SERIES ANALYTICS
-- ============================================

-- Daily transaction volume
SELECT
    DATE_TRUNC('day', timestamp) as date,
    COUNT(*) as transfer_count,
    COUNT(DISTINCT from_account) as unique_senders,
    SUM(CAST(amount AS DOUBLE)) as total_volume
FROM catalog.silver.token_transfers_raw
WHERE transaction_successful = true
    AND asset_code = 'XLM'
    AND timestamp >= NOW() - INTERVAL '30 days'
GROUP BY DATE_TRUNC('day', timestamp)
ORDER BY date DESC;

-- Hourly Soroban activity
SELECT
    DATE_TRUNC('hour', ledger_closed_at) as hour,
    COUNT(*) as invocation_count,
    COUNT(DISTINCT source_account) as unique_accounts,
    SUM(tx_fee_charged) as total_fees
FROM catalog.silver.enriched_history_operations_soroban
WHERE ledger_closed_at >= NOW() - INTERVAL '7 days'
GROUP BY DATE_TRUNC('hour', ledger_closed_at)
ORDER BY hour DESC;
