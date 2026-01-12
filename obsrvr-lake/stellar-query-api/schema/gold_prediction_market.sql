-- ============================================================================
-- GOLD LAYER: Prediction Market Analytics (Polymarket-style)
-- ============================================================================
-- Contract: CDRCPTMQHCUX5WDQRDKEG5YQIFO43AWIVDMWL6RA3Y3ODLTTLXYK6CYS
-- Purpose: Track prediction market bets, settlements, and outcomes
-- Date: 2026-01-03
--
-- Contract Interface:
-- - place_bet(bettor: address, market_id: u64, outcome: symbol, amount: i128)
-- - settle_market(market_id: u64, result: symbol)
-- - create_market(market_type_id: u32, outcomes: vec<symbol>) -> u64
-- - lock_market(market_id: u64)
-- - cancel_market(market_id: u64)
-- - claim_payout(bettor: address, market_id: u64)
-- ============================================================================

-- ============================================================================
-- VIEW 1: All Bets Placed
-- ============================================================================
CREATE OR REPLACE VIEW gold_prediction_bets AS
SELECT
    -- TOID for unique operation identification
    (ledger_sequence::BIGINT << 32) | (transaction_index::BIGINT << 12) | operation_index::BIGINT as toid,

    -- Ledger context
    ledger_sequence,
    transaction_hash,
    closed_at,
    successful,

    -- Contract context
    contract_id as market_contract,

    -- Bet details extracted from arguments
    -- place_bet(bettor: address, market_id: u64, outcome: symbol, amount: i128)

    -- arg[0]: bettor address
    arguments_json::jsonb->0->>'address' as bettor_address,

    -- arg[1]: market_id (u64)
    (arguments_json::jsonb->1)::BIGINT as market_id,

    -- arg[2]: outcome (symbol - what they're betting on)
    TRIM(BOTH '"' FROM (arguments_json::jsonb->2)::TEXT) as predicted_outcome,

    -- arg[3]: amount (i128)
    (arguments_json::jsonb->3->>'value')::BIGINT as bet_amount_stroops,
    ROUND((arguments_json::jsonb->3->>'value')::BIGINT / 10000000.0, 7) as bet_amount_tokens,

    -- Metadata
    source_account as transaction_source,
    inserted_at as indexed_at

FROM contract_invocations_raw
WHERE
    contract_id = 'CDRCPTMQHCUX5WDQRDKEG5YQIFO43AWIVDMWL6RA3Y3ODLTTLXYK6CYS'
    AND function_name = 'place_bet'
    AND successful = true
ORDER BY ledger_sequence DESC, transaction_index DESC, operation_index DESC;

-- ============================================================================
-- VIEW 2: Market Settlements
-- ============================================================================
CREATE OR REPLACE VIEW gold_prediction_settlements AS
SELECT
    -- TOID for unique operation identification
    (ledger_sequence::BIGINT << 32) | (transaction_index::BIGINT << 12) | operation_index::BIGINT as toid,

    -- Ledger context
    ledger_sequence,
    transaction_hash,
    closed_at,
    successful,

    -- Contract context
    contract_id as market_contract,

    -- Settlement details extracted from arguments
    -- settle_market(market_id: u64, result: symbol)

    -- arg[0]: market_id (u64)
    (arguments_json::jsonb->0)::BIGINT as market_id,

    -- arg[1]: result (symbol - actual outcome)
    TRIM(BOTH '"' FROM (arguments_json::jsonb->1)::TEXT) as actual_outcome,

    -- Metadata
    source_account as oracle_address,
    inserted_at as indexed_at

FROM contract_invocations_raw
WHERE
    contract_id = 'CDRCPTMQHCUX5WDQRDKEG5YQIFO43AWIVDMWL6RA3Y3ODLTTLXYK6CYS'
    AND function_name = 'settle_market'
    AND successful = true
ORDER BY ledger_sequence DESC, transaction_index DESC, operation_index DESC;

-- ============================================================================
-- VIEW 3: Market Creations
-- ============================================================================
CREATE OR REPLACE VIEW gold_prediction_market_created AS
SELECT
    -- TOID for unique operation identification
    (ledger_sequence::BIGINT << 32) | (transaction_index::BIGINT << 12) | operation_index::BIGINT as toid,

    -- Ledger context
    ledger_sequence,
    transaction_hash,
    closed_at,
    successful,

    -- Contract context
    contract_id as market_contract,

    -- Market creation details
    -- create_market(market_type_id: u32, outcomes: vec<symbol>) -> u64

    -- arg[0]: market_type_id (0=Standard, 1=PropBet, 2=Tournament)
    (arguments_json::jsonb->0)::INTEGER as market_type_id,
    CASE (arguments_json::jsonb->0)::INTEGER
        WHEN 0 THEN 'Standard'
        WHEN 1 THEN 'PropBet'
        WHEN 2 THEN 'Tournament'
        ELSE 'Unknown'
    END as market_type,

    -- arg[1]: outcomes (vec<symbol>)
    arguments_json::jsonb->1 as possible_outcomes,
    jsonb_array_length(arguments_json::jsonb->1) as outcome_count,

    -- Metadata
    source_account as creator_address,
    inserted_at as indexed_at

FROM contract_invocations_raw
WHERE
    contract_id = 'CDRCPTMQHCUX5WDQRDKEG5YQIFO43AWIVDMWL6RA3Y3ODLTTLXYK6CYS'
    AND function_name = 'create_market'
    AND successful = true
ORDER BY ledger_sequence DESC, transaction_index DESC, operation_index DESC;

-- ============================================================================
-- VIEW 4: Complete Prediction Market Lifecycle
-- ============================================================================
CREATE OR REPLACE VIEW gold_prediction_market_lifecycle AS
SELECT
    -- Bet information
    b.toid as bet_toid,
    b.ledger_sequence as bet_ledger,
    b.closed_at as bet_time,
    b.bettor_address,
    b.market_id,
    b.predicted_outcome,
    b.bet_amount_tokens,

    -- Settlement information
    s.toid as settlement_toid,
    s.ledger_sequence as settlement_ledger,
    s.closed_at as settlement_time,
    s.actual_outcome,
    s.oracle_address,

    -- Bet result (CORRECT prediction matching)
    CASE
        WHEN b.predicted_outcome = s.actual_outcome THEN 'WON'
        WHEN b.predicted_outcome != s.actual_outcome THEN 'LOST'
        ELSE 'PENDING'
    END as bet_result,

    -- Time metrics
    EXTRACT(EPOCH FROM (s.closed_at - b.closed_at)) as seconds_to_settlement,
    s.ledger_sequence - b.ledger_sequence as ledgers_to_settlement,

    -- Metadata
    b.market_contract
FROM
    gold_prediction_bets b
    LEFT JOIN gold_prediction_settlements s
        ON b.market_id = s.market_id  -- Match by market_id
        AND s.ledger_sequence >= b.ledger_sequence  -- Settlement after bet
ORDER BY b.ledger_sequence DESC;

-- ============================================================================
-- VIEW 5: Market Summary Statistics
-- ============================================================================
CREATE OR REPLACE VIEW gold_prediction_market_summary AS
SELECT
    market_id,

    -- Bet statistics
    COUNT(*) as total_bets,
    COUNT(DISTINCT bettor_address) as unique_bettors,
    SUM(bet_amount_tokens) as total_volume,
    AVG(bet_amount_tokens) as avg_bet_size,
    MIN(bet_amount_tokens) as min_bet,
    MAX(bet_amount_tokens) as max_bet,

    -- Outcome distribution (before settlement)
    jsonb_object_agg(
        predicted_outcome,
        COUNT(*)
    ) as bets_by_outcome,

    -- Settlement info
    MAX(actual_outcome) as winning_outcome,
    MAX(settlement_time) as settled_at,
    MAX(oracle_address) as settled_by,

    -- Win/loss statistics
    COUNT(*) FILTER (WHERE bet_result = 'WON') as winning_bets,
    COUNT(*) FILTER (WHERE bet_result = 'LOST') as losing_bets,
    SUM(bet_amount_tokens) FILTER (WHERE bet_result = 'WON') as winning_volume,
    SUM(bet_amount_tokens) FILTER (WHERE bet_result = 'LOST') as losing_volume,

    -- Time metrics
    MIN(bet_time) as first_bet_at,
    MAX(bet_time) as last_bet_at,
    AVG(seconds_to_settlement) as avg_settlement_time

FROM gold_prediction_market_lifecycle
GROUP BY market_id
ORDER BY market_id DESC;

-- ============================================================================
-- EXAMPLE QUERIES
-- ============================================================================

-- Query 1: Show all markets with outcomes
-- SELECT * FROM gold_prediction_market_summary;

-- Query 2: Bettor performance across all markets
-- SELECT
--     bettor_address,
--     COUNT(DISTINCT market_id) as markets_participated,
--     COUNT(*) as total_bets,
--     SUM(bet_amount_tokens) as total_wagered,
--     COUNT(*) FILTER (WHERE bet_result = 'WON') as wins,
--     COUNT(*) FILTER (WHERE bet_result = 'LOST') as losses,
--     ROUND(100.0 * COUNT(*) FILTER (WHERE bet_result = 'WON') / NULLIF(COUNT(*), 0), 2) as win_rate_pct,
--     SUM(bet_amount_tokens) FILTER (WHERE bet_result = 'WON') as won_volume,
--     SUM(bet_amount_tokens) FILTER (WHERE bet_result = 'LOST') as lost_volume
-- FROM gold_prediction_market_lifecycle
-- GROUP BY bettor_address
-- ORDER BY total_wagered DESC;

-- Query 3: Market outcome distribution
-- SELECT
--     market_id,
--     predicted_outcome,
--     COUNT(*) as bet_count,
--     SUM(bet_amount_tokens) as total_on_outcome,
--     ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY market_id), 2) as pct_of_bets,
--     ROUND(100.0 * SUM(bet_amount_tokens) / SUM(SUM(bet_amount_tokens)) OVER (PARTITION BY market_id), 2) as pct_of_volume
-- FROM gold_prediction_bets
-- GROUP BY market_id, predicted_outcome
-- ORDER BY market_id, total_on_outcome DESC;

-- Query 4: Oracle performance (settlement patterns)
-- SELECT
--     oracle_address,
--     COUNT(*) as markets_settled,
--     COUNT(DISTINCT DATE(settlement_time)) as days_active,
--     MIN(settlement_time) as first_settlement,
--     MAX(settlement_time) as last_settlement
-- FROM gold_prediction_settlements
-- GROUP BY oracle_address;

-- ============================================================================
-- VERIFICATION
-- ============================================================================
SELECT
    'Bets Placed' as metric,
    COUNT(*) as count,
    SUM(bet_amount_tokens) as total_volume
FROM gold_prediction_bets

UNION ALL

SELECT
    'Markets Settled' as metric,
    COUNT(*) as count,
    NULL as total_volume
FROM gold_prediction_settlements

UNION ALL

SELECT
    'Active Markets' as metric,
    COUNT(DISTINCT market_id) as count,
    NULL as total_volume
FROM gold_prediction_bets;
