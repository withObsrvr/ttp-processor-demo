-- ============================================================================
-- GOLD LAYER: Complete Betting Contract Analytics
-- ============================================================================
-- Contract: CDRCPTMQHCUX5WDQRDKEG5YQIFO43AWIVDMWL6RA3Y3ODLTTLXYK6CYS
-- Purpose: Track complete betting lifecycle (bets placed + market settlements)
-- Date: 2026-01-03
-- ============================================================================

-- ============================================================================
-- VIEW 1: All Bets Placed
-- ============================================================================
CREATE OR REPLACE VIEW gold_betting_bets_placed AS
SELECT
    -- TOID for unique operation identification
    (ledger_sequence::BIGINT << 32) | (transaction_index::BIGINT << 12) | operation_index::BIGINT as toid,

    -- Ledger context
    ledger_sequence,
    transaction_hash,
    closed_at,
    successful,

    -- Contract context
    contract_id as betting_contract,

    -- Bet details extracted from arguments
    -- arg[0]: player address
    arguments_json::jsonb->0->>'address' as player_address,

    -- arg[1]: number (u32)
    (arguments_json::jsonb->1)::INTEGER as bet_number,

    -- arg[2]: color (String)
    TRIM(BOTH '"' FROM (arguments_json::jsonb->2)::TEXT) as bet_color,

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
CREATE OR REPLACE VIEW gold_betting_settlements AS
SELECT
    -- TOID for unique operation identification
    (ledger_sequence::BIGINT << 32) | (transaction_index::BIGINT << 12) | operation_index::BIGINT as toid,

    -- Ledger context
    ledger_sequence,
    transaction_hash,
    closed_at,
    successful,

    -- Contract context
    contract_id as betting_contract,

    -- Settlement details extracted from arguments
    -- arg[0]: winning number (u32)
    (arguments_json::jsonb->0)::INTEGER as winning_number,

    -- arg[1]: outcome (String)
    TRIM(BOTH '"' FROM (arguments_json::jsonb->1)::TEXT) as outcome,

    -- Metadata
    source_account as settlement_source,
    inserted_at as indexed_at

FROM contract_invocations_raw
WHERE
    contract_id = 'CDRCPTMQHCUX5WDQRDKEG5YQIFO43AWIVDMWL6RA3Y3ODLTTLXYK6CYS'
    AND function_name = 'settle_market'
    AND successful = true
ORDER BY ledger_sequence DESC, transaction_index DESC, operation_index DESC;

-- ============================================================================
-- VIEW 3: Complete Betting Lifecycle (Bets + Outcomes)
-- ============================================================================
CREATE OR REPLACE VIEW gold_betting_lifecycle AS
SELECT
    b.toid as bet_toid,
    b.ledger_sequence as bet_ledger,
    b.closed_at as bet_time,
    b.player_address,
    b.bet_number,
    b.bet_color,
    b.bet_amount_tokens,

    s.toid as settlement_toid,
    s.ledger_sequence as settlement_ledger,
    s.closed_at as settlement_time,
    s.winning_number,
    s.outcome,

    -- Calculate if bet won
    CASE
        WHEN b.bet_number = s.winning_number THEN 'WON'
        ELSE 'LOST'
    END as bet_result,

    -- Time between bet and settlement
    EXTRACT(EPOCH FROM (s.closed_at - b.closed_at)) as seconds_to_settlement,

    -- Metadata
    b.betting_contract
FROM
    gold_betting_bets_placed b
    LEFT JOIN gold_betting_settlements s
        ON s.ledger_sequence >= b.ledger_sequence  -- Settlement happens after bet
        AND s.ledger_sequence - b.ledger_sequence < 100  -- Within reasonable window
ORDER BY b.ledger_sequence DESC;

-- ============================================================================
-- EXAMPLE QUERIES
-- ============================================================================

-- Query 1: Show the complete betting lifecycle
-- SELECT * FROM gold_betting_lifecycle;

-- Query 2: Player statistics
-- SELECT
--     player_address,
--     COUNT(*) as total_bets,
--     SUM(bet_amount_tokens) as total_wagered,
--     SUM(CASE WHEN bet_result = 'WON' THEN 1 ELSE 0 END) as wins,
--     SUM(CASE WHEN bet_result = 'LOST' THEN 1 ELSE 0 END) as losses,
--     ROUND(100.0 * SUM(CASE WHEN bet_result = 'WON' THEN 1 ELSE 0 END) / COUNT(*), 2) as win_percentage
-- FROM gold_betting_lifecycle
-- GROUP BY player_address;

-- Query 3: Market settlement history
-- SELECT
--     settlement_ledger,
--     settlement_time,
--     winning_number,
--     outcome,
--     COUNT(*) as bets_on_this_market,
--     SUM(bet_amount_tokens) as total_market_volume
-- FROM gold_betting_lifecycle
-- GROUP BY settlement_ledger, settlement_time, winning_number, outcome
-- ORDER BY settlement_time DESC;

-- Query 4: Number betting patterns
-- SELECT
--     bet_number,
--     COUNT(*) as times_bet,
--     SUM(bet_amount_tokens) as total_on_number,
--     AVG(bet_amount_tokens) as avg_bet_size
-- FROM gold_betting_bets_placed
-- GROUP BY bet_number
-- ORDER BY times_bet DESC;

-- ============================================================================
-- VERIFICATION
-- ============================================================================
-- Check data is flowing
SELECT
    'Bets Placed' as view_name,
    COUNT(*) as count,
    MAX(ledger_sequence) as latest_ledger
FROM gold_betting_bets_placed

UNION ALL

SELECT
    'Settlements' as view_name,
    COUNT(*) as count,
    MAX(ledger_sequence) as latest_ledger
FROM gold_betting_settlements

UNION ALL

SELECT
    'Complete Lifecycle' as view_name,
    COUNT(*) as count,
    MAX(bet_ledger) as latest_ledger
FROM gold_betting_lifecycle;
