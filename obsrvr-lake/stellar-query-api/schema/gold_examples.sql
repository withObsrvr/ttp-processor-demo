-- ============================================================================
-- GOLD LAYER EXAMPLES: Contract Invocation Views
-- ============================================================================
-- Purpose: Demonstrate Gold layer patterns for extracting business logic
--          from contract invocations using SQL views
-- Date: 2026-01-03
--
-- Pattern: Silver table → Gold view (tenant-specific business logic)
-- ============================================================================

-- ============================================================================
-- EXAMPLE 1: Token Transfers (Generic SAC Pattern)
-- ============================================================================
-- Contract: CBIELTK6YBZJU5UP2WWQEUCYKLPU6AUNZ2BQ4WWFEIE3USCIHMXQDAMA
-- Function: transfer(from: Address, to: Address, amount: i128)
-- Use Case: Track token movements for analytics, compliance, volume tracking
-- ============================================================================

CREATE OR REPLACE VIEW gold_token_transfers AS
SELECT
    -- TOID for unique operation identification
    (ledger_sequence::BIGINT << 32) | (transaction_index::BIGINT << 12) | operation_index::BIGINT as toid,

    -- Ledger context
    ledger_sequence,
    transaction_hash,
    closed_at,
    successful,

    -- Contract context
    contract_id as token_contract,

    -- Business fields extracted from arguments_json
    -- arg[0]: from address
    arguments_json::jsonb->0->>'address' as from_address,
    (arguments_json::jsonb->0->>'type') as from_type,  -- 'account' or 'contract'

    -- arg[1]: to address
    arguments_json::jsonb->1->>'address' as to_address,
    (arguments_json::jsonb->1->>'type') as to_type,

    -- arg[2]: amount (i128 value)
    (arguments_json::jsonb->2->>'value')::BIGINT as amount_stroops,
    ROUND((arguments_json::jsonb->2->>'value')::BIGINT / 10000000.0, 7) as amount_tokens,

    -- Metadata
    source_account as transaction_source,
    inserted_at as indexed_at

FROM contract_invocations_raw
WHERE
    function_name = 'transfer'
    AND successful = true
    -- Filter to specific token contracts if needed:
    -- AND contract_id = 'CBIELTK6YBZJU5UP2WWQEUCYKLPU6AUNZ2BQ4WWFEIE3USCIHMXQDAMA'
ORDER BY ledger_sequence DESC, transaction_index DESC, operation_index DESC;

-- Example query: Get total transfer volume by contract
-- SELECT
--     token_contract,
--     COUNT(*) as transfer_count,
--     SUM(amount_tokens) as total_volume,
--     COUNT(DISTINCT from_address) as unique_senders,
--     COUNT(DISTINCT to_address) as unique_receivers
-- FROM gold_token_transfers
-- WHERE closed_at > NOW() - INTERVAL '24 hours'
-- GROUP BY token_contract
-- ORDER BY total_volume DESC;


-- ============================================================================
-- EXAMPLE 2: Betting Activity (Complex Multi-Type Arguments)
-- ============================================================================
-- Contract: CDRCPTMQHCUX5WDQRDKEG5YQIFO43AWIVDMWL6RA3Y3ODLTTLXYK6CYS
-- Function: place_bet(player: Address, number: u32, color: String, amount: i128)
-- Use Case: Track betting patterns, player activity, risk analysis
-- ============================================================================

CREATE OR REPLACE VIEW gold_betting_activity AS
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

    -- Business fields extracted from arguments_json
    -- arg[0]: player address
    arguments_json::jsonb->0->>'address' as player_address,

    -- arg[1]: number (u32)
    (arguments_json::jsonb->1)::INTEGER as bet_number,

    -- arg[2]: color (String)
    arguments_json::jsonb->2::TEXT as bet_color,

    -- arg[3]: amount (i128)
    (arguments_json::jsonb->3->>'value')::BIGINT as bet_amount_stroops,
    ROUND((arguments_json::jsonb->3->>'value')::BIGINT / 10000000.0, 7) as bet_amount_tokens,

    -- Metadata
    source_account as transaction_source,
    inserted_at as indexed_at

FROM contract_invocations_raw
WHERE
    function_name = 'place_bet'
    AND successful = true
ORDER BY ledger_sequence DESC, transaction_index DESC, operation_index DESC;

-- Example query: Player betting patterns
-- SELECT
--     player_address,
--     COUNT(*) as total_bets,
--     SUM(bet_amount_tokens) as total_wagered,
--     AVG(bet_amount_tokens) as avg_bet_size,
--     array_agg(DISTINCT bet_color) as colors_played,
--     array_agg(DISTINCT bet_number) as numbers_played
-- FROM gold_betting_activity
-- WHERE closed_at > NOW() - INTERVAL '7 days'
-- GROUP BY player_address
-- ORDER BY total_wagered DESC
-- LIMIT 10;


-- ============================================================================
-- EXAMPLE 3: Price Oracle Updates (Array Arguments)
-- ============================================================================
-- Contract: CD4KFB23CQLJ47RIPESVBTQ444R75M6QVEZULWXHHFYYZT7SS2VGXOPW
-- Function: write_prices(prices: Vec<PriceData>)
-- Use Case: Track price updates, oracle data quality, update frequency
-- ============================================================================

CREATE OR REPLACE VIEW gold_price_oracle_updates AS
SELECT
    -- TOID for unique operation identification
    (ledger_sequence::BIGINT << 32) | (transaction_index::BIGINT << 12) | operation_index::BIGINT as toid,

    -- Ledger context
    ledger_sequence,
    transaction_hash,
    closed_at,
    successful,

    -- Contract context
    contract_id as oracle_contract,

    -- Price data metadata
    jsonb_array_length(arguments_json::jsonb->0) as price_count,

    -- Raw arguments for detailed analysis
    arguments_json::jsonb->0 as price_data_array,

    -- Metadata
    source_account as oracle_source,
    inserted_at as indexed_at

FROM contract_invocations_raw
WHERE
    function_name = 'write_prices'
    AND successful = true
ORDER BY ledger_sequence DESC, transaction_index DESC, operation_index DESC;

-- Example query: Oracle update frequency
-- SELECT
--     oracle_contract,
--     DATE_TRUNC('hour', closed_at) as hour,
--     COUNT(*) as update_count,
--     SUM(price_count) as total_prices_updated,
--     AVG(price_count) as avg_prices_per_update
-- FROM gold_price_oracle_updates
-- WHERE closed_at > NOW() - INTERVAL '24 hours'
-- GROUP BY oracle_contract, DATE_TRUNC('hour', closed_at)
-- ORDER BY hour DESC;


-- ============================================================================
-- GOLD LAYER PATTERN DOCUMENTATION
-- ============================================================================

/*
** GOLD LAYER PATTERN: Tenant-Specific Views **

1. PURPOSE
   - Transform Silver contract_invocations_raw into business-specific data
   - Extract and validate business fields from arguments_json
   - Enable SQL-based analytics without custom processors

2. STRUCTURE
   CREATE VIEW gold_<tenant>_<use_case> AS
   SELECT
     -- TOID (unique operation ID)
     (ledger_sequence::BIGINT << 32) | (transaction_index::BIGINT << 12) | operation_index::BIGINT as toid,

     -- Ledger context
     ledger_sequence, transaction_hash, closed_at, successful,

     -- Business fields (extract from arguments_json)
     arguments_json::jsonb->0->> ... as field1,
     arguments_json::jsonb->1->> ... as field2,

     -- Metadata
     source_account, inserted_at
   FROM contract_invocations_raw
   WHERE function_name = 'your_function'
     AND successful = true;

3. JSON PATH PATTERNS

   a) Address extraction:
      arguments_json::jsonb->0->>'address' as address_field

   b) i128/u64 numbers:
      (arguments_json::jsonb->0->>'value')::BIGINT as amount_stroops

   c) Strings:
      arguments_json::jsonb->0::TEXT as string_field

   d) Regular numbers (u32, i32):
      (arguments_json::jsonb->0)::INTEGER as number_field

   e) Arrays (get length):
      jsonb_array_length(arguments_json::jsonb->0) as array_length

   f) Nested structures:
      arguments_json::jsonb->0->'field'->>'subfield'

4. VALIDATION PATTERNS

   a) Address validation (Stellar G... addresses):
      WHERE arguments_json::jsonb->0->>'address' ~ '^G[A-Z2-7]{55}$'

   b) Amount > 0:
      WHERE (arguments_json::jsonb->2->>'value')::BIGINT > 0

   c) Successful only:
      WHERE successful = true

   d) Date range:
      WHERE closed_at BETWEEN '2026-01-01' AND '2026-02-01'

5. STELLARCARBON PATTERN (Template)

   CREATE VIEW gold_stellarcarbon_carbon_transfers AS
   SELECT
     (ledger_sequence::BIGINT << 32) | (transaction_index::BIGINT << 12) | operation_index::BIGINT as toid,
     ledger_sequence, transaction_hash, closed_at, successful,

     -- arg[0]: funder
     arguments_json::jsonb->0->>'address' as funder,

     -- arg[1]: recipient
     arguments_json::jsonb->1->>'address' as recipient,

     -- arg[2]: amount
     (arguments_json::jsonb->2->>'value')::BIGINT as carbon_credits,

     -- arg[3]: project_id
     arguments_json::jsonb->3::TEXT as project_id,

     -- arg[4]: memo
     arguments_json::jsonb->4::TEXT as memo,

     -- arg[5]: email
     arguments_json::jsonb->5::TEXT as contact_email,

     source_account, inserted_at
   FROM contract_invocations_raw
   WHERE contract_id = 'STELLARCARBON_CONTRACT_ADDRESS'
     AND function_name = 'transfer_carbon_credits'  -- Or actual function name
     AND successful = true
     AND arguments_json::jsonb->0->>'address' ~ '^G[A-Z2-7]{55}$'  -- Valid funder
     AND arguments_json::jsonb->1->>'address' ~ '^G[A-Z2-7]{55}$'  -- Valid recipient
     AND (arguments_json::jsonb->2->>'value')::BIGINT > 0;  -- Amount > 0

6. TESTING PATTERN

   -- Step 1: Find your contract
   SELECT DISTINCT contract_id, function_name, COUNT(*)
   FROM contract_invocations_raw
   GROUP BY contract_id, function_name;

   -- Step 2: Examine arguments structure
   SELECT arguments_json
   FROM contract_invocations_raw
   WHERE contract_id = 'YOUR_CONTRACT'
     AND function_name = 'your_function'
   LIMIT 1;

   -- Step 3: Build extraction query
   SELECT
     arguments_json::jsonb->0 as arg0,
     arguments_json::jsonb->1 as arg1,
     arguments_json::jsonb->2 as arg2
   FROM contract_invocations_raw
   WHERE contract_id = 'YOUR_CONTRACT'
   LIMIT 5;

   -- Step 4: Create view with proper types
   -- (Use patterns from section 3 above)

7. PERFORMANCE NOTES

   - Views are computed on read (no materialization)
   - For high-volume contracts, consider materialized views:
     CREATE MATERIALIZED VIEW gold_high_volume AS ...;
     REFRESH MATERIALIZED VIEW gold_high_volume;

   - Add indexes on contract_invocations_raw for common filters:
     CREATE INDEX idx_contract_function ON contract_invocations_raw(contract_id, function_name);
     CREATE INDEX idx_closed_at ON contract_invocations_raw(closed_at DESC);

8. EXPORT PATTERNS

   a) CSV export:
      COPY (SELECT * FROM gold_token_transfers WHERE ...)
      TO '/tmp/transfers.csv' CSV HEADER;

   b) JSON export:
      SELECT json_agg(row_to_json(t))
      FROM (SELECT * FROM gold_token_transfers WHERE ...) t;

   c) Parquet export (via DuckLake):
      -- Export to S3 cold storage for analytics

*/


-- ============================================================================
-- VERIFICATION QUERIES
-- ============================================================================

-- Check all Gold views are working
SELECT
    'Token Transfers' as view_name,
    COUNT(*) as row_count,
    MAX(ledger_sequence) as latest_ledger
FROM gold_token_transfers

UNION ALL

SELECT
    'Betting Activity' as view_name,
    COUNT(*) as row_count,
    MAX(ledger_sequence) as latest_ledger
FROM gold_betting_activity

UNION ALL

SELECT
    'Price Oracle Updates' as view_name,
    COUNT(*) as row_count,
    MAX(ledger_sequence) as latest_ledger
FROM gold_price_oracle_updates;


-- ============================================================================
-- CLEANUP (Optional)
-- ============================================================================

-- Drop views when no longer needed:
-- DROP VIEW IF EXISTS gold_token_transfers;
-- DROP VIEW IF EXISTS gold_betting_activity;
-- DROP VIEW IF EXISTS gold_price_oracle_updates;
