-- Test Contract Invocation Extraction & Transformation
-- Run these queries after deployment to verify data flow

-- ============================================================================
-- BRONZE LAYER TESTS (PostgreSQL Hot Buffer)
-- ============================================================================

-- Test 1: Verify new columns exist in Bronze
SELECT column_name, data_type, is_nullable
FROM information_schema.columns
WHERE table_name = 'operations_row_v2'
  AND column_name IN ('transaction_index', 'soroban_arguments_json')
ORDER BY column_name;

-- Expected: 2 rows showing both columns exist

-- Test 2: Check if Bronze has contract invocations with new fields populated
SELECT
    ledger_sequence,
    transaction_index,
    operation_index,
    transaction_hash,
    type,
    soroban_contract_id,
    soroban_function,
    LENGTH(soroban_arguments_json) as args_json_length,
    transaction_successful
FROM operations_row_v2
WHERE type = 24  -- InvokeHostFunction
  AND soroban_contract_id IS NOT NULL
  AND soroban_function IS NOT NULL
  AND soroban_arguments_json IS NOT NULL
ORDER BY ledger_sequence DESC
LIMIT 10;

-- Expected: Recent contract invocations with populated arguments_json

-- Test 3: Count total contract invocations with complete data
SELECT
    COUNT(*) as total_invocations,
    COUNT(DISTINCT soroban_contract_id) as unique_contracts,
    COUNT(DISTINCT soroban_function) as unique_functions,
    MIN(ledger_sequence) as earliest_ledger,
    MAX(ledger_sequence) as latest_ledger
FROM operations_row_v2
WHERE type = 24
  AND soroban_contract_id IS NOT NULL
  AND soroban_function IS NOT NULL
  AND soroban_arguments_json IS NOT NULL;

-- Expected: Counts of contract invocations

-- Test 4: Inspect a sample contract invocation argument structure
SELECT
    ledger_sequence,
    transaction_hash,
    soroban_contract_id,
    soroban_function,
    soroban_arguments_json,
    transaction_successful
FROM operations_row_v2
WHERE type = 24
  AND soroban_contract_id IS NOT NULL
  AND soroban_function IS NOT NULL
  AND soroban_arguments_json IS NOT NULL
ORDER BY ledger_sequence DESC
LIMIT 1;

-- Expected: Full contract invocation record with JSON arguments

-- Test 5: Verify TOID components are present
SELECT
    ledger_sequence,
    transaction_index,
    operation_index,
    -- Compute TOID (requires bit shifting - PostgreSQL syntax)
    (ledger_sequence::BIGINT << 32) | (transaction_index::BIGINT << 12) | operation_index::BIGINT as toid,
    transaction_hash,
    soroban_contract_id,
    soroban_function
FROM operations_row_v2
WHERE type = 24
  AND transaction_index IS NOT NULL
  AND soroban_contract_id IS NOT NULL
ORDER BY ledger_sequence DESC
LIMIT 5;

-- Expected: Contract invocations with computed TOID values

-- ============================================================================
-- SILVER LAYER TESTS (PostgreSQL Silver Hot)
-- ============================================================================
-- Run these queries against the silver_hot database

-- Test 6: Verify contract_invocations_raw table exists
SELECT
    table_name,
    (SELECT COUNT(*) FROM information_schema.columns WHERE table_name = 'contract_invocations_raw') as column_count
FROM information_schema.tables
WHERE table_name = 'contract_invocations_raw';

-- Expected: 1 row showing table exists with 12 columns

-- Test 7: Check Silver contract invocations count
SELECT
    COUNT(*) as total_invocations,
    COUNT(DISTINCT contract_id) as unique_contracts,
    COUNT(DISTINCT function_name) as unique_functions,
    MIN(ledger_sequence) as earliest_ledger,
    MAX(ledger_sequence) as latest_ledger,
    MIN(closed_at) as earliest_time,
    MAX(closed_at) as latest_time
FROM contract_invocations_raw;

-- Expected: Counts matching or close to Bronze (depending on transformer progress)

-- Test 8: Sample recent contract invocations from Silver
SELECT
    ledger_sequence,
    transaction_index,
    operation_index,
    (ledger_sequence::BIGINT << 32) | (transaction_index::BIGINT << 12) | operation_index::BIGINT as toid,
    transaction_hash,
    contract_id,
    function_name,
    LENGTH(arguments_json) as args_length,
    successful,
    closed_at
FROM contract_invocations_raw
ORDER BY ledger_sequence DESC, transaction_index DESC, operation_index DESC
LIMIT 10;

-- Expected: Recent contract invocations with TOID

-- Test 9: Verify specific contract invocations (filter by contract_id)
SELECT
    ledger_sequence,
    function_name,
    arguments_json,
    successful,
    closed_at
FROM contract_invocations_raw
WHERE contract_id = 'YOUR_CONTRACT_ID_HERE'  -- Replace with actual contract ID
ORDER BY ledger_sequence DESC
LIMIT 10;

-- Expected: Invocations for specific contract

-- Test 10: Check transformation latency (Bronze vs Silver)
SELECT
    'Bronze' as layer,
    MAX(ledger_sequence) as max_ledger
FROM operations_row_v2
WHERE type = 24 AND soroban_contract_id IS NOT NULL

UNION ALL

SELECT
    'Silver' as layer,
    MAX(ledger_sequence) as max_ledger
FROM contract_invocations_raw;

-- Expected: Silver should be close to Bronze (< 10 ledgers behind)

-- ============================================================================
-- ARGUMENT PARSING TEST
-- ============================================================================

-- Test 11: Parse JSON arguments (example for Stellarcarbon)
SELECT
    ledger_sequence,
    contract_id,
    function_name,
    arguments_json,
    -- Extract first 3 arguments (adjust for your contract)
    arguments_json::jsonb->0 as arg0,
    arguments_json::jsonb->1 as arg1,
    arguments_json::jsonb->2 as arg2,
    successful
FROM contract_invocations_raw
WHERE function_name = 'transfer'  -- Or your function name
  AND jsonb_array_length(arguments_json::jsonb) >= 3
ORDER BY ledger_sequence DESC
LIMIT 5;

-- Expected: Parsed arguments showing ScVal structure

-- ============================================================================
-- PERFORMANCE TEST
-- ============================================================================

-- Test 12: Check index usage on contract_id
EXPLAIN ANALYZE
SELECT COUNT(*)
FROM contract_invocations_raw
WHERE contract_id = 'YOUR_CONTRACT_ID_HERE';

-- Expected: Should use idx_contract_invocations_contract_id (Index Scan)

-- Test 13: Check index usage on function_name
EXPLAIN ANALYZE
SELECT COUNT(*)
FROM contract_invocations_raw
WHERE function_name = 'transfer';

-- Expected: Should use idx_contract_invocations_function (Index Scan)
