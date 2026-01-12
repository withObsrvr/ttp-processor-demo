-- Call Graph Example Queries
-- For Freighter wallet integration and cross-contract call analysis
-- Date: 2026-01-04

-- ============================================================================
-- BASIC QUERIES
-- ============================================================================

-- 1. Find all operations with cross-contract calls
SELECT
    ledger_sequence,
    transaction_hash,
    operation_index,
    soroban_contract_id as main_contract,
    soroban_function as main_function,
    max_call_depth,
    array_length(contracts_involved, 1) as num_contracts,
    contracts_involved
FROM operations_row_v2
WHERE max_call_depth > 0
ORDER BY ledger_sequence DESC
LIMIT 100;

-- 2. Get full call graph for a specific transaction
SELECT
    transaction_hash,
    operation_index,
    soroban_contract_id,
    soroban_function,
    contract_calls_json,
    contracts_involved,
    max_call_depth
FROM operations_row_v2
WHERE transaction_hash = 'YOUR_TX_HASH_HERE'
  AND contract_calls_json IS NOT NULL;

-- 3. Find all operations involving a specific contract (as caller or callee)
SELECT
    ledger_sequence,
    transaction_hash,
    operation_index,
    soroban_contract_id,
    soroban_function,
    contracts_involved,
    max_call_depth
FROM operations_row_v2
WHERE 'CCONTRACT_ID_HERE' = ANY(contracts_involved)
ORDER BY ledger_sequence DESC
LIMIT 50;

-- ============================================================================
-- FREIGHTER USE CASE: "Contracts Involved" Display
-- ============================================================================

-- 4. Get contracts involved for Freighter display (single transaction)
-- Returns data formatted for Freighter's "Contracts Involved" section
SELECT
    transaction_hash,
    soroban_contract_id as primary_contract,
    soroban_function as primary_function,
    contracts_involved,
    max_call_depth,
    CASE
        WHEN max_call_depth = 0 THEN 'Direct call'
        WHEN max_call_depth = 1 THEN 'Single nested call'
        ELSE format('%s levels of nesting', max_call_depth)
    END as call_complexity
FROM operations_row_v2
WHERE transaction_hash = 'YOUR_TX_HASH_HERE'
  AND soroban_contract_id IS NOT NULL;

-- 5. Parse call graph JSON to show call hierarchy
-- Returns individual calls from the JSON array
SELECT
    transaction_hash,
    ledger_sequence,
    call->>'from_contract' as from_contract,
    call->>'to_contract' as to_contract,
    call->>'function' as function_name,
    (call->>'call_depth')::int as call_depth,
    (call->>'execution_order')::int as execution_order,
    (call->>'successful')::boolean as successful
FROM operations_row_v2,
     jsonb_array_elements(contract_calls_json) as call
WHERE transaction_hash = 'YOUR_TX_HASH_HERE'
ORDER BY (call->>'execution_order')::int;

-- ============================================================================
-- ANALYTICS QUERIES
-- ============================================================================

-- 6. Most active cross-contract callers (contracts that call other contracts)
SELECT
    call->>'from_contract' as caller_contract,
    COUNT(*) as call_count,
    COUNT(DISTINCT call->>'to_contract') as unique_callees,
    array_agg(DISTINCT call->>'function') as functions_called
FROM operations_row_v2,
     jsonb_array_elements(contract_calls_json) as call
WHERE contract_calls_json IS NOT NULL
  AND created_at > NOW() - INTERVAL '7 days'
GROUP BY call->>'from_contract'
ORDER BY call_count DESC
LIMIT 20;

-- 7. Most called contracts (contracts that receive calls from others)
SELECT
    call->>'to_contract' as callee_contract,
    COUNT(*) as times_called,
    COUNT(DISTINCT call->>'from_contract') as unique_callers,
    array_agg(DISTINCT call->>'function') as functions_invoked
FROM operations_row_v2,
     jsonb_array_elements(contract_calls_json) as call
WHERE contract_calls_json IS NOT NULL
  AND created_at > NOW() - INTERVAL '7 days'
GROUP BY call->>'to_contract'
ORDER BY times_called DESC
LIMIT 20;

-- 8. Call depth distribution (how deep are contract call chains?)
SELECT
    max_call_depth,
    COUNT(*) as operation_count,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) as percentage
FROM operations_row_v2
WHERE max_call_depth IS NOT NULL
  AND max_call_depth > 0
  AND created_at > NOW() - INTERVAL '7 days'
GROUP BY max_call_depth
ORDER BY max_call_depth;

-- 9. Most common call patterns (from -> to -> function)
SELECT
    call->>'from_contract' as from_contract,
    call->>'to_contract' as to_contract,
    call->>'function' as function_name,
    COUNT(*) as occurrence_count
FROM operations_row_v2,
     jsonb_array_elements(contract_calls_json) as call
WHERE contract_calls_json IS NOT NULL
  AND created_at > NOW() - INTERVAL '7 days'
GROUP BY
    call->>'from_contract',
    call->>'to_contract',
    call->>'function'
ORDER BY occurrence_count DESC
LIMIT 50;

-- 10. Contracts with most complex call graphs (by number of contracts involved)
SELECT
    soroban_contract_id as main_contract,
    soroban_function,
    array_length(contracts_involved, 1) as contracts_count,
    max_call_depth,
    transaction_hash,
    ledger_sequence
FROM operations_row_v2
WHERE contracts_involved IS NOT NULL
  AND array_length(contracts_involved, 1) > 2
ORDER BY array_length(contracts_involved, 1) DESC, max_call_depth DESC
LIMIT 50;

-- ============================================================================
-- SOROSWAP/DEX ANALYSIS
-- ============================================================================

-- 11. Find Soroswap-style aggregator patterns
-- (operations where a router calls multiple pools/tokens)
SELECT
    ledger_sequence,
    transaction_hash,
    soroban_contract_id as router_contract,
    soroban_function,
    contracts_involved,
    max_call_depth,
    jsonb_array_length(contract_calls_json) as total_calls
FROM operations_row_v2
WHERE soroban_function IN ('swap', 'swap_exact_tokens_for_tokens', 'swap_tokens_for_exact_tokens')
  AND max_call_depth >= 2
  AND array_length(contracts_involved, 1) >= 3
ORDER BY ledger_sequence DESC
LIMIT 100;

-- 12. Token transfer patterns in DeFi operations
SELECT
    transaction_hash,
    ledger_sequence,
    call->>'from_contract' as caller,
    call->>'to_contract' as token_contract,
    (call->>'call_depth')::int as depth
FROM operations_row_v2,
     jsonb_array_elements(contract_calls_json) as call
WHERE call->>'function' = 'transfer'
  AND created_at > NOW() - INTERVAL '24 hours'
ORDER BY ledger_sequence DESC
LIMIT 100;

-- ============================================================================
-- DEBUGGING/MONITORING
-- ============================================================================

-- 13. Failed cross-contract calls
SELECT
    transaction_hash,
    ledger_sequence,
    call->>'from_contract' as from_contract,
    call->>'to_contract' as to_contract,
    call->>'function' as function_name,
    (call->>'call_depth')::int as call_depth
FROM operations_row_v2,
     jsonb_array_elements(contract_calls_json) as call
WHERE (call->>'successful')::boolean = false
ORDER BY ledger_sequence DESC
LIMIT 50;

-- 14. Call graph extraction stats (for monitoring)
SELECT
    DATE_TRUNC('hour', created_at) as hour,
    COUNT(*) as total_invoke_ops,
    COUNT(CASE WHEN contract_calls_json IS NOT NULL THEN 1 END) as with_call_graph,
    COUNT(CASE WHEN max_call_depth > 0 THEN 1 END) as with_cross_calls,
    AVG(max_call_depth) FILTER (WHERE max_call_depth > 0) as avg_call_depth,
    MAX(max_call_depth) as max_call_depth_seen
FROM operations_row_v2
WHERE type = 24  -- InvokeHostFunction
  AND created_at > NOW() - INTERVAL '24 hours'
GROUP BY DATE_TRUNC('hour', created_at)
ORDER BY hour DESC;

-- ============================================================================
-- SILVER LAYER VIEWS (for future Silver transformation)
-- ============================================================================

-- 15. Create a view for flattened contract calls (Silver layer candidate)
-- This would be materialized in the Silver layer
CREATE OR REPLACE VIEW v_contract_calls_flattened AS
SELECT
    o.ledger_sequence,
    o.transaction_hash,
    o.operation_index,
    o.created_at as closed_at,
    o.soroban_contract_id as root_contract,
    o.soroban_function as root_function,
    call->>'from_contract' as from_contract,
    call->>'to_contract' as to_contract,
    call->>'function' as function_name,
    (call->>'call_depth')::int as call_depth,
    (call->>'execution_order')::int as execution_order,
    (call->>'successful')::boolean as successful,
    o.ledger_range
FROM operations_row_v2 o,
     jsonb_array_elements(o.contract_calls_json) as call
WHERE o.contract_calls_json IS NOT NULL;

-- 16. Create a view for contract relationships (Silver layer candidate)
CREATE OR REPLACE VIEW v_contract_relationships AS
SELECT DISTINCT
    call->>'from_contract' as caller_contract,
    call->>'to_contract' as callee_contract,
    call->>'function' as function_name,
    COUNT(*) as call_count,
    MIN(o.ledger_sequence) as first_seen_ledger,
    MAX(o.ledger_sequence) as last_seen_ledger
FROM operations_row_v2 o,
     jsonb_array_elements(o.contract_calls_json) as call
WHERE o.contract_calls_json IS NOT NULL
GROUP BY
    call->>'from_contract',
    call->>'to_contract',
    call->>'function';
