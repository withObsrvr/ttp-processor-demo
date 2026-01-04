# Call Graph Feature - Cross-Contract Call Tracking

**Date:** 2026-01-04
**Status:** Cycle 1 Complete (Bronze Extraction + Schema)
**Priority:** Freighter wallet "Contracts Involved" display

---

## Overview

This feature enables tracking of cross-contract call hierarchies in Stellar Soroban transactions. When a DeFi aggregator like Soroswap executes a swap, it may call multiple contracts in a nested hierarchy:

```
Soroswap Router (swap)
  └─ Aquarius Pool (do_swap)
      ├─ USDC Token (transfer)
      └─ XLM Token (transfer)
```

The call graph feature captures this hierarchy at Bronze layer ingestion time, enabling Freighter to display "Contracts Involved" for user transactions.

---

## Schema Changes

### New Columns in `operations_row_v2`

```sql
-- Call Graph (3 columns)
contract_calls_json JSONB,    -- Array of {from, to, function, depth, order}
contracts_involved TEXT[],    -- All contracts in the call chain
max_call_depth INT            -- Maximum depth of nested calls (0 = no sub-calls)
```

### Indexes

```sql
-- GIN index for array containment queries
CREATE INDEX idx_operations_contracts_involved
ON operations_row_v2 USING GIN (contracts_involved)
WHERE contracts_involved IS NOT NULL;

-- GIN index for JSONB queries
CREATE INDEX idx_operations_contract_calls
ON operations_row_v2 USING GIN (contract_calls_json)
WHERE contract_calls_json IS NOT NULL;

-- B-tree index for filtering by call depth
CREATE INDEX idx_operations_call_depth
ON operations_row_v2 (max_call_depth)
WHERE max_call_depth IS NOT NULL AND max_call_depth > 0;
```

---

## Data Format

### contract_calls_json

JSON array of call relationships:

```json
[
  {
    "from_contract": "CSWAP123...",
    "to_contract": "CPOOL456...",
    "function": "do_swap",
    "call_depth": 1,
    "execution_order": 0,
    "successful": true
  },
  {
    "from_contract": "CPOOL456...",
    "to_contract": "CUSDC789...",
    "function": "transfer",
    "call_depth": 2,
    "execution_order": 1,
    "successful": true
  }
]
```

### contracts_involved

Array of all contract IDs in the call chain:

```sql
'{CSWAP123..., CPOOL456..., CUSDC789..., CXLM012...}'
```

### max_call_depth

- `0` or `NULL`: No cross-contract calls (direct contract invocation only)
- `1`: Single level of nesting (contract calls one other contract)
- `2+`: Deep nesting (contract calls contract that calls contract...)

---

## How It Works

### Extraction Sources

The call graph is extracted from two sources:

1. **Diagnostic Events** (fn_call/fn_return patterns)
   - Soroban emits diagnostic events for function calls and returns
   - Tracks call stack by monitoring fn_call → fn_return pairs
   - Captures actual execution order and nesting depth

2. **Authorization Sub-Invocations**
   - SorobanAuthorizationEntry contains authorized sub-invocations
   - Represents the permission structure of the call
   - Complements diagnostic events with auth context

### Integration Point

Call graph extraction is integrated into `extractOperations()` in `extractors.go`:

```go
// For InvokeHostFunction operations
if op.Body.Type == xdr.OperationTypeInvokeHostFunction {
    // Extract basic contract invocation details
    contractID, functionName, argsJSON, _ := extractContractInvocationDetails(op)

    // Extract call graph for cross-contract tracking
    integrateCallGraph(tx, i, op, &opData)
}
```

---

## Example Queries

### Freighter: Get Contracts Involved

```sql
SELECT
    transaction_hash,
    soroban_contract_id as primary_contract,
    soroban_function as primary_function,
    contracts_involved,
    max_call_depth
FROM operations_row_v2
WHERE transaction_hash = 'abc123...'
  AND soroban_contract_id IS NOT NULL;
```

### Find All Operations Involving a Contract

```sql
SELECT *
FROM operations_row_v2
WHERE 'CCONTRACT_ID' = ANY(contracts_involved)
ORDER BY ledger_sequence DESC
LIMIT 50;
```

### Parse Call Hierarchy

```sql
SELECT
    call->>'from_contract' as from_contract,
    call->>'to_contract' as to_contract,
    call->>'function' as function_name,
    (call->>'call_depth')::int as call_depth
FROM operations_row_v2,
     jsonb_array_elements(contract_calls_json) as call
WHERE transaction_hash = 'abc123...'
ORDER BY (call->>'execution_order')::int;
```

See `stellar-postgres-ingester/schema/call_graph_queries.sql` for more examples.

---

## Files

### New Files

| File | Description |
|------|-------------|
| `stellar-postgres-ingester/go/call_graph.go` | Call graph extraction logic |
| `stellar-postgres-ingester/go/call_graph_test.go` | Unit tests |
| `stellar-postgres-ingester/migrations/002_add_call_graph_fields.sql` | Schema migration |
| `stellar-postgres-ingester/schema/call_graph_queries.sql` | Example queries |
| `CALL_GRAPH_FEATURE.md` | This documentation |

### Modified Files

| File | Changes |
|------|---------|
| `stellar-postgres-ingester/go/types.go` | Added ContractCall, CallGraphResult types |
| `stellar-postgres-ingester/go/extractors.go` | Integrated call graph extraction |
| `stellar-postgres-ingester/go/writer.go` | Added new columns to INSERT |
| `v3_postgres_schema.sql` | Added new columns |

---

## API Endpoints (Planned - Cycle 3)

```
GET /api/v1/silver/tx/{hash}/contracts-involved
→ { "contracts_involved": ["CXXX...", "CYYY...", "CZZZ..."], "count": 3 }

GET /api/v1/silver/tx/{hash}/call-graph
→ { "calls": [...], "max_depth": 2 }

GET /api/v1/silver/contracts/{id}/callers
→ { "callers": [{ "contract_id": "CXXX", "call_count": 100 }] }
```

---

## Testing

### Unit Tests

```bash
cd stellar-postgres-ingester/go
GOWORK=off go test -v ./...
```

### Integration Testing

1. Run the ingester against testnet
2. Find Soroswap or other DEX transactions
3. Query for call graph data
4. Verify contracts_involved matches expected hierarchy

---

## Migration

For existing databases, run:

```bash
psql -d your_database -f stellar-postgres-ingester/migrations/002_add_call_graph_fields.sql
```

This migration is safe to run multiple times (uses `IF NOT EXISTS`).

---

## Limitations

1. **Historical Data**: Call graphs are only extracted for new ledgers going forward. Historical backfill is out of scope for this cycle.

2. **Function Name Inference**: For some diagnostic events, function names are inferred from patterns rather than explicitly extracted.

3. **Failed Transactions**: Call graphs for failed transactions may be incomplete if the failure occurred mid-execution.

---

## Next Steps (Cycle 2 & 3)

### Cycle 2: Silver Transformation (Weeks 3-4)
- Create `contract_invocation_calls` table
- Create `contract_invocation_hierarchy` table
- Implement Silver layer transformer

### Cycle 3: API Layer (Weeks 5-6)
- REST API endpoints
- Freighter-optimized response format
- Documentation and handoff
