# Contract Calls API Documentation

API endpoints for querying cross-contract call graphs. Designed for Freighter wallet's "Contracts Involved" feature.

## Overview

These endpoints expose the contract call graph data extracted from Stellar's Soroban diagnostic events. When a DeFi aggregator like Soroswap executes a swap, it triggers a chain of cross-contract calls (Router → Pool → Token contracts). These endpoints allow querying this call hierarchy.

## Base URL

```
http://localhost:8080/api/v1/silver
```

## Transaction-Centric Endpoints

### Get Contracts Involved in Transaction

Returns all unique contracts that participated in a transaction.

```bash
GET /api/v1/silver/tx/{hash}/contracts-involved
```

**Example:**
```bash
curl "http://localhost:8080/api/v1/silver/tx/abc123def456/contracts-involved"
```

**Response:**
```json
{
  "transaction_hash": "abc123def456",
  "contracts_involved": [
    "CABCDEFG1234567890",
    "CXYZ987654321",
    "CTOKEN_USDC_123",
    "CTOKEN_XLM_456"
  ],
  "count": 4
}
```

---

### Get Transaction Call Graph

Returns the detailed call graph showing caller → callee relationships with function names.

```bash
GET /api/v1/silver/tx/{hash}/call-graph
```

**Example:**
```bash
curl "http://localhost:8080/api/v1/silver/tx/abc123def456/call-graph"
```

**Response:**
```json
{
  "transaction_hash": "abc123def456",
  "calls": [
    {
      "from": "CABCDEFG1234567890",
      "to": "CXYZ987654321",
      "function": "swap",
      "depth": 1,
      "order": 0,
      "successful": true
    },
    {
      "from": "CXYZ987654321",
      "to": "CTOKEN_USDC_123",
      "function": "transfer",
      "depth": 2,
      "order": 1,
      "successful": true
    },
    {
      "from": "CXYZ987654321",
      "to": "CTOKEN_XLM_456",
      "function": "transfer",
      "depth": 2,
      "order": 2,
      "successful": true
    }
  ],
  "count": 3
}
```

---

### Get Transaction Hierarchy

Returns pre-computed ancestry chains for efficient relationship queries.

```bash
GET /api/v1/silver/tx/{hash}/hierarchy
```

**Example:**
```bash
curl "http://localhost:8080/api/v1/silver/tx/abc123def456/hierarchy"
```

**Response:**
```json
{
  "transaction_hash": "abc123def456",
  "hierarchies": [
    {
      "root": "CABCDEFG1234567890",
      "child": "CXYZ987654321",
      "path": ["CABCDEFG1234567890", "CXYZ987654321"],
      "depth": 1
    },
    {
      "root": "CABCDEFG1234567890",
      "child": "CTOKEN_USDC_123",
      "path": ["CABCDEFG1234567890", "CXYZ987654321", "CTOKEN_USDC_123"],
      "depth": 2
    }
  ],
  "count": 2
}
```

---

## Contract-Centric Endpoints

### Get Recent Calls for Contract

Returns recent calls involving a specific contract (as caller or callee).

```bash
GET /api/v1/silver/contracts/{id}/recent-calls?limit=100
```

**Example:**
```bash
curl "http://localhost:8080/api/v1/silver/contracts/CSOROSWAP_ROUTER/recent-calls?limit=50"
```

**Response:**
```json
{
  "contract_id": "CSOROSWAP_ROUTER",
  "calls": [
    {
      "from": "CSOROSWAP_ROUTER",
      "to": "CAQUARIUS_POOL",
      "function": "swap",
      "depth": 1,
      "transaction_hash": "tx123",
      "ledger_sequence": 12345678,
      "successful": true,
      "closed_at": "2026-01-04T12:00:00Z"
    }
  ],
  "as_caller": 45,
  "as_callee": 10,
  "total": 55
}
```

---

### Get Contract Callers

Returns contracts that call a specific contract.

```bash
GET /api/v1/silver/contracts/{id}/callers?limit=50
```

**Example:**
```bash
curl "http://localhost:8080/api/v1/silver/contracts/CUSDC_TOKEN/callers?limit=20"
```

**Response:**
```json
{
  "contract_id": "CUSDC_TOKEN",
  "callers": [
    {
      "contract_id": "CSOROSWAP_ROUTER",
      "call_count": 1500,
      "functions": ["transfer", "approve", "balance"],
      "last_call": "2026-01-04T11:55:00Z"
    },
    {
      "contract_id": "CAQUARIUS_POOL",
      "call_count": 800,
      "functions": ["transfer"],
      "last_call": "2026-01-04T11:50:00Z"
    }
  ],
  "count": 2
}
```

---

### Get Contract Callees

Returns contracts called by a specific contract.

```bash
GET /api/v1/silver/contracts/{id}/callees?limit=50
```

**Example:**
```bash
curl "http://localhost:8080/api/v1/silver/contracts/CSOROSWAP_ROUTER/callees?limit=20"
```

**Response:**
```json
{
  "contract_id": "CSOROSWAP_ROUTER",
  "callees": [
    {
      "contract_id": "CAQUARIUS_POOL",
      "call_count": 1200,
      "functions": ["swap", "quote", "get_reserves"],
      "last_call": "2026-01-04T11:55:00Z"
    },
    {
      "contract_id": "CUSDC_TOKEN",
      "call_count": 900,
      "functions": ["transfer", "balance"],
      "last_call": "2026-01-04T11:55:00Z"
    }
  ],
  "count": 2
}
```

---

### Get Contract Call Summary

Returns aggregated statistics for a contract's call activity.

```bash
GET /api/v1/silver/contracts/{id}/call-summary
```

**Example:**
```bash
curl "http://localhost:8080/api/v1/silver/contracts/CSOROSWAP_ROUTER/call-summary"
```

**Response:**
```json
{
  "contract_id": "CSOROSWAP_ROUTER",
  "total_calls_as_caller": 15000,
  "total_calls_as_callee": 500,
  "unique_callers": 50,
  "unique_callees": 25,
  "first_seen": "2025-06-01T00:00:00Z",
  "last_seen": "2026-01-04T11:55:00Z"
}
```

---

## Freighter-Optimized Endpoint

### Get Contracts for Freighter Display

Returns contracts in a format optimized for Freighter wallet's UI.

```bash
GET /api/v1/freighter/tx/{hash}/contracts
```

**Example:**
```bash
curl "http://localhost:8080/api/v1/freighter/tx/abc123def456/contracts"
```

**Response:**
```json
{
  "transaction_hash": "abc123def456",
  "contracts_involved": [
    {
      "contract_id": "CSOROSWAP_ROUTER",
      "calls_made": 2,
      "calls_received": 0,
      "functions": [],
      "is_root": true
    },
    {
      "contract_id": "CAQUARIUS_POOL",
      "calls_made": 2,
      "calls_received": 1,
      "functions": ["swap"],
      "is_root": false
    },
    {
      "contract_id": "CUSDC_TOKEN",
      "calls_made": 0,
      "calls_received": 1,
      "functions": ["transfer"],
      "is_root": false
    },
    {
      "contract_id": "CXLM_TOKEN",
      "calls_made": 0,
      "calls_received": 1,
      "functions": ["transfer"],
      "is_root": false
    }
  ],
  "total_contracts": 4,
  "total_calls": 3,
  "display_format": "freighter_v1"
}
```

---

## Use Cases

### Freighter Wallet - Display Contracts Involved

When a user submits a transaction through Freighter that involves smart contracts:

1. After transaction submission, call `/api/v1/freighter/tx/{hash}/contracts`
2. Display the `contracts_involved` list in the transaction details
3. Show `is_root: true` contract as the "Primary Contract"
4. Show other contracts as "Also Involved"

### Block Explorer - Transaction Details

For displaying full contract interaction details:

1. Call `/api/v1/silver/tx/{hash}/call-graph` to get the full call tree
2. Visualize as a tree diagram showing function calls
3. Use `depth` and `order` for proper ordering

### Analytics - Contract Popularity

To analyze which contracts are most frequently called:

1. Call `/api/v1/silver/contracts/{id}/call-summary` for each contract of interest
2. Compare `total_calls_as_callee` across contracts
3. Use `unique_callers` to identify ecosystem integration

---

## Error Responses

All endpoints return standard error format:

```json
{
  "error": "transaction not found"
}
```

Common status codes:
- `400` - Bad request (missing parameters)
- `404` - Resource not found
- `500` - Internal server error

---

## Rate Limits

- Default: 100 requests/minute per IP
- Batch endpoints: 10 requests/minute per IP

---

## Schema Reference

### contract_invocation_calls table

```sql
CREATE TABLE contract_invocation_calls (
    call_id BIGSERIAL PRIMARY KEY,
    ledger_sequence BIGINT NOT NULL,
    transaction_index INT NOT NULL,
    operation_index INT NOT NULL,
    transaction_hash VARCHAR(64) NOT NULL,
    from_contract VARCHAR(100) NOT NULL,
    to_contract VARCHAR(100) NOT NULL,
    function_name VARCHAR(100),
    call_depth INT NOT NULL,
    execution_order INT NOT NULL,
    successful BOOLEAN NOT NULL,
    closed_at TIMESTAMP NOT NULL,
    ledger_range BIGINT NOT NULL
);
```

### contract_invocation_hierarchy table

```sql
CREATE TABLE contract_invocation_hierarchy (
    transaction_hash VARCHAR(64) NOT NULL,
    root_contract VARCHAR(100) NOT NULL,
    child_contract VARCHAR(100) NOT NULL,
    path_depth INT NOT NULL,
    full_path TEXT[] NOT NULL,
    ledger_range BIGINT NOT NULL,
    PRIMARY KEY (transaction_hash, root_contract, child_contract)
);
```
