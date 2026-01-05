## Silver Layer API Quick Reference

Complete API endpoint reference for building Stellar block explorers and analytics tools.

## Base URL

```
http://localhost:8092/api/v1/silver
```

## Authentication

None required (add as needed for production deployments)

## Response Format

All endpoints return JSON with CORS enabled.

**Success Response:**
```json
{
  "data": [...],
  "count": 10
}
```

**Error Response:**
```json
{
  "error": "error message"
}
```

---

## Cursor-Based Pagination

All list endpoints support cursor-based pagination for efficiently iterating through large result sets.

### How It Works

1. **Initial Request:** Make a request without a cursor to get the first page
2. **Next Page:** Use the `cursor` from the response to get the next page
3. **Check `has_more`:** When `has_more` is `false`, you've reached the end

### Pagination Parameters

| Name | Type | Description |
|------|------|-------------|
| cursor | string | Opaque cursor from previous response |
| limit | int | Number of records per page |

### Pagination Response Fields

| Field | Type | Description |
|-------|------|-------------|
| cursor | string | Cursor for next page (only if `has_more` is true) |
| has_more | bool | Whether more pages exist |

### Example: Paginating Through Operations

```bash
# First page
curl "http://localhost:8092/api/v1/silver/operations/enriched?account_id=GXXXXXX&limit=100"

# Response includes:
# {
#   "operations": [...],
#   "count": 100,
#   "cursor": "MjEzNzkxODo1",
#   "has_more": true
# }

# Next page using cursor
curl "http://localhost:8092/api/v1/silver/operations/enriched?account_id=GXXXXXX&limit=100&cursor=MjEzNzkxODo1"
```

### Important Notes

- **Cursor is opaque:** Don't parse or construct cursors - they may change format
- **Mutually exclusive:** `cursor` and `start_ledger` cannot be used together
- **Stable ordering:** Results are ordered by ledger sequence (descending)
- **No duplicates:** Pagination ensures no duplicate records across pages

### Endpoints with Cursor Support

| Endpoint | Cursor Type |
|----------|-------------|
| `/operations/enriched` | Operation cursor |
| `/payments` | Operation cursor |
| `/operations/soroban` | Operation cursor |
| `/transfers` | Transfer cursor |
| `/accounts/history` | Account cursor |

---

## Account Endpoints

### Get Current Account State

Returns the latest state of a specific account.

**Endpoint:** `GET /accounts/current`

**Parameters:**
| Name | Type | Required | Description |
|------|------|----------|-------------|
| account_id | string | Yes | Stellar account ID (GXXX...) |

**Example:**
```bash
curl "http://localhost:8092/api/v1/silver/accounts/current?account_id=GXXXXXX"
```

**Response:**
```json
{
  "account": {
    "account_id": "GXXXXXX",
    "balance": "1000000000",
    "sequence_number": "12345",
    "num_subentries": 5,
    "ledger_sequence": 2137918,
    "closed_at": "2024-12-16T10:00:00Z"
  }
}
```

---

### Get Account History (Time-Travel)

Returns historical snapshots of an account with SCD Type 2 tracking.

**Endpoint:** `GET /accounts/history`

**Parameters:**
| Name | Type | Required | Default | Description |
|------|------|----------|---------|-------------|
| account_id | string | Yes | - | Stellar account ID |
| limit | int | No | 50 | Max records (1-500) |
| cursor | string | No | - | Pagination cursor from previous response |

**Example:**
```bash
curl "http://localhost:8092/api/v1/silver/accounts/history?account_id=GXXXXXX&limit=10"
```

**Response:**
```json
{
  "account_id": "GXXXXXX",
  "history": [
    {
      "account_id": "GXXXXXX",
      "balance": "1000000000",
      "ledger_sequence": 2137918,
      "valid_from": "2024-12-16T10:00:00Z",
      "valid_to": null
    }
  ],
  "count": 1,
  "cursor": "MjEzNzkxOA==",
  "has_more": false
}
```

---

### Get Top Accounts

Returns accounts with highest balances (leaderboard).

**Endpoint:** `GET /accounts/top`

**Parameters:**
| Name | Type | Required | Default | Description |
|------|------|----------|---------|-------------|
| limit | int | No | 100 | Max records (1-1000) |

**Example:**
```bash
curl "http://localhost:8092/api/v1/silver/accounts/top?limit=50"
```

---

## Operations Endpoints

### Get Enriched Operations

Returns operations with full transaction and ledger context.

**Endpoint:** `GET /operations/enriched`

**Parameters:**
| Name | Type | Required | Default | Description |
|------|------|----------|---------|-------------|
| account_id | string | No | - | Filter by account |
| tx_hash | string | No | - | Filter by transaction |
| start_ledger | int | No | - | Start ledger sequence (mutually exclusive with cursor) |
| end_ledger | int | No | - | End ledger sequence |
| payments_only | bool | No | false | Only payment operations |
| soroban_only | bool | No | false | Only Soroban operations |
| limit | int | No | 100 | Max records (1-1000) |
| cursor | string | No | - | Pagination cursor from previous response |

**Example:**
```bash
curl "http://localhost:8092/api/v1/silver/operations/enriched?account_id=GXXXXXX&limit=50"
```

**Response:**
```json
{
  "operations": [
    {
      "transaction_hash": "TXXXXXX",
      "operation_id": 123456,
      "ledger_sequence": 2137918,
      "ledger_closed_at": "2024-12-16T10:00:00Z",
      "source_account": "GXXXXXX",
      "type": 1,
      "type_name": "PAYMENT",
      "destination": "GYYYYYY",
      "asset_code": "USDC",
      "amount": "100.0000000",
      "tx_successful": true,
      "tx_fee_charged": 100,
      "is_payment_op": true,
      "is_soroban_op": false
    }
  ],
  "count": 1,
  "cursor": "MjEzNzkxODoxMjM0NTY=",
  "has_more": true
}
```

---

### Get Payments Only

Convenience endpoint for payment operations only.

**Endpoint:** `GET /payments`

**Parameters:**
| Name | Type | Required | Default | Description |
|------|------|----------|---------|-------------|
| account_id | string | No | - | Filter by account |
| limit | int | No | 50 | Max records (1-500) |
| cursor | string | No | - | Pagination cursor from previous response |

**Example:**
```bash
curl "http://localhost:8092/api/v1/silver/payments?account_id=GXXXXXX&limit=20"
```

---

### Get Soroban Operations

Convenience endpoint for Soroban operations only.

**Endpoint:** `GET /operations/soroban`

**Parameters:**
| Name | Type | Required | Default | Description |
|------|------|----------|---------|-------------|
| account_id | string | No | - | Filter by account |
| limit | int | No | 50 | Max records (1-500) |
| cursor | string | No | - | Pagination cursor from previous response |

**Example:**
```bash
curl "http://localhost:8092/api/v1/silver/operations/soroban?limit=20"
```

---

## Token Transfer Endpoints

### Get Token Transfers

Returns unified view of classic Stellar payments + Soroban transfers.

**Endpoint:** `GET /transfers`

**Parameters:**
| Name | Type | Required | Default | Description |
|------|------|----------|---------|-------------|
| source_type | string | No | - | "classic" or "soroban" |
| asset_code | string | No | - | Filter by asset |
| from_account | string | No | - | Filter by sender |
| to_account | string | No | - | Filter by receiver |
| start_time | ISO8601 | No | -24h | Start timestamp |
| end_time | ISO8601 | No | now | End timestamp |
| limit | int | No | 100 | Max records (1-1000) |
| cursor | string | No | - | Pagination cursor from previous response |

**Example:**
```bash
curl "http://localhost:8092/api/v1/silver/transfers?asset_code=USDC&limit=50"
```

**Response:**
```json
{
  "transfers": [
    {
      "timestamp": "2024-12-16T10:00:00Z",
      "transaction_hash": "TXXXXXX",
      "ledger_sequence": 2137918,
      "source_type": "classic",
      "from_account": "GXXXXXX",
      "to_account": "GYYYYYY",
      "asset_code": "USDC",
      "asset_issuer": "GZZZZZZ",
      "amount": "100.0000000",
      "transaction_successful": true
    }
  ],
  "count": 1
}
```

---

### Get Transfer Statistics

Returns aggregated transfer statistics.

**Endpoint:** `GET /transfers/stats`

**Parameters:**
| Name | Type | Required | Default | Description |
|------|------|----------|---------|-------------|
| group_by | string | Yes | "asset" | "asset", "source_type", "hour", or "day" |
| start_time | ISO8601 | No | -24h | Start timestamp |
| end_time | ISO8601 | No | now | End timestamp |

**Example:**
```bash
curl "http://localhost:8092/api/v1/silver/transfers/stats?group_by=asset"
```

**Response:**
```json
{
  "stats": [
    {
      "asset_code": "USDC",
      "source_type": "classic",
      "transfer_count": 1523,
      "unique_senders": 342,
      "unique_receivers": 398,
      "total_volume": 1234567.89
    }
  ],
  "count": 1,
  "group_by": "asset"
}
```

---

## Block Explorer Specific Endpoints

### Get Account Overview (All-in-One)

Returns comprehensive account information in single request.

**Endpoint:** `GET /explorer/account`

**Parameters:**
| Name | Type | Required | Description |
|------|------|----------|-------------|
| account_id | string | Yes | Stellar account ID |

**Example:**
```bash
curl "http://localhost:8092/api/v1/silver/explorer/account?account_id=GXXXXXX"
```

**Response:**
```json
{
  "account": { /* current account state */ },
  "recent_operations": [ /* last 10 operations */ ],
  "recent_transfers": [ /* last 10 transfers */ ],
  "operations_count": 10,
  "transfers_count": 5
}
```

---

### Get Transaction Details (All-in-One)

Returns full transaction information with all operations.

**Endpoint:** `GET /explorer/transaction`

**Parameters:**
| Name | Type | Required | Description |
|------|------|----------|-------------|
| tx_hash | string | Yes | Transaction hash |

**Example:**
```bash
curl "http://localhost:8092/api/v1/silver/explorer/transaction?tx_hash=TXXXXXX"
```

**Response:**
```json
{
  "transaction": {
    "transaction_hash": "TXXXXXX",
    "ledger_sequence": 2137918,
    "ledger_closed_at": "2024-12-16T10:00:00Z",
    "successful": true,
    "fee_charged": 100,
    "operation_count": 3
  },
  "operations": [ /* all operations */ ],
  "transfers": [ /* token transfers */ ]
}
```

---

### Get Asset Overview

Returns asset statistics and recent activity.

**Endpoint:** `GET /explorer/asset`

**Parameters:**
| Name | Type | Required | Description |
|------|------|----------|-------------|
| asset_code | string | Yes | Asset code (e.g., USDC) |
| asset_issuer | string | No | Asset issuer |

**Example:**
```bash
curl "http://localhost:8092/api/v1/silver/explorer/asset?asset_code=USDC"
```

**Response:**
```json
{
  "asset_code": "USDC",
  "recent_transfers": [ /* last 100 transfers */ ],
  "stats_24h": {
    "transfer_count": 1523,
    "unique_senders": 342,
    "unique_receivers": 398,
    "total_volume": 1234567.89
  },
  "transfer_count": 100
}
```

---

## Operation Types

| Code | Name | Description |
|------|------|-------------|
| 0 | CREATE_ACCOUNT | Create new account |
| 1 | PAYMENT | Send payment |
| 2 | PATH_PAYMENT_STRICT_RECEIVE | Path payment with strict receive |
| 3 | MANAGE_SELL_OFFER | Manage sell offer |
| 4 | CREATE_PASSIVE_SELL_OFFER | Create passive sell offer |
| 5 | SET_OPTIONS | Set account options |
| 6 | CHANGE_TRUST | Change trustline |
| 7 | ALLOW_TRUST | Allow trust |
| 8 | ACCOUNT_MERGE | Merge accounts |
| 9 | INFLATION | Inflation (deprecated) |
| 10 | MANAGE_DATA | Manage data entry |
| 11 | BUMP_SEQUENCE | Bump sequence number |
| 12 | MANAGE_BUY_OFFER | Manage buy offer |
| 13 | PATH_PAYMENT_STRICT_SEND | Path payment with strict send |
| 14 | CREATE_CLAIMABLE_BALANCE | Create claimable balance |
| 15 | CLAIM_CLAIMABLE_BALANCE | Claim claimable balance |
| 16 | BEGIN_SPONSORING_FUTURE_RESERVES | Begin sponsoring reserves |
| 17 | END_SPONSORING_FUTURE_RESERVES | End sponsoring reserves |
| 18 | REVOKE_SPONSORSHIP | Revoke sponsorship |
| 19 | CLAWBACK | Clawback asset |
| 20 | CLAWBACK_CLAIMABLE_BALANCE | Clawback claimable balance |
| 21 | SET_TRUST_LINE_FLAGS | Set trustline flags |
| 22 | LIQUIDITY_POOL_DEPOSIT | Deposit to liquidity pool |
| 23 | LIQUIDITY_POOL_WITHDRAW | Withdraw from liquidity pool |
| 24 | INVOKE_HOST_FUNCTION | Invoke Soroban contract |
| 25 | EXTEND_FOOTPRINT_TTL | Extend Soroban TTL |
| 26 | RESTORE_FOOTPRINT | Restore Soroban state |

---

## Common Query Patterns

### Block Explorer Home Page
```bash
# Latest payments
GET /api/v1/silver/payments?limit=20

# Transfer stats
GET /api/v1/silver/transfers/stats?group_by=asset

# Top accounts
GET /api/v1/silver/accounts/top?limit=10
```

### Account Page
```bash
# Account state + recent activity
GET /api/v1/silver/explorer/account?account_id=GXXXXXX

# Balance history
GET /api/v1/silver/accounts/history?account_id=GXXXXXX&limit=20
```

### Transaction Page
```bash
# Transaction details
GET /api/v1/silver/explorer/transaction?tx_hash=TXXXXXX
```

### Asset Page
```bash
# Asset overview
GET /api/v1/silver/explorer/asset?asset_code=USDC

# Asset transfers over time
GET /api/v1/silver/transfers/stats?group_by=hour&start_time=2024-01-01T00:00:00Z
```

### Analytics Dashboard
```bash
# Network activity
GET /api/v1/silver/transfers/stats?group_by=hour

# Classic vs Soroban
GET /api/v1/silver/transfers/stats?group_by=source_type

# Recent Soroban activity
GET /api/v1/silver/operations/soroban?limit=50
```

---

## Rate Limits

**Default:** None (add as needed for production)

**Recommended for public API:**
- 60 requests/minute per IP for general queries
- 10 requests/minute for stats endpoints
- 5 requests/minute for top accounts

---

## Caching Recommendations

| Endpoint | TTL | Reason |
|----------|-----|--------|
| /accounts/current | 30s | Frequent updates |
| /accounts/top | 5min | Expensive query |
| /transfers/stats | 5min | Expensive aggregation |
| /payments | 30s | Real-time feed |
| /explorer/* | 1min | Balanced performance |

---

## Error Codes

| Code | Description |
|------|-------------|
| 200 | Success |
| 400 | Bad request (missing/invalid parameters) |
| 404 | Resource not found |
| 429 | Rate limit exceeded |
| 500 | Internal server error |
| 503 | Service unavailable |

---

## CORS

All endpoints include CORS headers:
```
Access-Control-Allow-Origin: *
Access-Control-Allow-Methods: GET, POST, OPTIONS
Access-Control-Allow-Headers: Content-Type, Authorization
```

---

## Examples

See `examples/block_explorer_demo.html` for interactive demo with working code examples.
