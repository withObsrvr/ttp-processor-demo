# API Reference

Complete reference for all Obsrvr Lake API endpoints. All examples use the testnet API.

## Base URL

```
https://gateway.withobsrvr.com/lake/v1/testnet
```

## Authentication

All requests require an API key:

```bash
curl -H "Authorization: Api-Key YOUR_API_KEY" "https://gateway.withobsrvr.com/lake/v1/testnet/api/v1/..."
```

---

## Silver Layer Endpoints

Analytics-ready, pre-processed data optimized for common queries.

### Account Endpoints

#### Get Current Account State

Returns the current balance and state of a Stellar account.

```bash
GET /api/v1/silver/accounts/current?account_id={account_id}
```

**Parameters:**
| Parameter | Required | Description |
|-----------|----------|-------------|
| `account_id` | Yes | Stellar account address (G...) |

**Example:**
```bash
curl -H "Authorization: Api-Key $API_KEY" \
  "https://gateway.withobsrvr.com/lake/v1/testnet/api/v1/silver/accounts/current?account_id=GAIH3ULLFQ4DGSECF2AR555KZ4KNDGEKN4AFI4SU2M7B43MGK3QJZNSR"
```

**Response:**
```json
{
  "account": {
    "account_id": "GAIH3ULLFQ4DGSECF2AR555KZ4KNDGEKN4AFI4SU2M7B43MGK3QJZNSR",
    "balance": 10000000000,
    "sequence_number": 123456789,
    "num_subentries": 3,
    "last_modified_ledger": 21379180
  }
}
```

> **Note:** Balance is in stroops. Divide by 10,000,000 for XLM.

---

#### Get Account Signers

Returns the signers and thresholds for an account (Horizon-compatible format).

```bash
GET /api/v1/silver/accounts/signers?account_id={account_id}
```

**Parameters:**
| Parameter | Required | Description |
|-----------|----------|-------------|
| `account_id` | Yes | Stellar account address (G...) |

**Example:**
```bash
curl -H "Authorization: Api-Key $API_KEY" \
  "https://gateway.withobsrvr.com/lake/v1/testnet/api/v1/silver/accounts/signers?account_id=GAIH3ULLFQ4DGSECF2AR555KZ4KNDGEKN4AFI4SU2M7B43MGK3QJZNSR"
```

**Response:**
```json
{
  "account_id": "GAIH3ULLFQ4DGSECF2AR555KZ4KNDGEKN4AFI4SU2M7B43MGK3QJZNSR",
  "signers": [
    {
      "key": "GAIH3ULLFQ4DGSECF2AR555KZ4KNDGEKN4AFI4SU2M7B43MGK3QJZNSR",
      "weight": 1,
      "type": "ed25519_public_key"
    }
  ],
  "thresholds": {
    "low_threshold": 0,
    "med_threshold": 0,
    "high_threshold": 0
  }
}
```

---

#### Get Account History

Returns historical balance snapshots for an account.

```bash
GET /api/v1/silver/accounts/history?account_id={account_id}&limit={limit}&cursor={cursor}
```

**Parameters:**
| Parameter | Required | Description |
|-----------|----------|-------------|
| `account_id` | Yes | Stellar account address (G...) |
| `limit` | No | Max results (default: 50, max: 500) |
| `cursor` | No | Pagination cursor from previous response |

**Example:**
```bash
curl -H "Authorization: Api-Key $API_KEY" \
  "https://gateway.withobsrvr.com/lake/v1/testnet/api/v1/silver/accounts/history?account_id=GAIH3ULLFQ4DGSECF2AR555KZ4KNDGEKN4AFI4SU2M7B43MGK3QJZNSR&limit=10"
```

---

#### List All Accounts

Returns a paginated list of all accounts with sorting and filtering.

```bash
GET /api/v1/silver/accounts?limit={limit}&cursor={cursor}&sort_by={field}&order={asc|desc}&min_balance={stroops}
```

**Parameters:**
| Parameter | Required | Description |
|-----------|----------|-------------|
| `limit` | No | Max results (default: 100, max: 1000) |
| `cursor` | No | Pagination cursor |
| `sort_by` | No | Sort field: `balance`, `last_modified_ledger` (default: balance) |
| `order` | No | Sort order: `asc`, `desc` (default: desc) |
| `min_balance` | No | Minimum balance filter in stroops |

**Example:**
```bash
# Top accounts by balance
curl -H "Authorization: Api-Key $API_KEY" \
  "https://gateway.withobsrvr.com/lake/v1/testnet/api/v1/silver/accounts?limit=20&sort_by=balance&order=desc"

# Accounts with at least 1000 XLM
curl -H "Authorization: Api-Key $API_KEY" \
  "https://gateway.withobsrvr.com/lake/v1/testnet/api/v1/silver/accounts?min_balance=10000000000"
```

---

#### Get Top Accounts by Balance

Returns top accounts ranked by XLM balance.

```bash
GET /api/v1/silver/accounts/top?limit={limit}
```

**Parameters:**
| Parameter | Required | Description |
|-----------|----------|-------------|
| `limit` | No | Max results (default: 100, max: 1000) |

**Example:**
```bash
curl -H "Authorization: Api-Key $API_KEY" \
  "https://gateway.withobsrvr.com/lake/v1/testnet/api/v1/silver/accounts/top?limit=20"
```

---

#### Get Account Balances

Returns all balances (XLM + trustlines) for an account in a single call.

```bash
GET /api/v1/silver/accounts/{account_id}/balances
```

**Parameters:**
| Parameter | Required | Description |
|-----------|----------|-------------|
| `account_id` | Yes | Stellar account address (G...) in URL path |

**Example:**
```bash
curl -H "Authorization: Api-Key $API_KEY" \
  "https://gateway.withobsrvr.com/lake/v1/testnet/api/v1/silver/accounts/GAIH3ULLFQ4DGSECF2AR555KZ4KNDGEKN4AFI4SU2M7B43MGK3QJZNSR/balances"
```

**Response:**
```json
{
  "account_id": "GAIH3ULLFQ4DGSECF2AR555KZ4KNDGEKN4AFI4SU2M7B43MGK3QJZNSR",
  "balances": [
    {
      "asset_type": "native",
      "asset_code": "XLM",
      "balance": "7583577198.9309600",
      "balance_stroops": 75835771989309600
    },
    {
      "asset_type": "credit_alphanum4",
      "asset_code": "USDC",
      "asset_issuer": "GA5ZSEJYB37JRC5AVCIA5MOP4RHTM335X2KGX3IHOJAPP5RE34K4KZVN",
      "balance": "250.0000000",
      "balance_stroops": 2500000000,
      "limit": "922337203685.4775807",
      "is_authorized": true
    }
  ],
  "total_balances": 2
}
```

> **Note:** This is the preferred endpoint for wallet integrations - returns both XLM and all trustlines in one call.

---

#### Get Account Activity Feed

Returns a unified timeline of all account activity (payments, contract calls, etc.).

```bash
GET /api/v1/silver/accounts/{account_id}/activity?limit={limit}&cursor={cursor}
```

**Parameters:**
| Parameter | Required | Description |
|-----------|----------|-------------|
| `account_id` | Yes | Stellar account address (G...) in URL path |
| `limit` | No | Max results (default: 50, max: 200) |
| `cursor` | No | Pagination cursor |

**Example:**
```bash
curl -H "Authorization: Api-Key $API_KEY" \
  "https://gateway.withobsrvr.com/lake/v1/testnet/api/v1/silver/accounts/GAIH3ULLFQ4DGSECF2AR555KZ4KNDGEKN4AFI4SU2M7B43MGK3QJZNSR/activity?limit=20"
```

**Response:**
```json
{
  "account_id": "GAIH3ULLFQ4DGSECF2AR555KZ4KNDGEKN4AFI4SU2M7B43MGK3QJZNSR",
  "activity": [
    {
      "type": "payment_sent",
      "timestamp": "2026-01-05T12:30:00Z",
      "ledger_sequence": 21379180,
      "tx_hash": "abc123...",
      "details": {
        "to": "GBXYZ...",
        "amount": "100.0000000",
        "asset_code": "XLM"
      }
    },
    {
      "type": "contract_call",
      "timestamp": "2026-01-05T12:25:00Z",
      "ledger_sequence": 21379175,
      "tx_hash": "def456...",
      "details": {
        "contract_id": "CABC...",
        "function": "swap",
        "role": "caller"
      }
    }
  ],
  "has_more": true,
  "cursor": "eyJsIjoyMTM3OTE3NSwid..."
}
```

---

### Operations Endpoints

#### Get Enriched Operations

Returns operations with full context and decoded details.

```bash
GET /api/v1/silver/operations/enriched?account_id={account_id}&tx_hash={hash}&limit={limit}&cursor={cursor}
```

**Parameters:**
| Parameter | Required | Description |
|-----------|----------|-------------|
| `account_id` | No | Filter by account |
| `tx_hash` | No | Filter by transaction hash |
| `payments_only` | No | Only return payment operations (true/false) |
| `soroban_only` | No | Only return Soroban operations (true/false) |
| `start_ledger` | No | Start of ledger range |
| `end_ledger` | No | End of ledger range |
| `limit` | No | Max results (default: 100, max: 1000) |
| `cursor` | No | Pagination cursor (mutually exclusive with start_ledger) |

**Example:**
```bash
# Get operations for an account
curl -H "Authorization: Api-Key $API_KEY" \
  "https://gateway.withobsrvr.com/lake/v1/testnet/api/v1/silver/operations/enriched?account_id=GAIH3ULLFQ4DGSECF2AR555KZ4KNDGEKN4AFI4SU2M7B43MGK3QJZNSR&limit=50"

# Get operations for a transaction
curl -H "Authorization: Api-Key $API_KEY" \
  "https://gateway.withobsrvr.com/lake/v1/testnet/api/v1/silver/operations/enriched?tx_hash=abc123..."
```

---

#### Get Payments Only

Convenience endpoint that returns only payment operations.

```bash
GET /api/v1/silver/payments?account_id={account_id}&limit={limit}&cursor={cursor}
```

**Example:**
```bash
curl -H "Authorization: Api-Key $API_KEY" \
  "https://gateway.withobsrvr.com/lake/v1/testnet/api/v1/silver/payments?account_id=GAIH3ULLFQ4DGSECF2AR555KZ4KNDGEKN4AFI4SU2M7B43MGK3QJZNSR"
```

---

#### Get Soroban Operations Only

Convenience endpoint that returns only Soroban smart contract operations.

```bash
GET /api/v1/silver/operations/soroban?account_id={account_id}&limit={limit}&cursor={cursor}
```

**Example:**
```bash
curl -H "Authorization: Api-Key $API_KEY" \
  "https://gateway.withobsrvr.com/lake/v1/testnet/api/v1/silver/operations/soroban?account_id=GAIH3ULLFQ4DGSECF2AR555KZ4KNDGEKN4AFI4SU2M7B43MGK3QJZNSR"
```

---

### Token Transfer Endpoints

#### Get Token Transfers

Returns unified view of all token movements (payments, path payments, claimable balance claims).

```bash
GET /api/v1/silver/transfers?from_account={account}&to_account={account}&asset_code={code}&source_type={type}&limit={limit}&cursor={cursor}
```

**Parameters:**
| Parameter | Required | Description |
|-----------|----------|-------------|
| `from_account` | No | Filter by sender |
| `to_account` | No | Filter by recipient |
| `asset_code` | No | Filter by asset (e.g., USDC, XLM) |
| `source_type` | No | Filter by type: `classic` or `soroban` |
| `start_time` | No | Start of time range (RFC3339 format) |
| `end_time` | No | End of time range (RFC3339 format) |
| `limit` | No | Max results (default: 100, max: 1000) |
| `cursor` | No | Pagination cursor |

**Example:**
```bash
# Get USDC transfers for an account
curl -H "Authorization: Api-Key $API_KEY" \
  "https://gateway.withobsrvr.com/lake/v1/testnet/api/v1/silver/transfers?from_account=GAIH3...&asset_code=USDC"

# Get all transfers in the last hour
curl -H "Authorization: Api-Key $API_KEY" \
  "https://gateway.withobsrvr.com/lake/v1/testnet/api/v1/silver/transfers?start_time=2026-01-05T11:00:00Z&end_time=2026-01-05T12:00:00Z"
```

---

#### Get Transfer Statistics

Returns aggregated transfer statistics.

```bash
GET /api/v1/silver/transfers/stats?group_by={field}&start_time={time}&end_time={time}
```

**Parameters:**
| Parameter | Required | Description |
|-----------|----------|-------------|
| `group_by` | No | Grouping: `asset`, `source_type`, `hour`, `day` (default: asset) |
| `start_time` | No | Start of time range (default: 24 hours ago) |
| `end_time` | No | End of time range (default: now) |

**Example:**
```bash
# Get transfer volume by asset
curl -H "Authorization: Api-Key $API_KEY" \
  "https://gateway.withobsrvr.com/lake/v1/testnet/api/v1/silver/transfers/stats?group_by=asset"

# Get hourly transfer counts
curl -H "Authorization: Api-Key $API_KEY" \
  "https://gateway.withobsrvr.com/lake/v1/testnet/api/v1/silver/transfers/stats?group_by=hour&start_time=2026-01-04T00:00:00Z"
```

---

### Token/Asset Endpoints

#### Get Token Holders

Returns accounts holding a specific token, ranked by balance.

```bash
GET /api/v1/silver/assets/{asset}/holders?limit={limit}&cursor={cursor}&min_balance={stroops}
```

**Asset Parameter Format:**
- For XLM: `/assets/XLM/holders` or `/assets/native/holders`
- For other assets: `/assets/CODE:ISSUER/holders` (e.g., `/assets/USDC:GA5ZSEJYB37.../holders`)

**Query Parameters:**
| Parameter | Required | Description |
|-----------|----------|-------------|
| `limit` | No | Max results (default: 100, max: 1000) |
| `cursor` | No | Pagination cursor |
| `min_balance` | No | Minimum balance filter in stroops |

**Example:**
```bash
# Get top XLM holders
curl -H "Authorization: Api-Key $API_KEY" \
  "https://gateway.withobsrvr.com/lake/v1/testnet/api/v1/silver/assets/XLM/holders?limit=20"

# Get USDC holders with at least 1000 USDC
curl -H "Authorization: Api-Key $API_KEY" \
  "https://gateway.withobsrvr.com/lake/v1/testnet/api/v1/silver/assets/USDC:GA5ZSEJYB37JRC5AVCIA5MOP4RHTM335X2KGX3IHOJAPP5RE34K4KZVN/holders?min_balance=10000000000&limit=50"
```

**Response:**
```json
{
  "asset": {
    "code": "XLM",
    "type": "native"
  },
  "holders": [
    {
      "account_id": "GAIH3ULLFQ4DGSECF2AR555KZ4KNDGEKN4AFI4SU2M7B43MGK3QJZNSR",
      "balance": "7583577198.9309600",
      "balance_stroops": 75835771989309600,
      "rank": 1
    },
    {
      "account_id": "GBXYZ...",
      "balance": "5000000.0000000",
      "balance_stroops": 50000000000000,
      "rank": 2
    }
  ],
  "total_holders": 2,
  "has_more": true,
  "cursor": "eyJiIjo1MDAwMDAwMDAwMDAwMCwi..."
}
```

> **Note:** Balances are returned in both human-readable format (7 decimals) and stroops for precise calculations.

---

#### Get Token Statistics

Returns aggregated statistics for a specific token.

```bash
GET /api/v1/silver/assets/{asset}/stats
```

**Asset Parameter Format:**
- For XLM: `/assets/XLM/stats` or `/assets/native/stats`
- For other assets: `/assets/CODE:ISSUER/stats`

**Example:**
```bash
# Get XLM network statistics
curl -H "Authorization: Api-Key $API_KEY" \
  "https://gateway.withobsrvr.com/lake/v1/testnet/api/v1/silver/assets/XLM/stats"

# Get USDC statistics
curl -H "Authorization: Api-Key $API_KEY" \
  "https://gateway.withobsrvr.com/lake/v1/testnet/api/v1/silver/assets/USDC:GA5ZSEJYB37JRC5AVCIA5MOP4RHTM335X2KGX3IHOJAPP5RE34K4KZVN/stats"
```

**Response:**
```json
{
  "asset": {
    "code": "XLM",
    "type": "native"
  },
  "stats": {
    "total_holders": 8500000,
    "total_trustlines": 0,
    "circulating_supply": "105000000000.0000000",
    "top_10_concentration": 0.15,
    "transfers_24h": 25000,
    "volume_24h": "541235926.8349459",
    "unique_accounts_24h": 12500
  },
  "generated_at": "2026-01-05T18:00:00Z"
}
```

**Response Fields:**
| Field | Description |
|-------|-------------|
| `total_holders` | Number of accounts with non-zero balance |
| `total_trustlines` | Total trustlines (0 for XLM) |
| `circulating_supply` | Sum of all balances |
| `top_10_concentration` | Fraction of supply held by top 10 accounts (0.0-1.0) |
| `transfers_24h` | Number of transfers in last 24 hours |
| `volume_24h` | Total transfer volume in last 24 hours |
| `unique_accounts_24h` | Unique accounts involved in transfers |

> **Note:** Statistics are computed in real-time and may take a few seconds for tokens with high holder counts.

---

### Smart Contract Endpoints

#### Get Top Active Contracts

Returns the most active contracts for a given period.

```bash
GET /api/v1/silver/contracts/top?period={period}&limit={limit}
```

**Parameters:**
| Parameter | Required | Description |
|-----------|----------|-------------|
| `period` | No | Time period: `24h`, `7d`, `30d` (default: 24h) |
| `limit` | No | Max results (default: 20, max: 100) |

**Example:**
```bash
curl -H "Authorization: Api-Key $API_KEY" \
  "https://gateway.withobsrvr.com/lake/v1/testnet/api/v1/silver/contracts/top?period=7d&limit=20"
```

**Response:**
```json
{
  "period": "7d",
  "contracts": [
    {
      "contract_id": "CAUGJT4GREIY3WHOUUU5RIUDGSPVREF5CDCYJOWMHOVT2GWQT5JEETGJ",
      "total_calls": 15437,
      "unique_callers": 89,
      "top_function": "swap",
      "last_activity": "2026-01-05T12:57:07Z"
    }
  ],
  "count": 20
}
```

---

#### Get Contract Analytics Summary

Returns comprehensive analytics for a specific contract.

```bash
GET /api/v1/silver/contracts/{contract_id}/analytics
```

**Example:**
```bash
curl -H "Authorization: Api-Key $API_KEY" \
  "https://gateway.withobsrvr.com/lake/v1/testnet/api/v1/silver/contracts/CAUGJT4GREIY3WHOUUU5RIUDGSPVREF5CDCYJOWMHOVT2GWQT5JEETGJ/analytics"
```

**Response:**
```json
{
  "contract_id": "CAUGJT4GREIY3WHOUUU5RIUDGSPVREF5CDCYJOWMHOVT2GWQT5JEETGJ",
  "stats": {
    "total_calls_as_caller": 850,
    "total_calls_as_callee": 2008,
    "unique_callers": 5,
    "unique_callees": 850
  },
  "timeline": {
    "first_seen": "2025-12-19T03:55:21Z",
    "last_activity": "2026-01-05T12:57:07Z"
  },
  "top_functions": [
    {"name": "transfer", "count": 154},
    {"name": "swap", "count": 89}
  ],
  "daily_calls_7d": [
    {"date": "2026-01-05", "count": 762},
    {"date": "2026-01-04", "count": 645}
  ]
}
```

---

#### Get Contract Callers

Returns contracts that call a specific contract.

```bash
GET /api/v1/silver/contracts/{contract_id}/callers?limit={limit}
```

**Example:**
```bash
curl -H "Authorization: Api-Key $API_KEY" \
  "https://gateway.withobsrvr.com/lake/v1/testnet/api/v1/silver/contracts/CAUGJT4.../callers?limit=20"
```

---

#### Get Contract Callees

Returns contracts called by a specific contract.

```bash
GET /api/v1/silver/contracts/{contract_id}/callees?limit={limit}
```

**Example:**
```bash
curl -H "Authorization: Api-Key $API_KEY" \
  "https://gateway.withobsrvr.com/lake/v1/testnet/api/v1/silver/contracts/CAUGJT4.../callees?limit=20"
```

---

#### Get Contract Call Summary

Returns aggregated call statistics for a contract.

```bash
GET /api/v1/silver/contracts/{contract_id}/call-summary
```

**Response:**
```json
{
  "contract_id": "CAUGJT4...",
  "total_calls_as_caller": 150,
  "total_calls_as_callee": 200,
  "unique_callers": 10,
  "unique_callees": 5,
  "first_seen": "2025-12-01T00:00:00Z",
  "last_seen": "2026-01-05T12:00:00Z"
}
```

---

#### Get Contract Recent Calls

Returns recent calls involving a specific contract.

```bash
GET /api/v1/silver/contracts/{contract_id}/recent-calls?limit={limit}
```

**Example:**
```bash
curl -H "Authorization: Api-Key $API_KEY" \
  "https://gateway.withobsrvr.com/lake/v1/testnet/api/v1/silver/contracts/CAUGJT4.../recent-calls?limit=50"
```

---

### Transaction Contract Analysis

#### Get Contracts Involved in Transaction

Returns all contracts involved in a transaction.

```bash
GET /api/v1/silver/tx/{tx_hash}/contracts-involved
```

**Example:**
```bash
curl -H "Authorization: Api-Key $API_KEY" \
  "https://gateway.withobsrvr.com/lake/v1/testnet/api/v1/silver/tx/abc123.../contracts-involved"
```

**Response:**
```json
{
  "transaction_hash": "abc123...",
  "contracts_involved": ["CABC...", "CDEF...", "CGHI..."],
  "count": 3
}
```

---

#### Get Transaction Call Graph

Returns the complete call graph for a transaction.

```bash
GET /api/v1/silver/tx/{tx_hash}/call-graph
```

**Example:**
```bash
curl -H "Authorization: Api-Key $API_KEY" \
  "https://gateway.withobsrvr.com/lake/v1/testnet/api/v1/silver/tx/abc123.../call-graph"
```

**Response:**
```json
{
  "transaction_hash": "abc123...",
  "calls": [
    {
      "from": "CABC...",
      "to": "CDEF...",
      "function": "swap",
      "depth": 0,
      "order": 0,
      "successful": true
    },
    {
      "from": "CDEF...",
      "to": "CGHI...",
      "function": "transfer",
      "depth": 1,
      "order": 1,
      "successful": true
    }
  ],
  "count": 2
}
```

---

#### Get Transaction Call Hierarchy

Returns the contract hierarchy for a transaction.

```bash
GET /api/v1/silver/tx/{tx_hash}/hierarchy
```

---

#### Get Transaction Contracts Summary (Wallet-friendly)

Returns contracts in a wallet-optimized format (for Freighter, etc.).

```bash
GET /api/v1/silver/tx/{tx_hash}/contracts-summary
```

**Response:**
```json
{
  "transaction_hash": "abc123...",
  "contracts_involved": [
    {
      "contract_id": "CABC...",
      "calls_made": 2,
      "calls_received": 0,
      "functions": [],
      "is_root": true
    },
    {
      "contract_id": "CDEF...",
      "calls_made": 1,
      "calls_received": 1,
      "functions": ["swap"],
      "is_root": false
    }
  ],
  "total_contracts": 2,
  "total_calls": 2,
  "display_format": "wallet_v1"
}
```

---

### Network Statistics

#### Get Network Stats

Returns headline network statistics.

```bash
GET /api/v1/silver/stats/network
```

**Example:**
```bash
curl -H "Authorization: Api-Key $API_KEY" \
  "https://gateway.withobsrvr.com/lake/v1/testnet/api/v1/silver/stats/network"
```

**Response:**
```json
{
  "generated_at": "2026-01-05T12:57:07Z",
  "data_freshness": "real-time",
  "accounts": {
    "total": 8500000,
    "active_24h": 12500,
    "created_24h": 150
  },
  "ledger": {
    "current_sequence": 21379180,
    "avg_close_time_seconds": 5.0
  },
  "operations_24h": {
    "total": 85000,
    "payments": 25000,
    "path_payments": 5000,
    "create_account": 150,
    "contract_invoke": 30000
  }
}
```

---

### DEX Offers Endpoints

#### List Offers

Returns DEX order book offers with optional filtering.

```bash
GET /api/v1/silver/offers?seller_id={account_id}&limit={limit}&cursor={cursor}
```

**Parameters:**
| Parameter | Required | Description |
|-----------|----------|-------------|
| `seller_id` | No | Filter by seller account |
| `limit` | No | Max results (default: 100, max: 1000) |
| `cursor` | No | Pagination cursor |

**Example:**
```bash
curl -H "Authorization: Api-Key $API_KEY" \
  "https://gateway.withobsrvr.com/lake/v1/testnet/api/v1/silver/offers?limit=50"
```

**Response:**
```json
{
  "offers": [
    {
      "offer_id": 32,
      "seller_id": "GBBD47IF6LWK7P7MDEVSCWR7DPUWV3NY3DTQEVFL4NAT4AQH3ZLLFLA5",
      "selling": {
        "code": "USDC",
        "issuer": "GBBD47IF6LWK7P7MDEVSCWR7DPUWV3NY3DTQEVFL4NAT4AQH3ZLLFLA5",
        "type": "credit_alphanum4"
      },
      "buying": {
        "code": "XLM",
        "type": "native"
      },
      "amount": "1000.0000000",
      "price": "0.1000000",
      "price_r": {"n": 1, "d": 10},
      "last_modified_ledger": 341708
    }
  ],
  "count": 1,
  "cursor": "MTIzNDU=",
  "has_more": true
}
```

---

#### Get Offers by Trading Pair

Returns offers for a specific trading pair.

```bash
GET /api/v1/silver/offers/pair?selling={asset}&buying={asset}&limit={limit}&cursor={cursor}
```

**Asset Format:**
- Native: `XLM` or `native`
- Credit: `CODE:ISSUER` (e.g., `USDC:GA5ZSEJYB37...`)

**Example:**
```bash
curl -H "Authorization: Api-Key $API_KEY" \
  "https://gateway.withobsrvr.com/lake/v1/testnet/api/v1/silver/offers/pair?selling=XLM&buying=USDC:GBBD47IF6LWK7P7MDEVSCWR7DPUWV3NY3DTQEVFL4NAT4AQH3ZLLFLA5"
```

---

#### Get Single Offer

```bash
GET /api/v1/silver/offers/{offer_id}
```

**Example:**
```bash
curl -H "Authorization: Api-Key $API_KEY" \
  "https://gateway.withobsrvr.com/lake/v1/testnet/api/v1/silver/offers/32"
```

---

### Liquidity Pool Endpoints

#### List Liquidity Pools

Returns AMM liquidity pools.

```bash
GET /api/v1/silver/liquidity-pools?limit={limit}&cursor={cursor}
```

**Example:**
```bash
curl -H "Authorization: Api-Key $API_KEY" \
  "https://gateway.withobsrvr.com/lake/v1/testnet/api/v1/silver/liquidity-pools?limit=20"
```

**Response:**
```json
{
  "liquidity_pools": [
    {
      "pool_id": "4cd1f6defba237eecbc5fefe259f89ebc4b5edd49116beb5536c4034fc48d63f",
      "pool_type": "constant_product",
      "fee_bp": 30,
      "trustline_count": 1,
      "total_shares": "824548.7936328",
      "reserves": [
        {
          "asset": {"code": "XLM", "type": "native"},
          "amount": "787612.8481550"
        },
        {
          "asset": {"code": "USDC", "issuer": "GBBD47...", "type": "credit_alphanum4"},
          "amount": "884041.9339546"
        }
      ],
      "last_modified_ledger": 341605
    }
  ],
  "count": 1,
  "cursor": "NGNkMWY2...",
  "has_more": true
}
```

---

#### Get Liquidity Pools by Asset

Returns pools containing a specific asset.

```bash
GET /api/v1/silver/liquidity-pools/asset?asset={asset}&limit={limit}&cursor={cursor}
```

**Example:**
```bash
curl -H "Authorization: Api-Key $API_KEY" \
  "https://gateway.withobsrvr.com/lake/v1/testnet/api/v1/silver/liquidity-pools/asset?asset=USDC:GBBD47IF6LWK7P7MDEVSCWR7DPUWV3NY3DTQEVFL4NAT4AQH3ZLLFLA5"
```

---

#### Get Single Liquidity Pool

```bash
GET /api/v1/silver/liquidity-pools/{pool_id}
```

**Example:**
```bash
curl -H "Authorization: Api-Key $API_KEY" \
  "https://gateway.withobsrvr.com/lake/v1/testnet/api/v1/silver/liquidity-pools/4cd1f6defba237eecbc5fefe259f89ebc4b5edd49116beb5536c4034fc48d63f"
```

---

### Claimable Balance Endpoints

#### List Claimable Balances

Returns claimable balances with optional sponsor filter.

```bash
GET /api/v1/silver/claimable-balances?sponsor={account_id}&limit={limit}&cursor={cursor}
```

**Example:**
```bash
curl -H "Authorization: Api-Key $API_KEY" \
  "https://gateway.withobsrvr.com/lake/v1/testnet/api/v1/silver/claimable-balances?limit=50"
```

**Response:**
```json
{
  "claimable_balances": [
    {
      "balance_id": "0df201fa1b4d55ea53338618f126407dd010de2d4af183b5a827f9d2e915a9da",
      "sponsor": "GD5BHGR576NGTAYDSL4QWMJPNIRGRDY4GBCLBHA4OEFCPMR4XWEEPFDB",
      "asset": {
        "code": "SKY",
        "issuer": "GBNXP4SXWU4BNQPYJOE26R2IHELYQUAFPGIJ3RDITDG3JYHG7H4L3TET",
        "type": "credit_alphanum4"
      },
      "amount": "10.0000000",
      "claimants_count": 2,
      "flags": 0,
      "last_modified_ledger": 341088
    }
  ],
  "count": 1,
  "has_more": false
}
```

---

#### Get Claimable Balances by Asset

```bash
GET /api/v1/silver/claimable-balances/asset?asset={asset}&limit={limit}&cursor={cursor}
```

---

#### Get Single Claimable Balance

```bash
GET /api/v1/silver/claimable-balances/{balance_id}
```

---

### Trade Endpoints

#### List Trades

Returns DEX trade history with optional filtering.

```bash
GET /api/v1/silver/trades?seller_account={account}&buyer_account={account}&selling_asset={asset}&buying_asset={asset}&trade_type={type}&limit={limit}&cursor={cursor}
```

**Parameters:**
| Parameter | Required | Description |
|-----------|----------|-------------|
| `seller_account` | No | Filter by seller account |
| `buyer_account` | No | Filter by buyer account |
| `selling_asset` | No | Filter by sold asset (CODE:ISSUER or XLM) |
| `buying_asset` | No | Filter by bought asset |
| `trade_type` | No | Filter by type: `orderbook` or `liquidity_pool` |
| `limit` | No | Max results (default: 100, max: 1000) |
| `cursor` | No | Pagination cursor |

**Example:**
```bash
curl -H "Authorization: Api-Key $API_KEY" \
  "https://gateway.withobsrvr.com/lake/v1/testnet/api/v1/silver/trades?selling_asset=XLM&buying_asset=USDC:GBBD47IF6LWK7P7MDEVSCWR7DPUWV3NY3DTQEVFL4NAT4AQH3ZLLFLA5&limit=50"
```

**Response:**
```json
{
  "trades": [
    {
      "history_operation_id": 123456789,
      "order": 0,
      "ledger_closed_at": "2026-01-05T12:30:00Z",
      "seller": {
        "account_id": "GABC...",
        "asset": {"code": "XLM", "type": "native"},
        "amount": "100.0000000"
      },
      "buyer": {
        "account_id": "GDEF...",
        "asset": {"code": "USDC", "issuer": "GBBD47...", "type": "credit_alphanum4"},
        "amount": "10.0000000"
      },
      "price": "0.1000000",
      "trade_type": "orderbook"
    }
  ],
  "count": 1,
  "cursor": "MTIzNDU2Nzg5OjA=",
  "has_more": true
}
```

---

### Effects Endpoints

#### List Effects

Returns operation side effects with optional filtering.

```bash
GET /api/v1/silver/effects?account_id={account}&effect_type={type}&limit={limit}&cursor={cursor}
```

**Parameters:**
| Parameter | Required | Description |
|-----------|----------|-------------|
| `account_id` | No | Filter by account |
| `effect_type` | No | Filter by effect type (e.g., `account_credited`, `trade`) |
| `limit` | No | Max results (default: 100, max: 1000) |
| `cursor` | No | Pagination cursor |

**Common Effect Types:**
- `account_created`, `account_removed`
- `account_credited`, `account_debited`
- `trustline_created`, `trustline_updated`, `trustline_removed`
- `trade`, `offer_created`, `offer_removed`
- `data_created`, `data_updated`, `data_removed`

**Example:**
```bash
curl -H "Authorization: Api-Key $API_KEY" \
  "https://gateway.withobsrvr.com/lake/v1/testnet/api/v1/silver/effects?account_id=GAIH3ULLFQ4DGSECF2AR555KZ4KNDGEKN4AFI4SU2M7B43MGK3QJZNSR&limit=50"
```

---

### Soroban Endpoints

#### Get Contract Code Metadata

Returns WASM metadata for deployed contracts.

```bash
GET /api/v1/silver/soroban/contract-code?hash={wasm_hash}
```

**Example:**
```bash
curl -H "Authorization: Api-Key $API_KEY" \
  "https://gateway.withobsrvr.com/lake/v1/testnet/api/v1/silver/soroban/contract-code?hash=3a7b5b6b6ede5f5f17484d10b777e3d7680c24a7b4f6a92cea419469b6e88421"
```

**Response:**
```json
{
  "contract_code": {
    "hash": "3a7b5b6b6ede5f5f17484d10b777e3d7680c24a7b4f6a92cea419469b6e88421",
    "metrics": {
      "n_functions": 132,
      "n_instructions": 21604,
      "n_data_segments": 3,
      "n_exports": 42,
      "n_imports": 36
    },
    "last_modified_ledger": 338326
  }
}
```

---

#### Get TTL Entry

Returns Time-To-Live info for contract storage entries.

```bash
GET /api/v1/silver/soroban/ttl?key_hash={key_hash}
```

---

#### Get Expiring TTL Entries

Returns entries expiring within N ledgers (critical for monitoring).

```bash
GET /api/v1/silver/soroban/ttl/expiring?within_ledgers={n}&limit={limit}&cursor={cursor}
```

**Example:**
```bash
curl -H "Authorization: Api-Key $API_KEY" \
  "https://gateway.withobsrvr.com/lake/v1/testnet/api/v1/silver/soroban/ttl/expiring?within_ledgers=100000&limit=50"
```

**Response:**
```json
{
  "ttl_entries": [
    {
      "key_hash": "04513904c5115e1ea69b0ce47ad264ebd95e791f5e9a29515a09b073a0e2ac63",
      "live_until_ledger": 338630,
      "ledgers_remaining": 5000,
      "expired": false,
      "last_modified_ledger": 337911
    }
  ],
  "count": 5,
  "within_ledgers": 100000,
  "has_more": true,
  "cursor": "MzM4NjMwOmIyMTA5NjJm..."
}
```

---

#### Get Expired TTL Entries

```bash
GET /api/v1/silver/soroban/ttl/expired?limit={limit}&cursor={cursor}
```

---

#### Get Evicted Keys

Returns contract data entries that expired and were evicted.

```bash
GET /api/v1/silver/soroban/evicted-keys?contract_id={contract_id}&limit={limit}&cursor={cursor}
```

---

#### Get Restored Keys

Returns contract data entries that were restored after eviction.

```bash
GET /api/v1/silver/soroban/restored-keys?contract_id={contract_id}&limit={limit}&cursor={cursor}
```

---

#### Get Soroban Network Config

Returns Soroban network configuration parameters.

```bash
GET /api/v1/silver/soroban/config
```

**Response:**
```json
{
  "config": {
    "instructions": {
      "ledger_max": 100000000,
      "tx_max": 100000000,
      "fee_rate_per_increment": 25
    },
    "memory": {
      "tx_limit_bytes": 41943040
    },
    "ledger_limits": {
      "max_read_entries": 200,
      "max_read_bytes": 200000,
      "max_write_entries": 100,
      "max_write_bytes": 66560
    },
    "contract": {
      "max_size_bytes": 65536
    }
  }
}
```

---

#### Get Soroban Config Limits (Simplified)

```bash
GET /api/v1/silver/soroban/config/limits
```

---

#### Get Contract Data

Returns contract storage entries.

```bash
GET /api/v1/silver/soroban/contract-data?contract_id={contract_id}&durability={durability}&limit={limit}&cursor={cursor}
```

**Parameters:**
| Parameter | Required | Description |
|-----------|----------|-------------|
| `contract_id` | Yes | Contract address (C...) |
| `durability` | No | Filter: `persistent` or `temporary` |
| `limit` | No | Max results (default: 100, max: 1000) |
| `cursor` | No | Pagination cursor |

**Example:**
```bash
curl -H "Authorization: Api-Key $API_KEY" \
  "https://gateway.withobsrvr.com/lake/v1/testnet/api/v1/silver/soroban/contract-data?contract_id=CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG6QB3RVFT5RMCDH74N2&limit=50"
```

---

#### Get Single Contract Data Entry

```bash
GET /api/v1/silver/soroban/contract-data/entry?contract_id={contract_id}&key_hash={key_hash}
```

---

### Block Explorer Endpoints

#### Get Account Overview

Returns comprehensive account overview for block explorer display.

```bash
GET /api/v1/silver/explorer/account?account_id={account_id}
```

**Response includes:**
- Current account state
- 10 most recent operations
- 10 most recent transfers (sent + received)

---

#### Get Transaction Details

Returns full transaction details with all operations.

```bash
GET /api/v1/silver/explorer/transaction?tx_hash={tx_hash}
```

---

#### Get Asset Overview

Returns asset statistics and recent transfers.

```bash
GET /api/v1/silver/explorer/asset?asset_code={code}&asset_issuer={issuer}
```

---

## Index Plane Endpoints

Fast lookup endpoints optimized for sub-100ms queries.

### Transaction Lookup

#### Fast Transaction Lookup by Hash

```bash
GET /transactions/{tx_hash}
GET /api/v1/index/transactions/{tx_hash}
```

**Example:**
```bash
curl -H "Authorization: Api-Key $API_KEY" \
  "https://gateway.withobsrvr.com/lake/v1/testnet/transactions/abc123..."
```

---

#### Batch Transaction Lookup

```bash
POST /api/v1/index/transactions/lookup
```

**Body:**
```json
{
  "hashes": ["abc123...", "def456...", "ghi789..."]
}
```

**Limits:** Maximum 1000 hashes per request

---

#### Index Health

Returns index coverage statistics.

```bash
GET /api/v1/index/health
```

---

### Contract Event Index

#### Get Ledgers for Contract

Returns ledgers containing events from a specific contract.

```bash
GET /api/v1/index/contracts/{contract_id}/ledgers?start_ledger={ledger}&end_ledger={ledger}&limit={limit}
```

**Parameters:**
| Parameter | Required | Description |
|-----------|----------|-------------|
| `contract_id` | Yes | Contract address (C...) or hex hash |
| `start_ledger` | No | Minimum ledger sequence |
| `end_ledger` | No | Maximum ledger sequence |
| `limit` | No | Max results (default: 1000, max: 10000) |

**Example:**
```bash
curl -H "Authorization: Api-Key $API_KEY" \
  "https://gateway.withobsrvr.com/lake/v1/testnet/api/v1/index/contracts/CAUGJT4.../ledgers?limit=100"
```

---

#### Get Contract Event Summary

Returns detailed event information for a contract.

```bash
GET /api/v1/index/contracts/{contract_id}/summary?start_ledger={ledger}&end_ledger={ledger}
```

---

#### Batch Contract Lookup

```bash
POST /api/v1/index/contracts/lookup
```

**Body:**
```json
{
  "contract_ids": ["CABC...", "CDEF...", "CGHI..."]
}
```

**Limits:** Maximum 100 contracts per request

---

#### Contract Index Health

```bash
GET /api/v1/index/contracts/health
```

---

## Bronze Layer Endpoints

Raw blockchain data mirroring Stellar's native data model.

### Available Tables

```bash
GET /api/v1/bronze/ledgers
GET /api/v1/bronze/transactions
GET /api/v1/bronze/operations
GET /api/v1/bronze/effects
GET /api/v1/bronze/trades
GET /api/v1/bronze/accounts
GET /api/v1/bronze/trustlines
GET /api/v1/bronze/offers
GET /api/v1/bronze/contract_events
```

All Bronze endpoints support:
- `limit` - Max results
- `cursor` - Pagination cursor
- `start_ledger` / `end_ledger` - Ledger range filters
- Table-specific filters (e.g., `account_id`, `asset_code`)

---

## Pagination

All list endpoints support cursor-based pagination:

```bash
# First page
RESPONSE=$(curl -s -H "Authorization: Api-Key $API_KEY" \
  "https://gateway.withobsrvr.com/lake/v1/testnet/api/v1/silver/transfers?limit=100")

# Check for more pages
HAS_MORE=$(echo "$RESPONSE" | jq '.has_more')
CURSOR=$(echo "$RESPONSE" | jq -r '.cursor')

# Get next page
if [ "$HAS_MORE" = "true" ]; then
  curl -H "Authorization: Api-Key $API_KEY" \
    "https://gateway.withobsrvr.com/lake/v1/testnet/api/v1/silver/transfers?limit=100&cursor=$CURSOR"
fi
```

---

## Horizon API Equivalents

| What you want | Horizon Endpoint | Obsrvr Lake Equivalent |
|---------------|------------------|------------------------|
| Account details | `GET /accounts/{id}` | `/silver/accounts/current?account_id={id}` |
| Account balances | `GET /accounts/{id}` (balances field) | `/silver/accounts/{id}/balances` |
| Account signers | `GET /accounts/{id}` (signers field) | `/silver/accounts/signers?account_id={id}` |
| Account operations | `GET /accounts/{id}/operations` | `/silver/operations/enriched?account_id={id}` |
| Account payments | `GET /accounts/{id}/payments` | `/silver/payments?account_id={id}` |
| Account effects | `GET /accounts/{id}/effects` | `/bronze/effects?account_id={id}` |
| Transaction details | `GET /transactions/{hash}` | `/transactions/{hash}` |
| Transaction operations | `GET /transactions/{hash}/operations` | `/silver/operations/enriched?tx_hash={hash}` |
| Ledger info | `GET /ledgers/{sequence}` | `/bronze/ledgers?sequence={sequence}` |
| All accounts | `GET /accounts` | `/silver/accounts` |
| Asset holders | `GET /accounts?asset=CODE:ISSUER` | `/silver/assets/CODE:ISSUER/holders` |
| Asset stats | N/A | `/silver/assets/CODE:ISSUER/stats` |
| Order book | `GET /order_book` | `/bronze/offers` (with filters) |

---

## Error Responses

All endpoints return errors in a consistent format:

```json
{
  "error": "account_id required"
}
```

Common HTTP status codes:
- `400` - Bad request (invalid parameters)
- `404` - Resource not found
- `500` - Internal server error

---

## Rate Limits

- Default: 100 requests/minute per API key
- Batch endpoints: Count as 1 request regardless of batch size
- Contact support for higher limits
