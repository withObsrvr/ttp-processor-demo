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
GET /api/v1/silver/operations/enriched?account_id={account_id}&tx_hash={hash}&limit={limit}&cursor={cursor}&order={order}
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
| `order` | No | Sort order: `asc` or `desc` (default: desc) |

**Response includes `_meta` with data boundaries:**
```json
{
  "operations": [...],
  "_meta": {
    "scanned_ledger": 829045,
    "available_ledgers": {"oldest": 277, "latest": 829045}
  }
}
```

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
GET /api/v1/silver/payments?account_id={account_id}&limit={limit}&cursor={cursor}&order={order}
```

> **Note:** Supports `order=asc` or `order=desc` (default: desc). Response includes `_meta` with data boundaries.

**Example:**
```bash
curl -H "Authorization: Api-Key $API_KEY" \
  "https://gateway.withobsrvr.com/lake/v1/testnet/api/v1/silver/payments?account_id=GAIH3ULLFQ4DGSECF2AR555KZ4KNDGEKN4AFI4SU2M7B43MGK3QJZNSR"
```

---

#### Get Soroban Operations Only

Convenience endpoint that returns only Soroban smart contract operations. Response includes Soroban-specific fields: `contract_id`, `function_name`, and `arguments_json` (when available).

```bash
GET /api/v1/silver/operations/soroban?account_id={account_id}&limit={limit}&cursor={cursor}&order={order}
```

> **Note:** Supports `order=asc` or `order=desc` (default: desc). Response includes `_meta` with data boundaries. Soroban operations now include `contract_id`, `function_name`, and `arguments_json` fields for contract invocations.

**Example:**
```bash
curl -H "Authorization: Api-Key $API_KEY" \
  "https://gateway.withobsrvr.com/lake/v1/testnet/api/v1/silver/operations/soroban?account_id=GAIH3ULLFQ4DGSECF2AR555KZ4KNDGEKN4AFI4SU2M7B43MGK3QJZNSR"
```

---

#### Get Soroban Operations by Function

Filter Soroban operations by contract ID and/or function name. At least one of `contract_id` or `function_name` is required.

```bash
GET /api/v1/silver/operations/soroban/by-function?contract_id={contract_id}&function_name={function_name}&limit={limit}&cursor={cursor}&order={order}
```

> **Alias:** This endpoint is also available as `GET /api/v1/silver/calls` with the same parameters.

**Parameters:**
| Parameter | Required | Description |
|-----------|----------|-------------|
| `contract_id` | No* | Filter by contract ID (C...) |
| `function_name` | No* | Filter by function name (e.g., `swap`, `transfer`) |
| `limit` | No | Max results (default: 50, max: 500) |
| `cursor` | No | Pagination cursor |
| `order` | No | Sort order: `asc` or `desc` (default: desc) |

> *At least one of `contract_id` or `function_name` is required.

**Example:**
```bash
# Get all swap calls on a specific contract
curl -H "Authorization: Api-Key $API_KEY" \
  "https://gateway.withobsrvr.com/lake/v1/testnet/api/v1/silver/operations/soroban/by-function?contract_id=CAUGJT4...&function_name=swap&limit=20"

# Get all transfer function calls across all contracts
curl -H "Authorization: Api-Key $API_KEY" \
  "https://gateway.withobsrvr.com/lake/v1/testnet/api/v1/silver/operations/soroban/by-function?function_name=transfer&limit=50"
```

**Response:**
```json
{
  "contract_id": "CAUGJT4...",
  "function_name": "swap",
  "soroban_operations": [
    {
      "transaction_hash": "abc123...",
      "ledger_sequence": 830000,
      "ledger_closed_at": "2026-02-20T12:30:00Z",
      "source_account": "GABC...",
      "type": 24,
      "type_name": "INVOKE_HOST_FUNCTION",
      "contract_id": "CAUGJT4...",
      "function_name": "swap",
      "arguments_json": "[...]",
      "is_soroban_op": true,
      "tx_successful": true
    }
  ],
  "count": 20,
  "has_more": true,
  "cursor": "..."
}
```

---

### Token Transfer Endpoints

#### Get Token Transfers

Returns unified view of all token movements (payments, path payments, claimable balance claims).

```bash
GET /api/v1/silver/transfers?from_account={account}&to_account={account}&asset_code={code}&source_type={type}&limit={limit}&cursor={cursor}&order={order}
```

> **Note:** Supports `order=asc` or `order=desc` (default: desc). Response includes `_meta` with data boundaries.

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

#### List All Assets

Returns a paginated list of all assets on the network with holder counts, supply, and volume metrics. Use this to discover assets for a CoinMarketCap-style asset list.

```bash
GET /api/v1/silver/assets?limit={limit}&sort_by={field}&order={order}
```

**Query Parameters:**
| Parameter | Required | Default | Description |
|-----------|----------|---------|-------------|
| `limit` | No | 100 | Max results (max: 1000) |
| `cursor` | No | - | Pagination cursor |
| `sort_by` | No | `holder_count` | Sort field: `holder_count`, `volume_24h`, `transfers_24h`, `circulating_supply` |
| `order` | No | `desc` | Sort order: `asc` or `desc` |
| `search` | No | - | Filter by asset code prefix |
| `min_holders` | No | - | Minimum holder count filter |
| `min_volume_24h` | No | - | Minimum 24h volume in stroops |
| `asset_type` | No | - | Filter by type: `credit_alphanum4`, `credit_alphanum12`, `native` |

**Examples:**
```bash
# Top 20 assets by holder count
curl -H "Authorization: Api-Key $API_KEY" \
  "https://gateway.withobsrvr.com/lake/v1/testnet/api/v1/silver/assets?limit=20&sort_by=holder_count"

# Search for USDC variants
curl -H "Authorization: Api-Key $API_KEY" \
  "https://gateway.withobsrvr.com/lake/v1/testnet/api/v1/silver/assets?search=USDC"

# Assets with at least 5 holders
curl -H "Authorization: Api-Key $API_KEY" \
  "https://gateway.withobsrvr.com/lake/v1/testnet/api/v1/silver/assets?min_holders=5"
```

**Response:**
```json
{
  "assets": [
    {
      "asset_code": "DSD",
      "asset_issuer": "GAVRROR6AWJGKOHWLKUGO3OR3UHZ47GBXHHW6PBIVPG55VBMVD3TMLNA",
      "asset_type": "credit_alphanum4",
      "holder_count": 85,
      "circulating_supply": "299761.3000000",
      "volume_24h": "0.0000000",
      "transfers_24h": 0,
      "top_10_concentration": 0,
      "first_seen": "2026-01-12T08:32:11Z",
      "last_activity": "2026-01-12T19:43:02Z"
    },
    {
      "asset_code": "USDC",
      "asset_issuer": "GBBD47IF6LWK7P7MDEVSCWR7DPUWV3NY3DTQEVFL4NAT4AQH3ZLLFLA5",
      "asset_type": "credit_alphanum4",
      "holder_count": 16,
      "circulating_supply": "30532569877.3492241",
      "volume_24h": "0.0000000",
      "transfers_24h": 0,
      "top_10_concentration": 0,
      "first_seen": "2026-01-12T17:33:37Z",
      "last_activity": "2026-01-12T19:42:57Z"
    }
  ],
  "total_assets": 10,
  "cursor": "MjowOmhvbGRlcl9jb3VudDpkZXNjOlNTTFg6R0JL...",
  "has_more": true,
  "generated_at": "2026-01-12T19:43:03Z"
}
```

---

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

Returns recent calls involving a specific contract. Queries both hot (recent) and cold (historical) storage, so results include the full call history back to 2024.

```bash
GET /api/v1/silver/contracts/{contract_id}/recent-calls?limit={limit}&cursor={cursor}
```

**Parameters:**
| Parameter | Required | Description |
|-----------|----------|-------------|
| `contract_id` | Yes | Contract address (C...) in URL path |
| `limit` | No | Max results (default: 50) |
| `cursor` | No | Pagination cursor for keyset pagination |

**Example:**
```bash
curl -H "Authorization: Api-Key $API_KEY" \
  "https://gateway.withobsrvr.com/lake/v1/testnet/api/v1/silver/contracts/CAUGJT4.../recent-calls?limit=50"
```

> **Note:** This endpoint now uses UNION ALL across hot and cold storage, providing complete history with cursor-based pagination.

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

### CAP-67 Unified Event Stream

Exposes the full CAP-67 unified event stream. Events are derived from `token_transfers_raw` — event type is inferred from sender/recipient nullity: `NULL from = mint`, `NULL to = burn`, otherwise `transfer`.

#### Get Unified Events

Returns the unified event stream with optional filters.

```bash
GET /api/v1/silver/events
```

**Parameters:**
| Parameter | Required | Description |
|-----------|----------|-------------|
| `contract_id` | No | Filter by token contract ID (C...) |
| `event_type` | No | Filter by type: `transfer`, `mint`, `burn` |
| `source_type` | No | Filter by source: `classic` or `soroban` |
| `start_ledger` | No | Start of ledger range |
| `end_ledger` | No | End of ledger range |
| `limit` | No | Max results (default: 20, max: 200) |
| `cursor` | No | Pagination cursor |
| `order` | No | Sort order: `asc` or `desc` (default: desc) |

**Example:**
```bash
curl -H "Authorization: Api-Key $API_KEY" \
  "https://gateway.withobsrvr.com/lake/v1/testnet/api/v1/silver/events?event_type=mint&limit=10"
```

**Response:**
```json
{
  "events": [
    {
      "event_id": "830000-abc123-0",
      "contract_id": "CDLZFC3...",
      "ledger_sequence": 830000,
      "tx_hash": "abc123...",
      "closed_at": "2026-02-20T12:30:00Z",
      "event_type": "mint",
      "from": null,
      "to": "GXYZ...",
      "amount": "1000.0000000",
      "asset_code": "USDC",
      "asset_issuer": "GBBD47...",
      "source_type": "soroban",
      "event_index": 0
    }
  ],
  "count": 1,
  "has_more": true,
  "next_cursor": "eyJsIjo4MzAwMDAsInQiOiJhYmMxMjMiLC..."
}
```

---

#### Get Events by Contract

Returns events for a specific token contract.

```bash
GET /api/v1/silver/events/by-contract?contract_id={contract_id}
```

**Parameters:**
| Parameter | Required | Description |
|-----------|----------|-------------|
| `contract_id` | Yes | Token contract ID (C...) |
| `event_type` | No | Filter by type: `transfer`, `mint`, `burn` |
| `limit` | No | Max results (default: 20, max: 200) |
| `cursor` | No | Pagination cursor |
| `order` | No | Sort order: `asc` or `desc` (default: desc) |

**Example:**
```bash
curl -H "Authorization: Api-Key $API_KEY" \
  "https://gateway.withobsrvr.com/lake/v1/testnet/api/v1/silver/events/by-contract?contract_id=CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG6QB3RVFT5RMCDH74N2&limit=20"
```

**Response:**
```json
{
  "contract_id": "CDLZFC3...",
  "events": [...],
  "count": 20,
  "has_more": true,
  "next_cursor": "..."
}
```

---

#### Get Address Events

Returns all events where an address appears as sender or receiver.

```bash
GET /api/v1/silver/address/{addr}/events
```

**Parameters:**
| Parameter | Required | Description |
|-----------|----------|-------------|
| `addr` | Yes | Stellar account or contract address (in URL path) |
| `event_type` | No | Filter by type: `transfer`, `mint`, `burn` |
| `source_type` | No | Filter by source: `classic` or `soroban` |
| `limit` | No | Max results (default: 20, max: 200) |
| `cursor` | No | Pagination cursor |

**Example:**
```bash
curl -H "Authorization: Api-Key $API_KEY" \
  "https://gateway.withobsrvr.com/lake/v1/testnet/api/v1/silver/address/GAIH3ULLFQ4DGSECF2AR555KZ4KNDGEKN4AFI4SU2M7B43MGK3QJZNSR/events?limit=20"
```

**Response:**
```json
{
  "address": "GAIH3ULLFQ4DGSECF2AR555KZ4KNDGEKN4AFI4SU2M7B43MGK3QJZNSR",
  "events": [...],
  "count": 20,
  "has_more": true,
  "next_cursor": "..."
}
```

---

#### Get Transaction Events

Returns all events for a specific transaction.

```bash
GET /api/v1/silver/tx/{hash}/events
```

**Example:**
```bash
curl -H "Authorization: Api-Key $API_KEY" \
  "https://gateway.withobsrvr.com/lake/v1/testnet/api/v1/silver/tx/abc123.../events"
```

**Response:**
```json
{
  "transaction_hash": "abc123...",
  "events": [
    {
      "event_id": "830000-abc123-0",
      "event_type": "transfer",
      "from": "GABC...",
      "to": "GDEF...",
      "amount": "100.0000000",
      "asset_code": "USDC",
      "source_type": "soroban"
    }
  ],
  "count": 1
}
```

---

### SEP-41 Token API

Query SEP-41 fungible tokens — metadata, balances, transfers, and holder statistics. Balances are computed from transfer history (total received minus total sent).

#### Get Token Metadata

Returns metadata for a SEP-41 token by contract ID.

```bash
GET /api/v1/silver/tokens/{contract_id}
```

**Example:**
```bash
curl -H "Authorization: Api-Key $API_KEY" \
  "https://gateway.withobsrvr.com/lake/v1/testnet/api/v1/silver/tokens/CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG6QB3RVFT5RMCDH74N2"
```

**Response:**
```json
{
  "contract_id": "CDLZFC3...",
  "asset_code": "USDC",
  "asset_issuer": "GBBD47...",
  "source_type": "soroban",
  "holder_count": 256,
  "transfer_count": 14500,
  "first_seen": "2025-11-01T00:00:00Z",
  "last_activity": "2026-02-20T12:30:00Z"
}
```

---

#### Get Token Balances

Returns computed balances for all holders of a token, ranked by balance.

```bash
GET /api/v1/silver/tokens/{contract_id}/balances?limit={limit}&cursor={cursor}
```

**Parameters:**
| Parameter | Required | Description |
|-----------|----------|-------------|
| `contract_id` | Yes | Token contract ID (C...) in URL path |
| `limit` | No | Max results (default: 20, max: 200) |
| `cursor` | No | Pagination cursor |

**Example:**
```bash
curl -H "Authorization: Api-Key $API_KEY" \
  "https://gateway.withobsrvr.com/lake/v1/testnet/api/v1/silver/tokens/CDLZFC3.../balances?limit=20"
```

**Response:**
```json
{
  "contract_id": "CDLZFC3...",
  "balances": [
    {
      "address": "GABC...",
      "balance": "500000.0000000",
      "balance_raw": 5000000000000,
      "total_received": 7500000000000,
      "total_sent": 2500000000000,
      "tx_count": 42,
      "last_activity_ledger": 829800,
      "last_activity": "2026-02-20T12:00:00Z"
    }
  ],
  "count": 20,
  "has_more": true,
  "next_cursor": "..."
}
```

> **Note:** Balances are derived from transfer history (received - sent), not contract state. This provides a full audit trail but may differ from on-chain state for contracts with non-standard accounting.

---

#### Get Single Balance

Returns the balance of a specific address for a token.

```bash
GET /api/v1/silver/tokens/{contract_id}/balance/{address}
```

**Example:**
```bash
curl -H "Authorization: Api-Key $API_KEY" \
  "https://gateway.withobsrvr.com/lake/v1/testnet/api/v1/silver/tokens/CDLZFC3.../balance/GABC..."
```

**Response:**
```json
{
  "contract_id": "CDLZFC3...",
  "balance": {
    "address": "GABC...",
    "balance": "500000.0000000",
    "balance_raw": 5000000000000,
    "total_received": 7500000000000,
    "total_sent": 2500000000000,
    "tx_count": 42,
    "last_activity_ledger": 829800,
    "last_activity": "2026-02-20T12:00:00Z"
  }
}
```

---

#### Get Token Transfers

Returns transfer history for a specific token.

```bash
GET /api/v1/silver/tokens/{contract_id}/transfers?limit={limit}&cursor={cursor}
```

**Parameters:**
| Parameter | Required | Description |
|-----------|----------|-------------|
| `contract_id` | Yes | Token contract ID (C...) in URL path |
| `event_type` | No | Filter by type: `transfer`, `mint`, `burn` |
| `limit` | No | Max results (default: 20, max: 200) |
| `cursor` | No | Pagination cursor |
| `order` | No | Sort order: `asc` or `desc` (default: desc) |

**Example:**
```bash
curl -H "Authorization: Api-Key $API_KEY" \
  "https://gateway.withobsrvr.com/lake/v1/testnet/api/v1/silver/tokens/CDLZFC3.../transfers?limit=50"
```

**Response:**
```json
{
  "contract_id": "CDLZFC3...",
  "transfers": [
    {
      "event_id": "830000-abc123-0",
      "event_type": "transfer",
      "from": "GABC...",
      "to": "GDEF...",
      "amount": "100.0000000",
      "asset_code": "USDC",
      "source_type": "soroban",
      "tx_hash": "abc123...",
      "ledger_sequence": 830000,
      "closed_at": "2026-02-20T12:30:00Z"
    }
  ],
  "count": 50,
  "has_more": true,
  "next_cursor": "..."
}
```

---

#### Get Token Stats

Returns aggregate statistics for a token.

```bash
GET /api/v1/silver/tokens/{contract_id}/stats
```

**Example:**
```bash
curl -H "Authorization: Api-Key $API_KEY" \
  "https://gateway.withobsrvr.com/lake/v1/testnet/api/v1/silver/tokens/CDLZFC3.../stats"
```

**Response:**
```json
{
  "contract_id": "CDLZFC3...",
  "holder_count": 256,
  "total_supply": "10000000.0000000",
  "total_supply_raw": 100000000000000,
  "transfers_24h": 150,
  "volume_24h": "250000.0000000",
  "volume_24h_raw": 2500000000000,
  "asset_code": "USDC"
}
```

---

#### Get Address Token Portfolio

Returns all token holdings for an address across all SEP-41 tokens.

```bash
GET /api/v1/silver/address/{addr}/token-balances
```

**Example:**
```bash
curl -H "Authorization: Api-Key $API_KEY" \
  "https://gateway.withobsrvr.com/lake/v1/testnet/api/v1/silver/address/GAIH3ULLFQ4DGSECF2AR555KZ4KNDGEKN4AFI4SU2M7B43MGK3QJZNSR/token-balances"
```

**Response:**
```json
{
  "address": "GAIH3ULLFQ4DGSECF2AR555KZ4KNDGEKN4AFI4SU2M7B43MGK3QJZNSR",
  "holdings": [
    {
      "contract_id": "CDLZFC3...",
      "asset_code": "USDC",
      "asset_issuer": "GBBD47...",
      "source_type": "soroban",
      "balance": "500000.0000000",
      "balance_raw": 5000000000000,
      "tx_count": 42,
      "last_activity": "2026-02-20T12:00:00Z"
    }
  ],
  "count": 3
}
```

> **Tip:** Combine with `/silver/accounts/{id}/balances` to get a complete view of an address's holdings (XLM + trustlines + SEP-41 tokens).

---

### SEP-50 NFT API (Coming Soon)

The following endpoints are reserved for SEP-50 non-fungible token support and will be available when the standard is finalized:

- `GET /silver/nfts/{contract_id}` — Collection metadata
- `GET /silver/nfts/{contract_id}/tokens` — Token list
- `GET /silver/nfts/{contract_id}/tokens/{token_id}` — Single token
- `GET /silver/nfts/{contract_id}/transfers` — Transfer history

---

### Transaction Decoding

Human-readable transaction summaries, contract interface detection, and ScVal decoding.

#### Get Decoded Transaction

Returns a human-readable decoded version of a transaction with summary, operations, and events.

```bash
GET /api/v1/silver/tx/{hash}/decoded
```

**Example:**
```bash
curl -H "Authorization: Api-Key $API_KEY" \
  "https://gateway.withobsrvr.com/lake/v1/testnet/api/v1/silver/tx/abc123.../decoded"
```

**Response:**
```json
{
  "tx_hash": "abc123...",
  "summary": {
    "description": "Swapped 100.0000000 USDC for 48.5000000 XLM",
    "type": "swap",
    "involved_contracts": ["CDEF..."],
    "swap": {
      "sold_asset": "USDC",
      "sold_amount": "100.0000000",
      "bought_asset": "XLM",
      "bought_amount": "48.5000000",
      "router": "CDEF...",
      "trader": "GABC..."
    }
  },
  "fee": 100,
  "ledger_sequence": 830000,
  "closed_at": "2026-02-20T12:30:00Z",
  "successful": true,
  "operation_count": 1,
  "operations": [
    {
      "index": 0,
      "type": 24,
      "type_name": "INVOKE_HOST_FUNCTION",
      "source_account": "GABC...",
      "contract_id": "CDEF...",
      "function_name": "swap",
      "arguments_json": "[...]",
      "is_soroban_op": true
    }
  ],
  "events": [
    {
      "event_id": "830000-abc123-0",
      "event_type": "transfer",
      "from": "GABC...",
      "to": "CDEF...",
      "amount": "100.0000000",
      "asset_code": "USDC"
    }
  ]
}
```

**Summary Types:**
| Type | Pattern | Example |
|------|---------|---------|
| `transfer` | SEP-41 transfer detected | "Transferred 100 USDC to GXYZ..." |
| `mint` | Mint detected (no sender) | "Minted 100 USDC to GXYZ..." |
| `burn` | Burn detected (no receiver) | "Burned 50 USDC from GABC..." |
| `swap` | Two counter-directional transfers | "Swapped 100 USDC for 48.5 XLM..." |
| `contract_call` | Generic contract invocation | "Called swap on contract CDEF..." |
| `classic` | Non-Soroban operation | "Payment of 100 XLM to GXYZ..." |

**Structured Detail Fields:**

When `type` is `swap`, the `swap` field is populated:
| Field | Type | Description |
|-------|------|-------------|
| `sold_asset` | string | Asset sold |
| `sold_amount` | string | Amount sold |
| `bought_asset` | string | Asset received |
| `bought_amount` | string | Amount received |
| `router` | string | Router contract (first involved contract) |
| `trader` | string | Address that initiated the swap |

When `type` is `transfer`, `mint`, or `burn`, the corresponding field is populated:
| Field | Type | Description |
|-------|------|-------------|
| `asset` | string | Asset code |
| `amount` | string | Amount |
| `from` | string | Sender address (omitted for mints) |
| `to` | string | Receiver address (omitted for burns) |

---

#### Get Full Transaction Analysis

Returns a composite view combining the decoded transaction, contracts involved, and contract call graph in a single request. Ideal for transaction detail pages.

```bash
GET /api/v1/silver/tx/{hash}/full
```

**Example:**
```bash
curl -H "Authorization: Api-Key $API_KEY" \
  "https://gateway.withobsrvr.com/lake/v1/testnet/api/v1/silver/tx/abc123.../full"
```

**Response:**
```json
{
  "transaction": {
    "tx_hash": "abc123...",
    "ledger_sequence": 830000,
    "closed_at": "2026-02-20T12:30:00Z",
    "successful": true,
    "fee": 100,
    "operation_count": 1
  },
  "summary": {
    "description": "Swapped 100 USDC for 48.5 XLM",
    "type": "swap",
    "involved_contracts": ["CDEF..."],
    "swap": { "sold_asset": "USDC", "sold_amount": "100.0000000", "bought_asset": "XLM", "bought_amount": "48.5000000", "trader": "GABC...", "router": "CDEF..." }
  },
  "operations": [ ... ],
  "events": [ ... ],
  "contracts_involved": ["CDEF...", "CXYZ..."],
  "call_graph": [
    {
      "from_contract": "GABC...",
      "to_contract": "CDEF...",
      "function_name": "swap",
      "call_depth": 0,
      "execution_order": 1,
      "successful": true
    }
  ]
}
```

> **Tip:** This endpoint replaces the need to call `/tx/{hash}/decoded`, `/tx/{hash}/contracts-involved`, and `/tx/{hash}/call-graph` separately.

---

#### Get Contract Interface

Returns the detected interface (SEP-41 or unknown) for a contract based on observed function calls.

```bash
GET /api/v1/silver/contracts/{id}/interface
```

**Example:**
```bash
curl -H "Authorization: Api-Key $API_KEY" \
  "https://gateway.withobsrvr.com/lake/v1/testnet/api/v1/silver/contracts/CDLZFC3.../interface"
```

**Response (SEP-41 detected):**
```json
{
  "contract_id": "CDLZFC3...",
  "detected_type": "sep41_token",
  "interface": [
    {"name": "transfer", "params": ["from", "to", "amount"]},
    {"name": "balance", "params": ["id"]},
    {"name": "decimals", "params": []},
    {"name": "name", "params": []},
    {"name": "symbol", "params": []}
  ],
  "observed_functions": ["transfer", "balance", "approve", "decimals"]
}
```

**Response (unknown contract):**
```json
{
  "contract_id": "CXYZ...",
  "detected_type": "unknown",
  "observed_functions": ["swap", "add_liquidity", "remove_liquidity"]
}
```

> **Note:** SEP-41 detection requires at least 3 of 5 standard SEP-41 function signatures to be observed in historical calls.

---

#### Decode ScVal

Decodes a Soroban ScVal from XDR (base64) or JSON into a human-readable format.

```bash
POST /api/v1/silver/decode/scval
```

**Request Body:**
```json
{
  "xdr": "AAAAEQAAAAEAAAAGAAAADwAAAARuYW1l...",
  "type_hint": "i128"
}
```

Or with JSON input:
```json
{
  "json": {"type": "i128", "lo": "1000000", "hi": "0"}
}
```

**Response:**
```json
{
  "type": "i128",
  "value": "1000000",
  "display": "1000000"
}
```

**Supported ScVal Types:** bool, u32, i32, u64, i64, u128, i128, address, symbol, string, bytes, vec, map

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

#### Get Data Boundaries

Returns the available ledger range in the data store. Useful for understanding data freshness.

```bash
GET /api/v1/silver/data-boundaries
```

**Example:**
```bash
curl -H "Authorization: Api-Key $API_KEY" \
  "https://gateway.withobsrvr.com/lake/v1/testnet/api/v1/silver/data-boundaries"
```

**Response:**
```json
{
  "available_ledgers": {
    "oldest": 277,
    "latest": 829045
  },
  "generated_at": "2026-02-03T18:05:58Z"
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
GET /api/v1/silver/trades?seller_account={account}&buyer_account={account}&selling_asset={asset}&buying_asset={asset}&trade_type={type}&limit={limit}&cursor={cursor}&order={order}
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
| `order` | No | Sort order: `asc` or `desc` (default: asc) |

> **Note:** Trades default to ascending order (oldest first). Response includes `_meta` with data boundaries.

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
GET /api/v1/silver/effects?account_id={account}&effect_type={type}&limit={limit}&cursor={cursor}&order={order}
```

**Parameters:**
| Parameter | Required | Description |
|-----------|----------|-------------|
| `account_id` | No | Filter by account |
| `effect_type` | No | Filter by effect type (e.g., `account_credited`, `trade`) |
| `limit` | No | Max results (default: 100, max: 1000) |
| `cursor` | No | Pagination cursor |
| `order` | No | Sort order: `asc` or `desc` (default: asc) |

> **Note:** Effects default to ascending order (oldest first). Response includes `_meta` with data boundaries.

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

## Gold Layer Endpoints

Compliance-ready, audit-grade data exports with checksums and reproducibility guarantees.

### Gold Snapshot Endpoints

Point-in-time snapshots for assets and accounts.

#### Get Asset Balance Snapshot

Returns all holders of an asset at a specific point in time with supply statistics.

```bash
GET /api/v1/gold/snapshots/balance?asset_code={code}&asset_issuer={issuer}&timestamp={iso8601}&limit={limit}
```

**Parameters:**
| Parameter | Required | Description |
|-----------|----------|-------------|
| `asset_code` | Yes | Asset code (XLM, USDC, etc.) |
| `asset_issuer` | No | Issuer account (empty for XLM) |
| `timestamp` | Yes | Point-in-time snapshot (ISO8601 format) |
| `limit` | No | Max results (default: 1000, max: 10000) |

**Example:**
```bash
# Get XLM holders at a specific timestamp
curl -H "Authorization: Api-Key $API_KEY" \
  "https://gateway.withobsrvr.com/lake/v1/testnet/api/v1/gold/snapshots/balance?asset_code=XLM&timestamp=2026-01-12T23:59:59Z&limit=100"
```

**Response:**
```json
{
  "asset": {"code": "XLM", "type": "native"},
  "snapshot_at": "2026-01-12T23:59:59Z",
  "snapshot_ledger": 445620,
  "summary": {
    "total_holders": 2402,
    "total_supply": "96876575117.7241628",
    "circulating_supply": "96876575117.7241628"
  },
  "holders": [
    {
      "account_id": "GBFAIH5WKAJQ77NG6BZG7TGVGXHPX4SQLIJ7BENJMCVCZSUZPSISCLU5",
      "balance": "90321229899.5872227",
      "percent_of_supply": "93.23"
    }
  ]
}
```

---

#### Get Account Snapshot

Returns an account's state at a specific point in time.

```bash
GET /api/v1/gold/snapshots/account?account_id={account_id}&timestamp={iso8601}
```

**Parameters:**
| Parameter | Required | Description |
|-----------|----------|-------------|
| `account_id` | Yes | Stellar account address (G...) |
| `timestamp` | Yes | Point-in-time snapshot (ISO8601 format) |

**Example:**
```bash
curl -H "Authorization: Api-Key $API_KEY" \
  "https://gateway.withobsrvr.com/lake/v1/testnet/api/v1/gold/snapshots/account?account_id=GAIH3ULLFQ4DGSECF2AR555KZ4KNDGEKN4AFI4SU2M7B43MGK3QJZNSR&timestamp=2026-01-12T23:59:59Z"
```

---

### Gold Compliance Archive Endpoints

Regulatory audit-ready exports with SHA-256 checksums, methodology documentation, and full reproducibility.

#### Get Transaction Archive

Returns all transactions for an asset within a date range with complete lineage.

```bash
GET /api/v1/gold/compliance/transactions?asset_code={code}&asset_issuer={issuer}&start_date={date}&end_date={date}&format={format}&limit={limit}
```

**Parameters:**
| Parameter | Required | Description |
|-----------|----------|-------------|
| `asset_code` | Yes | Asset code (XLM, USDC, etc.) |
| `asset_issuer` | No | Issuer account (empty for XLM) |
| `start_date` | Yes | Start of period (ISO8601 or YYYY-MM-DD) |
| `end_date` | Yes | End of period (ISO8601 or YYYY-MM-DD) |
| `include_failed` | No | Include failed transactions (default: false) |
| `format` | No | Output format: `json`, `csv`, `parquet` (default: json) |
| `limit` | No | Max results (default: 1000, max: 10000) |

**Example:**
```bash
# Get XLM transactions for a 3-day period
curl -H "Authorization: Api-Key $API_KEY" \
  "https://gateway.withobsrvr.com/lake/v1/testnet/api/v1/gold/compliance/transactions?asset_code=XLM&start_date=2026-01-10&end_date=2026-01-12&limit=1000"

# Export as CSV
curl -H "Authorization: Api-Key $API_KEY" \
  "https://gateway.withobsrvr.com/lake/v1/testnet/api/v1/gold/compliance/transactions?asset_code=XLM&start_date=2026-01-10&end_date=2026-01-12&format=csv" -o transactions.csv

# Export as Parquet
curl -H "Authorization: Api-Key $API_KEY" \
  "https://gateway.withobsrvr.com/lake/v1/testnet/api/v1/gold/compliance/transactions?asset_code=XLM&start_date=2026-01-10&end_date=2026-01-12&format=parquet" -o transactions.parquet
```

**Response (JSON):**
```json
{
  "archive_id": "txn_XLM_20260112_2a69b6e7",
  "asset": {"code": "XLM", "type": "native"},
  "period": {
    "start": "2026-01-10T00:00:00Z",
    "end": "2026-01-12T23:59:59Z",
    "start_ledger": 444441,
    "end_ledger": 445540
  },
  "summary": {
    "total_transactions": 2000,
    "total_volume": "282223726516929.0000000",
    "unique_accounts": 147,
    "successful_transactions": 2001,
    "failed_transactions": 0
  },
  "transactions": [
    {
      "ledger_sequence": 444441,
      "closed_at": "2026-01-12T11:16:52Z",
      "transaction_hash": "1eea14f5b065dc2b06502306c64b14a5445a22f98bbddf8019774331324e9d99",
      "operation_index": 0,
      "operation_type": "PATH_PAYMENT_STRICT_SEND",
      "from_account": "GCKA6J3H4AQ56JMEOTFNBDI2R776SQKESJGRLCGFW3NQ65FDEQTQ6WQY",
      "to_account": "GCKA6J3H4AQ56JMEOTFNBDI2R776SQKESJGRLCGFW3NQ65FDEQTQ6WQY",
      "amount": "11044436416",
      "successful": true
    }
  ],
  "checksum": "sha256:ed6fe58950e0f64dcfa3cc7065eafc9d4b7b025e657077d80ae5d20505d2aeb6",
  "methodology_version": "compliance_transactions_v1",
  "generated_at": "2026-01-12T12:55:46Z"
}
```

**Key Fields:**
| Field | Description |
|-------|-------------|
| `archive_id` | Unique identifier for this archive |
| `checksum` | SHA-256 hash for data integrity verification |
| `methodology_version` | Version of calculation methodology used |

---

#### Get Balance Archive

Returns all holders of an asset at a specific timestamp with supply statistics.

```bash
GET /api/v1/gold/compliance/balances?asset_code={code}&asset_issuer={issuer}&timestamp={iso8601}&format={format}&min_balance={amount}&limit={limit}
```

**Parameters:**
| Parameter | Required | Description |
|-----------|----------|-------------|
| `asset_code` | Yes | Asset code (XLM, USDC, etc.) |
| `asset_issuer` | No | Issuer account (empty for XLM) |
| `timestamp` | Yes | Point-in-time snapshot (ISO8601 format) |
| `min_balance` | No | Minimum balance filter |
| `format` | No | Output format: `json`, `csv`, `parquet` (default: json) |
| `limit` | No | Max results (default: 1000, max: 10000) |

**Example:**
```bash
# Get all XLM holders at end of day
curl -H "Authorization: Api-Key $API_KEY" \
  "https://gateway.withobsrvr.com/lake/v1/testnet/api/v1/gold/compliance/balances?asset_code=XLM&timestamp=2026-01-12T23:59:59Z&limit=1000"
```

**Response:**
```json
{
  "archive_id": "bal_XLM_20260112_c804a2d3",
  "asset": {"code": "XLM", "type": "native"},
  "snapshot_at": "2026-01-12T23:59:59Z",
  "snapshot_ledger": 445620,
  "summary": {
    "total_holders": 2402,
    "total_supply": "96876575117.7241628",
    "issuer_balance": "0.0000000",
    "circulating_supply": "96876575117.7241628"
  },
  "holders": [
    {
      "account_id": "GBFAIH5WKAJQ77NG6BZG7TGVGXHPX4SQLIJ7BENJMCVCZSUZPSISCLU5",
      "balance": "90321229899.5872227",
      "percent_of_supply": "93.23"
    }
  ],
  "checksum": "sha256:aef76b6c0a48f112fe631ad3185bc49663e3b3622fd331debf67fb99c792c67e",
  "methodology_version": "compliance_balances_v1",
  "generated_at": "2026-01-12T12:55:46Z"
}
```

---

#### Get Supply Timeline

Returns daily supply totals for an asset over a date range, tracking minting and burning.

```bash
GET /api/v1/gold/compliance/supply?asset_code={code}&asset_issuer={issuer}&start_date={date}&end_date={date}&interval={interval}&format={format}
```

**Parameters:**
| Parameter | Required | Description |
|-----------|----------|-------------|
| `asset_code` | Yes | Asset code (XLM, USDC, etc.) |
| `asset_issuer` | No | Issuer account (empty for XLM) |
| `start_date` | Yes | Start of period (ISO8601 or YYYY-MM-DD) |
| `end_date` | Yes | End of period (ISO8601 or YYYY-MM-DD) |
| `interval` | No | Aggregation interval: `1h`, `1d`, `1w` (default: 1d) |
| `format` | No | Output format: `json`, `csv`, `parquet` (default: json) |

**Example:**
```bash
# Get daily supply timeline
curl -H "Authorization: Api-Key $API_KEY" \
  "https://gateway.withobsrvr.com/lake/v1/testnet/api/v1/gold/compliance/supply?asset_code=XLM&start_date=2026-01-10&end_date=2026-01-12&interval=1d"
```

**Response:**
```json
{
  "archive_id": "supply_XLM_20260112_ef1fc775",
  "asset": {"code": "XLM", "type": "native"},
  "period": {
    "start": "2026-01-10T00:00:00Z",
    "end": "2026-01-12T23:59:59Z"
  },
  "interval": "1d",
  "timeline": [
    {
      "timestamp": "2026-01-12T00:00:00Z",
      "ledger_sequence": 445625,
      "total_supply": "96876575521.5867546",
      "circulating_supply": "96876575521.5867546",
      "issuer_balance": "0",
      "holder_count": 2410,
      "supply_change": "0.0000000"
    }
  ],
  "summary": {
    "start_supply": "96876575521.5867546",
    "end_supply": "96876575521.5867546",
    "net_minted": "0.0000000",
    "peak_supply": "96876575521.5867546",
    "peak_date": "2026-01-12",
    "lowest_supply": "96876575521.5867546",
    "lowest_date": "2026-01-12",
    "data_points": 1
  },
  "checksum": "sha256:487d571f2f1fadc74866c0c6336c6456ff41b2e12e542aeb505e3f91df0aade3",
  "methodology_version": "supply_timeline_v1",
  "generated_at": "2026-01-12T12:55:46Z"
}
```

**Use Cases:**
- Track minting/burning activity for stablecoins
- Prove no unauthorized supply changes occurred
- Audit supply integrity for regulatory compliance

---

#### Create Full Compliance Archive (Async)

Creates a complete compliance package with all artifacts asynchronously.

```bash
POST /api/v1/gold/compliance/archive
```

**Request Body:**
```json
{
  "asset_code": "XLM",
  "asset_issuer": "",
  "start_date": "2026-01-10",
  "end_date": "2026-01-12",
  "include": ["transactions", "balances", "supply"],
  "balance_snapshots": ["2026-01-12"]
}
```

**Parameters:**
| Field | Required | Description |
|-------|----------|-------------|
| `asset_code` | Yes | Asset code (XLM, USDC, etc.) |
| `asset_issuer` | No | Issuer account (empty for XLM) |
| `start_date` | Yes | Start of period (YYYY-MM-DD) |
| `end_date` | Yes | End of period (YYYY-MM-DD) |
| `include` | Yes | Array: `transactions`, `balances`, `supply` |
| `balance_snapshots` | No | Dates for balance snapshots (defaults to end_date) |

**Example:**
```bash
curl -X POST -H "Authorization: Api-Key $API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"asset_code":"XLM","start_date":"2026-01-10","end_date":"2026-01-12","include":["transactions","balances","supply"],"balance_snapshots":["2026-01-12"]}' \
  "https://gateway.withobsrvr.com/lake/v1/testnet/api/v1/gold/compliance/archive"
```

**Response:**
```json
{
  "archive_id": "archive_XLM_20260112_b5dc52d3",
  "status": "pending",
  "callback_url": "/api/v1/gold/compliance/archive/archive_XLM_20260112_b5dc52d3",
  "created_at": "2026-01-12T12:54:34Z"
}
```

---

#### Get Archive Status

Check the status of an async archive generation.

```bash
GET /api/v1/gold/compliance/archive/{archive_id}
```

**Example:**
```bash
curl -H "Authorization: Api-Key $API_KEY" \
  "https://gateway.withobsrvr.com/lake/v1/testnet/api/v1/gold/compliance/archive/archive_XLM_20260112_b5dc52d3"
```

**Response (Complete):**
```json
{
  "archive_id": "archive_XLM_20260112_b5dc52d3",
  "status": "complete",
  "asset": {"code": "XLM", "type": "native"},
  "period": {
    "start": "2026-01-10T00:00:00Z",
    "end": "2026-01-12T23:59:59Z"
  },
  "artifacts": [
    {
      "name": "transactions.json",
      "type": "transactions",
      "row_count": 2111,
      "checksum": "sha256:ed6fe58950e0f64dcfa3cc7065eafc9d4b7b025e657077d80ae5d20505d2aeb6"
    },
    {
      "name": "balances_2026-01-12.json",
      "type": "balance_snapshot",
      "row_count": 2384,
      "snapshot_at": "2026-01-12T23:59:59Z",
      "checksum": "sha256:aef76b6c0a48f112fe631ad3185bc49663e3b3622fd331debf67fb99c792c67e"
    },
    {
      "name": "supply_timeline.json",
      "type": "supply_timeline",
      "row_count": 3,
      "checksum": "sha256:fea63c38cf4ab924e94229ff46c5a8a1eebee2d72c0bb382f006fc37563f09c7"
    },
    {
      "name": "methodology.md",
      "type": "documentation",
      "checksum": "sha256:51c0b3cc4a58b3f5f2506337ed951bf902bfbeeedc490c2ec7833724ff5062e3"
    },
    {
      "name": "manifest.json",
      "type": "manifest",
      "checksum": "sha256:ea941772af690a61ef67e354c2719c6699deab80f9b89ea41dadc47cf386a4f6"
    }
  ],
  "methodology_version": "compliance_archive_v1",
  "created_at": "2026-01-12T12:54:34Z",
  "completed_at": "2026-01-12T12:54:34Z"
}
```

**Status Values:**
| Status | Description |
|--------|-------------|
| `pending` | Archive generation queued |
| `processing` | Archive is being generated |
| `complete` | Archive ready for download |
| `failed` | Generation failed (see error field) |

---

#### Download Archive Artifact

Download a specific artifact from a completed archive.

```bash
GET /api/v1/gold/compliance/archive/{archive_id}/download/{artifact_name}
```

**Example:**
```bash
# Download methodology document
curl -H "Authorization: Api-Key $API_KEY" \
  "https://gateway.withobsrvr.com/lake/v1/testnet/api/v1/gold/compliance/archive/archive_XLM_20260112_b5dc52d3/download/methodology.md"

# Download manifest
curl -H "Authorization: Api-Key $API_KEY" \
  "https://gateway.withobsrvr.com/lake/v1/testnet/api/v1/gold/compliance/archive/archive_XLM_20260112_b5dc52d3/download/manifest.json"
```

**Available Artifacts:**
| Artifact | Description |
|----------|-------------|
| `methodology.md` | Documentation explaining how data was derived |
| `manifest.json` | Table of contents with all checksums |

> **Note:** For data artifacts (transactions, balances, supply), use the GET endpoints with format parameter.

---

#### Get Archive Lineage

Returns audit trail of all archives created.

```bash
GET /api/v1/gold/compliance/lineage?asset_code={code}&asset_issuer={issuer}&limit={limit}
```

**Example:**
```bash
curl -H "Authorization: Api-Key $API_KEY" \
  "https://gateway.withobsrvr.com/lake/v1/testnet/api/v1/gold/compliance/lineage?asset_code=XLM&limit=10"
```

**Response:**
```json
{
  "lineage": [
    {
      "archive_id": "archive_XLM_20260112_b5dc52d3",
      "asset": {"code": "XLM", "type": "native"},
      "period_start": "2026-01-10T00:00:00Z",
      "period_end": "2026-01-12T23:59:59Z",
      "created_at": "2026-01-12T12:54:34Z",
      "completed_at": "2026-01-12T12:54:34Z",
      "artifact_count": 5,
      "status": "complete"
    }
  ],
  "count": 1
}
```

---

### Archive Contents Reference

A complete compliance archive contains 5 documents:

| Document | Purpose | Key Question Answered |
|----------|---------|----------------------|
| `transactions.json` | Transaction lineage | "What moved, when, between whom?" |
| `balances_*.json` | Point-in-time holdings | "Who held how much at this moment?" |
| `supply_timeline.json` | Supply integrity | "Was anything minted or burned?" |
| `methodology.md` | Audit trail | "How were these numbers calculated?" |
| `manifest.json` | Archive integrity | "Is this archive complete and untampered?" |

### Reproducibility

All compliance archives include:

1. **SHA-256 Checksums** - Each artifact has a cryptographic hash for integrity verification
2. **Methodology Version** - Documents which calculation rules were used
3. **Reproducibility Key** - A hash derived from inputs that should match when regenerating

To verify an archive:
1. Re-request with identical parameters (asset, dates, methodology version)
2. Compare checksums - they should be identical
3. If checksums match, data integrity is verified

### Export Formats

All GET endpoints support three output formats:

| Format | Use Case | Parameter |
|--------|----------|-----------|
| JSON | API integration, inspection | `format=json` (default) |
| CSV | Spreadsheet analysis, Excel import | `format=csv` |
| Parquet | Big data tools, DuckDB, Spark | `format=parquet` |

**Parquet Example with DuckDB:**
```bash
# Download parquet file
curl -H "Authorization: Api-Key $API_KEY" \
  "https://gateway.withobsrvr.com/lake/v1/testnet/api/v1/gold/compliance/balances?asset_code=XLM&timestamp=2026-01-12T23:59:59Z&format=parquet" \
  -o balances.parquet

# Query with DuckDB
duckdb -c "SELECT account_id, balance, percent_of_supply FROM 'balances.parquet' ORDER BY CAST(balance AS DOUBLE) DESC LIMIT 10"
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
| Transaction events | N/A | `/silver/tx/{hash}/events` |
| Decoded transaction | N/A | `/silver/tx/{hash}/decoded` |
| Full transaction analysis | N/A | `/silver/tx/{hash}/full` |
| Soroban calls (alias) | N/A | `/silver/calls` |
| Ledger info | `GET /ledgers/{sequence}` | `/bronze/ledgers?sequence={sequence}` |
| All accounts | `GET /accounts` | `/silver/accounts` |
| Asset holders | `GET /accounts?asset=CODE:ISSUER` | `/silver/assets/CODE:ISSUER/holders` |
| Asset stats | N/A | `/silver/assets/CODE:ISSUER/stats` |
| Token metadata | N/A | `/silver/tokens/{contract_id}` |
| Token holders | N/A | `/silver/tokens/{contract_id}/balances` |
| Token balance | N/A | `/silver/tokens/{contract_id}/balance/{addr}` |
| Token transfers | N/A | `/silver/tokens/{contract_id}/transfers` |
| Address token portfolio | N/A | `/silver/address/{addr}/token-balances` |
| Unified event stream | N/A | `/silver/events` |
| Contract interface | N/A | `/silver/contracts/{id}/interface` |
| Soroban ops by function | N/A | `/silver/operations/soroban/by-function` |
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
