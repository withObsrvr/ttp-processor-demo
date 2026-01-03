# Stellar Query API - Silver Layer Documentation

## Overview

The Silver API provides analytics-ready queries over Stellar blockchain data with a **Lambda Architecture** combining real-time hot buffer (PostgreSQL) and historical cold storage (DuckLake/Parquet on Backblaze B2).

**Base URL**: `http://134.209.123.25:8092/api/v1/silver`

**Version**: 0.2.0 (Hot + Cold unified querying)

---

## Architecture

### Lambda Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                     Stellar Testnet                          │
└─────────────────────┬───────────────────────────────────────┘
                      ↓
         ┌────────────────────────┐
         │ stellar-postgres-      │ Ingests raw ledger data
         │ ingester               │
         └────────┬───────────────┘
                  ↓
         ┌────────────────────────┐
         │ PostgreSQL stellar_hot │ Bronze: Raw blockchain data
         │ (Hot Buffer)           │ ~30 minutes retention
         └────────┬───────────────┘
                  ↓
         ┌────────────────────────┐
         │ bronze-silver-         │ Transforms to analytics
         │ transformer            │
         └────────┬───────────────┘
                  ↓
         ┌────────────────────────┐
         │ PostgreSQL silver_hot  │ Silver: Analytics-ready
         │ (Hot Buffer)           │ ~3 hours retention
         └────────┬───────┬───────┘
                  ↓       │
    ┌─────────────┘       └────────────┐
    ↓ (Every 3 hours)                  ↓ (Real-time queries)
┌───────────────────┐          ┌──────────────────┐
│ silver-cold-      │          │ stellar-query-   │
│ flusher           │          │ api              │
└────────┬──────────┘          └──────────────────┘
         ↓                              ↑
┌────────────────────┐                  │
│ DuckLake Silver    │                  │
│ (Parquet/B2)       │──────────────────┘
│ Historical Storage │  (Historical queries)
└────────────────────┘
```

### Data Layers

| Layer | Storage | Retention | Purpose | Query Speed |
|-------|---------|-----------|---------|-------------|
| **Hot** | PostgreSQL silver_hot | ~3 hours | Real-time analytics | < 100ms |
| **Cold** | DuckLake/Parquet (B2) | Unlimited | Historical analytics | 200-500ms |

### Query Strategy

All Silver endpoints use **hot-first querying**:

1. **Query hot buffer first** (PostgreSQL silver_hot) - most recent data
2. **Fallback to cold storage** (DuckLake Silver) if not found or more data needed
3. **Merge results** intelligently (hot takes precedence for deduplication)
4. **Return unified response** - user sees seamless experience

---

## Data Model

### Tables Available

#### Current State Tables (5)
Point-in-time snapshots of ledger entities:

- **accounts_current** - Current account balances and metadata
- **trustlines_current** - Active trustlines
- **offers_current** - Active offers
- **claimable_balances_current** - Claimable balance entries
- **contract_data_current** - Soroban contract data

#### Snapshot Tables (5)
Historical state at each ledger:

- **accounts_snapshot** - Account states over time
- **trustlines_snapshot** - Trustline states over time
- **offers_snapshot** - Offer states over time
- **account_signers_snapshot** - Signer configurations over time
- **claimable_balances_snapshot** - Claimable balance states over time

#### Event/Enriched Tables (3)
Transaction and operation events with full context:

- **enriched_history_operations** - All operations with transaction context
- **enriched_history_operations_soroban** - Soroban-specific operations
- **token_transfers_raw** - Token transfer events (classic + Soroban)

---

## API Endpoints

### Health Check

**Endpoint**: `GET /health`

**Description**: Check API health and available layers

**Example**:
```bash
curl http://134.209.123.25:8092/health
```

**Response**:
```json
{
    "status": "healthy",
    "layers": {
        "bronze": true,
        "hot": true,
        "silver": true
    }
}
```

---

## Account Endpoints

### Get Account Current State

**Endpoint**: `GET /api/v1/silver/accounts/current`

**Description**: Get the current state of a specific account

**Parameters**:
- `account_id` (required) - Stellar account ID

**Query Strategy**: Hot first → Cold fallback

**Example**:
```bash
curl 'http://134.209.123.25:8092/api/v1/silver/accounts/current?account_id=GBBMU27AES3BR3VKGFHZGTX5JNFGRJFBHD2C6Q3SUP7I2SU6DP6ZKN6O'
```

**Response**:
```json
{
    "account": {
        "account_id": "GBBMU27AES3BR3VKGFHZGTX5JNFGRJFBHD2C6Q3SUP7I2SU6DP6ZKN6O",
        "balance": "9999.5857200",
        "sequence_number": "1937030260847",
        "num_subentries": 18,
        "last_modified_ledger": 262762,
        "updated_at": "2026-01-01T22:40:14.030431Z"
    }
}
```

**Use Cases**:
- Check account balance
- Verify account exists
- Get current account state for transactions
- Real-time account monitoring

---

### Get Top Accounts by Balance

**Endpoint**: `GET /api/v1/silver/accounts/top`

**Description**: Get accounts sorted by balance (leaderboard)

**Parameters**:
- `limit` (optional, default: 100, max: 1000) - Number of accounts to return

**Query Strategy**: Query both hot + cold in parallel → Merge → Deduplicate → Sort

**Example**:
```bash
curl 'http://134.209.123.25:8092/api/v1/silver/accounts/top?limit=10'
```

**Response**:
```json
{
    "accounts": [
        {
            "account_id": "GAIH3ULLFQ4DGSECF2AR555KZ4KNDGEKN4AFI4SU2M7B43MGK3QJZNSR",
            "balance": "8120477198.9309600",
            "sequence_number": "695784702052",
            "num_subentries": 0,
            "last_modified_ledger": 262215,
            "updated_at": "2026-01-01T21:54:41.129717Z"
        },
        {
            "account_id": "GB2YBCXS52THP5WTCECVY6YX5O6I5DXNGCGSAWRVE2RHZ45V25GF6KZT",
            "balance": "9999.9999900",
            "sequence_number": "1126204849520641",
            "num_subentries": 0,
            "last_modified_ledger": 262216,
            "updated_at": "2026-01-01T21:54:41.132107Z"
        }
    ],
    "count": 10
}
```

**Use Cases**:
- Leaderboards showing richest accounts
- Wealth distribution analysis
- Whale watching
- Network statistics

---

### Get Account History

**Endpoint**: `GET /api/v1/silver/accounts/history`

**Description**: Get historical snapshots of an account's state over time

**Parameters**:
- `account_id` (required) - Stellar account ID
- `limit` (optional, default: 50, max: 500) - Number of snapshots to return

**Query Strategy**: Hot first → Cold if needed → Merge chronologically

**Example**:
```bash
curl 'http://134.209.123.25:8092/api/v1/silver/accounts/history?account_id=GBBMU27AES3BR3VKGFHZGTX5JNFGRJFBHD2C6Q3SUP7I2SU6DP6ZKN6O&limit=10'
```

**Response**:
```json
{
    "account_id": "GBBMU27AES3BR3VKGFHZGTX5JNFGRJFBHD2C6Q3SUP7I2SU6DP6ZKN6O",
    "count": 10,
    "history": [
        {
            "account_id": "GBBMU27AES3BR3VKGFHZGTX5JNFGRJFBHD2C6Q3SUP7I2SU6DP6ZKN6O",
            "balance": "9999.5857200",
            "sequence_number": "1937030260847",
            "ledger_sequence": 262762,
            "closed_at": "2026-01-01T22:40:13Z",
            "valid_to": null
        },
        {
            "account_id": "GBBMU27AES3BR3VKGFHZGTX5JNFGRJFBHD2C6Q3SUP7I2SU6DP6ZKN6O",
            "balance": "9999.5857400",
            "sequence_number": "1937030260846",
            "ledger_sequence": 262761,
            "closed_at": "2026-01-01T22:40:08Z",
            "valid_to": "2026-01-01T22:40:13Z"
        }
    ]
}
```

**Use Cases**:
- Historical balance tracking
- Account state time-travel queries
- Audit trails
- Balance change analysis

---

## Operations Endpoints

### Get Enriched Operations

**Endpoint**: `GET /api/v1/silver/operations/enriched`

**Description**: Get operations with full transaction context and enriched metadata

**Parameters**:
- `limit` (optional, default: 100, max: 1000) - Number of operations to return
- `account_id` (optional) - Filter by source or destination account
- `tx_hash` (optional) - Filter by transaction hash
- `start_ledger` (optional) - Start ledger sequence (inclusive)
- `end_ledger` (optional) - End ledger sequence (inclusive)
- `payments_only` (optional) - Set to `true` to only return payment operations
- `soroban_only` (optional) - Set to `true` to only return Soroban operations

**Query Strategy**: Hot first → Cold if limit not met → Merge

**Example**:
```bash
curl 'http://134.209.123.25:8092/api/v1/silver/operations/enriched?limit=5'
```

**Filtered Example**:
```bash
# Get all payments for a specific account
curl 'http://134.209.123.25:8092/api/v1/silver/operations/enriched?account_id=GBBMU27AES3BR3VKGFHZGTX5JNFGRJFBHD2C6Q3SUP7I2SU6DP6ZKN6O&payments_only=true&limit=10'

# Get operations in a ledger range
curl 'http://134.209.123.25:8092/api/v1/silver/operations/enriched?start_ledger=262700&end_ledger=262800&limit=100'
```

**Response**:
```json
{
    "count": 5,
    "filters": {
        "AccountID": "",
        "TxHash": "",
        "StartLedger": 0,
        "EndLedger": 0,
        "PaymentsOnly": false,
        "SorobanOnly": false,
        "Limit": 5
    },
    "operations": [
        {
            "transaction_hash": "75e18024a364e01eb3159d7a0fc621d509aa864f151ee2afdd8d15aeef480874",
            "operation_id": 1,
            "ledger_sequence": 262772,
            "ledger_closed_at": "2026-01-01T22:41:03Z",
            "source_account": "GC6ICL6XLKEYWWQAHTOGIDZC3VG3ZO7XPEUV3AV3O4EUZTLBMNJUB4AY",
            "type": 1,
            "type_name": "PAYMENT",
            "destination": "GBMHKGWUZZV52MSNYX5SHZB2LFTX2N2WIFPNZBZ4GTMWUJX3TN2JWF4H",
            "amount": "24700000",
            "tx_successful": false,
            "tx_fee_charged": 200,
            "is_payment_op": true,
            "is_soroban_op": false
        }
    ]
}
```

**Operation Types**:
- 0: CREATE_ACCOUNT
- 1: PAYMENT
- 2: PATH_PAYMENT_STRICT_RECEIVE
- 3: MANAGE_SELL_OFFER
- 6: CHANGE_TRUST
- 10: MANAGE_DATA
- 24: INVOKE_HOST_FUNCTION (Soroban)
- [Full list in response type_name field]

**Use Cases**:
- Transaction history for accounts
- Payment flow analysis
- Smart contract interaction tracking
- Network activity monitoring

---

### Get Payments Only

**Endpoint**: `GET /api/v1/silver/payments`

**Description**: Convenience endpoint for payment operations only

**Parameters**:
- `limit` (optional, default: 50, max: 500) - Number of payments to return
- `account_id` (optional) - Filter by source or destination account

**Query Strategy**: Hot first (payments_only=true) → Cold if needed

**Example**:
```bash
curl 'http://134.209.123.25:8092/api/v1/silver/payments?limit=10'
```

**Response**:
```json
{
    "count": 10,
    "payments": [
        {
            "transaction_hash": "ee027ffd751907c1ce26edf1253232c28dee863845a48d81aafb8c3c64e57e4d",
            "operation_id": 3,
            "ledger_sequence": 262774,
            "ledger_closed_at": "2026-01-01T22:41:13Z",
            "source_account": "GC6ICL6XLKEYWWQAHTOGIDZC3VG3ZO7XPEUV3AV3O4EUZTLBMNJUB4AY",
            "type": 1,
            "type_name": "PAYMENT",
            "destination": "GBMHKGWUZZV52MSNYX5SHZB2LFTX2N2WIFPNZBZ4GTMWUJX3TN2JWF4H",
            "amount": "300000",
            "tx_successful": false,
            "tx_fee_charged": 400,
            "is_payment_op": true,
            "is_soroban_op": false
        }
    ]
}
```

**Use Cases**:
- Payment flow analysis
- Transaction volume tracking
- Payment network graphs
- Value transfer monitoring

---

### Get Soroban Operations Only

**Endpoint**: `GET /api/v1/silver/operations/soroban`

**Description**: Convenience endpoint for Soroban smart contract operations

**Parameters**:
- `limit` (optional, default: 50, max: 500) - Number of operations to return
- `account_id` (optional) - Filter by source account

**Example**:
```bash
curl 'http://134.209.123.25:8092/api/v1/silver/operations/soroban?limit=10'
```

**Use Cases**:
- Smart contract analytics
- Soroban adoption tracking
- Contract interaction monitoring

---

## Token Transfer Endpoints

### Get Token Transfers

**Endpoint**: `GET /api/v1/silver/transfers`

**Description**: Get token transfers from both classic (trustline) and Soroban sources

**Parameters**:
- `limit` (optional, default: 100, max: 1000) - Number of transfers to return
- `source_type` (optional) - Filter by "classic" or "soroban"
- `asset_code` (optional) - Filter by asset code
- `from_account` (optional) - Filter by sender account
- `to_account` (optional) - Filter by receiver account
- `start_time` (optional) - Start time (RFC3339 format, default: 24h ago)
- `end_time` (optional) - End time (RFC3339 format, default: now)

**Query Strategy**: Hot first → Cold if needed

**Example**:
```bash
curl 'http://134.209.123.25:8092/api/v1/silver/transfers?limit=10'
```

**Filtered Example**:
```bash
# Get only Soroban transfers
curl 'http://134.209.123.25:8092/api/v1/silver/transfers?source_type=soroban&limit=10'

# Get transfers for specific asset
curl 'http://134.209.123.25:8092/api/v1/silver/transfers?asset_code=USDC&limit=10'
```

**Response**:
```json
{
    "count": 10,
    "filters": {
        "SourceType": "",
        "AssetCode": "",
        "FromAccount": "",
        "ToAccount": "",
        "StartTime": "2025-12-31T22:41:24Z",
        "EndTime": "2026-01-01T22:41:24Z",
        "Limit": 10
    },
    "transfers": [
        {
            "timestamp": "2026-01-01T22:41:18Z",
            "transaction_hash": "02b04e5d7e197d67beb3c777dcf0082d15f69774272a27dd468ba36fe751018d",
            "ledger_sequence": 262775,
            "source_type": "soroban",
            "token_contract_id": "d7928b72c2703ccfeaf7eb9ff4ef4d504a55a8b979fc9b450ea2c842b4d1ce61",
            "transaction_successful": true
        },
        {
            "timestamp": "2026-01-01T22:41:13Z",
            "transaction_hash": "2589665e7366a466096dcaa38acca3a1439e8a7c833959c9162943ca896f61d6",
            "ledger_sequence": 262774,
            "source_type": "classic",
            "from_account": "GCTXUX2HYYV7ZWDA665CLGLWKOPWOPUGGGDPAYUZPHUCJSPCWI3YQMQO",
            "to_account": "GDTJH5EKYI6ENUAJG6DDMFIQMFMK64AYGT4W3SFYO6SAPMDDWW2I6UKC",
            "amount": "10000",
            "transaction_successful": true
        }
    ]
}
```

**Use Cases**:
- Token flow analysis
- Asset velocity tracking
- Cross-border payment monitoring
- Soroban token adoption

---

### Get Token Transfer Statistics

**Endpoint**: `GET /api/v1/silver/transfers/stats`

**Description**: Get aggregated transfer statistics grouped by various dimensions

**Parameters**:
- `group_by` (required) - Grouping dimension: "asset", "source_type", "hour", or "day"
- `start_time` (optional) - Start time (RFC3339 format, default: 24h ago)
- `end_time` (optional) - End time (RFC3339 format, default: now)

**Query Strategy**: Cold storage only (aggregations are expensive)

**Example**:
```bash
# Group by asset
curl 'http://134.209.123.25:8092/api/v1/silver/transfers/stats?group_by=asset'

# Group by hour
curl 'http://134.209.123.25:8092/api/v1/silver/transfers/stats?group_by=hour&start_time=2026-01-01T00:00:00Z'
```

**Response**:
```json
{
    "stats": [
        {
            "asset_code": "USDC",
            "source_type": "classic",
            "transfer_count": 156,
            "unique_senders": 42,
            "unique_receivers": 38,
            "total_volume": 1250000.50
        }
    ],
    "count": 1,
    "group_by": "asset",
    "start_time": "2025-12-31T22:00:00Z",
    "end_time": "2026-01-01T22:00:00Z"
}
```

**Use Cases**:
- Asset volume analysis
- Network activity heatmaps
- Time-series analytics
- Comparative analysis (classic vs Soroban)

---

## Block Explorer Endpoints

### Get Account Overview

**Endpoint**: `GET /api/v1/silver/explorer/account`

**Description**: Get comprehensive account overview with current state, recent operations, and transfers

**Parameters**:
- `account_id` (required) - Stellar account ID

**Query Strategy**: Multiple parallel queries combined:
- Account current: Hot → Cold
- Recent operations: Hot → Cold (limit 10)
- Recent transfers: Hot → Cold (last 7 days, limit 10)

**Example**:
```bash
curl 'http://134.209.123.25:8092/api/v1/silver/explorer/account?account_id=GBBMU27AES3BR3VKGFHZGTX5JNFGRJFBHD2C6Q3SUP7I2SU6DP6ZKN6O'
```

**Response**:
```json
{
    "account": {
        "account_id": "GBBMU27AES3BR3VKGFHZGTX5JNFGRJFBHD2C6Q3SUP7I2SU6DP6ZKN6O",
        "balance": "9999.5857200",
        "sequence_number": "1937030260847",
        "num_subentries": 18,
        "last_modified_ledger": 262762,
        "updated_at": "2026-01-01T22:40:14.030431Z"
    },
    "recent_operations": [
        {
            "transaction_hash": "fc894d15...",
            "operation_id": 1,
            "ledger_sequence": 262762,
            "type_name": "MANAGE_DATA"
        }
    ],
    "recent_transfers": [],
    "operations_count": 10,
    "transfers_count": 0
}
```

**Use Cases**:
- Block explorer account pages
- Account detail views
- Comprehensive account analysis
- User dashboards

---

### Get Transaction Details

**Endpoint**: `GET /api/v1/silver/explorer/transaction`

**Description**: Get full transaction details with all operations and transfers

**Parameters**:
- `tx_hash` (required) - Transaction hash

**Example**:
```bash
curl 'http://134.209.123.25:8092/api/v1/silver/explorer/transaction?tx_hash=fc894d15a2a97869b42c4b4722b6f707d872b5ddfd45ac86e8e2fea2950224fb'
```

**Response**:
```json
{
    "transaction": {
        "transaction_hash": "fc894d15a2a97869b42c4b4722b6f707d872b5ddfd45ac86e8e2fea2950224fb",
        "ledger_sequence": 262762,
        "ledger_closed_at": "2026-01-01T22:40:13Z",
        "successful": true,
        "fee_charged": 200,
        "operation_count": 2
    },
    "operations": [
        {
            "operation_id": 0,
            "type_name": "MANAGE_DATA",
            "source_account": "GBBMU27AES3BR3VKGFHZGTX5JNFGRJFBHD2C6Q3SUP7I2SU6DP6ZKN6O"
        },
        {
            "operation_id": 1,
            "type_name": "MANAGE_DATA",
            "source_account": "GBBMU27AES3BR3VKGFHZGTX5JNFGRJFBHD2C6Q3SUP7I2SU6DP6ZKN6O"
        }
    ],
    "transfers": []
}
```

**Use Cases**:
- Block explorer transaction pages
- Transaction verification
- Operation analysis

---

### Get Asset Overview

**Endpoint**: `GET /api/v1/silver/explorer/asset`

**Description**: Get asset statistics and recent transfers

**Parameters**:
- `asset_code` (required) - Asset code
- `asset_issuer` (optional) - Asset issuer account

**Example**:
```bash
curl 'http://134.209.123.25:8092/api/v1/silver/explorer/asset?asset_code=USDC'
```

**Response**:
```json
{
    "asset_code": "USDC",
    "recent_transfers": [],
    "stats_24h": {
        "asset_code": "USDC",
        "transfer_count": 156,
        "unique_senders": 42,
        "unique_receivers": 38,
        "total_volume": 1250000.50
    },
    "transfer_count": 0
}
```

**Use Cases**:
- Block explorer asset pages
- Asset analytics
- Market analysis

---

## Access Methods

### From Server (Direct)

SSH into the server and query localhost:

```bash
ssh -i /path/to/obsrvr-lake-prod.pem root@134.209.123.25

curl 'http://localhost:8092/api/v1/silver/accounts/current?account_id=YOUR_ACCOUNT_ID'
```

### From Local Machine (SSH Tunnel)

Create an SSH tunnel to access the API:

```bash
# Terminal 1: Create tunnel
ssh -i /path/to/obsrvr-lake-prod.pem -L 8092:localhost:8092 root@134.209.123.25

# Terminal 2: Query localhost
curl 'http://localhost:8092/api/v1/silver/accounts/current?account_id=YOUR_ACCOUNT_ID'
```

### Public Access (Optional)

To enable public access, open port 8092 on the firewall:

```bash
ssh -i /path/to/obsrvr-lake-prod.pem root@134.209.123.25 "ufw allow 8092/tcp"
```

Then query directly:
```bash
curl 'http://134.209.123.25:8092/api/v1/silver/accounts/current?account_id=YOUR_ACCOUNT_ID'
```

---

## Performance Characteristics

### Latency

| Query Type | Hot Buffer | Cold Storage | Combined |
|------------|------------|--------------|----------|
| Single entity | 50-100ms | 200-500ms | 50-100ms (hot first) |
| List (10 items) | 100-200ms | 300-600ms | 100-300ms (merge) |
| Aggregations | N/A | 500-1000ms | 500-1000ms (cold only) |

### Data Freshness

- **Hot Buffer**: < 1 minute lag from testnet
- **Cold Storage**: 3 hour flush interval
- **Queries**: Always return freshest data available

### Capacity

- **Hot Buffer**: Last ~3 hours of data
- **Cold Storage**: Full blockchain history
- **Combined**: Unlimited historical + real-time

---

## Error Handling

### HTTP Status Codes

- `200 OK` - Success
- `400 Bad Request` - Invalid parameters
- `404 Not Found` - Entity not found
- `500 Internal Server Error` - Server error

### Error Response Format

```json
{
    "error": "account_id required"
}
```

---

## Best Practices

### Query Optimization

1. **Use specific filters** - Narrow queries with account_id, tx_hash, or ledger range
2. **Set appropriate limits** - Don't request more data than you need
3. **Cache results** - Account current state can be cached for ~30 seconds
4. **Use pagination** - For large result sets, use limit + offset pattern

### Common Patterns

**Real-time Account Monitoring**:
```bash
# Poll every 5 seconds for account updates
while true; do
  curl -s 'http://localhost:8092/api/v1/silver/accounts/current?account_id=YOUR_ACCOUNT'
  sleep 5
done
```

**Historical Analysis**:
```bash
# Get account history for trend analysis
curl 'http://localhost:8092/api/v1/silver/accounts/history?account_id=YOUR_ACCOUNT&limit=100'
```

**Payment Tracking**:
```bash
# Monitor payments for a specific account
curl 'http://localhost:8092/api/v1/silver/payments?account_id=YOUR_ACCOUNT&limit=50'
```

---

## Data Retention

### Hot Buffer (PostgreSQL silver_hot)

- **Retention**: ~3 hours
- **Flush Interval**: Every 3 hours (configurable)
- **Purpose**: Real-time queries
- **Cleanup**: Automatic after flush to cold storage

### Cold Storage (DuckLake Silver)

- **Retention**: Unlimited
- **Storage**: Parquet files on Backblaze B2
- **Compression**: ~20:1 ratio (Snappy)
- **Cost**: $0.005/GB/month

---

## Integration Examples

### Python

```python
import requests

BASE_URL = "http://localhost:8092/api/v1/silver"

# Get account current state
def get_account(account_id):
    response = requests.get(
        f"{BASE_URL}/accounts/current",
        params={"account_id": account_id}
    )
    return response.json()

# Get recent payments
def get_payments(account_id, limit=10):
    response = requests.get(
        f"{BASE_URL}/payments",
        params={"account_id": account_id, "limit": limit}
    )
    return response.json()

# Example usage
account = get_account("GBBMU27AES3BR3VKGFHZGTX5JNFGRJFBHD2C6Q3SUP7I2SU6DP6ZKN6O")
print(f"Balance: {account['account']['balance']} XLM")
```

### JavaScript/Node.js

```javascript
const axios = require('axios');

const BASE_URL = 'http://localhost:8092/api/v1/silver';

// Get account current state
async function getAccount(accountId) {
    const response = await axios.get(`${BASE_URL}/accounts/current`, {
        params: { account_id: accountId }
    });
    return response.data;
}

// Get account history
async function getAccountHistory(accountId, limit = 10) {
    const response = await axios.get(`${BASE_URL}/accounts/history`, {
        params: { account_id: accountId, limit }
    });
    return response.data;
}

// Example usage
(async () => {
    const account = await getAccount('GBBMU27AES3BR3VKGFHZGTX5JNFGRJFBHD2C6Q3SUP7I2SU6DP6ZKN6O');
    console.log(`Balance: ${account.account.balance} XLM`);
})();
```

### Bash/cURL

```bash
#!/bin/bash

BASE_URL="http://localhost:8092/api/v1/silver"
ACCOUNT_ID="GBBMU27AES3BR3VKGFHZGTX5JNFGRJFBHD2C6Q3SUP7I2SU6DP6ZKN6O"

# Get account and extract balance
BALANCE=$(curl -s "${BASE_URL}/accounts/current?account_id=${ACCOUNT_ID}" | \
          jq -r '.account.balance')

echo "Current balance: ${BALANCE} XLM"

# Get recent operations count
OPS_COUNT=$(curl -s "${BASE_URL}/operations/enriched?account_id=${ACCOUNT_ID}&limit=100" | \
            jq -r '.count')

echo "Recent operations: ${OPS_COUNT}"
```

---

## Monitoring and Observability

### Health Monitoring

Monitor API health:
```bash
# Simple health check
curl http://localhost:8092/health

# Monitor all layers
watch -n 5 'curl -s http://localhost:8092/health | jq .'
```

### Metrics to Track

1. **Query Latency** - Monitor response times
2. **Hot Buffer Coverage** - Check ledger range in hot buffer
3. **Cold Storage Growth** - Monitor Parquet file counts
4. **Error Rates** - Track 4xx and 5xx responses

---

## Troubleshooting

### Account Not Found

**Issue**: `404 Not Found` when querying account

**Causes**:
- Account doesn't exist on testnet
- Account was just created (wait ~30 seconds for ingestion)
- Account hasn't been modified recently (check cold storage)

**Solution**:
```bash
# Verify account exists on testnet
curl "https://horizon-testnet.stellar.org/accounts/${ACCOUNT_ID}"

# Check if account is in cold storage
curl "http://localhost:8092/api/v1/silver/accounts/history?account_id=${ACCOUNT_ID}&limit=1"
```

### Slow Queries

**Issue**: Queries taking > 5 seconds

**Causes**:
- Large result sets without limits
- Querying only cold storage (hot buffer empty)
- Network issues with Backblaze B2

**Solution**:
- Add appropriate limits
- Use filters to narrow results
- Check hot buffer has recent data

### Stale Data

**Issue**: Data not updating in real-time

**Causes**:
- Ingester not running
- Transformer not running
- Hot buffer not being populated

**Solution**:
```bash
# Check service status
nomad job status stellar-postgres-ingester
nomad job status bronze-silver-transformer

# Check hot buffer has recent data
psql $PG_SILVER -c "SELECT MAX(ledger_sequence) FROM accounts_snapshot;"
```

---

## Future Enhancements

### Planned Features

1. **Additional Endpoints**:
   - Ledger details
   - Contract code and data queries
   - Liquidity pool analytics

2. **Performance Optimizations**:
   - Query result caching (60s TTL)
   - Partition pruning by ledger range
   - Aggregation pre-computation

3. **Analytics**:
   - Account growth metrics
   - Payment volume by asset
   - Network activity heatmaps
   - Soroban contract analytics

4. **Public Access**:
   - Traefik SSL termination
   - Domain name (api.obsrvr-lake.withobsrvr.com)
   - Rate limiting
   - API keys

---

## Support

For issues or questions:
- Check logs: `nomad alloc logs -job stellar-query-api`
- Review health: `curl http://localhost:8092/health`
- Verify services: `nomad job status`

## Changelog

### v0.2.0 (2026-01-01)
- ✨ Added hot+cold unified querying
- ✨ Created SilverHotReader for PostgreSQL silver_hot
- ✨ Created UnifiedSilverReader combining hot + cold
- 🚀 All endpoints now query both hot and cold sources
- 📊 Real-time analytics with < 1 minute lag

### v0.1.0 (2025-12-31)
- 🎉 Initial Silver API release
- 📊 Basic DuckLake Silver querying (cold only)
- 🔍 Account, operations, transfers endpoints
