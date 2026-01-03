# Silver API Sample Queries & Data

## Overview
The Silver API provides analytics-ready data from DuckLake (Parquet files on Backblaze B2), offering efficient querying of historical Stellar blockchain data.

**Base URL**: `http://134.209.123.25:8092/api/v1/silver`

**Current Statistics**:
- Total rows flushed: 7,923
- Last watermark: Ledger 262,217
- Flush interval: Every 3 hours
- Next flush: 2h 45m

---

## 1. Health Check

**Endpoint**: `GET /health`

**Query**:
```bash
curl http://134.209.123.25:8092/health
```

**Response**:
```json
{
    "layers": {
        "bronze": true,
        "hot": true,
        "silver": true
    },
    "status": "healthy"
}
```

---

## 2. Top Accounts by Balance

**Endpoint**: `GET /api/v1/silver/accounts/top?limit=10`

**Query**:
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
            "account_id": "GBBMU27AES3BR3VKGFHZGTX5JNFGRJFBHD2C6Q3SUP7I2SU6DP6ZKN6O",
            "balance": "9999.5869000",
            "sequence_number": "1937030260816",
            "num_subentries": 12,
            "last_modified_ledger": 262215,
            "updated_at": "2026-01-01T21:54:41.12971Z"
        }
        // ... more accounts
    ],
    "count": 7
}
```

**Use Cases**:
- Leaderboards showing richest accounts
- Distribution analysis
- Whale watching

---

## 3. Account Current State

**Endpoint**: `GET /api/v1/silver/accounts/current?account_id={account_id}`

**Query**:
```bash
curl 'http://134.209.123.25:8092/api/v1/silver/accounts/current?account_id=GBBMU27AES3BR3VKGFHZGTX5JNFGRJFBHD2C6Q3SUP7I2SU6DP6ZKN6O'
```

**Response**:
```json
{
    "account": {
        "account_id": "GBBMU27AES3BR3VKGFHZGTX5JNFGRJFBHD2C6Q3SUP7I2SU6DP6ZKN6O",
        "balance": "9999.5869000",
        "sequence_number": "1937030260816",
        "num_subentries": 12,
        "last_modified_ledger": 262215,
        "updated_at": "2026-01-01T21:54:41.12971Z"
    }
}
```

**Fields**:
- `balance`: Account balance in stroops (XLM * 10^7)
- `sequence_number`: Last transaction sequence number
- `num_subentries`: Number of trustlines, offers, data entries, etc.
- `last_modified_ledger`: Ledger where account was last modified

---

## 4. Account History (Snapshots)

**Endpoint**: `GET /api/v1/silver/accounts/history?account_id={account_id}&limit={limit}`

**Query**:
```bash
curl 'http://134.209.123.25:8092/api/v1/silver/accounts/history?account_id=GBBMU27AES3BR3VKGFHZGTX5JNFGRJFBHD2C6Q3SUP7I2SU6DP6ZKN6O&limit=10'
```

**Response**:
```json
{
    "account_id": "GBBMU27AES3BR3VKGFHZGTX5JNFGRJFBHD2C6Q3SUP7I2SU6DP6ZKN6O",
    "count": 1,
    "history": [
        {
            "account_id": "GBBMU27AES3BR3VKGFHZGTX5JNFGRJFBHD2C6Q3SUP7I2SU6DP6ZKN6O",
            "balance": "9999.5869000",
            "sequence_number": "1937030260816",
            "ledger_sequence": 262215,
            "closed_at": "2026-01-01T21:54:35Z"
        }
    ]
}
```

**Use Cases**:
- Historical balance tracking
- Account state time-travel queries
- Audit trails

---

## 5. Enriched Operations

**Endpoint**: `GET /api/v1/silver/operations/enriched?limit={limit}`

**Query**:
```bash
curl 'http://134.209.123.25:8092/api/v1/silver/operations/enriched?limit=5'
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
            "transaction_hash": "65c7d513ec279929084a57b2121e4c689711777e578ccda19ce7c745326fa668",
            "operation_id": 3,
            "ledger_sequence": 262216,
            "ledger_closed_at": "2026-01-01T21:54:40Z",
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
        // ... more operations
    ]
}
```

**Filter Parameters**:
- `account_id`: Filter by source or destination account
- `tx_hash`: Filter by transaction hash
- `start_ledger` / `end_ledger`: Ledger range
- `payments_only=true`: Only payment operations
- `soroban_only=true`: Only Soroban smart contract operations

**Example Filtered Query**:
```bash
# Get all payments in a ledger range
curl 'http://134.209.123.25:8092/api/v1/silver/operations/enriched?start_ledger=262200&end_ledger=262220&payments_only=true'
```

---

## 6. Payments Only

**Endpoint**: `GET /api/v1/silver/payments?limit={limit}`

**Query**:
```bash
curl 'http://134.209.123.25:8092/api/v1/silver/payments?limit=5'
```

**Response**:
```json
{
    "count": 4,
    "payments": [
        {
            "transaction_hash": "65c7d513ec279929084a57b2121e4c689711777e578ccda19ce7c745326fa668",
            "operation_id": 3,
            "ledger_sequence": 262216,
            "ledger_closed_at": "2026-01-01T21:54:40Z",
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
        // ... more payments
    ]
}
```

**Use Cases**:
- Payment flow analysis
- Transaction volume tracking
- Payment network graphs

---

## Data Storage Architecture

```
PostgreSQL silver_hot (Real-time buffer)
           ↓ (Flush every 3 hours)
DuckLake Silver (Parquet on Backblaze B2)
           ↓
Stellar Query API (DuckDB reader)
```

**Current Data in Silver Layer**:
- `accounts_current`: 7 rows
- `accounts_snapshot`: 9 historical snapshots
- `enriched_history_operations`: 10 operations
- `enriched_history_operations_soroban`: 7,894 Soroban operations
- `token_transfers_raw`: 3 token transfers

**Total**: 7,923 rows across 13 tables

---

## Available Tables

### Current State Tables (5)
- `accounts_current` - Current account balances and metadata
- `trustlines_current` - Active trustlines
- `offers_current` - Active offers
- `claimable_balances_current` - Claimable balance entries
- `contract_data_current` - Soroban contract data

### Snapshot Tables (5)
- `accounts_snapshot` - Historical account states
- `trustlines_snapshot` - Historical trustline states
- `offers_snapshot` - Historical offer states
- `account_signers_snapshot` - Historical signer configurations
- `claimable_balances_snapshot` - Historical claimable balances

### Event/Enriched Tables (3)
- `enriched_history_operations` - All operations with full context
- `enriched_history_operations_soroban` - Soroban-specific operations
- `token_transfers_raw` - Token transfer events

---

## Query Performance

**Storage Format**: Apache Parquet (columnar)
**Compression**: Snappy
**Storage Location**: Backblaze B2 (S3-compatible)
**Catalog**: PostgreSQL metadata store

**Benefits**:
- Fast analytical queries on historical data
- Efficient compression (20:1 typical ratio)
- Columnar format ideal for aggregations
- Low storage costs ($0.005/GB/month)

---

## Future Enhancements

1. **Additional Endpoints**:
   - `/api/v1/silver/explorer/ledger/{sequence}` - Ledger details
   - `/api/v1/silver/explorer/transaction/{hash}` - Transaction details
   - `/api/v1/silver/transfers/token` - Token transfer analysis

2. **Analytics**:
   - Account growth metrics
   - Payment volume by asset
   - Network activity heatmaps
   - Soroban contract analytics

3. **Optimizations**:
   - Query result caching
   - Partition pruning by ledger range
   - Aggregation pre-computation
