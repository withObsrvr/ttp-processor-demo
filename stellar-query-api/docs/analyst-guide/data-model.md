# Data Model

Obsrvr Lake organizes data into two layers: **Bronze** (raw) and **Silver** (analytics-ready). Each layer has both hot (recent) and cold (historical) storage.

## Bronze vs Silver

### Bronze Layer - Raw Data

Bronze tables mirror Stellar's native data model. Use Bronze when you need:
- Raw, unprocessed blockchain data
- Building custom analytics from scratch
- Cross-referencing with Horizon API responses

```
┌─────────────────────────────────────────────────────────────────┐
│ BRONZE LAYER                                                     │
│                                                                  │
│ Tables:                                                          │
│   ledgers          - Block metadata (sequence, close time)       │
│   transactions     - Transaction envelopes and results           │
│   operations       - Individual operations within transactions   │
│   effects          - Side effects of operations                  │
│   trades           - DEX trades                                  │
│   accounts         - Account snapshots (per ledger)              │
│   trustlines       - Asset trustlines                           │
│   offers           - Order book offers                          │
│   contract_events  - Raw Soroban contract events                │
│                                                                  │
│ API: /api/v1/bronze/*                                           │
└─────────────────────────────────────────────────────────────────┘
```

### Silver Layer - Analytics-Ready

Silver tables are pre-processed for common analytics queries. Use Silver when you want:
- Fast answers to common questions
- Pre-joined, denormalized data
- No complex SQL required

```
┌─────────────────────────────────────────────────────────────────┐
│ SILVER LAYER                                                     │
│                                                                  │
│ Core Tables:                                                     │
│   accounts_current           - Latest state for each account    │
│                                (balance, signers, thresholds)   │
│   enriched_operations        - Operations with decoded types    │
│   token_transfers            - Unified payment/path payment     │
│   contract_invocation_calls  - Smart contract call graph        │
│                                                                  │
│ Additional Tables:                                               │
│   trustlines_current         - Current trustline states         │
│   offers_current             - Current DEX order book offers    │
│   claimable_balances_current - Current claimable balances       │
│   contract_data_current      - Current Soroban contract state   │
│                                                                  │
│ API: /api/v1/silver/*                                           │
└─────────────────────────────────────────────────────────────────┘
```

## Hot vs Cold Storage

The API automatically queries both and merges results. You don't need to manage this.

| Storage | Technology | Data Range | Query Speed | Best For |
|---------|------------|------------|-------------|----------|
| **Hot** | PostgreSQL | Last ~7 days | Sub-second | Real-time queries, recent activity |
| **Cold** | DuckLake (Parquet) | Full history | 1-10 seconds | Historical analysis, large scans |

### How Merging Works

1. Your query hits the API
2. API checks if the query needs hot data, cold data, or both
3. Results are merged, deduplicated, and returned in order
4. Cursor pagination works seamlessly across hot/cold boundary

You'll never see duplicate records or gaps between hot and cold.

## Key Tables Explained

### accounts_current

Current state of every account that's ever existed on Stellar.

| Column | Description |
|--------|-------------|
| `account_id` | Stellar public key (G...) |
| `balance` | XLM balance in stroops (divide by 10^7 for XLM) |
| `sequence_number` | Account sequence for transaction signing |
| `num_subentries` | Count of trustlines, offers, signers, data |
| `last_modified_ledger` | When account was last changed |
| `signers` | JSON array of account signers with weights and types |
| `master_weight` | Weight of the master key (default: 1) |
| `low_threshold` | Threshold for low-security operations |
| `med_threshold` | Threshold for medium-security operations |
| `high_threshold` | Threshold for high-security operations |

```bash
# Get current balance
curl "https://gateway.withobsrvr.com/api/v1/silver/accounts/current?account_id=G..."

# Get account signers and thresholds
curl "https://gateway.withobsrvr.com/api/v1/silver/accounts/signers?account_id=G..."
```

### enriched_operations

Operations with human-readable fields and joined metadata.

| Column | Description |
|--------|-------------|
| `operation_id` | Unique operation identifier |
| `type` | Operation type (payment, create_account, etc.) |
| `source_account` | Account that submitted the operation |
| `transaction_hash` | Parent transaction |
| `details` | Type-specific decoded details (JSON) |
| `ledger_sequence` | Block number |
| `closed_at` | Timestamp |

```bash
# Get operations for an account
curl "https://gateway.withobsrvr.com/api/v1/silver/operations/enriched?account_id=G..."
```

### token_transfers

Unified view of all token movements (payments, path payments, claimable balance claims).

| Column | Description |
|--------|-------------|
| `from_account` | Sender |
| `to_account` | Recipient |
| `asset_code` | Asset code (XLM, USDC, etc.) |
| `asset_issuer` | Asset issuer (null for XLM) |
| `amount` | Amount transferred |
| `transaction_hash` | Transaction containing the transfer |

```bash
# Get USDC transfers for an account
curl "https://gateway.withobsrvr.com/api/v1/silver/transfers?account_id=G...&asset_code=USDC"
```

### contract_invocation_calls

Smart contract call graph for Soroban transactions.

| Column | Description |
|--------|-------------|
| `transaction_hash` | Transaction containing the call |
| `from_contract` | Calling contract (C...) |
| `to_contract` | Called contract (C...) |
| `function_name` | Function being called |
| `call_depth` | Nesting level (0 = top-level) |
| `execution_order` | Order of execution within transaction |
| `successful` | Whether the call succeeded |

```bash
# Get call graph for a transaction
curl "https://gateway.withobsrvr.com/api/v1/silver/tx/{hash}/call-graph"
```

## Stroops and Amounts

All amounts in Stellar are stored as integers in "stroops" (1 stroop = 0.0000001 XLM).

To convert to decimal:
```
Amount in XLM = stroops / 10,000,000
Amount in XLM = stroops / 10^7
```

Example: `balance: 10000000000` = 1,000 XLM

## Timestamps

All timestamps are in RFC3339 format (ISO 8601):
```
"2026-01-05T12:57:07Z"
```

The `Z` suffix indicates UTC. Ledger close times are when the Stellar network finalized that block.

## Pagination

All list endpoints support cursor-based pagination:

```bash
# First page
curl ".../api/v1/silver/transfers?limit=100"

# Response includes cursor
{
  "transfers": [...],
  "cursor": "MjEzNzkxODo1",
  "has_more": true
}

# Next page
curl ".../api/v1/silver/transfers?limit=100&cursor=MjEzNzkxODo1"
```

Continue until `has_more: false`.
