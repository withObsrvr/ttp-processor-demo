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
│ BRONZE LAYER (9 tables)                                         │
│                                                                  │
│ Event Stream Tables (append-only, ordered by ledger):            │
│   ledgers                     - Block metadata and protocol info │
│   transactions                - Transaction envelopes and results│
│   operations                  - Individual operations            │
│   effects                     - Side effects of operations       │
│   trades                      - DEX trades                       │
│   contract_events             - Soroban contract events          │
│                                                                  │
│ State Tables (current state):                                    │
│   accounts                    - Account states                   │
│   trustlines                  - Asset trustline states           │
│   offers                      - DEX offer states                 │
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
│ SILVER LAYER (Organized by Domain)                              │
│                                                                  │
│ Account Tables:                                                  │
│   accounts                    - List/search accounts            │
│   accounts_current            - Latest state for each account   │
│   accounts_history            - Historical account snapshots    │
│   accounts_top                - Leaderboard by balance          │
│   accounts_signers            - Account signer configurations   │
│   accounts_balances           - XLM + trustline balances        │
│   accounts_activity           - Account activity summary        │
│                                                                  │
│ Operations & Payments:                                           │
│   enriched_history_operations - Operations with full context    │
│   enriched_history_operations_soroban - Soroban operations only │
│   payments                    - Payment operations only         │
│   token_transfers             - Unified transfers (classic+SAC) │
│                                                                  │
│ DEX Tables:                                                      │
│   trades                      - DEX trade history               │
│   offers                      - Current DEX order book          │
│   liquidity_pools             - AMM pool states                 │
│                                                                  │
│ Effects:                                                         │
│   effects                     - Operation side effects          │
│   effect_types                - Effect type aggregations        │
│                                                                  │
│ Claimable Balances:                                              │
│   claimable_balances          - Current claimable balances      │
│                                                                  │
│ Soroban/Contract Tables:                                         │
│   contract_code               - Deployed WASM code              │
│   contract_data               - Contract state data             │
│   contract_invocations        - Contract call history           │
│   ttl                         - Contract data TTL/expiration    │
│   evicted_keys                - Expired contract entries        │
│   restored_keys               - Restored contract entries       │
│   config_settings             - Soroban network config          │
│                                                                  │
│ CAP-67 Events:                                                   │
│   unified_events              - CAP-67 unified event stream     │
│                                                                  │
│ SEP-41 Tokens:                                                   │
│   tokens                      - Token metadata & stats          │
│   token_balances              - Computed holder balances         │
│   token_portfolio             - Address token holdings          │
│                                                                  │
│ Transaction Decoding:                                            │
│   decoded_transactions        - Human-readable tx summaries     │
│   contract_interfaces         - Detected contract ABIs          │
│                                                                  │
│ Analytics Views:                                                 │
│   stats_network               - Network-wide statistics         │
│   contracts_top               - Most active contracts           │
│   contract_analytics          - Per-contract analytics          │
│                                                                  │
│ API: /api/v1/silver/*                                           │
└─────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│ SEMANTIC LAYER (Meaning-oriented, human-readable)               │
│                                                                  │
│ Activities:                                                      │
│   semantic_activities         - Unified on-chain action feed    │
│                                 (payments, swaps, contract calls)│
│                                                                  │
│ Entities:                                                        │
│   semantic_entities_contracts - Contract registry with types,   │
│                                 usage stats, observed functions  │
│                                                                  │
│ Flows:                                                           │
│   semantic_flows_value        - Normalized value transfers      │
│                                 (transfers, mints, burns)       │
│                                                                  │
│ API: /api/v1/semantic/*                                         │
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

---

## Silver Tables Reference

### Account Tables

#### accounts

List and search all accounts with flexible ordering.

**Endpoint:** `GET /api/v1/silver/accounts`

| Column | Type | Description |
|--------|------|-------------|
| `account_id` | string | Stellar public key (G...) |
| `balance` | string | XLM balance (decimal format) |
| `sequence_number` | string | Account sequence for transaction signing |
| `num_subentries` | int | Count of trustlines, offers, signers, data |
| `last_modified_ledger` | int | When account was last changed |
| `updated_at` | timestamp | Internal update timestamp |

```bash
curl -H "Authorization: Api-Key $API_KEY" \
  "https://gateway.withobsrvr.com/lake/v1/testnet/api/v1/silver/accounts?limit=10"
```

#### accounts_current

Get current state for a specific account.

**Endpoint:** `GET /api/v1/silver/accounts/current`

| Parameter | Required | Description |
|-----------|----------|-------------|
| `account_id` | Yes | Stellar account ID (G...) |

```bash
curl -H "Authorization: Api-Key $API_KEY" \
  "https://gateway.withobsrvr.com/lake/v1/testnet/api/v1/silver/accounts/current?account_id=GAIH3ULLFQ4DGSECF2AR555KZ4KNDGEKN4AFI4SU2M7B43MGK3QJZNSR"
```

#### accounts_history

Historical snapshots of an account's state over time.

**Endpoint:** `GET /api/v1/silver/accounts/history`

| Column | Type | Description |
|--------|------|-------------|
| `account_id` | string | Stellar public key |
| `balance` | string | XLM balance at this point |
| `sequence_number` | string | Sequence at this point |
| `ledger_sequence` | int | Ledger when this snapshot was taken |
| `closed_at` | timestamp | Ledger close time |
| `valid_to` | timestamp | When this state was superseded (null = current) |

```bash
curl -H "Authorization: Api-Key $API_KEY" \
  "https://gateway.withobsrvr.com/lake/v1/testnet/api/v1/silver/accounts/history?account_id=G...&limit=10"
```

#### accounts_top

Accounts ranked by XLM balance (leaderboard).

**Endpoint:** `GET /api/v1/silver/accounts/top`

```bash
curl -H "Authorization: Api-Key $API_KEY" \
  "https://gateway.withobsrvr.com/lake/v1/testnet/api/v1/silver/accounts/top?limit=100"
```

#### accounts_balances

All balances for an account (XLM + trustline assets).

**Endpoint:** `GET /api/v1/silver/accounts/{id}/balances`

| Column | Type | Description |
|--------|------|-------------|
| `asset_type` | string | `native`, `credit_alphanum4`, `credit_alphanum12` |
| `asset_code` | string | Asset code (XLM, USDC, etc.) |
| `asset_issuer` | string | Issuer account (null for native) |
| `balance` | string | Decimal balance |
| `balance_stroops` | int | Balance in stroops (for native) |

```bash
curl -H "Authorization: Api-Key $API_KEY" \
  "https://gateway.withobsrvr.com/lake/v1/testnet/api/v1/silver/accounts/GAIH3ULLFQ4DGSECF2AR555KZ4KNDGEKN4AFI4SU2M7B43MGK3QJZNSR/balances"
```

---

### Operations & Payments

#### enriched_history_operations

All operations with transaction and ledger context.

**Endpoint:** `GET /api/v1/silver/operations/enriched`

| Column | Type | Description |
|--------|------|-------------|
| `transaction_hash` | string | Parent transaction hash |
| `operation_id` | int | Index within transaction |
| `ledger_sequence` | int | Block number |
| `ledger_closed_at` | timestamp | Block timestamp |
| `source_account` | string | Account that submitted the operation |
| `type` | int | Operation type code |
| `type_name` | string | Human-readable type (PAYMENT, CREATE_ACCOUNT, etc.) |
| `destination` | string | Target account (for payments) |
| `amount` | string | Amount (for payments/transfers) |
| `tx_successful` | bool | Whether the transaction succeeded |
| `tx_fee_charged` | int | Fee in stroops |
| `is_payment_op` | bool | True for payment operations |
| `is_soroban_op` | bool | True for Soroban operations |
| `contract_id` | string | Contract address for Soroban ops (C...) |
| `function_name` | string | Called function name for Soroban ops |
| `arguments_json` | string | Function arguments as JSON for Soroban ops |

**Parameters:**
| Name | Type | Description |
|------|------|-------------|
| `account_id` | string | Filter by source or destination account |
| `tx_hash` | string | Filter by transaction hash |
| `start_ledger` | int | Start ledger (inclusive) |
| `end_ledger` | int | End ledger (inclusive) |
| `payments_only` | bool | Only payment operations |
| `soroban_only` | bool | Only Soroban operations |
| `limit` | int | Max records (1-1000, default 100) |
| `cursor` | string | Pagination cursor |

```bash
curl -H "Authorization: Api-Key $API_KEY" \
  "https://gateway.withobsrvr.com/lake/v1/testnet/api/v1/silver/operations/enriched?limit=50"
```

#### payments

Convenience endpoint for payment operations only.

**Endpoint:** `GET /api/v1/silver/payments`

Same schema as `enriched_history_operations`, filtered to `is_payment_op=true`.

```bash
curl -H "Authorization: Api-Key $API_KEY" \
  "https://gateway.withobsrvr.com/lake/v1/testnet/api/v1/silver/payments?account_id=G...&limit=20"
```

#### operations_soroban

Convenience endpoint for Soroban smart contract operations. Includes `contract_id`, `function_name`, and `arguments_json` fields.

**Endpoint:** `GET /api/v1/silver/operations/soroban`

Same schema as `enriched_history_operations`, filtered to `is_soroban_op=true`.

**Additional Endpoint:**
- `GET /operations/soroban/by-function` - Filter by `contract_id` and/or `function_name`

---

### Token Transfers

#### token_transfers

Unified view of all token movements (classic payments, path payments, SAC transfers).

**Endpoint:** `GET /api/v1/silver/transfers`

| Column | Type | Description |
|--------|------|-------------|
| `timestamp` | timestamp | Transfer timestamp |
| `transaction_hash` | string | Transaction containing the transfer |
| `ledger_sequence` | int | Block number |
| `source_type` | string | `classic` or `soroban` |
| `from_account` | string | Sender |
| `to_account` | string | Recipient |
| `asset_code` | string | Asset code (XLM, USDC, etc.) |
| `asset_issuer` | string | Asset issuer (null for XLM) |
| `amount` | string | Amount transferred |
| `token_contract_id` | string | Contract ID for SAC tokens |
| `transaction_successful` | bool | Whether transaction succeeded |

**Parameters:**
| Name | Type | Description |
|------|------|-------------|
| `source_type` | string | Filter: `classic` or `soroban` |
| `asset_code` | string | Filter by asset |
| `from_account` | string | Filter by sender |
| `to_account` | string | Filter by receiver |
| `start_time` | ISO8601 | Start timestamp (default: -24h) |
| `end_time` | ISO8601 | End timestamp (default: now) |
| `limit` | int | Max records (1-1000, default 100) |

```bash
curl -H "Authorization: Api-Key $API_KEY" \
  "https://gateway.withobsrvr.com/lake/v1/testnet/api/v1/silver/transfers?asset_code=USDC&limit=50"
```

#### transfers_stats

Aggregated transfer statistics.

**Endpoint:** `GET /api/v1/silver/transfers/stats`

| Parameter | Required | Description |
|-----------|----------|-------------|
| `group_by` | Yes | `asset`, `source_type`, `hour`, or `day` |

```bash
curl -H "Authorization: Api-Key $API_KEY" \
  "https://gateway.withobsrvr.com/lake/v1/testnet/api/v1/silver/transfers/stats?group_by=asset"
```

---

### DEX Tables

#### trades

DEX trade history (orderbook + AMM).

**Endpoint:** `GET /api/v1/silver/trades`

| Column | Type | Description |
|--------|------|-------------|
| `ledger_sequence` | int | Block number |
| `transaction_hash` | string | Transaction hash |
| `operation_index` | int | Operation index |
| `trade_index` | int | Trade index within operation |
| `trade_type` | string | `orderbook` or `liquidity_pool` |
| `timestamp` | timestamp | Trade timestamp |
| `seller.account_id` | string | Seller account |
| `selling.asset.code` | string | Sold asset code |
| `selling.asset.issuer` | string | Sold asset issuer |
| `selling.amount` | string | Amount sold |
| `buyer.account_id` | string | Buyer account |
| `buying.asset.code` | string | Bought asset code |
| `buying.asset.issuer` | string | Bought asset issuer |
| `buying.amount` | string | Amount bought |
| `price` | string | Trade price |

```bash
curl -H "Authorization: Api-Key $API_KEY" \
  "https://gateway.withobsrvr.com/lake/v1/testnet/api/v1/silver/trades?limit=20"
```

#### trades/stats

Trade statistics by asset pair.

**Endpoint:** `GET /api/v1/silver/trades/stats`

| Column | Type | Description |
|--------|------|-------------|
| `group` | string | Asset pair (e.g., "USDC/XLM") |
| `trade_count` | int | Number of trades |
| `volume_selling` | string | Total volume sold |
| `volume_buying` | string | Total volume bought |
| `unique_sellers` | int | Unique seller count |
| `unique_buyers` | int | Unique buyer count |
| `avg_price` | string | Average trade price |

```bash
curl -H "Authorization: Api-Key $API_KEY" \
  "https://gateway.withobsrvr.com/lake/v1/testnet/api/v1/silver/trades/stats"
```

#### offers

Current DEX order book offers.

**Endpoint:** `GET /api/v1/silver/offers`

| Column | Type | Description |
|--------|------|-------------|
| `offer_id` | int | Unique offer ID |
| `seller_id` | string | Account selling |
| `selling.code` | string | Selling asset code |
| `selling.issuer` | string | Selling asset issuer |
| `selling.type` | string | Asset type |
| `buying.code` | string | Buying asset code |
| `buying.issuer` | string | Buying asset issuer |
| `buying.type` | string | Asset type |
| `amount` | string | Amount for sale |
| `price` | string | Price (decimal) |
| `price_r.n` | int | Price numerator |
| `price_r.d` | int | Price denominator |
| `last_modified_ledger` | int | Last update ledger |

**Additional Endpoints:**
- `GET /offers/pair` - Filter by trading pair
- `GET /offers/{id}` - Get single offer

```bash
curl -H "Authorization: Api-Key $API_KEY" \
  "https://gateway.withobsrvr.com/lake/v1/testnet/api/v1/silver/offers?limit=20"
```

#### liquidity_pools

AMM (Automated Market Maker) liquidity pool states.

**Endpoint:** `GET /api/v1/silver/liquidity-pools`

| Column | Type | Description |
|--------|------|-------------|
| `pool_id` | string | Unique pool identifier (hash) |
| `pool_type` | string | Pool type (`constant_product`) |
| `fee_bp` | int | Fee in basis points (30 = 0.3%) |
| `trustline_count` | int | Accounts holding pool shares |
| `total_shares` | string | Total pool shares outstanding |
| `reserves[0].asset.code` | string | First asset code |
| `reserves[0].asset.issuer` | string | First asset issuer |
| `reserves[0].amount` | string | First asset amount |
| `reserves[1].asset.code` | string | Second asset code |
| `reserves[1].asset.issuer` | string | Second asset issuer |
| `reserves[1].amount` | string | Second asset amount |
| `last_modified_ledger` | int | Last update ledger |

**Additional Endpoints:**
- `GET /liquidity-pools/asset` - Filter by asset
- `GET /liquidity-pools/{id}` - Get single pool

```bash
curl -H "Authorization: Api-Key $API_KEY" \
  "https://gateway.withobsrvr.com/lake/v1/testnet/api/v1/silver/liquidity-pools?limit=10"
```

---

### Effects

#### effects

Operation side effects (append-only event stream).

**Endpoint:** `GET /api/v1/silver/effects`

| Column | Type | Description |
|--------|------|-------------|
| `ledger_sequence` | int | Block number |
| `transaction_hash` | string | Parent transaction |
| `operation_index` | int | Parent operation index |
| `effect_index` | int | Effect index within operation |
| `effect_type` | int | Effect type code |
| `effect_type_string` | string | Human-readable type |
| `account_id` | string | Affected account |
| `asset.code` | string | Asset code (if applicable) |
| `asset.type` | string | Asset type |
| `amount` | string | Amount (if applicable) |
| `timestamp` | timestamp | Effect timestamp |

```bash
curl -H "Authorization: Api-Key $API_KEY" \
  "https://gateway.withobsrvr.com/lake/v1/testnet/api/v1/silver/effects?limit=20"
```

#### effects/types

Aggregated effect type counts.

**Endpoint:** `GET /api/v1/silver/effects/types`

```bash
curl -H "Authorization: Api-Key $API_KEY" \
  "https://gateway.withobsrvr.com/lake/v1/testnet/api/v1/silver/effects/types"
```

**Response:**
```json
{
  "effect_types": [
    {"type": 3, "name": "account_debited", "count": 435963},
    {"type": 2, "name": "account_credited", "count": 122865}
  ],
  "total_effects": 558828
}
```

---

### Claimable Balances

**Endpoint:** `GET /api/v1/silver/claimable-balances`

| Column | Type | Description |
|--------|------|-------------|
| `balance_id` | string | Unique claimable balance ID |
| `asset.code` | string | Asset code |
| `asset.issuer` | string | Asset issuer |
| `amount` | string | Claimable amount |
| `sponsor` | string | Sponsor account |
| `claimants` | array | List of claimant predicates |
| `last_modified_ledger` | int | Last update ledger |

**Additional Endpoints:**
- `GET /claimable-balances/asset` - Filter by asset
- `GET /claimable-balances/{id}` - Get single balance

---

### Soroban Tables

#### contract_code

Deployed Soroban smart contract WASM code.

**Endpoint:** `GET /api/v1/silver/soroban/contract-code`

| Parameter | Required | Description |
|-----------|----------|-------------|
| `hash` | Yes | Contract code hash |

#### contract_data

Soroban contract state data entries.

**Endpoint:** `GET /api/v1/silver/soroban/contract-data`

| Parameter | Type | Description |
|-----------|------|-------------|
| `contract_id` | string | Contract address (C...) |
| `limit` | int | Max records |

**Additional Endpoints:**
- `GET /soroban/contract-data/entry` - Get specific entry by key

#### ttl

Time-to-live (TTL) for contract data entries.

**Endpoint:** `GET /api/v1/silver/soroban/ttl`

| Column | Type | Description |
|--------|------|-------------|
| `key_hash` | string | Hash of the contract data key |
| `live_until_ledger_seq` | int | Ledger when entry expires |
| `last_modified_ledger` | int | When TTL was last updated |

**Additional Endpoints:**
- `GET /soroban/ttl/expiring` - Entries expiring soon
- `GET /soroban/ttl/expired` - Already expired entries

#### evicted_keys

Contract data entries that have expired and been evicted.

**Endpoint:** `GET /api/v1/silver/soroban/evicted-keys`

| Column | Type | Description |
|--------|------|-------------|
| `contract_id` | string | Contract that owned the data |
| `key_hash` | string | Hash of the evicted key |
| `ledger_sequence` | int | Ledger when eviction occurred |
| `closed_at` | timestamp | Timestamp of eviction |

```bash
curl -H "Authorization: Api-Key $API_KEY" \
  "https://gateway.withobsrvr.com/lake/v1/testnet/api/v1/silver/soroban/evicted-keys?limit=10"
```

#### restored_keys

Contract data entries that have been restored after eviction.

**Endpoint:** `GET /api/v1/silver/soroban/restored-keys`

| Column | Type | Description |
|--------|------|-------------|
| `contract_id` | string | Contract that owns the data |
| `key_hash` | string | Hash of the restored key |
| `ledger_sequence` | int | Ledger when restoration occurred |
| `closed_at` | timestamp | Timestamp of restoration |

```bash
curl -H "Authorization: Api-Key $API_KEY" \
  "https://gateway.withobsrvr.com/lake/v1/testnet/api/v1/silver/soroban/restored-keys?limit=10"
```

#### config_settings

Soroban network configuration parameters.

**Endpoint:** `GET /api/v1/silver/soroban/config`

**Additional Endpoints:**
- `GET /soroban/config/limits` - Just the limit parameters

---

### Network Statistics

#### stats/network

Network-wide statistics and metrics.

**Endpoint:** `GET /api/v1/silver/stats/network`

```bash
curl -H "Authorization: Api-Key $API_KEY" \
  "https://gateway.withobsrvr.com/lake/v1/testnet/api/v1/silver/stats/network"
```

**Response:**
```json
{
  "generated_at": "2026-01-10T05:57:58Z",
  "data_freshness": "real-time",
  "accounts": {
    "total": 354402,
    "active_24h": 363,
    "created_24h": 362
  },
  "ledger": {
    "current_sequence": 406095,
    "avg_close_time_seconds": 5
  },
  "operations_24h": {
    "total": 1556,
    "payments": 441,
    "path_payments": 263,
    "create_account": 362,
    "account_merge": 41,
    "change_trust": 294,
    "manage_offer": 4,
    "contract_invoke": 67,
    "other": 84
  }
}
```

---

### Contract Analytics

#### contracts/top

Most active smart contracts by call count.

**Endpoint:** `GET /api/v1/silver/contracts/top`

| Parameter | Type | Description |
|-----------|------|-------------|
| `period` | string | Time period: `24h`, `7d`, `30d` (default: 24h) |
| `limit` | int | Max results (default: 10) |

```bash
curl -H "Authorization: Api-Key $API_KEY" \
  "https://gateway.withobsrvr.com/lake/v1/testnet/api/v1/silver/contracts/top?period=24h&limit=10"
```

**Response:**
```json
{
  "contracts": [
    {
      "contract_id": "CAUGJT4GREIY3WHOUUU5RIUDGSPVREF5CDCYJOWMHOVT2GWQT5JEETGJ",
      "total_calls": 488,
      "unique_callers": 5,
      "top_function": "transfer",
      "unknown_calls": 440,
      "last_activity": "2026-01-09T13:31:00Z"
    }
  ],
  "count": 10,
  "period": "24h"
}
```

#### contracts/{id}/analytics

Comprehensive analytics for a specific contract.

**Endpoint:** `GET /api/v1/silver/contracts/{id}/analytics`

```bash
curl -H "Authorization: Api-Key $API_KEY" \
  "https://gateway.withobsrvr.com/lake/v1/testnet/api/v1/silver/contracts/CAUGJT4GREIY3WHOUUU5RIUDGSPVREF5CDCYJOWMHOVT2GWQT5JEETGJ/analytics"
```

**Response:**
```json
{
  "contract_id": "CAUGJT4GREIY3WHOUUU5RIUDGSPVREF5CDCYJOWMHOVT2GWQT5JEETGJ",
  "stats": {
    "total_calls_as_caller": 1551,
    "total_calls_as_callee": 3735,
    "unique_callers": 5,
    "unique_callees": 1551
  },
  "timeline": {
    "first_seen": "2025-12-19T03:55:21Z",
    "last_activity": "2026-01-09T13:31:00Z"
  },
  "top_functions": [
    {"name": "unknown", "count": 3429},
    {"name": "transfer", "count": 306}
  ],
  "daily_calls_7d": [
    {"date": "2026-01-09", "count": 679},
    {"date": "2026-01-08", "count": 200}
  ]
}
```

**Additional Contract Endpoints:**
- `GET /contracts/{id}/recent-calls` - Recent invocations (hot + cold storage)
- `GET /contracts/{id}/callers` - Who calls this contract
- `GET /contracts/{id}/callees` - Contracts this one calls
- `GET /contracts/{id}/call-summary` - Call statistics
- `GET /contracts/{id}/interface` - Detected contract interface (SEP-41 or unknown)

#### Transaction Contract Analysis

Analyze smart contract interactions within a transaction.

- `GET /tx/{hash}/contracts-involved` - All contracts in transaction
- `GET /tx/{hash}/call-graph` - Contract call graph
- `GET /tx/{hash}/hierarchy` - Transaction operation hierarchy
- `GET /tx/{hash}/contracts-summary` - Summary of contract calls
- `GET /tx/{hash}/events` - CAP-67 unified events for the transaction
- `GET /tx/{hash}/decoded` - Human-readable decoded transaction with summary

```bash
curl -H "Authorization: Api-Key $API_KEY" \
  "https://gateway.withobsrvr.com/lake/v1/testnet/api/v1/silver/tx/abc123.../call-graph"
```

---

### CAP-67 Unified Events

The CAP-67 unified event stream provides a single view of all token events (transfers, mints, burns) across both classic Stellar and Soroban smart contracts. Event types are derived from the `token_transfers_raw` table.

**Endpoints:**
- `GET /events` - Full event stream with filters
- `GET /events/by-contract` - Events for a specific contract
- `GET /address/{addr}/events` - Events for an address (as sender or receiver)
- `GET /tx/{hash}/events` - Events for a transaction

| Column | Type | Description |
|--------|------|-------------|
| `event_id` | string | Composite key: `{ledger}-{tx_hash}-{index}` |
| `contract_id` | string | Token contract address (C...) |
| `ledger_sequence` | int | Block number |
| `tx_hash` | string | Transaction hash |
| `closed_at` | timestamp | Block close time |
| `event_type` | string | `transfer`, `mint`, or `burn` |
| `from` | string | Sender (null for mints) |
| `to` | string | Receiver (null for burns) |
| `amount` | string | Amount transferred |
| `asset_code` | string | Asset code (USDC, XLM, etc.) |
| `asset_issuer` | string | Asset issuer (null for native) |
| `source_type` | string | `classic` or `soroban` |
| `event_index` | int | Index within the transaction |

**Event Type Derivation:**
| Condition | Event Type |
|-----------|-----------|
| `from` is null | `mint` |
| `to` is null | `burn` |
| Both present | `transfer` |

```bash
# Get recent mint events
curl -H "Authorization: Api-Key $API_KEY" \
  "https://gateway.withobsrvr.com/lake/v1/testnet/api/v1/silver/events?event_type=mint&limit=10"

# Get all events for a contract
curl -H "Authorization: Api-Key $API_KEY" \
  "https://gateway.withobsrvr.com/lake/v1/testnet/api/v1/silver/events/by-contract?contract_id=CDLZFC3..."
```

---

### SEP-41 Token Data

SEP-41 token data provides metadata, balances, and transfer history for fungible tokens. Balances are computed from the full transfer history (received minus sent), providing a built-in audit trail.

**Endpoints:**
- `GET /tokens/{contract_id}` - Token metadata
- `GET /tokens/{contract_id}/balances` - All holder balances
- `GET /tokens/{contract_id}/balance/{address}` - Single holder balance
- `GET /tokens/{contract_id}/transfers` - Transfer history
- `GET /tokens/{contract_id}/stats` - Aggregate statistics
- `GET /address/{addr}/token-balances` - Address token portfolio

#### Token Metadata

| Column | Type | Description |
|--------|------|-------------|
| `contract_id` | string | Token contract address (C...) |
| `asset_code` | string | Asset code if available |
| `asset_issuer` | string | Asset issuer if available |
| `source_type` | string | `classic` or `soroban` |
| `decimals` | int | Token decimal places (default: 7) |
| `holder_count` | int | Number of addresses with non-zero balance |
| `transfer_count` | int | Total number of transfers |
| `first_seen` | timestamp | First observed transfer |
| `last_activity` | timestamp | Most recent transfer |

#### Token Balances

| Column | Type | Description |
|--------|------|-------------|
| `address` | string | Holder address (G... or C...) |
| `balance` | string | Net balance (received - sent), decimal format |
| `balance_raw` | int | Net balance in raw units |
| `total_received` | int | Total amount received |
| `total_sent` | int | Total amount sent |
| `tx_count` | int | Number of transfers involving this address |
| `last_activity_ledger` | int | Most recent transfer ledger |
| `last_activity` | timestamp | Most recent transfer timestamp |

#### Token Stats

| Column | Type | Description |
|--------|------|-------------|
| `contract_id` | string | Token contract address |
| `holder_count` | int | Active holders |
| `total_supply` | string | Total supply (decimal) |
| `total_supply_raw` | int | Total supply (raw) |
| `transfers_24h` | int | Transfers in last 24 hours |
| `volume_24h` | string | Volume in last 24 hours (decimal) |
| `volume_24h_raw` | int | Volume in last 24 hours (raw) |
| `asset_code` | string | Asset code if available |

```bash
# Get top USDC holders
curl -H "Authorization: Api-Key $API_KEY" \
  "https://gateway.withobsrvr.com/lake/v1/testnet/api/v1/silver/tokens/CDLZFC3.../balances?limit=20"

# Get all token holdings for an address
curl -H "Authorization: Api-Key $API_KEY" \
  "https://gateway.withobsrvr.com/lake/v1/testnet/api/v1/silver/address/GAIH3.../token-balances"
```

---

### Decoded Transactions

Transaction decoding provides human-readable summaries, Soroban operation details, and contract interface detection.

**Endpoints:**
- `GET /tx/{hash}/decoded` - Full decoded transaction
- `GET /contracts/{id}/interface` - Contract interface detection
- `POST /decode/scval` - Decode Soroban ScVal values

#### Decoded Transaction

| Column | Type | Description |
|--------|------|-------------|
| `tx_hash` | string | Transaction hash |
| `summary.description` | string | Human-readable summary |
| `summary.type` | string | Detected type: transfer, mint, burn, swap, contract_call, classic |
| `summary.involved_contracts` | array | Contract IDs involved |
| `summary.swap` | object | Structured swap details (present when type=swap) |
| `summary.transfer` | object | Structured transfer details (present when type=transfer) |
| `summary.mint` | object | Structured mint details (present when type=mint) |
| `summary.burn` | object | Structured burn details (present when type=burn) |
| `fee` | int | Transaction fee in stroops |
| `ledger_sequence` | int | Block number |
| `closed_at` | timestamp | Block close time |
| `successful` | bool | Whether the transaction succeeded |
| `operation_count` | int | Number of operations |
| `operations[]` | array | Decoded operations with type, contract, function details |
| `events[]` | array | Associated CAP-67 events |

#### Decoded Operation (within transaction)

| Column | Type | Description |
|--------|------|-------------|
| `index` | int | Operation index |
| `type` | int | Operation type code |
| `type_name` | string | Human-readable type name |
| `source_account` | string | Operation source account |
| `contract_id` | string | Contract address (Soroban ops only) |
| `function_name` | string | Called function name (Soroban ops only) |
| `arguments_json` | string | Function arguments as JSON (Soroban ops only) |
| `destination` | string | Destination (payment ops only) |
| `asset_code` | string | Asset code (payment ops only) |
| `amount` | string | Amount (payment ops only) |
| `is_soroban_op` | bool | Whether this is a Soroban operation |

```bash
# Get human-readable transaction summary
curl -H "Authorization: Api-Key $API_KEY" \
  "https://gateway.withobsrvr.com/lake/v1/testnet/api/v1/silver/tx/abc123.../decoded"
```

---

### Explorer Endpoints

Convenience endpoints for block explorer UIs.

#### explorer/account

Comprehensive account overview in single request.

**Endpoint:** `GET /api/v1/silver/explorer/account`

Returns account state + recent operations + recent transfers.

```bash
curl -H "Authorization: Api-Key $API_KEY" \
  "https://gateway.withobsrvr.com/lake/v1/testnet/api/v1/silver/explorer/account?account_id=G..."
```

#### explorer/transaction

Full transaction details with all operations.

**Endpoint:** `GET /api/v1/silver/explorer/transaction`

```bash
curl -H "Authorization: Api-Key $API_KEY" \
  "https://gateway.withobsrvr.com/lake/v1/testnet/api/v1/silver/explorer/transaction?tx_hash=abc123..."
```

#### explorer/asset

Asset statistics and recent activity.

**Endpoint:** `GET /api/v1/silver/explorer/asset`

```bash
curl -H "Authorization: Api-Key $API_KEY" \
  "https://gateway.withobsrvr.com/lake/v1/testnet/api/v1/silver/explorer/asset?asset_code=USDC"
```

---

## Operation Type Reference

| Code | Name | Description |
|------|------|-------------|
| 0 | CREATE_ACCOUNT | Create new account |
| 1 | PAYMENT | Send payment |
| 2 | PATH_PAYMENT_STRICT_RECEIVE | Path payment (strict receive) |
| 3 | MANAGE_SELL_OFFER | Manage sell offer |
| 4 | CREATE_PASSIVE_SELL_OFFER | Create passive sell offer |
| 5 | SET_OPTIONS | Set account options |
| 6 | CHANGE_TRUST | Change trustline |
| 7 | ALLOW_TRUST | Allow trust (deprecated) |
| 8 | ACCOUNT_MERGE | Merge accounts |
| 9 | INFLATION | Inflation (deprecated) |
| 10 | MANAGE_DATA | Manage data entry |
| 11 | BUMP_SEQUENCE | Bump sequence number |
| 12 | MANAGE_BUY_OFFER | Manage buy offer |
| 13 | PATH_PAYMENT_STRICT_SEND | Path payment (strict send) |
| 14 | CREATE_CLAIMABLE_BALANCE | Create claimable balance |
| 15 | CLAIM_CLAIMABLE_BALANCE | Claim claimable balance |
| 16 | BEGIN_SPONSORING_FUTURE_RESERVES | Begin sponsoring |
| 17 | END_SPONSORING_FUTURE_RESERVES | End sponsoring |
| 18 | REVOKE_SPONSORSHIP | Revoke sponsorship |
| 19 | CLAWBACK | Clawback asset |
| 20 | CLAWBACK_CLAIMABLE_BALANCE | Clawback claimable balance |
| 21 | SET_TRUST_LINE_FLAGS | Set trustline flags |
| 22 | LIQUIDITY_POOL_DEPOSIT | Deposit to AMM pool |
| 23 | LIQUIDITY_POOL_WITHDRAW | Withdraw from AMM pool |
| 24 | INVOKE_HOST_FUNCTION | Invoke Soroban contract |
| 25 | EXTEND_FOOTPRINT_TTL | Extend Soroban TTL |
| 26 | RESTORE_FOOTPRINT | Restore Soroban state |

---

## Effect Type Reference

Common effect types you'll encounter:

| Code | Name | Description |
|------|------|-------------|
| 0 | account_created | New account created |
| 1 | account_removed | Account merged/removed |
| 2 | account_credited | Account received funds |
| 3 | account_debited | Account sent funds |
| 10 | trustline_created | New trustline established |
| 11 | trustline_removed | Trustline removed |
| 12 | trustline_updated | Trustline modified |
| 20 | offer_created | DEX offer created |
| 21 | offer_removed | DEX offer removed |
| 22 | offer_updated | DEX offer modified |
| 30 | trade | DEX trade executed |

---

## Stroops and Amounts

All amounts in Stellar are stored as integers in "stroops" (1 stroop = 0.0000001 XLM).

The API returns amounts in **decimal format** for readability:

```json
{
  "balance": "6891567198.9309600"
}
```

To convert to stroops: multiply by 10,000,000

```
6891567198.9309600 × 10,000,000 = 68915671989309600 stroops
```

---

## Timestamps

All timestamps are in RFC3339 format (ISO 8601):

```
"2026-01-10T05:57:58Z"
```

The `Z` suffix indicates UTC. Ledger close times are when the Stellar network finalized that block.

---

## Pagination

All list endpoints support cursor-based pagination:

```bash
# First page
curl -H "Authorization: Api-Key $API_KEY" \
  "https://gateway.withobsrvr.com/lake/v1/testnet/api/v1/silver/transfers?limit=100"

# Response includes cursor
{
  "transfers": [...],
  "cursor": "NDA2MTI2OjIwMjYtMDEtMTBUMDY6MDA6MjNa",
  "has_more": true
}

# Next page
curl -H "Authorization: Api-Key $API_KEY" \
  "https://gateway.withobsrvr.com/lake/v1/testnet/api/v1/silver/transfers?limit=100&cursor=NDA2MTI2OjIwMjYtMDEtMTBUMDY6MDA6MjNa"
```

Continue until `has_more: false`.

**Important Notes:**
- Cursors are opaque strings - don't parse or construct them
- `cursor` and `start_ledger` are mutually exclusive
- Results are ordered by ledger sequence (descending by default)
- No duplicate records across pages

---

## Semantic Layer Tables Reference

The semantic layer sits on top of Silver tables and provides meaning-oriented views for answering human questions directly. These tables are materialized in real-time by the `silver-realtime-transformer`.

### semantic_activities

Unified feed of all on-chain actions with human-readable descriptions.

**Endpoint:** `GET /api/v1/semantic/activities`

| Column | Type | Description |
|--------|------|-------------|
| `id` | string | Unique ID (`{transaction_hash}:{operation_index}`) |
| `ledger_sequence` | int | Block number |
| `timestamp` | timestamp | When the activity occurred |
| `activity_type` | string | `payment`, `contract_call`, `account_created`, `path_payment`, `manage_offer`, `inflation` |
| `description` | string | Human-readable summary (e.g., "Sent 100 XLM to G...") |
| `source_account` | string | Account that initiated the action |
| `destination_account` | string | Target account (for payments) |
| `contract_id` | string | Contract address (for Soroban ops) |
| `asset_code` | string | Asset code (for value transfers) |
| `amount` | string | Transfer amount |
| `is_soroban` | bool | Whether this is a Soroban operation |
| `soroban_function_name` | string | Function called (for contract_call) |
| `transaction_hash` | string | Parent transaction hash |
| `successful` | bool | Whether the transaction succeeded |
| `fee_charged` | int | Fee in stroops |

### semantic_entities_contracts

Contract registry with type classification, usage stats, and observed function signatures.

**Endpoint:** `GET /api/v1/semantic/contracts`

| Column | Type | Description |
|--------|------|-------------|
| `contract_id` | string | Contract address (C...) |
| `contract_type` | string | `sep41_token`, `unknown` (more types coming) |
| `token_name` | string | Token name (if SEP-41) |
| `token_symbol` | string | Token symbol (if SEP-41) |
| `token_decimals` | int | Token decimals (if SEP-41) |
| `deployer_account` | string | Account that deployed the contract |
| `deployed_at` | timestamp | When contract was deployed |
| `total_invocations` | int | Total number of calls to this contract |
| `last_activity` | timestamp | Most recent invocation |
| `unique_callers` | int | Number of distinct callers |
| `observed_functions` | array | Function names observed (e.g., `["transfer", "mint"]`) |

### semantic_flows_value

Normalized value transfers across all asset types.

**Endpoint:** `GET /api/v1/semantic/flows`

| Column | Type | Description |
|--------|------|-------------|
| `id` | string | Unique ID (`{tx_hash}:{flow_index}`) |
| `ledger_sequence` | int | Block number |
| `timestamp` | timestamp | When the transfer occurred |
| `flow_type` | string | `transfer`, `mint`, `burn` |
| `from_account` | string | Sender (null for mints) |
| `to_account` | string | Recipient (null for burns) |
| `contract_id` | string | Contract address (for Soroban transfers) |
| `asset_code` | string | Asset code |
| `asset_issuer` | string | Asset issuer |
| `asset_type` | string | `native`, `credit_alphanum4`, `credit_alphanum12`, `soroban_token` |
| `amount` | string | Transfer amount |
| `transaction_hash` | string | Parent transaction hash |
| `operation_type` | int | Stellar operation type code (1=payment, 2=path_payment, 13=path_payment_strict_send, 24=invoke_host_function) |
| `successful` | bool | Whether the transaction succeeded |

---

## Authentication

All requests require an API key:

```bash
curl -H "Authorization: Api-Key YOUR_API_KEY" \
  "https://gateway.withobsrvr.com/lake/v1/testnet/api/v1/silver/..."
```

---

## Networks

| Network | Base URL |
|---------|----------|
| Testnet | `https://gateway.withobsrvr.com/lake/v1/testnet` |
| Mainnet | Coming soon |
