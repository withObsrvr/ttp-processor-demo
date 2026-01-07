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
│ BRONZE LAYER (19 tables)                                         │
│                                                                  │
│ Event Stream Tables (append-only, ordered by ledger):            │
│   ledgers_row_v2           - Block metadata and protocol info    │
│   transactions_row_v2      - Transaction envelopes and results   │
│   operations_row_v2        - Individual operations               │
│   effects_row_v1           - Side effects of operations          │
│   trades_row_v1            - DEX trades                          │
│   contract_events_stream_v1 - Soroban contract events            │
│   evicted_keys_state_v1    - Expired contract data keys          │
│   restored_keys_state_v1   - Restored contract data keys         │
│                                                                  │
│ Snapshot Tables (current state, deduplicated by key):            │
│   accounts_snapshot_v1     - Account state snapshots             │
│   trustlines_snapshot_v1   - Asset trustline snapshots           │
│   account_signers_snapshot_v1 - Account signer snapshots         │
│   native_balances_snapshot_v1 - XLM balance snapshots            │
│   offers_snapshot_v1       - DEX offer snapshots                 │
│   liquidity_pools_snapshot_v1 - AMM pool snapshots               │
│   claimable_balances_snapshot_v1 - Claimable balance snapshots   │
│   contract_data_snapshot_v1 - Soroban contract data              │
│   contract_code_snapshot_v1 - Soroban WASM code metadata         │
│   ttl_snapshot_v1          - Contract data TTL snapshots         │
│   config_settings_snapshot_v1 - Network config parameters        │
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
│ SILVER LAYER (18 tables)                                         │
│                                                                  │
│ Core Tables:                                                     │
│   accounts_current           - Latest state for each account    │
│   enriched_history_operations - Operations with decoded types   │
│   enriched_history_operations_soroban - Soroban ops only        │
│   token_transfers_raw        - Unified payment/path payment     │
│   contract_invocations_raw   - Smart contract call graph        │
│                                                                  │
│ State Tables (Current Values):                                   │
│   trustlines_current         - Current trustline states         │
│   offers_current             - Current DEX order book offers    │
│   claimable_balances_current - Current claimable balances       │
│   liquidity_pools_current    - Current AMM pool states          │
│   native_balances_current    - Current XLM balances             │
│                                                                  │
│ Event Tables (Append-Only):                                      │
│   trades                     - DEX trade history                 │
│   effects                    - Operation side effects            │
│                                                                  │
│ Soroban Tables:                                                  │
│   contract_data_current      - Current contract state data       │
│   contract_code_current      - Deployed contract WASM code       │
│   ttl_current                - Contract data TTL/expiration      │
│   evicted_keys               - Expired contract data entries     │
│   restored_keys              - Restored contract data entries    │
│   config_settings_current    - Network configuration params      │
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
curl -H "Authorization: Api-Key $API_KEY" \
  "https://gateway.withobsrvr.com/lake/v1/testnet/api/v1/silver/accounts/current?account_id=G..."

# Get account signers and thresholds
curl -H "Authorization: Api-Key $API_KEY" \
  "https://gateway.withobsrvr.com/lake/v1/testnet/api/v1/silver/accounts/signers?account_id=G..."
```

### enriched_history_operations

Operations with human-readable fields and joined metadata.

| Column | Description |
|--------|-------------|
| `transaction_hash` | Parent transaction hash |
| `operation_index` | Index within transaction |
| `ledger_sequence` | Block number |
| `source_account` | Account that submitted the operation |
| `type` | Operation type code |
| `type_string` | Human-readable operation type (payment, create_account, etc.) |
| `created_at` | Timestamp |
| `transaction_successful` | Whether the transaction succeeded |
| `is_payment_op` | True for payment operations |
| `is_soroban_op` | True for Soroban contract operations |

```bash
# Get operations for an account
curl -H "Authorization: Api-Key $API_KEY" \
  "https://gateway.withobsrvr.com/lake/v1/testnet/api/v1/silver/operations/enriched?account_id=G..."
```

### enriched_history_operations_soroban

Filtered view containing only Soroban smart contract operations. Same schema as `enriched_history_operations` but pre-filtered for contract invocations.

| Column | Description |
|--------|-------------|
| `contract_id` | Target contract address (C...) |
| `function_name` | Function being called |
| `host_function_type` | Type of host function invocation |
| `parameters` | Function parameters (JSON) |

```bash
# Get Soroban operations for an account
curl -H "Authorization: Api-Key $API_KEY" \
  "https://gateway.withobsrvr.com/lake/v1/testnet/api/v1/silver/operations/soroban?account_id=G..."
```

### token_transfers_raw

Unified view of all token movements (payments, path payments, claimable balance claims).

| Column | Description |
|--------|-------------|
| `timestamp` | Transfer timestamp |
| `transaction_hash` | Transaction containing the transfer |
| `ledger_sequence` | Block number |
| `source_type` | Transfer type: `classic` or `soroban` |
| `from_account` | Sender |
| `to_account` | Recipient |
| `asset_code` | Asset code (XLM, USDC, etc.) |
| `asset_issuer` | Asset issuer (null for XLM) |
| `amount` | Amount transferred in stroops |
| `token_contract_id` | Contract ID for Soroban tokens |

```bash
# Get USDC transfers for an account
curl -H "Authorization: Api-Key $API_KEY" \
  "https://gateway.withobsrvr.com/lake/v1/testnet/api/v1/silver/transfers?account_id=G...&asset_code=USDC"
```

### contract_invocations_raw

Soroban contract invocations with TOID support for precise ordering.

| Column | Description |
|--------|-------------|
| `ledger_sequence` | Block number |
| `transaction_index` | Index within ledger |
| `operation_index` | Index within transaction |
| `transaction_hash` | Transaction hash |
| `source_account` | Account that submitted the transaction |
| `contract_id` | Target contract address (C...) |
| `function_name` | Function being called |
| `arguments_json` | Function arguments (JSON) |
| `successful` | Whether the call succeeded |
| `closed_at` | Timestamp |

```bash
# Get call graph for a transaction
curl -H "Authorization: Api-Key $API_KEY" \
  "https://gateway.withobsrvr.com/lake/v1/testnet/api/v1/silver/tx/{hash}/call-graph"
```

### liquidity_pools_current

Current state of AMM (Automated Market Maker) liquidity pools.

| Column | Description |
|--------|-------------|
| `liquidity_pool_id` | Unique pool identifier (hash) |
| `type` | Pool type (constant_product) |
| `fee` | Fee in basis points (30 = 0.3%) |
| `trustline_count` | Number of accounts holding pool shares |
| `total_pool_shares` | Total pool shares outstanding |
| `asset_a_type` | First asset type (native, credit_alphanum4, etc.) |
| `asset_a_code` | First asset code |
| `asset_a_issuer` | First asset issuer |
| `asset_a_amount` | Amount of first asset in pool |
| `asset_b_type` | Second asset type |
| `asset_b_code` | Second asset code |
| `asset_b_issuer` | Second asset issuer |
| `asset_b_amount` | Amount of second asset in pool |
| `last_modified_ledger` | When pool was last changed |

### native_balances_current

Current XLM balances for all accounts. More efficient than accounts_current when you only need balances.

| Column | Description |
|--------|-------------|
| `account_id` | Stellar public key (G...) |
| `balance` | XLM balance in stroops |
| `last_modified_ledger` | When balance was last changed |

### trades

DEX trade history (append-only event stream).

| Column | Description |
|--------|-------------|
| `history_operation_id` | Operation that created this trade |
| `order` | Order within the operation |
| `ledger_closed_at` | Timestamp of trade |
| `selling_account_id` | Account that sold |
| `selling_asset_type` | Sold asset type |
| `selling_asset_code` | Sold asset code |
| `selling_asset_issuer` | Sold asset issuer |
| `selling_amount` | Amount sold |
| `buying_account_id` | Account that bought |
| `buying_asset_type` | Bought asset type |
| `buying_asset_code` | Bought asset code |
| `buying_asset_issuer` | Bought asset issuer |
| `buying_amount` | Amount bought |
| `price_n` | Price numerator |
| `price_d` | Price denominator |
| `type` | Trade type (orderbook, liquidity_pool) |
| `liquidity_pool_id` | Pool ID if AMM trade (null for orderbook) |

### effects

Operation side effects (append-only event stream). Effects provide granular details about what changed.

| Column | Description |
|--------|-------------|
| `history_operation_id` | Parent operation |
| `order` | Order within operation |
| `type` | Effect type (account_created, trustline_created, etc.) |
| `type_string` | Human-readable effect type |
| `details` | JSON with effect-specific data |
| `ledger_sequence` | Block number |
| `closed_at` | Timestamp |

### contract_code_current

Deployed Soroban smart contract WASM code.

| Column | Description |
|--------|-------------|
| `contract_code_hash` | Hash of the WASM code |
| `contract_code_ext_v` | Extension version |
| `last_modified_ledger` | When code was deployed/updated |
| `ledger_entry_change` | Change type (created, updated) |

### ttl_current

Time-to-live (TTL) for Soroban contract data entries. Tracks when data expires.

| Column | Description |
|--------|-------------|
| `key_hash` | Hash of the contract data key |
| `live_until_ledger_seq` | Ledger when entry expires |
| `last_modified_ledger` | When TTL was last updated |
| `ledger_entry_change` | Change type |

### evicted_keys

Contract data entries that have expired and been evicted from the ledger.

| Column | Description |
|--------|-------------|
| `contract_id` | Contract that owned the data |
| `key_hash` | Hash of the evicted key |
| `ledger_sequence` | Ledger when eviction occurred |
| `closed_at` | Timestamp of eviction |

### restored_keys

Contract data entries that have been restored after eviction.

| Column | Description |
|--------|-------------|
| `contract_id` | Contract that owns the data |
| `key_hash` | Hash of the restored key |
| `ledger_sequence` | Ledger when restoration occurred |
| `closed_at` | Timestamp of restoration |

### config_settings_current

Soroban network configuration parameters. These control limits and fees for smart contracts.

| Column | Description |
|--------|-------------|
| `config_setting_id` | Configuration parameter ID |
| `ledger_max_instructions` | Max instructions per ledger |
| `tx_max_instructions` | Max instructions per transaction |
| `fee_rate_per_instructions_increment` | Fee rate for instructions |
| `tx_memory_limit` | Max memory per transaction |
| `ledger_max_read_ledger_entries` | Max ledger entries read per ledger |
| `ledger_max_read_bytes` | Max bytes read per ledger |
| `ledger_max_write_ledger_entries` | Max ledger entries written per ledger |
| `ledger_max_write_bytes` | Max bytes written per ledger |
| `tx_max_read_ledger_entries` | Max ledger entries read per transaction |
| `tx_max_read_bytes` | Max bytes read per transaction |
| `tx_max_write_ledger_entries` | Max ledger entries written per transaction |
| `tx_max_write_bytes` | Max bytes written per transaction |
| `contract_max_size_bytes` | Max WASM contract size |
| `config_setting_xdr` | Raw XDR for additional settings |
| `last_modified_ledger` | When setting was last changed |

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
curl -H "Authorization: Api-Key $API_KEY" \
  "https://gateway.withobsrvr.com/lake/v1/testnet/api/v1/silver/transfers?limit=100"

# Response includes cursor
{
  "transfers": [...],
  "cursor": "MjEzNzkxODo1",
  "has_more": true
}

# Next page
curl -H "Authorization: Api-Key $API_KEY" \
  "https://gateway.withobsrvr.com/lake/v1/testnet/api/v1/silver/transfers?limit=100&cursor=MjEzNzkxODo1"
```

Continue until `has_more: false`.
