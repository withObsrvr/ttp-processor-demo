# Obsrvr Lake Analyst Guide

Obsrvr Lake provides analytics-ready access to Stellar and Soroban blockchain data. This guide helps analysts, developers, and data teams get started quickly.

## New to Obsrvr Lake?

| Resource | Time | Description |
|----------|------|-------------|
| [Getting Started](./getting-started.md) | 1 hour | Structured onboarding with hands-on examples |
| [First 10 Queries](./first-10-queries.md) | 15 min | Copy-paste ready queries for common tasks |
| [Horizon Migration Guide](./horizon-migration.md) | 30 min | Translate Horizon API calls to Query API |

## Who is this for?

- **Analysts** exploring on-chain activity, token flows, and contract interactions
- **Developers** building dashboards, bots, or integrations
- **Data teams** who need historical + real-time blockchain data

## Quick Start: Your First Query in 2 Minutes

Get the current balance of any Stellar account:

```bash
curl -H "Authorization: Api-Key YOUR_API_KEY" \
  "https://gateway.withobsrvr.com/lake/v1/testnet/api/v1/silver/accounts/current?account_id=GAIH3ULLFQ4DGSECF2AR555KZ4KNDGEKN4AFI4SU2M7B43MGK3QJZNSR"
```

Get the top 10 most active smart contracts in the last 24 hours:

```bash
curl -H "Authorization: Api-Key YOUR_API_KEY" \
  "https://gateway.withobsrvr.com/lake/v1/testnet/api/v1/silver/contracts/top?period=24h&limit=10"
```

Get detailed analytics for a specific contract:

```bash
curl -H "Authorization: Api-Key YOUR_API_KEY" \
  "https://gateway.withobsrvr.com/lake/v1/testnet/api/v1/silver/contracts/CAUGJT4GREIY3WHOUUU5RIUDGSPVREF5CDCYJOWMHOVT2GWQT5JEETGJ/analytics"
```

Get all CAP-67 events (transfers, mints, burns) for a contract:

```bash
curl -H "Authorization: Api-Key YOUR_API_KEY" \
  "https://gateway.withobsrvr.com/lake/v1/testnet/api/v1/silver/events/by-contract?contract_id=CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG6QB3RVFT5RMCDH74N2&limit=10"
```

Get top holders for a SEP-41 token:

```bash
curl -H "Authorization: Api-Key YOUR_API_KEY" \
  "https://gateway.withobsrvr.com/lake/v1/testnet/api/v1/silver/tokens/CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG6QB3RVFT5RMCDH74N2/balances?limit=10"
```

Get a human-readable summary of a transaction:

```bash
curl -H "Authorization: Api-Key YOUR_API_KEY" \
  "https://gateway.withobsrvr.com/lake/v1/testnet/api/v1/silver/tx/YOUR_TX_HASH/decoded"
```

Search across accounts, contracts, transactions, ledgers, and assets with one query:

```bash
curl -H "Authorization: Api-Key YOUR_API_KEY" \
  "https://gateway.withobsrvr.com/lake/v1/testnet/api/v1/silver/search?q=USDC"
```

Get available DEX trading pairs and latest prices:

```bash
curl -H "Authorization: Api-Key YOUR_API_KEY" \
  "https://gateway.withobsrvr.com/lake/v1/testnet/api/v1/silver/prices/pairs"
```

Get raw contract events (all event types including diagnostic):

```bash
curl -H "Authorization: Api-Key YOUR_API_KEY" \
  "https://gateway.withobsrvr.com/lake/v1/testnet/api/v1/silver/events/generic?limit=10"
```

Get a compliance archive with all XLM holders at a specific point in time:

```bash
curl -H "Authorization: Api-Key YOUR_API_KEY" \
  "https://gateway.withobsrvr.com/lake/v1/testnet/api/v1/gold/compliance/balances?asset_code=XLM&timestamp=2026-01-12T23:59:59Z&limit=100"
```

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────┐
│                         YOUR APPLICATION                            │
└─────────────────────────────────────────────────────────────────────┘
                                │
                                │ REST API
                                ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    OBSRVR LAKE QUERY API                            │
│                                                                     │
│   ┌─────────────────────────┐   ┌─────────────────────────┐       │
│   │   HOT (PostgreSQL)      │   │   COLD (DuckLake)       │       │
│   │   Last 7 days           │   │   Full history          │       │
│   │   Sub-second queries    │   │   Analytics-optimized   │       │
│   └─────────────────────────┘   └─────────────────────────┘       │
│                                                                     │
│   ┌─────────────────────────────────────────────────────────────┐ │
│   │ BRONZE (Raw)          │ SILVER (Analytics)  │ GOLD (Audit)  │ │
│   │ ledgers, transactions │ accounts, transfers │ snapshots,    │ │
│   │ operations, effects   │ contracts, offers   │ compliance    │ │
│   │ trades, accounts      │ liquidity_pools     │ archives,     │ │
│   │ trustlines, offers    │ soroban tables      │ checksums     │ │
│   │ contract_events       │ analytics...        │ methodology   │ │
│   └─────────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────────┘
```

The API automatically queries both hot and cold storage, merging results seamlessly. You don't need to think about where data lives.

## What's Next?

**Start Here:**
- [Getting Started](./getting-started.md) - One-hour structured onboarding
- [First 10 Queries](./first-10-queries.md) - Essential queries for common tasks

**Deep Dives:**
- [Data Model](./data-model.md) - Complete schema reference for all Bronze and Silver tables
- [API Reference](./common-queries.md) - Complete endpoint documentation with examples
- [Horizon Migration](./horizon-migration.md) - Side-by-side Horizon vs Query API mappings
- [nebu for Analysts](./nebu-for-analysts.md) - Build custom data pipelines (power users)
- [Hubble Comparison](./vs-hubble.md) - How Obsrvr Lake compares to Stellar Hubble

**Soroban & Smart Contracts:**
- [Contract Metadata](./common-queries.md#get-contract-metadata) - Creator, WASM info, storage summary, observed functions
- [Contract Storage](./common-queries.md#get-contract-storage) - Contract data entries with TTL information
- [Soroban Stats](./common-queries.md#get-soroban-network-statistics) - Network-wide Soroban runtime statistics
- [CAP-67 Unified Events](./common-queries.md#cap-67-unified-event-stream) - Unified event stream (transfers, mints, burns)
- [Generic Contract Events](./common-queries.md#generic-contract-events-bronze) - Raw Soroban events (contract, system, diagnostic)
- [SEP-41 Token API](./common-queries.md#sep-41-token-api) - Token metadata, balances, transfers, and portfolios
- [Smart Wallet Detection](./common-queries.md#smart-wallet-detection-sep-50) - Detect SEP-50 smart wallets
- [Transaction Decoding](./common-queries.md#transaction-decoding) - Human-readable transaction summaries
- [Transaction Diffs](./common-queries.md#get-transaction-balance--state-diffs) - Balance and state changes per transaction
- [Soroban Function Filter](./common-queries.md#get-soroban-operations-by-function) - Filter operations by contract and function

**Fees & Statistics:**
- [Fee Statistics](./common-queries.md#get-fee-statistics) - Fee percentiles, surge detection, aggregate data
- [Ledger Fee Distribution](./common-queries.md#get-ledger-fee-distribution) - Per-ledger fee histogram
- [Transaction Summaries](./common-queries.md#get-transaction-summaries-batch) - Batch transaction lookup

**Market Data & Search:**
- [Unified Search](./common-queries.md#unified-search) - Search across all data types with one query
- [Trading Pairs](./common-queries.md#list-available-trading-pairs) - Available DEX markets
- [Latest Prices](./common-queries.md#get-latest-price) - Current trade prices with 24h volume
- [OHLC Candles](./common-queries.md#get-ohlc-candles) - Price charts and candlestick data

**Compliance & Auditing:**
- [Gold Layer Endpoints](./common-queries.md#gold-layer-endpoints) - Point-in-time snapshots and compliance archives
- [Compliance Archives](./common-queries.md#gold-compliance-archive-endpoints) - Audit-ready exports with checksums

## Networks

| Network | Base URL |
|---------|----------|
| Testnet | `https://gateway.withobsrvr.com/lake/v1/testnet` |
| Mainnet | Coming soon |

All requests require an API key header: `Authorization: Api-Key YOUR_API_KEY`

## Getting Help

- API returns helpful error messages with suggested fixes
- All endpoints support `?limit` and cursor-based pagination
- Use `jq` for readable JSON output: `curl ... | jq .`
