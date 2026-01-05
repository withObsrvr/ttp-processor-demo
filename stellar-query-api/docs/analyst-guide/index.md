# Obsrvr Lake Analyst Guide

Obsrvr Lake provides analytics-ready access to Stellar and Soroban blockchain data. This guide helps analysts, developers, and data teams get started quickly.

## Who is this for?

- **Analysts** exploring on-chain activity, token flows, and contract interactions
- **Developers** building dashboards, bots, or integrations
- **Data teams** who need historical + real-time blockchain data

## Quick Start: Your First Query in 2 Minutes

Get the current balance of any Stellar account:

```bash
curl "https://obsrvr-lake-testnet.withobsrvr.com/api/v1/silver/accounts/current?account_id=GAIH3ULLFQ4DGSECF2AR555KZ4KNDGEKN4AFI4SU2M7B43MGK3QJZNSR"
```

Get the top 10 most active smart contracts in the last 24 hours:

```bash
curl "https://obsrvr-lake-testnet.withobsrvr.com/api/v1/silver/contracts/top?period=24h&limit=10"
```

Get detailed analytics for a specific contract:

```bash
curl "https://obsrvr-lake-testnet.withobsrvr.com/api/v1/silver/contracts/CAUGJT4GREIY3WHOUUU5RIUDGSPVREF5CDCYJOWMHOVT2GWQT5JEETGJ/analytics"
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
│   │ BRONZE (Raw)               │ SILVER (Analytics-ready)       │ │
│   │ ledgers, transactions,     │ accounts_current,              │ │
│   │ operations, effects,       │ enriched_operations,           │ │
│   │ trades, accounts           │ token_transfers,               │ │
│   │                            │ contract_invocation_calls      │ │
│   └─────────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────────┘
```

The API automatically queries both hot and cold storage, merging results seamlessly. You don't need to think about where data lives.

## What's Next?

- [Data Model](./data-model.md) - Understand Bronze vs Silver layers
- [Common Queries](./common-queries.md) - Recipes for the top 10 use cases
- [nebu for Analysts](./nebu-for-analysts.md) - Build custom data pipelines (power users)
- [Full API Reference](../SILVER_API_REFERENCE.md) - Complete endpoint documentation

## Networks

| Network | Base URL |
|---------|----------|
| Testnet | `https://obsrvr-lake-testnet.withobsrvr.com` |
| Mainnet | Coming soon |

## Getting Help

- API returns helpful error messages with suggested fixes
- All endpoints support `?limit` and cursor-based pagination
- Use `jq` for readable JSON output: `curl ... | jq .`
