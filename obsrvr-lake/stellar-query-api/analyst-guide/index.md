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
