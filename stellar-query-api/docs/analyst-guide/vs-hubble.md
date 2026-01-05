# Obsrvr Lake vs Hubble

An honest comparison to help you choose the right tool for your needs.

## Overview

| | Hubble (SDF) | Obsrvr Lake |
|---|---|---|
| **What is it?** | BigQuery dataset with Stellar data | REST API + CLI for Stellar data |
| **Query method** | SQL via BigQuery | REST API or nebu CLI |
| **Data freshness** | ~1 hour delay | Real-time (hot) + historical (cold) |
| **Soroban support** | Limited | Full contract call graphs |

## Feature Comparison

| Feature | Hubble | Obsrvr Lake |
|---------|--------|-------------|
| Raw blockchain data | Yes | Yes (Bronze layer) |
| Pre-aggregated analytics | Some | Yes (Silver layer) |
| Real-time data (< 1 min) | No | Yes |
| Historical data | Full | Full |
| Contract call graphs | No | Yes |
| Contract analytics | Limited | Full (top contracts, callers, callees) |
| SQL query interface | Yes | No (use nebu + DuckDB) |
| REST API | No | Yes |
| CLI tool | No | Yes (nebu) |
| Self-hosted option | No | Yes (flowctl) |
| Custom pipelines | Limited | Yes (nebu processors) |

## Data Model Comparison

### Hubble
- Raw tables matching Horizon's data model
- Some pre-joined views for common queries
- Requires SQL knowledge for most queries

### Obsrvr Lake
- **Bronze layer**: Raw data (similar to Hubble)
- **Silver layer**: Analytics-ready tables with denormalized data
- REST endpoints for common queries (no SQL needed)
- nebu CLI for custom extraction

## When to Use Hubble

Choose Hubble when:

1. **You need raw SQL access** - Complex ad-hoc queries that change frequently
2. **You're in the GCP ecosystem** - Easy to join with other BigQuery datasets
3. **You have BigQuery expertise** - Your team knows SQL and BigQuery
4. **Batch analysis is fine** - 1-hour data delay is acceptable
5. **You need very large aggregations** - BigQuery scales to petabytes

Example Hubble use case:
```sql
-- Custom aggregation across all time
SELECT
    DATE(closed_at) as date,
    COUNT(DISTINCT source_account) as unique_accounts
FROM `crypto-stellar.crypto_stellar.history_operations`
WHERE type = 'payment'
GROUP BY 1
ORDER BY 1 DESC
```

## When to Use Obsrvr Lake

Choose Obsrvr Lake when:

1. **You need real-time data** - Monitoring, dashboards, alerts
2. **You want REST API integration** - Building apps, not writing SQL
3. **You need Soroban contract analytics** - Call graphs, contract relationships
4. **You want to build custom pipelines** - nebu + processors
5. **You prefer self-hosted** - Run your own instance with flowctl

Example Obsrvr Lake use case:
```bash
# Real-time: Get top contracts by activity
curl ".../api/v1/silver/contracts/top?period=24h"

# Contract analytics (not available in Hubble)
curl ".../api/v1/silver/contracts/CAUGJT.../analytics"

# Custom pipeline with nebu
nebu follow --processor token-transfer | your-custom-filter
```

## Pricing

| | Hubble | Obsrvr Lake |
|---|---|---|
| Query cost | BigQuery pricing ($5/TB scanned) | API usage-based (TBD) |
| Storage cost | Free (SDF pays) | Included |
| Minimum cost | $0 (but easy to run up bills) | TBD |

Note: BigQuery charges per TB scanned. A single query scanning historical data can cost $10-100+. Obsrvr Lake pricing is still being finalized.

## Data Freshness

| | Hubble | Obsrvr Lake |
|---|---|---|
| Typical delay | 30-60 minutes | Seconds |
| Worst case delay | Hours (during issues) | Minutes |
| Real-time streaming | No | Yes (nebu follow) |

## Soroban/Smart Contract Support

This is where Obsrvr Lake significantly differs:

| Feature | Hubble | Obsrvr Lake |
|---------|--------|-------------|
| Contract events | Basic | Full |
| Contract-to-contract calls | No | Yes |
| Call graphs | No | Yes |
| Function-level analytics | No | Yes |
| Top contracts ranking | No | Yes |
| Contract caller/callee analysis | No | Yes |

Example queries only possible with Obsrvr Lake:
```bash
# What contracts call this DEX?
curl ".../api/v1/silver/contracts/CDEX.../callers"

# Full call tree for a swap transaction
curl ".../api/v1/silver/tx/abc123/call-graph"

# Most active contracts today
curl ".../api/v1/silver/contracts/top?period=24h"
```

## Can I Use Both?

Yes. Common pattern:

1. Use **Obsrvr Lake** for:
   - Real-time dashboards and monitoring
   - API integrations in your app
   - Soroban contract analytics

2. Use **Hubble** for:
   - Deep historical analysis
   - Complex ad-hoc SQL queries
   - Joining with other GCP datasets

## Migration Path

Moving from Hubble to Obsrvr Lake:

| Hubble Query | Obsrvr Lake Equivalent |
|--------------|------------------------|
| `SELECT * FROM history_accounts WHERE id = 'G...'` | `GET /silver/accounts/current?account_id=G...` |
| `SELECT * FROM history_operations WHERE source_account = 'G...'` | `GET /silver/operations/enriched?account_id=G...` |
| Custom SQL aggregation | `nebu extract` + DuckDB |

For complex SQL queries, use nebu to extract data, then analyze with DuckDB locally:

```bash
# Extract data with nebu
nebu extract --processor token-transfer --start X --end Y > data.json

# Analyze with DuckDB (SQL)
duckdb -c "SELECT ... FROM 'data.json'"
```

## Summary

**Choose Hubble if:** You're a SQL expert who needs ad-hoc historical analysis and doesn't need real-time data.

**Choose Obsrvr Lake if:** You're building applications, need real-time data, or need Soroban contract analytics.

**Use both if:** You need real-time for your app (Lake) plus deep historical analysis (Hubble).
