# Getting Started: One-Hour Analyst Onboarding

**Goal:** By the end of this hour, you'll be able to confidently answer real questions about Stellar and Soroban activity using Obsrvr Lake.

---

## Minute 0-5: Orientation

### What is Obsrvr Lake?

Obsrvr Lake provides **analytics-ready blockchain data** for the Stellar and Soroban ecosystem:

- **Real-time freshness** - Data is seconds old, not hours
- **Full historical depth** - Query back to genesis
- **Simple REST APIs** - No SQL required for common questions
- **Optional power tools** - nebu CLI and DuckDB for custom analysis

### Mental Model

```
┌─────────────────────────────────────────────────────┐
│                  YOUR APPLICATION                    │
└─────────────────────────────────────────────────────┘
                         │
                    REST API
                         ▼
┌─────────────────────────────────────────────────────┐
│               OBSRVR LAKE QUERY API                  │
│                                                      │
│   Silver Layer (use this)     Bronze Layer (raw)    │
│   - Pre-processed             - Blockchain truth    │
│   - Fast answers              - Custom derivations  │
│   - No SQL needed             - Full detail         │
└─────────────────────────────────────────────────────┘
```

**Rule of thumb:**
- Asking a **question**? Use **Silver**.
- Building a **custom dataset**? Start from **Bronze**.

You'll use Silver 90% of the time.

---

## Minute 5-20: Your First 3 Queries (The "Aha" Moment)

Let's run three queries that demonstrate what Obsrvr Lake can do.

### Prerequisites

You need:
1. An API key (contact Obsrvr team)
2. curl or any HTTP client
3. A terminal

Set up your environment:
```bash
export API_KEY="your-api-key-here"
```

### Query 1: Get Current Account State

**Question:** What's the balance of the Stellar Foundation account?

```bash
curl -H "Authorization: Api-Key $API_KEY" \
  "https://gateway.withobsrvr.com/lake/v1/testnet/api/v1/silver/accounts/current?account_id=GAIH3ULLFQ4DGSECF2AR555KZ4KNDGEKN4AFI4SU2M7B43MGK3QJZNSR"
```

**What you get:**
- Current XLM balance
- Sequence number
- Last modified ledger
- Account flags and settings

This replaces 80% of Horizon "account lookup" usage with sub-second response times.

---

### Query 2: Find Most Active Smart Contracts

**Question:** Which Soroban contracts have the most activity in the last 24 hours?

```bash
curl -H "Authorization: Api-Key $API_KEY" \
  "https://gateway.withobsrvr.com/lake/v1/testnet/api/v1/silver/contracts/top?period=24h&limit=10"
```

**What you get:**
- Top contracts ranked by call count
- Unique caller counts
- Most popular functions
- Last activity timestamps

**This query is not possible in Hubble without complex SQL.** You'd need to write aggregations across multiple tables. Here, it's one API call.

---

### Query 3: Analyze a Specific Contract

**Question:** Who's calling this contract and what functions are they using?

```bash
curl -H "Authorization: Api-Key $API_KEY" \
  "https://gateway.withobsrvr.com/lake/v1/testnet/api/v1/silver/contracts/CAUGJT4GREIY3WHOUUU5RIUDGSPVREF5CDCYJOWMHOVT2GWQT5JEETGJ/analytics"
```

**What you get:**
- Call statistics (as caller and callee)
- Unique callers and callees
- Top functions by usage
- Daily call counts for the last 7 days
- First seen and last activity timestamps

This is your gateway to Soroban analytics.

---

## Minute 20-35: Understanding the Data Model

### Bronze vs Silver: When to Use Each

| Layer | Tables | Use When You Need |
|-------|--------|-------------------|
| **Silver** | 18 analytics tables | Fast answers, dashboards, common questions |
| **Bronze** | 19 raw tables | Custom derivations, full blockchain detail |

**Silver tables** are pre-processed:
- `accounts_current` - Latest state per account
- `token_transfers_raw` - Unified payments
- `trades` - DEX trade history
- `contract_invocations_raw` - Soroban call graph
- And 14 more...

**Bronze tables** mirror the blockchain:
- `ledgers_row_v2` - Block data
- `transactions_row_v2` - Transaction envelopes
- `operations_row_v2` - Individual operations
- And 16 more...

### Hot vs Cold: You Don't Manage This

| Storage | Data Age | Speed | Technology |
|---------|----------|-------|------------|
| **Hot** | Last ~7 days | Sub-second | PostgreSQL |
| **Cold** | Full history | 1-10 seconds | DuckLake (Parquet) |

**The API merges them automatically.** You query once, results come from both, seamlessly deduplicated. No configuration needed.

### Key Concepts

**Stroops:** All amounts are in stroops (1 XLM = 10,000,000 stroops). Divide by 10^7 for XLM.

**Cursors:** Pagination uses opaque cursor strings. Pass `cursor` from the response to get the next page.

**Assets:** Specified as `CODE:ISSUER` (e.g., `USDC:GA5Z...`) or `XLM` for native.

---

## Minute 35-50: Common Patterns

### Pattern 1: Pagination

All list endpoints support cursor-based pagination:

```bash
# First page
curl -H "Authorization: Api-Key $API_KEY" \
  "https://gateway.withobsrvr.com/lake/v1/testnet/api/v1/silver/transfers?limit=100"

# Response includes cursor
# {
#   "transfers": [...],
#   "cursor": "MjEzNzkxODo1",
#   "has_more": true
# }

# Next page
curl -H "Authorization: Api-Key $API_KEY" \
  "https://gateway.withobsrvr.com/lake/v1/testnet/api/v1/silver/transfers?limit=100&cursor=MjEzNzkxODo1"
```

Continue until `has_more: false`.

### Pattern 2: Filtering

Most endpoints accept filter parameters:

```bash
# Filter transfers by account
/silver/transfers?from_account=G...

# Filter transfers by asset
/silver/transfers?asset_code=USDC

# Combine filters
/silver/transfers?from_account=G...&asset_code=USDC&limit=50
```

### Pattern 3: Time Windows

Contract and stats endpoints support time periods:

```bash
# Last 24 hours (default)
/silver/contracts/top?period=24h

# Last 7 days
/silver/contracts/top?period=7d

# Last 30 days
/silver/contracts/top?period=30d
```

### Pattern 4: Asset Formatting

Assets are specified as `CODE:ISSUER`:

```bash
# Native XLM
/silver/assets/XLM/holders

# USDC with issuer
/silver/assets/USDC:GA5ZSEJYB37JRC5AVCIA5MOP4RHTM335X2KGX3IHOJAPP5RE34K4KZVN/holders
```

### Pattern 5: Joining in Your BI Tool

Obsrvr Lake returns JSON. Export to your preferred tool:

```bash
# Save to file
curl -H "Authorization: Api-Key $API_KEY" \
  "https://gateway.withobsrvr.com/lake/v1/testnet/api/v1/silver/accounts/top?limit=1000" \
  > top_accounts.json

# Convert to CSV with jq
cat top_accounts.json | jq -r '.accounts[] | [.rank, .account_id, .balance] | @csv' > top_accounts.csv
```

---

## Minute 50-60: Choose Your Path

You're now productive with Obsrvr Lake. Choose what to learn next based on your goals:

### Path A: Dashboard Builder
- Focus on: `/silver/stats/network`, `/silver/contracts/top`, `/silver/assets/{asset}/stats`
- Tools: Grafana, Tableau, or any BI tool that consumes JSON
- Next read: [API Reference](./common-queries.md) for all available stats endpoints

### Path B: Protocol Analyst
- Focus on: Contract analytics, call graphs, token flows
- Queries: `/silver/contracts/{id}/analytics`, `/silver/tx/{hash}/call-graph`
- Next read: [API Reference](./common-queries.md) - Smart Contract section

### Path C: DeFi Researcher
- Focus on: DEX trades, liquidity pools, offers
- Queries: `/silver/trades`, `/silver/liquidity-pools`, `/silver/offers`
- Next read: [API Reference](./common-queries.md) - DEX and Liquidity Pool sections

### Path D: Power User
- Focus on: Custom analysis with raw data
- Tools: nebu CLI, DuckDB, Python/Pandas
- Next read: [nebu for Analysts](./nebu-for-analysts.md)

---

## Quick Reference Card

### Base URL
```
https://gateway.withobsrvr.com/lake/v1/testnet
```

### Authentication
```bash
curl -H "Authorization: Api-Key YOUR_API_KEY" "..."
```

### Most Used Endpoints

| What You Want | Endpoint |
|---------------|----------|
| Account balance | `/silver/accounts/current?account_id=G...` |
| All balances | `/silver/accounts/{id}/balances` |
| Top accounts | `/silver/accounts/top` |
| Token transfers | `/silver/transfers` |
| Asset holders | `/silver/assets/{asset}/holders` |
| Top contracts | `/silver/contracts/top` |
| Contract details | `/silver/contracts/{id}/analytics` |
| DEX trades | `/silver/trades` |
| Liquidity pools | `/silver/liquidity-pools` |

### Response Pattern
```json
{
  "data": [...],
  "count": 10,
  "cursor": "abc123...",
  "has_more": true
}
```

---

## What's Next?

- [First 10 Queries](./first-10-queries.md) - Copy-paste ready examples
- [Data Model](./data-model.md) - Deep dive into tables and columns
- [API Reference](./common-queries.md) - Complete endpoint documentation
- [nebu CLI](./nebu-for-analysts.md) - Power user tools
- [vs Hubble](./vs-hubble.md) - When to use which platform

---

## Getting Help

- API returns helpful error messages with suggested fixes
- All endpoints support `?limit` and cursor-based pagination
- Use `jq` for readable JSON output: `curl ... | jq .`

You're ready to start analyzing Stellar and Soroban data.
