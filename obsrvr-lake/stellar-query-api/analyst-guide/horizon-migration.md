# Horizon to Query API Migration Guide

> **For developers migrating from Stellar Horizon to Obsrvr Gateway Query API**

This guide helps you translate your existing Horizon API calls to equivalent Query API endpoints. The Query API provides ~75% coverage of Horizon's read-only functionality, plus additional analytics and Soroban-specific endpoints.

## Quick Reference

| What You Need | Horizon | Query API | Coverage |
|---------------|---------|-----------|----------|
| Account info | `/accounts/{id}` | `/silver/accounts/{id}` | Full |
| Account transactions | `/accounts/{id}/transactions` | `/silver/accounts/{id}/transactions` | Full |
| Account operations | `/accounts/{id}/operations` | `/silver/accounts/{id}/operations` | Full |
| Account effects | `/accounts/{id}/effects` | `/silver/accounts/{id}/effects` | Full |
| Account offers | `/accounts/{id}/offers` | `/silver/accounts/{id}/offers` | Full |
| Account trades | `/accounts/{id}/trades` | `/silver/accounts/{id}/trades` | Full |
| Assets | `/assets` | `/silver/assets` | Full |
| Ledgers | `/ledgers` | `/silver/ledgers` | Full |
| Transactions | `/transactions` | `/silver/transactions` | Full |
| Operations | `/operations` | `/silver/operations/enriched` | Full |
| Effects | `/effects` | `/silver/effects` | Full |
| Trades | `/trades` | `/silver/trades` | Full |
| Offers | `/offers` | `/silver/offers` | Full |
| Liquidity Pools | `/liquidity_pools` | `/silver/liquidity-pools` | Partial* |
| Claimable Balances | `/claimable_balances` | `/silver/claimable-balances` | Partial* |
| Order Book | `/order_book` | Not available | Planned |
| Fee Stats | `/fee_stats` | Not available | Planned |
| Path Finding | `/paths/*` | Not available | N/A** |
| Submit Transaction | `POST /transactions` | Not available | N/A** |

*Sub-resource endpoints (trades, effects, operations) not yet available
**Out of scope - Query API is read-only

---

## Base URLs

**Horizon (Public)**
```
https://horizon.stellar.org          # Mainnet
https://horizon-testnet.stellar.org  # Testnet
```

**Obsrvr Gateway Query API**
```
https://gateway.withobsrvr.com/lake/v1/mainnet  # Mainnet
https://gateway.withobsrvr.com/lake/v1/testnet  # Testnet
```

**Authentication**

Horizon requires no authentication. Query API requires an API key:

```bash
# Horizon
curl "https://horizon.stellar.org/accounts/GABC..."

# Query API
curl -H "Authorization: Api-Key YOUR_API_KEY" \
  "https://gateway.withobsrvr.com/lake/v1/testnet/api/v1/silver/accounts/GABC..."
```

---

## Accounts

### Get Account Details

**Horizon**
```bash
curl "https://horizon.stellar.org/accounts/GABC..."
```

**Query API**
```bash
curl -H "Authorization: Api-Key $API_KEY" \
  "$GATEWAY/api/v1/silver/accounts/GABC..."
```

### Get Account Transactions

**Horizon**
```bash
curl "https://horizon.stellar.org/accounts/GABC.../transactions?limit=10&order=desc"
```

**Query API**
```bash
curl -H "Authorization: Api-Key $API_KEY" \
  "$GATEWAY/api/v1/silver/accounts/GABC.../transactions?limit=10"
```

### Get Account Operations

**Horizon**
```bash
curl "https://horizon.stellar.org/accounts/GABC.../operations?limit=20"
```

**Query API**
```bash
curl -H "Authorization: Api-Key $API_KEY" \
  "$GATEWAY/api/v1/silver/accounts/GABC.../operations?limit=20"
```

### Get Account Effects

**Horizon**
```bash
curl "https://horizon.stellar.org/accounts/GABC.../effects?limit=20"
```

**Query API**
```bash
curl -H "Authorization: Api-Key $API_KEY" \
  "$GATEWAY/api/v1/silver/accounts/GABC.../effects?limit=20"
```

### Get Account Offers

**Horizon**
```bash
curl "https://horizon.stellar.org/accounts/GABC.../offers"
```

**Query API**
```bash
curl -H "Authorization: Api-Key $API_KEY" \
  "$GATEWAY/api/v1/silver/accounts/GABC.../offers"
```

### Get Account Trades

**Horizon**
```bash
curl "https://horizon.stellar.org/accounts/GABC.../trades?limit=20"
```

**Query API**
```bash
curl -H "Authorization: Api-Key $API_KEY" \
  "$GATEWAY/api/v1/silver/accounts/GABC.../trades?limit=20"
```

### Get Account Balances (Trustlines)

**Horizon** (embedded in account response)
```bash
curl "https://horizon.stellar.org/accounts/GABC..." | jq '.balances'
```

**Query API** (dedicated endpoint)
```bash
curl -H "Authorization: Api-Key $API_KEY" \
  "$GATEWAY/api/v1/silver/accounts/GABC.../trustlines"
```

---

## Assets

### List Assets

**Horizon**
```bash
curl "https://horizon.stellar.org/assets?asset_code=USDC&limit=10"
```

**Query API**
```bash
curl -H "Authorization: Api-Key $API_KEY" \
  "$GATEWAY/api/v1/silver/assets?asset_code=USDC&limit=10"
```

### Get Asset by Code and Issuer

**Horizon**
```bash
curl "https://horizon.stellar.org/assets?asset_code=USDC&asset_issuer=GA5ZSE..."
```

**Query API**
```bash
curl -H "Authorization: Api-Key $API_KEY" \
  "$GATEWAY/api/v1/silver/assets/USDC:GA5ZSE..."
```

---

## Ledgers

### List Ledgers

**Horizon**
```bash
curl "https://horizon.stellar.org/ledgers?limit=10&order=desc"
```

**Query API**
```bash
curl -H "Authorization: Api-Key $API_KEY" \
  "$GATEWAY/api/v1/silver/ledgers?limit=10"
```

### Get Ledger by Sequence

**Horizon**
```bash
curl "https://horizon.stellar.org/ledgers/54930000"
```

**Query API**
```bash
curl -H "Authorization: Api-Key $API_KEY" \
  "$GATEWAY/api/v1/silver/ledgers/54930000"
```

### Get Ledger Transactions

**Horizon**
```bash
curl "https://horizon.stellar.org/ledgers/54930000/transactions"
```

**Query API**
```bash
curl -H "Authorization: Api-Key $API_KEY" \
  "$GATEWAY/api/v1/silver/ledgers/54930000/transactions"
```

### Get Ledger Operations

**Horizon**
```bash
curl "https://horizon.stellar.org/ledgers/54930000/operations"
```

**Query API**
```bash
curl -H "Authorization: Api-Key $API_KEY" \
  "$GATEWAY/api/v1/silver/ledgers/54930000/operations"
```

---

## Transactions

### List Transactions

**Horizon**
```bash
curl "https://horizon.stellar.org/transactions?limit=20&order=desc"
```

**Query API**
```bash
curl -H "Authorization: Api-Key $API_KEY" \
  "$GATEWAY/api/v1/silver/transactions?limit=20"
```

### Get Transaction by Hash

**Horizon**
```bash
curl "https://horizon.stellar.org/transactions/abc123..."
```

**Query API**
```bash
curl -H "Authorization: Api-Key $API_KEY" \
  "$GATEWAY/api/v1/silver/transactions/abc123..."
```

### Get Transaction Operations

**Horizon**
```bash
curl "https://horizon.stellar.org/transactions/abc123.../operations"
```

**Query API**
```bash
curl -H "Authorization: Api-Key $API_KEY" \
  "$GATEWAY/api/v1/silver/transactions/abc123.../operations"
```

### Get Transaction Effects

**Horizon**
```bash
curl "https://horizon.stellar.org/transactions/abc123.../effects"
```

**Query API**
```bash
curl -H "Authorization: Api-Key $API_KEY" \
  "$GATEWAY/api/v1/silver/transactions/abc123.../effects"
```

---

## Operations

### List Operations

**Horizon**
```bash
curl "https://horizon.stellar.org/operations?limit=20"
```

**Query API** (enriched with additional context)
```bash
curl -H "Authorization: Api-Key $API_KEY" \
  "$GATEWAY/api/v1/silver/operations/enriched?limit=20"
```

### Get Operation by ID

**Horizon**
```bash
curl "https://horizon.stellar.org/operations/12345"
```

**Query API**
```bash
curl -H "Authorization: Api-Key $API_KEY" \
  "$GATEWAY/api/v1/silver/operations/12345"
```

### Filter by Operation Type

**Horizon**
```bash
# No direct filter - must iterate through results
```

**Query API** (native filtering)
```bash
curl -H "Authorization: Api-Key $API_KEY" \
  "$GATEWAY/api/v1/silver/operations/enriched?type=payment"
```

---

## Effects

### List Effects

**Horizon**
```bash
curl "https://horizon.stellar.org/effects?limit=20"
```

**Query API**
```bash
curl -H "Authorization: Api-Key $API_KEY" \
  "$GATEWAY/api/v1/silver/effects?limit=20"
```

### Filter by Effect Type

**Horizon**
```bash
# Limited filtering
```

**Query API** (native filtering)
```bash
curl -H "Authorization: Api-Key $API_KEY" \
  "$GATEWAY/api/v1/silver/effects?type=account_credited"
```

---

## Trades

### List Trades

**Horizon**
```bash
curl "https://horizon.stellar.org/trades?limit=20"
```

**Query API**
```bash
curl -H "Authorization: Api-Key $API_KEY" \
  "$GATEWAY/api/v1/silver/trades?limit=20"
```

### Filter Trades by Asset Pair

**Horizon**
```bash
curl "https://horizon.stellar.org/trades?base_asset_type=native&counter_asset_type=credit_alphanum4&counter_asset_code=USDC&counter_asset_issuer=GA5ZSE..."
```

**Query API**
```bash
curl -H "Authorization: Api-Key $API_KEY" \
  "$GATEWAY/api/v1/silver/trades?base_asset=native&counter_asset=USDC:GA5ZSE..."
```

### Trade Aggregations (Candlesticks)

**Horizon**
```bash
curl "https://horizon.stellar.org/trade_aggregations?base_asset_type=native&counter_asset_type=credit_alphanum4&counter_asset_code=USDC&counter_asset_issuer=GA5ZSE...&resolution=3600000"
```

**Query API**
```bash
curl -H "Authorization: Api-Key $API_KEY" \
  "$GATEWAY/api/v1/silver/trades/aggregations?base_asset=native&counter_asset=USDC:GA5ZSE...&resolution=1h"
```

---

## Offers

### List Offers

**Horizon**
```bash
curl "https://horizon.stellar.org/offers?limit=20"
```

**Query API**
```bash
curl -H "Authorization: Api-Key $API_KEY" \
  "$GATEWAY/api/v1/silver/offers?limit=20"
```

### Get Offer by ID

**Horizon**
```bash
curl "https://horizon.stellar.org/offers/12345"
```

**Query API**
```bash
curl -H "Authorization: Api-Key $API_KEY" \
  "$GATEWAY/api/v1/silver/offers/12345"
```

---

## Liquidity Pools

### List Liquidity Pools

**Horizon**
```bash
curl "https://horizon.stellar.org/liquidity_pools?limit=20"
```

**Query API**
```bash
curl -H "Authorization: Api-Key $API_KEY" \
  "$GATEWAY/api/v1/silver/liquidity-pools?limit=20"
```

### Get Liquidity Pool by ID

**Horizon**
```bash
curl "https://horizon.stellar.org/liquidity_pools/abcd1234..."
```

**Query API**
```bash
curl -H "Authorization: Api-Key $API_KEY" \
  "$GATEWAY/api/v1/silver/liquidity-pools/abcd1234..."
```

### Liquidity Pool Trades (Not Yet Available)

**Horizon**
```bash
curl "https://horizon.stellar.org/liquidity_pools/abcd1234.../trades"
```

**Query API Workaround** - Filter trades endpoint:
```bash
# Coming soon: /silver/liquidity-pools/{id}/trades
# Current workaround - filter trades by type:
curl -H "Authorization: Api-Key $API_KEY" \
  "$GATEWAY/api/v1/silver/trades?trade_type=liquidity_pool&limit=100"
```

---

## Claimable Balances

### List Claimable Balances

**Horizon**
```bash
curl "https://horizon.stellar.org/claimable_balances?claimant=GABC..."
```

**Query API**
```bash
curl -H "Authorization: Api-Key $API_KEY" \
  "$GATEWAY/api/v1/silver/claimable-balances?claimant=GABC..."
```

### Get Claimable Balance by ID

**Horizon**
```bash
curl "https://horizon.stellar.org/claimable_balances/00000000..."
```

**Query API**
```bash
curl -H "Authorization: Api-Key $API_KEY" \
  "$GATEWAY/api/v1/silver/claimable-balances/00000000..."
```

### Claimable Balance Operations (Not Yet Available)

**Horizon**
```bash
curl "https://horizon.stellar.org/claimable_balances/00000000.../operations"
```

**Query API Workaround** - Filter operations by type:
```bash
# Coming soon: /silver/claimable-balances/{id}/operations
# Current workaround - filter operations by CB types:
curl -H "Authorization: Api-Key $API_KEY" \
  "$GATEWAY/api/v1/silver/operations/enriched?type=create_claimable_balance&limit=100"
```

---

## Not Available in Query API

### Order Book

**Horizon**
```bash
curl "https://horizon.stellar.org/order_book?selling_asset_type=native&buying_asset_type=credit_alphanum4&buying_asset_code=USDC&buying_asset_issuer=GA5ZSE..."
```

**Workaround**: Query offers and aggregate client-side:
```bash
curl -H "Authorization: Api-Key $API_KEY" \
  "$GATEWAY/api/v1/silver/offers?selling_asset=native&buying_asset=USDC:GA5ZSE...&limit=200"
```

### Fee Stats

**Horizon**
```bash
curl "https://horizon.stellar.org/fee_stats"
```

**Workaround**: Use Horizon for fee stats, or hardcode reasonable defaults:
- Low priority: 100 stroops
- Medium priority: 200 stroops
- High priority: 1000 stroops

### Path Finding

**Horizon**
```bash
curl "https://horizon.stellar.org/paths/strict-receive?source_account=GABC...&destination_asset_type=credit_alphanum4&destination_asset_code=USDC&destination_asset_issuer=GA5ZSE...&destination_amount=100"
```

**Not Available**: Path finding requires real-time graph computation on live order book state. Continue using Horizon for this functionality.

### Transaction Submission

**Horizon**
```bash
curl -X POST "https://horizon.stellar.org/transactions" \
  -d "tx=AAAA..."
```

**Not Available**: Query API is read-only. Use Horizon or Stellar RPC for transaction submission.

---

## Query API Exclusive Features

These endpoints have no Horizon equivalent:

### Contract Events (Soroban)

```bash
# Get contract events
curl -H "Authorization: Api-Key $API_KEY" \
  "$GATEWAY/api/v1/silver/contracts/{contract_id}/events"

# Get contract invocations
curl -H "Authorization: Api-Key $API_KEY" \
  "$GATEWAY/api/v1/silver/contracts/{contract_id}/invocations"
```

### Token Transfers (SAC)

```bash
# Get all token transfers
curl -H "Authorization: Api-Key $API_KEY" \
  "$GATEWAY/api/v1/silver/transfers?asset=USDC:GA5ZSE...&limit=100"

# Get transfers for an account
curl -H "Authorization: Api-Key $API_KEY" \
  "$GATEWAY/api/v1/silver/accounts/{id}/transfers"
```

### Compliance Data

```bash
# Get OFAC screening status
curl -H "Authorization: Api-Key $API_KEY" \
  "$GATEWAY/api/v1/gold/compliance/accounts/{id}"
```

### Analytics & Snapshots

```bash
# Account balance snapshots
curl -H "Authorization: Api-Key $API_KEY" \
  "$GATEWAY/api/v1/gold/snapshots/accounts?at_ledger=54930000"

# Asset statistics
curl -H "Authorization: Api-Key $API_KEY" \
  "$GATEWAY/api/v1/gold/assets/{asset}/stats"
```

### Fast Hash Lookups (Index Layer)

```bash
# Lookup transaction by hash (faster than Silver)
curl -H "Authorization: Api-Key $API_KEY" \
  "$GATEWAY/api/v1/index/transactions/abc123..."

# Lookup operation by ID
curl -H "Authorization: Api-Key $API_KEY" \
  "$GATEWAY/api/v1/index/operations/12345"
```

---

## Key Differences

| Aspect | Horizon | Query API |
|--------|---------|-----------|
| Authentication | None required | API key required |
| Rate Limits | Varies by instance | Varies by plan |
| Data Freshness | Real-time (~5s) | Near real-time (~10-30s) |
| Historical Data | Full history | Full history |
| Streaming | SSE supported | Not yet available |
| Write Operations | Yes (submit tx) | No (read-only) |
| Soroban Support | Basic | Comprehensive |
| Analytics | None | Built-in |

## Pagination Differences

**Horizon** uses cursor-based pagination:
```bash
curl "https://horizon.stellar.org/transactions?cursor=12345&limit=20"
```

**Query API** uses offset or ledger-based pagination:
```bash
# Offset-based
curl -H "Authorization: Api-Key $API_KEY" \
  "$GATEWAY/api/v1/silver/transactions?offset=100&limit=20"

# Ledger range
curl -H "Authorization: Api-Key $API_KEY" \
  "$GATEWAY/api/v1/silver/transactions?start_ledger=54930000&end_ledger=54931000"
```

---

## Migration Checklist

- [ ] Get API key from Obsrvr Gateway
- [ ] Update base URL in your application
- [ ] Add Authorization header to all requests
- [ ] Update pagination logic (cursor → offset/ledger)
- [ ] Test all endpoints your application uses
- [ ] Keep Horizon for: path finding, transaction submission, fee stats
- [ ] Explore new Query API features: Soroban events, transfers, analytics

## Getting Help

- **Documentation**: See other guides in this folder
- **API Reference**: Check swagger.yaml in the repository
- **Support**: Contact Obsrvr support for API key issues
