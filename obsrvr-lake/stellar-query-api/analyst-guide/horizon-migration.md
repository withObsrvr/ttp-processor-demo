# Horizon to Query API Migration Guide

> **For developers migrating from Stellar Horizon to Obsrvr Gateway Query API**

This guide maps **currently deployed and verified** Query API routes to familiar Horizon use cases.

Important:
- Query API is **read-only**
- Query API requires an API key
- not every Horizon route has a 1:1 equivalent yet
- where a direct equivalent is missing, this guide points to the closest supported route or marks it unsupported

## Quick Reference

| What You Need | Horizon | Query API | Coverage |
|---------------|---------|-----------|----------|
| Account current state | `/accounts/{id}` | `/silver/accounts/current?account_id={id}` | Partial |
| Account balances / trustlines | embedded in `/accounts/{id}` | `/silver/accounts/{id}/balances` | Full |
| Account offers | `/accounts/{id}/offers` | `/silver/accounts/{id}/offers` | Full |
| Assets list | `/assets` | `/silver/assets` | Full |
| Asset holders | `/assets?...` + client logic | `/silver/assets/{asset}/holders` | Full |
| Asset stats | none | `/silver/assets/{asset}/stats` | Query API only |
| Top accounts | none | `/silver/accounts/top` | Query API only |
| Ledgers list | `/ledgers` | Not currently available as a silver route | Not supported |
| Single ledger | `/ledgers/{seq}` | `/silver/ledgers/{seq}` or `/silver/ledger/{seq}` | Full |
| Transactions list | `/transactions` | Not currently available as a silver route | Not supported |
| Transaction decode | none | `/silver/tx/{hash}/decoded` | Query API only |
| Transaction full receipt | none | `/silver/tx/{hash}/full` | Query API only |
| Transaction diffs | none | `/silver/tx/{hash}/diffs` | Query API only |
| Transaction effects | `/transactions/{hash}/effects` | `/silver/tx/{hash}/effects` or `/silver/effects/transaction/{tx_hash}` | Full |
| Effects list | `/effects` | `/silver/effects` | Full |
| Trades list | `/trades` | `/silver/trades` | Full |
| Offers list | `/offers` | `/silver/offers` | Full |
| Liquidity pools | `/liquidity_pools` | `/silver/liquidity-pools` | Partial |
| Claimable balances | `/claimable_balances` | `/silver/claimable-balances` | Partial |
| Fee stats | `/fee_stats` | `/silver/stats/fees` | Partial |
| Top contracts | none | `/silver/contracts/top` or `/silver/stats/contracts` | Query API only |
| Contract metadata | none | `/silver/contracts/{id}/metadata` | Query API only |
| Contract recent calls | none | `/silver/contracts/{id}/recent-calls` | Query API only |
| Generic contract events | none | `/silver/events/generic` | Query API only |
| Path finding | `/paths/*` | Not available | Unsupported |
| Submit transaction | `POST /transactions` | Not available | Unsupported |

---

## Base URLs

**Horizon (Public)**
```
https://horizon.stellar.org
https://horizon-testnet.stellar.org
```

**Obsrvr Gateway Query API**
```
https://gateway.withobsrvr.com/lake/v1/testnet
```

**Authentication**

```bash
curl -H "Authorization: Api-Key YOUR_API_KEY" \
  "https://gateway.withobsrvr.com/lake/v1/testnet/api/v1/silver/accounts/current?account_id=G..."
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
  "$GATEWAY/api/v1/silver/accounts/current?account_id=GABC..."
```

> Query API does not currently expose a deployed `/silver/accounts/{id}` route.

### Get Account Balances / Trustlines

**Horizon**
```bash
curl "https://horizon.stellar.org/accounts/GABC..." | jq '.balances'
```

**Query API**
```bash
curl -H "Authorization: Api-Key $API_KEY" \
  "$GATEWAY/api/v1/silver/accounts/GABC.../balances"
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

### Account Transactions / Operations / Effects / Trades

These Horizon-style account subresources are **not currently deployed** as 1:1 routes.

Use these supported alternatives instead:
- account activity timeline:
  - `/api/v1/silver/accounts/{id}/activity`
- effects filtered by account:
  - `/api/v1/silver/effects?account_id={id}`
- trades filtered by account:
  - `/api/v1/silver/trades?account_id={id}`
- transfers filtered by account:
  - `/api/v1/silver/transfers?from_account={id}`
  - `/api/v1/silver/transfers?to_account={id}`

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

### Get Asset Holders

**Query API**
```bash
curl -H "Authorization: Api-Key $API_KEY" \
  "$GATEWAY/api/v1/silver/assets/USDC:GA5ZSE.../holders?limit=10"
```

### Get Asset Stats

**Query API**
```bash
curl -H "Authorization: Api-Key $API_KEY" \
  "$GATEWAY/api/v1/silver/assets/USDC:GA5ZSE.../stats"
```

> A direct deployed `/silver/assets/{asset}` route is not currently available.

---

## Ledgers

### List Ledgers

**Horizon**
```bash
curl "https://horizon.stellar.org/ledgers?limit=10&order=desc"
```

**Query API**
Not currently available as a deployed silver route.

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

Compatibility alias:
```bash
curl -H "Authorization: Api-Key $API_KEY" \
  "$GATEWAY/api/v1/silver/ledger/54930000"
```

### Ledger Transactions / Operations

These Horizon-style ledger subresources are **not currently deployed** as silver routes.

---

## Transactions

### List Transactions

**Horizon**
```bash
curl "https://horizon.stellar.org/transactions?limit=20&order=desc"
```

**Query API**
Not currently available as a deployed silver route.

### Get Transaction Receipt / Decode

**Query API**
```bash
curl -H "Authorization: Api-Key $API_KEY" \
  "$GATEWAY/api/v1/silver/tx/{hash}/decoded"

curl -H "Authorization: Api-Key $API_KEY" \
  "$GATEWAY/api/v1/silver/tx/{hash}/full"

curl -H "Authorization: Api-Key $API_KEY" \
  "$GATEWAY/api/v1/silver/tx/{hash}/diffs"
```

### Get Transaction Effects

**Horizon**
```bash
curl "https://horizon.stellar.org/transactions/abc123.../effects"
```

**Query API**
```bash
curl -H "Authorization: Api-Key $API_KEY" \
  "$GATEWAY/api/v1/silver/tx/abc123.../effects"
```

Also supported:
```bash
curl -H "Authorization: Api-Key $API_KEY" \
  "$GATEWAY/api/v1/silver/effects/transaction/abc123..."
```

### Get Transaction Operations

A direct deployed `/silver/transactions/{hash}/operations` route is not currently available.

Closest alternatives:
- `/api/v1/silver/tx/{hash}/decoded`
- `/api/v1/silver/tx/{hash}/full`

---

## Operations

### List Operations

**Horizon**
```bash
curl "https://horizon.stellar.org/operations?limit=20"
```

**Query API**
```bash
curl -H "Authorization: Api-Key $API_KEY" \
  "$GATEWAY/api/v1/silver/operations/enriched?limit=20"
```

### Get Operation by ID

A direct deployed `/silver/operations/{id}` route is not currently available.

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

**Query API**
```bash
curl -H "Authorization: Api-Key $API_KEY" \
  "$GATEWAY/api/v1/silver/trades/by-pair?base_asset=native&counter_asset=USDC:GA5ZSE..."
```

> `/silver/trades/aggregations` is not currently deployed.

---

## Offers

### List Offers

```bash
curl -H "Authorization: Api-Key $API_KEY" \
  "$GATEWAY/api/v1/silver/offers?limit=20"
```

### Get Offer by ID

```bash
curl -H "Authorization: Api-Key $API_KEY" \
  "$GATEWAY/api/v1/silver/offers/12345"
```

---

## Liquidity Pools

### List Liquidity Pools

```bash
curl -H "Authorization: Api-Key $API_KEY" \
  "$GATEWAY/api/v1/silver/liquidity-pools?limit=20"
```

### Get Liquidity Pool by ID

```bash
curl -H "Authorization: Api-Key $API_KEY" \
  "$GATEWAY/api/v1/silver/liquidity-pools/abcd1234..."
```

---

## Claimable Balances

### List Claimable Balances

```bash
curl -H "Authorization: Api-Key $API_KEY" \
  "$GATEWAY/api/v1/silver/claimable-balances?claimant=GABC..."
```

### Get Claimable Balance by ID

```bash
curl -H "Authorization: Api-Key $API_KEY" \
  "$GATEWAY/api/v1/silver/claimable-balances/00000000..."
```

---

## Query API Exclusive Features

### Contract Analytics

```bash
curl -H "Authorization: Api-Key $API_KEY" \
  "$GATEWAY/api/v1/silver/contracts/top?limit=20"

curl -H "Authorization: Api-Key $API_KEY" \
  "$GATEWAY/api/v1/silver/stats/contracts?limit=20"

curl -H "Authorization: Api-Key $API_KEY" \
  "$GATEWAY/api/v1/silver/contracts/{contract_id}/metadata"

curl -H "Authorization: Api-Key $API_KEY" \
  "$GATEWAY/api/v1/silver/contracts/{contract_id}/recent-calls"
```

### Transfers

```bash
curl -H "Authorization: Api-Key $API_KEY" \
  "$GATEWAY/api/v1/silver/transfers?asset_code=USDC&limit=100"

curl -H "Authorization: Api-Key $API_KEY" \
  "$GATEWAY/api/v1/silver/transfers?from_account=GABC...&limit=100"

curl -H "Authorization: Api-Key $API_KEY" \
  "$GATEWAY/api/v1/silver/transfers?to_account=GABC...&limit=100"
```

### Generic Contract Events

```bash
curl -H "Authorization: Api-Key $API_KEY" \
  "$GATEWAY/api/v1/silver/events/generic?limit=100"
```

### Compliance & Snapshot Features

See the current analyst guide and API reference for the actively supported Gold endpoints.
Some older example paths that appeared in earlier docs are not currently deployed verbatim.

---

## Not Available in Query API

- order book
- path finding
- transaction submission
- several Horizon-style account and ledger subresources
- several singular transaction/operation list routes under `/silver/transactions/*`

---

## Key Differences

| Aspect | Horizon | Query API |
|--------|---------|-----------|
| Authentication | None required | API key required |
| Write operations | Yes | No |
| Soroban support | Basic | Extensive |
| Analytics | Limited | Built-in |
| Contract analytics | No | Yes |
| Transaction receipts | Limited | Yes |

## Pagination Differences

Query API primarily uses **cursor-based pagination** on feed endpoints.
Keep the returned `cursor` and pass it back on the next request.

Example:
```bash
curl -H "Authorization: Api-Key $API_KEY" \
  "$GATEWAY/api/v1/silver/transfers?limit=100"
```

---

## Migration Checklist

- [ ] Get an API key from Obsrvr Gateway
- [ ] Update base URL to `$GATEWAY/api/v1/...`
- [ ] Add `Authorization: Api-Key ...` to all requests
- [ ] Re-check every Horizon mapping against the **currently deployed** routes
- [ ] Prefer silver analytics routes over trying to preserve Horizon path shapes
- [ ] Keep Horizon or RPC for transaction submission and path finding
- [ ] Use Query API-specific tx receipt and contract analytics endpoints where useful

## Getting Help

- Use the analyst guide index and common queries reference for current routes
- Treat this document as a migration aid, not a promise of strict Horizon parity
- Verify uncertain routes against the live gateway before integrating
