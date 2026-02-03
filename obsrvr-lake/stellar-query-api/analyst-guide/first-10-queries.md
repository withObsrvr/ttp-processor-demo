# First 10 Queries Every Analyst Should Run

These are the essential queries that cover the most common analyst needs. Each is copy-paste ready and works on testnet.

**Base URL:** `https://gateway.withobsrvr.com/lake/v1/testnet`

**All requests require:** `Authorization: Api-Key YOUR_API_KEY`

---

## Account Queries

### 1. Get Current Account State

**Question:** What's the current balance and status of this account?

```bash
curl -H "Authorization: Api-Key $API_KEY" \
  "https://gateway.withobsrvr.com/lake/v1/testnet/api/v1/silver/accounts/current?account_id=GAIH3ULLFQ4DGSECF2AR555KZ4KNDGEKN4AFI4SU2M7B43MGK3QJZNSR"
```

**Response:**
```json
{
  "account": {
    "account_id": "GAIH3ULLFQ4DGSECF2AR555KZ4KNDGEKN4AFI4SU2M7B43MGK3QJZNSR",
    "balance": 75835771989309600,
    "sequence_number": 123456789,
    "num_subentries": 3,
    "last_modified_ledger": 341708
  }
}
```

**When to use:** Account lookup, balance verification, checking if account exists.

> **Tip:** Balance is in stroops. Divide by 10,000,000 for XLM.

---

### 2. Get All Balances (XLM + Tokens)

**Question:** What tokens does this account hold, and how much of each?

```bash
curl -H "Authorization: Api-Key $API_KEY" \
  "https://gateway.withobsrvr.com/lake/v1/testnet/api/v1/silver/accounts/GAIH3ULLFQ4DGSECF2AR555KZ4KNDGEKN4AFI4SU2M7B43MGK3QJZNSR/balances"
```

**Response:**
```json
{
  "account_id": "GAIH3ULLFQ4DGSECF2AR555KZ4KNDGEKN4AFI4SU2M7B43MGK3QJZNSR",
  "balances": [
    {
      "asset_type": "native",
      "asset_code": "XLM",
      "balance": "7583577198.9309600",
      "balance_stroops": 75835771989309600
    },
    {
      "asset_type": "credit_alphanum4",
      "asset_code": "USDC",
      "asset_issuer": "GA5ZSEJYB37JRC5AVCIA5MOP4RHTM335X2KGX3IHOJAPP5RE34K4KZVN",
      "balance": "250.0000000",
      "balance_stroops": 2500000000,
      "is_authorized": true
    }
  ],
  "total_balances": 2
}
```

**When to use:** Wallet integrations, portfolio views, checking token holdings.

---

### 3. Find Top Accounts by Balance

**Question:** Who are the largest XLM holders?

```bash
curl -H "Authorization: Api-Key $API_KEY" \
  "https://gateway.withobsrvr.com/lake/v1/testnet/api/v1/silver/accounts/top?limit=10"
```

**Response:**
```json
{
  "accounts": [
    {
      "account_id": "GAIH3ULLFQ4DGSECF2AR555KZ4KNDGEKN4AFI4SU2M7B43MGK3QJZNSR",
      "balance": 75835771989309600,
      "rank": 1
    }
  ],
  "count": 10
}
```

**When to use:** Whale tracking, network concentration analysis, top holder reports.

---

## Token Queries

### 4. Get Token Transfer History

**Question:** What USDC transfers has this account sent or received?

```bash
curl -H "Authorization: Api-Key $API_KEY" \
  "https://gateway.withobsrvr.com/lake/v1/testnet/api/v1/silver/transfers?from_account=GAIH3ULLFQ4DGSECF2AR555KZ4KNDGEKN4AFI4SU2M7B43MGK3QJZNSR&asset_code=USDC&limit=20"
```

**Response:**
```json
{
  "transfers": [
    {
      "timestamp": "2026-01-05T12:30:00Z",
      "transaction_hash": "abc123...",
      "from_account": "GAIH3...",
      "to_account": "GBXYZ...",
      "asset_code": "USDC",
      "asset_issuer": "GA5ZSEJYB37...",
      "amount": 1000000000
    }
  ],
  "count": 1,
  "has_more": true,
  "cursor": "eyJsIjoyMTM3OTE4MCwi..."
}
```

**When to use:** Transaction history, payment tracking, flow analysis.

> **Tip:** Use `to_account` instead for incoming transfers.

---

### 5. Find Top Holders of an Asset

**Question:** Who holds the most USDC?

```bash
curl -H "Authorization: Api-Key $API_KEY" \
  "https://gateway.withobsrvr.com/lake/v1/testnet/api/v1/silver/assets/USDC:GA5ZSEJYB37JRC5AVCIA5MOP4RHTM335X2KGX3IHOJAPP5RE34K4KZVN/holders?limit=10"
```

**Response:**
```json
{
  "asset": {
    "code": "USDC",
    "issuer": "GA5ZSEJYB37JRC5AVCIA5MOP4RHTM335X2KGX3IHOJAPP5RE34K4KZVN",
    "type": "credit_alphanum4"
  },
  "holders": [
    {
      "account_id": "GAIH3...",
      "balance": "1000000.0000000",
      "balance_stroops": 10000000000000,
      "rank": 1
    }
  ],
  "total_holders": 150,
  "has_more": true
}
```

**When to use:** Token distribution analysis, holder concentration, ecosystem reports.

---

## Smart Contract Queries

### 6. Find Most Active Contracts (24h)

**Question:** Which smart contracts have the most activity right now?

```bash
curl -H "Authorization: Api-Key $API_KEY" \
  "https://gateway.withobsrvr.com/lake/v1/testnet/api/v1/silver/contracts/top?period=24h&limit=10"
```

**Response:**
```json
{
  "period": "24h",
  "contracts": [
    {
      "contract_id": "CAUGJT4GREIY3WHOUUU5RIUDGSPVREF5CDCYJOWMHOVT2GWQT5JEETGJ",
      "total_calls": 15437,
      "unique_callers": 89,
      "top_function": "swap",
      "last_activity": "2026-01-05T12:57:07Z"
    }
  ],
  "count": 10
}
```

**When to use:** Ecosystem monitoring, finding trending protocols, activity dashboards.

> **Tip:** Try `period=7d` or `period=30d` for longer trends.

---

### 7. Analyze a Contract's Usage

**Question:** Who's calling this contract and what functions are they using?

```bash
curl -H "Authorization: Api-Key $API_KEY" \
  "https://gateway.withobsrvr.com/lake/v1/testnet/api/v1/silver/contracts/CAUGJT4GREIY3WHOUUU5RIUDGSPVREF5CDCYJOWMHOVT2GWQT5JEETGJ/analytics"
```

**Response:**
```json
{
  "contract_id": "CAUGJT4GREIY3WHOUUU5RIUDGSPVREF5CDCYJOWMHOVT2GWQT5JEETGJ",
  "stats": {
    "total_calls_as_caller": 850,
    "total_calls_as_callee": 2008,
    "unique_callers": 5,
    "unique_callees": 850
  },
  "timeline": {
    "first_seen": "2025-12-19T03:55:21Z",
    "last_activity": "2026-01-05T12:57:07Z"
  },
  "top_functions": [
    {"name": "transfer", "count": 154},
    {"name": "swap", "count": 89}
  ],
  "daily_calls_7d": [
    {"date": "2026-01-05", "count": 762},
    {"date": "2026-01-04", "count": 645}
  ]
}
```

**When to use:** Protocol analysis, usage patterns, function popularity.

---

## DeFi Queries

### 8. View DEX Trades for an Asset Pair

**Question:** What trades have happened between XLM and USDC?

```bash
curl -H "Authorization: Api-Key $API_KEY" \
  "https://gateway.withobsrvr.com/lake/v1/testnet/api/v1/silver/trades?selling_asset=XLM&buying_asset=USDC:GBBD47IF6LWK7P7MDEVSCWR7DPUWV3NY3DTQEVFL4NAT4AQH3ZLLFLA5&limit=20"
```

**Response:**
```json
{
  "trades": [
    {
      "ledger_closed_at": "2026-01-05T12:30:00Z",
      "seller": {
        "account_id": "GABC...",
        "asset": {"code": "XLM", "type": "native"},
        "amount": "100.0000000"
      },
      "buyer": {
        "account_id": "GDEF...",
        "asset": {"code": "USDC", "issuer": "GBBD47...", "type": "credit_alphanum4"},
        "amount": "10.0000000"
      },
      "price": "0.1000000",
      "trade_type": "orderbook"
    }
  ],
  "count": 1,
  "has_more": true
}
```

**When to use:** DEX analytics, price discovery, volume analysis.

---

### 9. Check Liquidity Pool Reserves

**Question:** What's the current state of AMM liquidity pools?

```bash
curl -H "Authorization: Api-Key $API_KEY" \
  "https://gateway.withobsrvr.com/lake/v1/testnet/api/v1/silver/liquidity-pools?limit=10"
```

**Response:**
```json
{
  "liquidity_pools": [
    {
      "pool_id": "4cd1f6defba237eecbc5fefe259f89ebc4b5edd49116beb5536c4034fc48d63f",
      "pool_type": "constant_product",
      "fee_bp": 30,
      "trustline_count": 1,
      "total_shares": "824548.7936328",
      "reserves": [
        {
          "asset": {"code": "XLM", "type": "native"},
          "amount": "787612.8481550"
        },
        {
          "asset": {"code": "USDC", "issuer": "GBBD47...", "type": "credit_alphanum4"},
          "amount": "884041.9339546"
        }
      ],
      "last_modified_ledger": 341605
    }
  ],
  "count": 1,
  "has_more": true
}
```

**When to use:** AMM analytics, liquidity monitoring, TVL calculations.

> **Tip:** Use `/liquidity-pools/asset?asset=USDC:G...` to find pools containing a specific token.

---

### 10. Monitor Expiring Contract Data (TTL)

**Question:** Which contract data entries are about to expire?

```bash
curl -H "Authorization: Api-Key $API_KEY" \
  "https://gateway.withobsrvr.com/lake/v1/testnet/api/v1/silver/soroban/ttl/expiring?within_ledgers=100000&limit=20"
```

**Response:**
```json
{
  "ttl_entries": [
    {
      "key_hash": "04513904c5115e1ea69b0ce47ad264ebd95e791f5e9a29515a09b073a0e2ac63",
      "live_until_ledger": 338630,
      "ledgers_remaining": 5000,
      "expired": false,
      "last_modified_ledger": 337911
    }
  ],
  "count": 5,
  "within_ledgers": 100000,
  "has_more": true
}
```

**When to use:** Contract maintenance, TTL monitoring, preventing data loss.

---

## What's Next?

Now that you've run these queries, explore:

- [Getting Started Guide](./getting-started.md) - One-hour structured onboarding
- [Data Model](./data-model.md) - Understanding Bronze vs Silver layers
- [API Reference](./common-queries.md) - Complete endpoint documentation
- [nebu CLI](./nebu-for-analysts.md) - Power user tools for custom analysis

---

## Quick Reference

| Query | Endpoint | Use Case |
|-------|----------|----------|
| Account state | `/silver/accounts/current` | Balance lookup |
| All balances | `/silver/accounts/{id}/balances` | Portfolio view |
| Top accounts | `/silver/accounts/top` | Whale tracking |
| Transfers | `/silver/transfers` | Payment history |
| Asset holders | `/silver/assets/{asset}/holders` | Distribution analysis |
| Top contracts | `/silver/contracts/top` | Activity monitoring |
| Contract analytics | `/silver/contracts/{id}/analytics` | Usage patterns |
| DEX trades | `/silver/trades` | Trading activity |
| Liquidity pools | `/silver/liquidity-pools` | AMM state |
| TTL expiring | `/silver/soroban/ttl/expiring` | Contract maintenance |
