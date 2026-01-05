# Common Queries

Recipe book for the most common analytics tasks. All examples use the testnet API.

## Base URL

```
https://gateway.withobsrvr.com
```

## Account Queries

### Get Current Account Balance

```bash
curl "https://gateway.withobsrvr.com/api/v1/silver/accounts/current?account_id=GAIH3ULLFQ4DGSECF2AR555KZ4KNDGEKN4AFI4SU2M7B43MGK3QJZNSR" | jq .
```

Response:
```json
{
  "account_id": "GAIH3ULLFQ4DGSECF2AR555KZ4KNDGEKN4AFI4SU2M7B43MGK3QJZNSR",
  "balance": 10000000000,
  "sequence_number": 123456789,
  "num_subentries": 3,
  "last_modified_ledger": 21379180
}
```

Note: Balance is in stroops. Divide by 10,000,000 for XLM.

### Get Top Accounts by Balance

```bash
curl "https://gateway.withobsrvr.com/api/v1/silver/accounts/top?limit=20" | jq .
```

### Get Account Operation History

```bash
curl "https://gateway.withobsrvr.com/api/v1/silver/operations/enriched?account_id=GAIH3ULLFQ4DGSECF2AR555KZ4KNDGEKN4AFI4SU2M7B43MGK3QJZNSR&limit=50" | jq .
```

## Payment Queries

### Get All Payments for an Account

```bash
curl "https://gateway.withobsrvr.com/api/v1/silver/payments?account_id=GAIH3ULLFQ4DGSECF2AR555KZ4KNDGEKN4AFI4SU2M7B43MGK3QJZNSR&limit=100" | jq .
```

### Get Token Transfers (Payments + Path Payments)

```bash
curl "https://gateway.withobsrvr.com/api/v1/silver/transfers?account_id=GAIH3ULLFQ4DGSECF2AR555KZ4KNDGEKN4AFI4SU2M7B43MGK3QJZNSR&limit=100" | jq .
```

### Filter by Asset

```bash
# USDC transfers only
curl "https://gateway.withobsrvr.com/api/v1/silver/transfers?account_id=GAIH3ULLFQ4DGSECF2AR555KZ4KNDGEKN4AFI4SU2M7B43MGK3QJZNSR&asset_code=USDC" | jq .
```

### Get Transfer Statistics

```bash
curl "https://gateway.withobsrvr.com/api/v1/silver/transfers/stats?account_id=GAIH3ULLFQ4DGSECF2AR555KZ4KNDGEKN4AFI4SU2M7B43MGK3QJZNSR" | jq .
```

## Smart Contract Queries

### Top Active Contracts (24h/7d/30d)

```bash
# Most active in last 24 hours
curl "https://gateway.withobsrvr.com/api/v1/silver/contracts/top?period=24h&limit=20" | jq .

# Most active in last 7 days
curl "https://gateway.withobsrvr.com/api/v1/silver/contracts/top?period=7d&limit=20" | jq .
```

Response:
```json
{
  "period": "24h",
  "contracts": [
    {
      "contract_id": "CAUGJT4GREIY3WHOUUU5RIUDGSPVREF5CDCYJOWMHOVT2GWQT5JEETGJ",
      "total_calls": 537,
      "unique_callers": 5,
      "top_function": "transfer",
      "last_activity": "2026-01-05T12:57:07Z"
    }
  ],
  "count": 20
}
```

### Contract Analytics Summary

```bash
curl "https://gateway.withobsrvr.com/api/v1/silver/contracts/CAUGJT4GREIY3WHOUUU5RIUDGSPVREF5CDCYJOWMHOVT2GWQT5JEETGJ/analytics" | jq .
```

Response:
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

### Contracts Involved in a Transaction

For Freighter-style "contracts involved" display:

```bash
curl "https://gateway.withobsrvr.com/api/v1/silver/tx/abc123.../contracts-involved" | jq .
```

### Full Call Graph for a Transaction

```bash
curl "https://gateway.withobsrvr.com/api/v1/silver/tx/abc123.../call-graph" | jq .
```

Response:
```json
{
  "transaction_hash": "abc123...",
  "calls": [
    {
      "from": "CABC...",
      "to": "CDEF...",
      "function": "swap",
      "depth": 0,
      "order": 0,
      "successful": true
    },
    {
      "from": "CDEF...",
      "to": "CGHI...",
      "function": "transfer",
      "depth": 1,
      "order": 1,
      "successful": true
    }
  ],
  "count": 2
}
```

### Who Calls a Contract?

```bash
curl "https://gateway.withobsrvr.com/api/v1/silver/contracts/CAUGJT4.../callers?limit=20" | jq .
```

### What Contracts Does It Call?

```bash
curl "https://gateway.withobsrvr.com/api/v1/silver/contracts/CAUGJT4.../callees?limit=20" | jq .
```

## Transaction Lookup

### Fast Transaction Lookup by Hash

```bash
curl "https://gateway.withobsrvr.com/transactions/{hash}" | jq .
```

This uses the Index Plane for sub-100ms lookups across all history.

## Pagination Example

All list endpoints support cursor-based pagination:

```bash
# First page
RESPONSE=$(curl -s "https://gateway.withobsrvr.com/api/v1/silver/transfers?limit=100")
echo "$RESPONSE" | jq '.transfers | length'

# Get cursor for next page
CURSOR=$(echo "$RESPONSE" | jq -r '.cursor')
HAS_MORE=$(echo "$RESPONSE" | jq '.has_more')

# Continue if there's more
if [ "$HAS_MORE" = "true" ]; then
  curl "https://gateway.withobsrvr.com/api/v1/silver/transfers?limit=100&cursor=$CURSOR" | jq .
fi
```

## Horizon API Equivalents

| What you want | Horizon Endpoint | Obsrvr Lake Equivalent |
|---------------|------------------|------------------------|
| Account details | `GET /accounts/{id}` | `/silver/accounts/current?account_id={id}` |
| Account operations | `GET /accounts/{id}/operations` | `/silver/operations/enriched?account_id={id}` |
| Account payments | `GET /accounts/{id}/payments` | `/silver/payments?account_id={id}` |
| Account effects | `GET /accounts/{id}/effects` | `/bronze/effects?account_id={id}` |
| Transaction details | `GET /transactions/{hash}` | `/transactions/{hash}` |
| Ledger info | `GET /ledgers/{sequence}` | `/bronze/ledgers?sequence={sequence}` |

## Filtering and Sorting

Most endpoints support these query parameters:

| Parameter | Description | Example |
|-----------|-------------|---------|
| `limit` | Max results (default 100, max 10000) | `?limit=500` |
| `cursor` | Pagination cursor from previous response | `?cursor=MjEzNzkxODo1` |
| `account_id` | Filter by account | `?account_id=GABC...` |
| `start_ledger` | Filter by ledger range | `?start_ledger=21000000` |
| `end_ledger` | Filter by ledger range | `?end_ledger=21379180` |
| `asset_code` | Filter by asset | `?asset_code=USDC` |
| `asset_issuer` | Filter by asset issuer | `?asset_issuer=GA5Z...` |

Note: `cursor` and `start_ledger` are mutually exclusive.
