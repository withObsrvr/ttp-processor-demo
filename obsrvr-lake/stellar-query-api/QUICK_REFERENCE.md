# Silver API Quick Reference

## Base URL
```
http://134.209.123.25:8092/api/v1/silver
```

## Quick Start

### SSH Tunnel (for local access)
```bash
ssh -i /path/to/obsrvr-lake-prod.pem -L 8092:localhost:8092 root@134.209.123.25
```

### Test Connection
```bash
curl http://localhost:8092/health
```

---

## Common Queries

### Get Account
```bash
curl 'http://localhost:8092/api/v1/silver/accounts/current?account_id=YOUR_ACCOUNT_ID'
```

### Get Account History
```bash
curl 'http://localhost:8092/api/v1/silver/accounts/history?account_id=YOUR_ACCOUNT_ID&limit=10'
```

### Get Top Accounts
```bash
curl 'http://localhost:8092/api/v1/silver/accounts/top?limit=10'
```

### Get Recent Operations
```bash
curl 'http://localhost:8092/api/v1/silver/operations/enriched?limit=10'
```

### Get Account Operations
```bash
curl 'http://localhost:8092/api/v1/silver/operations/enriched?account_id=YOUR_ACCOUNT_ID&limit=10'
```

### Get Payments Only
```bash
curl 'http://localhost:8092/api/v1/silver/payments?limit=10'
```

### Get Account Payments
```bash
curl 'http://localhost:8092/api/v1/silver/payments?account_id=YOUR_ACCOUNT_ID&limit=10'
```

### Get Token Transfers
```bash
curl 'http://localhost:8092/api/v1/silver/transfers?limit=10'
```

### Get Soroban Operations
```bash
curl 'http://localhost:8092/api/v1/silver/operations/soroban?limit=10'
```

### Get Account Overview (Explorer)
```bash
curl 'http://localhost:8092/api/v1/silver/explorer/account?account_id=YOUR_ACCOUNT_ID'
```

---

## Filters

### By Ledger Range
```bash
curl 'http://localhost:8092/api/v1/silver/operations/enriched?start_ledger=262000&end_ledger=263000&limit=100'
```

### By Transaction Hash
```bash
curl 'http://localhost:8092/api/v1/silver/operations/enriched?tx_hash=YOUR_TX_HASH'
```

### Payments Only
```bash
curl 'http://localhost:8092/api/v1/silver/operations/enriched?payments_only=true&limit=10'
```

### Soroban Only
```bash
curl 'http://localhost:8092/api/v1/silver/operations/enriched?soroban_only=true&limit=10'
```

### By Asset
```bash
curl 'http://localhost:8092/api/v1/silver/transfers?asset_code=USDC&limit=10'
```

### By Source Type
```bash
curl 'http://localhost:8092/api/v1/silver/transfers?source_type=soroban&limit=10'
```

---

## Pretty Print with jq

```bash
curl -s 'http://localhost:8092/api/v1/silver/accounts/current?account_id=YOUR_ACCOUNT_ID' | jq .
```

---

## Architecture

```
Hot Buffer (PostgreSQL)  →  Query returns immediately (< 100ms)
     ↓ (if not found)
Cold Storage (Parquet)   →  Query historical data (200-500ms)
     ↓
   Merged Result         →  Complete view returned
```

**Data Freshness**:
- Hot: Last ~3 hours (< 1 minute lag from testnet)
- Cold: Historical (3+ hours old)

---

## All Endpoints

| Endpoint | Purpose |
|----------|---------|
| `/health` | Health check |
| `/accounts/current` | Get account state |
| `/accounts/history` | Get account snapshots |
| `/accounts/top` | Get top accounts by balance |
| `/operations/enriched` | Get operations with context |
| `/operations/soroban` | Get Soroban operations |
| `/payments` | Get payment operations |
| `/transfers` | Get token transfers |
| `/transfers/stats` | Get transfer statistics |
| `/explorer/account` | Get account overview |
| `/explorer/transaction` | Get transaction details |
| `/explorer/asset` | Get asset overview |

---

## Common Parameters

- `account_id` - Stellar account ID
- `tx_hash` - Transaction hash
- `limit` - Number of results (default varies by endpoint)
- `start_ledger` - Start ledger sequence
- `end_ledger` - End ledger sequence
- `payments_only` - Filter to payments (true/false)
- `soroban_only` - Filter to Soroban (true/false)
- `asset_code` - Asset code
- `source_type` - "classic" or "soroban"

---

## Example Responses

### Account Current
```json
{
  "account": {
    "account_id": "GXXX...",
    "balance": "9999.5857200",
    "sequence_number": "1937030260847",
    "num_subentries": 18,
    "last_modified_ledger": 262762,
    "updated_at": "2026-01-01T22:40:14Z"
  }
}
```

### Operations
```json
{
  "count": 5,
  "operations": [
    {
      "transaction_hash": "75e18024...",
      "operation_id": 1,
      "ledger_sequence": 262772,
      "ledger_closed_at": "2026-01-01T22:41:03Z",
      "source_account": "GC6ICL6X...",
      "type": 1,
      "type_name": "PAYMENT",
      "destination": "GBMHKGWU...",
      "amount": "24700000",
      "tx_successful": false
    }
  ]
}
```

---

## Monitoring

### Check Health
```bash
curl http://localhost:8092/health
```

### Watch Health
```bash
watch -n 5 'curl -s http://localhost:8092/health | jq .'
```

### Check Services
```bash
nomad job status stellar-query-api
nomad job status stellar-postgres-ingester
nomad job status bronze-silver-transformer
nomad job status silver-cold-flusher
```

---

## For Full Documentation

See: `SILVER_API_DOCUMENTATION.md`
