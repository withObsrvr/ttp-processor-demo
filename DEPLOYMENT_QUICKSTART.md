# Contract Invocations - Quick Deployment Reference

## TL;DR - Complete Deployment

```bash
# 1. Apply database migrations
psql -h $BRONZE_HOST -U stellar -d stellar_hot \
  -f stellar-postgres-ingester/migrations/001_add_contract_invocation_fields.sql

psql -h $SILVER_HOST -U stellar -d silver_hot \
  -f silver-realtime-transformer/migrations/001_add_contract_invocations_table.sql

# 2. Build binaries
cd stellar-postgres-ingester/go && go build -o ../bin/stellar-postgres-ingester . && cd ../..
cd silver-realtime-transformer/go && go build -o ../bin/silver-realtime-transformer . && cd ../..

# 3. Deploy (Nomad)
cd /home/tillman/Documents/infra/environments/prod/do-obsrvr-lake/.nomad
nomad job stop stellar-postgres-ingester
nomad job run stellar-postgres-ingester.nomad
nomad job stop silver-realtime-transformer
nomad job run silver-realtime-transformer.nomad

# 4. Verify
psql -h $BRONZE_HOST -U stellar -d stellar_hot -c \
  "SELECT COUNT(*) FROM operations_row_v2 WHERE type=24 AND soroban_arguments_json IS NOT NULL;"

psql -h $SILVER_HOST -U stellar -d silver_hot -c \
  "SELECT COUNT(*), MAX(ledger_sequence) FROM contract_invocations_raw;"
```

---

## Quick Verification Queries

### Bronze: Check Contract Invocations Extraction
```sql
SELECT
    ledger_sequence,
    transaction_index,
    operation_index,
    soroban_contract_id,
    soroban_function,
    LENGTH(soroban_arguments_json) as args_len
FROM operations_row_v2
WHERE type = 24
  AND soroban_arguments_json IS NOT NULL
ORDER BY ledger_sequence DESC
LIMIT 5;
```

### Silver: Check Contract Invocations Transformation
```sql
SELECT
    ledger_sequence,
    (ledger_sequence::BIGINT << 32) | (transaction_index::BIGINT << 12) | operation_index::BIGINT as toid,
    contract_id,
    function_name,
    LENGTH(arguments_json) as args_len,
    successful
FROM contract_invocations_raw
ORDER BY ledger_sequence DESC
LIMIT 5;
```

### Check Latency (Bronze vs Silver)
```sql
-- Bronze max ledger
SELECT 'Bronze' as layer, MAX(ledger_sequence) as max_ledger
FROM operations_row_v2 WHERE type = 24 AND soroban_contract_id IS NOT NULL

UNION ALL

-- Silver max ledger
SELECT 'Silver' as layer, MAX(ledger_sequence) as max_ledger
FROM contract_invocations_raw;
```

---

## Monitor Logs

```bash
# Bronze ingester logs
nomad alloc logs -f $(nomad job allocs stellar-postgres-ingester | grep running | awk '{print $1}')

# Silver transformer logs
nomad alloc logs -f $(nomad job allocs silver-realtime-transformer | grep running | awk '{print $1}')

# Look for contract invocation processing
nomad alloc logs $(nomad job allocs stellar-postgres-ingester | grep running | awk '{print $1}') \
  | grep -i "contract\|invocation\|soroban"
```

---

## Common Issues & Fixes

**Issue: No soroban_arguments_json populated**
- Check: Operation is type 24 AND has soroban_contract_id AND soroban_function
- Fix: Verify Bronze ingester restarted after deployment

**Issue: Silver not populating contract_invocations_raw**
- Check: `SELECT * FROM realtime_transformer_checkpoint;`
- Fix: Ensure Silver transformer restarted and checkpoint is progressing

**Issue: Missing historical data**
- Current behavior: Only NEW data will have contract invocation fields
- Fix: Requires Bronze re-ingestion from earlier checkpoint (if needed)

---

## Environment Variables

Make sure these are set for your deployment:

**Bronze Ingester:**
```bash
DATABASE_URL=postgresql://stellar:password@host:5432/stellar_hot
NETWORK_PASSPHRASE="Test SDF Network ; September 2015"  # Or mainnet
```

**Silver Transformer:**
```bash
BRONZE_DB_URL=postgresql://stellar:password@bronze-host:5432/stellar_hot
SILVER_DB_URL=postgresql://stellar:password@silver-host:5432/silver_hot
```

---

## Success Criteria

✅ Bronze: `transaction_index` and `soroban_arguments_json` columns exist
✅ Bronze: New contract invocations have populated arguments
✅ Silver: `contract_invocations_raw` table exists with indexes
✅ Silver: Data flowing into contract_invocations_raw
✅ Latency: Silver < 10 ledgers behind Bronze
✅ Logs: No errors related to contract invocation processing

---

## Rollback

```bash
# Stop new services
nomad job stop stellar-postgres-ingester
nomad job stop silver-realtime-transformer

# Redeploy previous versions
nomad job run stellar-postgres-ingester-previous.nomad
nomad job run silver-realtime-transformer-previous.nomad

# Note: Database columns can stay (they're nullable, won't break old code)
```

---

See `DEPLOY_CONTRACT_INVOCATIONS.md` for full deployment guide.
See `TEST_CONTRACT_INVOCATIONS.sql` for comprehensive test queries.
