# V2 Quick Start Guide

## Running V2 (Same as V1 workflow)

### Mainnet:
```bash
cd /home/tillman/Documents/ttp-processor-demo/ducklake-ingestion-obsrvr-v2
./ducklake-ingestion-obsrvr -config config/mainnet-large-ingest.secret.yaml
```

### Testnet:
```bash
cd /home/tillman/Documents/ttp-processor-demo/ducklake-ingestion-obsrvr-v2
./ducklake-ingestion-obsrvr -config config/testnet-large-ingest.secret.yaml
```

**That's it!** Same command structure as V1, but 534x faster.

---

## What Changed from V1?

### 1. **Binary Location**:
- V1: `/home/tillman/Documents/ttp-processor-demo/ducklake-ingestion-obsrvr/`
- V2: `/home/tillman/Documents/ttp-processor-demo/ducklake-ingestion-obsrvr-v2/`

### 2. **Performance**:
- V1: 126 seconds for 25 mainnet ledgers
- V2: **0.236 seconds** for 25 mainnet ledgers (534x faster!)
- V2: **~1 second** for 100 mainnet ledgers (4x more data, 126x faster!)

### 3. **Batch Sizes**:
| Network | V1 | V2 | Improvement |
|---------|----|----|-------------|
| Mainnet | 25 ledgers | 100 ledgers | 4x larger batches |
| Testnet | 200 ledgers | 200 ledgers | Same (already optimal) |

### 4. **Everything Else is Identical**:
- ✅ Same config file format
- ✅ Same PostgreSQL catalog
- ✅ Same Parquet storage
- ✅ Same quality checks (19 checks)
- ✅ Same metadata tracking
- ✅ Same Obsrvr Data Culture compliance

---

## Building from Source (Optional)

### Regular build:
```bash
cd /home/tillman/Documents/ttp-processor-demo/ducklake-ingestion-obsrvr-v2/go
GOWORK=off go build -o ../ducklake-ingestion-obsrvr
```

### Nix build:
```bash
cd /home/tillman/Documents/ttp-processor-demo/ducklake-ingestion-obsrvr-v2
nix build
./result/bin/ducklake-ingestion-obsrvr-v2 -config config/mainnet-large-ingest.secret.yaml
```

### Nix development shell:
```bash
cd /home/tillman/Documents/ttp-processor-demo/ducklake-ingestion-obsrvr-v2
nix develop
# Now in shell with DuckDB 1.4.1, Go 1.24, etc.
```

---

## Monitoring V2 Performance

Look for these log patterns:

### Good Performance (Mainnet, 100 ledgers):
```
[FLUSH] Starting multi-table flush: 100 ledgers, 26500 transactions, 83800 operations, 28650 balances
[QUALITY] Completed 19 quality checks in 30ms
[FLUSH] V2: Appending 100 ledgers via Appender API...
[FLUSH] ✓ V2: Appended and flushed 100 ledgers in 56ms
[FLUSH] V2: Appending 26500 transactions via Appender API...
[FLUSH] ✓ V2: Appended and flushed 26500 transactions in 145ms
[FLUSH] V2: Appending 83800 operations via Appender API...
[FLUSH] ✓ V2: Appended and flushed 83800 operations in 480ms
[FLUSH] V2: Appending 28650 native_balances via Appender API...
[FLUSH] ✓ V2: Appended and flushed 28650 native_balances in 61ms
✅ Quality Checks: 18 passed, 1 FAILED (liabilities_validation - known mainnet data issue)
[FLUSH] ✅ COMPLETE: Flushed 100 ledgers in ~1.2s total
```

**Expected**: Total flush < 2 seconds for 100 mainnet ledgers

### Good Performance (Testnet, 200 ledgers):
```
[FLUSH] ✅ COMPLETE: Flushed 200 ledgers, 433 transactions, 577 operations, 614 balances in 75ms total
```

**Expected**: Total flush ~75-80ms for 200 testnet ledgers

---

## Switching Between V1 and V2

### To use V1:
```bash
cd /home/tillman/Documents/ttp-processor-demo/ducklake-ingestion-obsrvr
./ducklake-ingestion-obsrvr -config config/mainnet-large-ingest.secret.yaml
```

### To use V2:
```bash
cd /home/tillman/Documents/ttp-processor-demo/ducklake-ingestion-obsrvr-v2
./ducklake-ingestion-obsrvr -config config/mainnet-large-ingest.secret.yaml
```

⚠️ **Important**: Do NOT run both simultaneously on same catalog!

---

## Verifying Data

### Check row counts:
```bash
psql -h /home/tillman/Documents/obsrvr-console/.postgres-sockets ducklake_obsrvr_unified

-- Mainnet
SELECT COUNT(*) FROM obsrvr_mainnet_unified.mainnet.ledgers_row_v2;
SELECT COUNT(*) FROM obsrvr_mainnet_unified.mainnet.transactions_row_v2;

-- Testnet
SELECT COUNT(*) FROM obsrvr_test_unified.testnet.ledgers_row_v2;
SELECT COUNT(*) FROM obsrvr_test_unified.testnet.transactions_row_v2;
```

### Check latest ledger:
```sql
SELECT MAX(sequence) FROM obsrvr_mainnet_unified.mainnet.ledgers_row_v2;
```

### Check metadata:
```sql
-- See all datasets
SELECT * FROM obsrvr_mainnet_unified.mainnet._meta_datasets;

-- See latest lineage
SELECT * FROM obsrvr_mainnet_unified.mainnet._meta_lineage ORDER BY created_at DESC LIMIT 10;

-- See quality checks
SELECT * FROM obsrvr_mainnet_unified.mainnet._meta_quality ORDER BY created_at DESC LIMIT 10;
```

---

## Performance Expectations

### Mainnet (100 ledgers per batch):
- **Throughput**: 20-40 ledgers/sec
- **Flush time**: < 2 seconds
- **Per ledger**: ~25-50ms (vs 5,040ms in V1!)

### Testnet (200 ledgers per batch):
- **Throughput**: 40-50 ledgers/sec
- **Flush time**: ~75ms
- **Per ledger**: ~2-3ms (vs ~500ms in V1!)

### Why Throughput is "Only" 20-40 ledgers/sec:
The bottleneck is now **network streaming** from stellar-live-source-datalake, not processing! V2 processes so fast that it's waiting for data most of the time.

In V1, processing was the bottleneck (120 seconds flush time).
In V2, network is the bottleneck (flush in milliseconds).

---

## Documentation

All docs in `docs/`:
- `DAY1_POC_RESULTS.md` - Initial validation
- `DAY2-3_PERFORMANCE_RESULTS.md` - Testnet benchmarks
- `MAINNET_VALIDATION.md` - 534x speedup validation
- `TECHNICAL_HANDOFF.md` - Complete technical guide

---

## Need Help?

Check:
1. `docs/TECHNICAL_HANDOFF.md` - Complete guide
2. `docs/MAINNET_VALIDATION.md` - Performance validation
3. `config/README.md` - Config file documentation

---

## Summary

✅ **V2 is a drop-in replacement for V1**
- Same command structure
- Same config format
- Same data output
- 534x faster!

Just change directory from `ducklake-ingestion-obsrvr` to `ducklake-ingestion-obsrvr-v2` and run the same commands.
