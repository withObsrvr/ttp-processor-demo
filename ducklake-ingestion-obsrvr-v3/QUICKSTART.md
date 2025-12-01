# Quick Start Guide

## Two-Phase Ingestion Strategy

### Phase 1: Backfill Historical Data

**Goal:** Ingest ledgers 60,000,000 - 60,100,000 as fast as possible

**Config:** `config/mainnet-bulk-backfill.secret.yaml`

**Settings:**
- `batch_size: 5000` - Large batches for speed
- `end_ledger: 60100000` - Stops at this ledger
- `quality.enabled: false` - Disabled for speed

**Run:**
```bash
./ducklake-ingestion-obsrvr-v3 -config config/mainnet-bulk-backfill.secret.yaml
```

**Expected Output:**
```
Starting ingestion from ledger 60000000
Processed 5000 ledgers (50 ledgers/sec), current: 60005000
Processed 10000 ledgers (50 ledgers/sec), current: 60010000
...
Reached end ledger 60100000, stopping ingestion
✅ COMPLETE: Flushed X ledgers...
```

**Time:** 100K ledgers ≈ 20-30 minutes (at 50-100 ledgers/sec)

---

### Phase 2: Live Ingestion

**Goal:** Continuously ingest new ledgers from 60,100,001 onward

**Before starting:**
1. Update `start_ledger` in config to match where backfill stopped + 1
2. Verify backfill completed successfully

**Config:** `config/mainnet-live.secret.yaml`

**Settings:**
- `live_batch_size: 1` - Low latency
- `end_ledger: 0` - Never stop
- `live_mode: true` - Poll for new ledgers
- `quality.enabled: true` - Enable quality checks

**Run:**
```bash
./ducklake-ingestion-obsrvr-v3 -config config/mainnet-live.secret.yaml
```

**Expected Output:**
```
Starting ingestion from ledger 60100001
[BACKFILL] Processed 100 ledgers (catch-up phase)
🔴 LIVE MODE: Starting continuous ingestion from ledger 60100101
  Poll interval: 5s
  Batch size: 1 ledgers
  Commit interval: 5s
✅ [LIVE] Processed ledger 60100102 (buffered, 1/1)
✅ [LIVE] Flushed 1 ledgers (latest: 60100102)
```

**Runs forever** - Press Ctrl+C to stop

---

## Quick Reference

| Phase | Config File | batch_size | Stops? | Quality Checks |
|-------|-------------|------------|--------|----------------|
| Backfill | mainnet-bulk-backfill.secret.yaml | 5000 | Yes (at end_ledger) | Disabled |
| Live | mainnet-live.secret.yaml | 1 | No | Enabled |

---

## Next Steps

1. **Start backfill** with `mainnet-bulk-backfill.secret.yaml`
2. **Monitor progress** - calculate estimated completion time
3. **When backfill completes** - switch to `mainnet-live.secret.yaml`
4. **Run live mode** indefinitely for real-time data

For more details, see:
- `PERFORMANCE_TUNING.md` - Optimize for speed
- `INGESTION_MODES.md` - Understand backfill vs live modes
