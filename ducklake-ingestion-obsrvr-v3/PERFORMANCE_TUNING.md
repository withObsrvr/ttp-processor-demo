# Performance Tuning Guide: Ingesting 60M+ Ledgers

## Current Performance Problem

**Observed:** Processing 1 ledger in ~270ms
- 60 million ledgers × 0.27s = **187 days** ⚠️

**Bottlenecks identified:**
1. `batch_size: 1` - Flushing after every ledger
2. Quality checks on every flush (~10-20ms overhead)
3. Multiple small appender flushes (13ms × 19 tables)
4. Single-threaded processing

## Optimization Levels

### Level 1: Increase Batch Size (IMMEDIATE - 10-20x speedup)

**Change:**
```yaml
ducklake:
  batch_size: 1000  # Was: 1
  commit_interval_seconds: 300  # Was: 5
```

**Expected improvement:**
- Flush overhead amortized over 1000 ledgers
- 270ms/ledger → ~50ms/ledger (estimated)
- **60M ledgers = ~35 days** (5.3x faster)

**Try progressively larger batches:**
- Start: 1000 ledgers
- Monitor memory, then try: 2000, 5000, 10000
- Sweet spot is usually 5000-10000 ledgers

### Level 2: Disable Quality Checks (5-10% speedup)

**Change:**
```yaml
quality:
  enabled: false  # Disable during backfill
```

**Expected improvement:**
- Saves ~10-20ms per flush
- Re-enable after backfill completes
- **~3-5 day reduction**

### Level 3: Use PostgreSQL Catalog for Multi-Worker (4-8x speedup)

**Problem:** DuckDB catalog = single writer only
**Solution:** PostgreSQL catalog supports concurrent writers

**Change config:**
```yaml
ducklake:
  # PostgreSQL catalog (multi-writer support)
  catalog_path: "ducklake:postgres:postgresql://user:pass@host:5432/obsrvr_lake_catalog"

  # Increase workers
  num_workers: 4  # Or 8, depending on CPU cores
```

**Expected improvement:**
- 4 workers = 4x throughput
- 8 workers = 6-7x throughput (diminishing returns)
- **35 days / 4 = ~9 days**

### Level 4: Optimize Source Throughput

**Check if stellar-live-source-datalake is the bottleneck:**

```bash
# Monitor gRPC source performance
# Look for slow ledger delivery
```

**Options:**
1. Use Galexie datastore directly (if available)
2. Run multiple ingestion ranges in parallel
3. Increase gRPC buffer sizes

## Recommended Configuration

### For DuckDB Catalog (Single Worker)

```yaml
ducklake:
  batch_size: 5000
  commit_interval_seconds: 600  # 10 minutes
  num_workers: 1

quality:
  enabled: false  # Disable during backfill
```

**Expected time:** ~20-30 days

### For PostgreSQL Catalog (Multi-Worker)

```yaml
ducklake:
  catalog_path: "ducklake:postgres:..."
  batch_size: 5000
  commit_interval_seconds: 600
  num_workers: 4

quality:
  enabled: false
```

**Expected time:** ~5-7 days

## Advanced: Parallel Range Processing

If you have PostgreSQL catalog, you can run **multiple ingestion processes** simultaneously:

**Process 1:** Ledgers 1 - 20M
```yaml
source:
  start_ledger: 1
  end_ledger: 20000000
```

**Process 2:** Ledgers 20M - 40M
```yaml
source:
  start_ledger: 20000001
  end_ledger: 40000000
```

**Process 3:** Ledgers 40M - 60M
```yaml
source:
  start_ledger: 40000001
  end_ledger: 60000000
```

**Expected time with 3 parallel processes:** ~2-3 days

## Monitoring Progress

Track ingestion rate:
```bash
# Watch logs for metrics
tail -f logs/ingestion.log | grep "ledgers/sec"

# Expected rates:
# - batch_size=1:    3-4 ledgers/sec (too slow)
# - batch_size=1000: 20-50 ledgers/sec (better)
# - batch_size=5000: 50-100 ledgers/sec (good)
# - 4 workers:       200-400 ledgers/sec (excellent)
```

## Memory Considerations

**Batch size vs memory:**
- 1000 ledgers ≈ 200-500 MB RAM
- 5000 ledgers ≈ 1-2 GB RAM
- 10000 ledgers ≈ 2-4 GB RAM

**If you hit OOM errors:**
1. Reduce batch_size
2. Increase system swap
3. Add more RAM

## After Backfill Completes

1. **Re-enable quality checks:**
   ```yaml
   quality:
     enabled: true
   ```

2. **Switch to live mode settings:**
   - Automatically uses `live_batch_size: 1`
   - Automatically uses `live_commit_interval_seconds: 5`

3. **Run quality validation:**
   ```sql
   SELECT * FROM _meta_quality WHERE status = 'FAILED';
   ```

## Performance Comparison Table

| Configuration | Ledgers/sec | 60M Ledgers | Notes |
|--------------|-------------|-------------|-------|
| Current (batch=1) | 3-4 | 187 days | ❌ Too slow |
| batch=1000 | 20-40 | 35 days | ✅ Better |
| batch=5000 | 50-100 | 14 days | ✅ Good |
| batch=5000 + PostgreSQL (4 workers) | 200-400 | 3-7 days | ✅✅ Excellent |
| Parallel ranges (3 processes) | 600-1200 | 1-3 days | ✅✅✅ Optimal |

## Recommended Approach

**Phase 1: Quick Test**
1. Use `mainnet-bulk-backfill.secret.yaml` config
2. Test with 100K ledgers to validate throughput
3. Measure ledgers/sec

**Phase 2: Optimize**
1. If < 50 ledgers/sec: Consider PostgreSQL catalog
2. If > 50 ledgers/sec: Increase batch_size to 5000-10000
3. Monitor memory usage

**Phase 3: Full Backfill**
1. Run full backfill with optimized settings
2. Monitor progress daily
3. Estimate completion time

**Phase 4: Switch to Live**
1. Backfill completes, automatically switches to live mode
2. Uses live_batch_size=1, live_commit_interval_seconds=5
3. Re-enable quality checks
