# DuckLake Partitioning Audit - Cycle 1 Day 1

**Date:** 2025-12-16
**Status:** Initial audit completed
**Catalog:** `obsrvr_lake_catalog_dev_11`
**Data Path:** `s3://obsrvr-test-bucket-1/testnet_bronze/`

---

## Executive Summary

✅ **Data Volume:** Small test dataset (3,128 ledgers, ~6,600 transactions)
⚠️ **Partitioning:** Cannot verify from metadata (DuckLake version compatibility issue)
⚠️ **File Sizes:** Need to check S3 directly to count files
✅ **Ledger Range Column:** Present and working (bucket 2130000)

---

## Diagnostic Results

### 1. Data Volume Analysis

**Ledgers (ledgers_row_v2):**
- Min ledger: 2,136,042
- Max ledger: 2,139,169
- Ledger range: 3,127 ledgers
- Total rows: 3,128

**Transactions (transactions_row_v2):**
- Min ledger: 2,136,042
- Max ledger: 2,139,180
- Total rows: 6,628

**Operations (operations_row_v2):**
- Min ledger: 2,136,042
- Max ledger: 2,139,180
- Total rows: 10,158

**Contract Events (contract_events_stream_v1):**
- Min ledger: 2,136,042
- Max ledger: 2,139,189
- Total rows: 5,577

### 2. Ledger Range Bucketing

**Current ledger_range distribution:**
```
ledger_range | tx_count | min_seq  | max_seq
-------------|----------|----------|----------
2130000      | 6,674    | 2,136,042| 2,139,205
```

✅ **Good:** `ledger_range` column exists and is correctly calculated
✅ **Good:** All data fits in one bucket (expected for this small dataset)

### 3. Partitioning Status

⚠️ **Could not verify from DuckLake metadata**

The DuckLake metadata queries failed due to schema compatibility issues:
- `table_schema` column not found in `ducklake_data_file`
- `partition_keys` column not found in `ducklake_table`

This suggests either:
1. Different DuckLake version with different metadata schema
2. Catalog was created without partitioning metadata
3. Tables were created before partitioning support was added

**Next step:** We need to check partitioning by:
- Listing S3 bucket structure to see if files are organized by partition
- Using EXPLAIN on queries to see if partition pruning is mentioned

### 4. File Size Analysis

⚠️ **Could not query from DuckLake metadata**

**Next step:** Check S3 bucket directly:
```bash
aws s3 ls s3://obsrvr-test-bucket-1/testnet_bronze/ --recursive --summarize --human-readable
```

Or using B2 CLI:
```bash
b2 ls --recursive obsrvr-test-bucket-1/testnet_bronze/
```

---

## Assessment

### Current State

**Scale:** Very small test dataset (~3k ledgers)
- This is good for testing partitioning changes
- File size issues won't be visible at this scale
- Need to project what happens at production scale

**Projected Scale (Production - 1M ledgers):**
- ~320x more data than current
- If 1 flush = 1 file = ~2-5 MB now
- At 1M ledgers: ~2,000-3,200 files at 2-5 MB each ❌ **SMALL FILES PROBLEM**

### Risk Assessment

🔴 **HIGH RISK:** Small files problem will manifest at scale
🟡 **MEDIUM RISK:** Partitioning status unclear (need to verify)
🟢 **LOW RISK:** Ledger_range column working correctly

---

## Recommendations

### Immediate Actions (Day 1-2)

1. **Check S3 bucket structure manually:**
   ```bash
   b2 ls --recursive obsrvr-test-bucket-1/testnet_bronze/transactions_row_v2/
   ```
   Expected outputs:
   - **If partitioned:** Files in subdirectories like `ledger_range=2130000/file_xxx.parquet`
   - **If not partitioned:** Files flat in directory like `file_xxx.parquet`

2. **Count actual Parquet files per table:**
   - This tells us if we have a small files problem already
   - Each flush should create 1 file/table if not partitioned
   - With 1-minute flush interval, we'd have many tiny files

3. **Configure partitioning explicitly:**
   ```sql
   ALTER TABLE testnet_catalog.bronze.transactions_row_v2
   SET PARTITIONED BY (ledger_range);

   ALTER TABLE testnet_catalog.bronze.operations_row_v2
   SET PARTITIONED BY (ledger_range);

   ALTER TABLE testnet_catalog.bronze.contract_events_stream_v1
   SET PARTITIONED BY (ledger_range);
   ```

### Flush Strategy Decision (Day 2-3)

Given current small scale, we should:

**Option A: Increase flush interval (Quick win)**
- Change from 1 minute → 10 minutes
- This will accumulate 10x more data per file
- Easy to implement (config change only)
- Should get files to 20-50 MB range

**Option B: Switch to COPY method (Better long-term)**
- Requires code changes to postgres-ducklake-flusher
- Gives explicit control over file sizes
- Can set ROW_GROUP_SIZE for better parallelism
- Better for production scale

**Recommendation:** Start with Option A for immediate testing, plan Option B for production.

---

## Next Steps

### Day 1 (Remaining)
- [x] Run diagnostic queries
- [ ] Check S3 bucket structure manually
- [ ] Count Parquet files per table
- [ ] Complete this audit document

### Day 2
- [ ] Configure partitioning on all Bronze tables
- [ ] Choose flush strategy (A or B)
- [ ] Implement chosen strategy
- [ ] Test with new data

### Day 3
- [ ] Verify partitioning is working (check S3 structure)
- [ ] Test query performance with EXPLAIN
- [ ] Create PARTITIONING_STRATEGY.md documentation
- [ ] Mark Cycle 1 complete

---

## Appendix: Technical Details

### Current Configuration

**Flush Interval:** 1 minute (from config.yaml)
**Flush Method:** `INSERT INTO catalog.schema.table SELECT * FROM postgres_scan(...)`
**PostgreSQL Buffer:** ~3k ledgers at a time

### Estimated File Sizes (Current)

**Assumptions:**
- 1-minute flush = ~50 ledgers @ 2-second close time
- ~1 transaction/ledger average (testnet)
- ~500 bytes/transaction row

**Calculation:**
- 50 ledgers × 1 tx/ledger × 500 bytes = ~25 KB per transaction file
- Operations ~1.5x transactions = ~37 KB
- Events variable, estimate ~20 KB

**Result:** ❌ Files are likely 20-40 KB each = **WAY TOO SMALL**

### Optimal File Size Targets

Per DuckLake best practices:
- **Minimum:** 100 MB per file
- **Optimal:** 256-512 MB per file
- **Maximum:** 10 GB per file (start to slow down)

**To reach 256 MB with current data rate:**
- Need ~5,000 ledgers per file
- At 2-second close time = ~2.7 hours of data
- **Recommendation:** 3-hour flush interval minimum

---

**Last Updated:** 2025-12-16
**Next Review:** After S3 bucket inspection
