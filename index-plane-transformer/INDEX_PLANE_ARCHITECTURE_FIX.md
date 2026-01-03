# Index Plane Architecture Fix - Shape Up Pitch

**Problem Statement**: Index Plane Transformer is reading from Bronze Cold (3+ hour lag) and bypassing DuckLake catalog, making it effectively useless for real-time transaction hash lookups.

**Current State**: ❌ BROKEN
```
Stellar → PostgreSQL Hot → [3 hour wait] → Bronze Cold (Parquet) → Index Plane
          stellar_hot       postgres-ducklake-flusher    ↑
                                                    index-plane-transformer
```
**Result**: Transaction hash index lags by 3+ hours

**Target State**: ✅ CORRECT
```
Stellar → PostgreSQL Hot → Index Plane DuckLake
          stellar_hot       ↑
                      index-plane-transformer (30s polling)
```
**Result**: Transaction hash index lags by ~30 seconds

---

## Appetite

**Time Budget**: 4-6 hours (1 day max)

This is a straightforward refactor following the existing `silver-realtime-transformer` pattern.

---

## Solution - Fat Marker Sketch

### Read from Bronze Hot (PostgreSQL stellar_hot)

Instead of:
```go
// WRONG: Reading from Bronze Cold Parquet files
tablePath := fmt.Sprintf("s3://%s/%s/*.parquet", bucket, table)
query := fmt.Sprintf("SELECT * FROM read_parquet('%s')", tablePath)
```

Do this:
```go
// CORRECT: Reading from Bronze Hot PostgreSQL
query := `
    SELECT
        hash as tx_hash,
        ledger_sequence,
        application_order,
        operation_count,
        successful,
        closed_at
    FROM transactions
    WHERE ledger_sequence > $1 AND ledger_sequence <= $2
    ORDER BY ledger_sequence, application_order
`
```

### Write to Index Plane DuckLake (Cold Storage)

Keep the existing Index Writer - it's correct:
- Writes to DuckLake catalog: `testnet_catalog.index.tx_hash_index`
- Stores data in partitioned Parquet on B2
- Uses proper DuckLake metadata tracking

### Use PostgreSQL for Both Source and Checkpoint

```yaml
bronze_hot:
  host: private-obsrvr-lake-hot-buffer-prod-do-user-13721579-0.g.db.ondigitalocean.com
  port: 25060
  database: stellar_hot
  user: doadmin
  password: ${PG_HOT_PASSWORD}
  sslmode: require

checkpoint:
  host: private-obsrvr-lake-catalog-prod-do-user-13721579-0.g.db.ondigitalocean.com
  port: 25060
  database: obsrvr_lake_catalog_prod
  user: doadmin
  password: ${CATALOG_PASSWORD}
  table: index.transformer_checkpoint
```

---

## Scope Lines

### MUST HAVE (Core Fix)
- Replace DuckDB `SilverReader` with PostgreSQL `BronzeHotReader`
- Read from `stellar_hot.transactions` table (Bronze Hot)
- Keep existing `IndexWriter` (writes to DuckLake - this is correct)
- Update config structure to match `silver-realtime-transformer` pattern
- Update Nomad job template with PostgreSQL connection

### NICE TO HAVE
- Add connection pooling for PostgreSQL
- Add retry logic for transient PostgreSQL failures
- Better logging for PostgreSQL query performance

### COULD HAVE (Future Work)
- Metrics on PostgreSQL query latency
- Adaptive batch sizing based on data volume
- Multi-threaded processing for high-volume periods

---

## Rabbit Holes to Avoid

### ❌ DON'T try to read from both Bronze Hot AND Bronze Cold
Just read from Hot. Keep it simple.

### ❌ DON'T change the Index Writer
The Index Writer correctly uses DuckLake. Only the Reader needs fixing.

### ❌ DON'T worry about backfilling historical data yet
Focus on real-time indexing first. Historical backfill is a separate cycle.

### ❌ DON'T add complex caching or optimization
PostgreSQL → DuckLake is fast enough. Ship the simple version.

---

## No-Gos (Explicitly Out of Scope)

- Historical backfill of existing transactions
- Index Plane query API (that's a future cycle)
- Partition optimization or compaction
- Deduplication logic (transactions table has unique hashes per ledger)
- Performance tuning beyond basic batching

---

## Done Looks Like

### Success Criteria

When this cycle is complete:

1. **Code compiles and runs**
   ```bash
   cd index-plane-transformer
   go build ./go
   ./index-plane-transformer -config config.yaml
   # Outputs: "Connected to PostgreSQL stellar_hot"
   ```

2. **Reads from Bronze Hot (PostgreSQL)**
   ```
   2026/01/02 15:00:00 📊 Reading transactions from stellar_hot
   2026/01/02 15:00:00 📊 Found 1,234 transactions in ledgers 100000-110000
   2026/01/02 15:00:01 ✅ Indexed 1,234 transactions
   ```

3. **Writes to Index Plane DuckLake**
   ```bash
   # Check catalog
   psql $CATALOG_DB -c "SELECT COUNT(*) FROM index_meta.ducklake_data_file WHERE table_id = ..."
   # Returns: 1 (new parquet file created)

   # Check B2
   b2 ls b2://obsrvr-prod-testnet-index/tx_hash_index/
   # Shows: ledger_range=1/part_1735826500.parquet
   ```

4. **Near real-time indexing (<60 second lag)**
   ```
   Last ingestion: 15:00:00
   Last index:     15:00:30
   Lag:            30 seconds ✅
   ```

5. **Checkpoint saves correctly**
   ```sql
   SELECT * FROM index.transformer_checkpoint;
   --  id | last_ledger_sequence | last_processed_at
   --  1  | 110000              | 2026-01-02 15:00:30
   ```

### Not Done Until

- [ ] Config structure matches `silver-realtime-transformer`
- [ ] Reads from `stellar_hot.transactions` via PostgreSQL driver
- [ ] Writes to `testnet_catalog.index.tx_hash_index` via DuckDB
- [ ] Checkpoint saves to catalog PostgreSQL
- [ ] Nomad job deploys successfully
- [ ] Health endpoint shows <60s lag
- [ ] Index files appear in B2 within 1 minute of new transactions

---

## Hill Chart Tracking

### Left Side (Figuring Things Out) - 0-50%

- [ ] **10%** - Read silver-realtime-transformer code to understand pattern
- [ ] **20%** - Design new config structure for PostgreSQL + DuckLake
- [ ] **30%** - Prototype BronzeHotReader with PostgreSQL
- [ ] **40%** - Test reading from stellar_hot.transactions
- [ ] **50%** - Confirm query returns correct data

### Right Side (Making It Happen) - 50-100%

- [ ] **60%** - Replace SilverReader with BronzeHotReader in transformer
- [ ] **70%** - Update config.yaml and config.go
- [ ] **80%** - Update Nomad job template with PostgreSQL env vars
- [ ] **90%** - Deploy to production and verify logs
- [ ] **100%** - Confirm index files in B2 + checkpoint saves

---

## Risks and Mitigations

### Risk 1: Transactions table schema mismatch
**Impact**: Medium
**Mitigation**: Check schema before starting. The `stellar_hot.transactions` table should have:
- `hash` (VARCHAR) - transaction hash
- `ledger_sequence` (BIGINT)
- `application_order` (INTEGER)
- `successful` (BOOLEAN)
- `closed_at` (TIMESTAMP)

### Risk 2: PostgreSQL connection limit
**Impact**: Low
**Mitigation**: Use a single connection, not connection pool. Same pattern as silver-realtime-transformer.

### Risk 3: Query performance on large batches
**Impact**: Low
**Mitigation**: Start with 10,000 ledger batch size (existing). Monitor and adjust if needed.

### Risk 4: Breaking existing deployment
**Impact**: Low
**Mitigation**: This is a new service still in testing. No production dependencies yet.

---

## Cool-Down Recommendations

After completing this cycle, take 1 day to:

1. **Monitor production** - Watch logs for 24 hours to ensure stability
2. **Document learnings** - Update README with architecture diagram
3. **Explore optimizations** - Test different batch sizes, measure query latency
4. **Plan next cycle** - Query API for transaction hash lookups

---

## Related Work

This fix enables:
- **Next cycle**: Index Plane Query API (fast tx_hash → ledger lookups)
- **Future**: Historical backfill to index existing transactions
- **Future**: Index Plane for other lookup patterns (account, asset, etc.)

---

## Technical Details Reference

### Example PostgreSQL Query

```sql
-- Read transactions from Bronze Hot
SELECT
    hash as tx_hash,
    ledger_sequence,
    application_order,
    operation_count,
    successful,
    closed_at,
    FLOOR(ledger_sequence / 100000) as ledger_range
FROM transactions
WHERE ledger_sequence > 100000
  AND ledger_sequence <= 110000
ORDER BY ledger_sequence, application_order;
```

### Example DuckDB Write (Keep This - It's Correct)

```sql
-- Write to Index Plane DuckLake
COPY temp_tx_index TO 's3://obsrvr-prod-testnet-index/tx_hash_index/ledger_range=1/part_1735826500.parquet'
(FORMAT PARQUET, COMPRESSION 'snappy');
```

### Files to Modify

1. `go/config.go` - Change config structure
2. `go/bronze_reader.go` - NEW: Replace silver_reader.go with PostgreSQL version
3. `go/transformer.go` - Update to use BronzeHotReader
4. `go/main.go` - Update initialization
5. `config.yaml` - Update with PostgreSQL connection details
6. `.nomad/index-plane-transformer.nomad` - Update template with PG env vars

### Files to Keep

- `go/index_writer.go` - ✅ Correct! Writes to DuckLake
- `go/checkpoint.go` - ✅ Correct! Uses PostgreSQL catalog
- `go/health.go` - ✅ Correct! No changes needed
- `go/types.go` - ✅ Correct! TransactionIndex struct is good

---

**Ready to ship!** This is a clear, focused refactor following an existing pattern.
