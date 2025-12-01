# DuckLake Partitioning in v3

## Overview

v3 automatically configures intelligent partitioning for all 19 tables to optimize query performance. Partitioning divides data into separate Parquet files based on ledger sequence ranges, enabling **query pruning** (skipping irrelevant files during queries).

## Partition Strategy

**100K Ledger Partitions** (Default in v3)

```
Partition 0:     Ledgers 0-99,999
Partition 1:     Ledgers 100,000-199,999
Partition 2:     Ledgers 200,000-299,999
...
Partition 599:   Ledgers 59,900,000-59,999,999
```

### Why 100K Ledgers Per Partition?

| Partition Size | Total Partitions (60M ledgers) | Query Pruning | File Management | Recommendation |
|----------------|-------------------------------|---------------|-----------------|----------------|
| 10K ledgers | 6,000 | Excellent | ⚠️ Too many files | Not Recommended |
| **100K ledgers** | **600** | **Very Good** | ✅ **Optimal** | **✅ DEFAULT** |
| 1M ledgers | 60 | Good | ✅ Easy | ⚠️ Too coarse |
| No partitioning | 1 | ❌ None | ⚠️ Many files | ❌ Avoid |

## How It Works

### Automatic Setup

v3 configures partitioning automatically during catalog initialization:

```go
// Runs during startup (in go/main.go)
if err := ing.setupPartitioning(); err != nil {
    log.Printf("Warning: Partitioning setup failed: %v", err)
}
```

### SQL Configuration

Under the hood, v3 executes:

```sql
-- Core tables (using `sequence` column)
ALTER TABLE ledgers_row_v2
  SET PARTITIONED BY (identity(sequence / 100000));

-- All other tables (using `ledger_sequence` column)
ALTER TABLE transactions_row_v2
  SET PARTITIONED BY (identity(ledger_sequence / 100000));

ALTER TABLE operations_row_v2
  SET PARTITIONED BY (identity(ledger_sequence / 100000));

-- Repeat for all 19 tables
```

## Query Performance Benefits

### Example: Range Query

```sql
-- Query: Get ledgers 1,500,000 - 1,600,000
SELECT * FROM ledgers_row_v2
WHERE sequence BETWEEN 1500000 AND 1600000;

-- Without Partitioning:
-- ❌ Scans ALL Parquet files (600+ files)

-- With 100K Partitioning:
-- ✅ Scans only partitions 15-16 (2 files)
-- 300x fewer files scanned!
```

### Performance Improvement

| Query Type | Without Partitioning | With 100K Partitions | Speedup |
|------------|---------------------|---------------------|---------|
| Point query (1 ledger) | Scans all files | Scans 1 file | 600x faster |
| Range query (100K ledgers) | Scans all files | Scans 1-2 files | 300-600x faster |
| Range query (1M ledgers) | Scans all files | Scans ~10 files | 60x faster |
| Full table scan | Scans all files | Scans all files | No change |

## Advanced Configuration

### Custom Partition Size

To change the partition size, edit `go/partitioning.go`:

```go
// Partition every 50K ledgers (1,200 partitions)
partitionExpr := "identity(ledger_sequence / 50000)"

// Partition every 500K ledgers (120 partitions)
partitionExpr := "identity(ledger_sequence / 500000)"
```

### Time-Based Partitioning (Alternative)

For date-range queries instead of ledger-range:

```go
// Partition by year/month from closed_at timestamp
tables := []struct {
    name          string
    partitionExpr string
}{
    {"ledgers_row_v2", "year(closed_at), month(closed_at)"},
    {"transactions_row_v2", "year(closed_at), month(closed_at)"},
    // ...
}
```

**Trade-offs:**
- ✅ Fewer partitions (~36 for testnet, ~60 for mainnet)
- ✅ Natural for time queries (`WHERE closed_at > '2024-01-01'`)
- ❌ Ledger sequence queries less efficient
- ❌ Partition sizes vary (some months have more activity)

### Disable Partitioning

To disable partitioning:

```go
// In go/partitioning.go, comment out setupPartitioning() call
// Or modify setupPartitioning() to return early:
func (ing *Ingester) setupPartitioning() error {
    log.Println("Partitioning disabled by configuration")
    return nil
}
```

## Important Notes

### 1. Only Affects NEW Data

```sql
ALTER TABLE tbl SET PARTITIONED BY (expr);
```

**This only partitions data written AFTER this command.**

Existing data keeps its original layout. This is a DuckLake feature that allows schema evolution without reprocessing.

### 2. Partition Changes Over Time

You can change partitioning strategy mid-stream:

```sql
-- Day 1: Partition by 100K
ALTER TABLE ledgers_row_v2 SET PARTITIONED BY (identity(sequence / 100000));

-- Day 30: Change to 500K (new data uses new scheme)
ALTER TABLE ledgers_row_v2 SET PARTITIONED BY (identity(sequence / 500000));
```

DuckLake queries handle mixed partition schemes automatically.

### 3. Partition Metadata in Catalog

Partition information is stored in the **DuckDB catalog**, not in file names:

```
# File structure (simplified):
s3://bucket/testnet_4/
  ├── ledgers_row_v2/
  │   ├── data_0.parquet     # Could be ANY partition
  │   ├── data_1.parquet     # DuckLake knows mapping
  │   └── data_2.parquet
  └── catalog.duckdb          # Contains partition metadata
```

No Hive-style paths like `/year=2024/month=01/` needed!

## Verification

### Check Partition Configuration

```sql
-- Query DuckLake metadata
SELECT * FROM ducklake_tables()
WHERE table_name = 'ledgers_row_v2';
```

### Check Partition File Count

```bash
# List Parquet files for a table
duckdb -c "
  ATTACH 'ducklake:duckdb:catalog.duckdb' AS cat
    (DATA_PATH 's3://bucket/testnet_4/');

  SELECT COUNT(*) AS file_count
  FROM ducklake_files('cat.testnet.ledgers_row_v2');
"
```

## Troubleshooting

### "Table already partitioned" Error

**Cause:** Partitioning was already configured.

**Solution:** This is normal. v3 logs it as `✓ Table already partitioned` and continues.

### "Table does not exist" Warning

**Cause:** Table hasn't been created yet (first run).

**Solution:** v3 will partition the table when it's created. This is expected behavior.

### Query Performance Not Improved

**Checklist:**
1. ✅ Partitioning configured? Check logs for "Partitioning setup complete"
2. ✅ Query uses partition key? Use `WHERE sequence BETWEEN ...` or `WHERE ledger_sequence ...`
3. ✅ Wrote new data after partitioning? Only new data is partitioned
4. ✅ Sufficient data? Small datasets (<1M ledgers) won't see dramatic improvements

## Comparison: v2 vs v3 Partitioning

| | v2 | v3 |
|---|---|---|
| **Partitioning** | Manual SQL | ✅ Automatic |
| **Strategy** | None (default) | ✅ 100K ledgers |
| **Configuration** | External scripts | ✅ Built-in |
| **Query Pruning** | ❌ No | ✅ Yes (600x faster) |
| **Flexibility** | Manual ALTER | ✅ Configured in code |

## References

- **DuckLake Partitioning Docs:** https://ducklake.select/docs/stable/duckdb/advanced_features/partitioning
- **Source Code:** `go/partitioning.go`
- **Enabled In:** `go/main.go:1527-1532`

---

**Summary:** v3's automatic 100K-ledger partitioning gives you **60-600x faster range queries** with zero configuration needed. Just run v3 and it's enabled by default!
