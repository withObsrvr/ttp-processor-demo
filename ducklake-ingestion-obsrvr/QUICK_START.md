# DuckLake Query Quick Start

## ✅ Yes, You're Using DuckLake!

Your processor writes to **DuckLake v0.3** with:
- **PostgreSQL catalog**: Tracks 20 tables, schemas, versions
- **Parquet data**: All data stored in `./obsrvr_test_data/`
- **Version**: 0.3 (created by DuckDB v1.4.1)

## 🚀 Quick Commands

### Query Through Catalog (Recommended)

```bash
cd ~/Documents/ttp-processor-demo/ducklake-ingestion-obsrvr

# Interactive SQL
./query.sh

# Quick query
./query.sh "SELECT type_string, COUNT(*) FROM obsrvr_test.core.operations_row_v1 GROUP BY type_string ORDER BY COUNT(*) DESC"

# Run saved query
./query.sh catalog_stats.sql
```

### Available Pre-built Queries

```bash
# Daily stats (Discord bot format)
./query.sh catalog_stats.sql

# Operation types breakdown
./query.sh "SELECT type_string, COUNT(*) as count FROM obsrvr_test.core.operations_row_v1 GROUP BY type_string ORDER BY count DESC"

# Quality check summary
./query.sh "SELECT check_name, SUM(CASE WHEN passed THEN 1 ELSE 0 END) as passed, SUM(CASE WHEN NOT passed THEN 1 ELSE 0 END) as failed FROM obsrvr_test.core._meta_quality GROUP BY check_name ORDER BY failed DESC"
```

## 📊 Example Queries

### Count Soroban Transactions

```sql
SELECT COUNT(DISTINCT transaction_hash) as soroban_tx
FROM obsrvr_test.core.operations_row_v1
WHERE type_string = 'OperationTypeInvokeHostFunction';
```

### Daily Transaction Stats

```sql
SELECT
    DATE(closed_at) as date,
    SUM(transaction_count) as total_tx,
    SUM(operation_count) as total_ops
FROM obsrvr_test.core.ledgers_row_v2
GROUP BY date
ORDER BY date DESC;
```

### Top Payment Destinations

```sql
SELECT
    payment_to,
    COUNT(*) as payment_count,
    SUM(payment_amount) / 10000000.0 as total_xlm
FROM obsrvr_test.core.operations_row_v1
WHERE type_string = 'OperationTypePayment'
  AND payment_to IS NOT NULL
GROUP BY payment_to
ORDER BY total_xlm DESC
LIMIT 10;
```

## 🔍 Available Tables

Query through catalog with clean names:

```sql
-- Data tables
obsrvr_test.core.ledgers_row_v2
obsrvr_test.core.transactions_row_v1
obsrvr_test.core.operations_row_v1
obsrvr_test.core.native_balances_snapshot_v1

-- Metadata tables (Obsrvr Data Culture)
obsrvr_test.core._meta_datasets
obsrvr_test.core._meta_lineage
obsrvr_test.core._meta_quality
obsrvr_test.core._meta_changes
```

## 💡 Tips

### View Catalog Info

```sql
-- List all tables
SELECT table_name, path FROM ducklake_table;

-- Check catalog version
SELECT * FROM ducklake_metadata;
```

### Convert Stroops to XLM

```sql
-- Divide by 10,000,000
SELECT fee_charged / 10000000.0 as fee_xlm
FROM obsrvr_test.core.transactions_row_v1;
```

### Filter by Date

```sql
SELECT *
FROM obsrvr_test.core.transactions_row_v1
WHERE DATE(created_at) = '2025-08-17';
```

## ⚠️ Why System DuckDB Fails

```bash
# System DuckDB v1.3.2
$ duckdb -c "ATTACH 'ducklake:postgres:...'"
# ❌ Error: Only DuckLake versions 0.1 and 0.2 are supported

# Nix flake DuckDB v1.4.1
$ ./query.sh "SELECT 1"
# ✅ Works! Supports DuckLake v0.3
```

**Solution**: Always use `./query.sh` which runs DuckDB v1.4.1 from the nix flake.

## 🎯 Discord Bot Stats

To generate Discord bot style stats:

```bash
./query.sh catalog_stats.sql
```

Output includes:
- Transactions count
- Operations count
- Average base fee
- Total fees (XLM)
- Soroban TX count
- Soroban operations count
- Soroban fees (XLM)

## 📚 More Info

- Full query guide: `QUERYING.md`
- Processor README: `README.md`
- Performance guide: `PERFORMANCE_OPTIMIZATION.md`
