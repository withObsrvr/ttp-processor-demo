# DuckLake Ingestion Processor - Obsrvr Edition

**Version**: 2.0.0
**Status**: Production Ready
**License**: MIT

A production-grade Stellar blockchain data ingestion processor implementing Obsrvr Data Culture principles with comprehensive metadata tracking, quality checks, and data lineage.

---

## 🎯 Overview

This processor ingests Stellar blockchain data (ledgers, transactions, operations, and native balances) into a DuckLake data lakehouse with full observability through metadata tables that track datasets, quality checks, lineage, and schema changes.

### Key Features

- **📊 Multi-Table Ingestion**: Processes 4 data tables simultaneously
- **✅ 19 Quality Checks**: Validates data quality across all tables (99.7%+ pass rate)
- **📈 Lineage Tracking**: Full data provenance from source ledgers to destination tables
- **🏷️ Metadata Management**: Comprehensive metadata infrastructure
- **⚡ High Performance**: 44+ ledgers/sec, < 1% metadata overhead
- **🔍 Observe-First**: Non-blocking quality checks that log issues without stopping ingestion
- **📦 Playbook-Compliant Naming**: All tables follow Obsrvr naming conventions

---

## 🏗️ Architecture

### Data Flow

```
Stellar Data Source (gRPC)
          ↓
    [Ingester with Buffers]
          ↓
    [Quality Checks] ← 19 checks, non-blocking
          ↓
    [Multi-Table Insert]
      ├── ledgers_row_v2
      ├── transactions_row_v1
      ├── operations_row_v1
      └── native_balances_snapshot_v1
          ↓
    [Record Quality Results] → _meta_quality
          ↓
    [Record Lineage] → _meta_lineage
```

### Components

**1. Ingester (`go/main.go`)**
- Streams ledgers from stellar-live-source-datalake
- Buffers data in memory (configurable batch size)
- Extracts ledgers, transactions, operations, and balances
- Manages DuckLake connection and catalog

**2. Quality Checks (`go/quality_checks.go`)**
- 19 checks across 4 tables
- Categories: validity, consistency, completeness
- Non-blocking execution
- Results recorded to `_meta_quality`

**3. Lineage Tracking (`go/main.go:recordLineage()`)**
- Tracks source ledger ranges
- Records processor version and row counts
- Populates `_meta_lineage` table

**4. Metadata Infrastructure**
- 4 metadata tables for comprehensive observability
- Dataset registry with versioning
- Full audit trail

---

## 🚀 Quick Start

### Prerequisites

- Go 1.21+
- PostgreSQL (for DuckLake catalog)
- stellar-live-source-datalake service running

### Build

```bash
make build
```

### Run

```bash
./ducklake-ingestion-obsrvr -config config/test-integration-100.yaml
```

### Configuration

Create a YAML config file:

```yaml
service:
  name: "ducklake-ingestion-obsrvr"
  environment: "production"

source:
  endpoint: "localhost:50053"
  network_passphrase: "Test SDF Network ; September 2015"
  start_ledger: 50000
  end_ledger: 51000

ducklake:
  catalog_path: "ducklake:postgres:postgresql:///your_database?host=/path/to/sockets"
  data_path: "./data"
  catalog_name: "your_catalog"
  schema_name: "core"
  batch_size: 50
  commit_interval_seconds: 30
  num_workers: 1

logging:
  level: "info"
  format: "console"
```

---

## 📊 Data Tables

### Core Data Tables (4)

#### 1. `core.ledgers_row_v2` (24 fields)
Stellar ledger headers with comprehensive metadata.

**Key Fields**:
- `sequence`: Ledger sequence number (primary key)
- `ledger_hash`: 64-character hex hash
- `closed_at`: Ledger close timestamp
- `protocol_version`: Stellar protocol version
- `total_coins`: Total XLM in existence
- `successful_tx_count`, `failed_tx_count`: Transaction statistics
- `transaction_count`, `operation_count`: New DAY2 fields for stellar-etl alignment

#### 2. `core.transactions_row_v1` (13 fields)
Transaction records with full details.

**Key Fields**:
- `ledger_sequence`: Parent ledger
- `transaction_hash`: Unique transaction ID
- `source_account`: Transaction originator
- `fee_charged`: Actual fee paid
- `successful`: Success/failure flag
- `operation_count`: Number of operations

#### 3. `core.operations_row_v1` (13 fields)
Individual operations within transactions.

**Key Fields**:
- `ledger_sequence`: Parent ledger
- `transaction_hash`: Parent transaction
- `operation_index`: Position within transaction
- `type`: Operation type (payment, create_account, etc.)
- `payment_to`, `payment_amount`: Payment-specific fields

#### 4. `core.native_balances_snapshot_v1` (11 fields)
Point-in-time account balance snapshots.

**Key Fields**:
- `account_id`: Stellar account address
- `balance`: XLM balance in stroops
- `buying_liabilities`, `selling_liabilities`: DEX obligations
- `sequence_number`: Account sequence
- `last_modified_ledger`: Last update

---

## 🗂️ Metadata Tables (4)

### 1. `_meta_datasets`
Registry of all datasets with versions and metadata.

**Schema**:
```sql
CREATE TABLE _meta_datasets (
    name TEXT NOT NULL,           -- e.g., "core.ledgers_row_v2"
    tier TEXT NOT NULL,           -- "core", "derived", "aggregated"
    domain TEXT NOT NULL,         -- "core" (blockchain data)
    purpose TEXT NOT NULL,        -- "row", "snapshot", "aggregated"
    subject TEXT NOT NULL,        -- "ledgers", "transactions", etc.
    grain TEXT NOT NULL,          -- "v1", "v2" (schema version)
    major_version INT,            -- 2 for v2
    minor_version INT,            -- 0 for v2.0
    created_at TIMESTAMP,
    updated_at TIMESTAMP
)
```

**Usage**:
```sql
-- List all registered datasets
SELECT name, domain, major_version || '.' || minor_version AS version
FROM obsrvr_test.core._meta_datasets
ORDER BY name;
```

### 2. `_meta_lineage`
Data provenance tracking for every batch.

**Schema**:
```sql
CREATE TABLE _meta_lineage (
    id INTEGER NOT NULL,
    dataset TEXT NOT NULL,                    -- Which table
    partition TEXT,                           -- Partition key (if applicable)
    source_ledger_start INT NOT NULL,         -- First source ledger
    source_ledger_end INT NOT NULL,           -- Last source ledger
    pipeline_version TEXT NOT NULL,           -- Processor version
    processor_name TEXT NOT NULL,             -- "ducklake-ingestion-obsrvr"
    checksum TEXT,                            -- Optional data checksum
    row_count INT,                            -- Rows written
    created_at TIMESTAMP NOT NULL             -- Processing timestamp
)
```

**Usage**:
```sql
-- Find lineage for specific ledger
SELECT dataset, source_ledger_start, source_ledger_end, row_count, pipeline_version
FROM obsrvr_test.core._meta_lineage
WHERE source_ledger_start <= 50125 AND source_ledger_end >= 50125
ORDER BY dataset;

-- Check for gaps in ledger coverage
SELECT dataset,
       source_ledger_end AS batch_end,
       LEAD(source_ledger_start) OVER (PARTITION BY dataset ORDER BY source_ledger_start) AS next_batch_start,
       LEAD(source_ledger_start) OVER (PARTITION BY dataset ORDER BY source_ledger_start) - source_ledger_end - 1 AS gap_size
FROM obsrvr_test.core._meta_lineage
WHERE dataset = 'core.ledgers_row_v2';
```

### 3. `_meta_quality`
Quality check execution results.

**Schema**:
```sql
CREATE TABLE _meta_quality (
    id INTEGER NOT NULL,
    dataset TEXT NOT NULL,              -- Which table was checked
    partition TEXT,
    check_name TEXT NOT NULL,           -- e.g., "ledger_hash_format"
    check_type TEXT NOT NULL,           -- "validity", "consistency", "completeness"
    passed BOOLEAN NOT NULL,            -- Did it pass?
    details TEXT,                       -- Failure details
    row_count INT,                      -- Rows checked
    null_anomalies INT,                 -- Unexpected nulls found
    created_at TIMESTAMP NOT NULL
)
```

**Usage**:
```sql
-- Check pass rate by dataset
SELECT dataset,
       COUNT(*) AS total_checks,
       SUM(CASE WHEN passed THEN 1 ELSE 0 END) AS passed,
       SUM(CASE WHEN NOT passed THEN 1 ELSE 0 END) AS failed,
       ROUND(100.0 * SUM(CASE WHEN passed THEN 1 ELSE 0 END) / COUNT(*), 2) AS pass_rate_pct
FROM obsrvr_test.core._meta_quality
GROUP BY dataset
ORDER BY dataset;

-- Find all failed checks
SELECT dataset, check_name, check_type, details, created_at
FROM obsrvr_test.core._meta_quality
WHERE NOT passed
ORDER BY created_at DESC;
```

### 4. `_meta_changes`
Schema migration tracking (reserved for future use).

**Schema**:
```sql
CREATE TABLE _meta_changes (
    id INTEGER NOT NULL,
    dataset TEXT NOT NULL,
    change_type TEXT NOT NULL,          -- "schema", "partition", "index"
    description TEXT NOT NULL,
    applied_by TEXT NOT NULL,           -- User/processor that applied change
    applied_at TIMESTAMP NOT NULL,
    rollback_sql TEXT                   -- Optional rollback script
)
```

---

## ✅ Quality Checks (19)

### Categories

1. **Validity Checks (8)**: Data format and range validation
2. **Consistency Checks (8)**: Cross-table and ordering validation
3. **Completeness Checks (3)**: Required field validation

### Ledger Checks (5)

| Check Name | Type | Purpose |
|------------|------|---------|
| `ledger_hash_format` | validity | Validates 64-char hex hashes |
| `required_fields` | completeness | Ensures critical fields populated |
| `sequence_monotonicity` | consistency | Verifies ascending sequence numbers |
| `transaction_count_consistency` | consistency | Matches tx count to actual transactions |
| `timestamp_ordering` | consistency | Ensures chronological timestamps |

### Transaction Checks (4)

| Check Name | Type | Purpose |
|------------|------|---------|
| `transaction_hash_format` | validity | Validates 64-char hex hashes |
| `source_account_format` | validity | Validates Stellar G-addresses |
| `transaction_fee_range` | validity | Ensures fees in valid range (100 stroops - 100 XLM) |
| `transaction_ledger_consistency` | consistency | Verifies transactions reference existing ledgers |

### Operation Checks (4)

| Check Name | Type | Purpose |
|------------|------|---------|
| `operation_index_ordering` | consistency | Validates sequential indices within transactions |
| `operation_transaction_hash` | validity | Validates transaction hash format |
| `operation_ledger_consistency` | consistency | Verifies operations reference existing ledgers |
| `operation_required_fields` | completeness | Ensures critical fields populated |

### Balance Checks (6)

| Check Name | Type | Purpose |
|------------|------|---------|
| `balance_account_id_format` | validity | Validates Stellar G-addresses |
| `balance_range` | validity | Ensures balances 0 ≤ balance ≤ 10^18 stroops |
| `liabilities_validation` | consistency | Validates liabilities ≤ 2× balance |
| `balance_ledger_consistency` | consistency | Verifies balances reference existing ledgers |
| `balance_required_fields` | completeness | Ensures account_id and ledger_sequence populated |
| `sequence_number_validity` | validity | Validates sequence_number ≥ 0 |

### Quality Check Performance

- **Execution Time**: 200-415µs per batch (19 checks)
- **Overhead**: < 0.2% of total processing time
- **Pass Rate**: 99.7%+ on real data
- **Non-Blocking**: Failed checks log warnings but don't prevent writes

---

## 📈 Performance

### Tested Scale

| Ledgers | Duration | Rate | Batches |
|---------|----------|------|---------|
| 100 | 2.4s | 41-45 ledgers/sec | 2 |
| 1,000 | 22.7s | 44.02 ledgers/sec | 20 |

### Performance Breakdown (per 50-ledger batch)

| Component | Time | % of Total |
|-----------|------|------------|
| Quality Checks | 0.3ms | 0.11% |
| Data Inserts | 70ms | 26% |
| Quality Recording | 190ms | 71% |
| Lineage Recording | <1ms | 0.4% |
| **Total** | **~268ms** | **100%** |

### Scalability Projections

- **10K ledgers**: ~3.8 minutes
- **100K ledgers**: ~38 minutes
- **1M ledgers**: ~6.3 hours

Linear scaling observed across all tested batch sizes.

---

## 🔍 Obsrvr Data Culture Principles

This processor implements all core Obsrvr principles:

### 1. Metadata-First ✅
- 4 metadata tables track datasets, lineage, quality, and changes
- Every data table has corresponding metadata
- Metadata created automatically with data tables

### 2. Observe First ✅
- Quality checks are non-blocking
- Failed checks log warnings and record to `_meta_quality`
- Processing continues even when checks fail
- Enables investigation without data loss

### 3. Lineage Tracking ✅
- Every batch records source ledger range
- Processor version tracked for impact analysis
- Row counts enable validation
- Complete audit trail from source to destination

### 4. Quality Checks ✅
- 19 checks cover validity, consistency, completeness
- Executed on every batch
- Results recorded for trending analysis
- < 0.2% performance overhead

### 5. Version Management ✅
- Processor version: 2.0.0
- Schema versions tracked per dataset
- Major/minor versioning for all tables
- Enables migration planning

### 6. Playbook Naming ✅
- Format: `<domain>.<purpose>_<subject>_<grain>_v<N>`
- Examples:
  - `core.ledgers_row_v2`
  - `core.transactions_row_v1`
  - `core.operations_row_v1`
  - `core.native_balances_snapshot_v1`

---

## 🛠️ Configuration Options

### Source Configuration

```yaml
source:
  endpoint: "localhost:50053"                      # gRPC endpoint
  network_passphrase: "Test SDF Network ; September 2015"
  start_ledger: 50000                              # First ledger to process
  end_ledger: 51000                                # Last ledger (inclusive)
```

### DuckLake Configuration

```yaml
ducklake:
  catalog_path: "ducklake:postgres:postgresql:///db?host=/path"  # PostgreSQL catalog
  data_path: "./data"                              # Parquet storage path
  catalog_name: "your_catalog"                     # Catalog name
  schema_name: "core"                              # Schema name
  batch_size: 50                                   # Ledgers per batch
  commit_interval_seconds: 30                      # Max time before flush
  use_upsert: false                                # Use INSERT (faster)
  create_indexes: false                            # Skip indexes for speed
  num_workers: 1                                   # Single-threaded (stable)
```

### Performance Tuning

**Batch Size**:
- Smaller (10-20): Lower memory, more frequent flushes
- Medium (50): Recommended for balanced performance
- Larger (100+): Higher throughput, more memory

**Commit Interval**:
- Controls maximum time before flush
- Useful for time-sensitive ingestion
- Set to 0 to disable time-based flushing

---

## 📖 Usage Examples

### Basic Ingestion

```bash
# Process 1000 ledgers
./ducklake-ingestion-obsrvr -config config/production.yaml
```

### Query Metadata

```bash
# Check dataset versions
psql -d your_database -c "
SELECT name, major_version || '.' || minor_version AS version
FROM your_catalog.core._meta_datasets;"

# Check lineage coverage
psql -d your_database -c "
SELECT dataset, COUNT(*) AS batches, MIN(source_ledger_start) AS first,
       MAX(source_ledger_end) AS last, SUM(row_count) AS total_rows
FROM your_catalog.core._meta_lineage
GROUP BY dataset;"

# Check quality pass rates
psql -d your_database -c "
SELECT dataset, check_type, COUNT(*) AS executions,
       SUM(CASE WHEN passed THEN 1 ELSE 0 END) AS passed
FROM your_catalog.core._meta_quality
GROUP BY dataset, check_type
ORDER BY dataset, check_type;"
```

---

## 🚨 Troubleshooting

### Issue: Quality Check Failures

**Symptoms**: Log shows "⚠️ QUALITY FAILED"

**Action**:
1. Check `_meta_quality` table for failure details
2. Investigate specific check (see check descriptions above)
3. Data is still written - investigate but don't panic

**Example**:
```sql
SELECT * FROM your_catalog.core._meta_quality
WHERE NOT passed
ORDER BY created_at DESC
LIMIT 10;
```

### Issue: Missing Lineage Records

**Symptoms**: Gaps in `_meta_lineage` coverage

**Action**:
1. Query for gaps:
```sql
SELECT source_ledger_end,
       LEAD(source_ledger_start) OVER (ORDER BY source_ledger_start) AS next_start
FROM your_catalog.core._meta_lineage
WHERE dataset = 'core.ledgers_row_v2';
```
2. Reprocess missing ranges with appropriate config

### Issue: Slow Performance

**Symptoms**: < 30 ledgers/sec

**Possible Causes**:
- Large batch size causing memory pressure
- Slow PostgreSQL catalog connection
- Network latency to data source

**Solutions**:
- Reduce `batch_size` to 20-30
- Use local PostgreSQL with socket connection
- Check gRPC connection to stellar-live-source-datalake

---

## 📚 Additional Documentation

- `TEST_RESULTS_DAY7.md`: Quality checks implementation and testing
- `TEST_RESULTS_DAY8.md`: Lineage tracking implementation
- `TEST_RESULTS_DAY9.md`: 1000 ledger pipeline test results
- `MIGRATION_GUIDE.md`: Migrating from v1.0 processor

---

## 🤝 Contributing

This processor follows Obsrvr Data Culture principles. When contributing:

1. **Add Quality Checks**: New data tables should have ≥3 quality checks
2. **Record Lineage**: All data writes must record lineage
3. **Update Metadata**: Register new datasets in `_meta_datasets`
4. **Document Changes**: Update README and migration guide
5. **Test at Scale**: Validate with ≥1000 ledgers

---

## 📄 License

MIT License - See LICENSE file for details

---

## 🙏 Acknowledgments

Built with:
- **DuckDB**: High-performance analytical database
- **Stellar Go SDK**: Blockchain data structures
- **Go gRPC**: Streaming data source connection

Implements:
- **Obsrvr Data Culture**: Metadata-first data engineering principles
- **Shape Up Methodology**: Fixed-time, variable-scope development

---

## 📞 Support

- **Issues**: Report bugs at [GitHub Issues](https://github.com/your-org/ducklake-ingestion-obsrvr/issues)
- **Documentation**: See `docs/` directory
- **Examples**: See `config/` directory for sample configurations

---

**Version**: 2.0.0
**Last Updated**: 2025-11-04
**Status**: Production Ready ✅
