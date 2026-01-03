# ducklake-ingestion-obsrvr-v3

**DuckLake Ingestion Processor with DuckDB Catalog (Version 3.0)**

Stellar blockchain data processor that ingests ledgers into DuckLake format with comprehensive table extraction and quality checks.

## 🆕 What's New in v3

### DuckDB Catalog as Default
- **v2**: PostgreSQL catalog (requires separate database server)
- **v3**: DuckDB catalog (single file, simpler operations)

**Trade-off:**
- ✅ Simpler deployment, no database server needed
- ✅ Lower operational costs
- ✅ Easy backups (just copy .duckdb file)
- ⚠️ Single writer only (vs PostgreSQL multi-writer)

### Critical Bug Fixes
1. **Quality Check Chunking** (`quality_checks.go`)
   - Fixed PostgreSQL parameter limit crash (65,535 params)
   - Now chunks inserts into 500 rows/batch (5,000 params)
   - Prevents crashes on large batches

2. **Nil Pointer Safety** (`restored_keys.go`, `evicted_keys.go`)
   - Replaced expensive `defer recover()` with explicit nil checks
   - Better performance, visible errors instead of hidden panics
   - Improved throughput in high-volume ingestion

3. **Memory Optimization** (`contract_data.go`)
   - Optimized `nullableString` pointer allocations
   - Reduced GC pressure in high-throughput scenarios
   - Better documentation of allocation patterns

## 📋 Architecture

```
stellar-live-source-datalake (gRPC)
  ↓
ducklake-ingestion-obsrvr-v3
  ↓ writes Parquet files
  ↓ catalog metadata → DuckDB file
s3://bucket/data/ + catalog.duckdb
  ↓
Query via DuckDB (unlimited readers)
```

## 🚀 Quick Start

### 1. Build the Binary

```bash
cd go
GOWORK=off go build -o ../ducklake-ingestion-obsrvr-v3
```

### 2. Create Directories

```bash
mkdir -p catalogs test_data
```

### 3. Run Test (10 Ledgers)

```bash
./ducklake-ingestion-obsrvr-v3 -config config/test-10-ledgers.yaml
```

**Expected Output:**
```
✅ Attached DuckDB catalog: catalogs/test.duckdb
✅ Created schema: testnet
✅ Ingesting ledgers 100000-100010
✅ Wrote 10 ledgers
✅ Quality Checks: All 19 checks passed
```

### 4. Query the Data

**Option A: DuckDB CLI**
```bash
duckdb

D ATTACH 'ducklake:duckdb:/path/to/catalogs/test.duckdb' AS catalog
  (DATA_PATH '/path/to/test_data/', METADATA_SCHEMA 'testnet');

D SELECT COUNT(*) FROM catalog.testnet.ledgers_row_v2;
┌──────────┐
│   10     │
└──────────┘

D SELECT sequence, closed_at, transaction_count
  FROM catalog.testnet.ledgers_row_v2
  LIMIT 3;
```

**Option B: HTTP Query API (Concurrent with Ingestion)**
```bash
# Start ingestion with Query API enabled
./ducklake-ingestion-obsrvr-v3 -config config/test-10-ledgers.yaml --query-port :8080

# Query while ingestion is running
curl -X POST http://localhost:8080/query \
  -H "Content-Type: application/json" \
  -d '{"sql": "SELECT COUNT(*) FROM testnet.ledgers_row_v2"}'

# Response: {"columns":["count"],"rows":[[10]],"row_count":1,"execution_time_ms":2}
```

See **EXAMPLE_QUERIES.md** for more query patterns and API usage.

## ⚙️ Configuration

### Local DuckDB Catalog (Development)

```yaml
ducklake:
  catalog_path: "ducklake:duckdb:/path/to/catalogs/testnet.duckdb"
  data_path: "/path/to/data/"
  num_workers: 1  # Keep at 1 for DuckDB catalog
```

### S3 DuckDB Catalog (Production)

```yaml
ducklake:
  catalog_path: "ducklake:duckdb:s3://bucket/catalogs/testnet.duckdb"
  data_path: "s3://bucket/testnet_4/"

  # S3/Backblaze B2 credentials
  aws_access_key_id: "your-key"
  aws_secret_access_key: "your-secret"
  aws_region: "us-west-004"
  aws_endpoint: "https://s3.us-west-004.backblazeb2.com"

  num_workers: 1  # DuckDB catalog = single writer
```

### PostgreSQL Catalog (Multi-Writer)

If you need multiple concurrent writers, use PostgreSQL instead:

```yaml
ducklake:
  catalog_path: "ducklake:postgres:postgresql://user:pass@host:5432/db"
  num_workers: 6  # Can use multiple workers with PostgreSQL
```

## 📊 Extracted Tables (19 Total)

### Core Tables
- `ledgers_row_v2` - Ledger headers (24 fields)
- `transactions_row_v2` - Transactions (46 fields)
- `operations_row_v2` - Operations (58 fields)

### Effects & Trades
- `effects_row_v1` - State changes (25 fields)
- `trades_row_v1` - DEX trades (17 fields)

### Account State
- `accounts_snapshot_v1` - Account records
- `trustlines_snapshot_v1` - Asset trustlines
- `native_balances_snapshot_v1` - XLM balances (11 fields)
- `account_signers_snapshot_v1` - Signer configuration

### Soroban (Smart Contracts)
- `contract_data_snapshot_v1` - Contract storage
- `contract_code_snapshot_v1` - Contract WASM
- `contract_events_row_v1` - Contract events
- `config_settings_snapshot_v1` - Network config
- `ttl_snapshot_v1` - TTL entries

### Soroban Archival
- `restored_keys_state_v1` - Restored entries
- `evicted_keys_state_v1` - Evicted entries

### DEX State
- `offers_snapshot_v1` - Order book offers
- `claimable_balances_snapshot_v1` - Claimable balances
- `liquidity_pools_snapshot_v1` - AMM pools

## 🔍 HTTP Query API

### Concurrent Queries During Ingestion

v3 includes an HTTP Query API that allows querying the DuckLake catalog while 24/7 ingestion is running. No more stopping ingestion to run queries!

**Start with Query API enabled:**
```bash
./ducklake-ingestion-obsrvr-v3 \
  -config config/testnet-duckdb.yaml \
  --multi-network \
  --query-port :8080
```

### API Endpoints

**1. Query Data** - `POST /query`
```bash
curl -X POST http://localhost:8080/query \
  -H "Content-Type: application/json" \
  -d '{
    "sql": "SELECT sequence, closed_at, transaction_count FROM testnet.ledgers_row_v2 ORDER BY sequence DESC LIMIT 10",
    "limit": 1000,
    "offset": 0
  }'
```

**2. Health Check** - `GET /health`
```bash
curl http://localhost:8080/health
# {"status":"ok","db_connected":true}
```

**3. Metrics** - `GET /metrics`
```bash
curl http://localhost:8080/metrics
# Prometheus-format connection pool metrics
```

### Query Features

- ✅ **Concurrent with ingestion** - MVCC allows unlimited readers
- ✅ **Query validation** - Prevents dangerous operations (DROP, DELETE, etc.)
- ✅ **Automatic limits** - Default 1,000 rows, max 10,000
- ✅ **Query timeout** - 30 seconds max execution time
- ✅ **Connection pooling** - 20 concurrent queries supported
- ✅ **Zero impact** - Queries don't block ingestion

### Example Queries

**Count ledgers:**
```bash
curl -X POST http://localhost:8080/query \
  -H "Content-Type: application/json" \
  -d '{"sql": "SELECT COUNT(*) FROM testnet.ledgers_row_v2"}'
```

**Recent transactions:**
```bash
curl -X POST http://localhost:8080/query \
  -H "Content-Type: application/json" \
  -d '{"sql": "SELECT sequence, transaction_count, successful_tx_count FROM testnet.ledgers_row_v2 WHERE transaction_count > 0 ORDER BY sequence DESC LIMIT 20"}'
```

**Time-based analysis:**
```bash
curl -X POST http://localhost:8080/query \
  -H "Content-Type: application/json" \
  -d '{"sql": "SELECT DATE_TRUNC('"'"'hour'"'"', closed_at) as hour, SUM(transaction_count) as total_txs FROM testnet.ledgers_row_v2 GROUP BY hour ORDER BY hour DESC LIMIT 24"}'
```

For comprehensive query examples, see **go/EXAMPLE_QUERIES.md**.

### Performance

**Tested with 10,000 ledgers:**
- ✅ Ingestion: 47.31 ledgers/sec (unaffected)
- ✅ Query latency: 0-10ms for simple queries
- ✅ Concurrent queries: 5 simultaneous queries, all 1-3ms
- ✅ Zero wait time: Connection pool not saturated

## 🐳 Docker Deployment

### Build with Nix (Reproducible)

```bash
docker build -f Dockerfile.nix -t ducklake-ingestion-obsrvr-v3:nix .
```

### Build Standard

```bash
docker build -t ducklake-ingestion-obsrvr-v3:latest .
```

### Run Container

```bash
docker run \
  -v $(pwd)/config:/app/config \
  -v $(pwd)/catalogs:/app/catalogs \
  -v $(pwd)/data:/app/data \
  ducklake-ingestion-obsrvr-v3:latest \
  -config /app/config/testnet-duckdb.yaml
```

## 🔍 Quality Checks

v3 includes comprehensive quality checks (chunked inserts to prevent crashes):

- Sequence monotonicity
- Hash format validation
- Transaction count consistency
- Timestamp ordering
- Required fields completeness
- Balance range validation
- Liabilities consistency
- Operation index ordering
- Ledger reference integrity

## 📈 Performance

**Single Worker (DuckDB Catalog):**
- ~100 ledgers/second
- ~1GB memory usage
- ~10MB/sec network I/O

**Multi-Worker (PostgreSQL Catalog):**
- ~600 ledgers/second (6 workers)
- ~6GB memory usage
- ~60MB/sec network I/O

## 🔧 Troubleshooting

### DuckDB Catalog Locked

```
Error: database is locked
```

**Cause:** Another process is writing to the catalog
**Solution:** DuckDB catalog = single writer only. Use PostgreSQL for multi-writer.

### Quality Check Parameter Limit

v3 fixes this automatically by chunking, but if you see:

```
Error: too many parameters (PostgreSQL 65k limit)
```

Reduce `batch_size` in config or ensure you're using v3 (not v2).

## 📚 Documentation

- **Full Config Reference:** See `config/testnet-duckdb.yaml`
- **Query Examples:** See `go/EXAMPLE_QUERIES.md` (HTTP API), `QUERYING_V2.md` (DuckDB CLI)
- **WAL Implementation:** See `go/WAL_IMPLEMENTATION_SUMMARY.md`
- **Shape Up Plans:** See `go/SHAPE_UP_QUERY_API.md`
- **Deployment Guide:** See `QUICKSTART.md`

## 🆚 Version Comparison

| Feature | v2 | v3 |
|---------|----|----|
| Default Catalog | PostgreSQL | **DuckDB** |
| Quality Check Fix | ❌ Can crash | ✅ Chunked |
| Nil Pointer Safety | ⚠️ recover() | ✅ Explicit checks |
| Memory Optimization | Standard | ✅ Optimized |
| Multi-Writer | ✅ Yes | ⚠️ No (PostgreSQL optional) |
| **HTTP Query API** | ❌ No | ✅ **Yes** |
| **Concurrent Queries** | ❌ Blocked | ✅ **MVCC** |
| Operational Complexity | High | **Low** |

## 📝 License

Apache 2.0

## 🙋 Support

See project issues or documentation for help.
