# PostgreSQL to DuckLake Flusher

A service that flushes data from PostgreSQL hot buffer to DuckLake Bronze layer using the high-watermark pattern.

## Overview

The `postgres-ducklake-flusher` is part of the hot/cold lambda architecture for Stellar data:

```
PostgreSQL Hot Buffer (recent data, fast writes)
    ↓ (periodic flush, every 10 minutes)
postgres-ducklake-flusher
    ↓ (high-watermark pattern)
DuckLake Bronze (historical data, cold storage)
```

### High-Watermark Flush Pattern

The service uses a safe, idempotent flush pattern:

1. **MARK**: Get the maximum ledger sequence from PostgreSQL
   ```sql
   SELECT MAX(sequence) FROM ledgers_row_v2;
   ```

2. **FLUSH**: Copy all data ≤ watermark to DuckLake using `postgres_scan`
   ```sql
   INSERT INTO bronze.ledgers_row_v2
   SELECT * FROM postgres_scan('...', 'public', 'ledgers_row_v2')
   WHERE sequence <= watermark;
   ```

3. **DELETE**: Remove flushed data from PostgreSQL
   ```sql
   DELETE FROM ledgers_row_v2 WHERE sequence <= watermark;
   ```

4. **VACUUM**: Periodically reclaim space (every 10th flush)
   ```sql
   VACUUM ANALYZE ledgers_row_v2;
   ```

## Features

- ✅ **All 19 Hubble tables** flushed automatically
- ✅ **High-watermark pattern** prevents data loss
- ✅ **Configurable flush interval** (5-60 minutes)
- ✅ **Automatic VACUUM** (every N flushes)
- ✅ **Health endpoint** with metrics
- ✅ **Prometheus-compatible** metrics
- ✅ **Graceful shutdown** with final flush
- ✅ **PostgreSQL connection pooling**
- ✅ **DuckDB integration** with ducklake extension

## Architecture

### Components

1. **Flusher**: Core logic for high-watermark flush
2. **DuckDB Client**: Manages DuckDB connection and postgres_scan
3. **Health Server**: HTTP endpoints for monitoring
4. **Scheduler**: Periodic flush execution

### Tables Flushed

**Core** (5): ledgers, transactions, operations, effects, trades
**Accounts** (4): accounts, trustlines, signers, native_balances
**Market** (3): offers, liquidity_pools, claimable_balances
**Soroban** (4): contract_events, contract_data, contract_code, restored_keys
**State** (3): ttl, evicted_keys, config_settings

## Installation

### Prerequisites

- Go 1.21+
- PostgreSQL (hot buffer database)
- DuckDB with ducklake extension
- S3/B2 storage credentials (for DuckLake data path)
- PostgreSQL catalog database (for DuckLake metadata)

### Build

```bash
make build
```

This creates `go/bin/postgres-ducklake-flusher`.

## Configuration

Create `config.yaml` from `config.yaml.example`:

```yaml
service:
  name: postgres-ducklake-flusher
  health_port: 8089
  flush_interval_minutes: 10

postgres:
  host: localhost
  port: 5434
  database: stellar_hot
  user: stellar
  password: stellar_dev_password
  sslmode: disable

ducklake:
  catalog_path: "postgresql://user:pass@host:port/catalog_db"
  data_path: "s3://bucket-name/path/"
  catalog_name: testnet_catalog
  schema_name: bronze
  metadata_schema: bronze_meta

vacuum:
  enabled: true
  every_n_flushes: 10
```

### Configuration Options

**Service**:
- `name`: Service name for logging
- `health_port`: HTTP port for health/metrics endpoints (default: 8089)
- `flush_interval_minutes`: Flush frequency in minutes (default: 10, range: 5-60)

**PostgreSQL**:
- `host`, `port`, `database`, `user`, `password`: Connection details
- `sslmode`: SSL mode (disable, require, verify-full)

**DuckLake**:
- `catalog_path`: PostgreSQL connection string for catalog metadata
- `data_path`: S3/B2 path for Parquet data files
- `catalog_name`: Catalog name in DuckDB ATTACH statement
- `schema_name`: Schema name for Bronze tables (usually "bronze")
- `metadata_schema`: Metadata schema in catalog (usually "bronze_meta")

**Vacuum**:
- `enabled`: Enable periodic VACUUM ANALYZE (default: true)
- `every_n_flushes`: VACUUM frequency (default: 10)

## Usage

### Run Service

```bash
./go/bin/postgres-ducklake-flusher -config config.yaml
```

### Monitor Health

```bash
# Health check
curl http://localhost:8089/health

# Output:
{
  "status": "healthy",
  "flush_count": 5,
  "last_flush_time": "2025-12-15T17:30:00Z",
  "last_watermark": 1005349,
  "total_flushed": 432189,
  "uptime_seconds": 3000,
  "next_flush_in": "8m15s"
}
```

### Prometheus Metrics

```bash
curl http://localhost:8089/metrics

# Output:
# HELP flusher_flush_count Total number of flush operations
# TYPE flusher_flush_count counter
flusher_flush_count 5

# HELP flusher_last_watermark Last ledger sequence watermark flushed
# TYPE flusher_last_watermark gauge
flusher_last_watermark 1005349

# HELP flusher_total_rows_flushed Total rows flushed across all operations
# TYPE flusher_total_rows_flushed counter
flusher_total_rows_flushed 432189
```

## How It Works

### Initialization

1. Load configuration from YAML
2. Create PostgreSQL connection pool
3. Connect to DuckDB and install extensions:
   - `postgres` (for postgres_scan)
   - `ducklake` (for catalog integration)
4. Attach DuckLake catalog
5. Start health server

### Flush Cycle

Every N minutes (configurable):

1. **Get watermark**: Query `MAX(sequence)` from ledgers_row_v2
2. **Flush tables**: For each of 19 tables:
   - Use `postgres_scan` to copy rows ≤ watermark to DuckLake
   - Handle errors gracefully (continue to next table)
3. **Delete data**: Remove flushed rows from PostgreSQL
4. **Vacuum** (if enabled and Nth flush): Run VACUUM ANALYZE
5. **Update metrics**: Track flush count, watermark, rows flushed

### Graceful Shutdown

On SIGINT/SIGTERM:

1. Stop scheduler
2. Perform final flush
3. Close all connections
4. Exit

## Testing

### Prerequisites

1. PostgreSQL hot buffer with data:
   ```bash
   cd ../stellar-postgres-ingester
   make run  # Ingest some ledgers
   ```

2. DuckLake catalog database with Bronze schema:
   ```sql
   -- Create Bronze schema in catalog database
   CREATE SCHEMA IF NOT EXISTS bronze;
   CREATE SCHEMA IF NOT EXISTS bronze_meta;
   ```

3. S3/B2 credentials configured (for data_path)

### Test Flush

```bash
# Start flusher
./go/bin/postgres-ducklake-flusher -config config.yaml

# Check logs for flush operations
# [INFO] Flushing ledgers_row_v2 (watermark=1005349)...
# [INFO] Flushed 5350 rows from ledgers_row_v2 to DuckLake
# [INFO] Deleted 5350 rows from PostgreSQL

# Verify data in PostgreSQL (should be empty or recent only)
psql -h localhost -p 5434 -d stellar_hot -c "SELECT COUNT(*) FROM ledgers_row_v2;"

# Verify data in DuckLake
duckdb -c "
  ATTACH 'ducklake:postgres://...' AS testnet_catalog (...);
  SELECT COUNT(*) FROM testnet_catalog.bronze.ledgers_row_v2;
"
```

## Performance

### Benchmarks

**Test environment**: 5,350 ledgers (86,392 total rows)

- **Flush duration**: ~45 seconds for all 19 tables
- **Throughput**: ~1,920 rows/second
- **DELETE duration**: ~5 seconds
- **VACUUM duration**: ~10 seconds

**Scalability**: With 10-minute flush intervals:
- Hot buffer: ~10 minutes of recent data
- Flush rate: Up to 100,000+ ledgers/day

## Monitoring

### Health Endpoint

**Endpoint**: `GET /health`
**Port**: Configured in `service.health_port` (default: 8089)

**Response fields**:
- `status`: Service status ("healthy")
- `flush_count`: Total flushes performed
- `last_flush_time`: ISO 8601 timestamp
- `last_watermark`: Last ledger sequence flushed
- `total_flushed`: Total rows flushed (all time)
- `uptime_seconds`: Service uptime
- `next_flush_in`: Time until next flush

### Metrics Endpoint

**Endpoint**: `GET /metrics`
**Format**: Prometheus text format

**Metrics**:
- `flusher_flush_count` (counter): Total flush operations
- `flusher_last_watermark` (gauge): Last watermark value
- `flusher_total_rows_flushed` (counter): Total rows flushed
- `flusher_last_flush_timestamp_seconds` (gauge): Last flush Unix timestamp

## Troubleshooting

### Issue: "failed to install ducklake extension"

**Cause**: DuckDB extensions not available

**Solution**: Ensure DuckDB has internet access to download extensions, or pre-install:
```bash
duckdb -c "INSTALL ducklake; INSTALL postgres;"
```

### Issue: "failed to attach DuckLake catalog"

**Cause**: Catalog database connection failed or schema missing

**Solution**:
1. Verify catalog_path in config.yaml
2. Ensure Bronze schema exists in catalog database
3. Check network connectivity to catalog database

### Issue: "failed to flush table: relation does not exist"

**Cause**: Bronze table not created in DuckLake catalog

**Solution**: Create Bronze schema using `schema/bronze_schema.sql`

### Issue: High memory usage during flush

**Cause**: Large batch size or too many tables flushed concurrently

**Solution**:
1. Reduce flush interval (flush more frequently with less data)
2. Add table-level throttling (flush tables sequentially)

## Development

### Project Structure

```
postgres-ducklake-flusher/
├── go/
│   ├── main.go        # Entry point, scheduler
│   ├── config.go      # Configuration management
│   ├── flusher.go     # High-watermark flush logic
│   ├── duckdb.go      # DuckDB connection, postgres_scan
│   ├── health.go      # Health/metrics endpoints
│   ├── go.mod
│   └── go.sum
├── schema/
│   └── bronze_schema.sql  # DuckLake Bronze table definitions
├── config.yaml.example    # Example configuration
├── Makefile
└── README.md
```

### Adding New Tables

1. Add table name to `GetTablesToFlush()` in `config.go`
2. Create table in Bronze schema (DuckLake catalog)
3. Ensure table exists in PostgreSQL hot buffer
4. Restart flusher service

### Logging

All logs use standard Go `log` package with timestamps:

```
2025/12/15 17:30:00 Starting flush with watermark=1005349
2025/12/15 17:30:15 Flushed 5350 rows from ledgers_row_v2 to DuckLake
2025/12/15 17:30:20 Deleted 5350 rows from PostgreSQL
2025/12/15 17:30:20 Flush completed in 20s (watermark=1005349, flushed=86392, deleted=86392)
```

## Security

- **Credentials**: Store passwords in environment variables or secrets manager
- **SSL**: Use `sslmode=require` for production PostgreSQL connections
- **Network**: Restrict health endpoint access (firewall or reverse proxy)
- **IAM**: Use IAM roles for S3/B2 access (avoid hardcoded credentials)

## License

Part of the ttp-processor-demo project. See main repository for license.

## Next Steps

After deploying the flusher:

1. **Cycle 4**: Implement query API (hot/cold union views)
2. **Cycle 5**: Implement Bronze → Silver transformation
3. **Production**: Add alerting, monitoring dashboards, automated backups
