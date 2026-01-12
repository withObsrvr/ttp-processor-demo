# Silver Cold Flusher

A service that flushes data from PostgreSQL silver_hot buffer to DuckLake Silver layer using the high-watermark pattern.

## Overview

The `silver-cold-flusher` is part of the silver layer lambda architecture for Stellar data:

```
silver_hot (PostgreSQL - recent data, fast queries)
    ↓ (periodic flush, every 3 hours)
silver-cold-flusher
    ↓ (high-watermark pattern)
silver_cold (DuckLake Parquet - historical data, analytics-optimized)
```

## Architecture

**Cycle 4 of the Silver Layer Implementation**

This service completes the silver layer by adding cold storage archival:
- **Cycle 1**: Silver hot buffer + real-time pipeline ✅
- **Cycle 2**: UPSERT pattern for current state tables ✅
- **Cycle 3**: Incremental SCD Type 2 for snapshot tables ✅
- **Cycle 4**: Silver cold flusher (this service) 🏗️

## High-Watermark Flush Pattern

The service uses a safe, idempotent flush pattern:

1. **MARK**: Get the maximum ledger from realtime transformer checkpoint
   ```sql
   SELECT MAX(last_ledger_sequence) FROM realtime_transformer_checkpoint;
   ```

2. **FLUSH**: Copy all data ≤ watermark to DuckLake using `postgres_scan`
   ```sql
   INSERT INTO silver_cold.enriched_history_operations
   SELECT * FROM postgres_scan('...', 'public', 'enriched_history_operations')
   WHERE ledger_sequence <= watermark;
   ```

3. **DELETE**: Remove flushed data from PostgreSQL
   ```sql
   DELETE FROM enriched_history_operations WHERE ledger_sequence <= watermark;
   ```

4. **VACUUM**: Periodically reclaim space (every 10th flush)
   ```sql
   VACUUM ANALYZE enriched_history_operations;
   ```

## Tables Flushed (18 Total)

### Current State Tables (10)
1. accounts_current
2. account_signers_current
3. trustlines_current
4. offers_current
5. claimable_balances_current
6. liquidity_pools_current
7. contract_data_current
8. contract_code_current
9. config_settings_current
10. ttl_current

### Snapshot Tables (4) - SCD Type 2
11. accounts_snapshot
12. trustlines_snapshot
13. offers_snapshot
14. account_signers_snapshot

### Enriched/Event Tables (3)
15. enriched_history_operations
16. token_transfers_raw
17. soroban_history_operations

## Configuration

Create `config.yaml`:

```yaml
service:
  name: silver-cold-flusher
  health_port: "8095"
  flush_interval_hours: 3

postgres:
  host: localhost
  port: 5434
  database: silver_hot
  user: stellar
  password: stellar_dev_password
  sslmode: disable

ducklake:
  catalog_path: "postgresql://user:pass@host:port/catalog_db"
  data_path: "s3://bucket-name/testnet_silver/"
  catalog_name: testnet_catalog
  schema_name: silver
  metadata_schema: silver_meta

vacuum:
  enabled: true
  every_n_flushes: 10
```

## Building

```bash
make build
```

This creates `bin/silver-cold-flusher`.

## Running

### Start Service
```bash
./scripts/start.sh
```

### Stop Service
```bash
./scripts/stop.sh
```

### Monitor Health
```bash
curl http://localhost:8095/health
```

Example response:
```json
{
  "service": "silver-cold-flusher",
  "status": "healthy",
  "flush_count": 8,
  "last_flush_time": "2025-12-16T23:00:00Z",
  "last_watermark": 2145500,
  "total_flushed": 43440,
  "uptime_seconds": 28800,
  "next_flush_in": "2h15m"
}
```

### Prometheus Metrics
```bash
curl http://localhost:8095/metrics
```

## How It Works

### Initialization
1. Load configuration from YAML
2. Connect to PostgreSQL silver_hot
3. Connect to DuckDB and install extensions (postgres, ducklake, httpfs)
4. Attach DuckLake catalog
5. Start health server
6. Run initial flush

### Flush Cycle
Every N hours (configurable):

1. **Get watermark**: Query checkpoint from realtime transformer
2. **Flush tables**: For each of 18 tables, use postgres_scan to copy
3. **Delete data**: Remove flushed rows from silver_hot
4. **Vacuum** (every Nth flush): Reclaim space
5. **Update metrics**: Track stats for monitoring

### Graceful Shutdown
On SIGINT/SIGTERM:
1. Stop scheduler
2. Perform final flush
3. Close all connections
4. Exit

## Performance

**Benchmarks** (estimated for 3 hours of data):
- Flush duration: ~2-3 minutes for all 18 tables
- Throughput: ~5,000 rows/second
- DELETE duration: ~10 seconds
- VACUUM duration: ~15 seconds

**Scalability**: With 3-hour flush intervals:
- silver_hot: ~3 hours of recent data
- Flush rate: Up to 24,000 ledgers/day

## Partitioning

All tables partitioned by `ledger_range`:
```sql
ledger_range = FLOOR(ledger_sequence / 100000)
```

**Benefits**:
- Partition pruning for range queries
- Easy TTL enforcement (drop old partitions)
- Efficient updates/deletes within partitions

## Monitoring

### Health Endpoint
- **URL**: `GET /health`
- **Port**: 8095 (default)
- **Fields**: status, flush_count, last_flush_time, last_watermark, total_flushed, uptime_seconds, next_flush_in

### Metrics Endpoint
- **URL**: `GET /metrics`
- **Format**: Prometheus text format
- **Metrics**: flusher_flush_count, flusher_last_watermark, flusher_total_rows_flushed, flusher_last_flush_timestamp_seconds

## Troubleshooting

### Issue: "failed to install ducklake extension"
**Solution**: Ensure DuckDB has internet access or pre-install extensions:
```bash
duckdb -c "INSTALL ducklake; INSTALL postgres; INSTALL httpfs;"
```

### Issue: "failed to attach DuckLake catalog"
**Solution**:
1. Verify catalog_path in config.yaml
2. Ensure silver schema exists in catalog
3. Check network connectivity

### Issue: "failed to flush table: relation does not exist"
**Solution**: Create silver schema using `schema/silver_schema.sql`

## File Structure

```
silver-cold-flusher/
├── go/
│   ├── main.go        # Entry point + scheduler
│   ├── config.go      # Configuration management
│   ├── flusher.go     # High-watermark flush logic
│   ├── duckdb.go      # DuckDB connection + postgres_scan
│   ├── health.go      # Health/metrics endpoints
│   ├── go.mod
│   └── go.sum
├── schema/
│   └── silver_schema.sql  # DuckLake silver table definitions
├── scripts/
│   ├── start.sh       # Start service script
│   └── stop.sh        # Stop service script
├── config.yaml         # Configuration
├── Makefile
├── README.md
└── SHAPE_UP_CYCLE4.md  # Design document
```

## Next Steps

After deploying the silver cold flusher:

1. **Monitor**: Watch flush cycles, verify no data loss
2. **Tune**: Adjust flush_interval_hours based on data volume
3. **Query API** (optional Cycle 5): Build query API that unions hot + cold
4. **Production**: Add alerting, monitoring dashboards, automated backups

## Dependencies

- Go 1.21+
- PostgreSQL silver_hot database (from Cycles 1-3)
- DuckDB with ducklake extension
- S3/B2 storage credentials
- PostgreSQL catalog database for DuckLake metadata

## License

Part of the ttp-processor-demo project. See main repository for license.

---

**Cycle 4 Status**: 🏗️ In Progress - Service scaffold complete, schema pending, testing pending
