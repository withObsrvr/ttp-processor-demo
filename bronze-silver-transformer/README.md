# Bronze → Silver Transformer

Production-ready service that transforms DuckLake Bronze (raw Stellar ledger data) into analytics-ready Silver layer tables.

## Overview

The Bronze → Silver Transformer is a critical component of the Stellar data pipeline that:
- Runs as a **long-running background service**
- Polls DuckLake Bronze every 15 minutes for new data
- Transforms raw ledger data into **18 analytics-ready Silver tables**
- Maintains checkpoint for reliable restart and incremental processing
- Uses **SQL-based transformations** via DuckDB (no in-memory processing)

## Architecture

```
PostgreSQL Hot Buffer → postgres-ducklake-flusher → DuckLake Bronze
                                                           ↓
                                              bronze-silver-transformer
                                                           ↓
                                                   DuckLake Silver
                                                      (18 tables)
```

**Key Design Decisions:**
- **Same PostgreSQL catalog** for Bronze and Silver (different schemas)
- **SQL transformations** leveraging DuckDB analytics engine
- **Batch processing** every 15 minutes (configurable)
- **Full table refresh** pattern (CREATE OR REPLACE TABLE)
- **Checkpoint-based** progress tracking for reliability

## Silver Layer Tables (18 Total)

### Phase 1: Current State Tables (10 tables)
Latest state of all entities:
- `accounts_current`
- `account_signers_current`
- `trustlines_current`
- `offers_current`
- `claimable_balances_current`
- `liquidity_pools_current`
- `contract_data_current`
- `contract_code_current`
- `config_settings_current`
- `ttl_current`

### Phase 2: Historical Snapshots with SCD Type 2 (5 tables)
Time-travel queries with valid_from/valid_to timestamps:
- `accounts_snapshot`
- `trustlines_snapshot`
- `liquidity_pools_snapshot`
- `contract_data_snapshot`
- `evicted_keys_snapshot`

### Phase 3: Enriched Operations (2 tables)
Operations joined with transaction and ledger context:
- `enriched_history_operations` (all operations with 3-way JOIN)
- `enriched_history_operations_soroban` (Soroban only)

### Phase 4: Token Transfers Analytics (1 table)
Unified view of classic payments + Soroban transfers:
- `token_transfers_raw`

## Performance

**Tested with Stellar Testnet:**
- 100 ledgers processed in **4 minutes 27 seconds**
- Processing rate: **~2.67 seconds per ledger**
- 15-minute capacity: **~337 ledgers per cycle**

**Transformation Breakdown:**
- Phase 1 (10 current tables): 57 seconds
- Phase 2 (5 snapshot tables): 46 seconds
- Phase 3 (2 enriched operations): 127 seconds
- Phase 4 (1 token transfers): 23 seconds

## Quick Start

### 1. Build

```bash
make build
```

### 2. Configure

Create `config.yaml`:

```yaml
service:
  name: bronze-silver-transformer
  health_port: "8093"
  transform_interval_minutes: 15

ducklake:
  catalog_path: "ducklake:postgres:postgresql://user:pass@host:port/catalog_db?sslmode=require"
  catalog_name: mainnet_catalog
  metadata_schema: bronze_meta

  bronze_schema: bronze
  bronze_data_path: "s3://bucket/mainnet_bronze/"

  silver_schema: silver
  silver_data_path: "s3://bucket/mainnet_silver/"

  aws_access_key_id: "YOUR_KEY"
  aws_secret_access_key: "YOUR_SECRET"
  aws_region: "us-west-004"
  aws_endpoint: "https://s3.us-west-004.backblazeb2.com"

checkpoint:
  table: silver_transformer_checkpoint
  schema: bronze
```

### 3. Run

**Local development:**
```bash
make run
```

**Docker:**
```bash
make docker-run
```

**Systemd (production):**
```bash
make deploy-systemd
```

**Kubernetes:**
```bash
kubectl apply -f k8s/deployment.yaml
```

### 4. Verify

```bash
# Check health
curl http://localhost:8093/health

# View logs
make logs-systemd  # or logs-docker
```

## Deployment

See **[DEPLOYMENT_GUIDE.md](./DEPLOYMENT_GUIDE.md)** for complete deployment instructions including:
- Systemd service setup
- Docker/Docker Compose deployment
- Kubernetes deployment
- Monitoring and alerting
- Backup and recovery procedures
- Production checklist

## Querying Silver Layer

### Option 1: Direct DuckDB Access (Recommended for Analytics)

```go
// See examples/query_silver.go for full example
db, _ := sql.Open("duckdb", "")

// Attach catalog
db.Exec(`ATTACH 'ducklake:postgres://...' AS catalog (DATA_PATH 's3://bucket/network_silver/')`)

// Query current accounts
rows, _ := db.Query(`
    SELECT account_id, balance, ledger_sequence
    FROM catalog.silver.accounts_current
    ORDER BY CAST(balance AS BIGINT) DESC
    LIMIT 10
`)
```

See `examples/silver_queries.sql` for 30+ example queries.

### Option 2: Via stellar-query-api (For Applications)

Extend the existing stellar-query-api to expose Silver layer via REST API:

```bash
GET /api/v1/silver/accounts/current
GET /api/v1/silver/accounts/history/{account_id}
GET /api/v1/silver/operations/enriched?type=payment
GET /api/v1/silver/transfers/stats?group_by=asset
```

See **[STELLAR_QUERY_API_INTEGRATION.md](./STELLAR_QUERY_API_INTEGRATION.md)** for implementation guide.

## Monitoring

### Health Endpoint

```bash
curl http://localhost:8093/health
```

Returns:
```json
{
  "status": "healthy",
  "transformations_total": 42,
  "transformation_errors": 0,
  "last_ledger_sequence": 2137918,
  "last_transformation_time": "2024-12-16T10:33:12Z"
}
```

### Prometheus Metrics

Available at `:8093/metrics`:

- `bronze_silver_transformations_total` - Total transformation cycles
- `bronze_silver_transformation_errors_total` - Failed transformations
- `bronze_silver_transformation_duration_seconds` - Transformation time
- `bronze_silver_last_ledger_sequence` - Last processed ledger

## Configuration

### Service Configuration

| Option | Default | Description |
|--------|---------|-------------|
| `service.health_port` | 8093 | Health endpoint port |
| `service.transform_interval_minutes` | 15 | Transformation frequency |

### DuckLake Configuration

| Option | Required | Description |
|--------|----------|-------------|
| `ducklake.catalog_path` | Yes | PostgreSQL catalog connection string |
| `ducklake.catalog_name` | Yes | Catalog name (e.g., mainnet_catalog) |
| `ducklake.metadata_schema` | Yes | DuckLake metadata schema (e.g., bronze_meta) |
| `ducklake.bronze_schema` | Yes | Bronze schema name (source) |
| `ducklake.bronze_data_path` | Yes | S3 path to Bronze data |
| `ducklake.silver_schema` | Yes | Silver schema name (destination) |
| `ducklake.silver_data_path` | Yes | S3 path to Silver data |
| `ducklake.aws_access_key_id` | Yes | S3/B2 access key |
| `ducklake.aws_secret_access_key` | Yes | S3/B2 secret key |
| `ducklake.aws_region` | Yes | S3 region |
| `ducklake.aws_endpoint` | Yes | S3 endpoint URL |

### Checkpoint Configuration

| Option | Default | Description |
|--------|---------|-------------|
| `checkpoint.table` | silver_transformer_checkpoint | Checkpoint table name |
| `checkpoint.schema` | bronze | Schema for checkpoint table |

## Development

### Prerequisites

- Go 1.24+
- DuckDB Go bindings
- Access to Bronze DuckLake catalog

### Project Structure

```
bronze-silver-transformer/
├── go/
│   ├── main.go                    # Entry point
│   ├── config.go                  # Configuration loading
│   ├── duckdb.go                  # DuckDB catalog connection
│   ├── transformer.go             # Main transformation scheduler
│   ├── checkpoint.go              # Checkpoint management
│   ├── health.go                  # Health endpoint
│   ├── metrics.go                 # Prometheus metrics
│   └── transformations/
│       ├── current.go             # 10 current state tables
│       ├── snapshot.go            # 5 snapshot tables (SCD Type 2)
│       ├── enriched.go            # 2 enriched operations tables
│       └── analytics.go           # 1 token transfers table
├── examples/
│   ├── query_silver.go            # Go query examples
│   └── silver_queries.sql         # SQL query examples
├── config.yaml                    # Development config
├── config.test.yaml               # Test config
├── config.prod.yaml               # Production config template
├── Dockerfile                     # Docker build
├── docker-compose.yml             # Docker Compose deployment
├── bronze-silver-transformer.service  # Systemd service file
├── Makefile                       # Build and deployment tasks
├── README.md                      # This file
├── DEPLOYMENT_GUIDE.md            # Complete deployment guide
└── STELLAR_QUERY_API_INTEGRATION.md  # API integration guide
```

### Build and Test

```bash
# Build binary
make build

# Run tests
make test

# Run locally with test config
make run-test

# Format code
make fmt

# Lint code
make lint
```

## Troubleshooting

### Transformation Too Slow

**Problem:** Transformation takes > 5 minutes, can't keep up with 15-min interval

**Solutions:**
- Increase `transform_interval_minutes` to 30 or 60
- Increase CPU/memory resources (2-4 cores, 4-8GB RAM)
- Check Phase 3 enriched operations (slowest queries)

### Checkpoint Not Advancing

**Problem:** `last_ledger_sequence` stays the same

**Debug:**
```sql
-- Check Bronze max ledger
SELECT MAX(sequence) FROM catalog.bronze.ledgers_row_v2;

-- Check Silver checkpoint
SELECT * FROM catalog.bronze.silver_transformer_checkpoint;
```

**Solutions:**
- Verify Bronze data is flowing (check postgres-ducklake-flusher)
- Check service logs for errors
- Restart service if stuck

### Out of Memory

**Problem:** Service crashes with OOM errors

**Solutions:**
- Increase memory limit (4GB → 8GB)
- Reduce ledgers per batch (process more frequently with smaller batches)
- Optimize queries with filters

## Important Notes

### Single Instance Only

⚠️ **Do NOT run multiple instances** of bronze-silver-transformer on the same catalog:
- DuckLake writes are not concurrent-safe
- Multiple instances would cause race conditions and data corruption
- Only run **1 instance per network** (mainnet, testnet, etc.)

### Checkpoint Management

The checkpoint table tracks progress and enables restart:

```sql
-- View checkpoint
SELECT * FROM catalog.bronze.silver_transformer_checkpoint;

-- Reset to specific ledger (if needed)
UPDATE catalog.bronze.silver_transformer_checkpoint
SET last_ledger_sequence = 2000000;

-- Clear checkpoint (reprocess all from beginning)
DELETE FROM catalog.bronze.silver_transformer_checkpoint;
```

### Silver Layer Rebuild

If Silver tables need to be rebuilt:

1. Stop service
2. Drop Silver schema: `DROP SCHEMA catalog.silver CASCADE;`
3. Clear/reset checkpoint
4. Restart service (will recreate schema and reprocess)

## Contributing

When adding new Silver tables:

1. Create transformation in `go/transformations/`
2. Add to appropriate phase in `transformer.go`
3. Update this README with table description
4. Add example queries to `examples/silver_queries.sql`
5. Test with real Bronze data before deploying

## Support

- **Documentation:** See `DEPLOYMENT_GUIDE.md` and `STELLAR_QUERY_API_INTEGRATION.md`
- **Examples:** See `examples/` directory
- **Issues:** Check service logs and health endpoint first
- **Questions:** Review troubleshooting section above

## License

Same license as parent project.
