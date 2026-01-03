# Index Plane Transformer

Fast transaction hash index transformer for Obsrvr Lake. Reads from Silver (DuckLake) and writes to Index Plane (DuckLake) for <500ms transaction lookups at mainnet scale (60M+ ledgers).

## Overview

The Index Plane Transformer creates a fast lookup index from transaction hashes to ledger sequences:

```
Silver (enriched_operations) → Index Plane (tx_hash_index)
```

**Query Pattern:**
- **Without Index:** Scan entire Silver table → 10-30s for 60M ledgers
- **With Index:** Lookup in Index Plane → <500ms p95

## Architecture

```
┌──────────────────────┐
│ Silver (DuckLake/B2) │
│  enriched_operations │
└──────────┬───────────┘
           │ (read)
           ↓
    ┌──────────────┐
    │ Transformer  │
    │  - Poll 30s  │
    │  - Batch 10k │
    │  - Extract   │
    └──────┬───────┘
           │ (write)
           ↓
┌──────────────────────────┐
│ Index Plane (DuckLake/B2)│
│     tx_hash_index        │
│  (partitioned by range)  │
└──────────────────────────┘
```

## Features

- ✅ **Real-time indexing** - Polls Silver every 30s for new data
- ✅ **Checkpoint-based restart** - Resumes from last position
- ✅ **Partitioned storage** - 100k ledgers per partition for efficient queries
- ✅ **Parquet compression** - Snappy compression for optimal storage
- ✅ **Health monitoring** - HTTP health endpoint with metrics
- ✅ **Graceful shutdown** - Handles SIGINT/SIGTERM

## Quick Start

### Prerequisites

- Go 1.21+
- Access to Silver DuckLake (B2 bucket)
- PostgreSQL catalog database (for checkpoints)
- B2 credentials with access to Index bucket

### Installation

```bash
# Install dependencies
make deps

# Build
make build

# Run
make run
```

### Configuration

Copy `config.yaml` and set environment variables:

```bash
export B2_KEY_ID="your-b2-key-id"
export B2_KEY_SECRET="your-b2-secret"
export B2_SILVER_BUCKET="obsrvr-prod-testnet-silver"
export B2_INDEX_BUCKET="obsrvr-prod-testnet-index"
export CATALOG_HOST="your-catalog-host"
export CATALOG_DB="obsrvr_lake_catalog_prod"
export CATALOG_USER="your-user"
export CATALOG_PASSWORD="your-password"
```

### Run Locally

```bash
./index-plane-transformer -config config.yaml
```

## Configuration

See `config.yaml` for full configuration options.

### Key Settings

| Setting | Default | Description |
|---------|---------|-------------|
| `poll_interval_seconds` | 30 | How often to check for new data |
| `batch_size` | 10000 | Ledgers to process per cycle |
| `target_file_size_mb` | 128 | Target Parquet file size |
| `compression` | snappy | Parquet compression (snappy, gzip, zstd) |
| `health.port` | 8096 | Health endpoint port |

## Health Endpoint

Check service health:

```bash
curl http://localhost:8096/health
```

Response:
```json
{
  "status": "healthy",
  "last_ledger_processed": 45000000,
  "lag_seconds": 15,
  "transformations_total": 4500,
  "transformation_errors": 0,
  "last_transformation_duration_ms": 234
}
```

## Data Schema

### Input (Silver)

Reads from `silver.enriched_operations`:
```sql
SELECT
    transaction_hash,
    ledger_sequence,
    application_order,
    operation_count,
    tx_successful,
    ledger_closed_at
FROM silver.enriched_operations
GROUP BY transaction_hash, ledger_sequence, ...
```

### Output (Index Plane)

Writes to `index.tx_hash_index`:
```
tx_hash              VARCHAR    Primary lookup key
ledger_sequence      BIGINT     Ledger where tx appears
application_order    INTEGER    Order within ledger
operation_count      INTEGER    Number of operations
successful           BOOLEAN    Transaction success
closed_at            TIMESTAMP  Ledger close time
ledger_range         BIGINT     Partition key (seq / 100000)
created_at           TIMESTAMP  Index creation time
```

**Storage layout:**
```
s3://obsrvr-prod-testnet-index/tx_hash_index/
  ledger_range=1/part_*.parquet
  ledger_range=2/part_*.parquet
  ledger_range=3/part_*.parquet
  ...
```

## Checkpoint Management

Checkpoints stored in PostgreSQL catalog:

```sql
SELECT * FROM index.transformer_checkpoint;
```

**Restart behavior:**
1. Load last processed ledger from checkpoint
2. Resume from that ledger + 1
3. Process new data in batches
4. Update checkpoint after each successful batch

## Deployment

### Docker

```bash
# Build image
make docker-build

# Push to registry
make docker-push

# Run container
docker run -d \
  -e B2_KEY_ID="..." \
  -e B2_KEY_SECRET="..." \
  -e B2_SILVER_BUCKET="obsrvr-prod-testnet-silver" \
  -e B2_INDEX_BUCKET="obsrvr-prod-testnet-index" \
  -e CATALOG_HOST="..." \
  -e CATALOG_DB="..." \
  -e CATALOG_USER="..." \
  -e CATALOG_PASSWORD="..." \
  -p 8096:8096 \
  withobsrvr/index-plane-transformer:latest
```

### Nomad

See `.nomad/index-plane-transformer.nomad` for Nomad job definition.

**Resources:**
- CPU: 1000 MHz
- Memory: 2048 MB

**Deploy:**
```bash
nomad job run .nomad/index-plane-transformer.nomad
```

## Monitoring

### Logs

```bash
# Nomad
nomad alloc logs -f <alloc-id>

# Docker
docker logs -f <container-id>
```

### Metrics

Check health endpoint for metrics:
- `last_ledger_processed` - Latest indexed ledger
- `lag_seconds` - Time since last processing
- `transformations_total` - Total transformation cycles
- `transformation_errors` - Total errors encountered

### Performance

Expected performance:
- **Throughput:** ~10,000 ledgers per cycle (~30s)
- **Latency:** <500ms per transformation cycle
- **Lag:** <60s under normal operation

## Troubleshooting

### Service won't start

```bash
# Check configuration
./index-plane-transformer -config config.yaml

# Check database connectivity
psql "host=$CATALOG_HOST port=25060 user=$CATALOG_USER dbname=$CATALOG_DB"

# Check B2 credentials
b2 authorize-account "$B2_KEY_ID" "$B2_KEY_SECRET"
b2 list-buckets
```

### Checkpoint errors

```bash
# Check checkpoint table exists
psql "$CATALOG_DB" -c "SELECT * FROM index.transformer_checkpoint;"

# Manually reset checkpoint
psql "$CATALOG_DB" -c "UPDATE index.transformer_checkpoint SET last_ledger_sequence = 0 WHERE id = 1;"
```

### No new data processing

```bash
# Check Silver has data
# (requires DuckDB with Silver access)

# Check checkpoint vs Silver max ledger
curl http://localhost:8096/health | jq '.last_ledger_processed'
```

### High lag

Increase `batch_size` in config:
```yaml
service:
  batch_size: 20000  # Increase from 10000
```

## Development

### Run tests
```bash
make test
```

### Format code
```bash
make fmt
```

### Lint code
```bash
make lint
```

### Build locally
```bash
make build
```

## Related Services

- **silver-realtime-transformer** - Similar polling pattern (Bronze → Silver)
- **silver-cold-flusher** - DuckLake writer pattern
- **stellar-query-api** - Consumer of Index Plane

## Documentation

- `schema/README.md` - Schema documentation
- `INDEX_PLANE_IMPLEMENTATION_README.md` - Implementation guide
- `docs/shapes/index-plane-cycle1-tx-hash-index.md` - Shape Up document

## License

Proprietary - WithObsrvr Platform
