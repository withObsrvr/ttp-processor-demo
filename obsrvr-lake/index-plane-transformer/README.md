# Index Plane Transformer

Fast transaction hash index transformer for Obsrvr Lake. The current implementation reads from Bronze Hot PostgreSQL (`transactions_row_v2`) and writes a compact transaction-hash lookup index to Index Plane DuckLake for fast transaction resolution.

## Overview

The Index Plane Transformer creates a fast lookup index from transaction hashes to ledger sequences:

```
Bronze Hot PostgreSQL (transactions_row_v2) → Index Plane DuckLake (tx_hash_index)
```

The transformer processes ledger ranges incrementally using a checkpoint stored in PostgreSQL. Each cycle:
1. loads the last processed ledger from `index.transformer_checkpoint`
2. reads transactions from `transactions_row_v2`
3. writes index rows into DuckLake
4. advances the checkpoint after a successful batch

**Query Pattern:**
- **Without Index:** scan large historical transaction data to resolve a hash
- **With Index:** lookup in Index Plane, then jump directly to the target ledger

## Architecture

```
┌──────────────────────────────┐
│ Bronze Hot PostgreSQL        │
│     transactions_row_v2      │
└──────────────┬───────────────┘
               │ (read)
               ↓
       ┌──────────────┐
       │ Transformer  │
       │ - poll/grpc  │
       │ - batch 10k  │
       │ - checkpoint │
       └──────┬───────┘
              │ (write)
              ↓
┌──────────────────────────────┐
│ Index Plane DuckLake (B2)    │
│        index.tx_hash_index   │
│   partitioned by ledger_range│
└──────────────────────────────┘

Checkpoint store: PostgreSQL table `index.transformer_checkpoint`
```

## Features

- ✅ **Near-real-time indexing** - Polls Bronze Hot every 30s by default, with optional gRPC trigger mode
- ✅ **Checkpoint-based restart** - Resumes from last processed ledger using PostgreSQL checkpoint state
- ✅ **Partitioned storage** - 100k ledgers per partition for efficient queries
- ✅ **Parquet compression** - Snappy compression for optimal storage
- ✅ **Health monitoring** - HTTP health endpoint with metrics
- ✅ **Graceful shutdown** - Handles SIGINT/SIGTERM

## Quick Start

### Prerequisites

- Go 1.21+
- Access to Bronze Hot PostgreSQL (`stellar_hot`)
- PostgreSQL database containing `index.transformer_checkpoint`
- B2 credentials with access to the Index bucket

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
export PG_HOT_HOST="your-bronze-hot-host"
export PG_HOT_USER="your-bronze-hot-user"
export PG_HOT_PASSWORD="your-bronze-hot-password"
export B2_KEY_ID="your-b2-key-id"
export B2_KEY_SECRET="your-b2-secret"
export B2_INDEX_BUCKET="obsrvr-prod-testnet-index"
```

Important current behavior:
- source reads come from `bronze_hot`
- the index target is DuckLake on B2
- the current `main.go` opens the checkpoint/catalog connection using the same PostgreSQL connection settings as `bronze_hot`

### Run Locally

```bash
./index-plane-transformer -config config.yaml
```

## Configuration

See `config.yaml` for full configuration options.

### Key Settings

| Setting | Default | Description |
|---------|---------|-------------|
| `service.poll_interval_seconds` | 30 | How often to poll Bronze Hot when not using gRPC |
| `service.batch_size` | 10000 | Ledgers to process per cycle |
| `service.allow_retention_gap_start` | `false` | Fail fast on hot-retention gaps unless explicitly overridden |
| `bronze_source.mode` | `poll` | Trigger mode: `poll` or `grpc` |
| `checkpoint.table` | `index.transformer_checkpoint` | PostgreSQL checkpoint table |
| `index_cold.partition_size` | 100000 | Ledger partition size for DuckLake data |
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

### Input (Bronze Hot)

Reads from `transactions_row_v2`:
```sql
SELECT
    transaction_hash,
    ledger_sequence,
    operation_count,
    successful,
    created_at
FROM transactions_row_v2
WHERE ledger_sequence > $1 AND ledger_sequence <= $2
ORDER BY ledger_sequence, transaction_hash
```

### Output (Index Plane)

Writes to `index.tx_hash_index`:
```
tx_hash              VARCHAR    Primary lookup key
ledger_sequence      BIGINT     Ledger where tx appears
operation_count      INTEGER    Number of operations
successful           BOOLEAN    Transaction success
closed_at            TIMESTAMP  Ledger close/creation time from source row
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

Checkpoints are stored in PostgreSQL:

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
# Check Bronze Hot has data
psql "$BRONZE_HOT_DB" -c "SELECT MAX(ledger_sequence) FROM transactions_row_v2;"

# Check checkpoint progress
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

## Operational notes

### Current limitations

- The transformer only reads from Bronze Hot PostgreSQL. If old ledgers have been evicted from hot storage, this service cannot backfill them from Bronze Cold/DuckLake by itself.
- The current write path is append-oriented. Safe operation depends on monotonic checkpoint progression and avoiding accidental replay of already-indexed ranges.
- The current `main.go` uses the same PostgreSQL connection settings for both source reads and checkpoint/catalog access.

### Recommended operational safeguards

1. Keep checkpoint state persistent and treat manual checkpoint resets as a controlled recovery operation.
2. Alert if checkpoint falls behind the oldest retained ledger in Bronze Hot.
3. Prefer one active writer instance per network/index table. The transformer now acquires a PostgreSQL advisory lock on startup and exits if another writer already holds it.
4. Replay-safe writes are enabled on the target insert path: rows whose `tx_hash` already exists in the target table are skipped so a crash after target writes but before checkpoint save will not duplicate those rows on restart.
5. By default the transformer now fails fast on retention gaps. To intentionally begin from the currently retained hot window and accept missing older history, set `service.allow_retention_gap_start: true`.

## Related Services

- **silver-realtime-transformer** - similar incremental hot-path pattern
- **contract-event-index-transformer** - another hot-to-DuckLake index builder
- **stellar-query-api** - consumer of Index Plane

## Documentation

- `schema/README.md` - Schema documentation
- `INDEX_PLANE_IMPLEMENTATION_README.md` - Implementation guide
- `docs/shapes/index-plane-cycle1-tx-hash-index.md` - Shape Up document

## License

Proprietary - WithObsrvr Platform
