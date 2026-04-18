# Contract Event Index Transformer

Fast lookup index for finding which ledgers contain events from specific Stellar smart contracts.

## Overview

The Contract Event Index Transformer is a continuous indexing service that:
- Polls Bronze Hot (`contract_events_stream_v1`) every 30 seconds by default, with optional gRPC trigger mode
- Groups events by `(contract_id, ledger_sequence)`
- Writes aggregated index to DuckLake (Parquet on B2)
- Uses replay-safe inserts so repeated batches skip already indexed contract/ledger pairs
- Acquires a PostgreSQL advisory lock so only one writer instance is active at a time
- Detects Bronze Hot retention gaps and fails fast unless explicitly overridden
- Enables O(1) lookups: "Which ledgers have events from contract X?"

## Architecture

```
Bronze Hot (PostgreSQL)          DuckLake (B2/Parquet)
contract_events_stream_v1  →  index.contract_events_index
       ↓ poll every 30s
[contract-event-index-transformer]
       ↓ WriteBatch + CHECKPOINT
```

## Use Case

**Alpha User: Convergence (Stellarcarbon)**
- Continuous auditing of carbon credit contract events
- Cursor-based pagination through all ledgers containing contract activity
- Public transparency without API keys

**Example Query:**
"Which ledgers contain events from contract STELLARCARBON...?"
→ Returns: `[276592, 276610, 276625, ..., 283820]`

## Components

### Source Files

- **main.go** - Entrypoint with signal handling
- **transformer.go** - Polling loop and orchestration
- **bronze_reader.go** - Reads from Bronze Hot PostgreSQL
- **index_writer.go** - Writes to DuckLake with CHECKPOINT
- **checkpoint.go** - PostgreSQL-backed checkpoint management
- **config.go** - Configuration structs
- **health.go** - HTTP health endpoint

### Key Features

- **Polling Loop / gRPC Triggering**: 30-second interval by default, or Bronze SourceService event-driven mode
- **Batch Processing**: 1000 ledgers per batch (configurable)
- **PostgreSQL Checkpoint**: Resumes from catalog-backed checkpoint state
- **Singleton Writer Protection**: PostgreSQL advisory lock prevents overlapping writer instances
- **Replay-Safe Writes**: existing `(contract_id, ledger_sequence)` rows are skipped on replay
- **Retention Gap Detection**: fails fast by default when Bronze Hot no longer contains history required by the checkpoint
- **Partitioning**: Automatic by ledger_range (ledger_sequence / 100000)
- **Health Endpoint**: HTTP :8096/health with richer source/checkpoint stats

## Configuration

### config.yaml

```yaml
service:
  name: contract-event-index-transformer
  version: 1.0.0
  allow_retention_gap_start: false

bronze_source:
  mode: poll
  endpoint: localhost:50054

bronze_hot:
  host: "${PG_BRONZE_HOT_HOST}"
  port: 25060
  database: "${PG_BRONZE_HOT_DB}"
  user: "${PG_BRONZE_HOT_USER}"
  password: "${PG_BRONZE_HOT_PASSWORD}"
  sslmode: require

catalog:
  host: "${PG_CATALOG_HOST}"
  port: 25060
  database: "${PG_CATALOG_DB}"
  user: "${PG_CATALOG_USER}"
  password: "${PG_CATALOG_PASSWORD}"
  sslmode: require

s3:
  access_key_id: "${B2_KEY_ID}"
  secret_access_key: "${B2_KEY_SECRET}"
  endpoint: "s3.us-west-004.backblazeb2.com"
  region: "us-west-004"
  bucket: "${B2_BUCKET}"

indexing:
  poll_interval: "30s"
  batch_size: 1000
  checkpoint_table: "index.contract_event_transformer_checkpoint"

health:
  port: 8096
```

## Building

### Local Build

```bash
make build
# Creates: ./contract-event-index-transformer
```

### Docker Build

```bash
make docker-build
# Creates: withobsrvr/contract-event-index-transformer:latest
```

### Dependencies

```bash
make deps
# Installs:
# - github.com/duckdb/duckdb-go/v2
# - github.com/lib/pq
# - gopkg.in/yaml.v3
```

## Running

### Local

```bash
# Set environment variables
export PG_BRONZE_HOT_HOST=localhost
export PG_BRONZE_HOT_DB=stellar_hot
export PG_BRONZE_HOT_USER=user
export PG_BRONZE_HOT_PASSWORD=pass
export PG_CATALOG_HOST=localhost
export PG_CATALOG_DB=catalog
export PG_CATALOG_USER=user
export PG_CATALOG_PASSWORD=pass
export B2_KEY_ID=your_key
export B2_KEY_SECRET=your_secret
export B2_BUCKET=your_bucket

# Run
./contract-event-index-transformer -config config.yaml
```

### Docker

```bash
docker run --rm -it \
  -e PG_BRONZE_HOT_HOST=host \
  -e PG_BRONZE_HOT_DB=stellar_hot \
  -e PG_BRONZE_HOT_USER=user \
  -e PG_BRONZE_HOT_PASSWORD=pass \
  -e PG_CATALOG_HOST=host \
  -e PG_CATALOG_DB=catalog \
  -e PG_CATALOG_USER=user \
  -e PG_CATALOG_PASSWORD=pass \
  -e B2_KEY_ID=key \
  -e B2_KEY_SECRET=secret \
  -e B2_BUCKET=bucket \
  withobsrvr/contract-event-index-transformer:latest
```

## Schema

### contract_events_index Table

```sql
CREATE TABLE IF NOT EXISTS index.contract_events_index (
    contract_id VARCHAR NOT NULL,
    ledger_sequence BIGINT NOT NULL,
    event_count INTEGER NOT NULL,
    first_seen_at TIMESTAMP NOT NULL,
    ledger_range BIGINT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (contract_id, ledger_sequence)
);
```

**Fields:**
- `contract_id`: Contract address (e.g., CABC123...)
- `ledger_sequence`: Ledger number containing events
- `event_count`: Number of events from this contract in this ledger
- `first_seen_at`: When this pair was first indexed
- `ledger_range`: Partition key = ledger_sequence / 100000
- `created_at`: Index creation timestamp

## Operational safeguards

- **Fail fast on retention gap by default**: if Bronze Hot has already aged beyond the saved checkpoint, startup/catch-up fails unless `service.allow_retention_gap_start: true` is set.
- **Catalog-backed checkpointing**: checkpoint state now lives in PostgreSQL (`index.contract_event_transformer_checkpoint`) instead of a local file, so reschedules do not lose progress.
- **Intentional bootstrap override**: set `allow_retention_gap_start: true` only when you knowingly want to start from the currently retained hot window and accept missing older history.
- **No further silent gaps after startup**: once running and caught up, the transformer should continue from Hot without creating new gaps as long as it stays healthy.
- **Practical note**: the catalog database user must be able to create/update `index.contract_event_transformer_checkpoint`. In normal operation this is handled by `schema/contract_events_index.sql` during schema init. If the table is missing after a reset, apply that schema file before restarting the transformer.

## Health Endpoint

### GET /health

```bash
curl http://localhost:8096/health | jq
```

**Response:**
```json
{
  "status": "healthy",
  "transformer": "contract-event-index",
  "checkpoint": {
    "last_ledger": 283820,
    "last_updated": "2025-01-03T10:30:05Z"
  },
  "index": {
    "total_contract_ledger_pairs": 12543,
    "unique_contracts": 87,
    "min_ledger": 276592,
    "max_ledger": 283820,
    "ledger_coverage": 7228,
    "last_updated": "2025-01-03T10:40:00Z"
  },
  "uptime_seconds": 120,
  "stats": {
    "transformations_total": 10,
    "transformation_errors": 0,
    "last_transform_duration_ms": 234,
    "lag_seconds": 5
  }
}
```

## Data Flow

1. **Poll Bronze Hot**:
   ```sql
   SELECT contract_id, ledger_sequence, COUNT(*) as event_count
   FROM bronze.contract_events_stream_v1
   WHERE ledger_sequence > $checkpoint
   GROUP BY contract_id, ledger_sequence
   ORDER BY ledger_sequence
   ```

2. **Transform to Index Rows**:
   ```go
   ContractEventIndexRow{
       ContractID:     "CABC123...",
       LedgerSequence: 283820,
       EventCount:     3,
       FirstSeenAt:    time.Now(),
       LedgerRange:    2, // 283820 / 100000
   }
   ```

3. **Write to DuckLake**:
   ```go
   indexWriter.WriteBatch(ctx, rows)
   indexWriter.Checkpoint() // Flush to Parquet
   ```

4. **Update Checkpoint**:
   ```sql
   UPDATE index.contract_event_transformer_checkpoint
   SET last_ledger_sequence = 283820,
       last_processed_at = NOW()
   WHERE id = 1;
   ```

## Performance

- **Throughput**: ~1000 ledgers per batch (30 seconds)
- **Latency**: Eventually consistent (30-second lag max)
- **Dataset Size**: ~MB per year per contract
- **Memory**: 2GB for DuckDB operations
- **Storage**: Parquet files on B2 (compressed)

## Monitoring

### Logs

```bash
# Successful indexing
✅ Indexed 145 contract-ledger pairs (ledgers 283797→283820)

# No new data
ℹ️  No contract events found in ledger range 283821-283850

# Errors
❌ Transformation error: failed to query contract events: ...
```

### Metrics (via Health Endpoint)

- `transformations_total`: Total successful transformation cycles
- `transformation_errors`: Total errors
- `last_transform_duration_ms`: Duration of last cycle
- `lag_seconds`: Time since last processing

## Troubleshooting

### No Data Being Indexed

**Check Bronze Hot:**
```sql
SELECT COUNT(*) FROM bronze.contract_events_stream_v1;
SELECT MAX(ledger_sequence) FROM bronze.contract_events_stream_v1;
```

**Check Checkpoint:**
```sql
SELECT * FROM index.contract_event_transformer_checkpoint;
```

**Reset Checkpoint:**
```sql
UPDATE index.contract_event_transformer_checkpoint
SET last_ledger_sequence = 0,
    last_processed_at = NOW()
WHERE id = 1;
```

### CHECKPOINT Errors

**Common causes:**
- S3 credentials invalid
- S3 bucket doesn't exist
- Network connectivity to B2

**Fix:**
```bash
# Verify S3 access
aws s3 ls s3://your-bucket/ --endpoint-url=https://s3.us-west-004.backblazeb2.com
```

### High Memory Usage

**Solution:**
- Reduce `batch_size` in config
- Increase memory allocation in Nomad job

## Development

### Run Tests

```bash
make test
```

### Format Code

```bash
make fmt
```

### Lint Code

```bash
make lint
```

## Deployment

See [DEPLOYMENT.md](DEPLOYMENT.md) for production deployment instructions.

## Integration

### Query API

After indexing, use stellar-query-api to query the index:

```bash
# Get ledgers for a contract
curl "http://localhost:8092/api/v1/index/contracts/CABC123.../ledgers"

# Response
{
  "contract_id": "CABC123...",
  "ledgers": [276592, 276610, 276625],
  "total": 3
}
```

See [stellar-query-api/CONTRACT_INDEX_API.md](../stellar-query-api/CONTRACT_INDEX_API.md) for full API documentation.

## License

Proprietary - Obsrvr Lake Platform

## Support

- GitHub Issues: https://github.com/withobsrvr/obsrvr-lake/issues
- Documentation: https://docs.obsrvr.com
