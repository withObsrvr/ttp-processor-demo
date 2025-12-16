# Stellar PostgreSQL Ingester

**Cycle 2: Hot Buffer Ingestion Service**

High-performance ingester that streams Stellar blockchain data from `stellar-live-source-datalake` and writes to PostgreSQL hot buffer.

## Architecture

```
stellar-live-source-datalake (gRPC :50053)
    ↓ StreamRawLedgers
stellar-postgres-ingester (THIS)
    ↓ Batch INSERT
PostgreSQL Hot Buffer (:5434/stellar_hot)
```

## Features

- **gRPC Streaming**: Consumes raw ledgers from stellar-live-source-datalake
- **Batch Processing**: Configurable batch size (default: 50 ledgers)
- **Checkpointing**: File-based checkpoint for crash recovery
- **Health Endpoint**: HTTP /health and /metrics endpoints
- **Retry Logic**: Automatic retry on transient errors
- **Performance**: Processes 100+ ledgers/sec with UNLOGGED tables

## Quick Start

### Prerequisites

1. **PostgreSQL Hot Buffer** running (from Cycle 1):
   ```bash
   cd ../postgres-hot-buffer
   docker-compose up -d
   ```

2. **stellar-live-source-datalake** running:
   ```bash
   cd ../stellar-live-source-datalake
   # Start the data source service
   ```

### Configuration

Copy example config:
```bash
cp config.yaml.example config.yaml
```

Edit `config.yaml` to match your setup:
```yaml
source:
  endpoint: "localhost:50053"
  network_passphrase: "Test SDF Network ; September 2015"
  start_ledger: 1000000  # Starting ledger

postgres:
  host: localhost
  port: 5434
  database: stellar_hot
  batch_size: 50
```

### Build and Run

```bash
# Build
make build

# Run
make run

# Or run with example config
make run-example
```

## Usage

### Command Line

```bash
./bin/stellar-postgres-ingester -config config.yaml
```

### Health Check

```bash
# Health endpoint
curl http://localhost:8088/health

# Prometheus metrics
curl http://localhost:8088/metrics
```

### Monitoring

Health response includes:
- `last_ledger`: Last processed ledger sequence
- `total_ledgers`: Total ledgers ingested
- `total_transactions`: Total transactions processed
- `total_operations`: Total operations processed
- `processing_rate_ledgers_per_sec`: Current ingestion rate
- `error_count`: Total errors encountered

## Configuration Reference

### Service

- `service.name`: Service name for logging
- `service.health_port`: HTTP health endpoint port (default: 8088)

### Source

- `source.endpoint`: stellar-live-source-datalake gRPC endpoint
- `source.network_passphrase`: Stellar network passphrase
- `source.start_ledger`: Starting ledger sequence
- `source.end_ledger`: Ending ledger (0 = continuous)

### PostgreSQL

- `postgres.host`: PostgreSQL host
- `postgres.port`: PostgreSQL port
- `postgres.database`: Database name
- `postgres.user`: Database user
- `postgres.password`: Database password
- `postgres.sslmode`: SSL mode (disable/require)
- `postgres.batch_size`: Ledgers per batch (default: 50)
- `postgres.commit_interval_seconds`: Auto-commit interval (default: 5)
- `postgres.max_retries`: Retry attempts on errors (default: 3)

### Checkpoint

- `checkpoint.file_path`: Path to checkpoint file

## Checkpoint Format

Checkpoint file (`checkpoint.json`):
```json
{
  "last_ledger": 1000500,
  "last_ledger_hash": "abc123...",
  "last_ledger_range": 1000000,
  "last_update_time": "2025-12-13T12:34:56Z",
  "total_ledgers": 500,
  "total_transactions": 1234,
  "total_operations": 5678
}
```

## Current Status (Cycle 2 - In Progress)

### ✅ Implemented (Ledgers Only)

- [x] gRPC client to stellar-live-source-datalake
- [x] Batch INSERT for `ledgers_row_v2` table
- [x] File-based checkpointing
- [x] Health endpoint with metrics
- [x] Retry logic with exponential backoff
- [x] Graceful shutdown

### 🚧 In Progress

- [ ] Extend to all 19 tables:
  - [ ] Transactions table
  - [ ] Operations table
  - [ ] Effects table
  - [ ] Trades table
  - [ ] 7 Snapshot tables
  - [ ] 7 Soroban tables

### 📋 Planned

- [ ] Prometheus metrics endpoint
- [ ] Configurable flush helpers integration
- [ ] Multi-threaded batch processing
- [ ] Backpressure handling

## Performance

**Current (Ledgers Only):**
- Throughput: ~100 ledgers/sec
- Batch size: 50 ledgers
- Commit interval: 5 seconds
- Memory: ~50MB

**Expected (All Tables):**
- Throughput: ~50-75 ledgers/sec
- Memory: ~200-500MB

## Troubleshooting

### Connection refused to PostgreSQL

**Problem**: Cannot connect to PostgreSQL.

**Solution**: Ensure PostgreSQL hot buffer is running:
```bash
cd ../postgres-hot-buffer
docker-compose ps
docker-compose up -d
```

### Connection refused to gRPC source

**Problem**: Cannot connect to stellar-live-source-datalake.

**Solution**: Start the data source:
```bash
cd ../stellar-live-source-datalake
# Start the service
```

### Checkpoint file permission denied

**Problem**: Cannot write checkpoint file.

**Solution**: Create directory with proper permissions:
```bash
sudo mkdir -p /var/lib/stellar-postgres-ingester
sudo chown $USER /var/lib/stellar-postgres-ingester
```

Or use local path in config:
```yaml
checkpoint:
  file_path: "./checkpoint.json"
```

### Slow ingestion rate

**Problem**: Processing < 10 ledgers/sec.

**Possible causes**:
1. Batch size too small - increase `postgres.batch_size`
2. Network latency - move ingester closer to PostgreSQL
3. Disk I/O bottleneck - check PostgreSQL server disk usage

## Development

### Build

```bash
make build
```

### Test

```bash
make test
```

### Format Code

```bash
make fmt
```

### Lint

```bash
make vet
```

## Next Steps (Cycle 2 Continuation)

1. **Extend to all 19 tables** - Add extraction and INSERT logic
2. **Benchmark performance** - Measure throughput with all tables
3. **Integration testing** - End-to-end test with real data
4. **Documentation** - Complete API and troubleshooting docs

## References

- Cycle 2 Shape Up Pitch: `../shape-up/cycle-2-hot-ingestion-service.md`
- PostgreSQL Schema: `../postgres-hot-buffer/README.md`
- Data Source: `../stellar-live-source-datalake/README.md`

## License

MIT
