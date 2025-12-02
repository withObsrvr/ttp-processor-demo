# Flowctl Pipeline Configurations

This directory contains flowctl pipeline configurations for processing Stellar contract events.

## Quick Start (Recommended)

**The easiest way to run the pipeline is with the provided script:**

```bash
# Start pipeline (processes from ledger 1000 onwards)
./run-pipeline.sh

# Start from specific ledger
./run-pipeline.sh 5000 0

# Stop pipeline
./stop-pipeline.sh
```

See [RUNNING_PIPELINE.md](RUNNING_PIPELINE.md) for detailed documentation.

---

## Available Pipelines

### 1. Historical Data Pipeline (Datalake Source)
**File**: `contract-events-flowctl-pipeline.secret.yaml`

Processes historical contract events from archived ledger data in GCS.

**Architecture**:
```
GCS Storage → stellar-live-source-datalake → contract-events-processor → postgres-consumer → PostgreSQL
```

**Use Cases**:
- Backfilling historical contract event data
- Reprocessing specific ledger ranges
- Development/testing with reproducible data
- Cost-effective bulk processing (no RPC rate limits)

**Prerequisites**:
```bash
# 1. GCS credentials
gcloud auth application-default login

# 2. PostgreSQL database setup
createdb contract_events
psql -d contract_events -f contract-events-postgres-consumer/config/schema/contract_events.sql

# 3. Build Docker images
cd stellar-live-source-datalake && make docker-build
cd ../contract-events-processor && make docker-build
cd ../contract-events-postgres-consumer && make docker-build
```

**Run**:
```bash
# Must use container orchestrator (process mode doesn't work with Nix images)
flowctl run contract-events-flowctl-pipeline.secret.yaml --orchestrator container
```

---

### 2. Live Streaming Pipeline (RPC Source)
**File**: `contract-events-flowctl-pipeline-live.yaml`

Processes contract events in real-time from Stellar RPC.

**Architecture**:
```
Stellar RPC → stellar-live-source → contract-events-processor → postgres-consumer → PostgreSQL
```

**Use Cases**:
- Real-time contract event monitoring
- Live analytics and dashboards
- Production event streaming
- Monitoring specific contracts

**Prerequisites**:
```bash
# 1. PostgreSQL database setup
createdb contract_events
psql -d contract_events -f contract-events-postgres-consumer/config/schema/contract_events.sql

# 2. Build Docker images
cd stellar-live-source && make docker-build
cd ../contract-events-processor && make docker-build
cd ../contract-events-postgres-consumer && make docker-build
```

**Run**:
```bash
# Must use container orchestrator (process mode doesn't work with Nix images)
flowctl run contract-events-flowctl-pipeline-live.yaml --orchestrator container
```

---

## Docker Networking in Flowctl

When using `--orchestrator container`, flowctl uses **host network mode** for containers:

### Host Network Mode
All containers share the host's network namespace, which means:
- **Use `localhost`** for all service-to-service communication
- **Use `localhost`** to access host services (control plane, PostgreSQL)
- Services can bind to and access any port on the host network
- No container-specific DNS names or special hostnames needed

### Example Addresses:
- **Data Source**: `localhost:50052`
- **Processor**: `localhost:50053`
- **Control Plane**: `localhost:8080` (flowctl runs on host)
- **PostgreSQL**: `localhost:5432` (PostgreSQL runs on host)

### Why Host Network Mode?
Host network mode simplifies networking and eliminates the need for:
- Port mappings (`-p 8080:8080`)
- Container DNS resolution
- Special hostnames like `host.docker.internal`
- Custom Docker networks

---

## Flowctl Feature Support

### ✅ Currently Supported
- **Health Checks**: Each component specifies `health_endpoint` and `health_port`
- **Service Registration**: Components register with flowctl control plane
- **Heartbeat Metrics**: Services send periodic heartbeats with processing stats
- **Component Dependencies**: Define execution order with `inputs` field
- **Environment Variables**: Configure services via `env` map
- **Docker Images**: Pull and run OCI container images
- **Resource Limits**: Set CPU/memory constraints (via Docker)

### 🚧 Planned but Not Yet Implemented
- **Preflight Checks**: No automated pre-execution validation
- **Monitoring Queries**: No built-in SQL query execution
- **Alerts**: No alert definitions or handling
- **Prometheus Export**: Metrics collected but not exported to Prometheus

### Manual Workarounds

Until these features are implemented, use manual checks:

```bash
# Before running pipeline:
# 1. Verify PostgreSQL schema
psql -d contract_events -c "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'contract_events'"

# 2. Check credentials
ls -l ~/.config/gcloud/application_default_credentials.json

# During pipeline execution:
# 3. Monitor with SQL queries
watch -n 5 "psql -d contract_events -c 'SELECT COUNT(*) FROM contract_events'"

# 4. Check component health
watch -n 10 "curl -s http://localhost:8081/health && echo && curl -s http://localhost:8089/health && echo && curl -s http://localhost:8090/health"
```

---

## Pipeline Components

### Source: stellar-live-source-datalake
- **Purpose**: Reads ledgers from cloud storage (GCS/S3/B2)
- **Output**: Raw ledger data via gRPC
- **Port**: 50052 (gRPC), 8081 (health)

### Source: stellar-live-source
- **Purpose**: Streams ledgers from Stellar RPC
- **Output**: Raw ledger data via gRPC
- **Port**: 50052 (gRPC), 8081 (health)

### Processor: contract-events-processor
- **Purpose**: Extracts and filters Soroban contract events
- **Input**: Raw ledger data
- **Output**: Filtered contract events via gRPC
- **Port**: 50053 (gRPC), 8089 (health)

### Sink: contract-events-postgres-consumer
- **Purpose**: Saves contract events to PostgreSQL
- **Input**: Contract events stream
- **Output**: PostgreSQL database
- **Port**: 8090 (health)

---

## Configuration Options

### Filtering Contract Events

Edit the pipeline YAML to filter events by contract ID or event type:

```yaml
env:
  # Filter by specific contract IDs (comma-separated)
  FILTER_CONTRACT_IDS: "CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC,CAF3..."

  # Filter by event types (comma-separated)
  FILTER_EVENT_TYPES: "transfer,mint,burn,swap"

  # Include failed transactions (default: false)
  INCLUDE_FAILED: "true"
```

### Ledger Range Processing

Specify the ledger range in the postgres-consumer args:

```yaml
# Process ledgers 1000-2000
args: ["1000", "2000"]

# Process from 1000 onwards continuously
args: ["1000", "0"]

# Start from current ledger and stream
args: ["0", "0"]
```

### Storage Backend (Datalake Source)

Change the storage backend by modifying environment variables:

```yaml
# Google Cloud Storage
STORAGE_TYPE: "GCS"
BUCKET_NAME: "my-bucket"
GOOGLE_APPLICATION_CREDENTIALS: "/path/to/credentials.json"

# AWS S3
STORAGE_TYPE: "S3"
BUCKET_NAME: "my-bucket"
AWS_REGION: "us-west-2"
AWS_ACCESS_KEY_ID: "..."
AWS_SECRET_ACCESS_KEY: "..."

# Backblaze B2
STORAGE_TYPE: "S3"
BUCKET_NAME: "my-bucket"
S3_ENDPOINT: "s3.us-west-004.backblazeb2.com"
AWS_ACCESS_KEY_ID: "..."
AWS_SECRET_ACCESS_KEY: "..."

# Local filesystem
STORAGE_TYPE: "FS"
BUCKET_PATH: "/path/to/ledgers"
```

---

## Monitoring

### Health Checks

All services expose health endpoints:

```bash
# Source health
curl http://localhost:8081/health

# Processor health
curl http://localhost:8089/health

# Consumer health
curl http://localhost:8090/health
```

### Metrics

Check processing stats:

```bash
# Events processed
psql -d contract_events -c "SELECT COUNT(*) FROM contract_events"

# Latest ledger
psql -d contract_events -c "SELECT MAX(ledger_sequence) FROM contract_events"

# Events by contract
psql -d contract_events -c "
  SELECT contract_id, COUNT(*) as event_count
  FROM contract_events
  GROUP BY contract_id
  ORDER BY event_count DESC
  LIMIT 10
"

# Events by type
psql -d contract_events -c "
  SELECT event_type, COUNT(*) as event_count
  FROM contract_events
  GROUP BY event_type
  ORDER BY event_count DESC
"
```

### Flowctl Dashboard

If flowctl control plane is running:

```bash
# View pipeline status
flowctl status contract-events-pipeline

# View service metrics
flowctl metrics contract-events-processor
flowctl metrics postgres-consumer

# View logs
flowctl logs contract-events-processor
flowctl logs postgres-consumer
```

---

## Troubleshooting

### Pipeline not starting

1. Check Docker images are built:
   ```bash
   docker images | grep withobsrvr
   ```

2. Verify PostgreSQL is accessible:
   ```bash
   psql -d contract_events -c "SELECT 1"
   ```

3. Check schema is initialized:
   ```bash
   psql -d contract_events -c "\dt contract_events"
   ```

### No events being processed

1. Check if source is producing ledgers:
   ```bash
   curl http://localhost:8081/health
   ```

2. Verify processor is receiving ledgers:
   ```bash
   curl http://localhost:8089/health
   ```

3. Check consumer is connected:
   ```bash
   curl http://localhost:8090/health
   ```

4. Ensure ledgers contain contract events:
   ```bash
   # Not all ledgers have contract events
   # Try a range with known contract activity
   ```

### GCS credentials error

```bash
# Re-authenticate
gcloud auth application-default login

# Or set explicit path
export GOOGLE_APPLICATION_CREDENTIALS="/home/user/.config/gcloud/application_default_credentials.json"
```

### PostgreSQL connection error

```bash
# Check PostgreSQL is running
sudo systemctl status postgresql

# Check database exists
psql -l | grep contract_events

# Recreate if needed
dropdb contract_events
createdb contract_events
psql -d contract_events -f contract-events-postgres-consumer/config/schema/contract_events.sql
```

---

## Example Queries

### Recent transfer events
```sql
SELECT
  contract_id,
  event_type,
  topics,
  event_data,
  ledger_closed_at
FROM contract_events
WHERE event_type = 'transfer'
ORDER BY ledger_closed_at DESC
LIMIT 10;
```

### Events for a specific contract
```sql
SELECT
  event_type,
  COUNT(*) as count,
  MIN(ledger_closed_at) as first_seen,
  MAX(ledger_closed_at) as last_seen
FROM contract_events
WHERE contract_id = 'CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC'
GROUP BY event_type
ORDER BY count DESC;
```

### Events in a transaction
```sql
SELECT
  event_index,
  event_type,
  contract_id,
  topics,
  event_data
FROM contract_events
WHERE tx_hash = 'abc123...'
ORDER BY event_index;
```

### JSONB queries (find events with specific topic values)
```sql
-- Find transfer events where first topic (from) equals a specific address
SELECT *
FROM contract_events
WHERE event_type = 'transfer'
  AND topics->0->>'address' = 'GDQP2KPQGKIHYJGXNUIYOMHARUARCA7DJT5FO2FFOOKY3B2WSQHG4W37';
```

---

## Production Recommendations

1. **Use connection pooling**: Configure PostgreSQL max_connections appropriately
2. **Enable monitoring**: Set up Prometheus metrics collection
3. **Configure alerts**: Set up alerts for processing lag
4. **Backup database**: Regular pg_dump backups of contract_events
5. **Index tuning**: Monitor query patterns and add indexes as needed
6. **Partitioning**: For large datasets, consider partitioning by ledger_sequence
7. **Resource limits**: Set appropriate memory/CPU limits in production
8. **Credentials management**: Use secrets manager instead of plaintext credentials

---

## Related Documentation

- [Contract Events Processor README](contract-events-processor/README.md)
- [PostgreSQL Consumer README](contract-events-postgres-consumer/README.md)
- [Stellar Live Source Datalake](stellar-live-source-datalake/README.md)
- [Flowctl Documentation](https://github.com/withObsrvr/flowctl)
