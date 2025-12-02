# PostgreSQL Consumer for Contract Events

Save Soroban contract events to PostgreSQL with a hybrid schema: indexed columns for hot paths + JSONB for flexible queries.

## Features

✅ **Hybrid Schema** - Indexed metadata + JSONB payload
✅ **No Schema in Go** - Schema managed by SQL files (Shape Up pattern)
✅ **Flexible Queries** - Query any field via JSONB + GIN indexes
✅ **Docker Auto-Init** - PostgreSQL schema auto-created on startup
✅ **Production Ready** - Connection pooling, health checks, stats
✅ **21+ Example Queries** - Analytics, time-series, aggregations

## Architecture Pattern

```
Infrastructure → CREATE TABLE (schema.sql)
Application    → INSERT (Go consumer)
Query Layer    → SELECT (JSONB queries)
```

**Key principle:** Infrastructure creates schema, Go only inserts data.

## Quick Start

### Option 1: Docker Compose (Recommended)

```bash
# Start PostgreSQL only
docker-compose up postgres -d

# Wait for schema initialization
sleep 3

# Build and run consumer
make build
CONTRACT_EVENTS_SERVICE_ADDRESS=localhost:50053 \
POSTGRES_HOST=localhost \
POSTGRES_PORT=5433 \
./contract-events-postgres-consumer 1000 1100
```

### Option 2: Full Stack with Docker Compose

```bash
# Start everything (stellar-live-source + processor + postgres + consumer)
docker-compose --profile with-source --profile with-consumer up -d

# Check logs
docker-compose logs -f postgres-consumer
```

### Option 3: Local PostgreSQL

```bash
# 1. Create database
createdb contract_events

# 2. Initialize schema
psql -d contract_events -f config/schema/contract_events.sql

# 3. Build consumer
make build

# 4. Run
POSTGRES_DB=contract_events \
./contract-events-postgres-consumer 1000 2000
```

## Configuration

Environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `CONTRACT_EVENTS_SERVICE_ADDRESS` | `localhost:50053` | Contract events processor address |
| `POSTGRES_HOST` | `localhost` | PostgreSQL host |
| `POSTGRES_PORT` | `5432` | PostgreSQL port |
| `POSTGRES_DB` | `contract_events` | Database name |
| `POSTGRES_USER` | `postgres` | Database user |
| `POSTGRES_PASSWORD` | `postgres` | Database password |
| `POSTGRES_SSLMODE` | `disable` | SSL mode |
| `FILTER_CONTRACT_IDS` | - | Comma-separated contract IDs |
| `FILTER_EVENT_TYPES` | - | Comma-separated event types |
| `INCLUDE_FAILED` | `false` | Include failed transactions |
| `ENABLE_FLOWCTL` | `false` | Enable flowctl control plane integration |
| `FLOWCTL_ENDPOINT` | `localhost:8080` | Flowctl control plane gRPC address |
| `FLOWCTL_HEARTBEAT_INTERVAL` | `10s` | Heartbeat interval |
| `HEALTH_PORT` | `8089` | Health check HTTP port |
| `STELLAR_NETWORK` | `testnet` | Network name for flowctl metadata |

## Schema Design

### Hybrid Approach

**Indexed Columns** (fast lookups):
- `ledger_sequence` - Time-series queries
- `contract_id` - Contract-specific queries
- `event_type` - Event filtering
- `tx_hash` - Transaction lookup
- `ledger_closed_at` - Chronological queries

**JSONB Columns** (flexible queries):
- `topics` - Decoded event topics as JSON array
- `event_data` - Decoded event data as JSON object

**Raw XDR** (verification):
- `topics_xdr` - Base64 XDR array
- `data_xdr` - Base64 XDR string

### Why Hybrid?

```sql
-- Fast: Uses contract_id index
SELECT * FROM contract_events WHERE contract_id = 'CABC123...';

-- Also fast: Uses GIN index on JSONB
SELECT * FROM contract_events
WHERE topics::text LIKE '%GABC123...%';

-- Super fast: Uses composite index
SELECT * FROM contract_events
WHERE contract_id = 'CABC...'
  AND event_type = 'transfer'
  AND ledger_closed_at >= NOW() - INTERVAL '7 days';
```

## Example Queries

### Basic Queries

```sql
-- Recent events
SELECT * FROM contract_events
ORDER BY ledger_sequence DESC
LIMIT 10;

-- Events by type
SELECT event_type, COUNT(*)
FROM contract_events
GROUP BY event_type;

-- Specific contract
SELECT * FROM contract_events
WHERE contract_id = 'CABC123...'
ORDER BY ledger_sequence DESC;
```

### JSONB Queries (The Magic!)

```sql
-- Find transfers > 1000
SELECT
    ledger_sequence,
    contract_id,
    event_data->>'amount' as amount
FROM contract_events
WHERE event_type = 'transfer'
  AND (event_data->>'amount')::numeric > 1000;

-- Find all events with specific address
SELECT * FROM contract_events
WHERE topics::text LIKE '%GABC123...%';

-- Extract nested JSONB field
SELECT
    event_data->'asset'->>'code' as asset,
    SUM((event_data->>'amount')::numeric) as total_volume
FROM contract_events
WHERE event_type = 'transfer'
GROUP BY asset
ORDER BY total_volume DESC;
```

### Analytics Queries

```sql
-- Daily event volume
SELECT
    DATE(ledger_closed_at) as date,
    event_type,
    COUNT(*) as count
FROM contract_events
GROUP BY date, event_type
ORDER BY date DESC;

-- Most active contracts
SELECT
    contract_id,
    COUNT(*) as events,
    COUNT(DISTINCT event_type) as types
FROM contract_events
GROUP BY contract_id
ORDER BY events DESC
LIMIT 10;

-- Failure rate by contract
SELECT
    contract_id,
    COUNT(*) as total,
    SUM(CASE WHEN NOT tx_successful THEN 1 ELSE 0 END) as failures,
    ROUND(100.0 * SUM(CASE WHEN NOT tx_successful THEN 1 ELSE 0 END) / COUNT(*), 2) as failure_pct
FROM contract_events
GROUP BY contract_id
ORDER BY failure_pct DESC;
```

**See `config/queries/examples.sql` for 21+ query examples!**

## Query Performance

**Indexed queries (< 10ms):**
- By ledger sequence
- By contract ID
- By event type
- By transaction hash
- By timestamp

**JSONB queries with GIN index (< 100ms):**
- Contains checks in topics/data
- JSON field extraction
- Nested JSON queries

**Tips:**
1. Add custom indexes for your specific queries
2. Use materialized views for complex analytics
3. Partition large tables by date

## Custom Indexes

Add indexes without code changes:

```sql
-- Index for USDC transfers
CREATE INDEX idx_usdc_transfers
ON contract_events((event_data->'asset'->>'code'))
WHERE event_data->'asset'->>'code' = 'USDC';

-- Index for specific contract + type
CREATE INDEX idx_contract_transfers
ON contract_events(contract_id, event_type)
WHERE event_type = 'transfer';

-- Index for amount range queries
CREATE INDEX idx_amount
ON contract_events(((event_data->>'amount')::numeric))
WHERE event_type IN ('transfer', 'mint', 'burn');
```

## Monitoring

### Check Consumer Stats

Consumer logs stats every 10 seconds:

```
2025/12/01 10:00:00 Stats: 1000 events processed, 1000 inserted, 5 ledgers, last: 1005
2025/12/01 10:00:10 Stats: 2000 events processed, 2000 inserted, 10 ledgers, last: 1010
```

### Query Database Stats

```sql
-- Total events
SELECT COUNT(*) FROM contract_events;

-- Events by day
SELECT
    DATE(ledger_closed_at) as date,
    COUNT(*) as events
FROM contract_events
GROUP BY date
ORDER BY date DESC;

-- Recent activity
SELECT
    MAX(ledger_sequence) as latest_ledger,
    MAX(ledger_closed_at) as latest_time,
    COUNT(*) as total_events
FROM contract_events;
```

### Database Health

```bash
# Connect to database
docker-compose exec postgres psql -U postgres -d contract_events

# Or locally
psql -h localhost -p 5433 -U postgres -d contract_events

# Check table size
\dt+ contract_events

# Check indexes
\di+ contract_events*
```

## Flowctl Integration

The PostgreSQL consumer supports integration with [flowctl](https://github.com/withObsrvr/flowctl), a control plane for managing and monitoring Stellar data pipelines.

### Features

- **Service Registration** - Registers as a SINK service type
- **Health Endpoint** - Exposes health check at `/health` with stats
- **Metrics Reporting** - Sends insert metrics via heartbeats
- **Graceful Degradation** - Continues operating if flowctl is unavailable

### Usage

**Enable with environment variables:**

```bash
export ENABLE_FLOWCTL=true
export FLOWCTL_ENDPOINT=localhost:8080
export HEALTH_PORT=8090

./contract-events-postgres-consumer 1000 0
```

**With Docker Compose:**

```bash
# Set environment variables
export ENABLE_FLOWCTL=true
export FLOWCTL_ENDPOINT=flowctl:8080

# Start with consumer profile
docker-compose --profile with-consumer up -d
```

### Health Endpoint

When flowctl integration is enabled, the consumer exposes health stats:

```bash
curl http://localhost:8090/health
```

Response:
```json
{
  "status": "healthy",
  "stats": {
    "events_processed": 10000,
    "events_inserted": 9950,
    "events_failed": 50,
    "ledgers_processed": 100,
    "last_ledger": 2000
  }
}
```

### Metrics Reported

The consumer reports these metrics via heartbeats:

- `events_processed` - Total events received from processor
- `events_inserted` - Events successfully inserted to PostgreSQL
- `events_failed` - Failed insert operations
- `ledgers_processed` - Number of ledgers processed
- `last_ledger` - Most recent ledger sequence

### Service Metadata

Registered with flowctl as:

- **Service Type**: `SINK`
- **Input Event Types**: `contract_event_service.ContractEvent`
- **Metadata**: Network name, sink type (postgres), database name

## Production Deployment

### AWS RDS

```bash
# 1. Create RDS PostgreSQL instance
aws rds create-db-instance \
  --db-instance-identifier contract-events-db \
  --db-instance-class db.t3.medium \
  --engine postgres \
  --master-username admin \
  --master-user-password YourPassword123

# 2. Initialize schema
psql -h your-rds-endpoint.rds.amazonaws.com \
     -U admin -d postgres \
     -f config/schema/contract_events.sql

# 3. Run consumer
POSTGRES_HOST=your-rds-endpoint.rds.amazonaws.com \
POSTGRES_DB=postgres \
POSTGRES_USER=admin \
POSTGRES_PASSWORD=YourPassword123 \
POSTGRES_SSLMODE=require \
./contract-events-postgres-consumer 1000 0
```

### Google Cloud SQL

```bash
# 1. Create Cloud SQL instance
gcloud sql instances create contract-events \
  --database-version=POSTGRES_16 \
  --tier=db-f1-micro \
  --region=us-central1

# 2. Initialize schema
gcloud sql connect contract-events --user=postgres < config/schema/contract_events.sql

# 3. Run consumer with Cloud SQL Proxy
cloud_sql_proxy -instances=PROJECT:REGION:contract-events=tcp:5432 &

POSTGRES_HOST=localhost \
./contract-events-postgres-consumer 1000 0
```

### Kubernetes

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: contract-events-postgres-consumer
spec:
  replicas: 1
  template:
    spec:
      containers:
      - name: consumer
        image: contract-events-postgres-consumer:latest
        env:
        - name: CONTRACT_EVENTS_SERVICE_ADDRESS
          value: "contract-events-processor:50053"
        - name: POSTGRES_HOST
          valueFrom:
            secretKeyRef:
              name: postgres-creds
              key: host
        - name: POSTGRES_PASSWORD
          valueFrom:
            secretKeyRef:
              name: postgres-creds
              key: password
        args: ["1000", "0"]
```

## Troubleshooting

### Schema not found

```bash
# Error: table contract_events does not exist

# Fix: Initialize schema
psql -h localhost -p 5433 -U postgres -d contract_events \
     -f config/schema/contract_events.sql
```

### Connection refused

```bash
# Check PostgreSQL is running
docker-compose ps postgres

# Test connection
psql -h localhost -p 5433 -U postgres -d contract_events -c "SELECT 1"

# Check logs
docker-compose logs postgres
```

### Slow queries

```sql
-- Check query plan
EXPLAIN ANALYZE
SELECT * FROM contract_events
WHERE event_data->>'from' = 'GABC123...';

-- Add index if needed
CREATE INDEX idx_from ON contract_events((event_data->>'from'));
```

### Disk space

```sql
-- Check table size
SELECT pg_size_pretty(pg_total_relation_size('contract_events'));

-- Archive old data
DELETE FROM contract_events
WHERE ledger_closed_at < NOW() - INTERVAL '90 days';

-- Vacuum
VACUUM ANALYZE contract_events;
```

## Performance Tuning

### Connection Pool

```go
// Already configured in code:
db.SetMaxOpenConns(25)
db.SetMaxIdleConns(5)
db.SetConnMaxLifetime(5 * time.Minute)
```

### Batch Inserts (Future Enhancement)

For high throughput, consider batch inserts:

```go
// Insert 100 events per batch
batch := make([]*pb.ContractEvent, 0, 100)
for event := range stream {
    batch = append(batch, event)
    if len(batch) >= 100 {
        insertBatch(db, batch)
        batch = batch[:0]
    }
}
```

### Partitioning (For Large Datasets)

```sql
-- Partition by month (for > 10M rows)
CREATE TABLE contract_events_2025_01 PARTITION OF contract_events
FOR VALUES FROM ('2025-01-01') TO ('2025-02-01');

CREATE TABLE contract_events_2025_02 PARTITION OF contract_events
FOR VALUES FROM ('2025-02-01') TO ('2025-03-01');
```

## Use Cases

### 1. DeFi Protocol Monitoring

```bash
# Monitor specific DEX contract
FILTER_CONTRACT_IDS="CDEX123..." \
FILTER_EVENT_TYPES="swap" \
./contract-events-postgres-consumer 1000 0
```

Query:
```sql
-- Swap volume by asset pair
SELECT
    event_data->'tokenA'->>'symbol' as token_a,
    event_data->'tokenB'->>'symbol' as token_b,
    COUNT(*) as swap_count,
    SUM((event_data->>'amountA')::numeric) as volume_a
FROM contract_events
WHERE event_type = 'swap'
GROUP BY token_a, token_b
ORDER BY swap_count DESC;
```

### 2. Token Transfer Analytics

```bash
# Track all token transfers
FILTER_EVENT_TYPES="transfer,mint,burn" \
./contract-events-postgres-consumer 1000 0
```

Query:
```sql
-- Daily transfer volume
SELECT
    DATE(ledger_closed_at) as date,
    COUNT(*) as transfers,
    COUNT(DISTINCT event_data->>'from') as unique_senders,
    SUM((event_data->>'amount')::numeric) as total_volume
FROM contract_events
WHERE event_type = 'transfer'
GROUP BY date
ORDER BY date DESC;
```

### 3. Compliance & Auditing

```bash
# Include all events (including failed)
INCLUDE_FAILED=true \
./contract-events-postgres-consumer 1000 0
```

Query:
```sql
-- Large transfers for compliance review
SELECT *
FROM contract_events
WHERE event_type = 'transfer'
  AND (event_data->>'amount')::numeric > 10000
  AND ledger_closed_at >= NOW() - INTERVAL '7 days'
ORDER BY ledger_closed_at DESC;
```

## Development

### Project Structure

```
go-consumer-postgres/
├── config/
│   ├── schema/
│   │   └── contract_events.sql     # Schema (NO Go code!)
│   └── queries/
│       └── examples.sql            # 21+ query examples
├── go/
│   ├── main.go                     # Consumer (~380 lines, no schema!)
│   └── go.mod                      # Dependencies
├── docker-compose.yml              # Full stack
├── Dockerfile                      # Multi-stage build
├── Makefile                        # Build automation
└── README.md                       # This file
```

### Build from Source

```bash
# Install dependencies
cd go && go mod tidy

# Build
make build

# Run
./contract-events-postgres-consumer 1000 2000
```

## Next Steps

- See [contract-events-processor](../contract-events-processor) for the gRPC processor setup
- View `config/queries/examples.sql` for 21+ query patterns
- Add custom indexes for your specific queries
- Create materialized views for dashboards
- Integrate with [flowctl](https://github.com/withObsrvr/flowctl) for pipeline monitoring

## Contributing

This demonstrates the Shape Up pattern:
- ✅ Infrastructure creates schema
- ✅ Go only inserts data
- ✅ JSONB enables flexible queries

No schema management in Go code! 🎉
