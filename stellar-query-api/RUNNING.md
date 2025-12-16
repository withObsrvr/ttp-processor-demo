# Running the Hot/Cold Lambda Architecture

This guide shows how to run the complete stellar data pipeline with hot (PostgreSQL) and cold (DuckLake) storage.

## Architecture Overview

```
Stellar Data Source (GCS Archive)
    ↓ gRPC Stream
stellar-live-source-datalake (Port 50053)
    ↓ gRPC Stream  
stellar-postgres-ingester → PostgreSQL Hot Buffer (Port 5434)
                             (last ~10-20 minutes of data)
    ↓ Manual Flush (future: automated every 10 min)
postgres-ducklake-flusher → DuckLake Bronze (S3/B2 + PostgreSQL Catalog)
                             (historical data)
    ↑ Query both sources
stellar-query-api (Port 8092)
```

## Prerequisites

1. **PostgreSQL Hot Buffer** (already running on port 5434)
2. **DuckLake Bronze Catalog** (PostgreSQL on DigitalOcean)
3. **Backblaze B2 Storage** (for DuckLake data files)
4. **GCS Access** (for stellar-live-source-datalake)

## Step-by-Step Startup

### 1. Start PostgreSQL Hot Buffer (if not running)

```bash
# Check if running
PGPASSWORD=stellar_dev_password psql -h localhost -p 5434 -U stellar -d stellar_hot -c "SELECT 1"

# If not running, start it (you should already have this running)
# This was set up in Cycle 1
```

### 2. Start Stellar Data Source

```bash
cd ~/Documents/ttp-processor-demo/stellar-live-source-datalake

# Start with GCS archive (no flowctl)
ENABLE_FLOWCTL=false \
STORAGE_TYPE=GCS \
BUCKET_NAME=stellar-testnet-ledgers \
NETWORK_PASSPHRASE="Test SDF Network ; September 2015" \
PORT=:50053 \
HEALTH_PORT=8088 \
nix develop --command bash -c "./result/bin/server" > /tmp/stellar-source.log 2>&1 &

# Verify it's running
sleep 3
curl http://localhost:8088/health
```

### 3. Start PostgreSQL Ingester

```bash
cd ~/Documents/ttp-processor-demo/stellar-postgres-ingester

# Start the ingester
POSTGRES_HOST=localhost \
POSTGRES_PORT=5434 \
POSTGRES_DB=stellar_hot \
POSTGRES_USER=stellar \
POSTGRES_PASSWORD=stellar_dev_password \
POSTGRES_SSLMODE=disable \
LIVE_SOURCE_ENDPOINT="localhost:50053" \
NETWORK_PASSPHRASE="Test SDF Network ; September 2015" \
START_LEDGER=1000000 \
./bin/stellar-postgres-ingester > /tmp/postgres-ingester.log 2>&1 &

# Verify it's ingesting
sleep 5
tail -20 /tmp/postgres-ingester.log

# Check data is flowing
PGPASSWORD=stellar_dev_password psql -h localhost -p 5434 -U stellar -d stellar_hot -c \
  "SELECT COUNT(*) as total, MIN(sequence) as min_seq, MAX(sequence) as max_seq FROM ledgers_row_v2;"
```

### 4. Start Query API

```bash
cd ~/Documents/ttp-processor-demo/stellar-query-api

# Start the query API
./bin/stellar-query-api -config config.yaml > /tmp/query-api.log 2>&1 &

# Verify it's running
sleep 3
curl http://localhost:8092/health

# Test a query (hot storage)
curl "http://localhost:8092/ledgers?start=1039200&end=1039205&limit=5" | python3 -m json.tool
```

## Monitoring & Verification

### Check Data Flow

```bash
# 1. Check stellar-live-source is streaming
tail -f /tmp/stellar-source.log

# 2. Check ingester is writing to PostgreSQL
tail -f /tmp/postgres-ingester.log

# 3. Check PostgreSQL has recent data
PGPASSWORD=stellar_dev_password psql -h localhost -p 5434 -U stellar -d stellar_hot -c \
  "SELECT 
    (SELECT COUNT(*) FROM ledgers_row_v2) as ledgers,
    (SELECT COUNT(*) FROM transactions_row_v2) as transactions,
    (SELECT COUNT(*) FROM operations_row_v2) as operations,
    (SELECT MAX(sequence) FROM ledgers_row_v2) as latest_ledger;"

# 4. Test Query API endpoints
curl "http://localhost:8092/transactions?start=1039200&end=1039210&limit=10"
curl "http://localhost:8092/operations?start=1039200&end=1039210&limit=10"
```

### Monitor Throughput

```bash
# Watch ledger ingestion rate
watch -n 2 'PGPASSWORD=stellar_dev_password psql -h localhost -p 5434 -U stellar -d stellar_hot -c "SELECT MAX(sequence) FROM ledgers_row_v2;"'

# Check ingester logs for rate
tail -f /tmp/postgres-ingester.log | grep "ledgers/sec"
```

## Troubleshooting

### No data flowing to PostgreSQL

```bash
# Check stellar-live-source is running
curl http://localhost:8088/health

# Check ingester can connect
tail -20 /tmp/postgres-ingester.log

# Check PostgreSQL connection
PGPASSWORD=stellar_dev_password psql -h localhost -p 5434 -U stellar -d stellar_hot -c "\dt"
```

### Query API errors

```bash
# Check logs
tail -50 /tmp/query-api.log

# Verify DuckLake connection
# (API connects on startup - check logs for "Connected to DuckLake")

# Test hot storage directly
PGPASSWORD=stellar_dev_password psql -h localhost -p 5434 -U stellar -d stellar_hot -c \
  "SELECT * FROM ledgers_row_v2 ORDER BY sequence DESC LIMIT 1;"
```

### Restart Everything

```bash
# Stop all processes
pkill -f stellar-live-source-datalake
pkill -f stellar-postgres-ingester  
pkill -f stellar-query-api

# Wait a moment
sleep 2

# Restart in order (steps 2-4 above)
```

## Querying Data

### Hot Storage (Recent Data - Last ~10-20 minutes)

```bash
# Get latest ledgers
curl "http://localhost:8092/ledgers?start=1039200&end=1039300&limit=100"

# Get recent transactions
curl "http://localhost:8092/transactions?start=1039200&end=1039300&limit=100"

# Get recent operations
curl "http://localhost:8092/operations?start=1039200&end=1039300&limit=100"
```

### Cold Storage (Historical Data)

```bash
# Query old ledgers (will hit DuckLake if flushed)
curl "http://localhost:8092/ledgers?start=1000000&end=1000100&limit=100"

# The API automatically routes to:
# - Hot (PostgreSQL) for recent data
# - Cold (DuckLake) for historical data
# - UNION of both when query spans the boundary
```

## Process Summary

**Running Processes:**
1. `stellar-live-source-datalake` (Port 50053) - Data source
2. `stellar-postgres-ingester` - Writes to PostgreSQL
3. `stellar-query-api` (Port 8092) - Unified query API
4. `PostgreSQL` (Port 5434) - Hot buffer

**Data Flow:**
- GCS Archive → stellar-live-source-datalake → stellar-postgres-ingester → PostgreSQL
- Query API reads from both PostgreSQL (hot) and DuckLake (cold)

**Typical Throughput:**
- Ingestion: 600-1000 ledgers/sec
- Hot buffer: Last 2600 ledgers (~10-15 minutes on testnet)
- Query latency: <100ms for hot, <2s for cold

## Next Steps (Future Cycles)

- **Cycle 3**: Automated flush (postgres-ducklake-flusher runs every 10 minutes)
- **Cycle 5**: Bronze → Silver transformation (analytics-ready decoded data)
