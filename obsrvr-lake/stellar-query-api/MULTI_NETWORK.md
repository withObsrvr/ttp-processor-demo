# Running Testnet and Mainnet Simultaneously

This guide shows how to run the complete hot/cold lambda architecture for both Stellar testnet and mainnet at the same time.

## Architecture: Dual Network Deployment

```
TESTNET PIPELINE:
GCS testnet archive → stellar-live-source-datalake (Port 50053, Health 8088)
                   → stellar-postgres-ingester → PostgreSQL (Port 5434)
                   → stellar-query-api (Port 8092)

MAINNET PIPELINE:
GCS mainnet archive → stellar-live-source-datalake (Port 50054, Health 8098)
                   → stellar-postgres-ingester → PostgreSQL (Port 5435)
                   → stellar-query-api (Port 8093)
```

## Port Allocation Strategy

| Service                      | Testnet | Mainnet |
|------------------------------|---------|---------|
| stellar-live-source (gRPC)   | 50053   | 50054   |
| live-source health endpoint  | 8088    | 8098    |
| postgres-ingester health     | 8089    | 8099    |
| stellar-query-api            | 8092    | 8093    |
| PostgreSQL database          | 5434    | 5435    |

## Database Separation

### PostgreSQL Configuration

Run two separate PostgreSQL instances or use two databases in the same instance:

**Option A: Separate PostgreSQL Instances (Recommended)**
```bash
# Testnet PostgreSQL (Port 5434)
docker run -d \
  --name postgres-stellar-testnet \
  -e POSTGRES_USER=stellar \
  -e POSTGRES_PASSWORD=stellar_dev_password \
  -e POSTGRES_DB=stellar_hot \
  -p 5434:5432 \
  -v postgres-testnet-data:/var/lib/postgresql/data \
  postgres:15

# Mainnet PostgreSQL (Port 5435)
docker run -d \
  --name postgres-stellar-mainnet \
  -e POSTGRES_USER=stellar \
  -e POSTGRES_PASSWORD=stellar_dev_password \
  -e POSTGRES_DB=stellar_hot \
  -p 5435:5432 \
  -v postgres-mainnet-data:/var/lib/postgresql/data \
  postgres:15
```

**Option B: Single PostgreSQL with Multiple Databases**
```bash
# Single PostgreSQL instance
docker run -d \
  --name postgres-stellar \
  -e POSTGRES_USER=stellar \
  -e POSTGRES_PASSWORD=stellar_dev_password \
  -p 5434:5432 \
  -p 5435:5432 \
  -v postgres-stellar-data:/var/lib/postgresql/data \
  postgres:15

# Create separate databases
psql -h localhost -p 5434 -U stellar -c "CREATE DATABASE stellar_hot_testnet;"
psql -h localhost -p 5434 -U stellar -c "CREATE DATABASE stellar_hot_mainnet;"
```

### DuckLake Catalog Separation

Use separate schemas in the same PostgreSQL catalog or separate catalogs:

**Testnet DuckLake Catalog:**
```yaml
catalog_connection: "postgresql://doadmin:PASSWORD@catalog-host:25060/obsrvr_lake_catalog_testnet"
data_path: "s3://obsrvr-test-bucket-1/testnet/"
metadata_schema: "testnet_bronze"
```

**Mainnet DuckLake Catalog:**
```yaml
catalog_connection: "postgresql://doadmin:PASSWORD@catalog-host:25060/obsrvr_lake_catalog_mainnet"
data_path: "s3://obsrvr-test-bucket-1/mainnet/"
metadata_schema: "mainnet_bronze"
```

## Service Configuration Files

### Testnet Configurations

**stellar-live-source-datalake/config-testnet.yaml:**
```yaml
service:
  name: stellar-live-source-testnet
  port: :50053
  health_port: 8088

backend:
  type: ARCHIVE

archive:
  storage_type: GCS
  bucket_name: "obsrvr-stellar-ledger-data-testnet-data"
  path: "landing/ledgers/testnet"
  ledgers_per_file: 1
  files_per_partition: 64000

network:
  passphrase: "Test SDF Network ; September 2015"

flowctl:
  enabled: false
```

**stellar-postgres-ingester/config-testnet.yaml:**
```yaml
service:
  name: stellar-postgres-ingester-testnet
  health_port: 8089

source:
  endpoint: "localhost:50053"
  network_passphrase: "Test SDF Network ; September 2015"
  start_ledger: 1000000

postgres:
  host: localhost
  port: 5434
  database: stellar_hot
  user: stellar
  password: stellar_dev_password
  sslmode: disable
  batch_size: 50
  max_connections: 20

checkpoint:
  file_path: /var/lib/stellar-postgres-ingester/checkpoint-testnet.json
```

**stellar-query-api/config-testnet.yaml:**
```yaml
service:
  name: stellar-query-api-testnet
  port: 8092

postgres:
  host: localhost
  port: 5434
  database: stellar_hot
  user: stellar
  password: stellar_dev_password
  sslmode: disable
  max_connections: 10

ducklake:
  catalog_name: testnet_catalog
  catalog_connection: "postgresql://doadmin:PASSWORD@catalog-host:25060/obsrvr_lake_catalog_testnet?sslmode=require"
  schema_name: testnet_bronze
  metadata_schema: testnet_bronze
  data_path: "s3://obsrvr-test-bucket-1/testnet/"

  aws_region: us-west-004
  aws_endpoint: s3.us-west-004.backblazeb2.com
  aws_access_key_id: "YOUR_KEY_ID"
  aws_secret_access_key: "YOUR_SECRET_KEY"
```

### Mainnet Configurations

**stellar-live-source-datalake/config-mainnet.yaml:**
```yaml
service:
  name: stellar-live-source-mainnet
  port: :50054
  health_port: 8098

backend:
  type: ARCHIVE

archive:
  storage_type: GCS
  bucket_name: "obsrvr-stellar-ledger-data-mainnet-data"
  path: "landing/ledgers/mainnet"
  ledgers_per_file: 1
  files_per_partition: 64000

network:
  passphrase: "Public Global Stellar Network ; September 2015"

flowctl:
  enabled: false
```

**stellar-postgres-ingester/config-mainnet.yaml:**
```yaml
service:
  name: stellar-postgres-ingester-mainnet
  health_port: 8099

source:
  endpoint: "localhost:50054"
  network_passphrase: "Public Global Stellar Network ; September 2015"
  start_ledger: 50000000

postgres:
  host: localhost
  port: 5435
  database: stellar_hot
  user: stellar
  password: stellar_dev_password
  sslmode: disable
  batch_size: 50
  max_connections: 20

checkpoint:
  file_path: /var/lib/stellar-postgres-ingester/checkpoint-mainnet.json
```

**stellar-query-api/config-mainnet.yaml:**
```yaml
service:
  name: stellar-query-api-mainnet
  port: 8093

postgres:
  host: localhost
  port: 5435
  database: stellar_hot
  user: stellar
  password: stellar_dev_password
  sslmode: disable
  max_connections: 10

ducklake:
  catalog_name: mainnet_catalog
  catalog_connection: "postgresql://doadmin:PASSWORD@catalog-host:25060/obsrvr_lake_catalog_mainnet?sslmode=require"
  schema_name: mainnet_bronze
  metadata_schema: mainnet_bronze
  data_path: "s3://obsrvr-test-bucket-1/mainnet/"

  aws_region: us-west-004
  aws_endpoint: s3.us-west-004.backblazeb2.com
  aws_access_key_id: "YOUR_KEY_ID"
  aws_secret_access_key: "YOUR_SECRET_KEY"
```

## Startup Scripts

### start-testnet.sh
```bash
#!/bin/bash

set -e

echo "🌐 Starting Stellar Testnet Pipeline"
echo "======================================"

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

# 1. Check PostgreSQL Testnet
echo -e "\n${YELLOW}Checking PostgreSQL Testnet (Port 5434)...${NC}"
if PGPASSWORD=stellar_dev_password psql -h localhost -p 5434 -U stellar -d stellar_hot -c "SELECT 1" > /dev/null 2>&1; then
    echo -e "${GREEN}✓${NC} PostgreSQL testnet is running"
else
    echo -e "${RED}✗${NC} PostgreSQL testnet is NOT running. Starting..."
    docker start postgres-stellar-testnet || docker run -d \
      --name postgres-stellar-testnet \
      -e POSTGRES_USER=stellar \
      -e POSTGRES_PASSWORD=stellar_dev_password \
      -e POSTGRES_DB=stellar_hot \
      -p 5434:5432 \
      -v postgres-testnet-data:/var/lib/postgresql/data \
      postgres:15
    sleep 5
fi

# 2. Start Stellar Live Source (Testnet)
echo -e "\n${YELLOW}Starting Stellar Live Source (Testnet)...${NC}"
cd ~/Documents/ttp-processor-demo/stellar-live-source-datalake

if pgrep -f "stellar_live_source_datalake.*50053" > /dev/null; then
    echo -e "${GREEN}✓${NC} Already running"
else
    BACKEND_TYPE=ARCHIVE \
    ARCHIVE_STORAGE_TYPE=GCS \
    ARCHIVE_BUCKET_NAME="obsrvr-stellar-ledger-data-testnet-data" \
    ARCHIVE_PATH="landing/ledgers/testnet" \
    LEDGERS_PER_FILE=1 \
    FILES_PER_PARTITION=64000 \
    NETWORK_PASSPHRASE="Test SDF Network ; September 2015" \
    PORT=:50053 \
    HEALTH_PORT=8088 \
    ENABLE_FLOWCTL=false \
    nohup ./stellar_live_source_datalake > /tmp/stellar-source-testnet.log 2>&1 &

    echo -e "${GREEN}✓${NC} Started (Port 50053, Health 8088)"
    sleep 3
fi

# 3. Start PostgreSQL Ingester (Testnet)
echo -e "\n${YELLOW}Starting PostgreSQL Ingester (Testnet)...${NC}"
cd ~/Documents/ttp-processor-demo/stellar-postgres-ingester

if pgrep -f "stellar-postgres-ingester.*5434" > /dev/null; then
    echo -e "${GREEN}✓${NC} Already running"
else
    POSTGRES_HOST=localhost \
    POSTGRES_PORT=5434 \
    POSTGRES_DB=stellar_hot \
    POSTGRES_USER=stellar \
    POSTGRES_PASSWORD=stellar_dev_password \
    POSTGRES_SSLMODE=disable \
    LIVE_SOURCE_ENDPOINT="localhost:50053" \
    NETWORK_PASSPHRASE="Test SDF Network ; September 2015" \
    START_LEDGER=1000000 \
    HEALTH_PORT=8089 \
    nohup ./bin/stellar-postgres-ingester > /tmp/postgres-ingester-testnet.log 2>&1 &

    echo -e "${GREEN}✓${NC} Started (Health 8089)"
    sleep 3
fi

# 4. Start Query API (Testnet)
echo -e "\n${YELLOW}Starting Query API (Testnet)...${NC}"
cd ~/Documents/ttp-processor-demo/stellar-query-api

if pgrep -f "stellar-query-api.*8092" > /dev/null; then
    echo -e "${GREEN}✓${NC} Already running"
else
    nohup ./bin/stellar-query-api -config config-testnet.yaml > /tmp/query-api-testnet.log 2>&1 &

    echo -e "${GREEN}✓${NC} Started (Port 8092)"
    sleep 3
fi

echo -e "\n${GREEN}✅ Testnet pipeline is running!${NC}"
echo ""
echo "Service Endpoints:"
echo "  - Live Source Health: http://localhost:8088/health"
echo "  - Ingester Health: http://localhost:8089/health"
echo "  - Query API: http://localhost:8092/health"
echo ""
echo "Logs:"
echo "  tail -f /tmp/stellar-source-testnet.log"
echo "  tail -f /tmp/postgres-ingester-testnet.log"
echo "  tail -f /tmp/query-api-testnet.log"
echo ""
```

### start-mainnet.sh
```bash
#!/bin/bash

set -e

echo "🌍 Starting Stellar Mainnet Pipeline"
echo "======================================"

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

# 1. Check PostgreSQL Mainnet
echo -e "\n${YELLOW}Checking PostgreSQL Mainnet (Port 5435)...${NC}"
if PGPASSWORD=stellar_dev_password psql -h localhost -p 5435 -U stellar -d stellar_hot -c "SELECT 1" > /dev/null 2>&1; then
    echo -e "${GREEN}✓${NC} PostgreSQL mainnet is running"
else
    echo -e "${RED}✗${NC} PostgreSQL mainnet is NOT running. Starting..."
    docker start postgres-stellar-mainnet || docker run -d \
      --name postgres-stellar-mainnet \
      -e POSTGRES_USER=stellar \
      -e POSTGRES_PASSWORD=stellar_dev_password \
      -e POSTGRES_DB=stellar_hot \
      -p 5435:5432 \
      -v postgres-mainnet-data:/var/lib/postgresql/data \
      postgres:15
    sleep 5
fi

# 2. Start Stellar Live Source (Mainnet)
echo -e "\n${YELLOW}Starting Stellar Live Source (Mainnet)...${NC}"
cd ~/Documents/ttp-processor-demo/stellar-live-source-datalake

if pgrep -f "stellar_live_source_datalake.*50054" > /dev/null; then
    echo -e "${GREEN}✓${NC} Already running"
else
    BACKEND_TYPE=ARCHIVE \
    ARCHIVE_STORAGE_TYPE=GCS \
    ARCHIVE_BUCKET_NAME="obsrvr-stellar-ledger-data-mainnet-data" \
    ARCHIVE_PATH="landing/ledgers/mainnet" \
    LEDGERS_PER_FILE=1 \
    FILES_PER_PARTITION=64000 \
    NETWORK_PASSPHRASE="Public Global Stellar Network ; September 2015" \
    PORT=:50054 \
    HEALTH_PORT=8098 \
    ENABLE_FLOWCTL=false \
    nohup ./stellar_live_source_datalake > /tmp/stellar-source-mainnet.log 2>&1 &

    echo -e "${GREEN}✓${NC} Started (Port 50054, Health 8098)"
    sleep 3
fi

# 3. Start PostgreSQL Ingester (Mainnet)
echo -e "\n${YELLOW}Starting PostgreSQL Ingester (Mainnet)...${NC}"
cd ~/Documents/ttp-processor-demo/stellar-postgres-ingester

if pgrep -f "stellar-postgres-ingester.*5435" > /dev/null; then
    echo -e "${GREEN}✓${NC} Already running"
else
    POSTGRES_HOST=localhost \
    POSTGRES_PORT=5435 \
    POSTGRES_DB=stellar_hot \
    POSTGRES_USER=stellar \
    POSTGRES_PASSWORD=stellar_dev_password \
    POSTGRES_SSLMODE=disable \
    LIVE_SOURCE_ENDPOINT="localhost:50054" \
    NETWORK_PASSPHRASE="Public Global Stellar Network ; September 2015" \
    START_LEDGER=50000000 \
    HEALTH_PORT=8099 \
    nohup ./bin/stellar-postgres-ingester > /tmp/postgres-ingester-mainnet.log 2>&1 &

    echo -e "${GREEN}✓${NC} Started (Health 8099)"
    sleep 3
fi

# 4. Start Query API (Mainnet)
echo -e "\n${YELLOW}Starting Query API (Mainnet)...${NC}"
cd ~/Documents/ttp-processor-demo/stellar-query-api

if pgrep -f "stellar-query-api.*8093" > /dev/null; then
    echo -e "${GREEN}✓${NC} Already running"
else
    nohup ./bin/stellar-query-api -config config-mainnet.yaml > /tmp/query-api-mainnet.log 2>&1 &

    echo -e "${GREEN}✓${NC} Started (Port 8093)"
    sleep 3
fi

echo -e "\n${GREEN}✅ Mainnet pipeline is running!${NC}"
echo ""
echo "Service Endpoints:"
echo "  - Live Source Health: http://localhost:8098/health"
echo "  - Ingester Health: http://localhost:8099/health"
echo "  - Query API: http://localhost:8093/health"
echo ""
echo "Logs:"
echo "  tail -f /tmp/stellar-source-mainnet.log"
echo "  tail -f /tmp/postgres-ingester-mainnet.log"
echo "  tail -f /tmp/query-api-mainnet.log"
echo ""
```

### start-both.sh
```bash
#!/bin/bash

echo "🚀 Starting Both Testnet and Mainnet Pipelines"
echo "=============================================="

# Start testnet
./start-testnet.sh

echo ""
echo "Waiting 5 seconds before starting mainnet..."
sleep 5

# Start mainnet
./start-mainnet.sh

echo ""
echo "=============================================="
echo "✅ Both pipelines are running!"
echo ""
echo "Testnet Endpoints:"
echo "  - Query API: http://localhost:8092/health"
echo "  - PostgreSQL: localhost:5434"
echo ""
echo "Mainnet Endpoints:"
echo "  - Query API: http://localhost:8093/health"
echo "  - PostgreSQL: localhost:5435"
echo ""
```

### stop-all.sh
```bash
#!/bin/bash

echo "🛑 Stopping All Stellar Pipelines"
echo "=================================="

# Kill processes by pattern
echo "Stopping stellar-live-source instances..."
pkill -f "stellar_live_source_datalake"

echo "Stopping postgres-ingester instances..."
pkill -f "stellar-postgres-ingester"

echo "Stopping query-api instances..."
pkill -f "stellar-query-api"

echo "Stopping PostgreSQL containers (optional)..."
docker stop postgres-stellar-testnet postgres-stellar-mainnet 2>/dev/null || true

echo ""
echo "✅ All services stopped"
```

## Process Management with systemd

For production deployments, use systemd service files:

### /etc/systemd/system/stellar-testnet.service
```ini
[Unit]
Description=Stellar Testnet Pipeline
After=network.target docker.service
Requires=docker.service

[Service]
Type=forking
User=stellar
WorkingDirectory=/home/stellar/ttp-processor-demo
ExecStart=/home/stellar/ttp-processor-demo/stellar-query-api/start-testnet.sh
ExecStop=/usr/bin/pkill -f "stellar.*testnet"
Restart=on-failure
RestartSec=10

[Install]
WantedBy=multi-user.target
```

### /etc/systemd/system/stellar-mainnet.service
```ini
[Unit]
Description=Stellar Mainnet Pipeline
After=network.target docker.service
Requires=docker.service

[Service]
Type=forking
User=stellar
WorkingDirectory=/home/stellar/ttp-processor-demo
ExecStart=/home/stellar/ttp-processor-demo/stellar-query-api/start-mainnet.sh
ExecStop=/usr/bin/pkill -f "stellar.*mainnet"
Restart=on-failure
RestartSec=10

[Install]
WantedBy=multi-user.target
```

### Enable and manage:
```bash
sudo systemctl daemon-reload
sudo systemctl enable stellar-testnet stellar-mainnet
sudo systemctl start stellar-testnet stellar-mainnet
sudo systemctl status stellar-testnet stellar-mainnet
```

## Monitoring Both Networks

### Health Check Script (check-all.sh)
```bash
#!/bin/bash

echo "🔍 Health Check - All Services"
echo "=============================="

check_service() {
    local name=$1
    local url=$2

    if curl -sf "$url" > /dev/null 2>&1; then
        echo "✓ $name - OK"
    else
        echo "✗ $name - FAILED"
    fi
}

echo ""
echo "TESTNET:"
check_service "Live Source" "http://localhost:8088/health"
check_service "Ingester" "http://localhost:8089/health"
check_service "Query API" "http://localhost:8092/health"

echo ""
echo "MAINNET:"
check_service "Live Source" "http://localhost:8098/health"
check_service "Ingester" "http://localhost:8099/health"
check_service "Query API" "http://localhost:8093/health"

echo ""
echo "DATABASE STATUS:"
echo "Testnet PostgreSQL:"
PGPASSWORD=stellar_dev_password psql -h localhost -p 5434 -U stellar -d stellar_hot -c \
  "SELECT COUNT(*) as ledgers, MIN(sequence) as min_seq, MAX(sequence) as max_seq FROM ledgers_row_v2;" 2>/dev/null

echo ""
echo "Mainnet PostgreSQL:"
PGPASSWORD=stellar_dev_password psql -h localhost -p 5435 -U stellar -d stellar_hot -c \
  "SELECT COUNT(*) as ledgers, MIN(sequence) as min_seq, MAX(sequence) as max_seq FROM ledgers_row_v2;" 2>/dev/null
```

## Resource Considerations

Running both networks simultaneously requires:

**CPU:**
- Each live-source: 1-2 cores
- Each ingester: 1-2 cores
- Each query-api: 0.5-1 core
- Total: ~6-10 cores recommended

**Memory:**
- Each live-source: 512MB - 1GB
- Each ingester: 1-2GB
- Each query-api: 512MB - 1GB
- PostgreSQL testnet: 2-4GB
- PostgreSQL mainnet: 4-8GB
- Total: ~12-20GB recommended

**Storage:**
- PostgreSQL testnet hot buffer: ~10-50GB
- PostgreSQL mainnet hot buffer: ~50-200GB
- DuckLake cold storage: grows over time (S3/B2)

**Network:**
- Mainnet is ~5-10x more active than testnet
- Budget for ~100-500 Mbps for mainnet ingestion
- Testnet: ~10-50 Mbps

## Quick Start

1. **Setup PostgreSQL databases:**
```bash
docker run -d --name postgres-stellar-testnet -e POSTGRES_USER=stellar -e POSTGRES_PASSWORD=stellar_dev_password -e POSTGRES_DB=stellar_hot -p 5434:5432 -v postgres-testnet-data:/var/lib/postgresql/data postgres:15

docker run -d --name postgres-stellar-mainnet -e POSTGRES_USER=stellar -e POSTGRES_PASSWORD=stellar_dev_password -e POSTGRES_DB=stellar_hot -p 5435:5432 -v postgres-mainnet-data:/var/lib/postgresql/data postgres:15
```

2. **Create configuration files:**
```bash
# Copy and edit the config files above for each service
cd stellar-query-api
cp config.yaml config-testnet.yaml
cp config.yaml config-mainnet.yaml
# Edit with network-specific values
```

3. **Make scripts executable:**
```bash
chmod +x start-testnet.sh start-mainnet.sh start-both.sh stop-all.sh check-all.sh
```

4. **Start both networks:**
```bash
./start-both.sh
```

5. **Monitor status:**
```bash
./check-all.sh
```

6. **Query both networks:**
```bash
# Testnet
curl "http://localhost:8092/ledgers?start=1000000&end=1000010&limit=10"

# Mainnet
curl "http://localhost:8093/ledgers?start=50000000&end=50000010&limit=10"
```

## Troubleshooting

**Port conflicts:**
- If ports are already in use, modify the port allocation in config files
- Check for existing processes: `lsof -i :8092`

**Database connection issues:**
- Verify PostgreSQL is running: `docker ps | grep postgres`
- Test connection: `psql -h localhost -p 5434 -U stellar -d stellar_hot`

**Out of memory:**
- Reduce batch sizes in ingester configs
- Reduce max_connections in PostgreSQL configs
- Add swap space or increase system RAM

**One network falls behind:**
- Mainnet is more active, may need more resources
- Monitor ingestion rate in logs
- Consider running mainnet on a separate server
