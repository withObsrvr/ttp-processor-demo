# Contract Data Processor - Testing Guide

## Prerequisites

1. **Stellar Live Source Datalake** must be running:
   ```bash
   cd /home/tillman/Documents/ttp-processor-demo/stellar-live-source-datalake
   ./stellar-live-source-datalake
   ```

2. **Flowctl** (optional but recommended):
   ```bash
   cd /home/tillman/projects/obsrvr/flowctl
   ./bin/flowctl serve
   ```

## Quick Start Test

### 1. Basic Functionality Test

Start the processor with minimal configuration:

```bash
cd /home/tillman/Documents/ttp-processor-demo/contract-data-processor

# Set required environment variables
export NETWORK_PASSPHRASE="Test SDF Network ; September 2015"
export SOURCE_ENDPOINT="localhost:50053"  # stellar-live-source-datalake
export GRPC_ADDRESS=":50054"
export FLIGHT_ADDRESS=":8816"
export HEALTH_PORT="8089"

# Run the processor
./contract-data-processor
```

### 2. Test with Flowctl Integration

```bash
# Enable flowctl
export FLOWCTL_ENABLED=true
export FLOWCTL_ENDPOINT="localhost:8080"
export SERVICE_NAME="contract-data-processor"
export SERVICE_VERSION="1.0.0"

./contract-data-processor
```

## Testing Each Component

### 1. Health Check

```bash
# Check if the service is healthy
curl http://localhost:8089/health
```

Expected response:
```json
{
  "status": "healthy",
  "service": "contract-data-processor"
}
```

### 2. Prometheus Metrics

```bash
# View all metrics
curl http://localhost:8089/metrics | grep contract_data_

# Key metrics to look for:
# - contract_data_contracts_processed_total
# - contract_data_current_ledger
# - contract_data_processing_errors_total
# - contract_data_flowctl_registered (should be 1 if flowctl is enabled)
```

### 3. gRPC Control Plane Test

Use grpcurl to test the control plane:

```bash
# List available services
grpcurl -plaintext localhost:50054 list

# Start processing a range of ledgers
grpcurl -plaintext -d '{
  "session_id": "test-session-1",
  "start_ledger": 500000,
  "end_ledger": 500010
}' localhost:50054 contractdata.v1.ControlService/StartProcessing

# Check session status
grpcurl -plaintext -d '{
  "session_id": "test-session-1"
}' localhost:50054 contractdata.v1.ControlService/GetSessionStatus

# Stop processing
grpcurl -plaintext -d '{
  "session_id": "test-session-1"
}' localhost:50054 contractdata.v1.ControlService/StopProcessing
```

### 4. Arrow Flight Data Plane Test

Create a test client to consume Arrow Flight data:

```python
# Save as test_flight_client.py
import pyarrow.flight as flight

# Connect to Flight server
client = flight.FlightClient("grpc://localhost:8816")

# List available streams
for flight_info in client.list_flights():
    print(f"Available flight: {flight_info.descriptor}")

# Get contract data stream
ticket = flight.Ticket(b"contract-data-stream")
reader = client.do_get(ticket)

# Read schema
schema = reader.schema
print(f"Schema: {schema}")

# Read data
for batch in reader:
    print(f"Received batch with {batch.data.num_rows} rows")
    # Print first few rows
    df = batch.data.to_pandas()
    print(df.head())
```

### 5. Flowctl Integration Test

```bash
# Check if the processor is registered
grpcurl -plaintext localhost:8080 flowctl.ControlPlane/ListServices | grep contract-data-processor

# The test script we created earlier
./scripts/test-flowctl-integration.sh
```

## Test Scenarios

### Scenario 1: Filter Specific Contracts

```bash
# Test with contract ID filtering
export FILTER_CONTRACT_IDS="CCJZ5X3K7LRSIPKDVB2NE5MWA32FY6SHXDVQDUQW5FWLVHT2OHWB3NKP"

./contract-data-processor
```

### Scenario 2: Filter by Asset

```bash
# Filter for specific asset contracts
export FILTER_ASSET_CODES="USDC,EURC"

./contract-data-processor
```

### Scenario 3: High Volume Test

```bash
# Process a large range
grpcurl -plaintext -d '{
  "session_id": "volume-test",
  "start_ledger": 500000,
  "end_ledger": 510000
}' localhost:50054 contractdata.v1.ControlService/StartProcessing

# Monitor metrics
watch -n 1 'curl -s http://localhost:8089/metrics | grep -E "(contracts_processed|current_ledger|processing_rate)"'
```

## Debugging

### 1. Enable Debug Logging

```bash
export LOG_LEVEL=debug
./contract-data-processor
```

### 2. Check Specific Contract Data

Look for logs showing extracted data:
- Key and Val XDR encoding
- Contract instance detection
- Asset contract ID calculation

### 3. Common Issues

**Issue**: "Failed to connect to data source"
- Ensure stellar-live-source-datalake is running on port 50053

**Issue**: "Flowctl registration failed"
- Check if flowctl is running on port 8080
- The processor will continue with a simulated ID

**Issue**: No data being processed
- Check if the ledger range contains contract data
- Stellar testnet contract data starts around ledger 300000+

## Performance Testing

### 1. Memory Usage

```bash
# Monitor memory while processing
ps aux | grep contract-data-processor
```

### 2. Processing Rate

```bash
# Calculate processing rate from metrics
curl -s http://localhost:8089/metrics | grep -E "contracts_per_second|bytes_per_second"
```

### 3. Arrow Flight Throughput

```python
# Measure streaming performance
import time
import pyarrow.flight as flight

client = flight.FlightClient("grpc://localhost:8816")
ticket = flight.Ticket(b"contract-data-stream")
reader = client.do_get(ticket)

start = time.time()
total_rows = 0

for batch in reader:
    total_rows += batch.data.num_rows

elapsed = time.time() - start
print(f"Processed {total_rows} rows in {elapsed:.2f} seconds")
print(f"Rate: {total_rows/elapsed:.2f} rows/second")
```

## Integration with Other Components

### 1. PostgreSQL Consumer

```bash
cd /home/tillman/Documents/ttp-processor-demo/contract-data-processor/consumer/postgresql

# Set database credentials
export DB_PASSWORD="your_password"
export FLIGHT_ENDPOINT="localhost:8816"

# Run the consumer
go run .
```

### 2. Parquet Writer (if implemented)

```bash
# Configure parquet output
export PARQUET_OUTPUT_DIR="/tmp/contract-data"
export PARQUET_FILE_SIZE="100MB"

# The processor would write Parquet files to the specified directory
```

## Verification Steps

1. **Verify Contract Data Extraction**:
   - Check logs for "Extracted Key XDR" and "Extracted Val XDR"
   - Look for "Contract instance type: wasm" or "stellar_asset"
   - Verify "Calculated asset contract ID" for asset contracts

2. **Verify Arrow Flight**:
   - Connect with a Flight client
   - Verify schema has all 18 fields
   - Check data includes KeyXdr, ValXdr, AssetContractId

3. **Verify Flowctl**:
   - Check metrics endpoint shows `contract_data_flowctl_registered 1`
   - Look for "Registered with flowctl control plane" in logs
   - Monitor heartbeat logs every 10 seconds

## Summary

The contract data processor can be tested at multiple levels:
- **Unit level**: Individual contract data extraction
- **Integration level**: gRPC control plane and Arrow Flight data plane
- **System level**: Full pipeline with flowctl monitoring
- **Performance level**: High-volume processing and throughput

Start with the basic functionality test and progressively test more advanced features based on your needs.