# Stellar Live Source (RPC with Protocol 23)

This service connects to Stellar RPC endpoints and streams ledger data via gRPC to consumers. With **Protocol 23**, the RPC can transparently access both recent ledgers (from local storage) and historical ledgers (from external datastores like GCS or S3).

> **Protocol 23 Enhancement**: Stellar RPC nodes can now automatically fetch historical data from external datastores when requested ledgers are beyond their local retention window. This provides unified access to the entire blockchain history through a single RPC endpoint.

## Features

- Connects to Stellar RPC nodes for real-time ledger data
- Streams raw ledger data via gRPC
- Supports continuous streaming from a specified ledger
- Health check endpoint for monitoring
- Optional Flowctl integration for control plane management

## Prerequisites

- Go 1.21+
- Protocol Buffers compiler (protoc)
- Access to a Stellar RPC endpoint

## Building

### Build with Make

```bash
# Generate protobuf code
make gen-proto

# Build the service
make build

# Run the service
make run
```

### Direct Go Build

```bash
cd go
go build -o ../stellar-live-source main.go
```

## Configuration

The service is configured via environment variables:

### Required Configuration

- `RPC_ENDPOINT`: Stellar RPC endpoint URL (e.g., "https://soroban-testnet.stellar.org")
- `NETWORK_PASSPHRASE`: Stellar network passphrase

### Protocol 23: External Datastore Configuration

Configure these on your RPC server (not this client) to enable historical access:

- `SERVE_LEDGERS_FROM_DATASTORE`: Enable external datastore access (default: true)
- `DATASTORE_TYPE`: Storage backend type: "GCS" or "S3"
- `DATASTORE_BUCKET_PATH`: Path to ledger data in the bucket
- `DATASTORE_REGION`: AWS region (S3 only, default: us-east-1)

### Protocol 23: Datastore Schema Configuration

- `LEDGERS_PER_FILE`: Number of ledgers per file (default: 1)
- `FILES_PER_PARTITION`: Files per partition (default: 64000)

### Protocol 23: Buffered Storage Backend

- `BUFFER_SIZE`: Buffer size for batch processing (default: 100)
- `NUM_WORKERS`: Number of parallel workers (default: 10)
- `RETRY_LIMIT`: Max retries for failed requests (default: 3)
- `RETRY_WAIT`: Wait time between retries (default: 5s)

### Caching Configuration (Client Enhancement)

- `CACHE_HISTORICAL_RESPONSES`: Cache historical ledgers locally (default: true)
- `HISTORICAL_CACHE_DURATION`: Cache TTL (default: 1h)
- `ENABLE_PREDICTIVE_PREFETCH`: Enable predictive prefetching (default: true)
- `PREFETCH_CONCURRENCY`: Number of concurrent prefetch workers (default: 10)

### Optional Configuration

- `PORT`: gRPC service port (default: 50052)
- `HEALTH_PORT`: Health check HTTP port (default: 8088)
- `ENABLE_FLOWCTL`: Enable flowctl integration (default: false)
- `FLOWCTL_ENDPOINT`: Control plane endpoint if flowctl is enabled

## Running

### Method 1: Using Make

```bash
# Set environment variables
export RPC_ENDPOINT="https://soroban-testnet.stellar.org"
export NETWORK_PASSPHRASE="Test SDF Network ; September 2015"

# Run the service
make run
```

### Method 2: Using Environment File

Create a `.env` file:

```bash
# Stellar RPC Configuration
RPC_ENDPOINT=https://soroban-testnet.stellar.org
NETWORK_PASSPHRASE="Test SDF Network ; September 2015"

# Server Configuration
PORT=50052
HEALTH_PORT=8088

# Optional: Enable Flowctl integration
ENABLE_FLOWCTL=false
FLOWCTL_ENDPOINT=localhost:8080
```

Then run:

```bash
source .env && make run
```

### Method 3: Direct Execution

```bash
cd go
RPC_ENDPOINT="https://soroban-testnet.stellar.org" \
NETWORK_PASSPHRASE="Test SDF Network ; September 2015" \
go run main.go
```

### Method 4: Docker (if Dockerfile exists)

```bash
docker build -t stellar-live-source .
docker run -e RPC_ENDPOINT="https://soroban-testnet.stellar.org" \
           -e NETWORK_PASSPHRASE="Test SDF Network ; September 2015" \
           -p 50052:50052 \
           -p 8088:8088 \
           stellar-live-source
```

## Protocol 23: How It Works

When you request ledgers from a Protocol 23-enabled RPC:

1. **Recent Ledgers (within retention window)**
   - Served directly from RPC's local storage
   - Fastest response time
   - Typically last 7 days of ledgers

2. **Historical Ledgers (beyond retention window)**
   - RPC automatically fetches from configured external datastore
   - Transparent to the client - same API, same response format
   - May have slightly higher latency for first access
   - Our client caches these for improved performance

3. **Unified Access Pattern**
   ```go
   // Same code works for both recent and historical ledgers!
   resp, err := client.StreamRawLedgers(ctx, &StreamLedgersRequest{
       StartLedger: 1,  // Can be from genesis or recent - RPC handles it
   })
   ```

## Available RPC Endpoints

### Testnet
- https://soroban-testnet.stellar.org (Protocol 23 enabled)
- https://horizon-testnet.stellar.org

### Mainnet
- https://soroban.stellar.org (Protocol 23 enabled)
- https://horizon.stellar.org

### Futurenet
- https://rpc-futurenet.stellar.org

## gRPC Interface

The service exposes a single gRPC method:

```protobuf
service RawLedgerService {
    rpc StreamRawLedgers(StreamLedgersRequest) returns (stream RawLedger) {}
}
```

- `StreamLedgersRequest`: Contains the starting ledger sequence
- `RawLedger`: Contains the ledger sequence and raw XDR bytes

## Health Check

The service exposes a health check endpoint on the configured `HEALTH_PORT`:

```bash
# Check service health
curl http://localhost:8088/health
```

## Example Usage

### Starting the Service

```bash
# For testnet
export RPC_ENDPOINT="https://soroban-testnet.stellar.org"
export NETWORK_PASSPHRASE="Test SDF Network ; September 2015"
make run
```

### Connecting a Client

The service listens on port 50052 by default. Clients can connect and stream ledgers:

```go
// Example Go client
conn, err := grpc.Dial("localhost:50052", grpc.WithInsecure())
client := rawledger.NewRawLedgerServiceClient(conn)

stream, err := client.StreamRawLedgers(context.Background(), &rawledger.StreamLedgersRequest{
    StartLedger: 1000000,
})

for {
    ledger, err := stream.Recv()
    if err != nil {
        break
    }
    // Process ledger
}
```

## Monitoring

When Flowctl integration is enabled, the service reports metrics to the control plane:

```bash
export ENABLE_FLOWCTL=true
export FLOWCTL_ENDPOINT=localhost:8080
make run
```

## Troubleshooting

### Connection Issues

1. Verify the RPC endpoint is accessible:
   ```bash
   curl -X POST https://soroban-testnet.stellar.org \
        -H "Content-Type: application/json" \
        -d '{"jsonrpc":"2.0","method":"getHealth","params":[],"id":1}'
   ```

2. Check network passphrase matches the network:
   - Testnet: "Test SDF Network ; September 2015"
   - Mainnet: "Public Global Stellar Network ; September 2015"
   - Futurenet: "Test SDF Future Network ; October 2022"

### Performance Tuning

For high-throughput scenarios:

1. Ensure sufficient network bandwidth to the RPC endpoint
2. Monitor the health endpoint for processing metrics
3. Consider running multiple instances for load balancing

## Development

### Project Structure

```
stellar-live-source/
├── go/
│   ├── gen/
│   │   └── raw_ledger_service/
│   │       ├── raw_ledger_service.pb.go
│   │       └── raw_ledger_service_grpc.pb.go
│   ├── server/
│   │   ├── server.go
│   │   └── flowctl.go
│   ├── main.go
│   ├── go.mod
│   └── go.sum
├── protos/
│   └── raw_ledger_service/
│       └── raw_ledger_service.proto
├── Makefile
└── README.md
```

### Testing

Run tests:

```bash
cd go
go test ./...
```

## License

This project is licensed under the Apache License 2.0.