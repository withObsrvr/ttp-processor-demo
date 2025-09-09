# Stellar Live Source (RPC)

This service connects to Stellar RPC endpoints and streams ledger data via gRPC to consumers. It provides real-time access to Stellar blockchain data through RPC nodes.

> **Note**: This service connects to **Stellar RPC endpoints**. If you need to read from storage backends (GCS, S3, filesystem), use the `stellar-live-source-datalake` service instead.

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

## Available RPC Endpoints

### Testnet
- https://soroban-testnet.stellar.org
- https://horizon-testnet.stellar.org

### Mainnet
- https://soroban.stellar.org
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