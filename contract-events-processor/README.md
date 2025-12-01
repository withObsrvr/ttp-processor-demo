# Contract Events Processor

A standalone gRPC service that extracts Soroban contract events from Stellar ledgers with support for filtering by contract ID, event type, and transaction status.

## Features

- **Streaming gRPC API** - Continuous event stream from any ledger
- **V3/V4 Compatible** - Works with both pre-Soroban and Soroban ledgers
- **Event Filtering** - Filter by contract IDs, event types, or transaction status
- **Dual Format** - Returns both raw XDR and decoded JSON
- **Type Detection** - Automatically detects common event types (transfer, mint, burn, swap, etc.)
- **Language Agnostic** - Consume from Python, Node.js, Rust, Go, or any gRPC-supported language

## Architecture

```
Stellar Network
      ↓
[stellar-live-source] ← Fetches raw ledgers via RPC
      ↓
[contract-events-processor] ← Extracts and filters contract events
      ↓
Consumers (Python, Node.js, Rust, etc.)
```

## Quick Start

### 1. Start the Services

```bash
# Start the full pipeline (stellar-live-source + contract-events-processor)
docker-compose up -d

# Check health
curl http://localhost:8089/health

# View logs
docker-compose logs -f contract-events-processor
```

The service will:
1. Connect to stellar-live-source (port 50052)
2. Start streaming Stellar testnet ledgers
3. Extract contract events and serve via gRPC (port 50053)

### 2. Run the Demo Consumer

```bash
# Build and run the example consumer
cd examples/go-consumer
./run.sh 1000 1100

# Or with custom filters
FILTER_EVENT_TYPES="transfer,mint" ./run.sh 1000 0
```

Output:
```
================================================================================
Contract Events Stream
================================================================================

--- Event #1 ---
Ledger:       1234 (closed at 1701234567)
TX Hash:      abc123...
TX Status:    ✓ Successful
Contract:     CABC123...
Event Type:   transfer
Event Index:  0
Op Index:     1

Topics (3):
  [0] transfer
  [1] {type: account, address: GABC...}
  [2] {type: account, address: GDEF...}
...
```

### Build from Source

**Prerequisites:**
- Go 1.24+
- protoc (Protocol Buffers compiler)
- protoc-gen-go and protoc-gen-go-grpc plugins

```bash
# Build
make build

# Run
export NETWORK_PASSPHRASE="Test SDF Network ; September 2015"
export LIVE_SOURCE_ADDRESS="localhost:50052"
./contract_events_processor
```

## Configuration

Environment variables:

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `NETWORK_PASSPHRASE` | Yes | - | Stellar network passphrase |
| `LIVE_SOURCE_ADDRESS` | No | `localhost:50052` | stellar-live-source gRPC address |
| `PORT` | No | `:50053` | gRPC server port |
| `HEALTH_PORT` | No | `8089` | Health check HTTP port |
| `ENABLE_FLOWCTL` | No | `false` | Enable flowctl control plane integration |
| `FLOWCTL_ENDPOINT` | No | `localhost:8080` | Flowctl control plane gRPC address |
| `FLOWCTL_HEARTBEAT_INTERVAL` | No | `10s` | Heartbeat interval (e.g., "10s", "30s") |
| `STELLAR_NETWORK` | No | `testnet` | Network name for flowctl metadata |

## gRPC API

### GetContractEvents RPC

Streams contract events with optional filtering.

**Request:**
```protobuf
message GetContractEventsRequest {
    uint32 start_ledger = 1;
    uint32 end_ledger = 2;  // 0 for continuous streaming

    repeated string contract_ids = 3;     // Filter by contract IDs
    repeated string event_types = 4;      // Filter by event types
    bool include_failed = 5;              // Include events from failed txs
    bool include_diagnostics = 6;         // Include diagnostic events
}
```

**Response:**
```protobuf
message ContractEvent {
    EventMeta meta = 1;                  // Ledger/tx metadata
    string contract_id = 2;              // Contract address
    string event_type = 3;               // Detected type (transfer, mint, etc.)

    repeated ScValue topics = 4;         // Event topics (XDR + JSON)
    ScValue data = 5;                    // Event data (XDR + JSON)

    bool in_successful_tx = 6;
    int32 event_index = 7;
    optional int32 operation_index = 8;
}
```

## Usage Examples

### Python Consumer

```python
import grpc
import contract_event_service_pb2
import contract_event_service_pb2_grpc

# Connect to service
channel = grpc.insecure_channel('localhost:50053')
stub = contract_event_service_pb2_grpc.ContractEventServiceStub(channel)

# Stream all events from ledger 1000
request = contract_event_service_pb2.GetContractEventsRequest(
    start_ledger=1000,
    end_ledger=0,  # Continuous
)

for event in stub.GetContractEvents(request):
    print(f"Event: {event.event_type} from {event.contract_id}")
    print(f"Topics: {[t.json for t in event.topics]}")
    print(f"Data: {event.data.json}")
```

### Filter by Contract ID

```python
# Only get events from specific contracts
request = contract_event_service_pb2.GetContractEventsRequest(
    start_ledger=1000,
    contract_ids=[
        "CABC123...",  # Your contract address
        "CDEF456...",
    ],
)

for event in stub.GetContractEvents(request):
    print(f"Filtered event: {event.event_type}")
```

### Filter by Event Type

```python
# Only get transfer events
request = contract_event_service_pb2.GetContractEventsRequest(
    start_ledger=1000,
    event_types=["transfer", "mint", "burn"],
)

for event in stub.GetContractEvents(request):
    print(f"Token event: {event.event_type}")
```

### Go Consumer

```go
package main

import (
    "context"
    "log"

    pb "github.com/withObsrvr/contract-events-processor/gen/contract_event_service"
    "google.golang.org/grpc"
)

func main() {
    // Connect
    conn, err := grpc.Dial("localhost:50053", grpc.WithInsecure())
    if err != nil {
        log.Fatal(err)
    }
    defer conn.Close()

    client := pb.NewContractEventServiceClient(conn)

    // Stream events
    stream, err := client.GetContractEvents(context.Background(), &pb.GetContractEventsRequest{
        StartLedger: 1000,
    })
    if err != nil {
        log.Fatal(err)
    }

    for {
        event, err := stream.Recv()
        if err != nil {
            log.Fatal(err)
        }

        log.Printf("Event: %s from %s", event.EventType, event.ContractId)
    }
}
```

## Event Types Detected

The processor automatically detects common event types:

- `transfer` - Token transfers
- `mint` - Token minting
- `burn` - Token burning
- `swap` - Token swaps (DEX)
- `deposit` / `withdraw` - Liquidity operations
- `stake` / `unstake` - Staking operations
- `approval` / `revoke` - Token approvals
- `claim` - Reward claims
- And more...

## Health Check

```bash
# Check service health
curl http://localhost:8089/health
```

Response:
```json
{
  "status": "healthy",
  "stats": {
    "processed_ledgers": 1000,
    "events_found": 5432,
    "successful_events": 5000,
    "failed_events": 432,
    "last_ledger": 2000,
    "last_processed_time": "2025-12-01T10:30:00Z"
  }
}
```

## Flowctl Integration

The contract-events-processor supports integration with [flowctl](https://github.com/withObsrvr/flowctl), a control plane for managing and monitoring Stellar data pipelines.

### Features

- **Service Registration** - Automatically registers as a PROCESSOR service
- **Health Monitoring** - Reports health status via heartbeats
- **Metrics Reporting** - Sends processing metrics (ledgers processed, events sent, etc.)
- **Pipeline Visibility** - View service status in flowctl dashboard

### Usage

**Enable with environment variables:**

```bash
export ENABLE_FLOWCTL=true
export FLOWCTL_ENDPOINT=localhost:8080
export FLOWCTL_HEARTBEAT_INTERVAL=10s
export STELLAR_NETWORK=testnet

./contract_events_processor
```

**Or with Docker Compose:**

```bash
# Set environment variables
export ENABLE_FLOWCTL=true
export FLOWCTL_ENDPOINT=flowctl:8080

# Start services
docker-compose up -d
```

### Metrics Reported

The processor reports these metrics via heartbeats:

- `ledgers_processed` - Total ledgers processed
- `events_sent` - Total events sent to consumers
- `events_filtered` - Events filtered out
- `total_events_found` - Total events extracted from ledgers
- `last_processed_ledger` - Most recent ledger sequence
- `processing_latency_ms` - Processing latency in milliseconds

### Service Metadata

Registered with flowctl as:

- **Service Type**: `PROCESSOR`
- **Input Event Types**: `raw_ledger_service.RawLedgerChunk`
- **Output Event Types**: `contract_event_service.ContractEvent`
- **Metadata**: Network name, processor type

### Graceful Degradation

If flowctl is unavailable:
- Service continues operating normally
- Uses simulated service ID for local tracking
- Logs warning but doesn't fail

This ensures the processor remains operational even if flowctl is down.

## Development

### Project Structure

```
contract-events-processor/
├── protos/                  # Protocol Buffer definitions
│   └── contract_event_service.proto
├── go/
│   ├── main.go             # Entry point
│   ├── config/             # Configuration
│   ├── server/             # gRPC server implementation
│   │   ├── server.go       # RPC handlers
│   │   ├── processor.go    # Event extraction logic
│   │   └── scval_converter.go  # XDR to JSON conversion
│   └── gen/                # Generated protobuf code
├── Dockerfile
├── docker-compose.yml
├── Makefile
└── README.md
```

### Make Targets

```bash
make gen-proto   # Generate protobuf code
make build       # Build binary
make run         # Build and run
make clean       # Clean build artifacts
```

### Testing

```bash
# Unit tests (if available)
go test ./...

# Integration test with stellar-live-source
docker-compose up
# In another terminal
python examples/simple_consumer.py
```

## Production Deployment

### Kubernetes

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: contract-events-processor
spec:
  replicas: 1
  selector:
    matchLabels:
      app: contract-events-processor
  template:
    metadata:
      labels:
        app: contract-events-processor
    spec:
      containers:
      - name: processor
        image: ghcr.io/withobsrvr/contract-events-processor:latest
        ports:
        - containerPort: 50053
        - containerPort: 8089
        env:
        - name: NETWORK_PASSPHRASE
          value: "Public Global Stellar Network ; September 2015"
        - name: LIVE_SOURCE_ADDRESS
          value: "stellar-live-source:50052"
        livenessProbe:
          httpGet:
            path: /health
            port: 8089
          initialDelaySeconds: 10
          periodSeconds: 30
```

## Troubleshooting

### Service won't start

**Error:** `Failed to connect to stellar-live-source`

**Solution:** Ensure stellar-live-source is running and accessible:
```bash
curl http://localhost:8088/health  # stellar-live-source health
docker-compose logs stellar-live-source
```

### No events being received

1. Check if stellar-live-source is processing ledgers:
```bash
curl http://localhost:8088/health | jq .metrics.last_processed_ledger
```

2. Verify your filter criteria aren't too restrictive:
```python
# Start with no filters to see all events
request = contract_event_service_pb2.GetContractEventsRequest(
    start_ledger=1000,
)
```

3. Check if the ledger range has contract events:
```bash
# Look at processor stats
curl http://localhost:8089/health | jq .stats
```

## Performance Considerations

- **Memory:** ~50-100MB base usage
- **CPU:** Minimal, scales with event volume
- **Network:** ~1-10 Mbps depending on ledger throughput
- **Latency:** < 100ms per ledger on average

## Related Projects

- [stellar-live-source](../stellar-live-source) - Raw ledger data source
- [contract-events-postgres-consumer](../contract-events-postgres-consumer) - PostgreSQL consumer for contract events
- [ttp-processor](../ttp-processor) - Token Transfer Protocol processor
- Shape Up documentation in [shape-up/](../shape-up/)

## Contributing

This is part of the OBSRVR data pipeline toolkit. See [CONTRIBUTING.md](../CONTRIBUTING.md) for guidelines.

## License

[Your License Here]
