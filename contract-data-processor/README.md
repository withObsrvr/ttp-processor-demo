# Contract Data Processor

A hybrid service that processes Stellar smart contract (Soroban) data with a gRPC control plane and Apache Arrow Flight data plane for high-performance analytics.

## Architecture

This service implements a hybrid architecture:
- **Control Plane (gRPC)**: Manages processing sessions, filters, and metrics
- **Data Plane (Arrow Flight)**: Streams contract data in columnar format for analytics

```
stellar-live-source-datalake → Contract Data Processor → Arrow Flight Clients
                                    ├── gRPC Control
                                    └── Arrow Flight Data
```

## Features

- Extract Soroban contract data from Stellar ledgers using official stellar/go processors
- Filter by contract IDs, asset codes, and issuers
- Stream data in Apache Arrow columnar format
- Parallel processing with configurable workers
- Real-time metrics and health monitoring
- Support for deleted contract tracking
- Session-based processing management

## Building

### Using Nix (Recommended)
```bash
# Enter development shell
nix develop

# Build the processor
nix build

# Build Docker image
nix build .#docker

# Run directly
nix run
```

### Using Make
```bash
# Generate protobuf files
make gen-proto

# Build the processor
make build

# Run tests
make test

# Run the processor
make run
```

## Configuration

The service is configured via environment variables:

| Variable | Description | Default |
|----------|-------------|---------|
| `GRPC_ADDRESS` | gRPC control plane address | `:50054` |
| `FLIGHT_ADDRESS` | Arrow Flight data plane address | `:8816` |
| `HEALTH_PORT` | HTTP health/metrics port | `8089` |
| `SOURCE_ENDPOINT` | stellar-live-source-datalake endpoint | `localhost:50053` |
| `NETWORK_PASSPHRASE` | Stellar network | `Test SDF Network ; September 2015` |
| `BATCH_SIZE` | Entries per Arrow batch | `1000` |
| `WORKER_COUNT` | Parallel processing workers | `4` |
| `FILTER_CONTRACT_IDS` | Comma-separated contract IDs | (empty) |
| `FILTER_ASSET_CODES` | Comma-separated asset codes | (empty) |
| `FILTER_ASSET_ISSUERS` | Comma-separated issuers | (empty) |
| `INCLUDE_DELETED` | Include deleted entries | `false` |

## Usage

### Starting the Service
```bash
./contract-data-processor
```

### Control Plane Operations

Start processing session:
```bash
grpcurl -plaintext -d '{
  "start_ledger": 1000000,
  "end_ledger": 1001000,
  "batch_size": 1000
}' localhost:50054 contractdata.v1.ControlService/StartProcessing
```

Check session status:
```bash
grpcurl -plaintext -d '{
  "session_id": "YOUR_SESSION_ID"
}' localhost:50054 contractdata.v1.ControlService/GetStatus
```

Configure filters:
```bash
grpcurl -plaintext -d '{
  "asset_codes": ["USDC", "EURC"],
  "include_deleted": false
}' localhost:50054 contractdata.v1.ControlService/ConfigureFilters
```

Get metrics:
```bash
grpcurl -plaintext -d '{
  "session_id": "YOUR_SESSION_ID"
}' localhost:50054 contractdata.v1.ControlService/GetMetrics
```

Stop processing:
```bash
grpcurl -plaintext -d '{
  "session_id": "YOUR_SESSION_ID"
}' localhost:50054 contractdata.v1.ControlService/StopProcessing
```

### Data Plane Access

Connect to Arrow Flight endpoint on port 8816 using any Arrow Flight client to stream contract data:

```python
import pyarrow.flight as flight

# Connect to Flight server
client = flight.connect("grpc://localhost:8816")

# Get flight info
info = client.get_flight_info(
    flight.FlightDescriptor.for_path(["contract", "data"])
)

# Stream data
reader = client.do_get(info.endpoints[0].ticket)
table = reader.read_all()
```

## Schema

The Arrow schema includes 18 fields:

| Field | Type | Nullable | Description |
|-------|------|----------|-------------|
| `contract_id` | string | No | Contract address |
| `contract_key_type` | string | No | Type of contract key |
| `contract_durability` | string | No | Temporary or Persistent |
| `asset_code` | string | Yes | Asset code (for SAC) |
| `asset_issuer` | string | Yes | Asset issuer address |
| `asset_type` | string | Yes | Asset type descriptor |
| `balance_holder` | string | Yes | Address holding balance |
| `balance` | int64 | Yes | Balance amount |
| `last_modified_ledger` | uint32 | No | Last modification ledger |
| `deleted` | bool | No | Deletion flag |
| `ledger_sequence` | uint32 | No | Current ledger |
| `closed_at` | timestamp_us | No | Ledger close time |
| `ledger_key_hash` | string | Yes | Ledger entry hash |
| `key_xdr` | string | Yes | Contract key XDR |
| `val_xdr` | string | Yes | Contract value XDR |
| `contract_instance_type` | string | Yes | Instance type |
| `contract_instance_wasm_hash` | string | Yes | WASM hash |
| `expiration_ledger_seq` | uint32 | Yes | Expiration ledger |

## Health & Monitoring

### Health Endpoints
- `GET /health` - Overall service health
- `GET /metrics` - Prometheus metrics

### Prometheus Metrics
- `contracts_processed` - Total contract data entries processed
- `entries_skipped` - Entries filtered out
- `batches_created` - Arrow batches created
- `bytes_processed` - Total bytes processed
- `processing_rate` - Entries per second

## Development

### Running Tests
```bash
cd go
go test -v ./...
```

### Debugging with gRPC UI
```bash
grpcui -plaintext localhost:50054
```

### Using the Development Shell
```bash
nix develop
# Now you have all tools available
make gen-proto
make test
```

## Performance

- Parallel processing with configurable workers
- Columnar data format for efficient analytics
- Batch processing to reduce overhead
- Memory-mapped Arrow buffers for zero-copy operations
- Worker pool pattern for CPU-bound operations

## Integration

This service integrates with:
- **stellar-live-source-datalake**: For raw ledger data
- **PostgreSQL consumers**: For storing contract data (see consumer/ directory)
- **Analytics tools**: Via Arrow Flight protocol
- **flowctl**: For monitoring and control (when enabled)

## Architecture Notes

The hybrid architecture separates concerns:
1. **Control Plane (gRPC)**: Low-latency management operations
2. **Data Plane (Arrow Flight)**: High-throughput data streaming

This design allows:
- Independent scaling of control and data operations
- Use of specialized protocols for each concern
- Better resource utilization
- Clear separation of management and data flow

## Troubleshooting

### Common Issues

1. **Connection to data source fails**
   - Ensure stellar-live-source-datalake is running
   - Check SOURCE_ENDPOINT configuration

2. **No data being streamed**
   - Verify filters aren't too restrictive
   - Check if ledger range contains contract data

3. **High memory usage**
   - Reduce BATCH_SIZE
   - Decrease WORKER_COUNT

## License

[License information]