# Stellar Arrow Source

Native Apache Arrow data source for Stellar ledger data streaming via Arrow Flight protocol.

## Overview

The Stellar Arrow Source is the first component in Phase 1 of the Apache Arrow implementation plan. It provides:

- **Native Arrow Flight streaming** of Stellar ledger data
- **Zero-copy data processing** using Apache Arrow
- **Structured logging** with zerolog
- **Production-ready health monitoring**
- **Nix-based development environment**

## Quick Start

### Using Nix (Recommended)

```bash
# Enter development environment
make nix-shell

# Build the service
make build

# Run tests
make test

# Start the service
make run
```

### Traditional Go Development

```bash
# Install dependencies
cd go && go mod download

# Build
make build

# Run
./stellar-arrow-source
```

## Configuration

Configure via environment variables:

```bash
export PORT=8815                    # Arrow Flight port
export HEALTH_PORT=8088             # Health check port
export SOURCE_ENDPOINT=localhost:8080  # Existing source service
export NETWORK_PASSPHRASE="Test SDF Network ; September 2015"
export BATCH_SIZE=1000              # Records per batch
export LOG_LEVEL=info               # debug, info, warn, error
export FLOWCTL_ENABLED=false        # Enable flowctl integration
```

## API Endpoints

### Arrow Flight (Port 8815)

The service provides native Arrow Flight streaming:

```
arrow-flight://localhost:8815
```

**Stream Types:**
- `stellar_ledger` - Complete ledger data
- `ttp_events` - Token transfer events (future)
- `transactions` - Individual transactions (future)

**Ticket Format:**
```
stream_type:start_ledger:end_ledger
```

Examples:
- `stellar_ledger:100:200` - Ledgers 100-200
- `stellar_ledger:500:` - From ledger 500 (unbounded)
- `stellar_ledger` - From current ledger

### Health Endpoints (Port 8088)

- `GET /health` - Service health status
- `GET /ready` - Readiness check
- `GET /metrics` - Basic Prometheus metrics

## Arrow Schemas

### Stellar Ledger Schema

Native Arrow schema optimized for analytics:

```
ledger_sequence: uint32
ledger_close_time: timestamp[us]
ledger_hash: fixed_size_binary[32]
previous_ledger_hash: fixed_size_binary[32]
transaction_count: uint32
operation_count: uint32
successful_transaction_count: uint32
failed_transaction_count: uint32
protocol_version: uint32
base_fee: uint32
base_reserve: uint32
max_tx_set_size: uint32
close_time_resolution: uint32
ledger_close_meta_xdr: binary
```

## Client Examples

### Go Client

```go
import (
    "github.com/apache/arrow/go/v17/arrow/flight"
    "google.golang.org/grpc"
)

// Connect to Arrow Flight server
conn, _ := grpc.Dial("localhost:8815", grpc.WithInsecure())
client := flight.NewFlightServiceClient(conn)

// Stream ledger data
ticket := &flight.Ticket{Ticket: []byte("stellar_ledger:100:200")}
stream, _ := client.DoGet(context.Background(), ticket)

for {
    data, err := stream.Recv()
    if err == io.EOF { break }
    
    // Process Arrow record
    record, _ := flight.DeserializeRecord(data, allocator)
    fmt.Printf("Received %d records\n", record.NumRows())
    record.Release()
}
```

### Python Client

```python
import pyarrow.flight as flight

# Connect to server
client = flight.FlightClient("grpc://localhost:8815")

# Get flight info
descriptor = flight.FlightDescriptor.for_command(b"stellar_ledger:100:200")
flight_info = client.get_flight_info(descriptor)

# Stream data
for batch in client.do_get(flight_info.endpoints[0].ticket):
    table = batch.to_table()
    print(f"Received {len(table)} records")
```

## Development

### Project Structure

```
stellar-arrow-source/
├── go/
│   ├── main.go              # Service entry point
│   ├── logging/             # Structured logging with zerolog
│   ├── server/              # Arrow Flight server implementation
│   ├── schema/              # Arrow schema definitions
│   ├── converter/           # XDR to Arrow conversion
│   └── client/              # Source client integration
├── protos/                  # Protocol definitions (future)
├── flake.nix               # Nix development environment
└── Makefile                # Build automation
```

### Testing

```bash
# Unit tests
make test

# Arrow-specific tests
make test-arrow-schemas
make test-arrow-flight

# Integration tests
make test-integration

# Benchmarks
make benchmark
```

### Logging

The service uses structured logging with zerolog:

```json
{
  "level": "info",
  "component": "stellar-arrow-source",
  "version": "v1.0.0",
  "operation": "arrow_stream_start",
  "start_ledger": 100,
  "end_ledger": 200,
  "stream_type": "stellar_ledger",
  "native_processing": true,
  "time": "2025-08-03T10:15:30Z",
  "message": "Starting native Arrow stream"
}
```

## Performance

Phase 1 targets:
- **200+ ledgers/sec** processing rate
- **Zero-copy Arrow operations** for maximum efficiency
- **<100ms latency** for stream initiation
- **Memory efficient** with Arrow allocators

## Architecture

```
Client → Arrow Flight → Stellar Arrow Source → Existing Sources
                            ↓
                      Native Arrow Records
                            ↓
                      Zero-copy streaming
```

## Next Steps

Phase 1 establishes the foundation. Future phases will add:

- **Phase 2**: Native Arrow TTP processing with vectorized operations
- **Phase 3**: Parquet analytics sink with real-time streaming
- **Phase 4**: Production deployment and legacy migration

## Contributing

1. Use the Nix development environment: `make nix-shell`
2. Follow structured logging patterns with zerolog
3. All processing must use native Arrow operations
4. Write tests for new functionality
5. Run benchmarks to validate performance

## License

MIT License - see LICENSE file for details.