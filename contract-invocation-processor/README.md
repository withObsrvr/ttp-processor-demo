# Contract Invocation Processor

A high-performance gRPC service for streaming Stellar Soroban contract invocation events with comprehensive filtering capabilities and Protocol 23 support.

## Overview

The Contract Invocation Processor extracts and streams contract invocation events from Stellar blockchain data, supporting all Soroban operations:

- **Contract Calls** (`InvokeHostFunction` with `InvokeContract`)
- **Contract Creation** (`InvokeHostFunction` with `CreateContract`)
- **WASM Upload** (`InvokeHostFunction` with `UploadContractWasm`)

### Key Features

- **Real-time Streaming**: Low-latency gRPC streaming of contract events
- **Advanced Filtering**: Filter by contracts, functions, accounts, content patterns, and more
- **Protocol 23 Support**: Archive state tracking and automatic restoration monitoring
- **Multi-language Support**: Generated clients for Go, Node.js, and WebAssembly
- **Enterprise Monitoring**: Comprehensive metrics, health checks, and logging
- **Flowctl Integration**: Optional integration with Obsrvr flowctl orchestration

## Quick Start

### Prerequisites

- **Go 1.23+**
- **Protocol Buffers compiler** (`protoc`)
- **Make** (for build automation)

### Installation

1. **Install development tools**:
```bash
make install-tools
```

2. **Initialize the project**:
```bash
make setup
```

3. **Build the processor**:
```bash
make build
```

4. **Run the processor**:
```bash
make run
```

The processor will start on port `50054` with health checks on port `8089`.

## Configuration

Configure the processor using environment variables:

### Required Configuration

| Variable | Description | Default |
|----------|-------------|---------|
| `NETWORK_PASSPHRASE` | Stellar network passphrase | `"Public Global Stellar Network ; September 2015"` |
| `SOURCE_SERVICE_ADDRESS` | Address of raw ledger source service | `localhost:50052` |

### Optional Configuration

| Variable | Description | Default |
|----------|-------------|---------|
| `PROCESSOR_PORT` | gRPC service port | `50054` |
| `HEALTH_PORT` | Health check server port | `8089` |
| `LOG_LEVEL` | Logging level (debug, info, warn, error) | `info` |
| `ENABLE_FLOWCTL` | Enable flowctl integration | `false` |
| `FLOWCTL_ENDPOINT` | Flowctl control plane endpoint | `localhost:8080` |

### Network-Specific Configuration

**Mainnet (Pubnet)**:
```bash
export NETWORK_PASSPHRASE="Public Global Stellar Network ; September 2015"
make run
```

**Testnet**:
```bash
make run-testnet
```

## Usage

### Basic Streaming

Connect to the gRPC service and stream contract invocation events:

```go
// Go client example
client := pb.NewContractInvocationServiceClient(conn)
req := &pb.GetInvocationsRequest{
    StartLedger: 1000000,
    EndLedger:   0, // 0 = live stream
}

stream, err := client.GetContractInvocations(ctx, req)
for {
    event, err := stream.Recv()
    if err != nil {
        break
    }
    // Process event
    processContractInvocationEvent(event)
}
```

### Advanced Filtering

Filter events by various criteria:

```go
req := &pb.GetInvocationsRequest{
    StartLedger: 1000000,
    EndLedger:   1001000,
    
    // Basic filters
    ContractIds: []string{"CBIELTK6YBZJU5UP2WWQEUCYKLPU6AUNZ2BQ4WWFEIE3USCIHMXQDAMA"},
    FunctionNames: []string{"transfer", "mint"},
    InvokingAccounts: []string{"GDQNY3PBOJOKYZSRMK2S7LHHGWZIUISD4QORETLMXEWXBI7KFZZMKTL3"},
    SuccessfulOnly: true,
    TypeFilter: pb.InvocationTypeFilter_INVOCATION_TYPE_FILTER_CONTRACT_CALL,
    
    // Advanced content filtering
    ContentFilter: &pb.EventContentFilter{
        HasDiagnosticEvents: true,
        HasStateChanges: true,
        TopicFilters: []*pb.TopicFilter{
            {
                TopicIndex: 0,
                FilterType: &pb.TopicFilter_ExactMatch{
                    ExactMatch: "transfer",
                },
            },
        },
    },
    
    // Response options
    Options: &pb.ResponseOptions{
        Inclusion: &pb.ContentInclusion{
            IncludeDiagnosticEvents: true,
            IncludeStateChanges: true,
            IncludeContractCalls: true,
            IncludeArchiveInfo: true,
        },
    },
}
```

### Consumer Applications

Example consumer applications are provided in multiple languages:

- **Node.js**: `../consumer_app/contract_invocation_node/`
- **Go WASM**: `../consumer_app/contract_invocation_go_wasm/`

Build all consumers:
```bash
make build-consumers
```

## Architecture

### Data Flow

```
Raw Ledger Source → Contract Invocation Processor → Consumer Applications
      (gRPC)              (Event Processing)            (gRPC Clients)
```

### Event Types

The processor generates three types of events:

1. **ContractCall**: Function invocations on existing contracts
2. **CreateContract**: New contract deployments
3. **UploadWasm**: WASM code uploads

Each event includes:
- **Metadata**: Ledger info, transaction hash, success status
- **Details**: Contract IDs, function names, arguments
- **Diagnostics**: Events emitted during execution
- **State Changes**: Storage modifications
- **Protocol 23 Data**: Archive restoration info

### Protocol 23 Support

The processor includes full Protocol 23 compatibility:

- **Dual BucketList Support**: Tracks live state vs. hot archive
- **Archive Restoration Monitoring**: Detects automatic restorations
- **Enhanced State Tracking**: Differentiates between data sources

## Development

### Build Commands

```bash
make help              # Show all available commands
make setup             # Complete development setup
make build             # Build the processor binary
make test              # Run tests
make test-coverage     # Run tests with coverage
make lint              # Run linting
make format            # Format code
make clean             # Clean build artifacts
```

### Protocol Buffer Generation

```bash
make gen-proto         # Generate Go code from .proto files
```

### Docker Development

```bash
make docker-build      # Build Docker image
make docker-run        # Run in Docker container
```

### Testing

Run the full test suite:

```bash
make validate          # Run proto gen, lint, and tests
```

Watch for changes during development:

```bash
make dev-watch         # Requires 'entr' tool
```

## Monitoring

### Health Checks

The processor exposes health information at `http://localhost:8089/health`:

```json
{
  "status": "healthy",
  "metrics": {
    "total_processed": 15420,
    "total_invocations": 1832,
    "successful_invocations": 1745,
    "failed_invocations": 87,
    "contract_calls": 1650,
    "create_contracts": 95,
    "upload_wasms": 87,
    "archive_restorations": 23,
    "error_count": 2,
    "last_processed_ledger": 1001420,
    "processing_latency": "45ms",
    "total_events_emitted": 1832
  }
}
```

### Metrics

Key metrics tracked:

- **Processing Metrics**: Ledgers processed, latency, throughput
- **Event Metrics**: Invocations by type, success/failure rates
- **Protocol 23 Metrics**: Archive restorations, data source distribution
- **Error Metrics**: Error counts, last error details

### Logging

Structured logging with configurable levels:

```bash
LOG_LEVEL=debug make run    # Enable debug logging
```

## Integration

### Flowctl Integration

Enable integration with Obsrvr flowctl:

```bash
export ENABLE_FLOWCTL=true
export FLOWCTL_ENDPOINT=localhost:8080
make run
```

The processor will:
- Register with the control plane
- Send periodic heartbeats with metrics
- Report health status
- Support graceful shutdown

### Pipeline Configuration

Example flowctl pipeline configuration:

```yaml
apiVersion: flow.obsrvr.dev/v1
kind: Pipeline
metadata:
  name: contract-invocation-pipeline
spec:
  components:
    sources:
      - id: stellar-data-source
        type: stellar-live-source-datalake
    processors:
      - id: contract-invocation-processor
        type: contract-invocation-processor
        config:
          network_passphrase: "Public Global Stellar Network ; September 2015"
    sinks:
      - id: contract-events-sink
        type: stdout-sink
```

## Troubleshooting

### Common Issues

1. **"protoc not found"**
   ```bash
   # Install Protocol Buffers compiler
   # macOS: brew install protobuf
   # Ubuntu: apt-get install protobuf-compiler
   make install-tools
   ```

2. **"failed to connect to source service"**
   ```bash
   # Ensure source service is running
   # Check SOURCE_SERVICE_ADDRESS configuration
   export SOURCE_SERVICE_ADDRESS=localhost:50052
   ```

3. **"gRPC connection failed"**
   ```bash
   # Check port availability
   netstat -an | grep 50054
   # Verify firewall settings
   ```

4. **High memory usage**
   ```bash
   # Reduce batch sizes or add filtering
   # Monitor metrics at /health endpoint
   ```

### Debug Mode

Enable detailed logging:

```bash
LOG_LEVEL=debug make run
```

Check service status:

```bash
curl http://localhost:8089/health | jq
```

## Performance

### Optimization Tips

1. **Use Filtering**: Reduce network traffic with specific filters
2. **Batch Processing**: Use `max_events_per_response` for high-volume streams
3. **Content Inclusion**: Only include needed data (diagnostic events, state changes)
4. **Time Windows**: Use bounded ranges for historical queries

### Benchmarks

Typical performance on modern hardware:

- **Throughput**: 1000+ events/second
- **Latency**: <50ms P99
- **Memory**: <500MB under load
- **CPU**: <10% single core utilization

## Contributing

### Development Workflow

1. Make changes to `.proto` files or Go code
2. Run `make validate` to ensure quality
3. Add tests for new functionality
4. Update documentation as needed
5. Submit pull request

### Code Style

- Follow Go standard formatting (`make format`)
- Use structured logging with zap
- Include comprehensive error handling
- Write tests for new features

## License

[Add license information]

## Support

For questions and support:
- Create an issue in the repository
- Check the troubleshooting guide above
- Review the example consumer applications