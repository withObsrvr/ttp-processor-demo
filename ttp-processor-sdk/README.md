# Token Transfer Processor SDK

A Go processor implementation that extracts SEP-41 token transfer events from Stellar ledgers using the official Stellar Go SDK's `token_transfer` processor, wrapped with flowctl-sdk for integration into data pipelines.

**Latest Release:** v2.0.0-sdk (Go 1.25) - [Cycle 1 Cleanup Complete](./CYCLE1_CLEANUP_COMPLETE.md)

## Overview

The Token Transfer Processor SDK provides a standardized way to parse Stellar network transaction data and derive token transfer events. It functions as a facade to [CAP-67](https://stellar.org/protocol/cap-67), automatically generating standardized events from ledger data without requiring manual parsing of complex transaction structures.

This implementation follows the [Stellar Token Transfer Processor](https://developers.stellar.org/docs/data/indexers/build-your-own/processors/token-transfer-processor) specification and can be deployed either as a standalone processor or integrated into flowctl-based data pipelines.

## Features

### Token Event Types

The processor captures token movements from diverse operations and generates five standardized event types:

- **Transfer** - Asset movement between entities (payments, path payments, DEX trades)
- **Mint** - Token creation by issuers
- **Burn** - Token destruction (merges, revocations)
- **Clawback** - Forced asset recovery
- **Fee** - Network fees and Soroban refunds

### Supported Operations

Extracts events from all Stellar operations that involve token movements:

- Payment and Path Payment operations
- DEX trades (Manage Buy/Sell Offer, Create Passive Sell Offer)
- Account merges
- Trustline revocations
- Claimable balance operations
- Liquidity pool operations
- Clawback operations
- Smart contract events (Soroban)

### Event Metadata

Each event includes comprehensive context:

- Ledger sequence number
- Transaction hash
- Operation index (1-indexed per SEP-35)
- Transaction index
- Timestamp (ledger close time)
- Contract address (for Soroban events)
- Muxed account information (when applicable)

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    TTP Processor SDK                         │
│                                                              │
│  ┌──────────────────────────────────────────────────────┐  │
│  │           flowctl-sdk Integration Layer              │  │
│  │  • gRPC Server (Process RPC)                         │  │
│  │  • Component Registration                            │  │
│  │  • Health Checks & Metrics                           │  │
│  │  • Event Type Routing                                │  │
│  └────────────────┬─────────────────────────────────────┘  │
│                   │                                          │
│                   ▼                                          │
│  ┌──────────────────────────────────────────────────────┐  │
│  │         Stellar Token Transfer Processor             │  │
│  │  (github.com/stellar/go-stellar-sdk/processors)      │  │
│  │  • EventsFromLedger()                                │  │
│  │  • EventsFromTransaction()                           │  │
│  │  • EventsFromOperation()                             │  │
│  └──────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────┘

Input: stellar.ledger.v1 (RawLedger protobuf)
   │
   ├─► Parse LedgerCloseMeta XDR
   ├─► Extract token transfer events
   ├─► Convert to protobuf format
   │
Output: stellar.token.transfer.v1 (TokenTransferBatch protobuf)
```

## Installation

### As a Go Module

```bash
go get github.com/withObsrvr/ttp-processor-sdk
```

### From Source

```bash
git clone https://github.com/withObsrvr/ttp-processor-demo.git
cd ttp-processor-demo/ttp-processor-sdk/go
go build -o ttp-processor main.go
```

## Usage

### Standalone Mode

Run the processor as a standalone gRPC service:

```bash
# Required environment variables
export NETWORK_PASSPHRASE="Public Global Stellar Network ; September 2015"
export PORT=":50051"
export HEALTH_PORT="8088"

# Optional: Component identification
export COMPONENT_ID="ttp-processor"

./ttp-processor
```

The processor exposes:
- **gRPC Service** on port 50051 (configurable via `PORT`)
- **Health Check** endpoint on port 8088 at `/health`

### With flowctl Integration

Enable flowctl integration for automatic pipeline orchestration:

```bash
export NETWORK_PASSPHRASE="Public Global Stellar Network ; September 2015"
export PORT=":50051"
export HEALTH_PORT="8088"

# Enable flowctl integration
export ENABLE_FLOWCTL="true"
export FLOWCTL_ENDPOINT="localhost:8080"
export FLOWCTL_HEARTBEAT_INTERVAL="10s"

./ttp-processor
```

The processor will:
1. Register with the flowctl control plane
2. Declare input event types: `stellar.ledger.v1`
3. Declare output event types: `stellar.token.transfer.v1`
4. Automatically wire into pipelines based on event type matching
5. Send heartbeats and metrics to the control plane

### Pipeline Configuration (flowctl)

Create a pipeline YAML file:

```yaml
name: stellar-token-transfer-pipeline
description: Extract token transfer events from Stellar ledgers

components:
  - id: stellar-source
    type: source
    image: withobsrvr/stellar-live-source-sdk:latest
    env:
      BACKEND_TYPE: RPC
      RPC_ENDPOINT: https://horizon-testnet.stellar.org
      NETWORK_PASSPHRASE: "Test SDF Network ; September 2015"
      START_LEDGER: "1000000"
      END_LEDGER: "1001000"
      ENABLE_FLOWCTL: "true"

  - id: ttp-processor
    type: processor
    image: withobsrvr/ttp-processor-sdk:latest
    env:
      NETWORK_PASSPHRASE: "Test SDF Network ; September 2015"
      ENABLE_FLOWCTL: "true"

  - id: consumer
    type: consumer
    image: your-consumer:latest
    env:
      ENABLE_FLOWCTL: "true"
```

Run the pipeline:

```bash
flowctl run pipeline.yaml
```

flowctl will automatically:
- Start all components
- Wire source → processor → consumer based on event types
- Monitor component health
- Handle reconnections and failures

## Configuration

### Environment Variables

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `NETWORK_PASSPHRASE` | Yes | - | Stellar network passphrase |
| `PORT` | No | `:50051` | gRPC server port |
| `HEALTH_PORT` | No | `8088` | Health check HTTP port |
| `COMPONENT_ID` | No | `ttp-processor` | Unique component identifier |
| `ENABLE_FLOWCTL` | No | `false` | Enable flowctl integration |
| `FLOWCTL_ENDPOINT` | No | `localhost:8080` | flowctl control plane address |
| `FLOWCTL_HEARTBEAT_INTERVAL` | No | `10s` | Heartbeat interval |

### Network Passphrases

**Mainnet (Pubnet)**:
```bash
export NETWORK_PASSPHRASE="Public Global Stellar Network ; September 2015"
```

**Testnet**:
```bash
export NETWORK_PASSPHRASE="Test SDF Network ; September 2015"
```

**Futurenet**:
```bash
export NETWORK_PASSPHRASE="Test SDF Future Network ; October 2022"
```

## Protocol Buffer Schema

### Input: RawLedger

```protobuf
message RawLedger {
  uint32 sequence = 1;
  bytes ledger_close_meta_xdr = 2;
}
```

### Output: TokenTransferBatch

```protobuf
message TokenTransferBatch {
  repeated TokenTransferEvent events = 1;
}

message TokenTransferEvent {
  TokenTransferEventMeta meta = 1;
  oneof event {
    Transfer transfer = 2;
    Mint mint = 3;
    Burn burn = 4;
    Clawback clawback = 5;
    Fee fee = 6;
  }
}
```

Full schema: [flow-proto/stellar/v1/token_transfers.proto](https://github.com/withObsrvr/flow-proto/blob/main/proto/stellar/v1/token_transfers.proto)

## Processing Modes

### Default Mode (Recommended)

Analyzes operations, results, and ledger entry changes. Compatible with all Stellar ledgers.

```go
ttpProcessor := token_transfer.NewEventsProcessor(networkPassphrase)
events, err := ttpProcessor.EventsFromLedger(ledgerCloseMeta)
```

This mode is used by default in the SDK and works with any Stellar network.

### Unified Events Mode

Reads directly from CAP-67 unified events. Requires stellar-core to be configured with:
- `EMIT_CLASSIC_EVENTS=true`
- `BACKFILL_STELLAR_ASSET_EVENTS=true`

More efficient but less compatible. Only use if your ledger source supports unified events.

## API Granularity Levels

The underlying Stellar processor provides three levels of granularity:

### 1. Ledger-Level Processing

Process an entire ledger and get chronologically ordered events:

```go
events, err := ttpProcessor.EventsFromLedger(ledgerCloseMeta)
```

### 2. Transaction-Level Processing

Process a single transaction, separating fee events from operation events:

```go
feeEvents, opEvents, err := ttpProcessor.EventsFromTransaction(
    txMeta,
    ledgerSequence,
    ledgerClosedAt,
    txHash,
    txIndex,
)
```

### 3. Operation-Level Processing

Process individual operations:

```go
events, err := ttpProcessor.EventsFromOperation(
    operation,
    opResult,
    txMeta,
    ledgerSequence,
    ledgerClosedAt,
    txHash,
    txIndex,
    opIndex,
)
```

The SDK uses ledger-level processing by default for optimal performance and correct event ordering.

## Event Ordering

Event ordering depends on the Stellar protocol version:

**Pre-Protocol 23**:
1. All fee events first
2. Per-transaction operations
3. Immediate Soroban refunds

**Protocol 23+**:
1. All fee events
2. All operations
3. All Soroban refunds at end

The processor automatically applies the correct ordering based on the ledger's protocol version.

## Development

### Building from Source

```bash
cd ttp-processor-sdk/go
go build -o ttp-processor main.go
```

### Running Tests

```bash
go test ./...
```

### Dependencies

The processor uses:
- **Go 1.25+** - Modern Go toolchain with container-aware GOMAXPROCS
- **Stellar Go SDK** - Official Stellar processor implementation
  - `github.com/stellar/go-stellar-sdk/processors/token_transfer`
- **flowctl-sdk** - Pipeline integration framework
  - `github.com/withObsrvr/flowctl-sdk/pkg/processor`
  - `github.com/withObsrvr/flowctl-sdk/pkg/flowctl` (metrics & health)
- **flow-proto** - Protobuf definitions
  - `github.com/withObsrvr/flow-proto/go/gen/stellar/v1`

## Docker Deployment

### Build Image

```bash
docker build -t ttp-processor-sdk:latest .
```

### Run Container

```bash
docker run -d \
  --name ttp-processor \
  -p 50051:50051 \
  -p 8088:8088 \
  -e NETWORK_PASSPHRASE="Public Global Stellar Network ; September 2015" \
  -e ENABLE_FLOWCTL="true" \
  -e FLOWCTL_ENDPOINT="host.docker.internal:8080" \
  ttp-processor-sdk:latest
```

## Integration Examples

### With Stellar Live Source

```yaml
components:
  - id: stellar-source
    type: source
    image: withobsrvr/stellar-live-source-sdk:latest
    env:
      BACKEND_TYPE: RPC
      RPC_ENDPOINT: https://rpc-pubnet.stellar.org
      START_LEDGER: "50000000"
      OUTPUT_EVENT_TYPE: "stellar.ledger.v1"

  - id: ttp-processor
    type: processor
    image: withobsrvr/ttp-processor-sdk:latest
    env:
      INPUT_EVENT_TYPE: "stellar.ledger.v1"
      OUTPUT_EVENT_TYPE: "stellar.token.transfer.v1"
```

### With PostgreSQL Consumer

```yaml
components:
  - id: ttp-processor
    type: processor
    # ... processor config

  - id: postgres-sink
    type: consumer
    image: withobsrvr/postgres-consumer:latest
    env:
      INPUT_EVENT_TYPE: "stellar.token.transfer.v1"
      POSTGRES_HOST: "localhost"
      POSTGRES_DB: "stellar_events"
```

## Performance Considerations

### Event Volume

Token transfer events can be generated at high volume:
- Mainnet: ~5-50 events per ledger on average
- High-activity ledgers: 100+ events possible
- Batch processing: Events are grouped by ledger for efficiency

### Memory Usage

- Processor maintains minimal state
- Events are streamed, not buffered
- Memory usage scales with ledger complexity, not history

### Throughput

- Can process 5-10 ledgers/second on modern hardware
- gRPC streaming enables backpressure handling
- Suitable for both real-time and historical processing

## Monitoring

### Health Endpoint

```bash
curl http://localhost:8088/health
```

Response:
```json
{
  "status": "healthy",
  "component_id": "ttp-processor",
  "version": "2.0.0-sdk",
  "uptime_seconds": 3600
}
```

### Metrics (when flowctl enabled)

The processor reports metrics to flowctl:
- Events processed count
- Processing latency
- Error rates
- Current ledger sequence

## Troubleshooting

### Common Issues

**"Failed to unmarshal XDR"**
- Ensure `NETWORK_PASSPHRASE` matches your ledger source
- Verify XDR data is not corrupted

**"No token transfer events in ledger"**
- Normal for ledgers without token operations
- Check logs for actual event counts

**"Connection refused"**
- Verify gRPC port is accessible
- Check firewall and Docker network settings

**flowctl registration fails**
- Ensure flowctl control plane is running
- Verify `FLOWCTL_ENDPOINT` is correct
- Check network connectivity

## Comparison with Stellar Reference Implementation

This SDK wraps the official Stellar token transfer processor from `github.com/stellar/go-stellar-sdk/processors/token_transfer`. Key differences:

| Feature | Stellar Processor | TTP Processor SDK |
|---------|-------------------|-------------------|
| Core Logic | ✅ Official implementation | ✅ Uses Stellar processor |
| Event Types | ✅ Transfer, Mint, Burn, Clawback, Fee | ✅ Same |
| Standalone Use | ✅ Go library | ✅ gRPC service |
| Pipeline Integration | ❌ Not included | ✅ flowctl-sdk wrapper |
| Protobuf I/O | ❌ Go structs only | ✅ Protobuf events |
| Auto-wiring | ❌ Manual setup | ✅ Event type routing |
| Health Checks | ❌ Not included | ✅ Built-in |
| Metrics | ❌ Custom implementation | ✅ flowctl integration |

The SDK provides a production-ready service wrapper around Stellar's processor while maintaining 100% compatibility with the official event extraction logic.

## Contributing

Contributions are welcome! This processor aims to stay aligned with Stellar's reference implementation.

When contributing:
1. Ensure compatibility with latest Stellar Go SDK
2. Follow Stellar's event schema and ordering rules
3. Add tests for new functionality
4. Update documentation

## References

- [Stellar Token Transfer Processor Documentation](https://developers.stellar.org/docs/data/indexers/build-your-own/processors/token-transfer-processor)
- [CAP-67: Smart Contract Standardized Asset](https://stellar.org/protocol/cap-67)
- [SEP-41: Token Interface](https://github.com/stellar/stellar-protocol/blob/master/ecosystem/sep-0041.md)
- [Stellar Go SDK Processors](https://github.com/stellar/go-stellar-sdk/tree/master/processors)
- [flowctl-sdk Documentation](https://github.com/withObsrvr/flowctl-sdk)

## License

[MIT License](LICENSE)

## Support

- **Issues**: [GitHub Issues](https://github.com/withObsrvr/ttp-processor-demo/issues)
- **Stellar Developers**: [Stellar Discord](https://discord.gg/stellardev)
