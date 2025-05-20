# Ledger to JSON-RPC Processor

This processor converts raw Stellar ledger data into responses that match the Stellar JSON-RPC 2.0 schema. It's designed to work within the Flow/flowctl runtime, receiving LedgerClose records on its inlet and emitting JSON-RPC method results on its outlet.

## Features

- Converts historical ledger data into structures matching Stellar-RPC's JSON-RPC 2.0 schema (v20.1.0)
- Supports the following methods:
  - `getLedgers`
  - `getTransactions`
  - `getEvents`
  - `getFeeStats`
  - `getLedgerEntries`
  - `getTransaction`
  - `getLatestLedger`
- Handles pagination, cursors, filtering, and different output formats (base64 XDR vs. decoded JSON)
- Provides retention window errors identical to live RPC
- Fully integrates with Flow control plane for monitoring and management

## Configuration

The processor is configured via environment variables:

| Variable | Description | Default |
|----------|-------------|---------|
| `PORT` | The port on which the service listens for gRPC requests | 50053 |
| `HEALTH_PORT` | The port on which the health check HTTP server listens | 8089 |
| `NETWORK_PASSPHRASE` | The Stellar network passphrase (required) | - |
| `SOURCE_SERVICE_ADDRESS` | The address of the raw ledger source service | localhost:50052 |
| `RETENTION_WINDOW_DAYS` | How many days of history to retain | 7 |
| `BLOOM_FP_RATE` | False positive rate for the transaction hash Bloom filter | 0.01 |
| `ENABLE_JSON_XDR` | Enable JSON XDR format optimization | false |
| `ENABLE_FLOWCTL` | Enable integration with flowctl control plane | false |
| `FLOWCTL_ENDPOINT` | The endpoint for the flowctl control plane | localhost:8080 |
| `FLOWCTL_HEARTBEAT_INTERVAL` | Interval for sending heartbeats to flowctl | 10s |

## API

The processor exposes a gRPC API with the following methods:

### GetJsonRpcResponses

Streams JSON-RPC responses for a given ledger range and method.

```protobuf
rpc GetJsonRpcResponses(GetJsonRpcRequest) returns (stream LedgerJsonRpcEvent) {}
```

### GetJsonRpcMethod

Executes a single JSON-RPC method against the ledger data store.

```protobuf
rpc GetJsonRpcMethod(JsonRpcMethodRequest) returns (JsonRpcResponse) {}
```

## Building and Running

### Prerequisites

- Go 1.22 or later
- Protocol Buffer compiler (protoc)
- Make

### Build

```bash
make build
```

### Run

```bash
NETWORK_PASSPHRASE="Test SDF Network ; September 2015" ./ledger-jsonrpc-processor
```

## Health Checks

The processor exposes a health endpoint at `/health` that returns the current status and metrics:

```json
{
  "status": "healthy",
  "metrics": {
    "success_count": 1234,
    "error_count": 0,
    "total_processed": 1234,
    "total_events_emitted": 5678,
    "last_processed_ledger": 12345678,
    "processing_latency": "15ms"
  },
  "config": {
    "retention_window_days": 7,
    "bloom_fp_rate": 0.01,
    "enable_json_xdr": false
  }
}
```

## Flow Integration

When `ENABLE_FLOWCTL` is set to `true`, the processor will register with the Flow control plane and report metrics. The processor defines the following:

- Service Type: `PROCESSOR`
- Input Event Types: `raw_ledger_service.RawLedger`  
- Output Event Types: `ledger_jsonrpc_service.LedgerJsonRpcEvent`

## License

Apache License 2.0