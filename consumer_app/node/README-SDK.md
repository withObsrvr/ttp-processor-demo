# SDK-based TTP Events Consumer

This is an SDK-based consumer implementation that follows the flowctl SDK architecture pattern.

## Architecture

Unlike the old direct gRPC client approach, this SDK-based consumer:

1. **Runs as a gRPC server** - Implements the `ConsumerService` interface
2. **Registers with flowctl** - Connects to the flowctl control plane for service discovery
3. **Receives events via RPC** - Processors send events to the consumer via `Consume()` RPC calls
4. **Event-driven** - Subscribes to event types (e.g., `stellar.token.transfer.v1`)

### Event Flow

```
ttp-processor-sdk → flowctl control plane → SDK consumer
                          ↓
                    (routes by event type)
```

The control plane matches:
- Processor output type: `stellar.token.transfer.v1`
- Consumer input type: `stellar.token.transfer.v1`

Then establishes a gRPC connection between them.

## Configuration

Configure via environment variables:

```bash
# Consumer Identity
COMPONENT_ID="ttp-consumer-node"

# gRPC Server
PORT=":8090"                    # Consumer gRPC endpoint
HEALTH_PORT="8089"              # Health check HTTP port
MAX_CONCURRENT="10"             # Max concurrent event processing

# Flowctl Integration (optional but recommended)
ENABLE_FLOWCTL="true"
FLOWCTL_ENDPOINT="localhost:8080"
FLOWCTL_HEARTBEAT_INTERVAL="10000"  # milliseconds
```

## Running the SDK Consumer

### Prerequisites

1. **flowctl control plane** must be running:
   ```bash
   # Start flowctl on port 8080
   flowctl serve
   ```

2. **ttp-processor-sdk** must be running and registered with flowctl:
   ```bash
   cd ../../ttp-processor-sdk/go
   ENABLE_FLOWCTL=true ./bin/ttp-processor
   ```

3. **Data source** must be running and feeding the processor

### Start the Consumer

```bash
# Install dependencies
npm install

# Start with flowctl integration
ENABLE_FLOWCTL=true npm run start:sdk

# Or start standalone (no flowctl)
npm run start:sdk
```

### Development Mode

```bash
# Auto-restart on file changes
npm run dev:sdk
```

## How It Works

### 1. Consumer Startup

The consumer:
- Starts a gRPC server on port 8090
- Implements the `ConsumerService` interface
- Starts an HTTP health endpoint on port 8089

### 2. Flowctl Registration

If `ENABLE_FLOWCTL=true`:
- Registers with control plane as a consumer
- Declares input event types: `["stellar.token.transfer.v1"]`
- Sends heartbeats every 10 seconds with metrics

### 3. Event Processing

When a processor emits events:
- Control plane routes events to consumer's `Consume()` RPC
- Consumer processes event in `onConsume()` handler
- Returns success/error response
- Updates metrics

### 4. Event Handler

The `onConsume` handler receives events like:

```typescript
{
  id: "token-transfers-12345",
  type: "stellar.token.transfer.v1",
  payload: <Buffer>, // TokenTransferBatch protobuf
  metadata: {
    ledger_sequence: "12345",
    events_count: "3",
  },
  stellar_cursor: {
    ledger_sequence: 12345
  }
}
```

## Health Checks

Health endpoint is available at:

```bash
curl http://localhost:8089/health
```

Returns:
```json
{
  "status": "healthy",
  "metrics": {
    "success_count": 150,
    "error_count": 2,
    "total_processed": 152,
    "last_processed_ledger": 12345,
    "processing_latency": "25ms"
  }
}
```

## Comparison: Old vs SDK

### Old Direct gRPC Approach

```typescript
// Consumer connects TO processor
const client = new EventServiceClient('localhost:50051');
const stream = client.GetTTPEvents({
  start_ledger: 1000,
  end_ledger: 2000
});

stream.on('data', (event) => {
  // Process event
});
```

**Problems:**
- Hardcoded processor endpoint
- Consumer must know where processor is
- No service discovery
- Manual metrics/health implementation
- No concurrency control

### SDK Approach

```typescript
// Consumer runs as server
const consumer = new SDKConsumer(config);

consumer.onConsume(async (event) => {
  // Process event - SDK handles everything else
});

await consumer.start();
```

**Benefits:**
- Dynamic service discovery via flowctl
- Automatic metrics and health checks
- Built-in concurrency control
- Graceful shutdown handling
- 70-85% less boilerplate

## Troubleshooting

### Consumer starts but receives no events

1. Check flowctl is running:
   ```bash
   curl http://localhost:8080/health
   ```

2. Check processor is registered:
   ```bash
   # Look for ttp-processor in registered components
   ```

3. Check event type matching:
   - Processor must output: `stellar.token.transfer.v1`
   - Consumer must input: `stellar.token.transfer.v1`

### Connection refused errors

Make sure services start in this order:
1. flowctl control plane (port 8080)
2. Data source (e.g., stellar-live-source)
3. Processor (ttp-processor-sdk)
4. Consumer (this app)

### No events being processed

Check that the data source is actually producing ledgers and the processor is extracting events from them. Look at processor logs for:
- "Extracted token transfer events"
- Event counts

## Next Steps

1. **Add proto generation** - Generate TypeScript code from flow-proto
2. **Implement decode** - Decode `TokenTransferBatch` from event payload
3. **Add storage** - Store events in database, S3, etc.
4. **Add filtering** - Filter events by asset, amount, etc.

## See Also

- [Go SDK Consumer Example](../../contract-events-postgres-consumer-sdk/)
- [flowctl Documentation](../../../docs/flowctl/)
- [flow-proto Definitions](https://github.com/withObsrvr/flow-proto)
