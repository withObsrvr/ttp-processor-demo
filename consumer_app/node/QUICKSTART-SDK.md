# SDK Consumer Quickstart

## What Changed?

Your old Node consumer used **direct gRPC client** pattern:
```
Consumer → (calls GetTTPEvents) → Processor
```

The new SDK consumer uses **event-driven pattern**:
```
Processor → flowctl control plane → (calls Consume) → Consumer
```

## Quick Start

### 1. Start the SDK Consumer

```bash
cd consumer_app/node

# Without flowctl (standalone)
PORT=":9090" HEALTH_PORT="9089" ENABLE_FLOWCTL="false" npm run start:sdk

# With flowctl integration
PORT=":9090" HEALTH_PORT="9089" ENABLE_FLOWCTL="true" npm run start:sdk
```

The consumer will:
- ✅ Start gRPC server on port 9090
- ✅ Start health endpoint on port 9089
- ✅ Register with flowctl (if enabled)
- ✅ Wait for events from processors

### 2. Test Health Endpoint

```bash
curl http://localhost:9089/health
```

Expected output:
```json
{
  "status": "healthy",
  "metrics": {
    "success_count": 0,
    "error_count": 0,
    "total_processed": 0,
    "last_processed_ledger": 0,
    "processing_latency": "0ms"
  }
}
```

### 3. Connect to ttp-processor-sdk

The `ttp-processor-sdk` will send events to your consumer automatically via flowctl.

**Prerequisites:**
1. flowctl control plane running on port 8080
2. ttp-processor-sdk running and registered with flowctl
3. Data source feeding ledgers to the processor

**Full pipeline:**
```bash
# Terminal 1: Start flowctl
cd /path/to/flowctl
flowctl serve

# Terminal 2: Start data source
cd stellar-live-source-sdk/go
ENABLE_FLOWCTL=true ./bin/stellar-live-source

# Terminal 3: Start processor
cd ttp-processor-sdk/go
ENABLE_FLOWCTL=true ./bin/ttp-processor

# Terminal 4: Start consumer
cd consumer_app/node
ENABLE_FLOWCTL=true PORT=":9090" HEALTH_PORT="9089" npm run start:sdk
```

Events will flow automatically:
```
Data Source → Processor → flowctl → Consumer
                                       ↓
                                  Your handler
```

## Configuration

All configuration via environment variables:

```bash
# Identity
COMPONENT_ID="ttp-consumer-node"

# Network
PORT=":9090"              # gRPC server port
HEALTH_PORT="9089"        # HTTP health port

# Performance
MAX_CONCURRENT="10"       # Max concurrent events

# Flowctl (optional)
ENABLE_FLOWCTL="true"
FLOWCTL_ENDPOINT="localhost:8080"
FLOWCTL_HEARTBEAT_INTERVAL="10000"  # milliseconds
```

## How to Process Events

The event handler in `src/index-sdk.ts`:

```typescript
consumer.onConsume(async (event: any) => {
    // event.type = "stellar.token.transfer.v1"
    // event.payload = TokenTransferBatch (protobuf bytes)
    // event.metadata = { ledger_sequence, events_count, ... }
    // event.stellar_cursor = { ledger_sequence }

    // Decode payload
    const batch = TokenTransferBatch.decode(event.payload);

    // Process each transfer event
    for (const ttpEvent of batch.events) {
        if (ttpEvent.transfer) {
            console.log(`Transfer: ${ttpEvent.transfer.from} → ${ttpEvent.transfer.to}`);
            console.log(`Amount: ${ttpEvent.transfer.amount}`);
        }
    }
});
```

## Comparison

### Old (Direct gRPC):
```typescript
// Consumer initiates connection
const client = new EventServiceClient('localhost:50051');
const stream = client.GetTTPEvents({
    start_ledger: 1000,
    end_ledger: 2000
});

stream.on('data', (event) => {
    // Process
});
```

**Issues:**
- ❌ Hardcoded processor address
- ❌ Manual stream handling
- ❌ Manual metrics/health
- ❌ No service discovery
- ❌ 100+ lines of boilerplate

### New (SDK):
```typescript
// Consumer receives events
const consumer = new SDKConsumer(config);

consumer.onConsume(async (event) => {
    // Process - SDK handles everything
});

await consumer.start();
```

**Benefits:**
- ✅ Dynamic discovery via flowctl
- ✅ Automatic metrics/health
- ✅ Concurrency control
- ✅ Graceful shutdown
- ✅ 20 lines of business logic

## Troubleshooting

### Port already in use

Change ports:
```bash
PORT=":9090" HEALTH_PORT="9089" npm run start:sdk
```

### No events received

1. Check flowctl is running: `curl http://localhost:8080/health`
2. Check processor is registered with flowctl
3. Check event types match:
   - Processor outputs: `stellar.token.transfer.v1`
   - Consumer inputs: `stellar.token.transfer.v1`

### Connection refused

Start in order:
1. flowctl (port 8080)
2. Data source
3. Processor
4. Consumer

## Next Steps

1. **Generate proto types** - Add TypeScript proto generation for full type safety
2. **Add storage** - Store events in DB/S3
3. **Add filtering** - Filter by asset, amount, etc.
4. **Add batching** - Process events in batches

## See Also

- [README-SDK.md](./README-SDK.md) - Full documentation
- [Go SDK Consumer](../../contract-events-postgres-consumer-sdk/)
- [ttp-processor-sdk](../../ttp-processor-sdk/)
