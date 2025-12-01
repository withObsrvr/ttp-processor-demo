# Demo Consumer Guide

A simple stdout consumer for the contract-events-processor that demonstrates how to consume contract events.

## Quick Start

```bash
# From contract-events-processor directory
cd examples/go-consumer

# Run with default settings (ledgers 1000-1100)
./run.sh

# Stream ledgers 1000-2000
./run.sh 1000 2000

# Stream continuously from ledger 1000
./run.sh 1000 0
```

## Features

✅ **Simple stdout output** - Easy to read event stream
✅ **Filtering support** - Filter by contract ID, event type
✅ **Graceful shutdown** - Press Ctrl+C to stop cleanly
✅ **Statistics summary** - Shows total events and ledger range
✅ **Pretty printing** - JSON formatted with indentation

## Environment Variables

```bash
# Service address
CONTRACT_EVENTS_SERVICE_ADDRESS="localhost:50053" ./run.sh 1000 0

# Filter by specific contracts
FILTER_CONTRACT_IDS="CABC123...,CDEF456..." ./run.sh 1000 0

# Filter by event types
FILTER_EVENT_TYPES="transfer,mint,burn" ./run.sh 1000 0

# Include failed transactions
INCLUDE_FAILED=true ./run.sh 1000 0

# Include diagnostic events
INCLUDE_DIAGNOSTICS=true ./run.sh 1000 0

# Combine multiple filters
FILTER_CONTRACT_IDS="CABC..." \
FILTER_EVENT_TYPES="transfer" \
INCLUDE_FAILED=false \
./run.sh 1000 0
```

## Example Output

```
Contract Events Consumer
========================
Service:      localhost:50053
Start Ledger: 1000
End Ledger:   1100

2025/12/01 07:20:00 Connecting to Contract Events service at localhost:50053
2025/12/01 07:20:00 Requesting contract events from ledger 1000 to 1100

================================================================================
Contract Events Stream
================================================================================

--- Event #1 ---
Ledger:       1234 (closed at 1701234567)
TX Hash:      abc123def456...
TX Status:    ✓ Successful
Contract:     CABC123...
Event Type:   transfer
Event Index:  0
Op Index:     1

Topics (3):
  [0] transfer
  [1] map[address:GABC... type:account]
  [2] map[address:GDEF... type:account]

Data:
  {
    "amount": "1000000000",
    "asset": {
      "code": "USDC",
      "issuer": "GXYZ..."
    }
  }

--- Event #2 ---
Ledger:       1235 (closed at 1701234568)
TX Hash:      def456ghi789...
TX Status:    ✓ Successful
Contract:     CDEF456...
Event Type:   mint
Event Index:  0
Op Index:     (transaction-level event)

Topics (2):
  [0] mint
  [1] map[address:GABC... type:account]

Data:
  {
    "amount": "5000000000"
  }

================================================================================
Stream Summary:
  Total Events: 42
  Ledgers Processed: 10
  Ledger Range: 1000 - 1100
================================================================================
```

## Building from Source

```bash
cd examples/go-consumer

# Install dependencies
go mod tidy

# Build
make build

# Run
./contract-events-consumer 1000 2000
```

## Integration Patterns

This consumer demonstrates patterns for:

### 1. Basic Streaming
```go
stream, err := client.GetContractEvents(ctx, request)
for {
    event, err := stream.Recv()
    // Process event
}
```

### 2. Filtering
```go
request := &pb.GetContractEventsRequest{
    StartLedger: 1000,
    ContractIds: []string{"CABC...", "CDEF..."},
    EventTypes: []string{"transfer", "mint"},
    IncludeFailed: false,
}
```

### 3. Graceful Shutdown
```go
ctx, cancel := context.WithCancel(context.Background())
defer cancel()

sigChan := make(chan os.Signal, 1)
signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
go func() {
    <-sigChan
    cancel()
}()
```

### 4. Event Processing
```go
// Access decoded JSON
var jsonData interface{}
json.Unmarshal([]byte(event.Data.Json), &jsonData)

// Or access raw XDR
xdrBytes := event.Data.XdrBase64
```

## Use Cases

### Monitor Specific Contracts
```bash
# Track a DEX contract
FILTER_CONTRACT_IDS="CDEX123..." \
FILTER_EVENT_TYPES="swap" \
./run.sh 1000 0
```

### Token Transfer Tracking
```bash
# Monitor all token transfers
FILTER_EVENT_TYPES="transfer,mint,burn" \
./run.sh 1000 0
```

### Failed Transaction Analysis
```bash
# Include failed transactions to analyze errors
INCLUDE_FAILED=true \
./run.sh 1000 0
```

## Performance

- **Memory:** ~15MB binary size, minimal runtime overhead
- **Throughput:** Can handle 100+ events/sec
- **Latency:** < 10ms per event processing

## Extending the Consumer

To build a production consumer:

1. **Database Integration** - Save to PostgreSQL/MongoDB
```go
for {
    event, err := stream.Recv()
    // Save to database
    db.Insert(event)
}
```

2. **Message Queue** - Forward to Kafka/RabbitMQ
```go
for {
    event, err := stream.Recv()
    // Publish to queue
    producer.Send(event)
}
```

3. **Webhooks** - Trigger HTTP callbacks
```go
for {
    event, err := stream.Recv()
    // POST to webhook
    http.Post(webhookURL, event)
}
```

4. **Analytics** - Feed into analytics pipeline
```go
for {
    event, err := stream.Recv()
    // Send to analytics
    analytics.Track(event)
}
```

## Troubleshooting

### Consumer can't connect

```bash
# Check if contract-events-processor is running
curl http://localhost:8089/health

# Check if stellar-live-source is running
curl http://localhost:8088/health

# Verify services with docker-compose
docker-compose ps
```

### No events received

```bash
# Check if filters are too restrictive
# Try without filters first
./run.sh 1000 1100

# Check service stats
curl http://localhost:8089/health | jq .stats
```

### Stream stops unexpectedly

```bash
# Check service logs
docker-compose logs contract-events-processor

# Increase end_ledger or use 0 for continuous
./run.sh 1000 0
```

## Next Steps

- See [main README](../../README.md) for Python and Node.js examples
- Check [quick-start-contract-events.md](../../shape-up/quick-start-contract-events.md) for more patterns
- View [technical-design-contract-events.md](../../shape-up/technical-design-contract-events.md) for architecture details
