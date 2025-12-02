# Contract Events Consumer (Go Example)

A simple Go consumer that connects to the contract-events-processor and prints events to stdout.

## Quick Start

```bash
# Build
make build

# Run - stream ledgers 1000-2000
./contract-events-consumer 1000 2000

# Run - stream from 1000 continuously
./contract-events-consumer 1000 0
```

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `CONTRACT_EVENTS_SERVICE_ADDRESS` | `localhost:50053` | Contract events processor address |
| `FILTER_CONTRACT_IDS` | - | Comma-separated contract IDs to filter |
| `FILTER_EVENT_TYPES` | - | Comma-separated event types to filter |
| `INCLUDE_FAILED` | `false` | Include events from failed transactions |
| `INCLUDE_DIAGNOSTICS` | `false` | Include diagnostic events |

## Usage Examples

### Basic Usage

```bash
# Stream ledgers 1000-1100
./contract-events-consumer 1000 1100
```

### Filter by Contract

```bash
# Only show events from specific contracts
FILTER_CONTRACT_IDS="CABC123...,CDEF456..." \
./contract-events-consumer 1000 0
```

### Filter by Event Type

```bash
# Only show transfer and mint events
FILTER_EVENT_TYPES="transfer,mint,burn" \
./contract-events-consumer 1000 0
```

### Include Failed Transactions

```bash
# Include events from failed transactions
INCLUDE_FAILED=true \
./contract-events-consumer 1000 0
```

### Combined Filters

```bash
# Combine multiple filters
FILTER_CONTRACT_IDS="CABC123..." \
FILTER_EVENT_TYPES="transfer,swap" \
INCLUDE_FAILED=false \
./contract-events-consumer 1000 0
```

## Output Format

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

Data:
  {
    "amount": "1000000000",
    "asset": {
      "code": "USDC",
      "issuer": "GXYZ..."
    }
  }

--- Event #2 ---
...

================================================================================
Stream Summary:
  Total Events: 42
  Ledgers Processed: 10
  Ledger Range: 1000 - 1100
================================================================================
```

## With Docker Compose

If you're running the full pipeline with docker-compose:

```bash
# Start the pipeline
cd ../..
docker-compose up -d

# Wait for services to be healthy
sleep 5

# Run consumer
cd examples/go-consumer
CONTRACT_EVENTS_SERVICE_ADDRESS="localhost:50053" \
./contract-events-consumer 1000 0
```

## Graceful Shutdown

Press `Ctrl+C` to stop streaming gracefully:

```
^C
Received shutdown signal, stopping...

================================================================================
Stream Summary:
  Total Events: 156
  Ledgers Processed: 50
  Ledger Range: 1000 - 1050
================================================================================
```

## Building from Source

```bash
# Install dependencies
go mod tidy

# Build
go build -o contract-events-consumer main.go

# Run
./contract-events-consumer 1000 2000
```

## Integration with Other Services

This consumer can be used as a template for building production consumers that:

- Save events to PostgreSQL
- Forward to a message queue (Kafka, RabbitMQ)
- Trigger webhooks
- Feed analytics pipelines
- Monitor specific contracts

See the main README for more consumer examples in other languages.
