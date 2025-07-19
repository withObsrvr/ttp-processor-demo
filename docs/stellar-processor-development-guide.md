# Stellar Processor Development Guide

A comprehensive guide for building custom Stellar processors based on official patterns and the working TTP processor implementation.

## Table of Contents

1. [Overview](#overview)
2. [Architecture Patterns](#architecture-patterns)
3. [Protocol Buffer Conventions](#protocol-buffer-conventions)
4. [Building Your First Processor](#building-your-first-processor)
5. [Official Processor References](#official-processor-references)
6. [Protocol 23 Compatibility](#protocol-23-compatibility)
7. [Best Practices](#best-practices)
8. [Testing and Deployment](#testing-and-deployment)
9. [Examples and Templates](#examples-and-templates)

## Overview

Stellar processors are microservices that transform raw ledger data into domain-specific events. They follow a consistent architecture pattern:

```
Raw Ledger Source → Processor → Consumer Applications
     (XDR data)      (Events)     (Business Logic)
```

### Key Components

- **Input**: `xdr.LedgerCloseMeta` from ledger sources
- **Processing**: Official Stellar processor libraries
- **Output**: Protocol buffer-defined events via gRPC streams
- **Pattern**: Producer-consumer with gRPC streaming

## Architecture Patterns

### 1. Data Flow Architecture

Based on the working TTP processor implementation:

```go
// Core data flow pattern
func (s *EventServer) GetTTPEvents(req *eventservice.GetEventsRequest, stream eventservice.EventService_GetTTPEventsServer) error {
    // 1. Connect to raw ledger source
    sourceReq := &rawledger.StreamLedgersRequest{StartLedger: req.StartLedger}
    rawLedgerStream, err := s.rawLedgerClient.StreamRawLedgers(sourceCtx, sourceReq)
    
    // 2. Process each ledger
    for {
        rawLedgerMsg, err := rawLedgerStream.Recv()
        
        // 3. Unmarshal XDR data
        var lcm xdr.LedgerCloseMeta
        _, err = xdr.Unmarshal(bytes.NewReader(rawLedgerMsg.LedgerCloseMetaXdr), &lcm)
        
        // 4. Extract domain events using official processor
        events, err := s.processor.EventsFromLedger(lcm)
        
        // 5. Stream events to consumer
        for _, event := range events {
            stream.Send(event)
        }
    }
}
```

### 2. Service Structure Pattern

Every processor follows this structure:

```
processor-service/
├── go/
│   ├── main.go              # Service entry point
│   ├── server/
│   │   └── server.go        # gRPC service implementation
│   ├── gen/                 # Generated protobuf code
│   └── go.mod
├── protos/
│   ├── your_service/        # Your service definitions
│   └── ingest/              # Stellar data type definitions
├── Makefile                 # Build automation
└── api/
    └── openapi.yaml         # API documentation
```

### 3. Dependency Pattern

Processors depend on three key Stellar libraries:

```go
import (
    "github.com/stellar/go/processors/your_processor"  // Official processor logic
    "github.com/stellar/go/xdr"                        // XDR data structures
    "github.com/stellar/go/support/errors"             // Error handling
)
```

## Protocol Buffer Conventions

### 1. Service Definition Pattern

Based on `event_service.proto`:

```protobuf
syntax = "proto3";

package your_domain.v1;

option go_package = "github.com/your-org/your-processor/gen/your_service";

import "google/protobuf/timestamp.proto";
import "ingest/asset/asset.proto";

// Service definition follows verb-based naming
service YourEventService {
    rpc GetYourEvents(GetEventsRequest) returns (stream YourEvent);
}

// Request follows standard pattern
message GetEventsRequest {
    uint32 start_ledger = 1;
    uint32 end_ledger = 2;    // 0 for unbounded stream
}

// Event structure includes common fields
message YourEvent {
    uint32 ledger_sequence = 1;
    google.protobuf.Timestamp ledger_close_time = 2;
    string transaction_hash = 3;
    uint32 operation_index = 4;
    
    // Domain-specific fields
    stellar.ingest.Asset asset = 5;
    string amount = 6;
    string from_account = 7;
    string to_account = 8;
}
```

### 2. Import Conventions

Standard imports for Stellar processors:

```protobuf
// Time handling
import "google/protobuf/timestamp.proto";

// Stellar data types
import "ingest/asset/asset.proto";

// For more complex processors, also import:
import "ingest/processors/other_processor/events.proto";
```

### 3. Go Package Naming

Follow Protocol 23 package structure:

```protobuf
// OLD (deprecated)
option go_package = "github.com/stellar/go/ingest/processors/token_transfer";

// NEW (Protocol 23+)
option go_package = "github.com/stellar/go/processors/token_transfer";
```

## Building Your First Processor

### Step 1: Define Your Domain Events

Create `protos/your_service/your_service.proto`:

```protobuf
syntax = "proto3";

package your_domain.v1;

option go_package = "github.com/your-org/your-processor/gen/your_service";

import "google/protobuf/timestamp.proto";
import "ingest/asset/asset.proto";

service YourEventService {
    rpc GetYourEvents(GetEventsRequest) returns (stream YourEvent);
}

message GetEventsRequest {
    uint32 start_ledger = 1;
    uint32 end_ledger = 2;
}

message YourEvent {
    uint32 ledger_sequence = 1;
    google.protobuf.Timestamp ledger_close_time = 2;
    string transaction_hash = 3;
    uint32 operation_index = 4;
    
    // Your domain-specific fields here
    string your_custom_field = 5;
}
```

### Step 2: Create Processor Logic

If using an official Stellar processor:

```go
package server

import (
    "github.com/stellar/go/processors/your_processor" // Use official processor
    "github.com/stellar/go/xdr"
)

type EventServer struct {
    processor *your_processor.EventsProcessor
    // ... other fields
}

func NewEventServer(passphrase string) *EventServer {
    processor := your_processor.NewEventsProcessor(passphrase)
    return &EventServer{processor: processor}
}

func (s *EventServer) processLedger(lcm xdr.LedgerCloseMeta) error {
    events, err := s.processor.EventsFromLedger(lcm)
    if err != nil {
        return err
    }
    
    // Convert to your protobuf events and stream
    for _, event := range events {
        protoEvent := convertToProto(event)
        s.stream.Send(protoEvent)
    }
    return nil
}
```

### Step 3: Implement Custom Processing

For custom logic not covered by official processors:

```go
func (s *EventServer) extractCustomEvents(lcm xdr.LedgerCloseMeta) ([]*YourEvent, error) {
    var events []*YourEvent
    
    // Access transaction set
    switch lcm.V {
    case 0:
        return s.processV0(lcm.V0)
    case 1:
        return s.processV1(lcm.V1)
    case 2:
        return s.processV2(lcm.V2) // Protocol 23+
    }
    
    return events, nil
}

func (s *EventServer) processV1(v1 *xdr.LedgerCloseMetaV1) ([]*YourEvent, error) {
    var events []*YourEvent
    
    // Iterate through transactions
    for i, txResult := range v1.TxProcessing {
        tx := v1.TxSet.V1TxSet.Phases[0].V0Components[0].TxsMaybeDiscountedFee.Txs[i]
        
        // Process each operation
        for opIndex, op := range tx.Operations {
            if isYourTargetOperation(op) {
                event := &YourEvent{
                    LedgerSequence: uint32(v1.LedgerHeader.Header.LedgerSeq),
                    TransactionHash: txResult.TxResult.TransactionHash.HexString(),
                    OperationIndex: uint32(opIndex),
                    // Extract your custom fields
                }
                events = append(events, event)
            }
        }
    }
    
    return events, nil
}
```

### Step 4: Service Implementation

```go
func (s *EventServer) GetYourEvents(req *yourservice.GetEventsRequest, stream yourservice.YourEventService_GetYourEventsServer) error {
    // Follow the TTP processor pattern from server.go:154-324
    ctx := stream.Context()
    
    // Connect to raw ledger source
    sourceReq := &rawledger.StreamLedgersRequest{StartLedger: req.StartLedger}
    rawLedgerStream, err := s.rawLedgerClient.StreamRawLedgers(ctx, sourceReq)
    
    for {
        // Get raw ledger
        rawLedgerMsg, err := rawLedgerStream.Recv()
        
        // Unmarshal XDR
        var lcm xdr.LedgerCloseMeta
        _, err = xdr.Unmarshal(bytes.NewReader(rawLedgerMsg.LedgerCloseMetaXdr), &lcm)
        
        // Process with your logic
        events, err := s.extractCustomEvents(lcm)
        
        // Stream to consumer
        for _, event := range events {
            stream.Send(event)
        }
    }
}
```

## Official Processor References

### Available Processors

From https://github.com/stellar/go/tree/master/processors:

1. **Token Transfer Processor** (`processors/token_transfer`)
   - Tracks asset transfers between accounts
   - Handles all payment operations
   - Used in the working TTP processor example

2. **Offer Processor** (if available)
   - Tracks DEX offer creation/updates
   - Manages orderbook changes

3. **Liquidity Pool Processor** (if available)
   - Tracks AMM pool operations
   - Handles pool creation and swaps

### Using Official Processors

```go
import "github.com/stellar/go/processors/token_transfer"

// Initialize with network passphrase
processor := token_transfer.NewEventsProcessor("Test SDF Network ; September 2015")

// Process ledger to get events
events, err := processor.EventsFromLedger(ledgerCloseMeta)
```

### Protocol 23 Processor Updates

With Protocol 23, processors moved locations:

```go
// OLD import path (deprecated)
import "github.com/stellar/go/ingest/processors/token_transfer"

// NEW import path (Protocol 23+)
import "github.com/stellar/go/processors/token_transfer"
```

## Protocol 23 Compatibility

### LedgerCloseMeta Format Changes

Protocol 23 introduced V2 format:

```go
func (s *EventServer) handleLedgerCloseMeta(lcm xdr.LedgerCloseMeta) error {
    switch lcm.V {
    case 0:
        return s.processV0(lcm.V0)
    case 1:
        return s.processV1(lcm.V1)
    case 2:
        // Protocol 23+ V2 format
        return s.processV2(lcm.V2)
    default:
        return fmt.Errorf("unsupported LedgerCloseMeta version: %d", lcm.V)
    }
}
```

### Dual BucketList Support (CAP-62)

Protocol 23 processors should validate dual bucket lists:

```go
func (s *EventServer) validateProtocol23Features(lcm xdr.LedgerCloseMeta) error {
    if lcm.LedgerSequence() >= PROTOCOL_23_ACTIVATION_LEDGER {
        // Validate dual bucket list hash
        if lcm.V == 1 && lcm.V1 != nil {
            return s.validateDualBucketList(lcm.V1)
        }
    }
    return nil
}
```

### Update Dependencies

Use Protocol 23 branches:

```bash
go get github.com/stellar/go@protocol-23
```

## Best Practices

### 1. Error Handling

Follow robust error handling patterns:

```go
func (s *EventServer) processWithRetry(lcm xdr.LedgerCloseMeta) error {
    const maxRetries = 3
    
    for attempt := 0; attempt < maxRetries; attempt++ {
        events, err := s.processor.EventsFromLedger(lcm)
        if err == nil {
            return s.streamEvents(events)
        }
        
        s.logger.Warn("processing failed, retrying",
            zap.Int("attempt", attempt+1),
            zap.Error(err))
            
        if attempt < maxRetries-1 {
            time.Sleep(time.Duration(attempt+1) * time.Second)
        }
    }
    
    return fmt.Errorf("failed after %d attempts", maxRetries)
}
```

### 2. Metrics and Monitoring

Include comprehensive metrics:

```go
type ProcessorMetrics struct {
    SuccessCount        int64
    ErrorCount          int64
    TotalEventsEmitted  int64
    ProcessingLatency   time.Duration
    LastProcessedLedger uint32
}

func (s *EventServer) recordMetrics(ledgerSeq uint32, eventCount int, duration time.Duration) {
    s.metrics.SuccessCount++
    s.metrics.TotalEventsEmitted += int64(eventCount)
    s.metrics.ProcessingLatency = duration
    s.metrics.LastProcessedLedger = ledgerSeq
}
```

### 3. Context Handling

Proper context cancellation:

```go
func (s *EventServer) GetEvents(req *GetEventsRequest, stream EventService_GetEventsServer) error {
    ctx := stream.Context()
    sourceCtx, cancel := context.WithCancel(ctx)
    defer cancel()
    
    // Check for consumer cancellation
    select {
    case <-ctx.Done():
        cancel() // Cancel upstream
        return status.FromContextError(ctx.Err()).Err()
    default:
        // Continue processing
    }
}
```

### 4. Configuration Management

Environment-based configuration:

```go
type ProcessorConfig struct {
    NetworkPassphrase string
    SourceEndpoint    string
    LogLevel          string
    MetricsPort       int
}

func LoadConfig() *ProcessorConfig {
    return &ProcessorConfig{
        NetworkPassphrase: getEnvOrDefault("NETWORK_PASSPHRASE", "Test SDF Network ; September 2015"),
        SourceEndpoint:    getEnvOrDefault("SOURCE_ENDPOINT", "localhost:8080"),
        LogLevel:         getEnvOrDefault("LOG_LEVEL", "info"),
        MetricsPort:      getEnvAsInt("METRICS_PORT", 8088),
    }
}
```

## Testing and Deployment

### 1. Unit Testing

Test processor logic with known ledgers:

```go
func TestYourProcessor(t *testing.T) {
    processor := NewYourProcessor("Test SDF Network ; September 2015")
    
    // Load test ledger data
    lcm := loadTestLedgerCloseMeta(t, "testdata/ledger_123456.xdr")
    
    events, err := processor.ProcessLedger(lcm)
    require.NoError(t, err)
    
    assert.Len(t, events, expectedEventCount)
    assert.Equal(t, uint32(123456), events[0].LedgerSequence)
}
```

### 2. Integration Testing

Test with live ledger source:

```go
func TestIntegration(t *testing.T) {
    // Skip in short mode
    if testing.Short() {
        t.Skip("skipping integration test")
    }
    
    server := NewEventServer("Test SDF Network ; September 2015", "localhost:8080")
    
    // Test bounded stream
    req := &GetEventsRequest{
        StartLedger: 100000,
        EndLedger:   100002,
    }
    
    events := collectEvents(t, server, req)
    assert.NotEmpty(t, events)
}
```

### 3. Performance Testing

Benchmark processing performance:

```go
func BenchmarkProcessLedger(b *testing.B) {
    processor := NewYourProcessor("Test SDF Network ; September 2015")
    lcm := loadTestLedgerCloseMeta(b, "testdata/busy_ledger.xdr")
    
    b.ResetTimer()
    for i := 0; i < b.N; i++ {
        _, err := processor.ProcessLedger(lcm)
        if err != nil {
            b.Fatal(err)
        }
    }
}
```

### 4. Docker Deployment

Create production-ready Docker images:

```dockerfile
FROM golang:1.21-alpine AS builder

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN go build -o processor ./cmd/processor

FROM alpine:latest
RUN apk add --no-cache ca-certificates
WORKDIR /root/

COPY --from=builder /app/processor .

EXPOSE 8080 8088
CMD ["./processor"]
```

## Examples and Templates

### 1. Makefile Template

```makefile
.PHONY: build gen-proto run test clean

build:
	go build -o $(SERVICE_NAME) ./cmd/$(SERVICE_NAME)

gen-proto:
	protoc --go_out=. --go_opt=paths=source_relative \
		--go-grpc_out=. --go-grpc_opt=paths=source_relative \
		protos/*/*.proto

run: build
	./$(SERVICE_NAME)

test:
	go test -v ./...

test-integration:
	go test -v -tags=integration ./...

clean:
	rm -f $(SERVICE_NAME)
	rm -rf gen/

docker-build:
	docker build -t $(SERVICE_NAME):latest .

docker-run:
	docker run -p 8080:8080 -p 8088:8088 $(SERVICE_NAME):latest
```

### 2. Complete Processor Template

See the working TTP processor implementation in:
- `/ttp-processor/go/server/server.go` - Complete service implementation
- `/ttp-processor/protos/` - Proto definitions
- `/ttp-processor/go/main.go` - Service entry point

### 3. Proto File Template

```protobuf
syntax = "proto3";

package your_domain.v1;

option go_package = "github.com/your-org/your-processor/gen/your_service";

import "google/protobuf/timestamp.proto";
import "ingest/asset/asset.proto";

service YourEventService {
    rpc GetYourEvents(GetEventsRequest) returns (stream YourEvent);
}

message GetEventsRequest {
    uint32 start_ledger = 1;
    uint32 end_ledger = 2;
}

message YourEvent {
    uint32 ledger_sequence = 1;
    google.protobuf.Timestamp ledger_close_time = 2;
    string transaction_hash = 3;
    uint32 operation_index = 4;
    
    // Your domain-specific fields
    YourCustomData data = 5;
}

message YourCustomData {
    string field1 = 1;
    int64 field2 = 2;
    stellar.ingest.Asset asset = 3;
}
```

## Next Steps

1. **Study the Official Processors**: Review https://github.com/stellar/go/tree/master/processors
2. **Examine Proto Definitions**: Study https://github.com/stellar/go/tree/protocol-23/protos
3. **Use the Working Example**: Build on the TTP processor implementation
4. **Test with Protocol 23**: Ensure compatibility with latest Stellar protocol

## Resources

- [Stellar Go SDK](https://github.com/stellar/go)
- [Official Processors](https://github.com/stellar/go/tree/master/processors)
- [Protocol 23 Protos](https://github.com/stellar/go/tree/protocol-23/protos)
- [Working TTP Processor](../ttp-processor/)
- [Stellar Developer Documentation](https://developers.stellar.org)

This guide provides the foundation for building robust, production-ready Stellar processors following official patterns and best practices.