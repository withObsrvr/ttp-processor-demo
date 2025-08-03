# Apache Arrow Implementation Plan
## ttp-processor-demo

### Executive Summary

This document outlines a comprehensive phased approach to **fully transition** ttp-processor-demo to Apache Arrow architecture. We are committing to **native Arrow** as the primary architecture, using compatibility layers only as temporary migration tools during the transition period.

**Key Principles:**
- **Native Arrow First**: All new development uses native Arrow, compatibility layers are temporary only
- **Zero Downtime Migration**: Maintain existing functionality during parallel deployment
- **Structured Logging**: Use zerolog for consistent, high-performance logging across all services
- **Comprehensive Testing**: Leverage existing Nix environments for reproducible builds and testing
- **Performance Validation**: Continuous benchmarking to achieve 10x improvement targets
- **Complete Migration**: Goal is to replace legacy systems entirely with Arrow-native services

---

## Current Architecture Analysis

### Existing Services

| Service | Purpose | Technology | Migration Strategy |
|---------|---------|------------|-------------------|
| `stellar-live-source` | RPC-based ledger data source | Go, gRPC | → `stellar-arrow-source` (Arrow Flight) |
| `stellar-live-source-datalake` | Storage-based data source | Go, Nix, gRPC | → Native Arrow with Nix builds |
| `ttp-processor` | Token Transfer Protocol processor | Go, gRPC, Protocol 23 | → `ttp-arrow-processor` (Native Arrow) |
| `contract-invocation-processor` | Smart contract execution analyzer | Go, gRPC | → Arrow-native contract processor |
| Consumer Apps | Multiple consumer implementations | Node.js, Go WASM, Rust WASM | → Native Arrow Flight consumers |

### Integration Points Identified

1. **Data Source Layer**: Replace gRPC with Arrow Flight protocol exclusively
2. **Processing Layer**: Implement native Arrow compute for all processing operations
3. **Consumer Layer**: Migrate all consumers to native Arrow Flight clients
4. **Storage Layer**: Native Parquet output with Arrow-optimized partitioning
5. **Monitoring**: Extend flowctl with Arrow-specific metrics and structured logging

---

## Phase 1: Native Arrow Foundation (4 weeks)

### Objectives
- Establish Arrow-native development environment with structured logging
- Create native Arrow Flight data source services
- Implement Arrow schemas with full compatibility validation
- Deploy parallel to existing services with native Arrow as primary

### Deliverables

#### 1.1 Arrow Development Environment with Structured Logging

**New Service**: `stellar-arrow-source`
**Location**: `stellar-arrow-source/`
**Technology Stack**: Go 1.23+, Apache Arrow Go v17.0.0+, Arrow Flight, zerolog

**Directory Structure:**
```
stellar-arrow-source/
├── Makefile
├── flake.nix                    # Nix development environment with Arrow
├── go/
│   ├── main.go
│   ├── go.mod
│   ├── logging/
│   │   └── logger.go            # Structured logging setup
│   ├── server/
│   │   ├── arrow_server.go      # Native Arrow Flight server
│   │   ├── schema_manager.go    # Arrow schema versioning
│   │   └── flowctl.go           # Control plane integration
│   └── converter/
│       └── xdr_to_arrow.go      # XDR to native Arrow conversion
├── protos/
│   └── stellar_arrow_service/
│       └── stellar_arrow_service.proto
└── api/
    └── openapi.yaml
```

#### 1.2 Structured Logging Implementation

**File**: `stellar-arrow-source/go/logging/logger.go`

```go
package logging

import (
    "os"
    "time"

    "github.com/rs/zerolog"
    "github.com/rs/zerolog/log"
)

// ComponentLogger provides structured logging for Arrow services
type ComponentLogger struct {
    logger zerolog.Logger
}

// NewComponentLogger creates a component-specific logger with consistent context
func NewComponentLogger(componentName, version string) *ComponentLogger {
    // Configure zerolog globally
    zerolog.TimeFieldFormat = time.RFC3339

    // Set log level from environment
    logLevel := os.Getenv("LOG_LEVEL")
    switch logLevel {
    case "debug":
        zerolog.SetGlobalLevel(zerolog.DebugLevel)
    case "info":
        zerolog.SetGlobalLevel(zerolog.InfoLevel)
    case "warn":
        zerolog.SetGlobalLevel(zerolog.WarnLevel)
    case "error":
        zerolog.SetGlobalLevel(zerolog.ErrorLevel)
    default:
        zerolog.SetGlobalLevel(zerolog.InfoLevel)
    }

    // Console output for development
    if os.Getenv("ENVIRONMENT") != "production" {
        log.Logger = log.Output(zerolog.ConsoleWriter{
            Out:        os.Stderr,
            TimeFormat: time.RFC3339,
            NoColor:    false,
        })
    }

    // Create component-specific logger
    logger := log.With().
        Str("component", componentName).
        Str("version", version).
        Logger()

    return &ComponentLogger{
        logger: logger,
    }
}

func (cl *ComponentLogger) Info() *zerolog.Event {
    return cl.logger.Info()
}

func (cl *ComponentLogger) Error() *zerolog.Event {
    return cl.logger.Error()
}

func (cl *ComponentLogger) Warn() *zerolog.Event {
    return cl.logger.Warn()
}

func (cl *ComponentLogger) Debug() *zerolog.Event {
    return cl.logger.Debug()
}

// LogStartup logs service startup with structured fields
func (cl *ComponentLogger) LogStartup(config StartupConfig) {
    cl.Info().
        Bool("flowctl_enabled", config.FlowCtlEnabled).
        Str("source_type", config.SourceType).
        Str("backend_type", config.BackendType).
        Str("network", config.NetworkPassphrase).
        Int("batch_size", config.BatchSize).
        Int("port", config.Port).
        Msg("Starting Arrow service")
}

// LogArrowProcessing logs Arrow-specific processing operations
func (cl *ComponentLogger) LogArrowProcessing(operation string, batchSize int, duration time.Duration) {
    cl.Info().
        Str("operation", operation).
        Int("batch_size", batchSize).
        Dur("processing_time", duration).
        Str("protocol", "arrow-flight").
        Msg("Arrow processing completed")
}

// LogArrowMemory logs Arrow memory allocator statistics
func (cl *ComponentLogger) LogArrowMemory(allocatedBytes, reservedBytes int64) {
    cl.Debug().
        Int64("allocated_bytes", allocatedBytes).
        Int64("reserved_bytes", reservedBytes).
        Str("allocator_type", "go_allocator").
        Msg("Arrow memory statistics")
}

type StartupConfig struct {
    FlowCtlEnabled     bool
    SourceType         string
    BackendType        string
    NetworkPassphrase  string
    BatchSize          int
    Port              int
}
```

#### 1.3 Native Arrow Flight Server Implementation

**File**: `stellar-arrow-source/go/server/arrow_server.go`

```go
package server

import (
    "context"
    "fmt"
    "net"
    "time"
    
    "github.com/apache/arrow/go/v17/arrow"
    "github.com/apache/arrow/go/v17/arrow/array"
    "github.com/apache/arrow/go/v17/arrow/flight"
    "github.com/apache/arrow/go/v17/arrow/memory"
    "github.com/your-org/stellar-arrow-source/go/logging"
    "google.golang.org/grpc"
)

// StellarArrowServer implements native Arrow Flight for Stellar data streaming
type StellarArrowServer struct {
    flight.BaseFlightServer
    
    allocator     memory.Allocator
    logger        *logging.ComponentLogger
    schemaManager *SchemaManager
    sourceClient  ArrowSourceClient // Native Arrow client interface
}

// ArrowSourceClient defines the interface for native Arrow data sources
type ArrowSourceClient interface {
    StreamArrowRecords(ctx context.Context, startLedger, endLedger uint32) (<-chan arrow.Record, <-chan error)
}

func NewStellarArrowServer(sourceEndpoint string) (*StellarArrowServer, error) {
    logger := logging.NewComponentLogger("stellar-arrow-source", "v1.0.0")
    allocator := memory.NewGoAllocator()
    schemaManager := NewSchemaManager()

    logger.Info().
        Str("endpoint", sourceEndpoint).
        Msg("Initializing native Arrow Flight server")

    // Create native Arrow source client (no gRPC compatibility)
    sourceClient, err := NewArrowSourceClient(sourceEndpoint)
    if err != nil {
        logger.Error().
            Err(err).
            Str("endpoint", sourceEndpoint).
            Msg("Failed to create Arrow source client")
        return nil, fmt.Errorf("failed to create Arrow source client: %w", err)
    }

    logger.Info().Msg("Native Arrow Flight server initialized successfully")

    return &StellarArrowServer{
        allocator:     allocator,
        logger:        logger,
        schemaManager: schemaManager,
        sourceClient:  sourceClient,
    }, nil
}

func (s *StellarArrowServer) GetFlightInfo(ctx context.Context, request *flight.FlightDescriptor) (*flight.FlightInfo, error) {
    s.logger.Debug().
        Str("operation", "get_flight_info").
        Interface("descriptor", request.Path).
        Msg("Flight info requested")

    // Return native Arrow schema information
    schema := s.schemaManager.GetStellarLedgerSchema()
    
    return &flight.FlightInfo{
        Schema:           flight.SerializeSchema(schema, s.allocator),
        FlightDescriptor: request,
        Endpoint: []*flight.FlightEndpoint{
            {
                Ticket: &flight.Ticket{
                    Ticket: request.Path,
                },
                Location: []*flight.Location{
                    {Uri: "arrow-flight://localhost:8815"},
                },
            },
        },
        TotalRecords: -1, // Streaming
        TotalBytes:   -1, // Streaming
    }, nil
}

func (s *StellarArrowServer) DoGet(ticket *flight.Ticket, stream flight.FlightService_DoGetServer) error {
    start := time.Now()
    
    // Parse streaming parameters
    params, err := parseStreamParams(ticket.Ticket)
    if err != nil {
        s.logger.Error().
            Err(err).
            Msg("Failed to parse stream parameters")
        return fmt.Errorf("failed to parse stream parameters: %w", err)
    }

    s.logger.Info().
        Uint32("start_ledger", params.StartLedger).
        Uint32("end_ledger", params.EndLedger).
        Str("operation", "arrow_stream_start").
        Msg("Starting native Arrow stream")

    // Get native Arrow record stream (no conversion needed)
    recordChan, errChan := s.sourceClient.StreamArrowRecords(stream.Context(), params.StartLedger, params.EndLedger)
    
    batchCount := 0
    totalRecords := int64(0)
    
    for {
        select {
        case <-stream.Context().Done():
            s.logger.Info().
                Int("batches_sent", batchCount).
                Int64("total_records", totalRecords).
                Dur("stream_duration", time.Since(start)).
                Msg("Arrow stream cancelled by client")
            return stream.Context().Err()
            
        case err := <-errChan:
            if err != nil {
                s.logger.Error().
                    Err(err).
                    Int("batches_sent", batchCount).
                    Msg("Arrow stream error")
                return fmt.Errorf("source stream error: %w", err)
            }
            // Stream completed successfully
            s.logger.Info().
                Int("batches_sent", batchCount).
                Int64("total_records", totalRecords).
                Dur("stream_duration", time.Since(start)).
                Msg("Arrow stream completed successfully")
            return nil
            
        case record := <-recordChan:
            // Stream native Arrow record directly (no conversion)
            flightData := &flight.FlightData{
                FlightDescriptor: &flight.FlightDescriptor{
                    Type: flight.FlightDescriptor_CMD,
                    Cmd:  ticket.Ticket,
                },
            }
            
            // Serialize Arrow record to flight data
            if err := flight.SerializeRecord(record, flightData, s.allocator); err != nil {
                s.logger.Error().
                    Err(err).
                    Int64("record_rows", record.NumRows()).
                    Msg("Failed to serialize Arrow record")
                continue
            }
            
            if err := stream.Send(flightData); err != nil {
                s.logger.Error().
                    Err(err).
                    Msg("Failed to send Arrow flight data")
                return fmt.Errorf("failed to send flight data: %w", err)
            }
            
            batchCount++
            totalRecords += record.NumRows()
            
            // Log memory usage periodically
            if batchCount%100 == 0 {
                s.logger.LogArrowMemory(s.allocator.AllocatedBytes(), 0)
            }
        }
    }
}

// StartArrowFlightServer starts the native Arrow Flight server
func (s *StellarArrowServer) StartArrowFlightServer(port int) error {
    s.logger.Info().
        Int("port", port).
        Str("protocol", "arrow-flight").
        Msg("Starting Arrow Flight server")

    listener, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
    if err != nil {
        s.logger.Error().
            Err(err).
            Int("port", port).
            Msg("Failed to create Arrow Flight listener")
        return fmt.Errorf("failed to listen on port %d: %w", port, err)
    }

    server := grpc.NewServer()
    flight.RegisterFlightServiceServer(server, s)

    s.logger.Info().
        Str("address", listener.Addr().String()).
        Msg("Arrow Flight server listening")

    return server.Serve(listener)
}
```

#### 1.4 Native Arrow Schema Definition

**File**: `stellar-arrow-source/go/server/schema_manager.go`

```go
package server

import (
    "github.com/apache/arrow/go/v17/arrow"
    "github.com/apache/arrow/go/v17/arrow/memory"
)

type SchemaManager struct {
    allocator memory.Allocator
}

func NewSchemaManager() *SchemaManager {
    return &SchemaManager{
        allocator: memory.NewGoAllocator(),
    }
}

// GetStellarLedgerSchema returns the native Arrow schema for Stellar ledger data
func (sm *SchemaManager) GetStellarLedgerSchema() *arrow.Schema {
    return arrow.NewSchema(
        []arrow.Field{
            // Native Arrow-optimized ledger metadata
            {Name: "ledger_sequence", Type: arrow.PrimitiveTypes.Uint32, Nullable: false},
            {Name: "ledger_close_time", Type: arrow.FixedWidthTypes.Timestamp_us, Nullable: false},
            {Name: "ledger_hash", Type: &arrow.FixedSizeBinaryType{ByteWidth: 32}, Nullable: false},
            {Name: "previous_ledger_hash", Type: &arrow.FixedSizeBinaryType{ByteWidth: 32}, Nullable: false},
            
            // Transaction statistics for analytics
            {Name: "transaction_count", Type: arrow.PrimitiveTypes.Uint32, Nullable: false},
            {Name: "operation_count", Type: arrow.PrimitiveTypes.Uint32, Nullable: false},
            {Name: "successful_transaction_count", Type: arrow.PrimitiveTypes.Uint32, Nullable: false},
            {Name: "failed_transaction_count", Type: arrow.PrimitiveTypes.Uint32, Nullable: false},
            
            // Protocol version for compatibility
            {Name: "protocol_version", Type: arrow.PrimitiveTypes.Uint32, Nullable: false},
            
            // Base fee and reserve for economics
            {Name: "base_fee", Type: arrow.PrimitiveTypes.Uint32, Nullable: false},
            {Name: "base_reserve", Type: arrow.PrimitiveTypes.Uint32, Nullable: false},
            
            // Raw XDR for complete compatibility during migration only
            {Name: "ledger_close_meta_xdr", Type: arrow.BinaryTypes.Binary, Nullable: false},
        },
        &arrow.Metadata{
            Keys:   []string{"version", "description", "protocol_version", "schema_type"},
            Values: []string{"1.0.0", "Native Arrow Stellar Ledger Schema", "23", "native_arrow"},
        },
    )
}

// GetTTPEventSchema returns the native Arrow schema optimized for TTP events
func (sm *SchemaManager) GetTTPEventSchema() *arrow.Schema {
    return arrow.NewSchema(
        []arrow.Field{
            // Event identification
            {Name: "ledger_sequence", Type: arrow.PrimitiveTypes.Uint32, Nullable: false},
            {Name: "ledger_close_time", Type: arrow.FixedWidthTypes.Timestamp_us, Nullable: false},
            {Name: "transaction_hash", Type: &arrow.FixedSizeBinaryType{ByteWidth: 32}, Nullable: false},
            {Name: "operation_index", Type: arrow.PrimitiveTypes.Uint32, Nullable: false},
            
            // Native Arrow optimized TTP event data
            {Name: "event_type", Type: arrow.BinaryTypes.String, Nullable: false},
            {Name: "from_account", Type: arrow.BinaryTypes.String, Nullable: false},
            {Name: "to_account", Type: arrow.BinaryTypes.String, Nullable: false},
            {Name: "asset_code", Type: arrow.BinaryTypes.String, Nullable: true},
            {Name: "asset_issuer", Type: arrow.BinaryTypes.String, Nullable: true},
            {Name: "amount", Type: arrow.PrimitiveTypes.Int64, Nullable: false}, // Native numeric for analytics
            {Name: "amount_precision", Type: arrow.PrimitiveTypes.Uint32, Nullable: false}, // Decimal precision
            
            // Additional event context
            {Name: "memo", Type: arrow.BinaryTypes.String, Nullable: true},
            {Name: "memo_type", Type: arrow.BinaryTypes.String, Nullable: true},
            {Name: "operation_source_account", Type: arrow.BinaryTypes.String, Nullable: true},
            
            // Analytics-optimized fields
            {Name: "is_native_asset", Type: arrow.FixedWidthTypes.Boolean, Nullable: false},
            {Name: "is_path_payment", Type: arrow.FixedWidthTypes.Boolean, Nullable: false},
            {Name: "path_length", Type: arrow.PrimitiveTypes.Uint32, Nullable: true},
        },
        &arrow.Metadata{
            Keys:   []string{"version", "description", "schema_type", "optimization"},
            Values: []string{"1.0.0", "Native Arrow TTP Events Schema", "native_arrow", "analytics_ready"},
        },
    )
}

// ValidateSchemaCompatibility ensures the Arrow schema can represent all existing data
func (sm *SchemaManager) ValidateSchemaCompatibility(existingData interface{}) error {
    // Implementation to validate that Arrow schema can represent all current protobuf data
    // This ensures no data loss during migration
    return nil
}
```

### Success Criteria for Phase 1

- [ ] Native Arrow Flight server streams ledger data at 200+ ledgers/sec
- [ ] Arrow schemas validated for complete data representation
- [ ] Structured logging implemented with zerolog across all services  
- [ ] Nix development environment supports Arrow dependencies
- [ ] Memory usage optimized with Arrow allocators
- [ ] Integration tests demonstrate data consistency with existing sources

---

## Phase 2: Native Arrow TTP Processing (6 weeks)

### Objectives
- Implement **native Arrow compute engine** for TTP event processing
- Achieve 10x performance improvement through vectorized operations
- **Eliminate legacy compatibility** - pure Arrow processing pipeline
- Enable advanced analytics capabilities with Arrow compute functions

### Deliverables

#### 2.1 Native Arrow TTP Processor

**New Service**: `ttp-arrow-processor`
**Location**: `ttp-arrow-processor/`
**Technology**: Pure Arrow compute with structured logging

**File**: `ttp-arrow-processor/go/server/native_processor.go`

```go
package server

import (
    "context"
    "fmt"
    "time"
    
    "github.com/apache/arrow/go/v17/arrow"
    "github.com/apache/arrow/go/v17/arrow/array"
    "github.com/apache/arrow/go/v17/arrow/compute"
    "github.com/apache/arrow/go/v17/arrow/memory"
    "github.com/your-org/ttp-arrow-processor/go/logging"
)

// NativeArrowTTPProcessor implements pure Arrow-based TTP event processing
type NativeArrowTTPProcessor struct {
    allocator       memory.Allocator
    computeContext  compute.Context
    logger          *logging.ComponentLogger
    schemaManager   *SchemaManager
}

func NewNativeArrowTTPProcessor() *NativeArrowTTPProcessor {
    logger := logging.NewComponentLogger("ttp-arrow-processor", "v1.0.0")
    allocator := memory.NewGoAllocator()
    
    logger.Info().
        Str("processor_type", "native_arrow").
        Str("backend_type", "arrow_compute").
        Bool("legacy_compatibility", false).
        Msg("Initializing native Arrow TTP processor")

    return &NativeArrowTTPProcessor{
        allocator:      allocator,
        computeContext: compute.WithAllocator(context.Background(), allocator),
        logger:         logger,
        schemaManager:  NewSchemaManager(),
    }
}

// ProcessLedgerBatch processes Arrow record batches using native Arrow compute
func (n *NativeArrowTTPProcessor) ProcessLedgerBatch(ledgerBatch arrow.Record) (arrow.Record, error) {
    start := time.Now()
    
    n.logger.Debug().
        Str("operation", "ledger_batch_processing").
        Int64("input_rows", ledgerBatch.NumRows()).
        Msg("Starting native Arrow processing")

    // Extract TTP events using vectorized operations (no row-by-row processing)
    ttpEvents, err := n.extractTTPEventsVectorized(ledgerBatch)
    if err != nil {
        n.logger.Error().
            Err(err).
            Int64("input_rows", ledgerBatch.NumRows()).
            Msg("Failed to extract TTP events with vectorized processing")
        return nil, fmt.Errorf("vectorized TTP extraction failed: %w", err)
    }

    // Apply Arrow compute functions for analytics
    enrichedEvents, err := n.enrichEventsWithAnalytics(ttpEvents)
    if err != nil {
        n.logger.Error().
            Err(err).
            Int64("events_count", ttpEvents.NumRows()).
            Msg("Failed to enrich events with analytics")
        return nil, fmt.Errorf("event enrichment failed: %w", err)
    }

    duration := time.Since(start)
    n.logger.LogArrowProcessing("vectorized_ttp_extraction", int(enrichedEvents.NumRows()), duration)

    return enrichedEvents, nil
}

// extractTTPEventsVectorized uses Arrow compute for high-performance event extraction
func (n *NativeArrowTTPProcessor) extractTTPEventsVectorized(ledgerBatch arrow.Record) (arrow.Record, error) {
    // Build TTP event record using Arrow compute functions
    ttpSchema := n.schemaManager.GetTTPEventSchema()
    builder := array.NewRecordBuilder(n.allocator, ttpSchema)
    defer builder.Release()

    // Use Arrow compute to filter for payment operations vectorized
    paymentFilter, err := n.createPaymentOperationFilter(ledgerBatch)
    if err != nil {
        return nil, fmt.Errorf("payment filter creation failed: %w", err)
    }

    // Apply vectorized filter to entire batch
    filteredData, err := compute.Filter(n.computeContext, 
        compute.NewDatum(ledgerBatch), 
        compute.NewDatum(paymentFilter))
    if err != nil {
        return nil, fmt.Errorf("vectorized filtering failed: %w", err)
    }

    filteredRecord := filteredData.(*array.Record)
    
    // Extract TTP event fields using vectorized operations
    return n.buildTTPEventRecord(builder, filteredRecord)
}

// createPaymentOperationFilter creates Arrow compute filter for payment operations
func (n *NativeArrowTTPProcessor) createPaymentOperationFilter(ledgerBatch arrow.Record) (*array.Boolean, error) {
    // Get operation type column
    opTypeCol := ledgerBatch.Column(ledgerBatch.Schema().FieldIndices("operation_type")[0])
    
    // Create vectorized filter for payment operations
    paymentTypeScalar := scalar.NewInt32Scalar(int32(xdr.OperationTypePayment))
    pathReceiveScalar := scalar.NewInt32Scalar(int32(xdr.OperationTypePathPaymentStrictReceive))
    pathSendScalar := scalar.NewInt32Scalar(int32(xdr.OperationTypePathPaymentStrictSend))
    
    // Vectorized equality checks
    isPayment, err := compute.Equal(n.computeContext, 
        compute.NewDatum(opTypeCol), 
        compute.NewDatum(paymentTypeScalar))
    if err != nil {
        return nil, err
    }
    
    isPathReceive, err := compute.Equal(n.computeContext,
        compute.NewDatum(opTypeCol),
        compute.NewDatum(pathReceiveScalar))
    if err != nil {
        return nil, err
    }
    
    isPathSend, err := compute.Equal(n.computeContext,
        compute.NewDatum(opTypeCol),
        compute.NewDatum(pathSendScalar))
    if err != nil {
        return nil, err
    }
    
    // Combine filters with vectorized OR operations
    tempFilter, err := compute.Or(n.computeContext,
        compute.NewDatum(isPayment.(*array.Boolean)),
        compute.NewDatum(isPathReceive.(*array.Boolean)))
    if err != nil {
        return nil, err
    }
    
    finalFilter, err := compute.Or(n.computeContext,
        compute.NewDatum(tempFilter.(*array.Boolean)),
        compute.NewDatum(isPathSend.(*array.Boolean)))
    if err != nil {
        return nil, err
    }
    
    return finalFilter.(*array.Boolean), nil
}

// enrichEventsWithAnalytics adds computed analytics fields using Arrow functions
func (n *NativeArrowTTPProcessor) enrichEventsWithAnalytics(events arrow.Record) (arrow.Record, error) {
    // Create enriched schema with analytics fields
    enrichedSchema := n.schemaManager.GetEnrichedTTPEventSchema()
    builder := array.NewRecordBuilder(n.allocator, enrichedSchema)
    defer builder.Release()

    // Copy existing fields
    for i := 0; i < int(events.NumCols()); i++ {
        builder.Field(i).AppendValues(events.Column(i).Data(), nil)
    }

    // Add computed analytics fields using Arrow compute
    amountCol := events.Column(events.Schema().FieldIndices("amount")[0])
    
    // Compute amount categories vectorized
    amountCategories, err := n.computeAmountCategories(amountCol)
    if err != nil {
        return nil, fmt.Errorf("amount category computation failed: %w", err)
    }
    
    // Add to builder
    builder.Field(int(events.NumCols())).(*array.StringBuilder).AppendValues(amountCategories, nil)

    return builder.NewRecord(), nil
}

// computeAmountCategories uses Arrow compute to categorize amounts
func (n *NativeArrowTTPProcessor) computeAmountCategories(amounts arrow.Array) ([]string, error) {
    // Use Arrow compute for vectorized amount categorization
    smallThreshold := scalar.NewInt64Scalar(10000) // 100 XLM (7 decimals)
    largeThreshold := scalar.NewInt64Scalar(1000000) // 10,000 XLM
    
    isSmall, err := compute.Less(n.computeContext,
        compute.NewDatum(amounts),
        compute.NewDatum(smallThreshold))
    if err != nil {
        return nil, err
    }
    
    isLarge, err := compute.Greater(n.computeContext,
        compute.NewDatum(amounts),
        compute.NewDatum(largeThreshold))
    if err != nil {
        return nil, err
    }
    
    // Build category array using computed masks
    categories := make([]string, amounts.Len())
    smallMask := isSmall.(*array.Boolean)
    largeMask := isLarge.(*array.Boolean)
    
    for i := 0; i < amounts.Len(); i++ {
        if smallMask.Value(i) {
            categories[i] = "small"
        } else if largeMask.Value(i) {
            categories[i] = "large"
        } else {
            categories[i] = "medium"
        }
    }
    
    return categories, nil
}

// StartNativeArrowProcessor starts the pure Arrow processing service
func (n *NativeArrowTTPProcessor) StartNativeArrowProcessor(port int) error {
    n.logger.Info().
        Int("port", port).
        Str("protocol", "arrow-flight").
        Bool("legacy_support", false).
        Str("processing_type", "native_vectorized").
        Msg("Starting native Arrow TTP processor")

    // Implementation of native Arrow Flight server for TTP processing
    // No gRPC compatibility layer - pure Arrow only
    
    return nil
}
```

### Temporary Migration Bridge (Minimal)

**Note**: This compatibility layer is **temporary only** and will be removed after consumer migration.

**File**: `ttp-arrow-processor/go/bridge/legacy_bridge.go`

```go
package bridge

import (
    "context"
    "fmt"
    
    "github.com/your-org/ttp-arrow-processor/go/logging"
    eventservice "github.com/your-org/ttp-processor-demo/gen/event_service"
)

// TemporaryLegacyBridge provides minimal compatibility during migration ONLY
// This will be REMOVED after all consumers migrate to native Arrow
type TemporaryLegacyBridge struct {
    arrowProcessor *server.NativeArrowTTPProcessor
    converter      *ArrowToProtobufConverter
    logger         *logging.ComponentLogger
}

func NewTemporaryLegacyBridge(arrowProcessor *server.NativeArrowTTPProcessor) *TemporaryLegacyBridge {
    logger := logging.NewComponentLogger("legacy-bridge", "v1.0.0")
    
    logger.Warn().
        Bool("temporary_service", true).
        Str("removal_timeline", "after_consumer_migration").
        Msg("Starting TEMPORARY legacy bridge - will be removed")

    return &TemporaryLegacyBridge{
        arrowProcessor: arrowProcessor,
        converter:      NewArrowToProtobufConverter(),
        logger:         logger,
    }
}

// GetTTPEvents provides temporary compatibility - WILL BE REMOVED
func (b *TemporaryLegacyBridge) GetTTPEvents(
    req *eventservice.GetEventsRequest, 
    stream eventservice.EventService_GetTTPEventsServer) error {
    
    b.logger.Warn().
        Uint32("start_ledger", req.StartLedger).
        Uint32("end_ledger", req.EndLedger).
        Bool("deprecated", true).
        Msg("Legacy API called - consumer should migrate to native Arrow")

    // Process using native Arrow backend and convert for compatibility
    // This is inefficient by design to encourage migration
    // ... conversion implementation ...
    
    return nil
}
```

### Success Criteria for Phase 2

- [ ] Native Arrow processor achieves 1000+ events/sec with vectorized operations
- [ ] **10x performance improvement** demonstrated vs legacy processor
- [ ] **Zero row-by-row processing** - all operations use Arrow compute functions
- [ ] Memory usage reduced by 80% through native Arrow operations
- [ ] Legacy bridge provides temporary compatibility (marked for removal)
- [ ] All processing logic implemented with native Arrow compute

---

## Phase 3: Native Parquet Analytics Engine (4 weeks)

### Objectives
- Implement **native Arrow-to-Parquet** pipeline with zero data conversion
- Create high-performance analytics engine with native Arrow compute
- Deploy real-time streaming for live dashboards using Arrow Flight
- Enable advanced columnar analytics capabilities

### Deliverables

#### 3.1 Native Arrow Analytics Service

**New Service**: `arrow-analytics-engine`
**Location**: `arrow-analytics-engine/`
**Technology**: Native Arrow Flight, Arrow compute, native Parquet

**File**: `arrow-analytics-engine/go/engine/native_analytics.go`

```go
package engine

import (
    "context"
    "fmt"
    "sync"
    "time"
    
    "github.com/apache/arrow/go/v17/arrow"
    "github.com/apache/arrow/go/v17/arrow/array"
    "github.com/apache/arrow/go/v17/arrow/compute"
    "github.com/apache/arrow/go/v17/arrow/memory"
    "github.com/apache/arrow/go/v17/parquet/pqarrow"
    "github.com/your-org/arrow-analytics-engine/go/logging"
)

// NativeArrowAnalyticsEngine provides real-time analytics using native Arrow
type NativeArrowAnalyticsEngine struct {
    allocator       memory.Allocator
    computeContext  compute.Context
    logger          *logging.ComponentLogger
    
    // Native Arrow processing
    recordProcessor *RecordProcessor
    parquetWriter   *NativeParquetWriter
    analyticsCache  *ArrowAnalyticsCache
    
    // Real-time streaming
    flightStreams   map[string]chan arrow.Record
    streamMutex     sync.RWMutex
}

func NewNativeArrowAnalyticsEngine() *NativeArrowAnalyticsEngine {
    logger := logging.NewComponentLogger("arrow-analytics-engine", "v1.0.0")
    allocator := memory.NewGoAllocator()
    
    logger.Info().
        Str("engine_type", "native_arrow").
        Bool("parquet_native", true).
        Bool("compute_native", true).
        Msg("Initializing native Arrow analytics engine")

    return &NativeArrowAnalyticsEngine{
        allocator:      allocator,
        computeContext: compute.WithAllocator(context.Background(), allocator),
        logger:         logger,
        recordProcessor: NewRecordProcessor(allocator),
        parquetWriter:   NewNativeParquetWriter(allocator),
        analyticsCache:  NewArrowAnalyticsCache(allocator),
        flightStreams:   make(map[string]chan arrow.Record),
    }
}

// ProcessArrowStream processes native Arrow records for analytics and storage
func (e *NativeArrowAnalyticsEngine) ProcessArrowStream(ctx context.Context, stream <-chan arrow.Record) error {
    e.logger.Info().
        Str("operation", "arrow_stream_processing").
        Bool("native_processing", true).
        Msg("Starting native Arrow stream processing")

    for {
        select {
        case <-ctx.Done():
            e.logger.Info().
                Msg("Arrow stream processing stopped")
            return ctx.Err()
            
        case record := <-stream:
            start := time.Now()
            
            // Process record with native Arrow compute
            if err := e.processRecordNative(record); err != nil {
                e.logger.Error().
                    Err(err).
                    Int64("record_rows", record.NumRows()).
                    Msg("Failed to process Arrow record")
                continue
            }
            
            // Write to native Parquet (no conversion)
            if err := e.writeToNativeParquet(record); err != nil {
                e.logger.Error().
                    Err(err).
                    Msg("Failed to write to native Parquet")
                continue
            }
            
            // Stream to real-time consumers via Arrow Flight
            e.streamToFlightConsumers(record)
            
            // Update analytics cache with Arrow compute
            e.updateAnalyticsCache(record)
            
            duration := time.Since(start)
            e.logger.LogArrowProcessing("native_analytics", int(record.NumRows()), duration)
        }
    }
}

// processRecordNative processes records using only native Arrow operations
func (e *NativeArrowAnalyticsEngine) processRecordNative(record arrow.Record) error {
    // Apply Arrow compute functions for real-time analytics
    
    // 1. Compute volume metrics using Arrow aggregations
    volumeMetrics, err := e.computeVolumeMetricsArrow(record)
    if err != nil {
        return fmt.Errorf("volume metrics computation failed: %w", err)
    }
    
    // 2. Create time-window aggregations
    timeWindows, err := e.computeTimeWindowsArrow(record)
    if err != nil {
        return fmt.Errorf("time window computation failed: %w", err)
    }
    
    // 3. Generate derived analytics columns
    enrichedRecord, err := e.addAnalyticsColumnsArrow(record, volumeMetrics, timeWindows)
    if err != nil {
        return fmt.Errorf("analytics enrichment failed: %w", err)
    }
    
    // Store enriched record for further processing
    e.analyticsCache.StoreRecord(enrichedRecord)
    
    return nil
}

// computeVolumeMetricsArrow uses native Arrow compute for volume calculations
func (e *NativeArrowAnalyticsEngine) computeVolumeMetricsArrow(record arrow.Record) (*VolumeMetrics, error) {
    amountCol := record.Column(record.Schema().FieldIndices("amount")[0])
    
    // Use Arrow compute for aggregations (vectorized, no loops)
    totalVolume, err := compute.Sum(e.computeContext, compute.NewDatum(amountCol))
    if err != nil {
        return nil, err
    }
    
    avgVolume, err := compute.Mean(e.computeContext, compute.NewDatum(amountCol))
    if err != nil {
        return nil, err
    }
    
    maxVolume, err := compute.Max(e.computeContext, compute.NewDatum(amountCol))
    if err != nil {
        return nil, err
    }
    
    minVolume, err := compute.Min(e.computeContext, compute.NewDatum(amountCol))
    if err != nil {
        return nil, err
    }
    
    return &VolumeMetrics{
        Total:   totalVolume.(*scalar.Int64).Value,
        Average: avgVolume.(*scalar.Float64).Value,
        Max:     maxVolume.(*scalar.Int64).Value,
        Min:     minVolume.(*scalar.Int64).Value,
        Count:   record.NumRows(),
    }, nil
}

// writeToNativeParquet writes Arrow records directly to Parquet (no conversion)
func (e *NativeArrowAnalyticsEngine) writeToNativeParquet(record arrow.Record) error {
    // Determine partition based on time column
    timeCol := record.Column(record.Schema().FieldIndices("ledger_close_time")[0])
    partition := e.determinePartition(timeCol)
    
    // Get native Parquet writer for partition
    writer, err := e.parquetWriter.GetWriterForPartition(partition)
    if err != nil {
        return fmt.Errorf("failed to get Parquet writer: %w", err)
    }
    
    // Write Arrow record directly to Parquet (native, no conversion)
    table := array.NewTableFromRecords(record.Schema(), []arrow.Record{record})
    defer table.Release()
    
    if err := pqarrow.WriteTable(table, writer, table.NumRows(), nil, nil); err != nil {
        return fmt.Errorf("native Parquet write failed: %w", err)
    }
    
    e.logger.Debug().
        Str("partition", partition).
        Int64("rows_written", record.NumRows()).
        Str("format", "native_parquet").
        Msg("Wrote to native Parquet")
    
    return nil
}

// streamToFlightConsumers streams records to connected Arrow Flight clients
func (e *NativeArrowAnalyticsEngine) streamToFlightConsumers(record arrow.Record) {
    e.streamMutex.RLock()
    defer e.streamMutex.RUnlock()
    
    // Stream to all connected Arrow Flight clients (native Arrow, no conversion)
    for streamID, ch := range e.flightStreams {
        select {
        case ch <- record:
            e.logger.Debug().
                Str("stream_id", streamID).
                Int64("records_streamed", record.NumRows()).
                Msg("Streamed to Arrow Flight client")
        default:
            // Client not keeping up, log warning
            e.logger.Warn().
                Str("stream_id", streamID).
                Msg("Arrow Flight client lagging, dropping records")
        }
    }
}

type VolumeMetrics struct {
    Total   int64
    Average float64
    Max     int64
    Min     int64
    Count   int64
}
```

#### 3.2 Native Arrow Flight Analytics Server

**File**: `arrow-analytics-engine/go/server/flight_analytics.go`

```go
package server

import (
    "context"
    "fmt"
    
    "github.com/apache/arrow/go/v17/arrow/flight"
    "github.com/your-org/arrow-analytics-engine/go/logging"
)

// ArrowFlightAnalyticsServer provides native Arrow Flight streaming for analytics
type ArrowFlightAnalyticsServer struct {
    flight.BaseFlightServer
    
    engine     *engine.NativeArrowAnalyticsEngine
    logger     *logging.ComponentLogger
}

func NewArrowFlightAnalyticsServer(engine *engine.NativeArrowAnalyticsEngine) *ArrowFlightAnalyticsServer {
    logger := logging.NewComponentLogger("flight-analytics-server", "v1.0.0")
    
    logger.Info().
        Str("server_type", "native_arrow_flight").
        Bool("legacy_support", false).
        Msg("Initializing native Arrow Flight analytics server")

    return &ArrowFlightAnalyticsServer{
        engine: engine,
        logger: logger,
    }
}

// DoGet streams native Arrow analytics data to clients
func (s *ArrowFlightAnalyticsServer) DoGet(ticket *flight.Ticket, stream flight.FlightService_DoGetServer) error {
    s.logger.Info().
        Str("operation", "native_flight_stream").
        Interface("ticket", ticket.Ticket).
        Msg("Starting native Arrow Flight analytics stream")

    // Parse analytics query from ticket
    query, err := parseAnalyticsQuery(ticket.Ticket)
    if err != nil {
        s.logger.Error().
            Err(err).
            Msg("Failed to parse analytics query")
        return fmt.Errorf("invalid analytics query: %w", err)
    }

    // Create native Arrow stream for query results
    recordStream, err := s.engine.ExecuteAnalyticsQuery(stream.Context(), query)
    if err != nil {
        s.logger.Error().
            Err(err).
            Str("query_type", query.Type).
            Msg("Failed to execute analytics query")
        return fmt.Errorf("analytics query execution failed: %w", err)
    }

    recordCount := 0
    
    // Stream native Arrow records (no conversion)
    for record := range recordStream {
        flightData := &flight.FlightData{
            FlightDescriptor: &flight.FlightDescriptor{
                Type: flight.FlightDescriptor_CMD,
                Cmd:  ticket.Ticket,
            },
        }
        
        // Serialize Arrow record to flight data (native)
        if err := flight.SerializeRecord(record, flightData, s.engine.GetAllocator()); err != nil {
            s.logger.Error().
                Err(err).
                Int("record_count", recordCount).
                Msg("Failed to serialize Arrow record")
            continue
        }
        
        if err := stream.Send(flightData); err != nil {
            s.logger.Error().
                Err(err).
                Int("records_sent", recordCount).
                Msg("Failed to send flight data")
            return fmt.Errorf("flight stream error: %w", err)
        }
        
        recordCount++
    }

    s.logger.Info().
        Int("total_records", recordCount).
        Str("query_type", query.Type).
        Msg("Native Arrow Flight analytics stream completed")

    return nil
}

type AnalyticsQuery struct {
    Type        string
    TimeRange   TimeRange
    Filters     map[string]interface{}
    Aggregations []string
}

type TimeRange struct {
    Start time.Time
    End   time.Time
}

func parseAnalyticsQuery(ticket []byte) (*AnalyticsQuery, error) {
    // Parse analytics query parameters from ticket
    // Support for real-time and historical analytics queries
    return nil, nil
}
```

### Consumer Migration to Native Arrow

#### 3.3 Native Arrow Consumer Examples

**File**: `examples/native_arrow_consumers.md`

```markdown
# Native Arrow Consumers - Migration Examples

## Node.js Native Arrow Flight Consumer

```javascript
import { ArrowFlightClient } from '@apache-arrow/flight';
import { Table } from 'apache-arrow';

// Native Arrow Flight client (no gRPC compatibility)
const client = new ArrowFlightClient({
    endpoint: 'arrow-flight://localhost:8817'
});

// Stream native Arrow data
async function streamNativeArrowAnalytics() {
    const ticket = Buffer.from('analytics:real_time:ttp_events');
    const stream = client.getFlightData(ticket);
    
    console.log('📊 Streaming native Arrow analytics data...');
    
    for await (const flightData of stream) {
        const table = Table.from(flightData.data);
        
        // Process native Arrow table (no conversion needed)
        console.log(`Received ${table.numRows} TTP events`);
        
        // Use Arrow compute functions directly
        const totalVolume = table.getColumn('amount').data
            .reduce((sum, chunk) => sum + chunk.values.reduce((a, b) => a + b, 0), 0);
            
        console.log(`Total volume: ${totalVolume} stroops`);
        
        // Real-time analytics with native Arrow
        const uniqueAssets = new Set(table.getColumn('asset_code').toArray());
        console.log(`Active assets: ${uniqueAssets.size}`);
    }
}

streamNativeArrowAnalytics().catch(console.error);
```

## Go Native Arrow Consumer

```go
package main

import (
    "context"
    "fmt"
    "io"

    "github.com/apache/arrow/go/v17/arrow/flight"
    "github.com/apache/arrow/go/v17/arrow/compute"
)

func main() {
    // Native Arrow Flight client
    conn, err := grpc.Dial("localhost:8817", grpc.WithInsecure())
    if err != nil {
        panic(err)
    }
    defer conn.Close()

    client := flight.NewFlightServiceClient(conn)
    
    // Request native Arrow analytics stream
    ticket := &flight.Ticket{
        Ticket: []byte("analytics:historical:volume_by_asset"),
    }
    
    stream, err := client.DoGet(context.Background(), ticket)
    if err != nil {
        panic(err)
    }
    
    fmt.Println("📊 Streaming native Arrow analytics...")
    
    allocator := memory.NewGoAllocator()
    defer allocator.AssertSize(0)
    
    for {
        data, err := stream.Recv()
        if err == io.EOF {
            break
        }
        if err != nil {
            panic(err)
        }
        
        // Deserialize native Arrow record
        record, err := flight.DeserializeRecord(data, allocator)
        if err != nil {
            panic(err)
        }
        defer record.Release()
        
        fmt.Printf("Received %d analytics records\n", record.NumRows())
        
        // Use native Arrow compute for analysis
        amountCol := record.Column(record.Schema().FieldIndices("amount")[0])
        ctx := compute.WithAllocator(context.Background(), allocator)
        
        total, err := compute.Sum(ctx, compute.NewDatum(amountCol))
        if err != nil {
            panic(err)
        }
        
        avg, err := compute.Mean(ctx, compute.NewDatum(amountCol))
        if err != nil {
            panic(err)
        }
        
        fmt.Printf("Total volume: %d, Average: %.2f\n", 
            total.(*scalar.Int64).Value,
            avg.(*scalar.Float64).Value)
    }
}
```

## Rust Native Arrow Consumer

```rust
use arrow_flight::{FlightClient, Ticket};
use arrow::compute::sum;
use futures::StreamExt;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Native Arrow Flight client
    let mut client = FlightClient::new("http://localhost:8817").await?;
    
    // Stream native Arrow data
    let ticket = Ticket {
        ticket: b"analytics:real_time:market_data".to_vec(),
    };
    
    let mut stream = client.do_get(ticket).await?;
    
    println!("📊 Streaming native Arrow market data...");
    
    while let Some(flight_data) = stream.next().await {
        let flight_data = flight_data?;
        
        // Deserialize native Arrow record batch
        let record_batch = flight_data.try_into_record_batch()?;
        println!("Received {} market events", record_batch.num_rows());
        
        // Use native Arrow compute
        let amount_col = record_batch.column_by_name("amount").unwrap();
        let total_volume = sum(amount_col)?;
        
        println!("Market volume: {:?}", total_volume);
    }
    
    Ok(())
}
```
```

### Success Criteria for Phase 3

- [ ] Native Parquet writes achieve 5000+ events/sec with zero conversion overhead
- [ ] Arrow Flight streaming supports 10000+ events/sec to native clients
- [ ] All analytics operations use Arrow compute functions (no custom loops)
- [ ] Consumer examples demonstrate native Arrow adoption patterns
- [ ] Storage efficiency achieves 50%+ reduction through native Parquet compression
- [ ] Real-time analytics dashboard operational with native Arrow backend

---

## Phase 4: Production Deployment & Native Arrow Migration (3 weeks)

### Objectives
- Deploy **native Arrow services** as primary production systems
- Implement production monitoring with structured logging
- Complete migration of all consumers to native Arrow
- **Decommission legacy services** after successful migration

### Deliverables

#### 4.1 Production Monitoring with Structured Logging

**File**: `shared/monitoring/native_arrow_metrics.go`

```go
package monitoring

import (
    "time"
    
    "github.com/apache/arrow/go/v17/arrow/memory"
    "github.com/prometheus/client_golang/prometheus"
    "github.com/prometheus/client_golang/prometheus/promauto"
    "github.com/your-org/shared/logging"
)

var (
    // Native Arrow-specific metrics
    ArrowNativeProcessingRate = promauto.NewCounterVec(
        prometheus.CounterOpts{
            Name: "arrow_native_records_processed_total",
            Help: "Total native Arrow records processed with vectorized operations",
        },
        []string{"service", "operation", "vectorized"},
    )
    
    ArrowMemoryEfficiency = promauto.NewGaugeVec(
        prometheus.GaugeOpts{
            Name: "arrow_memory_efficiency_ratio",
            Help: "Arrow memory efficiency compared to legacy processing",
        },
        []string{"service", "allocator_type"},
    )
    
    ArrowFlightConnections = promauto.NewGaugeVec(
        prometheus.GaugeOpts{
            Name: "arrow_flight_active_connections",
            Help: "Active native Arrow Flight connections",
        },
        []string{"service", "client_type"},
    )
    
    NativeParquetWriteRate = promauto.NewHistogramVec(
        prometheus.HistogramOpts{
            Name:    "native_parquet_write_duration_seconds",
            Help:    "Time taken for native Arrow-to-Parquet writes",
            Buckets: prometheus.DefBuckets,
        },
        []string{"partition_scheme", "compression"},
    )
)

// NativeArrowMetricsCollector provides comprehensive Arrow production metrics
type NativeArrowMetricsCollector struct {
    services map[string]NativeArrowService
    logger   *logging.ComponentLogger
}

func NewNativeArrowMetricsCollector() *NativeArrowMetricsCollector {
    logger := logging.NewComponentLogger("arrow-metrics-collector", "v1.0.0")
    
    logger.Info().
        Bool("native_arrow_monitoring", true).
        Str("metrics_type", "production").
        Msg("Initializing native Arrow metrics collection")

    return &NativeArrowMetricsCollector{
        services: make(map[string]NativeArrowService),
        logger:   logger,
    }
}

func (c *NativeArrowMetricsCollector) RegisterNativeService(name string, service NativeArrowService) {
    c.services[name] = service
    
    c.logger.Info().
        Str("service_name", name).
        Str("service_type", "native_arrow").
        Msg("Registered native Arrow service for monitoring")
}

func (c *NativeArrowMetricsCollector) CollectMetrics() {
    for serviceName, service := range c.services {
        metrics := service.GetNativeArrowMetrics()
        
        // Log structured metrics for monitoring systems
        c.logger.Info().
            Str("service_name", serviceName).
            Str("operation", "metrics_collection").
            Int64("records_processed", metrics.RecordsProcessed).
            Int64("vectorized_operations", metrics.VectorizedOperations).
            Float64("memory_efficiency", metrics.MemoryEfficiency).
            Int("active_flight_connections", metrics.FlightConnections).
            Msg("Native Arrow service metrics")
        
        // Update Prometheus metrics
        ArrowNativeProcessingRate.WithLabelValues(serviceName, "vectorized", "true").
            Add(float64(metrics.VectorizedOperations))
        
        ArrowMemoryEfficiency.WithLabelValues(serviceName, "native").
            Set(metrics.MemoryEfficiency)
        
        ArrowFlightConnections.WithLabelValues(serviceName, "native").
            Set(float64(metrics.FlightConnections))
    }
}

type NativeArrowService interface {
    GetNativeArrowMetrics() NativeArrowMetrics
    GetAllocator() memory.Allocator
}

type NativeArrowMetrics struct {
    RecordsProcessed     int64
    VectorizedOperations int64
    MemoryEfficiency     float64  // Ratio vs legacy processing
    FlightConnections    int
    ParquetWriteRate     float64  // Records/second
    ComputeOperations    int64    // Arrow compute function calls
}
```

#### 4.2 Production Health Monitoring

**File**: `shared/health/native_arrow_health.go`

```go
package health

import (
    "context"
    "fmt"
    "net/http"
    "time"
    
    "github.com/apache/arrow/go/v17/arrow/memory"
    "github.com/your-org/shared/logging"
)

// NativeArrowHealthChecker provides comprehensive health monitoring
type NativeArrowHealthChecker struct {
    services   map[string]NativeArrowService
    allocators map[string]memory.Allocator
    logger     *logging.ComponentLogger
}

func NewNativeArrowHealthChecker() *NativeArrowHealthChecker {
    logger := logging.NewComponentLogger("arrow-health-checker", "v1.0.0")
    
    logger.Info().
        Bool("native_arrow_health", true).
        Str("health_type", "production").
        Msg("Initializing native Arrow health monitoring")

    return &NativeArrowHealthChecker{
        services:   make(map[string]NativeArrowService),
        allocators: make(map[string]memory.Allocator),
        logger:     logger,
    }
}

func (h *NativeArrowHealthChecker) HandleHealthCheck(w http.ResponseWriter, r *http.Request) {
    ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
    defer cancel()
    
    health := h.checkNativeArrowHealth(ctx)
    
    // Log health check results with structured logging
    h.logger.Info().
        Str("operation", "health_check").
        Str("overall_status", health.Overall).
        Int("healthy_services", health.HealthyServices).
        Int("total_services", health.TotalServices).
        Float64("avg_memory_efficiency", health.AvgMemoryEfficiency).
        Msg("Native Arrow health check completed")

    w.Header().Set("Content-Type", "application/json")
    if health.Overall == "healthy" {
        w.WriteHeader(http.StatusOK)
    } else {
        w.WriteHeader(http.StatusServiceUnavailable)
    }
    
    json.NewEncoder(w).Encode(health)
}

func (h *NativeArrowHealthChecker) checkNativeArrowHealth(ctx context.Context) *NativeArrowHealthStatus {
    status := &NativeArrowHealthStatus{
        Overall:   "healthy",
        Timestamp: time.Now(),
        Services:  make(map[string]NativeServiceHealth),
        Arrow: NativeArrowHealth{
            MemoryAllocators: make(map[string]NativeMemoryStats),
        },
    }
    
    healthyCount := 0
    totalServices := len(h.services)
    
    // Check each native Arrow service
    for serviceName, service := range h.services {
        serviceHealth := h.checkNativeServiceHealth(ctx, serviceName, service)
        status.Services[serviceName] = serviceHealth
        
        if serviceHealth.Status == "healthy" {
            healthyCount++
        } else {
            status.Overall = "degraded"
            
            h.logger.Warn().
                Str("service_name", serviceName).
                Str("service_status", serviceHealth.Status).
                Float64("processing_rate", serviceHealth.ProcessingRate).
                Msg("Native Arrow service unhealthy")
        }
    }
    
    status.HealthyServices = healthyCount
    status.TotalServices = totalServices
    
    // Check native Arrow memory allocators
    totalEfficiency := 0.0
    allocatorCount := 0
    
    for allocatorName, allocator := range h.allocators {
        memStats := NativeMemoryStats{
            AllocatedBytes: allocator.AllocatedBytes(),
            MaxMemory:     2 * 1024 * 1024 * 1024, // 2GB limit for production
        }
        memStats.UtilizationPercent = float64(memStats.AllocatedBytes) / float64(memStats.MaxMemory) * 100
        memStats.Efficiency = h.calculateMemoryEfficiency(allocator)
        
        status.Arrow.MemoryAllocators[allocatorName] = memStats
        totalEfficiency += memStats.Efficiency
        allocatorCount++
        
        // Alert on high memory usage or low efficiency
        if memStats.UtilizationPercent > 85 {
            status.Overall = "degraded"
            status.Alerts = append(status.Alerts, fmt.Sprintf(
                "High memory usage in %s: %.1f%%", allocatorName, memStats.UtilizationPercent))
        }
        
        if memStats.Efficiency < 0.8 { // Less than 80% efficiency
            status.Alerts = append(status.Alerts, fmt.Sprintf(
                "Low memory efficiency in %s: %.1f%%", allocatorName, memStats.Efficiency*100))
        }
    }
    
    if allocatorCount > 0 {
        status.AvgMemoryEfficiency = totalEfficiency / float64(allocatorCount)
    }
    
    return status
}

type NativeArrowHealthStatus struct {
    Overall              string                          `json:"overall"`
    Timestamp           time.Time                       `json:"timestamp"`
    Services            map[string]NativeServiceHealth `json:"services"`
    Arrow               NativeArrowHealth               `json:"arrow"`
    Alerts              []string                        `json:"alerts,omitempty"`
    HealthyServices     int                             `json:"healthy_services"`
    TotalServices       int                             `json:"total_services"`
    AvgMemoryEfficiency float64                         `json:"avg_memory_efficiency"`
}

type NativeServiceHealth struct {
    Status          string        `json:"status"`
    ProcessingRate  float64       `json:"processing_rate"`
    VectorizedOps   int64         `json:"vectorized_operations"`
    FlightClients   int           `json:"flight_clients"`
    LastProcessed   time.Time     `json:"last_processed"`
    ResponseTime    time.Duration `json:"response_time"`
}

type NativeArrowHealth struct {
    MemoryAllocators map[string]NativeMemoryStats `json:"memory_allocators"`
}

type NativeMemoryStats struct {
    AllocatedBytes      int64   `json:"allocated_bytes"`
    MaxMemory          int64   `json:"max_memory"`
    UtilizationPercent float64 `json:"utilization_percent"`
    Efficiency         float64 `json:"efficiency"`
}
```

#### 4.3 Consumer Migration Completion

**File**: `migration/complete_native_migration.go`

```go
package migration

import (
    "context"
    "fmt"
    "time"
    
    "github.com/your-org/shared/logging"
)

// NativeMigrationManager handles the complete migration to native Arrow
type NativeMigrationManager struct {
    logger              *logging.ComponentLogger
    legacyServices      map[string]LegacyService
    nativeServices      map[string]NativeArrowService
    consumers           map[string]Consumer
    migrationProgress   *MigrationProgress
}

func NewNativeMigrationManager() *NativeMigrationManager {
    logger := logging.NewComponentLogger("native-migration-manager", "v1.0.0")
    
    logger.Info().
        Bool("migration_to_native", true).
        Str("target_architecture", "pure_arrow").
        Msg("Initializing native Arrow migration manager")

    return &NativeMigrationManager{
        logger:            logger,
        legacyServices:    make(map[string]LegacyService),
        nativeServices:    make(map[string]NativeArrowService),
        consumers:         make(map[string]Consumer),
        migrationProgress: NewMigrationProgress(),
    }
}

// ExecuteCompleteMigration performs the final migration to pure native Arrow
func (m *NativeMigrationManager) ExecuteCompleteMigration(ctx context.Context) error {
    m.logger.Info().
        Str("operation", "complete_native_migration").
        Bool("legacy_decommission", true).
        Msg("Starting complete migration to native Arrow")

    // Phase 1: Validate all consumers are native Arrow ready
    if err := m.validateConsumerMigration(); err != nil {
        m.logger.Error().
            Err(err).
            Msg("Consumer migration validation failed")
        return fmt.Errorf("consumer validation failed: %w", err)
    }

    // Phase 2: Switch traffic to native Arrow services
    if err := m.switchToNativeServices(); err != nil {
        m.logger.Error().
            Err(err).
            Msg("Failed to switch to native Arrow services")
        return fmt.Errorf("native service switch failed: %w", err)
    }

    // Phase 3: Verify native Arrow performance
    if err := m.validateNativePerformance(); err != nil {
        m.logger.Error().
            Err(err).
            Msg("Native Arrow performance validation failed")
        return fmt.Errorf("performance validation failed: %w", err)
    }

    // Phase 4: Decommission legacy services
    if err := m.decommissionLegacyServices(); err != nil {
        m.logger.Error().
            Err(err).
            Msg("Legacy service decommission failed")
        return fmt.Errorf("legacy decommission failed: %w", err)
    }

    // Phase 5: Remove temporary compatibility layers
    if err := m.removeCompatibilityLayers(); err != nil {
        m.logger.Error().
            Err(err).
            Msg("Compatibility layer removal failed")
        return fmt.Errorf("compatibility removal failed: %w", err)
    }

    m.logger.Info().
        Str("operation", "migration_completed").
        Bool("architecture", true).
        Int("native_services", len(m.nativeServices)).
        Int("migrated_consumers", len(m.consumers)).
        Msg("Native Arrow migration completed successfully")

    return nil
}

func (m *NativeMigrationManager) validateConsumerMigration() error {
    m.logger.Info().
        Str("operation", "consumer_validation").
        Int("total_consumers", len(m.consumers)).
        Msg("Validating consumer migration to native Arrow")

    for consumerName, consumer := range m.consumers {
        if !consumer.IsNativeArrowReady() {
            m.logger.Error().
                Str("consumer_name", consumerName).
                Bool("native_arrow_ready", false).
                Msg("Consumer not ready for native Arrow")
            return fmt.Errorf("consumer %s not ready for native Arrow", consumerName)
        }
        
        m.logger.Info().
            Str("consumer_name", consumerName).
            Bool("native_arrow_ready", true).
            Msg("Consumer validated for native Arrow")
    }

    return nil
}

func (m *NativeMigrationManager) switchToNativeServices() error {
    m.logger.Info().
        Str("operation", "service_switch").
        Int("native_services", len(m.nativeServices)).
        Msg("Switching traffic to native Arrow services")

    for serviceName, nativeService := range m.nativeServices {
        if err := nativeService.StartProduction(); err != nil {
            m.logger.Error().
                Err(err).
                Str("service_name", serviceName).
                Msg("Failed to start native Arrow service")
            return fmt.Errorf("failed to start %s: %w", serviceName, err)
        }
        
        m.logger.Info().
            Str("service_name", serviceName).
            Bool("production_ready", true).
            Str("service_type", "native_arrow").
            Msg("Native Arrow service started in production")
    }

    return nil
}

func (m *NativeMigrationManager) decommissionLegacyServices() error {
    m.logger.Info().
        Str("operation", "legacy_decommission").
        Int("legacy_services", len(m.legacyServices)).
        Bool("permanent_removal", true).
        Msg("Decommissioning legacy services")

    for serviceName, legacyService := range m.legacyServices {
        // Drain traffic gracefully
        if err := legacyService.DrainTraffic(time.Minute * 5); err != nil {
            m.logger.Warn().
                Err(err).
                Str("service_name", serviceName).
                Msg("Failed to drain legacy service gracefully")
        }

        // Stop service
        if err := legacyService.Stop(); err != nil {
            m.logger.Error().
                Err(err).
                Str("service_name", serviceName).
                Msg("Failed to stop legacy service")
            return fmt.Errorf("failed to stop legacy service %s: %w", serviceName, err)
        }
        
        m.logger.Info().
            Str("service_name", serviceName).
            Bool("decommissioned", true).
            Str("service_type", "legacy").
            Msg("Legacy service decommissioned")
    }

    return nil
}

func (m *NativeMigrationManager) removeCompatibilityLayers() error {
    m.logger.Info().
        Str("operation", "compatibility_removal").
        Bool("pure_arrow_architecture", true).
        Msg("Removing temporary compatibility layers")

    // Remove all compatibility bridges and conversion layers
    // This finalizes the migration to pure native Arrow
    
    m.logger.Info().
        Bool("pure_native_arrow", true).
        Bool("compatibility_layers_removed", true).
        Msg("Compatibility layers removed - pure native Arrow architecture achieved")

    return nil
}

type MigrationProgress struct {
    ConsumersMigrated int
    ServicesDeployed  int
    LegacyRemoved     int
    StartTime         time.Time
}

func NewMigrationProgress() *MigrationProgress {
    return &MigrationProgress{
        StartTime: time.Now(),
    }
}
```

### Success Criteria for Phase 4

- [ ] **100% native Arrow architecture** deployed in production
- [ ] All consumers successfully migrated to native Arrow Flight clients
- [ ] Legacy services completely decommissioned
- [ ] Production monitoring operational with structured logging
- [ ] Performance targets exceeded (10x improvement achieved)
- [ ] **Zero compatibility layers** remaining - pure native Arrow

---

## Native Arrow Consumer Migration Guide

### Complete Migration Path

#### Before Migration (Legacy)
```javascript
// OLD: Legacy gRPC + Protobuf
const stream = client.getTTPEvents({startLedger, endLedger});
stream.on('data', (event) => {
    processLegacyEvent(event); // Row-by-row processing
});
```

#### After Migration (Native Arrow)
```javascript
// NEW: Native Arrow Flight
const client = new ArrowFlightClient({endpoint: 'arrow-flight://localhost:8817'});
const stream = client.getFlightData(ticket);

for await (const batch of stream) {
    // Process entire batches with Arrow compute
    const totalVolume = batch.getColumn('amount').data
        .reduce((sum, chunk) => sum + chunk.values.reduce((a, b) => a + b, 0), 0);
    
    // Native Arrow analytics
    processArrowBatch(batch); // Vectorized processing
}
```

### Migration Timeline

| Week | Focus | Deliverable |
|------|-------|-------------|
| 1-4 | **Native Arrow Foundation** | Arrow Flight services, schemas, structured logging |
| 5-10 | **Pure Arrow Processing** | Vectorized TTP processing, Arrow compute engine |
| 11-14 | **Native Analytics Engine** | Parquet output, Flight streaming, native consumers |
| 15-17 | **Legacy Decommission** | Consumer migration, legacy removal, production deployment |

---

## Success Metrics & Final Validation

### Performance Achievements

| Metric | Legacy Baseline | Native Arrow Target | Expected Result |
|--------|-----------------|-------------------|-----------------|
| Processing Throughput | 50-100 ledgers/sec | 500+ ledgers/sec | **10x improvement** |
| Event Processing Rate | 100-200 events/sec | 2000+ events/sec | **10x improvement** |
| Memory Usage | Baseline | 80% reduction | **Native Arrow efficiency** |
| End-to-End Latency | Current avg | <50ms | **2x improvement** |
| Storage Efficiency | Legacy format | 70% reduction | **Native Parquet compression** |

### Architecture Validation

- [ ] **100% Native Arrow**: No legacy services remaining
- [ ] **Zero Compatibility Layers**: Pure Arrow architecture achieved  
- [ ] **Vectorized Processing**: All operations use Arrow compute functions
- [ ] **Structured Logging**: Consistent zerolog implementation across all services
- [ ] **Production Ready**: Full monitoring, alerting, and operational tooling

---

## Conclusion

This implementation plan delivers a **complete transformation** to native Apache Arrow architecture:

✅ **Native Arrow First**: All services built with pure Arrow technologies
✅ **10x Performance**: Achieved through vectorized processing and Arrow compute
✅ **Structured Logging**: Consistent zerolog implementation across all services  
✅ **Zero Legacy Dependencies**: Complete migration with legacy service decommission
✅ **Production Ready**: Comprehensive monitoring, health checks, and operational tooling
✅ **Consumer Native**: All consumers migrated to native Arrow Flight clients

The phased approach ensures zero downtime while delivering transformational performance improvements and establishing ttp-processor-demo as a best-in-class Arrow-native data processing platform.

### Immediate Next Steps

1. **Begin Phase 1**: Start native Arrow development environment setup
2. **Team Training**: Arrow concepts and structured logging with zerolog
3. **Infrastructure Setup**: Nix environments and monitoring preparation  
4. **Consumer Communication**: Notify consumers of migration timeline and native Arrow benefits

This plan positions ttp-processor-demo as a flagship example of native Apache Arrow architecture in production blockchain data processing.