# Parquet Consumer Implementation Plan for ttp-processor-demo

## Overview

This document provides a detailed implementation plan for creating a Parquet-based consumer that saves Token Transfer Protocol (TTP) events to Parquet files. The implementation will be based on Apache Arrow for high-performance columnar data processing and storage.

## Architecture Design

### Component Overview

```
ttp-processor → Parquet Consumer → Parquet Files
   (gRPC)         (Arrow/Parquet)     (Data Lake)
```

### Data Flow
1. **Event Ingestion**: Consume TTP events from ttp-processor via gRPC
2. **Batch Accumulation**: Collect events into Arrow Record Batches
3. **Parquet Writing**: Convert Arrow batches to compressed Parquet files
4. **Partitioning**: Organize files by date, hour, and event type
5. **Metadata Management**: Track file locations and schema versions

## Implementation Components

### 1. Core Consumer Service

#### 1.1 Service Structure
```go
package main

import (
    "context"
    "fmt"
    "path/filepath"
    "sync"
    "time"

    "github.com/apache/arrow/go/v17/arrow"
    "github.com/apache/arrow/go/v17/arrow/array"
    "github.com/apache/arrow/go/v17/arrow/memory"
    "github.com/apache/arrow/go/v17/parquet/compress"
    "github.com/apache/arrow/go/v17/parquet/pqarrow"
    "github.com/apache/arrow/go/v17/parquet"
    
    eventservice "github.com/withObsrvr/ttp-processor/gen/event_service"
    "go.uber.org/zap"
    "google.golang.org/grpc"
    "google.golang.org/grpc/credentials/insecure"
)

type ParquetConsumerConfig struct {
    // Connection settings
    TTPProcessorEndpoint string `json:"ttp_processor_endpoint" default:"localhost:50051"`
    
    // Output settings
    OutputPath          string `json:"output_path" default:"./data"`
    PartitioningScheme  string `json:"partitioning_scheme" default:"date_hour"`
    
    // Performance settings
    BatchSize           int    `json:"batch_size" default:"1000"`
    FlushInterval       time.Duration `json:"flush_interval" default:"30s"`
    MaxFileSize         int64  `json:"max_file_size" default:"134217728"` // 128MB
    
    // Compression settings
    CompressionCodec    string `json:"compression_codec" default:"snappy"`
    CompressionLevel    int    `json:"compression_level" default:"1"`
    
    // Schema settings
    SchemaVersion       string `json:"schema_version" default:"1.0.0"`
    EnableSchemaEvolution bool  `json:"enable_schema_evolution" default:"true"`
}

type ParquetConsumer struct {
    config          *ParquetConsumerConfig
    client          eventservice.EventServiceClient
    conn            *grpc.ClientConn
    logger          *zap.Logger
    
    // Arrow components
    allocator       memory.Allocator
    schema          *arrow.Schema
    builder         *array.RecordBuilder
    
    // Batching and flushing
    eventBatch      []EventData
    batchMutex      sync.Mutex
    lastFlush       time.Time
    
    // File management
    currentFile     *ParquetFile
    fileManager     *FileManager
    
    // Metrics
    metrics         *ConsumerMetrics
}

type EventData struct {
    Timestamp       time.Time
    LedgerSequence  uint32
    TransactionHash string
    EventType       string
    SourceAccount   string
    DestAccount     string
    AssetCode       string
    Amount          string
    // Additional fields based on TTP event schema
}

type ParquetFile struct {
    Path        string
    Writer      *pqarrow.FileWriter
    RowCount    int64
    FileSize    int64
    StartTime   time.Time
    EndTime     time.Time
}

type ConsumerMetrics struct {
    mu sync.RWMutex
    EventsReceived     int64
    EventsWritten      int64
    FilesCreated       int64
    BatchesProcessed   int64
    ProcessingLatency  time.Duration
    LastFlushTime      time.Time
    ErrorCount         int64
    LastError          error
}
```

#### 1.2 Service Initialization
```go
func NewParquetConsumer(config *ParquetConsumerConfig) (*ParquetConsumer, error) {
    logger, err := zap.NewProduction()
    if err != nil {
        return nil, fmt.Errorf("failed to initialize logger: %w", err)
    }

    // Connect to TTP processor
    conn, err := grpc.Dial(config.TTPProcessorEndpoint, 
        grpc.WithTransportCredentials(insecure.NewCredentials()))
    if err != nil {
        return nil, fmt.Errorf("failed to connect to TTP processor: %w", err)
    }

    client := eventservice.NewEventServiceClient(conn)
    
    // Initialize Arrow components
    allocator := memory.NewGoAllocator()
    schema := createTTPEventSchema()
    builder := array.NewRecordBuilder(allocator, schema)
    
    // Initialize file manager
    fileManager := NewFileManager(config.OutputPath, config.PartitioningScheme, logger)
    
    return &ParquetConsumer{
        config:      config,
        client:      client,
        conn:        conn,
        logger:      logger,
        allocator:   allocator,
        schema:      schema,
        builder:     builder,
        eventBatch:  make([]EventData, 0, config.BatchSize),
        fileManager: fileManager,
        metrics:     &ConsumerMetrics{},
    }, nil
}

func createTTPEventSchema() *arrow.Schema {
    return arrow.NewSchema(
        []arrow.Field{
            {Name: "timestamp", Type: arrow.FixedWidthTypes.Timestamp_us, Nullable: false},
            {Name: "ledger_sequence", Type: arrow.PrimitiveTypes.Uint32, Nullable: false},
            {Name: "transaction_hash", Type: &arrow.FixedSizeBinaryType{ByteWidth: 32}, Nullable: false},
            {Name: "event_type", Type: arrow.BinaryTypes.String, Nullable: false},
            {Name: "source_account", Type: arrow.BinaryTypes.String, Nullable: false},
            {Name: "dest_account", Type: arrow.BinaryTypes.String, Nullable: false},
            {Name: "asset_code", Type: arrow.BinaryTypes.String, Nullable: true},
            {Name: "asset_issuer", Type: arrow.BinaryTypes.String, Nullable: true},
            {Name: "amount", Type: arrow.BinaryTypes.String, Nullable: false},
            {Name: "memo", Type: arrow.BinaryTypes.String, Nullable: true},
        },
        &arrow.Metadata{
            Keys:   []string{"version", "description", "created_by"},
            Values: []string{"1.0.0", "TTP Events Schema for Parquet Storage", "parquet-consumer"},
        },
    )
}
```

### 2. Event Processing and Batching

#### 2.1 Event Stream Processing
```go
func (pc *ParquetConsumer) Start(ctx context.Context, startLedger, endLedger uint32) error {
    pc.logger.Info("starting parquet consumer",
        zap.Uint32("start_ledger", startLedger),
        zap.Uint32("end_ledger", endLedger))

    // Start flush timer
    flushTicker := time.NewTicker(pc.config.FlushInterval)
    defer flushTicker.Stop()

    // Start event processing goroutine
    eventChan := make(chan EventData, pc.config.BatchSize*2)
    errorChan := make(chan error, 1)

    go pc.processEventStream(ctx, startLedger, endLedger, eventChan, errorChan)

    for {
        select {
        case <-ctx.Done():
            pc.logger.Info("context cancelled, stopping consumer")
            return pc.flushAndClose()
            
        case event := <-eventChan:
            if err := pc.addEventToBatch(event); err != nil {
                pc.logger.Error("failed to add event to batch", zap.Error(err))
                continue
            }
            
        case <-flushTicker.C:
            if err := pc.flushBatch(); err != nil {
                pc.logger.Error("failed to flush batch", zap.Error(err))
            }
            
        case err := <-errorChan:
            pc.logger.Error("error in event stream processing", zap.Error(err))
            return err
        }
    }
}

func (pc *ParquetConsumer) processEventStream(ctx context.Context, startLedger, endLedger uint32, 
    eventChan chan<- EventData, errorChan chan<- error) {
    
    request := &eventservice.GetEventsRequest{
        StartLedger: startLedger,
        EndLedger:   endLedger,
    }

    stream, err := pc.client.GetTTPEvents(ctx, request)
    if err != nil {
        errorChan <- fmt.Errorf("failed to get TTP events stream: %w", err)
        return
    }

    for {
        select {
        case <-ctx.Done():
            return
        default:
            event, err := stream.Recv()
            if err == io.EOF {
                pc.logger.Info("event stream ended")
                return
            }
            if err != nil {
                errorChan <- fmt.Errorf("failed to receive event: %w", err)
                return
            }

            eventData := pc.convertProtobufToEventData(event)
            
            select {
            case eventChan <- eventData:
                pc.metrics.mu.Lock()
                pc.metrics.EventsReceived++
                pc.metrics.mu.Unlock()
            case <-ctx.Done():
                return
            }
        }
    }
}

func (pc *ParquetConsumer) convertProtobufToEventData(event *token_transfer.TokenTransferEvent) EventData {
    return EventData{
        Timestamp:       time.Unix(event.Timestamp.Seconds, int64(event.Timestamp.Nanos)),
        LedgerSequence:  event.LedgerSequence,
        TransactionHash: event.TransactionHash,
        EventType:       event.EventType.String(),
        SourceAccount:   event.SourceAccount,
        DestAccount:     event.DestAccount,
        AssetCode:       event.Asset.Code,
        Amount:          event.Amount,
        // Map additional fields as needed
    }
}
```

#### 2.2 Batch Management
```go
func (pc *ParquetConsumer) addEventToBatch(event EventData) error {
    pc.batchMutex.Lock()
    defer pc.batchMutex.Unlock()

    pc.eventBatch = append(pc.eventBatch, event)

    // Check if batch is full
    if len(pc.eventBatch) >= pc.config.BatchSize {
        return pc.flushBatchUnsafe()
    }

    return nil
}

func (pc *ParquetConsumer) flushBatch() error {
    pc.batchMutex.Lock()
    defer pc.batchMutex.Unlock()
    return pc.flushBatchUnsafe()
}

func (pc *ParquetConsumer) flushBatchUnsafe() error {
    if len(pc.eventBatch) == 0 {
        return nil
    }

    startTime := time.Now()
    
    // Convert batch to Arrow Record
    record, err := pc.buildArrowRecord()
    if err != nil {
        return fmt.Errorf("failed to build Arrow record: %w", err)
    }
    defer record.Release()

    // Write to Parquet
    if err := pc.writeToParquet(record); err != nil {
        return fmt.Errorf("failed to write to Parquet: %w", err)
    }

    // Update metrics
    pc.metrics.mu.Lock()
    pc.metrics.BatchesProcessed++
    pc.metrics.EventsWritten += int64(len(pc.eventBatch))
    pc.metrics.ProcessingLatency = time.Since(startTime)
    pc.metrics.LastFlushTime = time.Now()
    pc.metrics.mu.Unlock()

    // Clear batch
    pc.eventBatch = pc.eventBatch[:0]
    pc.lastFlush = time.Now()

    pc.logger.Debug("flushed batch to Parquet",
        zap.Int("event_count", record.NumRows()),
        zap.Duration("processing_time", time.Since(startTime)))

    return nil
}

func (pc *ParquetConsumer) buildArrowRecord() (arrow.Record, error) {
    // Reset builder
    pc.builder.Release()
    pc.builder.Retain()

    // Build arrays for each field
    timestampBuilder := pc.builder.Field(0).(*array.TimestampBuilder)
    ledgerSeqBuilder := pc.builder.Field(1).(*array.Uint32Builder)
    txHashBuilder := pc.builder.Field(2).(*array.FixedSizeBinaryBuilder)
    eventTypeBuilder := pc.builder.Field(3).(*array.StringBuilder)
    sourceAccBuilder := pc.builder.Field(4).(*array.StringBuilder)
    destAccBuilder := pc.builder.Field(5).(*array.StringBuilder)
    assetCodeBuilder := pc.builder.Field(6).(*array.StringBuilder)
    assetIssuerBuilder := pc.builder.Field(7).(*array.StringBuilder)
    amountBuilder := pc.builder.Field(8).(*array.StringBuilder)
    memoBuilder := pc.builder.Field(9).(*array.StringBuilder)

    for _, event := range pc.eventBatch {
        timestampBuilder.Append(arrow.Timestamp(event.Timestamp.UnixMicro()))
        ledgerSeqBuilder.Append(event.LedgerSequence)
        
        // Convert hex hash to bytes
        hashBytes, err := hex.DecodeString(event.TransactionHash)
        if err != nil {
            return nil, fmt.Errorf("failed to decode transaction hash: %w", err)
        }
        txHashBuilder.Append(hashBytes)
        
        eventTypeBuilder.Append(event.EventType)
        sourceAccBuilder.Append(event.SourceAccount)
        destAccBuilder.Append(event.DestAccount)
        
        if event.AssetCode != "" {
            assetCodeBuilder.Append(event.AssetCode)
        } else {
            assetCodeBuilder.AppendNull()
        }
        
        // Handle nullable fields similarly
        amountBuilder.Append(event.Amount)
        memoBuilder.AppendNull() // Add memo handling as needed
    }

    return pc.builder.NewRecord(), nil
}
```

### 3. Parquet File Management

#### 3.1 File Writer Implementation
```go
func (pc *ParquetConsumer) writeToParquet(record arrow.Record) error {
    // Determine output file based on partitioning scheme
    outputPath, err := pc.fileManager.GetOutputPath(record)
    if err != nil {
        return fmt.Errorf("failed to determine output path: %w", err)
    }

    // Check if we need a new file
    if pc.currentFile == nil || pc.shouldRotateFile(outputPath) {
        if err := pc.rotateFile(outputPath); err != nil {
            return fmt.Errorf("failed to rotate file: %w", err)
        }
    }

    // Write record to current file
    table := array.NewTableFromRecords(record.Schema(), []arrow.Record{record})
    defer table.Release()

    props := pc.createWriterProperties()
    arrowProps := pqarrow.DefaultWriterProps()

    if err := pqarrow.WriteTable(table, pc.currentFile.Writer, 
        table.NumRows(), props, arrowProps); err != nil {
        return fmt.Errorf("failed to write table to Parquet: %w", err)
    }

    // Update file metadata
    pc.currentFile.RowCount += int64(record.NumRows())
    pc.currentFile.EndTime = time.Now()

    return nil
}

func (pc *ParquetConsumer) createWriterProperties() *parquet.WriterProperties {
    var compressionCodec compress.Compression
    
    switch pc.config.CompressionCodec {
    case "snappy":
        compressionCodec = compress.Codecs.Snappy
    case "gzip":
        compressionCodec = compress.Codecs.Gzip
    case "lz4":
        compressionCodec = compress.Codecs.Lz4
    case "zstd":
        compressionCodec = compress.Codecs.Zstd
    case "brotli":
        compressionCodec = compress.Codecs.Brotli
    default:
        compressionCodec = compress.Codecs.Snappy
    }

    return parquet.NewWriterProperties(
        parquet.WithCompression(compressionCodec),
        parquet.WithCompressionLevel(pc.config.CompressionLevel),
        parquet.WithVersion(parquet.V2_LATEST),
        parquet.WithDataPageSize(8192),  // 8KB pages
        parquet.WithMaxRowGroupLength(1000000), // 1M rows per group
    )
}

func (pc *ParquetConsumer) shouldRotateFile(newPath string) bool {
    if pc.currentFile == nil {
        return true
    }

    // Rotate if path changed (different partition)
    if pc.currentFile.Path != newPath {
        return true
    }

    // Rotate if file size exceeded
    if pc.currentFile.FileSize > pc.config.MaxFileSize {
        return true
    }

    // Rotate based on time (e.g., hourly rotation)
    if time.Since(pc.currentFile.StartTime) > time.Hour {
        return true
    }

    return false
}

func (pc *ParquetConsumer) rotateFile(newPath string) error {
    // Close current file if exists
    if pc.currentFile != nil && pc.currentFile.Writer != nil {
        if err := pc.currentFile.Writer.Close(); err != nil {
            pc.logger.Error("failed to close current file", zap.Error(err))
        }
        pc.logger.Info("closed parquet file",
            zap.String("path", pc.currentFile.Path),
            zap.Int64("rows", pc.currentFile.RowCount))
    }

    // Create new file
    file, err := os.Create(newPath)
    if err != nil {
        return fmt.Errorf("failed to create file %s: %w", newPath, err)
    }

    writer, err := pqarrow.NewFileWriter(pc.schema, file, 
        pc.createWriterProperties(), pqarrow.DefaultWriterProps())
    if err != nil {
        file.Close()
        return fmt.Errorf("failed to create Parquet writer: %w", err)
    }

    pc.currentFile = &ParquetFile{
        Path:      newPath,
        Writer:    writer,
        RowCount:  0,
        FileSize:  0,
        StartTime: time.Now(),
    }

    pc.metrics.mu.Lock()
    pc.metrics.FilesCreated++
    pc.metrics.mu.Unlock()

    pc.logger.Info("created new parquet file", zap.String("path", newPath))
    
    return nil
}
```

#### 3.2 Partitioning Strategy
```go
type FileManager struct {
    basePath           string
    partitioningScheme string
    logger             *zap.Logger
}

func NewFileManager(basePath, scheme string, logger *zap.Logger) *FileManager {
    return &FileManager{
        basePath:           basePath,
        partitioningScheme: scheme,
        logger:             logger,
    }
}

func (fm *FileManager) GetOutputPath(record arrow.Record) (string, error) {
    // Extract first timestamp for partitioning
    timestampArray := record.Column(0).(*array.Timestamp)
    if timestampArray.Len() == 0 {
        return "", fmt.Errorf("empty record")
    }

    timestamp := time.Unix(0, int64(timestampArray.Value(0))*1000) // Convert microseconds

    switch fm.partitioningScheme {
    case "date":
        return fm.buildDatePartitionPath(timestamp), nil
    case "date_hour":
        return fm.buildDateHourPartitionPath(timestamp), nil
    case "date_hour_type":
        eventType := fm.extractEventType(record)
        return fm.buildDateHourTypePartitionPath(timestamp, eventType), nil
    default:
        return fm.buildSimplePath(timestamp), nil
    }
}

func (fm *FileManager) buildDatePartitionPath(timestamp time.Time) string {
    dateStr := timestamp.Format("2006-01-02")
    filename := fmt.Sprintf("ttp_events_%s_%d.parquet", 
        dateStr, timestamp.Unix())
    return filepath.Join(fm.basePath, "year="+timestamp.Format("2006"),
        "month="+timestamp.Format("01"), "day="+timestamp.Format("02"), filename)
}

func (fm *FileManager) buildDateHourPartitionPath(timestamp time.Time) string {
    dateHourStr := timestamp.Format("2006-01-02T15")
    filename := fmt.Sprintf("ttp_events_%s_%d.parquet", 
        dateHourStr, timestamp.Unix())
    return filepath.Join(fm.basePath, "year="+timestamp.Format("2006"),
        "month="+timestamp.Format("01"), "day="+timestamp.Format("02"),
        "hour="+timestamp.Format("15"), filename)
}

func (fm *FileManager) buildDateHourTypePartitionPath(timestamp time.Time, eventType string) string {
    dateHourStr := timestamp.Format("2006-01-02T15")
    filename := fmt.Sprintf("ttp_events_%s_%s_%d.parquet", 
        dateHourStr, eventType, timestamp.Unix())
    return filepath.Join(fm.basePath, "year="+timestamp.Format("2006"),
        "month="+timestamp.Format("01"), "day="+timestamp.Format("02"),
        "hour="+timestamp.Format("15"), "event_type="+eventType, filename)
}

func (fm *FileManager) extractEventType(record arrow.Record) string {
    eventTypeArray := record.Column(3).(*array.String)
    if eventTypeArray.Len() > 0 {
        return eventTypeArray.Value(0)
    }
    return "unknown"
}
```

### 4. Configuration and Deployment

#### 4.1 Configuration Management
```go
// config.json
{
    "service": {
        "name": "ttp-parquet-consumer",
        "version": "1.0.0"
    },
    "consumer": {
        "ttp_processor_endpoint": "localhost:50051",
        "start_ledger": 0,
        "end_ledger": 0,
        "batch_size": 1000,
        "flush_interval": "30s"
    },
    "output": {
        "output_path": "./data/parquet",
        "partitioning_scheme": "date_hour",
        "max_file_size": 134217728,
        "compression_codec": "snappy",
        "compression_level": 1
    },
    "monitoring": {
        "health_port": 8088,
        "metrics_port": 9090,
        "log_level": "info"
    }
}
```

#### 4.2 Docker Configuration
```dockerfile
# Dockerfile
FROM golang:1.23-alpine AS builder

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=1 GOOS=linux go build -o parquet-consumer ./cmd/consumer

FROM alpine:latest

RUN apk --no-cache add ca-certificates
WORKDIR /root/

COPY --from=builder /app/parquet-consumer .
COPY --from=builder /app/config.json .

CMD ["./parquet-consumer", "-config", "config.json"]
```

#### 4.3 Makefile
```makefile
# Makefile
.PHONY: build test run clean docker-build docker-run

BINARY_NAME=parquet-consumer
DOCKER_IMAGE=ttp-parquet-consumer:latest

build:
	CGO_ENABLED=1 go build -o bin/$(BINARY_NAME) ./cmd/consumer

test:
	go test -v ./...

run: build
	./bin/$(BINARY_NAME) -config config.json

clean:
	rm -rf bin/
	rm -rf data/

docker-build:
	docker build -t $(DOCKER_IMAGE) .

docker-run: docker-build
	docker run -v $(PWD)/data:/root/data $(DOCKER_IMAGE)

install-deps:
	go mod download
	go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest

lint:
	golangci-lint run

benchmark:
	go test -bench=. -benchmem ./...
```

## Performance Considerations

### Memory Management
- **Arrow Memory Pool**: Use memory.NewGoAllocator() for automatic garbage collection
- **Record Release**: Always call record.Release() to prevent memory leaks
- **Batch Size Tuning**: Balance memory usage vs. compression efficiency

### Compression Optimization
- **Snappy**: Fastest compression, good for real-time processing
- **Zstd**: Best compression ratio, suitable for archival storage
- **LZ4**: Good balance of speed and compression

### Partitioning Strategy
- **Date Partitioning**: Enables efficient time-range queries
- **Event Type Partitioning**: Optimizes for event-type specific analytics
- **Hybrid Partitioning**: Combine date and event type for maximum flexibility

## Monitoring and Observability

### Health Checks
```go
func (pc *ParquetConsumer) HealthCheck() map[string]interface{} {
    pc.metrics.mu.RLock()
    defer pc.metrics.mu.RUnlock()
    
    return map[string]interface{}{
        "status": "healthy",
        "events_received": pc.metrics.EventsReceived,
        "events_written": pc.metrics.EventsWritten,
        "files_created": pc.metrics.FilesCreated,
        "last_flush": pc.metrics.LastFlushTime.Format(time.RFC3339),
        "uptime": time.Since(pc.startTime).String(),
    }
}
```

### Prometheus Metrics
```go
var (
    eventsReceived = prometheus.NewCounterVec(
        prometheus.CounterOpts{
            Name: "ttp_events_received_total",
            Help: "Total number of TTP events received",
        },
        []string{"status"},
    )
    
    eventsWritten = prometheus.NewCounterVec(
        prometheus.CounterOpts{
            Name: "ttp_events_written_total",
            Help: "Total number of TTP events written to Parquet",
        },
        []string{"partition"},
    )
    
    processingLatency = prometheus.NewHistogramVec(
        prometheus.HistogramOpts{
            Name:    "ttp_processing_duration_seconds",
            Help:    "Time taken to process and write batches",
            Buckets: prometheus.DefBuckets,
        },
        []string{"operation"},
    )
)
```

## Testing Strategy

### Unit Tests
- Arrow record building and validation
- Parquet file writing and reading
- Partitioning logic verification
- Configuration parsing and validation

### Integration Tests
- End-to-end processing with real TTP events
- File system integration and cleanup
- Error handling and recovery scenarios
- Performance benchmarking

### Load Testing
- High-volume event processing
- Memory usage under sustained load
- File rotation and management
- Network resilience testing

## Deployment Guide

### Prerequisites
- Apache Arrow C++ libraries
- Go 1.23+
- Sufficient disk space for Parquet files
- Network connectivity to ttp-processor service

### Installation Steps
1. Clone repository and install dependencies
2. Build binary with CGO enabled
3. Configure service parameters
4. Set up monitoring and health checks
5. Deploy and verify functionality

### Operational Procedures
- **File Cleanup**: Implement retention policies for old Parquet files
- **Schema Evolution**: Handle schema changes gracefully
- **Backup Strategy**: Regular backup of critical Parquet files
- **Performance Monitoring**: Track throughput and latency metrics

## Conclusion

This implementation plan provides a comprehensive approach to creating a high-performance Parquet consumer for TTP events. The design leverages Apache Arrow for optimal columnar data processing while providing production-ready features like partitioning, compression, monitoring, and error handling.

**Key Benefits:**
- **High Performance**: Arrow-native processing for maximum throughput
- **Efficient Storage**: Parquet compression reduces storage costs by 50-80%
- **Analytics Ready**: Direct compatibility with analytics tools and data lakes
- **Production Features**: Monitoring, health checks, and operational tooling
- **Scalable Architecture**: Supports horizontal scaling and high-volume processing