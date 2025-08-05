# Hybrid Storage Architecture for Stellar Data Platform

A comprehensive guide to implementing a tiered storage architecture that combines real-time Protocol Buffer streaming with long-term Apache Arrow/Parquet archival storage for optimal performance and cost efficiency.

## Architecture Overview

This hybrid approach leverages the strengths of both Protocol Buffers and Apache Arrow by using them for their optimal use cases:

- **Protocol Buffers**: Real-time streaming, single-record processing, low-latency operations
- **Apache Arrow/Parquet**: Batch analytics, historical data, columnar storage, cost-effective archival

## Tiered Storage Strategy

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│ Stellar Node    │───▶│ Contract Data    │───▶│ Real-time       │
│                 │    │ Processor        │    │ PostgreSQL      │
└─────────────────┘    └──────────────────┘    └─────────────────┘
                                │                        │
                                │                        │
                                ▼                        ▼
                       ┌──────────────────┐    ┌─────────────────┐
                       │ Archival         │◀───│ Data Lifecycle  │
                       │ Consumer         │    │ Manager         │
                       └──────────────────┘    └─────────────────┘
                                │
                                ▼
                       ┌──────────────────┐
                       │ Parquet Files    │
                       │ (S3/GCS)         │
                       └──────────────────┘
```

## Data Lifecycle Tiers

### Hot Tier (0-30 days)
- **Storage**: PostgreSQL/ClickHouse
- **Format**: Protocol Buffer → Relational
- **Access Pattern**: High-frequency queries, real-time dashboards
- **Latency**: <100ms
- **Cost**: High per TB

### Warm Tier (30 days - 1 year)
- **Storage**: Compressed database or columnar store
- **Format**: Protocol Buffer → Columnar (optional)
- **Access Pattern**: Regular analytics, reporting
- **Latency**: 1-10 seconds
- **Cost**: Medium per TB

### Cold Tier (1+ years)
- **Storage**: Object storage (S3/GCS/Azure Blob)
- **Format**: Parquet (Apache Arrow)
- **Access Pattern**: Historical analysis, compliance, ML training
- **Latency**: 10+ seconds
- **Cost**: Very low per TB

## Pipeline Architecture

### Dual Consumer Pattern

Your Contract Data Processor can feed multiple consumers simultaneously:

```go
// Contract Data Processor serves multiple consumers
type ContractDataProcessor struct {
    // ... existing fields
    consumers []Consumer
}

type Consumer interface {
    ProcessContractData(data *pb.ContractData) error
}

// Real-time consumer
type PostgreSQLConsumer struct {
    db *sql.DB
}

// Archival consumer
type ArchivalConsumer struct {
    batchSize    int
    currentBatch []*pb.ContractData
    storage      ArchivalStorage
}
```

### Implementation Options

#### Option 1: Parallel Streaming
```
Contract Data Processor
├─── gRPC Stream 1 ───▶ PostgreSQL Consumer
└─── gRPC Stream 2 ───▶ Archival Consumer
```

#### Option 2: Fan-out at Processor
```
Contract Data Processor
├─── Direct Write ────▶ PostgreSQL
└─── Queue Message ──▶ Archival Consumer
```

#### Option 3: CDC from PostgreSQL
```
Contract Data Processor ──▶ PostgreSQL ──▶ Change Data Capture ──▶ Archival Consumer
```

## Archival Consumer Implementation

### Go Implementation

```go
// archival_consumer/main.go
package main

import (
    "context"
    "fmt"
    "log"
    "time"

    "github.com/apache/arrow/go/v17/arrow"
    "github.com/apache/arrow/go/v17/arrow/array"
    "github.com/apache/arrow/go/v17/arrow/memory"
    "github.com/apache/arrow/go/v17/parquet/file"
    "github.com/apache/arrow/go/v17/parquet/pqarrow"
    pb "your-org/ttp-processor-demo/protos"
)

type ArchivalConsumer struct {
    allocator    memory.Allocator
    batchSize    int
    currentBatch []*pb.ContractData
    storage      ArchivalStorage
    transformer  *ArrowTransformer
}

type ArchivalStorage interface {
    WriteParquetBatch(ctx context.Context, batch arrow.Record, partition string) error
}

func NewArchivalConsumer(storage ArchivalStorage, batchSize int) *ArchivalConsumer {
    allocator := memory.NewGoAllocator()
    return &ArchivalConsumer{
        allocator:   allocator,
        batchSize:   batchSize,
        storage:     storage,
        transformer: NewArchivalArrowTransformer(allocator),
    }
}

func (c *ArchivalConsumer) ProcessContractData(data *pb.ContractData) error {
    c.currentBatch = append(c.currentBatch, data)
    
    if len(c.currentBatch) >= c.batchSize {
        return c.flushBatch()
    }
    
    return nil
}

func (c *ArchivalConsumer) flushBatch() error {
    if len(c.currentBatch) == 0 {
        return nil
    }
    
    // Convert to Arrow record
    record, err := c.transformer.TransformBatch(c.currentBatch)
    if err != nil {
        return fmt.Errorf("failed to transform batch: %w", err)
    }
    defer record.Release()
    
    // Determine partition (year/month/day)
    partition := c.getPartition(c.currentBatch[0].ClosedAt)
    
    // Write to storage
    if err := c.storage.WriteParquetBatch(context.Background(), record, partition); err != nil {
        return fmt.Errorf("failed to write batch: %w", err)
    }
    
    log.Printf("Archived batch of %d records to partition %s", len(c.currentBatch), partition)
    
    // Clear batch
    c.currentBatch = c.currentBatch[:0]
    return nil
}

func (c *ArchivalConsumer) getPartition(timestamp int64) string {
    t := time.Unix(timestamp, 0).UTC()
    return fmt.Sprintf("year=%d/month=%02d/day=%02d", t.Year(), t.Month(), t.Day())
}

// Periodic flush for partial batches
func (c *ArchivalConsumer) StartPeriodicFlush(interval time.Duration) {
    ticker := time.NewTicker(interval)
    go func() {
        for range ticker.C {
            if len(c.currentBatch) > 0 {
                if err := c.flushBatch(); err != nil {
                    log.Printf("Error flushing batch: %v", err)
                }
            }
        }
    }()
}
```

### Arrow Transformer for Archival

```go
// archival_transformer.go
package main

import (
    "time"

    "github.com/apache/arrow/go/v17/arrow"
    "github.com/apache/arrow/go/v17/arrow/array"
    "github.com/apache/arrow/go/v17/arrow/memory"
    pb "your-org/ttp-processor-demo/protos"
)

type ArrowTransformer struct {
    allocator memory.Allocator
    schema    *arrow.Schema
}

func NewArchivalArrowTransformer(allocator memory.Allocator) *ArrowTransformer {
    // Optimized schema for analytics and compression
    schema := arrow.NewSchema([]arrow.Field{
        // Core identifiers
        {Name: "contract_id", Type: arrow.BinaryTypes.String, Nullable: false},
        {Name: "ledger_sequence", Type: arrow.PrimitiveTypes.Uint32, Nullable: false},
        {Name: "closed_at", Type: &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}, Nullable: false},
        
        // Contract key information
        {Name: "contract_key_type", Type: arrow.BinaryTypes.String, Nullable: false},
        {Name: "contract_durability", Type: arrow.BinaryTypes.String, Nullable: false},
        
        // Asset information (for analytics)
        {Name: "asset_code", Type: arrow.BinaryTypes.String, Nullable: true},
        {Name: "asset_issuer", Type: arrow.BinaryTypes.String, Nullable: true},
        {Name: "asset_type", Type: arrow.BinaryTypes.String, Nullable: true},
        
        // Balance information
        {Name: "balance_holder", Type: arrow.BinaryTypes.String, Nullable: true},
        {Name: "balance", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
        
        // Metadata
        {Name: "last_modified_ledger", Type: arrow.PrimitiveTypes.Uint32, Nullable: false},
        {Name: "deleted", Type: arrow.FixedWidthTypes.Boolean, Nullable: false},
        
        // Raw data (compressed)
        {Name: "key_xdr", Type: arrow.BinaryTypes.Binary, Nullable: true}, // Binary for compression
        {Name: "val_xdr", Type: arrow.BinaryTypes.Binary, Nullable: true},
        
        // Derived analytics fields
        {Name: "partition_date", Type: &arrow.Date32Type{}, Nullable: false}, // For partition pruning
        {Name: "contract_type", Type: arrow.BinaryTypes.String, Nullable: true}, // stellar_asset, custom, etc.
    }, &arrow.Metadata{})
    
    return &ArrowTransformer{
        allocator: allocator,
        schema:    schema,
    }
}

func (t *ArrowTransformer) TransformBatch(batch []*pb.ContractData) (arrow.Record, error) {
    builders := make([]array.Builder, len(t.schema.Fields()))
    
    // Initialize builders
    builders[0] = array.NewStringBuilder(t.allocator)    // contract_id
    builders[1] = array.NewUint32Builder(t.allocator)    // ledger_sequence
    builders[2] = array.NewTimestampBuilder(t.allocator, &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}) // closed_at
    builders[3] = array.NewStringBuilder(t.allocator)    // contract_key_type
    builders[4] = array.NewStringBuilder(t.allocator)    // contract_durability
    builders[5] = array.NewStringBuilder(t.allocator)    // asset_code
    builders[6] = array.NewStringBuilder(t.allocator)    // asset_issuer
    builders[7] = array.NewStringBuilder(t.allocator)    // asset_type
    builders[8] = array.NewStringBuilder(t.allocator)    // balance_holder
    builders[9] = array.NewInt64Builder(t.allocator)     // balance
    builders[10] = array.NewUint32Builder(t.allocator)   // last_modified_ledger
    builders[11] = array.NewBooleanBuilder(t.allocator)  // deleted
    builders[12] = array.NewBinaryBuilder(t.allocator, arrow.BinaryTypes.Binary) // key_xdr
    builders[13] = array.NewBinaryBuilder(t.allocator, arrow.BinaryTypes.Binary) // val_xdr
    builders[14] = array.NewDate32Builder(t.allocator)   // partition_date
    builders[15] = array.NewStringBuilder(t.allocator)   // contract_type
    
    // Populate data
    for _, record := range batch {
        // Core fields
        builders[0].(*array.StringBuilder).Append(record.ContractId)
        builders[1].(*array.Uint32Builder).Append(record.LedgerSequence)
        builders[2].(*array.TimestampBuilder).Append(arrow.Timestamp(record.ClosedAt * 1000)) // Convert to microseconds
        
        // Contract key info
        builders[3].(*array.StringBuilder).Append(record.ContractKeyType)
        builders[4].(*array.StringBuilder).Append(record.ContractDurability)
        
        // Asset fields (nullable)
        if record.ContractDataAssetCode != "" {
            builders[5].(*array.StringBuilder).Append(record.ContractDataAssetCode)
        } else {
            builders[5].(*array.StringBuilder).AppendNull()
        }
        
        if record.ContractDataAssetIssuer != "" {
            builders[6].(*array.StringBuilder).Append(record.ContractDataAssetIssuer)
        } else {
            builders[6].(*array.StringBuilder).AppendNull()
        }
        
        if record.ContractDataAssetType != "" {
            builders[7].(*array.StringBuilder).Append(record.ContractDataAssetType)
        } else {
            builders[7].(*array.StringBuilder).AppendNull()
        }
        
        // Balance fields
        if record.ContractDataBalanceHolder != "" {
            builders[8].(*array.StringBuilder).Append(record.ContractDataBalanceHolder)
        } else {
            builders[8].(*array.StringBuilder).AppendNull()
        }
        
        if record.ContractDataBalance != "" {
            if balance, err := strconv.ParseInt(record.ContractDataBalance, 10, 64); err == nil {
                builders[9].(*array.Int64Builder).Append(balance)
            } else {
                builders[9].(*array.Int64Builder).AppendNull()
            }
        } else {
            builders[9].(*array.Int64Builder).AppendNull()
        }
        
        // Metadata
        builders[10].(*array.Uint32Builder).Append(record.LastModifiedLedger)
        builders[11].(*array.BooleanBuilder).Append(record.Deleted)
        
        // Raw XDR data (as binary for better compression)
        if record.KeyXdr != "" {
            builders[12].(*array.BinaryBuilder).Append([]byte(record.KeyXdr))
        } else {
            builders[12].(*array.BinaryBuilder).AppendNull()
        }
        
        if record.ValXdr != "" {
            builders[13].(*array.BinaryBuilder).Append([]byte(record.ValXdr))
        } else {
            builders[13].(*array.BinaryBuilder).AppendNull()
        }
        
        // Derived fields for analytics
        partitionDate := time.Unix(record.ClosedAt, 0).UTC()
        daysSinceEpoch := int32(partitionDate.Unix() / 86400) // Days since Unix epoch
        builders[14].(*array.Date32Builder).Append(arrow.Date32(daysSinceEpoch))
        
        // Infer contract type from asset data
        contractType := inferContractType(record)
        builders[15].(*array.StringBuilder).Append(contractType)
    }
    
    // Build arrays
    arrays := make([]arrow.Array, len(builders))
    for i, builder := range builders {
        arrays[i] = builder.NewArray()
        defer arrays[i].Release()
    }
    
    // Create record
    return array.NewRecord(t.schema, arrays, int64(len(batch))), nil
}

func inferContractType(record *pb.ContractData) string {
    if record.ContractDataAssetCode != "" {
        return "stellar_asset"
    }
    if record.ContractInstanceType != "" {
        return "smart_contract"
    }
    return "unknown"
}
```

### S3 Storage Implementation

```go
// s3_storage.go
package main

import (
    "bytes"
    "context"
    "fmt"
    "path"

    "github.com/apache/arrow/go/v17/arrow"
    "github.com/apache/arrow/go/v17/parquet/pqarrow"
    "github.com/aws/aws-sdk-go-v2/aws"
    "github.com/aws/aws-sdk-go-v2/service/s3"
)

type S3ArchivalStorage struct {
    client *s3.Client
    bucket string
    prefix string
}

func NewS3ArchivalStorage(client *s3.Client, bucket, prefix string) *S3ArchivalStorage {
    return &S3ArchivalStorage{
        client: client,
        bucket: bucket,
        prefix: prefix,
    }
}

func (s *S3ArchivalStorage) WriteParquetBatch(ctx context.Context, record arrow.Record, partition string) error {
    // Create in-memory buffer for Parquet data
    var buf bytes.Buffer
    
    // Write Arrow record to Parquet format
    writer, err := pqarrow.NewFileWriter(record.Schema(), &buf, 
        pqarrow.DefaultWriterProps(),
        pqarrow.NewArrowWriterProperties())
    if err != nil {
        return fmt.Errorf("failed to create parquet writer: %w", err)
    }
    
    if err := writer.Write(record); err != nil {
        return fmt.Errorf("failed to write record: %w", err)
    }
    
    if err := writer.Close(); err != nil {
        return fmt.Errorf("failed to close writer: %w", err)
    }
    
    // Upload to S3
    key := path.Join(s.prefix, "contract_data", partition, fmt.Sprintf("batch_%d.parquet", time.Now().Unix()))
    
    _, err = s.client.PutObject(ctx, &s3.PutObjectInput{
        Bucket: aws.String(s.bucket),
        Key:    aws.String(key),
        Body:   bytes.NewReader(buf.Bytes()),
        ContentType: aws.String("application/octet-stream"),
        Metadata: map[string]string{
            "record_count": fmt.Sprintf("%d", record.NumRows()),
            "schema_version": "1.0.0",
        },
    })
    
    return err
}
```

## Data Lifecycle Management

### Automated Data Migration

```go
// lifecycle_manager.go
package main

import (
    "context"
    "database/sql"
    "fmt"
    "log"
    "time"

    _ "github.com/lib/pq"
)

type DataLifecycleManager struct {
    realtimeDB      *sql.DB
    archivalStorage ArchivalStorage
    retentionPeriod time.Duration
}

func NewDataLifecycleManager(db *sql.DB, storage ArchivalStorage, retention time.Duration) *DataLifecycleManager {
    return &DataLifecycleManager{
        realtimeDB:      db,
        archivalStorage: storage,
        retentionPeriod: retention,
    }
}

func (d *DataLifecycleManager) StartLifecycleProcess() {
    // Run daily at 2 AM UTC
    ticker := time.NewTicker(24 * time.Hour)
    go func() {
        for range ticker.C {
            if err := d.migrateOldData(); err != nil {
                log.Printf("Error migrating old data: %v", err)
            }
        }
    }()
}

func (d *DataLifecycleManager) migrateOldData() error {
    cutoffDate := time.Now().Add(-d.retentionPeriod)
    
    // Query old data from PostgreSQL
    rows, err := d.realtimeDB.Query(`
        SELECT contract_id, ledger_sequence, closed_at, contract_key_type, 
               contract_durability, asset_code, asset_issuer, asset_type,
               balance_holder, balance, last_modified_ledger, deleted,
               key_xdr, val_xdr
        FROM contract_data 
        WHERE closed_at < $1
        ORDER BY closed_at
    `, cutoffDate.Unix())
    if err != nil {
        return fmt.Errorf("failed to query old data: %w", err)
    }
    defer rows.Close()
    
    // Batch process and archive
    batchSize := 10000
    batch := make([]*pb.ContractData, 0, batchSize)
    
    consumer := NewArchivalConsumer(d.archivalStorage, batchSize)
    
    for rows.Next() {
        var record pb.ContractData
        var closedAt int64
        
        err := rows.Scan(
            &record.ContractId, &record.LedgerSequence, &closedAt,
            &record.ContractKeyType, &record.ContractDurability,
            &record.ContractDataAssetCode, &record.ContractDataAssetIssuer,
            &record.ContractDataAssetType, &record.ContractDataBalanceHolder,
            &record.ContractDataBalance, &record.LastModifiedLedger, &record.Deleted,
            &record.KeyXdr, &record.ValXdr,
        )
        if err != nil {
            return fmt.Errorf("failed to scan row: %w", err)
        }
        
        record.ClosedAt = closedAt
        
        if err := consumer.ProcessContractData(&record); err != nil {
            return fmt.Errorf("failed to process record: %w", err)
        }
    }
    
    // Flush any remaining records
    if err := consumer.flushBatch(); err != nil {
        return fmt.Errorf("failed to flush final batch: %w", err)
    }
    
    // Delete migrated data from PostgreSQL
    result, err := d.realtimeDB.Exec(`
        DELETE FROM contract_data WHERE closed_at < $1
    `, cutoffDate.Unix())
    if err != nil {
        return fmt.Errorf("failed to delete old data: %w", err)
    }
    
    deletedRows, _ := result.RowsAffected()
    log.Printf("Migrated and deleted %d rows older than %v", deletedRows, cutoffDate)
    
    return nil
}
```

## Query Layer for Historical Data

### Historical Query Service

```go
// historical_query_service.go
package main

import (
    "context"
    "database/sql"
    "fmt"
    "time"

    "github.com/marcboeker/go-duckdb"
)

type HistoricalQueryService struct {
    realtimeDB *sql.DB
    duckDB     *sql.DB
    hotDataCutoff time.Duration
}

func NewHistoricalQueryService(realtimeDB *sql.DB, s3Config S3Config) (*HistoricalQueryService, error) {
    // Initialize DuckDB with S3 extension
    duckDB, err := sql.Open("duckdb", "")
    if err != nil {
        return nil, err
    }
    
    // Configure S3 access
    _, err = duckDB.Exec(fmt.Sprintf(`
        INSTALL httpfs;
        LOAD httpfs;
        SET s3_region='%s';
        SET s3_access_key_id='%s';
        SET s3_secret_access_key='%s';
    `, s3Config.Region, s3Config.AccessKey, s3Config.SecretKey))
    
    return &HistoricalQueryService{
        realtimeDB: realtimeDB,
        duckDB: duckDB,
        hotDataCutoff: 30 * 24 * time.Hour, // 30 days
    }, err
}

func (h *HistoricalQueryService) QueryContractData(ctx context.Context, req *QueryRequest) (*QueryResponse, error) {
    if req.StartDate.After(time.Now().Add(-h.hotDataCutoff)) {
        // Query hot data from PostgreSQL
        return h.queryHotData(ctx, req)
    } else {
        // Query cold data from Parquet files
        return h.queryColdData(ctx, req)
    }
}

func (h *HistoricalQueryService) queryColdData(ctx context.Context, req *QueryRequest) (*QueryResponse, error) {
    // Build S3 path pattern for partitioned data
    s3Pattern := h.buildS3Pattern(req.StartDate, req.EndDate)
    
    query := fmt.Sprintf(`
        SELECT contract_id, ledger_sequence, closed_at, asset_code, balance
        FROM read_parquet('%s')
        WHERE closed_at BETWEEN %d AND %d
    `, s3Pattern, req.StartDate.Unix(), req.EndDate.Unix())
    
    if req.ContractId != "" {
        query += fmt.Sprintf(" AND contract_id = '%s'", req.ContractId)
    }
    
    query += " ORDER BY closed_at LIMIT " + fmt.Sprintf("%d", req.Limit)
    
    rows, err := h.duckDB.QueryContext(ctx, query)
    if err != nil {
        return nil, fmt.Errorf("failed to query parquet data: %w", err)
    }
    defer rows.Close()
    
    return h.scanResults(rows)
}

func (h *HistoricalQueryService) buildS3Pattern(start, end time.Time) string {
    // Build pattern like: s3://bucket/contract_data/year=*/month=*/day=*/*.parquet
    // This could be optimized to only include relevant partitions
    return "s3://stellar-data-lake/contract_data/year=*/month=*/day=*/*.parquet"
}
```

## Configuration and Deployment

### Docker Compose Configuration

```yaml
# docker-compose.archival.yml
version: '3.8'

services:
  contract-data-processor:
    build: ./contract-data-processor
    ports:
      - "50054:50054"  # gRPC
      - "8816:8816"    # Arrow Flight
    environment:
      - SOURCE_ENDPOINT=stellar-live-source:50053
      - ENABLE_DUAL_OUTPUT=true
    depends_on:
      - stellar-live-source
      - postgresql
      - redis

  postgresql-consumer:
    build: ./postgresql-consumer
    environment:
      - PROCESSOR_ENDPOINT=contract-data-processor:50054
      - DATABASE_URL=postgres://user:pass@postgresql:5432/stellar
    depends_on:
      - contract-data-processor
      - postgresql

  archival-consumer:
    build: ./archival-consumer
    environment:
      - PROCESSOR_ENDPOINT=contract-data-processor:50054
      - S3_BUCKET=stellar-data-lake
      - S3_PREFIX=contract_data
      - BATCH_SIZE=10000
      - AWS_REGION=us-west-2
    depends_on:
      - contract-data-processor
    volumes:
      - ~/.aws:/root/.aws:ro  # AWS credentials

  lifecycle-manager:
    build: ./lifecycle-manager
    environment:
      - DATABASE_URL=postgres://user:pass@postgresql:5432/stellar
      - S3_BUCKET=stellar-data-lake
      - RETENTION_DAYS=365
    depends_on:
      - postgresql
    volumes:
      - ~/.aws:/root/.aws:ro

  historical-query-service:
    build: ./historical-query-service
    ports:
      - "8080:8080"
    environment:
      - REALTIME_DB_URL=postgres://user:pass@postgresql:5432/stellar
      - S3_BUCKET=stellar-data-lake
    depends_on:
      - postgresql
    volumes:
      - ~/.aws:/root/.aws:ro

  postgresql:
    image: postgres:15
    environment:
      - POSTGRES_DB=stellar
      - POSTGRES_USER=user
      - POSTGRES_PASSWORD=pass
    volumes:
      - postgres_data:/var/lib/postgresql/data

volumes:
  postgres_data:
```

### Makefile Targets

```makefile
# Makefile additions for archival components

.PHONY: build-archival
build-archival:
	cd archival-consumer && go build -o archival-consumer ./...
	cd lifecycle-manager && go build -o lifecycle-manager ./...
	cd historical-query-service && go build -o historical-query-service ./...

.PHONY: run-archival-pipeline
run-archival-pipeline:
	docker-compose -f docker-compose.yml -f docker-compose.archival.yml up -d

.PHONY: test-archival
test-archival:
	# Test archival consumer
	cd archival-consumer && go test ./...
	# Test lifecycle manager  
	cd lifecycle-manager && go test ./...
	# Test historical queries
	cd historical-query-service && go test ./...

.PHONY: migrate-historical-data
migrate-historical-data:
	./lifecycle-manager --migrate-all --dry-run=false
```

## Benefits and Trade-offs

### Benefits

1. **Cost Optimization**
   - 90%+ cost reduction for historical data storage
   - S3 storage costs ~$0.023/GB/month vs PostgreSQL ~$0.50/GB/month

2. **Performance Optimization**  
   - Hot data queries: <100ms (PostgreSQL)
   - Cold data analytics: 10-60s (Parquet/DuckDB)
   - Columnar compression: 10:1 typical ratio

3. **Scalability**
   - Handle years of blockchain data efficiently
   - No impact on real-time performance
   - Horizontal scaling of query layer

4. **Analytics Capabilities**
   - Standard format for data science tools
   - Integration with Apache Spark, Presto, BigQuery
   - Time-series analytics optimized

### Trade-offs

1. **Complexity**
   - Additional components to maintain
   - Data pipeline orchestration
   - Eventual consistency between tiers

2. **Query Latency**
   - Historical queries are slower
   - Cold start times for complex analytics
   - Partition pruning optimization required

3. **Storage Overhead**
   - Temporary duplication during migration
   - Metadata overhead for small files
   - S3 request costs for frequent access

## Migration Strategy

### Phase 1: Add Archival Consumer (Week 1-2)
- Deploy archival consumer alongside existing PostgreSQL consumer
- Start archiving new data to Parquet files
- Validate data integrity and schema compatibility

### Phase 2: Historical Query Service (Week 3-4)  
- Deploy DuckDB-based query service
- Test performance with sample historical queries
- Integrate with existing dashboards/APIs

### Phase 3: Data Lifecycle Management (Week 5-6)
- Deploy lifecycle manager
- Migrate oldest data first (low risk)
- Monitor storage costs and query performance

### Phase 4: Full Production (Week 7-8)
- Enable automatic data migration
- Update monitoring and alerting
- Document operational procedures

## Monitoring and Observability

### Key Metrics to Track

```go
// Metrics for archival pipeline
var (
    ArchivalBatchesProcessed = prometheus.NewCounterVec(
        prometheus.CounterOpts{
            Name: "archival_batches_processed_total",
            Help: "Number of batches processed by archival consumer",
        },
        []string{"partition", "status"},
    )
    
    ArchivalLatency = prometheus.NewHistogramVec(
        prometheus.HistogramOpts{
            Name: "archival_batch_duration_seconds",
            Help: "Time taken to process archival batches",
        },
        []string{"partition"},
    )
    
    StorageCosts = prometheus.NewGaugeVec(
        prometheus.GaugeOpts{
            Name: "storage_cost_estimate_dollars",
            Help: "Estimated monthly storage costs",
        },
        []string{"tier", "storage_type"},
    )
)
```

This hybrid architecture gives you the best of both worlds: real-time performance with Protocol Buffers for operational workloads, and cost-effective, analytics-optimized storage with Apache Arrow/Parquet for historical data.