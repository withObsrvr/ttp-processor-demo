# Phase 3: Analytics & Parquet Storage

A focused Phase 3 implementation with just the essential features for immediate value.

## New Features

### 1. Real-time Analytics Engine

The analytics engine provides Arrow-native computations on streaming data:

```go
// Create analytics engine
engine := analytics.NewAnalyticsEngine(allocator, logger)

// Set up payment volume aggregation
pvResult, err := ttpAnalytics.AnalyzePaymentVolume(
    time.Hour,     // 1-hour window
    "USDC",       // Filter by asset
)

// Get results
data, err := ttpAnalytics.GetPaymentVolumeResults(pvResult)
fmt.Printf("USDC volume in last hour: %d\n", data.Volumes["USDC"])
```

**Supported Aggregations:**
- Sum, Average, Count, Min, Max
- Time-window based (sliding windows)
- Group by asset, account, or custom fields
- Real-time updates as data streams

### 2. Parquet File Storage

Archive Arrow data to Parquet files for long-term storage and analysis:

```go
// Configure Parquet writer
config := &storage.ParquetConfig{
    BasePath:        "/data/stellar-parquet",
    PartitionBy:     []string{"date"},
    PartitionFormat: "2006/01/02",         // Daily partitions
    MaxFileSize:     256 * 1024 * 1024,    // 256MB files
    Compression:     "zstd",               // Best compression
}

writer := storage.NewParquetWriter(config, allocator, logger)

// Write Arrow records
writer.WriteRecord(ctx, arrowRecord, metadata)

// Or use the archiver for automatic batching
archiver := storage.NewParquetArchiver(writer, 5*time.Minute, 1000, logger)
archiver.Start()
```

**Features:**
- Automatic partitioning by date/ledger range
- Configurable compression (snappy, gzip, lz4, zstd)
- File rotation based on size
- Buffered writing for efficiency

## Configuration

### Environment Variables

```bash
# Analytics
export ANALYTICS_WINDOW_SIZE=1h
export ANALYTICS_BUFFER_SIZE=10000

# Parquet Storage
export PARQUET_BASE_PATH=/data/stellar-parquet
export PARQUET_COMPRESSION=zstd
export PARQUET_MAX_FILE_SIZE=268435456  # 256MB
export PARQUET_PARTITION_FORMAT="2006/01/02"
export PARQUET_ARCHIVE_INTERVAL=5m
```

## Usage Examples

### Payment Flow Analysis

```go
// Analyze payment flows between accounts
engine := analytics.NewAnalyticsEngine(allocator, logger)

// Create aggregation for high-value payments
agg, err := engine.CreateAggregation(
    "high_value_payments",
    analytics.AggregationCount,
    30 * time.Minute,  // 30-minute window
    []string{"source_account", "destination_account"},
    []analytics.Filter{
        {Field: "amount", Operator: analytics.FilterGreaterThan, Value: 1000000000}, // > 100 XLM
    },
)

// Process streaming records
for record := range arrowStream {
    engine.ProcessRecord(record)
}

// Get results
results, err := engine.GetResults("high_value_payments")
```

### Archiving to Parquet

```go
// Set up daily archival
archiver := storage.NewParquetArchiver(
    parquetWriter,
    24 * time.Hour,  // Archive daily
    10000,           // Buffer 10k records
    logger,
)
archiver.Start()

// Add records as they stream
for record := range arrowStream {
    // Process in real-time
    processRecord(record)
    
    // Also archive
    archiver.AddRecord(record)
}
```

### Query Parquet Files

Once data is in Parquet, you can query it with various tools:

```python
# Python with PyArrow
import pyarrow.parquet as pq

# Read partitioned dataset
dataset = pq.ParquetDataset('/data/stellar-parquet')
table = dataset.read()

# Convert to Pandas for analysis
df = table.to_pandas()
daily_volume = df.groupby('asset_code')['amount'].sum()
```

```sql
-- DuckDB can query Parquet directly
SELECT 
    asset_code,
    SUM(amount) as total_volume,
    COUNT(*) as transaction_count
FROM '/data/stellar-parquet/2024/01/15/*.parquet'
WHERE successful = true
GROUP BY asset_code
ORDER BY total_volume DESC;
```

## Performance Considerations

### Analytics Engine
- Window size affects memory usage
- Larger windows = more memory but better insights
- Use filters to reduce data volume
- Group by high-cardinality fields carefully

### Parquet Storage
- Compression reduces file size by 50-80%
- ZSTD gives best compression ratio
- Snappy is fastest but larger files
- Daily partitions work well for most use cases
- 256MB file size balances performance and manageability

## Integration with Existing System

1. **Update main.go to initialize analytics:**
```go
// In main server startup
analyticsEngine := analytics.NewAnalyticsEngine(allocator, logger)
ttpAnalytics := analytics.NewTTPAnalytics(analyticsEngine, logger)

// In DoGet handler
go analyticsEngine.ProcessRecord(record)
```

2. **Add Parquet archival:**
```go
// In main server startup
parquetConfig := loadParquetConfig()
parquetWriter := storage.NewParquetWriter(parquetConfig, allocator, logger)
archiver := storage.NewParquetArchiver(parquetWriter, 5*time.Minute, 1000, logger)
archiver.Start()
defer archiver.Stop()

// In DoGet handler
go archiver.AddRecord(record)
```

## Benefits

1. **Real-time Insights**: Get payment volumes, flow patterns, and trends as data streams
2. **Long-term Storage**: Parquet files are 5-10x smaller than raw data
3. **SQL Analytics**: Query historical data with DuckDB, Spark, or any Parquet-compatible tool
4. **Cost Effective**: Compressed Parquet storage is much cheaper than keeping data in memory
5. **Future Proof**: Parquet is the standard for analytical workloads

## Next Steps

With analytics and Parquet storage in place, you can:
1. Build dashboards showing real-time payment flows
2. Create alerts for unusual payment patterns
3. Generate daily/weekly reports from Parquet data
4. Feed ML models with historical data
5. Provide data exports for customers

This focused Phase 3 gives you the essential analytics capabilities without the complexity of distributed systems or cloud-native features.