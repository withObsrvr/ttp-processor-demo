# Phase 3 Final: Proper Consumer Architecture

## Overview

Phase 3 has been properly restructured with the correct architecture:
- **Server**: Provides Arrow Flight streaming with optional analytics for monitoring
- **Consumers**: Separate applications for specific use cases (Parquet archival, real-time analytics)

## Architecture

```
┌─────────────────────────┐
│ Stellar Arrow Source    │
│ - Arrow Flight Server   │
│ - Optional Analytics    │
│   (for monitoring only) │
└───────────┬─────────────┘
            │ Arrow Flight Protocol
    ┌───────┴───────┬─────────────┬──────────────┐
    ▼               ▼             ▼              ▼
┌─────────┐  ┌──────────┐  ┌──────────┐  ┌─────────────┐
│ Parquet │  │Analytics │  │   TTP    │  │   Custom    │
│Consumer │  │ Consumer │  │Processor │  │  Consumer   │
└─────────┘  └──────────┘  └──────────┘  └─────────────┘
```

## What Was Completed

### 1. Server Cleanup
- Removed Parquet storage from server
- Kept analytics engine for optional server monitoring
- Server now focuses solely on streaming Arrow data

### 2. Parquet Consumer (`consumer_app/parquet/`)
A dedicated consumer application that:
- Connects to Arrow Flight server
- Streams ledger data
- Archives to Parquet files with partitioning
- Supports compression and file rotation

**Usage:**
```bash
./parquet-consumer localhost:8815 ./parquet-data 100000 100100
```

### 3. Analytics Consumer (`consumer_app/analytics/`)
A real-time analytics application that:
- Connects to Arrow Flight server
- Performs streaming analytics
- Supports multiple analysis types
- Displays results in real-time

**Usage:**
```bash
./analytics-consumer localhost:8815 payment-volume 60
```

## Running the System

### 1. Start the Arrow Source Server
```bash
MOCK_DATA=false \
SOURCE_ENDPOINT=localhost:50053 \
BATCH_SIZE=100 \
ARROW_PORT=8815 \
ENABLE_ANALYTICS=false \
./stellar-arrow-source
```

### 2. Run Consumers as Needed

**For Parquet Archival:**
```bash
cd consumer_app/parquet
go build -o parquet-consumer main.go
./parquet-consumer localhost:8815 /data/stellar-archive
```

**For Real-time Analytics:**
```bash
cd consumer_app/analytics
go build -o analytics-consumer main.go
./analytics-consumer localhost:8815 payment-volume 60
```

## Benefits of This Architecture

1. **Separation of Concerns**: Server focuses on data delivery, consumers handle specific use cases
2. **Scalability**: Multiple consumers can connect to the same server
3. **Flexibility**: Easy to add new consumer types without modifying the server
4. **Resource Efficiency**: Only run the consumers you need
5. **Maintainability**: Each component has a single responsibility

## Consumer Patterns

### Parquet Consumer Pattern
- Buffers records for efficient writing
- Rotates files based on size
- Partitions by date for query efficiency
- Compresses data for storage savings

### Analytics Consumer Pattern
- Maintains sliding time windows
- Aggregates data in real-time
- Filters based on criteria
- Displays periodic updates

### Creating New Consumers
Follow these patterns:
1. Connect to Arrow Flight server
2. Create appropriate ticket for data range
3. Process records as they stream
4. Handle graceful shutdown

## Configuration

### Server Configuration
```bash
ARROW_PORT=8815                    # Arrow Flight port
SOURCE_ENDPOINT=localhost:50053    # Stellar data source
BATCH_SIZE=100                     # Records per batch
ENABLE_ANALYTICS=false             # Server monitoring
```

### Parquet Consumer Configuration
```bash
PARTITION_FORMAT=2006/01/02        # Date partition format
MAX_FILE_SIZE=268435456            # 256MB files
COMPRESSION=zstd                   # Compression type
ARCHIVE_INTERVAL_MINUTES=5         # Archive frequency
```

### Analytics Consumer Configuration
Built into command line:
- Analysis type (payment-volume, high-value, account-activity)
- Window size in minutes

## Querying Archived Data

Once Parquet files are created:

```sql
-- DuckDB
SELECT 
    date_trunc('day', ledger_close_time) as day,
    asset_code,
    SUM(amount) as daily_volume
FROM '/data/stellar-archive/*/*.parquet'
WHERE successful = true
GROUP BY 1, 2
ORDER BY 1, 2;
```

## Next Steps

1. **Add More Consumers**:
   - Elasticsearch indexer
   - Kafka publisher
   - PostgreSQL writer
   - S3 uploader

2. **Enhance Existing Consumers**:
   - Add more analytics types
   - Support multiple partitioning strategies
   - Implement data retention policies

3. **Production Features**:
   - Consumer authentication
   - Rate limiting
   - Metrics and monitoring
   - Error recovery

## Summary

Phase 3 now follows the correct architectural pattern where the server provides data and consumers handle specific use cases. This makes the system more flexible, scalable, and maintainable. The Parquet and analytics functionality is now properly isolated in dedicated consumer applications that can be run independently based on needs.