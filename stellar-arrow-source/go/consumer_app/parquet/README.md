# Parquet Consumer for Stellar Arrow Source

A dedicated consumer application that connects to the Stellar Arrow Source server and archives data to Parquet files.

## Overview

This consumer:
- Connects to Arrow Flight server
- Streams Stellar ledger data
- Archives to Parquet files with partitioning
- Supports configurable compression and file rotation

## Usage

```bash
# Basic usage
./parquet-consumer <server> <output-path> [start-ledger] [end-ledger]

# Example: Archive ledgers 100000-100100
./parquet-consumer localhost:8815 ./parquet-data 100000 100100

# Stream all ledgers from 100000 onwards
./parquet-consumer localhost:8815 ./parquet-data 100000

# Stream all available ledgers
./parquet-consumer localhost:8815 ./parquet-data
```

## Configuration

Environment variables:
- `PARTITION_FORMAT`: Date format for partitioning (default: "2006/01/02")
- `MAX_FILE_SIZE`: Maximum file size before rotation (default: 256MB)
- `COMPRESSION`: Compression type (default: "zstd")
- `ARCHIVE_INTERVAL_MINUTES`: Archive interval in minutes (default: 5)
- `ARCHIVE_BUFFER_SIZE`: Buffer size before flush (default: 1000)

## Output Structure

```
parquet-data/
├── 2024/01/15/         # Date-based partitions
│   ├── data_20240115_140523.parquet
│   └── data_20240115_141023.parquet
└── default/            # Fallback partition
    └── data_20240115_142000.parquet
```

## Building

```bash
cd consumer_app/parquet
go build -o parquet-consumer main.go
```

## Example: Real-time Archival

```bash
# Start the consumer to archive all new ledgers
COMPRESSION=zstd \
MAX_FILE_SIZE=536870912 \
ARCHIVE_INTERVAL_MINUTES=1 \
./parquet-consumer localhost:8815 /data/stellar-archive
```

## Querying Archived Data

Once data is archived, you can query it with various tools:

### DuckDB
```sql
-- Install DuckDB and query
SELECT COUNT(*) FROM '/data/stellar-archive/*/*.parquet';

SELECT 
    date_trunc('hour', ledger_close_time) as hour,
    COUNT(*) as ledger_count,
    SUM(transaction_count) as total_txns
FROM '/data/stellar-archive/*/*.parquet'
GROUP BY 1
ORDER BY 1;
```

### Python with PyArrow
```python
import pyarrow.parquet as pq

# Read partitioned dataset
dataset = pq.ParquetDataset('/data/stellar-archive')
table = dataset.read()

# Convert to Pandas
df = table.to_pandas()
print(df.describe())
```

### Apache Spark
```scala
val df = spark.read.parquet("/data/stellar-archive")
df.createOrReplaceTempView("ledgers")

spark.sql("""
    SELECT asset_code, SUM(amount) as volume
    FROM ledgers
    WHERE successful = true
    GROUP BY asset_code
""").show()
```

## Performance

- Buffers records for efficient writing
- Automatic file rotation at size limits
- Compression reduces storage by 50-80%
- Partitioning enables efficient queries

## Monitoring

The consumer logs:
- Connection status
- Processing progress (records/second)
- File write statistics
- Any errors or warnings

## Use Cases

1. **Historical Analysis**: Archive ledgers for long-term analysis
2. **Data Lake**: Feed into corporate data lakes
3. **Backup**: Create compressed backups of blockchain data
4. **ETL Pipeline**: First step in data processing pipelines