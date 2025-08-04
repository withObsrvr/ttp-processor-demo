# Phase 2 Integration Guide - Apache Arrow Implementation

## Overview

Phase 2 enhances the stellar-arrow-source with production-ready features including performance optimization, monitoring, resilience, and extended schemas.

## New Components

### 1. Memory Pool Management (`memory/pool.go`)
- Custom memory allocator with pooling for Arrow allocations
- Memory usage tracking and limits
- Automatic garbage collection when memory pressure is high
- Leak detection for long-lived allocations

**Usage:**
```go
allocator := memory.NewPooledAllocator(1<<30, logger) // 1GB limit
allocator.MonitorMemory(30 * time.Second)
```

### 2. Compression Support (`compression/compressor.go`)
- Zstandard and LZ4 compression for Arrow IPC streams
- Configurable compression levels
- Compression metrics tracking

**Environment Variables:**
- `ARROW_COMPRESSION`: `none`, `zstd`, or `lz4` (default: `none`)
- `ARROW_COMPRESSION_LEVEL`: Compression level (1-22 for zstd)

### 3. Metrics Collection (`metrics/collector.go`)
- Prometheus metrics endpoint on port 9090
- Comprehensive metrics for:
  - Ledgers processed
  - Records streamed
  - Memory usage
  - Processing duration histograms
  - Error counts
  - Active connections

**Access Metrics:**
```
http://localhost:9090/metrics
```

### 4. Parallel Processing (`processing/parallel_processor.go`)
- Multi-worker XDR to Arrow conversion
- Configurable worker pool size
- Batch processing with order preservation
- Performance scaling with CPU cores

**Environment Variables:**
- `PARALLEL_WORKERS`: Number of parallel workers (default: CPU count)

### 5. Checkpoint/Resume (`checkpoint/manager.go`)
- Persistent stream state for resume capability
- Automatic checkpoint saving every 30 seconds
- Metadata storage for custom tracking
- Checkpoint cleanup for old streams

**Environment Variables:**
- `CHECKPOINT_DIR`: Directory for checkpoint files (default: `/tmp/stellar-arrow-checkpoints`)
- `CHECKPOINT_INTERVAL`: Auto-save interval (default: `30s`)

### 6. Error Recovery (`resilience/retry.go`)
- Exponential backoff retry logic
- Circuit breaker pattern for fault tolerance
- Configurable retry policies
- Automatic error classification

**Environment Variables:**
- `MAX_RETRY_ATTEMPTS`: Maximum retry attempts (default: 5)
- `CIRCUIT_BREAKER_THRESHOLD`: Failures before circuit opens (default: 5)

## Extended Arrow Schemas

### Transaction Schema (Enhanced)
New fields in Phase 2:
- Soroban smart contract fields
- Muxed account support
- Resource fees and limits
- Extended preconditions
- Complete memo types

### Operation Schema (New)
Comprehensive operation-level data:
- All Stellar operation types
- Operation-specific fields (nullable based on type)
- Soroban host function details
- Liquidity pool operations
- Sponsorship tracking

### Schema Access
```go
schemaManager := schema.NewSchemaManager()
txSchema := schemaManager.GetTransactionSchema()
opSchema := schemaManager.GetOperationSchema()
```

## Integration Steps

### 1. Update Environment Configuration
```bash
# Memory management
export ARROW_MAX_MEMORY=2147483648  # 2GB

# Compression
export ARROW_COMPRESSION=zstd
export ARROW_COMPRESSION_LEVEL=3

# Parallel processing
export PARALLEL_WORKERS=8

# Checkpointing
export CHECKPOINT_DIR=/var/lib/stellar-arrow/checkpoints
export CHECKPOINT_INTERVAL=1m

# Metrics
export METRICS_PORT=9090
```

### 2. Update Service Startup
The main server now initializes all Phase 2 components automatically:
- Memory pool with monitoring
- Metrics collection server
- Checkpoint manager
- Parallel processor pool

### 3. Monitor Performance
Access Prometheus metrics:
```bash
curl http://localhost:9090/metrics | grep stellar_arrow
```

Key metrics to monitor:
- `stellar_arrow_memory_usage_bytes`
- `stellar_arrow_processing_duration_seconds`
- `stellar_arrow_errors_total`
- `stellar_arrow_records_streamed_total`

### 4. Resume from Checkpoint
Streams automatically resume from the last checkpoint on restart:
```bash
# Checkpoint files stored in:
ls $CHECKPOINT_DIR/stream_*.checkpoint
```

### 5. Production Deployment

#### Resource Requirements
- Memory: 2-4GB recommended (configurable via ARROW_MAX_MEMORY)
- CPU: 4-8 cores for optimal parallel processing
- Disk: 100MB for checkpoints
- Network: High-bandwidth for streaming

#### High Availability
- Circuit breakers prevent cascading failures
- Automatic retries with exponential backoff
- Graceful degradation when upstream services fail
- Health endpoints for load balancer integration

#### Monitoring Setup
```yaml
# Prometheus scrape config
scrape_configs:
  - job_name: 'stellar-arrow-source'
    static_configs:
      - targets: ['localhost:9090']
```

## API Changes

### New Flight Descriptors
- `transactions:<start>:<end>` - Stream transaction-level data
- `operations:<start>:<end>` - Stream operation-level data

### Metadata Headers
- `X-Compression-Type`: Compression algorithm used
- `X-Checkpoint-ID`: Checkpoint identifier for resume
- `X-Batch-Size`: Current batch size configuration

## Performance Tuning

### Memory Optimization
```bash
# For high-throughput scenarios
export ARROW_MAX_MEMORY=8589934592  # 8GB
export ARROW_POOL_MONITOR_INTERVAL=10s
```

### Parallel Processing
```bash
# For CPU-intensive workloads
export PARALLEL_WORKERS=16
export BATCH_SIZE=100  # Process in larger batches
```

### Compression Trade-offs
- `none`: Maximum throughput, highest bandwidth
- `lz4`: Good balance of speed and compression
- `zstd`: Best compression ratio, higher CPU usage

## Troubleshooting

### Memory Issues
Check memory metrics:
```bash
curl -s http://localhost:9090/metrics | grep memory_usage
```

### Checkpoint Recovery
Inspect checkpoint files:
```bash
cat $CHECKPOINT_DIR/stream_*.checkpoint | jq .
```

### Performance Bottlenecks
Enable debug logging:
```bash
export LOG_LEVEL=debug
```

## Migration from Phase 1

1. Update go.mod dependencies
2. Run `go mod download`
3. Set new environment variables
4. Deploy with rolling update
5. Monitor metrics during transition

Phase 2 is fully backward compatible with Phase 1 clients.