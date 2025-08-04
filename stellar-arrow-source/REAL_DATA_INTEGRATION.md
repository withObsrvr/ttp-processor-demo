# Real Data Integration Guide

This guide explains how to connect stellar-arrow-source to real Stellar ledger data instead of mock data.

## Overview

The stellar-arrow-source has been updated to support both mock data (for testing) and real data from existing Stellar data sources. The implementation includes:

1. **Real gRPC Client** (`RealArrowSourceClient`) - Connects to stellar-live-source-datalake
2. **XDR to Arrow Conversion** - Converts raw Stellar XDR data to native Arrow format
3. **Efficient Batching** - Batches multiple ledgers into single Arrow records
4. **Configuration-based Selection** - Choose between mock and real data via environment variables

## Architecture

```
Stellar Data Source → stellar-arrow-source → Arrow Flight Clients
(stellar-live-source-datalake)     ↓
                              Native Arrow Records
                                   ↓
                              Zero-copy streaming
```

## Configuration

### Environment Variables

```bash
# Required: Stellar data source endpoint
export SOURCE_ENDPOINT="localhost:8080"  # stellar-live-source-datalake address

# Optional: Use mock data (default: false for real data)
export MOCK_DATA=false

# Standard stellar-arrow-source configuration
export PORT=8815
export HEALTH_PORT=8088
export NETWORK_PASSPHRASE="Test SDF Network ; September 2015"
export BATCH_SIZE=1000
export FLOWCTL_ENABLED=false
```

### Data Source Options

**Recommended: stellar-live-source-datalake**
- More backend options (RPC, Archive, Captive Core)
- Uses official Stellar Go SDK
- Better for production workloads
- Same gRPC interface as stellar-live-source

**Alternative: stellar-live-source**
- Simple RPC-based implementation
- Good for development and testing
- Limited to RPC endpoints only

## Setup Instructions

### Step 1: Start Stellar Data Source

**Option A: stellar-live-source-datalake**
```bash
cd stellar-live-source-datalake

# Configure backend (choose one):

# For RPC backend:
export BACKEND_TYPE=RPC
export RPC_ENDPOINT="https://horizon-testnet.stellar.org"

# For Archive backend (GCS):
export BACKEND_TYPE=ARCHIVE
export ARCHIVE_STORAGE_TYPE=GCS
export ARCHIVE_BUCKET_NAME="your-bucket"
export ARCHIVE_PATH="landing/ledgers/testnet"

# For Captive Core backend:
export BACKEND_TYPE=CAPTIVE_CORE
export STELLAR_CORE_BINARY_PATH="/path/to/stellar-core"
export NETWORK_PASSPHRASE="Test SDF Network ; September 2015"

# Start the service
make run
```

**Option B: stellar-live-source**
```bash
cd stellar-live-source

# Configure RPC endpoint
export RPC_ENDPOINT="https://horizon-testnet.stellar.org"
export NETWORK_PASSPHRASE="Test SDF Network ; September 2015"

# Start the service
make run
```

### Step 2: Configure stellar-arrow-source

```bash
cd stellar-arrow-source

# Configure to use real data
export MOCK_DATA=false
export SOURCE_ENDPOINT="localhost:8080"  # Data source address
export PORT=8815
export HEALTH_PORT=8088

# Start stellar-arrow-source
make run
```

### Step 3: Test the Integration

```bash
# Check health
curl http://localhost:8088/health

# Test with Arrow Flight client (example using Python)
python3 -c "
import pyarrow.flight as flight

client = flight.FlightClient('grpc://localhost:8815')
descriptor = flight.FlightDescriptor.for_command(b'stellar_ledger:100:200')
flight_info = client.get_flight_info(descriptor)

for batch in client.do_get(flight_info.endpoints[0].ticket):
    table = batch.to_table()
    print(f'Received {len(table)} ledger records')
    print('Schema:', table.schema)
    break
"
```

## Data Conversion Details

### XDR to Arrow Mapping

The `XDRBatchBuilder` converts Stellar `LedgerCloseMeta` XDR to Arrow format:

| Arrow Field | Type | Source |
|-------------|------|--------|
| ledger_sequence | uint32 | `ledgerHeader.LedgerSeq` |
| ledger_close_time | timestamp[us] | `ledgerHeader.ScpValue.CloseTime` |
| ledger_hash | fixed_size_binary[32] | `ledgerHeader.LedgerHash` |
| previous_ledger_hash | fixed_size_binary[32] | `ledgerHeader.PreviousLedgerHash` |
| transaction_count | uint32 | Counted from TxSet |
| operation_count | uint32 | Counted from transaction results |
| successful_transaction_count | uint32 | Filtered from TxProcessing |
| failed_transaction_count | uint32 | Filtered from TxProcessing |
| protocol_version | uint32 | `ledgerHeader.LedgerVersion` |
| base_fee | uint32 | `ledgerHeader.BaseFee` |
| base_reserve | uint32 | `ledgerHeader.BaseReserve` |
| max_tx_set_size | uint32 | `ledgerHeader.MaxTxSetSize` |
| close_time_resolution | uint32 | Default: 1 |
| ledger_close_meta_xdr | binary | Original XDR bytes |

### Supported LedgerCloseMeta Versions

- **V0**: Basic ledger metadata
- **V1**: Enhanced with transaction sets  
- **V2**: Protocol 23+ with additional fields

## Performance Characteristics

### Batching Strategy

- **Batch Size**: 100 ledgers per Arrow record (configurable)
- **Memory Efficiency**: Zero-copy operations where possible
- **Streaming**: Continuous processing without buffering entire datasets

### Expected Performance

- **Throughput**: 200+ ledgers/second
- **Latency**: <100ms for stream initiation
- **Memory**: Efficient Arrow memory management
- **Network**: gRPC streaming with backpressure handling

## Monitoring and Health Checks

### Health Endpoints

```bash
# Overall health
curl http://localhost:8088/health

# Readiness check
curl http://localhost:8088/ready

# Prometheus metrics
curl http://localhost:8088/metrics
```

### Logging

The service uses structured logging with the following operations:

- `real_source_client_init` - Client initialization
- `stellar_grpc_stream` - Stream connection
- `arrow_batch_sent` - Batch processing
- `real_stream_cancelled` - Stream termination

### Statistics

Monitor conversion statistics:
```bash
# Check health endpoint for detailed stats
curl http://localhost:8088/health | jq '.source_stats'
```

## Troubleshooting

### Common Issues

**Connection Failed**
```
Error: failed to connect to stellar source
```
- Check if stellar-live-source-datalake is running
- Verify SOURCE_ENDPOINT configuration
- Check network connectivity

**XDR Parsing Errors**
```
Error: failed to unmarshal XDR for ledger X
```
- Check if data source is providing valid XDR
- Verify network passphrase matches
- Check for Protocol version compatibility

**Memory Issues**
```
Error: arrow memory allocation failed
```
- Reduce batch size with BATCH_SIZE environment variable
- Monitor memory usage in health endpoints
- Check for Arrow record leaks

### Debug Mode

```bash
# Enable debug logging
export LOG_LEVEL=debug

# Use mock data for testing
export MOCK_DATA=true

# Check specific ledger ranges
export SOURCE_ENDPOINT="localhost:8080"
# Test with: stellar_ledger:100:110 (small range)
```

## Migration from Mock Data

1. **Test with Mock First**: Verify Arrow Flight functionality works
2. **Start Data Source**: Ensure stellar-live-source-datalake is operational
3. **Switch to Real Data**: Set `MOCK_DATA=false`
4. **Monitor Performance**: Watch health endpoints and logs
5. **Validate Data**: Compare Arrow records with expected ledger data

## Next Steps

This integration enables:

- **Phase 2**: Native Arrow TTP processing
- **Phase 3**: Parquet analytics integration  
- **Phase 4**: Production deployment with real-time streaming

## Support

For issues with real data integration:

1. Check stellar-live-source-datalake logs
2. Verify gRPC connectivity 
3. Monitor conversion statistics
4. Review XDR parsing errors
5. Check Arrow memory usage