# Protocol 23 Migration Guide

This document outlines the migration from custom withObsrvr libraries to official Stellar Go SDK for Protocol 23 support.

## Configuration Changes

### Old Configuration (Deprecated)
```bash
# DEPRECATED - These variables are still supported but will be removed
STORAGE_TYPE=GCS|S3|FS
BUCKET_NAME=stellar-datastore
LEDGERS_PER_FILE=64
FILES_PER_PARTITION=10
```

### New Configuration

#### Backend Type Selection
```bash
# Required: Choose backend type
BACKEND_TYPE=CAPTIVE_CORE|RPC|ARCHIVE

# Required for all backends
NETWORK_PASSPHRASE="Public Network; September 2015"
HISTORY_ARCHIVE_URLS="https://history.stellar.org/prd/core-live/core_live_001,https://history.stellar.org/prd/core-live/core_live_002"
```

#### Captive Core Backend (Recommended)
```bash
BACKEND_TYPE=CAPTIVE_CORE
STELLAR_CORE_BINARY_PATH=/usr/bin/stellar-core
STELLAR_CORE_CONFIG_PATH=/etc/stellar/stellar-core.cfg
```

#### RPC Backend
```bash
BACKEND_TYPE=RPC
RPC_ENDPOINT=https://stellar-rpc.example.com
```

#### Archive Backend (Using Official stellar/go/support/datastore)
```bash
BACKEND_TYPE=ARCHIVE
ARCHIVE_STORAGE_TYPE=S3|GCS
ARCHIVE_BUCKET_NAME=stellar-archive-bucket

# Optional: Archive schema configuration
LEDGERS_PER_FILE=64      # Number of ledgers per file
FILES_PER_PARTITION=10   # Number of files per partition

# For S3 backend:
AWS_REGION=us-east-1
S3_ENDPOINT_URL=https://s3.amazonaws.com  # Optional custom endpoint
S3_FORCE_PATH_STYLE=false                # Optional for MinIO/custom S3
```

## Backward Compatibility

The system automatically detects old configuration and migrates it:
- `STORAGE_TYPE=GCS|S3|FS` → `BACKEND_TYPE=ARCHIVE` (with fallback to RPC)
- `BUCKET_NAME` → `ARCHIVE_BUCKET_NAME`
- Deprecated variables are warned about but ignored

## Protocol 23 Features

- **Dual BucketList Support**: Handles live state and hot archive separation
- **Automatic Archive Restoration**: CAP-66 support for archived entries
- **Parallel Transaction Processing**: CAP-63 support
- **Enhanced Monitoring**: Protocol 23 specific metrics and validation

## Migration Steps

1. **Update Environment Variables**: Replace old config with new backend-specific config
2. **Choose Backend Type**: Start with CAPTIVE_CORE for best performance
3. **Test Configuration**: Verify health endpoint shows correct backend type
4. **Monitor Metrics**: Check logs for Protocol 23 validation events

## Health Check Updates

The health endpoint (`/health`) now includes:
- Backend type information
- Protocol 23 support status
- Migration warnings for deprecated config

## Performance Notes

- **Captive Core**: Best performance, requires stellar-core binary
- **RPC**: Good for development, depends on RPC server availability  
- **Archive**: Uses official stellar/go/support/datastore for historical data processing
  - Supports parallel processing with configurable buffer size
  - Optimized for cloud storage (S3, GCS)
  - Automatic retry logic with exponential backoff

## Implementation Details

### Official Library Usage

The implementation now uses **100% official Stellar Go SDK components**:

- ✅ `github.com/stellar/go/ingest/ledgerbackend` - LedgerBackend interface
- ✅ `github.com/stellar/go/support/datastore` - Cloud storage abstraction  
- ✅ `github.com/stellar/go/xdr` - Protocol 23 compatible XDR handling
- ❌ Removed all `withObsrvr/*` custom forks

### Archive Backend Features

The archive backend uses `BufferedStorageBackend` with:
- **Parallel Processing**: 100 ledger buffer with 5 worker goroutines
- **Retry Logic**: 3 retries with 1-second wait between attempts
- **Schema Support**: Configurable ledgers per file and partition structure
- **Cloud Native**: Direct integration with S3 and GCS APIs