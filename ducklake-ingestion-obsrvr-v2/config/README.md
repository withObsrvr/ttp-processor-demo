# V2 Configuration Files

This directory contains configuration files for ducklake-ingestion-obsrvr-v2.

## Running V2

### Mainnet (Production):
```bash
cd /home/tillman/Documents/ttp-processor-demo/ducklake-ingestion-obsrvr-v2
./ducklake-ingestion-obsrvr -config config/mainnet-large-ingest.secret.yaml
```

### Testnet (Testing):
```bash
cd /home/tillman/Documents/ttp-processor-demo/ducklake-ingestion-obsrvr-v2
./ducklake-ingestion-obsrvr -config config/testnet-large-ingest.secret.yaml
```

## Configuration Files

### Production Configs (secret):
- `mainnet-large-ingest.secret.yaml` - Mainnet continuous ingestion
- `testnet-large-ingest.secret.yaml` - Testnet continuous ingestion

These files are `.secret.yaml` to match V1 naming convention (not checked into git).

### Test Configs (included):
- `mainnet-v2.yaml` - Mainnet testing config (used for validation)
- `testnet-v2.yaml` - Testnet testing config (used for validation)

## V2 vs V1 Configuration Differences

### Performance Improvements Allow Larger Batches:

| Config | V1 Batch Size | V2 Batch Size | Reason |
|--------|---------------|---------------|--------|
| **Mainnet** | 25 ledgers | 100 ledgers | V2 is 534x faster, can handle 4x larger batches |
| **Testnet** | 200 ledgers | 200 ledgers | Already optimal for low-activity testnet |

### Timing Differences:

**V1 Mainnet** (25 ledgers):
- Flush time: ~120 seconds
- Total time: ~126 seconds
- Comment: "~10-12 min flush, within 15-min timeout"

**V2 Mainnet** (100 ledgers):
- Flush time: ~1 second (for 100 ledgers!)
- Total time: ~4 seconds
- No timeout needed - completes in milliseconds

## Prerequisites

Before running, ensure:

1. **PostgreSQL is running** at custom socket path:
   ```bash
   /home/tillman/Documents/obsrvr-console/.postgres-sockets
   ```

2. **Database exists**:
   ```sql
   -- For mainnet:
   CREATE DATABASE ducklake_obsrvr_unified;

   -- Schemas are created automatically by V2
   ```

3. **stellar-live-source-datalake is running**:
   - Mainnet: `localhost:50056`
   - Testnet: `localhost:50053`

## Updating Start Ledger

Edit the config file and update `start_ledger`:

```yaml
source:
  start_ledger: 59781703  # <-- Update this to your current ledger
```

To find current ledger:
- Mainnet: https://horizon.stellar.org/ledgers?order=desc&limit=1
- Testnet: https://horizon-testnet.stellar.org/ledgers?order=desc&limit=1

## Monitoring Performance

V2 logs show detailed timing for each batch:

```
[FLUSH] ✅ COMPLETE: Flushed 100 ledgers, 26500 transactions, 83800 operations, 28650 balances in 1.2s total
```

Key metrics:
- **Ledgers/sec**: Should be 20-40 (limited by network, not processing)
- **Flush time**: Should be < 2 seconds for 100 mainnet ledgers
- **Quality checks**: Should be ~30ms and all passing (or 18/19 for mainnet)

## Troubleshooting

### Error: "database does not exist"
Create the PostgreSQL database:
```bash
psql -h /home/tillman/Documents/obsrvr-console/.postgres-sockets postgres
CREATE DATABASE ducklake_obsrvr_unified;
```

### Error: "failed to connect to source"
Start stellar-live-source-datalake for the appropriate network.

### Slow performance (> 5 seconds per batch)
Check:
- PostgreSQL catalog latency
- Disk I/O for Parquet files
- Network latency to stellar-live-source-datalake

Should still be 20-50x faster than V1!

## Comparison to V1

You can run V1 and V2 side-by-side (different binary paths) but:
- ⚠️ **DO NOT run both on same catalog simultaneously**
- V2 writes to same tables as V1
- Use separate catalogs for testing if needed

V1 location:
```bash
/home/tillman/Documents/ttp-processor-demo/ducklake-ingestion-obsrvr/
```

V2 location:
```bash
/home/tillman/Documents/ttp-processor-demo/ducklake-ingestion-obsrvr-v2/
```

## Cloud Deployment (Future)

For B2/S3 storage, add credentials to config:

```yaml
ducklake:
  aws_access_key_id: "YOUR_KEY_ID"
  aws_secret_access_key: "YOUR_SECRET"
  aws_region: "us-west-002"
  aws_endpoint: "s3.us-west-002.backblazeb2.com"
  data_path: "s3://your-bucket/ducklake-data"
```

V2 Appender API works with B2/S3 just like V1.
