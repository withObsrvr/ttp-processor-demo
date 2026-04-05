# stellar-history-loader — Developer Handoff

## What Was Built

A parallel batch tool for processing Stellar blockchain history into Parquet/DuckLake/PostgreSQL at 100x the speed of the current streaming ingester.

### Architecture

```
Data Source (GCS/S3/RPC/XDR/FS)
    ↓
Orchestrator (splits range into N shards)
    ↓
N Workers (parallel goroutines)
    ↓ per worker:
    Prefetch Buffer (200 ledgers ahead)
    ↓
    XDR Decode (ONCE per ledger)
    ↓
    19 Parallel Extractors (goroutines)
    ↓
    Batch Accumulate → Parquet Writer
    ↓
Silver Transforms (DuckDB SQL over Parquet)
    ↓
DuckLake Push (Parquet → B2 cold storage + PostgreSQL catalog)
    ↓
Hot Buffer Load (tail N ledgers → PostgreSQL via COPY)
```

### Files (21 Go files, ~8,400 lines)

| File | Purpose |
|------|---------|
| `main.go` | CLI entry point, flag parsing |
| `orchestrator.go` | Shard splitting, worker lifecycle, progress reporting |
| `worker.go` | Pipeline: prefetch → decode → extract → write |
| `types.go` | 19 data types + LedgerMeta, BatchData |
| `extractors_core.go` | Transactions, operations, effects, trades |
| `extractors_ledger.go` | Ledger headers and metadata |
| `extractors_accounts.go` | Accounts, trustlines, signers, native balances |
| `extractors_market.go` | Offers, claimable balances, liquidity pools |
| `extractors_state.go` | Config settings, TTL, evicted keys |
| `extractors_soroban.go` | Contract events, data, code, restored keys, creations |
| `scval_converter.go` | Soroban ScVal → JSON decoder |
| `parquet_writer.go` | Generic Parquet writer with 19 table schemas |
| `silver.go` | DuckDB SQL transforms for silver layer |
| `source_archive.go` | GCS/S3 backend via BufferedStorageBackend |
| `source_rpc.go` | Stellar RPC backend |
| `ducklake.go` | DuckLake push (Parquet → B2 + PostgreSQL catalog) |
| `hot_buffer.go` | Hot buffer population (tail ledgers → PostgreSQL) |
| `checkpoint.go` | Per-shard checkpoint/resume |
| `validate.go` | Quality validation checks |
| `lineage.go` | Data lineage tracking |
| `catalog.go` | Output report with DuckDB query examples |

### Data Sources

| Type | Flag | Description |
|------|------|-------------|
| `GCS` | `-storage-type GCS -bucket path` | Google Cloud Storage archive |
| `S3` | `-storage-type S3 -bucket path` | Amazon S3 archive |
| `RPC` | `-storage-type RPC -bucket url` | Stellar RPC endpoint |
| `XDR` | `-storage-type XDR -bucket file` | nebu fetch output |
| `FS` | `-storage-type FS -bucket dir` | Individual XDR files |

**Important:** For GCS/S3, the `-bucket` flag must include the full path to the ledger data directory within the bucket, not just the bucket name.

Examples:
- Testnet: `-bucket "obsrvr-stellar-ledger-data-testnet-data/landing/ledgers/testnet"`
- Pubnet: `-bucket "obsrvr-stellar-ledger-data-pubnet-data/landing/ledgers/pubnet"`

The archive layout uses hex-complement naming: `FFFFFFFF--0-63999/FFFFFFFD--2.xdr.zst`. Check the `.config.json` in the ledger directory for `ledgersPerBatch` and `batchesPerPartition` values — these map to `-ledgers-per-file` and `-files-per-partition`.

### Test Results

**1,001 mainnet ledgers from GCS (obsrvr pubnet bucket):**
- 67.3 ledgers/sec with 8 workers
- 5.9M bronze rows across 16 tables
- 146K silver rows across 6 tables
- 15 seconds total (bronze + silver)
- 345 MB Parquet output

**~900K testnet ledgers from GCS (1 worker, local machine):**
- ~85-90 ledgers/sec sustained
- ~2.8 hours for 900K ledgers

### Usage

```bash
# Build
cd obsrvr-lake/stellar-history-loader
make build
```

#### Bronze extraction only (Parquet output)

```bash
./bin/stellar-history-loader \
  -start 3 -end 900000 \
  -workers 1 -batch-size 1000 \
  -output /tmp/history-output \
  -storage-type GCS \
  -bucket "obsrvr-stellar-ledger-data-testnet-data/landing/ledgers/testnet" \
  -ledgers-per-file 1 -files-per-partition 64000
```

#### Full pipeline: Bronze + DuckLake cold + PostgreSQL hot

```bash
./bin/stellar-history-loader \
  -start 3 -end 900000 \
  -workers 1 -batch-size 1000 \
  -output /tmp/history-output \
  -storage-type GCS \
  -bucket "obsrvr-stellar-ledger-data-testnet-data/landing/ledgers/testnet" \
  -ledgers-per-file 1 -files-per-partition 64000 \
  -pg-host obsrvr-lake-hot-buffer-prod-do-user-13721579-0.g.db.ondigitalocean.com \
  -pg-port 25060 \
  -pg-user doadmin \
  -pg-password <password> \
  -pg-database stellar_hot \
  -pg-sslmode require \
  -ducklake \
  -ducklake-catalog "postgresql://doadmin:<password>@obsrvr-lake-catalog-prod-do-user-13721579-0.g.db.ondigitalocean.com:25060/obsrvr_lake_catalog_prod?sslmode=require" \
  -ducklake-data-path "s3://obsrvr-lake-cold-prod" \
  -b2-key-id "<b2-key-id>" \
  -b2-key-secret "<b2-key-secret>" \
  -tail-ledgers 0
```

#### Pubnet from GCS

```bash
./bin/stellar-history-loader \
  -start 61790000 -end 61791000 \
  -workers 8 -batch-size 50 \
  -output /data/output \
  -storage-type GCS \
  -bucket "obsrvr-stellar-ledger-data-pubnet-data/landing/ledgers/pubnet" \
  -ledgers-per-file 1 -files-per-partition 64000 \
  -network-passphrase "Public Global Stellar Network ; September 2015" \
  -silver
```

#### From nebu fetch output

```bash
nebu fetch 61790000 61791000 --output ledgers.xdr
./bin/stellar-history-loader \
  -start 61790000 -end 61791000 \
  -workers 8 \
  -output /data/output \
  -storage-type XDR -bucket ledgers.xdr \
  -network-passphrase "Public Global Stellar Network ; September 2015" \
  -silver
```

#### Query output with DuckDB

```bash
duckdb -c "SELECT count(*) FROM read_parquet('/data/output/bronze/**/*.parquet')"
```

### Key Flags Reference

| Flag | Default | Description |
|------|---------|-------------|
| `-start` | (required) | Start ledger sequence |
| `-end` | (required) | End ledger sequence |
| `-output` | (required) | Output directory for Parquet files |
| `-workers` | CPU count | Number of parallel workers |
| `-batch-size` | 1000 | Ledgers per extraction batch |
| `-storage-type` | `FS` | Backend: GCS, S3, FS, XDR, RPC |
| `-bucket` | (required) | Full bucket path including sub-directory |
| `-ledgers-per-file` | 1 | Must match archive's `ledgersPerBatch` |
| `-files-per-partition` | 64000 | Must match archive's `batchesPerPartition` |
| `-network-passphrase` | testnet | Stellar network passphrase |
| `-silver` | false | Run silver transforms after extraction |
| `-validate` | false | Run quality validation checks |
| `-ducklake` | false | Push Parquet to DuckLake (B2 + catalog) |
| `-ducklake-catalog` | | PostgreSQL DSN for DuckLake catalog |
| `-ducklake-data-path` | | S3/B2 bucket path (e.g., `s3://obsrvr-lake-cold-prod`) |
| `-ducklake-metadata-schema` | `bronze_meta` | DuckLake metadata schema name |
| `-b2-key-id` | | B2/S3 access key ID (required with `-ducklake`) |
| `-b2-key-secret` | | B2/S3 secret key (required with `-ducklake`) |
| `-b2-endpoint` | `s3.us-west-004.backblazeb2.com` | B2/S3 endpoint |
| `-b2-region` | `us-west-004` | B2/S3 region |
| `-pg-host` | | PostgreSQL host (enables hot buffer loading) |
| `-pg-port` | 25060 | PostgreSQL port |
| `-pg-database` | `stellar_hot` | PostgreSQL database name |
| `-pg-user` | | PostgreSQL user |
| `-pg-password` | | PostgreSQL password |
| `-pg-sslmode` | `require` | PostgreSQL SSL mode |
| `-tail-ledgers` | 100000 | Recent ledgers to load into hot buffer (0 = all) |

### Splitting Large Ranges

For large history loads, split into multiple jobs. The checkpoint system allows resume within each job, but splitting prevents memory issues with very large ranges:

```bash
# Job 1: first half
./bin/stellar-history-loader -start 3 -end 900000 -workers 1 -output /tmp/job1 ...

# Job 2: second half
./bin/stellar-history-loader -start 900001 -end 1879604 -workers 1 -output /tmp/job2 ...
```

### Post-Load: Streaming Pipeline Handoff

After the history loader completes, use `handoff.sh` to set streaming pipeline checkpoints so services resume from where the loader left off:

```bash
./handoff.sh <last_loaded_ledger>
```

This sets checkpoints for the ingester, silver transformer, bronze flusher, and contract-event-index transformer. Then start the streaming Nomad jobs.

### What's Left

**Future:**
- SAC detection in contract data extractor (currently simplified)
- Call graph integration for operations
- Incremental catch-up mode
- Docker image for deployment
