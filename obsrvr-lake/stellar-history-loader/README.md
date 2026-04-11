# stellar-history-loader

Parallel batch tool for processing Stellar blockchain history into Parquet files and DuckLake. Designed for high-throughput backfill of the Obsrvr Lake bronze layer.

## Architecture

```
Data Source (GCS/S3/RPC/XDR/FS)
    |
Orchestrator (splits ledger range into N shards)
    |
N Workers (parallel goroutines)
    | per worker:
    Prefetch Buffer (200 ledgers ahead)
    |
    XDR Decode (ONCE per ledger)
    |
    21 Parallel Extractors (goroutines)
    |
    Batch Accumulate -> Parquet Writer (Snappy compressed)
    |
[Optional] DuckLake Push (INSERT INTO ... SELECT FROM read_parquet)
[Optional] Hot Buffer (PostgreSQL COPY for recent ledgers)
```

## Build

```bash
make build
```

Binary is output to `bin/stellar-history-loader`.

## Usage

### Extract from GCS archive

```bash
./bin/stellar-history-loader \
  --start 61790000 --end 61791000 \
  --workers 8 --batch-size 50 \
  --output /data/output \
  --storage-type GCS \
  --bucket "obsrvr-stellar-ledger-data-pubnet-data/landing/ledgers/pubnet" \
  --ledgers-per-file 1 --files-per-partition 64000 \
  --network-passphrase "Public Global Stellar Network ; September 2015"
```

### Extract from nebu fetch output

```bash
nebu fetch 61790000 61791000 --output ledgers.xdr

./bin/stellar-history-loader \
  --start 61790000 --end 61791000 \
  --workers 8 \
  --output /data/output \
  --storage-type XDR --bucket ledgers.xdr \
  --network-passphrase "Public Global Stellar Network ; September 2015"
```

### Extract from local XDR files

```bash
./bin/stellar-history-loader \
  --start 1000 --end 2000 \
  --workers 4 \
  --output /data/output \
  --storage-type FS --bucket /path/to/xdr/files \
  --network-passphrase "Test SDF Network ; September 2015"
```

### Push to DuckLake (B2/S3 cold storage)

Can run standalone on existing Parquet output, or chained after extraction:

```bash
./bin/stellar-history-loader \
  --start 61790000 --end 61791000 \
  --output /data/output \
  --ducklake \
  --ducklake-catalog "postgresql://user:pass@host:5432/catalog_db" \
  --ducklake-data-path "s3://obsrvr-lake-testnet/" \
  --ducklake-metadata-schema bronze_meta \
  --b2-key-id "$B2_KEY_ID" \
  --b2-key-secret "$B2_KEY_SECRET" \
  --b2-endpoint "s3.us-west-004.backblazeb2.com" \
  --b2-region "us-west-004"
```

### Load PostgreSQL hot buffer

Loads the most recent N ledgers into PostgreSQL for low-latency queries:

```bash
./bin/stellar-history-loader \
  --start 61790000 --end 61791000 \
  --output /data/output \
  --pg-host db.example.com --pg-port 25060 \
  --pg-database stellar_hot --pg-user admin --pg-password secret \
  --pg-sslmode require --tail-ledgers 100000
```

### Query output with DuckDB

```bash
# Count all rows across all tables
duckdb -c "SELECT count(*) FROM read_parquet('/data/output/bronze/**/*.parquet')"

# Inspect a specific table
duckdb -c "SELECT * FROM read_parquet('/data/output/bronze/transactions/**/*.parquet') LIMIT 10"

# Check column schema
duckdb -c "DESCRIBE SELECT * FROM read_parquet('/data/output/bronze/ledgers/**/*.parquet')"
```

## Flags

### Required

| Flag | Description |
|------|-------------|
| `--start` | Start ledger sequence |
| `--end` | End ledger sequence |
| `--output` | Output directory for Parquet files |
| `--bucket` | Storage bucket/path for ledger data (required unless `--ducklake` only) |

### Extraction

| Flag | Default | Description |
|------|---------|-------------|
| `--workers` | NumCPU | Number of parallel workers |
| `--batch-size` | 1000 | Ledgers per extraction batch |
| `--network-passphrase` | Test SDF Network | Stellar network passphrase |
| `--storage-type` | FS | Backend: `GCS`, `S3`, `FS`, `XDR`, `RPC` |
| `--ledgers-per-file` | 1 | Ledgers per archive file (GCS/S3) |
| `--files-per-partition` | 64000 | Files per archive partition (GCS/S3) |
| `--era-id` | (empty) | Era identifier for DuckLake partitioning |
| `--validate` | false | Run quality validation after extraction |

### DuckLake Push

| Flag | Default | Description |
|------|---------|-------------|
| `--ducklake` | false | Enable DuckLake push |
| `--ducklake-catalog` | | PostgreSQL catalog DSN |
| `--ducklake-data-path` | | S3/B2 bucket path (e.g., `s3://obsrvr-lake-testnet/`) |
| `--ducklake-metadata-schema` | bronze_meta | DuckLake metadata schema |
| `--ducklake-schema-sql` | | Path to v3_bronze_schema.sql (optional, embedded by default) |
| `--b2-key-id` | | B2/S3 access key ID |
| `--b2-key-secret` | | B2/S3 secret access key |
| `--b2-endpoint` | s3.us-west-004.backblazeb2.com | B2/S3 endpoint |
| `--b2-region` | us-west-004 | B2/S3 region |

### Hot Buffer (PostgreSQL)

| Flag | Default | Description |
|------|---------|-------------|
| `--pg-host` | | PostgreSQL host (enables hot buffer) |
| `--pg-port` | 25060 | PostgreSQL port |
| `--pg-database` | stellar_hot | Database name |
| `--pg-user` | | Username |
| `--pg-password` | | Password |
| `--pg-sslmode` | require | SSL mode |
| `--tail-ledgers` | 100000 | Recent ledgers to load |

## Data Sources

| Type | Description | Example |
|------|-------------|---------|
| `GCS` | Google Cloud Storage archive | `--bucket obsrvr-stellar-ledger-data-pubnet-data/landing/ledgers/pubnet` |
| `S3` | Amazon S3 archive | `--bucket my-stellar-archive/ledgers` |
| `RPC` | Stellar RPC endpoint | `--bucket https://soroban-testnet.stellar.org` |
| `XDR` | nebu fetch output (framed XDR file) | `--bucket ledgers.xdr` |
| `FS` | Directory of individual `<sequence>.xdr` files | `--bucket /path/to/xdr/` |

## Bronze Tables (21)

All tables are written as Parquet files under `<output>/bronze/<table>/range_<NNNNNNN>/`.

| Table | Description |
|-------|-------------|
| `ledgers` | Ledger headers and metadata |
| `transactions` | Transaction envelopes, fees, results |
| `operations` | Individual operations with type-specific fields |
| `effects` | State changes from operations |
| `trades` | DEX trade executions |
| `accounts_snapshot` | Account state snapshots |
| `trustlines_snapshot` | Trust line snapshots |
| `account_signers_snapshot` | Multi-sig signer configurations |
| `native_balances` | XLM balance tracking |
| `offers_snapshot` | DEX offer state |
| `claimable_balances_snapshot` | Claimable balance entries |
| `liquidity_pools_snapshot` | AMM pool state |
| `config_settings` | Soroban network configuration |
| `ttl_snapshot` | Contract storage TTL tracking |
| `evicted_keys` | Archived storage key tracking |
| `restored_keys` | Storage key restoration tracking |
| `contract_events` | Soroban contract events |
| `contract_data_snapshot` | Contract storage data |
| `contract_code_snapshot` | WASM contract code metadata |
| `contract_creations` | Contract deployment events |
| `token_transfers` | Unified token transfer events (transfer/mint/burn/clawback/fee) |

Tables map to DuckLake via `mapToDuckLakeTable()` (e.g., `transactions` -> `transactions_row_v2`).

## Output Structure

```
<output>/
  bronze/
    ledgers/range_0000000/shard_0000_*.parquet
    transactions/range_0000000/shard_0000_*.parquet
    operations/range_0000000/shard_0000_*.parquet
    ...
    token_transfers/range_0000000/shard_0000_*.parquet
  _meta_lineage/lineage.parquet
  checkpoint.json
```

- Parquet files are partitioned by `ledger_range` (floor to nearest 10,000)
- Each worker writes its own shard files (no contention)
- Checkpoint enables resume on failure (per-shard tracking)
- Lineage tracks row counts per table per batch

## Performance

Measured on 1,001 mainnet ledgers from GCS with 8 workers:
- 67.3 ledgers/sec
- 5.9M bronze rows across 21 tables
- 345 MB Parquet output
- 15 seconds total

Projected for 63M ledger full history:
- GCE VM near bucket with 64 workers: ~1-2 days
- Local NVMe with FS backend: ~35 minutes
