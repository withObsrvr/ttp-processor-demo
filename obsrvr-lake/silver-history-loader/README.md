# silver-history-loader

Bounded batch loader for building Silver cold history directly from Bronze DuckLake, without using `silver_hot` PostgreSQL as an intermediate sink.

This implementation processes deterministic ledger chunks and writes directly into a Silver DuckLake catalog. It currently materializes:

- `enriched_ledgers`
- `enriched_history_operations`
- `enriched_history_operations_soroban`
- `token_transfers_raw` (classic + SEP-41/Soroban event-derived transfers)
- `contract_invocations_raw`
- `contract_metadata`
- `semantic_activities`
- `semantic_flows_value`
- `accounts_snapshot`
- `trustlines_snapshot`
- `offers_snapshot`
- `account_signers_snapshot`
- `effects`
- `trades`
- `evicted_keys`
- `restored_keys`
- `contract_data_changes`
- `balance_changes`

In addition to the data tables above, the loader maintains `silver_load_manifest`, a bookkeeping/control table that tracks per-chunk, per-table load status (it is not a materialized data output).

`contract_data_changes` and `balance_changes` are append/history tables intended to rebuild current-state projections such as `contract_data_current`, `address_balances_current`, `native_balances_current`, and `trustlines_current`.

## Build

```bash
make build
```

DuckDB/DuckLake requires CGO; the Makefile uses `GOWORK=off CGO_ENABLED=1`.

## Run a bounded chunk

```bash
./bin/silver-history-loader \
  --network mainnet \
  --start-ledger 18000001 \
  --end-ledger 18100000 \
  --chunk-size 100000 \
  --bronze-ducklake-catalog "$BRONZE_DUCKLAKE_CATALOG" \
  --bronze-data-path "$BRONZE_DATA_PATH" \
  --silver-ducklake-catalog "$SILVER_DUCKLAKE_CATALOG" \
  --silver-data-path "$SILVER_DATA_PATH" \
  --s3-key-id "$S3_KEY_ID" \
  --s3-secret "$S3_SECRET"
```

## Resume

```bash
./bin/silver-history-loader ... --resume
```

A chunk is skipped only when every implemented table has a `completed` entry in `silver_load_manifest`. Per-table publication is transactional: the loader begins a DuckDB transaction, deletes the exact network/table/range, inserts replacement rows, and commits before marking the manifest row completed.

## Verify

```bash
./bin/silver-history-loader ... --verify
```

Verification checks Bronze ledger coverage, completed manifest entries, readability of `enriched_ledgers`, and that `enriched_ledgers` has no positive ledger-sequence gaps within the requested range (any gap fails verification).
