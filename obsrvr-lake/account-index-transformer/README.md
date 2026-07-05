# Account Index Transformer

Builds and maintains the Index Plane table used to route account-history queries:

```text
index.account_ledger_index(account_id, account_bucket, ledger_range)
```

The table is range-granular and partitioned by `account_bucket`, where the bucket is
`crc32(account_id) % 256`.

## Incremental Mode

Default mode reads participant rows from `silver_hot` and writes distinct account/range entries to
DuckLake.

```bash
make run
```

The incremental checkpoint is:

```text
index.account_ledger_transformer_checkpoint
```

## Cold Backfill

Cold backfill reads Silver DuckLake and writes through the same account index writer:

```bash
./bin/account-index-transformer \
  -config config.yaml \
  -backfill \
  -start-ledger 1 \
  -end-ledger 0 \
  -batch-ledgers 100000
```

`-end-ledger 0` means "use the max ledger present in cold Silver".

The backfill checkpoint is:

```text
index.account_ledger_backfill_checkpoint
```

Cold backfill intentionally indexes only the cold-safe participant columns:

- `enriched_history_operations.source_account`
- `enriched_history_operations.destination`
- `token_transfers_raw.from_account`
- `token_transfers_raw.to_account`
- `contract_invocations_raw.source_account`

Do not add cold `enriched_history_operations.from_account`, `to_address`, or `address` until the
cold data is reprocessed with string-compatible types.
