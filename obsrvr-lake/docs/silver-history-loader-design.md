# Silver History Loader Design

Date: 2026-06-08

> **Status note (as-built differs from this design).** This document is the
> original design proposal. The shipped `silver-history-loader` diverges from
> several specifics below — for current, authoritative behavior see
> `obsrvr-lake/silver-history-loader/README.md` and the code in
> `silver-history-loader/go/`. Known differences:
> - **Flags:** the loader uses `--bronze-ducklake-catalog` / `--bronze-data-path`
>   and `--silver-ducklake-catalog` / `--silver-data-path` (catalog + data-path
>   pairs), not the `--bronze-ducklake-uri` / `--silver-output-uri` single-URI
>   flags described here.
> - **Commit protocol:** publication is an in-place transactional
>   delete-by-(network, range)-then-insert per table (one DuckDB transaction),
>   not a write-to-staging / validate / atomic-swap protocol.
> - **Manifest:** `silver_load_manifest` has no primary key; idempotency comes
>   from an explicit delete-before-insert in `markManifest`.
> - **Output layout:** physical Parquet layout is delegated to DuckLake; there is
>   no `ledger_bucket` column or Hive-style partition path produced by the loader.
> - **Checksums:** the manifest "checksum" is a synthetic `rows=N;range=A-B;schema=...`
>   descriptor, not a file hash; verification queries the attached DuckLake table,
>   not raw Parquet globs.

## Purpose

Build a historical Silver backfill loader for Obsrvr Lake mainnet that creates a complete, durable Silver cold archive directly from Bronze cold data.

The current Silver cold replay path writes into PostgreSQL `silver_hot`. That is useful for hot APIs and realtime state, but it is the wrong primary sink for full mainnet history. The new loader must write partitioned Parquet/DuckLake directly.

## Target architecture

```text
Bronze cold archive / Bronze DuckLake
  -> silver-history-loader
  -> Silver cold archive: Parquet/DuckLake
```

This is separate from the hot/realtime path:

```text
Bronze hot/live
  -> silver-realtime-transformer
  -> silver_hot PostgreSQL
  -> serving projections
```

PostgreSQL must not be required as an intermediate sink for historical Silver.

## Developer task

Build `silver-history-loader`: a bounded, chunked, resumable historical loader that reads Bronze cold ledger data and writes schema-versioned Silver tables as partitioned Parquet/DuckLake.

The loader should be analogous operationally to `stellar-history-loader`, but it emits Silver-normalized data instead of Bronze/raw history.

## Relationship to `stellar-history-loader`

`silver-history-loader` should reuse the same operational pattern as `stellar-history-loader`, but operate one layer higher in the lake.

```text
stellar-history-loader:
  Stellar history archives / raw ledger source
    -> Bronze cold archive

silver-history-loader:
  Bronze cold archive
    -> Silver cold archive
```

Comparison:

| Concern | `stellar-history-loader` | `silver-history-loader` |
|---|---|---|
| Input | Stellar history archives / raw ledger source | Bronze cold DuckLake/Parquet |
| Output | Bronze raw lake | Silver normalized lake |
| Main job | Ingest raw chain history | Transform Bronze history into analytics-ready Silver tables |
| Data shape | raw ledger/tx/op/effect/XDR-oriented records | enriched operations, token transfers, contract invocations, semantic facts, snapshots/state changes |
| Sink | Bronze DuckLake/Parquet | Silver DuckLake/Parquet |
| Chunking | ledger/checkpoint ranges | ledger ranges |
| Resume model | completed source ranges | completed table/range manifests |
| Verification | ledger coverage and raw data availability | ledger coverage, table row counts, schema checks, readable Parquet/DuckLake output |
| PostgreSQL required? | no | no |

Functional chunk flow:

```text
for each ledger chunk:
  1. read Bronze cold rows for the requested range
  2. run Silver transformation logic
  3. produce typed Silver table batches
  4. write to staging Parquet/DuckLake
  5. validate staged output
  6. publish/commit output atomically
  7. record manifest/checkpoint metadata
```

Key distinction: `stellar-history-loader` mostly preserves source history; `silver-history-loader` derives Silver data products from source history. Because it derives data, it needs stronger validation around schema, row counts, transform errors, and table-level chunk completeness.

## Primary goals

1. Backfill complete mainnet Silver history from ledger `3` through current Bronze target, currently `62,799,999`.
2. Write directly to cold storage as partitioned Parquet/DuckLake.
3. Reuse the existing Silver transformation semantics from `silver-realtime-transformer` where possible.
4. Be chunked, resumable, idempotent, and safe to rerun.
5. Produce manifest/checkpoint metadata for auditability and grant evidence.
6. Keep `silver_hot` as a hot/realtime database, not the historical archive.

## Non-goals

- Do not make `silver_hot` PostgreSQL the required sink for full history.
- Do not depend on `pg_dump` for historical durability.
- Do not only emit `*_current` tables as the historical record.
- Do not require one giant monolithic job to process all mainnet history.

## Inputs

The loader must accept bounded ranges and cold storage locations:

```bash
silver-history-loader \
  --network mainnet \
  --start-ledger 3 \
  --end-ledger 62799999 \
  --chunk-size 100000 \
  --bronze-ducklake-uri <bronze-uri> \
  --silver-output-uri <silver-uri>
```

It must also support processing a single chunk independently:

```bash
silver-history-loader \
  --network mainnet \
  --start-ledger 18000001 \
  --end-ledger 18100000 \
  --bronze-ducklake-uri <bronze-uri> \
  --silver-output-uri <silver-uri>
```

## Transformation source

Use the existing Silver transformer semantics from:

```text
/home/tillman/Documents/ttp-processor-demo/obsrvr-lake/silver-realtime-transformer
```

Important requirement: separate transformation logic from Postgres insert logic.

The reusable core should look conceptually like:

```text
Bronze rows -> typed Silver rows -> output writer
```

Where output writer can be:

- PostgreSQL writer for realtime/hot path
- Parquet/DuckLake writer for historical cold path

## Silver cold output tables

The loader should preserve the current `silver-cold-flusher` cold-table contract where those tables represent append/history/fact data, while avoiding PostgreSQL as the intermediate sink.

Current `silver-cold-flusher` writes to DuckLake using the table names and schemas from `silver_hot`. The current source of truth for its flush list is:

```text
/home/tillman/Documents/ttp-processor-demo/obsrvr-lake/silver-cold-flusher/go/config.go
```

Historical/append tables that the new loader should match directly include:

- `enriched_history_operations`
- `enriched_history_operations_soroban`
- `token_transfers_raw`
- `contract_invocations_raw`
- `contract_metadata`
- `semantic_activities`
- `semantic_flows_value`
- `effects`
- `evicted_keys`
- `trades`
- `restored_keys`

Snapshot/history tables that the new loader should either write directly or produce through the same state-change/snapshot derivation logic include:

- `accounts_snapshot`
- `trustlines_snapshot`
- `offers_snapshot`
- `account_signers_snapshot`

Current-state tables currently flushed by `silver-cold-flusher` should not be the primary historical source of truth. They should be derived later from append/state-change history unless there is a compatibility reason to materialize them in cold as snapshots:

- `accounts_current`
- `trustlines_current`
- `offers_current`
- `claimable_balances_current`
- `contract_data_current`
- `token_registry`
- `native_balances_current`
- `ttl_current`
- `address_balances_current`

If new historical tables are introduced for better derivation, such as `account_state_changes`, `balance_changes`, `trustline_state_changes`, or `contract_data_changes`, document how they map back to the existing flusher-compatible tables and API expectations.

## Current-state derivation model

Current-state tables must be rebuildable from Silver history.

Pattern:

```text
Silver history/deltas
  -> current-state projector
  -> current-state tables
```

Example account current derivation:

```sql
CREATE TABLE accounts_current AS
SELECT *
FROM (
  SELECT
    *,
    row_number() OVER (
      PARTITION BY account_id
      ORDER BY ledger_sequence DESC, operation_index DESC
    ) AS rn
  FROM account_state_changes
  WHERE ledger_sequence <= 62799999
)
WHERE rn = 1
  AND deleted_at_ledger IS NULL;
```

Example balance current derivation:

```sql
CREATE TABLE address_balances_current AS
SELECT *
FROM (
  SELECT
    *,
    row_number() OVER (
      PARTITION BY address, asset_type, asset_code, asset_issuer
      ORDER BY ledger_sequence DESC, operation_index DESC, effect_index DESC
    ) AS rn
  FROM balance_changes
  WHERE ledger_sequence <= 62799999
)
WHERE rn = 1
  AND deleted = false
  AND balance > 0;
```

For performance at full mainnet scale, support periodic snapshots plus deltas:

```text
Silver state changes
  -> snapshot every N ledgers
  -> apply deltas after latest snapshot
  -> current state at target ledger
```

## Output layout

Write partitioned Parquet/DuckLake using deterministic partitioning.

Recommended layout:

```text
silver/
  network=mainnet/
    table=enriched_transactions/
      ledger_bucket=000180/
        part-000.parquet
    table=enriched_operations/
      ledger_bucket=000180/
        part-000.parquet
```

Recommended `ledger_bucket`:

```text
floor(ledger_sequence / 100000)
```

Every output row should include:

- `network`
- `ledger_sequence`
- `ledger_closed_at` where applicable
- stable natural key fields
- `_schema_version`
- `_loaded_at`
- `_source_bronze_start_ledger`
- `_source_bronze_end_ledger`

## Idempotency and commit protocol

Rerunning a ledger range must not duplicate or corrupt output.

Required behavior:

1. Write chunk output to a temporary/staging location.
2. Validate staged files.
3. Atomically publish or replace the final partition/chunk.
4. Update manifest only after successful publish.
5. On failure, leave final output untouched and mark chunk failed.

Acceptable implementation options:

- overwrite exact `network/table/ledger_bucket` partitions for the processed range
- write chunk-scoped files with deterministic names and replace them atomically
- maintain a manifest table/file that identifies the active successful chunk outputs

## Manifest/checkpoint metadata

Create a manifest for auditability and resume.

Suggested schema:

```sql
silver_load_manifest (
  network text,
  start_ledger bigint,
  end_ledger bigint,
  table_name text,
  output_path text,
  row_count bigint,
  checksum text,
  schema_version text,
  status text,
  error_message text,
  started_at timestamp,
  completed_at timestamp,
  primary key (network, start_ledger, end_ledger, table_name)
)
```

Manifest status values:

- `running`
- `completed`
- `failed`
- `superseded`

The loader should support:

```bash
silver-history-loader --resume ...
silver-history-loader --verify --start-ledger X --end-ledger Y ...
```

## Verification requirements

The loader must provide verification commands or subcommands that prove:

1. Bronze source covers the requested range.
2. Every requested chunk has completed manifest entries.
3. Output Parquet files are readable by DuckDB.
4. There are no missing ledger ranges in `enriched_ledgers`.
5. Row counts exist per table and chunk.
6. Checksums or equivalent file integrity metadata are available.

Example verification query:

```sql
SELECT
  min(ledger_sequence) AS min_ledger,
  max(ledger_sequence) AS max_ledger,
  count(*) AS ledger_rows
FROM read_parquet('silver/network=mainnet/table=enriched_ledgers/**/*.parquet');
```

Gap check example:

```sql
WITH ordered AS (
  SELECT
    ledger_sequence,
    lag(ledger_sequence) OVER (ORDER BY ledger_sequence) AS prev_ledger
  FROM read_parquet('silver/network=mainnet/table=enriched_ledgers/**/*.parquet')
)
SELECT *
FROM ordered
WHERE prev_ledger IS NOT NULL
  AND ledger_sequence <> prev_ledger + 1;
```

## Failure behavior

If a chunk fails:

- do not publish partial output as completed
- mark the chunk/table as failed in the manifest
- log network, start ledger, end ledger, table, and error
- allow rerun of only the failed chunk
- do not require replay from ledger 3

## Operational deployment

Create a Nomad parameterized batch job:

```text
silver-history-loader.nomad
```

Required dispatch parameters:

- `network`
- `start_ledger`
- `end_ledger`
- `chunk_size`
- `bronze_ducklake_uri`
- `silver_output_uri`

Example dispatch shape:

```bash
nomad job dispatch \
  -meta network=mainnet \
  -meta start_ledger=18000001 \
  -meta end_ledger=18100000 \
  -meta chunk_size=100000 \
  silver-history-loader
```

## Acceptance criteria

The work is complete when:

1. A bounded `silver-history-loader` can process any requested mainnet ledger range from Bronze cold.
2. The loader writes partitioned Parquet/DuckLake directly to Silver cold storage.
3. The loader is idempotent and can safely rerun a completed or failed chunk.
4. The loader records manifest metadata for each chunk and table.
5. The loader provides verification for completeness and readable output.
6. Mainnet ledger `3 -> 62,799,999` can be backfilled without using PostgreSQL as the historical sink.
7. Current-state tables are documented as rebuildable projections from append/history Silver tables.
8. Existing `silver-realtime-transformer` semantics are reused or matched, so hot and cold Silver semantics do not diverge.

## Initial implementation status

An initial tracer-bullet implementation now exists at:

```text
/home/tillman/Documents/ttp-processor-demo/obsrvr-lake/silver-history-loader
```

It is a bounded DuckDB/DuckLake batch CLI with:

- `--network`, `--start-ledger`, `--end-ledger`, `--chunk-size`
- Bronze DuckLake attach flags
- Silver DuckLake attach flags
- S3/B2 credential flags or `S3_KEY_ID` / `S3_SECRET` env fallback
- `--resume`
- `--verify`
- `silver_load_manifest` writes per table/chunk
- idempotent range delete + insert per implemented table

Implemented direct-to-cold tables:

- `enriched_ledgers`
- `enriched_history_operations`
- `enriched_history_operations_soroban`
- `token_transfers_raw` for classic and SEP-41/Soroban event-derived transfers
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

`contract_data_changes` and `balance_changes` are history/delta tables for rebuilding compatibility current-state tables such as `contract_data_current`, `address_balances_current`, `native_balances_current`, and `trustlines_current` without treating those current-state tables as the primary historical record.

Remaining hardening should focus on a production tracer run against a small mainnet range, comparing row counts against the old Postgres replay/flusher path, then extracting shared transform SQL/logic from `silver-realtime-transformer` to avoid long-term semantic drift.

## Summary for developer

Build a direct-to-cold Silver historical loader. It should read Bronze cold data, transform it into typed Silver historical/fact tables, and write partitioned Parquet/DuckLake with manifest/checkpoint metadata. Do not route full mainnet history through `silver_hot`. Treat `*_current` tables as derived projections over append/history Silver data, optionally accelerated with periodic snapshots.
