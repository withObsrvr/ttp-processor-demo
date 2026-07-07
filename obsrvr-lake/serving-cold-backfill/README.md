# serving-cold-backfill

Flowctl-compatible component for materializing selected `serving` schema feed
tables directly from Bronze/Silver cold DuckLake.

The binary validates a bounded ledger range, plans deterministic chunks, emits
structured JSON lifecycle events, records durable manifest status, produces a
checkpoint handoff plan, classifies failures, and executes chunked
delete+insert SQL for implemented feed projections.

## Contract

```bash
serving-cold-backfill \
  --network mainnet \
  --start-ledger 3 \
  --end-ledger 62799999 \
  --chunk-size 100000 \
  --bronze-ducklake-catalog "$BRONZE_DUCKLAKE_CATALOG" \
  --bronze-data-path "$BRONZE_DATA_PATH" \
  --silver-ducklake-catalog "$SILVER_DUCKLAKE_CATALOG" \
  --silver-data-path "$SILVER_DATA_PATH" \
  --target-postgres "$TARGET_POSTGRES" \
  --serving-schema serving \
  --manifest-path /var/lib/flowctl/serving-backfill-manifest.jsonl \
  --resume
```

Environment inputs accepted for flowctl process-managed runs:

```text
ENABLE_FLOWCTL=true
FLOWCTL_ENDPOINT=<host:port>
FLOWCTL_COMPONENT_ID=serving-cold-backfill
FLOWCTL_RUN_ID=<pipeline run id>
FLOWCTL_ATTEMPT=<attempt number>
START_LEDGER=<inclusive start>
END_LEDGER=<inclusive end>
CHUNK_SIZE=<ledger count>
BRONZE_DUCKLAKE_CATALOG=<catalog DSN/path>
BRONZE_DATA_PATH=<DuckLake data path>
SILVER_DUCKLAKE_CATALOG=<catalog DSN/path>
SILVER_DATA_PATH=<DuckLake data path>
TARGET_POSTGRES=<dsn>
SERVING_SCHEMA=serving
```

The current implementation executes through DuckDB/DuckLake. `TARGET_POSTGRES`
is retained as part of the stable flowctl contract; direct PostgreSQL write
plumbing is still a follow-on integration step.

Lifecycle events are newline-delimited JSON on stdout:

```text
component.run_started
component.chunk_started
component.projection_started
component.projection_completed
component.chunk_completed
component.run_completed
component.failed
```

Secrets are not echoed in events, summaries, or status output.

## Required Projection Set

Implemented feed projections:

```text
sv_ledger_stats_recent
sv_transactions_recent
sv_operations_recent
sv_contract_calls_recent
sv_tx_receipts
```

Each implemented feed projection uses chunked idempotent delete+insert
semantics and records `serving.sv_backfill_manifest` rows per
`run_id/projection/network/chunk`.

Implemented current-state projections:

```text
sv_accounts_current
sv_account_balances_current
sv_network_stats_current
sv_assets_current
sv_asset_stats_current
sv_contracts_current
sv_contract_storage_current
sv_contract_stats_current
sv_contract_function_stats_current
```

Current-state projections run after feed chunks and use full table replacement
for the materialized serving table. After all enabled feed and current
projections verify, the component writes `serving.sv_projection_checkpoints`
for every enabled projection to `--end-ledger`.

The broader contract still lists every required first-release serving table:

```text
sv_ledger_stats_recent
sv_transactions_recent
sv_operations_recent
sv_events_recent
sv_explorer_events_recent
sv_contract_calls_recent
sv_tx_receipts
sv_accounts_current
sv_account_balances_current
sv_network_stats_current
sv_assets_current
sv_asset_stats_current
sv_contracts_current
sv_contract_stats_current
sv_contract_function_stats_current
```

Each required projection is marked for checkpoint handoff in status output.
Events/explorer feed projections remain part of the broader contract but are
not enabled by this binary yet, so they are not checkpointed.

## Classified Extra Tables

`--status` reports the serving tables listed by the design as
`seeded_or_registry_managed` or `out_of_scope` with rationale. This keeps
production gaps visible without blocking the initial flowctl contract skeleton.

## Status

```bash
serving-cold-backfill --status
```

prints machine-readable readiness, capabilities, required projection metadata,
and classified extra tables.

## Development

```bash
make test
make build
```
