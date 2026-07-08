# silver-current-state-projector

Flowctl-compatible skeleton for building Silver current-state tables directly
from Silver cold DuckLake history/snapshot tables.

This binary validates a bounded ledger range, plans deterministic chunks, emits
structured JSON lifecycle events, records durable manifest status, classifies
failures, and executes idempotent replacement SQL for the implemented current
tables.

## Contract

Required inputs:

```bash
silver-current-state-projector \
  --network mainnet \
  --start-ledger 3 \
  --end-ledger 62799999 \
  --chunk-size 100000 \
  --silver-ducklake-catalog "$SILVER_DUCKLAKE_CATALOG" \
  --silver-data-path "$SILVER_DATA_PATH" \
  --manifest-path /var/lib/flowctl/silver-current-manifest.jsonl \
  --resume
```

Environment inputs accepted for flowctl process-managed runs:

```text
ENABLE_FLOWCTL=true
FLOWCTL_ENDPOINT=<host:port>
FLOWCTL_COMPONENT_ID=silver-current-state-projector
FLOWCTL_RUN_ID=<pipeline run id>
FLOWCTL_ATTEMPT=<attempt number>
START_LEDGER=<inclusive start>
END_LEDGER=<inclusive end>
CHUNK_SIZE=<ledger count>
SILVER_DUCKLAKE_CATALOG=<catalog DSN/path>
SILVER_DATA_PATH=<DuckLake data path>
```

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

## Projection Contract

Implemented projections:

```text
silver.accounts_current
silver.trustlines_current
silver.offers_current
silver.contract_data_current
silver.ttl_current
silver.native_balances_current
silver.address_balances_current
```

For each implemented projection the projector:

- selects the latest source row as of `--end-ledger`
- deletes existing target rows for the requested `--network`
- inserts the replacement current-state rows
- verifies duplicate entity keys
- verifies projected max ledger does not exceed `--end-ledger`
- writes `silver.silver_current_projector_manifest`

`--manifest-path` additionally writes JSONL records for batch-local status
mirroring.

Production blockers that remain explicit in status output:

```text
silver.claimable_balances_current
silver.token_registry
```

`silver.ttl_current` is derived from Bronze cold `ttl_snapshot_v1`, so
production runs must provide the Bronze DuckLake catalog/data inputs in addition
to the Silver catalog/data inputs.

## Status

```bash
silver-current-state-projector --status
```

prints machine-readable readiness, capabilities, and projection metadata.

## Development

```bash
make test
make build
```
