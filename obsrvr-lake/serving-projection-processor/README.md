# serving-projection-processor

Lean serving-layer projector for Obsrvr Lake.

## Current status

Initial implementation includes:
- serving schema auto-apply
- checkpoint tracking in `serving.sv_projection_checkpoints`
- `ledgers_recent` projector
- `transactions_recent` projector
- `accounts_current` projector
- `account_balances` projector
- `network_stats` projector
- `asset_stats` projector
- `contracts_current` projector
- `contract_stats` projector
- `operations_recent` projector
- `events_recent` projector
- `contract_calls_recent` projector

The projector reads directly from `bronze_hot` PostgreSQL and writes into the `serving` schema on the existing `silver_hot` PostgreSQL cluster.

It also exposes HTTP observability endpoints so projector health can be checked without digging through logs.

## Why this service exists

This service is the start of a serving-layer architecture where:
- DuckLake remains the historical truth
- hot Postgres remains the real-time operational source
- serving Postgres tables provide low-latency explorer/customer APIs

## Run

1. Copy `config.yaml.example` to `config.yaml`
2. Fill in credentials
3. Apply schema only:

```bash
cd serving-projection-processor/go
go run . -config ../config.yaml -apply-schema-only
```

4. Start projector:

```bash
cd serving-projection-processor/go
go run . -config ../config.yaml
```

## Health / status / metrics

Default port:
- `8097`

Endpoints:
- `/health`
- `/status`
- `/metrics`

Per-projector visibility now includes:
- last run duration
- rows applied
- rows deleted
- last successful run
- last checkpoint
- last error

## Current target tables

- `serving.sv_ledger_stats_recent`
- `serving.sv_transactions_recent`
- `serving.sv_accounts_current`
- `serving.sv_account_balances_current`
- `serving.sv_network_stats_current`
- `serving.sv_assets_current`
- `serving.sv_asset_stats_current`
- `serving.sv_contracts_current`
- `serving.sv_contract_storage_current`
- `serving.sv_contract_storage_summary`
- `serving.sv_contract_stats_current`
- `serving.sv_contract_function_stats_current`
- `serving.sv_operations_recent`
- `serving.sv_events_recent`
- `serving.sv_contract_calls_recent`
- `serving.sv_transactions_by_account`
- `serving.sv_operations_by_account`
- `serving.sv_effects_by_account`

Current status of recent-feed projectors:
- `contract_calls_recent` now runs incrementally with checkpointed upserts plus 30-day retention cleanup
- `operations_recent` now runs incrementally with checkpointed upserts plus 30-day retention cleanup
- `events_recent` now runs incrementally with checkpointed upserts plus 30-day retention cleanup
- `effects_by_account` runs incrementally after a full `sv_effects_by_account`
  backfill watermark exists, preserving complete-history semantics for Horizon
  account effects.
- `contract_storage` runs incrementally after authoritative cold current-state
  and serving snapshots establish the handoff watermark. It upserts changed
  entries, applies TTL changes, and consumes guarded deletion/eviction
  tombstones; it never rebuilds serving state from the pruned hot window.
- `transactions_recent` includes Horizon transaction hydration columns:
  `transaction_id`, `tx_envelope`, `tx_result`, `tx_meta`, `tx_fee_meta`,
  and `tx_signers`. These fields let `stellar-query-api` hydrate Horizon
  transaction resources from serving without scanning Bronze on the request path.

These power the first fast-path explorer surfaces and establish the projector/checkpoint pattern for the rest of the serving architecture.

`stellar-query-api` has also been patched to prefer these serving tables for the first fast-path endpoints:
- `/silver/stats/network`
- `/silver/accounts/current`
- `/silver/accounts/top`
- `/silver/accounts/{id}/balances`
- `/silver/assets`
- `/silver/assets/{asset}/holders`
- `/silver/assets/{asset}/stats`
- `/silver/contracts/top`
- `/silver/contracts/{id}/metadata`
- `/api/v1/horizon-compat/transactions/{hash}`
- `/api/v1/horizon-compat/accounts/{id}/transactions`

## Documentation

Read these before extending the service:

- `../docs/minimal-serving-schema.sql`
- `../docs/minimal-serving-projection-plan.md`
- `../docs/serving-layer-implementation-notes.md`
- `../docs/horizon-compat-deployment-runbook.md`
