# Direct Cold Serving Backfill Design

Date: 2026-06-18

## Status

Proposed. This document describes the next set of processes needed after a successful Bronze cold repair and `silver-history-loader` cold Silver rebuild.

## Problem

Current historical rebuild flow can now build Silver cold history directly from Bronze cold:

```text
Bronze DuckLake/cold -> silver-history-loader -> Silver DuckLake/cold
```

However, normal low-latency API serving tables are still produced by `serving-projection-processor` jobs that read hot PostgreSQL sources (`bronze_hot` / `silver_hot`) and write to the `serving` schema in PostgreSQL.

Backfilling full mainnet history through `silver_hot` is too slow and operationally undesirable. We need direct cold backfill jobs that read Bronze/Silver DuckLake and materialize the serving layer for a bounded ledger range, then allow normal live processors to continue from `end_ledger + 1`.

## Goals

1. Backfill serving tables directly from Bronze/Silver cold DuckLake for a bounded ledger range.
2. Avoid full historical replay through `silver_hot` PostgreSQL.
3. Preserve the existing serving schema/API contract where practical.
4. Make the backfill resumable, idempotent, chunked, and safe to rerun.
5. Write serving projector checkpoints at the backfill end ledger so live processors can continue from `end_ledger + 1`.
6. Support an operator flow like:

```bash
serving-cold-backfill \
  --network mainnet \
  --start-ledger 3 \
  --end-ledger 62799999 \
  --chunk-size 100000 \
  --bronze-ducklake-catalog ... \
  --bronze-data-path ... \
  --silver-ducklake-catalog ... \
  --silver-data-path ... \
  --target-postgres ... \
  --serving-schema serving \
  --resume
```

## Non-goals

- Do not use `silver_hot` as the full-history intermediate sink.
- Do not require the realtime gRPC/hot pipeline to be running during cold serving backfill.
- Do not bulk-delete Bronze or Silver cold objects.
- Do not change public query API contracts unless explicitly versioned.

## Existing relevant components

### `silver-history-loader`

Path:

```text
obsrvr-lake/silver-history-loader
```

Current cold Silver tables populated:

```text
enriched_ledgers
enriched_history_operations
enriched_history_operations_soroban
token_transfers_raw
contract_invocations_raw
contract_metadata
semantic_activities
semantic_flows_value
accounts_snapshot
trustlines_snapshot
offers_snapshot
account_signers_snapshot
effects
trades
evicted_keys
restored_keys
contract_data_changes
balance_changes
silver_load_manifest
```

### `serving-projection-processor`

Path:

```text
obsrvr-lake/serving-projection-processor
```

Current serving outputs include:

```text
serving.sv_ledger_stats_recent
serving.sv_transactions_recent
serving.sv_accounts_current
serving.sv_account_balances_current
serving.sv_network_stats_current
serving.sv_assets_current
serving.sv_asset_stats_current
serving.sv_contracts_current
serving.sv_contract_stats_current
serving.sv_contract_function_stats_current
serving.sv_operations_recent
serving.sv_events_recent
serving.sv_contract_calls_recent
serving.sv_projection_checkpoints
```

Current processor reads mostly from `bronze_hot` and `silver_hot` PostgreSQL, not from cold DuckLake.

## flowctl compatibility requirement

Both `silver-current-state-projector` and `serving-cold-backfill` must be designed as flowctl-compatible data-plane components, not only one-off shell scripts.

The near-term implementation may be launched by Nomad wrappers, but the program interfaces should be stable enough for `flowctl` to own orchestration later.

Each program should:

- accept bounded ledger ranges and deterministic chunk identifiers
- be idempotent for the same network/range/chunk
- support `--resume` against durable manifest/checkpoint state
- emit structured logs/events for: run start, chunk start, projection/table start, projection/table complete, chunk complete, verification failure, retryable infrastructure failure, fatal data failure, and run complete
- expose machine-readable status/health when running as a long-lived service, or produce machine-readable summaries when running as a batch job
- write durable manifest/checkpoint rows that `flowctl` can read without scraping logs
- distinguish retryable infrastructure failures from data validation failures, preferably through structured error codes/classes
- avoid owning global orchestration beyond the assigned range/chunk
- never require secrets to be passed through `flowctl` logs or operator-visible status messages

`flowctl` should eventually own:

- chunk planning
- retry policy
- resume state
- failure classification
- dependency ordering between Bronze verification, Silver history load, Silver current projection, serving backfill, checkpoint handoff, and live start
- operator commands such as `plan`, `start`, `status`, `resume`, `retry-chunk`, `verify`, and `handoff-live`

This design should align with:

```text
obsrvr-lake/docs/flowctl-control-plane-data-plane-architecture.md
```

## Proposed new processes

Implement two bounded cold backfill jobs:

```text
1. silver-current-state-projector
2. serving-cold-backfill
```

These are separate because Silver current-state tables are a lake concern, while serving tables are API/materialized-view concerns.

## Process 1: `silver-current-state-projector`

### Purpose

Build Silver current-state tables directly from Silver cold history/change/snapshot tables.

### Inputs

```text
Silver DuckLake catalog/data path
network
as-of ledger / end ledger
```

### Outputs

At minimum:

```text
silver.accounts_current
silver.trustlines_current
silver.offers_current
silver.contract_data_current
silver.native_balances_current
silver.address_balances_current
```

Required additional outputs that are not yet covered by `silver-history-loader` and need explicit implementation/source mapping:

```text
silver.claimable_balances_current
silver.ttl_current
silver.token_registry
```

These are not optional for parity with the existing Silver cold/flusher contract. If any cannot be implemented in the first pass, the developer must document the blocker, source-table gap, and API impact before the backfill is considered production-complete.

### Derivation rules

Initial derivations:

```text
accounts_current
  <- latest accounts_snapshot per account_id where ledger_sequence <= end_ledger

trustlines_current
  <- latest trustlines_snapshot per account_id + asset where ledger_sequence <= end_ledger

offers_current
  <- latest offers_snapshot per offer_id where ledger_sequence <= end_ledger

contract_data_current
  <- latest contract_data_changes per contract_id + key_hash where ledger_sequence <= end_ledger and deleted = false

native_balances_current
  <- latest balance_changes where asset_type='native' per address where ledger_sequence <= end_ledger

address_balances_current
  <- latest balance_changes per address + asset tuple where ledger_sequence <= end_ledger
```

### Idempotency

For current-state tables, use full table replacement inside a transaction where feasible:

```sql
BEGIN;
DELETE FROM silver.<table> WHERE network = $network;
INSERT INTO silver.<table> SELECT ... WHERE ledger_sequence <= $end_ledger;
COMMIT;
```

If full replacement is too large, use staging tables:

```text
silver_meta/projector manifest says target range/end ledger
write silver.<table>__staging_<run_id>
validate staging
transactionally replace target table
record completed manifest
```

### Verification

For each table:

- table exists
- rows are readable
- max ledger/source ledger <= `end_ledger`
- no duplicate primary entity keys
- expected nonzero counts for known active-era ranges

## Process 2: `serving-cold-backfill`

### Purpose

Materialize `serving` schema tables directly from Bronze/Silver cold DuckLake, without replaying full history through `silver_hot`.

### Inputs

```text
network
start_ledger
end_ledger
chunk_size
Bronze DuckLake catalog/data path
Silver DuckLake catalog/data path
target PostgreSQL DSN
target serving schema
resume flag
```

### Outputs

Target PostgreSQL schema:

```text
serving.sv_ledger_stats_recent
serving.sv_transactions_recent
serving.sv_operations_recent
serving.sv_events_recent
serving.sv_explorer_events_recent
serving.sv_contract_calls_recent
serving.sv_tx_receipts
serving.sv_accounts_current
serving.sv_account_balances_current
serving.sv_network_stats_current
serving.sv_assets_current
serving.sv_asset_stats_current
serving.sv_contracts_current
serving.sv_contract_stats_current
serving.sv_contract_function_stats_current
serving.sv_projection_checkpoints
```

Also add a backfill manifest table:

```sql
CREATE TABLE IF NOT EXISTS serving.sv_backfill_manifest (
  run_id TEXT NOT NULL,
  projection_name TEXT NOT NULL,
  network TEXT NOT NULL,
  start_ledger BIGINT NOT NULL,
  end_ledger BIGINT NOT NULL,
  status TEXT NOT NULL,
  row_count BIGINT NOT NULL DEFAULT 0,
  error_message TEXT,
  started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  completed_at TIMESTAMPTZ,
  PRIMARY KEY (run_id, projection_name, start_ledger, end_ledger)
);
```

## Projection classes

### Recent/feed projections

These are bounded by ledger range and can be chunked:

```text
sv_ledger_stats_recent       <- silver.enriched_ledgers
sv_transactions_recent       <- bronze.transactions_row_v2 + silver/enriched data as needed
sv_operations_recent         <- silver.enriched_history_operations
sv_events_recent             <- bronze.contract_events_stream_v1 / silver effects as needed
sv_explorer_events_recent    <- bronze.contract_events_stream_v1 + event classifier/rules + contract labels as needed
sv_contract_calls_recent     <- silver.contract_invocations_raw
sv_tx_receipts               <- bronze transactions/operations/effects + silver enriched rows as needed
```

Retention behavior is not an open design choice for parity with the current serving implementation: `*_recent` serving tables should materialize the latest 30-day window as of `end_ledger`, not full history. Full historical queries should use Silver DuckLake, not serving Postgres.

### Current-state projections

These should be built as-of `end_ledger`, not chunk-by-chunk append state:

```text
sv_accounts_current          <- silver.accounts_current
sv_account_balances_current  <- silver.address_balances_current / native_balances_current
sv_assets_current            <- aggregate sv_account_balances_current
sv_asset_stats_current       <- aggregate sv_account_balances_current
sv_contracts_current         <- silver.contract_metadata + silver.contract_data_current
sv_contract_stats_current    <- aggregate silver.contract_invocations_raw / contract_events
sv_contract_function_stats_current <- aggregate silver.contract_invocations_raw by contract/function
sv_network_stats_current     <- aggregate silver.enriched_ledgers + tx/op counts
```

Current-state projections should run after `silver-current-state-projector`.

Serving schema tables that exist but do not currently have active projector implementations must be explicitly classified by the developer as one of: backfilled now, seeded/registry-managed, live-only, or out-of-scope with rationale. These include at least:

```text
sv_offers_current
sv_liquidity_pools_current
sv_trades_recent
sv_asset_holders_top
sv_search_entities
sv_asset_metadata
sv_contract_labels
sv_defi_protocols
sv_defi_protocol_contracts
sv_defi_markets_current
sv_defi_positions_current
sv_defi_position_components_current
sv_defi_user_totals_current
sv_defi_user_totals_history
sv_defi_position_history
sv_defi_prices_current
sv_defi_protocol_status
```

## Handoff checkpoints

After successful cold backfill to `end_ledger`, set serving checkpoints so live processors continue from `end_ledger + 1`.

Existing checkpoint table:

```text
serving.sv_projection_checkpoints
```

For each projection:

```sql
INSERT INTO serving.sv_projection_checkpoints (
  projection_name,
  network,
  last_ledger_sequence,
  last_closed_at,
  updated_at
)
VALUES ($projection_name, $network, $end_ledger, $end_closed_at, now())
ON CONFLICT (projection_name, network)
DO UPDATE SET
  last_ledger_sequence = EXCLUDED.last_ledger_sequence,
  last_closed_at = EXCLUDED.last_closed_at,
  updated_at = now();
```

Use `silver.enriched_ledgers` to find `end_closed_at`.

## Live continuation flow

For range:

```text
START=3
END=62799999
LIVE_START=62800000
```

Operator flow:

```text
1. Verify Bronze cold for START..END.
2. Run silver-history-loader for START..END.
3. Verify Silver history and manifest.
4. Run silver-current-state-projector --as-of-ledger END.
5. Run serving-cold-backfill --start START --end END.
6. Verify serving counts/checkpoints.
7. Start live Bronze/Silver processors from LIVE_START.
8. Start normal serving-projection-processor jobs; they continue from checkpoint END.
9. Start query API.
```

## Failure handling

### Chunk/feed projections

For chunked feed projections:

- write manifest `running`
- delete target rows for `(network, projection, ledger range)`
- insert replacement rows
- validate row count/readability
- write manifest `completed`

On failure, resume from same failed chunk.

### Current projections

For full current-state projections:

- build staging
- validate duplicates/counts
- replace target
- checkpoint only after replacement succeeds

Do not advance checkpoints before data is committed.

## Verification requirements

### Silver current verification

- no duplicate entity keys in current tables
- all rows have `network=mainnet`
- no source ledger greater than `end_ledger`
- spot counts against Silver history are plausible

### Serving verification

- every enabled projection has checkpoint `end_ledger`
- serving tables are readable
- recent-feed max ledger <= `end_ledger`
- recent-feed min ledger respects configured retention window or requested full-history mode
- current tables have no duplicate entity keys
- query API smoke tests pass against serving-backed routes

## Suggested implementation structure

Reuse patterns from:

```text
silver-history-loader/go
serving-projection-processor/go
```

Suggested new directories:

```text
obsrvr-lake/silver-current-state-projector/
obsrvr-lake/serving-cold-backfill/
```

Each should have:

```text
README.md
Dockerfile
Makefile
go/main.go
go/go.mod
nomad/*.nomad or infra-generated jobspec wrapper
```

Add infra wrappers:

```text
scripts/14-build-silver-current-state.sh
scripts/15-serving-cold-backfill.sh
scripts/16-verify-cold-to-serving-handoff.sh
```

## Open questions for developer

1. Which Bronze/Silver source is authoritative for `sv_events_recent` and `sv_explorer_events_recent`?
2. What is the exact source mapping for `claimable_balances_current`, `ttl_current`, and `token_registry`?
3. Should `serving-cold-backfill` write to `silver_hot.serving` or a separate serving PostgreSQL database/schema? Current production layout uses `silver_hot.serving`.
4. Should live projectors be modified to support DuckLake sources directly, or should only bounded cold backfill read DuckLake while live projectors continue reading hot PostgreSQL?
5. How should registry/bootstrap tables such as `sv_defi_protocols`, `sv_asset_metadata`, and `sv_contract_labels` be seeded in production?

## Required initial delivery

The initial implementation must populate all required serving projections in one coordinated backfill release. Do not ship a partial single-projection-only implementation as the operational solution.

Required projections for the first usable release:

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
sv_projection_checkpoints
```

Development may still use narrow ledger ranges for validation, but the delivered tool must run all enabled projections together for a bounded range and advance checkpoints only after the complete projection set succeeds.

Acceptance criteria:

1. Can run all projections for a small bounded range, e.g. `--start-ledger 3 --end-ledger 100002`.
2. Can run all projections for the full production range, e.g. `--start-ledger 3 --end-ledger 62799999`.
3. Populates every required serving table idempotently.
4. Writes `serving.sv_backfill_manifest` entries for every projection/chunk or projection/run.
5. Writes/updates `serving.sv_projection_checkpoints` for every live projection to the requested `end_ledger` only after all required projections pass verification.
6. Rerun with `--resume` produces no duplicates and converges to the same row counts/checkpoints.
7. If any projection fails, the process exits nonzero, does not advance final handoff checkpoints, and can resume safely.
