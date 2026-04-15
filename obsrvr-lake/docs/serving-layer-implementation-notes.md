# Serving Layer Implementation Notes

This document explains what has been implemented for the new serving-layer architecture, why the implementation is structured this way, and what the next developer should do next.

## Why this exists

The existing `stellar-query-api` can answer many explorer-style queries from:
- hot PostgreSQL
- DuckLake / DuckDB
- or a unified DuckDB federation layer

That federation approach is too slow for request-time explorer and customer-facing API endpoints such as:
- current account lookup
- top accounts
- account balances
- asset holders
- token stats
- network stats

The long-term solution is:
- **DuckLake for historical truth and rebuildability**
- **serving Postgres for low-latency product surfaces**
- **projection processors** to keep serving tables current

This avoids rebuilding full Horizon-style historical relational storage.

---

## Architectural takeaway: DuckLake yes, DuckDB ATTACH POSTGRES no for hot paths

A recurring source of confusion is that two very different access patterns exist in the current system:

1. **DuckLake cold reads**
   - DuckDB reads catalog metadata from PostgreSQL (`bronze_meta`, `silver_meta`, etc.)
   - then prunes and reads only the needed Parquet files from object storage
   - this is generally a good fit for historical and analytical queries

2. **DuckDB `ATTACH ... (TYPE POSTGRES)` hot reads**
   - DuckDB federates live PostgreSQL tables through `postgres_scan`
   - for large hot tables this often behaves like a wide row-stream into DuckDB's execution engine
   - in practice this has been much slower than querying PostgreSQL directly for selective product/API reads

The practical rule going forward is:

> **Use DuckLake/DuckDB for cold historical queries.**
> **Use direct PostgreSQL or serving projections for hot product-facing queries.**

In other words:
- DuckLake is excellent for cold truth and rebuildability
- serving Postgres is the right place for low-latency explorer/customer endpoints
- the unified DuckDB reader should increasingly become **cold-first**, not the default path for hot reads on large tables

This is why many recent optimizations have the same shape:
- bypass DuckDB for hot reads
- query Postgres directly when the request is recent/selective
- add serving projections for the highest-value endpoints

Related design doc for near-real-time serving:
- `docs/event-driven-serving-trigger-design.md`

---

## High-level architecture

```text
bronze_hot postgres  ----->
                           \
                            serving-projection-processor ---> serving.* tables in silver_hot postgres
                           /
 silver_hot postgres  ---->

DuckLake remains the historical source of truth for rebuilds, backfills, and analytics.
```

Current implementation uses the **simple path**:
- existing cluster
- existing `silver_hot` Postgres database
- new schema: `serving`

This choice minimizes operational complexity while validating the architecture.

---

## What has been implemented

## 1. Serving schema applied to existing cluster

Applied to:
- database: `silver_hot`
- schema: `serving`

Source file:
- `docs/minimal-serving-schema.sql`

The schema contains:
- current-state tables
- recent-feed tables
- aggregate tables
- search tables
- checkpoint / rebuild metadata tables

Important operational table:
- `serving.sv_projection_checkpoints`

This is the state store for projector progress.

---

## 2. New service scaffold

Created:
- `obsrvr-lake/serving-projection-processor`

Key files:
- `go/main.go`
- `go/config.go`
- `go/db.go`
- `go/checkpoint_store.go`
- `go/schema_init.go`
- `go/schema/serving_schema.sql`

The service currently:
- connects directly to source Postgres databases
- connects directly to the serving Postgres target
- optionally auto-applies the serving schema
- runs enabled projectors on a polling loop

Implemented projectors in the service right now:
- `ledgers_recent`
- `transactions_recent`
- `accounts_current`
- `account_balances`
- `network_stats`
- `asset_stats`
- `contracts_current`
- `contract_stats`
- `operations_recent`
- `events_recent`
- `contract_calls_recent`

This service does **not** use gRPC for projections.

### Why direct Postgres instead of gRPC?
Projection processors are internal materialization jobs. Direct DB access is preferred because it:
- avoids an extra service hop
- avoids serialization overhead
- makes batching and checkpointing easier
- is simpler to rebuild and reason about

Use gRPC later only for enrichments or external event streams if truly necessary.

---

## 3. Implemented projectors

## `ledgers_recent`
Source:
- `bronze_hot.ledgers_row_v2`

Target:
- `serving.sv_ledger_stats_recent`

Checkpoint key:
- max `sequence`

Why first:
- easiest append-like source
- immediately useful for Prism home/network/ledger pages
- establishes the checkpoint/upsert pattern

### Current mapped fields
- ledger_sequence
- closed_at
- ledger_hash
- prev_hash
- protocol_version
- base_fee_stroops
- max_tx_set_size
- successful_tx_count
- failed_tx_count
- operation_count
- soroban_op_count
- close_time_seconds

### Not yet mapped
- events_emitted
- total_fee_charged_stroops
- total_cpu_insns
- total_read_bytes
- total_write_bytes
- total_rent_stroops

Those can be added later as the bronze source usage expands.

---

## `transactions_recent`
Source:
- `bronze_hot.transactions_row_v2`

Target:
- `serving.sv_transactions_recent`

Checkpoint key:
- max `ledger_sequence`

Why now:
- powers home recent txs and tx headers
- avoids repeated expensive request-time summaries for very recent tx views
- provides a feed table that later aggregate projectors can read from

### Current mapped fields
- tx_hash
- ledger_sequence
- created_at
- source_account
- fee_charged_stroops
- max_fee_stroops
- successful
- operation_count
- tx_type (basic heuristic)
- summary_text (basic heuristic)
- primary_contract_id
- memo_type
- memo_value
- account_sequence
- is_soroban
- cpu_insns
- read_bytes
- write_bytes

### Important note
This projector currently writes a **basic transaction summary**, not a rich explorer narrative.
That is intentional.

The goal of v1 is to establish a fast serving table and upgrade the summary later.

---

## `accounts_current`
Source:
- `silver_hot.accounts_current`

Target:
- `serving.sv_accounts_current`

Checkpoint key:
- max `last_modified_ledger`

Why now:
- powers current account lookup
- supports top accounts from indexed serving table
- removes a major class of slow DuckDB federation queries

### Current mapped fields
- account_id
- balance_stroops
- sequence_number
- num_subentries
- created_at (currently sourced from `closed_at` until true creation-time enrichment is added)
- last_modified_ledger
- updated_at
- home_domain
- master_weight
- low_threshold
- med_threshold
- high_threshold

### Not yet mapped
- signers_json
- smart account enrichment
- first_seen_ledger

Those should be added by later enrichers or projectors.

---

## `account_balances`
Source:
- `silver_hot.accounts_current`
- `silver_hot.trustlines_current`

Target:
- `serving.sv_account_balances_current`

Checkpoint key:
- max `last_modified_ledger` across accounts/trustlines in the processed batch

Why now:
- this table unlocks multiple slow endpoints at once:
  - account balances
  - asset holders
  - top holders
  - token stats

### Implementation strategy
This projector does **not** try to incrementally mutate individual asset rows.
Instead it:
1. finds accounts changed since the last checkpoint
2. deletes existing serving balances for those accounts
3. re-inserts current XLM balance and all trustlines for those accounts

### Why this approach?
Because for a v1 projector it is:
- simpler
- safer
- idempotent
- easier to reason about

It trades a little extra write work for much lower implementation complexity.

### Current mapped fields
- account_id
- asset_key
- asset_code
- asset_issuer
- asset_type
- balance_stroops
- balance_display
- limit_stroops
- is_authorized
- last_modified_ledger
- updated_at

### Important note
`asset_key` is synthesized as:
- `XLM` for native
- `POOL:<liquidity_pool_id>` for pool trustlines
- `CODE:ISSUER` for issued assets

If this convention changes later, update both projector and readers together.

---

## What has **not** been implemented yet

Still pending:
- serving-backed API fast paths for recent activity endpoints
- deeper contract detail serving enrichment
- retention cleanup / compaction for recent-feed tables

That work is still needed before the architecture delivers its full value.

---

## Design decisions

## Decision 1: Use the existing `silver_hot` cluster
Reason:
- fastest route to proving the architecture
- no new cluster provisioning
- least operational complexity
- easy later migration to a dedicated serving DB if needed

## Decision 2: Separate schema, not separate database
Reason:
- same as above
- clean logical separation via `serving.*`
- no cross-database complexity

## Decision 3: Polling loops first, not event streaming
Reason:
- simple and robust
- easy to restart and resume
- easier to reason about while schema and semantics stabilize

## Decision 4: Checkpoint by ledger where possible
Reason:
- deterministic ordering
- replayable
- aligns with blockchain source semantics

## Decision 5: Prefer idempotent upsert / replace semantics
Reason:
- easier correctness story
- less bug-prone under retries and restarts
- simpler to recover from partial failures

---

## Known limitations

1. `transactions_recent` summaries are still basic heuristics.
2. `accounts_current.created_at` is not a true account creation timestamp yet.
3. `account_balances` rewrites all balances for changed accounts; this is fine for v1 but may need optimization later.
4. No retention cleanup exists yet for recent-feed tables.
5. No health endpoint yet for the new service.
6. No Docker/Nomad deployment is wired yet.
7. The query API has not yet been updated to read from `serving.*`.

---

## Query API fast-paths implemented

`stellar-query-api` has now been patched to prefer serving-table reads for:
- `/silver/stats/network`
- `/silver/accounts/current`
- `/silver/accounts/top`
- `/silver/accounts/{id}/balances`
- `/silver/assets`
- `/silver/assets/{asset}/holders`
- `/silver/assets/{asset}/stats`
- `/silver/contracts/top`
- `/silver/contracts/{id}/metadata`

Implementation approach:
- try serving read first from `silver_hot.serving.*`
- if serving data is unavailable or empty, fall back to the previous implementation path

Why this approach:
- safe incremental rollout
- no flag day migration
- preserves historical fallback behavior while serving projections catch up

## Recommended next steps

## Next code step
Wire `stellar-query-api` recent-activity fast paths to the new feed tables:
- contract recent calls
- account activity / recent operations
- recent events surfaces

Why next:
- the bounded recent-feed serving tables now exist
- the next value is shifting request-time explorer reads off broader silver scans and unified federation paths

## After that
Add retention cleanup and reconciliation logic for recent-feed tables:
- `sv_operations_recent`
- `sv_events_recent`
- `sv_contract_calls_recent`
- `sv_transactions_recent`
- `sv_ledger_stats_recent`

## Then
Optimize the rebuild-style recent projectors into incremental updaters if needed.

---

## How a new developer should work on this

When adding a new projector:
1. identify a single source table or tightly-related source set
2. choose the serving target table
3. decide checkpoint key
4. write idempotent upsert logic
5. update `config.yaml.example`
6. update this document and the service README
7. only then wire readers in `stellar-query-api`

Do **not** start by patching API handlers to read from half-populated serving tables.

The intended order is:
- build projector
- verify data correctness
- backfill enough data
- then switch API read path

---

## Smoke test status

A short-lived binary was copied to the production droplet and run against the existing clusters.

Observed successful population of:
- `serving.sv_ledger_stats_recent`
- `serving.sv_transactions_recent`
- `serving.sv_accounts_current`
- `serving.sv_account_balances_current`
- `serving.sv_network_stats_current`
- `serving.sv_assets_current`
- `serving.sv_asset_stats_current`
- `serving.sv_contracts_current`
- `serving.sv_contract_stats_current`
- `serving.sv_contract_function_stats_current`

Important source-schema mismatches found during bring-up:
1. `silver_hot.accounts_current.balance` is `text`, not `bigint`
2. threshold column names in prod are:
   - `low_threshold`
   - `med_threshold`
   - `high_threshold`

The projector code was updated to match production reality.

## Files to read first

For the next developer, start with:
- `docs/minimal-serving-schema.sql`
- `docs/minimal-serving-projection-plan.md`
- `docs/serving-layer-implementation-notes.md`
- `serving-projection-processor/README.md`
- `serving-projection-processor/go/main.go`
- `serving-projection-processor/go/checkpoint_store.go`

These are the core design and implementation entry points.


 ### Rollout notes

 Deployed the next constrained serving-layer slice to production without adding new Nomad
 capacity.

 #### Deployed

 - serving-projection-processor
     - enabled only:
           - contract_calls_recent
     - kept disabled:
           - operations_recent
           - events_recent
 - stellar-query-api
     - added serving fast path for:
           - /api/v1/silver/contracts/{id}/recent-calls
     - behavior:
           1. try serving.sv_contract_calls_recent
           2. fall back to existing unified/hot path if serving data is absent

 #### Images

 - withobsrvr/stellar-query-api:2026-04-14-03

 #### Nomad

 Updated:
 -
 /home/tillman/Documents/infra/environments/prod/do-obsrvr-lake/.nomad/serving-projection-proc
 essor.nomad
 -
 /home/tillman/Documents/infra/environments/prod/do-obsrvr-lake/.nomad/stellar-query-api.nomad

 Both jobs rolled successfully and are healthy.

 #### Serving data

 Observed population:
 - serving.sv_contract_calls_recent = 5960

 Processor log confirms:
 - projector=contract_calls_recent network=testnet rebuilt recent contract calls serving table

 #### Gateway verification

 Verified through gateway.withobsrvr.com:
 - /api/v1/silver/contracts/{id}/recent-calls

 Checks performed:
 - response shape valid
 - real recent rows returned
 - success/failure values populated
 - cursor pagination works
 - second-page fetch had no overlap with first page
 - tested across multiple contract types:
     - token-style transfer
     - deposit-style contracts
     - failed calls also surfaced correctly

 #### Current production serving fast paths now include

 - /api/v1/silver/stats/network
 - /api/v1/silver/accounts/current
 - /api/v1/silver/accounts/top
 - /api/v1/silver/accounts/{id}/balances
 - /api/v1/silver/assets
 - /api/v1/silver/assets/{asset}/holders
 - /api/v1/silver/assets/{asset}/stats
 - /api/v1/silver/contracts/top
 - /api/v1/silver/contracts/{id}/metadata
 - /api/v1/silver/contracts/{id}/recent-calls

 #### Why rollout was constrained

 No additional Nomad server capacity is currently available, so only the smallest useful
 recent-activity slice was enabled:
 - contract_calls_recent

 Heavier recent-feed projectors remain intentionally deferred:
 - operations_recent
 - events_recent

 #### Recommended next step

 Hold here and observe:
 - Nomad allocation stability
 - stellar-query-api behavior under normal traffic
 - DB load on silver_hot

 Only after that, consider the next slice:
 - operations_recent for account activity / recent operations
 - events_recent later, if capacity allows