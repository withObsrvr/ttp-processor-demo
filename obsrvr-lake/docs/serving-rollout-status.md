# Serving Layer Rollout Status

This document explains:
- what we are trying to achieve with the serving layer
- what has already been implemented
- what is currently deployed in production
- what remains to be done

It is intended as a practical handoff/status document, not a design brainstorm.

---

## Goal

The goal is to move high-value explorer and customer-facing API reads away from:
- broad hot-table scans
- DuckDB hot+cold federation at request time
- repeated request-time aggregation

and onto compact, indexed, low-latency serving tables in PostgreSQL.

The target architecture is:

- **DuckLake** remains the historical source of truth
- **hot Postgres** remains the operational ingest/transform source
- **serving Postgres tables** provide product-facing fast paths
- **projection processors** materialize serving tables from bronze/silver sources

In short:

> DuckLake is the historical truth.
> Serving Postgres is the product index.

### Important architectural note

One key lesson from production rollout work:

- **DuckLake + DuckDB** is a good fit for cold/historical reads because DuckDB can use catalog metadata, partition pruning, and Parquet statistics.
- **DuckDB + `ATTACH POSTGRES`** has proven to be a poor fit for many hot API reads on large PostgreSQL tables, because those queries can devolve into broad `postgres_scan` work rather than the selective indexed access we want.

So the direction of travel is:
- keep DuckLake for cold truth and rebuildability
- prefer direct Postgres for recent/selective hot reads
- prefer serving projections for stable product-facing endpoints
- treat unified DuckDB hot federation as a fallback, not the primary long-term path for high-value hot endpoints

Related next-step design doc:
- `docs/event-driven-serving-trigger-design.md`

---

## What we are trying to optimize

The serving layer is specifically meant to improve these classes of API surfaces:

1. **current-state lookups**
   - current account
   - account balances
   - current asset stats
   - current contract metadata

2. **leaderboard/ranking surfaces**
   - top accounts
   - top assets
   - top contracts

3. **bounded recent-feed surfaces**
   - recent contract calls
   - account activity
   - recent generic events

4. **compact headline stats**
   - network stats

These are the surfaces where request-time federation and large-table scans were least desirable.

---

## Serving-layer approach

### Data sources used

The serving processor reads from existing production sources:

- `bronze_hot` Postgres
- `silver_hot` Postgres

### Serving target

The current implementation writes into:
- database: `silver_hot`
- schema: `serving`

This was intentionally chosen to validate the architecture without provisioning a separate serving cluster.

### Operational model

The current serving processor is:
- a polling service
- running as a Nomad job
- writing directly to serving tables
- using checkpoints for incremental projectors where implemented

---

## What has been implemented

## Serving schema

Serving schema is applied in production under:
- `serving.*`

This includes:
- current-state tables
- compact aggregate tables
- recent-feed tables
- checkpoint metadata table

Important table:
- `serving.sv_projection_checkpoints`

---

## Serving projection processor

Implemented service:
- `obsrvr-lake/serving-projection-processor`

Production status:
- deployed as a Nomad job
- running against production hot databases

### Implemented projectors

#### Current-state / aggregate projectors
- `ledgers_recent`
- `transactions_recent`
- `accounts_current`
- `account_balances`
- `network_stats`
- `asset_stats`
- `contracts_current`
- `contract_stats`

#### Recent-feed projectors
- `contract_calls_recent`
- `operations_recent`
- `events_recent`

---

## Current implementation status by projector

### `contract_calls_recent`
Status:
- **incremental**
- checkpointed
- upsert-based
- 30-day retention cleanup applied

Purpose:
- powers recent contract call surfaces
- supports contract recent activity pages

### `operations_recent`
Status:
- **incremental**
- checkpointed
- upsert-based
- 30-day retention cleanup applied

Purpose:
- powers account activity and recent operations surfaces

### `events_recent`
Status:
- **incremental**
- checkpointed
- upsert-based
- 30-day retention cleanup applied

Purpose:
- powers recent generic contract event surfaces

### `contracts_current`
Status:
- rebuild-style

Purpose:
- current contract metadata/index surface

### `contract_stats`
Status:
- rebuild-style

Purpose:
- top contracts and function-level contract stats

### `asset_stats`
Status:
- rebuild-style

Purpose:
- asset list / asset holder / asset stats fast paths

### `network_stats`
Status:
- rebuild/refresh style from serving inputs

Purpose:
- compact network headline stats

---

## What is deployed in production now

To avoid mixing serving work with other performance optimizations, production endpoint status should be read in three buckets.

### A. Serving-backed in prod

These endpoints currently try `serving.*` first and fall back if serving data is absent or insufficient.

#### Network
- `/api/v1/silver/stats/network`

#### Accounts
- `/api/v1/silver/accounts/current`
- `/api/v1/silver/accounts/top`
- `/api/v1/silver/accounts/{id}/balances`
- `/api/v1/silver/accounts/{id}/activity`

#### Assets
- `/api/v1/silver/assets`
- `/api/v1/silver/assets/{asset}/holders`
- `/api/v1/silver/assets/{asset}/stats`

#### Contracts
- `/api/v1/silver/contracts/top`
- `/api/v1/silver/contracts/{id}/metadata`
- `/api/v1/silver/contracts/{id}/recent-calls`

#### Events
- `/api/v1/silver/events/generic`
- `/api/v1/silver/events/contract/{contract_id}`

Serving-backed endpoints use the staged rollout model:
1. try serving read first
2. fall back to legacy/unified logic if serving data is absent or insufficient

### B. Direct-PG hot-path optimized in prod (not serving-backed)

These are important performance improvements, but they are not serving-table reads.

- `/api/v1/silver/effects`
  - recent/selective queries were routed to direct Postgres hot-path reads instead of the slower unified DuckDB federation path
  - this is a hot-path optimization, not a `serving.*` fast path

### C. Still unified/DuckDB-backed or otherwise not yet migrated

Known remaining non-serving hot spots include:
- `/api/v1/silver/transfers`
- several tx-detail explorer/receipt endpoints listed in the remaining-work section below

---

## Production rollout notes so far

### Contract metadata fallback
Serving-backed contract metadata was initially too sparse for some contracts.

This was corrected by making the API accept serving metadata only when it is materially populated, otherwise falling back to legacy enrichment.

That preserved a safe rollout while keeping fast paths where serving data is complete enough.

### Events projector stabilization
`events_recent` initially failed during rollout because the projector was reading from the wrong DB context.

This was corrected by:
- querying bronze hot explicitly from the bronze source pool
- inserting into serving via the serving target pool

Topic extraction was then improved so serving-backed event responses expose decoded topic values where available.

### DuckDB federation bottleneck discovered during rollout
A major performance bottleneck was identified in the UnifiedDuckDBReader path:
- hot Postgres data is read through DuckDB `ATTACH POSTGRES`
- slow `postgres_scan` queries on wide tables can serialize other DuckDB work behind them
- request-time helper calls such as available-ledger metadata can add significant overhead on already-slow paths

Operational takeaway:
- recent/selective queries should prefer either:
  - serving-table reads, or
  - direct SilverHotReader / hot Postgres reads
- broad unified DuckDB reads should be reserved for the cases where hot-path or serving-path alternatives do not exist yet

### Recent-feed sustainability improvements
The first recent-feed projectors were intentionally rebuild-style.

That has now been improved:
- `contract_calls_recent` is incremental
- `operations_recent` is incremental
- `events_recent` is incremental

This substantially reduces write amplification and makes the current prod setup more sustainable on the existing Nomad node.

---

## Current operational posture

### Recent-feed retention
Currently implemented retention policy for incremental recent tables:
- 30 days

Applied to:
- `serving.sv_contract_calls_recent`
- `serving.sv_operations_recent`
- `serving.sv_events_recent`

### Checkpointing
Incremental projectors use:
- `serving.sv_projection_checkpoints`

Current incremental projectors:
- `contract_calls_recent`
- `operations_recent`
- `events_recent`

### Deployment constraint
All of this has been deployed on the existing Nomad server.

No additional Nomad node was added.

Because of that, rollout has intentionally favored:
- incrementalization
- retention
- serving-first/fallback-safe reads

instead of adding a large number of additional heavy projectors all at once.

---

## What has been validated through gateway

The production deployment has been tested through:
- `https://gateway.withobsrvr.com/lake/v1/testnet/...`

Validated categories include:
- network stats
- top accounts
- account current
- account balances
- asset list
- asset holders
- asset stats
- top contracts
- contract metadata
- contract recent calls
- account activity
- generic events
- contract-scoped generic events
- effects hot-path behavior

Pagination was also verified for:
- contract recent calls
- account activity
- generic events

---

## What remains to be done

The major work remaining is no longer basic serving bring-up. It is now a mix of:
- operational hardening
- API compatibility cleanup
- migration of the remaining slow unified/DuckDB-backed explorer surfaces

### 1. Observability for the serving processor
Add better visibility for:
- per-projector run duration
- rows applied
- rows deleted by retention
- last successful run
- last checkpoint
- last error

### 2. Health/status surface for serving processor
Add either:
- a health endpoint
- or a serving status summary table / structured log view

so it is easy to see projector health without digging through logs.

### 3. Response-shape consistency cleanup
Continue aligning serving-backed responses with fallback responses so that:
- serving path and fallback path return the same semantics
- explorer/event surfaces do not drift subtly over time

### 4. Remaining slow tx-detail / receipt endpoints
These still need serving-layer or direct-PG migration work and are likely the highest-value remaining explorer endpoints:
- `/api/v1/silver/tx/{hash}/full`
- `/api/v1/silver/tx/{hash}/diffs`
- `/api/v1/silver/tx/{hash}/decoded`
- `/api/v1/silver/tx/batch/decoded`
- `/api/v1/silver/effects/transaction/{tx_hash}`

These matter because they power transaction receipt/detail experiences and recent-tx explorer surfaces.

### 5. Remaining non-serving hot-path candidates
Known paths that still need migration off heavier unified reads include:
- `/api/v1/silver/transfers`

This endpoint likely needs either:
- a serving-backed recent-feed path, or
- the same kind of direct-PG hot-path routing treatment used successfully elsewhere

### 6. Route compatibility / alias cleanup
There are also API-path compatibility gaps worth fixing:
- Prism expects `silver/stats/contracts`, while the currently deployed route is `/api/v1/silver/contracts/top`
- Prism may expect `/api/v1/silver/tx/{hash}/effects`, while the existing route shape is `/api/v1/silver/effects/transaction/{tx_hash}`
- `/api/v1/silver/ledger/{seq}` should be verified and added if still absent

These are not serving-table problems by themselves, but they affect product integration and should be tracked explicitly.

### 7. Optional future migration
If scale or isolation requires it later, the serving schema can be moved to:
- a separate serving database
- or a separate serving cluster

That is not required for the architecture to be valid today.

---

## Recommended near-term next steps

The next best work items are:

1. **add serving-processor observability**
2. **add serving-processor health/status reporting**
3. **clean up any remaining response-shape drift between serving and fallback paths**
4. **monitor retention + checkpoint progression in production**

This is the highest-value next phase because the main risk has shifted from “feature missing” to “operational visibility and maintenance”.

---

## How to resume this work

If a future developer or LLM is dropped into this project with little context, use this sequence.

### 1. Read these files first
- `obsrvr-lake/docs/serving-rollout-status.md`
- `obsrvr-lake/docs/serving-layer-implementation-notes.md`
- `obsrvr-lake/serving-projection-processor/README.md`
- `obsrvr-lake/serving-projection-processor/go/main.go`
- `obsrvr-lake/serving-projection-processor/go/config.go`
- `obsrvr-lake/serving-projection-processor/go/checkpoint_store.go`
- `obsrvr-lake/serving-projection-processor/go/schema/serving_schema.sql`

### 2. Inspect the current production job definitions
Check the current Nomad job files under:
- `/home/tillman/Documents/infra/environments/prod/do-obsrvr-lake/.nomad/stellar-query-api.nomad`
- `/home/tillman/Documents/infra/environments/prod/do-obsrvr-lake/.nomad/serving-projection-processor.nomad`

Those files are the operational source of truth for what is actually configured for deployment.

### 3. Verify live status before changing anything
Before implementing new work, check:
- Nomad job health
- projector logs
- current checkpoint positions
- a few gateway endpoints through `gateway.withobsrvr.com`

At minimum verify:
- serving processor is healthy
- API is healthy
- recent-feed projectors are advancing checkpoints

### 4. Prefer this priority order for future work
1. operational visibility / health / metrics
2. response-shape consistency cleanup
3. remaining tx-detail and explorer migration targets
4. new serving projectors only if they solve a real hot-path problem

### 5. Use this architectural rule of thumb
When deciding how to optimize an endpoint, prefer in this order:
1. `serving.*` read if a serving table exists
2. direct hot Postgres read if the query is recent/selective and serving does not exist yet
3. unified DuckDB only when request-time federation is still necessary

---

## Production jobs and components to know about

### Serving processor
- Job: `serving-projection-processor`
- Purpose: materialize and maintain `serving.*` tables

### API
- Job: `stellar-query-api`
- Purpose: serve serving-first explorer/customer APIs with fallback behavior

### Serving tables currently important for product fast paths
- `serving.sv_accounts_current`
- `serving.sv_account_balances_current`
- `serving.sv_assets_current`
- `serving.sv_asset_stats_current`
- `serving.sv_contracts_current`
- `serving.sv_contract_stats_current`
- `serving.sv_contract_function_stats_current`
- `serving.sv_network_stats_current`
- `serving.sv_contract_calls_recent`
- `serving.sv_operations_recent`
- `serving.sv_events_recent`
- `serving.sv_projection_checkpoints`

---

## Quick operational checklist

When touching this system in the future:
- do not assume docs match prod; verify Nomad jobs first
- do not assume serving responses are complete; verify fallback behavior
- do not route new endpoints through DuckDB if a direct PG or serving path is more appropriate
- do not add a new heavy projector without considering retention and checkpoint behavior
- do not enable large rebuild-style projectors casually on a constrained node

---

## Summary

We set out to replace slow request-time explorer reads with compact serving tables and safe serving-first API paths.

That has now been achieved for a meaningful set of production surfaces:
- network stats
- account current/top/balances/activity
- asset list/holders/stats
- contract top/metadata/recent calls
- generic contract events

The recent-feed projectors are no longer naive rebuilds; they now run incrementally with retention.

At this point, the serving layer is no longer just scaffolded or experimental.
It is actively deployed and powering production API fast paths.
