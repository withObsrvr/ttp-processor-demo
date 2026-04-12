# Minimal Serving Projection Plan for Obsrvr Lake

This plan turns DuckLake + hot Postgres into a **lean serving layer** for Prism and customer-facing APIs.

## Goal

Keep:
- **full bronze/silver history** in DuckLake
- **only current state, recent feeds, and compact aggregates** in serving Postgres

Avoid:
- storing full historical mirrors in Postgres
- rebuilding Horizon-scale operational cost
- request-time DuckDB federation for explorer-critical endpoints

---

## Architecture

```text
Stellar RPC / ingest
  -> bronze hot Postgres
  -> bronze DuckLake
  -> silver realtime transformer
  -> silver hot Postgres
  -> silver DuckLake
  -> serving projectors
  -> serving Postgres schema (serving.*)
  -> Prism / customer APIs
```

The serving layer is a **projection system**, not a canonical storage layer.

---

## Principles

1. **Current state stays current-only**
   - one row per account / asset / contract / pool / offer
   - no full snapshots in serving PG

2. **Recent feeds are retention-bounded**
   - keep 30-90 days in Postgres
   - archive everything in DuckLake

3. **Rollups are precomputed**
   - no request-time scans for top holders, top contracts, fee percentiles, 24h stats

4. **Deep history is a different path**
   - DuckLake
   - async jobs
   - analytics endpoints

---

## Recommended projectors

Use small focused projectors instead of one giant monolith. Each projector advances its own checkpoint in `serving.sv_projection_checkpoints`.

---

# 1. Current-state projectors

## 1.1 Account current projector

### Reads from
- `silver_hot.accounts_current`
- `silver_hot.accounts_signers` or equivalent signer fields in `accounts_current`
- optional semantic smart-wallet outputs

### Writes to
- `serving.sv_accounts_current`
- `serving.sv_search_entities` (account rows)

### Update trigger
- every new ledger or microbatch
- consume changed accounts only

### Responsibilities
- upsert current account header data
- maintain smart account flags
- update search row

### Prism/API surfaces
- `/silver/accounts/current`
- account page header
- account signers
- search

---

## 1.2 Account balances projector

### Reads from
- `silver_hot.accounts_current` for XLM
- `silver_hot.trustlines_current` for issued assets
- SEP-41 balance sources if maintained separately

### Writes to
- `serving.sv_account_balances_current`
- derived holder counts into `serving.sv_assets_current` / `serving.sv_asset_stats_current`

### Update trigger
- every changed account / trustline batch

### Responsibilities
- upsert `(account_id, asset_key)` balances
- delete zeroed/removed trustlines when appropriate
- drive ranked holder queries cheaply

### Prism/API surfaces
- account balances
- asset holders
- top holders
- token stats

---

## 1.3 Assets current projector

### Reads from
- `silver_hot.trustlines_current`
- token discovery outputs
- SEP-41 metadata outputs
- TOML / metadata enrichment

### Writes to
- `serving.sv_assets_current`
- `serving.sv_asset_metadata`
- `serving.sv_search_entities` (asset rows)

### Update trigger
- periodic microbatch, e.g. every 1-5 minutes

### Responsibilities
- maintain one row per asset
- enrich with verification/name/icon/domain
- update asset search documents

### Prism/API surfaces
- asset directory
- home top assets
- search
- asset previews

---

## 1.4 Contracts current projector

### Reads from
- `silver_hot.contract registry / token registry`
- semantic outputs
- contract metadata outputs
- storage summary outputs

### Writes to
- `serving.sv_contracts_current`
- `serving.sv_contract_labels`
- `serving.sv_search_entities` (contract rows)

### Update trigger
- near real-time for new contracts
- periodic refresh for metadata/storage summaries

### Responsibilities
- maintain current contract metadata
- attach names, types, wallet_type, exported functions
- store deploy info and rent summary

### Prism/API surfaces
- contract detail header
- trending contracts enrichment
- search
- smart wallet UI

---

## 1.5 Offers and pool current projector

### Reads from
- `silver_hot.offers_current`
- `silver_hot.liquidity_pools_current`

### Writes to
- `serving.sv_offers_current`
- `serving.sv_liquidity_pools_current`

### Update trigger
- every changed state batch

### Responsibilities
- keep only active offers and live pools

### Prism/API surfaces
- account offers
- asset/pool preview panels

---

# 2. Recent-feed projectors

These tables should use retention. Start with 30 days, extend to 90 if needed.

---

## 2.1 Recent transaction projector

### Reads from
- `bronze_hot.transactions_row_v2`
- silver decoded tx outputs if available
- silver tx summary / decode pipeline

### Writes to
- `serving.sv_transactions_recent`
- `serving.sv_search_entities` (tx rows)

### Update trigger
- every new ledger

### Responsibilities
- create one summarized tx row per hash
- attach source account, fee, summary_text, tx_type, Soroban resource stats
- enforce retention policy

### Retention
- 30-90 days in PG
- forever in DuckLake

### Prism/API surfaces
- home recent txs
- tx page header
- ledger tx list
- search

---

## 2.2 Recent operations projector

### Reads from
- `silver_hot.enriched_history_operations`

### Writes to
- `serving.sv_operations_recent`

### Update trigger
- every new ledger

### Responsibilities
- write op-level summaries
- populate account activity feeds
- enable ledger op breakdowns

### Retention
- 30-90 days

### Prism/API surfaces
- account activity
- tx operations
- ledger operation breakdown

---

## 2.3 Recent events projector

### Reads from
- silver generic event outputs
- contract event index outputs where useful

### Writes to
- `serving.sv_events_recent`

### Update trigger
- every new ledger / event batch

### Responsibilities
- flatten topic0-topic3
- add from/to/asset/amount where derivable
- preserve raw JSON for UI drilldown

### Retention
- 30-90 days

### Prism/API surfaces
- events firehose
- tx events
- contract recent events

---

## 2.4 Recent trades projector

### Reads from
- silver trades outputs

### Writes to
- `serving.sv_trades_recent`

### Update trigger
- every new trade batch

### Responsibilities
- maintain recent DEX activity
- support rolling volume aggregations

### Retention
- 30-90 days

### Prism/API surfaces
- asset volume rankings
- future market pages

---

## 2.5 Recent contract-calls projector

### Reads from
- `silver_hot.contract_invocations_raw`
- call graph / invocation summary tables

### Writes to
- `serving.sv_contract_calls_recent`

### Update trigger
- every new ledger / Soroban batch

### Responsibilities
- one row per invocation
- support recent calls, top contracts, account-contract interactions

### Retention
- 30-90 days

### Prism/API surfaces
- home trending contracts
- contract invocations table
- account contract interactions

---

## 2.6 Recent ledger projector

### Reads from
- `bronze_hot.ledgers_row_v2`
- optional bronze aggregate helpers

### Writes to
- `serving.sv_ledger_stats_recent`
- `serving.sv_search_entities` (ledger rows)

### Update trigger
- every closed ledger

### Responsibilities
- maintain a recent ledger fact table
- compute close_time_seconds, events_emitted, resource totals

### Retention
- 30-90 days or forever if small enough

### Prism/API surfaces
- home recent ledgers
- network recent ledgers
- ledger detail
- search

---

# 3. Aggregate projectors

These should run incrementally and occasionally reconcile from DuckLake.

---

## 3.1 Network stats projector

### Reads from
- `serving.sv_ledger_stats_recent`
- `serving.sv_transactions_recent`
- `serving.sv_operations_recent`
- `serving.sv_contract_calls_recent`
- Radar ingested validator counts
- optional bronze/silver direct aggregate helpers

### Writes to
- `serving.sv_network_stats_current`

### Update frequency
- every 15-60 seconds

### Responsibilities
- latest ledger
- tx 24h / failed 24h
- ops 24h + previous 24h
- fee percentiles
- active accounts / contracts 24h
- rent burned / avg cpu
- validator count

### Prism/API surfaces
- home network pulse
- network health page
- `/silver/stats/network`

---

## 3.2 Asset stats projector

### Reads from
- `serving.sv_account_balances_current`
- `serving.sv_trades_recent`
- `serving.sv_operations_recent`
- optional silver transfer tables

### Writes to
- `serving.sv_asset_stats_current`
- optional `serving.sv_asset_holders_top`

### Update frequency
- every 1-5 minutes

### Responsibilities
- holder_count
- trustline_count
- circulating_supply
- volume_24h
- transfers_24h
- unique_accounts_24h
- top10 concentration
- optional precomputed top-N holders

### Prism/API surfaces
- asset directory
- asset stats endpoint
- home top assets

---

## 3.3 Contract stats projector

### Reads from
- `serving.sv_contract_calls_recent`
- `serving.sv_events_recent`

### Writes to
- `serving.sv_contract_stats_current`
- `serving.sv_contract_function_stats_current`

### Update frequency
- every 1-5 minutes

### Responsibilities
- top contracts by 24h/7d/30d
- unique caller counts
- success/failure
- top function
- function-level rolling stats

### Prism/API surfaces
- home trending contracts
- contract detail header
- function stats table
- `/silver/contracts/top`

---

# 4. Search projector

## 4.1 Unified search projector

### Reads from
- `serving.sv_accounts_current`
- `serving.sv_assets_current`
- `serving.sv_contracts_current`
- `serving.sv_transactions_recent`
- `serving.sv_ledger_stats_recent`
- Radar validator snapshots (optional)

### Writes to
- `serving.sv_search_entities`

### Update trigger
- event-driven from the other projectors

### Responsibilities
- create one normalized search document per entity
- maintain rank_score for popular/high-value entities

### Prism/API surfaces
- global search

---

# 5. Reconciliation / rebuild jobs

Recent-feed and current-state projectors should be fed from hot Postgres for freshness.

But correctness comes from periodic rebuilds from DuckLake.

## Nightly or ad-hoc rebuild jobs

### 5.1 Current-state reconciliation job
- compare current-state serving tables with authoritative silver current-state tables
- repair drift / missed updates

### 5.2 Aggregate rebuild job
- rebuild `sv_asset_stats_current` and `sv_contract_stats_current` from recent-feed tables and/or DuckLake
- recompute percentiles / concentrations

### 5.3 Deep-history backfill job
- if a new endpoint needs historical warm data, backfill a bounded recent partition from DuckLake into serving PG

Write all rebuild state to:
- `serving.sv_rebuild_jobs`
- `serving.sv_projection_checkpoints`

---

## Mapping to existing obsrvr-lake components

Here is the pragmatic mapping from your existing repo to the new serving layer.

### Already close to the source of truth
- `stellar-postgres-ingester`
  - bronze hot source
- `postgres-ducklake-flusher`
  - bronze hot -> bronze DuckLake
- `silver-realtime-transformer`
  - silver hot producer
- `silver-cold-flusher`
  - silver hot -> silver DuckLake
- `token-discovery-processor`
  - asset/contract metadata enrichment
- `radar-network-ingester`
  - validator/network metadata for serving layer

### New recommended component
Create:
- `serving-projection-processor`

This service should:
- read from silver hot / bronze hot / radar snapshots
- write to `serving.*`
- maintain checkpoints
- own retention cleanup for recent-feed tables

You can implement it as one binary with multiple projector loops initially.

---

## Suggested internal projector modules

Inside `serving-projection-processor`, start with these modules:

1. `accounts_projector`
2. `balances_projector`
3. `assets_projector`
4. `contracts_projector`
5. `transactions_recent_projector`
6. `operations_recent_projector`
7. `events_recent_projector`
8. `contract_calls_recent_projector`
9. `ledgers_recent_projector`
10. `network_stats_projector`
11. `asset_stats_projector`
12. `contract_stats_projector`
13. `search_projector`
14. `retention_compactor`

---

## Rollout order

### Phase 1: unblock the slow Prism endpoints
Build these first:
- accounts projector
- balances projector
- transactions_recent projector
- ledgers_recent projector
- network_stats projector

This should let you reroute:
- `/silver/accounts/current`
- `/silver/accounts/{id}/balances`
- `/silver/accounts/top`
- `/silver/stats/network`
- home recent txs / ledgers

### Phase 2: asset and contract explorer strength
Add:
- assets projector
- asset_stats projector
- contracts projector
- contract_calls_recent projector
- contract_stats projector

This should let you reroute:
- `/silver/assets`
- `/silver/assets/{asset}/holders`
- `/silver/assets/{asset}/stats`
- `/silver/contracts/top`
- contract detail pages

### Phase 3: richer explorer UX
Add:
- operations_recent projector
- events_recent projector
- search projector
- offers/pools projector

This should power:
- tx events / effects views
- events firehose
- account activity
- global search
- account offers

---

## Endpoint routing strategy after rollout

## Serve from serving Postgres
- `/silver/accounts/current`
- `/silver/accounts/top`
- `/silver/accounts/{id}/balances`
- `/silver/accounts/{id}/offers`
- `/silver/assets`
- `/silver/assets/{asset}/holders`
- `/silver/assets/{asset}/stats`
- `/silver/contracts/top`
- `/silver/contracts/{id}/metadata`
- `/silver/transactions/summaries`
- `/silver/stats/network`
- Prism search endpoints
- Prism home/account/ledger/tx fragments

## Serve from DuckLake / analytics path
- long-range historical scans
- exports
- large event backfills
- compliance archive
- analyst-only deep queries

---

## Retention policy

Recommended starting policy:

### Forever in serving PG
- `sv_accounts_current`
- `sv_account_balances_current`
- `sv_assets_current`
- `sv_contracts_current`
- `sv_offers_current`
- `sv_liquidity_pools_current`
- `sv_network_stats_current`
- `sv_asset_stats_current`
- `sv_contract_stats_current`
- `sv_contract_function_stats_current`
- `sv_search_entities`
- checkpoints / rebuild metadata

### 30-90 days in serving PG
- `sv_transactions_recent`
- `sv_operations_recent`
- `sv_events_recent`
- `sv_trades_recent`
- `sv_contract_calls_recent`
- `sv_ledger_stats_recent`

### Forever in DuckLake
- everything historical

---

## Success criteria

You know the serving layer is working when:

1. Prism home/account/ledger/tx pages do not hit DuckLake on request path
2. current-state endpoints are simple indexed Postgres reads
3. top holders / top accounts / top contracts are precomputed or index-supported
4. recent feed pages query only retention-bounded recent tables
5. DuckLake is reserved for replay, rebuild, analytics, and exports

---

## Summary

The serving layer should be:
- **small** compared to Horizon
- **fast** compared to DuckDB federation
- **rebuildable** from DuckLake
- **selective** rather than universal

This is the core posture:

> DuckLake is the historical truth.
> Serving Postgres is the product index.
> Projectors are the bridge.
