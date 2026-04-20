# DeFi Position Processor Design

## Purpose

This document describes where the DeFi computation service should live, how it should work, and how a first complete implementation can be delivered using **Soroswap** as the reference protocol.

The intent is to separate:

1. **Obsrvr Lake as the data platform and serving substrate**
2. **A dedicated DeFi computation service** that derives normalized DeFi state from Obsrvr Lake inputs
3. **Product-facing APIs/services** that consume this normalized state

This aligns with the current direction of the DeFi Positions API foundation:

- `serving.sv_defi_*` tables hold normalized DeFi state
- `stellar-query-api` exposes read endpoints over those tables
- the missing piece is the service that computes and maintains that state

---

## Role of Obsrvr Lake vs. the DeFi Processor

### Obsrvr Lake should provide

- protocol registry data
- protocol contract-role mapping
- silver-layer contract calls/events/state inputs
- current serving tables for normalized DeFi state
- low-latency query API access over that state

### The DeFi processor should provide

- protocol-specific market discovery
- protocol-specific position calculation
- current user exposure aggregation
- protocol status/freshness/degraded-mode computation
- optional historical snapshots

### A downstream product-facing service may still provide

- customer-facing auth and onboarding
- wallet linking / account resolution UX
- product-level caching and SLOs
- polished documentation / SDKs / examples

---

## Recommended Service Placement

The DeFi computation logic should live as a **new standalone service** inside the Obsrvr Lake monorepo.

### Recommended path

```text
obsrvr-lake/
  defi-position-processor/
```

### Why it should be standalone

A dedicated service keeps DeFi computation separate from:

- `stellar-query-api` read concerns
- generic `serving-projection-processor` concerns
- lower-level silver transformation concerns

This is the cleanest boundary because DeFi computation is likely to become:

- protocol-adapter heavy
- stateful
- valuation-heavy
- operationally distinct
- expensive to backfill and reconcile

---

## Proposed Directory Layout

```text
obsrvr-lake/
  defi-position-processor/
    go/
      main.go
      config.go
      db.go
      checkpoint_store.go
      runner.go
      health.go

      adapters/
        adapter.go
        soroswap/
          adapter.go
          discovery.go
          positions.go
          valuation.go
          parser.go

      models/
        registry.go
        market.go
        position.go
        totals.go
        status.go

      readers/
        silver_reader.go
        serving_reader.go
        pricing_reader.go

      writers/
        serving_writer.go

      jobs/
        discovery.go
        incremental.go
        reconcile.go
        snapshot.go

    config.yaml.example
    Dockerfile
    Makefile
```

---

## Data Flow

The processor should consume Obsrvr Lake data and write normalized DeFi state back into serving tables.

```text
Stellar ledgers
  ↓
steller-postgres-ingester / silver-realtime-transformer
  ↓
stellar_hot / silver_hot / silver cold
  ↓
defi-position-processor
  ↓
serving.sv_defi_markets_current
serving.sv_defi_positions_current
serving.sv_defi_position_components_current
serving.sv_defi_user_totals_current
serving.sv_defi_protocol_status
  ↓
stellar-query-api
  ↓
gateway.withobsrvr.com / downstream products
```

---

## Inputs and Outputs

## Inputs

### Registry / serving inputs

- `serving.sv_defi_protocols`
- `serving.sv_defi_protocol_contracts`

### Silver / semantic inputs

Depending on protocol, the processor may read from:

- `contract_invocations_raw`
- contract events tables
- `contract_data_current`
- token / asset metadata
- liquidity pool / account balance / trustline-like state
- semantic or explorer-enriched contract views when useful

### Pricing inputs

Ideally:

- `serving.sv_defi_prices_current`

Interim options may include protocol-local price derivation, but long term pricing should be normalized.

## Outputs

The processor should write to:

- `serving.sv_defi_markets_current`
- `serving.sv_defi_positions_current`
- `serving.sv_defi_position_components_current`
- `serving.sv_defi_user_totals_current`
- `serving.sv_defi_protocol_status`

Optional snapshot outputs:

- `serving.sv_defi_user_totals_history`
- `serving.sv_defi_position_history`

---

## Processing Modes

The processor should support four operational modes.

## 1. Discovery

Purpose:

- discover markets/pools/vaults for each supported protocol
- refresh market metadata
- populate `sv_defi_markets_current`

## 2. Incremental current-state computation

Purpose:

- process newly-available ledgers since the last checkpoint
- identify impacted users, positions, and markets
- recompute current normalized state
- upsert serving tables

## 3. Reconciliation / backfill

Purpose:

- rebuild state after bugs, schema changes, or drift
- recompute by protocol, market, user, or ledger range
- validate incremental correctness

## 4. Snapshotting

Purpose:

- periodically write compact historical snapshots
- support portfolio history and position history endpoints

---

## Recommended Computation Model

Use a **registry-driven, adapter-based, impacted-entity recomputation model**.

### Why

This model is more robust than trying to maintain every value as a pure diff stream.

### Core idea

1. detect new protocol-relevant activity
2. identify impacted entities
   - markets
   - owner addresses
3. recompute those entities from canonical current state
4. replace/upsert the normalized current rows

### Advantages

- easier to reason about correctness
- easier to backfill and reconcile
- safer for DeFi math and protocol-specific semantics
- more tolerant of replay and repair workflows

---

## Adapter Model

Each protocol should implement a common adapter interface.

### Conceptual interface

```go
type ProtocolAdapter interface {
    ProtocolID() string

    DiscoverMarkets(ctx context.Context, deps AdapterDeps) ([]DefiMarketRecord, error)

    AffectedEntitiesFromLedgerRange(
        ctx context.Context,
        deps AdapterDeps,
        fromLedger, toLedger int64,
    ) (*AffectedEntities, error)

    RecomputeMarkets(
        ctx context.Context,
        deps AdapterDeps,
        marketIDs []string,
    ) ([]DefiMarketRecord, error)

    RecomputeUserPositions(
        ctx context.Context,
        deps AdapterDeps,
        ownerAddress string,
    ) ([]DefiPositionRecord, []DefiPositionComponentRecord, error)

    ComputeUserTotals(
        ctx context.Context,
        deps AdapterDeps,
        ownerAddress string,
        positions []DefiPositionRecord,
    ) (*DefiUserTotalsRecord, error)

    ComputeProtocolStatus(ctx context.Context, deps AdapterDeps) (*DefiProtocolStatusRecord, error)
}
```

### Shared dependencies

```go
type AdapterDeps struct {
    Silver   SilverReader
    Serving  ServingReader
    Pricing  PricingReader
    Writer   ServingWriter
    Clock    func() time.Time
}
```

---

## Checkpointing

The processor should maintain checkpoints, ideally one per protocol.

### Recommended storage

Use `serving.sv_projection_checkpoints` with names like:

- `defi-position-processor:soroswap`
- `defi-position-processor:aquarius`
- `defi-position-processor:blend`

### Why per-protocol checkpoints

This allows:

- one protocol to backfill or degrade without blocking all others
- easier debugging and replay
- protocol-specific operational visibility

---

## Incremental Processing Cycle

Each incremental cycle should:

1. load latest available silver ledger
2. load checkpoint per protocol
3. process the unhandled ledger range
4. detect impacted entities
5. recompute markets and users
6. upsert serving rows
7. update protocol status
8. advance checkpoint

### Pseudocode

```go
func (r *Runner) RunCycle(ctx context.Context) error {
    latest, err := r.silver.LatestLedger(ctx)
    if err != nil { return err }

    for _, adapter := range r.adapters {
        cp, err := r.checkpoints.Load(ctx, adapter.ProtocolID())
        if err != nil { return err }

        if cp >= latest {
            continue
        }

        affected, err := adapter.AffectedEntitiesFromLedgerRange(ctx, r.deps, cp+1, latest)
        if err != nil {
            r.statusWriter.MarkProtocolDegraded(ctx, adapter.ProtocolID(), err)
            continue
        }

        if len(affected.MarketIDs) > 0 {
            markets, err := adapter.RecomputeMarkets(ctx, r.deps, keys(affected.MarketIDs))
            if err != nil { return err }
            if err := r.writer.UpsertMarkets(ctx, markets); err != nil { return err }
        }

        for _, owner := range keys(affected.OwnerAddresses) {
            positions, components, err := adapter.RecomputeUserPositions(ctx, r.deps, owner)
            if err != nil { return err }

            if err := r.writer.ReplaceUserProtocolPositions(ctx, adapter.ProtocolID(), owner, positions, components); err != nil {
                return err
            }

            totals, err := adapter.ComputeUserTotals(ctx, r.deps, owner, positions)
            if err != nil { return err }

            if err := r.writer.UpsertUserTotals(ctx, totals); err != nil {
                return err
            }
        }

        status, err := adapter.ComputeProtocolStatus(ctx, r.deps)
        if err == nil {
            _ = r.writer.UpsertProtocolStatus(ctx, status)
        }

        if err := r.checkpoints.Save(ctx, adapter.ProtocolID(), latest); err != nil {
            return err
        }
    }

    return nil
}
```

---

# Soroswap Complete Example

Soroswap is a strong first implementation target because it already has real contract IDs and observable contract invocation behavior in current indexed data.

## Known Soroswap testnet contracts

### Router

- `CCJUD55AG6W5HAI5LRVNKAE5WDP5XGZBUDS5WNTIVDU7O264UZZE7BRD`

Observed functions in live data:

- `swap_exact_tokens_for_tokens`
- `add_liquidity`
- `remove_liquidity`

### Factory

- `CDP3HMUH6SMS3S7NPGNDJLULCOXXEPSHY4JKUKMBNQMATHDHWXRRJTBY`

### Reviewed hashes

- pair hash: `8447525edd62f72ffaf52136358034657ea0511a8fec1cd0ebde649f86cca464`
- factory hash: `86285a9234d3f0d687eaf88efe8d5d72172b38c9a86624c9934c0cbf2aff2993`
- router hash: `4b95bbf9caec2c6e00c786f53c5f392c2fcdb8435ac0a862ab5e0645eb65824c`
- token hash: `263d165c1a1bf5a0dddbd0f21744a55cd8cacedf96ade78f53c570df11490e46`

---

## Soroswap Support Scope (Phase 1)

Model Soroswap initially as:

- protocol category: `amm`
- current market type: `lp_pool`
- user exposure type: `lp_position`

### Included in first slice

- pool discovery
- LP position computation
- LP reserve-share valuation
- user totals aggregation
- protocol status

### Deferred initially

- claimable rewards
- fee attribution / advanced return decomposition
- sophisticated historical PnL
- broader route analytics beyond what is needed for market and position maintenance

---

## Soroswap Discovery

Purpose:

- discover and maintain Soroswap pool markets in `sv_defi_markets_current`

### Possible discovery strategies

#### Strategy A: pool creation / init calls

Use observed functions like:

- `init_standard_pool`
- `init_stableswap_pool`

#### Strategy B: pool inference from call graphs

Observe router calls into pool contracts during:

- `add_liquidity`
- `remove_liquidity`
- swaps

#### Strategy C: join to existing pool/asset metadata

Use existing Obsrvr Lake liquidity pool and asset metadata if it can be mapped to Soroswap pools.

### Soroswap market row shape

Each discovered pool becomes a normalized market row:

```text
market_id        = "soroswap:<pool_contract_id>"
protocol_id      = "soroswap"
market_type      = "lp_pool"
market_address   = <pool_contract_id>
router_address   = <router_contract_id>
input_asset_1    = token A
input_asset_2    = token B
as_of_ledger     = current ledger
as_of_time       = current time
```

### Example market record

```json
{
  "market_id": "soroswap:CPOOL...",
  "protocol_id": "soroswap",
  "market_type": "lp_pool",
  "market_address": "CPOOL...",
  "router_address": "CCJUD55AG6W5HAI5LRVNKAE5WDP5XGZBUDS5WNTIVDU7O264UZZE7BRD",
  "input_asset_1": {"symbol":"USDC"},
  "input_asset_2": {"symbol":"XLM"},
  "tvl_value_usd": "128420.8821000000",
  "metadata_json": {
    "factory_contract_id": "CDP3HMUH6SMS3S7NPGNDJLULCOXXEPSHY4JKUKMBNQMATHDHWXRRJTBY",
    "pool_contract_id": "CPOOL..."
  }
}
```

---

## Soroswap Incremental Impact Detection

For each unprocessed ledger range, scan Soroswap-relevant contract activity.

### Relevant functions

- `swap_exact_tokens_for_tokens`
- `add_liquidity`
- `remove_liquidity`

### Affected entities model

```go
type AffectedEntities struct {
    MarketIDs      map[string]struct{}
    OwnerAddresses map[string]struct{}
}
```

### Impact rules

#### `add_liquidity`

- affected market = target pool
- affected owner = caller / LP holder

#### `remove_liquidity`

- affected market = target pool
- affected owner = caller / LP holder

#### `swap_exact_tokens_for_tokens`

- affected market(s) = pools touched
- initial simple implementation: recompute market only
- user recomputation can remain limited to liquidity add/remove events in Phase 1

### Practical Phase 1 simplification

- recompute users only for liquidity add/remove
- recompute markets for liquidity add/remove + swaps

This keeps the first implementation tractable and correct enough for a strong vertical slice.

---

## Soroswap Position Model

A Soroswap LP position is modeled as:

- owner address
- pool contract
- current LP share ownership
- reserve-share decomposition into underlying assets

### Position identity

```text
position_id = "soroswap:lp:<owner_address>:<pool_contract_id>"
```

### Position type

```text
position_type = "lp_position"
```

### Market link

```text
market_id = "soroswap:<pool_contract_id>"
```

---

## Soroswap Position Computation

To compute a user's LP position, the adapter needs:

- owner LP share balance `s`
- total pool shares `S`
- reserve A `RA`
- reserve B `RB`
- price A
- price B

### Reserve-share formulas

If the user owns `s / S` of the pool:

- `ownerA = (s / S) * RA`
- `ownerB = (s / S) * RB`

Value:

- `valueA = ownerA * priceA`
- `valueB = ownerB * priceB`
- `current_value = valueA + valueB`

### Phase 1 normalized values

For an LP position:

- `deposit_value = current_value`
- `borrowed_value = 0`
- `net_value = current_value`
- `health_factor = null`
- `risk_status = null`

---

## Soroswap Position Components

Each LP position should write two component rows.

### Component A

```text
component_type = "lp_leg"
asset          = token A
amount         = ownerA
value          = valueA
price          = priceA
```

### Component B

```text
component_type = "lp_leg"
asset          = token B
amount         = ownerB
value          = valueB
price          = priceB
```

Rewards can be added later as:

```text
component_type = "reward"
```

---

## Example Soroswap Position Row

```json
{
  "position_id": "soroswap:lp:GABC...:CPOOL...",
  "protocol_id": "soroswap",
  "position_type": "lp_position",
  "status": "open",
  "owner_address": "GABC...",
  "market_id": "soroswap:CPOOL...",
  "market_address": "CPOOL...",
  "quote_currency": "USD",
  "share_amount": "125.1234567",
  "deposit_value": "482.1172000000",
  "borrowed_value": "0",
  "current_value": "482.1172000000",
  "net_value": "482.1172000000",
  "protocol_state_json": {
    "pool_share_fraction": "0.00428192",
    "pool_contract_id": "CPOOL..."
  },
  "valuation_json": {
    "method": "reserve_share"
  },
  "as_of_ledger": 123456,
  "as_of_time": "2026-01-01T00:00:00Z"
}
```

---

## Soroswap User Totals

For a user with only Soroswap LP positions:

- `total_value = sum(current_value)`
- `total_deposit_value = sum(deposit_value)`
- `total_borrowed_value = 0`
- `net_value = total_value`
- `open_position_count = number of open positions`
- `protocol_count = 1`

### Example `by_protocol_json`

```json
[
  {
    "protocol_id": "soroswap",
    "total_value": "482.1172",
    "total_deposit_value": "482.1172",
    "total_borrowed_value": "0",
    "net_value": "482.1172",
    "position_count": 1
  }
]
```

---

## Soroswap Protocol Status

The adapter should update `serving.sv_defi_protocol_status` based on:

- successful market discovery
- successful market/position recomputation
- availability of required pool inputs
- availability and freshness of pricing inputs
- checkpoint progress

### Example status outcomes

- `ok`
  - pool discovery working
  - pricing available
  - processing advancing

- `degraded`
  - some pools unresolved
  - some price inputs missing or stale

- `halted`
  - required source data not readable

- `backfill`
  - adapter explicitly rebuilding historical/current state

---

## Soroswap Processing Cycle Example

### On startup

1. load Soroswap registry rows
2. discover markets
3. upsert `sv_defi_markets_current`
4. initialize checkpoint if missing

### On each incremental cycle

1. load Soroswap checkpoint
2. read new ledgers since checkpoint
3. detect impacted markets and users
4. recompute markets
5. recompute impacted users' positions
6. recompute impacted users' totals
7. update protocol status
8. advance checkpoint

---

## Writer Semantics

The writer should support a protocol-scoped replace/upsert pattern.

### Example

For `ReplaceUserProtocolPositions(protocolID, ownerAddress, positions, components)`:

1. delete current rows for that protocol + owner
2. insert recomputed current rows
3. delete old component rows for those positions
4. insert recomputed component rows

This avoids stale partial state and simplifies correctness.

---

## Service Health Surface

The processor should expose a health endpoint with at least:

- latest observed silver ledger
- per-protocol processed checkpoint
- last successful cycle time
- degraded protocols
- discovered market count
- current position count

### Example

```json
{
  "status": "ok",
  "latest_silver_ledger": 123456,
  "protocols": {
    "soroswap": {
      "last_processed_ledger": 123455,
      "markets_discovered": 14,
      "positions_current": 832,
      "status": "ok"
    }
  }
}
```

---

## Deployment Model

The processor should deploy like other Obsrvr Lake services.

### Suggested Nomad job

- `defi-position-processor.nomad`

### Config inputs

- silver DB connection
- serving DB connection
- price source config
- enabled protocol list
- tick/trigger mode
- reconciliation controls
- snapshot interval

### Triggering

#### Near-term

- periodic polling every N seconds

#### Longer-term

- gRPC / event-driven trigger, similar to other processors in the stack

---

## Recommended First Milestone

### Milestone: Soroswap LP positions end-to-end

Deliver:

- new `defi-position-processor` service skeleton
- Soroswap adapter
- market discovery
- LP position computation
- component decomposition
- user totals computation
- protocol status updates
- read visibility through existing `stellar-query-api` DeFi endpoints

### Exclusions for milestone 1

- rewards
- advanced fee/PnL decomposition
- multi-protocol aggregation beyond Soroswap
- deep history analytics beyond optional simple snapshots

This would provide a real vertical slice and prove the architecture with live chain-backed data.

---

## Recommended Next Protocols After Soroswap

### Aquarius

Likely next easiest fit:

- router + pool discovery
- LP / stableswap reserve-share logic
- market and totals support similar to Soroswap

### Blend
n
More complex and better suited after the AMM slice is proven:

- lending deposits
- borrowing positions
- collateral / debt decomposition
- health factor computation
- backstop pool semantics

---

## Summary

A dedicated `defi-position-processor` should live inside the Obsrvr Lake monorepo as a standalone service that:

- reads protocol registry + silver-layer inputs
- uses protocol-specific adapters
- computes normalized markets, positions, components, totals, and protocol status
- writes them into `serving.sv_defi_*`
- supports incremental updates, reconciliation, and optional snapshotting

Soroswap is the recommended first complete example because:

- real registry contracts are already known
- live invocation behavior is already observable
- LP reserve-share valuation is tractable
- it provides a strong end-to-end proof point for the DeFi Positions architecture
