# DeFi Positions API — Concrete Implementation Plan

## Goal

Add a DeFi positions layer to Obsrvr Lake that supports:

- protocol registry and market registry
- normalized user positions across supported Stellar DeFi protocols
- aggregate exposure summaries
- historical position and exposure charts
- protocol health / freshness / degraded-mode reporting

This document defines:

1. actual SQL migration files for `serving.sv_defi_*` tables
2. OpenAPI-style endpoint definitions for `/api/v1/semantic/defi/*`
3. processor and rollout requirements for integrating this into Obsrvr Lake

---

## Deliverables added in this repo

### SQL migrations

Location:

- `serving-projection-processor/migrations/001_create_sv_defi_protocol_registry.sql`
- `serving-projection-processor/migrations/002_create_sv_defi_markets_current.sql`
- `serving-projection-processor/migrations/003_create_sv_defi_positions_current.sql`
- `serving-projection-processor/migrations/004_create_sv_defi_totals_and_history.sql`
- `serving-projection-processor/migrations/005_create_sv_defi_prices_and_status.sql`

### OpenAPI spec

Location:

- `stellar-query-api/docs/defi-openapi.yaml`

---

## What is being added to Obsrvr Lake

### New serving tables

The implementation adds the following new serving-layer tables:

- `serving.sv_defi_protocols`
- `serving.sv_defi_protocol_contracts`
- `serving.sv_defi_markets_current`
- `serving.sv_defi_positions_current`
- `serving.sv_defi_position_components_current`
- `serving.sv_defi_user_totals_current`
- `serving.sv_defi_user_totals_history`
- `serving.sv_defi_position_history`
- `serving.sv_defi_prices_current`
- `serving.sv_defi_protocol_status`

### New API surface

Planned endpoints:

- `GET /api/v1/semantic/defi/protocols`
- `GET /api/v1/semantic/defi/markets`
- `GET /api/v1/semantic/defi/status`
- `GET /api/v1/semantic/defi/exposure`
- `GET /api/v1/semantic/defi/positions`
- `GET /api/v1/semantic/defi/positions/{position_id}`
- `GET /api/v1/semantic/defi/positions/history`

---

## Why a new DeFi protocol registry is needed

Existing registry-like tables in Obsrvr Lake are useful but do not solve DeFi aggregation:

- `token_registry` answers token metadata questions
- `contract_registry` answers contract identity questions
- `event_classification_rules` answers event semantics questions
- `semantic_entities_contracts` answers observed contract-type questions

A DeFi positions system needs a different type of registry:

- which protocols are supported
- which contracts belong to a protocol deployment
- what role each contract plays
- which adapter should decode positions
- what pricing and health models apply
- how degraded status should be reported

That is why this design introduces an explicit `sv_defi_protocols` + `sv_defi_protocol_contracts` registry rather than overloading the existing registries.

---

## Serving table roles

### 1. `sv_defi_protocols`
Canonical protocol registry.

Use cases:
- `/semantic/defi/protocols`
- protocol capability checks
- adapter dispatch
- status labeling

### 2. `sv_defi_protocol_contracts`
Maps protocol IDs to contract IDs and contract roles.

Use cases:
- pool discovery
- market registry construction
- protocol ownership of contracts
- enrichment of explorer / DeFi responses

### 3. `sv_defi_markets_current`
Current protocol markets/pools/vaults.

Use cases:
- `/semantic/defi/markets`
- valuation and health inputs
- market metadata lookup during position computation

### 4. `sv_defi_positions_current`
Current normalized user positions.

Use cases:
- `/semantic/defi/positions`
- `/semantic/defi/positions/{position_id}`
- wallet dashboards
- protocol user summaries

### 5. `sv_defi_position_components_current`
Breaks complex positions into legs/components.

Use cases:
- detailed position view
- collateral/debt decomposition
- LP reserve-share decomposition
- rewards display

### 6. `sv_defi_user_totals_current`
Pre-aggregated user summary table.

Use cases:
- `/semantic/defi/exposure`
- low-latency account summary surfaces

### 7. `sv_defi_user_totals_history`
Exposure history by time bucket.

Use cases:
- user portfolio charts
- trend analysis

### 8. `sv_defi_position_history`
Per-position history snapshots.

Use cases:
- charting individual positions
- risk / liquidation retrospectives

### 9. `sv_defi_prices_current`
Normalized price layer for valuation.

Use cases:
- current value calculation
- health factor computation
- stale-price detection

### 10. `sv_defi_protocol_status`
Operational/freshness state for the DeFi layer.

Use cases:
- `/semantic/defi/status`
- degraded mode reporting
- monitoring and alerting

---

## SQL migration application

### Recommended execution order

Apply in this order:

1. `001_create_sv_defi_protocol_registry.sql`
2. `002_create_sv_defi_markets_current.sql`
3. `003_create_sv_defi_positions_current.sql`
4. `004_create_sv_defi_totals_and_history.sql`
5. `005_create_sv_defi_prices_and_status.sql`

### Example manual application

```bash
MIGS=obsrvr-lake/serving-projection-processor/migrations
export PGHOST="<your-postgres-host>"
export PGPORT=5432
export PGUSER="<your-postgres-user>"
export PGPASSWORD="$SILVER_HOT_PASSWORD"
PSQL_SILVER="psql -d silver_hot"

$PSQL_SILVER -f "$MIGS/001_create_sv_defi_protocol_registry.sql"
$PSQL_SILVER -f "$MIGS/002_create_sv_defi_markets_current.sql"
$PSQL_SILVER -f "$MIGS/003_create_sv_defi_positions_current.sql"
$PSQL_SILVER -f "$MIGS/004_create_sv_defi_totals_and_history.sql"
$PSQL_SILVER -f "$MIGS/005_create_sv_defi_prices_and_status.sql"
```

### Notes

All migrations are written to be safely re-runnable:

- `CREATE SCHEMA IF NOT EXISTS`
- `CREATE TABLE IF NOT EXISTS`
- `CREATE INDEX IF NOT EXISTS`

---

## API design summary

### `GET /api/v1/semantic/defi/protocols`
Returns supported DeFi protocols from `sv_defi_protocols`.

### `GET /api/v1/semantic/defi/markets`
Returns current markets from `sv_defi_markets_current`.

### `GET /api/v1/semantic/defi/status`
Returns status/freshness from `sv_defi_protocol_status`.

### `GET /api/v1/semantic/defi/exposure?address=...`
Returns one-row aggregate exposure from `sv_defi_user_totals_current`.

### `GET /api/v1/semantic/defi/positions?address=...`
Returns normalized positions from `sv_defi_positions_current`.
Can optionally join components or aggregate summary.

### `GET /api/v1/semantic/defi/positions/{position_id}`
Returns one position plus its rows from `sv_defi_position_components_current`.

### `GET /api/v1/semantic/defi/positions/history?address=...`
Returns history from `sv_defi_user_totals_history`.

Full OpenAPI-style definitions are in:

- `stellar-query-api/docs/defi-openapi.yaml`

---

## Processor architecture to add

### 1. DeFi protocol registry loader

Responsibilities:
- seed and maintain `sv_defi_protocols`
- seed and maintain `sv_defi_protocol_contracts`
- allow manual and code-managed configuration

Suggested implementation:
- start with code- or file-backed seed data
- later add admin mutation endpoints if needed

### 2. DeFi position processor

New service recommended:
- `defi-position-processor/`

Responsibilities:
- consume protocol registry
- resolve protocol contracts and markets
- read from silver/serving/index sources
- compute user positions incrementally
- upsert `sv_defi_positions_current`
- upsert `sv_defi_position_components_current`
- upsert `sv_defi_user_totals_current`
- update `sv_defi_protocol_status`

Inputs likely needed:
- `contract_events`
- `contract_data_current`
- token balances
- market metadata
- price inputs

### 3. DeFi history snapshotter

Responsibilities:
- periodically snapshot current totals and positions into history tables

Outputs:
- `sv_defi_user_totals_history`
- `sv_defi_position_history`

### 4. DeFi pricing input service/processor

Responsibilities:
- produce `sv_defi_prices_current`
- mark stale/degraded sources
- support multiple source types:
  - oracle
  - DEX TWAP
  - fixed/manual
  - derived price

---

## Suggested processor interfaces

Each supported protocol should have an adapter implementing a common interface.

Suggested conceptual interface:

```go
type ProtocolAdapter interface {
    ProtocolID() string
    DiscoverMarkets(ctx context.Context) ([]Market, error)
    ResolveUserPositions(ctx context.Context, address string) ([]Position, error)
    ComputePositionComponents(ctx context.Context, p Position) ([]Component, error)
    ComputeValuation(ctx context.Context, p Position, prices PriceBook) (Valuation, error)
    ComputeRisk(ctx context.Context, p Position, prices PriceBook) (RiskMetrics, error)
}
```

Initial protocol candidates:
- Blend
- Aquarius
- Soroswap
- FxDAO

---

## Data freshness and degraded mode

Every DeFi API response should include:

- `as_of_ledger`
- `as_of_time`
- `freshness_seconds`
- `data_status`

Recommended `data_status` values:
- `ok`
- `degraded`
- `stale_prices`
- `partial_protocol_coverage`
- `backfill_in_progress`
- `protocol_unavailable`

This requirement is specifically important for the target product because low-latency/high-availability is part of the evaluation criteria.

---

## Integration points in `stellar-query-api`

Recommended additions:

- `go/handlers_defi.go`
- `go/routes_defi.go`
- reader methods on top of `serving.sv_defi_*` tables
- optional analyst guide additions for DeFi examples

The OpenAPI spec in `stellar-query-api/docs/defi-openapi.yaml` can be used as the basis for handler annotations and documentation.

---

## Recommended implementation order

### Phase 1 — registry and current-state foundation
- apply SQL migrations
- seed `sv_defi_protocols`
- seed `sv_defi_protocol_contracts`
- implement current market + current position processor
- add `/semantic/defi/protocols`
- add `/semantic/defi/markets`
- add `/semantic/defi/status`

### Phase 2 — user-facing portfolio endpoints
- populate `sv_defi_positions_current`
- populate `sv_defi_position_components_current`
- populate `sv_defi_user_totals_current`
- add `/semantic/defi/exposure`
- add `/semantic/defi/positions`
- add `/semantic/defi/positions/{position_id}`

### Phase 3 — historical analytics
- snapshot `sv_defi_user_totals_history`
- snapshot `sv_defi_position_history`
- add `/semantic/defi/positions/history`

---

## What this design intentionally does not do yet

This document defines the data model and API contract, but does not yet implement:

- protocol-specific adapter code
- handler code in `stellar-query-api`
- price-source selection strategy implementation
- backfill jobs for historical DeFi state
- admin APIs for mutating protocol registry data

Those should be implemented after agreeing on the schema and endpoint contract in this document.

---

## Summary

This implementation adds a dedicated DeFi registry and serving layer to Obsrvr Lake instead of trying to overload the existing token/contract registries.

It gives Obsrvr Lake a concrete path to support:

- production DeFi user portfolio APIs
- multi-protocol normalized position queries
- historical exposure charts
- degraded/freshness-aware responses

The concrete artifacts in this repo now include:

- actual SQL migration files for all proposed `serving.sv_defi_*` tables
- an OpenAPI-style spec for the `/api/v1/semantic/defi/*` endpoints

These should be treated as the contract for the next implementation phase.
