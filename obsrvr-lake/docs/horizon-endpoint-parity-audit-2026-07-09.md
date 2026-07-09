# Stellar Horizon Endpoint Parity Audit - 2026-07-09

## Purpose

This audit compares the public Stellar Horizon HTTP surface in
`/home/tillman/Documents/stellar-horizon/internal/httpx/router.go` with the routes currently exposed
by `obsrvr-lake/stellar-query-api`.

The important distinction is:

- **Data parity** means Obsrvr Lake has the data or a close semantic primitive.
- **Endpoint parity** means a Horizon client could call the same route with the same parameter and
  response conventions.

Today, `stellar-query-api` has a lot of the data primitives, but it is not a Horizon-compatible API.
Most routes use Obsrvr-specific paths, response bodies, filters, cursors, and metadata.

## Status Legend

- **Covered**: Query API has a route that serves the same resource class with close semantics.
- **Partial**: Query API has the data or a nearby route, but route shape, filters, response shape, or
  history coverage differs.
- **Missing**: No current Query API route or reader supports the Horizon use case directly.
- **Delegate**: Should remain outside `stellar-query-api` and be handled by Horizon/RPC/Gateway.

## Public Horizon Surface

Horizon's public routes, excluding internal admin/metrics routes, are grouped as:

- root/network state: `/`, `/health`, `/fee_stats`
- accounts: `/accounts`, `/accounts/{id}`, `/accounts/{id}/data/{key}`, account history subroutes
- assets: `/assets`
- ledgers: `/ledgers`, `/ledgers/{seq}`, ledger history subroutes
- transactions: `/transactions`, `/transactions/{hash}`, transaction history subroutes, submission
- operations, payments, effects
- offers, trades, trade aggregations, order book, paths
- claimable balances and liquidity pools
- optional friendbot

## Parity Matrix

| Horizon route | Current Query API equivalent | Status | Notes |
| --- | --- | --- | --- |
| `GET /health` | `GET /health` | Covered | Health exists, but response shape is Obsrvr-specific. |
| `GET /` | `/api/v1/silver/stats/network`, `/api/v1/silver/data-boundaries` | Partial | Query API can expose network/data state, but not Horizon root/HAL links. |
| `GET /fee_stats` | `/api/v1/silver/stats/fees` | Partial | Semantically close; not Horizon response shape. |
| `GET /accounts` | `/api/v1/silver/accounts` | Partial | Account listing exists with Obsrvr filters/cursor/shape. |
| `GET /accounts/{account_id}` | `/api/v1/silver/accounts/current?account_id=...`, `/api/v1/silver/accounts/{id}/balances`, `/api/v1/silver/accounts/signers` | Partial | Data is split across several endpoints; no single Horizon account object with embedded balances/signers/data. |
| `GET /accounts/{account_id}/data/{key}` | none | Missing | Account data entries are not exposed as a current-state lookup. |
| `GET /accounts/{account_id}/offers` | `/api/v1/silver/accounts/{id}/offers` | Covered | Close endpoint-level equivalent, not Horizon response format. |
| `GET /accounts/{account_id}/transactions` | `/api/v1/silver/accounts/{id}/transactions` | Covered | Full-history account transactions route exists with index/coverage metadata. Response is not Horizon-shaped. |
| `GET /accounts/{account_id}/operations` | `/api/v1/silver/operations/enriched?account_id=...` | Partial | Data exists through a global filtered route; no nested Horizon route or operation response shape. |
| `GET /accounts/{account_id}/payments` | `/api/v1/silver/payments?account_id=...` | Partial | Data exists through a global filtered route. |
| `GET /accounts/{account_id}/effects` | `/api/v1/silver/effects?account_id=...` | Partial | Data exists through a global filtered route. |
| `GET /accounts/{account_id}/trades` | `/api/v1/silver/trades?account_id=...` | Partial | Data exists through a global filtered route. |
| `GET /assets` | `/api/v1/silver/assets` | Covered | Query API also has asset detail, holders, stats, links, and pairs. |
| `GET /ledgers` | `/api/v1/bronze/ledgers`, `/api/v1/silver/ledgers/recent` | Partial | Raw/recent ledger listing exists, but no Horizon-style full ledger collection route. |
| `GET /ledgers/{ledger_id}` | `/api/v1/silver/ledgers/{seq}`, `/api/v1/silver/ledger/{seq}` | Covered | Single ledger lookup exists. |
| `GET /ledgers/{ledger_id}/transactions` | `/api/v1/silver/transactions/summaries?ledger_sequence=...`, `/api/v1/silver/ledgers/{seq}/full` | Partial | Data can be assembled, but no Horizon nested transactions route. |
| `GET /ledgers/{ledger_id}/operations` | `/api/v1/silver/operations/enriched?start_ledger=N&end_ledger=N` | Partial | Data exists through range filter; no nested route/response format. |
| `GET /ledgers/{ledger_id}/payments` | `/api/v1/silver/operations/enriched?payments_only=true&start_ledger=N&end_ledger=N` | Partial | Could be served by the operations filter; no nested route. |
| `GET /ledgers/{ledger_id}/effects` | `/api/v1/silver/effects?ledger_sequence=N` | Partial | Data exists through a global filtered route. |
| `GET /transactions` | `/api/v1/bronze/transactions`, `/api/v1/silver/transactions/recent` | Partial | Raw/recent routes exist; no full Horizon transaction collection with paging across all history. |
| `GET /transactions/{tx_id}` | `/transactions/{hash}`, `/api/v1/index/transactions/{hash}`, `/api/v1/silver/tx/{hash}/full`, `/api/v1/silver/tx/{hash}/decoded` | Partial | Fast lookup and richer decoded views exist, but not Horizon transaction JSON. |
| `GET /transactions/{tx_id}/effects` | `/api/v1/silver/tx/{hash}/effects`, `/api/v1/silver/effects/transaction/{tx_hash}` | Covered | Close route-level equivalent, not Horizon response format. |
| `GET /transactions/{tx_id}/operations` | `/api/v1/silver/operations/enriched?tx_hash=...`, `/api/v1/silver/tx/{hash}/full` | Partial | Data exists; no nested Horizon route. |
| `GET /transactions/{tx_id}/payments` | `/api/v1/silver/operations/enriched?tx_hash=...&payments_only=true` | Partial | Data exists through filters; no nested route. |
| `POST /transactions` | none in Query API | Delegate | Transaction submission should remain Horizon/RPC/Gateway, not lake query. |
| `POST /transactions_async` | none in Query API | Delegate | Same as transaction submission. |
| `GET /operations` | `/api/v1/silver/operations/enriched` | Covered | Data exists; response is enriched/Obsrvr-shaped rather than Horizon operation records. |
| `GET /operations/{id}` | none | Missing | No operation-by-TOID route is exposed. |
| `GET /operations/{op_id}/effects` | none | Missing | Effects have `operation_id`, but no scoped operation-effects route. |
| `GET /payments` | `/api/v1/silver/payments` | Covered | Payment-operation collection exists. |
| `GET /effects` | `/api/v1/silver/effects` | Covered | Effect collection exists. |
| `GET /trades` | `/api/v1/silver/trades` | Covered | Trade collection exists. |
| `GET /trade_aggregations` | `/api/v1/silver/trades/stats`, `/api/v1/silver/prices/{base}/{counter}/ohlc` | Partial | Similar analytics exist, but not Horizon's trade aggregation contract. |
| `GET /offers` | `/api/v1/silver/offers` | Covered | Offer collection exists. |
| `GET /offers/{offer_id}` | `/api/v1/silver/offers/{id}` | Covered | Single offer lookup exists. |
| `GET /offers/{offer_id}/trades` | none | Missing | Trades do not currently expose offer-id scoped filtering. |
| `GET /order_book` | `/api/v1/silver/offers/pair` | Partial | Pair-filtered offers can approximate an order book, but no Horizon order book shape/depth contract. |
| `GET /paths`, `/paths/strict-receive`, `/paths/strict-send` | none | Delegate | Path finding is live-state/routing behavior; keep using Horizon/RPC/Gateway unless Obsrvr intentionally builds a pathfinder. |
| `GET /claimable_balances` | `/api/v1/silver/claimable-balances` | Covered | Route exists with dash naming and Obsrvr response. |
| `GET /claimable_balances/{id}` | `/api/v1/silver/claimable-balances/{id}` | Covered | Single current-state lookup exists. |
| `GET /claimable_balances/{id}/transactions` | none | Missing | Would require claimable-balance-scoped history indexing/filtering. |
| `GET /claimable_balances/{id}/operations` | none | Missing | Would require claimable-balance-scoped operation history. |
| `GET /liquidity_pools` | `/api/v1/silver/liquidity-pools` | Covered | Route exists with dash naming and Obsrvr response. |
| `GET /liquidity_pools/{liquidity_pool_id}` | `/api/v1/silver/liquidity-pools/{id}` | Covered | Single current-state lookup exists. |
| `GET /liquidity_pools/{id}/transactions` | none | Missing | Would require liquidity-pool-scoped transaction history. |
| `GET /liquidity_pools/{id}/operations` | none | Missing | Would require liquidity-pool-scoped operation history. |
| `GET /liquidity_pools/{id}/effects` | none | Missing | Would require liquidity-pool-scoped effect history. |
| `GET /liquidity_pools/{id}/trades` | none | Missing | Trades exist, but not filtered by liquidity pool id. |
| `GET/POST /friendbot` | none | Delegate | Friendbot belongs outside lake query. |

## What We Have That Horizon Does Not

`stellar-query-api` already goes well beyond Horizon in Soroban and analytics areas:

- contract metadata, storage, recent calls, callers/callees, call graph, hierarchy
- Soroban contract data, TTL, evicted/restored keys, config, contract code
- decoded transaction, semantic transaction, transaction diffs, receipts
- SEP-41 token balances/transfers/stats
- smart wallet/smart account lookup and state endpoints
- gold compliance/archive and semantic/DeFi endpoints

These should remain Obsrvr-native. Horizon parity should be an adapter layer over core readers, not a
replacement for the existing API.

## Main Gaps

1. **No Horizon-compatible response formatter.**
   Query API responses are plain Obsrvr JSON with `count`, `cursor`, `has_more`, `_meta`, etc. Horizon
   clients expect HAL-style `_links`, `_embedded.records`, `paging_token`, route-specific fields, and
   Horizon parameter names.

2. **Current account object is split.**
   Horizon's `GET /accounts/{id}` returns one account document with balances, signers, thresholds,
   flags, sponsorship, sequence, and data links. Query API exposes pieces separately.

3. **Nested history routes are mostly not mounted.**
   Many can be thin adapters over existing filtered routes:
   `/accounts/{id}/operations`, `/accounts/{id}/payments`, `/accounts/{id}/effects`,
   `/accounts/{id}/trades`, `/ledgers/{seq}/operations`, `/ledgers/{seq}/payments`,
   `/ledgers/{seq}/effects`, `/transactions/{hash}/operations`, and
   `/transactions/{hash}/payments`.

4. **Entity-scoped subresources need new indexes or filters.**
   These are not just path adapters:
   `/liquidity_pools/{id}/*`, `/claimable_balances/{id}/*`, `/offers/{id}/trades`,
   `/operations/{id}`, and `/operations/{id}/effects`.

5. **Order book, trade aggregations, and path finding are separate decisions.**
   Order book can probably be formatted from current offers. Trade aggregations can probably map onto
   OHLC/stats work with parameter translation. Path finding should be delegated unless Obsrvr wants to
   operate a pathfinder.

6. **Global full-history collections need care.**
   `GET /transactions` and `GET /ledgers` are expensive if implemented as naive hot+cold scans. They
   need either serving/index support, bounded defaults, or explicit documentation that Obsrvr exposes
   recent/filtered variants instead of Horizon's broad collection behavior.

## Recommended Implementation Shape

Build Horizon parity as a compatibility layer, not by reshaping the existing API.

Suggested route prefix:

- internal/direct: `/api/v1/horizon/...`
- gateway public option: `/horizon/v1/{network}/...` or another explicit compatibility prefix

The compatibility layer should:

1. Reuse existing readers and handlers where possible.
2. Translate Horizon path/query parameters into Obsrvr filter structs.
3. Format responses into Horizon-compatible collection and object shapes.
4. Emit Horizon-like links and paging tokens.
5. Keep mutation/live-routing endpoints delegated: transaction submission, async submission, friendbot,
   and path finding unless explicitly in scope.

## Suggested Slices

### Slice 1 - Read-only high-value adapter routes

Implement response formatting and adapters for routes that already have data support:

- `GET /accounts/{id}/transactions`
- `GET /accounts/{id}/operations`
- `GET /accounts/{id}/payments`
- `GET /accounts/{id}/effects`
- `GET /transactions/{hash}`
- `GET /transactions/{hash}/operations`
- `GET /transactions/{hash}/payments`
- `GET /transactions/{hash}/effects`
- `GET /operations`
- `GET /payments`
- `GET /effects`
- `GET /trades`

### Slice 2 - Current state object parity

Compose current-state responses:

- `GET /accounts/{id}`
- `GET /accounts/{id}/data/{key}`
- `GET /assets`
- `GET /offers`, `GET /offers/{id}`
- `GET /claimable_balances`, `GET /claimable_balances/{id}`
- `GET /liquidity_pools`, `GET /liquidity_pools/{id}`
- `GET /fee_stats`

### Slice 3 - Ledger and broad collections

Add or adapt:

- `GET /ledgers`
- `GET /ledgers/{seq}`
- `GET /ledgers/{seq}/transactions`
- `GET /ledgers/{seq}/operations`
- `GET /ledgers/{seq}/payments`
- `GET /ledgers/{seq}/effects`
- `GET /transactions`

This slice should be careful about serving/index support before exposing broad all-history behavior.

### Slice 4 - Missing scoped indexes

Add data support for:

- operation by id
- operation effects
- offer trades
- claimable-balance transactions/operations
- liquidity-pool transactions/operations/effects/trades

These are the first places where denormalized serving tables may be the right implementation, because
they are high-selectivity access paths over history.

## Bottom Line

Obsrvr Lake is already strong on **data parity** for the common read-only Horizon resources:
accounts, assets, ledgers, transactions, operations, payments, effects, offers, trades, claimable
balances, liquidity pools, and fee stats.

It is weak on **endpoint parity**:

- response shapes are not Horizon-compatible
- many nested routes are missing even where equivalent data exists
- several entity-scoped history routes need new indexes or serving tables
- transaction submission, friendbot, and path finding should be delegated rather than implemented in
  the lake query service

The fastest useful path is a read-only Horizon compatibility layer over the existing readers, starting
with account/transaction/operation/payment/effect routes and adding serving-backed indexes only where
the existing filters cannot answer the query efficiently.
