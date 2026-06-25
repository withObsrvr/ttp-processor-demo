# Audit: Stellar Query API vs stellardev Requirements

Date: 2026-06-14

## Context

Stellar dev is standing up his own indexer because existing options did not meet his needs: missing events, poor documentation, dead endpoints, and architectures that force him either to stream everything into his own database or operate a full indexing system.

His stated desired product is a simple semantic hosted query API: something he can pay for and consume directly, similar to market-data broker APIs, but for Soroban/Stellar.

This audit checks the actual `obsrvr-lake/stellar-query-api` codebase against his explicit requirements. It also notes the production delivery context: the API is intended to sit behind `obsrvr-gateway`, and users create/manage API keys in `obsrvr-console`. It distinguishes:

- data that exists in the lake,
- data exposed through a clean endpoint,
- query-layer gaps,
- indexing/projection gaps,
- and hosted/SaaS delivery gaps.

---

## 1. Coverage table

| # | Requirement | Status | Endpoint / handler / notes |
|---|---|---|---|
| 1 | Query ledger entries by contract ID without knowing ledger keys | ✅ Mostly covered | `GET /api/v1/silver/soroban/contract-data?contract_id=C...` → `go/handlers_silver.go: HandleContractData`. Also `GET /api/v1/silver/contracts/{id}/storage` → `go/handlers_silver.go: HandleContractStorage`. Both accept only `contract_id`; `key_hash` is optional. Backed by `contract_data_current`. Caveat: response is mostly raw storage/XDR and key hash, not decoded semantic entries. |
| 2 | All balances for an account across all assets, including arbitrary tokens | 🟡 Partial | `GET /api/v1/silver/accounts/{id}/balances` → `go/handlers_silver.go: HandleAccountBalances` returns XLM + classic trustlines from `accounts_current` / `trustlines_current`. Soroban token holdings exist separately at `GET /api/v1/silver/address/{addr}/token-balances` → `go/handlers_sep41.go: HandleAddressTokenPortfolio`, derived from `token_transfers_raw`. There is no single endpoint combining XLM, classic trustlines, and arbitrary Soroban token balances. Generic `address_balances_current` data exists in `go/silver_hot_reader.go: GetAddressBalancesCurrent`, but no public generic endpoint exposes it. |
| 3 | All interactions between two addresses | ❌ Missing | No endpoint accepts two addresses and returns relationship history. Relevant data exists across `token_transfers_raw`, `semantic_flows_value`, `semantic_activities`, `enriched_history_operations`, `contract_invocations_raw`, `contract_invocation_calls`, generic events, and transaction call graph tables. This is mostly a query-layer/composition gap for v1, with possible new indexing needed for deep Soroban address extraction. |
| 4 | Live Soroban state: all currently-live entries for a contract by contract ID, no replay | 🟡 Partial | `GET /api/v1/silver/soroban/contract-data?contract_id=...` and `GET /api/v1/silver/contracts/{id}/storage` list current-looking storage by contract ID. `HandleContractStorage` joins `contract_data_current` to `ttl_current`. Gaps: live correctness is not fully proven; deleted entries may not be removed; expired/evicted TTL rows are not clearly filtered; response is raw XDR rather than decoded domain records. |
| 5 | Historical balances for any contract/token | 🟡 Partial / mostly missing as clean API | SEP-41 transfer history exists via `GET /api/v1/silver/tokens/{contract_id}/transfers`. Current holder balances exist via `/tokens/{contract_id}/balances`; single current balance via `/tokens/{contract_id}/balance/{address}`. Classic account snapshots exist via `/silver/accounts/history`. `balance_changes` exists in `silver-history-loader`, and `address_balances_current` exists hot-side, but no endpoint exposes historical balance timeseries for arbitrary contract/token + address. |
| 6 | Historical transactions for an account | 🟡 Partial | `GET /api/v1/silver/accounts/{id}/activity` → `go/account_activity.go: HandleAccountActivity`, but `UnifiedSilverReader.GetAccountActivity` is hot-only/commented as not full history. `/api/v1/semantic/activities?account=...` can query cold semantic activities, but this is activity-level, not full transaction history. Bronze `/api/v1/bronze/transactions` is ledger-range only, not account-indexed. No clean full historical account transactions endpoint. |
| 7 | Historical events queryable without pre-saving them | ✅/🟡 | Generic raw contract events exist: `GET /api/v1/silver/events/generic` → `go/handlers_generic_events.go: HandleGenericEvents`; `GET /api/v1/silver/events/contract/{contract_id}` → `HandleContractGenericEvents`; bronze range endpoint `/api/v1/bronze/contract_events` → `go/query_service.go: HandleContractEvents`. Token/CAP-67 events exist at `/api/v1/silver/events`, `/events/by-contract`, `/address/{addr}/events`, `/tx/{hash}/events` in `go/handlers_events.go`. Good raw coverage; semantic decoding/classification is partial. |
| 8 | Pay-and-consume hosted model: auth, API keys, multi-tenancy, rate limiting, metering, docs | 🟡 Covered by adjacent gateway/console, not by this service directly | `stellar-query-api` itself has no customer auth middleware: `go/app.go` only chains recovery, request ID, logging, and CORS; `go/admin_auth.go` protects admin routes only. However production delivery is through `/lake/` in `obsrvr-gateway`: `obsrvr-gateway/internal/server/server.go` mounts `s.lakeProxy` behind `APIKeyAuth`, `RateLimitMiddleware`, and `WithUsageTracking`. Gateway auth validates `Authorization: Api-Key ...` in `obsrvr-gateway/internal/middleware/auth.go`; rate limits are in `internal/middleware/ratelimit.go`; usage records are stored in `api_usage`. Users create/manage keys in `obsrvr-console` via `apps/obsrvr_gateway/views.py:create_gateway_api_key` and `apps/obsrvr_gateway/client.py:create_key`. Remaining gap: docs/routing must make clear that external consumers use gateway URLs, not raw `stellar-query-api` URLs. |

---

## 2. The relationship-query gap

The hardest and most differentiating requirement is:

> Get me all the interactions X address had with Y address.

There is no endpoint today that directly answers this.

### Closest existing pieces

Direct token/value movement:

- `token_transfers_raw`
- `semantic_flows_value`
- `GET /api/v1/silver/transfers?from_account=&to_account=` → `go/handlers_silver.go: HandleTokenTransfers`

Single-account activity:

- `GET /api/v1/silver/accounts/{id}/activity` → `go/account_activity.go: HandleAccountActivity`
- This is one-sided and hot-biased.

Contract invocation/call graph data:

- `contract_invocations_raw`
- `contract_invocation_calls`
- `GET /api/v1/silver/contracts/{id}/recent-calls`
- `GET /api/v1/silver/tx/{hash}/call-graph`

Semantic activity/flows:

- `semantic_activities`
- `semantic_flows_value`
- `GET /api/v1/semantic/activities?account=...`
- `GET /api/v1/semantic/flows?account=...`

Raw events:

- `contract_events_stream_v1`
- `GET /api/v1/silver/events/generic`

### Query-layer vs indexing work

A first version is mainly query-layer work if “interaction” includes:

- X sent value to Y,
- Y sent value to X,
- X called contract Y,
- contract X called contract Y,
- X and Y co-appeared in the same token transfer/event/call graph.

A robust version needs new indexing if “interaction” includes:

- addresses buried in arbitrary Soroban event topics/data,
- addresses inside SCVal function arguments,
- smart-wallet signer/controller relationships,
- indirect interactions through routers, pools, aggregators, or wallet contracts.

### Recommended API shape

```http
GET /api/v1/silver/relationships/{address_a}/{address_b}
```

Recommended materialized edge model:

```text
ledger_sequence
closed_at
transaction_hash
interaction_type: transfer | contract_call | co_event | invocation_arg | signer | smart_wallet
address_a
address_b
direction
asset / contract_id / function_name
amount
source_table
confidence
```

This is likely a high-value differentiator because generic indexers generally expose raw events or contract calls, not semantic relationships.

Prism overlap: **very high**. Explorers and wallets both need relationship views.

---

## 3. The live-state gap

The API has endpoints that look aligned with the requirement:

- `GET /api/v1/silver/soroban/contract-data?contract_id=C...`
- `GET /api/v1/silver/contracts/{id}/storage`

They satisfy the “do not make me compute ledger keys” part.

However, live-state completeness/correctness is not fully closed.

### Code observations

`contract_data_current` schema is in:

- `obsrvr-lake/silver-realtime-transformer/go/schema/init_silver_hot_complete.sql`

It is keyed by:

```sql
PRIMARY KEY (contract_id, key_hash)
```

The realtime transformer reads current rows from Bronze with:

- `silver-realtime-transformer/go/bronze_reader.go: QueryContractDataSnapshot`
- `silver-realtime-transformer/go/bronze_cold_reader.go: QueryContractDataSnapshot`

Those queries filter:

```sql
WHERE ledger_sequence BETWEEN $1 AND $2
  AND deleted = false
```

The writer upserts into `contract_data_current` with:

- `silver-realtime-transformer/go/transformer.go: transformContractDataCurrent`
- `silver-realtime-transformer/go/silver_writer_batch.go: contractDataCurrentConflict`

I did not find corresponding deletion logic that removes a previously-current row when a later `deleted=true` row appears.

`HandleContractStorage` joins TTL data:

```sql
FROM contract_data_current cd
LEFT JOIN ttl_current t ON cd.key_hash = t.key_hash
WHERE cd.contract_id = $1
```

But it does not clearly filter out expired rows such as:

```sql
AND COALESCE(t.expired, false) = false
```

`HandleContractData` does not join TTL at all.

### Sorobandomains test case

For stellar dev's example — “tell me the contract ID and get all registered domains” — today he could likely list raw current-looking storage rows by contract ID. But to confidently reconstruct the live set of registered domains, the API must guarantee:

1. deleted rows are removed from `contract_data_current`,
2. evicted/expired entries are excluded or clearly marked,
3. keys/values are decoded enough for the client to identify domain records,
4. pagination can enumerate the full contract state.

Current status: **partial**.

---

## 4. The pay-and-consume gap

Important correction: the raw `stellar-query-api` service is not the intended public edge. It sits behind `obsrvr-gateway`, and users get/manage API keys inside `obsrvr-console`.

### Raw `stellar-query-api` service

The service itself still has no customer auth middleware. Its middleware chain in `go/app.go` is:

```go
return chainMiddleware(
    router,
    recoverPanicMiddleware,
    requestIDMiddleware,
    requestLoggingMiddleware,
    corsMiddleware,
)
```

`go/admin_auth.go` only protects selected admin routes with `X-Admin-Token`. So if this service were exposed directly, the pay-and-consume requirement would be missing.

### `obsrvr-gateway` delivery layer

The gateway does provide the missing SaaS edge pieces:

- API-key auth: `obsrvr-gateway/internal/middleware/auth.go: APIKeyAuth`
  - accepts `Authorization: Api-Key ...`
  - also supports `?api_key=` fallback
  - validates keys via `APIKeyService`
  - checks expiry and allowed endpoint types
  - adds API key and team ID to request context
- Lake/API proxy mounting: `obsrvr-gateway/internal/server/server.go`
  - mounts the Stellar lake/query API under `/lake/` when `s.lakeProxy.IsConfigured()`
  - authenticated routes include `/horizon/`, `/rpc/`, `/query/`, and `/lake/`
- Rate limiting: `obsrvr-gateway/internal/middleware/ratelimit.go`
  - emits `X-RateLimit-*` headers
  - tracks violations
  - supports per-key overrides
- Usage metering: `obsrvr-gateway/internal/middleware/auth.go: WithUsageTracking`
  - records `api_usage` for billing/analytics
  - weights query/events endpoints more heavily
- Admin APIs for teams, keys, usage:
  - `obsrvr-gateway/internal/server/server.go: setupAdminRoutes`
  - `POST /admin/v1/teams/{id}/keys`
  - `GET /admin/v1/teams/{id}/usage`
  - `GET /admin/v1/teams/{id}/usage/records`

Gateway migrations also include:

- `obsrvr-gateway/internal/migrations/001_initial_schema.sql` for `api_keys`, `api_usage`, teams, audit log
- `obsrvr-gateway/internal/migrations/002_rate_limiting.sql` for rate-limit support

### `obsrvr-console` key management

Users manage Gateway keys in Console:

- `obsrvr-console/apps/obsrvr_gateway/views.py: gateway_view` lists keys and usage
- `obsrvr-console/apps/obsrvr_gateway/views.py: create_gateway_api_key` creates keys for subscribed teams
- `obsrvr-console/apps/obsrvr_gateway/views.py: revoke_gateway_api_key` revokes keys
- `obsrvr-console/apps/obsrvr_gateway/client.py: create_key` calls Gateway admin API and grants endpoints: `horizon`, `rpc`, `events`, `tx_builder`, `query`, `lake`
- `obsrvr-console/apps/obsrvr_gateway/client.py: get_team_usage` and `list_usage_records` back usage UI

### Remaining delivery-model gap

The pay-and-consume infrastructure mostly exists in adjacent services, but this audit should verify these integration details before calling requirement 8 fully closed:

1. The externally documented URL should be the Gateway `/lake/...` path, not the raw `stellar-query-api` path.
2. Gateway `/lake/` should preserve all required Stellar Query API routes and query strings.
3. Console plans/subscriptions should clearly entitle access to these query endpoints.
4. External docs should show key creation, auth header, limits, example requests, and error semantics.
5. Usage metering should classify these semantic query routes correctly for billing.

Updated status: **partial-to-mostly-covered by gateway/console**, not missing. The remaining work is productization/docs/integration verification, not building auth from scratch.
---

## 5. Prioritized gap list

### P0 — Gateway/Console integration + public docs for pay-and-consume access

**Missing:** Not the core auth/rate-limit/metering system — that exists in `obsrvr-gateway` and `obsrvr-console`. What remains is integration verification and public-product polish for this specific Stellar Query API surface.

**Effort type:** Product/API integration and docs, not indexing.

**Why first:** The prospect's desired buying motion depends on knowing the hosted Gateway URL, creating a key in Console, and making successful authenticated requests without talking to us.

**Implementation checklist:**

- Document the public Gateway route for these endpoints, likely `/lake/...`.
- Confirm Gateway `/lake/` proxies every required `stellar-query-api` route.
- Add examples using `Authorization: Api-Key ...`.
- Confirm Console-created keys include `lake`/`query` permissions.
- Confirm usage metering and rate-limit headers appear on semantic query requests.
- Add a quickstart specifically for wallet builders.

**Prism overlap:** High.

---

### P0 — Relationship query between two addresses

**Missing:** First-class semantic relationship endpoint.

**Effort type:** v1 query-layer over existing tables; robust version may require new edge indexing.

**Existing data:**

- `token_transfers_raw`
- `semantic_flows_value`
- `semantic_activities`
- `contract_invocations_raw`
- `contract_invocation_calls`
- generic events

**Candidate route:**

```http
GET /api/v1/silver/relationships/{address_a}/{address_b}
```

**Why important:** This is the most differentiated capability versus generic indexers. It maps to how wallet builders think: relationships, not tables.

**Prism overlap:** Very high.

---

### P0/P1 — Harden live contract state correctness

**Missing:** Proven live-only `contract_data_current` semantics.

**Effort type:** Projection/indexing plus query polish.

**Needed work:**

- Delete/update `contract_data_current` when newer Bronze rows have `deleted=true`.
- Filter or clearly expose TTL expired/evicted status.
- Ensure restored keys are represented correctly.
- Add tests around delete/evict/restore/TTL expiry.
- Optionally add `live_only=true` default behavior to storage endpoints.
- Consider decoded key/value helpers for common schemas.

**Existing data:**

- `contract_data_snapshot_v1`
- `ttl_current`
- `evicted_keys`
- `restored_keys`
- `contract_data_current`

**Prism overlap:** Very high.

---

### P1 — One-call portfolio balances across XLM, classic assets, and arbitrary Soroban tokens

**Missing:** One clean endpoint for all balances.

**Effort type:** Mostly query-layer.

**Existing data/endpoints:**

- `/api/v1/silver/accounts/{id}/balances` for XLM + trustlines,
- `/api/v1/silver/address/{addr}/token-balances` for Soroban token holdings,
- `address_balances_current` in hot reader.

**Candidate route:**

```http
GET /api/v1/silver/addresses/{addr}/balances
```

Return unified rows with asset type, contract ID if applicable, raw/display balance, decimals/source, and last updated ledger.

**Prism overlap:** High.

---

### P1 — Historical balances for arbitrary token/account

**Missing:** Historical balance timeseries endpoint for any contract/token.

**Effort type:** Query-layer if `balance_changes` is complete; otherwise projection/backfill work.

**Candidate route:**

```http
GET /api/v1/silver/addresses/{addr}/balances/history?asset=...|contract_id=...
```

or:

```http
GET /api/v1/silver/tokens/{contract_id}/balances/{address}/history
```

**Existing data:**

- `balance_changes` from `silver-history-loader`,
- `token_transfers_raw`,
- classic account/trustline snapshots.

**Prism overlap:** High.

---

### P1 — Full historical account transactions

**Missing:** Account-indexed historical transactions endpoint.

**Effort type:** Query-layer initially; better with materialized `account_transactions` table.

**Candidate route:**

```http
GET /api/v1/silver/accounts/{id}/transactions
```

Should include classic and Soroban transactions, pagination, ledger/time filters, success filter, and semantic summary.

**Existing data:**

- `enriched_history_operations`,
- `semantic_activities`,
- `transactions_row_v2`,
- `contract_invocations_raw`,
- `token_transfers_raw`.

**Prism overlap:** High.

---

### P2 — Event API polish

**Missing:** Stronger semantic event decoding/classification and docs.

**Effort type:** Query/API layer plus optional classifiers.

**Existing endpoints:**

- `/api/v1/silver/events/generic`,
- `/api/v1/silver/events/contract/{contract_id}`,
- `/api/v1/silver/events`,
- `/api/v1/silver/events/by-contract`,
- `/api/v1/silver/address/{addr}/events`,
- `/api/v1/silver/tx/{hash}/events`.

**Prism overlap:** Medium/high.

---

## 6. Verdict

The codebase is closer than a generic indexer, especially on contract-data-by-contract, raw events, token transfers, call graphs, and semantic tables. stellar dev could consume a useful subset today: contract storage listing, raw historical events, token transfer history, and some account/asset endpoints. But the blocking gaps are real: no relationship query, incomplete confidence around live contract state, no one-call all-assets balance endpoint, and no clean historical account transaction/balance APIs for arbitrary tokens. The paid access layer is mostly handled by `obsrvr-gateway` and `obsrvr-console`, so the shortest path to “he can pay and consume” is: verify/document the Gateway `/lake/` route and Console key flow, ship a first-class `/relationships/{a}/{b}` endpoint over existing tables, expose unified address balances from `address_balances_current`, and harden/filter live contract storage state.
