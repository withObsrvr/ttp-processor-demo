# Horizon Endpoint Parity — Implementation Plan (2026-07-09)

Companion to [horizon-endpoint-parity-audit-2026-07-09.md](horizon-endpoint-parity-audit-2026-07-09.md).
The audit established *what* is missing; this document is *how* to build it: a read-only Horizon
compatibility layer inside `stellar-query-api`, shipped in four Shape Up cycles.

## Load-Bearing Discoveries

These four facts, verified against the code, shape the whole plan:

1. **We already ship Horizon's exact response contract as Go structs.**
   `stellar-query-api` depends on `github.com/stellar/go-stellar-sdk v0.6.0` (go.mod:11), which
   contains `protocols/horizon` (Transaction, Account, Ledger, Offer, Trade, ClaimableBalance,
   LiquidityPool, FeeStats, Root), `protocols/horizon/operations` (all 27 op types + `TypeNames`),
   `protocols/horizon/effects` (full effect enum + `EffectTypeNames`), `support/render/hal`
   (Page/`_links`/`_embedded.records`/link builder), and `support/render/problem` (RFC-7807
   `application/problem+json`). The compat layer **populates these structs instead of hand-writing
   JSON** — response-shape parity is guaranteed by construction, and the same structs are what
   every Go Horizon client unmarshals into.

2. **The schema has the raw XDR a Horizon Transaction requires, but the current writers do not
   populate it.** `envelope_xdr`, `result_xdr`, `fee_meta_xdr`, and `signatures` are *mandatory*
   (non-omitempty) fields on `protocols/horizon.Transaction`. Bronze `transactions_row_v2` defines
   matching columns (`tx_envelope`, `tx_result`, `tx_meta`, `tx_fee_meta`, `tx_signers` —
   bronze_schema_full.sql:83–88), plus `max_fee`, `memo_type`/`memo`, `account_sequence`. But the
   hot `stellar-postgres-ingester` transaction insert currently writes only summary columns
   (`stellar-postgres-ingester/go/writer.go:1507–1565`), and the cold `stellar-history-loader`
   Parquet row type does not include the XDR/signature columns (`parquet_writer.go:135–157`).
   **Cycle 1 must first establish a populated transaction-resource source** — either by populating
   those columns in hot+cold, or by adding a bounded raw-ledger/XDR fallback. The reader is still a
   major component, but it is not reader-only work.

3. **Real TOIDs exist but the operation readers don't use them — and switching them in place would
   change existing `/silver` behavior.**
   Migration `008_add_enriched_operation_toids.sql` added `transaction_id`/`operation_id` (SEP-35
   TOIDs) to `enriched_history_operations`, but `GetEnrichedOperations*` still returns
   `operation_index AS operation_id` (silver_reader.go:466). Horizon's `paging_token` for
   transactions/operations *is* the TOID, and effects page on `"{op_toid}-{index}"` (effects
   already read the real TOID column — unified_duckdb_reader.go:2838). But existing Obsrvr
   operation JSON and cursors are built around the legacy `operation_id` value being the operation
   index (`pagination.go:13–17`). The Horizon layer should therefore use additive TOID fields or
   Horizon-specific readers, not silently replace existing `/silver` cursor/response semantics.

4. **Two genuine data-plane gaps require transformer work, not adapters.**
   (a) Classic account data entries (`manage_data` key/value current state) have **no silver table
   and no reader** — `GET /accounts/{id}/data/{key}` and the account resource's `data` field need a
   new `account_data_current` table. (b) Silver `trades` rows carry no
   `base_offer_id`/`counter_offer_id`/`liquidity_pool_id`/`base_is_seller` — the Horizon trade
   resource can only be partially populated until the transformer captures them.

## Strategy

Build parity as an **adapter layer over existing readers** (per the audit's recommendation):

- **Where:** inside `stellar-query-api` — new `routes_horizon_compat.go` +
  `handlers_horizon_*.go` files.
  No new service. Registered from `app.routes()` (app.go:32) exactly like
  `registerSilverRoutes`, using the same `application` struct fields
  (`unifiedDuckDBReader`, `silverHotReader`, `indexReader`, `queryService`, …).
- **Route prefix:** internal `/api/v1/horizon-compat/...` via a
  `mux.PathPrefix("/api/v1/horizon-compat").Subrouter()`. Do **not** use `/horizon/v1/{network}` at
  the gateway: `obsrvr-gateway` already owns `/horizon/{network}/*` for the live Horizon proxy, so
  `/horizon/v1/testnet/...` would be interpreted as network `v1`. With no gateway routing change,
  the public path is the existing lake proxy path:
  `/lake/v1/{network}/api/v1/horizon-compat/...`.
- **Responses:** populate SDK structs → wrap collections in `hal.Page` with
  `self`/`next`/`prev` links and `_embedded.records`; errors rendered as `problem.P`
  (`bad_request`, `not_found`, `before_history` 410, `timeout` 504 mapped from
  `isQueryTimeout`). Set the `Latest-Ledger` response header from the serving ledger stats.
- **Link base URL:** `hal.LinkBuilder` needs the *externally visible* base. New env
  `HORIZON_COMPAT_BASE_URL` (e.g.
  `https://gateway.withobsrvr.com/lake/v1/testnet/api/v1/horizon-compat`); falls back to
  `{scheme}://{host}/api/v1/horizon-compat` from the request.
- **Paging params:** Horizon conventions — `cursor`/`order` (`asc`|`desc`, default asc)/`limit`
  (default 10, max 200), cursor `now` → TOID after latest ledger, cursor before elder history →
  410 `before_history`. One shared `parseHorizonPageQuery()` translates these into the existing
  Obsrvr filter structs (`OperationFilters`, `EffectFilters`, `TradeFilters`,
  `AccountTransactionsFilters`, …).
- **Existing API untouched.** All Obsrvr-native routes, response shapes, and cursors stay as they
  are. The compat layer can read through existing readers, add columns to structs, or introduce
  Horizon-specific readers, but it must not change existing `/silver` cursor or field semantics.
- **Delegated forever** (documented, return 404 `not_implemented` problem or omit routes):
  `POST /transactions`, `POST /transactions_async`, `/friendbot`, `/paths*`. SSE streaming
  (`Accept: text/event-stream`) is out of scope for all cycles — plain JSON GET only.

## Current Deployed Status - 2026-07-10

Cycles 5A and 5B have been deployed to public testnet under
`/api/v1/horizon-compat`.

Implemented routes:

- `GET /api/v1/horizon-compat/fee_stats`
- `GET /api/v1/horizon-compat/ledgers`
- `GET /api/v1/horizon-compat/ledgers/{sequence}`
- `GET /api/v1/horizon-compat/transactions/{hash}`
- `GET /api/v1/horizon-compat/transactions/{hash}/operations`
- `GET /api/v1/horizon-compat/transactions/{hash}/payments`
- `GET /api/v1/horizon-compat/transactions/{hash}/effects`
- `GET /api/v1/horizon-compat/accounts/{id}`
- `GET /api/v1/horizon-compat/accounts/{id}/transactions`
- `GET /api/v1/horizon-compat/accounts/{id}/operations`
- `GET /api/v1/horizon-compat/accounts/{id}/payments`
- `GET /api/v1/horizon-compat/accounts/{id}/effects`
- `GET /api/v1/horizon-compat/operations`
- `GET /api/v1/horizon-compat/operations/{id}`
- `GET /api/v1/horizon-compat/operations/{id}/effects`
- `GET /api/v1/horizon-compat/payments`
- `GET /api/v1/horizon-compat/effects`

Cycle 5B moved Horizon transaction hydration for recent account transaction
history onto `serving.sv_transactions_recent`. The serving table now stores
`transaction_id`, `tx_envelope`, `tx_result`, `tx_meta`, `tx_fee_meta`, and
`tx_signers`; `stellar-query-api` reads those fields first for
`/transactions/{hash}` and `/accounts/{id}/transactions`.

Public testnet smoke on 2026-07-10 passed for:

- `/health`
- `/api/v1/horizon-compat/fee_stats`
- `/api/v1/horizon-compat/ledgers?limit=1&order=desc`
- `/api/v1/horizon-compat/accounts/GBTHMMFWTAPFAHRGS33LKETZYJKBTNEENRN47EDZMZPT2BNCJO47GVQG`
- `/api/v1/horizon-compat/accounts/GBTHMMFWTAPFAHRGS33LKETZYJKBTNEENRN47EDZMZPT2BNCJO47GVQG/transactions?limit=1&order=desc`
- `/api/v1/horizon-compat/transactions/366bc4543a8fe66e09c021af35377c78df6e90e57f85582a0aad1617fcc027e8`

Both transaction responses included non-empty `envelope_xdr`, `result_xdr`,
`result_meta_xdr`, `fee_meta_xdr`, and signatures.

Current operator docs:

- `stellar-query-api/docs/HORIZON_COMPAT_API.md`
- `docs/horizon-compat-deployment-runbook.md`
- `stellar-query-api/scripts/horizon_compat_smoke.py`

## Implementation Status — 2026-07-09

Started Cycle 1 in `stellar-query-api` under `/api/v1/horizon-compat`.

Implemented and tested:

- `GET /api/v1/horizon-compat/transactions/{hash}` — SDK `horizon.Transaction` response shape,
  hot+cold bronze lookup, and a fail-closed `503 data_unavailable` guard when mandatory
  XDR/signature columns are empty.
- `GET /api/v1/horizon-compat/transactions/{hash}/operations`
- `GET /api/v1/horizon-compat/transactions/{hash}/payments`
- `GET /api/v1/horizon-compat/transactions/{hash}/effects`
- `GET /api/v1/horizon-compat/accounts/{id}/operations`
- `GET /api/v1/horizon-compat/accounts/{id}/payments`
- `GET /api/v1/horizon-compat/accounts/{id}/effects`
- `GET /api/v1/horizon-compat/operations`
- `GET /api/v1/horizon-compat/payments`
- `GET /api/v1/horizon-compat/effects`

Additional blocker work completed:

- hot `stellar-postgres-ingester` now writes `tx_envelope`, `tx_result`, `tx_meta`,
  `tx_fee_meta`, and JSON `tx_signers` into `transactions_row_v2`, including conflict updates that
  fill existing rows when the new write has XDR.
- cold `stellar-history-loader` now carries the same fields through local transaction extraction,
  Parquet rows, and hot-buffer reload columns.
- hot Postgres migration `008_add_transaction_xdr_fields.sql` adds the five transaction XDR/signature
  columns for existing bronze hot databases.
- cold silver schema definitions now include `transaction_id` and `operation_id` on
  `enriched_history_operations`.
- Horizon operation/payment routes now use a Horizon-specific operation reader that requires non-null
  `transaction_id`/`operation_id` and pages on real operation TOIDs without changing `/silver`
  operation semantics.

The operation/payment/effect routes return Horizon SDK page containers (`operations.OperationsPage`,
`effects.EffectsPage`) with HAL links. They are useful route surface scaffolding, but they are **not
full Horizon operation/effect parity yet**:

- operation records are mostly `operations.Base`, with `payment` populated from enriched payment
  columns; full per-type detail population from transaction envelope XDR remains.
- operation paging now uses real TOIDs where `enriched_history_operations.operation_id` is populated;
  any historical/null-TOID ranges need a silver replay or repair before they can serve complete
  Horizon-compatible operation history.
- effect paging still adapts the existing Obsrvr effect cursor shape; `/operations/{id}/effects`
  still needs an operation-ID filter path.
- `cursor=now` is accepted as a no-op for plain JSON reads, but live-follow/SSE semantics remain out
  of scope.
- historical note from 2026-07-09: account transaction routes were blocked for historical data until
  `transactions_row_v2` could be replayed or sidefilled with the new XDR/signature columns. Cycle 5B
  resolved this for the testnet recent serving window via `serving.sv_transactions_recent`.

### New files (all in `obsrvr-lake/stellar-query-api/go/`)

| File | Contents |
| --- | --- |
| `routes_horizon_compat.go` | `registerHorizonCompatRoutes(router)` — subrouter, all route mounts |
| `horizon_render.go` | HAL page assembly, link builder wiring, problem+json error mapping, `Latest-Ledger` header |
| `horizon_params.go` | cursor/order/limit parsing, TOID codec helpers (wrap SDK `toid` pkg), translation into Obsrvr filter structs |
| `horizon_tx_reader.go` | new bronze `transactions_row_v2` resource reader (hot + cold w/ index hint) |
| `horizon_operation_reader.go` | Horizon-only operation reader using real `transaction_id`/`operation_id` TOIDs |
| `handlers_horizon_tx.go` | `/transactions/{hash}` + tx sub-routes + account/ledger tx collections |
| `handlers_horizon_ops.go` | operations/payments collections + `/operations/{id}` + per-type population from envelope XDR |
| `handlers_horizon_effects.go` | effects collections + type mapping |
| `handlers_horizon_state.go` | accounts, offers, claimable balances, liquidity pools, assets, order book |
| `handlers_horizon_ledger.go` | ledgers + fee_stats + root |
| `horizon_*_test.go` | fixture tests per handler group |

## Resource Population Matrix

Where each Horizon resource's fields come from, and what's missing:

| Resource | Primary source | Gaps to close |
| --- | --- | --- |
| Transaction | **new** transaction-resource reader; source must be populated `transactions_row_v2` XDR/signature columns or a raw-ledger/XDR fallback | Current hot+cold writers do not populate those XDR/signature columns; Cycle 1 must close this before claiming Horizon transaction parity. Also needs TOID, preconditions, fee-bump inner/outer split |
| Operation (typed) | `enriched_history_operations` for Base + payment-family; **tx envelope XDR decode** for full per-type details | add TOID projection via additive fields or Horizon-specific reader; do not replace existing `/silver` `operation_id` meaning; per-type detail population (§Cycle 1) |
| Effect | silver `effects` (real op TOID, `details_json`, type int + string) | verify type-int table matches Horizon `EffectTypeNames`; map `details_json` → typed fields; add operation-id filter for `/operations/{id}/effects` |
| Account | `accounts_current` (**table already has** flags, thresholds, master_weight, liabilities, sponsor, num_sponsored/sponsoring, sequence_ledger/time — reader just doesn't project them) + balances reader + signers reader | widen `AccountCurrent` query; `data` field needs new `account_data_current` (Cycle 4; emit `data: {}` until then, documented) |
| Ledger | `scanLedgers` column set (query_service.go:917) is Horizon-complete incl. `ledger_header` for `header_xdr` | paging token = TOID(seq,0,0) — trivial |
| Offer | `OfferCurrent` — already has price_r, sponsor, last_modified_ledger | none (Horizon-complete) |
| Trade | `SilverTrade` | **data gap:** no offer IDs / lp id / base_is_seller / price_r → transformer change (Cycle 4); until then partial resource |
| ClaimableBalance | `claimable_balances_current` — table stores `claimants` JSON; reader returns only count | widen reader to project claimants array |
| LiquidityPool | `LiquidityPoolCurrent` | missing `sponsor` (minor; omitempty) |
| AssetStat | `/silver/assets` reader | map to `AssetStat` fields; verify accounts/balances breakdowns available |
| FeeStats | **new query** over bronze `transactions_row_v2` last 5 ledgers (`fee_charged` + `max_fee` distributions p10–p99, mode, min/max) + `max_tx_set_size` for `ledger_capacity_usage` | new but straightforward |
| Root | config (network passphrase) + serving ledger stats (latest/elder ledger) | trivial |

## Cycle 1 — Scaffolding + Transaction/Operation/Effect History Routes

**Appetite:** 2 weeks. This is the audit's Slice 1 — the highest-value read paths.

```
COULD HAVE ───────────── join=transactions on ops; muxed-account fields
NICE TO HAVE ─────────── full per-type detail for all 27 op types; preconditions on tx
MUST HAVE ══════════════ scaffolding (HAL/problem/params/base-URL); populated tx XDR source;
                         bronze/raw transaction-resource reader;
                         GET /transactions/{hash}; account {transactions,operations,
                         payments,effects}; tx {operations,payments,effects};
                         global {operations,payments,effects,trades}; TOID paging
```

Routes: `GET /transactions/{hash}`, `/transactions/{hash}/{operations,payments,effects}`,
`/accounts/{id}/{transactions,operations,payments,effects}`, `/operations`, `/payments`,
`/effects`, `/trades`.

Steps:

1. **Scaffolding** — `routes_horizon_compat.go`, `horizon_render.go`, `horizon_params.go`. HAL page
   assembly via `hal.Page` + `PopulateLinks()`; problem+json mapping from existing error
   taxonomy (`isQueryTimeout` → 504 `timeout`, reader "not found" → `problem.NotFound`, bad
   params → `MakeInvalidFieldProblem`); `HORIZON_COMPAT_BASE_URL`; `Latest-Ledger` header from
   `GetServingRecentLedgers`/serving stats.
2. **Transaction XDR source check + fix** — add a failing fixture/smoke test that proves whether
   `transactions_row_v2.tx_envelope`, `tx_result`, `tx_meta`, `tx_fee_meta`, and `tx_signers` are
   populated in hot and cold. If they are empty, this cycle includes the minimal data-plane fix:
   extend the hot ingester transaction row/write path and the cold history-loader Parquet row/write
   path to persist the Horizon-required XDR/signature fields going forward. For already-backfilled
   testnet ranges, either run a targeted replay/backfill or build a bounded raw-ledger fallback for
   the routes in this cycle. Do not ship `GET /transactions/{hash}` as Horizon-compatible with empty
   mandatory XDR fields.
3. **Transaction resource reader** (`horizon_tx_reader.go`) — `GetHorizonTransaction(ctx, hash)`
   and `GetHorizonTransactionsForLedger(ctx, seq)`: hot PG `transactions_row_v2` first, cold
   DuckLake with `IndexReader.LookupTransactionHash` ledger hint for partition pruning (same
   pattern as `GetTransactionDiffsWithLedgerHint`, cold_reader.go:633). Projects the full
   Horizon column set: hash, ledger, closed_at, source, account_sequence, fee_charged, max_fee,
   operation_count, envelope/result/meta/fee_meta XDR, signatures, memo_type/memo. Decode
   preconditions + fee-bump split from the envelope XDR (SDK `xdr` package) — same trick
   Horizon itself uses only for `memo_bytes`; we extend it to preconditions since we don't
   store them relationally.
4. **TOID plumbing** — do **not** replace existing `/silver` `operation_id`/cursor semantics.
   Add real `transaction_toid`/`operation_toid` projections to `EnrichedOperation` where safe, or
   build a Horizon-specific operation reader that selects `transaction_id`/`operation_id` TOIDs from
   `enriched_history_operations`. Add TOID to the account-transactions federated query
   (`buildAccountTransactionsQuery`, account_history.go:513) via the enriched-ops join; the
   serving-feed path already orders by real `toid`.
5. **Typed operation population** (`handlers_horizon_ops.go`) — Base from enriched columns
   (TOID paging token, type/type_i via `operations.TypeNames`, source, created_at, tx_hash,
   tx_successful). Per-type details: decode the transaction's envelope XDR (from the bronze tx
   reader, one decode per distinct tx on the page, memoized) and populate the SDK operation
   structs from `xdr.OperationBody`. **Priority order:** payment, create_account,
   path_payment_strict_{receive,send}, account_merge, change_trust, manage_{sell,buy}_offer,
   create_passive_sell_offer, manage_data, set_options, invoke_host_function. Remaining types
   ship as Base + whatever the switch already covers; finish the tail in Cycle 3.
6. **Effects** (`handlers_horizon_effects.go`) — verify silver effect type ints against
   `effects.EffectTypeNames` (unit test asserting the full mapping table); populate typed effect
   structs from `details_json`; paging token `fmt.Sprintf("%d-%d", opTOID, effectIndex)`;
   translate Horizon cursor → `EffectCursor` (decode TOID → ledger, op index). Add
   `EffectFilters.OperationID` and hot+cold WHERE support so `/operations/{id}/effects` can filter
   on the real effect `operation_id` column instead of scanning by ledger.
7. **Trades** — populate `horizon.Trade` from `SilverTrade` (base/counter legs, price as
   `TradePrice` derived from the scalar — compute n/d from the decimal string); leave
   offer-id/lp-id fields empty (omitempty) and `base_is_seller` best-effort; document the gap.
8. **Tests** — fixture tests per handler: golden JSON assertions produced by round-tripping
   through the SDK structs (guarantees a Go Horizon client can unmarshal); paging tests (next/prev
   link cursor correctness, order inversion, limit clamp, cursor `now`, 410 before-history);
   problem+json shape tests.

**Rabbit holes:** don't hand-write perfect detail population for all 27 op types (tail types wait);
don't implement SSE; don't touch muxed-account fields (omitempty — skip); don't build a generic
XDR→details framework — a plain switch is fine.

**No-gos:** no changes to existing Obsrvr response shapes; no new silver tables this cycle; no
submission/paths/friendbot. The transaction XDR persistence fix is allowed in this cycle because it
is an unblocker for transaction parity, not a new silver serving model.

**Done =** for a real testnet account and transaction, `GET /api/v1/horizon/transactions/{hash}`,
`/accounts/{id}/transactions`, `/accounts/{id}/payments`, and `/effects?cursor=...` unmarshal
cleanly with the SDK's `horizonclient` and field-match horizon.stellar.org testnet responses for
the same resources (excluding documented gaps: trade offer ids, tail op types, muxed fields).

## Cycle 2 — Current-State Objects

**Appetite:** 1 week. Audit Slice 2.

```
COULD HAVE ───────────── order_book formatted from offers pair reader
NICE TO HAVE ─────────── /assets with full AssetStat breakdowns; account data:{} → real
MUST HAVE ══════════════ GET /accounts/{id} (composite); offers{,/{id}};
                         claimable_balances{,/{id}}; liquidity_pools{,/{id}}; fee_stats
```

Steps:

1. **Composite account resource** — widen the `AccountCurrent` reader to project the columns
   `accounts_current` already has (flags, master_weight + thresholds, buying/selling liabilities
   on native balance, inflation_destination, sponsor, num_sponsored/num_sponsoring,
   sequence_ledger/sequence_time — init_silver_hot_complete.sql:266–293). Compose one
   `horizon.Account` from: widened current row + `GetAccountBalances` (trustlines → `Balance[]`
   with native synthesized from account row) + `GetAccountSigners` (master synthesized — reader
   already does this) + `data: {}` (until Cycle 4; documented). `paging_token` = account id.
2. **Offers/CBs/LPs** — thin adapters over existing readers. Widen the claimable-balance reader
   to project the stored `claimants` JSON (currently only `json_array_length` — 
   unified_duckdb_reader.go:2327) and map into `[]horizon.Claimant`. Paging tokens: offer id,
   `"{last_modified_ledger}-{balance_id}"`, pool id — per Horizon convention.
3. **`GET /accounts`** — adapt `HandleListAccounts` filters (Horizon supports `signer`, `sponsor`,
   `asset` filters; support what the existing reader can — `signer` needs the signers JSON
   containment query; mark unsupported filters as `invalid_field` problems rather than silently
   ignoring).
4. **fee_stats** — new aggregation over bronze last-5-ledgers: p10–p99/mode/min/max for both
   `fee_charged` and `max_fee`, `ledger_capacity_usage` from operation counts vs
   `max_tx_set_size`. Reuses the fee-stats handler's bronze access pattern (handlers_fee_stats.go).
5. **Root + health** — `GET /` populated from config + serving stats; `/health` passthrough.

**Done =** `horizonclient.AccountDetail()` against the compat layer returns a fully-populated
account (balances incl. liabilities + authorization flags, signers, thresholds, flags,
sponsorship counts) matching horizon.stellar.org testnet for the same account, `data` exception
documented; fee_stats returns all distribution fields.

## Cycle 3 — Ledgers + Broad Collections + Operation-by-ID

**Appetite:** 1 week. Audit Slice 3 plus the first Slice 4 item (it's cheap once TOIDs are wired).

```
COULD HAVE ───────────── GET /trade_aggregations mapped onto OHLC readers
NICE TO HAVE ─────────── GET /transactions global collection; tail op-type details
MUST HAVE ══════════════ ledgers{,/{seq}} + ledger {transactions,operations,payments,
                         effects}; GET /operations/{id}; GET /operations/{id}/effects
```

Steps:

1. **Ledgers** — `GET /ledgers/{seq}` from `scanLedgers` columns (Horizon-complete, incl.
   `header_xdr`); `GET /ledgers` paged by sequence with TOID paging tokens; bounded default
   (recent-first). Nested `/ledgers/{seq}/transactions` from the Cycle-1 bronze tx reader's
   per-ledger function; `/operations|/payments|/effects` from existing filtered readers with
   `start_ledger=end_ledger=seq`.
2. **`GET /operations/{id}`** — decode TOID → query enriched ops `WHERE operation_id = $toid`
   (real TOID column, hot + cold with ledger-part partition pruning — the TOID's high 32 bits
   *are* the ledger). `GET /operations/{id}/effects` uses the Cycle-1 `EffectFilters.OperationID`
   reader support to filter by the real effect `operation_id` column.
3. **`GET /transactions` (global)** — **verify first** whether `transactions_row_v2` stores the
   within-ledger application order (needed for exact TOIDs). If yes: TOID-paged scan with
   bounded window defaults. If no: ship recent-window-only behavior with documented bounds, or
   derive tx order from enriched-ops `transaction_id`. Do not ship an unbounded hot+cold scan.
4. **Tail op types** — finish per-type detail population deferred from Cycle 1.

**Done =** a Horizon client can walk `/ledgers?order=desc` → `/ledgers/{seq}/transactions` →
`/operations/{id}` → `/operations/{id}/effects` end-to-end on testnet; every hop's `_links` are
followable URLs.

## Cycle 4 — Data-Plane Additions (Scoped Indexes)

**Appetite:** 2 weeks. Audit Slice 4 — the only cycle that touches the transformer/serving plane.
Bet this cycle separately; it's independently shippable and the compat layer degrades gracefully
without it.

```
COULD HAVE ───────────── offer trades (/offers/{id}/trades); CB/LP scoped histories
NICE TO HAVE ─────────── trades enrichment (offer ids, lp id, base_is_seller, price_r)
MUST HAVE ══════════════ account_data_current table + transformer job +
                         GET /accounts/{id}/data/{key} + account.data population
```

Steps:

1. **`account_data_current`** — new silver hot table `(account_id, data_name, data_value,
   sponsor, last_modified_ledger)`; new transformer job in silver-realtime-transformer reading
   manage-data ledger-entry changes from bronze (same current-state pattern as
   `accounts_current`: upsert on change, delete on removal); cold-flush registration; reader +
   `GET /accounts/{id}/data/{key}` (returns `horizon.AccountData` — base64 value) + populate the
   account resource's `data` map.
2. **Trades enrichment** — extend the transformer's trade extraction to carry
   `base_offer_id`/`counter_offer_id`/`liquidity_pool_id`/`base_is_seller` and rational price
   n/d from the trade meta (columns exist in the source `ClaimAtom` XDR); additive columns on
   silver `trades`; backfill optional (document that pre-backfill rows stay partial).
3. **Scoped histories** — `/liquidity_pools/{id}/{transactions,operations,effects,trades}`,
   `/claimable_balances/{id}/{transactions,operations}`, `/offers/{id}/trades`. These are
   high-selectivity access paths over history → serving-projection tables (the
   serving-projection-processor by-account pattern generalizes to by-entity), **only if** the
   existing filtered readers can't answer them within interactive timeouts. Measure first:
   effects/trades already filter efficiently by lp-id-ish predicates in hot; cold needs the
   entity → ledger-range index. Cut these first if the week runs out.

**Done =** `GET /accounts/{id}/data/{key}` returns the same value as horizon.stellar.org testnet
for an account with data entries; account resource `data` map populated; new trades carry offer
ids.

## Verification (all cycles)

- **Unit:** `cd obsrvr-lake/stellar-query-api/go && go test ./...` — golden-fixture tests that
  unmarshal compat responses into SDK structs (`horizon.Transaction`, `operations.Payment`, …)
  and assert field-level equality; paging-semantics tests; effect/op type-mapping exhaustiveness
  tests.
- **Cross-diff against real Horizon:** stellar/go's `horizon-cmp` tool diffs two Horizon
  instances route-by-route. Point it at the compat layer vs `https://horizon-testnet.stellar.org`
  for the implemented route set with a crawl of real testnet accounts/txs. Documented, expected
  diffs (missing trade offer ids pre-Cycle-4, `data: {}` pre-Cycle-4) go in an allowlist.
- **Client smoke test:** the SDK's `horizonclient` pointed at the compat base URL — 
  `AccountDetail`, `TransactionDetail`, `Payments`, `Effects`, `Offers` calls must succeed and
  paginate.
- **Build:** `cd obsrvr-lake/stellar-query-api && docker build .` (service-dir context, image
  under `withobsrvr/`); transformer changes (Cycle 4): `cd obsrvr-lake/silver-realtime-transformer
  && docker build .`.
- **Gateway/public path:** no gateway routing change required for the first rollout. Expose through
  the existing lake proxy at `/lake/v1/{network}/api/v1/horizon-compat/...`; verify `_links` hrefs
  resolve through that path with `HORIZON_COMPAT_BASE_URL` set accordingly.

## Risks & Open Questions

- **Transaction XDR availability.** The transaction schema contains Horizon-required XDR/signature
  columns, but current hot/cold writers do not populate them. Cycle 1 must either persist these
  fields and replay/backfill enough testnet history for verification, or provide a bounded raw-ledger
  fallback. Without this, transaction and typed-operation parity will fail even if handlers compile.
- **Envelope-decode cost on ops pages.** Worst case 200 envelope decodes per page. Mitigation:
  memoize per tx within a request (ops cluster by tx), and the bronze tx reader fetch is batched
  per page. If it's still hot, materialize an op-details JSON at ingest later — don't pre-build it.
- **Federated account-tx TOIDs.** The federated path's per-tx TOID comes from an enriched-ops
  join; verify join cost inside interactive timeout on deep accounts (the account-ledger-index
  pruning applies). The serving-feed path is exact and preferred where coverage allows.
- **Effect type-int alignment.** The silver effects port (April 2026 parity work) was modeled on
  Horizon's types, but the mapping test in Cycle 1 is the gate — misalignment means a translation
  table, not a schema change.
- **`GET /transactions` global** hinges on within-ledger tx order being recoverable (Cycle 3
  step 3 verify-first).
- **History elder bound.** `before_history` (410) needs the elder-ledger boundary; source it from
  the existing data-boundaries reader.
- **Gateway path collision.** Existing `obsrvr-gateway` owns `/horizon/{network}/*` for live Horizon
  proxying. Keep the compatibility layer under the existing lake proxy path unless a separate
  gateway route is deliberately added later.

## Non-Goals (Permanent)

Transaction submission (sync + async), friendbot, path finding, SSE streaming, and Horizon's
ingestion/admin surfaces. The Obsrvr-native API remains the primary product surface; the compat
layer is additive.
