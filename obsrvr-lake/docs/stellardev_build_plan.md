# Build Plan — Shortest Path to "stellar dev can pay and consume"
### Sequenced tickets from the codebase audit. Hand each to Claude Code against the real repos.

**The wedge:** a semantic query API a wallet builder pays for and consumes directly. The audit says you're close — the scary blocker (paid-access layer) already exists in `obsrvr-gateway` + `obsrvr-console`. What's left is one differentiator, one correctness fix, some composition, and a docs/integration pass.

**Sequencing logic:** access path first (gates all revenue, mostly verification), then live-state correctness (a blocker on a capability he named), then the relationship differentiator, then convenience endpoints. If stellar dev's immediate close is wallet/account UX rather than the differentiated relationship demo, Ticket 4 (unified balances) can move ahead of Ticket 3. Every item is tagged with Prism overlap — high overlap = the same work powers your explorer.

**Discipline:** the goal is not a complete API. It's *stellar dev paying and consuming*. Build to his spec, ship the subset that closes him, defer the rest. Don't gold-plate.

**Repos in play (this plan crosses three):**
- `obsrvr-gateway` (Go) — `/home/tillman/Documents/obsrvr-gateway` — auth/proxy/metering. Ticket 1 only.
- `obsrvr-console` (Python/Django) — `/home/tillman/Documents/obsrvr-console` — API key creation + permissions. Ticket 1 only.
- `ttp-processor-demo/obsrvr-lake` (Go) — this repo — transformer + query API. Tickets 2–7.

Each ticket below is tagged with its **Repo(s)** and an **Appetite** (fixed time budget, Shape Up style — if it doesn't fit, cut scope, don't extend).

---

## TICKET 0 (P0) — Create real customer/demo fixtures
*Effort: discovery + documentation. Prevents empty demos and mock-driven acceptance.*
**Repo:** `ttp-processor-demo/obsrvr-lake` (docs/test config). **Appetite:** 0.5 day.

**Goal:** every launch-critical endpoint has a known real-world target for verification and demos.

**Tasks:**
1. Identify 1–2 real sorobandomains contracts whose storage can be listed by contract ID.
2. Identify 2 known addresses with shared transfer and/or contract-call history for the relationship endpoint.
3. Identify 1 wallet address with XLM, classic trustlines, and Soroban token balances for unified balance tests.
4. Record expected high-level facts only (not brittle full snapshots): e.g. “has live domain entries,” “A transferred token X to B,” “address has Soroban token holdings.”
5. Put these fixtures in docs or test config so all tickets can reuse them. **→ Recorded in [`stellardev_fixtures.md`](./stellardev_fixtures.md).** sorobandomains contracts (mainnet) for Ticket 2 are already captured there; addresses for Tickets 3–5 are still TODO.

**Acceptance:** Tickets 1–4 can be validated against real Stellar data without mock fixtures.
**Prism overlap:** High.
**Do NOT:** block on perfect/golden full-response snapshots; these are smoke/demo fixtures.

---

## TICKET 1 (P0) — Verify & document the pay-and-consume path — DONE
*Effort: integration + docs, not building. Low technical risk. Gates ALL paid usage. Do first.*

**Status (2026-06-21): Done, with ops follow-ups.** PR #24 was merged and deployed in `obsrvr-gateway`. Live validation against `https://gateway.withobsrvr.com/lake/v1/testnet/api/v1/silver/stats/network` confirmed authenticated requests return `HTTP 200`, unauthenticated requests return `HTTP 401`, Gateway DB `api_usage` rows are written, and post-deploy semantic Lake rows are metered as `endpoint_type=lake`, `network=testnet`, `weighted_cost=10`. Console key creation already grants `lake`. Docs added in Gateway: `docs/WALLET-BUILDER-LAKE-QUICKSTART.md` and `docs/TICKET-1-LAKE-PAY-AND-CONSUME-VERIFICATION.md`. Remaining ops follow-ups: public `X-RateLimit-*` headers were not visible in the external curl response, and live Prometheus labels still need verification from metrics access.
**Repos:** `obsrvr-gateway` (Go) + `obsrvr-console` (Python/Django). Crosses a language boundary — keep gateway routing/metering work separate from console permission-mapping work. **Appetite:** 2–3 days (mostly reading + one quickstart doc).

**Goal:** an external wallet builder can, without talking to you, create a key in `obsrvr-console`, hit the query API through `obsrvr-gateway`, and get authenticated, rate-limited, metered responses.

**Tasks:**
1. In `obsrvr-gateway/internal/server/server.go` and `internal/proxy/lake.go`, confirm `s.lakeProxy` (behind `APIKeyAuth`, `RateLimitMiddleware`, `WithUsageTracking`) proxies **every** query route stellar dev needs. The public Gateway shape is `/lake/v1/{network}/...`; `LakeProxy` strips that prefix before forwarding to `stellar-query-api`, so the full public paths should be like `/lake/v1/mainnet/api/v1/silver/soroban/contract-data`, `/lake/v1/mainnet/api/v1/silver/contracts/{id}/storage`, `/lake/v1/mainnet/api/v1/silver/accounts/{id}/balances`, `/lake/v1/mainnet/api/v1/silver/address/{addr}/token-balances`, `/lake/v1/mainnet/api/v1/silver/tokens/{contract_id}/transfers`, `/lake/v1/mainnet/api/v1/silver/events/*`, and (once built) `/lake/v1/mainnet/api/v1/silver/relationships/*` and `/lake/v1/mainnet/api/v1/silver/addresses/{addr}/balances`. List any route the `/lake/v1/{network}/` proxy does NOT currently forward.
2. Confirm Console-created keys (`apps/obsrvr_gateway/views.py:create_gateway_api_key`, `client.py:create_key`) carry the `lake`/`query` permission these routes require. Fix the permission mapping if any required route is gated out.
3. Confirm rate-limit + usage-tracking headers appear on a real authenticated request to a `/lake/` query route (verify `api_usage` rows get written). 
4. Confirm usage metering classifies semantic query routes correctly for billing. Today Gateway classifies `/lake/...` as endpoint type `lake`; `WithUsageTracking` only applies heavier weighting to `events` and `query`, so lake requests may currently be weighted like cheap proxy calls. Decide whether semantic `/lake/v1/{network}/api/v1/silver/...` requests need route-aware weights before launch.
5. Verify Gateway metrics preserve the original `/lake/v1/{network}/...` path/backend labels after proxying. `LakeProxy` mutates `r.URL.Path`; if Prometheus/global metrics read the mutated path, capture original path before proxy mutation so paid Lake traffic is observable separately from generic `/other` traffic.
6. Write a **wallet-builder quickstart**: the Gateway base URL, how to create a key in Console, `Authorization: Api-Key ...` examples, and 3 copy-paste curl examples using the endpoints he asked for. This is the doc that converts "we have endpoints" into "he can self-serve."

**Acceptance:** a fresh key created in Console can successfully call a `/lake/` query route through Gateway with auth + rate limit + a usage record written, Lake metrics remain attributable after proxying, and a quickstart doc exists that a stranger could follow.
**Prism overlap:** High (Prism consumes the same gateway path).
**Do NOT:** rebuild auth/keys/metering — they exist. This is verification + docs only.

---

## TICKET 2 (P0) — Harden live contract state correctness
*Effort: NOT polish — this is the riskiest build here (new transformer write path + state reconciliation + tests across two storage tiers). The one real correctness blocker. A capability he named (sorobandomains).*
**Repo:** `ttp-processor-demo/obsrvr-lake` (`silver-realtime-transformer` + `stellar-query-api`). **Appetite:** 1 week.

**Scope line (cut from the bottom if you run late):**
```
COULD HAVE ──── Task 4: evicted_keys/restored_keys live-state reconciliation  ← CUT FIRST, fast-follow
NICE TO HAVE ── Task 3: TTL on HandleContractData
MUST HAVE ═════ Tasks 1+2: delete-row handling + correct TTL-scoped live-only reads (hot AND cold)
```
**Hot-vs-cold landmine:** `contract_data_snapshot_v1` in bronze *hot* is effectively empty — data flushes to cold almost immediately. A read-path test that only hits hot tables can pass/fail for the wrong reason. Every acceptance check below MUST be run against the cold/backfill source path too, not just hot.

**Fixture reality (2026-06-21) — fix now, demo later:** sorobandomains is **mainnet-only**, mainnet ingestion is **not yet complete**, and no testnet sorobandomains deployment exists. So:
- **Ship the fix now** — the bug is code-verified without a fixture; validate on **testnet** against a real contract exercising create/update/delete (no mocks). Delete fixture located: contract `CCJQB4EE…`, key `eb4e8c42…` create@3207019 → delete@3207025 — full details in [`stellardev_fixtures.md`](./stellardev_fixtures.md).
- **The sorobandomains demo stellar dev named is mainnet-gated** — it can't be shown until mainnet ingestion lands. "Finish mainnet ingestion" is a prerequisite for that demo, separate from this ticket's code.

**Goal:** `GET /silver/contracts/{id}/storage` (and `/soroban/contract-data`) returns the **correct, complete, live-only** set of entries for a contract — so stellar dev can enumerate e.g. all live registered sorobandomains and trust the set.

**The bug (from audit):** `contract_data_current` (keyed `PRIMARY KEY (contract_id, key_hash)`) is upserted by `silver-realtime-transformer/go/transformer.go: transformContractDataCurrent`, but there appears to be **no logic removing a row when a later Bronze row has `deleted=true`** — so deleted entries may linger as phantom live state. TTL-expired rows also aren't filtered in the read path.

**Tasks:**
1. In the realtime transformer, add deletion handling: when a Bronze `contract_data` row with `deleted=true` arrives, DELETE (or tombstone) the matching `(contract_id, key_hash)` row from `contract_data_current`. Do **not** rely only on the current `QueryContractDataSnapshot` path, because that query filters `deleted = false`; add/verify deleted-row read paths for the same ledger range in hot (`BronzeReader`), cold/backfill (`BronzeColdReader`), and the `SourceManager` dispatch layer.
2. In `HandleContractStorage`, filter TTL-expired rows by default. The `LEFT JOIN ttl_current ON cd.key_hash = t.key_hash` already works and already computes `expired` / `live_until_ledger_seq` / `ttl_remaining` per entry (verified live on testnet, 2026-06-21) — so the **only real change here is to actually filter `expired` out by default** in `live_only` mode (add `AND COALESCE(t.expired, false) = false`, or compute `live_until_ledger_seq >= latest_known_ledger` if `expired` isn't guaranteed fresh), exposing expired rows only when callers opt out. **Note:** the audit's "join by `(contract_id, key_hash)` not `key_hash` alone" is **moot** — `key_hash` is `SHA256` of the full `LedgerKey` (embeds contract_id + durability) so it's globally unique; `key_hash` alone is correct. (The nebu zero-overlap scare was a nebu-standalone hashing quirk, not present in silver — see [`stellardev_fixtures.md`](./stellardev_fixtures.md).)
3. `HandleContractData` joins no TTL at all — add TTL awareness with the same live-only default, or explicitly document and name it as a raw/current-table endpoint that may include expired rows.
4. Apply `evicted_keys` / `restored_keys` to live state, not only append-only event tables: an evicted entry should not appear live; a restored one should appear live again when the latest restored snapshot/data is available. Define behavior when a restore event exists but the corresponding data snapshot is not available in the processed range.
5. Add tests around the four transitions: create → update → delete → evict/restore, asserting `contract_data_current` and read endpoints reflect only live entries.
6. Add `live_only=true` as the **default** on storage endpoints (let advanced callers opt into raw/all).

**Acceptance:** for a real fixture/test contract, listing storage returns exactly the live entries — no deleted, no expired/evicted (unless explicitly requested) — TTL is joined/scoped correctly. Tests MUST cover delete/TTL on **both** the hot path AND the cold/backfill path (`BronzeColdReader` / `SourceManager`) — a hot-only test is not sufficient because hot is near-empty for this table. Evict/restore (Task 4) may ship as a fast-follow if cut.
**Prism overlap:** Very high (the explorer shows contract state too — same correctness need).
**Why before Ticket 3:** building the relationship differentiator on top of subtly-wrong state is building on sand. Fix correctness first.

---

## TICKET 3 (P0) — Ship the relationship endpoint (the differentiator) — DONE
*Effort: v1 is query-layer over existing tables. THE thing generic indexers can't do.*

**Status (2026-06-21): Done, in review (PR #77).** `GET /api/v1/silver/relationships/{address_a}/{address_b}` is implemented in `stellar-query-api` (`relationships.go`) — keyset-paginated, parameterized SQL over hot+cold with hot-over-cold dedup, conservative typed-column co-event edges only (no SCVal/substring/raw-payload scanning, per v1 scope), `confidence` per edge, and an explicit `coverage`/`limitations` block. Reviewed via multi-agent PR review: no correctness blockers in the new code. Before-merge hardening committed (`003ad9c`): empty-schema guard (no false-empty 200s) + behavioral tests (cursor error paths, address validation, 400/503 contract, pagination/dedup against real in-memory DuckDB, no mocks). One inherited data issue tracked as follow-up: [#78](https://github.com/withObsrvr/ttp-processor-demo/issues/78) (cold `token_transfers_raw.amount` is `DOUBLE` → precision loss; `CAST AS VARCHAR` is the correct interim mitigation). v2 items (SCVal/topic args, smart-wallet signers, indirect/router relationships) remain deferred and are declared in the response's limitations block. **Demo note:** like Ticket 2, full value on mainnet awaits mainnet ingestion; testnet fixtures (`GBTORQK3…`/`GB5FCYPS…`) are live now.
**Repo:** `ttp-processor-demo/obsrvr-lake` (`stellar-query-api`). **Appetite:** 3–4 days.

**Goal:** `GET /api/v1/silver/relationships/{address_a}/{address_b}` returns the interaction history between two addresses — the capability stellar dev named and Goldsky/Subquery don't offer.

**Scope v1 (query-layer, ship this):** "interaction" = any of:
- X sent value to Y / Y sent value to X (`token_transfers_raw`, `semantic_flows_value`)
- X called contract Y / contract X called contract Y (`contract_invocations_raw`, `contract_invocation_calls`)
- X and Y co-appeared in the same transfer / event / call graph (`semantic_activities`, generic events) when both addresses are explicitly indexed/decoded in existing typed columns. Do **not** emit `co_event` edges from substring search, raw XDR/JSON payload search, or arbitrary SCVal/topic scanning. Do not pretend arbitrary SCVal/topic payload scanning is complete in v1.

**Defer to v2 (new indexing, do NOT build now):** addresses buried in arbitrary Soroban event topics/SCVal args, smart-wallet signer/controller relationships, indirect interactions through routers/pools/aggregators. Note these as known limitations in the response, don't block v1 on them.

**Response edge model (from audit):**
```
ledger_sequence, closed_at, transaction_hash,
interaction_type: transfer | contract_call | co_event,   // v1 subset
address_a, address_b, direction,
asset / contract_id / function_name, amount,
source_table, confidence
```
- Include `confidence` per edge (epistemic discipline — same as everywhere else in OBSRVR).
- Paginate (ledger/time cursor) and bound high-cardinality queries with sensible defaults (`limit`, optional ledger/time window) so high-activity addresses cannot accidentally trigger unbounded scans.
- Document the v1 vs v2 scope explicitly in the response/docs so he knows what's covered.

**Acceptance:** for two fixture addresses with known shared history, the endpoint returns transfer + contract-call + conservative typed-column co-event edges with type, direction, amount, source, and confidence, paginated within the target latency/window. The response includes a `coverage`/`limitations` block stating that v1 does not fully inspect arbitrary SCVal arguments or undecoded event payloads.
**Prism overlap:** Very high (explorers need relationship views too).
**Why it matters:** this is the "we do what generic indexers can't" proof point. It's also the most compelling thing to *show* stellar dev.

---

## TICKET 4 (P1) — One-call unified address balances
*Effort: mostly query-layer composition. High Prism overlap.*
**Repo:** `ttp-processor-demo/obsrvr-lake` (`stellar-query-api`). **Appetite:** 2 days.

**Goal:** `GET /api/v1/silver/addresses/{addr}/balances` returns XLM + classic trustlines + arbitrary Soroban token balances in one call (stellar wallet adds arbitrary tokens, so "all assets" must include Soroban tokens).

**Tasks:** compose `accounts_current` + `trustlines_current` (already in `HandleAccountBalances`) with Soroban holdings (`HandleAddressTokenPortfolio` / `token_transfers_raw`) and the generic `address_balances_current` (already readable via `silver_hot_reader.go: GetAddressBalancesCurrent`, just not exposed). Return unified rows: asset type, contract_id if applicable, raw + display balance, decimals/source, last-updated ledger.

**Acceptance:** one call returns every balance type for an address with arbitrary Soroban tokens included.
**Prism overlap:** High.

---

## TICKET 5 (P1) — Historical account transactions & historical balances
*Effort: query-layer first; materialized table if needed. High Prism overlap.*
**Repo:** `ttp-processor-demo/obsrvr-lake` (`stellar-query-api`; possible new materialized table in silver). **Appetite:** open-ended — COULD-HAVE. Don't start until stellar dev's usage pulls for it; if query-layer is too slow, the materialized-table path is a separate cycle, not part of this one.

**5a — `GET /api/v1/silver/accounts/{id}/transactions`:** full historical, classic + Soroban, paginated, ledger/time/success filters, semantic summary. Compose from `enriched_history_operations`, `semantic_activities`, `transactions_row_v2`, `contract_invocations_raw`, `token_transfers_raw`. (Current `/accounts/{id}/activity` is hot-only — this is the full-history version.) Consider a materialized `account_transactions` index if query-layer is too slow.

**5b — historical balances for arbitrary token/account:** `GET /silver/addresses/{addr}/balances/history?asset=|contract_id=` from `balance_changes` (silver-history-loader) + `token_transfers_raw`. Query-layer if `balance_changes` is complete; else projection/backfill.

**Acceptance:** full account tx history and per-asset balance timeseries for arbitrary tokens, paginated.
**Prism overlap:** High.

---

## TICKET 6 (P2) — Event API polish
*Effort: query/API layer + optional classifiers. Lower priority — raw coverage already exists.*
**Repo:** `ttp-processor-demo/obsrvr-lake` (`stellar-query-api`). **Appetite:** COULD-HAVE, polish only — fit it into a cool-down, don't budget a cycle for it.
Stronger semantic decoding/classification on the existing event endpoints (`/silver/events/*`), better docs. He has raw event coverage today; this is enhancement, not a blocker.

---

## TICKET 7 (P1) — Decode contract storage keys and values
*Effort: query-layer first; optional materialized decoded columns later. High Prism overlap.*
**Repo:** `ttp-processor-demo/obsrvr-lake` (`stellar-query-api`; optional ingester/Silver schema enrichment later). **Appetite:** 2–3 days for query-time decode; separate cycle for materialized decoded columns if needed.

**Goal:** contract-data endpoints return decoded JSON for both the original storage key and stored value, so customers do not need to decode Soroban XDR themselves.

**Key finding:** this does **not** require restarting mainnet backfill. Existing `contract_data_xdr` / `data_value` is full `xdr.ContractData`, which contains `Key` and `Val`. The original key is recoverable by decoding existing XDR; it is not limited to the one-way `key_hash`.

**Tasks:**
1. In `stellar-query-api`, decode `data_value` / `contract_data_xdr` into `xdr.ContractData` for `/contracts/{id}/storage`, `/soroban/contract-data`, and `/soroban/contract-data/entry`.
2. Add `key_decoded`, `value_decoded`, and optionally `key_xdr` / `value_xdr` to responses while preserving raw XDR.
3. Use a canonical SCVal-to-JSON serializer that keeps large integers as strings and handles vec/map/address/contract-instance shapes.
4. Treat per-row decode errors as non-fatal (`value_decoded_error`) so one malformed/unsupported value does not break the whole listing.
5. Document optional decoded-column materialization/backfill for performance/search, but do not block v1 on it.

**Acceptance:** a wallet builder can query contract storage and understand common storage entries without writing XDR decoding code. Existing mainnet backfill continues; no historical replay/restart required. Detailed plan: [`contract-data-decoding-plan.md`](./contract-data-decoding-plan.md).
**Prism overlap:** Very high.

---

## Execution order & the win condition

**Appetite rollup (Shape Up — fixed time, variable scope):**

| Ticket | Repo(s) | Appetite | Scope tier |
|---|---|---|---|
| 0 — fixtures | obsrvr-lake | 0.5 day | MUST |
| 1 — access path | obsrvr-gateway + obsrvr-console | 2–3 days | MUST |
| 2 — live-state correctness | obsrvr-lake | 1 week | MUST (evict/restore = cut line) |
| 3 — relationship endpoint | obsrvr-lake | 3–4 days | MUST (the differentiator) |
| 4 — unified balances | obsrvr-lake | 2 days | NICE |
| 5 — history | obsrvr-lake | open-ended | COULD (usage-pulled) |
| 6 — event polish | obsrvr-lake | cool-down | COULD |
| 7 — decoded contract storage | obsrvr-lake | 2–3 days | NICE / customer-value unlock |

**Close window = Tickets 0–3 ≈ ~2.5 weeks.** That's the bet: a key, the gateway, correct live state, and the relationship demo. Then a mandatory 2–3 day cool-down before Tickets 4–6, which his usage prioritizes.

0. **Ticket 0** (real fixtures) — gives every acceptance test/demo a real target; no mocks.
1. **Ticket 1** (access path) — unblocks paid usage; mostly verification. Days.
2. **Ticket 2** (live-state correctness) — blocker on a named capability; the one real bug. 
3. **Ticket 3** (relationship endpoint) — the differentiator; what you demo to him.
4. **Tickets 4–5** (balances, history) — convenience composition. Move Ticket 4 before Ticket 3 if the immediate close is wallet account UX rather than the relationship demo.
5. **Ticket 6** — polish.
6. **Ticket 7** — semantic contract storage; can be done without restarting mainnet backfill.

**The win condition is NOT "complete API." It's "stellar dev has a key, hits the gateway, gets relationship + live-state + balance data, and pays."** After Tickets 1–3 he can likely start consuming a useful, differentiated subset. Get him on it, then let his usage drive 4–6. Build *with* the customer, not toward an abstract ideal of completeness — the wedge is a named buyer, not a finished product.

**Every ticket has Prism overlap (high or very high).** That's the confirmation this is the load-bearing wedge: the work that closes your first paying customer is the same work your own explorer needs. You're not splitting effort between stellar dev and Prism — it's one build.
