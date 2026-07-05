# Query API Tier 1 Quick Wins — Shaped Cycle

_Written 2026-07-05, from the latency audit (`stellar-query-api/docs/latency-audit-2026-07-05.md`)
and the code-level endpoint→engine map. Tier 1 = wiring/config defects only. No new tables, no new
services, no index rebuilds — those are Tiers 2–4._

## Shape

**Problem:** Prism-critical endpoints time out (12s → 500) not because infrastructure is missing,
but because of a handful of wiring-level defects: an unconditional full-history scan on the
balances endpoint, events wired to cold-only, pruning machinery that exists but isn't reused, and
serving fast-paths falling through to DuckDB because their tables are empty. Every timeout also
burns query-api memory because nothing cancels the abandoned DuckDB scan.

**Appetite:** 1 week, including deploy and re-audit. If an item drags past its timebox, cut it and
ship the rest — these are independent fixes.

**Solution (fat marker):** Gate/bound the balances portfolio call; give events a default recent
window; make statement timeouts + bounded-failure the global policy for DuckDB-backed reads;
timebox two diagnoses (empty serving tables, tx-diffs coverage gap); wire the existing
account-index pruning into the second history endpoint.

**Rabbit holes (do not enter):**

- Do not redesign the unified balances response schema — just bound/gate the portfolio call.
- Do not build new serving tables (Tier 3). Item 4 is only "make existing projectors populate
  existing tables"; if the projector/table genuinely doesn't exist, write that up and stop.
- Do not touch index-plane partitioning (Tier 4), even though item 6's diagnosis will probably
  point at it.
- Do not deep-dive DuckDB query interruption internals. Verify `QueryContext` cancellation actually
  interrupts; if it doesn't, bound at the HTTP layer + connection limits and move on.
- Do not chase perfect snapshot-coverage semantics for item 5 — reuse the exact lookup + fallback
  already shipped in `GetAccountTransactions`, nothing more.

**No-gos:** new tables, new services, index rebuilds, Prism-side changes, gateway changes.

**Done:** re-run `scripts/latency_audit.py` against `https://obsrvr-lake-testnet.withobsrvr.com`.
Items 1–2's endpoints classify fast/ok. Zero 12s-timeout 500s anywhere in the audit — every
formerly-timing-out endpoint returns a bounded response (result, partial-with-warning, or clean
4xx/503) in under 10s. Query-api memory stable across the full audit run.

## Scope line

```text
COULD HAVE ─────────────  6. tx-diffs coverage-gap diagnosis
                          7. events/generic default window
NICE TO HAVE ───────────  4. empty serving-table diagnosis + config fix
                          5. accounts/history index pruning (ships dark until Tier 2 deploys)
MUST HAVE ══════════════  1. balances portfolio gate
                          2. events default recent window
                          3. global statement timeouts / bounded failure
                          8. re-audit verification
```

## Checklist

### 1. MUST — Gate the token-portfolio scan in `/addresses/{addr}/balances`

- [ ] `handlers_unified_balances.go` (~line 147): the handler **unconditionally** calls
      `unifiedReader.GetAddressTokenPortfolio(ctx, addr)` — a federated GROUP BY over full
      `token_transfers_raw` history — even after the serving/classic paths already answered.
- [ ] Wrap the portfolio call in a short context timeout (2–3s). On timeout: skip holdings, set
      `resp.Partial = true`, append a warning ("soroban token holdings unavailable"), return 200.
- [ ] Alternative if product prefers: `?include_tokens=true` opt-in. Pick one, don't build both.
- [ ] Note in code: the real fix is the state-based-balances serving table (Tier 3 /
      `project_soroban_state_based_balances`); this bound is the bridge.
- [ ] Verify: endpoint returns <1s for the audit's seed account, warm and cold.

### 2. MUST — Default recent window for `/silver/events`

- [ ] `routes_decode_gold_semantic.go:16` wires `NewEventHandlers(unifiedSilverReader.cold, …)` —
      unfiltered requests do `ORDER BY ledger DESC LIMIT n` over the entire cold Parquet history.
- [ ] Audit proof this works: with `start_ledger=end_ledger=latest` the same endpoint returned in
      **183ms** (partition pruning kicks in).
- [ ] Fix: when no ledger filter is supplied, default `start_ledger = latest - N` (N ≈ one
      hot-retention window; read latest from data-boundaries/watermark, not a hardcode). Document
      the default in the response metadata so consumers know the window they got.
- [ ] COULD (item 7): same default for `/silver/events/generic` (1765ms windowed — better than
      timeout; its real fix is the serving table, item 4).
- [ ] Verify: unfiltered `/silver/events?limit=10` classifies fast; explicit historical ranges
      still work.

### 3. MUST — Global statement timeouts and bounded failure

- [ ] Every DuckDB-backed read gets a per-request context timeout: interactive endpoints ~8s hard
      cap (below the gateway/client 12s), with the value in config, not scattered constants.
- [ ] Verify `database/sql` `QueryContext` on duckdb-go v2 actually interrupts the running query
      (test with a deliberate full cold scan + 1s ctx). If cancellation doesn't propagate, bound at
      the HTTP layer and cap concurrent DuckDB queries via `duckdb_resource_limits.go` instead.
- [ ] On timeout: return 503 (or 200 + `partial: true` where a hot-only answer exists) with
      coverage metadata — never a hung 12s connection followed by a 500.
- [ ] Endpoints with no selective filter and no serving path (accounts list, raw
      operations/payments/soroban feeds) either require a filter (400 with a helpful message) or
      answer hot-only/recent-window with a coverage flag. Do not leave any unbounded cold scan
      reachable from an unauthenticated GET.
- [ ] Verify: re-run the audit — zero rows with 12s timeouts; query-api RSS stable during the run.

### 4. NICE — Diagnose the empty serving tables (timebox: half a day)

- [ ] `events/generic`, `events/contract/{id}`, and `assets/XLM` all have direct-PG serving
      fast-paths (`GetServingGenericEvents`, `GetServingTokenStats`, …) that return early **only if
      the serving table has rows** — they currently fall through to cold DuckDB scans/joins.
- [ ] Find which service populates these `serving_*` tables (silver-realtime-transformer? a
      projector?) and why they're empty on testnet: not deployed, not configured, crashed, or the
      table was never created in prod (precedent: `contract_metadata` existed in DDL but not prod).
- [ ] Also check whether the known `token_registry` partitioning failure that FATAL-cascades in the
      flusher is implicated.
- [ ] If it's a config/deploy/DDL fix: apply it. If it needs new code: write up findings, park for
      Tier 3, stop.
- [ ] Verify: serving-backed endpoints classify fast without the cold fall-through.

### 5. NICE — Wire account-index pruning into `/silver/accounts/history`

- [ ] `GetAccountHistoryWithCursor` (`unified_duckdb_reader.go:382`) scans hot+cold
      `accounts_snapshot` by `account_id` with no ledger predicate — while the account-ledger-index
      lookup + coverage-watermark fallback is already implemented next door in
      `GetAccountTransactions` (`account_history.go:348`).
- [ ] Reuse the same reader + fallback semantics to add `ledger_range IN (...)` to the cold arm.
- [ ] This ships dark (index not yet backfilled — Tier 2 in flight); empty lookup + no watermark →
      today's behavior. The speedup arrives when the account-index-transformer deploys.
- [ ] Verify now: behavior unchanged with empty index; unit test the range-filter injection.

### 6. COULD — Diagnose the tx-diffs/events timeout (timebox: half a day, diagnosis only)

- [ ] `/tx/{hash}/diffs` is engineered to be fast (hot-PG fast path → index-pruned cold read via
      `GetTransactionDiffsWithLedgerHint`, `cold_reader.go:636`) yet timed out for a **recent** tx.
- [ ] Likely chain: hot-path miss/error → ledger-hint lookup hits `tx_hash_index`, whose point
      lookup is itself a full remote-S3 scan (partitioned by `ledger_range`, queried by `tx_hash`)
      → 12s. Trace one recent tx through the path and confirm where the time goes.
- [ ] Output: a written diagnosis pinning it on hot coverage, the index lookup (→ Tier 4), or both.
      **No fix in this cycle** unless it's a one-liner (e.g. hot path erroring on a nil field).

### 8. MUST — Re-audit and record the delta

- [ ] Re-run `scripts/latency_audit.py` (same seeds discovery, same classification) against
      testnet after deploy.
- [ ] Append a "2026-07 post-Tier-1" section to the latency audit doc with the before/after class
      counts per endpoint family.
- [ ] Set up the harness as a recurring probe (cron/CI) so regressions surface as diffs, not
      Prism incidents. (Setup only if trivial; otherwise note as follow-up.)

## Dependencies and sequencing

- Items 1, 2, 3 are independent — parallelizable, ship together in one image.
- Item 5 depends on nothing to *write*, but on Tier 2 (account-index-transformer deploy +
  backfill) to *matter*.
- Item 6's likely conclusion feeds Tier 4 (index repartitioning) — do not start Tier 4 from here.
- Deploy: stellar-query-api builds from SERVICE DIR; image under `withobsrvr/`; Nomad resubmit
  with changed `force_deploy` meta to pull.
