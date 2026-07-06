# Account Transactions Serving Feed — Shaped Cycle (Tier 3, slice 1)

_Written 2026-07-05. First Tier 3 slice, promoted by the Tier 2 gap: `accounts/{id}/transactions`
is not solved by range-level pruning (`pruned_ranges=[34]` — a single range — still times out on a
2k-ledger window; the ~2.1s one-ledger cold read shows the per-query floor). Companion docs:
`account-index-tier2-rollout-2026-07-05.md` (gap + rollout status), `query-api-tier1-quick-wins-2026-07-05.md`.
Decision triangulated from three independent sources: Horizon's participants design (below), the
Stellar RPC #1794 consensus ("using S3/GCS as a hash database is an inappropriate application of an
object store" — local index colocated with the serving node won), and our own testnet
measurements. Principle: the lake holds bulk data and coarse routing; interactive feed lookups live
in a local database next to the serving process._

## Prior art: how Horizon solved exactly this (verified against source)

Four load-bearing pieces from `stellar-horizon`:

1. **Skinny participants join table** — `history_transaction_participants(history_transaction_id
   bigint, history_account_id bigint)`, nothing else
   (`internal/db2/schema/migrations/1_initial_schema.sql:113`), with a UNIQUE composite B-tree on
   `(history_account_id, history_transaction_id)` (line 338). That index IS the "account
   transaction history" feature.
2. **Addresses normalized to integers** — `history_accounts(id bigint, address varchar)` plus a
   batch `AccountLoader` (`internal/db2/history/account_loader.go`). The 56-char address is stored
   once; participants carry 8-byte FKs.
3. **TOID: position = join key = cursor** — packed int64: ledger (32 bits) | tx apply order
   (20 bits) | op index (12 bits) (`go-stellar-sdk/toid`). No ledger column, no timestamp, no
   composite cursor. Pagination is `WHERE history_account_id = ? AND history_transaction_id <
   $cursor ORDER BY history_transaction_id DESC LIMIT n` — a pure B-tree walk. Subtle deliberate
   move (`internal/db2/history/transaction.go:101-113`): `ForAccount` switches the cursor column to
   `htp.history_transaction_id` so the cursor predicate lands on the participants index, and the
   join to tx details only sees the ~25 surviving rows.
4. **Participants extracted from the meta, not just declared fields** —
   `ParticipantsForTransaction` (`internal/ingest/processors/participants_processor.go:176`):
   envelope source + fee-bump source + `participantsForMeta(UnsafeMeta)` +
   `participantsForChanges(FeeChanges)` + per-op participants, deduped. Any account whose ledger
   entries were touched gets indexed.

Cautionary half of the story: what made full-history Horizon PG obese (tens of TB, forcing SDF to
cap public retention) was the transaction bodies and effects JSONB — NOT the participants index.
Our hot/cold split already solved the obese part; this feed is precisely the part of Horizon worth
keeping in Postgres.

What we adopt / adapt / skip:

- **Adopt:** address normalization; packed-int64 TOID cursor; UNIQUE (account, toid) B-tree as the
  read path; reaping/GC discipline.
- **Adapt:** Horizon joins a local `history_transactions` for details — we have no local cold tx
  table, so we **denormalize the render fields into the feed** instead (skinny-feed + per-row cold
  hydration would put ~25 cold point-reads on every deep page — the exact failure mode we're
  escaping).
- **Skip (for now):** meta-derived participant extraction. Our sources are the silver participant
  columns. A future bronze-side meta pass could add touched-account coverage AND recover the
  receive-side roles the cold INT32 columns lost — noted as future work, not this cycle.

## Shape

**Problem:** Account transaction pagination is the flagship Prism account-page need and the one
account surface Tier 2 could not make interactive: range routing prunes cold scans to the right
ranges, but a participant predicate cannot skip row groups *within* a range, and the per-query cold
floor (~2s) makes per-page cold scans structurally non-interactive. The answer must change the
read from "scan cold" to "walk a local B-tree."

**Appetite:** 1 week of active work; backfill wall-clock runs in the background (same rule as
Tier 2). Ship-or-kill on the code; the query-api flip is gated on verification.

**Solution (fat marker):** A Horizon-style participants feed in the serving Postgres:
`serving_accounts` (address → int64) + `account_transactions` (account_id, toid, tx_hash,
denormalized render fields), maintained by the account-index-transformer as a **second sink** on
its existing read paths (hot incremental + cold backfill), read by a new query-api fast path that
serves pages from the feed when coverage is complete and falls back to today's federated path when
it isn't.

**Rabbit holes (do not enter):**

- Do not build meta-derived participant extraction or try to fix the cold INT32 coverage gap here.
  The feed inherits exactly the participant coverage of the existing index sources; the coverage
  caveat stays documented and user-visible.
- Do not change the public API response shape or cursor format. The wire cursor stays in today's
  format; the feed's TOID is internal (encode/decode at the boundary).
- Do not build effect/event-level feeds in this slice. The v1.1 target includes
  `sv_operations_by_account` because Prism transaction/account pages need operation rows, but
  lower-granularity entities stay out until a consumer requires them.
- Do not merge feed + legacy results within one request beyond the head-of-feed hot patch (below).
  Coverage incomplete → serve entirely via the legacy path. No row-level blending.
- Do not add retention/reaping machinery beyond an orphan-accounts GC hook. Full history is the
  point; reaping knobs come when someone needs them.
- If the sizing measurement (step zero) comes back an order of magnitude above estimate, STOP and
  re-shape (lazy per-account materialization is the named fallback) — do not "just try it."

**No-gos:** Prism-side changes (until the flip is verified); new database engines (PG is decided —
RocksDB/SQLite were RPC's context, not ours); touching `tx_hash_index`/contract indexes (Tier 4);
dropping or de-scoping the range index (it still serves snapshots/history routing, the watermark,
and stays the safety net).

**Done (concrete):** The Tier 2 remaining gap closes: the sparse account (`GB32463O…` class)
paginates its **full** history at <500ms/page warm from the feed, including the 2k-ledger windows
that time out today; page results are byte-identical to the federated path on verification
accounts; the latency audit delta is recorded and Tier 2's M8 can be closed.

## Scope line

```text
COULD HAVE ─────────────  C1. fee_charged/memo fidelity for cold-era rows
                          C2. orphan serving_accounts GC job
                          C3. feed-backed accounts/history (snapshots) variant
NICE TO HAVE ───────────  N1. head-of-feed hot patch (live rows above feed watermark)
                          N2. per-account coverage introspection endpoint field
MUST HAVE ══════════════  M0. sizing measurement (gate)
                          M1. DDL: serving_accounts + account_transactions + indexes
                          M2. transformer second sink (hot incremental, dark)
                          M3. backfill mode → feed (cold sweep, resumable)
                          M4. query-api feed fast path behind flag + coverage gate
                          M5. differential verification vs federated path
                          M6. gated flip + audit delta
```

## Design decisions (defaults chosen; the two left-side items are flagged)

**Schema:**

```sql
CREATE TABLE serving_accounts (
  id      BIGSERIAL PRIMARY KEY,
  address TEXT NOT NULL UNIQUE
);

CREATE TABLE account_transactions (
  account_id    BIGINT   NOT NULL,          -- serving_accounts.id
  toid          BIGINT   NOT NULL,          -- ledger(32) | tx_order(20) | 0(12), Horizon packing
  tx_hash       BYTEA    NOT NULL,          -- 32 bytes, for detail hydration
  source_mask   SMALLINT NOT NULL,          -- bitmask: 1=eho, 2=token_transfers, 4=contract_invocations
  closed_at     TIMESTAMPTZ NOT NULL,
  successful    BOOLEAN  NOT NULL,
  activity_type SMALLINT NOT NULL,          -- enum-coded; maps to today's activity_type strings
  source_account_id BIGINT,                 -- serving_accounts.id; NULL where source lacks it
  PRIMARY KEY (account_id, toid)
);
```

Row ≈ 80 bytes. Writer upserts `ON CONFLICT (account_id, toid) DO UPDATE SET source_mask =
account_transactions.source_mask | EXCLUDED.source_mask`. Same tx seen via multiple source tables
collapses into one row (Horizon semantics), with source provenance preserved in the mask.

**Resolved decision 1 — TOID derivation.** Cursor-facing feed rows must use canonical TOIDs.
`silver-realtime-transformer` now carries Bronze `transaction_id` into
`enriched_history_operations` and computes `operation_id` as `transaction_id | (operation_index+1)`.
The incremental feed reads those fields directly; cold backfill joins Bronze
`operations_row_v2.transaction_id/operation_id`. Rows without canonical TOIDs are skipped rather
than populated with hash-derived ordering, because fabricated TOIDs violate Horizon cursor
semantics.

**⚠ Left-side decision 2 — denormalized column set.** Default above covers what Prism renders and
what the current endpoint returns, except `fee_charged`/`memo` (today only present on eho-sourced
rows anyway). Default: return them NULL from the feed path; C1 adds them later if Prism actually
uses them. If verification (M5) shows Prism regressions from NULL fee/memo, promote C1.

**Feed coverage semantics (reuse the Tier 2 watermark pattern):** the feed gets its own backfill
checkpoint + incremental checkpoint (same tables pattern, `serving` scope). Query-api: coverage
complete for the account's query span → serve from feed; otherwise → today's federated path
untouched. Binary switch per request, no blending. Head-of-feed lag (transformer poll interval
behind live): N1 patches page 1 with the existing fast hot arm bounded to `ledger >
feed_watermark`; until N1 lands, accept ~poll-interval staleness at the head.

**Ownership:** the account-index-transformer grows a second sink, not a new service — it already
reads exactly the right participant rows in both incremental (silver hot) and backfill (cold
DuckLake) modes. New PG writer + AccountLoader-style batch address resolver (in-process cache,
batch upsert per cycle). The DuckLake range-index sink is unchanged.

## Checklist

### M0 — Sizing measurement (gate; run before writing code)

- [x] Added repeatable sizing harness:
      `account-index-transformer/scripts/account_feed_sizing.py`. It counts distinct
      `(participant, tx)` pairs in silver hot across `enriched_history_operations`,
      `token_transfers_raw`, and `contract_invocations_raw`, then extrapolates row count and rough
      Postgres+index size.
- [ ] Run the sizing harness against testnet/mainnet and record the number in this doc.
      Expectation: ~100–200M rows ≈ 10–20GB with index on testnet Postgres.
- [ ] Kill criterion: if extrapolation lands an order of magnitude higher, stop and re-shape
      toward lazy per-account materialization.

### M1 — DDL

- [x] Initial serving schema slice added as `serving.sv_transactions_by_account` and
      `serving.sv_operations_by_account`, keyed by `(account_id, toid)` and
      `(account_id, operation_toid)`. This first cut keeps account ids as text; integer
      `serving_accounts` normalization remains the next space-saving refinement.
- [x] `serving.sv_watermarks` and `ops.consumers` added for coverage and consumer checkpoint
      metadata; `serving-cold-backfill` writes them after enabled projection verification.
- [ ] Add the normalized `serving_accounts` address table once the feed semantics are validated.

### M2 — Transformer second sink (incremental, ships dark)

- [x] Initial PG feed writer added for `serving.sv_transactions_by_account` and
      `serving.sv_operations_by_account`; upserts merge `source_mask` with bitwise OR.
- [ ] Address resolver with in-process cache / normalized `serving_accounts` table remains the next
      space-saving refinement. The first deployed slice keeps text `account_id` to match the
      backfill schema and reduce cutover risk.
- [x] TOID derivation implemented for hot rows from canonical Silver Hot `transaction_id` and
      `operation_id`. Migration
      `silver-realtime-transformer/migrations/008_add_enriched_operation_toids.sql` must be applied
      and `silver-realtime-transformer` restarted before enabling `serving_feed.enabled=true`; rows
      without canonical TOIDs are skipped instead of emitting fabricated cursor positions.
- [x] Wired into the existing incremental cycle after the range-index write. The feed uses its own
      `ops.consumers` checkpoint and logs failures without blocking the DuckLake range-index
      checkpoint.
- [ ] `SetMaxOpenConns(1)` lesson from Tier 2 applies to any new DuckDB handles; the PG sink uses
      the normal pool.

### M3 — Backfill mode → feed

- [x] Extend the existing backfill command to emit feed rows from the same cold sweep using
      cold-safe `source_account` and `destination` participants, joined to Bronze
      `operations_row_v2` for TOID fields.
- [x] Backfill now requires canonical Bronze `transaction_id` / `operation_id`; fabricated
      hash/order fallback TOIDs are not written to cursor-facing by-account tables.
- [ ] Resumable via its own checkpoint; dup-safe by upsert. Same Nomad batch job pattern.
- [ ] Monitor PG size + WAL churn during backfill (batch upserts, not row-at-a-time).

### M4 — Query-api feed fast path (behind `ACCOUNT_TX_FEED_ENABLED`, default false)

- [x] Initial text-account fast path added behind `ACCOUNT_TX_FEED_ENABLED=false` default.
- [x] Page query uses the feed B-tree and encodes TOID in the existing cursor tie-breaker.
- [x] Coverage gate checks `serving.sv_watermarks` and falls back to the federated path when the
      requested ledger span is not complete.
- [ ] Response shape identical to the federated path (NULL fee/memo per decision 2); coverage
      metadata extended with `source=feed|federated`.

### M5 — Differential verification

- [x] Initial harness added at `stellar-query-api/scripts/account_feed_diff.py`; it compares
      `source=federated` vs `source=feed` transaction hash sequences and reports per-source
      latency. Run only after deploying Query API with `ACCOUNT_TX_FEED_ENABLED=true`.
- [ ] Run the harness for a panel of accounts (sparse historical, receive-heavy, contract-heavy,
      brand new) and record results.
- [ ] Include the Tier 2 test accounts (`GB32463O…`, `GCFOH4PU…`).
- [ ] Latency: <500ms/page warm at multiple depths, including windows that time out today.

### M6 — Gated flip + close the loop

- [ ] Preconditions: M5 green, feed backfill checkpoint complete, incremental lag < poll interval.
- [ ] Flip `ACCOUNT_TX_FEED_ENABLED=true` on testnet; watch a day under Prism traffic.
- [ ] Re-run latency audit; append delta; close Tier 2 M8 with a pointer here.
- [ ] Update memory/docs: feed live; range index remains for snapshots/history + safety net.

## Dependencies and sequencing

```text
M0 gate ─► M1 ─► M2 (incremental, dark) ─► M3 (backfill) ─► M5 ─► M6
                └────────► M4 (query-api, parallel with M3) ──┘
```

- Hill: M0 and the two flagged decisions are the only left-side work; everything after M1 is
  right-side execution on proven patterns (Tier 2 playbook + Horizon design).
- Scope-cut order if the week runs short: C-items are already out; then N1 (accept head staleness),
  then N2. M5 differential verification is never cut — it is what makes the flip safe.
