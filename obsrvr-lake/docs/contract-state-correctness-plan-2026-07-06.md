# Harden Soroban Live Contract State Correctness — Plan (2026-07-06)

## Context

The LLM review's "Ticket 2 (P0)" flagged phantom live state in `contract_data_current`. Most of that ticket already shipped in PR #76 (`d79e0e6`, June 21): the transformer deletes rows whose latest bronze snapshot is `deleted=true`, read paths join `ttl_current` and filter `COALESCE(expired,false)=false`, and `live_only` defaults to true with `?live_only=false` opt-out. What remains — verified against the code — are the gaps that still fail the acceptance bar ("enumerate all live sorobandomains registrations; trust the set is complete and correct"):

1. **P1 — Stale `expired` flag (the real P0).** `ttl_current.expired`/`ttl_remaining` are computed once at ingest, relative to the ledger that last wrote the TTL entry (`stellar-postgres-ingester` via stellar-extract). Nothing updates them as the chain advances. An entry whose TTL lapses passively (e.g. an unrenewed domain) reads `expired=false` forever → live-only reads return dead entries.
2. **P2 — Duplicate keys across tiers.** Read queries `UNION ALL` hot + cold `contract_data_current` with no dedup. Worse than hot-vs-cold: the cold flusher is append-only per flush cycle, so cold holds **multiple versions per key**, and the cold `ttl_current` join can fan out one row into N.
3. **P3 — Evictions never clean up current state.** Evictions appear *only* in bronze `evicted_keys_state_v1` (LedgerCloseMeta V2 EvictedKeys section) — the snapshot tables never get `deleted=true`, so evicted rows sit in `contract_data_current` forever. (Restores need **no** transformer work: both explicit RestoreFootprint and protocol-23 auto-restore emit normal meta changes → fresh snapshot + TTL rows. Verified in stellar-extract v0.1.2 + SDK change reader.)
4. **P4 — Hot `expired` flag drift** for direct-SQL consumers of the hot table.
5. **P6 — Deleted-entry resurrection from cold** (found during planning): PR #76's delete pass removes only the *hot* row. If a still-unexpired entry was flushed to cold and later explicitly deleted (e.g. a burned domain), the cold copy survives with a future `live_until` and passes the P1 filter. This is acceptance-critical for "trust the live set"; without tombstones, P1/P2 still leave explicitly deleted cold rows visible.
6. **P5 — Upstream stellar-extract bugs** (separate ticket): `evicted_keys_state_v1.contract_id` is `hex(sha256(ScAddress))` — not a contract id — so the transformer's `hexToStrKey` (transformer.go:2569-2578) writes bogus `C...` strkeys into silver `evicted_keys`; restored-keys extraction misses fee-bump-wrapped and auto-restores.

Two architecture facts that constrain the design (from silver-cold-flusher):

- **Hot is a pruned window** — `deleteFlushedData` removes hot rows after flush (~50-min window on testnet). So hot `ttl_current` is NOT a complete source of TTL truth; the cold TTL join and its missing-table fallback must stay.
- **Cold is append-only multi-version** — dedup must rank versions, and rank **before** filtering (an old cold TTL version could pass the live filter while the newest fails it, resurrecting deleted-and-recreated entries).

Two serving-plane facts learned from the account-history/index work now also constrain the target shape:

- **Hot+cold union queries are a correctness bridge, not the serving target.** The account-history rollout showed that once an endpoint becomes interactive UI surface area, wide DuckLake/hot+cold rank windows are too fragile and too expensive to keep on the critical path. They are acceptable as a short-term correctness patch and as a fallback for uncovered ranges; they should not remain the primary path for Prism-style contract pages.
- **Projection metadata matters as much as the rows.** The account feed became usable only after it had explicit `serving.sv_watermarks`, `ops.consumers`, and projection checkpoints. Contract state needs the same treatment so the Query API can tell whether a fast serving table is complete enough to trust or must fall back to the hot+cold correctness query.

Implication: this plan should land in two phases. Phase 1 hardens the existing Query API read path so live reads stop lying. Phase 2 materializes the same semantics into a Postgres serving projection so common contract-storage reads avoid repeated hot+cold dedup work.

## Shape (Shape Up)

- **Problem:** live-only contract-state reads leak expired/evicted/deleted entries and double-count keys.
- **Appetite:** 1 week for Phase 1 correctness; follow-on serving projection work should be scoped separately but designed now.
- **Rabbit holes:** don't build a generic cold-compaction system; don't fix stellar-extract in this cycle; don't add caching infrastructure beyond the existing 2s-TTL pattern; don't let the hot+cold rank query become the permanent high-traffic serving path.
- **No-Gos:** no schema changes to bronze; no new services; no DuckLake DELETE support in the transformer.

```
COULD HAVE ───────────── P5 upstream ticket write-up; /ttl endpoint cursor fixes
PHASE 2 ──────────────── Postgres serving projection + watermarks/consumers for contract state
NICE TO HAVE ─────────── P3 eviction cleanup, P4 flag refresh
MUST HAVE ══════════════ P1 read-time expiry, P2 version-aware dedup, P6 tombstones, tests
```

- **Done =** a passively-expired entry (stale `expired=false`, `live_until` in the past) is absent from `/contracts/{id}/storage` and `/soroban/contract-data`; a key present in hot + 2 cold versions returns exactly one row (newest); a flushed-then-explicitly-deleted entry is absent from live reads; all existing tests + new tests green.

## Key design decisions

- **Current ledger source:** new `UnifiedDuckDBReader.CurrentLedgerForTTL(ctx)` — try `hot_db.serving.sv_ledger_stats_recent` (always advances even when Soroban is quiet), fall back to existing `GetCurrentLedger()` (`MAX(ledger_sequence) FROM hot.ttl_current`, unified_duckdb_reader.go:3844). Cache ~2s using the existing `availableLedgers` cache pattern (unified_duckdb_reader.go:41-43, 265-301). Fetch in Go and bind as a parameter — an inline SQL subquery can't be pushed down into the PG scan or parquet zone maps, and handlers need the value anyway to recompute response TTL fields. `0` = unknown → filter degrades gracefully to today's flag-only behavior.
- **Live predicate:** `ttl_live_until IS NULL OR (COALESCE(ttl_expired,false)=false AND ttl_live_until >= $current)`. Keep the flag check (it's only ever stale in the false-but-expired direction, so it never wrongly excludes). Keep LEFT JOIN + missing-TTL-means-live only after independently deduping TTL across tiers: a hot cd row's TTL may legitimately live only in cold (tier drift from pruning), so the query must not rank a hot cd/null-TTL joined row ahead of the cold TTL truth. If no TTL row exists in either tier, keep today's "missing TTL means live" behavior and document the tradeoff in a code comment.
- **Boundary:** live ⇔ `live_until_ledger_seq >= currentLedger` (CAP-46). Fix `normalizeTTLEntryForCurrentLedger` (handlers_silver.go:34-45) from `remaining <= 0` to `< 0` so filter and responses agree. Note the one-ledger shift on `/ttl/*` responses in the PR description.
- **Dedup shape — rank first, filter after:** dedupe `contract_data_current` and `ttl_current` independently, then join the winning rows by `key_hash`. For contract data, rank `UNION ALL` hot+cold rows with `ROW_NUMBER() OVER (PARTITION BY contract_id, key_hash ORDER BY last_modified_ledger DESC, ledger_sequence DESC, source ASC)`. For TTL, rank hot+cold rows with `ROW_NUMBER() OVER (PARTITION BY key_hash ORDER BY last_modified_ledger DESC, ledger_sequence DESC, source ASC)`. Join `cd.rn = 1` to `ttl.rn = 1`; then apply the live predicate; then ORDER/LIMIT. This avoids the tier-drift bug where the newest cd row is hot but its TTL row only exists in cold, and it collapses cold TTL fan-out onto the newest version. Dedup applies to `live_only=false` too; skip only the live predicate, not cd dedup. The `live_only=false` path may skip the TTL CTE if callers do not need TTL fields.
- **Fallbacks preserved:** cold-`ttl_current`-missing → rebuild with cold arm using NULL-cast TTL columns (cold rows pass via the `IS NULL` disjunct — matches today's semantics); then hot-only single-arm. Same error-string detection as today.
- **Key identity check before coding tombstones:** verify whether `key_hash` uniquely identifies the full ledger entry or only the contract-data key inside a contract. The read-path partition already uses `(contract_id, key_hash)` for contract data; tombstones and eviction cleanup currently propose `key_hash` only because bronze eviction `contract_id` is unreliable. Before implementing P6, prove that key-hash-only tombstones cannot delete or hide another contract's entry. If that proof fails, tombstones must store enough stable ledger-key identity to avoid cross-contract false positives.
- **Serving projection target:** Phase 2 should materialize a Postgres table such as `serving.sv_contract_storage_current`, not just optimize the hot+cold SQL. It should carry the already-deduped current row per `(contract_id, key_hash)` plus TTL/live/deleted state, `last_modified_ledger`, `source_ledger`, and enough value columns for `/contracts/{id}/storage`. Query API should read this table first when `serving.sv_watermarks` proves coverage, then fall back to the Phase 1 hot+cold correctness query for uncovered data.
- **Explorer output is not just raw storage.** The target product is a contract storage explorer UI with schema counts, key/value search, durability filters, TTL/status toggles, and row detail. The serving projection therefore needs decoded/display columns, not only raw `data_value`. If decoding is incomplete for a key/value, store both raw XDR/value and a best-effort display string so the UI can still render and filter predictably.

## Explorer-serving contract

The contract storage explorer needs a stable API contract that is richer than today's raw `/storage` shape. The serving layer should be designed around these UI needs:

- **Contract summary/header**
  - `contract_id`
  - network
  - optional protocol/contract label when known
  - total storage rows
  - counts by durability: instance, persistent, temporary
  - live, expired, and deleted counts
  - latest modified ledger
  - lowest live-until ledger for runway warnings
  - response coverage/source metadata (`serving`, `hot_cold_fallback`, `partial`)
- **Storage table rows**
  - `key_hash`
  - decoded/display key name when available
  - raw key/XDR representation for copy/debug
  - durability (`Instance`, `Persistent`, `Temporary`)
  - decoded value type (`Address`, `String`, `Map`, `Vec`, `U64`, etc.)
  - decoded/display value for table cells
  - raw value/XDR or JSON for detail view
  - last modified ledger
  - TTL live-until ledger
  - recomputed TTL remaining at request time
  - status (`live`, `expired`, `deleted`)
  - delete reason and deleted ledger when applicable
- **Interactive controls**
  - durability filter
  - live-only/include-expired toggle
  - search over decoded key/value and key hash
  - sort by key, durability, last modified ledger, TTL
  - direct `key_hash` lookup for row detail

The API should expose this through:

- `GET /api/v1/silver/contracts/{id}/storage`
  - parameters: `limit`, `cursor`, `durability`, `live_only`, `include_expired`, `search`, `sort`, `key_hash`
  - returns rows plus coverage metadata
- `GET /api/v1/silver/contracts/{id}/storage/summary`
  - returns counts, latest ledger, TTL runway, and coverage
- optional later: `GET /api/v1/silver/contracts/{id}/storage/schema`
  - returns grouped key/value type counts for the explorer sidebar

Decoded key/value fields are a first-class requirement. Without `key_display`, `value_type`, and `value_display`, the explorer can only show raw hashes/XDR and cannot deliver the intended product experience.

## Implementation steps

### Phase 1 MUST — query API correctness path (`obsrvr-lake/stellar-query-api/go/`)

1. **Current-ledger plumbing** — `unified_duckdb_reader.go`: add `CurrentLedgerForTTL` (cached, serving-stats-first). `handlers_silver.go:34-45`: boundary fix in `normalizeTTLEntryForCurrentLedger`.
2. **`GetContractData` rewrite (P1+P2)** — `unified_duckdb_reader.go:3674-3841`. Three query variants (main / cold-TTL-stripped / hot-only), all sharing the rank-then-filter shape below; `$current` appended after base params so one `args` slice serves all variants; `LIMIT limit+1` outermost to preserve `hasMore`; cursor tuple condition stays in per-arm WHERE (dedup-invariant). Outer SELECT keeps the exact 8 columns so the scan loop is untouched. Serves `/soroban/contract-data` and `/contract-data/entry`.

```sql
WITH cd_ranked AS (
  SELECT *,
         ROW_NUMBER() OVER (PARTITION BY contract_id, key_hash
           ORDER BY last_modified_ledger DESC, ledger_sequence DESC, source ASC) AS rn
  FROM (
    SELECT cd.*, 1 AS source
    FROM <hot>.contract_data_current cd
    WHERE <base>
    UNION ALL
    SELECT cd.*, 2 AS source
    FROM <cold>.contract_data_current cd
    WHERE <base>
  ) cd_all
), ttl_ranked AS (
  SELECT *,
         ROW_NUMBER() OVER (PARTITION BY key_hash
           ORDER BY last_modified_ledger DESC, ledger_sequence DESC, source ASC) AS rn
  FROM (
    SELECT t.*, 1 AS source
    FROM <hot>.ttl_current t
    UNION ALL
    SELECT t.*, 2 AS source
    FROM <cold>.ttl_current t
  ) ttl_all
)
SELECT cd.contract_id, cd.key_hash, cd.durability, cd.data_value,
       cd.asset_type, cd.asset_code, cd.asset_issuer, cd.last_modified_ledger
FROM cd_ranked cd
LEFT JOIN ttl_ranked ttl
  ON ttl.key_hash = cd.key_hash AND ttl.rn = 1
WHERE cd.rn = 1
  AND (ttl.live_until_ledger_seq IS NULL OR (COALESCE(ttl.expired,false) = false AND ttl.live_until_ledger_seq >= $cur))
ORDER BY cd.contract_id, cd.key_hash
LIMIT $limit
```

3. **`HandleContractStorage` rewrite (P1+P2)** — `handlers_silver.go:4240-4436`. Same independently-deduped cd/TTL shape with `ORDER BY key_hash LIMIT/OFFSET`; stop scanning stored `ttl_remaining`/`expired` and recompute in Go from `live_until` vs the same `currentLedger` used in the filter. Both fallback layers preserved.
4. **`SilverHotReader.GetContractData` (P1 only, no dedup)** — `silver_hot_reader.go:3910-3912`: condition becomes `(t.key_hash IS NULL OR (COALESCE(t.expired,false)=false AND t.live_until_ledger_seq >= $n))`. (Unused by handlers but test-covered; keep consistent.)
5. **Tests** — `contract_data_live_only_test.go`. Extend fixtures first: test `ttl_current` gains `last_modified_ledger`, `ledger_sequence` columns; insert helpers take `liveUntil, ledgerSeq` (without this, new-path tests error into fallbacks and silently test the wrong query). Cases:
   - passively-expired (`expired=false`, `live_until=150`, current=300) excluded live-only, included raw
   - hot/cold duplicate → one row, hot/newer wins
   - cold multi-version, newer TTL live + older expired → one row, live (proves rank-then-filter)
   - hot cd row with no hot TTL + cold TTL row → one row with cold TTL applied, not treated as missing-TTL live
   - no-TTL-row entry still returned live-only
   - flushed-then-explicitly-deleted cold row excluded by tombstone
   - existing cold-TTL-missing fallback test still passes
   - cursor pagination with a cross-tier duplicate (limit=2 → deduped page, correct `has_more`/cursor)
   - `/storage` response recompute: `live_until=400`, current=300 → `ttl_remaining=100`; boundary `live_until=300` present

### Phase 1 SHOULD — transformer cleanup and tombstones

6. **P6 tombstones (must-have for trustworthy live sets)** — new durable table `contract_data_tombstones(...)`; transformer's delete pass (and eviction pass, if P3 lands) inserts into it; cold arms of the two read queries add an anti-join so older cold copies cannot resurrect. Preferred columns after the key identity check:
   - `contract_id TEXT NULL`
   - `key_hash TEXT NOT NULL`
   - `deleted_ledger BIGINT NOT NULL`
   - `delete_reason TEXT NOT NULL` (`explicit_delete`, `eviction`)
   - `updated_at TIMESTAMPTZ NOT NULL DEFAULT now()`
   - unique key on the stable identity chosen above

   If `key_hash` is proven globally unique, key-hash-only is acceptable. If not, use `(contract_id, key_hash)` for explicit deletes and a separate eviction identity that matches the actual bronze evicted key. A restore/recreate must either remove the tombstone for keys being upserted or store enough ledger context so `cd.last_modified_ledger > deleted_ledger` wins. This is required for the acceptance bar; if it is cut, the PR must explicitly say flushed-then-deleted entries can still resurrect from cold.
7. **P3 eviction cleanup** — `silver-realtime-transformer/go/transformer.go`, in `transformContractDataCurrent` after the deleted-snapshot pass (:2420-2451): iterate `sourceManager.QueryEvictedKeys(ctx, start, end)` and `DELETE FROM contract_data_current WHERE key_hash = $1 AND ledger_sequence < $2` (eviction ledger), subject to the key identity check above. `SourceManager.QueryEvictedKeys` delegates to the active source mode (`SourceModeHot` or `SourceModeBackfill`); it does not read hot+cold in one call, so tests should cover whichever mode the transformer is running. The `ledger_sequence <` guard makes restore-after-evict in the same batch safe (restored upsert writes a higher `ledger_sequence`; conflict clause updates it, silver_writer_batch.go:443-453). Mirror the same cleanup for `ttl_current` **inside `transformTTLCurrent`** (:2503) — transform jobs run in parallel transactions with a jobs-own-their-tables invariant (transformer.go:866-947); don't cross-table-delete from another job's tx. Add indexes (new migration + `schema/init_silver_hot_complete.sql`): `CREATE INDEX IF NOT EXISTS idx_contract_data_key_hash ON contract_data_current(key_hash);` and `CREATE INDEX IF NOT EXISTS idx_ttl_key_hash ON ttl_current(key_hash);` — PKs do not cover the cross-table/delete access pattern consistently across hot/cold schemas.
8. **P4 hot flag refresh** — end of `transformTTLCurrent`: `UPDATE ttl_current SET expired=true, ttl_remaining=live_until_ledger_seq-$1, updated_at=NOW() WHERE expired=false AND live_until_ledger_seq < $1` with `$1=endLedger`. `idx_ttl_expiration` supports it; converges (each row flips once). Log rows-affected; possible deadlock with flusher deletes is retried next batch (known, tolerable).
9. **Transformer tests** — extend `contract_data_snapshot_test.go` pattern (in-memory DuckDB) with an `evicted_keys_state_v1` helper: evict removes row; evict-then-restore keeps row (higher ledger_sequence); ttl_current cleanup equivalents; P4 refresh flips only lapsed rows; tombstone insert/clear if P6 lands.

### Phase 2 SHOULD — Postgres serving projection for interactive reads

10. **Add serving schema for current contract storage** — extend the serving schema with a table shaped for the `/contracts/{id}/storage` access pattern:

```sql
CREATE TABLE IF NOT EXISTS serving.sv_contract_storage_current (
  contract_id TEXT NOT NULL,
  key_hash TEXT NOT NULL,
  durability TEXT,
  key_display TEXT,
  key_xdr TEXT,
  value_type TEXT,
  value_display TEXT,
  value_json JSONB,
  value_xdr TEXT,
  data_value TEXT,
  asset_type TEXT,
  asset_code TEXT,
  asset_issuer TEXT,
  last_modified_ledger BIGINT NOT NULL,
  ttl_live_until_ledger_seq BIGINT,
  ttl_last_modified_ledger BIGINT,
  expired BOOLEAN NOT NULL DEFAULT false,
  deleted BOOLEAN NOT NULL DEFAULT false,
  deleted_ledger BIGINT,
  delete_reason TEXT,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (contract_id, key_hash)
);

CREATE INDEX IF NOT EXISTS idx_sv_contract_storage_contract
  ON serving.sv_contract_storage_current(contract_id, key_hash);
CREATE INDEX IF NOT EXISTS idx_sv_contract_storage_ttl
  ON serving.sv_contract_storage_current(ttl_live_until_ledger_seq);
CREATE INDEX IF NOT EXISTS idx_sv_contract_storage_durability
  ON serving.sv_contract_storage_current(contract_id, durability);
```

This table should store the deduped latest row only. `expired` can be refreshed incrementally, but Query API should still compute live status from `ttl_live_until_ledger_seq >= currentLedger` to avoid stale reads.

Add a summary table or materialized view for first-paint explorer headers and sidebars:

```sql
CREATE TABLE IF NOT EXISTS serving.sv_contract_storage_summary (
  contract_id TEXT PRIMARY KEY,
  total_entries BIGINT NOT NULL DEFAULT 0,
  instance_entries BIGINT NOT NULL DEFAULT 0,
  persistent_entries BIGINT NOT NULL DEFAULT 0,
  temporary_entries BIGINT NOT NULL DEFAULT 0,
  live_entries BIGINT NOT NULL DEFAULT 0,
  expired_entries BIGINT NOT NULL DEFAULT 0,
  deleted_entries BIGINT NOT NULL DEFAULT 0,
  latest_modified_ledger BIGINT,
  min_live_until_ledger BIGINT,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

The summary table can be refreshed incrementally per touched contract after row upserts/deletes, or rebuilt by a bounded maintenance job for contracts touched in a ledger batch.

11. **Backfill from cold + hot using the Phase 1 semantics** — create a `serving-cold-backfill` mode for `sv_contract_storage_current`, following the by-account feed pattern:
   - process ledger chunks
   - rank contract data and TTL independently before filtering
   - apply tombstones/deletes
   - upsert into Postgres with deterministic conflict handling
   - write projection checkpoints
   - write `serving.sv_watermarks`
   - write `ops.consumers`

   Do not run an unbounded full-table duplicate check at the end by default; the by-account backfill OOM proved that uniqueness should be enforced by indexes and optional verification should be an explicit operator action.

12. **Incremental projection updates** — update the silver realtime transformer or a serving projection processor to maintain `sv_contract_storage_current` as ledgers arrive:
   - upsert live contract data changes
   - update TTL columns
   - decode key/value displays and value types when possible
   - mark explicit deletes and evictions
   - clear/override tombstones on restore/recreate when ledger ordering proves the new row wins
   - refresh `serving.sv_contract_storage_summary` for touched contracts
   - advance `serving.sv_watermarks` and `ops.consumers`

13. **Query API fast path** — make `/contracts/{id}/storage` and eventually `/soroban/contract-data` try Postgres serving first:
   - if `sv_watermarks` covers the requested/current range, serve from `serving.sv_contract_storage_current`
   - recompute `ttl_remaining` from `currentLedger`
   - apply `live_only` from serving columns
   - support explorer parameters: `durability`, `search`, `sort`, `key_hash`, `include_expired`
   - include response metadata such as `coverage.source = "serving"` and `partial=false`
   - fallback to Phase 1 hot+cold correctness query when serving coverage is missing or marked partial

14. **Serving tests and differential checks** — add a script similar to the account feed diff tooling:
   - compare serving vs Phase 1 hot+cold query for selected contracts
   - assert no duplicate `key_hash` per contract
   - assert passively expired rows are absent from live serving reads
   - assert `?live_only=false` shows expired/deleted status correctly when requested
   - assert decoded/display fields are present for known Soroban domain keys and gracefully fall back for unknown key/value shapes
   - assert summary counts match filtered row counts
   - run against a known sorobandomains contract and a contract with frequent TTL churn

### COULD HAVE — endpoints + upstream tickets

15. **`/ttl/expired` + `/ttl/expiring`** — `unified_duckdb_reader.go:3245-3410`, `handlers_silver.go:3588-3696`. Expired: `WHERE (expired = true OR live_until_ledger_seq < $cur)` + normalize response. Expiring: add `live_until_ledger_seq >= $cur` so already-expired entries drop out. While there (COULD): fix `GetTTLExpired` cursor direction (DESC order with ascending tuple cursor at :3340-3362) and `GetTTL`'s tier-arbitrary `LIMIT 1` (:3096-3105, add `ORDER BY last_modified_ledger DESC, source`).
16. **P5 upstream** — file against `github.com/withObsrvr/stellar-extract` v0.1.2: (a) evicted `contract_id` double-hash → bogus strkeys in silver `evicted_keys`; (b) restored-keys misses fee-bump + protocol-23 auto-restores. Deliverable: library release, ingester version bump, silver `evicted_keys` repair/backfill. Note that P3 keys on `key_hash` only if the key identity check proves that safe.

## Verification

- **Unit:** `cd obsrvr-lake/stellar-query-api/go && go test ./...` and `cd obsrvr-lake/silver-realtime-transformer/go && go test ./...` (new + existing; the live-only tests run against in-memory DuckDB with hot/cold schemas).
- **Build:** both services build in nix/Docker per established patterns (`cd obsrvr-lake/stellar-query-api && docker build .`; `cd obsrvr-lake/silver-realtime-transformer && docker build .` — images under `withobsrvr/`).
- **End-to-end (testnet, after deploy):** pick a contract with known expired entries; `GET /api/v1/silver/contracts/{id}/storage` via `https://gateway.withobsrvr.com/lake/v1/testnet/...` must exclude entries whose `live_until_ledger_seq < latest ledger` and contain no duplicate `key_hash`; `?live_only=false` shows them with recomputed `expired=true`. Acceptance: enumerate sorobandomains registrations and spot-check a lapsed domain is absent and a renewed one present.
- **Serving projection verification (Phase 2):** verify `serving.sv_contract_storage_current` row counts, `serving.sv_watermarks`, `ops.consumers`, and differential parity against the Phase 1 hot+cold correctness query before enabling the fast path by default. The API must expose whether the response came from `serving` or `hot_cold_fallback`.
- **Behavioral changes to document in PR:** offset-pagination page contents shift on `/storage` (duplicates no longer inflate pages); raw views are now deduped; `/ttl/*` boundary shifts by one ledger (protocol-accurate).

## Appendix: what PR #76 already covers (do not redo)

- Transformer deletes `contract_data_current` rows whose latest bronze snapshot in range is `deleted=true` (`transformer.go:2420-2451`, `QueryDeletedContractDataSnapshot` on hot + cold bronze readers).
- Live-only read filtering via `ttl_current` joins on `/contracts/{id}/storage`, `/soroban/contract-data`, `/contract-data/entry`; `live_only` defaults true.
- Tests: create→update→delete and delete-then-recreate at the bronze-query level; live-only filtering across hot+cold; cold-TTL-missing fallback.
- Restores require no transformer change: RestoreFootprint and protocol-23 auto-restore both emit normal meta changes that produce fresh `contract_data_snapshot_v1` + `ttl_snapshot_v1` rows.
