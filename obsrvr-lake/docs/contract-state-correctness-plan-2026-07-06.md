# Harden Soroban Live Contract State Correctness — Plan (2026-07-06)

## Context

The LLM review's "Ticket 2 (P0)" flagged phantom live state in `contract_data_current`. Most of that ticket already shipped in PR #76 (`d79e0e6`, June 21): the transformer deletes rows whose latest bronze snapshot is `deleted=true`, read paths join `ttl_current` and filter `COALESCE(expired,false)=false`, and `live_only` defaults to true with `?live_only=false` opt-out. What remains — verified against the code — are the gaps that still fail the acceptance bar ("enumerate all live sorobandomains registrations; trust the set is complete and correct"):

1. **P1 — Stale `expired` flag (the real P0).** `ttl_current.expired`/`ttl_remaining` are computed once at ingest, relative to the ledger that last wrote the TTL entry (`stellar-postgres-ingester` via stellar-extract). Nothing updates them as the chain advances. An entry whose TTL lapses passively (e.g. an unrenewed domain) reads `expired=false` forever → live-only reads return dead entries.
2. **P2 — Duplicate keys across tiers.** Read queries `UNION ALL` hot + cold `contract_data_current` with no dedup. Worse than hot-vs-cold: the cold flusher is append-only per flush cycle, so cold holds **multiple versions per key**, and the cold `ttl_current` join can fan out one row into N.
3. **P3 — Evictions never clean up current state.** Evictions appear *only* in bronze `evicted_keys_state_v1` (LedgerCloseMeta V2 EvictedKeys section) — the snapshot tables never get `deleted=true`, so evicted rows sit in `contract_data_current` forever. (Restores need **no** transformer work: both explicit RestoreFootprint and protocol-23 auto-restore emit normal meta changes → fresh snapshot + TTL rows. Verified in stellar-extract v0.1.2 + SDK change reader.)
4. **P4 — Hot `expired` flag drift** for direct-SQL consumers of the hot table.
5. **P6 — Deleted-entry resurrection from cold** (found during planning): PR #76's delete pass removes only the *hot* row. If a still-unexpired entry was flushed to cold and later explicitly deleted (e.g. a burned domain), the cold copy survives with a future `live_until` and passes the P1 filter.
6. **P5 — Upstream stellar-extract bugs** (separate ticket): `evicted_keys_state_v1.contract_id` is `hex(sha256(ScAddress))` — not a contract id — so the transformer's `hexToStrKey` (transformer.go:2569-2578) writes bogus `C...` strkeys into silver `evicted_keys`; restored-keys extraction misses fee-bump-wrapped and auto-restores.

Two architecture facts that constrain the design (from silver-cold-flusher):

- **Hot is a pruned window** — `deleteFlushedData` removes hot rows after flush (~50-min window on testnet). So hot `ttl_current` is NOT a complete source of TTL truth; the cold TTL join and its missing-table fallback must stay.
- **Cold is append-only multi-version** — dedup must rank versions, and rank **before** filtering (an old cold TTL version could pass the live filter while the newest fails it, resurrecting deleted-and-recreated entries).

## Shape (Shape Up)

- **Problem:** live-only contract-state reads leak expired/evicted/deleted entries and double-count keys.
- **Appetite:** 1 week.
- **Rabbit holes:** don't build a generic cold-compaction system; don't fix stellar-extract in this cycle; don't add caching infrastructure beyond the existing 2s-TTL pattern.
- **No-Gos:** no schema changes to bronze; no new services; no DuckLake DELETE support in the transformer.

```
COULD HAVE ───────────── P5 upstream ticket write-up; /ttl endpoint cursor fixes
NICE TO HAVE ─────────── P3 eviction cleanup, P4 flag refresh, P6 tombstones
MUST HAVE ══════════════ P1 read-time expiry, P2 version-aware dedup, tests
```

- **Done =** a passively-expired entry (stale `expired=false`, `live_until` in the past) is absent from `/contracts/{id}/storage` and `/soroban/contract-data`; a key present in hot + 2 cold versions returns exactly one row (newest); all existing tests + new tests green.

## Key design decisions

- **Current ledger source:** new `UnifiedDuckDBReader.CurrentLedgerForTTL(ctx)` — try `hot_db.serving.sv_ledger_stats_recent` (always advances even when Soroban is quiet), fall back to existing `GetCurrentLedger()` (`MAX(ledger_sequence) FROM hot.ttl_current`, unified_duckdb_reader.go:3844). Cache ~2s using the existing `availableLedgers` cache pattern (unified_duckdb_reader.go:41-43, 265-301). Fetch in Go and bind as a parameter — an inline SQL subquery can't be pushed down into the PG scan or parquet zone maps, and handlers need the value anyway to recompute response TTL fields. `0` = unknown → filter degrades gracefully to today's flag-only behavior.
- **Live predicate:** `ttl_live_until IS NULL OR (COALESCE(ttl_expired,false)=false AND ttl_live_until >= $current)`. Keep the flag check (it's only ever stale in the false-but-expired direction, so it never wrongly excludes). Keep LEFT JOIN + missing-TTL-means-live: a hot cd row's TTL may legitimately live only in cold (tier drift from pruning) — dropping real state is worse than briefly showing an expired entry. Document this in a code comment.
- **Boundary:** live ⇔ `live_until_ledger_seq >= currentLedger` (CAP-46). Fix `normalizeTTLEntryForCurrentLedger` (handlers_silver.go:34-45) from `remaining <= 0` to `< 0` so filter and responses agree. Note the one-ledger shift on `/ttl/*` responses in the PR description.
- **Dedup shape — rank first, filter after:** UNION ALL both arms with `source` (1=hot, 2=cold) and `t.last_modified_ledger AS ttl_modified`; `ROW_NUMBER() OVER (PARTITION BY contract_id, key_hash ORDER BY last_modified_ledger DESC, source ASC, ttl_modified DESC NULLS LAST)`; `WHERE rn = 1` in a nested subquery (matches `queryIssuerMetadata` style, handlers_silver.go:4530); then the live predicate; then ORDER/LIMIT. The `ttl_modified DESC` term collapses cold TTL fan-out onto the newest version. Dedup applies to `live_only=false` too (skip the TTL join and live predicate only).
- **Fallbacks preserved:** cold-`ttl_current`-missing → rebuild with cold arm using NULL-cast TTL columns (cold rows pass via the `IS NULL` disjunct — matches today's semantics); then hot-only single-arm. Same error-string detection as today.

## Implementation steps

### MUST — query API read path (`obsrvr-lake/stellar-query-api/go/`)

1. **Current-ledger plumbing** — `unified_duckdb_reader.go`: add `CurrentLedgerForTTL` (cached, serving-stats-first). `handlers_silver.go:34-45`: boundary fix in `normalizeTTLEntryForCurrentLedger`.
2. **`GetContractData` rewrite (P1+P2)** — `unified_duckdb_reader.go:3674-3841`. Three query variants (main / cold-TTL-stripped / hot-only), all sharing the rank-then-filter shape below; `$current` appended after base params so one `args` slice serves all variants; `LIMIT limit+1` outermost to preserve `hasMore`; cursor tuple condition stays in per-arm WHERE (dedup-invariant). Outer SELECT keeps the exact 8 columns so the scan loop is untouched. Serves `/soroban/contract-data` and `/contract-data/entry`.

```sql
SELECT contract_id, key_hash, durability, data_value,
       asset_type, asset_code, asset_issuer, last_modified_ledger
FROM (
  SELECT ..., ttl_live_until, ttl_expired,
         ROW_NUMBER() OVER (PARTITION BY contract_id, key_hash
           ORDER BY last_modified_ledger DESC, source ASC, ttl_modified DESC NULLS LAST) AS rn
  FROM (
    SELECT cd.*, t.live_until_ledger_seq AS ttl_live_until, t.expired AS ttl_expired,
           t.last_modified_ledger AS ttl_modified, 1 AS source
    FROM <hot>.contract_data_current cd LEFT JOIN <hot>.ttl_current t ON cd.key_hash = t.key_hash
    WHERE <base>
    UNION ALL
    SELECT cd.*, t.live_until_ledger_seq, t.expired, t.last_modified_ledger, 2 AS source
    FROM <cold>.contract_data_current cd LEFT JOIN <cold>.ttl_current t ON cd.key_hash = t.key_hash
    WHERE <base>
  ) all_rows
) ranked
WHERE rn = 1
  AND (ttl_live_until IS NULL OR (COALESCE(ttl_expired,false) = false AND ttl_live_until >= $cur))
ORDER BY contract_id, key_hash
LIMIT $limit
```

3. **`HandleContractStorage` rewrite (P1+P2)** — `handlers_silver.go:4240-4436`. Same shape with `ORDER BY key_hash LIMIT/OFFSET`; stop scanning stored `ttl_remaining`/`expired` (8 columns) and recompute in Go from `live_until` vs the same `currentLedger` used in the filter. Both fallback layers preserved.
4. **`SilverHotReader.GetContractData` (P1 only, no dedup)** — `silver_hot_reader.go:3910-3912`: condition becomes `(t.key_hash IS NULL OR (COALESCE(t.expired,false)=false AND t.live_until_ledger_seq >= $n))`. (Unused by handlers but test-covered; keep consistent.)
5. **Tests** — `contract_data_live_only_test.go`. Extend fixtures first: test `ttl_current` gains `last_modified_ledger`, `ledger_sequence` columns; insert helpers take `liveUntil, ledgerSeq` (without this, new-path tests error into fallbacks and silently test the wrong query). Cases:
   - passively-expired (`expired=false`, `live_until=150`, current=300) excluded live-only, included raw
   - hot/cold duplicate → one row, hot/newer wins
   - cold multi-version, newer TTL live + older expired → one row, live (proves rank-then-filter)
   - no-TTL-row entry still returned live-only
   - existing cold-TTL-missing fallback test still passes
   - cursor pagination with a cross-tier duplicate (limit=2 → deduped page, correct `has_more`/cursor)
   - `/storage` response recompute: `live_until=400`, current=300 → `ttl_remaining=100`; boundary `live_until=300` present

### NICE TO HAVE — endpoints + transformer

6. **`/ttl/expired` + `/ttl/expiring`** — `unified_duckdb_reader.go:3245-3410`, `handlers_silver.go:3588-3696`. Expired: `WHERE (expired = true OR live_until_ledger_seq < $cur)` + normalize response. Expiring: add `live_until_ledger_seq >= $cur` so already-expired entries drop out. While there (COULD): fix `GetTTLExpired` cursor direction (DESC order with ascending tuple cursor at :3340-3362) and `GetTTL`'s tier-arbitrary `LIMIT 1` (:3096-3105, add `ORDER BY last_modified_ledger DESC, source`).
7. **P3 eviction cleanup** — `silver-realtime-transformer/go/transformer.go`, in `transformContractDataCurrent` after the deleted-snapshot pass (:2420-2451): iterate `sourceManager.QueryEvictedKeys(ctx, start, end)` (already exists, hot+cold) and `DELETE FROM contract_data_current WHERE key_hash = $1 AND ledger_sequence < $2` (eviction ledger). Key-hash-only on purpose (bronze evicted `contract_id` is unreliable — P5); the `ledger_sequence <` guard makes restore-after-evict in the same batch safe (restored upsert writes a higher `ledger_sequence`; conflict clause updates it, silver_writer_batch.go:443-453). Mirror the same cleanup for `ttl_current` **inside `transformTTLCurrent`** (:2503) — transform jobs run in parallel transactions with a jobs-own-their-tables invariant (transformer.go:866-947); don't cross-table-delete from another job's tx. Add index (new migration + `schema/init_silver_hot_complete.sql`): `CREATE INDEX IF NOT EXISTS idx_contract_data_key_hash ON contract_data_current(key_hash);` — PK `(contract_id, key_hash)` doesn't cover key-hash-only deletes.
8. **P4 hot flag refresh** — end of `transformTTLCurrent`: `UPDATE ttl_current SET expired=true, ttl_remaining=live_until_ledger_seq-$1, updated_at=NOW() WHERE expired=false AND live_until_ledger_seq < $1` with `$1=endLedger`. `idx_ttl_expiration` supports it; converges (each row flips once). Log rows-affected; possible deadlock with flusher deletes is retried next batch (known, tolerable).
9. **P6 tombstones (stretch — cut first if the week runs out)** — new hot table `contract_data_tombstones(key_hash PK, deleted_ledger BIGINT)`; transformer's delete pass (and eviction pass) inserts into it; cold arms of the two read queries add `AND cd.key_hash NOT IN (SELECT key_hash FROM <hot>.contract_data_tombstones)` (anti-join; table stays small — explicit deletes are rare). A restore/recreate must remove the tombstone (delete from it in the upsert pass for keys being upserted). If cut: document as known limitation in the PR.
10. **Transformer tests** — extend `contract_data_snapshot_test.go` pattern (in-memory DuckDB) with an `evicted_keys_state_v1` helper: evict removes row; evict-then-restore keeps row (higher ledger_sequence); ttl_current cleanup equivalents; P4 refresh flips only lapsed rows; tombstone insert/clear if P6 lands.

### COULD HAVE — follow-up ticket (write-up only, no code this cycle)

11. **P5 upstream** — file against `github.com/withObsrvr/stellar-extract` v0.1.2: (a) evicted `contract_id` double-hash → bogus strkeys in silver `evicted_keys`; (b) restored-keys misses fee-bump + protocol-23 auto-restores. Deliverable: library release, ingester version bump, silver `evicted_keys` repair/backfill. Note that P3 keys on `key_hash` only, so it's immune to (a).

## Verification

- **Unit:** `cd obsrvr-lake/stellar-query-api/go && go test ./...` and `cd obsrvr-lake/silver-realtime-transformer/go && go test ./...` (new + existing; the live-only tests run against in-memory DuckDB with hot/cold schemas).
- **Build:** both services build in nix/Docker per established patterns (`cd obsrvr-lake/stellar-query-api && docker build .`; `cd obsrvr-lake/silver-realtime-transformer && docker build .` — images under `withobsrvr/`).
- **End-to-end (testnet, after deploy):** pick a contract with known expired entries; `GET /api/v1/silver/contracts/{id}/storage` via `https://gateway.withobsrvr.com/lake/v1/testnet/...` must exclude entries whose `live_until_ledger_seq < latest ledger` and contain no duplicate `key_hash`; `?live_only=false` shows them with recomputed `expired=true`. Acceptance: enumerate sorobandomains registrations and spot-check a lapsed domain is absent and a renewed one present.
- **Behavioral changes to document in PR:** offset-pagination page contents shift on `/storage` (duplicates no longer inflate pages); raw views are now deduped; `/ttl/*` boundary shifts by one ledger (protocol-accurate).

## Appendix: what PR #76 already covers (do not redo)

- Transformer deletes `contract_data_current` rows whose latest bronze snapshot in range is `deleted=true` (`transformer.go:2420-2451`, `QueryDeletedContractDataSnapshot` on hot + cold bronze readers).
- Live-only read filtering via `ttl_current` joins on `/contracts/{id}/storage`, `/soroban/contract-data`, `/contract-data/entry`; `live_only` defaults true.
- Tests: create→update→delete and delete-then-recreate at the bronze-query level; live-only filtering across hot+cold; cold-TTL-missing fallback.
- Restores require no transformer change: RestoreFootprint and protocol-23 auto-restore both emit normal meta changes that produce fresh `contract_data_snapshot_v1` + `ttl_snapshot_v1` rows.
