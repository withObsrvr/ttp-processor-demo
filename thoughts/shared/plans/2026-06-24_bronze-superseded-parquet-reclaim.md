---
date: 2026-06-24
researcher: Tillman Mosley III
git_commit: 6c50d80bbb8f53c9d438611e4f6d991b2a805e3f
branch: bugfix/tmosley/current-state-projector-single-pass
repository: ttp-processor-demo
topic: "Safely reclaim ~6.2 TB of superseded Bronze Parquet from B2"
tags: [ducklake, bronze, storage, b2, maintenance, reclaim, cold-storage]
status: draft
type: runbook
---

# Draft: Safely reclaim ~6.2 TB of superseded Bronze Parquet

## Shape

**Problem.** Bronze DuckLake holds ~12 TB of *live* Parquet plus **~6.36 TB of superseded
files** (`end_snapshot IS NOT NULL`) that the merge-only maintenance never garbage-collects.
The catalog metadata still references them and they still sit in B2, so we pay for them.
Silver is clean (58 GB), so this is a Bronze-only problem.

**Appetite.** 2–3 focused days. This is a careful, reversible reclaim — not a re-architecture.

**Scope line.**
```
COULD HAVE ───  b2-vs-metadata orphan diff (files in B2 not in metadata at all); recurring reaper job
NICE TO HAVE ─  reclaim the June-superseded set (~4.3 TB) with a tight age cutoff
MUST HAVE ════  reclaim the clearly-dead May-superseded set (~2.0 TB) reversibly, zero live-data risk
```

**Why it's safe (the core invariant).** A Bronze data file with `end_snapshot IS NOT NULL`
was dropped from the table lineage at that snapshot. The **current/live view always reads the
latest snapshot**, which by definition does not reference these files. Deleting them therefore
**cannot affect any current query.** The *only* capability it removes is time-travel to a
snapshot older than the file's `end_snapshot` (`... AT (VERSION => <old>)`). This is a "current +
recent" serving lake — confirm nothing does historical time-travel reads, then these files are dead weight.

**Rabbit holes (don't):** don't re-enable expire/cleanup inside the flusher/ingester (keeps the
merge-only safety policy intact); don't touch Silver; don't compact/merge as part of this (separate concern).

**No-gos:** never delete a file with `end_snapshot IS NULL` (live); never run during an active
Bronze flush/merge without coordination; never skip the catalog backup.

---

## Current state (measured 2026-06-24, mainnet catalog `obsrvr_lake_catalog_prod`)

| Bucket | Files | Size |
|---|---|---|
| Bronze live (`end_snapshot IS NULL`) | 41,083 | 12 TB |
| Bronze superseded ended in **May 2026** | 4,134 | **2,039 GB** |
| Bronze superseded ended in **June 2026** | 15,514 | **4,322 GB** |
| Silver superseded (out of scope) | 2,645 | 58 GB |

Snapshots: 4,214, spanning 2026-05-14 → 2026-06-17. Files keyed by `data_file_id`; relative
`path` (e.g. `ducklake-019e281e-...parquet`); full B2 key = `<data_path prefix>` + path.

---

## Why we cannot just re-enable the built-in cleanup

`postgres-ducklake-flusher/go/bronze_maintenance.go` (and the index/silver writers) call:
- `ducklake_expire_snapshots('<catalog>')` — **catalog-level, takes NO `older_than`/retain arg**
  in our pinned DuckLake (there's an explicit TODO: "doesn't currently accept a retain parameter").
- `ducklake_cleanup_old_files('<catalog>')` — physically deletes the unreferenced Parquet.

Because expire has no cutoff, it expires **every** snapshot up to the latest in one shot —
removing all time-travel and giving zero safety margin against an in-flight reader/writer. That
all-or-nothing behavior is why these were disabled (comment: "destroys cold storage data"). A safe
reclaim must keep control of *which* files die. → **Strategy B (surgical) is the recommendation;
Strategy A (native) is the faster option once we trust the file set.**

---

## Pre-flight (mandatory, both strategies)

1. **Back up the catalog metadata.** The entire lake's addressability lives in this PG.
   `pg_dump` `obsrvr_lake_catalog_prod` to a safe location (and snapshot the `bronze_meta` schema
   specifically). If anything goes wrong, the metadata is restorable.
2. **Confirm B2 versioning / lifecycle.** Check the bucket: if object versioning is ON (or a
   lifecycle "keep prior versions N days" rule exists), "deletes" become recoverable hides — this
   is our soak-window safety net. If versioning is OFF, we stage to a quarantine prefix instead
   (Strategy B Phase 1).
3. **Confirm no time-travel readers.** Verify query-api / any consumer only reads the current
   snapshot (no `AT (VERSION/TIMESTAMP)` usage). If true, all superseded files are reclaimable.
4. **Coordinate with Bronze writers.** `postgres-ducklake-flusher` + `stellar-history-loader`
   write Bronze and take the flusher write lock. Run the reclaim during a quiet window or pause
   the flusher; deleting *old* superseded files won't collide with new writes, but metadata-row
   deletes must not race a concurrent merge.
5. **Pick the age cutoff.** Target only files whose `end_snapshot.snapshot_time < now() − 14 days`
   (start conservative). This guarantees no recent reader/writer depends on them. With a 14-day
   cutoff the May set (2 TB) and the older slice of June qualify immediately.

---

## Strategy B — surgical, reversible (recommended)

### Step 1 — compute the exact target set (read-only)
```sql
-- candidates: superseded, comfortably behind all readers
CREATE TEMP TABLE reclaim_targets AS
SELECT df.data_file_id, df.table_id, df.path, df.file_size_bytes
FROM bronze_meta.ducklake_data_file df
JOIN bronze_meta.ducklake_snapshot s ON s.snapshot_id = df.end_snapshot
WHERE df.end_snapshot IS NOT NULL
  AND s.snapshot_time < now() - INTERVAL '14 days';

-- SAFETY ASSERTION 1: none of these paths are also a LIVE file (must return 0)
SELECT count(*) FROM reclaim_targets r
WHERE EXISTS (SELECT 1 FROM bronze_meta.ducklake_data_file l
              WHERE l.path = r.path AND l.end_snapshot IS NULL);

-- size + count we expect to reclaim
SELECT count(*) files, pg_size_pretty(sum(file_size_bytes)) sz FROM reclaim_targets;
```
SAFETY ASSERTION 1 returning anything > 0 is a hard stop (would mean a path is reused live).

### Step 2 — resolve full B2 keys & export the list
Resolve the storage prefix (DuckLake keeps it in the catalog `data_path` setting / per-table path)
and join to `reclaim_targets.path` to produce absolute B2 object keys. Export to a file
(`reclaim_keys.txt`). Keep the `(data_file_id, path)` mapping for the metadata cleanup in Step 4.

### Step 3 — Phase 1: hide / quarantine (REVERSIBLE)
- If bucket versioning is ON: `b2` hide/delete each key — the prior version remains restorable
  for the lifecycle window.
- If versioning is OFF: `b2 copy` each key to `bronze/_quarantine/<orig-key>` then delete the
  original. Quarantine is the rollback source.
- Do **not** delete metadata rows yet.
- **Soak 7 days.** During soak, watch the flusher / query-api / history-loader logs for any
  "file not found" / missing-object read errors. None expected (these aren't in the live view).

### Step 4 — Phase 2: commit (after a clean soak)
```sql
-- remove metadata rows for the reclaimed files (keeps catalog consistent)
DELETE FROM bronze_meta.ducklake_data_file
WHERE data_file_id IN (SELECT data_file_id FROM reclaim_targets);
```
Then hard-delete the quarantine prefix / expire the hidden B2 versions. Re-run the live-size
query to confirm Bronze dropped by ~the expected amount and live file count is unchanged.

### Rollback (any time before Step 4 commit)
Restore the hidden B2 versions / move quarantine objects back. Metadata was never touched in
Phase 1, so the catalog still references them correctly — restoring the bytes fully reverts.

---

## Strategy A — native DuckLake cleanup (faster, less granular)

Use only after Strategy B has proven the superseded set is genuinely dead, or for a future
routine reclaim. One-shot, run with the flusher paused and a fresh catalog backup:
```sql
CALL ducklake_expire_snapshots('<catalog>');   -- expires ALL snapshots up to latest
CALL ducklake_cleanup_old_files('<catalog>');  -- deletes now-unreferenced Parquet
```
**Risks vs B:** removes *all* time-travel (no recent-snapshot safety margin); not reversible;
trusts the cleanup implementation that was previously distrusted. **Mitigations:** (a) test on the
low-stakes `index` schema's catalog first and verify only superseded files disappear; (b) verify
post-run that `SELECT count(*) FROM ducklake_data_file WHERE end_snapshot IS NULL` is unchanged
(live files untouched) and that a sample of live queries still return; (c) keep the catalog dump
to restore metadata if cleanup over-deletes.

---

## Done (success demo)
- Bronze live Parquet unchanged (41,083 files, 12 TB); live queries unaffected.
- ≥ 2 TB (May set) reclaimed from B2 and removed from catalog metadata; B2 bill drops accordingly.
- Documented, repeatable runbook so the June set + a recurring 14-day-cutoff reaper can follow.

## Open questions to resolve before running
- Exact `data_path` prefix + whether `path_is_relative` is uniformly true (affects key resolution).
- Is B2 bucket versioning/lifecycle enabled? (decides Phase-1 mechanism + soak reversibility)
- Does the installed DuckLake nightly now support `ducklake_expire_snapshots(..., older_than =>)`?
  If yes, Strategy A becomes safe-by-cutoff and far simpler — verify before committing to B.
- Any consumer doing snapshot/time-travel reads? (must be "no" to reclaim the full set)
