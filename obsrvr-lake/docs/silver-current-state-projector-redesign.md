# silver-current-state-projector — failure analysis & publish redesign

**Status:** design handoff. The mainnet current-state backfill (`scripts/14-current-serving-backfill.sh` → `silver-current-state-projector` → `serving-cold-backfill`) failed in step 1 on `accounts_current` after ~25h, OOM-killed during the final publish (96 GiB Nomad alloc). Do **not** run `serving-cold-backfill` and do **not** rerun the projector unchanged.

Code: `obsrvr-lake/silver-current-state-projector/go/main.go`
Job: `infra/.../latitude/obsrvr-lake-mainnet/nomad/silver-current-state-projector.nomad` (`memory = 98304`)

---

## Root cause (corrected)

The earlier hand-off framed this as "huge staging table, then a full copy into the target." That's half of it. The real problem is **where** the staging lives.

`stagingTable()` (main.go:514) builds the staging name with `p.table(...)` (main.go:774), whose `schema()` (main.go:778) resolves to `"silver_catalog"."silver"`. `silver_catalog` is the **attached DuckLake catalog** (main.go:300, `--silver-catalog-name` default `silver_catalog`). Therefore:

- `silver.accounts_current__staging`
- `silver.accounts_current__keys__staging`
- `silver.accounts_current__delta__staging`

are **all DuckLake tables backed by B2 object storage**, not local DuckDB temp tables.

The per-window merge loop (main.go:391-417) runs, for each of the **1,256** windows:

1. `CREATE TABLE …__keys AS …`   → DuckLake write (B2 + catalog txn)
2. `CREATE TABLE …__delta AS …`  → DuckLake write
3. `DELETE FROM staging WHERE EXISTS (keys)` → DuckLake **delete-file** write
4. `INSERT INTO staging SELECT * FROM delta` → DuckLake write
5. `DROP …__keys`, `DROP …__delta` → 2 catalog txns

That's ~**7,500 DuckLake transactions against object storage** for one projection, and it steadily fragments `…__staging` with thousands of small Parquet data-files **and delete-files**. This is the ~25h staging cost. (Consistent with the known DuckLake write-throughput bottleneck for wide silver tables.)

### Why it OOM-killed at *publish*, not during staging

The final step (main.go:421-436) is one DuckLake transaction:

```sql
DELETE FROM silver.accounts_current WHERE network='mainnet';   -- cheap on a fresh build
INSERT INTO silver.accounts_current SELECT * FROM silver.accounts_current__staging;
```

The `SELECT * FROM …__staging` must read a full-mainnet-cardinality table (every account that ever existed) that is now spread across thousands of data-files with thousands of **delete-files that must be reconciled in memory**. DuckLake delete-vector reconciliation + Parquet decompression for that row count is what blew past 96 GiB. Adding RAM won't fix it — the input is pathologically fragmented.

### Secondary problems

- **No window-level durable checkpoint.** `component.ledger_window_completed` is only an emitted log event; the manifest is written once per `(chunk, projection)` *after* publish (main.go:441). A failure anywhere loses all 25h. Resume granularity = whole projection.
- **No parallelism / single process.** `PlannedChunks()` (main.go:767) returns one chunk for the full range unless `--chunk-start/--chunk-end` are passed; all 6 executable projections run serially in one process (main.go:332). `accounts_current` is merely the first.
- **Windowing bounds the per-step delta, not the staging size.** Staging still grows to full distinct-key cardinality regardless, so it never actually caps peak work — it only multiplies transaction count.

---

## Redesign: separate COMPUTE (local) from PUBLISH (DuckLake)

**Thesis:** touch DuckLake exactly twice per projection — read the source snapshot once, write the published result once. Do *all* intermediate work in **local DuckDB** (the in-process `memory` catalog and/or a local on-disk DuckDB file), with spill to local NVMe. This removes ~7,500 object-storage transactions and the delete-file fragmentation that caused the OOM.

### Phase 1 — compute current-state locally

Set DuckDB up to spill instead of OOM, then compute the projection into a **local** table:

```sql
SET memory_limit='80GB';
SET temp_directory='/data/obsrvr-lake/duckdb_tmp';   -- local NVMe, not B2
SET preserve_insertion_order=false;

CREATE TABLE memory.accounts_current AS         -- local 'memory' catalog, NOT silver_catalog
SELECT … FROM silver_catalog.silver.accounts_snapshot
WHERE network='mainnet' AND ledger_sequence <= :as_of
QUALIFY row_number() OVER (PARTITION BY account_id
                           ORDER BY ledger_sequence DESC, … ) = 1;
```

The source read stays on DuckLake (reading Parquet is fine); the heavy sort/aggregate spills to local disk. Prefer a **hash-aggregate `arg_max`** formulation over a window sort where the projection allows it (cheaper memory profile, streams the input):

```sql
-- latest row per key without a global sort
SELECT (arg_max(r, ledger_sequence)).*
FROM (SELECT account_id, ledger_sequence, {…cols…}::STRUCT(…) AS r
      FROM silver_catalog.silver.accounts_snapshot
      WHERE network='mainnet' AND ledger_sequence <= :as_of)
GROUP BY account_id;
```

If a single pass still peaks too high for a given projection, keep the windowed fold — **but against local `memory` tables**. 1,256 local CREATE/DELETE/INSERT operations are milliseconds each and produce no delete-files. The expensive part was never the windowing; it was running the windowing on object storage.

### Phase 2 — publish to DuckLake cheaply (pick one)

The publish source is now a clean local table, so the read side is a plain scan. Options, by preference:

**B (recommended): partitioned, idempotent, resumable publish.** Add a `publish_bucket = hash(key) % N` column in the local table (N≈16–64). Loop buckets, committing each:

```sql
DELETE FROM silver_catalog.silver.accounts_current WHERE network='mainnet' AND publish_bucket = :i;
INSERT INTO silver_catalog.silver.accounts_current SELECT … FROM memory.accounts_current WHERE publish_bucket = :i;
-- mark (projection, bucket) complete in the manifest, then continue
```

Bounds each commit, gives real resume granularity, and is idempotent on retry. (Don't persist `publish_bucket` to the target schema unless you want it; project it away on insert.)

**A: single append on fresh build.** If the target is empty, one `INSERT INTO target SELECT * FROM memory.<proj>` — far better than today because the *source* is clean (no delete-files). Loses resume granularity; fine for the very first build if Phase-1 output is verified first.

**C: build-aside + swap.** Build `silver.accounts_current__new`, verify, then rename/swap. **Verify DuckLake supports atomic `ALTER TABLE … RENAME` / `CREATE OR REPLACE TABLE` first** — do not rely on it unbenchmarked. If supported, this is the cleanest publish.

### Checkpoint & resume changes

- Persist a manifest row per **`(projection, publish_bucket)`** (extend `silver_current_projector_manifest`), not just per projection. `--resume` skips completed buckets. A kill now costs one bucket, not 25h.
- Gate `serving-cold-backfill` on **all** projections reporting `completed` in the manifest (it already won't run until the projector exits 0; make the projector's success contingent on per-bucket completion).

### Cleanup of the failed run

`accounts_current` exists but is suspect (publish was interrupted). Before rerun: drop `silver.accounts_current` (and any orphaned `*__staging` / `*__keys__staging` / `*__delta__staging` DuckLake tables left behind by the kill), and clear the matching manifest rows. Orphaned staging tables are pure B2 cost and will confound the next run.

---

## Why this is the right altitude

- It directly removes the two things that broke: ~7,500 object-storage txns (the 25h) and reading a delete-file-fragmented DuckLake table at publish (the OOM).
- It needs no new DuckLake feature for the recommended path (B) — only local-table compute + bounded DELETE/INSERT, both already proven in this codebase.
- It is incrementally adoptable: changing `stagingTable()`/`replaceProjection()` to use the local `memory` catalog for staging/keys/delta is a localized change to main.go:384-446; the publish-bucket loop replaces the single final transaction.

## Implementation status (2026-06-22)

Implemented in `main.go` on branch `feature/tmosley/current-state-projector-local-publish`:
- Staging / `__keys` / `__delta` now use `localTable()` → the in-process `memory` catalog, or an on-disk DuckDB attached as `scratch` when `--local-scratch-path` is set. **No intermediate writes hit DuckLake.** `configureLocal()` also sets `temp_directory` (spill) and optional `memory_limit`.
- The single giant final `DELETE+INSERT … SELECT * FROM staging` is replaced by `publishProjection()` — N hash-bucket transactions (`--publish-buckets`, default 32), each `DELETE … WHERE bucket=i; INSERT … WHERE bucket=i`, idempotent and checkpointed in a new `silver_current_projector_publish_manifest`. `--resume` skips completed buckets, so a kill costs one bucket, not the projection.
- Existing idempotency + verification tests pass through the new path; added `TestPublishBucketsRecordedAndResumable`.

**Required deploy changes (infra repo `silver-current-state-projector.nomad`):** set `LOCAL_SCRATCH_PATH` to a local NVMe dir (e.g. `/data/obsrvr-lake/duckdb_tmp`) so staging is on-disk, and `MEMORY_LIMIT` (e.g. `80GB`) so DuckDB spills instead of OOMing. Optionally tune `PUBLISH_BUCKETS`. Without `LOCAL_SCRATCH_PATH`, staging uses the in-memory catalog (fine for tests/small ranges, not full mainnet).

**Still not addressed here (separate follow-ups):** option C (build-aside + DuckLake rename), per-window checkpointing of the staging fold itself, and cross-projection parallelism. The single-vs-windowed Phase-1 compute (`arg_max` rewrite) is also left as a future optimization — this change fixes the publish OOM and the ~7,500 object-store transactions, which were the actual failure.

## Open items to verify before implementing

1. Local disk headroom at `temp_directory` for the largest projection's spill (accounts/address_balances are the big ones).
2. Whether `arg_max(struct)` is viable per projection given tie-break columns (some use a secondary `closed_at`/`created_at` order).
3. DuckLake `RENAME`/`CREATE OR REPLACE TABLE` semantics if pursuing option C.
4. Benchmark Phase-1 single-pass vs local-windowed on a real ledger subrange before committing to one.
