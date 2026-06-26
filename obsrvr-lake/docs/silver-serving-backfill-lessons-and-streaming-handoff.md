# Silver/Serving Cold Backfill — Lessons Learned & Streaming Handoff Runbook

_Written 2026-06-25 after the mainnet silver-current-state + serving cold backfill. Captures how to
safely start streaming after a backfill, every bug/footgun hit, and how to make the next backfill
easier._

---

## PART 1 — How to safely start streaming after a cold backfill

After a cold backfill rebuilds the Silver `*_current` tables (as-of some end ledger `N`, here
`62799999`) and populates `serving.*` + stamps `sv_projection_checkpoints`, two **service**-type
streaming jobs take over going forward:

| Job | Maintains | Resumes from |
|---|---|---|
| `silver-realtime-transformer` | Silver hot tables + `*_current` (+ semantic) | its OWN checkpoint: `realtime_transformer_checkpoint` (table in `silver_hot`, `id=1`, `last_ledger_sequence`), resumes at **checkpoint + 1** |
| `serving-projection-processor` | `serving.*` (current + recent) | `serving.sv_projection_checkpoints` (`WHERE sequence > checkpoint`) |

### Topology (verified 2026-06-25) — two layers hide behind the name "silver_hot"
- **Serving** (`serving.sv_*` + `sv_projection_checkpoints`) is in `…:6432/silver_hot` schema `serving`
  (`:6432` is a pooler over the same instance the backfill wrote on `:5432`). The streaming
  `serving-projection-processor` reads/writes serving HERE and resumes from these checkpoints — they
  are stamped at N, so **serving's handoff is consistent.** ✅
- **HOT silver** (`accounts_current`, … and `realtime_transformer_checkpoint`) is in
  `…:6432/silver_hot` schema `public`, maintained by `silver-realtime-transformer`. This is what
  `serving-projection-processor` READS to build the serving current tables.
- **Bronze hot** (source for new ledgers): `…:6432/stellar_hot`.

The projector backfill wrote silver **COLD** (DuckLake) and serving-cold-backfill read COLD → serving.
It did **NOT** touch HOT silver. So there is **no clobbering of HOT silver and no reason to reset the
transformer checkpoint** — an earlier draft said "reset to N", which was wrong: if the checkpoint is
behind N, resetting forward would SKIP ledgers and gap HOT silver. **Do not reset the transformer
checkpoint forward.** Let it resume naturally (idempotent UPSERT replay).

### The real prerequisite: HOT silver must be populated and current to ≥ N
`serving-projection-processor` reads HOT silver. If HOT silver is empty/stale it builds wrong/empty
serving current tables. As of the backfill, HOT silver looked **unpopulated** (no
`realtime_transformer_checkpoint`, no `public.accounts_current`) — the transformer hadn't built it.
So the transformer must run and bring HOT silver up to ≥ N (likely a **cold-replay seed from genesis**
the first time, then live streaming) BEFORE serving streaming is meaningful.

> Run `scripts/streaming-handoff-check.sh` (read-only) to see the exact state across all three DBs.

### Source chain (verified 2026-06-25) — where the start ledger is controlled
```
stellar-live-source-datalake  (:50052, GCS, REQUEST-DRIVEN)        ← no start config, no checkpoint
        ↓  ingester subscribes with start_ledger = its START_LEDGER (or checkpoint+1)
stellar-postgres-ingester     (reads :50052 → writes bronze hot → re-serves :50054)
        ↓  transformer subscribes with start_ledger = its checkpoint+1
silver-realtime-transformer   (reads :50054 → writes HOT silver)
        ↓
serving-projection-processor  (reads bronze hot + HOT silver → serving.*)
```
`stellar-live-source-datalake` streams from `UnboundedRange(req.StartLedger)` — it serves whatever the
subscriber asks, from GCS (all history). So you set the start in **two** places only:

| Job | Start control | Set to |
|---|---|---|
| stellar-live-source-datalake | none (request-driven) | — (start it as-is) |
| **stellar-postgres-ingester** | `START_LEDGER` env (checkpoint `/data/obsrvr-lake/ingester_checkpoint/checkpoint.json` wins if present — **verified absent**, so START_LEDGER applies) | **`62800000`** (N+1) |
| **silver-realtime-transformer** | `realtime_transformer_checkpoint` (id=1, resumes at +1) — **verified absent** | seed `last_ledger_sequence = 62799999` (→ requests N+1) |

> Footgun: if the ingester `START_LEDGER` is left at `3` with no checkpoint, it re-ingests from genesis.
> Always point it at N+1 before starting.

### Procedure (order: source → ingester → transformer → serving)
0. **Do the PART 4 prerequisites FIRST** — rebuild the streaming images for the current protocol, apply
   the bronze hot migrations to `stellar_hot`, and raise the Postgres `/dev/shm`. Skipping any of these
   blocked the start in 2026-06-25 (and the ingester bug turned the failures into silent gaps).
1. **Set start ledgers** (the two control points above): ingester `START_LEDGER=62800000`; ensure no
   stale ingester checkpoint; seed the transformer checkpoint to 62799999 — all three are done by
   `15-streaming-handoff-seed.sh` (run it INSTEAD of `02/03/04`).
2. **Start `stellar-live-source-datalake`** (no config change — request-driven from GCS).
3. **Start `stellar-postgres-ingester`** → it requests :50052 from N+1, fills **bronze hot** from N+1,
   re-broadcasts :50054 from N+1. Verify: `max(sequence)` in `stellar_hot.ledgers_row_v2` climbing from
   ~62800000 (the log should say "Starting fresh from ledger 62800000").
4. **Start the transformer** (checkpoint already seeded to N by `15-streaming-handoff-seed.sh` — NO
   start/stop dance: the seed pre-creates the row and the transformer's schema-init is
   `ON CONFLICT DO NOTHING`, so the seeded N survives; see PART 4). It subscribes to :50054 from N+1 and
   fills **HOT silver** incrementally. Verify: `realtime_transformer_checkpoint.last_ledger_sequence`
   advancing past N, and transforms succeeding (watch for `SQLSTATE 53100` shm errors → PART 4 prereq #3).
5. **Start `serving-projection-processor`** → resumes from `serving.sv_projection_checkpoints` (N),
   reads HOT silver `WHERE last_modified_ledger > N`, updates `serving.*` incrementally on the
   backfilled base.
6. **Verify continuity:** first streamed serving ledger = N+1; `max(ledger_sequence)` in
   `serving.sv_ledger_stats_recent` advancing, lag < ~30s; no gap in N..max.

> HOT silver does NOT need a full seed — serving reads only `last_modified_ledger > N`, and the base
> as-of N is already in `serving.*` (from the backfill) and in cold (from the projector). The transformer
> only needs to provide the post-N changes.

### Caveat — `AutoSkip`
The transformer has an `AutoSkip` config (auto-advance the checkpoint to hot MIN after K empty polls).
If Bronze hot doesn't yet cover N+1 when the transformer starts, AutoSkip can **jump the checkpoint
forward and skip ledgers** → gap in `*_current`. Ensure Bronze covers N+1 before starting, or confirm
AutoSkip is off for the handoff.

### Why no double-counting anywhere
Every table is **idempotent**: `*_current` = UPSERT/latest-per-key; `*_recent` = DELETE-by-range +
INSERT; `*_stats`/`*_assets`/`network_stats` = full recompute (`DELETE *; INSERT SELECT … aggregate`).
There are **no cumulative `x = x + delta` tables**, so overlap/replay is always safe — the only
failure mode is a *gap* (resume past a ledger that was never processed), which the start order above
avoids and which `*_current` idempotency makes recoverable by replaying from before the gap.

---

## PART 2 — Lessons learned (bugs, footguns, and the fixes)

### A. Memory / OOM (the projector)
- **Root cause was the READ, not the compute.** Full-table scans of cold DuckLake/Parquet from B2
  buffer **outside** DuckDB's `memory_limit`, so heavy ops hit the kernel limit (exit 137, no DuckDB
  OOM error). Key-hash partitioning did NOT help (still full scans).
- **Fix: slice the source by LEDGER RANGE.** `ledger_sequence BETWEEN start AND end` per window is
  pushdown-able → each window reads only its slice → bounded memory + O(N). The original windowed
  fold was right; its only bug was a cumulative `<= end` filter (O(N²)).
- **`--chunk-size` is the window size.** Smaller = faster *and* lighter here (500k was ~8× slower +
  spilled vs 50k). Big projections are still ~3–7h each; the per-window merge `DELETE` scans the
  growing staging table.

### B. Memory / OOM (serving) — two distinct ones
- **In-memory DuckDB does NOT spill by default.** `sql.Open("duckdb","")` with no `temp_directory`
  → large aggregates OOM. Fix: set `memory_limit` + `temp_directory` (disk scratch) +
  `preserve_insertion_order=false` + `SetMaxOpenConns(1)` so the PRAGMAs apply.
- **Reading a large *Postgres* table into DuckDB also buffers outside `memory_limit`.** Aggregating
  the 163M-row `sv_account_balances_current` (an attached-PG table) OOM'd even with spill. Fix: run
  serving→serving aggregates **server-side in Postgres** via `postgres_execute('serving_pg', …)` —
  zero rows pulled into DuckDB. (Generalize: if a projection reads a big table that *already lives in
  the target store*, aggregate it there, don't round-trip through DuckDB.)

### C. Correctness footguns
- **Current-state must scan from genesis, always.** Running the projector with a mid-history `--start`
  computed `*_current` over only that sub-range, and `publishProjection` (replace-all-buckets) then
  **deleted** every key last-modified earlier → truncated tables (corrupted accounts 9.8M→2.2M,
  trustlines too). Fix: window from ledger 1 regardless of `--start` (PR #84). **`--start` is a
  footgun for current-state; it should only bound feed/range projections.**
- **Serving never persisted (silent).** `serving-cold-backfill` validated `--target-postgres` as
  required but **never used it** — it built everything in the ephemeral in-memory DuckDB and discarded
  it on exit (serving schema stayed empty). Fix: `ATTACH … (TYPE postgres)` as `SERVING_CATALOG` and
  target `<catalog>.<schema>.<table>`. **Lesson: a "required but unused" flag is a smell; assert the
  data actually lands (row counts), not just that the job exits 0.**
- **`sv_contracts_current` duplicate keys.** Aggregated `contract_metadata` (multiple rows per
  contract) without dedup → verify failed. Fix: `QUALIFY row_number() … PARTITION BY contract_id`.

### D. Validation
- **"Completes without OOM" ≠ "output is correct."** The original `accounts_current` (9.8M) was a
  *stale undercount* (smaller source at the time / older build), not the truth (19.6M). We nearly
  "fixed" a correct rebuild. **Always reconcile output row counts against the source.**
- **`GROUP BY key` row count IS the distinct-key count** — it can't duplicate or invent keys. This
  is a fast way to reason about whether a `*_current` count is plausible (and it matched
  `native_balances_current` because every account holds XLM — same universe).

### E. Resume / handoff mechanics
- **Projector resume is run_id-AGNOSTIC** (`projectionComplete` matches component/network/start/end/
  projection) → re-dispatch skips completed projections across fresh run_ids.
- **Serving resume is run_id-SCOPED** (`chunkComplete` filters `run_id`), and the wrapper mints a
  fresh run_id per dispatch → to skip completed feed chunks you must pass `--flowctl-run-id <prior>`.
  Inconsistent with the projector; worth unifying.
- **Manifests must be PERSISTENT** to resume. Serving's was in-memory (see B) → resume never worked
  across dispatches until persistence was fixed.
- **Serving streaming handoff = `sv_projection_checkpoints`.** The backfill MUST stamp it (last
  ledger) or streaming restarts from 0.

### F. Tooling gotchas (cost real time)
- **nix-built DuckDB cannot load official precompiled extensions** — `INSTALL/LOAD httpfs` segfaults
  (ABI mismatch), so it can't read B2. **Workaround:** official `duckdb` binary under `steam-run`
  (FHS env): `NIXPKGS_ALLOW_UNFREE=1 nix shell --impure nixpkgs#steam-run -c bash -lc 'steam-run /path/to/duckdb -f q.sql'`.
  `steam-run` gives a private `/tmp` — put the SQL file under `$HOME`.
- **The cold catalog is DuckLake format 0.4.** Stable DuckDB 1.5.4's ducklake wants 1.0 and offers
  `AUTOMATIC_MIGRATION`. **DO NOT migrate** — the projector/serving run a 0.4-era nightly ducklake and
  would break on a 1.0 catalog.
- `ducklake` for a **stable** DuckDB is `INSTALL ducklake` (default repo), not `FROM core_nightly`
  (404 on stable). The project dir pins DuckDB 1.4.1 via direnv; outside it is 1.5.2.
- **`PlanChunks` returns nil for `start <= 0`** — window current-state from ledger 1, not 0.

### G. Operational patterns that worked
- **`--skip-silver` for all serving-only runs** — otherwise the wrapper also runs the projector,
  which (with a small `--start`) silently corrupts `*_current` (see C).
- **Fail-fast validation on a recent range** — serving's Postgres write path fails in the first
  minute if the attach is wrong, so a small `--start` proves persistence quickly (current projections
  are range-independent and build full state regardless).
- **Background watchers** that break on completion/failure markers in the wrapper log, rather than
  manual polling (also reduces in-session API rate-limit pressure).

---

## PART 3 — Recommendations to make next time easier

1. **Add a current-state genesis guard + drop `--start` confusion.** Done in code (always windows
   from genesis); consider rejecting a non-genesis `--start` for the projector outright with a clear
   error, so the footgun is impossible and obvious.
2. **Unify resume semantics** — make serving's `chunkComplete` run_id-agnostic like the projector's,
   so re-dispatch "just resumes" without `--flowctl-run-id` juggling.
3. **Bake `memory_limit` + `temp_directory` into every DuckDB-based component by default** (both the
   projector and serving needed it). A shared helper + a required `TEMP_DIRECTORY`/scratch volume in
   the job template would prevent the in-memory-no-spill OOM class entirely.
4. **Default to Postgres-native aggregation for serving→serving projections** (don't round-trip large
   target-store tables through DuckDB). Make `PostgresNative` the rule for any projection whose source
   is another serving table.
5. **Post-backfill output verification step** — automated row-count reconciliation of each `*_current`
   against `COUNT(DISTINCT key)` of its source, emitted as a report. Would have flagged the 9.8M/19.6M
   question immediately and confirmed completeness, not just non-crash.
6. **A combined "handoff" check** — one script that verifies serving checkpoints == N, silver
   `*_current` max == N, resets the transformer checkpoint to N, and confirms Bronze ≥ N+1 before
   anyone starts streaming.
7. **Speed:** the per-window merge `DELETE` scans the growing staging table; a primary-key/index on
   the local staging table would make it an index lookup (~5–10×). DuckDB ART index is in-memory
   (not spilled) — test memory headroom first.

---

## PART 4 — Streaming bring-up: what actually happened (2026-06-25) and the fixes

Starting the streaming pipeline after the backfill hit **five** blockers in a row, none of which were
in the original runbook (PART 1). Do the prerequisites BEFORE the start sequence.

### Prerequisites the original runbook missed
1. **Rebuild the streaming images for the current protocol.** The deployed tags pre-dated **protocol 27**
   (`#74 ccf2779` — the XDR/SDK bump). The ingester / transformer / live-source / serving-processor parse
   raw XDR, and the network was moving 26→27, so the old images would fail to parse current ledgers.
   Rebuilt all 4 from `main` (`73b3ad8`) → tag `proto27-73b3ad8-20260625175502`. **Always check the
   deployed image's commit vs the latest protocol/SDK bump before starting a post-backfill stream.**
   - `live-source-datalake` is the awkward one: its `raw_ledger_service` proto is **git-ignored** and must
     be generated into `go/gen` (that path satisfies BOTH live-source's own import and the ingester's
     `stellar-live-source-datalake/go/gen/...` import) before `CGO_ENABLED=0 go build -mod=vendor`. The
     other 3 build with a plain `docker build`.
2. **Apply the bronze hot migrations.** The **ingester has NO auto-migration** — its bronze schema comes
   from `stellar-postgres-ingester/migrations/0*.sql`, normally run by the reset-flow bootstrap
   (`02-clear-pg`). The handoff SKIPS `02`, so `007_add_versioning_metadata` (adds `era_id`/`version_label`
   to every bronze table) was never applied → every ingester batch failed `column "era_id" does not exist`
   and (via the bug below) silently skipped. Apply all (idempotent `ADD COLUMN IF NOT EXISTS`) to
   `stellar_hot` before starting:
   ```bash
   cd obsrvr-lake/stellar-postgres-ingester/migrations
   for m in $(ls 0*.sql|sort); do psql "$STELLAR_HOT_DSN" -f "$m"; done
   ```
   (Silver hot is fine — the transformer auto-applies its embedded schema via `EnsureSilverHotSchema`.)
   **This belongs in `15-streaming-handoff-seed.sh` (or a `15b`) so it can't be forgotten.**
3. **Raise the Postgres `/dev/shm`.** Default container `/dev/shm` is 64 MB; the transformer's parallel-scan
   transforms (`token_transfers`, `enriched_operations`, `contract_events_unmatched`) need larger DSM
   segments → `SQLSTATE 53100 could not resize shared memory segment … No space left on device`. Fix:
   `shm_size = 2147483648` (2 GB) in `obsrvr-lake-postgresql.nomad` docker config (verify with
   `docker inspect -f '{{.HostConfig.ShmSize}}'`). **Non-disruptive alternative** (no PG bounce):
   `ALTER SYSTEM SET max_parallel_workers_per_gather = 0; SELECT pg_reload_conf();` (slower; reversible).

### The seed step (replaces the "start/stop dance" in PART 1 step 4)
`15-streaming-handoff-seed.sh` (read-mostly; one write) does it cleanly: verifies serving checkpoints == N,
checks ingester `START_LEDGER == N+1`, and **pre-creates + seeds** `realtime_transformer_checkpoint` to N.
The transformer's own schema-init is `INSERT … (1,0,…) ON CONFLICT (id) DO NOTHING`, so the pre-seeded N
survives — **no start/stop dance needed.** Run it INSTEAD of `02/03/04`; do NOT run the clear steps.

### Ingester tuning for Soroban-heavy mainnet ledgers
- **OOM (exit 137) at `PG_BATCH_SIZE=500`.** Ledgers at ~62.8M are Soroban-heavy (~900k contract_events /
  500 ledgers); a 500-ledger batch blew the 6 GB limit mid-batch. Fix: `PG_BATCH_SIZE=100` (caps peak
  memory + commits more often) and `memory=16000` (host has 125 G, but the steady-state streaming set
  already reserves ~65 G — don't over-reserve; 16 G is margin, not a hog).
- Throughput: at batch=100 the ingester (~127 ledgers/min) is the pipeline bottleneck (slower than the
  transformer's ~200/min). Raise toward ~200 once stable if catch-up is too slow.

### ⚠️ THE BUG — ingester advances its checkpoint on a FAILED batch
**Root cause of every gap during bring-up.** When a batch write fails after retries, the ingester **still
advances `checkpoint.json` `last_ledger`** and resumes from the next batch — silently skipping the failed
range. Hit it **three times** (era_id failures, the OOM loop, the PG-bounce reset), each leaving a
100-ledger hole in `stellar_hot.ledgers_row_v2`.
- **Detect:**
  ```sql
  WITH s AS (SELECT sequence, lead(sequence) OVER (ORDER BY sequence) nxt FROM stellar_hot.ledgers_row_v2)
  SELECT sequence+1 gap_start, nxt-1 gap_end FROM s WHERE nxt-sequence>1;
  ```
- **Fix a gap (no hash validation on resume — verified in `checkpoint.go`):** stop the ingester, rewind
  `last_ledger` in `/data/obsrvr-lake/ingester_checkpoint/checkpoint.json` to `gap_start-1` (set
  `last_ledger_hash` to that ledger's `ledger_hash` from `ledgers_row_v2` for cleanliness — it's unused on
  resume), restart. It re-streams from the gap forward; already-present ledgers re-UPSERT idempotently.
  Do this BEFORE the transformer reads past the gap (it reads bronze hot per 500-ledger window), else the
  hole propagates into silver and you must rewind the transformer's checkpoint too.
- **The real fix (next dev cycle, #1 priority):** only advance the checkpoint after a SUCCESSFUL commit.
  Until then, every transient hiccup = a gap; **gap-scan periodically during catch-up.**

### PG-bounce cascade
Redeploying `obsrvr-lake-postgresql` (for the shm fix) reset every connected job's connections; the
ingester and transformer **hung on dead connections** (still "running" in Nomad, NOT auto-healed) and the
ingester's failed in-flight batch triggered the checkpoint-skip bug. **Lesson:** prefer the non-disruptive
`max_parallel_workers_per_gather=0` reload over a PG restart mid-bring-up; if you must bounce PG, expect to
**restart the ingester + transformer afterward** and gap-scan.

### Verified-good end state (2026-06-25 ~19:00Z)
Bronze continuous from 62800000 (gap=0); transformer checkpoint advancing
(`realtime_transformer_checkpoint` 62799999 → 62800499 → 62800999 → …), all transforms ✅, no shm errors;
ingester stable at batch=100 / 16 G, no OOM. Pipeline moving **bronze → silver_hot**; serving (`09`) next.
Catch-up to head will take hours (bronze is the bottleneck).

### Serving start + query-api: the gateway-timeout thread (2026-06-25)
Starting serving (`09`) and hitting the gateway surfaced more footguns — none data-corrupting, but all caused timeouts:

1. **Two serving processors apply the schema concurrently → lock-stall.** `09` starts BOTH
   `serving-projection-processor` (current-state/stats) and `-fast` (recent feeds); each auto-applies the
   serving schema on startup. Against the large backfilled serving tables (`sv_accounts_current` 19.6M,
   `sv_account_balances_current` 163M) the concurrent `CREATE INDEX`/DDL collide (one blocked on the other)
   and hang 10+ min, holding locks that stall the query-api. **Fix:** start them sequentially (let one
   finish the schema apply, then start the other), or make only one own schema application. **Do NOT
   restart a processor mid-index-build** — it rolls back the build and it never finishes.
2. **The serving cold-backfill creates tables+data but NOT all indexes.** So the first streaming start
   builds them — `CREATE INDEX` on 50M-row serving feed tables (`sv_operations_recent`, …) takes many
   minutes. One-time cost; let it finish (watch `pg_stat_progress_create_index`). **Better:** have the
   cold-backfill create the serving indexes, or the processor use `CREATE INDEX CONCURRENTLY`.
3. **Query-api `/home/summary` was 15s (gateway timeout) by recomputing what serving already precomputes.**
   `loadActivityMix` scanned the full ~14.5M-row `enriched_history_operations` for a 24h `COUNT(DISTINCT)`,
   and a second live fallback did the same for "active contracts" — but `serving.sv_network_stats_current`
   already has `ops_24h_*` / `active_contracts_24h`. The `GetServingNetworkStats` loader simply wasn't
   SELECTing those columns, so `Soroban.ActiveContracts24h` was 0 and the fallback fired. **Fix (shipped,
   `home_summary.go` + `silver_hot_reader.go`):** loader reads the precomputed columns; `loadActivityMix`
   returns them from the already-loaded `servingStats`. Result **15s → ~3s, `partial:false`, warnings
   cleared.** Caveat: activity-mix numbers are now op-level counts (`ops_24h_payments` /
   `ops_24h_contract_invoke`), not distinct-tx — add exact `*_tx_24h` columns to the projection if exactness
   matters. Index also added: `contract_data_current(key_hash)` (TTL-attention join; PK is
   `(contract_id, key_hash)` so key_hash alone wasn't indexed).
4. **Orphaned queries on context-timeout.** Each old 15s-timed-out `/home/summary` left its
   `COUNT(DISTINCT)` running **server-side for minutes** (Go ctx cancel didn't promptly kill the PG
   backend). They accumulate and burn CPU/IO — `pg_cancel_backend` the long-running orphans after a
   slow-endpoint episode.
5. **Residual latency during catch-up is contention, not code.** After the fixes `/home/summary` is ~3s —
   `COUNT(*)` on `sv_accounts_current` is only ~450ms; the rest is the serving processor's heavy aggregate
   rebuilds + the transformer saturating PG during the multi-day catch-up. Drops toward sub-second as
   catch-up settles. Use `reltuples` (from `pg_class`) instead of `COUNT(*)` for "total accounts" to shave it.

**General lesson:** the query-api should never recompute live what a serving projection already maintains —
verify each `/home/summary` sub-query reads a precomputed serving row, not a multi-million-row source scan.

---

## Reference — current working images & key identifiers
- **Streaming images (protocol 27, from `main` `73b3ad8`):** `proto27-73b3ad8-20260625175502` for all four:
  `withobsrvr/{stellar-live-source-datalake, stellar-postgres-ingester, silver-realtime-transformer,
  serving-projection-processor}`.
- **Query-api:** `withobsrvr/stellar-query-api:proto27-homesummary-73b3ad8-20260625213104` (protocol-27 +
  the `/home/summary` precompute fix — PART 4 §3). Still rebuild `stellar-history-loader` before any future
  bronze backfill.
- Projector: `withobsrvr/silver-current-state-projector:prfix-31c5948-20260625163849`
- Serving (cold backfill): `withobsrvr/serving-cold-backfill:pgnative-1a1747b-20260625142407`
- Mainnet backfill as-of ledger **N = 62799999**.
- Branch `bugfix/tmosley/current-state-projector-single-pass` **merged to `main` as PR #84 (`73b3ad8`)**.
- Handoff scripts: `…/obsrvr-lake-mainnet/scripts/15-streaming-handoff-seed.sh` (seed),
  `obsrvr-lake/scripts/streaming-handoff-check.sh` (read-only pre-flight).
- Detailed session handoff: `thoughts/shared/handoffs/general/2026-06-24_16-48-50_silver-current-state-backfill.md`.
