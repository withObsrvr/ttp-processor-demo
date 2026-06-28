# DuckLake 0.4 → 1.0 Catalog Migration — Path A (in-place), testnet first

_Written 2026-06-26. Migrates the DuckLake catalog from the in-development **0.4** format (produced by
`INSTALL ducklake FROM core_nightly`) to **stable 1.0**, by moving every catalog-touching component onto
pinned **stable `INSTALL ducklake` + DuckDB 1.5.4**. Companion to the roadmap doc (the lightweight-serving
fork) and the lessons doc (the "DO NOT migrate carelessly" warning — this is how to do it deliberately)._

## ⚠️⚠️ ANY 1.5.4 image deploy MIGRATES THE CATALOG ON ATTACH — read this FIRST
Every image built off `chore/ducklake-stable-1.5.4` (`INSTALL ducklake` stable + DuckDB 1.5.4) does
`ATTACH … (AUTOMATIC_MIGRATION TRUE)`. **The instant ONE such image attaches a catalog, that catalog
(per metadata schema) migrates 0.4 → 1.0 — irreversibly, with no prompt.** This includes *readers*, not
just flushers: `stellar-query-api` and `silver-realtime-transformer` both attach cold and will trigger
it. `stellar-query-api` attaches **`bronze_meta`, `silver_meta`, AND the index catalog**, so deploying
it alone migrates all three.

**Consequence:** do NOT deploy any 1.5.4 image to an environment until you are ready to migrate the WHOLE
lockstep set there — once the catalog moves, every component still on a 0.4/nightly image FATALs on attach
(`Only DuckLake versions 0.1…0.4 are supported`). This actually happened on mainnet 2026-06-27: a
`stellar-query-api` deploy (carrying an unrelated fix, but built off this branch) silently migrated the
mainnet bronze catalog and took `postgres-ducklake-flusher` down, forcing the mainnet migration early.

**Before deploying the first 1.5.4 image anywhere: take the catalog backup (below) and have the rest of the
lockstep set's `dl154` tags staged to deploy immediately after.**

## The mechanic (read this first)
- The catalog is **metadata in Postgres** (`ducklake_*` tables + `bronze_meta`/`silver_meta`/`index`
  schemas). **Parquet data on B2 is the source of record and is NOT rewritten** — this is a metadata-only
  migration.
- It is **one-way and in-place**. Your `pg_dump` of the catalog DB is the only rollback.
- The migration is **per catalog (per metadata schema): `bronze_meta`, `silver_meta`, `index` are each
  migrated separately**, by the first component that attaches *that schema* with `AUTOMATIC_MIGRATION TRUE`.
- After the code fix in `7358f17`, **all 10 components now set `AUTOMATIC_MIGRATION TRUE`** (silver-cold-flusher
  was the one exception — it errored "catalog version mismatch … set AUTOMATIC_MIGRATION to TRUE"). So each
  catalog migrates *automatically* on the first attach by any of its components. You don't run a migrate
  command; **you control which build attaches first.** This means **no canary** — it's all-or-nothing per
  catalog, so the whole set moves **lockstep** (a lagging 0.4 component breaks after migration).
- ⚠️ **Because the *other* 9 components auto-migrate on attach, the catalog backup MUST happen before you
  start *any* of them** — not just before silver-cold-flusher. Starting e.g. postgres-ducklake-flusher
  migrates `bronze_meta` immediately.

## Images (built off branch `chore/ducklake-stable-1.5.4`, commit `993f597`)
Tag: **`dl154-993f597-20260626103213`** for 9 of the catalog services, and
**`dl154-79b06c1-20260626110409`** for **`silver-cold-flusher`** (rebuilt with the ATTACH parity fix:
AUTOMATIC_MIGRATION TRUE + OVERRIDE_DATA_PATH TRUE on both its primary and create-fresh fallback).
All are `INSTALL ducklake` stable + `duckdb-go/v2 v2.10504.0` (DuckDB 1.5.4).

## The lockstep set
**Testnet (7 services)** — already bumped to the tag above in `obsrvr-lake-testnet/nomad/`:
| Role (vs catalog) | Service |
|---|---|
| writer (migrates on attach) | `silver-cold-flusher`, `postgres-ducklake-flusher`, `stellar-history-loader`, `index-plane-transformer`, `contract-event-index-transformer` |
| reader | `silver-realtime-transformer` (bronze cold reader), `stellar-query-api` |

> Testnet has **no** `serving-cold-backfill` / `silver-current-state-projector` / `silver-history-loader`
> nomad jobs, so the set is smaller than mainnet's. The **ingester is NOT in the set** (it writes bronze
> *hot* Postgres, not the catalog).

**Mainnet (10 services)** = the 7 above **plus** `serving-cold-backfill`, `silver-current-state-projector`,
`silver-history-loader`. (Images for all 10 are built under the same tag.)

---

## Pre-flight (do not skip)

1. **Compatibility check — the one real unknown.** On a throwaway DuckDB 1.5.4, confirm stable ducklake
   loads and is 1.x, and httpfs loads (the catalog services need both):
   ```
   INSTALL ducklake; LOAD ducklake; INSTALL httpfs; LOAD httpfs;
   SELECT extension_name, extension_version FROM duckdb_extensions()
   WHERE extension_name IN ('ducklake','httpfs');
   ```
   Then `ATTACH 'ducklake:postgres:...' AS t (METADATA_SCHEMA '...', AUTOMATIC_MIGRATION TRUE)` against a
   **copy** of the catalog and confirm it migrates + reads. If stable ducklake for 1.5.4 needs a different
   DuckDB, fix that here — before touching the real catalog.
2. **Confirm all 10 images built + pushed** (`/tmp/dl_build.log` → `PUSH_OK` ×10; or `docker manifest
   inspect withobsrvr/<svc>:dl154-993f597-20260626103213`).
3. **Locate the testnet catalog Postgres** (the DuckLake metadata DB) from the testnet nomad env
   (`BRONZE_DUCKLAKE_CATALOG` / `SILVER_DUCKLAKE_CATALOG` DSNs) and the testnet access runbook.
4. **Inventory the running testnet jobs** in the lockstep set so the stop is complete — one lagging
   nightly-0.4 job breaks after migration.

---

## Migration procedure (testnet)

Run from `obsrvr-lake-testnet/` (source `nomad/.secrets` for `NOMAD_ADDR`/token + SSH key).

1. **Quiesce — stop ALL 7 catalog jobs.** `AUTOMATIC_MIGRATION TRUE` means you cannot bring up one as a
   canary; the first stable attach migrates for everyone.
   ```
   for j in silver-cold-flusher postgres-ducklake-flusher stellar-history-loader \
            index-plane-transformer contract-event-index-transformer \
            silver-realtime-transformer stellar-query-api; do nomad job stop "$j"; done
   ```
2. **Back up the catalog (this IS your rollback).** `pg_dump` the catalog DB — at minimum the
   `bronze_meta`, `silver_meta`, and `index` schemas (and `public.ducklake_*` if present):
   ```
   pg_dump "$CATALOG_DSN" -n bronze_meta -n silver_meta -n index -Fc -f catalog-pre-1.0-$(date +%s).dump
   ```
   B2 Parquet is untouched, so this dump fully captures the rollback point.
3. **Deploy the lockstep set together** (tags already point at `dl154-993f597-20260626103213`). Bring up a
   **writer first** so its first `ATTACH` runs the migration; watch its logs:
   ```
   nomad job run nomad/silver-cold-flusher.nomad      # writer → triggers AUTOMATIC_MIGRATION
   # confirm it attached + migrated in the logs, then the rest:
   nomad job run nomad/postgres-ducklake-flusher.nomad
   nomad job run nomad/stellar-history-loader.nomad
   nomad job run nomad/index-plane-transformer.nomad
   nomad job run nomad/contract-event-index-transformer.nomad
   nomad job run nomad/silver-realtime-transformer.nomad
   nomad job run nomad/stellar-query-api.nomad
   ```
4. **Also start the obsrvr-lake-postgresql job** if the `shm_size` edit means a redeploy is desired (the
   transformer's parallel transforms will need it). It restarts PG — expect the connected jobs to reconnect.

---

## Verification
- **Catalog format is now 1.x** — check the version marker in the catalog DB (the `ducklake_*` metadata
  reports the new format; the `ATTACH` succeeds without "needs migration").
- **No "needs migration" / version-mismatch errors** in any of the 7 job logs.
- **Data intact** — table/snapshot lists + row counts match pre-migration; run the bronze-completeness
  invariants; `stellar-query-api` serves cold queries; the flushers write new cold partitions on 1.0.
- **Soak** — let streaming flush new cold data on 1.0, confirm no regressions.

## Rollback
Only clean **before significant new 1.0 cold writes** (testnet can always re-flush):
1. Stop the 7 jobs.
2. Restore the catalog dump (`pg_restore`) — back to 0.4 metadata.
3. Re-deploy the previous (nightly-0.4) images.
   B2 data is untouched; any *new* 1.0 snapshots written after migration are lost on restore (their Parquet
   remains on B2 but the restored 0.4 catalog won't reference them) — which is why you decide go/no-go
   during the soak, before meaningful new writes.

---

## Mainnet delta (after testnet is green)
Run the **same images/tag** (don't re-derive). Mainnet's lockstep set adds `serving-cold-backfill`,
`silver-current-state-projector`, `silver-history-loader` — and those are *also* heavy backfill jobs, so
ensure they're stopped during quiesce. Mainnet already has `shm_size`; only the ducklake tags + the
catalog backup differ in scale.

## Post-migration hardening (recommended)
- **Consider `AUTOMATIC_MIGRATION FALSE`** on the `ATTACH` statements once you're on stable, so a future
  format bump is a deliberate decision, not an automatic one on the next version jump.
- **Pin off `:latest`** — testnet `stellar-history-loader` and `silver-realtime-transformer` were on
  mutable `:latest` before this (now pinned to the dl154 tag). Keep them pinned.
- These images are also the path off the **rolling nightly** — that drift risk is gone once everything is
  on stable.
