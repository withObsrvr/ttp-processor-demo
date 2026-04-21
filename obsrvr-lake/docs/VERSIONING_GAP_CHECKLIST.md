# obsrvr-lake Incremental Versioning Gap Checklist

## Purpose

This document captures the current gap between:

1. the **desired reconciliation-safe versioning model** described in
   `/home/tillman/Documents/ttp-processor-demo/ducklake-ingestion-obsrvr-v3/INCREMENTAL_VERSIONING_GUIDE.md`
2. the **currently implemented obsrvr-lake ingestion/query stack**

It is intended to answer a practical operational question:

> If we need to reingest a historical ledger range with corrected extraction logic,
> can we safely keep old data, ingest corrected data alongside it, and route reads
> to the corrected version?

## Short Answer

**Not end-to-end yet.**

The repo already has substantial schema-level and partial pipeline support for:

- `ledger_range`
- `era_id`
- `version_label`
- snapshot history (`valid_to` in some silver tables)

But the currently active stack does **not yet** provide a complete, reconciliation-safe versioning system.

Main gaps:

- hot bronze ingestion does not consistently populate `era_id` / `version_label`
- some cold readers/flushers explicitly null them out
- no active-era / active-version resolver was found in the current obsrvr-lake runtime path
- no evidence that API queries enforce version filtering or overlay logic

---

## Desired End State

A reconciliation-safe versioning system should support:

- reingesting a ledger range with corrected logic without deleting old data
- preserving previous versions for audit/debugging
- selecting the active corrected version automatically in production reads
- supporting explicit historical queries against older versions when needed

Minimum required capabilities:

- all bronze writes populate `era_id` and `version_label`
- hot and cold paths preserve those fields consistently
- metadata exists for active era/version per network/dataset
- readers can choose:
  - explicit version filter, or
  - latest-version overlay
- silver transformations and APIs use version-aware reads

---

## Audit Summary

| Component | `ledger_range` | `era_id` / `version_label` in schema | Populates them | Preserves them | Routes by them | Reconciliation-safe today? |
|---|---:|---:|---:|---:|---:|---:|
| `stellar-postgres-ingester` | Yes | Partial / schema-aware | No (active hot path) | N/A | No | No |
| `stellar-history-loader` | Yes | Yes | Yes | Yes | No | Partial |
| `postgres-ducklake-flusher` | Yes | Yes | Depends on source | Partial | No | Partial |
| `silver-realtime-transformer` hot bronze reader/writer | Yes | Yes | Reads/writes some | Partial | No | Partial |
| `silver-realtime-transformer` cold reader | Yes | Yes in target shape | No (often nulls) | No | No | No |
| `silver-cold-flusher` | Yes | Yes | Partial | Partial / sometimes nulls | No | No |
| `stellar-query-api` | Yes | Yes | N/A | Returns fields | No | No |
| End-to-end runtime | Yes | Partial | Inconsistent | Inconsistent | No | No |

---

# Component Checklist

## 1. `stellar-postgres-ingester`

### Current findings

- Uses `ledger_range` actively.
- Local runtime structs do **not** consistently carry `EraID` / `VersionLabel`.
- Conversion helpers from `stellar-extract` explicitly drop library-only fields.
- Active PostgreSQL insert paths were observed writing `ledger_range` but not consistently writing `era_id` / `version_label`.

### Evidence

- `stellar-postgres-ingester/go/types.go`
- `stellar-postgres-ingester/go/writer.go`
- `stellar-postgres-ingester/go/fix_all_inserts.py`

### Gaps

- [ ] Add `EraID` to all relevant hot bronze runtime structs.
- [ ] Add `VersionLabel` to all relevant hot bronze runtime structs.
- [ ] Preserve `EraID` / `VersionLabel` in conversion helpers from `stellar-extract`.
- [ ] Update all hot bronze insert statements to write `era_id` / `version_label`.
- [ ] Ensure conflict/update clauses behave correctly when a new version is ingested for overlapping ledger ranges.
- [ ] Decide whether hot bronze should allow coexistence of multiple versions or remain single-version with explicit correction workflow.

### Risk if not fixed

Historical replay into corrected versions cannot be safely represented in hot bronze, even if downstream schemas support it.

---

## 2. `stellar-history-loader`

### Current findings

- Strongest version-aware component found.
- Parquet writer includes `era_id` and `version_label`.
- Validation explicitly checks that `version_label` is populated.

### Evidence

- `stellar-history-loader/go/parquet_writer.go`
- `stellar-history-loader/go/validate.go`

### Gaps

- [ ] Confirm `era_id` is always populated when intended, not just optional.
- [ ] Document the canonical rules for setting `era_id` and `version_label` during replay.
- [ ] Standardize version naming convention across loader and streaming paths.
- [ ] Define whether this loader is the preferred correction/replay mechanism.

### Risk if not fixed

Historical loader may be version-aware while streaming hot ingestion remains version-blind, causing divergence between hot and cold semantics.

---

## 3. `postgres-ducklake-flusher`

### Current findings

- Bronze DuckLake schemas include `era_id` / `version_label`.
- Some explicit-column table mappings preserve those columns.
- Actual correctness depends on source PostgreSQL tables having those values.

### Evidence

- `postgres-ducklake-flusher/schema/bronze_schema.sql`
- `postgres-ducklake-flusher/go/duckdb.go`

### Gaps

- [ ] Verify every flushed bronze table preserves `era_id` / `version_label`.
- [ ] Remove any remaining positional/implicit writes that can misalign version columns.
- [ ] Add validation/health checks for missing `version_label` in flushed bronze cold data.
- [ ] Define whether flusher should reject rows with missing version metadata for versioned datasets.

### Risk if not fixed

Cold storage may appear version-capable in schema while actually storing null or inconsistent version metadata.

---

## 4. `silver-realtime-transformer` hot path

### Current findings

- Bronze hot reader selects `era_id` / `version_label` from snapshot tables.
- Silver writer includes `era_id` / `version_label` in several current/snapshot inserts.
- Snapshot/history path has some SCD-style `valid_to` handling.
- Some current-state upsert conflict clauses do not appear to update `era_id` / `version_label`.

### Evidence

- `silver-realtime-transformer/go/bronze_reader.go`
- `silver-realtime-transformer/go/silver_writer.go`
- `silver-realtime-transformer/go/silver_writer_batch.go`

### Gaps

- [ ] Audit every `*_current` upsert to ensure `era_id` / `version_label` are handled intentionally.
- [ ] Define whether current-state tables are single active version only, or multi-version aware.
- [ ] Ensure silver snapshot writes preserve version metadata everywhere, not just selected tables.
- [ ] Add tests for overlapping-version inputs on the same ledger range.
- [ ] Decide how `valid_to` should interact with corrected replay versions.

### Risk if not fixed

Silver may preserve version metadata in some records while still resolving current state using non-versioned upsert semantics.

---

## 5. `silver-realtime-transformer` cold bronze reader

### Current findings

- Multiple cold queries explicitly project:
  - `NULL AS era_id`
  - `NULL AS version_label`

### Evidence

- `silver-realtime-transformer/go/bronze_cold_reader.go`

### Gaps

- [ ] Remove `NULL AS era_id` from cold bronze reader queries where source data has these fields.
- [ ] Remove `NULL AS version_label` from cold bronze reader queries where source data has these fields.
- [ ] Confirm cold Bronze schemas and Parquet files actually contain the fields for all relevant tables.
- [ ] Add tests verifying hot and cold bronze readers return the same version metadata semantics.

### Risk if not fixed

Any replay/versioning model breaks across hot/cold boundaries, because cold reads erase the metadata required for routing.

---

## 6. `silver-cold-flusher`

### Current findings

- Silver DuckLake schemas include `era_id` / `version_label`.
- Some flush paths explicitly insert `NULL AS era_id, NULL AS version_label`.

### Evidence

- `silver-cold-flusher/schema/silver_current_state_ducklake.sql`
- `silver-cold-flusher/schema/silver_schema.sql`
- `silver-cold-flusher/go/duckdb.go`

### Gaps

- [ ] Audit every silver cold flush path for preservation of version metadata.
- [ ] Eliminate hardcoded null version fields where upstream tables already contain them.
- [ ] Define whether silver cold should support overlapping versions in the same ledger range.
- [ ] Add validation queries checking for null `version_label` in versioned silver datasets.

### Risk if not fixed

Silver cold may silently collapse version semantics even if silver hot preserved them.

---

## 7. `stellar-query-api`

### Current findings

- Readers select `era_id` / `version_label` from hot and cold sources.
- Query service includes those fields in API responses when present.
- No evidence was found of:
  - active era lookup
  - active version lookup
  - version filter enforcement
  - overlay/latest-version windowing

### Evidence

- `stellar-query-api/go/hot_reader.go`
- `stellar-query-api/go/cold_reader.go`
- `stellar-query-api/go/query_service.go`

### Gaps

- [ ] Introduce version-aware query mode for production paths.
- [ ] Support explicit `era_id` / `version_label` filtering in readers.
- [ ] Add overlay/latest-version query strategy for overlapping versions.
- [ ] Decide whether version selection belongs in API handlers, readers, or a shared resolver library.
- [ ] Add tests proving APIs return corrected-version rows instead of stale rows when overlap exists.

### Risk if not fixed

The API can expose version metadata but cannot safely choose the correct version when multiple versions overlap.

---

# Cross-Cutting Gaps

## A. No shared active version metadata layer found

### Gap

No evidence was found in the active obsrvr-lake runtime path for:

- `_meta_eras`
- active version metadata tables
- status=`active` routing
- shared resolver API

### Checklist

- [ ] Define canonical metadata tables for active era/version by network and dataset.
- [ ] Define lineage model (`supersedes`, `reconciles`, `reprocessed_from`, etc.).
- [ ] Create operational commands/workflow for activating a corrected version.

---

## B. No shared resolver/query builder found

### Gap

The richer behavior described in `INCREMENTAL_VERSIONING_GUIDE.md` was not found in the current obsrvr-lake runtime path.

### Checklist

- [ ] Build shared resolver/query builder for active version selection.
- [ ] Support two query modes:
  - [ ] explicit version filter
  - [ ] latest-version overlay
- [ ] Reuse resolver from silver transformers, APIs, exports, and analyst tooling.

---

## C. Hot/cold semantics are not aligned

### Gap

Even where schemas support version fields, some hot/cold readers and flushers do not preserve them consistently.

### Checklist

- [ ] Produce a table-by-table compatibility matrix for hot vs cold version metadata.
- [ ] Ensure every table with `era_id` / `version_label` preserves them in both directions.
- [ ] Add integration tests comparing hot and cold query results for the same ledger range.

---

## D. Replay workflow is not yet productized

### Gap

There is no clearly documented, reconciliation-safe replay workflow that guarantees corrected data can coexist with previous data and be selected deterministically.

### Checklist

- [ ] Define replay process for ledger range correction.
- [ ] Define how corrected data is tagged (`era_id`, `version_label`).
- [ ] Define validation steps before activation.
- [ ] Define rollback steps.
- [ ] Define observability/health checks for mixed-version ranges.

---

# Priority Plan

## Phase 1: Make bronze hot version-capable

- [ ] Add `EraID` / `VersionLabel` to hot ingester structs.
- [ ] Persist them in all bronze hot tables.
- [ ] Add tests proving values survive extraction → write.

## Phase 2: Preserve version metadata across hot/cold

- [ ] Remove `NULL AS era_id/version_label` from cold readers/flushers where not appropriate.
- [ ] Validate all bronze and silver cold tables retain version metadata.

## Phase 3: Add routing metadata + resolver

- [ ] Implement active version metadata tables.
- [ ] Implement shared resolver/query helper.
- [ ] Add explicit filter and latest-overlay modes.

## Phase 4: Make silver/API version-aware

- [ ] Update silver transformers to use version-aware reads.
- [ ] Update query API to route by active version.
- [ ] Add overlapping-version tests.

## Phase 5: Operationalize replay

- [ ] Write runbook for partial replay with corrected logic.
- [ ] Validate on a small historical range.
- [ ] Add dashboards/alerts for overlapping version ranges.

---

# Decision Log / Open Questions

- [ ] Should hot PostgreSQL retain multiple versions side-by-side, or only cold/DuckLake?
- [ ] Is `stellar-history-loader` the canonical correction path, or should streaming/hot also support corrected overlap directly?
- [ ] Should `era_id` represent protocol era, replay campaign, or both?
- [ ] Should `version_label` be global (`v1`, `v2`) or dataset/campaign-specific (`config-fix-2026-04-v1`)?
- [ ] How should current-state silver tables behave when two versions overlap for the same business key?

---

# Recommended Next Implementation Slice

If the goal is the **smallest useful step**, start here:

1. make `stellar-postgres-ingester` actually populate `era_id` / `version_label`
2. stop nulling those fields in cold readers/flushers
3. add one shared query helper that filters by explicit version
4. test on one dataset first:
   - `config_settings_snapshot_v1`
   - or `accounts_snapshot_v1`

That would move the system from **schema-ready but operationally incomplete** to **minimally version-aware**.
