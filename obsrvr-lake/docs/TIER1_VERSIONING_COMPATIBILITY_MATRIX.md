# Tier 1 Versioning Compatibility Matrix

## Purpose

This document is the **Tier 1 audit view** for incremental versioning.

Tier 1 goal:

> make version metadata real and preserve it across hot/cold boundaries

For each representative table/path below, this matrix tracks whether:

- schema includes `era_id` / `version_label`
- writers populate them
- flushers preserve them
- readers preserve them
- the path is considered Tier-1-safe today

This is **not** the routing/read-enforcement audit for Tier 2/3.

---

## Legend

- **Yes**: implemented and observed in current code
- **Partial**: some path preserves metadata, but another important path still drops it
- **No**: not implemented / metadata lost
- **N/A**: not relevant for that step

---

## Summary Matrix

| Dataset / Path | Hot schema | Hot write populates | Bronze cold schema | Hot→cold preserves | Cold reader preserves | Silver hot schema | Silver hot write preserves | Silver cold preserves | Tier 1 status |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| `transactions_row_v2` | Yes | Yes | Yes | Yes | N/A | N/A | N/A | N/A | **Good** |
| `accounts_snapshot_v1` | Yes | Yes | Yes | Yes | Yes | `accounts_snapshot` / `accounts_current`: Yes | Yes | `accounts_snapshot` / `accounts_current`: Yes | **Good** |
| `trustlines_snapshot_v1` | Yes | Yes | Yes | Yes | Yes | `trustlines_snapshot`: Yes / `trustlines_current`: Yes | Yes | Yes | **Good** |
| `offers_snapshot_v1` | Yes | Yes | Yes | Yes | Yes | `offers_snapshot`: Yes / `offers_current`: Yes | Yes | Yes | **Good** |
| `account_signers_snapshot_v1` | Yes | Yes | Yes | Yes | Yes | `account_signers_snapshot`: Yes | Yes | Yes | **Good** |
| `contract_invocations_raw` | Source = bronze `operations_row_v2`: Yes | Yes | Source preserved in bronze cold ops | Yes | Yes | Yes | Yes | Yes | **Good** |
| `contract_metadata` | Source = bronze `contract_creations_v1`: Yes | Yes | Yes | Yes | Hot reader only today | Yes | Yes | Yes | **Good** |

---

## Detailed Notes

## 1. `transactions_row_v2`

### Status
- Hot schema: **Yes**
- Hot writer populates: **Yes**
- Bronze cold schema: **Yes**
- Bronze flusher preserves: **Yes**
- Tier 1: **Good**

### Evidence
- `stellar-postgres-ingester/go/types.go`
- `stellar-postgres-ingester/go/writer.go`
- `stellar-postgres-ingester/migrations/007_add_versioning_metadata.sql`
- `postgres-ducklake-flusher/go/duckdb.go`
- `postgres-ducklake-flusher/v3_bronze_schema.sql`

### Notes
This is the best simple proof that bronze-hot metadata is now real on a core append table.

---

## 2. `accounts_snapshot_v1` → `accounts_snapshot` / `accounts_current`

### Status
- Bronze hot schema/write: **Yes**
- Bronze hot→cold preserve: **Yes**
- Bronze cold reader preserve: **Yes**
- Silver hot snapshot schema/write: **Yes**
- Silver hot current schema/write: **Yes**
- Silver hot→cold preserve: **Yes**
- Tier 1: **Good**

### Evidence
- Bronze hot:
  - `stellar-postgres-ingester/go/types.go`
  - `stellar-postgres-ingester/go/writer.go`
- Bronze cold:
  - `postgres-ducklake-flusher/go/duckdb.go`
  - `silver-realtime-transformer/go/bronze_cold_reader.go`
- Silver hot:
  - `silver-realtime-transformer/go/schema/init_silver_hot_complete.sql`
  - `silver-realtime-transformer/go/transformer.go`
  - `silver-realtime-transformer/go/types.go`
  - `silver-realtime-transformer/go/silver_writer_batch.go`
- Silver cold:
  - `silver-cold-flusher/go/duckdb.go`
  - `silver-cold-flusher/schema/silver_schema.sql`

### Notes
`accounts_*` is the cleanest end-to-end Tier 1 success path in the active runtime stack.

---

## 3. `trustlines_snapshot_v1` → `trustlines_snapshot` / `trustlines_current`

### Status
- Bronze hot schema/write: **Yes**
- Bronze hot→cold preserve: **Yes**
- Bronze cold reader preserve: **Yes**
- Silver hot snapshot schema/write: **Yes**
- Silver hot current schema/write: **Yes**
- Silver cold preserve: **Yes**
- Tier 1: **Good**

### Evidence
- Bronze hot:
  - `stellar-postgres-ingester/go/writer.go`
- Bronze cold:
  - `postgres-ducklake-flusher/go/duckdb.go`
  - `silver-realtime-transformer/go/bronze_cold_reader.go`
- Silver snapshot path:
  - `silver-realtime-transformer/go/transformer.go`
  - `silver-realtime-transformer/go/types.go`
  - `silver-realtime-transformer/go/silver_writer_batch.go`
- Silver current path:
  - `silver-realtime-transformer/go/schema/init_silver_hot_complete.sql`
  - `silver-realtime-transformer/go/types.go`
  - `silver-realtime-transformer/go/bronze_reader.go`
  - `silver-realtime-transformer/go/bronze_cold_reader.go`
  - `silver-realtime-transformer/go/transformer.go`
  - `silver-realtime-transformer/go/silver_writer.go`
  - `silver-realtime-transformer/go/silver_writer_batch.go`
  - `silver-cold-flusher/go/duckdb.go`
  - `silver-cold-flusher/go/silver_schema.go`
  - `silver-cold-flusher/schema/silver_schema.sql`

### Notes
Both the snapshot/history path and the current-state path now preserve
`era_id` / `version_label`.

---

## 4. `offers_snapshot_v1` → `offers_snapshot` / `offers_current`

### Status
- Bronze hot schema/write: **Yes**
- Bronze hot→cold preserve: **Yes**
- Bronze cold reader preserve: **Yes**
- Silver hot snapshot schema/write: **Yes**
- Silver hot current schema/write: **Yes**
- Silver cold preserve: **Yes**
- Tier 1: **Good**

### Evidence
- Bronze hot:
  - `stellar-postgres-ingester/go/writer.go`
- Bronze cold:
  - `postgres-ducklake-flusher/go/duckdb.go`
  - `silver-realtime-transformer/go/bronze_cold_reader.go`
- Silver snapshot path:
  - `silver-realtime-transformer/go/transformer.go`
  - `silver-realtime-transformer/go/types.go`
  - `silver-realtime-transformer/go/silver_writer_batch.go`
- Silver current path:
  - `silver-realtime-transformer/go/schema/init_silver_hot_complete.sql`
  - `silver-realtime-transformer/go/types.go`
  - `silver-realtime-transformer/go/bronze_reader.go`
  - `silver-realtime-transformer/go/bronze_cold_reader.go`
  - `silver-realtime-transformer/go/transformer.go`
  - `silver-realtime-transformer/go/silver_writer.go`
  - `silver-realtime-transformer/go/silver_writer_batch.go`
  - `silver-cold-flusher/go/duckdb.go`
  - `silver-cold-flusher/go/silver_schema.go`
  - `silver-cold-flusher/schema/silver_schema.sql`

### Notes
Both the snapshot/history path and the current-state path now preserve
`era_id` / `version_label`.

---

## 5. `account_signers_snapshot_v1` → `account_signers_snapshot`

### Status
- Bronze hot schema/write: **Yes**
- Bronze hot→cold preserve: **Yes**
- Bronze cold reader preserve: **Yes**
- Silver hot snapshot schema/write: **Yes**
- Silver cold preserve: **Yes**
- Tier 1: **Good**

### Evidence
- `stellar-postgres-ingester/go/writer.go`
- `postgres-ducklake-flusher/go/duckdb.go`
- `silver-realtime-transformer/go/bronze_cold_reader.go`
- `silver-realtime-transformer/go/transformer.go`
- `silver-cold-flusher/schema/silver_schema.sql`

### Notes
This is another strong Tier 1 representative path because it is snapshot-only and avoids current-state ambiguity.

---

## 6. `contract_invocations_raw`

### Status
- Bronze source schema/write (`operations_row_v2`): **Yes**
- Bronze hot→cold preserve: **Yes**
- Bronze hot/cold readers preserve: **Yes**
- Silver hot schema/write: **Yes**
- Silver hot→cold preserve: **Yes**
- Tier 1: **Good**

### Evidence
- Bronze source:
  - `stellar-postgres-ingester/go/writer.go`
  - `postgres-ducklake-flusher/go/duckdb.go`
- Bronze readers:
  - `silver-realtime-transformer/go/bronze_reader.go`
  - `silver-realtime-transformer/go/bronze_cold_reader.go`
- Silver hot:
  - `silver-realtime-transformer/go/schema/init_silver_hot_complete.sql`
  - `silver-realtime-transformer/go/types.go`
  - `silver-realtime-transformer/go/transformer.go`
  - `silver-realtime-transformer/go/silver_writer.go`
  - `silver-realtime-transformer/go/silver_writer_batch.go`
- Silver cold:
  - `silver-cold-flusher/go/duckdb.go`
  - `silver-cold-flusher/go/silver_schema.go`
  - `silver-cold-flusher/schema/silver_schema.sql`

### Notes
This path was previously collapsing metadata in silver cold; it now preserves it.

---

## 7. `contract_metadata`

### Status
- Bronze source schema/write (`contract_creations_v1`): **Yes**
- Bronze hot→cold preserve: **Yes**
- Bronze reader preserve: **Hot reader only currently audited**
- Silver hot schema/write: **Yes**
- Silver hot→cold preserve: **Yes**
- Tier 1: **Good**

### Evidence
- Bronze source:
  - `stellar-postgres-ingester/go/writer.go`
  - `postgres-ducklake-flusher/go/duckdb.go`
- Silver hot:
  - `silver-realtime-transformer/go/schema/init_silver_hot_complete.sql`
  - `silver-realtime-transformer/go/transformer.go`
  - `silver-realtime-transformer/go/silver_writer.go`
  - `silver-realtime-transformer/go/silver_writer_batch.go`
- Silver cold:
  - `silver-cold-flusher/go/duckdb.go`
  - `silver-cold-flusher/go/silver_schema.go`
  - `silver-cold-flusher/schema/silver_schema.sql`

### Notes
This is Tier-1-good for preservation, but it still does **not** imply version-aware routing/selection.

---

## Remaining Tier 1 Gaps

These are the main remaining Tier 1 gaps after the current implementation slice.

### A. Tier 1 validation is representative, not exhaustive

Current operational checks validate representative tables, but there is not yet a full table-by-table integration test proving hot and cold parity everywhere.

Evidence:
- `/home/tillman/Documents/infra/environments/prod/latitude/obsrvr-lake-testnet/scripts/10-verify.sh`

---

## Recommended Next Implementation Slice

1. Add one integration-style validation for hot/cold parity on:
   - `accounts_snapshot_v1`
   - `contract_invocations_raw`
   - `trustlines_snapshot_v1`

That would move Tier 1 from **representatively working** to **nearly complete for active table families**.
