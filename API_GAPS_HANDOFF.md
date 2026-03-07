# API Gaps Fix — Handoff Document

## Date: 2026-03-06

## What Was Completed

### Fix 1: Expose 3 bronze ledger columns (MUST)

**Status: DEPLOYED + DUCKLAKE SCHEMA UPDATED — working**

Added `soroban_op_count`, `total_fee_charged`, `contract_events_count` to the bronze ledgers API response. Also updated the DuckLake catalog metadata to include these columns so cold storage queries resolve them.

**Code changes:**
- `obsrvr-lake/stellar-query-api/go/hot_reader.go` — Added 3 columns to SELECT in `QueryLedgers()`
- `obsrvr-lake/stellar-query-api/go/cold_reader.go` — Added 3 columns to SELECT in cold `QueryLedgers()`
- `obsrvr-lake/stellar-query-api/go/query_service.go` — Added `sql.NullInt64` scan variables and conditional result map entries in `scanLedgers()`

**DuckLake schema change:**
- Inserted 3 rows into `bronze_meta.ducklake_column` for `table_id=42` (active `ledgers_row_v2`):
  - column_id 69: `soroban_op_count` (int32), column_order 27
  - column_id 70: `total_fee_charged` (int64), column_order 28
  - column_id 71: `contract_events_count` (int32), column_order 29
  - All with `begin_snapshot=4145`, `nulls_allowed=true`, `initial_default=NULL`, `default_value=NULL`

**Current behavior:** Columns appear in API response and cold storage queries resolve without error. Historical parquet files return `null` for these fields (data was written before the columns existed). New ledger data flushed to cold by the ingester will include real values.

**Verification:**
```bash
curl -H "$AUTH" "$BASE/bronze/ledgers?start=1363100&end=1363130&limit=1" | jq '.ledgers[0] | {soroban_op_count, total_fee_charged, contract_events_count}'
# Returns null for historical data; will return values for newly-ingested ledgers
```

---

### Fix 2: Populate `protocol_version` in network stats (MUST)

**Status: DEPLOYED — working**

Added a query in `HandleNetworkStats` to fetch `protocol_version` from the latest ledger in `ledgers_row_v2` via the `unifiedReader` (tries bronze hot first, then bronze cold).

**Files changed:**
- `obsrvr-lake/stellar-query-api/go/network_stats.go` — Added protocol_version query block in `HandleNetworkStats`, before fee stats fetch

**Current behavior:** Returns `25` (correct for testnet).

**Verification:**
```bash
curl -H "$AUTH" "$BASE/silver/stats/network" | jq '.ledger.protocol_version'
# Returns: 25
```

---

### Fix 3: Compute real `avg_close_time_seconds` (NICE)

**Status: DEPLOYED — working**

Replaced hardcoded `5.0` with a real computation: queries the last 100 ledgers from bronze, computes `EXTRACT(EPOCH FROM MAX(closed_at) - MIN(closed_at)) / (COUNT(*) - 1)`, rounded to 2 decimal places.

**Files changed:**
- `obsrvr-lake/stellar-query-api/go/network_stats.go` — Added avg close time query block; added `"log"` and `"math"` imports

**Current behavior:** Returns `5.0` — this is the real computed value (Stellar testnet genuinely averages ~5 second ledger closes: 495 seconds across 99 gaps = 5.0s/ledger).

**Technical note:** The bronze hot buffer currently has no ledger data (flushed to cold), so the query falls through to bronze cold which succeeds. A non-fatal error log is emitted for the hot schema miss.

**Verification:**
```bash
curl -H "$AUTH" "$BASE/silver/stats/network" | jq '.ledger.avg_close_time_seconds'
# Returns: 5 (real computed, not hardcoded)
```

---

### Fix 4: Soroban state entries cold fallback (COULD)

**Status: DEPLOYED — code works, data empty (pipeline dependency)**

Added a cold bronze fallback in `HandleSorobanStats`: when silver `contract_data_current` returns 0 entries, it tries querying `contract_data_snapshot_v1` from bronze cold storage using `contract_durability` column and `COUNT(DISTINCT key_hash)`.

**Files changed:**
- `obsrvr-lake/stellar-query-api/go/handlers_fee_stats.go` — Added cold fallback block after the silver state query

**Current behavior:** Returns 0 for both persistent and temporary entries. Both silver `contract_data_current` and bronze cold `contract_data_snapshot_v1` are empty. This is a pipeline/data dependency — the code path is correct and will work once data flows through.

**Verification:**
```bash
curl -H "$AUTH" "$BASE/silver/stats/soroban" | jq '.state'
# Returns: {"persistent_entries": 0, "temporary_entries": 0}
```

---

### Fix 5: Contract metadata population (COULD)

**Status: NOT IMPLEMENTED — pipeline dependency only, no code gap**

The handler already has a fallback path that returns observed functions from `contract_invocations_raw`. The gap is missing data in `contract_metadata` / `contract_creations_v1`, not missing code. Requires either a backfill run or transformer update to populate.

---

## Remaining Issues / Follow-ups

1. **Historical parquet files lack new columns**: Existing cold storage parquet files for `ledgers_row_v2` were written before `soroban_op_count`, `total_fee_charged`, `contract_events_count` existed. These return `null`. Only newly-flushed data will have values. A backfill could rewrite historical parquet files if needed.

2. **Bronze hot buffer empty for ledgers**: The hot buffer flushes ledger data to cold quickly. The `current_sequence` in network stats shows `0` because it comes from `MAX(last_modified_ledger) FROM accounts_current` in the silver hot reader, and accounts_current may be empty. Consider also pulling `current_sequence` from bronze `ledgers_row_v2` as a fallback.

3. **Soroban state entries (pipeline)**: `contract_data_snapshot_v1` in both hot and cold bronze is empty. The silver transformer needs to either read from cold bronze or the ingester needs to retain this data longer in hot.

4. **Contract metadata (pipeline)**: `contract_metadata` table needs population via backfill tool or transformer update.

## Deployed Image

- `withobsrvr/stellar-query-api:latest` (Docker Hub)
- Nomad job version 22, deployment `4a877bcf` — successful
- `force_deploy` timestamp in Nomad job: `2026-03-07T01:30:00Z`
