# Unified Processor — Developer Handoff

## What Changed

### Phase 1: Silver Removal (bronze-only focus)
Removed all silver, semantic, and index processing from the unified processor to focus on bronze ingestion only. Silver processing was causing significant performance degradation when combined with bronze in a single pass.

**Deleted files:**
- `silver_transforms.go` — 5 silver transforms + 7 semantic transforms
- `silver_current_state.go` — 4 current-state materializations
- `index_builder.go` — stubbed index builder

**Modified:**
- `ducklake_writer.go` — stripped to bronze-only (write bronze + checkpoint)
- `schemas.go` — removed silverSchemaSQL(), indexSchemaSQL(), createSilverTables(), createIndexTables()
- `config.go` — removed SilverSchema and IndexSchema fields
- `processor.go` — updated pipeline comment

### Phase 2: V3 Schema Parity
Updated all bronze schemas, data types, extractors, and writers to match the V3 production schema used by `stellar-postgres-ingester` and `postgres-ducklake-flusher`.

**Key changes:**
- `pipeline_version` replaced with `era_id` + `version_label` across all 20 tables
- LedgerRow: added 10 V3 fields (Soroban stats, node_id, signature, ledger_header, etc.)
- TransactionRow: expanded from 28 to 49 columns (fee bump, preconditions, Soroban fee breakdown, signers)
- OperationRow: added operation_trace_code, trustor, authorize fields, claimants_count
- ConfigSettingRow: added 13 Soroban resource limit detail fields
- All snapshot types: added `created_at` (and `updated_at` on accounts)
- Timebounds changed from TEXT to BIGINT

## Current Architecture

```
Stellar Data Source (gRPC stream)
    ↓
Processor (per-ledger pipeline)
    ↓
XDR Decode → Bronze Extraction (20 parallel extractors)
    ↓
DuckLake Writer (single tx: write bronze → checkpoint)
    ↓
Periodic Flush (inlined rows → Parquet on B2)
```

## Schema Reference
See `BRONZE_SCHEMA_REFERENCE.sql` for the canonical V3 schema (20 tables, all column definitions with comments).

## What's NOT Done Yet
- Silver processing needs to be a **separate service/stage** (not combined with bronze)
- `go-stellar-sdk` is on v0.3.0 — v0.4.0 available but not yet upgraded
- `OperationTraceCode` extraction not fully implemented (set to nil)
- `ContractsInvolved` on operations not fully populated (set to nil)
- `SorobanDataResources` extraction for transaction footprints not implemented

## Build & Run
```bash
cd obsrvr-lake/obsrvr-lake-unified-processor/go
GOWORK=off go build ./...
```
