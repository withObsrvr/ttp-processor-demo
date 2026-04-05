# Unified Silver Processor — Developer Handoff

## Architecture Decision (2026-04-05)

DuckLake writes are too slow for wide silver tables (enriched_history_operations has 98 columns).
Per-column stats tracking in PostgreSQL metadata makes every DuckLake INSERT expensive.

**Decision: Silver uses PostgreSQL hot buffer + periodic DuckLake cold flush.**

```
stellar-live-source → unified-processor → DuckLake bronze (works great)
                            ↓ gRPC :50053
                      silver-processor → PostgreSQL silver_hot (fast writes)
                            ↓ periodic (every 3hrs)
                      silver-cold-flusher → DuckLake silver (batch, with inlining)

query-engine → DuckLake bronze + PostgreSQL silver_hot (via DuckDB ATTACH POSTGRES)
```

### Next Steps (not yet implemented)

**1. Silver processor → PostgreSQL writes**
- Replace `ducklake_writer.go` with `postgres_writer.go`
- Use `lib/pq` or `pgx` PostgreSQL driver
- Same transform logic (transforms.go) but INSERT into PostgreSQL instead of DuckLake
- Schema: use `init_silver_hot_complete.sql` from old transformer (PostgreSQL-native types)
- Checkpoint: `silver_processor_checkpoint` table in silver_hot

**2. Silver cold flusher**
- Adapt existing `silver-cold-flusher` or create new one
- Use DuckDB `postgres_scan` + INSERT INTO DuckLake pattern (proven approach)
- Enable data inlining (eliminates merge-adjacent-files maintenance)
- Periodic `ducklake_flush_inlined_data` to consolidate to Parquet
- Flush frequency: every 3 hours

**3. Query engine unified reader**
- Use DuckDB ATTACH POSTGRES to connect to silver_hot
- Pattern: `ATTACH 'dbname=silver_hot host=... ' AS hot_db (TYPE POSTGRES)`
- Silver queries check hot_db first, fall back to DuckLake silver cold
- Bronze queries stay DuckLake-only (no change)
- Reference: old `stellar-query-api/go/unified_duckdb_reader.go`

**4. Silver history loader**
- Phase 1 works: bronze DuckLake → local Parquet (~108 ledgers/sec)
- Phase 2 needs change: push to PostgreSQL silver_hot instead of DuckLake
- Use `COPY FROM` or bulk INSERT for fast PostgreSQL loads
- Then cold flusher handles DuckLake population

## Current State (2026-04-05)

### What's Built
- Silver processor service that consumes bronze data via gRPC from unified processor
- Silver history loader (batch tool) that runs INSERT-SELECT from bronze DuckLake → silver DuckLake
- 31 silver table DDLs in schemas.go
- Transform implementations for both real-time (gRPC) and batch (SQL) paths
- Proto definition for BronzeLedgerService streaming

### Architecture
```
Bronze history loader → backfills bronze DuckLake (fast, parallel)
Unified bronze processor → real-time bronze from chain tip → gRPC :50053

Silver history loader → backfills silver from bronze DuckLake (SQL transforms)
Unified silver processor → real-time silver from gRPC stream
```

### What Works
- Bronze pipeline: fully functional, V3 schema, 200k+ ledgers loaded
- Silver table creation: all 31 tables created in DuckLake
- Silver history loader: runs at ~3,000 ledgers/sec, populates event/stream tables
- gRPC streaming: bronze processor broadcasts to silver processor subscribers

### Known Issue: Schema Misalignment

**schemas.go does NOT exactly match the old silver-realtime-transformer's DDL.**

The schemas.go agent deviated from the old `init_silver_hot_complete.sql` in several places:
- Some columns renamed (e.g., `seller_account` → `seller_id`, `low_threshold` → `threshold_low`)
- Some columns dropped entirely from DDL
- The history loader fix agent then stripped columns from INSERT statements to match the broken DDL

**This needs to be fixed properly:**
1. schemas.go should be regenerated to match `init_silver_hot_complete.sql` exactly (adapted for DuckDB types)
2. History loader INSERT-SELECT queries should include ALL columns from bronze
3. Silver processor transforms.go should also include all columns

**Reference files:**
- Old transformer DDL: `silver-realtime-transformer/go/schema/init_silver_hot_complete.sql`
- Old transformer SQL: `silver-realtime-transformer/go/transformer.go` (105K)
- Bronze V3 schema: `obsrvr-lake-unified-processor/BRONZE_SCHEMA_REFERENCE.sql`

### DuckDB vs PostgreSQL Type Mapping (Expected Differences)
- `TEXT[]` → `TEXT` (JSON-encoded arrays — DuckDB doesn't support PG arrays)
- `DECIMAL(20,7)` → `DOUBLE` (acceptable precision loss for DuckDB)
- `SERIAL` → not used (no auto-increment PKs in DuckLake)
- `TIMESTAMPTZ` → `TIMESTAMP`
- `JSONB` → `TEXT`
- No `ON CONFLICT` — use DELETE+INSERT for upserts

### Files
```
obsrvr-lake-unified-silver-processor/
├── go/
│   ├── main.go              -- Entry point, gRPC client
│   ├── config.go            -- YAML config
│   ├── processor.go         -- Orchestration
│   ├── ducklake_writer.go   -- DuckLake connection + transform orchestration
│   ├── schemas.go           -- Silver DDL (NEEDS REALIGNMENT)
│   └── transforms.go        -- Per-ledger transforms from proto data
├── cmd/
│   └── silver-history-loader/
│       └── main.go          -- Batch SQL transforms (NEEDS REALIGNMENT)
├── Dockerfile
└── Makefile
```

### Deployment
- Bronze processor: Nomad job `obsrvr-lake-unified-processor` (port 8098 health, 50053 gRPC)
- Silver processor: Nomad job `obsrvr-lake-unified-silver-processor` (port 8099 health)
- Query engine: reads both `testnet_catalog.bronze.*` and `testnet_catalog.silver.*`
