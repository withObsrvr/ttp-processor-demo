# PostgreSQL Hot Buffer - Stellar Data Lake

PostgreSQL schema for the **hot buffer** layer of the Stellar hot/cold lambda architecture.

## Overview

This is the **hot layer** that stores recent Stellar blockchain data (last 10-20 minutes) before it's flushed to cold storage (DuckLake on S3).

### Architecture

```
Stellar Data Source
    ↓
[Cycle 2] stellar-postgres-ingester → PostgreSQL Hot Buffer (THIS)
    ↓ (flush every 10 mins)
[Cycle 3] postgres-ducklake-flusher → DuckLake Bronze (cold storage)
```

### Key Features

- **19 Hubble-compatible tables**: Complete Stellar data model (core + Soroban)
- **UNLOGGED tables**: 2-3x faster writes (safe for ephemeral staging)
- **38 critical indexes**: Optimized for flush performance only
- **Helper functions**: High-watermark flush orchestration
- **Migration-based**: Version-controlled schema evolution

## Quick Start

### Prerequisites

- Docker & Docker Compose
- golang-migrate (for migrations)

```bash
# Install golang-migrate
brew install golang-migrate  # macOS
# OR
go install -tags 'postgres' github.com/golang-migrate/migrate/v4/cmd/migrate@latest
```

### 1. Start PostgreSQL

```bash
docker-compose up -d
```

This starts:
- **PostgreSQL 16** on port `5434`
- **PgBouncer** (connection pooler) on port `6432`
- **Adminer** (web UI) on port `8081`

### 2. Run Migrations

```bash
migrate -database "postgres://stellar:stellar_dev_password@localhost:5434/stellar_hot?sslmode=disable" \
        -path schema/migrations up
```

### 3. Verify Setup

```bash
# Check tables created (should show 19 tables)
psql -h localhost -p 5434 -U stellar -d stellar_hot -c "\dt"

# Check indexes created (should show 38 indexes)
psql -h localhost -p 5434 -U stellar -d stellar_hot -c "\di" | wc -l

# Test flush helper functions
psql -h localhost -p 5434 -U stellar -d stellar_hot -c "SELECT * FROM get_high_watermark();"
```

### 4. Open Adminer Web UI

Visit http://localhost:8081

- **Server**: postgres-hot-buffer
- **Username**: stellar
- **Password**: stellar_dev_password
- **Database**: stellar_hot

## Schema Overview

### Tables (19 Total)

**Core Transaction Tables (3):**
- `ledgers_row_v2` - Ledger headers (24 fields)
- `transactions_row_v2` - Full transaction details (46 fields)
- `operations_row_v2` - All operation types (58 fields)

**Effects & Trades (2):**
- `effects_row_v1` - State changes from operations (25 fields)
- `trades_row_v1` - DEX trade executions (17 fields)

**Account State Snapshots (7):**
- `accounts_snapshot_v1` - Account state (23 fields)
- `trustlines_snapshot_v1` - Asset trustlines (14 fields)
- `native_balances_snapshot_v1` - XLM balances (11 fields)
- `account_signers_snapshot_v1` - Multi-sig configuration (9 fields)
- `offers_snapshot_v1` - DEX orderbook (15 fields)
- `claimable_balances_snapshot_v1` - Claimable balances (12 fields)
- `liquidity_pools_snapshot_v1` - AMM pools (17 fields)

**Soroban (Smart Contracts) (7):**
- `contract_events_stream_v1` - Contract events (16 fields)
- `contract_data_snapshot_v1` - Contract storage (17 fields)
- `contract_code_snapshot_v1` - WASM bytecode (18 fields)
- `config_settings_snapshot_v1` - Network config (19 fields)
- `ttl_snapshot_v1` - TTL entries (11 fields)
- `evicted_keys_state_v1` - Evicted keys (9 fields)
- `restored_keys_state_v1` - Restored keys (10 fields)

**Metadata (5):**
- `_meta_datasets` - Dataset registry
- `_meta_lineage` - Processing provenance
- `_meta_quality` - Quality checks
- `_meta_changes` - Schema evolution
- `_meta_eras` - Era management

### Indexes (38 Total)

Only flush-critical indexes (2 per table):

1. `ledger_range` - For high-watermark flush queries
2. `sequence`/`ledger_sequence` - For ordered iteration

**No analytics indexes** - hot buffer is staging only.

### Helper Functions (3 Total)

**1. get_high_watermark()** - Get current flush boundary

```sql
SELECT * FROM get_high_watermark();
```

Returns max `ledger_sequence` and `ledger_range` for all 19 tables.

**2. get_flush_candidates(minutes)** - Find ledger_ranges ready to flush

```sql
-- Find data older than 5 minutes
SELECT * FROM get_flush_candidates(5);
```

**3. cleanup_flushed_data(ledger_range)** - Delete flushed data

```sql
-- Cleanup after successful flush to cold storage
SELECT * FROM cleanup_flushed_data(1000000);
```

Returns count of deleted rows per table.

## High-Watermark Flush Pattern

Safe concurrent flush without blocking ingestion:

```sql
-- Step 1: MARK - Get high watermark
SELECT MIN(max_ledger_range) as watermark FROM get_high_watermark();
-- Result: 1000500

-- Step 2: FLUSH - Copy to DuckLake (via postgres-ducklake-flusher service)
-- INSERT INTO bronze.ledgers_row_v2
-- SELECT * FROM postgres_scan(..., 'ledgers_row_v2')
-- WHERE ledger_range <= 1000500;

-- Step 3: DELETE - Cleanup flushed data
SELECT * FROM cleanup_flushed_data(1000500);

-- Step 4: VACUUM (every 10th flush)
VACUUM ANALYZE;
```

**Why this is safe:**
- New data arriving during flush has `ledger_sequence > watermark`
- DELETE only touches `ledger_sequence <= watermark`
- No race condition between writer and flusher

## Type Mappings

DuckDB → PostgreSQL conversions:

| DuckDB | PostgreSQL | Reason |
|--------|------------|--------|
| `VARCHAR` | `TEXT` | No length limits |
| `TIMESTAMP` | `TIMESTAMPTZ` | Explicit UTC |
| `UINTEGER` | `BIGINT` | No unsigned in Postgres |

See [schema/types/type_mappings.md](schema/types/type_mappings.md) for full details.

## Performance Optimizations

### UNLOGGED Tables

All tables use `UNLOGGED` for 2-3x faster writes:

```sql
CREATE UNLOGGED TABLE ledgers_row_v2 (...);
```

**Trade-offs:**
- ✅ Much faster writes (no WAL logging)
- ✅ Perfect for ephemeral staging data
- ❌ Data lost on crash (table truncated)
- ✅ Safe because hot buffer is not source of truth

### FILLFACTOR=90

Snapshot tables use `FILLFACTOR=90` for frequent updates:

```sql
CREATE UNLOGGED TABLE accounts_snapshot_v1 (...)
WITH (FILLFACTOR=90);
```

Leaves 10% free space per page for HOT (Heap-Only Tuple) updates, reducing bloat.

### Development-Only Settings

**docker-compose.yml** includes aggressive performance settings:

```yaml
command: >
  postgres
  -c fsync=off
  -c synchronous_commit=off
  -c full_page_writes=off
```

⚠️ **WARNING**: These settings risk data loss on crash. Safe for hot buffer (staging layer) but **NEVER** use in production for source-of-truth data!

## Migration Management

### Apply All Migrations

```bash
migrate -database "postgres://stellar:stellar_dev_password@localhost:5434/stellar_hot?sslmode=disable" \
        -path schema/migrations up
```

### Rollback Last Migration

```bash
migrate -database "postgres://..." -path schema/migrations down 1
```

### Check Migration Version

```bash
migrate -database "postgres://..." -path schema/migrations version
```

### Create New Migration

```bash
migrate create -ext sql -dir schema/migrations -seq add_new_field
```

This creates:
- `NNN_add_new_field.up.sql`
- `NNN_add_new_field.down.sql`

## Connection Methods

### Direct PostgreSQL (for admin tasks)

```bash
psql -h localhost -p 5434 -U stellar -d stellar_hot
```

### Via PgBouncer (recommended for applications)

```bash
psql -h localhost -p 6432 -U stellar -d stellar_hot
```

### Connection String

```
postgres://stellar:stellar_dev_password@localhost:5434/stellar_hot?sslmode=disable
```

Via PgBouncer:
```
postgres://stellar:stellar_dev_password@localhost:6432/stellar_hot?sslmode=disable
```

## Monitoring

### Table Sizes

```sql
SELECT
    schemaname,
    tablename,
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) AS size
FROM pg_tables
WHERE schemaname = 'public'
ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC;
```

### Row Counts

```sql
SELECT * FROM get_high_watermark();
```

### Index Usage

```sql
SELECT
    schemaname,
    tablename,
    indexname,
    idx_scan,
    idx_tup_read,
    idx_tup_fetch
FROM pg_stat_user_indexes
ORDER BY idx_scan DESC;
```

### Bloat Check

```sql
SELECT
    tablename,
    pg_size_pretty(pg_total_relation_size(tablename::regclass)) as total_size,
    pg_size_pretty(pg_relation_size(tablename::regclass)) as table_size,
    pg_size_pretty(pg_total_relation_size(tablename::regclass) - pg_relation_size(tablename::regclass)) as indexes_size
FROM pg_tables
WHERE schemaname = 'public'
ORDER BY pg_total_relation_size(tablename::regclass) DESC;
```

## Troubleshooting

### "relation does not exist"

**Problem**: Tables not created.

**Solution**: Run migrations:
```bash
migrate -database "postgres://..." -path schema/migrations up
```

### "password authentication failed"

**Problem**: Wrong password or connection string.

**Solution**: Check docker-compose.yml for correct credentials:
- User: `stellar`
- Password: `stellar_dev_password`
- Database: `stellar_hot`

### "could not connect to server"

**Problem**: PostgreSQL not running.

**Solution**:
```bash
docker-compose ps  # Check if running
docker-compose up -d  # Start if not running
```

### Slow INSERT Performance

**Problem**: Too many indexes or LOGGED tables.

**Solution**:
1. Verify tables are `UNLOGGED`: `\d+ ledgers_row_v2`
2. Verify only 38 indexes exist: `\di | wc -l`
3. Check for foreign keys (there should be none): `\d ledgers_row_v2`

### Table Bloat

**Problem**: Deleted rows not reclaimed.

**Solution**:
```sql
VACUUM ANALYZE ledgers_row_v2;
-- OR for all tables
VACUUM ANALYZE;
```

Auto-VACUUM should handle this, but manual VACUUM after large deletes is faster.

## Production Deployment

### ⚠️ Critical Changes for Production

**1. Remove unsafe performance settings:**

```yaml
# REMOVE these from docker-compose.yml command section:
-c fsync=off                    # REMOVE
-c synchronous_commit=off       # REMOVE
-c full_page_writes=off         # REMOVE
```

**2. Use managed PostgreSQL:**
- AWS RDS
- Google Cloud SQL
- DigitalOcean Managed Databases
- Azure Database for PostgreSQL

**3. Enable proper backups:**
- Continuous archiving (WAL archiving)
- Point-in-time recovery (PITR)
- Regular base backups

**4. Monitoring:**
- Prometheus + Grafana
- Alert on flush failures
- Alert on bloat
- Alert on disk space

**5. Security:**
- Change default password
- Use SSL/TLS (`sslmode=require`)
- Network isolation (VPC)
- Least-privilege access

## File Structure

```
postgres-hot-buffer/
├── schema/
│   ├── migrations/
│   │   ├── 001_create_core_tables.up.sql        # Ledgers, txs, ops
│   │   ├── 001_create_core_tables.down.sql
│   │   ├── 002_create_effects_trades.up.sql     # Effects, trades
│   │   ├── 002_create_effects_trades.down.sql
│   │   ├── 003_create_snapshot_tables.up.sql    # 7 snapshot tables
│   │   ├── 003_create_snapshot_tables.down.sql
│   │   ├── 004_create_soroban_tables.up.sql     # 7 Soroban tables
│   │   ├── 004_create_soroban_tables.down.sql
│   │   ├── 005_create_metadata_tables.up.sql    # 5 metadata tables
│   │   ├── 005_create_metadata_tables.down.sql
│   │   ├── 006_create_critical_indexes.up.sql   # 38 indexes
│   │   ├── 006_create_critical_indexes.down.sql
│   │   ├── 007_create_flush_helpers.up.sql      # 3 functions
│   │   └── 007_create_flush_helpers.down.sql
│   └── types/
│       └── type_mappings.md                     # DuckDB → PostgreSQL
├── docker-compose.yml                           # PostgreSQL + PgBouncer + Adminer
└── README.md                                    # This file
```

## Next Steps

After completing this schema setup (Cycle 1):

**Cycle 2**: Build `stellar-postgres-ingester` service
- gRPC client to stellar-live-source-datalake
- Batch INSERT to this hot buffer
- Checkpoint mechanism

**Cycle 3**: Build `postgres-ducklake-flusher` service
- High-watermark flush pattern
- Flush to DuckLake Bronze every 10 minutes
- Use helper functions from this schema

**Cycle 4**: Build `stellar-query-api` service
- Query both hot (this) + cold (DuckLake) storage
- Transparent merging of results

## References

- Shape Up pitch: `../shape-up/cycle-1-hot-buffer-schema.md`
- Source schema: `/home/tillman/Documents/ttp-processor-demo/ducklake-ingestion-obsrvr-v3/go/tables.go`
- Migration tool: https://github.com/golang-migrate/migrate

## License

MIT

## Contributing

This is part of the Obsrvr hot/cold lambda architecture implementation. See Shape Up pitches in `../shape-up/` for full context.
