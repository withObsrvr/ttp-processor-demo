# Obsrvr Lake Deployment Documentation

Complete documentation for deploying, operating, and managing the Obsrvr Lake platform.

**Last Updated:** 2026-01-04 (v2.0 - Complete reset and deployment validation)

---

## Documentation Overview

| Document | Purpose | When to Use |
|----------|---------|-------------|
| **DEPLOYMENT_GUIDE.md** | Step-by-step deployment from scratch | First-time setup, new environments |
| **OPERATIONS_GUIDE.md** | Day-to-day operations, troubleshooting, resets | Daily management, issues, maintenance |
| **ENVIRONMENT_STRATEGY.md** | Prod vs non-prod strategy | Planning environments, cost decisions |
| **SILVER_API_DOCUMENTATION.md** | Query API reference | Building integrations, understanding endpoints |
| **QUICK_REFERENCE.md** | Common API queries | Quick lookups, testing |
| **V3_CALL_GRAPH_DEPLOYMENT.md** | Call graph feature deployment | Freighter integration, contract calls |

---

## Quick Start

### First Time Deployment

1. **Plan your environment** → Read `ENVIRONMENT_STRATEGY.md`
2. **Deploy infrastructure** → Follow `DEPLOYMENT_GUIDE.md`
3. **Verify everything works** → Use `QUICK_REFERENCE.md` to test
4. **Learn operations** → Read `OPERATIONS_GUIDE.md`

### Already Deployed?

- **Check health**: See "Daily Operations" in `OPERATIONS_GUIDE.md`
- **Fix issues**: See "Troubleshooting" in `OPERATIONS_GUIDE.md`
- **Reset data**: See "Complete Reset Procedure" below
- **Query API**: Use `QUICK_REFERENCE.md`

---

## Platform Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    Stellar Blockchain                        │
│                  (Testnet or Mainnet)                        │
└────────────────────────┬────────────────────────────────────┘
                         │
                         ▼
         ┌───────────────────────────────┐
         │  stellar-live-source-datalake │  Port 50052
         │  (gRPC ledger streaming)       │
         └───────────────┬───────────────┘
                         │
                         ▼
         ┌───────────────────────────────┐
         │  stellar-postgres-ingester    │  Health: 8089
         │  (Bronze layer ingestion)      │
         └───────────────┬───────────────┘
                         │
                         ▼
         ┌───────────────────────────────┐
         │   PostgreSQL stellar_hot       │  Hot Buffer
         │   (19 Hubble Bronze tables)    │  (last ~3 hours)
         │   + contract_calls_json        │
         │   + contracts_involved         │
         └───────────────┬───────────────┘
                         │
            ┌────────────┼────────────┐
            │            │            │
            ▼            ▼            ▼
┌──────────────────┐ ┌──────────────────┐ ┌──────────────────┐
│ postgres-ducklake│ │silver-realtime-  │ │index-plane-      │
│ -flusher         │ │transformer       │ │transformer       │
│ (Bronze→Cold)    │ │ Health: 8094     │ │ Health: 8096     │
│ Health: 8090     │ └────────┬─────────┘ └────────┬─────────┘
└────────┬─────────┘          │                    │
         │                    ▼                    ▼
         │          ┌──────────────────┐ ┌──────────────────┐
         │          │PostgreSQL        │ │DuckLake Index    │
         │          │silver_hot        │ │(tx hash lookups) │
         │          │(Silver analytics)│ └──────────────────┘
         │          └────────┬─────────┘
         │                   │
         ▼                   ▼
┌──────────────────┐ ┌──────────────────┐
│ DuckLake Bronze  │ │ silver-cold-     │
│ (Parquet on B2)  │ │ flusher          │
│ bronze_meta      │ │ Health: 8095     │
└──────────────────┘ └────────┬─────────┘
                              │
                              ▼
                    ┌──────────────────┐
                    │ DuckLake Silver  │
                    │ (Parquet on B2)  │
                    │ silver_meta      │
                    └──────────────────┘
                              │
                              ▼
         ┌───────────────────────────────┐
         │    stellar-query-api          │  Port 8092
         │    (unified hot+cold queries)  │
         │    ┌─────────┐  ┌──────────┐  │
         │    │   Hot   │  │   Cold   │  │
         │    │  (PG)   │  │ (Parquet)│  │
         │    └────┬────┘  └─────┬────┘  │
         │         └──────┬──────┘        │
         │                │               │
         │         Merged Results         │
         └────────────────┬──────────────┘
                          │
                          ▼
              ┌───────────────────────┐
              │   Consumer Apps       │
              │   - Freighter Wallet  │
              │   - Web dashboards    │
              │   - Block explorers   │
              └───────────────────────┘
```

---

## All Services (8 Total)

| Service | Purpose | Health Port | Memory |
|---------|---------|-------------|--------|
| `stellar-live-source-datalake` | gRPC ledger data source | 8088 | 2048 MB |
| `stellar-postgres-ingester` | Bronze layer ingestion | 8089 | 2048 MB |
| `silver-realtime-transformer` | Hot Silver analytics | 8094 | 2048 MB |
| `postgres-ducklake-flusher` | Bronze hot→cold | 8090 | 2560 MB |
| `silver-cold-flusher` | Silver hot→cold | 8095 | 3072 MB |
| `index-plane-transformer` | Transaction hash index | 8096 | 1024 MB |
| `contract-event-index-transformer` | Contract event index | 8097 | 1024 MB |
| `stellar-query-api` | REST API layer | 8092 | 1500 MB |

**Total Memory Requirement:** ~15 GB (fits on 16GB droplet with headroom)

---

## Database Architecture

### PostgreSQL Databases (3 Total)

| Database | Purpose | Host Pattern |
|----------|---------|--------------|
| `stellar_hot` | Bronze hot buffer | `private-obsrvr-lake-hot-buffer-prod-*` |
| `silver_hot` | Silver hot buffer | `private-obsrvr-lake-silver-hot-prod-*` |
| `obsrvr_lake_catalog_prod` | DuckLake catalog | `private-obsrvr-lake-catalog-prod-*` |

### DuckLake Catalog Schemas (3 Total)

| Schema | Purpose | Data Path |
|--------|---------|-----------|
| `bronze_meta` | Bronze layer metadata | `s3://bucket/bronze/` |
| `silver_meta` | Silver layer metadata | `s3://bucket/silver/` |
| `index` | Transaction/contract indexes | `s3://bucket/index/` |

### Storage (Backblaze B2)

| Prefix | Content |
|--------|---------|
| `bronze/` | Raw Hubble tables (Parquet) |
| `silver/` | Analytics tables (Parquet) |
| `index/` | Lookup indexes (Parquet) |

---

## Complete Reset Procedure

**Tested: 2026-01-04** - This procedure was validated end-to-end.

### Step 1: Stop All Pipeline Services

```bash
# SSH to server
ssh -i ssh/obsrvr-lake-prod.pem root@<DROPLET_IP>

# Set Nomad environment
export NOMAD_ADDR=http://localhost:4646
export NOMAD_TOKEN="<your-token>"

# Stop all services (except data source and query API if desired)
nomad job stop stellar-postgres-ingester
nomad job stop silver-realtime-transformer
nomad job stop postgres-ducklake-flusher
nomad job stop silver-cold-flusher
nomad job stop index-plane-transformer
nomad job stop contract-event-index-transformer
```

### Step 2: Truncate Bronze Hot Buffer

```bash
PGPASSWORD='<password>' psql \
  -h private-obsrvr-lake-hot-buffer-prod-*.db.ondigitalocean.com \
  -p 25060 -U doadmin -d stellar_hot << 'SQL'
TRUNCATE TABLE ledgers_row_v2 CASCADE;
TRUNCATE TABLE transactions_row_v2 CASCADE;
TRUNCATE TABLE operations_row_v2 CASCADE;
TRUNCATE TABLE effects_row_v2 CASCADE;
TRUNCATE TABLE trades_row_v2 CASCADE;
TRUNCATE TABLE accounts_snapshot_v1 CASCADE;
TRUNCATE TABLE offers_snapshot_v1 CASCADE;
TRUNCATE TABLE trustlines_snapshot_v1 CASCADE;
TRUNCATE TABLE account_signers_snapshot_v1 CASCADE;
TRUNCATE TABLE claimable_balances_snapshot_v1 CASCADE;
TRUNCATE TABLE liquidity_pools_snapshot_v1 CASCADE;
TRUNCATE TABLE config_settings_snapshot_v1 CASCADE;
TRUNCATE TABLE ttl_snapshot_v1 CASCADE;
TRUNCATE TABLE contract_events_stream_v1 CASCADE;
TRUNCATE TABLE contract_data_snapshot_v1 CASCADE;
TRUNCATE TABLE contract_code_snapshot_v1 CASCADE;
TRUNCATE TABLE native_balances_snapshot_v1 CASCADE;
TRUNCATE TABLE evicted_keys_state_v1 CASCADE;
TRUNCATE TABLE restored_keys_state_v1 CASCADE;
-- Reset index checkpoint
UPDATE index.transformer_checkpoint SET last_ledger_sequence = 0;
SQL
```

### Step 3: Truncate Silver Hot Buffer

```bash
PGPASSWORD='<password>' psql \
  -h private-obsrvr-lake-silver-hot-prod-*.db.ondigitalocean.com \
  -p 25060 -U doadmin -d silver_hot << 'SQL'
TRUNCATE TABLE enriched_history_operations CASCADE;
TRUNCATE TABLE enriched_history_operations_soroban CASCADE;
TRUNCATE TABLE accounts_current CASCADE;
TRUNCATE TABLE accounts_snapshot CASCADE;
TRUNCATE TABLE trustlines_current CASCADE;
TRUNCATE TABLE trustlines_snapshot CASCADE;
TRUNCATE TABLE offers_current CASCADE;
TRUNCATE TABLE offers_snapshot CASCADE;
TRUNCATE TABLE claimable_balances_current CASCADE;
TRUNCATE TABLE claimable_balances_snapshot CASCADE;
TRUNCATE TABLE contract_data_current CASCADE;
TRUNCATE TABLE account_signers_snapshot CASCADE;
TRUNCATE TABLE token_transfers_raw CASCADE;
TRUNCATE TABLE contract_invocations_raw CASCADE;
TRUNCATE TABLE contract_invocation_calls CASCADE;
TRUNCATE TABLE contract_invocation_hierarchy CASCADE;
-- Gold layer tables (if present)
TRUNCATE TABLE gold_token_transfers CASCADE;
-- Reset transformer checkpoint
DELETE FROM realtime_transformer_checkpoint;
INSERT INTO realtime_transformer_checkpoint
  (id, last_ledger_sequence, last_processed_at, transformer_version)
VALUES (1, 0, NOW(), 'v3-call-graph');
SQL
```

### Step 4: Clear Backblaze B2 Buckets

Clear via B2 console or CLI:
```bash
# Using b2 CLI
b2 rm --recursive b2://obsrvr-lake-testnet/bronze/
b2 rm --recursive b2://obsrvr-lake-testnet/silver/
b2 rm --recursive b2://obsrvr-lake-testnet/index/
```

### Step 5: Reset DuckLake Catalog (CRITICAL)

**This step is essential** - without it, flushers will fail with "No snapshot found in DuckLake" error.

```bash
PGPASSWORD='<password>' psql \
  -h private-obsrvr-lake-catalog-prod-*.db.ondigitalocean.com \
  -p 25060 -U doadmin -d obsrvr_lake_catalog_prod << 'SQL'
-- Reset bronze_meta
TRUNCATE TABLE bronze_meta.ducklake_schema CASCADE;
TRUNCATE TABLE bronze_meta.ducklake_table CASCADE;
TRUNCATE TABLE bronze_meta.ducklake_column CASCADE;
TRUNCATE TABLE bronze_meta.ducklake_view CASCADE;
TRUNCATE TABLE bronze_meta.ducklake_data_file CASCADE;
TRUNCATE TABLE bronze_meta.ducklake_snapshot CASCADE;
TRUNCATE TABLE bronze_meta.ducklake_snapshot_changes CASCADE;
TRUNCATE TABLE bronze_meta.ducklake_file_column_stats CASCADE;
TRUNCATE TABLE bronze_meta.ducklake_file_partition_value CASCADE;
TRUNCATE TABLE bronze_meta.ducklake_partition_column CASCADE;
TRUNCATE TABLE bronze_meta.ducklake_partition_info CASCADE;
-- Insert initial snapshot (REQUIRED)
INSERT INTO bronze_meta.ducklake_snapshot
  (snapshot_id, snapshot_time, schema_version, next_catalog_id, next_file_id)
VALUES (0, NOW(), 1, 1, 1);

-- Reset silver_meta (same pattern)
TRUNCATE TABLE silver_meta.ducklake_schema CASCADE;
TRUNCATE TABLE silver_meta.ducklake_table CASCADE;
TRUNCATE TABLE silver_meta.ducklake_column CASCADE;
TRUNCATE TABLE silver_meta.ducklake_view CASCADE;
TRUNCATE TABLE silver_meta.ducklake_data_file CASCADE;
TRUNCATE TABLE silver_meta.ducklake_snapshot CASCADE;
TRUNCATE TABLE silver_meta.ducklake_snapshot_changes CASCADE;
TRUNCATE TABLE silver_meta.ducklake_file_column_stats CASCADE;
TRUNCATE TABLE silver_meta.ducklake_file_partition_value CASCADE;
TRUNCATE TABLE silver_meta.ducklake_partition_column CASCADE;
TRUNCATE TABLE silver_meta.ducklake_partition_info CASCADE;
INSERT INTO silver_meta.ducklake_snapshot
  (snapshot_id, snapshot_time, schema_version, next_catalog_id, next_file_id)
VALUES (0, NOW(), 1, 1, 1);

-- Reset index (same pattern)
TRUNCATE TABLE index.ducklake_schema CASCADE;
TRUNCATE TABLE index.ducklake_table CASCADE;
TRUNCATE TABLE index.ducklake_column CASCADE;
TRUNCATE TABLE index.ducklake_view CASCADE;
TRUNCATE TABLE index.ducklake_data_file CASCADE;
TRUNCATE TABLE index.ducklake_snapshot CASCADE;
TRUNCATE TABLE index.ducklake_snapshot_changes CASCADE;
TRUNCATE TABLE index.ducklake_file_column_stats CASCADE;
TRUNCATE TABLE index.ducklake_file_partition_value CASCADE;
TRUNCATE TABLE index.ducklake_partition_column CASCADE;
TRUNCATE TABLE index.ducklake_partition_info CASCADE;
TRUNCATE TABLE index.files CASCADE;
TRUNCATE TABLE index.transformer_checkpoint CASCADE;
INSERT INTO index.ducklake_snapshot
  (snapshot_id, snapshot_time, schema_version, next_catalog_id, next_file_id)
VALUES (0, NOW(), 1, 1, 1);
SQL
```

### Step 6: Reset File-Based Checkpoints

```bash
# On the Nomad server
rm -f /opt/nomad/data/host_volumes/ingester_checkpoint/checkpoint.json
rm -f /opt/nomad/data/host_volumes/contract_index_checkpoint/checkpoint.json
```

### Step 7: Restart Services (Order Matters)

```bash
# Core pipeline first
nomad job run /root/stellar-postgres-ingester.nomad
sleep 10

# Wait for Bronze data before starting Silver
nomad job run /root/silver-realtime-transformer.nomad

# Cold storage flushers
nomad job run /root/postgres-ducklake-flusher.nomad
nomad job run /root/silver-cold-flusher.nomad

# Indexers
nomad job run /root/index-plane-transformer.nomad
nomad job run /root/contract-event-index-transformer.nomad
```

### Step 8: Verify Pipeline Health

```bash
# Check all jobs running
nomad job status

# Check Bronze progress
PGPASSWORD='<password>' psql -h <bronze-host> -p 25060 -U doadmin -d stellar_hot \
  -c "SELECT MIN(sequence), MAX(sequence), COUNT(*) FROM ledgers_row_v2;"

# Check Silver progress
PGPASSWORD='<password>' psql -h <silver-host> -p 25060 -U doadmin -d silver_hot \
  -c "SELECT last_ledger_sequence FROM realtime_transformer_checkpoint;"

# Check API health
curl http://localhost:8092/health
# Expected: {"layers":{"bronze":true,"contract_index":true,"hot":true,"index":true,"silver":true},"status":"healthy"}
```

---

## Common Issues and Fixes

### Issue: "No snapshot found in DuckLake"

**Cause:** Catalog tables truncated without inserting initial snapshot.

**Fix:** Insert initial snapshot (see Step 5 above):
```sql
INSERT INTO <schema>.ducklake_snapshot
  (snapshot_id, snapshot_time, schema_version, next_catalog_id, next_file_id)
VALUES (0, NOW(), 1, 1, 1);
```

### Issue: Memory Exhaustion on Nomad Node

**Cause:** Total job memory exceeds node capacity (16GB).

**Fix:** Reduce memory allocations:
```hcl
# In nomad job files
resources {
  memory = 1024  # Reduce from 2048
}
```

Current working allocations:
- index-plane-transformer: 1024 MB
- contract-event-index-transformer: 1024 MB

### Issue: Silver Transformer Checkpoint Error

**Cause:** Checkpoint row missing after truncate.

**Fix:** Re-insert checkpoint row:
```sql
INSERT INTO realtime_transformer_checkpoint
  (id, last_ledger_sequence, last_processed_at, transformer_version)
VALUES (1, 0, NOW(), 'v3-call-graph');
```

### Issue: Port 8088 Already in Use

**Cause:** Both `stellar-live-source-datalake` and `stellar-postgres-ingester` tried to use 8088.

**Fix:** Ingester health port changed to 8089.

### Issue: Function Name Column Too Short

**Cause:** Some Soroban function names are bytes hashes (>100 chars when serialized).

**Fix:** Extend column to TEXT:
```sql
ALTER TABLE contract_invocation_calls ALTER COLUMN function_name TYPE TEXT;
```

### Issue: "duplicate key value violates unique constraint ducklake_schema_pkey"

**Cause:** Partial catalog reset - truncated some tables but not schema definitions.

**Fix:** Complete catalog reset (truncate ALL ducklake_* tables in the schema).

---

## Health Check Commands

```bash
# All services health
curl http://localhost:8089/health  # Ingester
curl http://localhost:8094/health  # Silver transformer
curl http://localhost:8090/health  # Bronze flusher
curl http://localhost:8095/health  # Silver flusher
curl http://localhost:8096/health  # Index plane
curl http://localhost:8097/health  # Contract index
curl http://localhost:8092/health  # Query API

# Nomad job status
nomad job status

# Docker container logs
docker logs $(docker ps --filter "name=ingester" -q) 2>&1 | tail -20
docker logs $(docker ps --filter "name=transformer" -q | head -1) 2>&1 | tail -20
```

---

## Cost Breakdown

### Current Testnet Deployment

| Component | Spec | Monthly Cost |
|-----------|------|--------------|
| Droplet | s-8vcpu-16gb-amd | $96 |
| Volume | 200GB SSD | $20 |
| PostgreSQL stellar_hot | db-s-2vcpu-4gb | $60 |
| PostgreSQL silver_hot | db-s-2vcpu-4gb | $60 |
| PostgreSQL catalog | db-s-1vcpu-1gb | $15 |
| Backblaze B2 Bronze | ~50GB | $2.50 |
| Backblaze B2 Silver | ~30GB | $1.50 |
| **Total** | | **~$255/month** |

### Projected Mainnet Deployment

| Component | Spec | Monthly Cost |
|-----------|------|--------------|
| Droplet | s-16vcpu-32gb-amd | $192 |
| Volume | 500GB SSD | $50 |
| PostgreSQL stellar_hot | db-s-4vcpu-8gb | $120 |
| PostgreSQL silver_hot | db-s-4vcpu-8gb | $120 |
| PostgreSQL catalog | db-s-2vcpu-4gb | $60 |
| Backblaze B2 Bronze | ~200GB | $10 |
| Backblaze B2 Silver | ~150GB | $7.50 |
| **Total** | | **~$560/month** |

---

## Nomad Job Files Location

On the Nomad server:
```
/root/stellar-postgres-ingester.nomad
/root/silver-realtime-transformer.nomad
/root/postgres-ducklake-flusher.nomad
/root/silver-cold-flusher.nomad
/root/index-plane-transformer.nomad
/root/contract-event-index-transformer.nomad
/root/stellar-query-api.nomad
```

In the repository:
```
/home/tillman/Documents/infra/environments/prod/do-obsrvr-lake/.nomad/
```

---

## V3 Call Graph Feature

The V3 Call Graph feature tracks cross-contract calls for Freighter wallet integration:

### New Bronze Columns (operations_row_v2)
- `contract_calls_json` - JSON array of call graph
- `contracts_involved` - Array of contract IDs
- `max_call_depth` - Deepest nesting level

### New Silver Tables
- `contract_invocation_calls` - Flattened call relationships
- `contract_invocation_hierarchy` - Pre-computed ancestry

### New API Endpoints
- `GET /api/v1/silver/tx/{hash}/contracts-involved`
- `GET /api/v1/silver/tx/{hash}/call-graph`
- `GET /api/v1/freighter/tx/{hash}/contracts`
- `GET /api/v1/silver/contracts/{id}/callers`
- `GET /api/v1/silver/contracts/{id}/callees`

See `V3_CALL_GRAPH_DEPLOYMENT.md` for full details.

---

## Document Versions

| Document | Last Updated | Version |
|----------|--------------|---------|
| README_DEPLOYMENT.md | 2026-01-04 | 2.0 |
| DEPLOYMENT_GUIDE.md | 2026-01-01 | 1.0 |
| OPERATIONS_GUIDE.md | 2026-01-01 | 1.0 |
| ENVIRONMENT_STRATEGY.md | 2026-01-01 | 1.0 |
| SILVER_API_DOCUMENTATION.md | 2026-01-01 | 1.0 |
| QUICK_REFERENCE.md | 2026-01-01 | 1.0 |
| V3_CALL_GRAPH_DEPLOYMENT.md | 2026-01-04 | 1.0 |

---

## Changelog

### v2.0 (2026-01-04)
- Complete reset procedure validated end-to-end
- Added 8-service architecture (was 5)
- Added DuckLake catalog reset steps (critical for clean slate)
- Added memory allocation documentation
- Added common issues section with fixes
- Updated architecture diagram with all services
- Added V3 Call Graph feature documentation
- Added port assignments for all services

### v1.0 (2026-01-01)
- Initial documentation

---

Happy deploying!
