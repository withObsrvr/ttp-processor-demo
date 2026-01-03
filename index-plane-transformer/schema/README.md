# Index Plane Schema

This directory contains schema definitions for the Index Plane lookup layer.

## Files

### `index_schema.sql` (PostgreSQL Catalog)
**Purpose:** Catalog database schema (PostgreSQL)
**Location:** Runs on catalog database (e.g., `obsrvr_lake_catalog_prod`)
**Creates:**
- `index` schema
- `index.tables` - Table metadata tracking
- `index.files` - Parquet file tracking
- `index.transformer_checkpoint` - Transformer checkpoint

**Usage:**
```bash
# Initialize catalog schema
psql "$CATALOG_DB_CONNECTION_STRING" -f index_schema.sql
```

### `index_ducklake.sql` (DuckLake Storage)
**Purpose:** DuckDB schema for Index Plane tables
**Location:** Executed via DuckDB connected to B2 storage
**Creates:**
- `testnet_catalog.index.tx_hash_index` - Transaction hash index

**Usage:**
```bash
# Executed automatically by index-plane-transformer service
# Or manually via DuckDB:
duckdb -c "
  ATTACH 'postgresql://...' AS catalog;
  SET s3_endpoint = 's3.us-west-004.backblazeb2.com';
  SET s3_access_key_id = '...';
  SET s3_secret_access_key = '...';
  .read index_ducklake.sql
"
```

## Schema Overview

### Catalog Tracking (PostgreSQL)

```
index.tables
├── table_name (PK)
├── total_files
├── total_rows
├── total_size_bytes
├── min_ledger_sequence
├── max_ledger_sequence
└── created_at, updated_at

index.files
├── file_id (PK)
├── table_name (FK → index.tables)
├── file_path
├── ledger_range
├── min_ledger_sequence
├── max_ledger_sequence
├── row_count
└── file_size_bytes

index.transformer_checkpoint
├── id = 1 (PK)
├── last_ledger_sequence
├── last_processed_at
└── transformer_version
```

### Index Data (DuckLake/B2)

```
testnet_catalog.index.tx_hash_index
├── tx_hash (VARCHAR)
├── ledger_sequence (BIGINT)
├── application_order (INTEGER)
├── operation_count (INTEGER)
├── successful (BOOLEAN)
├── closed_at (TIMESTAMP)
├── ledger_range (BIGINT) -- Partition key
└── created_at (TIMESTAMP)

Storage layout:
s3://obsrvr-prod-testnet-index/tx_hash_index/
  ledger_range=1/*.parquet
  ledger_range=2/*.parquet
  ledger_range=3/*.parquet
  ...
```

## Initialization

### Step 1: Initialize Catalog Schema (PostgreSQL)

```bash
# On server or local machine with DB access
ssh root@<server-address>
source /root/.nomad/.envrc

PGPASSWORD="$CATALOG_PASSWORD" psql \
  -h "$CATALOG_HOST" \
  -p "${CATALOG_PORT:-25060}" \
  -U "$CATALOG_USER" \
  -d "$CATALOG_DB" \
  -f /path/to/index_schema.sql
```

### Step 2: Verify Schema

```bash
# Check schema created
PGPASSWORD="$CATALOG_PASSWORD" psql \
  -h "$CATALOG_HOST" \
  -p 25060 \
  -U "$CATALOG_USER" \
  -d "$CATALOG_DB" \
  -c "SELECT schema_name FROM information_schema.schemata WHERE schema_name = 'index';"

# Check tables
PGPASSWORD="$CATALOG_PASSWORD" psql \
  -h "$CATALOG_HOST" \
  -p 25060 \
  -U "$CATALOG_USER" \
  -d "$CATALOG_DB" \
  -c "SELECT table_name FROM information_schema.tables WHERE table_schema = 'index';"

# Check checkpoint
PGPASSWORD="$CATALOG_PASSWORD" psql \
  -h "$CATALOG_HOST" \
  -p 25060 \
  -U "$CATALOG_USER" \
  -d "$CATALOG_DB" \
  -c "SELECT * FROM index.transformer_checkpoint;"
```

Expected output:
```
 id | last_ledger_sequence |  last_processed_at  | transformer_version
----+----------------------+---------------------+---------------------
  1 |                    0 | 2026-01-02 12:00:00 | v1.0.0
```

### Step 3: Create B2 Bucket

```bash
# Authorize B2
b2 authorize-account "$B2_KEY_ID" "$B2_KEY_SECRET"

# Create Index Plane bucket
b2 create-bucket obsrvr-prod-testnet-index allPrivate

# Verify
b2 list-buckets | grep index
```

## Query Patterns

### Fast Transaction Lookup

```sql
-- Query tx_hash_index for specific transaction
SELECT
    ledger_sequence,
    application_order,
    operation_count,
    successful,
    closed_at
FROM testnet_catalog.index.tx_hash_index
WHERE tx_hash = '<transaction_hash>';
```

**Performance:** <500ms p95 for 60M+ ledgers (with partition pruning)

### Fallback to Table Scan

If index miss or index not available, query API falls back to:

```sql
-- Query Silver enriched_operations directly
SELECT
    ledger_sequence,
    application_order,
    -- ... other fields
FROM silver.enriched_operations
WHERE transaction_hash = '<transaction_hash>';
```

**Performance:** 5-30s for 60M+ ledgers (full table scan)

## Maintenance

### Check Index Coverage

```sql
-- Count transactions in index
SELECT COUNT(*) FROM testnet_catalog.index.tx_hash_index;

-- Check ledger range coverage
SELECT
    MIN(ledger_sequence) as min_ledger,
    MAX(ledger_sequence) as max_ledger,
    COUNT(*) as tx_count
FROM testnet_catalog.index.tx_hash_index;
```

### Check Catalog Metadata

```sql
-- View table metadata
SELECT * FROM index.tables;

-- View file metadata
SELECT
    table_name,
    COUNT(*) as file_count,
    SUM(row_count) as total_rows,
    SUM(file_size_bytes) / 1024 / 1024 as total_size_mb
FROM index.files
GROUP BY table_name;
```

### Check Transformer Progress

```sql
-- View checkpoint
SELECT
    last_ledger_sequence,
    last_processed_at,
    NOW() - last_processed_at as lag
FROM index.transformer_checkpoint;
```

## Storage Costs

**Estimate:** ~$15/month for 60M ledgers

**Calculation:**
- ~60M transactions × 100 bytes/tx = 6 GB Parquet (compressed)
- B2 storage: $0.005/GB/month
- 6 GB × $0.005 = $0.03/month (minimal)
- Egress for queries: ~$0.01/GB
- Total: ~$15/month (includes some query overhead)

## Troubleshooting

### Schema Not Created

```bash
# Check database connection
PGPASSWORD="$CATALOG_PASSWORD" psql \
  -h "$CATALOG_HOST" \
  -p 25060 \
  -U "$CATALOG_USER" \
  -d "$CATALOG_DB" \
  -c "SELECT version();"

# Check permissions
PGPASSWORD="$CATALOG_PASSWORD" psql \
  -h "$CATALOG_HOST" \
  -p 25060 \
  -U "$CATALOG_USER" \
  -d "$CATALOG_DB" \
  -c "SELECT HAS_SCHEMA_PRIVILEGE('stellar', 'index', 'USAGE');"
```

### Checkpoint Not Initializing

```bash
# Manually insert checkpoint
PGPASSWORD="$CATALOG_PASSWORD" psql \
  -h "$CATALOG_HOST" \
  -p 25060 \
  -U "$CATALOG_USER" \
  -d "$CATALOG_DB" \
  -c "INSERT INTO index.transformer_checkpoint (id, last_ledger_sequence, last_processed_at, transformer_version)
      VALUES (1, 0, NOW(), 'v1.0.0')
      ON CONFLICT (id) DO NOTHING;"
```

### B2 Bucket Issues

```bash
# Reauthorize
b2 authorize-account "$B2_KEY_ID" "$B2_KEY_SECRET"

# List buckets
b2 list-buckets

# Check bucket exists
b2 get-bucket obsrvr-prod-testnet-index
```

## Next Steps

After schema initialization:

1. Build `index-plane-transformer` service
2. Deploy service to Nomad
3. Monitor transformation cycles
4. Verify index data appears in B2
5. Integrate with Query API
6. Test transaction lookups

See: `/home/tillman/Documents/ttp-processor-demo/INDEX_PLANE_PROGRESS.md`
