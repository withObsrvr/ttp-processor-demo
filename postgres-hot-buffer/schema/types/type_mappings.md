# Type Mappings: DuckDB → PostgreSQL

This document describes how data types from `ducklake-ingestion-obsrvr-v3/go/tables.go` (DuckDB schema) are mapped to PostgreSQL for the hot buffer.

## Mapping Table

| DuckDB Type | PostgreSQL Type | Rationale | Example |
|-------------|-----------------|-----------|---------|
| `BIGINT` | `BIGINT` | Direct 1:1 mapping | `ledger_sequence BIGINT` |
| `INTEGER` / `INT` | `INTEGER` | Direct 1:1 mapping | `protocol_version INTEGER` |
| `VARCHAR` | `TEXT` | TEXT has no length limit, simpler | `ledger_hash TEXT` |
| `TIMESTAMP` | `TIMESTAMPTZ` | Explicit UTC timezone handling | `closed_at TIMESTAMPTZ` |
| `BOOLEAN` | `BOOLEAN` | Direct 1:1 mapping | `successful BOOLEAN` |
| `UINTEGER` | `BIGINT` | PostgreSQL has no unsigned integers, BIGINT prevents overflow | `tx_memory_limit BIGINT` |
| `TEXT` | `TEXT` | Direct 1:1 mapping | `contract_data_xdr TEXT` |

## Key Differences

### 1. VARCHAR → TEXT

**DuckDB:**
```sql
CREATE TABLE ledgers_row_v2 (
    ledger_hash VARCHAR NOT NULL
);
```

**PostgreSQL:**
```sql
CREATE UNLOGGED TABLE ledgers_row_v2 (
    ledger_hash TEXT NOT NULL
);
```

**Why:** PostgreSQL `TEXT` has no length limit and performs identically to `VARCHAR(n)`. Using `TEXT` eliminates the need to specify length constraints and simplifies schema management.

### 2. TIMESTAMP → TIMESTAMPTZ

**DuckDB:**
```sql
CREATE TABLE ledgers_row_v2 (
    closed_at TIMESTAMP NOT NULL
);
```

**PostgreSQL:**
```sql
CREATE UNLOGGED TABLE ledgers_row_v2 (
    closed_at TIMESTAMPTZ NOT NULL
);
```

**Why:** Stellar blockchain timestamps are always UTC. Using `TIMESTAMPTZ` makes the timezone explicit and prevents ambiguity. PostgreSQL stores `TIMESTAMPTZ` internally in UTC and converts to session timezone on retrieval.

### 3. UINTEGER → BIGINT

**DuckDB:**
```sql
CREATE TABLE config_settings_snapshot_v1 (
    tx_memory_limit UINTEGER
);
```

**PostgreSQL:**
```sql
CREATE UNLOGGED TABLE config_settings_snapshot_v1 (
    tx_memory_limit BIGINT  -- UINTEGER → BIGINT
);
```

**Why:** PostgreSQL doesn't have unsigned integer types. `BIGINT` (range: -9,223,372,036,854,775,808 to 9,223,372,036,854,775,807) safely stores any unsigned 32-bit value without overflow. While it wastes some space for values that would fit in `INTEGER`, it prevents potential overflow issues.

## PostgreSQL-Specific Enhancements

### UNLOGGED Tables

All hot buffer tables use `UNLOGGED`:

```sql
CREATE UNLOGGED TABLE ledgers_row_v2 (
    ...
);
```

**Benefits:**
- 2-3x faster writes (no WAL logging)
- Perfect for ephemeral staging data

**Trade-offs:**
- Data lost on unclean shutdown (table truncated)
- Safe because hot buffer is not source of truth

### FILLFACTOR for Snapshot Tables

Snapshot tables that receive frequent UPDATEs use `FILLFACTOR=90`:

```sql
CREATE UNLOGGED TABLE accounts_snapshot_v1 (
    ...
) WITH (FILLFACTOR=90);
```

**Tables with FILLFACTOR=90 (10 tables):**
- `accounts_snapshot_v1`
- `trustlines_snapshot_v1`
- `native_balances_snapshot_v1`
- `offers_snapshot_v1`
- `claimable_balances_snapshot_v1`
- `liquidity_pools_snapshot_v1`
- `contract_data_snapshot_v1`
- `ttl_snapshot_v1`

**Benefits:**
- Leaves 10% free space per page
- Enables HOT (Heap-Only Tuple) updates
- Reduces table bloat from frequent balance changes

**Not needed for row tables:**
- Ledgers, transactions, operations, effects, trades are INSERT-only
- No UPDATEs = no benefit from FILLFACTOR

## Constraints

### Primary Keys

All tables have appropriate PRIMARY KEYs defined:

```sql
-- Single-column PK
CREATE UNLOGGED TABLE ledgers_row_v2 (
    sequence BIGINT NOT NULL PRIMARY KEY,
    ...
);

-- Composite PK
CREATE UNLOGGED TABLE operations_row_v2 (
    transaction_hash TEXT NOT NULL,
    operation_index INTEGER NOT NULL,
    ...
    PRIMARY KEY (transaction_hash, operation_index)
);
```

### Foreign Keys

**NOT USED** in hot buffer:
- Foreign keys slow down batch INSERTs significantly
- Hot buffer is ephemeral staging layer
- Data integrity enforced by ingestion logic
- Cold storage (DuckLake) can add constraints if needed

## Indexes

Only 38 critical indexes for flush performance (2 per table):

```sql
-- High-watermark flush queries
CREATE INDEX idx_ledgers_range ON ledgers_row_v2 (ledger_range);

-- Ordered iteration
CREATE INDEX idx_ledgers_sequence ON ledgers_row_v2 (sequence);
```

**No analytics indexes:**
- Hot buffer is staging only (not queried analytically)
- Each index slows INSERT performance
- Cold storage (DuckLake) handles analytics queries

## Example Transformation

**DuckDB (tables.go):**
```sql
CREATE TABLE IF NOT EXISTS testnet_catalog.testnet.ledgers_row_v2 (
    sequence BIGINT NOT NULL,
    ledger_hash VARCHAR NOT NULL,
    closed_at TIMESTAMP NOT NULL,
    protocol_version INT NOT NULL,
    total_coins BIGINT NOT NULL,
    fee_pool BIGINT NOT NULL,
    base_fee INT NOT NULL,
    base_reserve INT NOT NULL,
    max_tx_set_size INT NOT NULL,
    successful_tx_count INT NOT NULL,
    failed_tx_count INT NOT NULL,
    ingestion_timestamp TIMESTAMP,
    ledger_range BIGINT,
    ...
);
```

**PostgreSQL (hot buffer):**
```sql
CREATE UNLOGGED TABLE ledgers_row_v2 (
    sequence BIGINT NOT NULL PRIMARY KEY,
    ledger_hash TEXT NOT NULL,
    closed_at TIMESTAMPTZ NOT NULL,
    protocol_version INTEGER NOT NULL,
    total_coins BIGINT NOT NULL,
    fee_pool BIGINT NOT NULL,
    base_fee INTEGER NOT NULL,
    base_reserve INTEGER NOT NULL,
    max_tx_set_size INTEGER NOT NULL,
    successful_tx_count INTEGER NOT NULL,
    failed_tx_count INTEGER NOT NULL,
    ingestion_timestamp TIMESTAMPTZ,
    ledger_range BIGINT NOT NULL,
    ...
);
```

**Changes:**
1. Added `UNLOGGED`
2. `VARCHAR` → `TEXT`
3. `TIMESTAMP` → `TIMESTAMPTZ`
4. `INT` → `INTEGER` (explicit)
5. Added `PRIMARY KEY`
6. Made `ledger_range` NOT NULL (critical for flush)

## Verification Query

After running migrations, verify type mappings:

```sql
SELECT
    table_name,
    column_name,
    data_type,
    character_maximum_length,
    is_nullable
FROM information_schema.columns
WHERE table_schema = 'public'
  AND table_name = 'ledgers_row_v2'
ORDER BY ordinal_position;
```

## Reference

**Source of truth:** `/home/tillman/Documents/ttp-processor-demo/ducklake-ingestion-obsrvr-v3/go/tables.go`

All type mappings maintain semantic equivalence while optimizing for PostgreSQL performance characteristics.
