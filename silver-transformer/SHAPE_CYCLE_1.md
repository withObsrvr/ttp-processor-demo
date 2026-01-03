# Shape Up: Silver Layer Foundation (Current & Snapshot Tables)

**Cycle**: 1 week
**Team**: Solo
**Status**: Ready to start

---

## Problem

The silver-transformer service currently only has a stubbed `enriched_history_operations` table. We need to implement the foundational silver layer tables that transform bronze snapshots into analytics-ready formats. Without these tables, analytics queries must scan full bronze snapshots repeatedly, and there's no way to track historical state changes over time.

**Current pain points:**
- No current state views (latest values only)
- No historical change tracking (SCD Type 2)
- Analytics queries hit bronze directly (slower, more complex)
- Can't answer "what was the account balance on date X?" queries

---

## Appetite

**1 week** (5 working days)

This is a medium-sized feature that establishes the foundation for all other silver layer work. It's well-scoped because:
- Bronze tables already exist and are stable
- Transformation logic is straightforward (filtering + windowing)
- No complex joins or business logic required
- Clear success criteria (tables populate and update)

---

## Solution

### Fat Marker Sketch

```
Bronze Catalog (DuckDB)                    Silver Catalog (DuckDB)
├─ accounts_snapshot_v1        ────────>   ├─ accounts_current
├─ trustlines_snapshot_v1      ────────>   │  (latest only)
├─ offers_snapshot_v1          ────────>   │
├─ ...8 more snapshot tables   ────────>   │
                                            │
                                            └─ accounts_snapshot
                                               (SCD Type 2: valid_from/valid_to)
                                               trustlines_snapshot
                                               ...5 snapshot tables
```

### Implementation Approach

**Part 1: Current State Tables (Days 1-2)**

Create 10 `*_current` tables that show the latest state:
- accounts_current
- account_signers_current
- trustlines_current
- offers_current
- claimable_balances_current
- liquidity_pools_current
- contract_data_current
- contract_code_current
- config_settings_current
- ttl_current

**SQL Pattern** (simple filter):
```sql
CREATE TABLE accounts_current AS
SELECT a.*
FROM bronze.accounts_snapshot_v1 a
WHERE a.closed_at = (SELECT MAX(closed_at) FROM bronze.ledgers_row_v2)
```

**Part 2: Historical Snapshot Tables (Days 3-4)**

Create 5 `*_snapshot` tables with SCD Type 2 tracking:
- accounts_snapshot
- trustlines_snapshot
- liquidity_pools_snapshot
- contract_data_snapshot
- evicted_keys_snapshot

**SQL Pattern** (windowing function):
```sql
CREATE TABLE accounts_snapshot AS
SELECT
    *,
    closed_at AS valid_from,
    LEAD(closed_at) OVER (PARTITION BY account_id ORDER BY closed_at) AS valid_to
FROM bronze.accounts_snapshot_v1
```

**Part 3: Integration & Testing (Day 5)**

- Wire up polling loop to refresh current tables on new ledgers
- Verify SCD Type 2 tracking works correctly
- Add metrics tracking (rows per table)
- Test checkpoint resume
- Update health endpoint with table stats

### Files to Modify

1. **silver-transformer/schema.go** - Add 15 new table creation functions
2. **silver-transformer/transformer.go** - Add refresh logic for current tables
3. **silver-transformer/main.go** - Add metrics for new tables
4. **silver-transformer/checkpoint.go** - Track per-table checkpoints if needed

---

## Rabbit Holes

**DON'T:**

1. **Don't add incremental updates yet**
   Start with full refresh each poll. Optimization comes later if needed.

2. **Don't worry about view materialization strategies**
   Simple CREATE TABLE AS SELECT is fine. No need for complex materialized view logic.

3. **Don't implement custom partitioning**
   Let DuckDB handle storage layout. Partitioning can be added later if performance requires it.

4. **Don't build a generic table factory**
   15 similar functions is fine. Premature abstraction wastes time.

5. **Don't add indexes or performance tuning**
   Get it working first. Optimize only if measurements show problems.

6. **Don't try to handle schema evolution yet**
   Assume bronze schema is stable. Evolution logic is out of scope.

---

## No-Gos

Explicitly **OUT OF SCOPE** for this cycle:

❌ Enriched history operations (different cycle)
❌ Token transfers analytics table (future cycle)
❌ Query API for silver layer (not needed yet)
❌ Multiple network support in silver (bronze handles this)
❌ Data quality validation (rely on bronze)
❌ Performance optimization (measurement not done yet)
❌ Backfilling historical data (process forward only)

---

## Done

**Demo:**

```bash
# Start bronze ingestion
cd ducklake-ingestion-obsrvr-v3
./bin/ducklake-ingestion-obsrvr-v3 -config config/query-api-test.yaml \
  --multi-network --query-port :8081

# Start silver transformer
cd ../silver-transformer
./silver-transformer

# Verify all 15 tables exist and are populated
curl http://localhost:8082/health | jq '.stats'

# Expected output:
{
  "accounts_current_rows": 1234,
  "trustlines_current_rows": 5678,
  ...
  "accounts_snapshot_rows": 12340,
  ...
}

# Query examples work:
# 1. Current account balances
echo "SELECT COUNT(*) FROM accounts_current" | duckdb catalogs/silver.duckdb

# 2. Historical balance at specific date
echo "SELECT * FROM accounts_snapshot
      WHERE account_id = 'GABC...'
      AND '2024-01-15' BETWEEN valid_from AND COALESCE(valid_to, '9999-12-31')" \
  | duckdb catalogs/silver.duckdb
```

**Success Criteria:**

✅ All 10 `*_current` tables created and populated
✅ All 5 `*_snapshot` tables with valid_from/valid_to timestamps
✅ Tables auto-update when bronze ingests new ledgers
✅ Health endpoint shows row counts for all 15 tables
✅ Checkpoint resume works (stop/start silver-transformer continues)
✅ Query examples return correct data
✅ Service runs stable for 1 hour of continuous ingestion

---

## Scope Line

```
COULD HAVE ─────────────────────────────────────────
│ • Per-table metrics histograms
│ • Automatic table compaction
│ • Custom logging per table type
│ • Configuration for refresh intervals
│
NICE TO HAVE ───────────────────────────────────────
│ • Incremental refresh for current tables
│ • Table size monitoring alerts
│ • Query examples in documentation
│ • Batch size auto-tuning
│
MUST HAVE ══════════════════════════════════════════
│ • All 10 *_current tables functional
│ • All 5 *_snapshot tables with SCD Type 2
│ • Auto-refresh on new bronze data
│ • Health endpoint with table stats
│ • Service stability (no crashes)
│ • Checkpoint resume
└─────────────────────────────────────────────────────
```

**Cutting Scope Strategy:**

If behind schedule at 50% mark (2.5 days):
1. Cut COULD HAVE features immediately
2. Reduce `*_current` tables to 6 most critical (accounts, trustlines, offers, claimable_balances, liquidity_pools, contract_data)
3. Reduce `*_snapshot` tables to 2 most critical (accounts, trustlines)
4. Ship with 8 tables instead of 15 - better than 0 tables

---

## Hill Chart

Track progress through the cycle:

```
Figuring it out          |          Making it happen
    (0-50%)              |              (50-100%)
                         |
   [Day 1-2.5]           |           [Day 2.5-5]
```

**Left Side (Figuring Out):**
- Understand bronze table schemas
- Design SQL patterns for current/snapshot
- Test window function approach
- Verify checkpoint strategy

**Right Side (Making Happen):**
- Implement all 15 table creation functions
- Wire up refresh logic
- Add metrics and health stats
- Test end-to-end with real data
- Document usage

**Red Flag:** If still on left side by end of Day 3 → Cut scope to 8 tables

---

## Cool-Down After This Cycle

**Mandatory 1-2 days** before starting Cycle 2 (Enriched Operations).

Cool-down activities:
- Review and refactor schema.go if repetitive
- Add integration test for snapshot tables
- Document field mappings in README
- Fix any bugs discovered during testing
- Catch up on any bronze layer issues
- Rest and mentally reset

---

## Dependencies

**Required:**
- ✅ Bronze ingestion running with Query API on :8081
- ✅ Bronze has all snapshot tables populated
- ✅ Silver-transformer service framework operational

**Not Required:**
- ❌ Query API for operations/transactions (different cycle)
- ❌ Silver schema for enriched operations (different cycle)

---

## Risk Assessment

**Low Risk** ✅

This cycle has minimal technical risk because:
- Bronze tables exist and are stable
- SQL patterns are well-understood
- No complex business logic
- Clear success criteria
- Can incrementally test each table

**Potential Issues:**
1. **DuckDB memory usage** - Mitigate: Monitor during testing, adjust batch sizes
2. **Polling performance** - Mitigate: Start with 1-second polls, increase if needed
3. **Schema mismatches** - Mitigate: Verify bronze schemas early (Day 1)

---

## References

- Implementation plan: [SILVER_LAYER_MAPPING.md](../SILVER_LAYER_MAPPING.md)
- Bronze mappings: [BRONZE_LAYER_MAPPING.md](../BRONZE_LAYER_MAPPING.md)
- Current code: schema.go:9-73, transformer.go:119-168
- Hubble docs: https://developers.stellar.org/docs/data/analytics/hubble/data-catalog/data-dictionary/silver
