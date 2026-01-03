# Shape Up: Enriched History Operations

**Cycle**: 1 week
**Team**: Solo
**Status**: Depends on Cycle 1 completion + bronze Query API extension
**Prerequisite**: Cool-down period after Cycle 1

---

## Problem

The current silver-transformer has `enriched_history_operations` schema defined but the transformation logic is stubbed. Analytics users need denormalized operation data that joins operations with their transaction and ledger context. Without this, they must:
- Write complex 3-way JOINs manually (operations + transactions + ledgers)
- Understand internal IDs and foreign key relationships
- Repeat the same JOIN logic across multiple queries

This enriched table is **the most important silver table** - it powers operation-level analytics for block explorers, payment tracking, and transaction analysis.

**Current state:**
- ✅ Schema exists in schema.go:24-73
- ⚠️ Transformation stubbed in transformer.go:119-168
- ❌ Bronze Query API only supports ledgers (not operations/transactions)
- ✅ Bronze has all required tables (operations_row_v2, transactions_row_v2, ledgers_row_v2)

---

## Appetite

**1 week** (5 working days)

This is well-scoped for a week because:
- Schema already defined and tested
- Bronze tables exist and are stable
- JOIN logic is straightforward (no complex business rules)
- Clear success criteria (operations with denormalized tx/ledger fields)

**Why not longer?** The core JOIN is simple. Additional time would be wasted on premature optimization.

**Why not shorter?** Need time to extend bronze Query API and test the full pipeline.

---

## Solution

### Fat Marker Sketch

```
Bronze Query API                          Silver Transformer
├─ /query?sql=...                         ├─ Query operations_row_v2
│  (existing)                             ├─ Query transactions_row_v2
│                                         ├─ Query ledgers_row_v2
└─ EXTEND with:                           │
   QueryOperations(lastSeq, limit)        └─ JOIN → enriched_history_operations
   QueryTransactions(lastSeq, limit)            ├─ operation fields
                                                ├─ tx_* denormalized fields
                                                └─ ledger_* denormalized fields
```

### Implementation Approach

**Part 1: Extend Bronze Query API (Days 1-2)**

Add to `ducklake-ingestion-obsrvr-v3/go/query_api.go`:

```go
// New endpoints
/query-operations?schema=testnet&last_seq=1000&limit=100
/query-transactions?schema=testnet&last_seq=1000&limit=100

// SQL executed:
SELECT * FROM operations_row_v2
WHERE ledger_sequence > $last_seq
ORDER BY ledger_sequence ASC
LIMIT $limit
```

Add to `silver-transformer/query_client.go`:

```go
type BronzeOperation struct {
    ID              string
    TransactionID   string
    LedgerSequence  int64
    Type            int
    TypeString      string
    Details         string
    // ... rest of fields
}

func (c *BronzeQueryClient) QueryOperations(schema string, lastSeq int64, limit int) ([]BronzeOperation, error)
func (c *BronzeQueryClient) QueryTransactions(schema string, lastSeq int64, limit int) ([]BronzeTransaction, error)
```

**Part 2: Implement JOIN Transformation (Days 3-4)**

Update `silver-transformer/transformer.go`:

```go
func (st *SilverTransformer) processNewLedgers() error {
    // Query all three bronze tables
    operations := st.bronzeClient.QueryOperations(st.config.BronzeSchema, st.lastProcessedSequence, st.config.BatchSize)
    transactions := st.bronzeClient.QueryTransactions(st.config.BronzeSchema, st.lastProcessedSequence, st.config.BatchSize)
    ledgers := st.bronzeClient.QueryLedgers(st.config.BronzeSchema, st.lastProcessedSequence, st.config.BatchSize)

    // Build lookup maps for efficient joining
    txMap := buildTransactionMap(transactions)
    ledgerMap := buildLedgerMap(ledgers)

    // Join and transform
    enriched := make([]EnrichedOperation, 0, len(operations))
    for _, op := range operations {
        tx := txMap[op.TransactionID]
        ledger := ledgerMap[tx.LedgerSequence]

        enriched = append(enriched, EnrichedOperation{
            // Operation fields
            ID:             op.ID,
            TransactionID:  op.TransactionID,
            Type:           op.Type,
            TypeString:     op.TypeString,
            // Denormalized transaction fields
            TransactionHash: tx.TransactionHash,
            TxSuccessful:   tx.Successful,
            TxAccount:      tx.Account,
            TxFeeCharged:   tx.FeeCharged,
            // Denormalized ledger fields
            LedgerSequence:   ledger.Sequence,
            LedgerClosedAt:   ledger.ClosedAt,
            LedgerTxCount:    ledger.TransactionCount,
            // Derived fields
            IsPayment:       isPaymentOperation(op.Type),
            BatchID:         generateBatchID(),
            BatchInsertTs:   time.Now(),
        })
    }

    // Insert into silver.enriched_history_operations
    return st.insertEnrichedOperations(enriched)
}
```

**Part 3: Soroban Filtering (Day 4)**

Simple filter for `enriched_history_operations_soroban`:

```go
func (st *SilverTransformer) createSorobanOpsView() error {
    _, err := st.silverDB.Exec(`
        CREATE TABLE enriched_history_operations_soroban AS
        SELECT * FROM enriched_history_operations
        WHERE type_string IN (
            'invoke_host_function',
            'extend_footprint_ttl',
            'restore_footprint'
        )
    `)
    return err
}
```

**Part 4: Testing & Metrics (Day 5)**

- End-to-end test with 10,000 ledgers
- Verify JOIN correctness (spot-check random operations)
- Measure transformation latency (target: < 1 second per batch)
- Add Prometheus metrics:
  - operations_processed_total
  - operations_join_latency_seconds
  - operations_insert_latency_seconds
- Update health endpoint with enriched ops stats

### Files to Modify

**Bronze Layer (ducklake-ingestion-obsrvr-v3):**
1. `go/query_api.go` - Add QueryOperations/QueryTransactions handlers
2. `go/query_api_test.go` - Tests for new endpoints

**Silver Layer (silver-transformer):**
3. `query_client.go` - Add client methods for operations/transactions
4. `transformer.go` - Implement JOIN transformation logic
5. `schema.go` - Add Soroban operations table creation
6. `main.go` - Add new Prometheus metrics

---

## Rabbit Holes

**DON'T:**

1. **Don't optimize the JOIN algorithm yet**
   In-memory hash join is fine for batches of 100 ops. Premature optimization.

2. **Don't add streaming joins**
   Process in batches. Streaming adds complexity without measured benefit.

3. **Don't implement partial operation updates**
   Operations are immutable. Full batch insert is fine.

4. **Don't add caching layers**
   Query API is fast enough. Cache adds bugs without measured need.

5. **Don't implement complex retry logic**
   Simple retry on error is fine. Circuit breakers are overkill.

6. **Don't add schema validation**
   Bronze guarantees schema. Silver trusts bronze. Extra validation wastes time.

7. **Don't try to parallelize the transformation**
   Single-threaded is fine. Concurrency adds bugs. Optimize only if measured slow.

---

## No-Gos

Explicitly **OUT OF SCOPE** for this cycle:

❌ Token transfers table (future cycle)
❌ Contract events parsing (handled by token_transfers later)
❌ Performance benchmarking beyond basic latency check
❌ Query API pagination (batch size is sufficient)
❌ Backfilling old data (process forward only)
❌ Multi-network enrichment differences (bronze handles this)
❌ Custom indexing or partitioning
❌ GraphQL or REST API on top of silver (just DuckDB for now)

---

## Done

**Demo:**

```bash
# Start bronze ingestion with Query API
cd ducklake-ingestion-obsrvr-v3
./bin/ducklake-ingestion-obsrvr-v3 -config config/query-api-test.yaml \
  --multi-network --query-port :8081

# Wait for 10,000 ledgers to ingest
curl http://localhost:8081/query -X POST -d '{"sql":"SELECT MAX(sequence) FROM testnet.ledgers_row_v2"}' | jq

# Start silver transformer
cd ../silver-transformer
./silver-transformer

# Watch it process
curl http://localhost:8082/health | jq '.stats'

# Expected output after a few minutes:
{
  "operations_processed": 45230,
  "last_processed_sequence": 10000,
  "enriched_operations_rows": 45230
}

# Verify JOIN correctness - pick random operation
echo "SELECT
    id,
    transaction_hash,  -- From transaction JOIN
    ledger_closed_at,  -- From ledger JOIN
    type_string,       -- From operation
    is_payment         -- Derived field
FROM enriched_history_operations
WHERE ledger_sequence = 5000
LIMIT 5" | duckdb catalogs/silver.duckdb

# Verify Soroban filtering
echo "SELECT COUNT(*) FROM enriched_history_operations_soroban
WHERE type_string NOT IN ('invoke_host_function', 'extend_footprint_ttl', 'restore_footprint')" \
  | duckdb catalogs/silver.duckdb
# Should return 0 (no non-Soroban ops in Soroban table)
```

**Success Criteria:**

✅ QueryOperations and QueryTransactions endpoints work
✅ enriched_history_operations table populated with denormalized data
✅ JOIN correctness verified (tx and ledger fields present and correct)
✅ enriched_history_operations_soroban filtered correctly
✅ Transformation latency < 1 second per batch of 100 operations
✅ Checkpoint resume works (stop/start continues processing)
✅ Health endpoint shows operation processing stats
✅ No data loss (operation count matches bronze)
✅ Service stable for 1 hour processing 10,000 ledgers

---

## Scope Line

```
COULD HAVE ─────────────────────────────────────────
│ • Parallel batch processing
│ • JOIN optimization for large batches
│ • Detailed operation type metrics
│ • Query API result streaming
│
NICE TO HAVE ───────────────────────────────────────
│ • Transformation latency percentiles (p50/p95/p99)
│ • Automatic batch size tuning
│ • Operation validation checks
│ • Query examples in documentation
│
MUST HAVE ══════════════════════════════════════════
│ • QueryOperations and QueryTransactions in bronze API
│ • enriched_history_operations with correct JOINs
│ • enriched_history_operations_soroban filtering
│ • Transformation latency < 1 second per batch
│ • No data loss (count matches bronze)
│ • Checkpoint resume
│ • Health endpoint stats
└─────────────────────────────────────────────────────
```

**Cutting Scope Strategy:**

If behind schedule at 50% mark (2.5 days):
1. Cut COULD HAVE features immediately
2. Skip enriched_history_operations_soroban (can add in cool-down)
3. Simplify JOIN to use DuckDB JOIN instead of in-memory (slower but less code)
4. Reduce test ledger count from 10,000 to 1,000
5. Ship basic enriched_history_operations even if latency is 2-3 seconds

**CORE MUST SHIP:** enriched_history_operations table with tx and ledger fields denormalized

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
- Test bronze Query API changes with curl
- Design JOIN strategy (in-memory vs DuckDB)
- Verify operation/transaction/ledger relationships
- Understand payment operation types

**Right Side (Making Happen):**
- Implement Query API endpoints
- Code the JOIN transformation
- Write Soroban filter
- Test end-to-end with real data
- Add metrics and monitoring

**Red Flag:** If still on left side by end of Day 3 → Simplify JOIN to use DuckDB directly

---

## Cool-Down After This Cycle

**Mandatory 1-2 days** before starting Cycle 3 (Token Transfers).

Cool-down activities:
- Refactor JOIN logic if repetitive
- Add integration tests for JOIN edge cases
- Document operation type mapping
- Measure actual latency percentiles
- Fix any bugs discovered
- Add query examples to README
- Rest before complex token transfers work

---

## Dependencies

**Required:**
- ✅ Cycle 1 completed (current/snapshot tables working)
- ✅ Bronze has operations_row_v2, transactions_row_v2, ledgers_row_v2
- ✅ Bronze ingestion running with Query API
- ⚠️ Must extend Query API with operations/transactions endpoints

**Blocked By:**
- ❌ Bronze Query API extension (part of this cycle, Day 1-2)

**Not Required:**
- ❌ Token transfers parsing (different cycle)
- ❌ Contract events processing (different cycle)

---

## Risk Assessment

**Medium Risk** ⚠️

This cycle has some risk because:
- **Dependency on bronze changes** - Must extend Query API first (Day 1-2 blocker)
- **JOIN complexity** - 3-way JOIN could have edge cases
- **Data volume** - Operations table is largest (could hit performance issues)

**Mitigation Strategies:**
1. **Bronze API risk** - Test Query API changes early (Day 1), validate with curl before silver code
2. **JOIN risk** - Start with small batch (10 ops), verify correctness, then scale up
3. **Performance risk** - Monitor latency from Day 3 onwards, simplify JOIN if slow

**Kill Criteria:**
- If Query API extension fails by end of Day 2 → Kill cycle, return to planning
- If JOIN latency > 5 seconds per batch by Day 4 → Ship simplified version or kill

---

## Success Metrics

Track these daily:

| Metric | Target | Kill If |
|--------|--------|---------|
| Query API endpoints working | Day 2 | Not working by Day 2.5 |
| First successful JOIN | Day 3 | No JOIN by Day 3.5 |
| Transformation latency | < 1s | > 5s by Day 4 |
| Data accuracy | 100% match | < 95% by Day 4 |
| Service stability | No crashes | > 3 crashes/day |

---

## References

- Architecture: [ARCHITECTURE.md](./ARCHITECTURE.md)
- Silver mapping: [SILVER_LAYER_MAPPING.md](../SILVER_LAYER_MAPPING.md)
- Current schema: schema.go:24-73
- Stubbed transformation: transformer.go:119-168
- Hubble docs: https://developers.stellar.org/docs/data/analytics/hubble/data-catalog/data-dictionary/silver#enriched_history_operations
