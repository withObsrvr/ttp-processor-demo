# Index Plane Lookup - Shape Up Pitch

**Problem Statement**: Users need fast transaction hash lookups, but current `/api/v1/silver/explorer/transaction?tx_hash=XXX` endpoint does a slow full-table scan of operations. The Index Plane provides O(1) hash→ledger lookups but has no API.

**Appetite**: 1 day (6-8 hours)

---

## Current State ❌

**How transaction lookups work today:**
```
User requests: /api/v1/silver/explorer/transaction?tx_hash=abc123
     ↓
Query: SELECT * FROM enriched_operations WHERE tx_hash = 'abc123'
     ↓
Full table scan (slow, gets slower as data grows)
     ↓
Returns: Operations for that transaction
```

**Problems:**
- ❌ Slow: Full table scan of operations table
- ❌ Expensive: Queries entire operations dataset
- ❌ Doesn't scale: Gets slower as ledger history grows
- ❌ Inefficient: Index Plane exists but unused

---

## Target State ✅

**New fast lookup path:**
```
User requests: /transactions/abc123
     ↓
Query Index Plane catalog: SELECT ledger_sequence WHERE tx_hash = 'abc123'
     ↓
Fast lookup using catalog metadata (finds correct Parquet file)
     ↓
Returns: Transaction details with ledger location

Query: /transactions/abc123/operations
     ↓
Uses ledger_sequence from index to query operations efficiently
     ↓
Returns: Operations for that transaction
```

**Benefits:**
- ✅ Fast: O(1) hash lookup using catalog
- ✅ Efficient: Only queries relevant ledger range
- ✅ Scalable: Performance constant regardless of history size
- ✅ Enables new features: Batch lookups, verification

---

## Solution - Fat Marker Sketch

### Add Index Plane Reader

Create `index_reader.go` similar to existing `silver_reader.go`:

```go
type IndexReader struct {
    catalogDB *sql.DB     // PostgreSQL catalog
    duckDB    *sql.DB     // DuckDB for Parquet queries
    config    IndexConfig
}

// Fast lookup: hash → ledger info
func (ir *IndexReader) LookupTransactionHash(ctx context.Context, txHash string) (*TxLocation, error)

// Batch lookup for multiple hashes
func (ir *IndexReader) LookupTransactionHashes(ctx context.Context, txHashes []string) ([]TxLocation, error)

// Verify hash exists in specific ledger
func (ir *IndexReader) VerifyTransaction(ctx context.Context, txHash string, ledgerSeq int64) (bool, error)
```

**Lookup strategy:**
1. Query catalog to find which Parquet file contains the hash
2. Query Parquet file directly for transaction details
3. Return ledger_sequence + metadata

### Add Index Plane Endpoints

New REST endpoints in `handlers_index.go`:

```go
// GET /transactions/{hash}
// GET /api/v1/index/transactions/{hash}
func HandleTransactionLookup(w http.ResponseWriter, r *http.Request)

// POST /api/v1/index/transactions/lookup
// Body: {"hashes": ["abc123", "def456", ...]}
func HandleBatchTransactionLookup(w http.ResponseWriter, r *http.Request)

// GET /api/v1/index/transactions/{hash}/verify?ledger={seq}
func HandleTransactionVerify(w http.ResponseWriter, r *http.Request)
```

### Integrate with Existing Endpoints

**Enhance `/api/v1/silver/explorer/transaction`:**
- If Index Plane available: Use index lookup first (fast)
- Fall back to operations query if not in index yet
- Best of both worlds: speed + recency

### Configuration

Add to `config.yaml`:
```yaml
index:
  enabled: true
  catalog_host: "catalog.db.host"
  catalog_port: 25060
  catalog_database: "stellar_hot"
  catalog_user: "doadmin"
  catalog_password: "${INDEX_CATALOG_PASSWORD}"
  ducklake_path: "/tmp/duckdb/index_reader.db"
  s3_endpoint: "s3.us-west-004.backblazeb2.com"
  s3_bucket: "obsrvr-prod-testnet-index"
  s3_access_key_id: "${B2_KEY_ID}"
  s3_secret_access_key: "${B2_KEY_SECRET}"
```

---

## Scope Lines

### MUST HAVE (Core functionality)
- Index Plane Reader implementation
- Catalog query for hash → ledger lookup
- `/transactions/{hash}` endpoint (simple lookup)
- Integration with existing transaction endpoint
- Config structure for Index Plane
- Error handling (hash not found, catalog unavailable)

### NICE TO HAVE
- Batch lookup endpoint (`POST /api/v1/index/transactions/lookup`)
- Response caching (5 minute TTL for lookups)
- Metrics (lookup count, cache hit rate)

### COULD HAVE (Future work)
- Transaction verification endpoint
- Partial hash search (prefix matching)
- GraphQL interface
- WebSocket notifications for new transactions

---

## Rabbit Holes to Avoid

### ❌ DON'T build a complex cache invalidation system
Keep it simple: 5-minute TTL cache or no cache initially. Index data is immutable.

### ❌ DON'T try to query DuckLake directly without catalog
Use the catalog metadata to find the right Parquet files. That's the whole point of DuckLake.

### ❌ DON'T optimize for partial hash matching yet
Full hash lookup is the requirement. Partial matching is a future nice-to-have.

### ❌ DON'T worry about backfilling historical transactions yet
Index Plane currently has recent data. Historical backfill is a separate cycle.

---

## No-Gos (Explicitly Out of Scope)

- Historical transaction backfill (Index Plane starts from ledger 100k+)
- Account transaction history (use Silver operations for that)
- Transaction search/filtering (only exact hash lookup)
- Real-time notifications/webhooks
- GraphQL API (REST only for now)

---

## Done Looks Like

### Success Criteria

**1. Code compiles and runs:**
```bash
cd stellar-query-api/go
go build -o ../bin/stellar-query-api
./bin/stellar-query-api -config config.yaml
# Outputs: "✓ Index Plane reader initialized"
```

**2. Fast transaction hash lookup works:**
```bash
# Lookup transaction hash
curl http://localhost:8092/transactions/abc123

# Response (< 100ms):
{
  "tx_hash": "abc123...",
  "ledger_sequence": 125000,
  "operation_count": 2,
  "successful": true,
  "closed_at": "2025-01-02T14:30:00Z"
}
```

**3. Not found handled gracefully:**
```bash
curl http://localhost:8092/transactions/nonexistent

# Response:
{
  "error": "transaction not found in index"
}
```

**4. Catalog integration working:**
```bash
# Check catalog query happens
# Logs show: "📋 Querying index catalog for hash abc123..."
# Logs show: "✅ Found in ledger range 1, file part_1767363193.parquet"
```

**5. Existing transaction endpoint enhanced:**
```bash
# Transaction details still work, but faster now
curl "http://localhost:8092/api/v1/silver/explorer/transaction?tx_hash=abc123"

# Logs show: "⚡ Using Index Plane for fast lookup"
```

### Not Done Until

- [ ] `index_reader.go` created and tested
- [ ] `handlers_index.go` created with lookup endpoint
- [ ] Config structure supports Index Plane
- [ ] `/transactions/{hash}` endpoint returns correct data
- [ ] Catalog query finds correct Parquet file
- [ ] Hash not found returns 404 with clear message
- [ ] Integration with existing transaction endpoint
- [ ] Health check includes Index Plane status
- [ ] Logs show catalog and Parquet queries

---

## Hill Chart Tracking

### Left Side (Figuring Things Out) - 0-50%

- [ ] **10%** - Read existing silver_reader.go to understand pattern
- [ ] **20%** - Design index_reader.go structure
- [ ] **30%** - Prototype catalog query (hash → file lookup)
- [ ] **40%** - Test DuckDB Parquet query with S3
- [ ] **50%** - Confirm query returns correct transaction data

### Right Side (Making It Happen) - 50-100%

- [ ] **60%** - Implement IndexReader with all methods
- [ ] **70%** - Create handlers_index.go with endpoints
- [ ] **80%** - Integrate with main_silver.go
- [ ] **90%** - Update config structure and Nomad job
- [ ] **100%** - Deploy and verify lookup < 100ms

---

## Risks and Mitigations

### Risk 1: Catalog schema mismatch
**Impact**: Medium
**Mitigation**: Use `index.files` and `index.tables` schema we already created. Verify with:
```sql
SELECT * FROM index.files LIMIT 1;
```

### Risk 2: DuckDB S3 authentication
**Impact**: Medium
**Mitigation**: Reuse same S3 config pattern as Silver reader. Same credentials, same pattern.

### Risk 3: Performance not actually faster
**Impact**: Low
**Mitigation**: Index lookups use catalog metadata (indexed), then direct Parquet file access. Should be < 100ms vs 500ms+ for full table scan.

### Risk 4: Index Plane doesn't have old data yet
**Impact**: Low
**Mitigation**: Add fallback to Silver operations query. Document ledger range coverage in `/health` endpoint.

---

## Technical Details Reference

### Example Catalog Query
```sql
-- Step 1: Find which file contains the hash
SELECT file_path, ledger_range, min_ledger_sequence, max_ledger_sequence
FROM index.files
WHERE table_name = 'tx_hash_index'
  AND min_ledger_sequence <= (SELECT ledger_sequence FROM recent_check)
  AND max_ledger_sequence >= (SELECT ledger_sequence FROM recent_check)
ORDER BY file_id DESC
LIMIT 1;
```

### Example DuckDB Parquet Query
```sql
-- Step 2: Query the Parquet file directly
SELECT
    tx_hash,
    ledger_sequence,
    operation_count,
    successful,
    closed_at
FROM read_parquet('s3://bucket/tx_hash_index/ledger_range=1/part_1767363193.parquet')
WHERE tx_hash = 'abc123';
```

### Response Format
```json
{
  "tx_hash": "abc123def456...",
  "ledger_sequence": 125000,
  "operation_count": 2,
  "successful": true,
  "closed_at": "2025-01-02T14:30:00Z",
  "ledger_range": 1,
  "indexed_at": "2025-01-02T14:30:05Z"
}
```

---

## Files to Create/Modify

### New Files
1. `go/index_reader.go` - Index Plane reader implementation
2. `go/handlers_index.go` - HTTP handlers for index endpoints
3. `INDEX_INTEGRATION_GUIDE.md` - How to enable Index Plane

### Modified Files
1. `go/config.go` - Add IndexConfig struct
2. `go/main_silver.go` - Initialize IndexReader, register routes
3. `go/handlers_silver.go` - Enhance transaction endpoint to use index
4. `config.yaml` - Add index configuration section
5. `README.md` - Document new /transactions/{hash} endpoint

### Keep Unchanged
- Hot/Cold readers (Bronze layer)
- Silver readers (analytics layer)
- All existing endpoints continue to work

---

## Example Usage

### Basic Lookup
```bash
# Lookup by hash
curl http://localhost:8092/transactions/abc123

# With full URL
curl http://localhost:8092/api/v1/index/transactions/abc123
```

### Batch Lookup (Nice to Have)
```bash
curl -X POST http://localhost:8092/api/v1/index/transactions/lookup \
  -H "Content-Type: application/json" \
  -d '{"hashes": ["abc123", "def456", "ghi789"]}'

# Response:
{
  "results": [
    {"tx_hash": "abc123", "ledger_sequence": 125000, ...},
    {"tx_hash": "def456", "ledger_sequence": 125010, ...},
    {"tx_hash": "ghi789", "error": "not found"}
  ]
}
```

### Integration with Block Explorer
```javascript
// Fast hash lookup
async function lookupTransaction(txHash) {
  const response = await fetch(`/transactions/${txHash}`);
  const data = await response.json();

  // Got ledger_sequence, now get full details
  const details = await fetch(
    `/api/v1/silver/explorer/transaction?tx_hash=${txHash}`
  );

  return { location: data, details: await details.json() };
}
```

---

**Ready to ship!** Clear scope, existing patterns to follow, focused on core hash lookup functionality.
