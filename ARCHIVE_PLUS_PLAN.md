# Archive+ Index Plane Implementation Plan

## Vision

Transform Obsrvr from a hot/cold data lake into a **platform-grade archive** that provides:
- Fast random lookups (tx_hash, events) at full-history scale
- Analytics-ready canonical tables (Silver layer)
- Operational confidence through coverage/freshness/lineage APIs
- Composable, reproducible datasets that compound in value

## Strategic Context

**Stellar is solving:** "How do I find a transaction?"
**Obsrvr is solving:** "How do I understand the ledger?"

This implementation adds the **Index Plane** - a dedicated lookup layer that sits beside Silver and provides fast random access without compromising the analytics-first architecture.

### Core Principle

> **DuckLake is truth. Exported indexes are accelerators.**

All indexes are derived from Bronze/Silver and can be rebuilt. They start as DuckLake tables (easy iteration) and can be promoted to specialized formats (RocksDB, PHF) when performance demands it.

---

## Architecture Overview

```
Stellar Data Source
  → Hot Buffer (PostgreSQL, last ~10–20 minutes)
  → Flush Service (Postgres → DuckLake Bronze)
  → Bronze → Silver Transformer (DuckDB SQL, checkpointed)
  → DuckLake Silver (canonical tables)
  → Index Plane (NEW - derived lookup artifacts)
  → Query API (unified hot + cold + indexes)
```

### New Components

1. **Index Plane Tables** (DuckLake)
   - `tx_hash_index`: Fast tx_hash → ledger_sequence lookup
   - `event_index`: Fast event queries and pagination
   - `meta_coverage`: Dataset completeness tracking

2. **Index Plane Transformer** (New Service)
   - Derives indexes from Silver tables
   - Checkpointed, incremental processing
   - Single-writer per catalog (DuckLake constraint)

3. **Enhanced Query API**
   - Routes tx_hash lookups through Index Plane
   - Routes event queries through Index Plane
   - Exposes meta endpoints for operational confidence

---

## Implementation Cycles

### Cycle 1: Index Plane Foundation + Tx Hash Index (1 week)
**Goal:** Ship working tx_hash lookup via DuckLake index

- Index Plane schema design and DuckLake tables
- Basic index transformer service (checkpointed)
- Derive tx_hash_index from Silver transactions
- Query API route for tx_hash lookups using index

**Ships:** `/explorer/transaction?tx_hash=...` backed by index

---

### Cycle 2: Event Index + Coverage Metadata (1 week)
**Goal:** Enable fast event queries and expose dataset coverage

- Event index schema and table
- Derive event_index from Silver events/operations
- Coverage metadata table and derivation
- Basic pagination support for events

**Ships:** Event queries with proper pagination + coverage visibility

---

### Cycle 3: Query API Integration (3-5 days)
**Goal:** Full integration of indexes into existing API endpoints

- Update all explorer endpoints to use indexes
- Hot/cold unification with index routing
- Performance testing and optimization
- Caching strategy for common queries

**Ships:** All explorer endpoints using Index Plane

---

### Cycle 4: Operational Meta Endpoints (3-5 days)
**Goal:** Platform trust through operational visibility

- `/meta/freshness` endpoint (pipeline lags)
- `/meta/coverage` endpoint (dataset ranges)
- `/meta/lineage` endpoint (transformation provenance)
- Health checks for Index Plane transformer

**Ships:** Operational confidence for platform users

---

### Cycle 5: Index Export to RocksDB (2 weeks - FUTURE)
**Goal:** Optimize hot-path lookups with specialized storage

- Export tx_hash_index to RocksDB snapshot
- Serving layer for RocksDB lookups
- Rebuild/refresh mechanics
- Performance comparison and migration

**Ships:** Sub-100ms tx_hash lookups at any scale

---

## Success Metrics

### Performance Targets (v1 - DuckLake indexes)
- Recent queries (hot): **<100ms**
- Historical queries (cold): **<2s** with pruning
- Transaction by hash: **p95 < 500ms** (index + table joins)
- Events backfill: proportional to results, not history scanned

### Performance Targets (v2 - RocksDB export)
- Transaction by hash: **p95 < 300ms**
- Eventually: **p95 < 100ms** with caching

### Operational Metrics
- Coverage endpoint shows exact ledger ranges
- Freshness endpoint shows pipeline lag < 5 minutes
- Index transformer processes incremental batch in < 30s

---

## Technical Constraints

1. **Single-writer rule:** Only one Index Plane transformer per catalog/network (DuckLake limitation)
2. **Incremental-only:** Transformer must checkpoint and support restarts
3. **Rebuild capability:** Any index can be rebuilt from Silver at any time
4. **Backward compatibility:** Existing API responses unchanged, only performance improves

---

## Risk Mitigation

### Rabbit Holes to Avoid
- Don't build RocksDB export until DuckLake indexes prove the pattern
- Don't optimize pagination extensively - basic offset/limit is fine for v1
- Don't add real-time index updates - incremental batching is sufficient
- Don't create complex index compaction strategies - simple append works

### Unknowns to Explore Early
- What is actual tx_hash lookup latency with DuckLake? (Measure in Cycle 1)
- How large do index tables grow? (Monitor from day 1)
- What partition strategy works best? (Test ledger-range vs. time-based)

---

## Cool-Down Between Cycles

After each cycle, take 1-2 days to:
- Fix bugs discovered
- Refactor rough edges
- Explore next cycle requirements
- Rest and regain perspective

**Do not skip cool-downs.** They prevent burnout and keep code quality high.

---

## Next Steps

1. Review this plan and adjust appetites/scope
2. Read Cycle 1 shape document: `./docs/shapes/cycle1-index-foundation.md`
3. Get approval before starting implementation
4. Track progress on hill (left = figuring out, right = building)

---

## Notes

- This plan assumes existing hot/cold architecture is stable
- Each cycle ships working software, even if not fully optimized
- Indexes are always optional accelerators - Silver tables remain source of truth
- The plan is front-loaded (harder cycles first) so we can cut scope later if needed
