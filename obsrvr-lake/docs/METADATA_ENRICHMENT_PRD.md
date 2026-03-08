# Product Requirements Document: Metadata Enrichment & Lineage

**Status:** Draft
**Target:** Cycle 5 (Culture Alignment)
**Owner:** Engineering / Data Architecture

---

## 1. Executive Summary

**The Problem:**
While `obsrvr-lake` delivers high-performance real-time data, it currently fails to meet the "Transparency and Provenance" principle of the Obsrvr Data Culture. Rows appear in tables effectively "magically," with no intrinsic record of *which* software version produced them, *what* specific source data generated them, or *whether* they passed internal quality checks.

**The Solution:**
Implement a standardized `_meta` column schema across all Silver and Gold layer tables. This moves metadata from being "implicit" (hidden in logs or deployment history) to "explicit" (stored alongside the data itself).

**Value Proposition:**
- **Debuggability:** Instantly trace a bad row back to a specific transformer version or source batch.
- **Trust:** Users can see quality flags directly on the data.
- **Culture:** Aligns the codebase with the "Data as a Living System" manifesto.

---

## 2. The "Data Culture" Standard

This PRD implements Principle #8 of the Obsrvr Manifesto: **Transparency and Provenance**.

> "Data without lineage is an orphan. Every record should know where it came from and how it got here."

We will implement this by embedding a "Genetic Tag" into every row in the Silver layer.

---

## 3. Schema Specification

### 3.1 The Standard Column

Every table in the Silver layer (`silver_hot` database) and future Gold layers MUST include a `_meta` column of type `JSONB`.

```sql
ALTER TABLE accounts_current 
ADD COLUMN _meta JSONB DEFAULT '{}'::jsonb;
```

### 3.2 The JSON Structure

The `_meta` JSON object will enforce the following schema:

| Key | Short Key | Type | Description | Example |
|-----|-----------|------|-------------|---------|
| **Version** | `v` | String | Semantic version or Git SHA of the transformer that produced this row. | `"v1.2.4-beta"` |
| **Source** | `src` | String | Reference to the upstream data source (e.g., Bronze table + ledger seq). | `"bronze.ledgers:12345678"` |
| **Timestamp** | `ts` | String | ISO 8601 timestamp of *processing* (not ledger close time). | `"2025-01-08T12:00:00Z"` |
| **Quality** | `q` | Object | Quality audit results. | `{"checks_passed": true, "flags": []}` |
| **Lineage** | `lin` | String | (Optional) Trace ID for distributed tracing context. | `"trace-001"` |

**Example Payload:**
```json
{
  "v": "silver-transformer-v0.4.1",
  "src": "bronze:ledgers_row_v2:124500",
  "ts": "2026-01-08T14:22:10Z",
  "q": {
    "ok": true
  }
}
```

---

## 4. Implementation Strategy

### Phase 1: Schema Migration (Database)
Create a migration script to add `_meta` columns to all existing Silver tables.
*   **Target Tables:** `accounts_current`, `trustlines_current`, `enriched_history_operations`, etc.
*   **Backfill:** Update existing rows with a default "legacy" tag: `{"v": "legacy", "note": "pre-meta-migration"}`.

### Phase 2: Code Implementation (Go Services)
Update `silver-realtime-transformer` to construct and write this object.

1.  **Define Struct:** Create a `Metadata` struct in the shared Go library.
2.  **Inject Version:** Inject the build version (Git SHA) into the binary at build time (`ldflags`).
3.  **Transformer Logic:** In the transformation loop, populate the `Metadata` struct for every resulting object.
4.  **SQL Writer:** Update the `INSERT` statements to include the `_meta` column serialization.

### Phase 3: Validation & Exposure
1.  **Query Test:** Verify that complex queries can still run efficiently (Postgres `JSONB` binary format is efficient, but we should ensure we don't index inside it unless necessary).
2.  **API Update:** Optionally expose this metadata in the `stellar-query-api` responses if requested (e.g., via a `?include_meta=true` flag).

---

## 5. Technical Specifications

### 5.1 Go Struct Definition

```go
type RowMetadata struct {
    Version   string `json:"v"`
    Source    string `json:"src"`
    Timestamp string `json:"ts"`
    Quality   QualityMeta `json:"q,omitempty"`
}

type QualityMeta struct {
    Passed bool     `json:"ok"`
    Flags  []string `json:"flags,omitempty"`
}
```

### 5.2 Build Pipeline Update

Update `Makefile` to inject version:

```makefile
VERSION := $(shell git describe --tags --always --dirty)
LDFLAGS := -X main.Version=$(VERSION)

build:
    go build -ldflags "$(LDFLAGS)" ...
```

### 5.3 SQL Migration Example

```sql
-- migrations/005_add_metadata_column.sql

BEGIN;

-- Add to accounts_current
ALTER TABLE accounts_current ADD COLUMN IF NOT EXISTS _meta JSONB DEFAULT '{"v": "legacy"}'::jsonb;

-- Add to enriched_history_operations
ALTER TABLE enriched_history_operations ADD COLUMN IF NOT EXISTS _meta JSONB DEFAULT '{"v": "legacy"}'::jsonb;

-- (Repeat for all tables)

COMMIT;
```

---

## 6. Success Metrics

*   **100% Coverage:** All rows inserted after Date X have a valid `_meta.v` field.
*   **Traceability:** A developer can take a random row from `silver_hot` and identify exactly which Git commit produced it within 60 seconds.
*   **Performance Impact:** Ingestion overhead increases by < 5% (due to JSON serialization). Storage overhead increases by < 10%.

## 7. Risks & Mitigation

*   **Risk:** JSONB storage bloat.
    *   **Mitigation:** Use short keys (`v`, `ts`) and omit empty fields.
*   **Risk:** Performance on high-throughput tables (`enriched_history_operations`).
    *   **Mitigation:** `_meta` is typically "write-only" for the pipeline and "read-rarely" for debugging. We will NOT add indexes to `_meta` content by default.

---

**Next Steps:**
1. Approve this PRD.
2. Developer assigns ticket to create `migrations/005_add_metadata.sql`.
3. Developer updates `silver-realtime-transformer/go` types.
