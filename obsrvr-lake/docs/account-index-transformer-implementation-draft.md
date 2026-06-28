# Account Index Transformer — implementation draft

_Written 2026-06-28. Code-level draft of the **account index-plane** (the unbuilt blocker for fast deep
account history). It's a third instance of the proven index-plane pattern, alongside
`index-plane-transformer` (tx-hash) and `contract-event-index-transformer` (contract→ledger). Companion
to the shaped item `account-index-plane-shaped-fix.md` and `project_account_history_cold_federation`._

## What it produces
A skinny `account_ledger_index(account_id, ledger_range, account_bucket)` table in the Index Plane
DuckLake, then read by the query-api to add `AND ledger_range IN (...)` to the **cold arm** of the account
history query — turning a 30s full cold scan into a pruned read of a handful of files.

## The one critical difference from the tx-hash index — bucket partitioning
The tx-hash reader does `WHERE tx_hash = ?` with **no partition pruning** (the table is partitioned by
`ledger_range`, but a hash doesn't tell you the range). It's fast only because `tx_hash_index` is skinny
and warmed — one row per transaction, scanned cheaply.

The account index is **much bigger** — ~20M accounts × ~10 distinct ledger-ranges each ≈ **~200M rows**.
A naive `WHERE account_id = ?` full scan would be ~1–2s — we'd have rebuilt the slow-lookup problem one
level down. So the account index **must prune on the lookup key**:

- Add a column `account_bucket = crc32(account_id) % 256`, computed in **Go** on both the writer (this
  service) and the reader (query-api) — identical `hash/crc32.ChecksumIEEE`, so they always agree.
- **Partition the DuckLake table by `account_bucket`.**
- The query-api passes `WHERE account_bucket = <N> AND account_id = '<id>'` → DuckLake prunes to ~1/256
  of the index (~800k rows) → ~10–50ms warm.

This is the load-bearing design decision; everything else mirrors the tx-hash transformer.

## Source of participant→ledger pairs
Read **silver hot `enriched_history_operations`** — participants are already extracted there as
`source_account, destination, from_account, to_address, address`.
- **Forward/streaming index:** all five roles.
- **One-time cold backfill** (pre-handoff history, from silver **cold**): only `source_account` and
  `destination` — the cold table typed `from_account/to_address/address` as int32 (backfill left them
  all-NULL; the int32 gotcha from the cold-federation work). So deep-history coverage is source+dest only
  until a cold reprocess fixes those columns. Document this; it matches the cold-arm participant filter.

---

## New service files (`obsrvr-lake/account-index-transformer/go/`)
Clone `index-plane-transformer/go` and change the account-specific files below. `main.go`,
`transformer.go`, `checkpoint.go`, `advisory_lock.go`, `health.go`, `grpc_client.go` are copied verbatim
(only the read/write call names and the source DB change — noted at the end).

### `bucket.go` (new — shared with the query-api)
```go
package main

import "hash/crc32"

// AccountIndexBuckets partitions the account index. account_bucket = crc32(account_id) % N, computed
// identically in Go on the writer (this service) and the reader (query-api) so DuckLake prunes to one
// bucket on lookup. Changing N requires a full index rebuild.
const AccountIndexBuckets = 256

// AccountBucket returns the deterministic partition bucket for an account ID.
func AccountBucket(accountID string) int64 {
	return int64(crc32.ChecksumIEEE([]byte(accountID)) % AccountIndexBuckets)
}
```

### `types.go`
```go
package main

// AccountLedgerIndex is one (account, ledger-range) presence row.
type AccountLedgerIndex struct {
	AccountID     string
	LedgerRange   int64
	AccountBucket int64
}
```

### `silver_reader.go` (replaces `bronze_hot_reader.go`; reads silver hot)
```go
// ReadParticipants returns DISTINCT (account_id, ledger_range) for any participant role appearing in
// enriched_history_operations within (fromLedger, toLedger]. ledger_range = ledger / partitionSize.
func (r *SilverHotReader) ReadParticipants(ctx context.Context, fromLedger, toLedger, partitionSize int64) ([]AccountLedgerIndex, error) {
	q := fmt.Sprintf(`
		SELECT DISTINCT account_id, CAST(ledger_sequence / %d AS BIGINT) AS ledger_range
		FROM (
			SELECT source_account AS account_id, ledger_sequence FROM enriched_history_operations
				WHERE ledger_sequence > $1 AND ledger_sequence <= $2 AND source_account IS NOT NULL
			UNION ALL SELECT destination,  ledger_sequence FROM enriched_history_operations
				WHERE ledger_sequence > $1 AND ledger_sequence <= $2 AND destination  IS NOT NULL
			UNION ALL SELECT from_account, ledger_sequence FROM enriched_history_operations
				WHERE ledger_sequence > $1 AND ledger_sequence <= $2 AND from_account IS NOT NULL
			UNION ALL SELECT to_address,   ledger_sequence FROM enriched_history_operations
				WHERE ledger_sequence > $1 AND ledger_sequence <= $2 AND to_address   IS NOT NULL
			UNION ALL SELECT address,      ledger_sequence FROM enriched_history_operations
				WHERE ledger_sequence > $1 AND ledger_sequence <= $2 AND address      IS NOT NULL
		) p`, partitionSize)

	rows, err := r.db.QueryContext(ctx, q, fromLedger, toLedger)
	if err != nil {
		return nil, fmt.Errorf("read participants: %w", err)
	}
	defer rows.Close()

	var out []AccountLedgerIndex
	for rows.Next() {
		var a AccountLedgerIndex
		if err := rows.Scan(&a.AccountID, &a.LedgerRange); err != nil {
			return nil, err
		}
		a.AccountBucket = AccountBucket(a.AccountID)
		out = append(out, a)
	}
	return out, rows.Err()
}
```
`GetLedgerBounds` stays as-is but points at `enriched_history_operations` (MIN/MAX ledger_sequence).

### `index_writer.go` — DDL + write (the deltas vs tx-hash)
DDL — partition by `account_bucket`, **not** `ledger_range`:
```go
createTableSQL := fmt.Sprintf(`
	CREATE TABLE IF NOT EXISTS %s (
		account_id VARCHAR,
		ledger_range BIGINT,
		account_bucket BIGINT
	)`, fullTableName)
// ...
partitionSQL := fmt.Sprintf(`ALTER TABLE %s SET PARTITIONED BY (account_bucket)`, fullTableName)
```
Write — **no per-batch anti-join.** Unlike the tx-hash writer (which dedups by `tx_hash` via a LEFT JOIN
against the whole table), the account index **tolerates duplicate `(account_id, ledger_range)` pairs**:
the reader uses set semantics (`ledger_range IN (...)`), so dups are harmless, and the full-index
anti-join would be far too expensive at ~200M rows. Compaction `DISTINCT`s them later.
```go
func (iw *IndexWriter) WriteAccountIndex(ctx context.Context, rows []AccountLedgerIndex) (int64, error) {
	if len(rows) == 0 {
		return 0, nil
	}
	full := fmt.Sprintf("%s.%s.%s", iw.config.CatalogName, iw.config.SchemaName, iw.config.TableName)

	var b strings.Builder
	for i, r := range rows {
		if i > 0 {
			b.WriteString(", ")
		}
		fmt.Fprintf(&b, "('%s', %d, %d)", r.AccountID, r.LedgerRange, r.AccountBucket)
	}
	insert := fmt.Sprintf("INSERT INTO %s (account_id, ledger_range, account_bucket) VALUES %s", full, b.String())
	res, err := iw.db.ExecContext(ctx, insert)
	if err != nil {
		return 0, fmt.Errorf("insert account index: %w", err)
	}
	n, _ := res.RowsAffected()
	return n, nil
}
```
Keep the rest of the tx-hash `index_writer.go` verbatim (DuckLake attach, `FlushInlinedData`,
`RunCheckpoint`/merge, S3 secret). Just fix the hardcoded `"transaction_hash_index"` in `RunCheckpoint`
to `iw.config.TableName`.

### `transformer.go` / `config.go` deltas
- `config.IndexCold.TableName: account_ledger_index`; `SchemaName: index` (same Index Plane catalog).
- Source DB = **silver hot** (rename `bronze_hot` config block → `silver_hot`, point at `silver_hot`/6432).
- In `runTransformationCycle`: `t.silverReader.ReadParticipants(ctx, lastLedger, endLedger, partitionSize)`
  → `t.indexWriter.WriteAccountIndex(ctx, rows)`.
- Optional: set `batch_size = partition_size` (100k) so each cycle covers one ledger_range and emits each
  pair once (eliminates cross-batch dups for completed ranges; the in-flight range still re-emits until it
  closes — harmless).

---

## Query-api integration (`obsrvr-lake/stellar-query-api/go/`)

### `account_index_reader.go` (new — mirror `index_reader.go`, with the bucket prune)
Copy `bucket.go` (`AccountBucket`) into the query-api package too, then:
```go
// LookupLedgerRanges returns the ledger_range buckets where account appears, pruned to one partition.
func (ir *AccountIndexReader) LookupLedgerRanges(ctx context.Context, accountID string) ([]int64, error) {
	bucket := AccountBucket(accountID)
	q := fmt.Sprintf(
		`SELECT DISTINCT ledger_range FROM %s.%s.%s WHERE account_bucket = ? AND account_id = ?`,
		ir.catalogName, ir.schemaName, ir.tableName)

	rows, err := ir.db.QueryContext(ctx, q, bucket, accountID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var ranges []int64
	for rows.Next() {
		var r int64
		if err := rows.Scan(&r); err != nil {
			return nil, err
		}
		ranges = append(ranges, r)
	}
	return ranges, rows.Err()
}
```
Warm it at startup like the tx-hash reader (`SELECT COUNT(*)` + a per-bucket scan) so first lookups are warm.

### The prune in `account_history.go` → `buildAccountTransactionsQuery`
Before building the **cold** arm, look up the ranges; if present, constrain the cold scan:
```go
var coldRangeFilter string
if ranges, err := h.accountIndex.LookupLedgerRanges(ctx, accountID); err == nil && len(ranges) > 0 {
	coldRangeFilter = " AND ledger_range IN (" + joinInt64(ranges) + ")"
}
// ... cold arm WHERE ... + coldRangeFilter
```
**Fallback is the current behavior:** if the index has no rows for the account (not yet built, or a brand-new
account) `coldRangeFilter` stays empty and the query runs exactly as today. So this is safe to ship dark —
it can only make the cold arm faster, never wrong.

---

## One-time cold backfill
Mirror the tx-hash cold backfill: a one-shot job that reads silver **cold** `enriched_history_operations`
(only `source_account`, `destination`) over the full pre-handoff range, computes `(account_id,
ledger_range, account_bucket)`, and bulk-inserts into `account_ledger_index`. Run it once; the forward
transformer covers everything after the handoff. (Same `WriteAccountIndex` path; just a cold source query.)

## Sizing, dups, maintenance
- **Size:** ~200M rows (skinny: VARCHAR + 2×BIGINT) → a few GB Parquet, bucketed 256 ways.
- **Lookup cost:** one bucket ≈ 800k rows → ~10–50ms warm. (Verify with `EXPLAIN` that `account_bucket = N`
  prunes partitions.)
- **Dups:** tolerated by design (set-semantics reader); periodic `DISTINCT` compaction keeps size bounded.
- **Maintenance:** reuse the tx-hash writer's `FlushInlinedData` + `RunCheckpoint` (merge-only, never
  expire/cleanup — cold is append-only).

## Rollout sequence
1. Scaffold the service (clone + the deltas above), deploy pointed at silver hot — forward index starts.
2. Run the cold backfill once (source+dest coverage for deep history).
3. Add `account_index_reader.go` + the `buildAccountTransactionsQuery` prune to the query-api, shipped
   dark (empty filter = today's behavior).
4. Verify on `GB32463O…`: cold arm `EXPLAIN` shows `ledger_range IN (...)` pruning; deep account history
   returns in <1s instead of timing out.

## Open decisions
- **Bucket count (256):** trades partition count vs rows-per-bucket. 256 → ~800k rows/bucket. Fine; revisit
  if accounts grow.
- **Index store:** this keeps it in the Index Plane DuckLake (consistent with tx-hash/contract). If lookup
  latency disappoints even bucketed, the fallback is to hold this skinny index in Postgres with a B-tree on
  `(account_bucket, account_id)` — it's small enough (~GBs) to live there cheaply. Decide on the measured
  number, not upfront.
