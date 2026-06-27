# `enriched_history_operations` participant indexes — shaped fix

_Written 2026-06-27 after a real account (`GB32463O…XVY7X`) 503'd in Prism. The hot
`enriched_history_operations` table is indexed on `source_account` but not on the other
participant columns, so the account-activity query seq-scans for any account that mostly
**receives**. Same class as the `tx_receipts` source-index fix._

## Problem
The account page's activity query (`account_activity.go`) filters:
```sql
... FROM enriched_history_operations
WHERE (source_account = $1 OR destination = $1)
  AND (is_payment_op = true OR (is_soroban_op = true AND source_account = $1))
```
`source_account` is indexed; **`destination` is not**. For a receive-heavy account (matches
almost entirely on `destination`), Postgres can't satisfy the `OR` from indexes and
**seq-scans ~175M hot rows** → exceeds the gateway timeout → **503**. Confirmed: `GB32463O…`
appears as `destination` 220× and `source_account` 0×, and a 4-way participant `COUNT` even
timed out at 60s for me.

## Appetite
**1–2 days.**

## Solution (fat-marker)
1. **`EXPLAIN ANALYZE`** the account-activity query for a receive-heavy account → confirm the
   `destination` seq scan.
2. **Add a btree index on the unindexed participant column(s)** the hot account/relationship
   queries actually filter on — `destination` is certain; audit `account_activity.go`,
   `relationships.go`, `account_history.go` for `destination_account` / `from_account` /
   `into_account` / `to_account` filters and index the ones used:
   ```sql
   CREATE INDEX CONCURRENTLY IF NOT EXISTS eho_destination_idx   ON enriched_history_operations (destination);
   -- add only if the column exists AND is filtered un-indexed (verify with the queries above):
   CREATE INDEX CONCURRENTLY IF NOT EXISTS eho_from_account_idx  ON enriched_history_operations (from_account);
   CREATE INDEX CONCURRENTLY IF NOT EXISTS eho_into_account_idx  ON enriched_history_operations (into_account);
   ```
   With `destination` indexed, `(source_account=$1 OR destination=$1)` becomes a **bitmap-OR of
   two index scans** instead of a seq scan.
3. **Durable home:** `enriched_history_operations` is written by **silver-realtime-transformer** —
   add these indexes to **its** silver schema so a fresh DB has them (or, for symmetry with the
   `tx_receipts` work, an idempotent `CONCURRENTLY` ensure-step). Apply `CONCURRENTLY` to prod now
   to un-break the page.
4. Re-`EXPLAIN` → confirm bitmap index scans, no seq scan; the page loads within the gateway budget.

## Rabbit holes (don't)
- Don't index every account-ish column — match the **actual filters** in the queries (over-indexing
  slows the transformer's writes on a 175M-row, high-ingest table).
- If the real access pattern is "all ops involving X across **all** roles," evaluate a single
  generated `participants text[]` + GIN instead of 3–4 btrees — but only if the queries are rewritten
  to use it; don't add a GIN nothing queries.
- Don't build these on the hot path synchronously (175M rows) — `CONCURRENTLY`, background.

## No-Gos
Changing the query semantics, the table schema beyond indexes, or the cold layer (separate item).

## Done
- `EXPLAIN` shows the account-activity query uses index/bitmap scans on `destination`, no seq scan.
- A receive-heavy account page (e.g. `GB32463O…`) loads **within the gateway timeout** (no 503).
- The indexes are created by the silver schema bootstrap, so a fresh `silver_hot` has them.

## Notes
- This fixes only the **hot** arm. The account page also misses **pre-handoff** history that lives
  in **silver cold** — that's the separate `account-history-cold-federation-shaped-fix.md`. Both are
  needed for a complete, fast account page.
