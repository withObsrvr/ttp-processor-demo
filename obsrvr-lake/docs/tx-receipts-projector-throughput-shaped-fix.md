# tx_receipts projector throughput — shaped fix

> **✅ IMPLEMENTED 2026-06-26 (index step)** — `EnsureSourceIndexes` added to
> `serving-projection-processor/go/schema_init.go`, wired in `main.go` as a background step on
> startup (when `schema.auto_apply`). It builds the 3 genuinely-missing `transaction_hash`
> indexes `CONCURRENTLY` + `IF NOT EXISTS` (silver `effects`, silver `semantic_activities`,
> bronze `transactions_row_v2` — the only three where `transaction_hash` isn't already a leading
> index column), drops any invalid leftover from an interrupted build, and is best-effort
> (logs + continues, never crashes the processor). `go build`/`vet`/`test` green.
> **Still TODO:** deploy → confirm the 3 indexes built → re-`EXPLAIN` the loaders → re-enable
> `tx_receipts` (`enabled: true`) and confirm ≤5s/ledger.

_Written 2026-06-26 after disabling `tx_receipts` on mainnet `-fast` (it was ~15× slower than real-time
and starving the other feeds). This is the durable fix to make it sustain real-time and re-enable it.
NOT a rewrite — it's missing indexes. Same family as the serving-constraints gap
(`serving-onconflict-constraints-shaped-fix.md`)._

## Problem
The `tx_receipts` projector (in `serving-projection-processor-fast`) runs at **~73s per ~256-tx batch
(~1 ledger)** even on HOT data — ~15× slower than mainnet's ~5s/ledger. So it can never catch up, and
because the `-fast` processor runs projectors in sequence, it **starves the other feeds**
(`operations_recent`, `contract_calls_recent` sat behind it at ~62947655 while it ground forward).
Currently **disabled** (`serving-projection-processor-fast.nomad` → `tx_receipts.enabled: false`);
tx-detail pages fall back to on-demand `/silver/tx/batch/decoded` (no data loss).

## What it is NOT
NOT an N+1 / per-transaction problem — the projector **already batches by hash**: each of its 5 loaders
(`tx_receipts_projector.go:298–455`) runs ONE query `WHERE transaction_hash = ANY($1)` for the whole
batch. The cost is **5 batched queries across 5 source tables**, several of which **sequential-scan**
because `transaction_hash` isn't an indexed leading column.

## Root cause (profiled on mainnet)
| Loader | Table | `transaction_hash` index? |
|---|---|---|
| `loadOperations` | silver `enriched_history_operations` | PK leads with `transaction_hash` ✅ |
| `loadEffects` | silver `effects` | `effects_pkey` — **verify `transaction_hash` is the leading column** |
| `loadTokenEvents` | silver `token_transfers_raw` | `idx_token_transfers_unique` — **verify leading column** |
| `loadSemantic` | silver `semantic_activities` | **❌ NONE — SEQ SCAN** (confirmed) |
| `loadTransactionDetails` | bronze `transactions_row_v2` | `transactions_row_v2_uniq` — **verify leading column** |

A `WHERE transaction_hash = ANY(...)` only uses an index when `transaction_hash` is the **leading**
column. `semantic_activities` has no such index at all → full seq scan of a multi-million-row table on
every batch; any of the unique indexes that don't *lead* with `transaction_hash` seq-scan too. ~seconds
per seq-scan × 5 loaders = the ~73s/batch.

## Appetite
**1–2 days.**

## Solution (fat-marker)
1. **`EXPLAIN ANALYZE` each of the 5 loader queries** with a realistic ~256-element hash array → confirm
   exactly which seq-scan (semantic_activities certainly; check effects/token_transfers_raw/transactions_row_v2).
2. **Add a `transaction_hash` btree index where missing or not-leading** (online, so no write block):
   ```sql
   CREATE INDEX CONCURRENTLY IF NOT EXISTS semantic_activities_txhash_idx     ON semantic_activities (transaction_hash);     -- confirmed missing
   -- add for effects / token_transfers_raw (silver) and transactions_row_v2 (bronze) ONLY if
   -- transaction_hash is not the leading column of an existing usable index:
   CREATE INDEX CONCURRENTLY IF NOT EXISTS effects_txhash_idx                 ON effects (transaction_hash);
   CREATE INDEX CONCURRENTLY IF NOT EXISTS token_transfers_raw_txhash_idx     ON token_transfers_raw (transaction_hash);
   CREATE INDEX CONCURRENTLY IF NOT EXISTS transactions_row_v2_txhash_idx     ON transactions_row_v2 (transaction_hash);
   ```
3. **Re-EXPLAIN** to confirm all 5 are index scans; consider raising `tx_receipts.batch_size` (currently
   256) once each query is an index lookup (more tx per round-trip = fewer planning hits).
4. **Create these indexes from the serving schema bootstrap / cold-backfill** — don't hand-apply to prod
   only (same gap as the serving constraints). Fold into the one "ensure serving schema" startup step.
5. **Re-enable** `tx_receipts` (`enabled: true`) and confirm it sustains **≤5s/ledger** and tracks head
   without starving the other `-fast` feeds.

## Rabbit holes (don't)
- Don't rewrite the 5 batched loaders into one mega-join — a 5-way join over millions of rows can be
  worse; the per-table batched loaders are fine once each is an index scan.
- Don't drop fields/sources to "speed it up" — fix the access path, not the output shape.
- Don't apply the indexes only to prod by hand — they belong in the schema bootstrap.
- `pgx.QueryExecModeSimpleProtocol` is used deliberately for `ANY($1)`; leave it unless EXPLAIN shows a
  planning issue after the indexes.

## No-Gos
Changing `sv_tx_receipts` output shape, its `ON CONFLICT (tx_hash)` key, or the per-table batching model.

## Done
- `EXPLAIN ANALYZE` shows **all 5 loader queries are index scans** (no seq scans) on a 256-hash batch.
- With `tx_receipts` re-enabled, it processes **≤5s/ledger** (sustains real-time), tracks head, and the
  other `-fast` feeds keep advancing.
- The required indexes are **created by the serving schema bootstrap** (a fresh serving DB has them).

## Notes
- This is the **third** instance of "serving streaming needs schema objects the backfill didn't create"
  (after the `ON CONFLICT` unique constraints and the `_recent`/`_current` indexes). All three should be
  one idempotent **"ensure serving schema (tables + constraints + indexes)"** step run on processor
  startup. See `serving-onconflict-constraints-shaped-fix.md`.
- Until fixed, `tx_receipts` stays disabled; tx-detail works via the on-demand `/silver/tx/batch/decoded`
  fallback (slower per request, no data loss).
