# Account Index Plane Implementation Plan

_Written 2026-07-05. This turns the existing account-index-plane shaped fix into an implementation
checklist. The goal is deep, paginated account history for Prism — complete within documented
coverage limits — without making query-api scan all cold Silver files for every account page._

_Revised 2026-07-05 after review against the actual `index-plane-transformer` / `stellar-query-api`
code and the 2026-06-28 code-level draft (`account-index-transformer-implementation-draft.md`):
reverted to range-granularity schema, documented the cold INT32 coverage limits, enumerated backfill
source tables, and recorded the multi-index migration traps. Second revision same day: flipped the
decision to clone-first (sibling `account-index-transformer` service), added coverage-watermark
fallback semantics for the query-api, and qualified the "complete history" wording._

## Decision

Clone the proven index-plane pattern as a sibling service: `obsrvr-lake/account-index-transformer`,
built from the `index-plane-transformer` codebase with only the reader/writer/table specifics
changed. This is the pattern `contract-event-index-transformer` already established — production
practice here is one service per index, so the clone is the boring, proven move.

The clone inherits everything the pattern already owns:

- DuckLake index catalog attachment
- B2/S3 Index Plane writes
- ledger-range partition conventions
- checkpointing
- health reporting
- Nomad deployment pattern

Backfill runs as a separate Nomad job or command mode, but uses the same repo/binary and the same
writer code path.

### Why not multi-index the existing transformer

An earlier revision of this plan proposed extending `index-plane-transformer` with an `indexes:`
config map. Rejected for now: the codebase is single-index end to end (one `IndexCold` table
config, one `BronzeHotReader`, one checkpoint table, one advisory lock, one health-stats struct),
the two indexes read from **different source databases** (`tx_hash` reads bronze hot `stellar_hot`;
`account_ledger` reads silver hot `silver_hot`), and the restructuring would touch a running
production service with two known traps:

- The deployed tx_hash checkpoint lives at `index.transformer_checkpoint` (the config default);
  renaming it without migrating the row restarts tx_hash indexing from zero or trips the
  retention-gap guard.
- `RunCheckpoint` in `index_writer.go` hardcodes `"transaction_hash_index"` in the merge call.

Clone-first trades some code duplication for zero risk to the running tx_hash transformer.
Consolidating the (then three) index transformers into one multi-index binary is legitimate
cool-down/refactor work for a later cycle, done when nothing depends on the outcome.

## Problem

Prism needs deep, paginated account history — complete within the coverage limits documented under
Backfill Mode. The current account read path can only serve:

- recent serving data, or
- slow hot+cold federation that filters cold Silver by account participant fields

Cold Silver is ledger/range organized. Account predicates like:

```sql
source_account = ?
OR destination = ?
OR from_account = ?
OR to_address = ?
OR address = ?
```

do not line up with the cold partition key. For sparse historical accounts, DuckDB must scan too much
cold data before it can return a small page.

The missing routing index is:

```text
account_id -> ledger ranges / ledger sequences where this account appears
```

## Target Table

Add a new Index Plane DuckLake table. Keep it skinny — **range granularity, three columns**:

```sql
CREATE TABLE IF NOT EXISTS <catalog>.index.account_ledger_index (
  account_id VARCHAR,
  account_bucket BIGINT,
  ledger_range BIGINT
);
```

Do NOT add `ledger_sequence` or per-row timestamps. At range granularity the index is roughly
20M accounts × ~10 ranges each ≈ **~200M rows / a few GB Parquet** — the sizing the bucket-pruned
10–50ms lookup target was validated against. Per-ledger granularity would be one row per distinct
(account, ledger) appearance — plausibly billions of rows — and the reader only ever runs
`SELECT DISTINCT ledger_range`, so the extra precision is never used at lookup time. If a
per-ledger consumer materializes later, add a separate table for it rather than widening this one.

Recommended logical uniqueness:

```text
(account_id, ledger_range)
```

DuckLake will not enforce this like Postgres. The writer should emit distinct rows per batch and
tolerate cross-batch duplicates — a full-index anti-join (the tx_hash writer's dedup approach) is
far too expensive at this scale. The reader uses set semantics (`DISTINCT` / `IN`), so duplicates
are harmless; periodic compaction can `DISTINCT` them away to bound storage.

## Partitioning

Use account-bucket partitioning for lookup pruning:

```text
account_bucket = crc32(account_id) % 256
```

Rationale:

- `ledger_range` is useful after the lookup, but not enough to make `WHERE account_id = ?` cheap.
- Bucket partitioning lets the query-api read roughly 1/256 of the index before filtering by account.
- `ledger_range` remains in the table so account history queries can prune cold Silver.

Both writer and reader must use the same Go implementation:

```go
const AccountIndexBuckets = 256

func AccountBucket(accountID string) int64 {
    return int64(crc32.ChecksumIEEE([]byte(accountID)) % AccountIndexBuckets)
}
```

Changing the bucket count requires a full rebuild.

Operational note — partition fan-out: `tx_hash_index` partitions by `ledger_range`, so each write
lands in one partition and old partitions close forever. This table partitions by `account_bucket`,
so **every incremental batch writes into up to 256 partitions, forever**. `DATA_INLINING_ROW_LIMIT`
on the catalog attach softens this (small batches inline into catalog Postgres until flushed), but
flush + merge across 256 always-hot partitions is a qualitatively different maintenance profile
than tx_hash — and this project has hit DuckLake small-file/B2-transaction explosions before.
Watch files-per-bucket from day one (see Validation).

## Where It Lives

Same Index Plane as `tx_hash_index`:

```text
DuckLake catalog: configured index_cold.catalog_name
Schema: index
Table: account_ledger_index
Object storage: configured B2 index bucket
Partition: account_bucket
```

Example storage shape:

```text
s3://<B2_INDEX_BUCKET>/account_ledger_index/
  account_bucket=0/part_*.parquet
  account_bucket=1/part_*.parquet
  ...
  account_bucket=255/part_*.parquet
```

## New Service: account-index-transformer

Create `obsrvr-lake/account-index-transformer` by cloning `index-plane-transformer/go` and changing
only the account-specific pieces (the code-level draft
`account-index-transformer-implementation-draft.md` has the full file-by-file deltas):

- `bucket.go` (new): `AccountIndexBuckets` / `AccountBucket` — shared verbatim with the query-api.
- `silver_reader.go` (replaces `bronze_hot_reader.go`): reads participant columns from silver hot.
- `index_writer.go`: account DDL, `PARTITIONED BY (account_bucket)`, no per-batch anti-join, and
  fix the cloned `RunCheckpoint` to use `iw.config.TableName` instead of the hardcoded
  `"transaction_hash_index"` — otherwise the account index never gets file merges.
- `config.go`: source block points at `silver_hot`; `table_name: account_ledger_index`;
  `checkpoint_table: index.account_ledger_transformer_checkpoint`.
- `main.go`, `transformer.go`, `checkpoint.go`, `advisory_lock.go`, `health.go`, `grpc_client.go`:
  copied verbatim apart from read/write call names.

Config sketch:

```yaml
service:
  name: account-index-transformer

silver_hot:            # source — participants are already normalized here
  host: ...
  database: silver_hot

index_cold:            # same Index Plane catalog + B2 bucket as tx_hash
  table_name: account_ledger_index
  partition_size: 100000

checkpoint:
  table: index.account_ledger_transformer_checkpoint

account_bucket_count: 256
```

Checkpoints are separate by construction — separate service, separate checkpoint table. A broken or
rebuilding account index cannot block tx hash indexing, and `index.transformer_checkpoint` (the
deployed tx_hash checkpoint) is never touched.

Incremental source is Silver hot, because account participant columns are already normalized there
(a different database than tx_hash's bronze hot source):

```text
silver.enriched_history_operations
silver.token_transfers_raw
silver.contract_invocations_raw
```

From hot, extract all participant fields:

- eho `source_account`
- eho `destination`
- eho `from_account`
- eho `to_address`
- eho `address`
- token transfer `from_account`
- token transfer `to_account`
- contract invocation `source_account`

For each processed ledger batch, write:

```text
DISTINCT account_id, account_bucket, ledger_range
```

Optional: set `batch_size = partition_size` (100k) so each cycle covers one ledger_range and emits
each pair at most once — eliminates cross-batch dups for completed ranges (the in-flight range still
re-emits until it closes, which is harmless).

## Backfill Mode

Yes, this can be backfilled.

The backfill must read cold Silver, not only hot Postgres, because complete history predates hot
retention.

Add a command or mode to the same `account-index-transformer` binary:

```text
account-index-transformer backfill \
  --start-ledger 1 \
  --end-ledger <handoff-or-latest-ledger> \
  --batch-ledgers 100000
```

Backfill source:

```text
cold Silver DuckLake
```

The backfill process attaches **two DuckLake catalogs** in one DuckDB instance — silver cold (read)
and the index catalog (write). The query-api already proves this dual-attach pattern.

Cold source tables and usable participant columns — this is **not** the same set as hot:

| Cold table | Usable columns | Not usable |
|---|---|---|
| `enriched_history_operations` | `source_account`, `destination` | `from_account`/`to_address`/`address` are typed INT32 and all-NULL in cold (backfill-era gotcha); comparing them to an account string errors |
| `token_transfers_raw` | `from_account`, `to_account` | — |
| `contract_invocations_raw` | `source_account` | — |

Coverage consequence: pre-handoff roles that only exist in the eho INT32 columns (path-payment
receivers via `from_account`/`to_address`, claimable-balance `address` roles) **cannot be indexed
from cold** until those columns are reprocessed. Deep-history coverage is therefore
source+destination plus token-transfer participants — not "complete". This limitation must flow
through to the coverage status the API exposes and Prism's labeling; it is a permanent caveat of
the current cold data, not a "still backfilling" state.

Backfill output (same skinny write path as incremental):

```text
index.account_ledger_index
```

Backfill should be resumable. Use a separate checkpoint:

```sql
CREATE TABLE IF NOT EXISTS index.account_ledger_backfill_checkpoint (
  id INTEGER PRIMARY KEY DEFAULT 1,
  last_ledger_sequence BIGINT NOT NULL,
  last_processed_at TIMESTAMP NOT NULL,
  status TEXT,
  error_text TEXT,
  CONSTRAINT single_account_ledger_backfill_checkpoint CHECK (id = 1)
);
```

The backfill can be heavy, but it is a one-time scan. It should run with conservative batch sizes and
resource limits so it does not starve query-api or streaming ingestion.

## Query-API Changes

Add an `AccountIndexReader` in `stellar-query-api`.

Lookup query:

```sql
SELECT DISTINCT ledger_range
FROM <catalog>.index.account_ledger_index
WHERE account_bucket = ?
  AND account_id = ?
ORDER BY ledger_range DESC
LIMIT ?
```

Cursor interaction: on page 2 and beyond, the lookup must also constrain
`AND ledger_range <= <cursor's ledger_range>` so deep pages pull the next batch of older ranges, not
the same newest ones. Only ranges at or below the hot/cold boundary matter for the cold arm — higher
ranges are the hot arm's job.

Then use those ranges in the cold account-history arm:

```sql
WHERE ledger_range IN (...)
  AND (
    source_account = ?
    OR destination = ?
  )
```

Important: the cold arm must keep the **per-schema participant filter** already shipped in
`account_history.go` (`buildAccountTransactionsQuery`) — cold eho's
`from_account`/`to_address`/`address` are INT32, and comparing them to an account string errors
(`Could not convert string to INT32`). The hot arm keeps all five participant columns. Do not
regress this when wiring in the range filter.

Fallback behavior — the meaning of an empty lookup depends on backfill coverage state, so the
reader needs a **coverage watermark** (the backfill checkpoint's indexed-range bounds) to interpret
it:

- Index disabled or missing (pre-rollout): hot/recent queries continue unchanged. Do NOT fall back
  to unbounded cold scans under Prism traffic — the cold arm needs a server-side statement timeout
  / context cancellation regardless of the index. Prism's 5s client fail-fast guards the UX today,
  but the abandoned DuckDB query keeps burning query-api memory server-side; that is the OOM
  pressure this bullet exists to prevent.
- Index present, empty lookup, backfill COMPLETE for the queried range: skip the cold arm entirely
  — the account provably does not appear in cold. This is a major win, not a degraded mode; most
  accounts have no deep history.
- Index present, empty lookup, backfill INCOMPLETE for the queried range: empty does not mean
  absent. Prune within the covered range; for the uncovered range, either run a capped/bounded scan
  or skip it and set the partial-coverage flag. Never silently return partial history as if it were
  complete.
- Prism shows indexing/backfilling status while coverage is incomplete, and the permanent
  deep-history coverage caveat (see Backfill Mode) after.

## Prism Changes

Once query-api can serve deep account history efficiently (complete within the documented coverage
limits):

- Point account activity pagination at `/api/v1/silver/accounts/{id}/transactions`.
- Show a bounded first page, then “Load more”.
- Use returned cursor from query-api.
- Keep mock data fallback for local/testing routes.
- Label partial coverage if the account index has not been fully backfilled.

Prism should not try to compensate by firing many transaction-detail or diff requests per account page.

## Rollout Plan

1. Add schema for `index.account_ledger_index` and checkpoint tables.
2. Scaffold `account-index-transformer` (clone + the deltas above; fix the cloned `RunCheckpoint`
   merge table name).
3. Implement incremental account indexing from Silver hot.
4. Deploy incremental indexing first, with account history query-api still using current behavior.
5. Add and test `AccountIndexReader` in query-api behind fallback behavior.
6. Run cold backfill as a controlled one-shot Nomad job.
7. Verify backfill row counts and lookup latency.
8. Enable account-index pruning in query-api.
9. Update Prism to use complete-history pagination.
10. Remove temporary low timeouts or partial-history copy only after complete-history latency is stable.

## Validation

Functional checks:

- Known historical account returns ledger ranges from `account_ledger_index`
  (e.g. the sparse `GB32463O…` account: ~887 sends over 27M ledgers, previously a 30s+ timeout).
- Account history includes pre-hot-retention operations.
- Pagination has no duplicate or missing transactions across pages.
- New accounts still work through hot/recent paths.
- Coverage status correctly reports the deep-history limitation (source+destination +
  token-transfer participants only), distinct from "backfill in progress".

Performance checks:

- Account index lookup is warm in tens of milliseconds; `EXPLAIN` confirms `account_bucket = N`
  actually prunes partitions.
- Cold account-history query includes `ledger_range IN (...)` and `EXPLAIN` shows file pruning.
- Deep account history page returns before gateway timeout.
- Query-api memory stays stable under Prism account-page load.

Operational checks:

- `tx_hash_index` checkpoint continues advancing, untouched — this work does not modify the
  existing `index-plane-transformer` at all.
- Account incremental checkpoint advances.
- Backfill checkpoint is resumable after restart.
- B2 object count and partition growth are acceptable; files-per-bucket stays bounded after a week
  of incremental writes (merge maintenance keeps up across 256 always-hot partitions).

## Risks

- Backfill is expensive and must be throttled.
- Bucket count is a rebuild-level decision.
- Participant extraction must be conservative; missing a participant role means incomplete account
  history. Cold backfill coverage is structurally limited by the eho INT32 columns (see Backfill
  Mode) — this is a data limitation, not an extraction bug, but it has the same symptom.
- Duplicate rows are acceptable for correctness but should be compacted to control storage.
- Partition fan-out: every incremental write touches up to 256 always-hot partitions. Small-file
  growth and merge pressure will look nothing like `tx_hash_index`; if maintenance can't keep up,
  lookup latency degrades.
- Query-api must not silently imply full coverage while backfill is incomplete — or after it
  completes, given the permanent cold coverage caveat.
- Clone-first duplicates the transformer scaffolding across three sibling services; divergence is
  the cost. Consolidating into one multi-index binary is future cool-down work (see Decision) —
  do not let the clones drift on shared concerns (DuckLake attach options, maintenance policy)
  in the meantime.

Escape hatch: if the bucket-pruned DuckLake lookup latency disappoints even after compaction, hold
this skinny index in Postgres instead, with a B-tree on `(account_bucket, account_id)` — at range
granularity it is small enough (~GBs) to live there cheaply. Decide on the measured number, not
upfront.

## Done Criteria

- `index.account_ledger_index` exists in the Index Plane and is populated for hot history and for
  cold history within the documented coverage limits (source+destination + token-transfer
  participants for the pre-handoff era).
- `account-index-transformer` maintains it incrementally; the running `index-plane-transformer`
  (`tx_hash_index`) is untouched.
- Query-api uses the index to prune cold account-history reads.
- Prism account pages can paginate deep account history without hammering Gateway/API.
- API responses expose coverage/partial status during and after rollout, including the permanent
  deep-history coverage caveat.
