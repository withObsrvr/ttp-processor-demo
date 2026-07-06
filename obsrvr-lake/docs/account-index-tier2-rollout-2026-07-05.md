# Account Index Plane Tier 2 — Rollout Cycle (Shaped)

_Written 2026-07-05. Tier 2 of the query-api improvement sequence: make the account index plane
real in production. The code scaffold landed in `16f239a` (account-index-transformer service +
query-api reader wired dark). Companion docs: `account-index-plane-implementation-plan.md` (design),
`query-api-tier1-quick-wins-2026-07-05.md` (Tier 1). Review findings from the `16f239a` check are
folded in here — notably the partition-size hardcode and the missing coverage watermark._

## Shape

**Problem:** The account index exists as committed code but not as production data. Deep/sparse
account history still times out (bounded to a 503 by Tier 1, but not answered). The pruning wiring
ships dark behind `ACCOUNT_LEDGER_INDEX_ENABLE_PRUNING`, and two review findings make flipping that
flag unsafe today: the query-api reader hardcodes the partition size the writer treats as
configurable, and there is no coverage watermark — enabling pruning mid-backfill would silently
drop cold history for accounts with activity in un-indexed ranges.

**Appetite:** 1 week of active work. The backfill's wall-clock runtime is not part of the appetite
— it runs in the background with a resumable checkpoint; the appetite covers writing, deploying,
and verifying. Ship-or-kill applies to the code and deploy; the flag flip is gated on verification,
not on the calendar — if backfill isn't verified complete by week's end, everything else ships and
the flip happens when verification passes.

**Solution (fat marker):** Land two small reader fixes (partition size from config, coverage
watermark with skip/prune/partial semantics). Build and deploy the incremental transformer to
Nomad. Run the cold backfill as a throttled one-shot batch job. Verify counts, pruning, and
latency against the known sparse account. Then — and only then — flip the pruning flag and record
the latency-audit delta.

**Rabbit holes (do not enter):**

- Do not redesign the checkpoint schema for the watermark — read the two checkpoint tables that
  already exist (`index.account_ledger_backfill_checkpoint`,
  `index.account_ledger_transformer_checkpoint`).
- Do not chase coverage perfection for the permanently-unindexable roles (cold eho INT32 columns).
  That caveat is documented in the plan doc; the watermark models *ledger-span* coverage only.
- Do not build Prism pagination in this cycle (plan rollout step 9 — its own small cycle once
  latency is proven).
- Do not optimize the lookup's cursor narrowing / `IN (...)` list size. Harmless until measured
  otherwise.
- Do not consolidate the three index transformers (future cool-down, per the plan doc).
- If backfill B2 transaction costs spike, pause the job and adjust batch size / flush cadence —
  do not redesign the write path mid-cycle.

**No-gos:** Prism-side changes, tx_hash/contract index repartitioning (Tier 4), serving tables
(Tier 3), multi-index binary consolidation, any modification to the running
`index-plane-transformer`.

**Done (concrete):** The sparse account `GB32463O…` (~887 sends over 27M ledgers behind ~220
recent receives) returns its full paginated history through the gateway in <1s per page, including
pre-hot-retention sends. Pruning is enabled in production behind the watermark guard. The
`tx_hash_index` checkpoint never stopped advancing. A post-Tier-2 delta section exists in the
latency audit doc.

## Scope line

```text
COULD HAVE ─────────────  C1. lookup cursor narrowing (ORDER BY DESC + range cursor)
                          C2. automated files-per-bucket probe/alert
NICE TO HAVE ───────────  N1. backfill --start-ledger honored / --force-restart
                          N2. writer SetMaxOpenConns(1)
                          N3. nits: config.yaml rebuild warning, dead catalogDB param,
                              unify 503/504 timeout responses
MUST HAVE ══════════════  M1. partition size single source of truth
                          M2. coverage watermark + skip/prune/partial semantics
                          M3. image build + push
                          M4. Nomad deploy: incremental transformer
                          M5. Nomad batch job + run cold backfill
                          M6. verification suite
                          M7. gated flag flip
                          M8. close the loop (audit delta)
```

## Checklist

### M1 — Partition size single source of truth

- [ ] `account_ledger_index_reader.go:113,117` hardcode `/100000`; the writer derives
      `ledger_range` from configurable `partition_size` (`config.go:149-151`). Replace the
      literals with a value read from env `ACCOUNT_LEDGER_INDEX_PARTITION_SIZE` (default 100000).
- [ ] Cross-referencing comments on BOTH sides (reader env + transformer `config.yaml`): these two
      values must never diverge; divergence silently prunes the wrong ranges.
- [ ] Unit test: reader range computation matches writer's for a non-default partition size.

### M2 — Coverage watermark

The gate for M7. Semantics come from the plan doc's "Fallback behavior" section.

- [ ] Read both checkpoint tables from the catalog Postgres (plain PG tables in the `index`
      schema): backfill (`last_ledger_sequence`, `status`) and incremental
      (`index.account_ledger_transformer_checkpoint`). Implementation choice: direct PG connection
      or through the existing DuckLake catalog attach — pick whichever is already plumbed, don't
      build new infrastructure.
- [ ] Compute covered span: backfill complete (status done, end meets the incremental start) →
      coverage = [1, incremental checkpoint]; otherwise coverage = the union that's actually
      indexed, with a floor where backfill has reached.
- [ ] Cache the watermark in-process with a short TTL (~30-60s) — no per-request PG roundtrip.
- [ ] Apply the three-state semantics in `account_history.go`:
      - empty lookup + coverage complete for the queried span → **skip the cold arm entirely**;
      - lookup hits → `ledger_range IN (...)` within the covered span;
      - uncovered span → unpruned but timeout-bounded, response flagged partial.
- [ ] Expose coverage state in the account-history response metadata (plan done-criteria).
- [ ] Unit tests for all three states, including the mid-backfill account-with-split-activity
      case (the silent-drop trap from the review).

### N1–N3 — Small fixes (bundle into the same PR as M1/M2)

- [ ] N1: `backfill.go:51-53` silently overrides a user-supplied `--start-ledger` behind the
      checkpoint. Honor it explicitly or add `--force-restart`; log loudly either way.
- [ ] N2: `index_writer.go:23-25` — `SetMaxOpenConns(1)` so the TEMP-table sequence is
      deterministically same-connection.
- [ ] N3: add "changing `account_bucket_count` requires a full index rebuild" to `config.yaml`;
      remove the unused `catalogDB` param from `NewIndexWriter`; make timeout responses
      consistently 503 (`account_history.go` currently returns 504).

### M3 — Image build + push

- [ ] Build from SERVICE DIR (self-contained go.mod):
      `cd obsrvr-lake/account-index-transformer && docker build .`
- [ ] Tag/push `withobsrvr/account-index-transformer:latest` (org must be `withobsrvr/`).

### M4 — Nomad deploy: incremental transformer

- [ ] Job spec in the infra repo (`environments/prod/latitude/obsrvr-lake-testnet/nomad/…`),
      always-on service job, `force_pull: true`, health port per `config.yaml`, secrets for
      silver_hot PG, catalog PG, B2 index bucket.
- [ ] Deploy; verify within a day:
      - `index.account_ledger_transformer_checkpoint` advancing;
      - `index.transformer_checkpoint` (tx_hash) untouched and still advancing;
      - health endpoint green, advisory lock held;
      - files-per-bucket and catalog inlined-data size sane (first real read on the
        256-always-hot-partitions fan-out risk).

### M5 — Nomad batch job + run cold backfill

- [ ] One-shot batch job (precedent: `silver-cold-replay.nomad`), `--batch-ledgers 100000`,
      conservative resources so it does not starve query-api or streaming ingestion.
- [ ] Run ledger 1 → handoff. Resumable via `index.account_ledger_backfill_checkpoint`; kill and
      restart is safe (dup-tolerant writes).
- [ ] Monitor while running: B2 class-C transaction counts, backfill checkpoint progress, catalog
      PG size. Pause + adjust batch size if costs spike (see rabbit holes).

### M6 — Verification suite (from the plan doc's Validation section)

- [ ] `GB32463O…` returns ledger ranges from `account_ledger_index`; deep history includes
      pre-hot-retention sends.
- [ ] Row count in the range-granularity ballpark (~200M order of magnitude, NOT billions — if
      it's billions, something is writing per-ledger rows).
- [ ] `EXPLAIN` on the lookup shows `account_bucket = N` partition pruning; warm lookup in tens
      of ms.
- [ ] Pagination across pages: no duplicate, no missing transactions (hot/cold boundary overlap
      dedup works).
- [ ] Backfill checkpoint survives a forced restart mid-run.

### M7 — Gated flag flip

- [ ] Preconditions (both, non-negotiable): M2 watermark deployed; M6 confirms backfill complete.
- [ ] Set `ACCOUNT_LEDGER_INDEX_ENABLE_PRUNING=true` in the query-api Nomad job; resubmit with
      `force_deploy` meta bump.
- [ ] Watch query-api memory + latency for a day under Prism traffic.

### M8 — Close the loop

- [ ] Re-run `scripts/latency_audit.py`; append a "post-Tier-2" delta section to the audit doc —
      account endpoints (`accounts/history`, `accounts/{id}/transactions`, `explorer/account`)
      should move from error/critical to fast/ok.
- [ ] Update the memory/plan docs: index plane is live; note the permanent cold coverage caveat
      is now user-visible via response metadata.
- [ ] Kick Prism pagination (plan step 9) to its own next cycle.

## Dependencies and sequencing

```text
M1, M2, N1-N3 (one PR)  ──┐
                          ├──► M7 flag flip ──► M8
M3 ─► M4 (incremental) ───┤
      M5 (backfill) ──► M6┘
```

- M3–M5 do not depend on M1/M2 — the pruning code ships dark either way. Start the transformer
  deploy and backfill immediately; write the watermark while it runs.
- M7 is the only hard gate: it waits on BOTH the M2 deploy and M6 verification.
- Hill: M2's semantics are the only left-side (figuring-out) work; everything else is right-side
  execution. If M2 is still unsettled at mid-week, cut the "skip cold arm" optimization and ship
  watermark-as-guard only (prune covered span, partial-flag the rest) — the safety property, not
  the speedup, is the must-have.

## Rollout status — 2026-07-05 23:15 UTC

Current state:

- M1/M2/N1-N3 code is implemented locally and deployed dark to testnet in
  `withobsrvr/stellar-query-api:16f239a-20260705224930`.
- Testnet query-api Nomad job explicitly sets:
  - `ACCOUNT_LEDGER_INDEX_PARTITION_SIZE=100000`
  - `ACCOUNT_LEDGER_INDEX_ENABLE_PRUNING=false`
- Query-api startup confirms the account ledger index attaches with 3,645,864 rows and logs
  `Account ledger index attached, but pruning is disabled`.
- A bounded account transaction request confirms response metadata is live:
  `coverage.account_index.enabled=true`, `pruning_enabled=false`, `status=not_used`.
- Account-index incremental transformer is deployed on
  `withobsrvr/account-index-transformer:16f239a-20260705224306`.
- Account-index backfill completed through ledger 3,449,201:
  `index.account_ledger_backfill_checkpoint` = `complete`.
- Account-index incremental checkpoint advanced to 3,455,408 as of 2026-07-05 23:00:28 UTC.

Gate status:

- M7 flag flip is **not safe yet**.
- The existing `index-plane-transformer` (`tx_hash_index`) was found running with a stale
  checkpoint:
  `index.transformer_checkpoint.last_ledger_sequence=3451786`.
- Logs before restart showed repeated DuckDB writer failures:
  `database has been invalidated because of a previous fatal error`, original error
  `Attempted to access index 0 within vector of size 0`.
- A task-only Nomad restart was attempted at 2026-07-05 23:10 UTC without changing the job spec or
  image. The transformer reattached cleanly, inserted 1,420 transactions, and advanced
  `index.transformer_checkpoint.last_ledger_sequence` to 3,455,535. This satisfies the immediate
  "not stopped" gate, but it should be watched because the same writer uses the temp-table pattern
  that poisoned the prior DuckDB process.

Next action:

- Continue M6 verification before enabling account-index pruning: verify account-index lookup
  pruning/latency, pagination correctness, and post-restart tx-hash checkpoint movement under at
  least one more live ledger cycle. Do not flip `ACCOUNT_LEDGER_INDEX_ENABLE_PRUNING=true` until
  those checks pass.

## Rollout status — 2026-07-05 23:40 UTC

Changes deployed to testnet:

- Query API pruning was enabled and deployed through Nomad.
- Latest testnet Query API image:
  `withobsrvr/stellar-query-api:16f239a-20260705233150`
  (`sha256:0947ce0657a4d995a8c88f3ca85bd3e52f27dc9922cc6bb8fa44e967d9c1ac83`).
- Nomad job version 35 deployed successfully with:
  - `ACCOUNT_LEDGER_INDEX_ENABLE_PRUNING=true`
  - `ACCOUNT_LEDGER_INDEX_PARTITION_SIZE=100000`
  - `ACCOUNT_LEDGER_INDEX_LOOKUP_TIMEOUT=1500ms`
- Startup confirms:
  - `Account ledger index reader warmed up (account_ledger_index: 3648199 rows)`
  - `Account Ledger Index attached for account history pruning`

Credential note:

- The testnet `.secrets` file contains Nomad, DockerHub, DigitalOcean DB, and API-key variables.
- It does **not** contain the B2/index-catalog credentials used by these Nomad specs.
- The testnet Nomad specs currently carry hardcoded local catalog/B2 values. They are valid in the
  deployed environment because the transformer and Query API attach/write successfully, but they
  should be moved into a real secret source before promoting this pattern.

Verification results:

- Query API `/health`: HTTP 200, `reader_mode=unified`.
- Account snapshots/history for
  `GCFOH4PUYAXJH75SLBXT7NZWOT2JWXGCBI6YF3RXV6LXUHJCCKA4HH4I`:
  - warmed lookup: HTTP 200 in ~0.48-0.79s
  - `coverage.account_index.status=complete`
  - `lookup_failed=false`
  - `pruned_ranges=[34]`
- First/cold account-index lookup can still exceed the 1500ms lookup budget. The API now returns
  HTTP 200 with `partial=true`, `lookup_failed=true`, and skips the cold scan instead of falling
  back to an unpruned timeout path.

Remaining gap:

- `accounts/{id}/transactions` is not fully solved by `account_id -> 100k-ledger_range` pruning.
- A one-ledger window returns HTTP 200 in ~2.1s, but a 2k-ledger window can still hit the
  interactive timeout when the account index successfully returns `pruned_ranges=[34]`.
- Diagnosis: the current account index is enough to route snapshot/history queries, but it is too
  coarse for transaction activity. Transaction history needs a more selective serving shape:
  `account_id -> exact ledger_sequence/transaction_hash/source_table` or a materialized
  `account_transactions` serving table.

Updated gate status:

- M7 pruning flag flip is complete for testnet.
- M8 is **not complete**. Do not call account transaction history solved until the more selective
  account activity index or serving projection exists and latency audit confirms it.

## Follow-on slice — Postgres index-plane relocation

Implemented locally as a dark mirror sink in `account-index-transformer`:

- `index_postgres.enabled=false` by default, with `mode=mirror`.
- Writes to Postgres `index.account_ledger_index` using the existing catalog Postgres connection.
- Converts each `ledger_range` into explicit `ledger_from` / `ledger_to` bounds using the same
  configured `index_cold.partition_size`.
- Upserts are idempotent on `(account_id, ledger_range)` and write a sink-specific checkpoint to
  `index.account_ledger_postgres_checkpoint`.
- Mirror-mode failures are logged and do not block the existing DuckLake account index checkpoint.
- Cold backfill also writes the Postgres mirror when `index_postgres.enabled=true`; unlike the
  incremental mirror, backfill treats Postgres write failures as fatal so its checkpoint cannot
  advance past missing Postgres index rows.
- If DuckLake backfill completed before the Postgres mirror existed, the backfill must be rerun
  with `-postgres-only=true -force-restart=true` from ledger 1/3. Otherwise Postgres only contains incremental ranges
  from the point the mirror was enabled.

Implemented locally as a Query API dark read option:

- `ACCOUNT_LEDGER_INDEX_SOURCE=ducklake` remains the default.
- `ACCOUNT_LEDGER_INDEX_SOURCE=postgres` skips DuckLake/S3 attach for the account ledger index and
  reads `index.account_ledger_index` directly from the catalog Postgres connection.
- Postgres-source coverage metadata comes from `index.account_ledger_postgres_checkpoint` plus the
  mirror table's `MIN(ledger_from)` / `MAX(ledger_to)`. It is only complete when the mirror starts
  at ledger 1 and reaches the checkpoint.
- Tests cover source parsing, the Postgres lookup query shape, and partial-vs-complete Postgres
  mirror coverage.
- Parity harness added:

```bash
python3 obsrvr-lake/stellar-query-api/scripts/account_index_source_diff.py \
  --ducklake-url https://<query-api-ducklake-source> \
  --postgres-url https://<query-api-postgres-source> \
  --account GB32463O... \
  --account GCFOH4PUYAXJH75SLBXT7NZWOT2JWXGCBI6YF3RXV6LXUHJCCKA4HH4I \
  --start-ledger 1 \
  --end-ledger 3500000
```

This is intentionally not a production cutover yet. The next relocation slice is parity testing:
run the same account panels against `ACCOUNT_LEDGER_INDEX_SOURCE=ducklake` and `postgres`, compare
returned range sets, then deploy testnet with the Postgres source only after the mirror checkpoint
has caught up.

Testnet Nomad preparation:

- `account-index-transformer.nomad`: template includes `index_postgres.enabled=true`.
- `account-index-transformer-backfill.nomad`: template includes `index_postgres.enabled=true`.
- `stellar-query-api.nomad`: explicitly pins `ACCOUNT_LEDGER_INDEX_SOURCE=ducklake`.
- All three specs validate with `nomad job validate`; driver validation was skipped because no
  local Nomad agent connection was available.

Testnet deployment note from 2026-07-06:

- Incremental Postgres mirroring is healthy and writes each new batch to
  `index.account_ledger_index`.
- Current Postgres mirror coverage is partial (`MIN(ledger_from)=3400000`), so it is not safe to
  globally cut Query API over to `ACCOUNT_LEDGER_INDEX_SOURCE=postgres` yet.
- Query API remains pinned to `ducklake` until the Postgres mirror backfill has been force-rerun
  and verified from ledger 1/3 through the hot/cold handoff. This mode must not touch the DuckLake account-index checkpoint while Query API remains pinned to `ducklake`.
