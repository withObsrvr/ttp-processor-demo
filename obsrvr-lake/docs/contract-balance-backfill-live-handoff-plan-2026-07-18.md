# Contract-Held Balance Backfill and Live Handoff Plan (2026-07-18)

## Status

Testnet operational rollout and the historical Bronze enrichment repair are
complete. Migration 011 is applied, immutable images are pinned,
direct-to-cold history was backfilled through the original handoff ledger
`H=3,675,487`, and the historical repair subsequently rebuilt Bronze and
Silver through ledger `3,680,534`. Checkpoint-neutral replay, the as-of
projector, forced cold-flusher persistence, direct hot/cold queries, and the
public APIs all passed. A controlled on-chain deletion fixture and a known
non-7-decimal token remain a separate acceptance-hardening follow-up; natural
zero-state removal and production arbitrary-precision values passed.

## Outcome

Make balances owned by Soroban contract addresses (`C...`) complete and
replayable across the Obsrvr Lake hot and cold paths. This includes smart
accounts holding native XLM or SAC assets through canonical persistent
`Balance(Address)` contract storage.

The completed system must provide:

1. Correct realtime balances in `silver_hot.address_balances_current`.
2. A deletion-preserving `silver.contract_balance_changes` history from
   genesis through the live tail.
3. A correct `silver.address_balances_current` cold projection as of a chosen
   ledger and a deletion-safe way to maintain it after handoff.
4. A checkpoint-neutral replay that can restore hot current state after a
   reset without rewinding `realtime_transformer_checkpoint`.
5. Repeatable testnet Nomad jobs, verification, and rollback steps.

## Scope and Non-Goals

In scope:

- Persistent canonical SAC/SEP-41 `Balance(Address)` entries decoded by the
  Bronze ingester.
- Contract-address holders (`C...`), including the native XLM SAC.
- Positive state, zero state, deletion tombstones, metadata, and token
  decimals.
- Testnet backfill and live handoff, followed by a mainnet-ready process.

Not in scope:

- Arbitrary application-specific balance storage layouts.
- Contract-storage balances whose holder is a classic `G...` address; the
  current Bronze decoder intentionally extracts contract holders only.
- Replacing transfer-history APIs for contracts without canonical balance
  storage.
- Rebuilding unrelated Silver or Serving projections during the targeted
  acceptance cycle.

## Current Implementation Facts

The deployed testnet code does the following:

- `silver-realtime-transformer` reads decoded balance rows from Bronze hot or
  cold, joins the most recent contract-instance metadata available as of the
  batch end, normalizes native asset metadata, and retains zero/deleted rows.
- The hot writer appends idempotent rows to
  `contract_balance_changes`, upserts positive current rows, and removes
  `address_balances_current` rows on zero/deletion.
- Migration `011_add_contract_balance_changes.sql` and the embedded Silver hot
  schema create the hot change table and indexes.
- `silver-history-loader` produces a cold `contract_balance_changes` history
  with `network`, tombstones, source ledger bounds, and metadata.
- `silver-current-state-projector` unions classic `balance_changes` with
  `contract_balance_changes`, selects the latest owner/asset row, uses token
  decimals for display values, and excludes deleted/zero contract balances.
- The Query API reads `silver_hot.address_balances_current`. It required no
  route or response-schema change for the hot materialized path and was later
  rebuilt and deployed from the current source as part of rollout closeout.
- The deployed testnet Bronze ingester already contains the native-XLM
  `Balance(Address)` decoder.

Local tests pass for the four changed components. The final transformer and
flusher correction images are deployed, and the completed batch jobs used
immutable history-loader and projector images recorded below.

## Topology and Missing Pieces

| Concern | Current state | Required implementation |
| --- | --- | --- |
| Realtime hot state | Deployed and caught up | Keep the controlled positive/deletion fixture as repeatable acceptance coverage |
| Historical hot repair | Full replay completed without checkpoint movement | Preserve dispatch and allocation evidence |
| Cold historical backfill | Smoke and 37 production chunks completed | Reuse the sequential wrapper for future bounded repairs |
| Cold live history | Upgraded flusher deployed and advanced past `H` | Monitor normal scheduled cycles |
| Cold current backfill | Projected and independently reconciled | Re-run from a fresh manifest for future rebuilds |
| Cold current live deletes | Zero-state removal and no-stale-row gate passed | Add an explicit deleted tombstone fixture |
| Migration workflow | Migration 011 applied and indexes verified | Promote additively with the code |
| Verification | Database, checkpoint, Nomad, and API gates passed | Add durable controlled fixtures |

## Mainnet Orchestration Source

There is no standalone
`obsrvr-lake-mainnet/nomad/silver-history-loader.nomad` to copy. Mainnet
`scripts/13-chunked-silver-history-loader.sh` dynamically writes a temporary
parameterized Nomad job, registers it, and dispatches sequential ledger chunks.

The testnet implementation should adapt that wrapper and materialize its
embedded HCL as a reviewable testnet jobspec:

```text
obsrvr-lake-testnet/nomad/silver-history-loader.nomad
obsrvr-lake-testnet/scripts/13-chunked-silver-history-loader.sh
obsrvr-lake-testnet/scripts/14-register-contract-balance-jobs.sh
```

Those files now exist. The jobspec defaults to
`contract_balance_changes`, is parameterized by ledger range/chunk, and takes
its image and credentials as Nomad HCL variables. The wrapper requires an
explicit tested image tag, registers the static jobspec, dispatches chunks
sequentially, polls completion, and supports `--resume-from`.

Required testnet adaptations:

- `NETWORK=testnet` / `--network testnet`.
- Testnet catalog host, bucket, data path, and metadata schema.
- A new immutable `silver-history-loader` image containing
  `contract_balance_changes` and transform selection.
- Resource values based on current testnet node availability. Do not copy the
  mainnet `30 CPU / 98 GiB` allocation blindly while realtime services are
  running.
- HCL variables or environment templates for credentials. Do not write raw
  credentials into documentation, generated artifacts, or committed files.
- Fresh manifest identity for smoke and full runs.
- `restart.attempts = 0`, sequential dispatch, bounded chunks, completion
  polling, log capture, and `--resume-from` behavior from the mainnet wrapper.

## Recommended Design Decisions

### 1. Provide an explicit contract-balance replay mode

Recommended interface:

```text
--contract-balance-replay
--contract-balance-replay-start N
--contract-balance-replay-end N
--contract-balance-replay-batch-size N
--contract-balance-replay-cold=true
```

It should call `transformAddressBalancesFromContractState` in bounded
transactions and must not reset or advance
`realtime_transformer_checkpoint`. Keeping this separate from the existing
smart-account authorization replay makes the broader scope clear: every
canonical `C...` holder is processed, not only classified smart accounts.

If the lower-change implementation extends `--smart-account-replay` instead,
its help text, logs, tests, and runbook must explicitly state that it also
rebuilds all contract-held balances.

Replay tests must prove:

- checkpoint value and timestamp are unchanged;
- a historical positive row materializes current state;
- a later zero/deletion removes current state;
- re-running the same range does not duplicate history rows;
- an older replay row cannot overwrite newer live current state;
- metadata outside the balance-change window is still applied.

### 2. Add loader transform selection before the targeted backfill

The current loader executes every implemented Silver transform for every
chunk. Re-running all Silver history merely to create one new table increases
risk and runtime.

Add:

```text
--tables contract_balance_changes
```

Selection must affect execution, manifest completeness, `--resume`, `--verify`,
status output, and unknown-table validation. An empty value should preserve the
existing all-table behavior.

### 3. Make the cold live-history handoff network-aware

Adding `contract_balance_changes` only to `GetTablesToFlush()` is insufficient.
The history-loader table has a `network` column while the hot table does not.
The flusher's intersection-by-name behavior would otherwise omit it and write
`NULL`; the current projector filters on `network = 'testnet'` and would ignore
those rows.

Required flusher changes:

- Add a configured network label and validate it.
- Add `contract_balance_changes` to the cold schema, flush inventory, event
  watermark maps, deletion inventory, and maintenance inventory.
- Project the configured network as a computed cold column during flush.
- Preserve the `ledger_sequence` watermark and range-replacement transaction.
- Test schema intersection, network population, idempotent reflush, and hot-row
  deletion only after a successful cold commit.

### 4. Maintain cold current state from the change stream

`address_balances_current` is currently flushed as a positive current table.
When a hot current row is deleted, there is no source row for that flusher path
to copy, so the old cold row can remain indefinitely.

Recommended implementation: after flushing a ledger range of
`contract_balance_changes`, reconcile only affected contract keys in
`silver.address_balances_current` in the same or a following guarded DuckLake
transaction:

1. Select the newest change per `(network, owner_address, asset_key)` in the
   flushed range.
2. Delete those keys from cold current.
3. Reinsert only latest rows with `deleted=false` and `balance_raw > 0`.
4. Preserve classic account rows and contract rows newer than the change.

The alternative is a separately scheduled incremental current projector. Do
not claim live cold-current correctness until one of these consumers exists.

### 5. Treat the Silver flusher checkpoint as a handoff boundary

`silver-cold-flusher` has one shared `cold_flusher_checkpoint`. When a new table
is added after that checkpoint has advanced, the flusher will never
automatically read older rows for the new table.

For this rollout:

- Pause the Silver cold flusher and record its watermark `H`.
- Require Bronze cold coverage through `H`.
- Backfill `contract_balance_changes` from ledger `3` through `H` directly
  from Bronze cold.
- Deploy the upgraded flusher and resume it from the existing shared
  checkpoint so it owns `H+1` onward.
- Validate the first post-handoff range before deleting historical replay rows
  from Silver hot.

Longer term, table-specific flush checkpoints would remove this class of
add-a-table handoff risk.

## Implementation Work Packages

### WP1 — Replay and hot-state completion

Files:

- `silver-realtime-transformer/go/main.go`
- `silver-realtime-transformer/go/transformer.go`
- focused replay tests

Deliverables:

- checkpoint-neutral balance replay mode;
- progress and row-count logs;
- cancellation and bounded batching;
- replay idempotency and version-guard tests.

### WP2 — Targeted cold history loader

Files:

- `silver-history-loader/go/main.go`
- loader tests and README

Deliverables:

- `--tables` selection;
- selected-table-aware manifests, resume, and verify;
- full and targeted regression tests.

### WP3 — Live cold history and cold-current reconciliation

Files:

- `silver-cold-flusher/go/config.go`
- `silver-cold-flusher/go/duckdb.go`
- `silver-cold-flusher/go/flusher.go`
- `silver-cold-flusher/go/silver_schema.go`
- `silver-cold-flusher/go/silver_maintenance.go`
- flusher tests

Deliverables:

- network-aware `contract_balance_changes` flushing;
- ledger-watermark handling and retention cleanup;
- deletion-safe cold current reconciliation;
- first-table-handoff procedure at shared checkpoint `H`.

### WP4 — Testnet Nomad and operational scripts

Files under `obsrvr-lake-testnet`:

- new `nomad/silver-history-loader.nomad`;
- new `scripts/13-chunked-silver-history-loader.sh`;
- new balance-only current projector job or a safe parameterized variant;
- updated `scripts/08-silver-migrations.sh`;
- updated `scripts/10-verify.sh`;
- updated Silver transformer/flusher image pins.

Implemented jobs:

```text
nomad/silver-contract-balance-replay.nomad
nomad/silver-history-loader.nomad
nomad/silver-current-state-projector-balances.nomad
```

The service jobs are pinned to the final immutable correction images recorded
in the execution result below. The batch jobs require an explicit `image`
variable, so an operator cannot accidentally dispatch an old or floating
build.

Do not repurpose the existing
`silver-current-state-projector-contract-storage` batch job unchanged. It is
currently pinned to `contract_data_current,ttl_current` and has completed
manifest state for that work.

### WP5 — Acceptance and documentation

Deliverables:

- positive native-XLM contract-holder fixture;
- zero/deletion fixture;
- non-7-decimal token fixture if one exists;
- hot database, cold database, checkpoint, allocation-log, and public API
  assertions;
- rollback and cleanup record;
- final image tags and handoff ledger recorded in the rollout note.

## Testnet Rollout Procedure

### Phase 0 — Preflight and immutable builds

1. Commit the code changes and run tests/vet for transformer, history loader,
   current projector, and cold flusher.
2. Build and push immutable images. Do not deploy `latest` or a tag whose SHA
   excludes dirty changes.
3. Record before-state:
   - transformer checkpoint;
   - Silver cold flusher checkpoint;
   - Bronze cold maximum ledger;
   - row counts and maximum ledgers for hot/cold balance tables;
   - current API response for the selected contract fixture.
4. Confirm enough free Nomad memory/CPU for each batch job.

The three parameterized jobs can be registered with the same secret variables
used by the existing jobs. Use the exact immutable image tag for each
component:

```text
scripts/14-register-contract-balance-jobs.sh \
  --transformer-image "$TRANSFORMER_IMAGE" \
  --history-image "$HISTORY_LOADER_IMAGE" \
  --projector-image "$PROJECTOR_IMAGE"
```

Registration does not dispatch any batch work.

The history wrapper performs registration and dispatch:

```text
scripts/13-chunked-silver-history-loader.sh \
  --image "$HISTORY_LOADER_IMAGE" \
  --start 3 --end "$H" \
  --tables contract_balance_changes \
  --chunk-size 100000
```

### Phase 1 — Realtime positive and deletion acceptance

1. Apply migration 011 and verify the table/indexes.
2. Deploy only the new realtime transformer image.
3. Transfer a known amount through the native SAC to a disposable controlled
   `C...` address; record ledger `L1`.
4. Verify Bronze decoded the holder/value.
5. Verify a non-deleted hot change row and a materialized current row with:
   `asset_type=native`, `asset_code=XLM`, `symbol=XLM`, `decimals=7`, and
   `balance_source=contract_storage_state`.
6. Verify smart-wallet and unified-address balance endpoints report the
   materialized balance.
7. Drain the controlled contract balance; record ledger `L2`.
8. Verify the latest change is zero/deleted and the hot current row is absent.

### Phase 2 — Establish cold history through handoff `H`

1. Pause `silver-cold-flusher`; leave the realtime transformer running.
2. Read `H` from `cold_flusher_checkpoint.last_flushed_watermark`.
3. Verify Bronze cold maximum ledger is at least `H` and that Bronze has no
   relevant coverage gap through `H`.
4. Register the testnet parameterized history-loader job.
5. Smoke one known ledger chunk into an isolated test schema and verify
   metadata, tombstones, decimals, and row identity.
6. Run the targeted production-schema backfill:

   ```text
   --network testnet
   --start-ledger 3
   --end-ledger H
   --tables contract_balance_changes
   --resume
   ```

7. Verify manifest completeness for every dispatched chunk and query cold
   counts/max ledger/tombstones directly.

### Phase 3 — Restore hot historical current state

1. Run the checkpoint-neutral contract-balance replay from Bronze cold for
   `3..H`.
2. Verify the realtime transformer checkpoint is byte-for-byte unchanged by
   the replay.
3. Verify historical smart-account fixtures now materialize from
   `contract_storage_state` in the public API.
4. Verify newer live rows were not overwritten by older replay batches.
5. Do not clean historical hot change rows yet.

### Phase 4 — Build cold current as of `H`

1. Register a fresh projector job using the new image.
2. Run only `address_balances_current`, with end ledger `H` and a fresh
   manifest identity. Do not let `--resume` skip a result produced by an older
   image for the same network/range/projection.
3. Verify:
   - no duplicate `(network, owner_address, asset_key)` keys;
   - no rows newer than `H`;
   - no latest deleted/zero contract rows;
   - native and non-7-decimal display values;
   - contract fixture parity between hot current and cold current as of `H`.

### Phase 5 — Start live cold handoff

1. Deploy the upgraded Silver cold flusher with network-aware change-history
   support and cold-current reconciliation.
2. Resume the flusher from its unchanged shared checkpoint `H`.
3. Wait for it to flush through a new watermark greater than `H`.
4. Produce another positive/zero contract balance pair after `H`.
5. Verify both history rows reach cold with `network=testnet` and that cold
   current follows the latest state, including deletion.
6. Once cold history through `H` and the first live range are verified, remove
   historical hot `contract_balance_changes` rows at or below `H` through the
   normal retention path or a separately reviewed bounded cleanup. Do not
   reset the shared flusher checkpoint.

### Phase 6 — Closeout

1. Re-enable the normal Silver flush schedule.
2. Run the expanded verification script.
3. Capture image tags, handoff ledger, counts, fixture addresses, API results,
   and any deviations in a dated rollout result.
4. Keep the additive table and migration on rollback; roll back only image pins
   unless data verification shows a targeted repair is necessary.

## Required Acceptance Gates

### Data correctness

- Positive native XLM balance is materialized with raw/display parity.
- Token decimals come from instance metadata or registry and are not blindly
  fixed at seven.
- Zero/deleted latest state is retained in history and absent from current.
- Replays and reflushes are idempotent.
- Hot and cold current agree at the same as-of ledger for selected fixtures.

### Continuity

- Realtime transformer checkpoint never moves backward.
- Historical replay does not move the realtime checkpoint at all.
- Cold history is continuous across `H` with no overlap-related duplicates and
  no `H/H+1` gap.
- Cold flusher advances only after every configured table commits.

### Operational safety

- No raw credentials are added to tracked files or vault notes.
- Batch allocations fit current node headroom.
- Existing contract-storage and TTL projector manifests/jobs are not reused.
- No shared catalog schema is dropped for a smoke test.
- Rollback restores prior image pins without dropping additive tables.

## Verification Query Shapes

Hot history/current:

```sql
SELECT owner_address, asset_key, asset_type, asset_code, symbol, decimals,
       balance_raw, ledger_sequence, deleted
FROM contract_balance_changes
WHERE owner_address = '<C_ADDRESS>'
ORDER BY ledger_sequence DESC;

SELECT owner_address, asset_key, asset_type, asset_code, symbol, decimals,
       balance_raw, balance_display, balance_source, last_updated_ledger
FROM address_balances_current
WHERE owner_address = '<C_ADDRESS>';
```

Checkpoint invariance:

```sql
SELECT last_ledger_sequence, last_processed_at
FROM realtime_transformer_checkpoint
WHERE id = 1;
```

Cold correctness after attaching the testnet Silver DuckLake catalog:

```sql
SELECT network, owner_address, asset_key, ledger_sequence, balance_raw, deleted
FROM silver.contract_balance_changes
WHERE owner_address = '<C_ADDRESS>'
ORDER BY ledger_sequence DESC;

SELECT network, owner_address, asset_key, asset_type, balance_raw,
       balance_display, last_updated_ledger
FROM silver.address_balances_current
WHERE owner_address = '<C_ADDRESS>';
```

Duplicate gate:

```sql
SELECT network, owner_address, asset_key, COUNT(*)
FROM silver.address_balances_current
GROUP BY network, owner_address, asset_key
HAVING COUNT(*) > 1;
```

## Testnet Execution Result — 2026-07-18

### Immutable artifacts

| Component | Final image | Digest |
| --- | --- | --- |
| Realtime transformer | `withobsrvr/silver-realtime-transformer:contract-balances-nulfix-599f9cb-20260718204231` | `sha256:7ddab015c67e8c25cd06a61a90b0ee8b01e0127e130554ebef7f319a7cff5e61` |
| History loader | `withobsrvr/silver-history-loader:contract-balances-599f9cb-20260718194946` | `sha256:a59ace19215b1100e8f1dbe4a5bc9139b1b4fc5d371dcd3ae851d81d79df1d0c` |
| Current-state projector | `withobsrvr/silver-current-state-projector:contract-balances-projectorfix2-599f9cb-20260718210630` | `sha256:befb8fc9afa8557db3bcaf6865a91a622007629b499698c0241056d88684fde6` |
| Silver cold flusher | `withobsrvr/silver-cold-flusher:contract-balances-reconcilefix2-599f9cb-20260718215010` | `sha256:feeb5e1d6484f26485b19d01442cc84ff60894cf443c217db1b13121bd3c6576` |
| Query API | `withobsrvr/stellar-query-api:contract-balances-query-599f9cb-20260718222057` | `sha256:4d7d5456309832b481ea484706f2b41ba24c98b45c1f046066f27b8dce98cc60` |

The image suffix records the source commit available at build time. The images
also contain the documented working-tree changes; their registry digests are
the immutable rollout identity.

### Handoff and batch execution

- Migration 011 was applied to `silver_hot`; the table and its three indexes
  were verified before replay.
- The Silver flusher was paused at `H=3,675,487`. Bronze cold coverage was
  ahead of `H` before the production backfill began.
- An isolated-schema history smoke produced 2,976 rows. The production
  `3..H` backfill then completed in 37 sequential chunks. Independent
  verification returned 780,366 rows with range `3..3,675,487` and a complete
  loader manifest checksum.
- The replay smoke processed 593 source rows. The full checkpoint-neutral
  replay processed 231,730 rows in about two minutes and left the realtime
  checkpoint value and timestamp unchanged.
- The balance-only projector completed all 37 source windows and published
  212,883 current rows as of `H`. The legacy target was evolved additively and
  the job used one publish bucket to avoid rescanning 5.7 million legacy rows
  four times.
- A bounded repair loaded `3,675,488..3,678,552`, and a fresh projector run
  rebuilt 212,926 current rows through the then-committed live watermark
  `3,678,564`.
- The final corrected flusher committed `3,678,565..3,678,848`, archived 47
  contract history rows, reconciled 9 affected contract-current keys, and
  advanced its checkpoint only after the DuckLake transaction committed.

### Runtime corrections discovered during rollout

The staged rollout caught four production-only compatibility issues without
moving a checkpoint past failed work:

1. Decoded optional token metadata could contain embedded NUL bytes, which
   PostgreSQL text rejects. The writer now sanitizes optional display metadata
   at the PostgreSQL boundary while validating identity and numeric fields.
2. The existing cold projection lacked the optional legacy `balance_changes`
   table and some current-target columns. The projector now detects the
   optional source and evolves the legacy target idempotently.
3. DuckDB inferred PostgreSQL unbounded `NUMERIC` balances as floating point,
   producing decimal suffixes or scientific notation. The flusher now casts
   `balance_raw` to text inside PostgreSQL before DuckDB inference, preserving
   values such as `13371215174167948914` exactly.
4. Correlated DuckLake delete/insert statements did not reliably reconcile
   current state. A local affected/latest staging table now drives both DML
   operations in the same guarded transaction.

### Final acceptance evidence

- Realtime transformer Nomad allocation
  `227c8d12-2de5-c6da-1f50-fb25c6981b81` is running and healthy on job version
  12. After replay it caught up to Bronze contract ledger `3,678,899` exactly.
- Silver cold flusher allocation
  `4b416da6-da43-af1f-2827-730d97fbc964` is running and healthy on job version
  18. Its verified checkpoint is `3,678,848`; the remaining hot tail is the
  expected input to the next scheduled flush, not a continuity gap.
- Query API allocation `cb9581db-1615-9456-c45e-ffd88d7519a9` is running and
  healthy on job version 92 with zero restarts. All optional indexes reported
  `ready` after startup.
- Final cold verification returned 780,986 history rows, including 620 rows
  after `H`, and 213,348 contract current rows through ledger `3,678,848`.
  Duplicate current-key groups, positive-state mismatches, and stale
  zero/deleted current rows were all zero.
- The public health endpoint returned HTTP 200 with Bronze, Silver, hot, cold,
  and index layers healthy. A positive contract holder returned three
  storage-state balances, while a naturally zeroed holder returned no current
  balances. The bounded unified contract-history request also returned HTTP
  200 across the hot/cold query path; that endpoint remains transfer-derived,
  separate from the new storage-state history table.
- Component tests pass for transformer, history loader, projector, and
  flusher; flusher `go vet`, operational `bash -n`, and `git diff --check` pass.

### Acceptance follow-up

No natural `deleted=true` contract tombstone occurred in the observed cold
range. Natural zero changes exercised the same current-removal path and the
independent stale-row gate passed. Before promoting the process as a durable
mainnet acceptance runbook, execute a controlled positive/deletion pair and a
known non-7-decimal token fixture on testnet and attach those ledger-specific
results to the rollout artifact.

## Historical Bronze Enrichment Repair — 2026-07-19

### Cause and implementation

The live Bronze ingester could decode canonical SAC `Balance(Address)` keys,
but older rows in `bronze.contract_data_snapshot_v1` had already reached
DuckLake with `balance_holder` and `balance` unset. Replaying Silver could not
recover information that was absent from its Bronze source, so the known smart
account remained empty even though Stellar Expert showed its native SAC
storage balance.

The repair added a resumable, idempotent Bronze enrichment mode to
`stellar-history-loader`. It scans bounded ledger chunks, decodes only vector
storage keys whose enrichment columns are missing, stages decoded rows, updates
the exact `(contract_id, ledger_sequence, ledger_key_hash)` identities, and
verifies every write in the same transaction. A DuckLake manifest records each
completed chunk and stable run identity; dry-run and rerun modes make the
operation auditable. The same XDR decoder is shared with normal history
extraction.

Testnet now has a reviewable parameterized Nomad job and registration script:

```text
nomad/bronze-contract-balance-enricher.nomad
scripts/15-register-bronze-contract-balance-enricher.sh
```

The Silver history job also gained an explicit `resume` variable and its
wrapper gained `--no-resume`, allowing a repaired source range to be rebuilt
even when an older manifest says that range previously completed.

### Executed repair

- Bronze repair image:
  `withobsrvr/stellar-history-loader:bronze-balance-enrichment-599f9cb-20260719001345`,
  digest
  `sha256:0b607ae25fb00392bebb5a226171b6b7f1409bff7ea4d0aabf81600bd7f9b72d`.
- Dry run over `3..3,680,534`: 11,078,638 vector-key candidates,
  499,642 decoded balances, 10,578,996 non-balance vectors, and zero decode
  failures.
- Canary dispatch `bronze-contract-balance-enricher/dispatch-1784423628-af1fb379`
  repaired the known ledger `2,996,166` row.
- Full dispatch `bronze-contract-balance-enricher/dispatch-1784423891-22b8e81d`
  completed 37 chunks with run ID
  `contract-balance-enrichment-full-20260719`. It repaired the remaining
  499,641 rows.
- Post-verification dispatch
  `bronze-contract-balance-enricher/dispatch-1784425102-046490a7` found zero
  remaining decodable rows, proving idempotent completion.
- The Silver history loader rebuilt all 37 chunks over `3..3,680,534` with
  `resume=false` in dispatch
  `silver-history-loader/dispatch-1784425778-54ad389d`.
- The balance-only projector completed through the Silver flusher boundary in
  dispatch
  `silver-current-state-projector-balances/dispatch-1784427893-b93697bf`.
- Final checkpoint-neutral replay
  `silver-contract-balance-replay/dispatch-1784463357-0a5b06f5` processed
  246,151 rows in 2m37s without rewinding the live transformer checkpoint.

### Serving continuity correction

The first replay exposed a separate lifecycle defect: the Silver cold flusher
archived and then deleted `address_balances_current` from PostgreSQL. That table
is not disposable history; it is the bounded latest-per-key table used by the
Query API. The replayed balance therefore appeared and then vanished after a
successful flush.

The flusher now retains `address_balances_current` in hot PostgreSQL while
continuing to delete append-only flushed history. Regression coverage proves
this table is the retention exception. The deployed image is
`withobsrvr/silver-cold-flusher:contract-balances-retain-hot-599f9cb-20260719121242`,
digest
`sha256:cd0603dd7c864be0fdf4926a37cb1f8f44191742634ccfe2dacb80b3e96a31eb`.
A forced real cycle committed through watermark `3,689,268` and logged that
the current-state serving table was retained.

### Closure evidence

For holder
`CCQBQIAG2E2L5NOIML2SGAJYMXPID3MAQNII5USMENID3SDJ4ATOU2HG` and native SAC
`CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC`:

- Bronze cold contains the enriched ledger `2,996,166` row with key hash
  `d2ccddb78a822eb99301d8ddf17861a387392655834cf957b708aba18eee4241`
  and raw balance `99950000000`.
- Silver cold `contract_balance_changes` contains the same non-deleted history
  row with `balance_source=contract_storage_state`.
- Silver cold and hot `address_balances_current` both contain the corresponding
  current row. Hot display is `9995.0000000000000000`; cold display is
  numerically equivalent `9995.0`.
- After the forced flusher cycle, the live transformer checkpoint was
  `3,689,307` and the Silver flusher checkpoint was `3,689,268`; neither moved
  backward.
- `GET /api/v1/silver/addresses/{address}/balances` returned HTTP 200 with one
  native XLM balance, raw `99950000000`, display
  `9995.0000000000000000`, and source `contract_storage_state`.
- `GET /api/v1/silver/smart-wallets/{contract_id}/balances` returned HTTP 200,
  `native_balance=9995.0000000000000000`, `partial=false`, and
  `balance_status=materialized`.
- Bronze flusher, Silver realtime transformer, Silver cold flusher, and Query
  API Nomad jobs were all running after closure.
- Tests pass for the Bronze history loader, Silver history loader, realtime
  transformer, current-state projector, cold flusher, and Query API. `go vet`
  passes for all five changed pipeline components, and `git diff --check`
  passes.

This closes the historical Bronze enrichment repair. The explicit deletion
and non-7-decimal fixture work remains intentionally separate.

## Rollback

1. Re-pin the previous transformer and Silver cold flusher images and resubmit
   their Nomad jobs.
2. Stop any new parameterized batch job dispatches.
3. Leave migration 011 and `contract_balance_changes` in place; they are
   additive and an older transformer will ignore them.
4. Do not move either checkpoint backward as a rollback mechanism.
5. Preserve failed-run manifests and logs for diagnosis.
6. If a cold current projection is wrong, rerun the prior known-good projector
   as of its recorded ledger rather than dropping the shared Silver schema.

## Open Questions

- Is a table-specific flusher checkpoint worth implementing before adding the
  next live-flushed table, or should it remain a follow-up?
- What testnet non-7-decimal token and controlled disposable smart account will
  be the durable acceptance fixtures?

## Definition of Done

The operational rollout has evidence for all five runtime paths. The remaining
fixture-hardening loop is complete when the explicit deletion and
non-7-decimal cases supplement the natural production observations:

1. New realtime contract balance appears and disappears correctly in hot/API.
2. Historical checkpoint-neutral replay restores known contract holders.
3. Direct-to-cold history is complete through handoff ledger `H`.
4. Cold current projection is correct as of `H`.
5. Post-`H` changes reach cold history and update cold current without a
   checkpoint gap; an explicit deleted tombstone is retained as follow-up
   acceptance coverage.
