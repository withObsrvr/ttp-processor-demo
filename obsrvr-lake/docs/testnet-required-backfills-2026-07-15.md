# Testnet Required Backfills and Live Handoff (2026-07-15)

## Scope

This rollout closes the two required historical-serving gaps identified during
the Horizon/account-balance investigation:

1. `serving.sv_effects_by_account` historical coverage and live maintenance.
2. Contract storage authoritative cold current state, serving current/summary
   state, and deletion-safe live maintenance.

Optional account balance history is not part of this rollout. Current account
and trustline balances already agree with Horizon testnet for the investigated
accounts; the missing requirement was complete effect history and correct
contract-storage state.

Smart-account historical state was also not part of this rollout. After the
July 15 reset/backfill sequence, the `smart_account_*` Silver hot tables only
contained the live tail and the serving smart-account projector rebuilt from
that incomplete source. The repair and repeatable mainnet process are captured
in [Smart Account Backfill and Serving Runbook (2026-07-16)](smart-account-backfill-serving-runbook-2026-07-16.md).

## Cutoff and Handoff Contract

The shared cold/live cutoff is ledger `3628006`.

- Cold backfills include ledgers through `3628006`.
- Live projectors start strictly after the serving watermark's
  `complete_thru` value.
- A projector checkpoint and both serving watermarks advance in the same
  PostgreSQL transaction as the corresponding writes/deletes.
- Contract-storage upserts and deletes are version guarded so an older event or
  tombstone cannot overwrite/delete a later restore.
- A live projector may advance an empty tail only through the committed Silver
  transformer checkpoint.

## Implemented Changes

### Effects

- Backfilled `serving.sv_effects_by_account` for `3541127..3628006`.
- Changed serving backfill watermark writes to extend only contiguous coverage
  rather than replacing an existing range.
- Prepared the checkpoint-aware `effects_by_account` live projector.

### Contract Storage

- Added append-only `contract_data_deletions` tombstones to Silver hot, the
  transformer, cold flusher, cold schema, and maintenance table inventory.
- Applied guarded current-row deletion in the same transformer transaction as
  each tombstone insert.
- Rebuilt `silver.contract_data_current` from the authoritative Bronze
  `contract_data_snapshot_v1` source rather than the nonexistent
  `silver.contract_data_changes` source.
- Added legacy-target migration and resumable bucket publication to the current
  state projector.
- Replaced the serving processor's destructive contract-storage rebuild with a
  checkpointed incremental projector that consumes current rows, TTL changes,
  tombstones, and evictions.
- Recomputes only summaries for affected contracts and advances current and
  summary watermarks transactionally.
- Changed the query API storage summary to read static totals from the
  materialized summary while recalculating TTL-sensitive counts against the
  current ledger.
- Added `(contract_id, live_until_ledger_seq)` to support large-contract dynamic
  expiry counts.
- Added a serving `key_hash` index for incremental TTL changes; a 2,308-change
  contract-storage batch then completed in about six seconds instead of taking
  minutes.

### Transformer continuity

- Reconciled `sequence_ledger` and `sequence_time` on legacy account current and
  snapshot tables during transformer startup.
- Added the pgx `exec` query mode for the Silver PgBouncer transaction-pool
  connection, avoiding named prepared-statement collisions.
- Confirmed repeated `accounts_snapshot` batches and full transformer
  checkpoints commit after deployment.

## Images

| Component | Image |
| --- | --- |
| Silver realtime transformer | `withobsrvr/silver-realtime-transformer:checkpoint-unblock-fd3b84b-20260715` |
| Silver cold flusher | `withobsrvr/silver-cold-flusher:contract-storage-tombstone-flush-fd3b84b-20260715` |
| Silver current-state projector | `withobsrvr/silver-current-state-projector:contract-storage-current-v4-fd3b84b-20260715` |
| Serving cold backfill | `withobsrvr/serving-cold-backfill:contiguous-backfill-fd3b84b-20260715` |
| Serving live projector | `withobsrvr/serving-projection-processor:backfill-live-handoff-v4-fd3b84b-20260715` |
| Query API | `withobsrvr/stellar-query-api:contract-storage-summary-v2-fd3b84b-20260715` |

## Completed Results

### Effects cold coverage

- Rows: `29,458,831`
- Coverage: `162..3628006`
- Rows added by catch-up: `585,282`

### Contract state cold coverage

- `silver.contract_data_current`: `6,172,483` rows as of ledger `3628006`
- `silver.ttl_current`: `7,120,269` rows as of ledger `3628006`
- Serving current handoff: `6,172,483` rows, maximum ledger `3628006`
- Serving summary handoff: `518,519` rows, maximum ledger `3628006`
- Duplicate serving `(contract_id, key_hash)` groups: `0`

### Query API large-contract check

Before the authoritative rebuild, contract
`CAESC7SC5EW5P2P3IM5Q7E64ZNDATVSN5F57NTCH5E7GJRPDM76KF7QM` had about 1.59
million serving storage rows. The prior summary query exceeded the interactive
timeout and returned `503`. The indexed, summary-assisted query completed in
about `0.3s` directly and the public endpoint returned `200` in `0.42s` after
the rebuild, while deriving `live_entries` and `expired_entries` from the
current ledger. The authoritative result contained `1,709,980` entries at the
time of the final smoke.

### Live handoff

- Effects and both contract-storage watermarks advanced beyond `3628006` and
  tracked the committed transformer checkpoint.
- In the final atomic database check, the transformer checkpoint, both live
  projector checkpoints, and both serving watermarks were exactly `3630381`
  (zero ledger lag).
- A serving row selected before live handoff was removed by its real
  post-cutoff tombstone at ledger `3628036`.
- The 43-entry contract
  `CAFLSFXZRGJNA37UFG55JWEJ33HAG5QENQZ44I6X7GF4GEBA7HOHHIFJ` returned all 43
  entries with `live_only=false` and zero with `live_only=true`; the data is
  present and every entry is expired at the current testnet ledger.
- Final public smokes returned `200`: health `0.25s`, large-contract live
  storage `0.12s`, large-contract summary `0.42s`, 43-entry unfiltered storage
  `0.17s`, live-only storage `0.10s`, and Horizon-compatible account effects
  `0.11s`.
- Final transformer and serving allocation logs contained no transformation or
  projector errors.

## Rollout Findings

The rollout surfaced and resolved five production-only integration gaps:

1. The current-state projector referenced nonexistent
   `silver.contract_data_changes`; Bronze snapshot history is authoritative.
2. The legacy cold current table lacked `network`; startup now migrates it and
   bucket publication replaces legacy `NULL` rows resumably.
3. PostgreSQL could not infer arithmetic bind types in the live TTL update;
   both operands now cast to `bigint` and the regression is tested.
4. TTL updates by `key_hash` needed a dedicated serving index. Concurrent index
   maintenance now explicitly verifies `indisvalid` after interrupted builds.
5. Account snapshot schema drift prevented the transformer checkpoint from
   advancing. Startup reconciliation and PgBouncer-safe pgx execution mode keep
   the live source and serving watermarks moving.

Follow-up finding:

6. Smart-account state is not restored by effects or contract-storage backfills.
   It requires `silver-realtime-transformer --smart-account-replay`, followed by
   a serving smart-account rebuild. Skipping that step can leave
   `/api/v1/silver/smart-accounts/*` with only live-tail state even though
   semantic smart-wallet classification still works.

## Tests

The following suites passed during the final rollout:

- `silver-realtime-transformer/go`: `go test ./...`
- `silver-cold-flusher/go`: `go test ./...`
- `silver-current-state-projector/go`: `GOWORK=off go test ./...`
- `serving-cold-backfill/go`: `GOWORK=off go test ./...`
- `serving-projection-processor/go`: `go test ./...`
- `stellar-query-api/go`: `go test ./...`
