# Testnet Rollout Follow-Up Cycles - 2026-07-09

## Context

The testnet Horizon compatibility rollout is mostly unblocked after the Bronze recovery:

- Historical Bronze `3..3513746` was loaded to DuckLake, except `contract_events_stream_v1`.
- Live ingest resumed from ledger `3513747`.
- Bronze flusher caught up through `3525079` during verification, and live flushing is running again.
- Public query health and DeFi endpoints are healthy.
- Horizon compatibility transaction lookup works on hot data.

Two important gaps remain:

- Historical `contract_events_stream_v1` was intentionally skipped because DuckLake rejected one Parquet shard with invalid UTF-8.
- Cold Horizon transaction lookup by hash can still full-scan cold Bronze and time out unless the query is constrained by ledger.

This document defines the next implementation cycles. The goal is to make the rollout durable, restore historical contract events, and continue Horizon parity without another full historical replay of every table.

## Closeout Status - 2026-07-10

Cycles 1-4 have been implemented and deployed to testnet.

Deployed images:

- `withobsrvr/stellar-query-api:cycle4-serving-20260710114316`
  - digest: `sha256:260d9a2123007519a5372bf54bcbcb6f5bc24f02d164c3c232f59258bf5c742b`
- `withobsrvr/serving-cold-backfill:cycle4-serving-20260710114316`
  - digest: `sha256:63091538545e20c0fb2662f0a37b520ef4b8882ad024df0168c36a75b13a9a67`
- `withobsrvr/serving-projection-processor:cycle4-serving-statsfix-20260710124212`
  - digest: `sha256:3e0e1a9b8a616f5ebb65119c9e3a78d882549c9df5224baa49e17b782d0b0f27`

Nomad rollout:

- `stellar-query-api` deployed successfully and is serving the new storage/activity routes.
- `serving-projection-processor-fast` deployed successfully.
- `serving-projection-processor` deployed successfully after a stats-source fix.
- `serving-cold-backfill-contract-storage` completed a storage-only serving backfill.

Serving backfill result:

- `serving.sv_contract_storage_current`: `5,919,336` rows.
- `serving.sv_contract_storage_summary`: `487,479` rows.
- Storage projection coverage: `3..3534034`.

Public smoke checks:

- `GET /health` returned `200`.
- `GET /api/v1/silver/contracts/CAFLSFXZRGJNA37UFG55JWEJ33HAG5QENQZ44I6X7GF4GEBA7HOHHIFJ/storage/summary`
  returned `43` total entries, `43` live entries, and coverage `3..3534034`.
- `GET /api/v1/silver/contracts/CAFLSFXZRGJNA37UFG55JWEJ33HAG5QENQZ44I6X7GF4GEBA7HOHHIFJ/storage?live_only=true&limit=1`
  returned from `serving.sv_contract_storage_current` with coverage `3..3534034`.
- `GET /api/v1/silver/contracts/CAFLSFXZRGJNA37UFG55JWEJ33HAG5QENQZ44I6X7GF4GEBA7HOHHIFJ/activity/summary`
  returned `46` invocations, `9` events, and classification `active_contract`.
- `GET /api/v1/silver/smart-accounts/stats` returned `9,152` smart-account contracts and `12,654` active signers.

Operational note:

The first contract-storage backfill also populated `sv_contract_activity_summary`, but verification failed because the live `contract_stats` projector advanced the activity table beyond the frozen backfill end ledger while the batch was running. The batch was stopped, rerun as storage-only with automatic restart disabled, and the live `contract_stats` projector was corrected to aggregate from `serving.sv_contract_calls_recent` instead of raw silver invocations. That keeps the live activity summary aligned with the backfilled serving call table.

Next cycle:

Cycle 5 should start as a smaller `Cycle 5A` slice focused on the common Horizon read-only migration surface: account root, ledgers, fee stats, operation by id, operation effects, and tighter Horizon paging semantics for account history routes.

## Cycle 5A Implementation Status - 2026-07-10

Implemented and deployed to testnet in `stellar-query-api`:

- `GET /api/v1/horizon-compat/accounts/{id}`
- `GET /api/v1/horizon-compat/accounts/{id}/transactions`
- `GET /api/v1/horizon-compat/ledgers`
- `GET /api/v1/horizon-compat/ledgers/{sequence}`
- `GET /api/v1/horizon-compat/fee_stats`
- `GET /api/v1/horizon-compat/operations/{id}`
- `GET /api/v1/horizon-compat/operations/{id}/effects`

Implementation notes:

- Account root is composed from current account state, balances, and signers using the existing SDK Horizon `Account` shape. Account data remains `{}` until an `account_data_current` data plane exists.
- Account transaction history uses the serving account feed when covered, preserves TOID-backed cursors, and hydrates transaction resources by TOID/ledger when Bronze XDR can be read quickly.
- If account transaction XDR hydration times out, the list route now returns a feed-backed transaction summary instead of failing the whole page. This keeps the migration route usable while marking full cold XDR hydration as a follow-up performance/data-plane concern.
- Ledger collection/detail uses the existing Bronze hot/cold ledger scans and emits TOID-style ledger paging tokens.
- Fee stats are computed from the latest five available ledgers and matching transaction fees.
- Operation detail uses the real `operation_id` TOID and adds the decoded ledger sequence as a cold-scan guard.
- Operation effects are filtered through the shared `EffectFilters.OperationID` path.

Verification:

- `go test -count=1 ./...` in `obsrvr-lake/stellar-query-api/go` passes locally.
- Deployed image: `withobsrvr/stellar-query-api:cycle5a-horizon-fix8-20260710143233`
  (`sha256:1c4f26fbbf17b49ffb5212e6f18b81fcce22ee2b5660ffc92edb0dd5eabbeb87`), Nomad job version 58, deployment `33301b01`.
- Public smoke against `https://obsrvr-lake-testnet.withobsrvr.com` passed:
  - `/health` -> `200` in `0.115s`
  - `/api/v1/horizon-compat/fee_stats` -> `200` in `0.108s`
  - `/api/v1/horizon-compat/ledgers?limit=1&order=desc` -> `200` in `0.386s`
  - `/api/v1/horizon-compat/ledgers/3535639` -> `200` in `0.108s`
  - `/api/v1/horizon-compat/operations?limit=1&order=desc` -> `200` in `1.116s`
  - `/api/v1/horizon-compat/operations/15185453875503105` -> `200` in `0.370s`
  - `/api/v1/horizon-compat/operations/15185453875503105/effects?limit=5` -> `200` in `0.369s`
  - `/api/v1/horizon-compat/accounts/GBTHMMFWTAPFAHRGS33LKETZYJKBTNEENRN47EDZMZPT2BNCJO47GVQG` -> `200` in `0.111s`
  - `/api/v1/horizon-compat/accounts/GBTHMMFWTAPFAHRGS33LKETZYJKBTNEENRN47EDZMZPT2BNCJO47GVQG/transactions?limit=1&order=desc` -> `200` in `4.245s`

## Cycle 5B Implementation Status - 2026-07-10

Implemented and deployed across serving and `stellar-query-api`:

- `serving.sv_transactions_recent` now carries the Horizon transaction resource fields needed to hydrate account transaction pages without re-scanning Bronze: `transaction_id`, `tx_envelope`, `tx_result`, `tx_meta`, `tx_fee_meta`, and `tx_signers`.
- The live transactions serving projector reads those fields from Bronze `transactions_row_v2` and preserves existing XDR values during upserts when an incoming row is partial.
- The serving cold backfill includes those fields when rebuilding `sv_transactions_recent`.
- `HorizonTransactionReader` now tries `serving.sv_transactions_recent` first for transaction hash and TOID+ledger lookups, then falls back to hot/cold Bronze when the serving row is absent, the schema has not been applied yet, or XDR fields are still missing.
- The account transactions Horizon handler no longer returns a feed-summary fallback when hydration fails. It returns `503 data_unavailable` instead of a partial Horizon transaction, and tests cover both the complete-serving and incomplete-hydration paths.

Verification:

- `go test -count=1 ./...` in `obsrvr-lake/stellar-query-api/go` passes locally.
- `go test -count=1 ./...` in `obsrvr-lake/serving-projection-processor/go` passes locally.
- `GOWORK=off GOCACHE=/tmp/go-build-cache go test -count=1 ./...` in `obsrvr-lake/serving-cold-backfill/go` passes locally.

Rollout result:

- Source commit: `d934f3d` (`Fix transaction recent backfill retention source`).
- Deployed tag: `cycle5b-horizon-d934f3d-202607101527`.
- `serving-projection-processor-fast` deployed successfully.
- `serving-projection-processor` deployed successfully.
- `serving-cold-backfill-transactions-recent-cycle5b` completed successfully for `sv_transactions_recent`.
  - Replay range: `3000000..3534034`.
  - Chunk copy counts: `606,097`, `778,758`, `854,683`, `762,011`, `1,029,022`, and `392,004` rows.
  - Serving checkpoint: `sv_transactions_recent|testnet|3534034`, updated `2026-07-10 15:54:13Z`.
  - Table verification for the replay window returned `4,397,958` unique rows spanning `3018954..3534034`; the lower bound reflects the table retention predicate and rows present in `transactions_row_v2`.
- `stellar-query-api` deployed successfully as Nomad job version `59`, deployment `93a29b93`, allocation `ec86e36e`.

Pre-rollout public smoke:

- `2026-07-10`: `GET /health`, `/api/v1/horizon-compat/fee_stats`, `/api/v1/horizon-compat/ledgers?limit=1&order=desc`, and `/api/v1/horizon-compat/accounts/{id}` all returned `200`.
- `GET /api/v1/horizon-compat/accounts/GBTHMMFWTAPFAHRGS33LKETZYJKBTNEENRN47EDZMZPT2BNCJO47GVQG/transactions?limit=1&order=desc` returned `200`, but the transaction record had empty `envelope_xdr`, empty `result_xdr`, empty `fee_meta_xdr`, missing `result_meta_xdr`, and `signatures: null`.
- `GET /api/v1/horizon-compat/transactions/366bc4543a8fe66e09c021af35377c78df6e90e57f85582a0aad1617fcc027e8` did not complete within roughly 60 seconds and was cancelled locally.

Post-rollout public smoke:

- `GET /health` -> `200` in `0.128s`.
- `GET /api/v1/horizon-compat/fee_stats` -> `200` in `0.120s`.
- `GET /api/v1/horizon-compat/ledgers?limit=1&order=desc` -> `200` in `0.384s`.
- `GET /api/v1/horizon-compat/accounts/GBTHMMFWTAPFAHRGS33LKETZYJKBTNEENRN47EDZMZPT2BNCJO47GVQG` -> `200` in `0.110s`.
- `GET /api/v1/horizon-compat/accounts/GBTHMMFWTAPFAHRGS33LKETZYJKBTNEENRN47EDZMZPT2BNCJO47GVQG/transactions?limit=1&order=desc` -> `200` in `0.144s`.
- `GET /api/v1/horizon-compat/transactions/366bc4543a8fe66e09c021af35377c78df6e90e57f85582a0aad1617fcc027e8` -> `200` in `0.104s`.
- Both transaction responses returned hash `366bc4543a8fe66e09c021af35377c78df6e90e57f85582a0aad1617fcc027e8`, ledger `3177525`, paging token `13647365957242880`, non-empty `envelope_xdr`, `result_xdr`, `result_meta_xdr`, `fee_meta_xdr`, and one signature.
- Conclusion: Cycle 5B is deployed and the account transaction hydration acceptance check passes on public testnet.

## Scope Line

Must have:

- Make cold transaction lookup by hash ledger-indexed so old Horizon-compatible transaction routes are usable.
- Repair contract event UTF-8 handling and backfill historical `contract_events_stream_v1` only.
- Harden verification and flusher operations so testnet rollouts do not hang or rely on fragile SIGTERM behavior.

Nice to have:

- Add denormalized serving tables that make Prism and other app queries cheaper.
- Add coverage/watermark metadata for app-facing serving tables.
- Expand Horizon parity route coverage using `horizon-cmp` and a customer route inventory.

Could have:

- Replay the repaired contract-events path on mainnet after testnet proves clean.
- Add a standalone data quality audit command for all string-bearing Bronze tables.

## Cycle 1: Horizon Cold Lookup Must-Haves

Appetite: 1-2 days.

Problem:

`/horizon/transactions/{hash}` can answer quickly for hot data, but old transaction hashes can still force a broad cold scan. The account-ledger index work already gives us the pattern: use a narrow lookup to find the ledger first, then query cold Bronze with an exact ledger predicate.

Implementation:

- Wire an index-backed hash-to-ledger lookup into the Horizon transaction reader path.
- Update the transaction-by-hash cold SQL to include `ledger_sequence = ? AND transaction_hash = ?` when a ledger hint is available.
- Preserve a bounded fallback path for cases where the index is unavailable, but make fallback visible in response metadata or logs.
- Add tests that assert the cold query shape includes the ledger predicate.
- Add a regression test for missing XDR fields in Horizon transaction resources.

Likely files:

- `obsrvr-lake/stellar-query-api/go/horizon_compat_handlers.go`
- `obsrvr-lake/stellar-query-api/go/horizon_transaction_reader.go`
- `obsrvr-lake/stellar-query-api/go/unified_duckdb_reader.go`
- `obsrvr-lake/stellar-query-api/go/*horizon*_test.go`

Done:

- A known cold transaction hash from before ledger `3513747` returns `200` through the public testnet Horizon compatibility route.
- Response includes Horizon-required transaction fields, especially envelope/result/meta XDR fields.
- Cold lookup stays bounded to the transaction's ledger and does not time out under normal testnet load.

## Cycle 2: Contract Events UTF-8 Repair And Historical Backfill

Appetite: 2-4 days.

Problem:

The historical loader completed extraction, but DuckLake rejected this contract events shard:

`/data/output/bronze/contract_events/range_0830000/shard_0003_1783636376497657682.parquet`

The failing table was `contract_events_stream_v1`. The current worker uses `stellar-extract` for contract events and writes decoded string fields directly into Parquet. One or more decoded fields can contain invalid UTF-8 bytes, which DuckLake/Postgres will reject when registering or scanning the data.

Observed code path:

- `obsrvr-lake/stellar-history-loader/go/worker.go` calls `extract.ExtractContractEvents`.
- `obsrvr-lake/stellar-history-loader/go/parquet_writer.go` maps `ContractEventData` into string columns without validation.
- Transaction memos already have a targeted sanitizer, but contract events do not.

Implementation:

1. Identify the exact bad column.
   - Add or run a small diagnostic that scans the failing Parquet shard and checks every contract event string field with `utf8.ValidString`.
   - Report ledger sequence, transaction hash, event id, and column name for each invalid value.

2. Add a contract event sanitizer.
   - Sanitize display/decoded fields before Parquet write:
     - `topics_decoded`
     - `data_decoded`
     - `topic0_decoded`
     - `topic1_decoded`
     - `topic2_decoded`
     - `topic3_decoded`
   - Keep authoritative XDR fields unchanged:
     - `contract_event_xdr`
     - `data_xdr`
   - Prefer lossless representation for invalid display strings, such as a structured base64 marker, instead of silently dropping bytes.
   - Log or count how many fields were repaired.

3. Add tests.
   - Unit test invalid UTF-8 in each decoded contract event field.
   - Unit test that valid JSON/valid UTF-8 fields are unchanged.
   - Regression test that sanitized rows can be written through the Parquet writer path.

4. Add table-specific replay support.
   - Add a loader option to extract and push only contract events, for example:
     - `--only-tables contract_events`
     - `--ducklake-only-tables contract_events_stream_v1`
   - Make the replay idempotent by deleting the target ledger range from `contract_events_stream_v1` before inserting, or by writing into a clean target range.

5. Backfill only historical contract events on testnet.
   - Replay `3..3513746` for contract events only.
   - Push only `contract_events_stream_v1` to DuckLake.
   - Verify counts before enabling app/API paths that depend on historical events.

Done:

- The known failing shard can be pushed into DuckLake.
- `contract_events_stream_v1` has historical rows for ledgers before `3513747`.
- A testnet contract with historical events can be queried through the API from cold data.
- No full replay of ledgers, transactions, operations, effects, trades, or other already-loaded Bronze tables is required.

## Cycle 3: Rollout Operations Hardening

Appetite: 1-2 days.

Problem:

The recovery worked, but two operational rough edges cost time:

- `10-verify.sh` hung because it used direct host curl calls without timeouts.
- The flusher's SIGTERM final flush was killed after about 30 seconds despite the intended long timeout, leaving partial cold rows that had to be manually cleaned up.

Implementation:

- Update verification scripts to use public HTTPS routes by default where appropriate.
- Add curl timeouts to every verification call.
- Make direct-host verification an explicit opt-in mode.
- Fix the effective Nomad shutdown timeout for `postgres-ducklake-flusher`, or stop relying on SIGTERM for large final flushes.
- Add a safe one-shot flush mode, such as `--flush-once`, for controlled catch-up work.
- Make flusher writes idempotent for a ledger range:
  - delete target range before insert, or
  - use a deterministic replacement strategy where supported.

Done:

- Verification scripts fail fast with actionable output instead of hanging.
- A live catch-up flush can be triggered intentionally and observed to completion.
- Retrying an interrupted flush does not require a custom cleanup program.

## Cycle 4: Denormalized Serving Tables For Apps

Appetite: 1 week.

Problem:

Prism and future app surfaces should not have to reconstruct common views from raw Bronze/Silver joins. We should keep the Horizon compatibility layer focused on Horizon contracts, while adding app-oriented serving tables for common workflows.

Initial serving tables:

- `serving.sv_contract_storage_current`
  - current storage rows by contract/key
  - TTL-aware live status
  - decoded key/data display fields
  - last-modified ledger/time

- `serving.sv_contract_storage_summary`
  - row counts by contract
  - live/expired/deleted counts
  - latest ledger
  - storage type counts

- `serving.sv_smart_account_contracts`
  - detected smart account contracts
  - family/provider/confidence
  - last seen ledger

- `serving.sv_smart_account_signers`
  - reverse lookup by signer address and credential id
  - active/inactive status
  - rule/context metadata where available

- `serving.sv_contract_activity_summary`
  - recent invocation/event counts
  - first/last seen ledger
  - basic activity classification

Implementation:

- Prefer serving processors for denormalized, app-facing projections.
- Keep query endpoints thin: read serving tables first, fall back to Silver only when coverage is incomplete.
- Add watermarks per serving projection.
- Return coverage metadata so clients know whether the response is current, partial, or fallback.
- Add Prism-facing examples and recommended query patterns.

Done:

- Prism can render contract storage and smart-account views without broad cold scans.
- Serving responses include projection watermark/coverage metadata.
- Backfill and live update paths use the same schema and response contracts.

Implementation status (2026-07-10):

- Added `serving.sv_contract_storage_summary`, populated by both the live serving processor and serving cold backfill from `serving.sv_contract_storage_current`.
- Added `serving.sv_contract_activity_summary`, populated by the live contract stats projector and by cold backfill from `serving.sv_contract_calls_recent` plus storage summary.
- Added app-facing `serving.sv_smart_account_contracts` and `serving.sv_smart_account_signers` alongside the existing detailed smart-account serving tables.
- Live serving projectors now upsert `serving.sv_watermarks` for contract storage, contract activity, and smart-account projections.
- Query API now exposes serving-backed:
  - `GET /api/v1/silver/contracts/{id}/storage/summary`
  - `GET /api/v1/silver/contracts/{id}/activity/summary`
  - `GET /api/v1/silver/contracts/{id}/storage` with serving coverage metadata and read-time TTL recomputation
  - smart-account lookup/state/stats endpoints with serving-first reads and silver fallback
- Tests cover serving processor schema/projectors, cold backfill projection counts/content, storage summary coverage, and smart-account serving lookup/fallback.

Deployment note:

- Apply the serving schema before enabling the new reads in production/testnet, then run serving cold backfill or the live serving processor so `sv_watermarks` exists. Until watermarks exist, smart-account handlers fall back to silver state; storage summary/activity endpoints return 404 for unmaterialized contracts.

## Cycle 5: Horizon Parity Expansion

Appetite: 1-2 weeks per slice, depending on the customer's route inventory.

Problem:

The Horizon compatibility layer should focus first on routes that unblock real migration. Until the customer's route inventory arrives, prioritize the common read-only routes heavy Horizon users usually depend on.

Priority routes:

- Transaction by hash.
- Account transaction history.
- Account operation history.
- Account payment history.
- Operation by id.
- Effects for transaction/account/operation.
- Ledger by sequence.
- Recent ledgers.
- Fee stats.
- Account root object.

Implementation:

- Populate `go-stellar-sdk` Horizon resource structs instead of hand-writing JSON.
- Use real TOIDs for operation ids and paging tokens where available.
- Decode transaction envelopes once per page to enrich operation-specific details.
- Use `horizon-cmp` against public Horizon testnet with a documented allowlist for known gaps.
- Keep submission, friendbot, paths, and SSE delegated rather than reimplemented.

Done:

- The customer route inventory maps to supported, delegated, or intentionally unsupported routes.
- `horizon-cmp` passes for the must-have route set with only documented differences.
- Compatibility routes have tests using `horizonclient` or SDK resource round-trips.

## Recommended Order

1. Cycle 1: cold transaction lookup performance.
2. Cycle 2: contract event UTF-8 repair and contract-events-only historical backfill.
3. Cycle 3: rollout hardening.
4. Cycle 4: denormalized serving tables.
5. Cycle 5: route expansion guided by the customer inventory.

Cycle 1 and Cycle 2 are the immediate blockers. Cycle 3 should happen before the next large operational run. Cycle 4 can start once contract events are restored, because smart-account and app projections depend on reliable event history.

## Bad UTF-8 Policy

Bad UTF-8 should be fixed at ingestion/write time, not by skipping rows or mutating authoritative XDR.

Rules:

- XDR columns remain authoritative and unchanged.
- Display/decoded columns must always be valid UTF-8.
- If decoded bytes cannot be represented safely, store a lossless marker that preserves the original bytes in base64.
- Tests should prove every string column written to Parquet is valid UTF-8.
- The loader should report repaired-field counts so a spike is visible during backfills.

This keeps DuckLake/Postgres happy without hiding source data loss or changing the canonical event payload.
