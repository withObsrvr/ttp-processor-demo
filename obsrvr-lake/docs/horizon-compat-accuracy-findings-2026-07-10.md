# Horizon Compatibility Accuracy Findings - 2026-07-10

Test date: 2026-07-10

Compared:

- Obsrvr: `https://obsrvr-lake-testnet.withobsrvr.com/api/v1/horizon-compat`
- SDF Horizon: `https://horizon-testnet.stellar.org`

This is a live API comparison, not a static code review. The goal was to test
how accurately the currently deployed Horizon compatibility endpoints match
Horizon testnet for the implemented route surface.

## Fixtures

Account:

```text
GBTHMMFWTAPFAHRGS33LKETZYJKBTNEENRN47EDZMZPT2BNCJO47GVQG
```

Known historical transaction:

```text
366bc4543a8fe66e09c021af35377c78df6e90e57f85582a0aad1617fcc027e8
ledger: 3177525
transaction paging token: 13647365957242880
operation id: 13647365957242881
```

## Post-Rollout Closeout

After the Cycle 5B closeout pass on 2026-07-10, the deployed public testnet
compatibility surface has no hard failures in the accuracy harness. Public
smoke passed against:

```bash
python3 obsrvr-lake/stellar-query-api/scripts/horizon_compat_smoke.py \
  --base-url https://obsrvr-lake-testnet.withobsrvr.com/api/v1/horizon-compat \
  --account GBTHMMFWTAPFAHRGS33LKETZYJKBTNEENRN47EDZMZPT2BNCJO47GVQG \
  --tx-hash 366bc4543a8fe66e09c021af35377c78df6e90e57f85582a0aad1617fcc027e8 \
  --timeout 20 --json
```

Final smoke result: `ok=true`.

Final accuracy command:

```bash
python3 obsrvr-lake/stellar-query-api/scripts/horizon_compat_accuracy.py \
  --timeout 20 --json
```

Final accuracy summary:

| Route | Verdict | Current behavior |
| --- | --- | --- |
| `/fee_stats` | Partial | `200`; latest ledger or capacity can differ by live-tip timing/calculation |
| `/ledgers?limit=1&order=desc` | Partial | `200`; live tip can lag SDF Horizon by a few ledgers |
| `/ledgers/3177525` | Pass | `200`; historical exact ledger now matches tested fields |
| `/accounts/{id}` | Partial | `200`; account current-state still lacks Horizon-exact balance/sequence fields |
| `/accounts/{id}/transactions?limit=1&order=desc` | Pass | `200`; transaction XDR/signatures present |
| `/transactions/{hash}` | Pass | `200`; core transaction fields and XDR fields present |
| `/transactions/{hash}/operations?limit=5` | Pass | `200`; record count and core operation fields match |
| `/transactions/{hash}/effects?limit=5` | Pass | `200`; tested fixture has zero effects on both sides |
| `/operations?limit=1&order=desc` | Partial | `200`; no timeout, but live latest row can differ and Soroban detail shape is not Horizon-exact |
| `/operations/{id}` | Partial | `200`; operation exists, but Soroban function/parameter formatting differs from Horizon |
| `/operations/{id}/effects?limit=5` | Pass | `200`; tested fixture has zero effects on both sides |
| `/payments?limit=1&order=desc` | Partial | `200`; Horizon payment semantics/order differ from Obsrvr payment-op projection |
| `/effects?limit=1&order=desc` | Partial | `200`; effect ID/paging token format differs |
| `/accounts/{id}/operations?limit=3&order=desc` | Pass | `200`; tested count and core fields match |
| `/accounts/{id}/payments?limit=3&order=desc` | Partial | `200`; payment semantics/order differ |
| `/accounts/{id}/effects?limit=3&order=desc` | Partial | `200`; account effect ordering and ID format differ |

The remaining work is compatibility quality, not route availability. The next
parity cycle should focus on:

- Horizon account root current-state parity: trustline balances, sequence
  ledger/time, liabilities, sponsor fields, auth flags, and balance
  last-modified ledger.
- Horizon Soroban operation resource formatting: preserve Horizon's generic
  host-function naming and parameter list when strict drop-in compatibility is
  required.
- Payment and effect semantics: Horizon's `/payments` includes payment-like
  Soroban effects differently than the current `is_payment_op` projection, and
  effect IDs/paging tokens need Horizon formatting.
- Live-tip comparison tolerance: public accuracy checks against SDF Horizon can
  report partials when Obsrvr is one or two ledgers behind during the test.

Deployed images for this closeout:

| Service | Image tag | Digest |
| --- | --- | --- |
| `stellar-query-api` | `cycle5b-horizon-f4973a1-20260710-ledger-timeout` | `sha256:d4a5336e11ff0f2748f5b798b3e944b74c5b769ddb53c2e9c6e960afe3ae8614` |
| `account-index-transformer` | `cycle5b-horizon-f4973a1-20260710-account-feed-indexes` | `sha256:a94ea5c1e3133965c637490c69b16c7a53b25096ed34cbb827933744b6ccb2e1` |
| `serving-cold-backfill` | `cycle5b-horizon-f4973a1-20260710-effects` | previously deployed for effects and by-account catch-up |
| `serving-projection-processor` | `cycle5b-horizon-f4973a1-20260710-effects` | previously deployed for effects schema/projection support |

Operational changes made during closeout:

- Backfilled `serving.sv_effects_by_account` for `162..3539251`; final row
  count at completion was `28860022`.
- Ran by-account catch-up for `3466466..3539759`, adding `504084`
  transaction-feed rows and `686476` operation-feed rows.
- Corrected by-account watermarks back to `complete_from=3` after the catch-up
  job rewrote them to the catch-up start ledger.
- Enabled `account-index-transformer.serving_feed` in the live testnet Nomad
  spec and verified it continues to write serving-feed rows.
- Created and verified valid Postgres indexes:
  `sv_operations_by_account_operation_idx` and
  `sv_operations_by_account_payment_page_idx`.

Final serving watermark check:

```text
serving.sv_effects_by_account|complete|162|3540468
serving.sv_operations_by_account|complete|3|3540468
serving.sv_transactions_by_account|complete|3|3540468
```

The sections below are retained as the historical pre-closeout findings and the
source-fix audit trail.

Latest Horizon transaction for the account at test time:

```text
e16c38b6dd4a07938746249a87f33cd4f5431974cf397c6417f62e608f646022
ledger: 3537853
transaction paging token: 15194962933100544
operation id: 15194962933100545
```

## Executive Summary

Obsrvr is accurate for exact Horizon transaction detail on the tested hashes:
core transaction fields, XDR fields, signatures, ledger, hash, fee, source
account, and paging token matched Horizon.

Recent ledger detail is also accurate on the tested core fields when queried by
exact sequence.

The compatibility layer is not yet accurate enough for strict Horizon client
drop-in use across the implemented route surface. The largest gaps are:

1. Collection JSON shape uses `_embedded.Records` instead of Horizon's
   `_embedded.records`.
2. Account root misses trustline balances and Horizon account fields such as
   `sequence_ledger` and `sequence_time`.
3. Account transaction history is stale even though direct transaction lookup
   has the latest transaction.
4. Operation detail and account-scoped operation/payment routes time out.
5. Operation collection records are mostly base records and miss Soroban
   operation details such as `function`, `parameters`, and `address`.
6. Effects routes time out or return gateway-level timeout behavior where
   Horizon returns `200`.

## Remediation Status

Source fixes added after this live comparison, pending build/deploy and public
retest:

- Normalize Horizon collection output to `_embedded.records`.
- Emit numeric Horizon operation paging tokens and accept numeric operation
  cursors.
- Reject stale unbounded serving account-transaction feed usage by falling back
  when `sv_transactions_by_account.complete_thru` is behind the latest serving
  ledger.
- Preserve trustline balances in account root responses when signer data is
  present.
- Populate and serve account `sequence_ledger`, `sequence_time`, sponsor,
  account auth flags, sponsoring counts, trustline liabilities, maintain-liability
  auth, clawback flag, sponsor, and trustline last-modified ledger through the
  source loaders, silver current state, serving projections, and Horizon account
  reader.
- Query exact operation detail hot-first, then cold, instead of unioning hot and
  cold for `/operations/{id}`.
- Query exact effects and recent global effects from a single tier first,
  avoiding the hot+cold union that caused timeout behavior.
- Hydrate Soroban invoke-host-function operation records with the available
  silver fields: `function`, `parameters`, `address`, and empty
  `asset_balance_changes`.
- Populate transaction `preconditions` from `envelope_xdr` when the envelope can
  be decoded.
- Match Horizon fee-stat calculation more closely: per-operation fee charged and
  max-fee distributions, discrete percentile selection, two-decimal ledger
  capacity usage, and latest-base-fee fallback when the last-five-ledger window
  has no transactions.
- Emit `transaction_id` and `operation_id` from `silver-history-loader` for
  historical enriched operations, and compute operation IDs at read time when
  `transaction_id` exists but `operation_id` is null.
- Use `serving.sv_operations_by_account` for account-scoped Horizon
  `/accounts/{id}/operations` and `/accounts/{id}/payments` pages when the
  serving watermark is complete and current enough; otherwise fall back to the
  unified reader.
- Use `serving.sv_effects_by_account` for account-scoped Horizon
  `/accounts/{id}/effects` pages when the serving watermark is complete and
  current enough; otherwise fall back to the unified reader.
- Avoid the hot+cold union for first-page account-scoped Horizon effects by
  querying hot first and filling from cold only when serving coverage is absent.
- Add `ledger_range` pruning to hot/cold ledger reads so exact historical
  ledger detail can hit one partition instead of scanning the whole cold table.
- Bound transaction-scoped effects by first resolving the transaction ledger and
  adding `ledger_sequence` plus `ledger_range` predicates to the effects query.

Remaining work after this source pass:

- Build and deploy the updated images, run the new schema migrations, and rerun
  the public smoke/accuracy comparison. The route results below are from the
  pre-fix deployment.
- Rebuild/replay affected testnet data so existing historical account snapshot
  and enriched operation rows actually contain the new sequence and TOID fields.
- Keep `serving.sv_transactions_by_account` fully current operationally; the
  code now avoids stale reads, but the feed itself still needs live catch-up.
- Rerun cold historical ledger detail and transaction-effect latency checks
  after deploy. Source now adds partition pruning, but the public endpoint still
  needs a deployed build and cold data with populated `ledger_range` values.
- Backfill and keep `serving.sv_effects_by_account` current; the source path is
  implemented, and `serving-projection-processor` can advance the table after a
  full-history backfill watermark exists, but the query fast path only activates
  when that watermark is complete and current enough.
- For already-materialized historical rows where both `transaction_id` and
  `operation_id` are null, run a TOID backfill or regenerate silver history from
  bronze transactions. The read-time fallback can only compute `operation_id`
  when `transaction_id` is present.

## Public Retest After Adding Accuracy Harness

Command:

```bash
python3 obsrvr-lake/stellar-query-api/scripts/horizon_compat_accuracy.py --timeout 20
```

Result on the currently deployed public testnet service:

| Route | Result | Obsrvr | Horizon | Detail |
| --- | --- | ---: | ---: | --- |
| `/fee_stats` | Partial | `200` in `0.116s` | `200` in `0.180s` | capacity usage differed: `0.082` vs `0.11` |
| `/ledgers?limit=1&order=desc` | Partial | `200` in `0.302s` | `200` in `0.131s` | Horizon-compatible lowercase `records` was empty on Obsrvr |
| `/ledgers/3177525` | Fail | `504` in `5.205s` | `200` in `0.148s` | cold historical ledger detail still times out |
| `/accounts/{id}` | Partial | `200` in `0.123s` | `200` in `0.148s` | sequence, sequence ledger/time, and balances differed |
| `/accounts/{id}/transactions?limit=1&order=desc` | Partial | `200` in `0.124s` | `200` in `0.137s` | Horizon-compatible lowercase `records` was empty on Obsrvr |
| `/transactions/{hash}` | Partial | `200` in `0.102s` | `200` in `0.190s` | preconditions presence differed |
| `/transactions/{hash}/operations?limit=5` | Partial | `200` in `0.272s` | `200` in `0.140s` | Obsrvr had 0 Horizon-compatible records; Horizon had 1 |
| `/transactions/{hash}/effects?limit=5` | Fail | `504` in `5.187s` | `200` in `0.137s` | timeout |
| `/operations?limit=1&order=desc` | Partial | `200` in `1.068s` | `200` in `0.184s` | Obsrvr had 0 Horizon-compatible records; Horizon had 1 |
| `/operations/{id}` | Fail | `404` in `0.125s` | `200` in `0.124s` | operation row not found through deployed path |
| `/operations/{id}/effects?limit=5` | Pass | `200` in `3.634s` | `200` in `0.142s` | both returned 0 records |
| `/payments?limit=1&order=desc` | Partial | `200` in `0.625s` | `200` in `0.135s` | Obsrvr had 0 Horizon-compatible records; Horizon had 1 |
| `/effects?limit=1&order=desc` | Partial | `200` in `1.054s` | `200` in `0.134s` | Obsrvr had 0 Horizon-compatible records; Horizon had 1 |
| `/accounts/{id}/operations?limit=3&order=desc` | Partial | `200` in `0.853s` | `200` in `0.130s` | Obsrvr had 0 Horizon-compatible records; Horizon had 3 |
| `/accounts/{id}/payments?limit=3&order=desc` | Partial | `200` in `0.458s` | `200` in `0.263s` | Obsrvr had 0 Horizon-compatible records; Horizon had 3 |
| `/accounts/{id}/effects?limit=3&order=desc` | Fail | `504` in `4.652s` | `200` in `0.133s` | timeout |

The separate deployment smoke passed for Cycle 5B transaction hydration, but
this comparison shows why that smoke is not sufficient for Horizon replacement
readiness. The current public service still needs the broader source fixes,
schema/data refresh, and retest before it can be presented as a migration target
for a heavy Horizon user.

## Local Verification For Pending Rollout

Tests:

```text
go test ./obsrvr-lake/stellar-query-api/go
go test ./obsrvr-lake/serving-projection-processor/go
go test ./obsrvr-lake/silver-realtime-transformer/go
go test ./obsrvr-lake/stellar-postgres-ingester/go
go test ./obsrvr-lake/postgres-ducklake-flusher/go
GOWORK=off GOCACHE=/tmp/go-build-codex go test .  # serving-cold-backfill/go
GOWORK=off GOCACHE=/tmp/go-build-codex go test .  # silver-current-state-projector/go
GOWORK=off GOCACHE=/tmp/go-build-codex go test .  # silver-history-loader/go
GOWORK=off GOCACHE=/tmp/go-build-codex go test .  # stellar-history-loader/go
python3 -m py_compile obsrvr-lake/stellar-query-api/scripts/horizon_compat_smoke.py obsrvr-lake/stellar-query-api/scripts/horizon_compat_accuracy.py
```

All passed locally.

Binary builds passed for:

```text
stellar-query-api
stellar-history-loader
stellar-postgres-ingester
postgres-ducklake-flusher
silver-history-loader
silver-realtime-transformer
silver-current-state-projector
serving-projection-processor
serving-cold-backfill
silver-cold-flusher
```

Docker images were built locally with `DOCKER_TAG=horizon-compat-local`:

| Image | Local image ID |
| --- | --- |
| `withobsrvr/stellar-query-api:horizon-compat-local` | `sha256:0198751c8ae865c5d45b49c88956f0dd1c360c11ab77e81d10730d69ef09aa42` |
| `withobsrvr/stellar-history-loader:horizon-compat-local` | `sha256:077abad146277307d82599b4c4ebef7989b54bf81000f7b2034b61f78d45c7ce` |
| `withobsrvr/stellar-postgres-ingester:horizon-compat-local` | `sha256:1dc2b668505710662f8e3061492897cf4ee44493c57abd45013272d47ce1be26` |
| `withobsrvr/postgres-ducklake-flusher:horizon-compat-local` | `sha256:f432d8f379a59e964323bb0bd783bf19c139047ed17b356ffb51ba7db72a2813` |
| `withobsrvr/silver-history-loader:horizon-compat-local` | `sha256:8729ad1a1778ef5a9e9cb6d21057f89068288c91f1d14643ef7c393223ef0247` |
| `withobsrvr/silver-realtime-transformer:horizon-compat-local` | `sha256:f1b4d0dea9a17a1c5cea9971c91d7b4da6da42cad61ca04c57aeab3376c72c83` |
| `withobsrvr/silver-current-state-projector:horizon-compat-local` | `sha256:924e7caf5db486b5e3e8d41e81ee01d9d07fea206059200e9abdc9ed47daf864` |
| `withobsrvr/serving-projection-processor:horizon-compat-local` | `sha256:0b018a13e641cad52cd0b17213cc956778050d97dcdf3c1fcf24c572468544bd` |
| `withobsrvr/serving-cold-backfill:horizon-compat-local` | `sha256:c5fe32976548459c5c7beea4667a45319457d5ca2a98fa83054aa290ff03ca8e` |
| `withobsrvr/silver-cold-flusher:horizon-compat-local` | `sha256:30bdf02f14fb94cc4fe565e069e9b510677e142adbcaf7575e735c0e7bb234a9` |

## Route Results

| Route | Obsrvr | Horizon | Result |
| --- | ---: | ---: | --- |
| `/fee_stats` | `200` in `0.131s` | `200` in `0.171s` | Partial: shape matches, values differ |
| `/ledgers?limit=1&order=desc` | `200` in `0.467s` | `200` in `0.133s` | Partial: records field casing mismatch; latest ledgers differed by one |
| `/ledgers/3177525` | `504` in `5.115s` | `200` in `0.132s` | Fail: cold historical ledger detail timed out |
| `/ledgers/3537850` | `200` in `0.121s` | `200` in `0.203s` | Pass: tested core fields matched |
| `/ledgers/3537851` | `200` in `0.099s` | `200` in `0.164s` | Pass: tested core fields matched |
| `/accounts/{id}` | `200` in `0.102s` | `200` in `0.131s` | Partial: missing trustline and sequence fields; stale state |
| `/accounts/{id}/transactions?limit=1&order=desc` | `200` in `0.107s` | `200` in `0.142s` | Partial/fail: stale account feed |
| `/transactions/{historical_hash}` | `200` in `0.104s` | `200` in `0.139s` | Pass on core transaction fields; response shape has extra/missing fields |
| `/transactions/{latest_horizon_hash}` | `200` in `0.125s` | `200` in `0.175s` | Pass on core transaction fields; response shape has extra/missing fields |
| `/transactions/{historical_hash}/operations?limit=5` | `200` in `0.260s` | `200` in `0.135s` | Fail: Obsrvr returned 0 records; Horizon returned 1 |
| `/transactions/{latest_horizon_hash}/operations?limit=5` | `200` in `3.396s` | `200` in `0.133s` | Partial: record exists but lacks Horizon Soroban detail fields |
| `/transactions/{historical_hash}/effects?limit=5` | `504` in `4.613s` | `200` in `0.159s` | Fail |
| `/operations?limit=1&order=desc` | `200` in `1.016s` | `200` in `0.134s` | Partial: collection shape and operation detail fields differ |
| `/operations/{id}` | `504` in about `4.1s` | `200` in about `0.13s` | Fail for tested historical and recent operation IDs |
| `/operations/{id}/effects?limit=5` | `504` in `4.186s` | `200` in `0.136s` | Fail |
| `/payments?limit=1&order=desc` | `200` in `0.528s` | `200` in `0.141s` | Partial: latest records and shape differed |
| `/effects?limit=1&order=desc` | client timeout after `20.086s` | `200` in `0.933s` | Fail |
| `/accounts/{id}/operations?limit=3&order=desc` | `504` in `4.119s` | `200` in `0.145s` | Fail |
| `/accounts/{id}/payments?limit=3&order=desc` | `504` in `4.117s` | `200` in `7.862s` | Fail |
| `/accounts/{id}/effects?limit=3&order=desc` | `504` in `4.114s` | `200` in `0.193s` | Fail |

## Findings

### 1. Collection Shape Is Not Horizon-Exact

Obsrvr collection responses use:

```json
"_embedded": {
  "Records": [...]
}
```

Horizon uses:

```json
"_embedded": {
  "records": [...]
}
```

Observed on:

- `/ledgers`
- `/accounts/{id}/transactions`
- `/transactions/{hash}/operations`
- `/operations`
- `/payments`

This is a high-priority compatibility issue. Many Horizon clients deserialize
`_embedded.records` exactly.

### 2. Transaction Detail Is The Strongest Match

For both the historical transaction and the latest Horizon account transaction,
the following fields matched exactly:

- `id`
- `paging_token`
- `successful`
- `hash`
- `ledger`
- `created_at`
- `source_account`
- `fee_account`
- `fee_charged`
- `max_fee`
- `operation_count`
- `envelope_xdr`
- `result_xdr`
- `fee_meta_xdr`
- `memo_type`
- `source_account_sequence`
- `signatures`

Mismatches:

- Horizon included `preconditions`; Obsrvr did not.
- Obsrvr included `result_meta_xdr`; Horizon testnet did not include it in the
  sampled transaction responses.

Interpretation:

The Cycle 5B transaction hydration work is effective. The remaining work is
response-shape polish, especially `preconditions` and deciding whether the
compatibility route should omit Obsrvr-only `result_meta_xdr` for strict Horizon
shape parity.

### 3. Recent Ledger Detail Matches, Historical Ledger Detail Times Out

Recent exact ledger detail checks passed:

- `/ledgers/3537850`
- `/ledgers/3537851`

For both, the tested fields matched Horizon:

- `id`
- `paging_token`
- `hash`
- `prev_hash`
- `sequence`
- `successful_transaction_count`
- `failed_transaction_count`
- `operation_count`
- `tx_set_operation_count`
- `closed_at`
- `total_coins`
- `fee_pool`
- `base_fee_in_stroops`
- `base_reserve_in_stroops`
- `max_tx_set_size`
- `protocol_version`

Historical ledger detail failed:

```text
GET /ledgers/3177525
Obsrvr: 504, failed to query ledgers from DuckLake: context deadline exceeded
Horizon: 200
```

Interpretation:

Ledger resource mapping is good when the reader can find the row quickly. The
problem is cold lookup performance/bounding for older ledger detail.

### 4. Fee Stats Shape Matches But Values Differ

Both returned the same top-level keys:

- `fee_charged`
- `last_ledger`
- `last_ledger_base_fee`
- `ledger_capacity_usage`
- `max_fee`

But values differed. Example:

```text
Obsrvr last_ledger: 3537850
Horizon last_ledger: 3537851
Obsrvr ledger_capacity_usage: 0.053
Horizon ledger_capacity_usage: 0.07
```

Some percentiles and modes differed materially even with only a one-ledger
freshness gap.

Interpretation:

This is a partial parity pass. Shape is close, but the aggregation window or
calculation likely does not exactly match Horizon.

### 5. Account Root Is Partial

Matching fields for the tested account:

- `id`
- `account_id`
- `subentry_count`
- `num_sponsored`
- `num_sponsoring`
- `paging_token`
- signers
- thresholds
- flags

Mismatches:

```text
sequence:
  Obsrvr  2724392246099826
  Horizon 2724392246099854

last_modified_ledger:
  Obsrvr  3537809
  Horizon 3537853

last_modified_time:
  Obsrvr  2026-07-10T17:37:54Z
  Horizon 2026-07-10T17:41:34Z
```

Missing Horizon fields:

- `sequence_ledger`
- `sequence_time`

Balance mismatch:

- Obsrvr returned only native XLM:
  - `35886.2452469`
- Horizon returned:
  - USDC trustline balance `1580.6092048`
  - native XLM `35886.2093937`
  - trustline fields including `limit`, `buying_liabilities`,
    `selling_liabilities`, authorization flags, and `last_modified_ledger`

Interpretation:

The account root is useful but not Horizon-accurate yet. Trustline balances and
Horizon account sequence fields need to be added. The native balance/sequence
also showed freshness lag.

### 6. Account Transaction History Is Stale

For the tested account:

```text
Obsrvr latest account transaction:
  hash: 366bc4543a8fe66e09c021af35377c78df6e90e57f85582a0aad1617fcc027e8
  ledger: 3177525
  created_at: 2026-06-19T20:29:57Z

Horizon latest account transaction:
  hash: e16c38b6dd4a07938746249a87f33cd4f5431974cf397c6417f62e608f646022
  ledger: 3537853
  created_at: 2026-07-10T17:41:34Z
```

Direct transaction lookup for Horizon's latest transaction succeeded on Obsrvr
and matched Horizon core transaction fields:

```text
GET /transactions/e16c38b6dd4a07938746249a87f33cd4f5431974cf397c6417f62e608f646022
Obsrvr: 200
Horizon: 200
```

Interpretation:

Bronze transaction ingestion has the latest transaction, but the account
transaction history path is not using a current enough account feed/source. This
is likely a serving/account-feed coverage or live-update issue, not a
transaction-resource hydration issue.

### 7. Operation Routes Are Not Horizon-Accurate Yet

Historical transaction operations:

```text
GET /transactions/366bc454.../operations?limit=5
Obsrvr: 200 with 0 records
Horizon: 200 with 1 record
```

Latest transaction operations:

```text
GET /transactions/e16c38.../operations?limit=5
Obsrvr: 200 with 1 record
Horizon: 200 with 1 record
```

For the latest transaction, core fields matched:

- operation id
- created_at
- source_account
- transaction_hash
- transaction_successful
- type
- type_i

But Obsrvr missed Horizon Soroban operation fields:

- `address`
- `asset_balance_changes`
- `function`
- `parameters`
- `salt`

Obsrvr operation `paging_token` was also a base64 cursor:

```text
MzUzNzg1MzoxNTE5NDk2MjkzMzEwMDU0NTphc2M=
```

Horizon operation `paging_token` was the numeric operation id:

```text
15194962933100545
```

Direct operation detail failed for multiple tested operation IDs:

```text
GET /operations/13647365957242881
GET /operations/15194967228063745
GET /operations/15194962933108737
GET /operations/15194984407969793

Obsrvr: 504 horizon GetOperationByID: context deadline exceeded
Horizon: 200
```

Interpretation:

Operation routes are route-surface complete but not data/shape complete. They
need bounded operation-id lookups, Horizon numeric paging tokens, and per-type
operation detail population from transaction envelope/XDR.

### 8. Effects Routes Need A Serving/Index Path

Failures observed:

```text
GET /transactions/366bc454.../effects?limit=5
Obsrvr: 504
Horizon: 200 with empty records

GET /operations/13647365957242881/effects?limit=5
Obsrvr: 504
Horizon: 200

GET /effects?limit=1&order=desc
Obsrvr: client timeout after 20s
Horizon: 200

GET /accounts/{id}/effects?limit=3&order=desc
Obsrvr: 504
Horizon: 200
```

Interpretation:

Effects compatibility should not rely on the current broad hot/cold fallback.
It needs a bounded serving or index-backed path and should return Horizon-style
empty pages quickly when no effects exist.

## Priority Recommendations

### P0 - Make Collection JSON Shape Horizon-Compatible

Fix `_embedded.Records` to `_embedded.records` for every compat collection.

This is the most client-visible shape mismatch and can break SDK/raw HTTP
consumers even when the underlying data is correct.

### P0 - Fix Account Transaction Feed Freshness

The direct transaction route found Horizon's latest account transaction, but
`/accounts/{id}/transactions` returned an older June transaction.

Investigate:

- `serving.sv_transactions_recent` live projector coverage
- account transaction feed coverage/watermark
- whether the account route falls back to stale serving rows instead of hot
  Bronze/federated data beyond the serving checkpoint

Acceptance:

For the tested account, Obsrvr and Horizon should return the same latest
transaction hash when queried at the same point in time or within a clearly
documented freshness lag.

### P0 - Fix Operation Detail Lookup Timeouts

`GET /operations/{id}` timed out for every tested operation id. Since operation
id encodes ledger sequence, this should be bounded by ledger rather than broad
scanning.

Acceptance:

- `/operations/15194962933100545` returns `200`
- response includes Horizon Soroban operation fields
- `paging_token` is `15194962933100545`, not an internal base64 cursor

### P1 - Add Account Trustline Balances To Horizon Account Root

Status: fixed in source, pending deploy/backfill/public retest.

Account root must include trustlines and Horizon balance fields:

- `asset_code`
- `asset_issuer`
- `limit`
- `buying_liabilities`
- `selling_liabilities`
- `is_authorized`
- `is_authorized_to_maintain_liabilities`
- `last_modified_ledger`

Also add account-level:

- `sequence_ledger`
- `sequence_time`

### P1 - Populate Operation Type-Specific Fields

Status: partially fixed in source. `invoke_host_function` now uses the
available silver fields for `address`, `function`, `parameters`, and empty
`asset_balance_changes`; `salt` still depends on deeper envelope/meta decoding.

For `invoke_host_function`, Horizon includes:

- `address`
- `function`
- `parameters`
- `salt`
- `asset_balance_changes`

### P1 - Add Effects Serving/Index Path

Status: fixed in source, pending deploy/backfill/public retest. Exact
transaction/operation and recent global effects now query a single tier first to
avoid the hot+cold union timeout. Account-scoped effects use
`serving.sv_effects_by_account` when its watermark is complete and current
enough; `serving-cold-backfill` creates the full-history table and
`serving-projection-processor` advances it live afterward.

Effects endpoints should be bounded by:

- transaction hash
- operation id / ledger
- account id
- global recent cursor

### P2 - Align Fee Stats Calculation

Status: fixed in source, pending deploy/public retest. The reader now follows
Horizon's last-five-ledger, per-operation fee distribution shape and rounded
capacity usage.

### P2 - Decide Strictness For Extra `result_meta_xdr`

Obsrvr returns `result_meta_xdr`; Horizon testnet did not in sampled responses.
If exact Horizon JSON shape is required, hide Obsrvr-only fields from the
compatibility route. If the field is intentionally additive, document it as an
extension and confirm SDK clients tolerate it.

## Current Accuracy Rating

| Area | Rating | Notes |
| --- | --- | --- |
| Transaction detail by hash | High | Core fields and XDR matched; preconditions missing |
| Recent ledger detail | High | Tested fields matched exactly |
| Fee stats | Medium -> pending retest | Source now uses Horizon-style per-operation distributions and rounded capacity |
| Account root | Medium-low -> pending retest | Source now serves trustlines, sequence fields, auth/liability fields, and sponsors when data is populated |
| Account transaction history | Low -> operational catch-up | Source avoids stale feed reads, but serving feed freshness must be verified after deployment |
| Operation collections | Low -> pending retest | Source now normalizes collection shape, numeric paging tokens, Soroban fields, account-scoped serving operations/payments, and historical operation ID fallback |
| Operation detail | Low -> pending retest | Source now queries hot first, then cold, and can compute operation IDs from transaction IDs |
| Effects | Low -> pending retest | Source now avoids hot+cold union for exact/recent paths and adds `sv_effects_by_account` for account pages, but public timeout behavior needs deploy/backfill smoke |

Overall: the compatibility layer is promising for transaction-resource lookup
and recent ledger lookup, but not yet ready as a broad Horizon replacement for a
heavy Horizon user. The next parity cycle should focus on collection JSON shape,
account feed freshness, operation detail, account root trustlines, and effects
performance before expanding to new route families.
