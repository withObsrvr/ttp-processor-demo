# Mainnet Serving Rollout Plan (2026-07-16)

## Purpose

Testnet now has the serving-backed behavior needed by Prism and the Query API:
fast smart-account authorization state, contract-storage explorer state,
account/effects feed support, account-ledger pruning, and newer hot/cold
pipeline fixes.

Mainnet is behind that testnet rollout. This document captures the current
mainnet gap and the recommended order to carry the testnet changes over without
overloading the single mainnet Nomad host or corrupting historical serving
coverage.

Related runbooks:

- [Testnet Required Backfills and Live Handoff](testnet-required-backfills-2026-07-15.md)
- [Smart Account Backfill and Serving Runbook](smart-account-backfill-serving-runbook-2026-07-16.md)

## Current State Summary

### Testnet

Testnet currently has the newer serving rollout:

- `stellar-query-api` has the smart-account and contract-storage summary routes.
- `serving-projection-processor` has the smart-account projector and shrink
  guard.
- `serving-projection-processor-fast` has live projectors for:
  - `tx_receipts`
  - `contract_storage`
  - `effects_by_account`
- `silver-realtime-transformer` has the checkpoint unblock and smart-account
  replay support.
- `silver-cold-flusher` has contract-storage tombstone support.
- `silver-current-state-projector-ttl` exists for contract-storage current
  state rebuilding.
- `serving-cold-backfill-by-account` exists for `sv_effects_by_account`.
- `serving-cold-backfill-contract-storage` exists for
  `sv_contract_storage_current` and `sv_contract_storage_summary`.
- `account-index-transformer` is deployed and running.
- `09-start-serving-and-query.sh` creates account-index checkpoint tables and
  deploys account-index-transformer.

Live testnet smoke:

```text
GET /api/v1/silver/smart-accounts/stats
HTTP 200
contract_count=13912
active_rule_count=21772
complete_thru=3630789
```

### Mainnet

Mainnet currently has older images and older job topology:

- `stellar-query-api` is on a June 30 image and returns `404` for
  `/api/v1/silver/smart-accounts/stats`.
- `serving-projection-processor` and `serving-projection-processor-fast` are on
  June 26 images.
- `serving-projection-processor-fast` still disables `tx_receipts` and does not
  include `contract_storage` or `effects_by_account`.
- `serving-projection-processor` does not include `smart_accounts`.
- `account-index-transformer` is missing.
- `account-index-transformer-backfill` is missing.
- Mainnet has an older monolithic `serving-cold-backfill` job rather than the
  testnet split backfills.
- Mainnet has `silver-current-state-projector`, but not the testnet
  contract-storage TTL/current-state projector job.
- Mainnet `09-start-serving-and-query.sh` only pre-creates
  `index.transformer_checkpoint`; it does not create account-index checkpoints
  or submit account-index-transformer.
- Mainnet `index-plane-transformer` is failing with exit code `137` under a
  3 GB memory limit while processing recent mainnet traffic.

Mainnet public smoke:

```text
GET /health
HTTP 200

GET /api/v1/silver/smart-accounts/stats
HTTP 404
```

Mainnet catalog state:

```text
index.contract_event_transformer_checkpoint
index.transformer_checkpoint
```

The account-index checkpoint tables are not present yet.

## Do Not Blindly Copy Testnet

Mainnet has much larger history and higher live throughput. The testnet jobs are
the right shape, but mainnet rollout needs different resource scheduling.

Do not run all of the following at the same time on the mainnet host:

- historical `stellar-history-loader`
- monolithic `serving-cold-backfill`
- account-index cold backfill
- contract-storage serving backfill
- current-state projector
- index-plane transformer maintenance or rebuild
- full live Query API traffic

The mainnet index transformer is already OOM-killed at 3 GB. Any rollout plan
must treat CPU, memory, DuckDB temp space, and S3/object-store pressure as shared
production resources.

## Desired Mainnet Target

Mainnet should converge to the testnet pattern:

### Live services

- `stellar-query-api` on the latest serving-aware image.
- `serving-projection-processor` with:
  - schema auto-apply,
  - `smart_accounts` enabled,
  - shrink guard enabled by default,
  - current/stat projectors as needed.
- `serving-projection-processor-fast` with:
  - recent ledgers,
  - recent transactions,
  - recent operations/events/explorer events,
  - recent contract calls,
  - `tx_receipts` only if the set-based/live-handoff implementation is proven
    safe for mainnet,
  - `contract_storage`,
  - `effects_by_account`.
- `account-index-transformer` with Postgres mirror and serving feed enabled.
- `contract-event-index-transformer`.
- `index-plane-transformer` stable under mainnet load.

### Backfill jobs

- Mainnet copy of `account-index-transformer-backfill`.
- Mainnet copy of `serving-cold-backfill-by-account`.
- Mainnet copy of `serving-cold-backfill-contract-storage`.
- Mainnet copy or updated version of `silver-current-state-projector-ttl`.

### Startup scripts

- Mainnet `09-start-serving-and-query.sh` should create:
  - `index.transformer_checkpoint`
  - `index.account_ledger_transformer_checkpoint`
  - `index.account_ledger_backfill_checkpoint`
- Mainnet startup should deploy account-index-transformer before
  `stellar-query-api`.

## Rollout Phases

### Phase 0: Freeze and Record Baseline

Do this before changing mainnet.

1. Record deployed Nomad image matrix:

   ```bash
   cd /home/tillman/Documents/infra/environments/prod/latitude/obsrvr-lake-mainnet/nomad
   source .secrets
   nomad job status
   ```

2. Record serving/index schemas:

   ```sql
   SELECT table_schema || '.' || table_name
   FROM information_schema.tables
   WHERE table_schema IN ('serving', 'index', 'ops')
   ORDER BY 1;
   ```

3. Record API smoke results:

   ```bash
   curl -sS https://obsrvr-lake-mainnet.withobsrvr.com/health
   curl -sS -w '\nHTTP %{http_code}\n' \
     https://obsrvr-lake-mainnet.withobsrvr.com/api/v1/silver/smart-accounts/stats
   ```

4. Record current live ledger and cold maxima for Bronze and Silver.

5. Record current CPU/memory allocation pressure:

   ```bash
   nomad node status -short
   nomad job status index-plane-transformer
   ```

Acceptance gate:

- We have a written baseline with job images, missing endpoints, schema state,
  and known unhealthy jobs.

### Phase 1: Prepare Mainnet Job Specs and Scripts

Make file-level changes first; do not deploy yet.

1. Add mainnet versions of missing testnet jobs:
   - `account-index-transformer.nomad`
   - `account-index-transformer-backfill.nomad`
   - `serving-cold-backfill-by-account.nomad`
   - `serving-cold-backfill-contract-storage.nomad`
   - `silver-current-state-projector-ttl.nomad`

2. Convert all network-specific values:
   - `testnet` to `mainnet`
   - `testnet_catalog` to `mainnet_catalog`
   - `obsrvr-lake-testnet` bucket to `obsrvr-lake-mainnet`
   - testnet passphrase to public passphrase where applicable
   - testnet host/IP to mainnet host/IP

3. Update mainnet `09-start-serving-and-query.sh` to:
   - create account-index checkpoint tables,
   - submit account-index-transformer,
   - keep query-api deployment last.

4. Update mainnet build/push script if needed so it reminds operators to update
   all affected jobs, not only Query API and serving processors.

5. Validate all Nomad jobs:

   ```bash
   nomad job validate account-index-transformer.nomad
   nomad job validate serving-cold-backfill-by-account.nomad
   nomad job validate serving-cold-backfill-contract-storage.nomad
   nomad job validate silver-current-state-projector-ttl.nomad
   ```

Acceptance gate:

- Mainnet job specs validate.
- No deployment has happened yet.
- A resource budget is assigned for each new job.

#### Phase 1 Preparation Result (2026-07-16)

Prepared in the mainnet infra directory:

- `nomad/account-index-transformer.nomad`
- `nomad/account-index-transformer-backfill.nomad`
- `nomad/serving-cold-backfill-by-account.nomad`
- `nomad/serving-cold-backfill-contract-storage.nomad`
- `nomad/silver-current-state-projector-ttl.nomad`
- `scripts/09-start-serving-and-query.sh`
- `scripts/00-build-and-push-serving-query-images.sh`

The three historical/current-state jobs and account-index backfill are
parameterized. Registering their jobspecs is inert; an operator must supply an
explicit `start_ledger`, `end_ledger`, and chunk/batch size to dispatch work.
This prevents an accidental full-mainnet backfill from a plain
`nomad job run`.

Credentials follow the existing mainnet convention:

- PostgreSQL credentials come from mainnet `nomad/.secrets`.
- B2 credentials are passed as HCL variables. The startup script reads the
  existing mainnet values from `silver-cold-flusher.nomad` without printing
  them.
- The new jobspecs do not embed additional credential literals.

Initial resource budgets:

| Job | Mode | CPU | Memory | Internal DuckDB limit |
| --- | --- | ---: | ---: | ---: |
| `account-index-transformer` | live service | 2,000 MHz | 4 GiB | process bounded by 2,500-ledger batches |
| `account-index-transformer-backfill` | parameterized batch | 2,000 MHz | 4 GiB | 2,500-ledger read batch; dispatch range controls history |
| `serving-cold-backfill-effects` | parameterized batch | 2,000 MHz | 8 GiB | 6 GB |
| `serving-cold-backfill-contract-storage` | parameterized batch | 3,000 MHz | 24 GiB | 12 GB |
| `silver-current-state-projector-contract-storage` | parameterized batch | 3,000 MHz | 24 GiB | 12 GB |

Operational constraint:

- Run only one DuckDB-heavy parameterized backfill at a time.
- Do not overlap the contract-storage serving backfill with the Silver
  current-state projector.
- Start mainnet backfills with a small recent range and increase only after
  measuring memory, temp-disk, S3, and PostgreSQL pressure.

Validation result:

- All five jobs passed `nomad job validate` against the mainnet Nomad agent.
- Both modified shell scripts passed `bash -n`.
- No Nomad job was submitted or dispatched during Phase 1 preparation.

### Phase 2: Deploy Code Images With Minimal Behavioral Change

Deploy newer images that are safe without historical backfills.

Recommended order:

1. `silver-realtime-transformer`
2. `silver-cold-flusher`
3. `postgres-ducklake-flusher`
4. `stellar-postgres-ingester`
5. `serving-projection-processor`
6. `serving-projection-processor-fast`
7. `stellar-query-api`

Important:

- Do not enable destructive smart-account shrink.
- Do not run historical serving backfills yet.
- Keep mainnet API health under observation.

Acceptance gate:

- `/health` remains `200`.
- Existing Prism-facing routes still work.
- New smart-account route may still return empty/404 until replay/backfill is
  complete. That is acceptable in this phase.

#### Phase 2 Deployment Result (2026-07-16)

Phase 2 is complete. The seven live jobs were deployed in the documented
order, and every latest Nomad deployment finished successfully with one healthy
allocation:

| Job | Deployed image |
| --- | --- |
| `silver-realtime-transformer` | `withobsrvr/silver-realtime-transformer:checkpoint-unblock-fd3b84b-20260715` |
| `silver-cold-flusher` | `withobsrvr/silver-cold-flusher:contract-storage-tombstone-flush-fd3b84b-20260715` |
| `postgres-ducklake-flusher` | `withobsrvr/postgres-ducklake-flusher:dl154-e271fdf-20260711135156` |
| `stellar-postgres-ingester` | `withobsrvr/stellar-postgres-ingester:horizon-compat-e271fdf-20260711142246` |
| `serving-projection-processor` | `withobsrvr/serving-projection-processor:52337f1-20260716032857` |
| `serving-projection-processor-fast` | `withobsrvr/serving-projection-processor:backfill-live-handoff-v4-fd3b84b-20260715` |
| `stellar-query-api` | `withobsrvr/stellar-query-api:contract-storage-summary-v2-fd3b84b-20260715` |

Mainnet feature flags were intentionally preserved:

- Smart-account serving was not enabled.
- Fast `tx_receipts`, `contract_storage`, `effects_by_account`, and account
  balance projectors were not enabled.
- Account-index pruning and the account-index serving feed remain disabled
  until Phase 5 establishes safe index coverage.
- No parameterized historical backfill or current-state rebuild was
  dispatched.

##### Compatibility repairs required during deployment

The newer live images exposed legacy mainnet schema drift that testnet had
already passed. The following nullable compatibility columns were added before
restarting the live transformer:

```sql
ALTER TABLE public.accounts_snapshot_v1
  ADD COLUMN IF NOT EXISTS sequence_ledger bigint,
  ADD COLUMN IF NOT EXISTS sequence_time bigint;

ALTER TABLE public.enriched_history_operations
  ADD COLUMN IF NOT EXISTS transaction_id bigint,
  ADD COLUMN IF NOT EXISTS operation_id bigint;

ALTER TABLE public.enriched_history_operations_soroban
  ADD COLUMN IF NOT EXISTS transaction_id bigint,
  ADD COLUMN IF NOT EXISTS operation_id bigint;
```

The first statement applies to `stellar_hot`; the latter two apply to
`silver_hot`.

The Silver transformer must connect directly to the Bronze PostgreSQL listener
on port `5432`. Using PgBouncer on port `6432` caused prepared-statement
failures with this image. The mainnet Nomad job now uses direct PostgreSQL and
`SILVER_QUERY_EXEC_MODE=exec`.

The legacy `serving.sv_account_balances_current` table had approximately
102 million rows but no unique key matching the projector's
`ON CONFLICT (account_id, asset_key)` clause. A concurrent unique index was
built and verified valid:

```sql
CREATE UNIQUE INDEX CONCURRENTLY sv_account_balances_current_uq
ON serving.sv_account_balances_current (account_id, asset_key);
```

The serving schema self-heal list and its parity test were updated so future
images repair this index automatically. The current deployed image does not
contain that source patch, so the verified production index must not be
dropped before a patched image is deployed.

##### Live processing verification

- `silver-realtime-transformer` caught up from its checkpoint and continues to
  process new two-ledger batches without transformation errors.
- `stellar-postgres-ingester` writes transactions, operations, effects, TTL,
  contract events/data, balances, and transfers at the live tail.
- `serving-projection-processor` successfully ran account balances after the
  unique-index repair and rebuilt current asset/contract tables.
- `serving-projection-processor-fast` advances recent ledger, transaction,
  operation, event, explorer-event, and contract-call checkpoints from the
  Silver gRPC trigger.
- `silver-cold-flusher` completed its startup flush, including
  `contract_data_deletions`, then resumed normal service.

##### API and Prism-facing smoke result

All checks below used current protocol-27 mainnet data:

| Route | Result | Observed total time |
| --- | --- | ---: |
| `/health` | `200`, all configured layers healthy | `0.12s` |
| `/api/v1/silver/ledgers/recent?limit=3` | `200`, current ledgers | `0.12s` |
| Gateway `/lake/v1/mainnet/api/v1/silver/ledgers/recent?limit=1` | authenticated `200` | `0.14s` |
| `/api/v1/silver/transactions/recent?limit=3` | `200` | `0.13s` |
| `/api/v1/silver/tx/{hash}/receipt` | `200`, complete 14-operation fixture | `0.11s` |
| `/api/v1/silver/tx/{hash}/diffs` | `200` | `0.12s` |
| `/api/v1/silver/tx/batch/decoded?ledger={sequence}&limit=5` | `200` | `0.46s` |
| `/api/v1/silver/explorer/account?account_id={account}` | `200` | `1.12s` |
| `/api/v1/silver/accounts/{account}/balances` | `200`, nine balances | `0.11s` |
| `/api/v1/horizon-compat/fee_stats` | `200` | `0.13s` |
| `/api/v1/home/summary` | `200`, non-partial | `3.44s` |
| `/api/v1/silver/smart-accounts/stats` | `200`, zero counts | `0.14s` |
| Prism `/v2/home` | `200`, live mainnet ledger data | `0.39s` |
| Prism `/v2/tx/{hash}` | `200` shell; all three HTMX data fragments `200` | `0.10-0.34s` |

The zero smart-account counts are expected in Phase 2 because the mainnet
smart-account projector and historical replay remain disabled. Query API also
logs two expected non-fatal startup conditions: the account-ledger index table
does not exist yet, and the initial index partition warm-up reaches its
60-second deadline. This produced a roughly 70-second deployment startup
window, after which Nomad health checks and public traffic were healthy.

Known post-deployment performance work remains outside this acceptance gate:

- Home summary is still an aggregate route in the 1.4-3.4 second range.
- Before the Phase 3 timeout work, most smart-wallet lookups were fast but
  some existing requests still took 3-9.5 seconds.
- Historical full-ledger reads can still take 5-10 seconds.
- Prism's generic transaction hero currently labels a classic multi-operation
  manage-offer transaction as "Called contract." The Query API classification
  and complete operation list are correct, so this is residual Prism
  presentation work rather than a serving-data failure.

These are not Phase 2 regressions, but they should remain visible during the
later serving/index phases.

### Phase 3: Repair Stabilization Before More Backfills

Stabilize the live deployment before adding more index or backfill pressure.
This phase includes the existing `index-plane-transformer` OOM loop plus the
deployment and latency issues identified during Phase 2.

#### 3.1 Stabilize the index-plane transformer

Before Phase 3, mainnet `index-plane-transformer` was in an OOM/restart loop.

Options:

1. Increase memory and reduce batch size if needed.
2. Temporarily pause `index-plane-transformer` during heavy backfills.
3. Run it only after hot ingestion is stable.
4. Add a dedicated backfill/maintenance window if the host cannot support live
   indexing plus backfills.

Minimum action:

- Stop the repeated OOM loop or raise memory enough that it can keep up with
  the live tail.

#### 3.2 Deploy the serving account-balance schema repair

The Phase 2 deployment required a manual unique index on the legacy
`serving.sv_account_balances_current` table. The source schema now includes
that index in its unique-index self-heal list, but the deployed projector image
does not contain the patch.

Actions:

1. Build and push a new `serving-projection-processor` image containing:
   - the `sv_account_balances_current_uq` self-heal entry,
   - its schema parity regression test.
2. Deploy the patched image to both regular and fast projector jobs as
   applicable without changing their mainnet feature flags.
3. Restart against the existing valid production index and confirm schema
   application is idempotent.
4. Verify the account-balances projector continues without
   `ON CONFLICT` constraint errors.

Do not drop or rebuild the existing 15 GB production index as part of this
deployment unless validation proves it invalid.

#### 3.3 Remove Query API warm-up from the readiness critical path

Phase 2 Query API startup took approximately 70 seconds because an optional
index partition scan consumed its 60-second deadline. The missing account index
was handled as non-fatal, but optional warm-up should not delay a healthy API
from serving hot and serving-table routes.

Actions:

1. Make optional DuckDB/index warm-up asynchronous after the HTTP listener is
   ready, or give it a short independent startup budget.
2. Skip account-ledger index warm-up entirely when pruning and the serving feed
   are disabled.
3. Preserve clear logs and health metadata showing whether each optional index
   is unavailable, warming, ready, or degraded.
4. Ensure a warm-up timeout cancels its query cleanly without emitting a
   misleading process-level interrupt error.
5. Add a restart test with the account-ledger table absent.

Target:

- Query API should become healthy within 15 seconds when PostgreSQL and the
  required serving sources are available, even if optional DuckDB indexes are
  absent or slow.

#### 3.4 Establish explicit latency gates

Record repeated warm and cold measurements through both the direct mainnet URL
and authenticated Gateway path.

Initial interactive targets:

| Route class | p95 target | Hard timeout |
| --- | ---: | ---: |
| Health, recent ledgers/transactions, receipts, balances | `<500ms` | `2s` |
| Account overview and decoded transaction batch | `<2s` | `4s` |
| Home summary | `<3s` | `4s` |
| Smart-wallet lookup | `<2s` | `4s` |
| Historical ledger detail | `<4s` | `6s` |

If smart-wallet or historical-ledger routes cannot meet these targets without
later serving/index backfills, record the dependency and return an explicit
partial/degraded response within the hard timeout rather than allowing Gateway
502/504 failures.

Use the focused acceptance harness:

```bash
cd obsrvr-lake/stellar-query-api
export OBSRVR_GATEWAY_API_KEY='<gateway API key>'

python3 scripts/phase3_latency_gate.py \
  --warm-runs 5 \
  --historical-ledger 40000000 \
  --out-prefix /tmp/mainnet-serving-phase3-latency
```

The script discovers current transaction, account, and smart-wallet fixtures,
runs one cold-first request plus repeated warm requests through the direct and
Gateway paths, rejects non-2xx and Gateway `502`/`504` responses, and writes
CSV, JSON, and Markdown evidence.

The pre-deploy baseline is recorded in
[Phase 3 Mainnet Serving Latency Gate: Pre-Deploy Baseline](../stellar-query-api/docs/mainnet-phase3-latency-predeploy-2026-07-16.md).
It found:

- serving-table, receipt, decoded-batch, home, and smart-wallet routes within
  their hard limits;
- account overview bounded below four seconds but slightly above its initial
  two-second p95 target;
- an uncached direct historical-ledger request exceeding the six-second hard
  limit because the handler retained a ten-second internal query budget.

The deployed Phase 3 Query API uses explicit budgets and returns marked
partial responses when optional sections cannot finish:

```text
QUERY_API_HOME_SUMMARY_TIMEOUT=2500ms
QUERY_API_SMART_WALLET_TIMEOUT=900ms
QUERY_API_LEDGER_FULL_TIMEOUT=3500ms
```

The ledger composite collector must stop waiting when its context expires.
Canceling the database queries alone is insufficient because a worker may take
additional time to observe cancellation. Smart-wallet detection similarly
returns `200` with `partial=true` and a warning when its chained evidence
lookups exhaust the route budget.

#### 3.5 Fix the Prism multi-operation hero classification

Prism currently renders a classic multi-operation manage-offer transaction as
"Called contract" even though the Query API correctly classifies it as
`multi_op` and returns all operations.

Actions:

1. Add a Prism fixture using the Phase 2 transaction shape: a successful
   classic transaction with multiple `manage_buy_offer` and
   `manage_sell_offer` operations and no contract invocation.
2. Route `multi_op` transactions without Soroban operations to an appropriate
   classic/multi-operation hero.
3. Keep the operation list and generic fallback usable when no more specific
   semantic hero applies.
4. Test the initial page and all HTMX transaction fragments.

Acceptance gate:

- `nomad job status index-plane-transformer` is running and not in repeated
  exit-137 backoff, or it is intentionally stopped with a documented reason.
- Both serving projector jobs run a patched image containing the account
  balance unique-index self-heal, and account-balance projection succeeds.
- Query API reaches healthy readiness within 15 seconds with the account-ledger
  index absent.
- The direct API and Gateway latency audit is recorded, with no tested
  interactive route exceeding its hard timeout.
- Prism renders the classic multi-operation fixture without labeling it as a
  contract call.
- No historical backfill is started until these stabilization gates pass.

#### 3.6 Phase 3 implementation and deployment record

Phase 3 was implemented and deployed on `2026-07-16`.

Published immutable images:

| Component | Immutable tag | Digest |
| --- | --- | --- |
| Index transformer | `withobsrvr/index-plane-transformer:phase3-19730c2-20260716` | `sha256:572e6f0a497f657e5c59fc9bf3f7fa4eaeb61b92479ced8ac30a9b0002394b66` |
| Serving projector | `withobsrvr/serving-projection-processor:phase3-19730c2-20260716` | `sha256:0181cf1522dc07587311a5e8686ce72ea161589aee4e43069beda5c028bc543e` |
| Query API | `withobsrvr/stellar-query-api:phase3-final-19730c2-20260716` | `sha256:4add6717e28fd17c613cadd0a28edd60c12b8fa4ca1c5967151a85c7af26f82e` |
| Prism | `withobsrvr/prism:phase3-4dc2697-20260716` | `sha256:61e72f3fe25710e9e5cd04e0976d17d253b5c3492344eb623d85cd5fffd39103` |

Deployment evidence:

- Regular and fast serving projector jobs are healthy at job version `9`
  (`b874396c` and `3968f80d`).
- Schema application created or retained
  `serving.sv_account_balances_current_uq(account_id, asset_key)`.
  Projector logs contain no subsequent account-balance `ON CONFLICT` errors.
- Query API is healthy at job version `17` (`29ead8da`).
  The listener became usable before optional warm-up completed. `/health`
  reports transaction and contract indexes `ready` and the account-ledger
  index `disabled`, as intended while pruning and feed reads are disabled.
- `index-plane-transformer` is healthy at job version `4` (`8715dc46`) with
  zero restarts. At `2026-07-16T23:04:49Z` it reported ledger `63509755`,
  13 seconds of lag, 381 transformations, zero transformation errors, and no
  checkpoint or retention gap.
- The index transformer uses `bronze_source.mode: poll` on mainnet. The
  upstream PostgreSQL ingester had stopped its gRPC listener after a source
  archive error while leaving its health listener alive. Restarting the
  ingester resumed ingestion, but poll mode removes that gRPC lifecycle from
  the index transformer's live-tail critical path. This is the required
  mainnet setting until the ingester replaces unbounded gRPC
  `GracefulStop()` with bounded shutdown and health reflects ingestion state.
- The index allocation uses `batch_size=100`, `cpu=4000`, and `memory=4096`.
  It has remained stable for more than three hours, although observed memory
  reached 3.8 GiB; retain a memory alert and do not add historical backfill
  load to this allocation.
- The public DigitalOcean Prism deployment renders transaction
  `fde8ca4f463e5522c714b1b7b048b183c2a3eb915c34293739c30d777fa49d0e`
  as a successful classic `multi_op` with two operations and no contract-call
  label. The local full-page and HTMX fragment tests also pass.

The final direct and authenticated Gateway audit is recorded in
[Phase 3 Mainnet Serving Latency Gate: Post-Deploy](../stellar-query-api/docs/mainnet-phase3-latency-postdeploy-2026-07-16.md).
All 120 requests returned `2xx` within their hard limits and no Gateway
`502`/`504` occurred. Eighteen of twenty route/target combinations met the
initial p95 target. Account overview was the only miss: direct exceeded its
two-second target by 148 ms and Gateway exceeded it by 116 ms. Both remained
well inside the four-second hard limit.

The acceptance audit must use a freshly discovered transaction. A transaction
fixed early in the rollout later aged out of Silver Hot and had no historical
transaction-index row (`ledger_hint=0`), correctly exposing the still-pending
index backfill dependency. That stale-fixture cold scan is not evidence that
the current live path regressed.

One audit attempt saw four client-side Python TLS handshake timeouts before an
HTTP response. A separate 20-connection `curl --no-keepalive` check returned
20/20 Gateway `200` responses with TLS setup normally below 50 ms, and the
clean repeated acceptance run had no transport failures.

Phase 3 go/no-go result: **PASS** for deployment stabilization and interactive
hard latency gates. Historical account, transaction-index, and smart-account
backfills remain separate later phases and must use their runbooks and
resource gates; they were not started as part of Phase 3.

### Phase 4: Smart Account Historical Replay

Smart-account serving cannot be restored by the live projector alone.

Follow [Smart Account Backfill and Serving Runbook](smart-account-backfill-serving-runbook-2026-07-16.md).

Recommended mainnet order:

1. Confirm smart-account Silver migration exists.
2. Stop or disable serving smart-account projector during replay if existing
   serving rows are meaningful.
3. Run narrow smart-account replay from Bronze cold:

   ```text
   silver-realtime-transformer --smart-account-replay
   ```

4. Verify Silver source tables:
   - `smart_account_context_rules`
   - `smart_account_signers`
   - `smart_account_policies`

5. Start `serving-projection-processor` with `smart_accounts` enabled.
6. Verify serving tables:
   - `serving.sv_smart_account_contracts`
   - `serving.sv_smart_account_signers`
   - `serving.sv_smart_account_rules_current`

Acceptance gate:

- `/api/v1/silver/smart-accounts/stats` returns `200`.
- Counts are plausible for mainnet, not near zero.
- A known mainnet OpenZeppelin smart account fixture returns rules.

### Phase 5: Account Index Rollout

Goal:

- Enable fast account-history pruning and account feed serving tables.

Order:

1. Create account-index checkpoint tables in mainnet catalog.
2. Deploy `account-index-transformer` for the live tail.
3. Verify `/health` on the transformer.
4. Run `account-index-transformer-backfill` in bounded ranges.

Mainnet backfill strategy:

- Do not start with full-history end-to-end.
- Backfill recent ranges first to validate correctness and throughput.
- Use resumable checkpoints.
- Prefer off-peak windows.
- Track rows/sec, ledgers/sec, DuckDB temp usage, S3 request pressure, and
  Postgres write pressure.

Acceptance gate:

- `index.account_ledger_index` exists.
- Postgres mirror exists if enabled.
- Transformer health shows zero errors.
- Query API account-history paths use pruning only when index coverage is safe.

### Phase 6: Contract Storage Current and Serving Backfill

Goal:

- Enable contract storage explorer endpoints to rely on serving current/summary
  rows rather than slow scans.

Order:

1. Deploy `silver-current-state-projector-ttl` mainnet job but run it in an
   explicit backfill/maintenance window.
2. Run contract-storage current-state rebuild from Bronze authoritative source.
3. Run `serving-cold-backfill-contract-storage` for bounded ranges.
4. Enable live `contract_storage` projector.
5. Verify watermarks hand off without gaps.

Mainnet-specific caution:

- Full mainnet contract storage will be much larger than testnet.
- Use bucketed/resumable publication.
- Do not run with other DuckDB-heavy jobs unless resources are isolated.

Acceptance gate:

- `serving.sv_contract_storage_current` exists and has no duplicate
  `(contract_id, key_hash)` groups.
- `serving.sv_contract_storage_summary` exists.
- Large-contract storage endpoint returns under interactive timeout.
- Live tombstones remove rows correctly after the backfill cutoff.

### Phase 7: Effects-by-Account Serving Backfill

Goal:

- Enable fast account activity/effects history for Prism and analyst routes.

Order:

1. Run `serving-cold-backfill-by-account` for a small recent mainnet range.
2. Validate row counts and watermarks.
3. Expand to longer ranges in planned chunks.
4. Enable live `effects_by_account` projector.

Acceptance gate:

- `serving.sv_effects_by_account` has contiguous coverage for the selected
  range.
- Account effects endpoints return quickly for known active mainnet accounts.
- Watermark advances with the live transformer checkpoint.

### Phase 8: Query API Cutover and Prism Validation

Deploy the latest Query API only after the serving sources it depends on are
present.

Minimum smoke tests:

```bash
curl -sS https://obsrvr-lake-mainnet.withobsrvr.com/health
curl -sS https://obsrvr-lake-mainnet.withobsrvr.com/api/v1/silver/smart-accounts/stats
curl -sS 'https://obsrvr-lake-mainnet.withobsrvr.com/api/v1/silver/contracts?limit=5'
curl -sS 'https://obsrvr-lake-mainnet.withobsrvr.com/api/v1/silver/ledgers/recent?limit=3'
```

Prism validation:

- `/v2/home`
- mainnet account page for active G-address
- mainnet smart-account page for known OpenZeppelin C-address
- transaction page with receipt available
- transaction page with receipt not yet materialized
- contract storage page for a large contract

Acceptance gate:

- No 15s App Platform 502/504 loops for normal interactive pages.
- Missing historical data is represented as partial/degraded, not mock success.
- Mainnet endpoints that rely on backfilled serving tables show sane coverage.

## Backfill Scheduling Strategy

Use this priority order:

1. Smart-account replay and serving rebuild, because it is narrow and unblocks
   Prism smart-account pages.
2. Account-index live transformer, because it improves query routing without a
   full historical backfill.
3. Account-index recent backfill.
4. Contract-storage current/summary backfill.
5. Effects-by-account backfill.
6. Longer account-index historical backfill.
7. Longer effects historical backfill.

For mainnet, prefer chunked historical ranges:

```text
recent 7 days -> recent 30 days -> recent 90 days -> older history
```

This gets Prism useful quickly while reducing risk from a multi-day full-chain
backfill.

## Monitoring During Rollout

Watch these continuously:

- Nomad allocation restarts and exit code `137`.
- PostgreSQL memory and disk.
- `/mnt/data/duckdb_cache` or configured DuckDB temp directory.
- Backfill manifest and summary files.
- Serving watermarks.
- Query API 5xx rate and latency.
- Gateway 502/504s.
- Prism logs for `context deadline exceeded`.

Useful Nomad commands:

```bash
nomad job status
nomad job status <job>
nomad alloc status <alloc>
nomad alloc logs <alloc> <task>
nomad alloc logs -stderr <alloc> <task>
```

Useful API checks:

```bash
curl -sS -w '\nHTTP %{http_code} total %{time_total}\n' \
  https://obsrvr-lake-mainnet.withobsrvr.com/health

curl -sS -w '\nHTTP %{http_code} total %{time_total}\n' \
  https://obsrvr-lake-mainnet.withobsrvr.com/api/v1/silver/smart-accounts/stats
```

## Rollback Plan

Rollback is image/job based.

For each deployed job, record the previous image before rollout. If a phase
fails:

1. Stop the new backfill job first.
2. Revert live serving/query jobs to previous image tags.
3. Keep newly-created tables unless they are proven corrupt; empty or incomplete
   serving tables can be ignored while the API is reverted.
4. Do not drop index/serving schemas during incident rollback.
5. If a serving shrink or destructive rebuild happened accidentally, stop the
   projector and restore from backfill/replay rather than trying to patch rows
   manually.

The safest rollback boundary is between phases. Do not start the next phase
until the previous phase has a written acceptance result.

## Open Questions Before Execution

1. What mainnet fixture smart accounts should we use for acceptance?
2. Do we have enough spare memory to run account-index live plus Query API plus
   serving live projectors?
3. Should the existing mainnet monolithic `serving-cold-backfill` be retired or
   kept for legacy current-state rebuilds?
4. What is the acceptable time window for each historical backfill chunk?
5. Do we want to prioritize recent data only for Prism first, then complete
   deeper history later?

## Initial Recommendation

Do the rollout in two separate work sessions.

Session 1:

1. Prepare/validate mainnet job specs.
2. Deploy updated live images.
3. Fix or intentionally pause the OOMing index-plane-transformer.
4. Deploy account-index-transformer live only.
5. Deploy Query API after confirming route availability.

Session 2:

1. Smart-account replay and serving rebuild.
2. Recent account-index backfill.
3. Recent contract-storage backfill.
4. Recent effects-by-account backfill.
5. Prism smoke tests.

Then schedule full historical backfills as background operations with explicit
chunk ranges and daily checkpoints.
