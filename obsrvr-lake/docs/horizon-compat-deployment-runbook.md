# Horizon Compatibility Deployment Runbook

Status: current after Cycle 5B testnet rollout on 2026-07-10.

This runbook covers the repeatable deployment path for the Horizon compatibility
surface in `stellar-query-api`, including the serving projection and data-plane
changes needed for account, operation, effect, and transaction parity.

## Scope

Services involved:

- `stellar-query-api`
- `stellar-history-loader`
- `stellar-postgres-ingester`
- `postgres-ducklake-flusher`
- `silver-history-loader`
- `silver-realtime-transformer`
- `silver-current-state-projector`
- `serving-projection-processor`
- `serving-cold-backfill`
- `silver-cold-flusher`
- `account-index-transformer`

Serving tables involved:

- `serving.sv_transactions_recent`
- `serving.sv_accounts_current`
- `serving.sv_account_balances_current`
- `serving.sv_transactions_by_account`
- `serving.sv_operations_by_account`
- `serving.sv_effects_by_account`

Public API prefix:

- `/api/v1/horizon-compat`

Gateway path:

- `/lake/v1/{network}/api/v1/horizon-compat`

No Gateway route change is required for this compatibility layer.

## Preconditions

1. Source changes are committed.
2. The same source revision is used for all three images.
3. PostgreSQL serving schema is reachable.
4. Bronze DuckLake contains the transaction XDR fields from `transactions_row_v2`.
5. Bronze and Silver cold tables have populated `ledger_range` values. Horizon
   exact ledger and effect routes use this field for cold partition pruning.
6. `serving.sv_transactions_recent` has the Cycle 5B columns:

```text
transaction_id
tx_envelope
tx_result
tx_meta
tx_fee_meta
tx_signers
```
7. PostgreSQL and Silver account schemas have the Horizon account sequence
   columns:

```text
sequence_ledger
sequence_time
```
8. Serving current account/balance tables have the Horizon account-root columns
   for sponsorship, auth flags, trustline liabilities, clawback, and
   last-modified ledger.
9. `account-index-transformer` has `serving_feed.enabled=true` so
   `serving.sv_transactions_by_account` and
   `serving.sv_operations_by_account` keep advancing after the historical
   catch-up.
10. Postgres serving has the operation-feed indexes required by Horizon
    account/transaction/global operation pages:

```sql
create index if not exists sv_operations_by_account_operation_idx
  on serving.sv_operations_by_account (operation_toid);

create index if not exists sv_operations_by_account_payment_page_idx
  on serving.sv_operations_by_account (account_id, operation_toid desc)
  where is_payment_op = true;
```

Do not add the partial payment index to DuckDB-backed cold-backfill index
creation; DuckDB does not support partial indexes. Keep the partial index in the
Postgres serving schema and the live `account-index-transformer` ensure path.

## Build And Push

Use one tag for all three images so the deployed set is auditable:

```bash
export TAG="cycle5b-horizon-$(git rev-parse --short HEAD)-$(date -u +%Y%m%d%H%M)"

docker build -t "withobsrvr/stellar-query-api:${TAG}" obsrvr-lake/stellar-query-api
docker build -t "withobsrvr/stellar-history-loader:${TAG}" obsrvr-lake/stellar-history-loader
docker build -t "withobsrvr/stellar-postgres-ingester:${TAG}" obsrvr-lake/stellar-postgres-ingester
docker build -t "withobsrvr/postgres-ducklake-flusher:${TAG}" obsrvr-lake/postgres-ducklake-flusher
docker build -t "withobsrvr/silver-history-loader:${TAG}" obsrvr-lake/silver-history-loader
docker build -t "withobsrvr/silver-realtime-transformer:${TAG}" obsrvr-lake/silver-realtime-transformer
docker build -t "withobsrvr/silver-current-state-projector:${TAG}" obsrvr-lake/silver-current-state-projector
docker build -t "withobsrvr/serving-projection-processor:${TAG}" obsrvr-lake/serving-projection-processor
docker build -t "withobsrvr/serving-cold-backfill:${TAG}" obsrvr-lake/serving-cold-backfill
docker build -t "withobsrvr/silver-cold-flusher:${TAG}" obsrvr-lake/silver-cold-flusher
docker build -t "withobsrvr/account-index-transformer:${TAG}" obsrvr-lake/account-index-transformer

docker push "withobsrvr/stellar-query-api:${TAG}"
docker push "withobsrvr/stellar-history-loader:${TAG}"
docker push "withobsrvr/stellar-postgres-ingester:${TAG}"
docker push "withobsrvr/postgres-ducklake-flusher:${TAG}"
docker push "withobsrvr/silver-history-loader:${TAG}"
docker push "withobsrvr/silver-realtime-transformer:${TAG}"
docker push "withobsrvr/silver-current-state-projector:${TAG}"
docker push "withobsrvr/serving-projection-processor:${TAG}"
docker push "withobsrvr/serving-cold-backfill:${TAG}"
docker push "withobsrvr/silver-cold-flusher:${TAG}"
docker push "withobsrvr/account-index-transformer:${TAG}"
```

Record the image digests in the rollout note.

## Apply Schemas

Apply the additive PostgreSQL migrations before restarting live writers:

```bash
psql "$BRONZE_POSTGRES_DSN" -f obsrvr-lake/stellar-postgres-ingester/migrations/008_add_transaction_xdr_fields.sql
psql "$BRONZE_POSTGRES_DSN" -f obsrvr-lake/stellar-postgres-ingester/migrations/009_add_account_sequence_ledger_time.sql
psql "$SILVER_POSTGRES_DSN" -f obsrvr-lake/silver-realtime-transformer/migrations/010_add_account_sequence_ledger_time.sql
```

Rebuild and redeploy `postgres-ducklake-flusher` before its first flush after
applying migration 009: Postgres appends `sequence_ledger`/`sequence_time` at
the end of `accounts_snapshot_v1`, and older flusher builds read that table
positionally, which would shift every value after `sequence_number` into the
wrong DuckLake column. The updated flusher uses an explicit column list and
adds the two columns to existing DuckLake tables on startup.

Deploy or run `serving-projection-processor` with the image built above. The
projector applies the serving schema idempotently, including `ALTER TABLE ...
ADD COLUMN IF NOT EXISTS` for the transaction hydration, account-root, and
trustline balance fields.

Manual local form:

```bash
cd obsrvr-lake/serving-projection-processor/go
go run . -config ../config.yaml -apply-schema-only
```

After schema apply, verify:

```sql
select column_name
from information_schema.columns
where table_schema = 'serving'
  and table_name = 'sv_transactions_recent'
  and column_name in (
    'transaction_id',
    'tx_envelope',
    'tx_result',
    'tx_meta',
    'tx_fee_meta',
    'tx_signers'
  )
order by column_name;
```

Verify account serving columns:

```sql
select table_name, column_name
from information_schema.columns
where table_schema = 'serving'
  and table_name in ('sv_accounts_current', 'sv_account_balances_current')
  and column_name in (
    'sequence_ledger',
    'sequence_time',
    'sponsor',
    'auth_required',
    'auth_revocable',
    'auth_immutable',
    'auth_clawback_enabled',
    'buying_liabilities_stroops',
    'selling_liabilities_stroops',
    'is_authorized_to_maintain_liabilities',
    'is_clawback_enabled',
    'last_modified_ledger'
  )
order by table_name, column_name;
```

## Backfill `sv_transactions_recent`

Run `serving-cold-backfill` for the recent transaction window that must support
Horizon transaction hydration.

Example:

```bash
serving-cold-backfill \
  --network testnet \
  --start-ledger 3000000 \
  --end-ledger 3534034 \
  --chunk-size 100000 \
  --bronze-ducklake-catalog "$BRONZE_DUCKLAKE_CATALOG" \
  --bronze-data-path "$BRONZE_DATA_PATH" \
  --silver-ducklake-catalog "$SILVER_DUCKLAKE_CATALOG" \
  --silver-data-path "$SILVER_DATA_PATH" \
  --target-postgres "$TARGET_POSTGRES" \
  --serving-schema serving \
  --feed-projections sv_transactions_recent \
  --skip-current \
  --manifest-path /var/lib/flowctl/serving-transactions-recent-manifest.jsonl \
  --resume
```

For the Cycle 5B testnet rollout, the successful backfill wrote these chunk
counts:

```text
3000000..3099999  606097
3100000..3199999  778758
3200000..3299999  854683
3300000..3399999  762011
3400000..3499999  1029022
3500000..3534034  392004
```

Checkpoint verification:

```sql
select projection_name, network, last_ledger_sequence, last_closed_at, updated_at
from serving.sv_projection_checkpoints
where projection_name = 'sv_transactions_recent';
```

Target result from Cycle 5B testnet:

```text
sv_transactions_recent|testnet|3534034
```

Target transaction hydration probe:

```sql
select ledger_sequence,
       length(tx_envelope),
       length(tx_result),
       length(tx_meta),
       length(tx_fee_meta),
       tx_signers is not null
from serving.sv_transactions_recent
where tx_hash = '366bc4543a8fe66e09c021af35377c78df6e90e57f85582a0aad1617fcc027e8';
```

Expected Cycle 5B testnet result:

```text
3177525|872|88|4028|400|t
```

## Backfill Account And Operation Data

The Horizon account and operation fixes are not purely query-side. Existing
historical rows must be regenerated or backfilled so the new fields are
populated.

Required data refresh:

- Bronze account rows: `sequence_ledger` and `sequence_time`.
- Silver current accounts: account sequence ledger/time, sponsor, auth flags,
  sponsoring counts.
- Silver trustlines: buying/selling liabilities, sponsor, maintain-liabilities
  auth, clawback, last-modified ledger.
- Historical enriched operations: `transaction_id` and `operation_id`.
- Cold ledgers/effects: populated `ledger_range` for partition-pruned exact
  Horizon ledger and effects reads.
- Serving current accounts and balances.
- `serving.sv_operations_by_account` for account-scoped Horizon operations and
  payments.
- `serving.sv_effects_by_account` for account-scoped Horizon effects. Run the
  full-history backfill first; the live `effects_by_account` projector only
  advances an existing complete watermark so it cannot accidentally advertise a
  hot-window-only table as complete history.

For a clean testnet parity refresh, prefer a full testnet replay from the
current oldest ledger through the chosen cutoff. If time is constrained, a
targeted validation replay must at least cover the fixture account and fixture
transaction ledgers used by the public comparison.

After the refresh, verify:

```sql
select count(*)
from serving.sv_accounts_current
where sequence_ledger is not null
  and sequence_time is not null;

select count(*)
from serving.sv_account_balances_current
where asset_type <> 'native'
  and last_modified_ledger is not null;

select count(*)
from serving.sv_operations_by_account
where operation_toid is not null;

select count(*)
from serving.sv_effects_by_account
where operation_toid is not null;

select table_name, status, complete_from, complete_thru
from serving.sv_watermarks
where table_name in (
  'serving.sv_transactions_by_account',
  'serving.sv_operations_by_account',
  'serving.sv_effects_by_account'
);
```

## Maintain By-Account Serving Feeds

`serving.sv_transactions_by_account` and
`serving.sv_operations_by_account` are maintained by
`account-index-transformer` when `serving_feed.enabled=true`.

Minimal config block:

```yaml
serving_feed:
  enabled: true
  schema: serving
  transactions_table: sv_transactions_by_account
  operations_table: sv_operations_by_account
  watermark_table: serving.sv_watermarks
  consumer_table: ops.consumers
  pipeline: account-index-transformer.account-feed
```

Deploy `account-index-transformer` after the by-account catch-up and verify the
live writer is active:

```text
Serving feed sink: enabled (serving.sv_transactions_by_account)
Serving feed wrote ... account operation rows
```

Verification:

```sql
select table_name, status, complete_from, complete_thru
from serving.sv_watermarks
where table_name in (
  'serving.sv_transactions_by_account',
  'serving.sv_operations_by_account'
)
order by table_name;

select c.relname, i.indisvalid, i.indisready
from pg_class c
join pg_index i on i.indexrelid = c.oid
where c.relname in (
  'sv_operations_by_account_operation_idx',
  'sv_operations_by_account_payment_page_idx'
)
order by c.relname;
```

Expected shape after a full-history testnet catch-up:

```text
serving.sv_operations_by_account|complete|3|<current-ledger>
serving.sv_transactions_by_account|complete|3|<current-ledger>
sv_operations_by_account_operation_idx|t|t
sv_operations_by_account_payment_page_idx|t|t
```

Important: `serving-cold-backfill` currently writes `complete_from` as the
range start for the run. If you run a catch-up like `3466466..3539759` after a
full-history backfill, restore `complete_from` to the true full-history floor
after the job completes:

```sql
update serving.sv_watermarks
set complete_from = 3, updated_at = now()
where table_name in (
  'serving.sv_transactions_by_account',
  'serving.sv_operations_by_account'
)
  and complete_from > 3;
```

Do this only when the earlier ledger range is already populated. Otherwise the
query API will correctly avoid serving-feed reads for ranges the table does not
cover.

## Deploy `stellar-query-api`

Deploy `stellar-query-api` from the same tag used for the serving processor and
backfill. This version prefers `serving.sv_transactions_recent` for Horizon
transaction hydration and returns `503 data_unavailable` rather than partial
transaction resources when required XDR is missing.

Required environment on the `stellar-query-api` job:

- `ACCOUNT_TX_FEED_ENABLED=true` — the `/accounts/{id}/transactions` serving
  feed path is gated on this flag and it defaults to **off**; without it every
  request silently takes the slower federated path.
- `NETWORK_PASSPHRASE` — e.g. `Test SDF Network ; September 2015`. Needed to
  derive fee-bump inner transaction hashes; without it fee-bump transactions
  are served without their `inner_transaction` block (logged at request time).

Nomad example (the jobspec lives in the infra repository under
`nomad/obsrvr-lake/`):

```bash
nomad job run stellar-query-api.nomad
nomad job status stellar-query-api
```

Acceptance:

- latest deployment is `successful`
- desired allocation count is healthy
- `/health` returns `200`

## Public Smoke

Use the checked-in smoke script:

```bash
python3 obsrvr-lake/stellar-query-api/scripts/horizon_compat_smoke.py \
  --base-url https://obsrvr-lake-testnet.withobsrvr.com/api/v1/horizon-compat \
  --account GBTHMMFWTAPFAHRGS33LKETZYJKBTNEENRN47EDZMZPT2BNCJO47GVQG \
  --tx-hash 366bc4543a8fe66e09c021af35377c78df6e90e57f85582a0aad1617fcc027e8
```

Gateway smoke:

```bash
python3 obsrvr-lake/stellar-query-api/scripts/horizon_compat_smoke.py \
  --base-url https://gateway.withobsrvr.com/lake/v1/testnet/api/v1/horizon-compat \
  --api-key "$API_KEY" \
  --account GBTHMMFWTAPFAHRGS33LKETZYJKBTNEENRN47EDZMZPT2BNCJO47GVQG \
  --tx-hash 366bc4543a8fe66e09c021af35377c78df6e90e57f85582a0aad1617fcc027e8
```

Cycle 5B acceptance requires:

- `/fee_stats` returns `200`
- `/ledgers?limit=1&order=desc` returns `200`
- `/accounts/{id}` returns `200`
- `/accounts/{id}/transactions?limit=1&order=desc` returns `200`
- `/transactions/{hash}` returns `200`
- both transaction responses include non-empty `envelope_xdr`, `result_xdr`,
  `result_meta_xdr`, `fee_meta_xdr`, and at least one signature

Then run the Horizon comparison harness:

```bash
python3 obsrvr-lake/stellar-query-api/scripts/horizon_compat_accuracy.py \
  --timeout 20
```

This compares the implemented high-priority routes against
`https://horizon-testnet.stellar.org`. Rollout is not complete until known
failed routes are either fixed or explicitly documented as accepted residual
gaps.

## Rollback

If the query API deploy fails before public smoke:

1. Revert the `stellar-query-api` Nomad image tag to the previous known-good tag.
2. Leave the serving schema columns in place; they are additive.
3. Leave the `sv_transactions_recent` backfill data in place unless it is
   proven corrupt.

If serving writes fail:

1. Stop the backfill batch.
2. Inspect `component.failed` JSON logs for the failing chunk and projection.
3. Fix the source/backfill issue.
4. Rerun with `--resume`.

Do not deploy a query API version that prefers serving transaction hydration
until the serving schema and backfill are verified.

## Related Docs

- `obsrvr-lake/stellar-query-api/docs/HORIZON_COMPAT_API.md`
- `obsrvr-lake/docs/testnet-rollout-followup-cycles-2026-07-09.md`
- `obsrvr-lake/docs/horizon-parity-implementation-plan-2026-07-09.md`
- `obsrvr-lake/serving-projection-processor/README.md`
- `obsrvr-lake/serving-cold-backfill/README.md`
