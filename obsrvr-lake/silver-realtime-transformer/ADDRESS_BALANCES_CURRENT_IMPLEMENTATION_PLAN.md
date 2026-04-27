# address_balances_current Performance Fix Implementation Plan

## Problem

Nomad logs show `address_balances_current` dominates silver-realtime-transformer runtime:

- All non-balance transforms complete in ~15–25 seconds per 500-ledger batch.
- `address_balances_current` takes ~10–12 minutes while only writing ~2.5k–2.8k rows.

The current implementation in `go/transformer.go` deletes all balances for impacted owners, then recomputes token balances by scanning historical `token_transfers_raw` for those owners. That work is not bounded by the current ledger window, so it gets slower as history grows.

## Target Design

Make `address_balances_current` a bounded, state-first projection:

1. Native XLM balances come from `native_balances_current`.
2. Classic asset balances come from `trustlines_current`.
3. Soroban/token state balances come from decoded `contract_data_snapshot_v1` via the existing `transformAddressBalancesFromContractState` path.
4. Event-derived `token_transfers_raw` deltas are optional fallback only, and must be bounded to the current ledger range.

The hot path must not scan all historical token transfers for every impacted account.

## Success Criteria

For a normal 500-ledger hot batch:

- `address_balances_current` completes in seconds, not minutes.
- Total batch runtime is close to the sum of the other transforms, ideally < 30s under current observed load.
- `address_balances_current` remains idempotent for reprocessed ledger windows.
- Checkpoint advancement is no longer blocked for ~10+ minutes by balance reconstruction.

## Phase 0: Measure Current Query

Before changing behavior, capture the current execution plan on production/testnet or a representative copy.

### SQL

Run `EXPLAIN (ANALYZE, BUFFERS, VERBOSE)` for the current `transformAddressBalancesCurrent` `insertTokens` query.

Also inspect `pg_stat_statements`:

```sql
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;

SELECT
  calls,
  total_exec_time,
  mean_exec_time,
  rows,
  query
FROM pg_stat_statements
WHERE query ILIKE '%address_balances_current%'
   OR query ILIKE '%token_transfers_raw%'
ORDER BY total_exec_time DESC
LIMIT 20;
```

This confirms whether the dominant cost is full scans, hash/sort spill, nested loops, or lock contention.

## Phase 1: Replace `transformAddressBalancesCurrent` with state projection

### Files

- `go/transformer.go`
- Optional helper file: `go/address_balances_current.go`

### Current Function

```go
func (rt *RealtimeTransformer) transformAddressBalancesCurrent(ctx context.Context, tx *sql.Tx, startLedger, endLedger int64) (int64, error)
```

Replace its delete/rebuild-from-history logic with three bounded sub-steps:

1. Upsert native balances from `native_balances_current`.
2. Upsert classic trustline balances from `trustlines_current`.
3. Delete zero/negative state-derived rows changed in this ledger window.

Keep `transformAddressBalancesFromContractState` after this function, as it already runs in Phase B and can overwrite state where Soroban contract storage is authoritative.

### Step 1A: Native XLM state projection

Use `native_balances_current.last_modified_ledger BETWEEN $1 AND $2`.

Recommended SQL shape:

```sql
INSERT INTO address_balances_current (
  owner_address, asset_key, asset_type, token_contract_id,
  asset_code, asset_issuer, symbol, decimals,
  balance_raw, balance_display, balance_source,
  last_updated_ledger, last_updated_at, updated_at
)
SELECT
  nb.account_id,
  'native',
  'native',
  NULL,
  'XLM',
  NULL,
  'XLM',
  7,
  nb.balance::numeric,
  (nb.balance::numeric / 10000000.0)::text,
  'classic_account_state',
  nb.last_modified_ledger,
  NULL,
  NOW()
FROM native_balances_current nb
WHERE nb.last_modified_ledger BETWEEN $1 AND $2
  AND COALESCE(nb.balance, 0) > 0
ON CONFLICT (owner_address, asset_key) DO UPDATE SET
  asset_type = EXCLUDED.asset_type,
  token_contract_id = EXCLUDED.token_contract_id,
  asset_code = EXCLUDED.asset_code,
  asset_issuer = EXCLUDED.asset_issuer,
  symbol = EXCLUDED.symbol,
  decimals = EXCLUDED.decimals,
  balance_raw = EXCLUDED.balance_raw,
  balance_display = EXCLUDED.balance_display,
  balance_source = EXCLUDED.balance_source,
  last_updated_ledger = EXCLUDED.last_updated_ledger,
  last_updated_at = EXCLUDED.last_updated_at,
  updated_at = NOW()
WHERE EXCLUDED.last_updated_ledger >= COALESCE(address_balances_current.last_updated_ledger, 0);
```

Then delete native rows that were updated to zero in this range:

```sql
DELETE FROM address_balances_current abc
USING native_balances_current nb
WHERE abc.owner_address = nb.account_id
  AND abc.asset_key = 'native'
  AND nb.last_modified_ledger BETWEEN $1 AND $2
  AND COALESCE(nb.balance, 0) <= 0
  AND nb.last_modified_ledger >= COALESCE(abc.last_updated_ledger, 0);
```

### Step 1B: Classic trustline state projection

Use `trustlines_current.last_modified_ledger BETWEEN $1 AND $2`.

Asset key recommendation:

```sql
CASE
  WHEN tl.liquidity_pool_id IS NOT NULL AND tl.liquidity_pool_id != '' THEN 'liquidity_pool:' || tl.liquidity_pool_id
  ELSE COALESCE(tl.asset_code, '') || ':' || COALESCE(tl.asset_issuer, '')
END
```

Recommended SQL shape:

```sql
INSERT INTO address_balances_current (
  owner_address, asset_key, asset_type, token_contract_id,
  asset_code, asset_issuer, symbol, decimals,
  balance_raw, balance_display, balance_source,
  last_updated_ledger, last_updated_at, updated_at
)
SELECT
  tl.account_id,
  CASE
    WHEN tl.liquidity_pool_id IS NOT NULL AND tl.liquidity_pool_id != '' THEN 'liquidity_pool:' || tl.liquidity_pool_id
    ELSE COALESCE(tl.asset_code, '') || ':' || COALESCE(tl.asset_issuer, '')
  END AS asset_key,
  tl.asset_type,
  NULL,
  tl.asset_code,
  tl.asset_issuer,
  COALESCE(tl.asset_code, tl.liquidity_pool_id),
  7,
  tl.balance::numeric,
  (tl.balance::numeric / 10000000.0)::text,
  'classic_trustline_state',
  tl.last_modified_ledger,
  tl.created_at,
  NOW()
FROM trustlines_current tl
WHERE tl.last_modified_ledger BETWEEN $1 AND $2
  AND COALESCE(tl.balance, 0) > 0
ON CONFLICT (owner_address, asset_key) DO UPDATE SET
  asset_type = EXCLUDED.asset_type,
  token_contract_id = EXCLUDED.token_contract_id,
  asset_code = EXCLUDED.asset_code,
  asset_issuer = EXCLUDED.asset_issuer,
  symbol = EXCLUDED.symbol,
  decimals = EXCLUDED.decimals,
  balance_raw = EXCLUDED.balance_raw,
  balance_display = EXCLUDED.balance_display,
  balance_source = EXCLUDED.balance_source,
  last_updated_ledger = EXCLUDED.last_updated_ledger,
  last_updated_at = EXCLUDED.last_updated_at,
  updated_at = NOW()
WHERE EXCLUDED.last_updated_ledger >= COALESCE(address_balances_current.last_updated_ledger, 0);
```

Then delete zero/removed trustline rows touched in the range:

```sql
DELETE FROM address_balances_current abc
USING trustlines_current tl
WHERE abc.owner_address = tl.account_id
  AND abc.asset_key = CASE
    WHEN tl.liquidity_pool_id IS NOT NULL AND tl.liquidity_pool_id != '' THEN 'liquidity_pool:' || tl.liquidity_pool_id
    ELSE COALESCE(tl.asset_code, '') || ':' || COALESCE(tl.asset_issuer, '')
  END
  AND tl.last_modified_ledger BETWEEN $1 AND $2
  AND COALESCE(tl.balance, 0) <= 0
  AND tl.last_modified_ledger >= COALESCE(abc.last_updated_ledger, 0);
```

### Step 1C: Count behavior

Return the sum of affected rows from native upsert/delete and trustline upsert/delete. Log sub-step timings so future logs identify which projection is slow:

```text
✅ address_balances_current.native: N rows in X
✅ address_balances_current.trustlines: N rows in X
✅ address_balances_current.cleanup: N rows in X
```

## Phase 2: Keep contract-state balances as authoritative for Soroban

The existing function:

```go
transformAddressBalancesFromContractState(...)
```

already runs immediately after `transformAddressBalancesCurrent`.

Keep that ordering:

```go
{"address_balances_current", rt.transformAddressBalancesCurrent},
{"address_balances_state", rt.transformAddressBalancesFromContractState},
```

This means native/trustline state is projected first, and decoded contract-storage balances can upsert afterwards.

Recommended follow-up improvement: rename the first function later to `transformAddressBalancesClassicState` to make the split explicit.

## Phase 3: Optional bounded event-delta fallback

Only implement this if state sources are insufficient for some asset classes.

Rules:

- Must read only `token_transfers_raw WHERE ledger_sequence BETWEEN $1 AND $2`.
- Must never recompute full historical balances.
- Should only handle assets not covered by `native_balances_current`, `trustlines_current`, or contract state.

Use net deltas per `(owner_address, asset_key)`:

```sql
WITH movement AS (
  SELECT
    from_account AS owner_address,
    COALESCE(token_contract_id, COALESCE(asset_code, '') || ':' || COALESCE(asset_issuer, '')) AS asset_key,
    -amount AS delta,
    token_contract_id,
    asset_code,
    asset_issuer,
    ledger_sequence,
    timestamp
  FROM token_transfers_raw
  WHERE ledger_sequence BETWEEN $1 AND $2
    AND transaction_successful = true
    AND from_account IS NOT NULL AND from_account != ''
    AND amount IS NOT NULL

  UNION ALL

  SELECT
    to_account AS owner_address,
    COALESCE(token_contract_id, COALESCE(asset_code, '') || ':' || COALESCE(asset_issuer, '')) AS asset_key,
    amount AS delta,
    token_contract_id,
    asset_code,
    asset_issuer,
    ledger_sequence,
    timestamp
  FROM token_transfers_raw
  WHERE ledger_sequence BETWEEN $1 AND $2
    AND transaction_successful = true
    AND to_account IS NOT NULL AND to_account != ''
    AND amount IS NOT NULL
), net AS (
  SELECT
    owner_address,
    asset_key,
    MAX(token_contract_id) AS token_contract_id,
    MAX(asset_code) AS asset_code,
    MAX(asset_issuer) AS asset_issuer,
    SUM(delta) AS delta,
    MAX(ledger_sequence) AS last_updated_ledger,
    MAX(timestamp) AS last_updated_at
  FROM movement
  WHERE asset_key IS NOT NULL AND asset_key != ':'
  GROUP BY owner_address, asset_key
)
INSERT INTO address_balances_current (...)
SELECT ...
FROM net
WHERE delta != 0
ON CONFLICT (owner_address, asset_key) DO UPDATE SET
  balance_raw = address_balances_current.balance_raw + EXCLUDED.balance_raw,
  balance_display = ((address_balances_current.balance_raw + EXCLUDED.balance_raw) / POWER(10::numeric, COALESCE(address_balances_current.decimals, EXCLUDED.decimals, 7)))::text,
  last_updated_ledger = GREATEST(COALESCE(address_balances_current.last_updated_ledger, 0), EXCLUDED.last_updated_ledger),
  last_updated_at = GREATEST(address_balances_current.last_updated_at, EXCLUDED.last_updated_at),
  updated_at = NOW();
```

Caution: event-delta fallback is not idempotent if a ledger window is reprocessed unless guarded. If used, add a per-window ledger marker or derive deltas into a separate idempotent balance ledger table. Prefer state projection first.

## Phase 4: Add supporting indexes

Create a new migration, for example:

`migrations/005_address_balances_current_state_projection_indexes.sql`

Recommended indexes:

```sql
-- silver_hot
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_native_balances_last_modified_account
  ON native_balances_current (last_modified_ledger, account_id);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_trustlines_last_modified_account_asset
  ON trustlines_current (
    last_modified_ledger,
    account_id,
    asset_type,
    COALESCE(asset_code, ''),
    COALESCE(asset_issuer, ''),
    COALESCE(liquidity_pool_id, '')
  );

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_address_balances_source_updated
  ON address_balances_current (balance_source, last_updated_ledger DESC);
```

If the bounded event fallback is implemented, add:

```sql
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_ttr_ledger_success_from_asset
  ON token_transfers_raw (
    ledger_sequence,
    from_account,
    COALESCE(token_contract_id, ''),
    COALESCE(asset_code, ''),
    COALESCE(asset_issuer, '')
  )
  WHERE transaction_successful = true
    AND from_account IS NOT NULL
    AND amount IS NOT NULL;

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_ttr_ledger_success_to_asset
  ON token_transfers_raw (
    ledger_sequence,
    to_account,
    COALESCE(token_contract_id, ''),
    COALESCE(asset_code, ''),
    COALESCE(asset_issuer, '')
  )
  WHERE transaction_successful = true
    AND to_account IS NOT NULL
    AND amount IS NOT NULL;
```

## Phase 5: Testing Plan

### Unit/integration scenarios

Use real rows, not mock data.

1. Native balance increases.
2. Native balance decreases.
3. Native balance becomes zero and row is removed.
4. Trustline balance increases.
5. Trustline balance decreases.
6. Trustline balance becomes zero and row is removed.
7. Multiple updates to same account/asset across consecutive batches.
8. Reprocessing an older batch does not overwrite newer state because of the `last_updated_ledger` guard.
9. Soroban contract state still overrides event/native-derived rows when newer.

### SQL verification

After running a test batch:

```sql
SELECT balance_source, COUNT(*)
FROM address_balances_current
GROUP BY balance_source
ORDER BY COUNT(*) DESC;
```

Check changed rows:

```sql
SELECT *
FROM address_balances_current
WHERE last_updated_ledger BETWEEN $1 AND $2
ORDER BY last_updated_ledger DESC
LIMIT 100;
```

Compare native projection:

```sql
SELECT COUNT(*)
FROM native_balances_current nb
LEFT JOIN address_balances_current abc
  ON abc.owner_address = nb.account_id
 AND abc.asset_key = 'native'
WHERE nb.balance > 0
  AND abc.owner_address IS NULL;
```

Compare trustline projection:

```sql
SELECT COUNT(*)
FROM trustlines_current tl
LEFT JOIN address_balances_current abc
  ON abc.owner_address = tl.account_id
 AND abc.asset_key = CASE
    WHEN tl.liquidity_pool_id IS NOT NULL AND tl.liquidity_pool_id != '' THEN 'liquidity_pool:' || tl.liquidity_pool_id
    ELSE COALESCE(tl.asset_code, '') || ':' || COALESCE(tl.asset_issuer, '')
  END
WHERE tl.balance > 0
  AND abc.owner_address IS NULL;
```

## Phase 6: Deployment Plan

1. Apply indexes concurrently on testnet.
2. Deploy transformer with new state-projection implementation to testnet.
3. Watch logs for three consecutive batches.
4. Confirm `address_balances_current` drops from ~10–12 minutes to seconds.
5. Validate API endpoints depending on balances through `gateway.withobsrvr.com`.
6. Deploy to mainnet only after testnet validates.

## Rollback Plan

Keep the old historical rebuild function in the code temporarily as:

```go
transformAddressBalancesCurrentHistoricalRebuild
```

Gate the new implementation behind config if desired:

```yaml
performance:
  address_balances_mode: "state" # "state" or "historical"
```

Recommended default should be `state`.

## Implementation Order

1. Add migration `005_address_balances_current_state_projection_indexes.sql`.
2. Extract current `transformAddressBalancesCurrent` into `transformAddressBalancesCurrentHistoricalRebuild` for rollback.
3. Implement new state-projection `transformAddressBalancesCurrent`.
4. Add sub-step timing logs.
5. Run Go tests/build.
6. Deploy to testnet.
7. Compare batch timings and validate rows.
8. Remove or config-gate historical path after confidence.
