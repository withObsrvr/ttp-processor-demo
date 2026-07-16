# Smart Account Backfill and Serving Runbook (2026-07-16)

## Why This Exists

Smart-account state has two layers:

1. **Silver hot state**: `smart_account_context_rules`, `smart_account_signers`,
   and `smart_account_policies`.
2. **Serving state**: `serving.sv_smart_account_contracts`,
   `serving.sv_smart_account_signers`, and
   `serving.sv_smart_account_rules_current`.

The serving smart-account projector is a mirror of Silver hot state. It does not
read Bronze cold directly and it does not perform the historical smart-account
replay. If Silver hot is reset and the narrow smart-account replay is skipped,
the serving projector will rebuild from an incomplete source.

That happened after the July 15 testnet reset/backfill work:

- July 8 baseline: `8875` serving smart-account contracts, and fixture contract
  `CCQBQIAG2E2L5NOIML2SGAJYMXPID3MAQNII5USMENID3SDJ4ATOU2HG` resolved.
- July 16 regression: Silver hot and serving both had only `2` smart-account
  contracts. The fixture still classified as a smart wallet through semantic
  classification, but `/api/v1/silver/smart-accounts/{contract}/rules` returned
  `404`.

## Source of Truth

Historical smart-account state is reconstructed by:

```text
silver-realtime-transformer --smart-account-replay
```

Use the narrow smart-account replay, not full `--cold-replay`, for repair on an
already-running network. The narrow replay:

- reads Bronze contract events from hot or cold,
- writes only `smart_account_*` state plus semantic wallet classification,
- does not reset or advance `realtime_transformer_checkpoint`.

Serving is then rebuilt by `serving-projection-processor` with the
`smart_accounts` projector enabled.

## Required Order

1. Apply Silver migration `009_add_smart_account_state.sql`.
2. Stop or disable the serving smart-account projector before replay if serving
   already has good historical rows. This prevents partial rebuilds mid-replay.
3. Run smart-account replay from Bronze cold for the historical range.
4. Restart or trigger `serving-projection-processor`.
5. Verify Silver source counts.
6. Verify serving counts and fixture endpoints.
7. Only then deploy Prism or query-api changes that depend on smart-account
   state.

## Confirmation Queries

Run against the target network's `silver_hot` database:

```sql
SELECT 'smart_account_context_rules' AS table_name,
       COUNT(*) AS rows,
       COUNT(DISTINCT contract_id) AS contracts,
       COALESCE(MAX(last_modified_ledger), 0) AS max_ledger
FROM smart_account_context_rules
UNION ALL
SELECT 'smart_account_signers',
       COUNT(*),
       COUNT(DISTINCT contract_id),
       COALESCE(MAX(last_modified_ledger), 0)
FROM smart_account_signers
UNION ALL
SELECT 'smart_account_policies',
       COUNT(*),
       COUNT(DISTINCT contract_id),
       COALESCE(MAX(last_modified_ledger), 0)
FROM smart_account_policies
UNION ALL
SELECT 'serving.sv_smart_account_contracts',
       COUNT(*),
       COUNT(DISTINCT contract_id),
       COALESCE(MAX(last_modified_ledger), 0)
FROM serving.sv_smart_account_contracts
UNION ALL
SELECT 'serving.sv_smart_account_signers',
       COUNT(*),
       COUNT(DISTINCT contract_id),
       COALESCE(MAX(last_modified_ledger), 0)
FROM serving.sv_smart_account_signers
UNION ALL
SELECT 'serving.sv_smart_account_rules_current',
       COUNT(*),
       COUNT(DISTINCT contract_id),
       COALESCE(MAX(last_modified_ledger), 0)
FROM serving.sv_smart_account_rules_current;
```

The July 16 regression produced:

```text
smart_account_context_rules            rows=2 contracts=2
smart_account_signers                  rows=4 contracts=2
smart_account_policies                 rows=1 contracts=1
serving.sv_smart_account_contracts     rows=2 contracts=2
serving.sv_smart_account_signers       rows=1 contracts=1
serving.sv_smart_account_rules_current rows=2 contracts=2
```

That confirms the source state was incomplete, not only serving.

## Testnet Replay Range

The known testnet replay that restored the July 8 baseline covered:

```text
1..3502597
```

The July 8 replay evidence:

- replayed `16984` smart-account events,
- touched `8027` wallet classifications,
- serving ended with `8875` smart-account contracts,
- credential lookup rows: `238`,
- address lookup rows: `3114`.

After later live ingestion, extend replay through the current Bronze cold max
ledger if the network has been reset and the state tables are empty.

## Fixture Verification

Use this testnet fixture:

```text
contract:   CCQBQIAG2E2L5NOIML2SGAJYMXPID3MAQNII5USMENID3SDJ4ATOU2HG
credential: 9ca5204617ab254b6b21cbae8a30c42377d0cd4f
```

Expected after replay and serving rebuild:

- `GET /api/v1/silver/smart-accounts/{contract}/rules` returns `200`.
- `GET /api/v1/silver/smart-accounts/lookup/credential/{credential}` returns
  the contract.
- `GET /api/v1/silver/smart-accounts/stats` has thousands of contracts on
  testnet, not `2`.
- `GET /api/v1/silver/smart-wallet/{contract}` continues to classify as
  `openzeppelin`.

## Serving Shrink Guard

`serving-projection-processor` now refuses to perform a destructive
smart-account serving rebuild when:

- existing serving contract count is at least `100`, and
- current Silver hot source contract count is less than 90% of serving.

This prevents a later hot reset from silently replacing a complete historical
serving projection with an incomplete live tail.

For intentional environment resets only, set:

```text
SERVING_SMART_ACCOUNTS_ALLOW_SHRINK=true
```

Do not set that in normal testnet or mainnet service jobs.

## Mainnet Rollout Checklist

Before mainnet enables smart-account endpoints:

1. Apply Silver smart-account migration.
2. Deploy transformer image with `--smart-account-replay` support.
3. Deploy serving image with the shrink guard.
4. Keep `smart_accounts` serving projector disabled or stopped during initial
   replay.
5. Run smart-account replay from Bronze cold for the selected historical range.
6. Start serving projection and verify counts.
7. Record acceptance fixtures in the rollout doc.
8. Add a post-reset checklist item: rerun smart-account replay after any
   `silver_hot.public` reset.

The mainnet process should not rely on the live transformer alone. Live
ingestion only maintains new state after the checkpoint. Historical
authorization state requires replay.
