# Horizon Compatibility API

Status: deployed on testnet as of 2026-07-10.

The Horizon compatibility surface lives inside `stellar-query-api` under:

```text
/api/v1/horizon-compat
```

On public testnet, use:

```text
https://obsrvr-lake-testnet.withobsrvr.com/api/v1/horizon-compat
```

Through Gateway, use the existing lake proxy path:

```text
https://gateway.withobsrvr.com/lake/v1/testnet/api/v1/horizon-compat
```

Gateway requests require an API key header.

## Response Contract

Handlers populate structs from `github.com/stellar/go-stellar-sdk/protocols/horizon`
and render Horizon-style HAL JSON and problem responses. This keeps response
shape aligned with Horizon where a route is implemented.

The compatibility layer is read-only. Transaction submission, friendbot,
pathfinding, and streaming are intentionally not implemented here.

## Pagination

Collection routes accept Horizon-style pagination parameters:

| Parameter | Default | Notes |
| --- | --- | --- |
| `cursor` | empty | Route-specific paging token from previous response |
| `limit` | `10` | Clamped to `200` |
| `order` | `asc` | `asc` or `desc` |

## Implemented Routes

| Route | Method | Status | Backing data |
| --- | --- | --- | --- |
| `/fee_stats` | GET | implemented | hot/cold ledger and transaction fee data |
| `/ledgers` | GET | implemented | hot/cold ledger data |
| `/ledgers/{sequence}` | GET | implemented | exact hot/cold ledger lookup with `ledger_range` pruning |
| `/transactions/{hash}` | GET | implemented | serving `sv_transactions_recent` first, then hot/cold Bronze fallback (ledger-bounded when the transaction location index resolves the hash; otherwise an unbounded cold scan) |
| `/transactions/{hash}/operations` | GET | implemented | serving `sv_operations_by_account` first, then enriched operations fallback |
| `/transactions/{hash}/payments` | GET | implemented | serving `sv_operations_by_account` first, payment subset, then fallback |
| `/transactions/{hash}/effects` | GET | implemented | effects |
| `/accounts/{id}` | GET | implemented | current account state, balances, signers |
| `/accounts/{id}/transactions` | GET | implemented | account transaction feed (serving path requires `ACCOUNT_TX_FEED_ENABLED=true`, default off) plus Horizon transaction hydration |
| `/accounts/{id}/operations` | GET | implemented | serving `sv_operations_by_account` first, then unified enriched operations fallback |
| `/accounts/{id}/payments` | GET | implemented | serving `sv_effects_by_account` payment-effect TOIDs hydrated from `sv_operations_by_account`, then fallback |
| `/accounts/{id}/effects` | GET | implemented | serving `sv_effects_by_account` first, then unified effects fallback |
| `/operations` | GET | implemented | serving `sv_operations_by_account` first, then enriched operations fallback |
| `/operations/{id}` | GET | implemented | serving `sv_operations_by_account` first, then enriched operation by TOID fallback |
| `/operations/{id}/effects` | GET | implemented | effects filtered by operation TOID |
| `/payments` | GET | implemented | serving `sv_operations_by_account` first, payment subset, then fallback |
| `/effects` | GET | implemented | effects |

## Operational Notes

- `ACCOUNT_TX_FEED_ENABLED=true` enables the serving fast path for
  `/accounts/{id}/transactions`; it defaults to off, in which case the route
  answers through the slower federated reader.
- `NETWORK_PASSPHRASE` must be set to derive fee-bump `inner_transaction`
  hashes; without it fee-bump transactions omit the `inner_transaction` block.
- `cursor=now` is treated as "no cursor": equivalent to Horizon for
  `order=desc`, but for `order=asc` this returns from the oldest available
  history where Horizon would return an empty page until new ledgers close.
- Emitted `_links` may reference routes this layer does not implement yet
  (account offers/trades/data, ledger sub-collections, global
  `/transactions`); following those links returns 404.

## Cycle 5B Transaction Hydration

Cycle 5B added Horizon transaction XDR fields to `serving.sv_transactions_recent`:

```text
transaction_id
tx_envelope
tx_result
tx_meta
tx_fee_meta
tx_signers
```

`GET /transactions/{hash}` and `GET /accounts/{id}/transactions` now use those
serving fields before scanning Bronze. This is the expected fast path for
recent-window Horizon transaction resources.

If a serving row exists but is missing required XDR fields, account transaction
history returns `503 data_unavailable` instead of returning a partial Horizon
transaction resource.

## Smoke Test

Use the committed smoke script for a fast deployment health check:

```bash
python3 obsrvr-lake/stellar-query-api/scripts/horizon_compat_smoke.py \
  --base-url https://obsrvr-lake-testnet.withobsrvr.com/api/v1/horizon-compat \
  --account GBTHMMFWTAPFAHRGS33LKETZYJKBTNEENRN47EDZMZPT2BNCJO47GVQG \
  --tx-hash 366bc4543a8fe66e09c021af35377c78df6e90e57f85582a0aad1617fcc027e8
```

For Gateway:

```bash
python3 obsrvr-lake/stellar-query-api/scripts/horizon_compat_smoke.py \
  --base-url https://gateway.withobsrvr.com/lake/v1/testnet/api/v1/horizon-compat \
  --api-key "$API_KEY" \
  --account GBTHMMFWTAPFAHRGS33LKETZYJKBTNEENRN47EDZMZPT2BNCJO47GVQG \
  --tx-hash 366bc4543a8fe66e09c021af35377c78df6e90e57f85582a0aad1617fcc027e8
```

The script checks:

- `/fee_stats`
- `/ledgers?limit=1&order=desc`
- `/accounts/{id}`
- `/accounts/{id}/transactions?limit=1&order=desc`
- `/transactions/{hash}`

For transaction responses, it verifies non-empty:

- `envelope_xdr`
- `result_xdr`
- `result_meta_xdr`
- `fee_meta_xdr`
- `signatures`

Smoke is intentionally not a Horizon accuracy check. It can pass while a route
is stale or shape-incompatible with Horizon.

For route-level comparison against `https://horizon-testnet.stellar.org`, run:

```bash
python3 obsrvr-lake/stellar-query-api/scripts/horizon_compat_accuracy.py \
  --timeout 20
```

This compares high-priority implemented routes, including account transaction
freshness, operation lookup, effects routes, account balances, and collection
record shape.

## Current Accuracy Caveats

The 2026-07-10 closeout smoke passed and the accuracy harness had no hard route
failures. Remaining differences are documented in
`obsrvr-lake/docs/horizon-compat-accuracy-findings-2026-07-10.md` and are
mostly compatibility-quality gaps:

- live-tip collections can differ by a few ledgers from SDF Horizon during the
  comparison window
- account root is not yet Horizon-exact for all balance, sequence, liability,
  sponsorship, and auth fields
- Soroban operation detail formatting differs from Horizon's generic
  host-function representation
- `/payments` semantics and effect ID/paging token formatting still differ from
  Horizon

## Not Implemented

The following Horizon route families are not implemented in the compatibility
prefix yet:

| Route family | Status |
| --- | --- |
| `/` root resource | not implemented |
| `/transactions` global collection | not implemented |
| `/accounts` global collection | not implemented |
| `/assets` | not implemented in Horizon shape |
| `/offers` | not implemented in Horizon shape |
| `/claimable_balances` | not implemented in Horizon shape |
| `/liquidity_pools` | not implemented in Horizon shape |
| `/trades` | not implemented in Horizon shape |
| `/order_book` | not implemented |
| `/paths/*` | delegated/out of scope |
| `POST /transactions` | delegated/out of scope |
| Friendbot | delegated/out of scope |
| SSE streaming | delegated/out of scope |

For these, use the existing Silver/Semantic routes where available, or add them
to the customer route inventory for a future parity cycle.
