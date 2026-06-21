# Relationships API (v1)

`GET /api/v1/silver/relationships/{address_a}/{address_b}` returns a paginated interaction history between two Stellar addresses.

`address_a` and `address_b` may be account IDs (`G...`) or contract IDs (`C...`).

## Example

```bash
curl 'http://localhost:8080/api/v1/silver/relationships/GBTORQK3ZR3RPJF4WTTSH5KVDOAZ4BJI7PD2ECLSBDNHRG4ICNC4JJZV/GB5FCYPSK4ET44OVBXLJHWFW5LNG3ZLPUFSJTJBCGIM43JIU4RGYRLCH?limit=50'
```

## Query parameters

- `limit` — default `100`, max `500`.
- `cursor` — opaque cursor returned by the previous page.
- `order` — `desc` default, or `asc`.
- `start_ledger` / `end_ledger` — optional bounded ledger window.

## Edge model

Each edge contains:

- `ledger_sequence`
- `closed_at`
- `transaction_hash`
- `interaction_type`: `transfer`, `contract_call`, or `co_event`
- `address_a`, `address_b`
- `direction`: `a_to_b`, `b_to_a`, or `same_activity`
- optional `asset`, `contract_id`, `function_name`, `amount`
- `source_table`
- `confidence`

## v1 coverage

Included:

- Direct value transfers from `token_transfers_raw` and `semantic_flows_value`.
- Account-to-contract calls from `contract_invocations_raw`.
- Contract-to-contract calls from `contract_invocation_calls` where available in hot storage.
- Conservative co-event edges from typed `semantic_activities` columns.

Limitations are returned in the response `coverage.limitations` block. v1 does **not** scan arbitrary Soroban SCVal arguments, raw XDR, raw JSON payloads, or undecoded event topics, and does not infer smart-wallet signer/controller or router/pool/aggregator indirect relationships.
