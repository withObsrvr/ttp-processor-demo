# Silver Events API

The Silver Events API deliberately exposes two different streams: a semantic token-transfer stream for wallet/token activity, and a raw generic contract-event stream for protocol-specific indexing and debugging.

## Semantic token-transfer stream

`GET /api/v1/silver/events`

Derived from `token_transfers_raw`. Use this for wallet/token activity feeds.

Supported filters:

- `contract_id` — token contract ID
- `event_type=transfer|mint|burn`
- `source_type=classic|soroban`
- `start_ledger`, `end_ledger`
- `limit`, `cursor`, `order=asc|desc`

Related focused routes:

- `GET /api/v1/silver/events/by-contract?contract_id=...`
- `GET /api/v1/silver/address/{addr}/events`
- `GET /api/v1/silver/tx/{hash}/events`

Response includes `coverage` explaining that this is not the complete raw Soroban event stream. Amounts are raw token units; use token metadata for display decimals.

## Raw generic contract-event stream

`GET /api/v1/silver/events/generic`

Reads raw `contract_events_stream_v1` rows with decoded topic/data fields where available. Use this for protocol-specific indexing, debugging, and non-transfer events.

Supported filters:

- `contract_id`
- `tx_hash`
- `event_type=contract|system|diagnostic`
- `topic_match`
- `topic0`, `topic1`, `topic2`, `topic3`
- `start_ledger`, `end_ledger`
- `limit`, `cursor`, `order=asc|desc`

Related focused route:

- `GET /api/v1/silver/events/contract/{contract_id}`

Response includes `coverage` explaining that raw events are not guaranteed to be semantically classified. For Prism-style classified events, use `GET /api/v1/explorer/events`.

## Validation behavior

Invalid enum values, malformed ledger bounds, and `start_ledger > end_ledger` return `400` instead of silently broadening the query or surfacing as storage errors.
