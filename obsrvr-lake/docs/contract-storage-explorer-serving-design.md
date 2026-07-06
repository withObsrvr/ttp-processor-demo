# Contract Storage Explorer Serving Design

Written 2026-07-06.

## Goal

Support a contract storage explorer like `/home/tillman/Downloads/obsrvr_contract_explorer.html` with a stable API and a database view/projection that make contract storage entries inspectable by humans, not just addressable by opaque storage hashes.

The UI needs to:

- load all current storage entries for one contract
- show storage type counts for instance, persistent, and temporary entries
- filter by storage type
- search by decoded key or decoded value
- sort by key, storage type, and last-updated ledger
- show live/expired status and TTL ledger
- open a row detail panel with key hash, decoded key/value, raw XDR, ledger, and copyable API URL

## Current API State

The existing endpoint is:

```text
GET /api/v1/silver/contracts/{id}/storage
```

It reads hot and cold `contract_data_current`, joins `ttl_current` when `live_only=true`, and returns:

- `contract_id`
- `entries[]`
- `count`
- `limit`
- `offset`
- `live_only`

Each entry currently includes:

- `contract_id`
- `key`
- `key_hash`
- `type`
- `durability`
- `size_bytes`
- `data_value`
- `key_xdr`
- `value_xdr`
- `key_decoded`
- `value_decoded`
- `key_decoded_error`
- `value_decoded_error`
- `last_modified_ledger`
- `closed_at`
- `live_until_ledger_seq`
- `ttl_remaining`
- `expired`

The lower-level endpoints are:

```text
GET /api/v1/silver/soroban/contract-data?contract_id=...
GET /api/v1/silver/soroban/contract-data/entry?contract_id=...&key_hash=...
```

Those return a leaner `contract_data` shape and already support single-entry lookup by `key_hash`.

## Key Hash Issue

`key_hash` is the storage row identifier. It is good for lookup, joins, pagination, and deduplication, but it is not a human-readable storage key.

For the explorer, the visible key column should come from decoded key data:

```json
{
  "key_hash": "a1b2...",
  "key_xdr": "...",
  "key_decoded": {
    "type": "symbol",
    "value": "admin",
    "display": "admin"
  }
}
```

The current storage endpoint sets `key = key_hash`. That is technically stable, but it makes the UI misleading because the `Key` column would show the hash instead of the actual contract storage key. Either remove/deprecate `key`, or redefine it as the decoded display key while keeping `key_hash` as the lookup identifier.

## Data Needed

The explorer row should be normalized to this UI shape:

```json
{
  "key_hash": "a1b2...",
  "storage_type": "persistent",
  "key_display": "domain::obsrvr",
  "key_type": "symbol",
  "value_type": "map",
  "value_display": "{\"owner\":\"G...\",\"resolver\":\"C...\"}",
  "last_modified_ledger": 55812388,
  "closed_at": "2026-07-06T12:00:00Z",
  "live_until_ledger_seq": 56812388,
  "ttl_remaining": 1000000,
  "expired": false,
  "size_bytes": 512,
  "key_xdr": "...",
  "value_xdr": "...",
  "data_value_xdr": "..."
}
```

Source mapping:

| Explorer field | Source |
| --- | --- |
| `contract_id` | `contract_data_current.contract_id` |
| `key_hash` | `contract_data_current.key_hash` |
| `storage_type` | normalized `contract_data_current.durability` |
| `data_value_xdr` | `contract_data_current.data_value` |
| `key_xdr` | decoded from `data_value_xdr` |
| `value_xdr` | decoded from `data_value_xdr` |
| `key_type` | decoded from `data_value_xdr` |
| `key_display` | decoded from `data_value_xdr` |
| `value_type` | decoded from `data_value_xdr` |
| `value_display` | decoded from `data_value_xdr` |
| `last_modified_ledger` | `contract_data_current.last_modified_ledger` |
| `closed_at` | `contract_data_current.closed_at` |
| `size_bytes` | `LENGTH(contract_data_current.data_value)` or precomputed |
| `live_until_ledger_seq` | `ttl_current.live_until_ledger_seq` joined by `key_hash` |
| `ttl_remaining` | `ttl_current.ttl_remaining` joined by `key_hash` |
| `expired` | `ttl_current.expired` joined by `key_hash` |

## Database View / Projection Requirement

There are two possible levels.

### Minimum Raw View

This is enough if the API continues to decode XDR at request time:

```sql
CREATE VIEW serving.v_contract_storage_current_raw AS
SELECT
  cd.contract_id,
  cd.key_hash,
  cd.durability,
  lower(
    regexp_replace(cd.durability, '^contractdatadurability', '', 'i')
  ) AS storage_type,
  cd.data_value AS data_value_xdr,
  length(cd.data_value) AS size_bytes,
  cd.last_modified_ledger,
  cd.closed_at,
  t.live_until_ledger_seq,
  t.ttl_remaining,
  coalesce(t.expired, false) AS expired,
  cd.ledger_range,
  cd.updated_at
FROM silver.contract_data_current cd
LEFT JOIN silver.ttl_current t
  ON t.key_hash = cd.key_hash;
```

Pros:

- simple
- uses existing tables
- no new decode persistence
- good enough for one-contract pages if result sets are modest

Cons:

- DB users still see opaque XDR
- search by human key/value requires API-side decode first
- every request repeats XDR decoding

### Recommended Decoded Serving Projection

For a proper explorer-backed DB view, persist decoded key/value fields in a serving table:

```text
serving.sv_contract_storage_current
```

Suggested columns:

- `network`
- `contract_id`
- `key_hash`
- `durability`
- `storage_type`
- `data_value_xdr`
- `key_xdr`
- `value_xdr`
- `key_decoded_type`
- `key_decoded_json`
- `key_display`
- `value_decoded_type`
- `value_decoded_json`
- `value_display`
- `decode_error`
- `size_bytes`
- `last_modified_ledger`
- `closed_at`
- `live_until_ledger_seq`
- `ttl_remaining`
- `expired`
- `search_text`
- `updated_at`

Primary key:

```text
(network, contract_id, key_hash)
```

Recommended indexes:

```text
(network, contract_id, storage_type)
(network, contract_id, last_modified_ledger DESC)
(network, contract_id, expired)
(network, contract_id, key_hash)
GIN/search index on search_text if this lives in Postgres
```

The projector should read changed rows from `contract_data_current`, decode the full `ContractDataEntry` XDR once, and upsert the decoded representation. TTL can be refreshed either by joining `ttl_current` during projection or by maintaining TTL fields from TTL change events.

## One Endpoint Option

Use one endpoint for the complete explorer screen:

```text
GET /api/v1/silver/contracts/{contract_id}/storage
```

Query params:

- `limit`, default 100, max 1000
- `cursor` preferred for stable pagination; keep `offset` only for compatibility
- `storage_type=instance|persistent|temporary`
- `key_hash=...` for single-row lookup
- `q=...` to search decoded key/value displays
- `include_expired=false` default; `true` for the Live/Expired toggle
- `include_raw=true|false` to include `data_value_xdr`, `key_xdr`, `value_xdr`
- `sort=last_modified_ledger|key|storage_type`
- `order=asc|desc`

Response:

```json
{
  "contract_id": "C...",
  "entries": [],
  "summary": {
    "total_entries": 123,
    "instance_entries": 3,
    "persistent_entries": 110,
    "temporary_entries": 10,
    "expired_entries": 2,
    "total_state_size_bytes": 123456
  },
  "pagination": {
    "limit": 100,
    "cursor": "opaque",
    "has_more": true
  },
  "coverage": {
    "source": "serving|silver_hot|unified_hot_cold",
    "live_only": true,
    "ttl_joined": true
  }
}
```

Pros:

- easiest frontend integration
- one round trip
- summary and rows stay consistent
- simplest API for external users

Cons:

- endpoint can become too broad
- every row request may pay for summary counts unless cached/precomputed
- large contracts need careful pagination and bounded search

Required changes to current endpoint:

- add `key_hash` filter
- always join TTL when available, even when returning expired rows
- replace `live_only` with clearer `include_expired` or preserve both with documented behavior
- stop using `key` to mean `key_hash`
- add summary counts
- prefer cursor pagination over offset for stable contract storage browsing
- optionally source from `serving.sv_contract_storage_current`

## Multiple Endpoint Option

Split the explorer into focused endpoints.

### 1. Contract Storage Summary

```text
GET /api/v1/silver/contracts/{contract_id}/storage/summary
```

Returns:

- `total_entries`
- `instance_entries`
- `persistent_entries`
- `temporary_entries`
- `expired_entries`
- `live_entries`
- `total_state_size_bytes`
- `last_modified_ledger`
- optional `estimated_monthly_rent_stroops`

### 2. Contract Storage Rows

```text
GET /api/v1/silver/contracts/{contract_id}/storage
```

Query params:

- `limit`
- `cursor`
- `storage_type`
- `include_expired`
- `q`
- `sort`
- `order`
- `include_raw`

Returns row entries only plus pagination and coverage metadata.

### 3. Single Storage Entry

```text
GET /api/v1/silver/contracts/{contract_id}/storage/{key_hash}
```

Returns one decoded row with raw fields. This should replace the awkward copy URL currently implied by the mockup:

```text
GET /api/v1/silver/contracts/{contract_id}/storage?key_hash=...
```

### 4. Optional Contract Storage Schema

```text
GET /api/v1/silver/contracts/{contract_id}/storage/schema
```

Returns counts and observed decoded key/value types:

```json
{
  "storage_types": [
    { "type": "instance", "count": 3 },
    { "type": "persistent", "count": 110 },
    { "type": "temporary", "count": 10 }
  ],
  "key_types": [
    { "type": "symbol", "count": 80 },
    { "type": "vec", "count": 43 }
  ],
  "value_types": [
    { "type": "map", "count": 60 },
    { "type": "address", "count": 20 },
    { "type": "u64", "count": 43 }
  ]
}
```

Pros:

- easier to cache summary/schema
- single-row detail is clean
- list endpoint stays bounded
- frontend can lazy-load details/raw XDR only when needed

Cons:

- more round trips
- more endpoints to document and maintain
- consistency between summary and rows needs source/version metadata

## Recommendation

Build the serving projection first, then expose a single list endpoint plus a single detail endpoint.

Recommended minimum:

```text
GET /api/v1/silver/contracts/{contract_id}/storage
GET /api/v1/silver/contracts/{contract_id}/storage/{key_hash}
```

The list endpoint should include summary counts by default because the explorer needs them immediately for the sidebar. The detail endpoint should include raw XDR by default because it is used after a row click, not for every list render.

Use the existing `/api/v1/silver/soroban/contract-data/entry` only as a compatibility path. It is useful internally, but the contract-centric route is better for a contract explorer.

## Acceptance Criteria

- Explorer can render its table without showing opaque hashes in the Key column.
- Explorer can search by decoded key/value display.
- Explorer can show both live and expired rows when requested.
- TTL fields are present whenever `ttl_current` has a matching key.
- `key_hash` works for single-row lookup.
- Response includes enough metadata to show storage-type counts without a second expensive query.
- Raw XDR remains available for detail/debug use.
- API behavior is bounded: max page size, cursor pagination, no unbounded cold scans.

