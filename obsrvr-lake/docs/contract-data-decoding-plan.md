# Contract Data Decoding Plan

## Why this matters

The current contract-data endpoints prove that contract storage is queryable, but they still expose the most important field as raw XDR:

```json
{
  "key_hash": "...",
  "data_value_xdr": "AAAA..."
}
```

That is not enough for the product promise. OBSRVR Lake should let wallet builders and application teams consume contract state without writing their own XDR / SCVal decoder.

The target API should return the original storage key and value as decoded JSON, while retaining raw XDR for verification and advanced users.

## Current endpoints

Direct `stellar-query-api` paths:

```text
GET /api/v1/silver/soroban/contract-data?contract_id=C...
GET /api/v1/silver/soroban/contract-data/entry?contract_id=C...&key_hash=...
GET /api/v1/silver/contracts/{id}/storage
```

Gateway paths:

```text
GET /lake/v1/{network}/api/v1/silver/soroban/contract-data?contract_id=C...
GET /lake/v1/{network}/api/v1/silver/contracts/{id}/storage
```

Current response fields are mostly:

- `contract_id`
- `key_hash`
- `durability`
- `data_value_xdr` / `data_value`
- `last_modified_ledger`
- TTL metadata on `/contracts/{id}/storage`

Missing:

- decoded original storage key
- decoded storage value JSON
- optional display strings for common SCVal shapes

## Important backfill finding

We do **not** need to restart mainnet backfill to recover the original storage key.

Reason: the stored `contract_data_xdr` / `data_value` is XDR for the full `xdr.ContractData` entry, not just the value. It contains:

```text
ContractData{
  Contract,
  Key,
  Durability,
  Val,
  Ext
}
```

So both the original storage key and value are recoverable by decoding the existing XDR already present in Bronze/Silver.

This is also visible in the ingester code: `TransformContractData` already computes `Key`, `KeyDecoded`, `Val`, and `ValDecoded` from `contractData.Key` and `contractData.Val`; the writer simply does not persist those decoded fields today.

## Recommended build path

### Phase 1 — Query-time decode, no backfill required

Implement decoding in `stellar-query-api` first.

For each row returned by the contract-data endpoints:

1. Read `data_value` / `contract_data_xdr`.
2. `xdr.SafeUnmarshalBase64` into `xdr.ContractData`.
3. Serialize `contractData.Key` into `key_decoded`.
4. Serialize `contractData.Val` into `value_decoded`.
5. Preserve raw fields for auditability.

Target response shape:

```json
{
  "contract_id": "C...",
  "key_hash": "...",
  "key_xdr": "base64 ScVal key, optional",
  "key_decoded": {
    "type": "vec",
    "value": [
      { "type": "symbol", "value": "Balance" },
      { "type": "address", "value": "G..." }
    ],
    "display": "Balance(G...)"
  },
  "durability": "ContractDataDurabilityPersistent",
  "value_xdr": "base64 ScVal value, optional",
  "value_decoded": {
    "type": "map",
    "value": {
      "amount": "1234567",
      "authorized": true,
      "clawback": false
    }
  },
  "contract_data_xdr": "raw full ContractData XDR",
  "last_modified_ledger": 123,
  "live_until_ledger_seq": 456,
  "expired": false
}
```

This gives customers decoded data immediately and does not require changing Bronze, Silver, or restarting any historical loader.

### Phase 2 — Persist decoded fields for performance and search

After Phase 1 proves the shape, add materialized decoded columns so high-volume queries do not decode large result sets on every request.

Recommended columns:

Bronze `contract_data_snapshot_v1`:

```sql
ALTER TABLE contract_data_snapshot_v1 ADD COLUMN IF NOT EXISTS contract_key_xdr TEXT;
ALTER TABLE contract_data_snapshot_v1 ADD COLUMN IF NOT EXISTS contract_key_decoded JSONB; -- or JSON/VARCHAR for DuckDB compatibility
ALTER TABLE contract_data_snapshot_v1 ADD COLUMN IF NOT EXISTS contract_value_decoded JSONB;
```

Silver `contract_data_current` and `contract_data_changes`:

```sql
ALTER TABLE contract_data_current ADD COLUMN IF NOT EXISTS contract_key_xdr TEXT;
ALTER TABLE contract_data_current ADD COLUMN IF NOT EXISTS contract_key_decoded JSONB;
ALTER TABLE contract_data_current ADD COLUMN IF NOT EXISTS contract_value_decoded JSONB;

ALTER TABLE contract_data_changes ADD COLUMN IF NOT EXISTS contract_key_xdr TEXT;
ALTER TABLE contract_data_changes ADD COLUMN IF NOT EXISTS contract_key_decoded JSON;
ALTER TABLE contract_data_changes ADD COLUMN IF NOT EXISTS contract_value_decoded JSON;
```

Exact JSON type depends on the storage engine:

- PostgreSQL hot tables: `JSONB` preferred.
- DuckLake/DuckDB cold tables: `JSON` or `VARCHAR` containing canonical JSON may be safer.

### Phase 3 — Backfill decoded columns in place, no ledger re-ingest

If decoded columns are added, backfill them from existing `contract_data_xdr` / `data_value`.

No source-ledger replay is required because existing rows contain enough information.

Options:

1. **SQL/UDF-style backfill** if a decoder is available inside the process.
2. **Small Go backfill job** that:
   - scans `contract_data_snapshot_v1` or `contract_data_changes`,
   - decodes `contract_data_xdr`,
   - writes `contract_key_xdr`, `contract_key_decoded`, `contract_value_decoded`,
   - is idempotent and resumable by ledger range.
3. **Lazy materialization** in query API:
   - decode on read,
   - optionally write decoded JSON back for rows missing decoded fields.

Recommended operational approach for mainnet:

- Do **not** stop or restart the current mainnet backfill for this.
- Ship query-time decoding first.
- Let mainnet Bronze/Silver finish.
- Run a separate decoded-column enrichment job later if performance/search needs it.

## Backfill impact assessment

### Does mainnet backfill need to restart?

No.

The already-written `contract_data_xdr` / `data_value` contains the complete `xdr.ContractData`, including both `Key` and `Val`.

### Do we need to reprocess history archives?

No, not for key/value decoding.

Historical archive replay would only be necessary if `contract_data_xdr` were missing or truncated, which is not the case in the current schema.

### What if we add new columns before mainnet backfill completes?

That is safe but not required.

If added before completion, new writes can populate decoded columns going forward, while older rows are enriched later. However, changing the active writer/schema during a near-complete mainnet backfill adds operational risk. Prefer query-time decode unless decoded-column performance is immediately required.

### What if we add new columns after mainnet backfill completes?

Also safe.

Run an idempotent enrichment job over existing rows. Because decoding is deterministic from `contract_data_xdr`, this does not change ledger coverage or correctness.

## Decoder requirements

Implement a canonical SCVal-to-JSON serializer that handles at least:

- `SCV_BOOL`
- `SCV_VOID`
- `SCV_ERROR`
- `SCV_U32`, `SCV_I32`, `SCV_U64`, `SCV_I64`
- `SCV_U128`, `SCV_I128`, `SCV_U256`, `SCV_I256` as strings
- `SCV_TIMEPOINT`, `SCV_DURATION`
- `SCV_BYTES`, `SCV_BYTES_N` as base64 and hex
- `SCV_STRING`
- `SCV_SYMBOL`
- `SCV_ADDRESS` as `G...` or `C...`
- `SCV_VEC`
- `SCV_MAP`
- `SCV_CONTRACT_INSTANCE`
- `SCV_LEDGER_KEY_CONTRACT_INSTANCE`
- `SCV_LEDGER_KEY_NONCE`

JSON should avoid lossy number conversion. Large integers should be strings.

## API compatibility

This can be additive and non-breaking:

- keep `data_value_xdr` / `data_value`
- add `key_decoded`
- add `value_decoded`
- optionally add `key_xdr` and `value_xdr`
- optionally add `decode=true|false`, default `true` once stable

For very large values, consider:

```text
?decode=false
?decode_depth=3
?include_raw=true|false
```

## Acceptance criteria

- A customer can query `/api/v1/silver/contracts/{id}/storage` and understand the returned storage without writing XDR decoding code.
- Response includes decoded key and decoded value for common Soroban storage shapes.
- Raw XDR remains available for verification.
- Decoding failures are per-row and non-fatal, e.g.:

```json
"value_decoded_error": "unsupported SCVal type ..."
```

- Existing mainnet backfill does not need to restart.
- Optional decoded-column enrichment can run after backfill as an idempotent job.

## Product note

This is the difference between a data lake that exposes raw blockchain internals and a semantic API that application developers can consume directly. For wallet builders, domain registries, token dashboards, and Prism, decoded contract storage should be considered core product surface, not a convenience feature.
