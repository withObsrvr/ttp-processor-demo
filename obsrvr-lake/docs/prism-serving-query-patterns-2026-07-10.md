# Prism Serving Query Patterns - 2026-07-10

## Contract Storage Explorer

Use the summary endpoint first:

`GET /api/v1/silver/contracts/{contract_id}/storage/summary`

The response is backed by `serving.sv_contract_storage_summary` and includes:

- total/live/expired/deleted entry counts
- persistent/temporary/instance counts
- latest storage ledger/time
- projection coverage from `serving.sv_watermarks`

Then fetch paged rows:

`GET /api/v1/silver/contracts/{contract_id}/storage?live_only=true&limit=100`

This endpoint now prefers `serving.sv_contract_storage_current` and recomputes TTL status against the current serving ledger at read time. Use `live_only=false` only for diagnostic views that need expired rows.

## Contract Activity Header

Use:

`GET /api/v1/silver/contracts/{contract_id}/activity/summary`

The response is backed by `serving.sv_contract_activity_summary` and is intended for contract headers/cards:

- first/last seen ledger and time
- invocation counts for 24h/7d/30d/all
- event counts for 24h/7d/30d when the event serving feed is populated
- 30d unique callers and success/failure counts
- coarse `activity_classification`

## Smart Accounts

Use the existing routes:

- `GET /api/v1/silver/smart-accounts/lookup/credential/{credential_id}`
- `GET /api/v1/silver/smart-accounts/lookup/address/{address}`
- `GET /api/v1/silver/smart-accounts/{contract_id}/rules`
- `GET /api/v1/silver/smart-accounts/stats`

These now read serving tables first:

- `serving.sv_smart_account_signers`
- `serving.sv_smart_account_contracts`
- `serving.sv_smart_account_rules_current`

If serving watermarks are absent, the handlers fall back to the silver smart-account state tables.

## Client Rule

Display `coverage.complete_thru` when available. If `coverage` is missing, treat the response as a fallback or unmaterialized serving view rather than a complete app projection.
