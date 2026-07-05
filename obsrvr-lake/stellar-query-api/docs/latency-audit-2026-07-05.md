# Stellar Query API Latency Audit - Testnet - 2026-07-05

## Scope

Tested direct testnet API:

`https://obsrvr-lake-testnet.withobsrvr.com`

Method:

- Safe GET endpoints only. No mutation/admin/archive POST/DELETE paths.
- Two runs per endpoint.
- 12 second client timeout for the broad audit.
- Seed data discovered from:
  - `/api/v1/silver/ledgers/recent?limit=3`
  - `/api/v1/silver/transactions/recent?limit=5`
  - `/api/v1/silver/data-boundaries`
- Results CSV: `/tmp/stellar-api-latency-audit-direct.csv`
- Harness: `scripts/latency_audit.py`

Seed values from this run:

- latest ledger: `3451692`
- recent tx: `ec5fdc6bd3e12effebe95efdba55489631a8aef75dd8151302b75e686025d3de`
- account: `GCLWKHHHGBOYXMTSFBJNGCFEWIQ4NZWAGZR6GPB4NLMSLBYW4UP3N4SQ`
- contract: `CDLZH2XWR46NF6EGV2JBEARYBOJQ5II2RZLNFBP63LCXBL524Y7ZNMOC`

## Classification

Endpoint classes use the worst run:

- fast: `<250ms`
- ok: `250ms-1s`
- slow: `1s-3s`
- critical: `>3s but returned`
- error: timeout, 5xx, or invalid request shape

Summary across 67 endpoint cases:

- fast: 31
- ok: 2
- slow: 4
- critical: 3
- error: 27

Some `error` rows are expected 400s because the generic audit used a minimal query shape. Those are not performance failures. The important failures are the repeated 12s/13s timeouts and 500s in the API logs.

## Fast APIs

These are usable for Prism-style interactive views today.

Core/recent:

- `/health`
- `/api/v1/silver/data-boundaries`
- `/api/v1/silver/ledgers/recent?limit=10`
- `/api/v1/silver/transactions/recent?limit=10`
- `/api/v1/silver/stats/network`
- `/api/v1/silver/stats/fees`

Hot ledger:

- `/api/v1/silver/ledger/{latest}/summary`
- `/api/v1/silver/ledger/{latest}/full`

Keyed account:

- `/api/v1/silver/accounts/current?account_id=...`
- `/api/v1/silver/accounts/{id}/balances`
- `/api/v1/silver/accounts/{id}/contracts`

Recent transaction:

- `/api/v1/silver/tx/{hash}/receipt`
- `/api/v1/silver/tx/{hash}/effects`
- `/api/v1/silver/tx/{hash}/decoded`
- `/api/v1/silver/tx/{hash}/semantic`
- `/api/v1/silver/tx/{hash}/full`
- `/api/v1/silver/tx/{hash}/contracts-summary`

Contracts/assets/semantic:

- `/api/v1/silver/contracts/{id}/analytics`
- `/api/v1/silver/contracts/{id}/metadata`
- `/api/v1/silver/assets?limit=10`
- `/api/v1/silver/assets/XLM/stats`
- `/api/v1/silver/assets/XLM/holders?limit=10`
- `/api/v1/semantic/*` tested paths
- `/api/v1/semantic/defi/*` tested paths

## Usable But Not Consistently Fast

These returned but show cold-cache or DuckLake metadata penalties.

- `/api/v1/home/summary`: 227-635ms
- `/api/v1/explorer/summary`: 226-261ms
- `/api/v1/silver/ledger/{older}/summary`: 562ms warm, 5087ms cold
- `/api/v1/silver/ledger/{older}/full`: 512ms warm, 2427ms cold
- `/api/v1/silver/ledger/{very_old}/full`: 468ms warm, 2894ms cold
- `/api/v1/silver/accounts/{id}/transactions?limit=5`: 769ms warm, 3505ms cold
- `/api/v1/silver/transfers/stats?group_by=asset`: 183ms warm, 12074ms cold
- `/api/v1/silver/contracts/{id}/recent-calls?limit=10`: 280ms warm, 1043ms cold
- `/api/v1/bronze/ledgers?...`: 309ms warm, 1579ms cold

Interpretation: these are cache-sensitive. They may look okay after warming but should not be assumed consistently interactive.

## Slow Or Unusable APIs

These timed out at 12s/13s or returned 500s in the API logs under ordinary small-limit requests.

Global/list feeds:

- `/api/v1/silver/accounts?limit=5`
- `/api/v1/silver/accounts/history?account_id=...&limit=10`
- `/api/v1/silver/accounts/{id}/offers?limit=5`
- `/api/v1/silver/explorer/account?account_id=...`
- `/api/v1/silver/operations/enriched?limit=10`
- `/api/v1/silver/payments?limit=10`
- `/api/v1/silver/operations/soroban?limit=10`
- `/api/v1/silver/events?limit=10`
- `/api/v1/silver/events/generic?limit=10`

Transaction details:

- `/api/v1/silver/tx/{hash}/diffs`
- `/api/v1/silver/tx/{hash}/events`

Address/token portfolio:

- `/api/v1/silver/addresses/{addr}/balances`
- `/api/v1/silver/address/{addr}/token-balances`

Contract/storage/event:

- `/api/v1/silver/contracts/{id}/storage?limit=25`
- `/api/v1/silver/events/contract/{id}?limit=10`

Index endpoints:

- `/api/v1/index/transactions/{hash}`
- `/api/v1/index/contracts/health`
- `/api/v1/index/contracts/{id}/ledgers`
- `/api/v1/index/contracts/{id}/summary`

Other:

- `/api/v1/silver/stats/soroban`
- `/api/v1/silver/assets/XLM`
- `/api/v1/silver/assets/XLM/pairs?limit=10`
- `/api/v1/bronze/stats/network`

## Filtered Retests

Additional targeted checks with narrower filters:

- `/api/v1/silver/events?start_ledger={latest}&end_ledger={latest}&limit=10`: 183ms
- `/api/v1/silver/events/generic?start_ledger={latest}&end_ledger={latest}&limit=10`: 1765ms
- `/api/v1/silver/contracts/{id}/storage?limit=25&durability=persistent`: 1788ms
- `/api/v1/silver/accounts/{id}/balances`: 103ms

Still timed out even with filters:

- `/api/v1/silver/operations/enriched?ledger_sequence={latest}&limit=10`
- `/api/v1/silver/payments?account_id={account}&limit=10`
- `/api/v1/silver/operations/soroban?account_id={account}&limit=10`
- `/api/v1/silver/contracts/{id}/storage?limit=25&live_only=false`
- `/api/v1/silver/tx/{hash}/diffs`
- `/api/v1/silver/tx/{hash}/events`
- `/api/v1/silver/addresses/{addr}/balances`

## Conclusions

The API has two distinct performance profiles.

Fast:

- hot/recent paths
- direct keyed account current/balances/contracts
- recent tx receipt/decode/semantic/full
- semantic and DeFi serving endpoints
- simple asset stats/holders

Slow:

- broad account/list/history endpoints
- unified address balances/token portfolio
- raw operations/payments/soroban feeds
- generic/global event feeds without tight ledger filters
- tx diffs/events
- contract storage/events
- index-plane point lookups backed directly by DuckLake
- cold aggregate stats

The main bottleneck pattern is not HTTP routing. It is broad DuckDB/DuckLake scans, often triggered by endpoints that look small because they have `limit=10` but still need to search cold data before they can return those 10 rows.

## Prism Guidance

Prefer these now:

- home: `/api/v1/home/summary`, `/api/v1/silver/ledgers/recent`, `/api/v1/silver/transactions/recent`
- account shell: `/api/v1/silver/accounts/current?account_id=...`
- account balances: `/api/v1/silver/accounts/{id}/balances`
- account contracts: `/api/v1/silver/accounts/{id}/contracts`
- recent tx: `/receipt`, `/decoded`, `/semantic`, `/full`, `/effects`
- contract shell: `/contracts/{id}/metadata`, `/contracts/{id}/analytics`, `/contracts/{id}/recent-calls`
- semantic/DeFi paths

Avoid as first-load dependencies:

- `/api/v1/silver/accounts`
- `/api/v1/silver/explorer/account`
- `/api/v1/silver/addresses/{addr}/balances`
- `/api/v1/silver/address/{addr}/token-balances`
- `/api/v1/silver/tx/{hash}/diffs`
- `/api/v1/silver/tx/{hash}/events`
- `/api/v1/silver/contracts/{id}/storage`
- global raw operations/payments/soroban/events feeds

## Recommended API Work

1. Add fast-fail/degraded behavior for endpoints that currently run past 12s and return 500.
2. Make expensive endpoints require a selective filter or default to hot-only/recent-only.
3. Build serving tables for:
   - account transaction/activity feed
   - account token portfolio
   - transaction diffs/events
   - contract storage summary/current live keys
   - account offers
4. Move Index Plane interactive lookups out of DuckLake point queries or add a cache/serving copy.
5. Add per-endpoint latency SLO tests in CI or a recurring synthetic probe.

## Post Tier 1 Deployment

Deployed to testnet as `withobsrvr/stellar-query-api:6b46cae-20260705213449`
on Nomad job version 31.

Changes verified:

- Query guards are active: interactive paths now use a 4s API timeout and optional enrichments use a 500ms timeout.
- `/api/v1/silver/events` and `/api/v1/silver/events/generic` default to a recent ledger window when unfiltered.
- `/api/v1/silver/addresses/{addr}/balances` no longer waits on the Soroban portfolio scan; transfer-history token portfolio data is opt-in with `include_tokens=true`.
- `/api/v1/silver/assets/XLM` no longer waits on optional enrichment joins by default; enrichment is opt-in with `include_enrichment=true`.
- `/api/v1/silver/explorer/account` now returns 200 for the audit seed instead of 503/timeouts, roughly 2.2-3.1s.
- `/api/v1/silver/assets/XLM` is fast in the final audit: ~102-107ms.
- `/api/v1/silver/addresses/{addr}/balances` is fast in the final audit: ~97-112ms.
- Raw recent-window operations endpoints improved substantially:
  - `/operations/enriched?limit=10`: ~170-230ms
  - `/payments?limit=50`: ~182-184ms
  - `/operations/soroban?limit=50`: ~179-185ms
- Events paths now classify fast/ok:
  - `/events?limit=10`: ~229-337ms
  - `/events/generic?limit=10`: ~377-396ms
- Prism-critical transaction detail paths are fast:
  - receipt, diffs, decoded, semantic, full, effects, contracts summary are ~100-174ms in the final audit.

Still not fixed for interactive use:

- Account list, account transactions, account offers: bounded 503 around 4.6-5.2s.
- Transaction events, contract storage/events, contract index lookup/summary/ledgers: bounded 503 around 4.6-5.2s.
- Contract index health, Soroban stats, bronze network stats: return 200 but still take roughly 4.6-7.1s.
- Account overview is successful but still slow, around 2.2-3.1s.
- Account history and some cold ledger first-hit reads still show object-storage/cache warmup latency, then repeat requests are usually ok.
- `/silver/address/{addr}/token-balances` remains a bounded 503 when the transfer-history portfolio scan cannot finish quickly; use `/addresses/{addr}/balances` for the fast default shell.

Final audit artifact:

- `/tmp/stellar-api-latency-audit-post-tier1.csv`

Conclusion: Tier 1 guardrails, recent-window defaults, and opt-in slow enrichments are deployed and working. The final full audit has zero 12s timeout rows. The API still needs serving projections or cached/index-backed copies for the remaining broad account, contract, stats, and index endpoints. The remaining slow paths are bounded, but several are still not useful as synchronous Prism first-load dependencies.
