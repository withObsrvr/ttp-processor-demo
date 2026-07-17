# Phase 3 Mainnet Serving Latency Gate: Pre-Deploy Baseline

- Recorded: `2026-07-16T18:00:18Z`
- Warm runs per route: `5`
- Historical ledger fixture: `40000000`
- Transaction fixture: `ffc89bbbdb63c741e0eacd49208352d7d7ed7104eadd912defbfd76148f238a9`
- Account fixture: `GAG2ZUYVXBJPLKNDF3TQFO2S6X7J7VDQJ5CMHRMQWG57K52KSHEHNVVK`
- Smart-wallet fixture: `CDASVYLQISK4WX7NC6C3GTUSYVZFOWQHSPWVGRP6R4COVQ2DK4K2SFBV`

The cold value is the first request made by the audit process. It does not
purge upstream, DuckDB, PostgreSQL, CDN, or Gateway caches. The acceptance gate
requires every request to return `2xx` within its hard timeout and rejects
Gateway `502`/`504` responses. Initial p95 targets are reported separately.

| Target | Route | Cold | Warm p50 | Warm p95 | Target | Hard | Result |
| --- | --- | ---: | ---: | ---: | ---: | ---: | --- |
| direct | `account_balances` | 125ms | 101ms | 111ms | `<500ms` | 2000ms | PASS, target |
| direct | `account_overview` | 1197ms | 1636ms | 2110ms | `<2000ms` | 4000ms | PASS, above target |
| direct | `decoded_transaction_batch` | 188ms | 149ms | 154ms | `<2000ms` | 4000ms | PASS, target |
| direct | `health` | 105ms | 103ms | 116ms | `<500ms` | 2000ms | PASS, target |
| direct | `historical_ledger_detail` | 6089ms | 2088ms | 2383ms | `<4000ms` | 6000ms | **FAIL**, target when warm |
| direct | `home_summary` | 2563ms | 2479ms | 2540ms | `<3000ms` | 4000ms | PASS, target |
| direct | `recent_ledgers` | 102ms | 102ms | 107ms | `<500ms` | 2000ms | PASS, target |
| direct | `recent_transactions` | 117ms | 101ms | 104ms | `<500ms` | 2000ms | PASS, target |
| direct | `smart_wallet_lookup` | 233ms | 196ms | 211ms | `<2000ms` | 4000ms | PASS, target |
| direct | `transaction_receipt` | 103ms | 103ms | 111ms | `<500ms` | 2000ms | PASS, target |
| Gateway | `account_balances` | 139ms | 127ms | 416ms | `<500ms` | 2000ms | PASS, target |
| Gateway | `account_overview` | 1169ms | 2119ms | 2215ms | `<2000ms` | 4000ms | PASS, above target |
| Gateway | `decoded_transaction_batch` | 233ms | 107ms | 127ms | `<2000ms` | 4000ms | PASS, target |
| Gateway | `health` | 149ms | 104ms | 131ms | `<500ms` | 2000ms | PASS, target |
| Gateway | `historical_ledger_detail` | 2359ms | 151ms | 155ms | `<4000ms` | 6000ms | PASS, target |
| Gateway | `home_summary` | 106ms | 108ms | 123ms | `<3000ms` | 4000ms | PASS, target |
| Gateway | `recent_ledgers` | 109ms | 109ms | 117ms | `<500ms` | 2000ms | PASS, target |
| Gateway | `recent_transactions` | 137ms | 116ms | 139ms | `<500ms` | 2000ms | PASS, target |
| Gateway | `smart_wallet_lookup` | 326ms | 156ms | 160ms | `<2000ms` | 4000ms | PASS, target |
| Gateway | `transaction_receipt` | 115ms | 120ms | 186ms | `<500ms` | 2000ms | PASS, target |

Hard gate: **FAIL**

Initial p95 targets: **NOT YET MET**

## Findings

1. Serving-table routes, receipts, and decoded hash batches are already
   consistently interactive.
2. Smart-wallet detection is healthy for a valid mainnet heuristic candidate.
3. Account overview remains bounded by the four-second hard limit, but its
   direct and Gateway warm p95 values narrowly miss the initial two-second
   target.
4. Home summary is below target directly and appears cacheable through Gateway.
5. A first direct read of ledger `40000000` exceeded the six-second public hard
   limit. The existing handler used a ten-second internal composite timeout,
   so Gateway could stop forwarding before the API produced a response.

## Phase 3 Response

The Query API Phase 3 image changes the full-ledger composite query budget to a
configurable five seconds:

```text
QUERY_API_LEDGER_FULL_TIMEOUT=5s
```

If optional sections do not finish within that budget, the endpoint returns a
bounded `200` response with:

```json
{
  "partial": true,
  "warnings": [
    "ledger detail query budget exhausted; unavailable sections were omitted"
  ]
}
```

This behavior must be verified against a different uncached historical ledger
after deployment. A historical backfill is not justified by warm latency alone.

## Reproduction

```bash
export OBSRVR_GATEWAY_API_KEY='<gateway API key>'

python3 scripts/phase3_latency_gate.py \
  --warm-runs 5 \
  --historical-ledger 40000000 \
  --out-prefix /tmp/mainnet-serving-phase3-latency
```

Use `--strict-targets` when the initial p95 targets, rather than only the hard
availability gate, are required to make the command fail.

