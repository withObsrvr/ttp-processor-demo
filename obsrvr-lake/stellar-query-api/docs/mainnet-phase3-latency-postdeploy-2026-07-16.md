# Phase 3 Mainnet Serving Latency Gate

- Recorded: `2026-07-16T23:18:21.414828+00:00`
- Warm runs per route: `5`
- Historical ledger fixture: `40000008`
- Transaction fixture: `ff8079414a2c68531255b774a7f3f428009f2d53f7c9aa333327a6e27230444c`
- Account fixture: `GAQ7KSLJH7CKSDBJI37KIXYISFRC6YYHBXV7OQJF5DAKXUJSFYOVPOSL`
- Smart-wallet fixture: `CDASVYLQISK4WX7NC6C3GTUSYVZFOWQHSPWVGRP6R4COVQ2DK4K2SFBV`

The cold value is the first request made by this audit process; it does not
purge upstream, DuckDB, PostgreSQL, CDN, or Gateway caches. The acceptance
gate requires every request to return 2xx within its hard timeout and rejects
Gateway 502/504 responses. Initial p95 targets are reported separately.

| Target | Route | Cold | Warm p50 | Warm p95 | Target | Hard | Result |
| --- | --- | ---: | ---: | ---: | ---: | ---: | --- |
| direct | `account_balances` | 97ms | 102ms | 114ms | <500ms | 2000ms | PASS, target |
| direct | `account_overview` | 1614ms | 2099ms | 2148ms | <2000ms | 4000ms | PASS, above target |
| direct | `decoded_transaction_batch` | 293ms | 147ms | 176ms | <2000ms | 4000ms | PASS, target |
| direct | `health` | 105ms | 120ms | 135ms | <500ms | 2000ms | PASS, target |
| direct | `historical_ledger_detail` | 3705ms | 3690ms | 3737ms | <4000ms | 6000ms | PASS, target |
| direct | `home_summary` | 2621ms | 1612ms | 2666ms | <3000ms | 4000ms | PASS, target |
| direct | `recent_ledgers` | 107ms | 109ms | 118ms | <500ms | 2000ms | PASS, target |
| direct | `recent_transactions` | 106ms | 102ms | 106ms | <500ms | 2000ms | PASS, target |
| direct | `smart_wallet_lookup` | 999ms | 1048ms | 1079ms | <2000ms | 4000ms | PASS, target |
| direct | `transaction_receipt` | 91ms | 104ms | 114ms | <500ms | 2000ms | PASS, target |
| gateway | `account_balances` | 118ms | 107ms | 113ms | <500ms | 2000ms | PASS, target |
| gateway | `account_overview` | 1116ms | 1657ms | 2116ms | <2000ms | 4000ms | PASS, above target |
| gateway | `decoded_transaction_batch` | 359ms | 125ms | 133ms | <2000ms | 4000ms | PASS, target |
| gateway | `health` | 173ms | 121ms | 127ms | <500ms | 2000ms | PASS, target |
| gateway | `historical_ledger_detail` | 1969ms | 151ms | 157ms | <4000ms | 6000ms | PASS, target |
| gateway | `home_summary` | 117ms | 113ms | 124ms | <3000ms | 4000ms | PASS, target |
| gateway | `recent_ledgers` | 132ms | 131ms | 150ms | <500ms | 2000ms | PASS, target |
| gateway | `recent_transactions` | 138ms | 124ms | 139ms | <500ms | 2000ms | PASS, target |
| gateway | `smart_wallet_lookup` | 1128ms | 151ms | 170ms | <2000ms | 4000ms | PASS, target |
| gateway | `transaction_receipt` | 114ms | 103ms | 114ms | <500ms | 2000ms | PASS, target |

Hard gate: **PASS**
Initial p95 targets: **NOT YET MET**

## Interpretation

- All 120 measured requests returned `2xx` within the route hard limit. No
  Gateway `502` or `504` was observed.
- Eighteen of twenty route/target combinations met their initial p95 target.
  Account overview was the only miss: direct exceeded two seconds by 148 ms
  and Gateway exceeded it by 116 ms. Both remained well below the four-second
  hard limit.
- Smart-wallet detection returns a marked partial response when its 900 ms
  evidence budget is exhausted. The HTTP handler does not wait for database
  cancellation cleanup; direct p95 was 1.079 seconds.
- Historical full-ledger reads stop collecting at 3.5 seconds and return
  completed sections with `partial=true` and warnings. The direct p95 was
  3.737 seconds and the Gateway path remained below its four-second target.
- The transaction and account fixtures were discovered immediately before the
  run. Reusing a transaction after it ages out of Silver Hot can force an
  unpruned cold scan until the historical transaction-index backfill is
  complete; that is a separate coverage test, not the live-path latency gate.
