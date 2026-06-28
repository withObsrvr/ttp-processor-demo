# Account index-plane — shaped fix

_Written 2026-06-27. The prerequisite for fast federated account history. Without it, the cold arm of
`/silver/accounts/{id}/transactions` scans deep cold for sparse accounts and times out (30s+). Reuses the
existing index-plane pattern (`tx_hash_index`, `contract_events_index`). Unblocks
`account-history-cold-federation-shaped-fix.md` (query-api wiring + Prism wiring already done, just gated
on cold-read speed)._

## Problem
The federated account-history cold arm filters `enriched_history_operations` (and token_transfers /
contract_invocations) by participant (`source_account = $1 OR destination = $1 OR …`). That filter is
**uncorrelated with the `ledger_range` partition key**, and cold has no per-account index — so DuckDB
must scan deep cold to find an account, every query. For a **sparse** account it's pathological:
`GB32463O…` has ~887 sends spread over ~27M ledgers behind ~220 recent receives, and the query times out
(measured 30s+ even bounded; 193s unbounded). The row-count "overscan" bound caps rows returned but does
**not** prune the scan. Result: account pages can't show pre-handoff history for real accounts.

## Reuse what already exists (don't invent)
The stack already has this exact pattern:
- `index-plane-transformer` writes **`index.tx_hash_index`** (`tx_hash → ledger_range`) into the DuckLake
  `index` catalog, partitioned by `ledger_range`.
- `contract-event-index-transformer` writes **`index.contract_events_index`** (`contract_id → ledger`).
- query-api reads them: `IndexReader.LookupTransactionHash` (`WHERE tx_hash = ?`),
  `ContractIndexReader` (`SELECT DISTINCT ledger_sequence … WHERE contract_id = ?`).

The account index-plane is `contract_events_index`, keyed by **account** instead of contract.

## Appetite
**2 weeks** (a real cycle: new index table + transformer path + backfill + query-api wiring).

## Solution (fat-marker)
1. **Index table** `index.account_ledger_index` in the DuckLake `index` catalog, partitioned by
   `ledger_range`:
   ```
   (account_id VARCHAR, ledger_range BIGINT, ledger_sequence BIGINT)   -- one row per (account, ledger) it appears in
   ```
   Keep it small: one row per account per ledger it participates in (NOT per op). Roles collapse — an
   account that is source OR destination OR from OR to OR address OR contract-invoker in a ledger gets one
   row. (Granularity note below.)
2. **Index transformer** — extend `index-plane-transformer` (or a sibling) to emit account rows alongside
   `tx_hash_index`, from the same silver source it already reads: for each ledger, `UNION` the distinct
   accounts across `enriched_history_operations` (source_account, destination, from_account, to_address,
   address), `token_transfers_raw` (from_account, to_account), `contract_invocations_raw` (source_account).
   Incremental per flush + a **one-time backfill** that scans cold once to build history (the expensive
   part, but it's one pass and it's what makes deep history queryable).
3. **Query-api** — `AccountIndexReader.LookupAccountLedgerRanges(account, beforeLedger, limit)` returns the
   account's `ledger_range`s (newest-first). Feed them into `buildAccountTransactionsQuery`'s **cold arm**
   as `AND ledger_range IN (<ranges>)` — turning the deep scan into a read of only the partitions where the
   account actually appears. Hot arm unchanged. Pagination walks the index's ledger_ranges.
4. **Re-tune Prism** — once cold reads are fast, raise the federated limit back up and drop the 5s
   fail-fast cap (it was added to survive the slow path).

## Granularity (the one real design decision)
- **Per-(account, ledger_range)** (50k-ledger grain): smallest index, prunes to 50k-ledger partitions —
  the cold arm still scans within a matched partition but only matched ones. Good enough for sparse
  accounts (few partitions); a dense account matches many partitions but has productive rows in each.
- **Per-(account, ledger_sequence)**: bigger index, finer pruning. Start with **ledger_range** (matches
  how cold is partitioned and how `contract_events_index` works); go finer only if EXPLAIN shows the
  in-partition scan still dominates.

## Rabbit holes (don't)
- Don't index per-operation (row explosion); index per (account, ledger) presence.
- Don't build a brand-new service if `index-plane-transformer` can emit a second table — reuse its silver
  read, its DuckLake write path, and its compaction.
- Don't skip the backfill — incremental-only leaves all pre-index history unqueryable (which is exactly
  the history users want).
- Don't change the federated query's output/dedup/pagination — only add the `ledger_range IN (...)` prune
  to the cold arm.

## No-Gos
Re-clustering cold by account (huge rewrite), a separate per-account materialized history table, changing
the hot path.

## Done
- `LookupAccountLedgerRanges('GB32463O…')` returns its ledger_ranges from the index in ms.
- `/silver/accounts/GB32463O…/transactions?limit=400&order=desc` returns in **a few seconds** (cold pruned
  to the account's partitions), including pre-handoff **sends**.
- The Prism account page (wiring already shipped) shows the full hot+cold history with sends labeled
  `Sent`; the 5s fail-fast cap is removed.
- Backfill has built the index over all cold history; incremental keeps it current per flush.

## Notes
- The federated query bounding + the int32-cold fix + the Prism wiring are **already in place** — this is
  the missing piece that makes them perform. See `account-history-cold-federation-shaped-fix.md` and
  `enriched-operations-participant-indexes-shaped-fix.md` (the hot-side analog of this).
- This generalizes beyond accounts: any "all activity for entity X" cold query (assets, claimable
  balances) has the same uncorrelated-filter problem and the same index-plane answer.
