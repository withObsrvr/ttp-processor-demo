# Account page reads cold (hot+cold federation) — shaped fix

_Written 2026-06-27. The account page shows only **post-handoff** activity because it reads the
hot table only; an account's full history lives in **silver cold**. The good news: the hot+cold
federation **already exists** in the codebase — the account page just doesn't use it._

## Problem
The account overview/activity endpoint (`account_activity.go` → `/api/v1/silver/explorer/account`)
reads **hot only** — `enriched_history_operations` covers just ledgers **62,800,000 → now** (the
streaming era). Any account active before the handoff shows incomplete history. Confirmed with a
real account, `GB32463O…XVY7X`:
- Its sequence number decodes to **887 transactions sourced (sent)** and creation at ledger
  **35,238,010** — so it has sent ~887 txns, all **before** ledger 62.8M.
- The account page shows **0 sends** — because those operations are in **silver cold**
  (`enriched_history_operations` ≈ 23.3B rows), which this endpoint never queries.

The data is intact and federation logic exists — this is a **read-path wiring** gap, not data loss.

## What already exists (don't rebuild)
`account_history.go` already implements **"hot+cold query-layer federation with hot-over-cold
de-duplication"**:
- `/api/v1/silver/accounts/{id}/transactions` — `buildAccountTransactionsQuery(hotSchema,
  coldSchema, …)` does `UNION ALL` of hot + cold with dedup (`account_history.go:340,399–440`).
- `GetAddressBalanceHistory` federates `balance_changes` hot+cold (`:444–534`).

The account page's activity endpoint (`account_activity.go`) is the **hot-only** holdout — its own
comment admits it (`:353` "for full history, we'd query cold too, but hot covers the main use case").

## Appetite
**~1 week.**

## Solution (fat-marker)
1. **Verify cold is actually wired** — confirm `coldSchema` is configured for the account readers
   (the federation arms are guarded by `if coldSchema != ""`; if it's empty, even
   `/accounts/{id}/transactions` is silently hot-only).
2. **Point the account page at the federated path** — either have Prism's account history call
   `/accounts/{id}/transactions` (federated) instead of `/explorer/account` (hot-only), or rework
   `account_activity.go`'s query to the same hot+cold `UNION ALL` + hot-over-cold dedup pattern.
3. **Bound the cold read** — silver cold is Parquet partitioned by `ledger_range` with **no
   per-account index**, so an unbounded participant filter scans partitions. Keep page loads sane by:
   pushing the participant filter **and** `LIMIT` into each `UNION` arm, paginating by ledger window
   (newest-first), and leaning on `ledger_range` partition pruning. Deep "all-history" pulls are
   slower by design — that's acceptable for a paginated history view.
4. **Label the boundary in the UI** meanwhile (cheap honesty): if only hot is shown, Prism should
   say "older activity (before ledger 62.8M) is in the cold archive" rather than implying the account
   never sent. Ship this first if the full fix slips.

## Rabbit holes (don't)
- **Don't scan all 23.3B cold rows per page load.** Always bound by ledger window + `LIMIT`; the
  federation query already orders + limits — preserve that.
- **Don't rebuild the federation** — reuse `account_history.go`'s `buildAccountTransactionsQuery` /
  dedup; don't fork a second hot+cold merger.
- **Don't duplicate hot rows** — the hot-over-cold `ROW_NUMBER` dedup handles the overlap window;
  keep it.
- **Don't block on a full cold account index.** A proper account→ledger-range index (index-plane)
  would make deep history fast, but it's a *separate bet* — paginated federation is enough for v1.

## No-Gos
Re-architecting cold storage, building a new federation layer, changing the streaming handoff point.

## Done
- The account page for `GB32463O…` shows its **pre-handoff sends** (the ~887 sent txns appear), hot
  and cold merged with no duplicates across the boundary.
- Typical account history pages load within the gateway timeout (paginated).
- If the full read isn't ready, the page at least **labels** the hot/cold boundary instead of
  silently showing partial history.

## Notes
- Depends on `enriched-operations-participant-indexes-shaped-fix.md` for the **hot** arm to be fast;
  the **cold** arm's deep-history performance is the open question that an account index-plane would
  later solve (ties to the earlier "indexing in DuckLake/DuckDB" roadmap discussion).
- Same root theme as the serving work: the data was backfilled correctly into cold; the **read path**
  is what needs to catch up.
