# Serving `ON CONFLICT` constraints — shaped fix

> **✅ IMPLEMENTED 2026-06-26** — self-heal block appended to
> `serving-projection-processor/go/schema/serving_schema.sql` (runs inside `EnsureServingSchema`
> on every startup). It adds the required unique index to any serving table that lacks one
> (the backfill case) and is a no-op when the table already has any unique index (PK or prior
> patch), so no redundant indexes. Validated on a throwaway Postgres (fresh tables → 1 unique
> index each; constraint-less table → index added) + `go test` green. Ships in the next image.

_Written 2026-06-26 after the mainnet serving stall. The serving cold-backfill creates serving tables
**without** the unique constraints the streaming processor's `ON CONFLICT` upserts require, so a fresh
streaming start throws `42P10` and serving freezes. Implements the Horizon-1 "handoff hardening" item
(roadmap doc, H1.3) for the serving side. The full constraint set below is the artifact — derived by
auditing every projector's `ON CONFLICT` clause against the live serving schema._

## Problem
`serving-cold-backfill` (and/or the schema bootstrap) creates the serving tables but **not** the unique
constraints. The streaming `serving-projection-processor` upserts with `INSERT … ON CONFLICT (cols) DO
UPDATE`, which requires a matching unique index/constraint. Without it every upsert fails:
```
ERROR: there is no unique or exclusion constraint matching the ON CONFLICT specification (SQLSTATE 42P10)
```
Critically the **checkpoint save** (`sv_projection_checkpoints ON CONFLICT (projection_name, network)`)
fails, so **no projector can advance its checkpoint → serving freezes at N** and re-processes the same
window forever. Hit prod **twice** (regular processor: `accounts_current`/`network_stats`; `-fast`: the 5
recent-feed tables) and **testnet serving will hit it identically**. We hand-patched prod with
`CREATE UNIQUE INDEX`; this is the durable fix so a fresh handoff never hits it.

## Appetite
**1 day.**

## Solution (fat-marker)
The `serving-projection-processor` already "auto-applies the serving schema" on startup. Extend that step
to **idempotently ensure every required `ON CONFLICT` constraint exists** before it starts projecting —
one loop over a `requiredServingConstraints` list issuing `CREATE UNIQUE INDEX IF NOT EXISTS …`. This fixes
both **fresh** backfills and **existing constraint-less** tables (the processor self-heals on startup —
which would have prevented this entire incident). Optionally also create them in `serving-cold-backfill`'s
table-creation so tables are born correct. Keep the key list **derived from the projectors' `ON CONFLICT`
clauses** (single source of truth) so a new projector can't silently drift.

## The complete constraint set (the artifact)
Upsert tables (need the unique index for `ON CONFLICT`):
| Table | Unique key |
|---|---|
| `sv_projection_checkpoints` | `(projection_name, network)` ← gates ALL checkpoint saves |
| `sv_accounts_current` | `(account_id)` |
| `sv_network_stats_current` | `(network)` |
| `sv_ledger_stats_recent` | `(ledger_sequence)` |
| `sv_operations_recent` | `(operation_id)` |
| `sv_transactions_recent` | `(tx_hash)` |
| `sv_tx_receipts` | `(tx_hash)` |
| `sv_contract_calls_recent` | `(call_id)` |
| `sv_events_recent` | `(event_id)` |
| `sv_explorer_events_recent` | `(event_id)` |

Ready-to-apply DDL (idempotent):
```sql
CREATE UNIQUE INDEX IF NOT EXISTS sv_projection_checkpoints_pn_uq  ON serving.sv_projection_checkpoints (projection_name, network);
CREATE UNIQUE INDEX IF NOT EXISTS sv_accounts_current_account_id_uq ON serving.sv_accounts_current (account_id);
CREATE UNIQUE INDEX IF NOT EXISTS sv_network_stats_current_net_uq  ON serving.sv_network_stats_current (network);
CREATE UNIQUE INDEX IF NOT EXISTS sv_ledger_stats_recent_ls_uq     ON serving.sv_ledger_stats_recent (ledger_sequence);
CREATE UNIQUE INDEX IF NOT EXISTS sv_operations_recent_op_uq       ON serving.sv_operations_recent (operation_id);
CREATE UNIQUE INDEX IF NOT EXISTS sv_transactions_recent_tx_uq     ON serving.sv_transactions_recent (tx_hash);
CREATE UNIQUE INDEX IF NOT EXISTS sv_tx_receipts_tx_uq             ON serving.sv_tx_receipts (tx_hash);
CREATE UNIQUE INDEX IF NOT EXISTS sv_contract_calls_recent_cc_uq   ON serving.sv_contract_calls_recent (call_id);
-- sv_events_recent / sv_explorer_events_recent (event_id) already exist in prod; keep in the list for fresh DBs.
```
**Recompute tables need NO constraint** (they DELETE+INSERT a full rebuild each cycle, no upsert):
`sv_assets_current`, `sv_asset_stats_current`, `sv_contracts_current`, `sv_contract_stats_current`,
`sv_contract_function_stats_current`, `sv_account_balances_current`.

## Rabbit holes (don't)
- Don't redesign the projector upsert model or change any `ON CONFLICT` key.
- Don't add constraints to the recompute (DELETE+INSERT) tables.
- Don't dedupe inside the migration — if `CREATE UNIQUE INDEX` fails on a duplicate, **fail loudly** (it
  signals a real upstream bug); don't silently drop rows.
- Keep every statement `IF NOT EXISTS` — it must be safe to run on every startup.

## No-Gos
Schema rewrite, touching streaming/projector logic, changing conflict keys.

## Done
- Against a constraint-less serving DB, starting `serving-projection-processor` **creates all required
  unique indexes** and then streams with **zero `42P10`** (regular *and* `-fast`).
- Test: drop the constraints on a scratch serving DB, start the processor, confirm it recreates them and
  the checkpoint advances.
- The key list lives in **one place**, derived from / asserted against the projectors' `ON CONFLICT`
  clauses, so adding a projector without a constraint fails a test rather than prod.

## Notes
- Same root cause as the earlier **missing serving indexes** (the 50M-row index builds on first start) —
  fold both into one "ensure serving schema (constraints + indexes)" startup step.
- **Until this ships:** apply the DDL above by hand to testnet serving before/when it streams, and any
  rebuilt-from-scratch serving DB, or it will freeze at N exactly as mainnet did.
- Connects to roadmap H1.3 (handoff hardening) — this is the serving-side concrete instance.
