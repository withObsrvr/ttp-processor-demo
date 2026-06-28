# Scaling Architecture: Lakehouse-as-Source + Derived Serving (and where quack fits)

_Written 2026-06-28, after the silver_hot starvation incident (account pages 45s-timing-out while the
backlog flush hammered the same Postgres). The current design serves live reads off the same database
the pipeline writes and mass-deletes from; that won't hold at community scale. This is the target
architecture, the migration path from where we are, and an honest read on betting the serving tier on
the DuckDB "quack" protocol. Companion to `project_ducklake_inlining_architecture` — this is that vision,
now unblocked by the 1.0 migration._

## The problem, named precisely
One Postgres (`silver_hot`) is doing two jobs with **opposite profiles**:
- **Write/batch plane** — transformer writes, the flusher's `postgres_scan` reads + **mass row-by-row
  DELETEs**, serving rebuilds, backfills. Throughput-oriented, lock- and IO-heavy, bursty.
- **Read/serving plane** — account/home/tx pages. Latency-sensitive, high-concurrency, tiny queries.

Co-located, the batch plane starves the serving plane. Connection tuning can't fix it — adding
connections to a saturated DB makes it worse. They must be **physically separated**.

## Target: three planes, the lake as system-of-record, serving derived from it
```
ingester ──► DuckLake (bronze, micro-batched)         system of record: cheap, append-only, replayable
                  │
   materializer reads recent lake ──► derives indexed read models
                  │
            Serving store (indexed read models) + read replicas    ← live reads ONLY here
                  │
            query-api / Prism
                  │
   (deep history) ──► index-plane ──► DuckLake cold                analytics + archive
```
The dependency **flips**: today it's `ingest → PG (source) → flush to cold`, with serving stuck on the
churning PG. The target is `ingest → lake (source) → materialize → serving (derived)`.

### Why this removes the starvation
- The **heavy work** (ingest, transform, compaction) lives on **DuckLake / object storage — isolated.**
- The serving store receives only **small incremental upserts** of read models — no mass deletes, no
  `postgres_scan`, nothing competing with account pages.
- Serving becomes **disposable and rebuildable** — reshape an `sv_*` model by replaying it from the lake.
  The lake is truth; serving is a fast projection.

### The non-negotiable principle
**Serving is always a derived, indexed read model — never queried straight off raw cold Parquet.** That's
the line that was crossed originally (the account page reads raw `enriched_history_operations`, the table
the flusher churns). Whatever engine backs serving, it reads precomputed, indexed models, not the lake.

## Where quack fits — the duck-stack bet, honestly
**What quack is:** an RPC protocol that turns DuckDB into a client-server DBMS — concurrent multi-client
network access, `quack:` protocol, supports TABLES/VIEWS/ACID/**INDEXES**, can query DuckLake, ~a few
thousand writes/sec on modest hardware. **Beta.**

It has two genuinely different possible roles here, with very different risk:

**Role 1 — concurrent access to the COLD lake (its sweet spot, lower risk).** Today each query-api / 
materializer attaches its own in-process DuckDB to DuckLake. A quack server lets many clients share one
DuckDB-over-DuckLake endpoint with proper concurrency. This helps cold reads, index-plane lookups, and
catalog concurrency — exactly what quack was built for. Good first place to try it.

**Role 2 — the SERVING store, all-duck (appealing, but unproven).** Put the indexed read models in
DuckDB tables and serve them via quack → no Postgres in the stack. Tempting for the duck bet. But three
honest caveats:
- **Quack doesn't change Parquet physics.** Serving one account from raw cold Parquet is still a deep
  scan — quack is transport + concurrency, not a point-lookup index. You still need indexed read models
  (and the index-plane); quack doesn't remove that work.
- **DuckDB's strength is analytical scans, not thousands of concurrent point reads.** Postgres is
  purpose-built for the hot-serving profile. Even with quack + DuckDB indexes, DuckDB as the hot-serving
  engine at community scale is unproven; "a few thousand writes/sec" is a *write* number, and the live
  explorer is read-fanout-dominated.
- **Beta.** Don't put the load-bearing serving tier of a public, community-scale explorer on a beta
  protocol as the only option.

**The play that honors the bet without taking the risk:** design the serving plane so the **store is
swappable**. Ship on **Postgres + read replicas** (proven) now. Run quack as a **parallel spike**:
1. Use it first for **Role 1** (concurrent cold-lake access) — low risk, its design center.
2. Then benchmark **Role 2**: load the `sv_*` read models into indexed DuckDB, serve via quack, and
   measure read **concurrency, p99 latency, and behavior under fan-out** against Postgres at target load.
   If it holds up, migrate the serving engine — **the architecture doesn't change, only the backend.**
   If it doesn't, you've lost nothing.

Betting on the duck stack is fine; betting your uptime on a beta as the *only* path is not. Swappable
serving lets you do both.

## Migration path from where we are (not a big-bang)
- **Today:** ingester → bronze hot PG → flush → bronze cold; transformer reads bronze hot PG → silver hot
  PG → flush → silver cold; serving-projection-processor reads silver hot → serving PG; query-api/Prism
  read silver hot + serving.
- **Step 0 — relief now (hours):** throttle the flusher (sleep between chunks) so it leaves DB headroom;
  it still drains the backlog. Site recovers without a full stop.
- **Step 1 — isolate reads (days):** streaming **read replica** of silver_hot; point the query-api's live
  reads at it. Biggest single win; standard PG.
- **Step 2 — kill the mass DELETE (1–2 wks):** native-partition the hot tables by `ledger_range`; flush a
  partition, then `DETACH`/`DROP` it (instant) instead of row-by-row DELETE. Removes the worst load.
- **Step 3 — flip to lake-as-source (a cycle):** ingester writes micro-batched bronze **DuckLake**
  directly (inlining + compaction for the small-file problem); transformer + serving materialize **from**
  the lake into the serving store. Retire hot-PG-as-source and the flush hop.
- **Step 4 — read models + index-plane:** account/entity pages served from indexed read models + the
  account index-plane; never raw tables. (See `account-index-plane-shaped-fix.md`.)
- **In parallel:** the **quack spike** — Role 1 (cold concurrency) first, then Role 2 (serving-store eval).

## Appetites / sequencing summary
| Step | Appetite | Risk | Payoff |
|---|---|---|---|
| 0 Throttle flusher | hours | low | site back up now |
| 1 Read replica | days | low | reads isolated from batch |
| 2 Partition + DROP | 1–2 wks | med | removes the delete storm |
| 3 Lake-as-source | a cycle | high | the real architecture; replayable |
| 4 Read models + index-plane | a cycle | med | fast deep history, decoupled |
| Quack spike (parallel) | a cycle | research | path to all-duck, de-risked |

## The bottom line
The fix isn't "serve from DuckLake" (we proved that's 30–45s/account this week). It's **lake as the
durable source of record + a fast, indexed, isolated serving projection on top of it.** That's your
inlining vision, now buildable on 1.0. Quack can make the stack all-duck and can help cold concurrency
today — adopt it deliberately as a swappable serving backend you've benchmarked, not as a bet you can't
back out of.
