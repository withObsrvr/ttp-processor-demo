# Quack serving-backend spike — shaped item

_Written 2026-06-28. We're betting on the duck stack and quack (beta RPC protocol that turns DuckDB into
a client-server DBMS — concurrent multi-client, `quack:` protocol, TABLES/VIEWS/ACID/INDEXES, can query
DuckLake) is appealing. Before putting any serving load on it, time-box a spike to get evidence. Companion
to `scaling-architecture-lakehouse-and-serving.md` — this informs the serving *engine*, not the
architecture (which keeps the serving store swappable regardless)._

## The question
Can DuckDB-via-quack serve our read path well enough — at community-scale concurrency, with acceptable
p99 — and is it operationally ready (it's beta)? If yes, the stack can be all-duck. If no, we stay on
Postgres + replicas and the architecture doesn't change.

## Appetite
**2 weeks, time-boxed.** It's research; ship a go/no-go, not a migration.

## Two roles — evaluate Role 1 first (lower risk, quack's design center)
1. **Role 1 — concurrent access to the COLD lake.** Replace each query-api/materializer attaching its own
   in-process DuckDB-over-DuckLake with **one shared quack server**. This is what quack is *for*; if it's
   shaky here, Role 2 won't be better. Lower blast radius (cold/analytics path, not hot serving).
2. **Role 2 — the SERVING store (all-duck).** Load the `sv_*` read models into **indexed DuckDB tables**,
   serve via quack, benchmark against Postgres for the live hot-read path.

## What to measure (the bar, not vibes)
- **Read p50/p95/p99** for the real serving query mix — account current-state lookup, home summary,
  recent feed, tx receipt — under **realistic concurrency** (100 / 500 / 1000 concurrent clients).
- **Throughput** (reads/sec) at an acceptable p99.
- **Read + write concurrency together** — the materializer upserting read models *while* serving reads
  (this is the real condition, and where Postgres is strong).
- **Index effectiveness** — confirm point lookups use DuckDB indexes (not scanning); this is the whole
  reason it could work at all.
- **Saturation behavior** — connection limits, backpressure, graceful degradation vs a cliff.
- **Operational** — HA story, restart/recovery, backup, observability; and a **multi-day soak** (it's
  beta — prove it doesn't fall over under sustained load).

## Go / no-go bar
- p99 point-read within ~**2× of Postgres** at target concurrency.
- No correctness issues under concurrent read+write.
- A credible **HA / ops** story (or a clear path to one).
- **Stable over a multi-day soak.**

## How to run it (do NOT touch prod serving)
- Stand up a quack server on a test box; load a **real-volume** snapshot of the `sv_*` read models into
  indexed DuckDB. Stand up a Postgres baseline on the same data.
- Replay a **realistic query mix** — capture the actual access shapes from the gateway/query-api logs
  (they show the real endpoints + cadence).
- Side-by-side: same data, same queries, same concurrency. Role 1 first; only proceed to Role 2 if Role 1
  holds.

## Rabbit holes (don't)
- Don't migrate any prod serving onto quack during the spike — the backend stays swappable; this is a
  test, not a commitment.
- Don't benchmark toy data — real `sv_*` volumes and real query shapes, or the result is meaningless.
- Don't confuse the quoted **write** number ("a few thousand writes/sec") with the **read-fanout** profile
  that actually decides serving.
- Don't let "it's all duck" override the numbers — the bar is latency/concurrency/stability, not stack
  purity. Quack also does NOT remove the index-plane or read-model work; it's transport + concurrency,
  not a Parquet point-index.

## No-gos
Putting the live, public community explorer's load-bearing serving tier on a beta protocol before it
passes the bar.

## Done
- A written **go/no-go** with numbers: quack-DuckDB vs Postgres on the serving query mix at target
  concurrency, plus the soak result and an HA/ops assessment.
- A recommendation split by role: **(1) adopt quack for cold-lake concurrency now** if Role 1 passes,
  **(2) adopt/defer the serving store** on the Role 2 evidence, **(3) keep the serving backend swappable**
  either way.

## Notes
- The architecture (`scaling-architecture-lakehouse-and-serving.md`) is store-agnostic — this spike picks
  the engine, it doesn't change the design.
- Weight the **beta soak/stability** result heavily; a great benchmark that flakes under a 3-day soak is a
  no-go for a public explorer.
