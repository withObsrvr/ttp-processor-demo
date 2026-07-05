# Obsrvr Platform Improvement Plan

**Scope:** cross-repo (obsrvr-lake, obsrvr-gateway, nebu, flowctl,
nebu-processor-registry, obsrvr-stellar-components)
**Date:** 2026-07-05
**Companion pitches:** `PITCH_BATCH_CONTRACT_DATA.md`, `PITCH_TYPESCRIPT_SDK.md`

## Diagnosis

The platform is supply-heavy and demand-light. Six repos of extraction,
orchestration, lakehouse, and gateway infrastructure — consumed through curl
examples on a testnet-only API with documented 30–45 s worst-case page loads.
A competitor (Creit Tech's hosted `api.stellarindexer.com`) is winning the
dApp-developer story with ~777 lines of TypeScript over a far shallower
backend. Second problem: solo-maintainer drift — at every seam between repos,
design docs and reality have diverged (details in the checklist below).

**Prescription:** stop widening the platform, finish the funnel
(mainnet → fast queries → batch endpoint → typed SDK), and put a CI floor
under everything so drift stops compounding.

## Bet sequence

| # | Bet | Appetite | Where |
|---|-----|----------|-------|
| 1 | Mainnet on the Lake path | 1–2 weeks | obsrvr-lake, gateway |
| 2 | Horizon 1 reliability (already shaped in roadmap) | 1 cycle (2 weeks) | obsrvr-lake |
| 3 | Batch contract-data endpoint | 3 days | see pitch |
| 4 | TypeScript SDK v0.1 | 1 week | see pitch |
| 5 | CI floor | ~1 afternoon per repo | all repos |
| 6 | Drift-cleanup pass | 2–3 days, timeboxed | all repos |
| 7 | flowctl: one production job or park | decide, then ≤2 weeks | flowctl, obsrvr-lake |

Bets 3–4 can start against testnet in parallel with 1–2; don't *market* the
SDK until 1–2 are done. Bets 5–6 are cool-down-sized work between cycles.

---

## 1. Mainnet on the Lake path — the blocker

**Why:** everything downstream is demoware until mainnet is live. The gateway
config already carries mainnet backend lists; the analyst guide says
"mainnet coming soon." This is the only hard blocker for every other bet.

**Done:** `GET /lake/v1/mainnet/api/v1/silver/ledgers/recent` returns current
mainnet data through the gateway with a paying-tier API key.

## 2. Horizon 1 reliability — execute the existing shape

**Why:** the roadmap (`docs/obsrvr-lake-roadmap-and-horizon-1-shaped-work.md`)
already shapes this; it just has to be the *next cycle*, ahead of new features:

- **Ingester checkpoint fix** — checkpoint currently advances on failure.
  Silent data gaps are fatal for an indexing product; worse than downtime.
- **Gap detection** — prove completeness, don't assume it.
- **Serving/batch contention** — the 30–45 s account pages and >50 s cold
  selective queries. A typed SDK over a slow API burns trust permanently.

**Done:** the roadmap's own Horizon 1 acceptance criteria, plus: p95 latency
targets set and measured for the endpoints the SDK will wrap.

## 3–4. Batch endpoint + TS SDK

Shaped separately — see `PITCH_BATCH_CONTRACT_DATA.md` and
`PITCH_TYPESCRIPT_SDK.md`. Sequence: endpoint first (3 days), SDK next
(1 week, with a defined fallback if the endpoint slips).

## 5. CI floor — one afternoon per repo

**Why:** nebu has a real API-stability gate; almost nothing else has any.
Current state:

| Repo | CI today | Minimum floor |
|------|----------|---------------|
| obsrvr-lake | none | build + vet + `go test ./...` on PR |
| obsrvr-stellar-components | none (9 test files never run in CI) | same, plus `make validate-pipelines` |
| obsrvr-gateway | deploy-only workflows | add test+vet gate before the deploy job |
| nebu-processor-registry | build/validate only | keep; add unit tests opportunistically |
| nebu | good (api-stability, release, sbom) | none needed |
| flowctl | good (pr-tests + smoke) | none needed |

One reusable workflow (setup-go → vet → build → test), copied per repo.
This also discharges the Culture Manifesto #9 gap the shapeup README parks
as "Cycle 7 (future)."

**Done:** a PR with a failing test cannot merge in any of the four repos above.

## 6. Drift-cleanup pass — timeboxed 2–3 days

**Why:** each item is ~an hour; collectively they're the difference between
"impressive solo velocity" and "which docs can I trust?" This is deciding,
not refactoring.

**obsrvr-gateway**
- [ ] `internal/resolver` (Cycle 5 Bronze Resolver) is fully written but
  imported nowhere — wire it to routes or delete it and keep the branch.
- [ ] README still says "Phase 0"; rewrite the feature list to match reality
  (auth, tiers, metering, Lake proxy, DuckDB query endpoints).
- [ ] `/debug/ratelimit` is flagged "remove in production" — gate it behind
  a config flag now.

**nebu-processor-registry**
- [ ] Decide: pointer registry or monorepo. Reality: all 32 processors set
  `repo.github: withObsrvr/nebu-processor-registry` @ `ref: main`. Either
  document it as a monorepo (and fix the trust-model docs that assume
  external repos), or move one processor out to prove the pointer model.
- [ ] `PROCESSORS.md` is stale (lists 29 of 32) despite being auto-generated —
  regenerate in CI on merge, stop committing it by hand.
- [ ] Pin `repo.ref` to tags, not `main`, once the model decision is made.

**nebu**
- [ ] Remove the committed stale `./nebu` binary (Apr 10 build; references
  the `run` command removed in PR #24) and the
  `.claude/settings.local.json` entry that references it.

**obsrvr-lake**
- [ ] flowctl integration is env vars + a design doc that flags itself
  "forward-looking, not as-built" — either resolve via item 7 below or mark
  the env plumbing experimental in the component READMEs.

**obsrvr-stellar-components**
- [ ] `metadata.json` + `Dockerfile.flowctl` exist for only 4 of 7 components;
  add them for quack-ducklake-server, index-materializer, ducklake-replica-sync
  (or document why infra/batch components are exempt from the manifest contract).

**flowctl**
- [ ] Finish the proto migration (already a shaped 2-week pitch; dual
  incompatible proto packages are live in the codebase today, and it blocks
  JS SDK components).

**Done:** every checkbox resolved as *done* or *explicitly declined in
writing* — no silent carryover.

## 7. flowctl — one production job or park it

**Why:** flowctl is the largest maintenance surface with no production
consumer; the Lake's real ingestion runs on its own components. flowctl's
recent chunk-run tracking (`UpsertChunkRun` etc.) points at the right answer:

**The one job:** control plane for Lake backfill/repair — chunk planning,
retries, resume, Bronze-readiness gates for `stellar-history-loader` /
`silver-history-loader` runs. This matches the chunk feature already built
and the lake's own control-plane design doc.

**The bet:** finish the proto migration, then wire chunk lifecycle into the
two history loaders. Everything else flowctl does (container drivers, K8s,
translation) stays demoted — the FLAGSHIP_SCOPE narrowing was correct.

**If the appetite doesn't fit:** park flowctl explicitly (README banner,
no half-maintained integration env vars) rather than carrying it.

**Done:** one real backfill of a ledger range runs through flowctl chunk
tracking end-to-end, with resume after a killed run — or flowctl is
documented as parked.

---

## Later bets (after the funnel is closed)

- **Schema-driven SDK codegen.** The registry's schema IDs
  (`nebu.token_transfer.v1`), `--describe-json`, and proto contracts form a
  machine-readable chain from processor to Lake table. Generate typed SDK
  modules from those contracts → every community processor becomes an SDK
  feature automatically. Do after the hand-written SDK proves the shape.
- **MCP over the Lake API.** The registry's nebu-mcp pattern (guardrails,
  compact output) pointed at gateway endpoints = the agent-native Stellar
  data platform. No incumbent in that market.
- **Serving-engine escalation (pg_duckdb / chDB).** Only if Horizon 2
  incremental projections + read replicas miss the latency targets set in
  item 2. Measure first — it's the roadmap's own escalation path, not the
  next step.
- **Second protocol SDK module (Soroswap).** The registry processors already
  hold the contract addresses; the SDK pattern will exist. Cheap follow-on.

## Standing no-gos

- No new ingestion components or registry processors until the SDK ships —
  supply already outruns demand.
- No GraphQL, no gRPC consumer API — REST + typed SDK is what dApp devs use.
- No ClickHouse migration ahead of the Horizon 2 measurement gate.
- No relitigating flowctl's K8s/container-driver demotion.
