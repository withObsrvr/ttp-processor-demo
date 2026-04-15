# Event-Driven Serving Trigger Design

This document describes the recommended design for moving the serving layer from coarse polling to notification-driven micro-batching, following the same architectural direction already used by `silver-realtime-transformer`.

## Why this exists

Today, `serving-projection-processor` runs on a fixed ticker.

That is acceptable for slow-changing aggregates, but it is not a good fit for near-real-time product surfaces such as:
- latest ledgers
- latest transactions
- explorer events
- recent contract calls
- recent operations

Stellar ledgers currently close roughly every 5 seconds, and that interval may decrease in the future. A serving layer that wakes every 300 seconds will always feel stale for those endpoints.

The goal of this design is to make the **freshness-sensitive** serving projectors react to new data as soon as the Silver layer has safely committed it.

---

## Design summary

### Core idea

Instead of:
- wait for a timer
- scan source databases for work
- maybe find new rows

we want:
- `silver-realtime-transformer` successfully commits a batch
- it emits a **silver checkpoint advanced** event
- a fast serving processor receives the event
- it runs only the recent-feed serving projectors
- it coalesces bursts into one follow-up run if a run is already in progress

### Key principle

> Trigger serving from **Silver checkpoint advancement**, not from Bronze ingest.

This matters because many serving projections depend on transformed `silver_hot` tables and enrichment tables such as:
- `enriched_history_operations`
- `contract_invocations_raw`
- `contract_registry`
- `token_registry`
- event classification rules

If serving is triggered too early from Bronze, it can wake before its source data is ready.

---

## Recommended runtime architecture

```text
stellar-postgres-ingester
  └── publishes bronze-committed events

silver-realtime-transformer
  └── subscribes to bronze-committed events
  └── transforms bronze → silver
  └── commits silver checkpoint
  └── publishes silver-checkpoint-advanced events

serving-projection-processor-fast
  └── subscribes to silver-checkpoint-advanced events
  └── runs only freshness-sensitive serving projectors
  └── optional slow fallback ticker for safety

serving-projection-processor-slow
  └── remains polling-based
  └── runs heavier current-state / aggregate projectors
```

---

## Processor split: fast vs slow

The serving layer should be split into two operational groups.

## Fast serving processor

Purpose:
- near-real-time recent feeds
- user-facing explorer/home surfaces

Recommended projectors:
- `ledgers_recent`
- `transactions_recent`
- `events_recent`
- `explorer_events_recent`
- `contract_calls_recent`
- optionally `operations_recent`

Trigger mode:
- gRPC event-driven
- fallback poll every 30-60s for safety

Freshness target:
- usually <= 1 ledger behind
- ideally 5-10s lag

## Slow serving processor

Purpose:
- heavier current-state projections
- aggregates and ranking surfaces

Recommended projectors:
- `accounts_current`
- `account_balances`
- `network_stats`
- `asset_stats`
- `contracts_current`
- `contract_stats`

Trigger mode:
- polling
- 60-300s depending on cost

---

## Event contract

The Silver transformer should publish an event after a successful batch commit.

Suggested event type:
- `stellar.silver.checkpoint_advanced`

Suggested payload:

```json
{
  "event_type": "stellar.silver.checkpoint_advanced",
  "network": "testnet",
  "from_ledger": 2034450,
  "to_ledger": 2034458,
  "closed_at": "2026-04-14T14:32:01Z",
  "source": "silver-realtime-transformer"
}
```

Minimum required fields:
- `network`
- `to_ledger`

Helpful fields:
- `from_ledger`
- `closed_at`
- `source`
- maybe `transform_batch_id`

---

## Trigger transport recommendation

### Preferred option: gRPC SourceService publisher from Silver transformer

This is the closest match to the existing Bronze → Silver design.

Silver transformer should expose a gRPC `SourceService` and stream `stellar.silver.checkpoint_advanced` events.

Why this is preferred:
- consistent with current event-driven architecture
- avoids inventing a second trigger mechanism
- clear commit boundary
- easy to reason about operationally

### Alternatives considered

#### PostgreSQL LISTEN/NOTIFY
Pros:
- low latency
- simple database signaling

Cons:
- tighter DB coupling
- less aligned with current gRPC architecture
- harder to evolve into richer event contracts

#### Separate notifier sidecar
Pros:
- avoids transformer code changes

Cons:
- still a polling design in disguise
- extra moving part
- less precise than publishing from the actual commit point

Recommendation:
- use the Silver transformer as the source of truth and publisher

---

## Serving processor behavior

## Coalescing runner model

The serving processor must not start overlapping projection runs on every notification.

Instead it should:
- remember the highest seen target ledger
- if a run is already in progress, mark `pending = true`
- once the current run completes, immediately run one more cycle if anything arrived during the run

Pseudo-flow:

```go
on SilverCheckpointAdvanced(toLedger):
    latestSeen = max(latestSeen, toLedger)

    if runInProgress:
        pending = true
        return

    runInProgress = true
    defer runInProgress = false

    for {
        pending = false
        runFastProjectorsUpTo(latestSeen)

        if !pending {
            break
        }
    }
```

This gives:
- no concurrent projector writes
- no thundering herd behavior
- good latency under bursty ledger arrival

## Fallback safety poll

Even in event-driven mode, keep a low-frequency fallback poll, for example:
- every 30s or 60s

This protects against:
- missed notifications during reconnects
- transient subscriber failures
- startup race windows

---

## Watermark semantics

The event should carry a target watermark (`to_ledger`).

The serving processor should attempt to catch up each fast projector until:
- projector checkpoint >= target watermark, or
- no more rows are available

This is better than a single blind pass because it makes the event boundary explicit.

---

## File-level implementation plan

## 1. `silver-realtime-transformer`

### New responsibilities
- host a gRPC event source for downstream subscribers
- emit `stellar.silver.checkpoint_advanced` after successful Silver checkpoint commit

### Expected file changes

#### `obsrvr-lake/silver-realtime-transformer/go/config.go`
Add config for the Silver event source server, for example:
- `service.grpc_port` or a dedicated downstream event port
- optional enable flag
- network / component metadata if needed

#### `obsrvr-lake/silver-realtime-transformer/go/main.go`
- initialize the Silver event source server
- start the gRPC server if enabled
- register shutdown handling

#### New file: `obsrvr-lake/silver-realtime-transformer/go/grpc_source.go`
Modeled after:
- `obsrvr-lake/stellar-postgres-ingester/go/grpc_source.go`

Responsibilities:
- implement flowctl `SourceService`
- maintain subscriber channels
- stream events
- expose current Silver checkpoint state
- publish `stellar.silver.checkpoint_advanced`

#### `obsrvr-lake/silver-realtime-transformer/go/transformer.go`
After the semantic transaction and Silver checkpoint commit succeed:
- publish a `silver checkpoint advanced` event
- only publish after commit succeeds
- include `to_ledger` from the committed checkpoint

Do **not** publish:
- before commit
- on partial success
- on failed batches

## 2. `serving-projection-processor`

### New responsibilities
- subscribe to Silver checkpoint events
- run fast projectors on notification
- keep slow projectors polling-based

### Expected file changes

#### `obsrvr-lake/serving-projection-processor/go/config.go`
Add trigger config, e.g.:

```yaml
trigger:
  mode: grpc
  endpoint: localhost:50055
  fallback_poll_seconds: 60
  event_type: stellar.silver.checkpoint_advanced
```

Also add a way to designate the processor role:
- `mode: fast|slow`
or simply configure enabled projectors per job, as already done today.

#### `obsrvr-lake/serving-projection-processor/go/main.go`
Refactor main loop so it can run in two modes:
- polling loop
- event-driven loop with fallback ticker

It should:
- connect to Silver event source if configured
- subscribe and receive checkpoint events
- coalesce triggers
- invoke projector runner

#### New file: `obsrvr-lake/serving-projection-processor/go/grpc_client.go`
Modeled after:
- `obsrvr-lake/silver-realtime-transformer/go/grpc_client.go`

Responsibilities:
- connect to downstream Silver source service
- auto-reconnect with backoff
- parse checkpoint-advanced events
- feed serving run loop

#### New file: `obsrvr-lake/serving-projection-processor/go/trigger_runner.go`
Recommended helper for:
- run-in-progress guard
- pending flag
- latest target ledger tracking
- invoking fast projectors up to watermark

#### Projector runner integration
Current projector interface is checkpoint-based and already suitable.

What should be added:
- optional ability to run until target watermark is reached
- or simply loop projector passes while checkpoint < target watermark and work is still available

This can be implemented in the central runner without changing every projector interface immediately.

## 3. Nomad jobs / deployment

### New fast job
Create:
- `serving-projection-processor-fast.nomad`

Config:
- short fallback poll, e.g. 30s or 60s
- gRPC trigger enabled
- only fast projectors enabled:
  - `ledgers_recent`
  - `transactions_recent`
  - `events_recent`
  - `explorer_events_recent`
  - `contract_calls_recent`
  - optionally `operations_recent`

### Keep or rename slow job
Either keep the current job as slow, or introduce:
- `serving-projection-processor-slow.nomad`

Config:
- polling only
- slower interval, e.g. 300s
- only heavy projectors enabled

### Silver transformer job
Update Nomad job to expose the Silver event source port.

Likely file:
- `infra/environments/prod/do-obsrvr-lake/.nomad/silver-realtime-transformer.nomad`

Likely additions:
- event source port env var
- service metadata / exposed port if needed

---

## Event ordering and correctness rules

1. Publish only after successful checkpoint commit.
2. Serving may lag but must never read ahead of committed Silver data.
3. Missed notifications must be recoverable via fallback polling.
4. Projectors remain checkpointed and idempotent.
5. No concurrent run loops should write the same serving tables from the same processor instance.

---

## Rollout plan

## Phase 1: safe infrastructure changes
- add Silver source-service publisher
- add serving gRPC subscriber client
- keep current polling behavior intact behind config flags

## Phase 2: dual-mode deployment
- deploy Silver publisher
- deploy `serving-projection-processor-fast`
- keep current slow polling job unchanged
- verify health/status/checkpoint movement

## Phase 3: switch freshness-sensitive endpoints to depend on fast processor
- latest transactions
- latest ledgers
- explorer events
- recent contract calls

## Phase 4: tune fallback and intervals
- fast fallback poll: 30-60s
- slow poll: 60-300s
- monitor DB load and run durations

---

## Health / observability additions recommended

For the fast serving processor, add status fields such as:
- `trigger_mode`
- `last_trigger_received_at`
- `last_trigger_to_ledger`
- `run_in_progress`
- `pending_trigger`
- `subscriber_connected`
- reconnect/error counters

For the Silver publisher, add:
- subscriber count
- last published checkpoint event
- publish error count

---

## Final recommendation

The correct long-term design for near-real-time serving is:

> **Silver commits drive serving refreshes.**

More concretely:
- keep DuckLake for cold truth
- keep serving Postgres for product reads
- split serving into fast and slow projection groups
- make the fast group event-driven from `silver-realtime-transformer`
- retain a slow fallback poll for resilience

This gives a serving layer that can keep pace with 5-second ledgers today and remain viable if ledger cadence gets faster later.
