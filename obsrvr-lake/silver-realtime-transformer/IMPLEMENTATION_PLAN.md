# Silver Realtime Transformer Performance Improvement Plan

## Goal

Bring `silver-realtime-transformer` closer to the performance profile of the optimized `stellar-postgres-ingester` by reducing:

- PostgreSQL write overhead
- repeated bronze-table scans per ledger window
- row-by-row upsert work in hot paths
- uncontrolled connection / transaction concurrency

This plan focuses on concrete, incremental changes that can be implemented and measured safely.

---

## Current Bottlenecks Observed

### 1. Silver writes still use `database/sql` + `lib/pq` + dynamic multi-row `INSERT`

Current files:

- `go/main.go`
- `go/silver_writer.go`
- `go/silver_writer_batch.go`
- `go/go.mod`

Current behavior:

- Builds SQL strings dynamically for each flush
- Uses large `INSERT ... VALUES (...), (...), ... ON CONFLICT ...`
- Repeats SQL parsing/planning overhead on the PostgreSQL side
- Spends CPU on placeholder generation and string assembly in Go

### 2. Phase A repeatedly scans the same bronze tables

Current files:

- `go/bronze_reader.go`
- `go/source_manager.go`
- `go/transformer.go`

Examples:

- `operations_row_v2` is scanned separately for:
  - enriched operations
  - token transfers
  - contract invocations
  - contract calls
- `accounts_snapshot_v1` is scanned twice:
  - current-state transform
  - snapshot append transform
- `trustlines_snapshot_v1` is scanned twice
- `offers_snapshot_v1` is scanned twice
- `contract_data_snapshot_v1` is scanned multiple times for:
  - contract data current
  - balance holder snapshots
  - token registry metadata

This improves modularity, but it multiplies IO and query work.

### 3. `transformAddressBalancesFromContractState` does per-row UPSERTs

Current file:

- `go/transformer.go`

Current behavior:

- scans balance-holder rows
- executes one `INSERT ... ON CONFLICT DO UPDATE` per row

This is a direct throughput bottleneck candidate.

### 4. DB pool sizing is not explicitly tuned

Current file:

- `go/main.go`

Current behavior:

- opens `bronzeDB` and `silverDB`
- does not set:
  - `SetMaxOpenConns`
  - `SetMaxIdleConns`
  - `SetConnMaxLifetime`
  - `SetConnMaxIdleTime`

With Phase A parallelism, this can create avoidable contention or poor pool behavior.

---

## Implementation Strategy

Implement in four phases, highest ROI first.

---

# Phase 1: Writer modernization with `pgx` and bulk load paths

## Objective

Replace the current dynamic `INSERT` hot path with faster PostgreSQL-native bulk write paths.

## Scope

### 1. Replace `lib/pq` / `database/sql` for silver writes with `pgx`

#### Files to change

- `go/go.mod`
- `go/main.go`
- `go/silver_writer.go`
- `go/silver_writer_batch.go`
- `go/transformer.go`

#### Tasks

1. Add:
   - `github.com/jackc/pgx/v5`
   - `github.com/jackc/pgx/v5/pgxpool`
2. Create a `pgxpool.Pool` for `silver_hot`
3. Migrate transaction usage from `*sql.Tx` to `pgx.Tx` for silver write paths
4. Keep bronze reads on `database/sql` initially to limit blast radius

#### Success criteria

- Service builds and runs with silver writes using `pgx`
- Existing transforms still produce identical rows

---

### 2. Add a bulk loader abstraction for append-only tables

#### New file

- `go/silver_writer_copy.go`

#### Tasks

Implement a bulk write abstraction with two modes:

1. **Direct `COPY FROM`** for append-only / `DO NOTHING` tables
2. **Stage + merge** for upsert tables

#### Direct `COPY FROM` target tables

Use first on these tables:

- `enriched_history_operations`
- `enriched_history_operations_soroban`
- `token_transfers_raw`
- `trades`
- `effects`
- `evicted_keys`
- `restored_keys`
- `contract_invocation_calls`
- `contract_invocation_hierarchy`
- snapshot append tables:
  - `accounts_snapshot`
  - `trustlines_snapshot`
  - `offers_snapshot`
  - `account_signers_snapshot`

#### Implementation notes

- Use `pgx.CopyFrom`
- Represent row batches as `[][]any` or typed `CopyFromSource`
- Preserve idempotency by copying into a temp staging table first when needed
- For append-only tables with unique constraints, use:
  - temp/unlogged staging table
  - `INSERT INTO target SELECT ... ON CONFLICT DO NOTHING`

#### Success criteria

- Bulk paths replace `BatchInserter` for at least the append-only tables
- Flush latency drops for large batches

---

### 3. Add stage-and-merge path for upsert tables

#### Target tables

- `accounts_current`
- `trustlines_current`
- `offers_current`
- `contract_invocations_raw`
- `contract_metadata`
- `liquidity_pools_current`
- `claimable_balances_current`
- `native_balances_current`
- `contract_data_current`
- `contract_code_current`
- `ttl_current`
- `token_registry`
- `config_settings_current`
- `address_balances_current` state-based rebuild path

#### Tasks

For each table family:

1. Create temp staging table per transaction
2. Bulk `COPY` rows into staging table
3. Merge with:
   - `INSERT INTO target (...) SELECT ... FROM staging`
   - `ON CONFLICT DO UPDATE`
4. Drop temp table automatically on transaction end

#### Success criteria

- Upsert-heavy tables no longer rely on huge dynamically generated `VALUES` lists
- CPU overhead in the transformer process is reduced

---

# Phase 2: Eliminate repeated bronze scans

## Objective

Reduce IO and query cost by consolidating repeated bronze reads.

## Scope

### 1. Introduce source-family transform groups

#### New concept

Instead of “one transform = one bronze query”, group transforms by bronze source family.

#### Proposed groups

##### Group A: `operations_row_v2`
Derive in one pass:

- enriched operations
- classic token transfers
- contract invocations
- contract calls

##### Group B: `contract_events_stream_v1`
Derive in one pass:

- soroban token transfers
- future event-derived transforms if added

##### Group C: `accounts_snapshot_v1`
Derive in one pass:

- `accounts_current`
- `accounts_snapshot`

##### Group D: `trustlines_snapshot_v1`
Derive in one pass:

- `trustlines_current`
- `trustlines_snapshot`

##### Group E: `offers_snapshot_v1`
Derive in one pass:

- `offers_current`
- `offers_snapshot`

##### Group F: `contract_data_snapshot_v1`
Derive in one pass:

- `contract_data_current`
- `token_registry`
- balance-holder state rows for address balances

##### Group G: one-table/one-transform tables
Keep as-is unless profiling shows need:

- trades
- effects
- native balances
- liquidity pools
- claimable balances
- contract code
- TTL
- restored keys
- evicted keys
- config settings

---

### 2. Add grouped reader methods

#### Files to change

- `go/bronze_reader.go`
- `go/source_manager.go`

#### Tasks

Add grouped query APIs such as:

- `QueryOperationsTransformBundle(...)`
- `QueryAccountSnapshotBundle(...)`
- `QueryTrustlineSnapshotBundle(...)`
- `QueryOfferSnapshotBundle(...)`
- `QueryContractDataBundle(...)`

These should project all columns needed by all dependent transforms.

#### Success criteria

- repeated scans of `operations_row_v2`, `accounts_snapshot_v1`, `trustlines_snapshot_v1`, `offers_snapshot_v1`, and `contract_data_snapshot_v1` are removed or significantly reduced

---

### 3. Refactor Phase A execution model

#### Files to change

- `go/transformer.go`

#### Tasks

Replace current `bronzeTransforms := []transformJob{...}` fan-out model with grouped transform workers.

Proposed new structure:

1. Build grouped jobs
2. Each grouped job:
   - performs one bronze read
   - derives rows for multiple silver tables
   - bulk-loads those rows into silver in one transaction
3. Keep bounded concurrency across groups

#### Success criteria

- fewer bronze queries per cycle
- lower DB read pressure on bronze hot
- lower total cycle duration for same ledger window

---

# Phase 3: Fix the known row-by-row bottleneck

## Objective

Batch `transformAddressBalancesFromContractState` and make it compatible with the new bulk loader.

## Scope

### 1. Replace per-row UPSERT with bulk staging

#### Files to change

- `go/transformer.go`
- `go/silver_writer_copy.go`

#### Current behavior

- one `ExecContext` per row in `transformAddressBalancesFromContractState`

#### New behavior

1. Read all balance-holder rows for the ledger window
2. Accumulate rows in memory up to configured batch size
3. Bulk load to temp staging table
4. Merge into `address_balances_current` with one SQL statement per batch

#### Suggested staging schema

Columns:

- `owner_address`
- `asset_key`
- `asset_type_hint`
- `asset_code`
- `asset_issuer`
- `balance_raw`
- `last_updated_ledger`
- `last_updated_at`

#### Success criteria

- no row-by-row UPSERT loop remains in this transform
- transform latency becomes proportional to batch count, not row count

---

### 2. Reuse contract-data grouped read from Phase 2

If Phase 2 is complete, this transform should consume rows already read as part of the `contract_data_snapshot_v1` family rather than executing its own query.

#### Success criteria

- no standalone `QueryBalanceHolderSnapshots(...)` call remains in the hot path

---

# Phase 4: Connection pool and concurrency tuning

## Objective

Make concurrency explicit and safe for both bronze and silver databases.

## Scope

### 1. Extend config with DB pool settings

#### File to change

- `go/config.go`

#### Add fields

For both `bronze_hot` and `silver_hot`:

- `max_open_conns`
- `max_idle_conns`
- `conn_max_lifetime_seconds`
- `conn_max_idle_time_seconds`

For runtime tuning:

- `performance.max_bronze_readers`
- `performance.max_silver_writers`
- keep `performance.batch_size`
- keep or rename `performance.insert_batch_size`

#### Success criteria

- pool sizing is configurable without code changes

---

### 2. Apply pool tuning in `main.go`

#### Files to change

- `go/main.go`

#### Tasks

For bronze DB:

- call `SetMaxOpenConns`
- call `SetMaxIdleConns`
- call `SetConnMaxLifetime`
- call `SetConnMaxIdleTime`

For silver `pgxpool`:

- configure pool size directly via pool config
- set health-check / lifetime options as needed

#### Success criteria

- connection usage remains stable under load
- no accidental overcommit from worker fan-out

---

### 3. Split read and write concurrency limits

#### Files to change

- `go/config.go`
- `go/transformer.go`

#### Tasks

Instead of one `MaxWorkers`, introduce:

- `MaxBronzeReaders`
- `MaxSilverWriters`

Use these to bound:

- grouped bronze scan concurrency
- concurrent silver write transactions

#### Success criteria

- read pressure and write pressure can be tuned independently

---

## Optional Follow-up Phase

# Phase 5: Incremental balance maintenance

## Objective

Reduce cost of `transformAddressBalancesCurrent`, which currently deletes and rebuilds balances for impacted owners.

## Current file

- `go/transformer.go`

## Current behavior

- delete impacted owners
- rebuild native balances
- rebuild token balances from transfer history

## Follow-up design

Move toward a delta-based approach:

1. compute per-batch transfer deltas
2. apply signed deltas to existing balances
3. preserve state-based overwrites from contract storage snapshots
4. keep full rebuild only for repair / replay / migration modes

## Notes

This phase should happen after Phases 1–4. It is more invasive and correctness-sensitive.

---

## Rollout Order

Implement in this order:

1. **Phase 1.1**: silver `pgx` migration
2. **Phase 1.2**: `COPY` for append-only tables
3. **Phase 3.1**: batch `transformAddressBalancesFromContractState`
4. **Phase 1.3**: stage-and-merge for upsert tables
5. **Phase 4**: connection pool and concurrency controls
6. **Phase 2**: grouped bronze reads / grouped transforms
7. **Phase 5**: incremental address balance maintenance

This order gets early wins without requiring a giant refactor up front.

---

## Measurement Plan

For each phase, benchmark with real ledger ranges and compare before/after.

## Metrics to capture

### End-to-end

- ledgers processed per second
- rows written per second
- transformation cycle duration
- p50 / p95 / p99 cycle duration

### Bronze read side

- queries per cycle
- total rows scanned per cycle
- time spent in bronze queries

### Silver write side

- time spent writing each table family
- rows flushed per batch
- transaction duration
- commit duration

### Process metrics

- CPU usage
- memory usage
- GC pressure
- open connections per DB

---

## Recommended Instrumentation Additions

Add per-transform timing and row counters in `go/transformer.go`:

- bronze read duration
- transform/derivation duration
- silver write duration
- commit duration
- rows produced per target table

Add structured logging for:

- grouped job start/end
- batch flush sizes
- pool saturation / wait indicators if available

---

## Risks and Mitigations

### Risk 1: `pgx` migration touches many transaction signatures

**Mitigation**
- migrate silver write path first
- keep bronze reads unchanged initially
- add adapter helpers where necessary

### Risk 2: grouped reads increase in-memory working set

**Mitigation**
- process grouped reads in streaming batches
- bound batch size in config
- avoid loading entire ledger windows into large in-memory maps unless necessary

### Risk 3: stage-and-merge logic may change conflict semantics

**Mitigation**
- reuse the exact existing `ON CONFLICT` clauses
- validate row counts and checksums on a real replay window

### Risk 4: more concurrency can hurt DB performance

**Mitigation**
- make concurrency tunable
- start conservatively
- benchmark on realistic ledger windows

---

## Concrete Deliverables

### Deliverable A: bulk write foundation

- `pgxpool` integrated for silver writes
- `silver_writer_copy.go` added
- append-only tables migrated to COPY/staging

### Deliverable B: address balance state batching

- `transformAddressBalancesFromContractState` no longer performs row-by-row upserts

### Deliverable C: pool tuning

- explicit bronze/silver pool config
- separate read/write concurrency limits

### Deliverable D: grouped transform execution

- duplicate bronze scans removed for major source families
- grouped workers implemented in `transformer.go`

---

## Suggested Initial Work Breakdown

### PR 1

- add config for pool sizing and separate concurrency
- tune bronze pool in `main.go`
- migrate silver connection to `pgxpool`

### PR 2

- add generic COPY/staging helper
- migrate append-only tables first

### PR 3

- batch `transformAddressBalancesFromContractState`
- move it onto staging merge path

### PR 4

- migrate upsert tables to stage-and-merge

### PR 5

- refactor Phase A into grouped bronze-source workers

### PR 6

- optional incremental address balance maintenance

---

## File-Level Change Summary

### Likely modified

- `obsrvr-lake/silver-realtime-transformer/go/go.mod`
- `obsrvr-lake/silver-realtime-transformer/go/main.go`
- `obsrvr-lake/silver-realtime-transformer/go/config.go`
- `obsrvr-lake/silver-realtime-transformer/go/transformer.go`
- `obsrvr-lake/silver-realtime-transformer/go/silver_writer.go`
- `obsrvr-lake/silver-realtime-transformer/go/silver_writer_batch.go`
- `obsrvr-lake/silver-realtime-transformer/go/bronze_reader.go`
- `obsrvr-lake/silver-realtime-transformer/go/source_manager.go`

### Likely added

- `obsrvr-lake/silver-realtime-transformer/go/silver_writer_copy.go`
- optionally `obsrvr-lake/silver-realtime-transformer/go/transform_groups.go`
- optionally `obsrvr-lake/silver-realtime-transformer/go/perf_metrics.go`

---

## Final Recommendation

If only the first three changes are implemented, the best ROI path is:

1. move silver writes to `pgx` and `COPY`
2. batch `transformAddressBalancesFromContractState`
3. explicitly tune pools and concurrency

If the team wants the largest structural gain after that, the next step is consolidating repeated bronze scans into grouped source-family transforms.
