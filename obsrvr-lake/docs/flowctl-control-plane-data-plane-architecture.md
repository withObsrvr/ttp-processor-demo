# Flowctl Control Plane / Data Plane Architecture for OBSRVR Lake

Date: 2026-06-12

## Purpose

The June 2026 mainnet Bronze gap repair exposed that too much pipeline orchestration lives in shell scripts. The scripts worked as emergency tooling, but they also became an ad-hoc control plane: chunk planning, Nomad dispatch, retry decisions, resume points, health checks, verification, and operator state were inferred from logs and PID files.

Future Stellar historical and live pipelines should be directed through `flowctl` / managed service flow. Pipeline components should implement clear data-plane responsibilities and report structured state to the control plane.

## Incident lessons

During Bronze repair we repeatedly saw failures that were not actual data-plane failures:

- `stellar-history-loader` chunks completed, but verification failed due to transient Postgres/catalog connectivity.
- Catalog file-stat gates falsely reported gaps after DuckLake `DELETE`/range replacement because stale data files/deletion vectors remain in metadata.
- Nomad reschedule initially retried failed allocations against stale `/data/output` state.
- Operators had to manually infer resume points from logs.
- Shell scripts encoded important policy: chunk size, retry behavior, Silver stop/rebuild sequencing, and Bronze readiness semantics.

The fixes added during the incident should remain in the data-plane components, but orchestration should move to `flowctl`.

## Desired split of responsibility

### Data plane: component-owned

Each component should own the correctness of its local work unit.

For `stellar-history-loader` / Bronze historical loader:

- Extract a requested ledger range.
- Write Bronze DuckLake rows idempotently.
- Delete existing rows for the full requested chunk range before insert.
- Fail hard on partial table write errors.
- Sanitize data that cannot be represented safely in Parquet/DuckDB, e.g. non-UTF8 text memos.
- Verify its own output using active DuckLake rows, not catalog file stats.
- Emit structured status for each phase.
- Exit with a typed result:
  - success
  - retryable infrastructure failure
  - non-retryable data/schema failure
  - verification failure

For `silver-history-loader`:

- Refuse to run over Bronze ranges that fail the Bronze direct readiness gate.
- Write Silver idempotently by exact requested range.
- Mark per-table manifests only after successful delete+insert+verify.
- Report rows written and source Bronze coverage used.

### Control plane: flowctl-owned

`flowctl` should own orchestration state and operator intent.

- Plan chunk ranges.
- Dispatch components to Nomad/process/docker.
- Track per-chunk lifecycle.
- Persist checkpoints and resume state.
- Retry only retryable failures with bounded backoff.
- Avoid Nomad-managed reschedule when local output/checkpoints can be stale.
- Decide next resume point after verification.
- Provide pause/resume/retry/abort commands.
- Aggregate component health and progress.
- Gate downstream Silver until Bronze is verified.
- Record run metadata for auditability.

## Component contract

Every OBSRVR Lake pipeline component should support the following control-plane interface.

### Required environment/config

```text
ENABLE_FLOWCTL=true
FLOWCTL_ENDPOINT=<host:port>
FLOWCTL_COMPONENT_ID=<stable component id>
FLOWCTL_RUN_ID=<pipeline run id>
FLOWCTL_ATTEMPT=<attempt number>
FLOWCTL_HEARTBEAT_INTERVAL_MS=10000
```

Historical range components should also accept:

```text
START_LEDGER=<inclusive start>
END_LEDGER=<inclusive end>
CHUNK_START=<inclusive chunk start>
CHUNK_END=<inclusive chunk end>
NETWORK_PASSPHRASE=<stellar network passphrase>
```

### Registration metadata

On startup, components register:

```json
{
  "component_id": "bronze-history-loader",
  "component_type": "source_or_processor",
  "pipeline": "obsrvr-mainnet-bronze-repair",
  "version": "chunk-delete-worker-memo-20260610",
  "network": "pubnet",
  "range_start": 30750003,
  "range_end": 31000002,
  "capabilities": [
    "range-extract",
    "ducklake-range-replace",
    "direct-row-verify",
    "idempotent-retry"
  ]
}
```

### Heartbeat/progress events

Components should emit structured progress events, not only logs:

```json
{
  "event_type": "component.progress",
  "component_id": "bronze-history-loader",
  "run_id": "mainnet-bronze-repair-20260612",
  "chunk_start": 30750003,
  "chunk_end": 31000002,
  "phase": "ducklake_push",
  "status": "running",
  "ledgers_processed": 250000,
  "tables_completed": ["ledgers_row_v2", "transactions_row_v2"]
}
```

### Completion event

```json
{
  "event_type": "component.completed",
  "component_id": "bronze-history-loader",
  "chunk_start": 30750003,
  "chunk_end": 31000002,
  "status": "verified",
  "row_counts": {
    "ledgers_row_v2": 250000,
    "transactions_row_v2": 22685377,
    "operations_row_v2": 48477141,
    "effects_row_v1": 7132850
  },
  "verification": {
    "gate": "bronze-silver-readiness-direct",
    "passed": true,
    "blocking_gaps": 0
  }
}
```

### Failure event

```json
{
  "event_type": "component.failed",
  "component_id": "bronze-history-loader",
  "chunk_start": 30750003,
  "chunk_end": 31000002,
  "phase": "verification",
  "failure_class": "retryable_infrastructure",
  "error": "catalog postgres connection timed out",
  "recommended_action": "retry_verification"
}
```

## Bronze readiness semantics

The operational Bronze gate for Silver should be row-level and DuckLake-aware.

Rules:

1. `ledgers_row_v2` must contain exactly one row per requested ledger.
2. A transaction ledger gap is blocking only if `operations_row_v2` or `effects_row_v1` contains rows for that ledger.
3. Transaction/operation/effect co-missing spans can be legitimate sparse/no-activity spans.
4. Catalog file-stat checks are diagnostic, not final, after DuckLake deletes/range replacement.

This logic should be inside a reusable library used by:

- `stellar-history-loader` self-verification
- `flowctl` Bronze gate command
- operator diagnostics
- pre-Silver gating

Current script implementation to preserve as reference:

```text
infra/.../scripts/verify-bronze-silver-readiness-direct-go.sh
```

## Proposed flowctl pipeline shape

### Bronze historical repair pipeline

```yaml
apiVersion: flowctl/v1
kind: Pipeline
metadata:
  name: obsrvr-mainnet-bronze-repair
  namespace: prod
  annotations:
    network: pubnet
    incident: bronze-gap-202606
spec:
  driver: nomad

  sources:
    - id: bronze-history-loader
      type: withobsrvr/stellar-history-loader@chunk-delete-worker-memo-20260610
      env:
        ENABLE_FLOWCTL: "true"
        FLOWCTL_ENDPOINT: "flowctl.service.consul:8080"
        NETWORK_PASSPHRASE: "Public Global Stellar Network ; September 2015"
        STORAGE_TYPE: "GCS"
        BUCKET: "obsrvr-stellar-ledger-data-pubnet-data/landing/ledgers/pubnet"
        DUCKLAKE_DATA_PATH: "s3://obsrvr-lake-mainnet/"
        DUCKLAKE_METADATA_SCHEMA: "bronze_meta"
      range:
        start: 28000003
        end: 35000002
        chunkSize: 250000
        maxWorkers: 8
      retryPolicy:
        retryableFailures:
          - catalog_timeout
          - b2_503
          - nomad_alloc_lost
        maxAttempts: 3
        backoff: exponential
      verification:
        mode: bronze-silver-readiness-direct
```

### Bronze to Silver dependency

Silver should declare an explicit dependency on a Bronze verified range:

```yaml
apiVersion: flowctl/v1
kind: Pipeline
metadata:
  name: obsrvr-mainnet-silver-rebuild
spec:
  driver: nomad

  preconditions:
    - type: bronze-readiness
      gate: bronze-silver-readiness-direct
      startLedger: 3
      endLedger: 62799999
      required: true

  processors:
    - id: silver-history-loader
      type: withobsrvr/silver-history-loader@leftjoinfix-20260609
      inputs: [bronze-ducklake]
      range:
        start: 3
        end: 62799999
        chunkSize: 100000
```

## Required flowctl capabilities

To support this well, `flowctl` needs first-class historical range orchestration.

### Range-run model

Add a persistent model similar to:

```text
PipelineRun
  id
  pipeline_name
  target_range_start
  target_range_end
  status

ChunkRun
  id
  pipeline_run_id
  component_id
  chunk_start
  chunk_end
  attempt
  status
  failure_class
  started_at
  completed_at
  verified_at
  row_counts_json
```

### Commands

Suggested operator commands:

```bash
flowctl runs list
flowctl runs show obsrvr-mainnet-bronze-repair
flowctl chunks list --run <run-id>
flowctl chunks retry --run <run-id> --from-ledger 30750003
flowctl chunks verify --run <run-id> --start 30750003 --end 31000002
flowctl runs resume --run <run-id>
flowctl runs pause --run <run-id>
flowctl gates bronze-readiness --start 3 --end 62799999
```

### Failure classification

The control plane should distinguish:

| Failure | Retry? | Next action |
|---|---:|---|
| B2 503 | yes | retry same chunk |
| catalog/Postgres timeout during verification | yes | retry verification first; if pass, continue |
| invalid data encoding | no | fix component; rerun same chunk |
| DuckLake insert failure | no/conditional | inspect; rerun same chunk after fix |
| direct gate blocking gap | no | rerun same chunk/range |
| allocation lost/OOM | yes with smaller chunk | reduce chunk size or workers |

## Implementation phases

### Phase 1 — Component hardening

- Keep current `stellar-history-loader` fixes.
- Add structured JSON event output alongside logs.
- Add a built-in `--verify-ducklake-direct` mode.
- Add explicit failure classes.
- Ensure `--start/--end` are passed into DuckLake pusher and used for range deletes.

### Phase 2 — flowctl observation

- Components register with flowctl.
- Heartbeats include ledger range, phase, and row counts.
- `flowctl status` shows active chunk and last verified chunk.
- Shell wrappers may still dispatch, but flowctl becomes the source of operator status.

### Phase 3 — flowctl orchestration

- Move chunk planning and dispatch into flowctl.
- Persist chunk attempts and verification outcomes.
- Implement retry policies and pause/resume.
- Remove PID/log-file resume as operational source of truth.

### Phase 4 — managed Bronze/Silver workflows

- Bronze repair/backfill is a flowctl pipeline.
- Silver rebuild is blocked by a declared Bronze readiness precondition.
- Production runbooks refer to flowctl commands, not shell-script internals.

## Non-goals

- Do not make flowctl responsible for transforming Stellar data.
- Do not make flowctl inspect raw XDR or Parquet row content directly unless running a verifier component.
- Do not use catalog file stats as final proof of active DuckLake row state after deletes.
- Do not rely on Nomad reschedule for historical chunks that use local output/checkpoints.

## Current immediate recommendation

Finish the current Bronze repair with the repaired scripts and direct gate. Then, before future large historical work, implement Phase 1 and Phase 2 so `stellar-history-loader` and `silver-history-loader` are flowctl-compatible and report durable chunk state.
