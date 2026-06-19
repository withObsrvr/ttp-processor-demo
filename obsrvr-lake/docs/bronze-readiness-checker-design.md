# Bronze Readiness Checker Design

Date: 2026-06-15

## Purpose

Build a standalone Go CLI that verifies whether a Bronze DuckLake range is safe for Silver processing. This tool should replace the current incident-only shell wrapper around `verify-bronze-silver-readiness-direct-go` and become the reusable Bronze pre-Silver gate for operators, Nomad jobs, and eventually `flowctl`.

The checker must read **active DuckLake rows**, not only DuckLake catalog file stats. Catalog file stats are useful for inventory, but after DuckLake `DELETE` / range replacement they can show stale data files and false gaps.

## Background

During the June 2026 mainnet Bronze gap repair, we found that:

- `ledgers_row_v2` was complete, but `transactions_row_v2` had missing ranges.
- Some transaction gaps were real blockers because `operations_row_v2` and/or `effects_row_v1` had rows in the same ledger span.
- Some apparent gaps were legitimate sparse/no-activity spans where transactions, operations, and effects were all absent.
- Catalog-stat gates reported false failures after range replacement because deletes/deletion vectors are not represented as simple active min/max coverage.

A direct row-level Go verifier was prototyped under infra scripts:

```text
/home/tillman/Documents/infra/environments/prod/latitude/obsrvr-lake-mainnet/scripts/verify-bronze-silver-readiness-direct-go.sh
/home/tillman/Documents/infra/environments/prod/latitude/obsrvr-lake-mainnet/scripts/verify-bronze-silver-readiness-direct-go/main.go
```

This document describes the production version to build in `obsrvr-lake`.

## Proposed location

```text
obsrvr-lake/bronze-readiness-checker/
  Makefile
  Dockerfile
  README.md
  go/
    go.mod
    main.go
    config.go
    ducklake.go
    checker.go
    output.go
```

## CLI interface

Example:

```bash
bronze-readiness-checker \
  --catalog "postgresql://user:pass@host:5432/obsrvr_lake_catalog_prod?sslmode=disable" \
  --data-path "s3://obsrvr-lake-mainnet/" \
  --metadata-schema bronze_meta \
  --schema bronze \
  --start 45800003 \
  --end 45850002 \
  --s3-endpoint s3.us-west-004.backblazeb2.com \
  --s3-region us-west-004 \
  --s3-key-id "$B2_KEY_ID" \
  --s3-key-secret "$B2_KEY_SECRET" \
  --format text
```

Required flags:

```text
--catalog              PostgreSQL DuckLake catalog DSN
--data-path            DuckLake data path, e.g. s3://obsrvr-lake-mainnet/
--metadata-schema      DuckLake metadata schema, usually bronze_meta
--schema               Bronze schema, usually bronze
--start                inclusive start ledger
--end                  inclusive end ledger
```

S3/B2 flags:

```text
--s3-endpoint
--s3-region
--s3-key-id
--s3-key-secret
```

Output/control flags:

```text
--format text|json        default: text
--quiet                   only print final status
--timeout                 optional total check timeout
--fail-on-warning         optional future behavior
```

Environment variable fallback should be supported for secrets/config:

```text
CATALOG_DSN
B2_BUCKET / DATA_PATH
B2_KEY_ID
B2_KEY_SECRET
B2_ENDPOINT
B2_REGION
```

## Readiness semantics

The tool implements a `bronze-silver-readiness` gate.

### Rule 1: ledgers must be complete

For the requested range:

```sql
SELECT count(DISTINCT sequence)
FROM bronze.ledgers_row_v2
WHERE sequence BETWEEN start AND end;
```

Expected:

```text
distinct_ledgers == end - start + 1
```

A missing ledger row is always blocking.

### Rule 2: transactions must exist where activity exists

For the requested range, build:

```text
tx_ledgers       = distinct ledger_sequence from transactions_row_v2
activity_ledgers = distinct ledger_sequence from operations_row_v2 UNION effects_row_v1
```

Blocking transaction gaps are:

```text
activity_ledgers - tx_ledgers
```

Group missing ledgers into contiguous ranges and report them.

### Rule 3: sparse/no-activity spans are allowed

Do not fail simply because a ledger has no transaction row if operations/effects are also absent.

This is important because non-ledger Bronze tables can be sparse.

### Rule 4: catalog file stats are not final proof

The checker must query active DuckLake tables, not only `bronze_meta.ducklake_file_column_stats`.

## Output: text mode

Successful example:

```text
Bronze direct Silver-readiness gate
  range: 45800003 -> 45850002

Summary:
effects_row_v1           rows=138776214 distinct_ledgers=49999 min=45800003 max=45850002
ledgers_row_v2           rows=50000     distinct_ledgers=50000 min=45800003 max=45850002
operations_row_v2        rows=47437273  distinct_ledgers=50000 min=45800003 max=45850002
transactions_row_v2      rows=10045151  distinct_ledgers=50000 min=45800003 max=45850002

Blocking tx gaps from actual rows:

PASSED
```

Failure example:

```text
Bronze direct Silver-readiness gate
  range: 46500003 -> 46600002

Summary:
...

Blocking tx gaps from actual rows:
transactions missing where activity exists: 46512345 -> 46512500 (156 ledgers)

FAILED: 1 blocking gap interval
```

## Output: JSON mode

JSON output is required for `flowctl` and future automation.

Example success:

```json
{
  "gate": "bronze-silver-readiness",
  "range": {
    "start": 45800003,
    "end": 45850002
  },
  "status": "passed",
  "summary": {
    "ledgers_row_v2": {
      "rows": 50000,
      "distinct_ledgers": 50000,
      "min_ledger": 45800003,
      "max_ledger": 45850002
    },
    "transactions_row_v2": {
      "rows": 10045151,
      "distinct_ledgers": 50000,
      "min_ledger": 45800003,
      "max_ledger": 45850002
    },
    "operations_row_v2": {
      "rows": 47437273,
      "distinct_ledgers": 50000,
      "min_ledger": 45800003,
      "max_ledger": 45850002
    },
    "effects_row_v1": {
      "rows": 138776214,
      "distinct_ledgers": 49999,
      "min_ledger": 45800003,
      "max_ledger": 45850002
    }
  },
  "blocking_gaps": []
}
```

Example failure:

```json
{
  "gate": "bronze-silver-readiness",
  "range": {
    "start": 46500003,
    "end": 46600002
  },
  "status": "failed",
  "blocking_gaps": [
    {
      "type": "transactions_missing_where_activity_exists",
      "start": 46512345,
      "end": 46512500,
      "missing_ledgers": 156
    }
  ]
}
```

## Exit codes

Use stable exit codes so operators and control planes can distinguish failures.

```text
0  passed
1  readiness failed: blocking data gaps found
2  invalid CLI/config
3  DuckLake/catalog connection failure
4  object store/S3 read failure
5  query/internal error
```

This distinction matters because catalog/object-store failures are often retryable, while blocking data gaps require rerunning the Bronze chunk/range.

## DuckLake connection behavior

The tool should use `duckdb-go/v2` with DuckLake loaded from `core_nightly`, matching the working incident prototype.

Connection steps:

1. Open DuckDB in-memory DB.
2. Install/load extensions:
   - `postgres`
   - `ducklake`
   - `httpfs`
3. Configure S3/B2 secret.
4. Attach DuckLake catalog:

```sql
ATTACH 'ducklake:postgres:<catalog-dsn>' AS bronze_catalog
(DATA_PATH '<data-path>', METADATA_SCHEMA '<metadata-schema>', DATA_INLINING_ROW_LIMIT 100, AUTOMATIC_MIGRATION TRUE, OVERRIDE_DATA_PATH TRUE);
```

Use the configured schema when querying:

```text
bronze_catalog.<schema>.ledgers_row_v2
```

## Performance considerations

The checker can scan very large active row sets in dense ranges. Initial implementation can use direct `DISTINCT ledger_sequence` queries because operational chunks are currently 50k–250k ledgers.

Future optimizations:

- Use temp tables for `tx_ledgers` and `activity_ledgers`.
- Use `EXCEPT` instead of `LEFT JOIN` if faster.
- Add optional `--threads` / DuckDB memory limit flags.
- Add `--check ledgers-only` for quick checks.
- Add `--summary-only` when gap ranges are not needed.

## Integration with Bronze repair wrapper

Replace current shell wrapper call:

```bash
./verify-bronze-silver-readiness-direct-go.sh --start <start> --end <end>
```

with:

```bash
bronze-readiness-checker --start <start> --end <end> ...
```

The wrapper should interpret exit codes:

```text
0: continue to next chunk
1: stop; rerun same chunk/range
3/4: retry verification; if persistent, stop for operator
5: stop for operator
```

## flowctl integration

This checker should become the verifier component / library for flowctl Bronze gates.

`flowctl` should be able to run:

```bash
flowctl gates bronze-readiness --start 3 --end 62799999
```

Internally this can call the checker binary or import its Go package.

Required JSON fields for flowctl:

```text
gate
range.start
range.end
status
summary per table
blocking_gaps[]
failure_class when infrastructure/query failure occurs
```

Suggested failure classes:

```text
blocking_data_gap
catalog_connection_error
object_store_error
query_error
config_error
```

## Testing plan

### Unit tests

- Group contiguous missing ledgers into ranges.
- Classify co-missing transaction/activity spans as non-blocking.
- Classify activity-without-transaction spans as blocking.
- Validate exit-code mapping.
- Validate JSON output shape.

### Integration tests

Use either a small local DuckLake catalog or a mocked query layer.

Scenarios:

1. Complete ledgers and matching tx/activity: pass.
2. Missing ledger row: fail.
3. Activity ledger missing transaction: fail.
4. No tx and no activity on ledger: pass.
5. Catalog connection failure: exit code 3.
6. S3/object read failure: exit code 4.

### Production smoke test

Run against known repaired chunks:

```bash
bronze-readiness-checker --start 45800003 --end 45850002 ...
bronze-readiness-checker --start 39750003 --end 40000002 ...
bronze-readiness-checker --start 30750003 --end 31000002 ...
```

Expected: pass.

## Deliverables

1. Go CLI under `obsrvr-lake/bronze-readiness-checker`.
2. Makefile with:

```bash
make fmt
make test
make build
make docker-build
```

3. Dockerfile for Nomad/flowctl execution.
4. README with operator examples.
5. JSON output support.
6. Exit-code documentation.
7. Replacement of incident shell wrapper usage once validated.

## Non-goals

- Do not build full Bronze repair orchestration in this tool.
- Do not dispatch Nomad jobs from this tool.
- Do not mutate DuckLake state.
- Do not use catalog file stats as final readiness proof.

This tool is a verifier only. Orchestration belongs in `flowctl` / managed service flow.
