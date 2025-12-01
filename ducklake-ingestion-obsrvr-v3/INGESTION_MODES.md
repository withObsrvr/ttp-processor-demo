# Ingestion Modes: Backfill vs Live

## Overview

The ingester has two distinct modes with different performance characteristics:

1. **Backfill Mode**: Fast bulk ingestion with large batches
2. **Live Mode**: Low-latency continuous ingestion with small batches

## Configuration

### `end_ledger` Behavior (IMPORTANT!)

- **`end_ledger: 0`**: Stream continuously (never stop)
- **`end_ledger: <number>`**: ALWAYS stop at that ledger (ignores `live_mode` setting)

**Example:**
```yaml
source:
  start_ledger: 1000000
  end_ledger: 2000000  # Will STOP at ledger 2000000
  live_mode: true      # This is IGNORED when end_ledger is set
```

Result: Ingestion STOPS at ledger 2000000 (does NOT continue to live mode)

### Backfill Mode Settings

Used during bulk ingestion (historical data):

```yaml
ducklake:
  batch_size: 5000              # Large batches for throughput
  commit_interval_seconds: 600   # Commit every 10 minutes
```

**Characteristics:**
- Optimized for maximum throughput
- Large batches reduce flush overhead
- Suitable for processing millions of ledgers

**Performance:** 50-100 ledgers/sec (single worker)

### Live Mode Settings

Used for continuous real-time ingestion:

```yaml
ducklake:
  live_batch_size: 1             # Process 1 ledger at a time
  live_commit_interval_seconds: 5 # Commit every 5 seconds
```

**Characteristics:**
- Optimized for low latency
- Small batches for quick data availability
- Polls for new ledgers every `poll_interval_seconds`

**Performance:** 3-5 ledgers/sec (matches Stellar ledger close time)

## Usage Patterns

### Pattern 1: Backfill Only (Recommended for Historical Data)

**Use case:** Ingest historical ledgers 1 - 60,000,000

```yaml
source:
  start_ledger: 1
  end_ledger: 60000000  # Stop at ledger 60M
  live_mode: false      # Not used (end_ledger takes precedence)

ducklake:
  batch_size: 5000
  commit_interval_seconds: 600

quality:
  enabled: false  # Disable for speed
```

**What happens:**
1. Starts at ledger 1
2. Processes with batch_size=5000
3. Stops at ledger 60,000,000
4. Exits

### Pattern 2: Live Only (Recommended for Real-Time)

**Use case:** Only ingest new ledgers from now on

```yaml
source:
  start_ledger: 60000000  # Start from current ledger
  end_ledger: 0           # Never stop
  live_mode: true         # Enable live polling

ducklake:
  batch_size: 100         # Not used in live mode
  live_batch_size: 1      # Used in live mode
  live_commit_interval_seconds: 5

quality:
  enabled: true  # Enable for data quality
```

**What happens:**
1. Starts at ledger 60,000,000
2. Streams available ledgers with batch_size=100 (backfill mode)
3. When stream ends (no more ledgers), switches to live_batch_size=1
4. Polls every 5 seconds for new ledgers
5. Runs forever

### Pattern 3: Backfill + Live (Two-Phase Manual)

**Use case:** Backfill historical data, then start live ingestion

**Phase 1: Backfill**
```yaml
# backfill.yaml
source:
  start_ledger: 1
  end_ledger: 60000000  # Current latest ledger
  live_mode: false

ducklake:
  batch_size: 5000
  commit_interval_seconds: 600
```

Run: `./ducklake-ingestion-obsrvr-v3 -config backfill.yaml`

Wait for completion (shows "Reached end ledger 60000000, stopping ingestion")

**Phase 2: Live**
```yaml
# live.yaml
source:
  start_ledger: 60000001  # Continue from where backfill stopped
  end_ledger: 0
  live_mode: true

ducklake:
  batch_size: 100
  live_batch_size: 1
  live_commit_interval_seconds: 5

quality:
  enabled: true  # Re-enable quality checks
```

Run: `./ducklake-ingestion-obsrvr-v3 -config live.yaml`

### Pattern 4: Continuous Stream (No Live Mode)

**Use case:** Stream all available ledgers but don't poll for new ones

```yaml
source:
  start_ledger: 1
  end_ledger: 0     # Never stop
  live_mode: false  # Don't switch to polling

ducklake:
  batch_size: 5000  # Always use large batches
```

**What happens:**
1. Streams all available ledgers with batch_size=5000
2. When source runs out of ledgers, exits
3. Does NOT poll for new ledgers

## Configuration Reference

### Source Settings

| Setting | Type | Description |
|---------|------|-------------|
| `start_ledger` | uint32 | First ledger to process |
| `end_ledger` | uint32 | Last ledger to process (0 = never stop) |
| `live_mode` | bool | Enable live polling (only used when end_ledger=0) |
| `poll_interval_seconds` | int | Seconds between polls in live mode (default: 5) |

### DuckLake Settings

| Setting | Type | Description |
|---------|------|-------------|
| `batch_size` | int | Ledgers per batch in backfill mode (default: 200) |
| `commit_interval_seconds` | int | Seconds between commits in backfill (default: 30) |
| `live_batch_size` | int | Ledgers per batch in live mode (default: 1) |
| `live_commit_interval_seconds` | int | Seconds between commits in live mode (default: 5) |

## Performance Tuning

### Backfill Performance

**Goal:** Maximize throughput

Recommended settings:
```yaml
ducklake:
  batch_size: 5000-10000
  commit_interval_seconds: 600-1800
  num_workers: 1  # DuckDB catalog limitation

quality:
  enabled: false
```

**Memory consideration:** 5000 ledgers ≈ 1-2 GB RAM

### Live Performance

**Goal:** Minimize latency

Recommended settings:
```yaml
ducklake:
  live_batch_size: 1
  live_commit_interval_seconds: 5

quality:
  enabled: true  # Safe for low volume
```

**Latency:** ~5-10 seconds from ledger close to data availability

## Common Mistakes

### ❌ Expecting live mode when end_ledger is set

```yaml
source:
  end_ledger: 60000000
  live_mode: true  # This does NOTHING!
```

**Fix:** Remove `end_ledger` if you want continuous streaming

### ❌ Using small batch_size for backfill

```yaml
ducklake:
  batch_size: 1  # Too slow!
```

**Fix:** Use 1000-10000 for backfill

### ❌ Running live mode without polling

```yaml
source:
  end_ledger: 0
  live_mode: false  # Won't poll for new ledgers
```

**Fix:** Set `live_mode: true` for continuous ingestion

## Decision Tree

```
Do you want to process historical ledgers?
├─ YES: Set end_ledger to stop point OR 0 for all available
│       Use batch_size=5000, quality.enabled=false
│
└─ NO: Set start_ledger to current ledger
       Set end_ledger=0, live_mode=true
       Use live_batch_size=1, quality.enabled=true

Already caught up and want real-time data?
└─ YES: Set end_ledger=0, live_mode=true
        The system will switch to polling automatically
```
