# Index Plane Transformer - Maintenance Guide

## Overview

The Index Plane transformer writes transaction index data to DuckLake (Parquet files on S3). Over time, many small Parquet files accumulate. Periodic maintenance improves query performance by consolidating small files into larger ones and cleaning up orphaned data.

## Maintenance Operations

The transformer provides HTTP endpoints for maintenance operations:

### Individual Operations

1. **Merge Adjacent Files** - Consolidates small Parquet files into larger ones
   ```bash
   curl -X POST http://<host>:8096/maintenance/merge
   ```

2. **Expire Old Snapshots** - Marks old snapshots for cleanup (retains 20 most recent)
   ```bash
   curl -X POST http://<host>:8096/maintenance/expire
   ```

3. **Cleanup Orphaned Files** - Removes orphaned Parquet files from S3
   ```bash
   curl -X POST http://<host>:8096/maintenance/cleanup
   ```

### Full Maintenance Cycle (Recommended)

Runs merge → expire → cleanup in sequence:

```bash
curl -X POST http://<host>:8096/maintenance/full
```

**Response:**
```json
{
  "status": "success",
  "files_before": 406,
  "files_after": 2,
  "files_merged": 404,
  "max_compact_files": 1000,
  "retain_snapshots": 20,
  "steps_completed": ["merge", "expire", "cleanup"]
}
```

## Concurrency and Safety

### ⚠️ Important: Deadlock Risk

Maintenance operations **may deadlock** if run while the transformer is actively inserting data. The risk depends on timing:

- ✅ **Safe**: Called between transformation cycles (30s poll interval)
- ⚠️ **Risky**: Called during active INSERT operations
- ❌ **Unsafe**: Called repeatedly in quick succession

### Recommended Workflow

**Option 1: Stop transformer during maintenance** (Safest)

```bash
# On the server
docker stop <transformer-container-id>

# Run full maintenance
curl -X POST http://<host>:8096/maintenance/full

# Restart transformer
docker start <transformer-container-id>
```

**Option 2: Use Nomad job dispatch** (for scheduled maintenance)

```bash
# Stop the transformer job
nomad job stop index-plane-transformer

# Wait for graceful shutdown
sleep 10

# Run maintenance from a separate one-off job
# (See nomad/maintenance-job.nomad example)

# Restart the transformer
nomad job run index-plane-transformer.nomad
```

**Option 3: Low-risk concurrent execution** (Advanced)

If you need to run maintenance without stopping:
- Monitor the transformer logs to ensure it's between transformation cycles
- Call the endpoint during the 30s poll interval
- Be prepared to restart the container if it deadlocks
- Never run maintenance more than once per hour

## Maintenance Schedule

### Recommended Frequency

- **Daily**: Not needed - DuckLake handles small file accumulation well
- **Weekly**: Run full maintenance cycle if write volume is high
- **Monthly**: Run full maintenance cycle for typical workloads
- **On-demand**: After backfilling historical data or bulk operations

### When to Run Maintenance

**Indicators you need maintenance:**
1. File count > 1000 (check via health endpoint or DuckLake metadata)
2. Query performance degrades noticeably
3. After large backfill operations
4. Storage costs increase unexpectedly (orphaned files)

**Check current file count:**
```bash
# Query DuckLake metadata
SELECT COUNT(*)
FROM __ducklake_metadata_testnet_catalog.index.ducklake_data_file df
JOIN __ducklake_metadata_testnet_catalog.index.ducklake_table t
  ON df.table_id = t.table_id
WHERE t.table_name = 'tx_hash_index';
```

## Maintenance Configuration

The maintenance endpoints use these defaults:

- `max_compact_files`: 1000 - Maximum files to merge in one operation
- `retain_snapshots`: 20 - Number of recent snapshots to retain

To customize, modify `index_writer.go` and rebuild the image.

## Troubleshooting

### Maintenance endpoint times out

**Cause**: Deadlock with active INSERT operations

**Solution**:
1. Check transformer logs: `docker logs <container-id> --tail 50`
2. If hung, restart the container: `docker restart <container-id>`
3. Run maintenance when transformer is stopped

### Files not being merged (files_merged: 0)

**Possible reasons**:
1. Files don't have adjacent ledger ranges
2. Files are already optimally sized
3. Only one file exists per partition

**Action**: This is normal - not all file arrangements benefit from merging

### Cleanup doesn't free storage

**Likely cause**: Snapshots haven't been expired yet

**Solution**: Run in sequence:
```bash
curl -X POST http://<host>:8096/maintenance/expire
sleep 5
curl -X POST http://<host>:8096/maintenance/cleanup
```

Or use the full maintenance cycle which does this automatically.

## Production Deployment Pattern

For production, schedule maintenance using a cron job or periodic Nomad job:

```bash
#!/bin/bash
# maintenance-cron.sh - Run weekly on Sundays at 2 AM

# Stop transformer
nomad job stop index-plane-transformer

# Wait for graceful shutdown
sleep 30

# Run full maintenance (with timeout)
curl -X POST --max-time 600 http://134.209.123.25:8096/maintenance/full

# Restart transformer
nomad job run /opt/nomad/jobs/index-plane-transformer.nomad
```

Crontab entry:
```
0 2 * * 0 /opt/scripts/maintenance-cron.sh
```

## Performance Impact

- **Merge operation**: 5-30 seconds per 100 files
- **Expire snapshots**: < 1 second
- **Cleanup files**: Depends on number of orphaned files (~1s per 100 files)
- **Full cycle**: 1-5 minutes for typical workloads

During maintenance:
- **Index reads**: Continue normally (snapshots ensure consistency)
- **Index writes**: May deadlock if concurrent (hence the stop-before-maintenance recommendation)

## Monitoring

Check maintenance status via health endpoint:

```bash
curl -s http://134.209.123.25:8096/health | jq
```

Key metrics:
- `last_ledger_processed`: Current checkpoint
- `lag_seconds`: How far behind real-time
- `transformation_errors`: Should be 0

After maintenance, verify query performance:

```sql
-- Check file count
SELECT COUNT(*) FROM __ducklake_metadata_testnet_catalog.index.ducklake_data_file;

-- Sample query to test performance
SELECT COUNT(*) FROM testnet_catalog.index.tx_hash_index;
```

## Summary

✅ **Do**:
- Run full maintenance cycle monthly or after bulk operations
- Stop transformer before maintenance for safety
- Monitor file count and query performance
- Keep at least 20 snapshots retained

❌ **Don't**:
- Run maintenance during active insertions (deadlock risk)
- Run maintenance more than once per hour
- Delete snapshots manually (use expire endpoint)
- Skip cleanup step after merging (wastes storage)

For questions or issues, check the transformer logs first:
```bash
docker logs <container-id> --tail 100 -f
```
