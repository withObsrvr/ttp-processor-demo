package main

import (
	"context"
	"fmt"
	"log"
	"time"
)

// Maintenance operation constants
const (
	// DefaultMaxCompactedFiles is the default maximum number of files to compact during merge
	DefaultMaxCompactedFiles = 1000
)

// BronzeTables is the centralized list of all Bronze tables
var BronzeTables = []string{
	"ledgers_row_v2",
	"transactions_row_v2",
	"operations_row_v2",
	"effects_row_v1",
	"trades_row_v1",
	"contract_events_stream_v1",
	"accounts_snapshot_v1",
	"trustlines_snapshot_v1",
	"account_signers_snapshot_v1",
	"native_balances_snapshot_v1",
	"offers_snapshot_v1",
	"liquidity_pools_snapshot_v1",
	"claimable_balances_snapshot_v1",
	"contract_data_snapshot_v1",
	"contract_code_snapshot_v1",
	"restored_keys_state_v1",
	"ttl_snapshot_v1",
	"evicted_keys_state_v1",
	"config_settings_snapshot_v1",
	"contract_creations_v1",
}

// HighVolumeBronzeTables are tables that accumulate files fastest and need
// controlled merge batch sizes to avoid memory spikes during compaction.
var HighVolumeBronzeTables = []string{
	"contract_events_stream_v1",
	"operations_row_v2",
	"transactions_row_v2",
	"accounts_snapshot_v1",
	"native_balances_snapshot_v1",
}

// RunCheckpoint performs automated DuckLake maintenance:
// Merges small Parquet files into larger ones for query performance.
//
// IMPORTANT: This intentionally does NOT run CHECKPOINT, expire_snapshots, or
// cleanup_old_files. Those operations delete historical Parquet files from S3,
// which destroys cold storage data. Our lakehouse architecture treats cold storage
// as a permanent append-only archive — files should never be deleted automatically.
//
// Callers should hold the flusher's write lock to avoid conflicts with concurrent flushes.
func (c *DuckDBClient) RunCheckpoint(ctx context.Context, maxCompactedFiles int) error {
	startTime := time.Now()
	log.Println("🔧 Running DuckLake merge maintenance (merge only, no expire/cleanup)...")

	successCount := 0
	for _, table := range HighVolumeBronzeTables {
		mergeSQL := fmt.Sprintf(
			`CALL ducklake_merge_adjacent_files('%s', '%s', schema => '%s', max_compacted_files => %d)`,
			c.config.CatalogName, table, c.config.SchemaName, maxCompactedFiles)
		if _, err := c.db.ExecContext(ctx, mergeSQL); err != nil {
			log.Printf("   ⚠️  merge %s: %v", table, err)
		} else {
			log.Printf("   ✅ merged %s", table)
			successCount++
		}
	}

	log.Printf("✅ DuckLake merge completed (%d/%d tables) in %s",
		successCount, len(HighVolumeBronzeTables), time.Since(startTime).Round(time.Millisecond))
	return nil
}

// RecreateAllBronzeTables drops all Bronze tables and recreates them with partitioning
// WARNING: This deletes ALL Bronze data and metadata!
// Use this for:
// - Initial deployment with partitioning
// - Schema migrations that require recreation
// - Recovering from corrupted metadata
func (c *DuckDBClient) RecreateAllBronzeTables(ctx context.Context) error {
	// Acquire exclusive lock - blocks all flushes during recreation
	if c.flusher != nil {
		c.flusher.mu.Lock()
		defer c.flusher.mu.Unlock()
	}

	log.Println("⚠️  WARNING: Recreating all Bronze tables with partitioning...")
	log.Println("⚠️  This will DELETE ALL Bronze data!")

	// Drop entire Bronze schema (cascades to all tables)
	dropSchemaSQL := fmt.Sprintf("DROP SCHEMA IF EXISTS %s.%s CASCADE",
		c.config.CatalogName, c.config.SchemaName)

	log.Printf("Dropping Bronze schema: %s.%s", c.config.CatalogName, c.config.SchemaName)

	if _, err := c.db.ExecContext(ctx, dropSchemaSQL); err != nil {
		return fmt.Errorf("failed to drop Bronze schema: %w", err)
	}

	log.Println("✅ Bronze schema dropped")

	// Recreate tables with partitioning
	log.Println("Recreating Bronze tables with partitioning...")

	if err := c.createBronzeTables(ctx); err != nil {
		return fmt.Errorf("failed to recreate Bronze tables: %w", err)
	}

	log.Println("✅ All Bronze tables recreated with partitioning")
	log.Println("ℹ️  New data will be organized into ledger_range partitions on S3")
	log.Println("ℹ️  Example: s3://bucket/bronze/transactions_row_v2/ledger_range=2/")

	return nil
}

// GetPartitionStatus returns partitioning status for all Bronze tables
// NOTE: This functionality is not yet implemented and currently cannot
//
//	query DuckLake metadata to determine actual partition status.
func (c *DuckDBClient) GetPartitionStatus(ctx context.Context) (map[string]bool, error) {
	// ctx is currently unused but kept in the signature for future implementations
	_ = ctx

	return nil, fmt.Errorf("GetPartitionStatus is not implemented: partition status cannot be determined from DuckLake metadata yet")
}

// mergeAllBronzeTablesInternal is the internal implementation without locking
func (c *DuckDBClient) mergeAllBronzeTablesInternal(ctx context.Context, maxFiles int) error {
	log.Printf("🔧 Starting file merge for all Bronze tables (max_compacted_files=%d)...", maxFiles)

	successCount := 0
	for _, table := range BronzeTables {
		// Merge files with partitioning-aware adjacency
		// DuckLake will merge files within each partition
		mergeSQL := fmt.Sprintf(`CALL ducklake_merge_adjacent_files('%s', '%s', schema => '%s', max_compacted_files => %d)`,
			c.config.CatalogName, table, c.config.SchemaName, maxFiles)

		if _, err := c.db.ExecContext(ctx, mergeSQL); err != nil {
			log.Printf("⚠️  Failed to merge %s: %v", table, err)
			continue
		}

		log.Printf("✅ Merged: %s", table)
		successCount++
	}

	if successCount == 0 {
		return fmt.Errorf("failed to merge files for all %d Bronze tables", len(BronzeTables))
	}

	if successCount < len(BronzeTables) {
		log.Printf("⚠️  File merge completed with partial success: %d/%d Bronze tables", successCount, len(BronzeTables))
	} else {
		log.Printf("✅ File merge completed for %d/%d Bronze tables", successCount, len(BronzeTables))
	}

	return nil
}

// MergeAllBronzeTables merges adjacent files for all Bronze tables
// This combines small Parquet files into larger ones for better query performance
// WARNING: Should be run when flusher is stopped/paused to avoid conflicts
func (c *DuckDBClient) MergeAllBronzeTables(ctx context.Context, maxFiles int) error {
	// Acquire exclusive lock - blocks all flushes during merge
	if c.flusher != nil {
		c.flusher.mu.Lock()
		defer c.flusher.mu.Unlock()
	}

	return c.mergeAllBronzeTablesInternal(ctx, maxFiles)
}

// expireBronzeSnapshotsInternal is the internal implementation without locking
func (c *DuckDBClient) expireBronzeSnapshotsInternal(ctx context.Context) error {
	log.Println("🗑️  Expiring old snapshots (catalog-level operation)...")

	// ducklake_expire_snapshots operates at catalog level
	expireSQL := fmt.Sprintf(`CALL ducklake_expire_snapshots('%s')`, c.config.CatalogName)

	if _, err := c.db.ExecContext(ctx, expireSQL); err != nil {
		return fmt.Errorf("failed to expire snapshots: %w", err)
	}

	log.Println("✅ Snapshots expired successfully")
	return nil
}

// ExpireBronzeSnapshots expires old snapshots to mark merged files for cleanup
// This is a catalog-level operation that expires snapshots for the Bronze catalog
func (c *DuckDBClient) ExpireBronzeSnapshots(ctx context.Context) error {
	// Acquire exclusive lock - blocks all flushes during snapshot expiration
	if c.flusher != nil {
		c.flusher.mu.Lock()
		defer c.flusher.mu.Unlock()
	}

	return c.expireBronzeSnapshotsInternal(ctx)
}

// cleanupBronzeOrphanedFilesInternal is the internal implementation without locking
func (c *DuckDBClient) cleanupBronzeOrphanedFilesInternal(ctx context.Context) error {
	log.Println("🧹 Cleaning up orphaned files...")

	// ducklake_cleanup_old_files operates at catalog level
	cleanupSQL := fmt.Sprintf(`CALL ducklake_cleanup_old_files('%s')`, c.config.CatalogName)

	if _, err := c.db.ExecContext(ctx, cleanupSQL); err != nil {
		return fmt.Errorf("failed to cleanup orphaned files: %w", err)
	}

	log.Println("✅ Orphaned files cleaned up successfully")
	return nil
}

// CleanupBronzeOrphanedFiles removes orphaned Parquet files from S3
// This should be called after ExpireBronzeSnapshots to actually delete old files
func (c *DuckDBClient) CleanupBronzeOrphanedFiles(ctx context.Context) error {
	// Acquire exclusive lock - blocks all flushes during cleanup
	if c.flusher != nil {
		c.flusher.mu.Lock()
		defer c.flusher.mu.Unlock()
	}

	return c.cleanupBronzeOrphanedFilesInternal(ctx)
}

// PerformBronzeMaintenanceCycle runs merge, expire, and cleanup in sequence
// This is the recommended maintenance workflow for Bronze tables
// WARNING: MUST be run when flusher is stopped/paused to avoid deadlocks
func (c *DuckDBClient) PerformBronzeMaintenanceCycle(ctx context.Context, maxFiles int) error {
	// Acquire exclusive lock once for entire maintenance cycle
	if c.flusher != nil {
		c.flusher.mu.Lock()
		defer c.flusher.mu.Unlock()
	}

	log.Println("🔧 Starting full Bronze maintenance cycle (merge → expire → cleanup)...")

	// Step 1: Merge small files into larger ones
	if err := c.mergeAllBronzeTablesInternal(ctx, maxFiles); err != nil {
		return fmt.Errorf("merge failed: %w", err)
	}

	// Step 2: Expire old snapshots to mark files for deletion
	if err := c.expireBronzeSnapshotsInternal(ctx); err != nil {
		return fmt.Errorf("expire snapshots failed: %w", err)
	}

	// Step 3: Actually delete orphaned files from S3
	if err := c.cleanupBronzeOrphanedFilesInternal(ctx); err != nil {
		return fmt.Errorf("cleanup failed: %w", err)
	}

	log.Println("✅ Full Bronze maintenance cycle completed successfully")
	return nil
}
