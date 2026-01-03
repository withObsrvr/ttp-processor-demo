package main

import (
	"context"
	"fmt"
	"log"
)

// RecreateAllBronzeTables drops all Bronze tables and recreates them with partitioning
// WARNING: This deletes ALL Bronze data and metadata!
// Use this for:
// - Initial deployment with partitioning
// - Schema migrations that require recreation
// - Recovering from corrupted metadata
func (c *DuckDBClient) RecreateAllBronzeTables(ctx context.Context) error {
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
// This can be used to verify that partitioning is correctly applied
func (c *DuckDBClient) GetPartitionStatus(ctx context.Context) (map[string]bool, error) {
	// Query DuckLake metadata to check partition status
	// This is a placeholder - DuckLake metadata queries may vary
	status := make(map[string]bool)

	bronzeTables := []string{
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
	}

	for _, table := range bronzeTables {
		// For now, assume all tables are partitioned after creation
		// In the future, we could query DuckLake metadata to verify
		status[table] = true
	}

	return status, nil
}

// MergeAllBronzeTables merges adjacent files for all Bronze tables
// This combines small Parquet files into larger ones for better query performance
// WARNING: Should be run when flusher is stopped/paused to avoid conflicts
func (c *DuckDBClient) MergeAllBronzeTables(ctx context.Context, maxFiles int) error {
	log.Printf("🔧 Starting file merge for all Bronze tables (max_compacted_files=%d)...", maxFiles)

	bronzeTables := []string{
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
	}

	successCount := 0
	for _, table := range bronzeTables {
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

	log.Printf("✅ File merge completed for %d/%d Bronze tables", successCount, len(bronzeTables))
	return nil
}

// ExpireBronzeSnapshots expires old snapshots to mark merged files for cleanup
// This is a catalog-level operation that expires snapshots for the Bronze catalog
func (c *DuckDBClient) ExpireBronzeSnapshots(ctx context.Context) error {
	log.Println("🗑️  Expiring old snapshots (catalog-level operation)...")

	// ducklake_expire_snapshots operates at catalog level
	expireSQL := fmt.Sprintf(`CALL ducklake_expire_snapshots('%s')`, c.config.CatalogName)

	if _, err := c.db.ExecContext(ctx, expireSQL); err != nil {
		return fmt.Errorf("failed to expire snapshots: %w", err)
	}

	log.Println("✅ Snapshots expired successfully")
	return nil
}

// CleanupBronzeOrphanedFiles removes orphaned Parquet files from S3
// This should be called after ExpireBronzeSnapshots to actually delete old files
func (c *DuckDBClient) CleanupBronzeOrphanedFiles(ctx context.Context) error {
	log.Println("🧹 Cleaning up orphaned files...")

	// ducklake_cleanup_old_files operates at catalog level
	cleanupSQL := fmt.Sprintf(`CALL ducklake_cleanup_old_files('%s')`, c.config.CatalogName)

	if _, err := c.db.ExecContext(ctx, cleanupSQL); err != nil {
		return fmt.Errorf("failed to cleanup orphaned files: %w", err)
	}

	log.Println("✅ Orphaned files cleaned up successfully")
	return nil
}

// PerformBronzeMaintenanceCycle runs merge, expire, and cleanup in sequence
// This is the recommended maintenance workflow for Bronze tables
// WARNING: MUST be run when flusher is stopped/paused to avoid deadlocks
func (c *DuckDBClient) PerformBronzeMaintenanceCycle(ctx context.Context, maxFiles int) error {
	log.Println("🔧 Starting full Bronze maintenance cycle (merge → expire → cleanup)...")

	// Step 1: Merge small files into larger ones
	if err := c.MergeAllBronzeTables(ctx, maxFiles); err != nil {
		return fmt.Errorf("merge failed: %w", err)
	}

	// Step 2: Expire old snapshots to mark files for deletion
	if err := c.ExpireBronzeSnapshots(ctx); err != nil {
		return fmt.Errorf("expire snapshots failed: %w", err)
	}

	// Step 3: Actually delete orphaned files from S3
	if err := c.CleanupBronzeOrphanedFiles(ctx); err != nil {
		return fmt.Errorf("cleanup failed: %w", err)
	}

	log.Println("✅ Full Bronze maintenance cycle completed successfully")
	return nil
}
