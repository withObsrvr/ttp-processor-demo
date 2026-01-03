package main

import (
	"fmt"
	"log"
)

// Maintenance operation constants
const (
	// DefaultMaxCompactedFiles is the default maximum number of files to compact during merge
	DefaultMaxCompactedFiles = 1000
)

// SilverTables is the centralized list of all Silver tables
var SilverTables = []string{
	// Snapshot tables (SCD Type 2)
	"accounts_snapshot",
	"trustlines_snapshot",
	"offers_snapshot",
	"account_signers_snapshot",

	// Current state tables
	"accounts_current",
	"trustlines_current",
	"offers_current",
	"claimable_balances_current",
	"contract_data_current",

	// Enriched operations tables
	"enriched_history_operations",
	"enriched_history_operations_soroban",
	"token_transfers_raw",
}

// RecreateAllSilverTables drops all Silver tables and recreates them with partitioning
// WARNING: This deletes ALL Silver data and metadata!
// Use this for:
// - Initial deployment with partitioning
// - Schema migrations that require recreation
// - Recovering from corrupted metadata
func (c *DuckDBClient) RecreateAllSilverTables() error {
	// Acquire exclusive lock - blocks all flushes during recreation
	if c.flusher != nil {
		c.flusher.mu.Lock()
		defer c.flusher.mu.Unlock()
	}

	log.Println("⚠️  WARNING: Recreating all Silver tables with partitioning...")
	log.Println("⚠️  This will DELETE ALL Silver data!")

	// Drop entire Silver schema (cascades to all tables)
	dropSchemaSQL := fmt.Sprintf("DROP SCHEMA IF EXISTS %s.%s CASCADE",
		c.config.CatalogName, c.config.SchemaName)

	log.Printf("Dropping Silver schema: %s.%s", c.config.CatalogName, c.config.SchemaName)

	if _, err := c.db.Exec(dropSchemaSQL); err != nil {
		return fmt.Errorf("failed to drop Silver schema: %w", err)
	}

	log.Println("✅ Silver schema dropped")

	// Recreate tables with partitioning
	log.Println("Recreating Silver tables with partitioning...")

	if err := c.createSilverTables(); err != nil {
		return fmt.Errorf("failed to recreate Silver tables: %w", err)
	}

	log.Println("✅ All Silver tables recreated with partitioning")
	log.Println("ℹ️  New data will be organized into ledger_range partitions on S3")
	log.Println("ℹ️  Example: s3://bucket/silver/accounts_snapshot/ledger_range=2/")

	return nil
}

// mergeAllSilverTablesInternal is the internal implementation without locking
func (c *DuckDBClient) mergeAllSilverTablesInternal(maxFiles int) error {
	log.Printf("🔧 Starting file merge for all Silver tables (max_compacted_files=%d)...", maxFiles)

	successCount := 0
	for _, table := range SilverTables {
		// Merge files with partitioning-aware adjacency
		// DuckLake will merge files within each partition
		mergeSQL := fmt.Sprintf(`CALL ducklake_merge_adjacent_files('%s', '%s', schema => '%s', max_compacted_files => %d)`,
			c.config.CatalogName, table, c.config.SchemaName, maxFiles)

		if _, err := c.db.Exec(mergeSQL); err != nil {
			log.Printf("⚠️  Failed to merge %s: %v", table, err)
			continue
		}

		log.Printf("✅ Merged: %s", table)
		successCount++
	}

	if successCount == 0 {
		return fmt.Errorf("failed to merge files for all %d Silver tables", len(SilverTables))
	}

	if successCount < len(SilverTables) {
		log.Printf("⚠️  File merge completed with partial success: %d/%d Silver tables", successCount, len(SilverTables))
	} else {
		log.Printf("✅ File merge completed for %d/%d Silver tables", successCount, len(SilverTables))
	}

	return nil
}

// MergeAllSilverTables merges adjacent files for all Silver tables
// This combines small Parquet files into larger ones for better query performance
// WARNING: Should be run when flusher is stopped/paused to avoid conflicts
func (c *DuckDBClient) MergeAllSilverTables(maxFiles int) error {
	// Acquire exclusive lock - blocks all flushes during merge
	if c.flusher != nil {
		c.flusher.mu.Lock()
		defer c.flusher.mu.Unlock()
	}

	return c.mergeAllSilverTablesInternal(maxFiles)
}

// expireSilverSnapshotsInternal is the internal implementation without locking
func (c *DuckDBClient) expireSilverSnapshotsInternal() error {
	log.Println("🗑️  Expiring old snapshots (catalog-level operation)...")

	// ducklake_expire_snapshots operates at catalog level
	expireSQL := fmt.Sprintf(`CALL ducklake_expire_snapshots('%s')`, c.config.CatalogName)

	if _, err := c.db.Exec(expireSQL); err != nil {
		return fmt.Errorf("failed to expire snapshots: %w", err)
	}

	log.Println("✅ Snapshots expired successfully")
	return nil
}

// ExpireSilverSnapshots expires old snapshots to mark merged files for cleanup
// This is a catalog-level operation that expires snapshots for the Silver catalog
func (c *DuckDBClient) ExpireSilverSnapshots() error {
	// Acquire exclusive lock - blocks all flushes during snapshot expiration
	if c.flusher != nil {
		c.flusher.mu.Lock()
		defer c.flusher.mu.Unlock()
	}

	return c.expireSilverSnapshotsInternal()
}

// cleanupSilverOrphanedFilesInternal is the internal implementation without locking
func (c *DuckDBClient) cleanupSilverOrphanedFilesInternal() error {
	log.Println("🧹 Cleaning up orphaned files...")

	// ducklake_cleanup_old_files operates at catalog level
	cleanupSQL := fmt.Sprintf(`CALL ducklake_cleanup_old_files('%s')`, c.config.CatalogName)

	if _, err := c.db.Exec(cleanupSQL); err != nil {
		return fmt.Errorf("failed to cleanup orphaned files: %w", err)
	}

	log.Println("✅ Orphaned files cleaned up successfully")
	return nil
}

// CleanupSilverOrphanedFiles removes orphaned Parquet files from S3
// This should be called after ExpireSilverSnapshots to actually delete old files
func (c *DuckDBClient) CleanupSilverOrphanedFiles() error {
	// Acquire exclusive lock - blocks all flushes during cleanup
	if c.flusher != nil {
		c.flusher.mu.Lock()
		defer c.flusher.mu.Unlock()
	}

	return c.cleanupSilverOrphanedFilesInternal()
}

// PerformSilverMaintenanceCycle runs merge, expire, and cleanup in sequence
// This is the recommended maintenance workflow for Silver tables
// WARNING: MUST be run when flusher is stopped/paused to avoid deadlocks
func (c *DuckDBClient) PerformSilverMaintenanceCycle(maxFiles int) error {
	// Acquire exclusive lock once for entire maintenance cycle
	if c.flusher != nil {
		c.flusher.mu.Lock()
		defer c.flusher.mu.Unlock()
	}

	log.Println("🔧 Starting full Silver maintenance cycle (merge → expire → cleanup)...")

	// Step 1: Merge small files into larger ones
	if err := c.mergeAllSilverTablesInternal(maxFiles); err != nil {
		return fmt.Errorf("merge failed: %w", err)
	}

	// Step 2: Expire old snapshots to mark files for deletion
	if err := c.expireSilverSnapshotsInternal(); err != nil {
		return fmt.Errorf("expire snapshots failed: %w", err)
	}

	// Step 3: Actually delete orphaned files from S3
	if err := c.cleanupSilverOrphanedFilesInternal(); err != nil {
		return fmt.Errorf("cleanup failed: %w", err)
	}

	log.Println("✅ Full Silver maintenance cycle completed successfully")
	return nil
}

// GetPartitionStatus returns partitioning status for all Silver tables
// NOTE: This functionality is not yet implemented and currently cannot
//
//	query DuckLake metadata to determine actual partition status.
func (c *DuckDBClient) GetPartitionStatus() (map[string]bool, error) {
	return nil, fmt.Errorf("GetPartitionStatus is not implemented: partition status cannot be determined from DuckLake metadata yet")
}
