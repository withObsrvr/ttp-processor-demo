package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"strings"
	"time"

	_ "github.com/duckdb/duckdb-go/v2"
)

// IndexWriter writes transaction index data to Index Plane DuckLake
type IndexWriter struct {
	db     *sql.DB
	config *IndexColdConfig
}

// NewIndexWriter creates a new Index writer
func NewIndexWriter(config *IndexColdConfig, catalogDB *sql.DB) (*IndexWriter, error) {
	// Open DuckDB connection for writing to Index Plane
	db, err := sql.Open("duckdb", config.DuckDBPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open DuckDB: %w", err)
	}

	writer := &IndexWriter{
		db:     db,
		config: config,
	}

	// Initialize DuckDB with DuckLake catalog
	if err := writer.initialize(); err != nil {
		db.Close()
		return nil, err
	}

	return writer, nil
}

// initialize sets up DuckDB extensions and DuckLake catalog
func (iw *IndexWriter) initialize() error {
	log.Println("🔧 Initializing Index Writer (DuckDB with DuckLake)...")

	// Install and load required extensions
	if _, err := iw.db.Exec("INSTALL ducklake"); err != nil {
		return fmt.Errorf("failed to install ducklake extension: %w", err)
	}
	if _, err := iw.db.Exec("LOAD ducklake"); err != nil {
		return fmt.Errorf("failed to load ducklake extension: %w", err)
	}
	if _, err := iw.db.Exec("INSTALL httpfs"); err != nil {
		return fmt.Errorf("failed to install httpfs extension: %w", err)
	}
	if _, err := iw.db.Exec("LOAD httpfs"); err != nil {
		return fmt.Errorf("failed to load httpfs extension: %w", err)
	}

	log.Println("✅ Extensions loaded")

	// Configure S3 credentials
	log.Println("🔑 Configuring S3 credentials...")
	secretSQL := fmt.Sprintf(`CREATE SECRET (
		TYPE S3,
		KEY_ID '%s',
		SECRET '%s',
		REGION '%s',
		ENDPOINT '%s',
		URL_STYLE 'path'
	)`, iw.config.S3AccessKeyID, iw.config.S3SecretAccessKey, iw.config.S3Region, iw.config.S3Endpoint)

	if _, err := iw.db.Exec(secretSQL); err != nil {
		return fmt.Errorf("failed to configure S3: %w", err)
	}

	log.Println("✅ S3 configured")

	// Attach DuckLake catalog
	log.Println("🔗 Attaching DuckLake catalog...")
	catalogPath := fmt.Sprintf("ducklake:postgres:postgresql://%s:%s@%s:%d/%s?sslmode=%s",
		iw.config.CatalogUser, iw.config.CatalogPassword,
		iw.config.CatalogHost, iw.config.CatalogPort, iw.config.CatalogDatabase, iw.config.CatalogSSLMode)

	dataPath := fmt.Sprintf("s3://%s/", iw.config.S3Bucket)

	// Use same schema for both data and metadata (index)
	// This ensures queries looking for metadata in 'index' schema will find it
	attachSQL := fmt.Sprintf(`ATTACH '%s' AS %s (DATA_PATH '%s', METADATA_SCHEMA '%s', DATA_INLINING_ROW_LIMIT 10000, AUTOMATIC_MIGRATION TRUE, OVERRIDE_DATA_PATH TRUE)`,
		catalogPath, iw.config.CatalogName, dataPath, iw.config.SchemaName)

	if _, err := iw.db.Exec(attachSQL); err != nil {
		return fmt.Errorf("failed to attach DuckLake catalog: %w", err)
	}

	log.Println("✅ DuckLake catalog attached")

	// Create Index Plane table in DuckLake
	if err := iw.createIndexTable(); err != nil {
		return fmt.Errorf("failed to create Index table: %w", err)
	}

	log.Println("✅ Index Writer initialized")

	return nil
}

// createIndexTable creates the configured index table in DuckLake catalog
func (iw *IndexWriter) createIndexTable() error {
	log.Println("Creating Index Plane schema and table in DuckLake catalog...")

	// Create schema first if it doesn't exist
	createSchemaSQL := fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s.%s",
		iw.config.CatalogName, iw.config.SchemaName)

	if _, err := iw.db.Exec(createSchemaSQL); err != nil {
		return fmt.Errorf("failed to create schema in DuckLake: %w", err)
	}

	log.Printf("✅ Schema created: %s.%s", iw.config.CatalogName, iw.config.SchemaName)

	// Create table in DuckLake catalog - DuckLake will manage Parquet storage automatically
	fullTableName := fmt.Sprintf("%s.%s.%s", iw.config.CatalogName, iw.config.SchemaName, iw.config.TableName)

	createTableSQL, partitionColumn := iw.createTableDDL(fullTableName)

	if _, err := iw.db.Exec(createTableSQL); err != nil {
		return fmt.Errorf("failed to create table in DuckLake: %w", err)
	}

	log.Printf("✅ Index Plane table created: %s", fullTableName)

	partitionSQL := fmt.Sprintf(`ALTER TABLE %s SET PARTITIONED BY (%s)`, fullTableName, partitionColumn)

	if _, err := iw.db.Exec(partitionSQL); err != nil {
		return fmt.Errorf("failed to set partitioning: %w", err)
	}

	log.Printf("✅ Partitioning enabled: %s", partitionColumn)
	log.Printf("ℹ️  Files will now be organized by %s, enabling efficient merges", partitionColumn)
	return nil
}

func (iw *IndexWriter) createTableDDL(fullTableName string) (string, string) {
	return fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			account_id VARCHAR,
			account_bucket BIGINT,
			ledger_range BIGINT
		)
	`, fullTableName), "account_bucket"
}

// DropIndexTable drops the account index table (for recreating with new schema)
func (iw *IndexWriter) DropIndexTable() error {
	fullTableName := fmt.Sprintf("%s.%s.%s", iw.config.CatalogName, iw.config.SchemaName, iw.config.TableName)

	log.Printf("⚠️  Dropping table: %s", fullTableName)

	dropSQL := fmt.Sprintf("DROP TABLE IF EXISTS %s", fullTableName)
	if _, err := iw.db.Exec(dropSQL); err != nil {
		return fmt.Errorf("failed to drop table: %w", err)
	}

	log.Printf("✅ Table dropped: %s", fullTableName)
	return nil
}

// RecreateTable drops and recreates the index table with proper partitioning
// WARNING: This deletes all indexed data AND purges old metadata!
func (iw *IndexWriter) RecreateTable() error {
	log.Println("⚠️  Recreating index table with partitioning...")

	// Drop existing table (this removes table from DuckLake metadata)
	if err := iw.DropIndexTable(); err != nil {
		return fmt.Errorf("failed to drop table: %w", err)
	}

	// IMPORTANT: DuckLake's DROP TABLE doesn't remove orphaned file metadata
	// We need to manually clean up old data_file records that have no parent table
	log.Println("🧹 Purging orphaned metadata from catalog...")
	if err := iw.purgeOrphanedMetadata(); err != nil {
		log.Printf("⚠️  Failed to purge orphaned metadata (continuing anyway): %v", err)
	}

	// Recreate with partitioning
	if err := iw.createIndexTable(); err != nil {
		return fmt.Errorf("failed to recreate table: %w", err)
	}

	log.Println("✅ Table recreated successfully with partitioning enabled")
	return nil
}

// purgeOrphanedMetadata removes orphaned file metadata from the catalog
// This is needed because DROP TABLE doesn't always clean up all metadata
func (iw *IndexWriter) purgeOrphanedMetadata() error {
	metadataSchema := fmt.Sprintf("__ducklake_metadata_%s.%s", iw.config.CatalogName, iw.config.SchemaName)

	// Delete ALL file metadata for this table name (across all table_ids)
	// This is more aggressive but necessary because DROP TABLE doesn't remove table records
	purgeSQL := fmt.Sprintf(`
		DELETE FROM %s.ducklake_data_file
		WHERE table_id IN (
			SELECT table_id
			FROM %s.ducklake_table
			WHERE table_name = '%s'
		)
	`, metadataSchema, metadataSchema, iw.config.TableName)

	result, err := iw.db.Exec(purgeSQL)
	if err != nil {
		return fmt.Errorf("failed to purge file metadata: %w", err)
	}

	rowsDeleted, _ := result.RowsAffected()
	log.Printf("🗑️  Purged %d file metadata records for table %s", rowsDeleted, iw.config.TableName)

	return nil
}

// WriteAccountLedgerRanges writes account ledger-range entries to Index Plane.
//
// Duplicates are tolerated intentionally. The table is range-granular and read
// with DISTINCT/set semantics; a whole-table anti-join would be more expensive
// than occasional duplicate rows.
func (iw *IndexWriter) WriteAccountLedgerRanges(ctx context.Context, rows []AccountLedgerIndex) (int64, error) {
	if len(rows) == 0 {
		return 0, nil
	}

	fullTableName := fmt.Sprintf("%s.%s.%s", iw.config.CatalogName, iw.config.SchemaName, iw.config.TableName)
	createTempSQL := `
		CREATE TEMP TABLE IF NOT EXISTS temp_account_ledger_index AS
		SELECT * FROM (VALUES
			('', CAST(0 AS BIGINT), CAST(0 AS BIGINT))
		) AS t(account_id, account_bucket, ledger_range)
		WHERE false
	`
	if _, err := iw.db.ExecContext(ctx, createTempSQL); err != nil {
		return 0, fmt.Errorf("failed to create account temp table: %w", err)
	}
	if _, err := iw.db.ExecContext(ctx, "DELETE FROM temp_account_ledger_index"); err != nil {
		return 0, fmt.Errorf("failed to clear account temp table: %w", err)
	}

	values := make([]string, 0, len(rows))
	seen := make(map[string]struct{}, len(rows))
	for _, row := range rows {
		if row.AccountID == "" {
			continue
		}
		key := fmt.Sprintf("%s|%d", row.AccountID, row.LedgerRange)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		values = append(values, fmt.Sprintf("('%s', %d, %d)", sqlString(row.AccountID), row.AccountBucket, row.LedgerRange))
	}
	if len(values) == 0 {
		return 0, nil
	}

	insertTempSQL := fmt.Sprintf("INSERT INTO temp_account_ledger_index VALUES %s", strings.Join(values, ", "))
	if _, err := iw.db.ExecContext(ctx, insertTempSQL); err != nil {
		return 0, fmt.Errorf("failed to insert account temp rows: %w", err)
	}

	copySQL := fmt.Sprintf("INSERT INTO %s SELECT * FROM temp_account_ledger_index", fullTableName)
	result, err := iw.db.ExecContext(ctx, copySQL)
	if err != nil {
		return 0, fmt.Errorf("failed to copy account rows to DuckLake table: %w", err)
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("failed to get account rows affected: %w", err)
	}
	log.Printf("✅ Inserted %d account ledger-range rows into %s", rowsAffected, fullTableName)
	return rowsAffected, nil
}

func sqlString(value string) string {
	return strings.ReplaceAll(value, "'", "''")
}

// FlushInlinedData consolidates inlined rows from the catalog into Parquet files on S3.
func (iw *IndexWriter) FlushInlinedData() (int64, error) {
	var schema, table string
	var rowsFlushed int64
	query := fmt.Sprintf("CALL ducklake_flush_inlined_data('%s', schema_name => '%s', table_name => '%s')",
		iw.config.CatalogName, iw.config.SchemaName, iw.config.TableName)
	err := iw.db.QueryRow(query).Scan(&schema, &table, &rowsFlushed)
	if err != nil {
		return 0, fmt.Errorf("flush inlined data failed: %w", err)
	}
	if rowsFlushed > 0 {
		log.Printf("✅ Flushed %d inlined rows to Parquet (%s.%s)", rowsFlushed, schema, table)
	}
	return rowsFlushed, nil
}

// GetIndexStats returns statistics about the index
func (iw *IndexWriter) GetIndexStats() (minLedger int64, maxLedger int64, totalTx int64, err error) {
	// Query the DuckLake table
	fullTableName := fmt.Sprintf("%s.%s.%s", iw.config.CatalogName, iw.config.SchemaName, iw.config.TableName)

	query := fmt.Sprintf(`
		SELECT
			COALESCE(MIN(ledger_range), 0),
			COALESCE(MAX(ledger_range), 0),
			COUNT(*)
		FROM %s
	`, fullTableName)

	err = iw.db.QueryRow(query).Scan(&minLedger, &maxLedger, &totalTx)
	if err != nil {
		// If no data exists yet, return zeros (not an error)
		if err.Error() == "sql: no rows in result set" {
			return 0, 0, 0, nil
		}
		return 0, 0, 0, fmt.Errorf("failed to get account index stats: %w", err)
	}

	return minLedger, maxLedger, totalTx, nil
}

// MergeAdjacentFiles merges small Parquet files into larger ones for better query performance
// NOTE: This function MUST NOT be called while the transformer is actively inserting data,
// as it will cause deadlocks. Only call when the transformer is stopped or paused.
//
// With partitioning enabled, this will merge files within each partition.
func (iw *IndexWriter) MergeAdjacentFiles(maxFiles int) error {
	log.Printf("🔧 Starting file merge (max_compacted_files=%d)...", maxFiles)

	// Merge files with partitioning-aware adjacency
	// DuckLake will merge files within each partition
	mergeSQL := fmt.Sprintf(`CALL ducklake_merge_adjacent_files('%s', '%s', schema => '%s', max_compacted_files => %d)`,
		iw.config.CatalogName, iw.config.TableName, iw.config.SchemaName, maxFiles)

	if _, err := iw.db.Exec(mergeSQL); err != nil {
		return fmt.Errorf("failed to merge adjacent files: %w", err)
	}

	log.Println("✅ File merge completed successfully")
	return nil
}

// ExpireSnapshots expires old snapshots to mark merged files for cleanup
// This is a catalog-level operation that expires snapshots older than the specified time
func (iw *IndexWriter) ExpireSnapshots(retainSnapshots int) error {
	log.Printf("🗑️  Expiring old snapshots (catalog-level operation)...")

	// ducklake_expire_snapshots operates at catalog level
	// Parameters: catalog_name, dry_run (default false), versions (default NULL), older_than (default NULL)
	// We'll call it with just the catalog name to use defaults (expires all old snapshots)
	expireSQL := fmt.Sprintf(`CALL ducklake_expire_snapshots('%s')`, iw.config.CatalogName)

	if _, err := iw.db.Exec(expireSQL); err != nil {
		return fmt.Errorf("failed to expire snapshots: %w", err)
	}

	log.Println("✅ Snapshots expired successfully")
	return nil
}

// CleanupOrphanedFiles removes orphaned Parquet files from S3
// This should be called after ExpireSnapshots to actually delete the old files
// Note: This is a catalog-level operation that cleans up all orphaned files
func (iw *IndexWriter) CleanupOrphanedFiles() error {
	log.Println("🧹 Cleaning up orphaned files...")

	// ducklake_cleanup_old_files operates at catalog level
	// Parameters: catalog_name, dry_run (default false), cleanup_all (default false), older_than (default NULL)
	cleanupSQL := fmt.Sprintf(`CALL ducklake_cleanup_old_files('%s')`, iw.config.CatalogName)

	if _, err := iw.db.Exec(cleanupSQL); err != nil {
		return fmt.Errorf("failed to cleanup orphaned files: %w", err)
	}

	log.Println("✅ Orphaned files cleaned up successfully")
	return nil
}

// PerformMaintenanceFullCycle runs merge, expire, and cleanup in sequence
// This is the recommended maintenance workflow for DuckLake tables
// WARNING: MUST be run when transformer is stopped/paused to avoid deadlocks
func (iw *IndexWriter) PerformMaintenanceFullCycle(maxFiles, retainSnapshots int) error {
	log.Println("🔧 Starting full maintenance cycle (merge → expire → cleanup)...")

	// Step 1: Merge small files into larger ones
	if err := iw.MergeAdjacentFiles(maxFiles); err != nil {
		return fmt.Errorf("merge failed: %w", err)
	}

	// Step 2: Expire old snapshots to mark files for deletion
	if err := iw.ExpireSnapshots(retainSnapshots); err != nil {
		return fmt.Errorf("expire snapshots failed: %w", err)
	}

	// Step 3: Actually delete orphaned files from S3
	if err := iw.CleanupOrphanedFiles(); err != nil {
		return fmt.Errorf("cleanup failed: %w", err)
	}

	log.Println("✅ Full maintenance cycle completed successfully")
	return nil
}

// GetFileCount returns the number of Parquet files for this table
func (iw *IndexWriter) GetFileCount() (int64, error) {
	// DuckLake creates metadata tables with __ducklake_metadata_ prefix
	metadataPrefix := fmt.Sprintf("__ducklake_metadata_%s.%s", iw.config.CatalogName, iw.config.SchemaName)

	// Count all files for this table's latest table_id
	// Note: ducklake_manifest_entry doesn't exist in DuckLake metadata schema
	query := fmt.Sprintf(`
		SELECT COUNT(*)
		FROM %s.ducklake_data_file df
		WHERE df.table_id = (
			SELECT table_id
			FROM %s.ducklake_table
			WHERE table_name = '%s'
			ORDER BY table_id DESC
			LIMIT 1
		)
	`, metadataPrefix, metadataPrefix, iw.config.TableName)

	var count int64
	err := iw.db.QueryRow(query).Scan(&count)
	if err != nil {
		log.Printf("⚠️  File count query failed: %v", err)
		return 0, nil
	}

	return count, nil
}

// GetMetadataDebugInfo returns metadata table information for debugging
func (iw *IndexWriter) GetMetadataDebugInfo() (map[string]interface{}, error) {
	metadataSchema := fmt.Sprintf("__ducklake_metadata_%s.%s", iw.config.CatalogName, iw.config.SchemaName)

	// Get table information
	tableQuery := fmt.Sprintf(`
		SELECT table_id, table_name, schema_id, end_snapshot
		FROM %s.ducklake_table
		WHERE table_name = '%s'
		ORDER BY table_id
	`, metadataSchema, iw.config.TableName)

	rows, err := iw.db.Query(tableQuery)
	if err != nil {
		return nil, fmt.Errorf("failed to query table info: %w", err)
	}
	defer rows.Close()

	var tables []map[string]interface{}
	for rows.Next() {
		var tableID int64
		var tableName string
		var schemaID int64
		var endSnapshot sql.NullInt64

		if err := rows.Scan(&tableID, &tableName, &schemaID, &endSnapshot); err != nil {
			return nil, fmt.Errorf("failed to scan table row: %w", err)
		}

		tableInfo := map[string]interface{}{
			"table_id":   tableID,
			"table_name": tableName,
			"schema_id":  schemaID,
		}
		if endSnapshot.Valid {
			tableInfo["end_snapshot"] = endSnapshot.Int64
		} else {
			tableInfo["end_snapshot"] = nil
		}
		tables = append(tables, tableInfo)
	}

	// Get file counts per table_id
	fileCountQuery := fmt.Sprintf(`
		SELECT df.table_id, COUNT(*) as file_count
		FROM %s.ducklake_data_file df
		GROUP BY df.table_id
		ORDER BY df.table_id
	`, metadataSchema)

	rows, err = iw.db.Query(fileCountQuery)
	if err != nil {
		return nil, fmt.Errorf("failed to query file counts: %w", err)
	}
	defer rows.Close()

	var fileCounts []map[string]interface{}
	for rows.Next() {
		var tableID int64
		var fileCount int64

		if err := rows.Scan(&tableID, &fileCount); err != nil {
			return nil, fmt.Errorf("failed to scan file count row: %w", err)
		}

		fileCounts = append(fileCounts, map[string]interface{}{
			"table_id":   tableID,
			"file_count": fileCount,
		})
	}

	return map[string]interface{}{
		"tables":      tables,
		"file_counts": fileCounts,
	}, nil
}

// RunCheckpoint performs automated DuckLake maintenance by merging small files.
//
// IMPORTANT: Does NOT run CHECKPOINT (which expires snapshots and deletes files from S3).
// Cold storage is a permanent append-only archive — files should never be deleted.
func (iw *IndexWriter) RunCheckpoint(ctx context.Context, maxCompactedFiles int) error {
	startTime := time.Now()
	log.Println("🔧 Running DuckLake merge maintenance (merge only, no expire/cleanup)...")

	mergeSQL := fmt.Sprintf(
		`CALL ducklake_merge_adjacent_files('%s', '%s', schema => '%s', max_compacted_files => %d)`,
		iw.config.CatalogName, iw.config.TableName, iw.config.SchemaName, maxCompactedFiles)
	if _, err := iw.db.ExecContext(ctx, mergeSQL); err != nil {
		return fmt.Errorf("merge failed: %w", err)
	}

	log.Printf("✅ DuckLake merge completed in %s", time.Since(startTime).Round(time.Millisecond))
	return nil
}

// Close closes the DuckDB connection
func (iw *IndexWriter) Close() error {
	if iw.db != nil {
		return iw.db.Close()
	}
	return nil
}
