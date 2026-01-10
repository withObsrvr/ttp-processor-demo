package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
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
	catalogPath := fmt.Sprintf("ducklake:postgres:postgresql://%s:%s@%s:%d/%s?sslmode=require",
		iw.config.CatalogUser, iw.config.CatalogPassword,
		iw.config.CatalogHost, iw.config.CatalogPort, iw.config.CatalogDatabase)

	dataPath := fmt.Sprintf("s3://%s/", iw.config.S3Bucket)

	// Use same schema for both data and metadata (index)
	// This ensures queries looking for metadata in 'index' schema will find it
	attachSQL := fmt.Sprintf(`ATTACH '%s' AS %s (DATA_PATH '%s', METADATA_SCHEMA '%s')`,
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

// createIndexTable creates the tx_hash_index table in DuckLake catalog
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

	// Create the table
	// NOTE: DuckLake doesn't support explicit sort order definition via CLUSTER BY or procedures
	// The sort order is implicitly determined by the order of data insertion
	// Our writes are already ordered by ledger_sequence, so files will be naturally organized
	createTableSQL := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			tx_hash VARCHAR,
			ledger_sequence BIGINT,
			operation_count INTEGER,
			successful BOOLEAN,
			closed_at TIMESTAMP,
			ledger_range BIGINT,
			created_at TIMESTAMP
		)
	`, fullTableName)

	if _, err := iw.db.Exec(createTableSQL); err != nil {
		return fmt.Errorf("failed to create table in DuckLake: %w", err)
	}

	log.Printf("✅ Index Plane table created: %s", fullTableName)

	// Set partitioning by ledger_range to organize files
	// This enables merge_adjacent_files to work properly
	// Partition by ledger_range (which groups ledgers into ranges like 100k-200k, 200k-300k, etc.)
	partitionSQL := fmt.Sprintf(`ALTER TABLE %s SET PARTITIONED BY (ledger_range)`, fullTableName)

	if _, err := iw.db.Exec(partitionSQL); err != nil {
		return fmt.Errorf("failed to set partitioning: %w", err)
	}

	log.Printf("✅ Partitioning enabled: ledger_range")
	log.Println("ℹ️  Files will now be organized by ledger range, enabling efficient merges")
	return nil
}

// DropIndexTable drops the tx_hash_index table (for recreating with new schema)
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

// WriteTransactions writes transaction index entries to Index Plane
func (iw *IndexWriter) WriteTransactions(ctx context.Context, transactions []TransactionIndex) (int64, error) {
	if len(transactions) == 0 {
		return 0, nil
	}

	// DuckLake table reference
	fullTableName := fmt.Sprintf("%s.%s.%s", iw.config.CatalogName, iw.config.SchemaName, iw.config.TableName)

	// Use a temp table approach like silver-cold-flusher
	// First create a temp table and load data there
	createTempSQL := `
		CREATE TEMP TABLE IF NOT EXISTS temp_tx_index AS
		SELECT * FROM (VALUES
			('', CAST(0 AS BIGINT), CAST(0 AS INTEGER), CAST(false AS BOOLEAN), CAST('1970-01-01' AS TIMESTAMP), CAST(0 AS BIGINT), CAST('1970-01-01' AS TIMESTAMP))
		) AS t(tx_hash, ledger_sequence, operation_count, successful, closed_at, ledger_range, created_at)
		WHERE false
	`

	if _, err := iw.db.Exec(createTempSQL); err != nil {
		return 0, fmt.Errorf("failed to create temp table: %w", err)
	}

	// Clear temp table
	if _, err := iw.db.Exec("DELETE FROM temp_tx_index"); err != nil {
		return 0, fmt.Errorf("failed to clear temp table: %w", err)
	}

	// Build VALUES list for bulk insert
	createdAt := time.Now()
	valuesList := ""
	for i, tx := range transactions {
		if i > 0 {
			valuesList += ", "
		}
		valuesList += fmt.Sprintf("('%s', %d, %d, %t, '%s', %d, '%s')",
			tx.TxHash,
			tx.LedgerSequence,
			tx.OperationCount,
			tx.Successful,
			tx.ClosedAt.UTC().Format("2006-01-02 15:04:05"),
			tx.LedgerRange,
			createdAt.UTC().Format("2006-01-02 15:04:05"),
		)
	}

	// Insert into temp table
	insertTempSQL := fmt.Sprintf("INSERT INTO temp_tx_index VALUES %s", valuesList)
	if _, err := iw.db.Exec(insertTempSQL); err != nil {
		return 0, fmt.Errorf("failed to insert into temp table: %w", err)
	}

	// Copy from temp to DuckLake table
	copySQL := fmt.Sprintf("INSERT INTO %s SELECT * FROM temp_tx_index", fullTableName)
	result, err := iw.db.Exec(copySQL)
	if err != nil {
		return 0, fmt.Errorf("failed to copy to DuckLake table: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("failed to get rows affected: %w", err)
	}

	// Force DuckDB to flush buffered data to Parquet files on S3
	// Without this, small batches stay in memory and no files are created
	if _, err := iw.db.Exec("CHECKPOINT"); err != nil {
		log.Printf("⚠️  CHECKPOINT failed (data may be buffered): %v", err)
	}

	log.Printf("✅ Inserted %d transactions into %s (DuckLake manages Parquet storage)", rowsAffected, fullTableName)

	return rowsAffected, nil
}

// GetIndexStats returns statistics about the index
func (iw *IndexWriter) GetIndexStats() (minLedger int64, maxLedger int64, totalTx int64, err error) {
	// Query the DuckLake table
	fullTableName := fmt.Sprintf("%s.%s.%s", iw.config.CatalogName, iw.config.SchemaName, iw.config.TableName)

	query := fmt.Sprintf(`
		SELECT
			COALESCE(MIN(ledger_sequence), 0),
			COALESCE(MAX(ledger_sequence), 0),
			COUNT(*)
		FROM %s
	`, fullTableName)

	err = iw.db.QueryRow(query).Scan(&minLedger, &maxLedger, &totalTx)
	if err != nil {
		// If no data exists yet, return zeros (not an error)
		if err.Error() == "sql: no rows in result set" {
			return 0, 0, 0, nil
		}
		return 0, 0, 0, fmt.Errorf("failed to get index stats: %w", err)
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

// Close closes the DuckDB connection
func (iw *IndexWriter) Close() error {
	if iw.db != nil {
		return iw.db.Close()
	}
	return nil
}
