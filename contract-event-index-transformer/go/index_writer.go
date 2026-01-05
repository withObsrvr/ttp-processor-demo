package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"time"

	_ "github.com/duckdb/duckdb-go/v2"
)

// ContractEventIndexRow represents a single row in the contract_events_index table
type ContractEventIndexRow struct {
	ContractID      string
	LedgerSequence  int64
	EventCount      int32
	FirstSeenAt     time.Time
	LedgerRange     int64 // ledger_sequence / 100000
}

// IndexWriter writes contract event index data to DuckLake
type IndexWriter struct {
	db     *sql.DB
	config *Config
}

// NewIndexWriter creates a new Index writer
func NewIndexWriter(config *Config) (*IndexWriter, error) {
	// Open DuckDB connection for writing to Index Plane
	db, err := sql.Open("duckdb", "")
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
	log.Println("🔧 Initializing Contract Event Index Writer (DuckDB with DuckLake)...")

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
	)`, iw.config.S3.AccessKeyID, iw.config.S3.SecretAccessKey, iw.config.S3.Region, iw.config.S3.Endpoint)

	if _, err := iw.db.Exec(secretSQL); err != nil {
		return fmt.Errorf("failed to configure S3: %w", err)
	}

	log.Println("✅ S3 configured")

	// Attach DuckLake catalog
	log.Println("🔗 Attaching DuckLake catalog...")
	catalogPath := iw.config.CatalogPath()
	dataPath := iw.config.DataPath()

	// Use 'index' schema for both data and metadata
	attachSQL := fmt.Sprintf(`ATTACH '%s' AS testnet_catalog (DATA_PATH '%s', METADATA_SCHEMA 'index')`,
		catalogPath, dataPath)

	if _, err := iw.db.Exec(attachSQL); err != nil {
		return fmt.Errorf("failed to attach DuckLake catalog: %w", err)
	}

	log.Println("✅ DuckLake catalog attached")

	// Create Contract Event Index table in DuckLake
	if err := iw.createIndexTable(); err != nil {
		return fmt.Errorf("failed to create index table: %w", err)
	}

	log.Println("✅ Contract Event Index Writer initialized")

	return nil
}

// createIndexTable creates the contract_events_index table in DuckLake catalog
func (iw *IndexWriter) createIndexTable() error {
	log.Println("Creating contract_events_index table in DuckLake catalog...")

	// Create schema first if it doesn't exist
	createSchemaSQL := "CREATE SCHEMA IF NOT EXISTS testnet_catalog.index"

	if _, err := iw.db.Exec(createSchemaSQL); err != nil {
		return fmt.Errorf("failed to create schema in DuckLake: %w", err)
	}

	log.Println("✅ Schema created: testnet_catalog.index")

	// Create table in DuckLake catalog - DuckLake will manage Parquet storage automatically
	fullTableName := "testnet_catalog.index.contract_events_index"

	createTableSQL := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			contract_id VARCHAR,
			ledger_sequence BIGINT,
			event_count INTEGER,
			first_seen_at TIMESTAMP,
			ledger_range BIGINT,
			created_at TIMESTAMP
		)
	`, fullTableName)

	if _, err := iw.db.Exec(createTableSQL); err != nil {
		return fmt.Errorf("failed to create table in DuckLake: %w", err)
	}

	log.Printf("✅ Contract Event Index table created: %s", fullTableName)

	// Set partitioning by ledger_range to organize files
	partitionSQL := fmt.Sprintf(`ALTER TABLE %s SET PARTITIONED BY (ledger_range)`, fullTableName)

	if _, err := iw.db.Exec(partitionSQL); err != nil {
		return fmt.Errorf("failed to set partitioning: %w", err)
	}

	log.Println("✅ Partitioning enabled: ledger_range")
	log.Println("ℹ️  Files will be organized by ledger range (ledger_sequence / 100000)")
	return nil
}

// WriteBatch writes contract event index entries to DuckLake
func (iw *IndexWriter) WriteBatch(ctx context.Context, rows []ContractEventIndexRow) (int64, error) {
	if len(rows) == 0 {
		return 0, nil
	}

	// DuckLake table reference
	fullTableName := "testnet_catalog.index.contract_events_index"

	// Create temp table for bulk insert
	createTempSQL := `
		CREATE TEMP TABLE IF NOT EXISTS temp_contract_index AS
		SELECT * FROM (VALUES
			('', CAST(0 AS BIGINT), CAST(0 AS INTEGER), CAST('1970-01-01' AS TIMESTAMP), CAST(0 AS BIGINT), CAST('1970-01-01' AS TIMESTAMP))
		) AS t(contract_id, ledger_sequence, event_count, first_seen_at, ledger_range, created_at)
		WHERE false
	`

	if _, err := iw.db.Exec(createTempSQL); err != nil {
		return 0, fmt.Errorf("failed to create temp table: %w", err)
	}

	// Clear temp table
	if _, err := iw.db.Exec("DELETE FROM temp_contract_index"); err != nil {
		return 0, fmt.Errorf("failed to clear temp table: %w", err)
	}

	// Build VALUES list for bulk insert
	createdAt := time.Now()
	valuesList := ""
	for i, row := range rows {
		if i > 0 {
			valuesList += ", "
		}
		valuesList += fmt.Sprintf("('%s', %d, %d, '%s', %d, '%s')",
			row.ContractID,
			row.LedgerSequence,
			row.EventCount,
			row.FirstSeenAt.UTC().Format("2006-01-02 15:04:05"),
			row.LedgerRange,
			createdAt.UTC().Format("2006-01-02 15:04:05"),
		)
	}

	// Insert into temp table
	insertTempSQL := fmt.Sprintf("INSERT INTO temp_contract_index VALUES %s", valuesList)
	if _, err := iw.db.Exec(insertTempSQL); err != nil {
		return 0, fmt.Errorf("failed to insert into temp table: %w", err)
	}

	// Copy from temp to DuckLake table
	copySQL := fmt.Sprintf("INSERT INTO %s SELECT * FROM temp_contract_index", fullTableName)
	result, err := iw.db.Exec(copySQL)
	if err != nil {
		return 0, fmt.Errorf("failed to copy to DuckLake table: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("failed to get rows affected: %w", err)
	}

	return rowsAffected, nil
}

// Checkpoint forces DuckDB to flush buffered data to Parquet files on S3
// CRITICAL: Must be called after WriteBatch to ensure data is persisted
func (iw *IndexWriter) Checkpoint() error {
	if _, err := iw.db.Exec("CHECKPOINT"); err != nil {
		return fmt.Errorf("CHECKPOINT failed: %w", err)
	}
	return nil
}

// GetIndexStats returns statistics about the index
func (iw *IndexWriter) GetIndexStats() (map[string]interface{}, error) {
	fullTableName := "testnet_catalog.index.contract_events_index"

	query := fmt.Sprintf(`
		SELECT
			COUNT(*) as total_ledger_pairs,
			COUNT(DISTINCT contract_id) as unique_contracts,
			COALESCE(MIN(ledger_sequence), 0) as min_ledger,
			COALESCE(MAX(ledger_sequence), 0) as max_ledger,
			MAX(created_at) as last_updated
		FROM %s
	`, fullTableName)

	var totalPairs int64
	var uniqueContracts int64
	var minLedger int64
	var maxLedger int64
	var lastUpdated sql.NullTime

	err := iw.db.QueryRow(query).Scan(&totalPairs, &uniqueContracts, &minLedger, &maxLedger, &lastUpdated)
	if err != nil {
		// If no data exists yet, return empty stats
		if err == sql.ErrNoRows {
			return map[string]interface{}{
				"total_contract_ledger_pairs": 0,
				"unique_contracts":             0,
				"min_ledger":                   0,
				"max_ledger":                   0,
				"ledger_coverage":              0,
				"last_updated":                 nil,
			}, nil
		}
		return nil, fmt.Errorf("failed to get index stats: %w", err)
	}

	// If table is empty, return empty stats
	if totalPairs == 0 {
		return map[string]interface{}{
			"total_contract_ledger_pairs": 0,
			"unique_contracts":             0,
			"min_ledger":                   0,
			"max_ledger":                   0,
			"ledger_coverage":              0,
			"last_updated":                 nil,
		}, nil
	}

	// Format last_updated if it's valid
	var lastUpdatedStr interface{}
	if lastUpdated.Valid {
		lastUpdatedStr = lastUpdated.Time.Format(time.RFC3339)
	} else {
		lastUpdatedStr = nil
	}

	return map[string]interface{}{
		"total_contract_ledger_pairs": totalPairs,
		"unique_contracts":             uniqueContracts,
		"min_ledger":                   minLedger,
		"max_ledger":                   maxLedger,
		"ledger_coverage":              maxLedger - minLedger + 1,
		"last_updated":                 lastUpdatedStr,
	}, nil
}

// Close closes the DuckDB connection
func (iw *IndexWriter) Close() error {
	if iw.db != nil {
		return iw.db.Close()
	}
	return nil
}

// MergeAdjacentFiles merges small Parquet files into larger ones for better query performance
func (iw *IndexWriter) MergeAdjacentFiles(maxFiles int) error {
	log.Printf("🔧 Starting file merge (max_compacted_files=%d)...", maxFiles)

	mergeSQL := fmt.Sprintf(`CALL ducklake_merge_adjacent_files('testnet_catalog', 'contract_events_index', schema => 'index', max_compacted_files => %d)`, maxFiles)

	if _, err := iw.db.Exec(mergeSQL); err != nil {
		return fmt.Errorf("failed to merge adjacent files: %w", err)
	}

	log.Println("✅ File merge completed successfully")
	return nil
}

// ExpireSnapshots expires old snapshots to mark merged files for cleanup
// Note: retainSnapshots parameter is currently not used by DuckLake's ducklake_expire_snapshots
// procedure, which uses its own internal retention policy. The parameter is kept for API
// compatibility and potential future DuckLake versions that may support it.
func (iw *IndexWriter) ExpireSnapshots(retainSnapshots int) error {
	log.Printf("🗑️  Expiring old snapshots (catalog-level operation, retain_snapshots=%d requested)...", retainSnapshots)

	// TODO: DuckLake's ducklake_expire_snapshots doesn't currently accept a retain parameter.
	// When DuckLake adds support for this, update the SQL to include the parameter.
	expireSQL := `CALL ducklake_expire_snapshots('testnet_catalog')`

	if _, err := iw.db.Exec(expireSQL); err != nil {
		return fmt.Errorf("failed to expire snapshots: %w", err)
	}

	log.Println("✅ Snapshots expired successfully")
	return nil
}

// CleanupOrphanedFiles removes orphaned Parquet files from S3
func (iw *IndexWriter) CleanupOrphanedFiles() error {
	log.Println("🧹 Cleaning up orphaned files...")

	cleanupSQL := `CALL ducklake_cleanup_old_files('testnet_catalog')`

	if _, err := iw.db.Exec(cleanupSQL); err != nil {
		return fmt.Errorf("failed to cleanup orphaned files: %w", err)
	}

	log.Println("✅ Orphaned files cleaned up successfully")
	return nil
}

// PerformMaintenanceFullCycle runs merge, expire, and cleanup in sequence
func (iw *IndexWriter) PerformMaintenanceFullCycle(maxFiles, retainSnapshots int) error {
	log.Println("🔧 Starting full maintenance cycle (merge → expire → cleanup)...")

	if err := iw.MergeAdjacentFiles(maxFiles); err != nil {
		return fmt.Errorf("merge failed: %w", err)
	}

	if err := iw.ExpireSnapshots(retainSnapshots); err != nil {
		return fmt.Errorf("expire snapshots failed: %w", err)
	}

	if err := iw.CleanupOrphanedFiles(); err != nil {
		return fmt.Errorf("cleanup failed: %w", err)
	}

	log.Println("✅ Full maintenance cycle completed successfully")
	return nil
}

// GetFileCount returns the number of Parquet files for this table
func (iw *IndexWriter) GetFileCount() (int64, error) {
	query := `
		SELECT COUNT(*)
		FROM __ducklake_metadata_testnet_catalog.index.ducklake_data_file df
		WHERE df.table_id = (
			SELECT table_id
			FROM __ducklake_metadata_testnet_catalog.index.ducklake_table
			WHERE table_name = 'contract_events_index'
			ORDER BY table_id DESC
			LIMIT 1
		)
	`

	var count int64
	err := iw.db.QueryRow(query).Scan(&count)
	if err != nil {
		log.Printf("⚠️  File count query failed: %v", err)
		return 0, nil
	}

	return count, nil
}
