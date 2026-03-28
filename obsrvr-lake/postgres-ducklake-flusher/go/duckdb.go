package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"strings"

	_ "github.com/duckdb/duckdb-go/v2"
)

// DuckDBClient manages the DuckDB connection and catalog
type DuckDBClient struct {
	db      *sql.DB
	config  *DuckLakeConfig
	flusher *Flusher // Reference to parent Flusher for mutex coordination
}

// NewDuckDBClient creates a new DuckDB client with catalog attached
func NewDuckDBClient(config *DuckLakeConfig) (*DuckDBClient, error) {
	// Open DuckDB connection (in-memory for now, no persistent state needed)
	db, err := sql.Open("duckdb", "")
	if err != nil {
		return nil, fmt.Errorf("failed to open DuckDB: %w", err)
	}

	client := &DuckDBClient{
		db:     db,
		config: config,
	}

	// Initialize DuckDB with required extensions and catalog
	if err := client.initialize(); err != nil {
		db.Close()
		return nil, err
	}

	return client, nil
}

// initialize sets up DuckDB extensions and attaches the catalog
func (c *DuckDBClient) initialize() error {
	ctx := context.Background()

	// Install and load postgres extension (for postgres_scan)
	log.Println("Installing postgres extension...")
	if _, err := c.db.ExecContext(ctx, "INSTALL postgres;"); err != nil {
		return fmt.Errorf("failed to install postgres extension: %w", err)
	}
	if _, err := c.db.ExecContext(ctx, "LOAD postgres;"); err != nil {
		return fmt.Errorf("failed to load postgres extension: %w", err)
	}

	// Install and load ducklake extension
	log.Println("Installing ducklake extension...")
	if _, err := c.db.ExecContext(ctx, "INSTALL ducklake;"); err != nil {
		return fmt.Errorf("failed to install ducklake extension: %w", err)
	}
	if _, err := c.db.ExecContext(ctx, "LOAD ducklake;"); err != nil {
		return fmt.Errorf("failed to load ducklake extension: %w", err)
	}

	// Install and load httpfs extension (for S3 access)
	log.Println("Installing httpfs extension...")
	if _, err := c.db.ExecContext(ctx, "INSTALL httpfs;"); err != nil {
		return fmt.Errorf("failed to install httpfs extension: %w", err)
	}
	if _, err := c.db.ExecContext(ctx, "LOAD httpfs;"); err != nil {
		return fmt.Errorf("failed to load httpfs extension: %w", err)
	}

	// Tune DuckDB for low-memory flushing
	if _, err := c.db.ExecContext(ctx, "SET threads=1"); err != nil {
		return fmt.Errorf("failed to set threads: %w", err)
	}
	if _, err := c.db.ExecContext(ctx, "SET preserve_insertion_order=false"); err != nil {
		return fmt.Errorf("failed to set preserve_insertion_order: %w", err)
	}

	// Configure S3 credentials
	if err := c.configureS3(ctx); err != nil {
		return fmt.Errorf("failed to configure S3: %w", err)
	}

	// Attach the DuckLake catalog (following v3 pattern)
	// Try WITHOUT TYPE ducklake first (for existing catalogs)
	log.Printf("Attaching DuckLake catalog: %s...", c.config.CatalogName)
	attachSQL := fmt.Sprintf(`
		ATTACH '%s'
		AS %s
		(DATA_PATH '%s', METADATA_SCHEMA '%s', AUTOMATIC_MIGRATION TRUE, OVERRIDE_DATA_PATH TRUE);
	`, c.config.CatalogPath, c.config.CatalogName, c.config.DataPath, c.config.MetadataSchema)

	_, err := c.db.ExecContext(ctx, attachSQL)
	if err != nil {
		errStr := err.Error()
		// Check if error is due to database not existing (first-time initialization)
		if strings.Contains(errStr, "database does not exist") || strings.Contains(errStr, "Cannot open database") {
			log.Printf("DuckLake catalog does not exist, creating new catalog...")

			// Try to create the catalog by using TYPE ducklake (allows creation)
			createAttachSQL := fmt.Sprintf(`
				ATTACH '%s'
				AS %s
				(TYPE ducklake, DATA_PATH '%s', METADATA_SCHEMA '%s', AUTOMATIC_MIGRATION TRUE, OVERRIDE_DATA_PATH TRUE);
			`, c.config.CatalogPath, c.config.CatalogName, c.config.DataPath, c.config.MetadataSchema)

			if _, err := c.db.ExecContext(ctx, createAttachSQL); err != nil {
				return fmt.Errorf("failed to create and attach DuckLake catalog: %w", err)
			}
			log.Println("Created and attached new DuckLake catalog")
		} else {
			return fmt.Errorf("failed to attach DuckLake catalog: %w", err)
		}
	} else {
		log.Printf("Attached existing DuckLake catalog: %s", c.config.CatalogName)
	}

	// Create schema if it doesn't exist
	createSchemaSQL := fmt.Sprintf(
		"CREATE SCHEMA IF NOT EXISTS %s.%s",
		c.config.CatalogName,
		c.config.SchemaName,
	)
	if _, err := c.db.ExecContext(ctx, createSchemaSQL); err != nil {
		return fmt.Errorf("failed to create schema: %w", err)
	}

	log.Printf("Schema ready: %s.%s", c.config.CatalogName, c.config.SchemaName)

	// Create Bronze tables if they don't exist
	if err := c.createBronzeTables(ctx); err != nil {
		return fmt.Errorf("failed to create Bronze tables: %w", err)
	}

	log.Println("DuckDB initialized successfully")
	return nil
}

// FlushTableFromPostgres flushes data from PostgreSQL to DuckLake using postgres_scan
func (c *DuckDBClient) FlushTableFromPostgres(ctx context.Context, postgresDSN, tableName string, watermark, lastFlushed int64) (int64, error) {
	// Build the INSERT query using postgres_scan
	// INSERT INTO catalog.schema.table
	// SELECT * FROM postgres_scan('dsn', 'public', 'table')
	// WHERE ledger_sequence <= watermark (or sequence for ledgers_row_v2)

	// Determine the sequence column name based on table type
	sequenceColumn := "ledger_sequence"
	switch tableName {
	case "ledgers_row_v2":
		sequenceColumn = "sequence"
	case "contract_creations_v1":
		sequenceColumn = "created_ledger"
	}

	// Build INSERT query - use explicit column lists for tables where PostgreSQL
	// column order differs from DuckLake schema (due to ALTER TABLE ADD COLUMN
	// appending columns at the end vs schema.sql defining them in logical order)
	insertSQL := c.buildFlushSQL(tableName, postgresDSN, sequenceColumn, lastFlushed, watermark)

	log.Printf("Flushing %s (watermark=%d)...", tableName, watermark)

	result, err := c.db.ExecContext(ctx, insertSQL)
	if err != nil {
		return 0, fmt.Errorf("failed to flush table %s: %w", tableName, err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("failed to get rows affected for %s: %w", tableName, err)
	}

	log.Printf("Flushed %d rows from %s to DuckLake", rowsAffected, tableName)
	return rowsAffected, nil
}

// explicitColumnTables maps table names to their column lists for explicit INSERT/SELECT.
// Required when PostgreSQL column order (from ALTER TABLE ADD COLUMN) differs from
// the DuckLake schema order defined in v3_bronze_schema.sql.
var explicitColumnTables = map[string]string{
	"ledgers_row_v2": `
		sequence, ledger_hash, previous_ledger_hash, closed_at, protocol_version,
		total_coins, fee_pool, base_fee, base_reserve, max_tx_set_size,
		successful_tx_count, failed_tx_count, ingestion_timestamp, ledger_range,
		transaction_count, operation_count, tx_set_operation_count,
		soroban_fee_write1kb, node_id, signature, ledger_header,
		bucket_list_size, live_soroban_state_size, evicted_keys_count,
		soroban_op_count, total_fee_charged, contract_events_count,
		era_id, version_label`,
	"contract_data_snapshot_v1": `
		contract_id, ledger_sequence, ledger_key_hash, contract_key_type, contract_durability,
		asset_code, asset_issuer, asset_type, balance_holder, balance,
		last_modified_ledger, ledger_entry_change, deleted, closed_at,
		contract_data_xdr, created_at, ledger_range,
		token_name, token_symbol, token_decimals,
		era_id, version_label`,
	"contract_events_stream_v1": `
		event_id, contract_id, ledger_sequence, transaction_hash, closed_at,
		event_type, in_successful_contract_call,
		topics_json, topics_decoded, data_xdr, data_decoded, topic_count,
		operation_index, event_index,
		topic0_decoded, topic1_decoded, topic2_decoded, topic3_decoded,
		created_at, ledger_range, era_id, version_label`,
}

// buildFlushSQL constructs the INSERT...SELECT SQL for flushing a table.
// Tables in explicitColumnTables get named columns to avoid positional mismatches;
// all other tables use SELECT *.
func (c *DuckDBClient) buildFlushSQL(tableName, postgresDSN, sequenceColumn string, lastFlushed, watermark int64) string {
	if cols, ok := explicitColumnTables[tableName]; ok {
		return fmt.Sprintf(`
			INSERT INTO %s.%s.%s (%s)
			SELECT %s
			FROM postgres_scan('%s', 'public', '%s')
			WHERE %s > %d AND %s <= %d;
		`, c.config.CatalogName, c.config.SchemaName, tableName, cols,
			cols,
			postgresDSN, tableName, sequenceColumn, lastFlushed, sequenceColumn, watermark)
	}
	return fmt.Sprintf(`
		INSERT INTO %s.%s.%s
		SELECT * FROM postgres_scan('%s', 'public', '%s')
		WHERE %s > %d AND %s <= %d;
	`, c.config.CatalogName, c.config.SchemaName, tableName,
		postgresDSN, tableName, sequenceColumn, lastFlushed, sequenceColumn, watermark)
}

// VerifyTableExists checks if a table exists in the DuckLake catalog
func (c *DuckDBClient) VerifyTableExists(ctx context.Context, tableName string) (bool, error) {
	query := fmt.Sprintf(`
		SELECT COUNT(*) FROM information_schema.tables
		WHERE table_schema = '%s' AND table_name = '%s';
	`, c.config.SchemaName, tableName)

	var count int
	err := c.db.QueryRowContext(ctx, query).Scan(&count)
	if err != nil {
		return false, fmt.Errorf("failed to verify table %s: %w", tableName, err)
	}

	return count > 0, nil
}

// GetTableRowCount returns the row count for a table in DuckLake
func (c *DuckDBClient) GetTableRowCount(ctx context.Context, tableName string) (int64, error) {
	query := fmt.Sprintf("SELECT COUNT(*) FROM %s.%s.%s;",
		c.config.CatalogName, c.config.SchemaName, tableName)

	var count int64
	err := c.db.QueryRowContext(ctx, query).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("failed to count rows in %s: %w", tableName, err)
	}

	return count, nil
}

// SetFlusher sets the reference to the parent Flusher for mutex coordination
func (c *DuckDBClient) SetFlusher(f *Flusher) {
	c.flusher = f
}

// Close closes the DuckDB connection
func (c *DuckDBClient) Close() error {
	if c.db != nil {
		return c.db.Close()
	}
	return nil
}

// configureS3 sets up S3/B2 credentials for DuckDB
func (c *DuckDBClient) configureS3(ctx context.Context) error {
	cfg := c.config

	// Strip https:// prefix from endpoint
	endpoint := strings.TrimPrefix(cfg.AWSEndpoint, "https://")
	endpoint = strings.TrimPrefix(endpoint, "http://")

	log.Println("Configuring S3 credentials...")

	// Drop any existing default S3 secret
	c.db.ExecContext(ctx, "DROP SECRET IF EXISTS __default_s3")

	// Create unnamed S3 secret (becomes default)
	createSecretSQL := fmt.Sprintf(`
		CREATE SECRET (
			TYPE S3,
			KEY_ID '%s',
			SECRET '%s',
			REGION '%s',
			ENDPOINT '%s',
			URL_STYLE 'path'
		)
	`, cfg.AWSAccessKeyID, cfg.AWSSecretAccessKey, cfg.AWSRegion, endpoint)

	if _, err := c.db.ExecContext(ctx, createSecretSQL); err != nil {
		return fmt.Errorf("failed to create S3 secret: %w", err)
	}

	log.Printf("S3 credentials configured for endpoint: %s", endpoint)
	return nil
}
