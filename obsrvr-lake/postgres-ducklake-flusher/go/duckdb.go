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
		(DATA_PATH '%s', METADATA_SCHEMA '%s');
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
				(TYPE ducklake, DATA_PATH '%s', METADATA_SCHEMA '%s');
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
func (c *DuckDBClient) FlushTableFromPostgres(ctx context.Context, postgresDSN, tableName string, watermark int64) (int64, error) {
	// Build the INSERT query using postgres_scan
	// INSERT INTO catalog.schema.table
	// SELECT * FROM postgres_scan('dsn', 'public', 'table')
	// WHERE ledger_sequence <= watermark (or sequence for ledgers_row_v2)

	// Determine the sequence column name based on table type
	sequenceColumn := "ledger_sequence"
	if tableName == "ledgers_row_v2" {
		sequenceColumn = "sequence"
	}

	insertSQL := fmt.Sprintf(`
		INSERT INTO %s.%s.%s
		SELECT * FROM postgres_scan('%s', 'public', '%s')
		WHERE %s <= %d;
	`, c.config.CatalogName, c.config.SchemaName, tableName,
		postgresDSN, tableName, sequenceColumn, watermark)

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
