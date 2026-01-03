package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"strings"

	_ "github.com/duckdb/duckdb-go/v2"
)

// DuckDBClient manages the DuckDB connection and DuckLake catalog
type DuckDBClient struct {
	db     *sql.DB
	config *DuckLakeConfig
}

// NewDuckDBClient creates a new DuckDB client with DuckLake catalog attached
func NewDuckDBClient(config *DuckLakeConfig) (*DuckDBClient, error) {
	// Open DuckDB connection (in-memory, stateless)
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

	// Attach the DuckLake catalog (contains both Bronze and Silver schemas)
	// The catalog metadata is in PostgreSQL, data files are in S3/B2
	log.Printf("Attaching DuckLake catalog: %s...", c.config.CatalogName)

	// Note: We use bronze_data_path as the base DATA_PATH
	// Silver data will be written with explicit path overrides
	attachSQL := fmt.Sprintf(`
		ATTACH '%s'
		AS %s
		(DATA_PATH '%s', METADATA_SCHEMA '%s');
	`, c.config.CatalogPath, c.config.CatalogName, c.config.BronzeDataPath, c.config.MetadataSchema)

	_, err := c.db.ExecContext(ctx, attachSQL)
	if err != nil {
		errStr := err.Error()
		// Check if error is due to catalog not existing
		if strings.Contains(errStr, "database does not exist") || strings.Contains(errStr, "Cannot open database") {
			log.Printf("DuckLake catalog does not exist, creating new catalog...")

			// Try to create the catalog by using TYPE ducklake
			createAttachSQL := fmt.Sprintf(`
				ATTACH '%s'
				AS %s
				(TYPE ducklake, DATA_PATH '%s', METADATA_SCHEMA '%s');
			`, c.config.CatalogPath, c.config.CatalogName, c.config.BronzeDataPath, c.config.MetadataSchema)

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

	// Create Silver schema if it doesn't exist
	// Bronze schema should already exist (created by flusher)
	createSilverSchemaSQL := fmt.Sprintf(
		"CREATE SCHEMA IF NOT EXISTS %s.%s",
		c.config.CatalogName,
		c.config.SilverSchema,
	)
	if _, err := c.db.ExecContext(ctx, createSilverSchemaSQL); err != nil {
		return fmt.Errorf("failed to create silver schema: %w", err)
	}

	log.Printf("Silver schema ready: %s.%s", c.config.CatalogName, c.config.SilverSchema)

	log.Println("DuckDB initialized successfully")
	return nil
}

// configureS3 sets up S3/B2 credentials for DuckDB
func (c *DuckDBClient) configureS3(ctx context.Context) error {
	log.Println("Configuring S3 credentials...")

	// Strip https:// prefix from endpoint if present (DuckDB adds it automatically)
	endpoint := c.config.AWSEndpoint
	endpoint = strings.TrimPrefix(endpoint, "https://")
	endpoint = strings.TrimPrefix(endpoint, "http://")

	// Create S3 secret for authentication
	createSecretSQL := fmt.Sprintf(`
		CREATE SECRET IF NOT EXISTS (
			TYPE S3,
			KEY_ID '%s',
			SECRET '%s',
			REGION '%s',
			ENDPOINT '%s',
			URL_STYLE 'path'
		);
	`, c.config.AWSAccessKeyID, c.config.AWSSecretAccessKey, c.config.AWSRegion, endpoint)

	if _, err := c.db.ExecContext(ctx, createSecretSQL); err != nil {
		return fmt.Errorf("failed to create S3 secret: %w", err)
	}

	log.Printf("S3 credentials configured for endpoint: %s", endpoint)
	return nil
}

// ExecuteTransformation executes a SQL transformation query
func (c *DuckDBClient) ExecuteTransformation(ctx context.Context, sqlQuery string) (int64, error) {
	result, err := c.db.ExecContext(ctx, sqlQuery)
	if err != nil {
		return 0, err
	}

	// Get rows affected (for CREATE TABLE AS SELECT, this is row count)
	rowsAffected, _ := result.RowsAffected()
	return rowsAffected, nil
}

// QueryMaxLedgerSequence gets the maximum ledger sequence from Bronze
func (c *DuckDBClient) QueryMaxLedgerSequence(ctx context.Context) (int64, error) {
	query := fmt.Sprintf(
		"SELECT COALESCE(MAX(sequence), 0) FROM %s.%s.ledgers_row_v2",
		c.config.CatalogName,
		c.config.BronzeSchema,
	)

	var maxSeq int64
	err := c.db.QueryRowContext(ctx, query).Scan(&maxSeq)
	if err != nil {
		return 0, fmt.Errorf("failed to query max ledger sequence: %w", err)
	}

	return maxSeq, nil
}

// ListTables queries DuckLake metadata to list all tables in the catalog
func (c *DuckDBClient) ListTables(ctx context.Context) error {
	// DuckLake metadata tables are in __ducklake_metadata_<catalog> prefix
	query := fmt.Sprintf(
		"SELECT schema_id, table_name FROM __ducklake_metadata_%s.%s.ducklake_table ORDER BY schema_id, table_name",
		c.config.CatalogName,
		c.config.MetadataSchema,
	)

	rows, err := c.db.QueryContext(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to query metadata: %w", err)
	}
	defer rows.Close()

	log.Println("📋 Tables in DuckLake catalog:")
	for rows.Next() {
		var schemaID int
		var table string
		if err := rows.Scan(&schemaID, &table); err != nil {
			return fmt.Errorf("failed to scan row: %w", err)
		}
		log.Printf("  - schema %d: %s", schemaID, table)
	}

	return nil
}

// DescribeTable shows the schema of a table by querying one row
func (c *DuckDBClient) DescribeTable(ctx context.Context, schema, table string) error {
	query := fmt.Sprintf("SELECT * FROM %s.%s.%s LIMIT 1", c.config.CatalogName, schema, table)

	rows, err := c.db.QueryContext(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to query table: %w", err)
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return fmt.Errorf("failed to get columns: %w", err)
	}

	log.Printf("📋 Columns in %s.%s:", schema, table)
	for _, col := range cols {
		log.Printf("  - %s", col)
	}

	return nil
}

// Close closes the DuckDB connection
func (c *DuckDBClient) Close() error {
	if c.db != nil {
		return c.db.Close()
	}
	return nil
}
