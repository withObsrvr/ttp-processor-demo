package main

import (
	"database/sql"
	"fmt"
	"log"

	_ "github.com/marcboeker/go-duckdb"
)

// DuckDBClient manages DuckDB connection and operations
type DuckDBClient struct {
	db     *sql.DB
	config *DuckLakeConfig
}

// NewDuckDBClient creates a new DuckDB client
func NewDuckDBClient(config *DuckLakeConfig) (*DuckDBClient, error) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		return nil, fmt.Errorf("failed to open DuckDB: %w", err)
	}

	client := &DuckDBClient{
		db:     db,
		config: config,
	}

	// Initialize DuckDB with extensions and catalog
	if err := client.initialize(); err != nil {
		db.Close()
		return nil, err
	}

	return client, nil
}

// initialize sets up DuckDB extensions and catalog
func (c *DuckDBClient) initialize() error {
	log.Println("🔧 Initializing DuckDB...")

	// Install required extensions
	log.Println("📦 Installing DuckDB extensions...")
	if _, err := c.db.Exec("INSTALL postgres"); err != nil {
		return fmt.Errorf("failed to install postgres extension: %w", err)
	}
	if _, err := c.db.Exec("LOAD postgres"); err != nil {
		return fmt.Errorf("failed to load postgres extension: %w", err)
	}

	if _, err := c.db.Exec("INSTALL ducklake"); err != nil {
		return fmt.Errorf("failed to install ducklake extension: %w", err)
	}
	if _, err := c.db.Exec("LOAD ducklake"); err != nil {
		return fmt.Errorf("failed to load ducklake extension: %w", err)
	}

	if _, err := c.db.Exec("INSTALL httpfs"); err != nil {
		return fmt.Errorf("failed to install httpfs extension: %w", err)
	}
	if _, err := c.db.Exec("LOAD httpfs"); err != nil {
		return fmt.Errorf("failed to load httpfs extension: %w", err)
	}

	log.Println("✅ Extensions loaded successfully")

	// Configure S3 credentials for DuckLake
	log.Println("🔑 Configuring S3 credentials...")
	s3ConfigQuery := `
		CREATE SECRET IF NOT EXISTS (
			TYPE S3,
			KEY_ID '004755c0f85715d0000000002',
			SECRET 'K004c0t0rlFIuwJpU9p6lQXQqGXS/ao',
			REGION 'us-west-004',
			ENDPOINT 's3.us-west-004.backblazeb2.com',
			URL_STYLE 'path'
		)
	`
	if _, err := c.db.Exec(s3ConfigQuery); err != nil {
		return fmt.Errorf("failed to configure S3 credentials: %w", err)
	}

	// Attach DuckLake catalog
	log.Printf("📂 Attaching DuckLake catalog: %s\n", c.config.CatalogName)
	attachQuery := fmt.Sprintf(`
		ATTACH 'ducklake:postgres:%s'
		AS %s
		(DATA_PATH '%s', METADATA_SCHEMA '%s')
	`, c.config.CatalogPath, c.config.CatalogName, c.config.DataPath, c.config.MetadataSchema)

	if _, err := c.db.Exec(attachQuery); err != nil {
		return fmt.Errorf("failed to attach DuckLake catalog: %w", err)
	}

	log.Println("✅ DuckLake catalog attached successfully")
	return nil
}

// FlushTable flushes a table from PostgreSQL to DuckLake using postgres_scan
func (c *DuckDBClient) FlushTable(tableName string, watermark int64, pgConnStr string) (int64, error) {
	query := fmt.Sprintf(`
		INSERT INTO %s.%s.%s
		SELECT * FROM postgres_scan('%s', 'public', '%s')
		WHERE last_modified_ledger <= %d
	`, c.config.CatalogName, c.config.SchemaName, tableName, pgConnStr, tableName, watermark)

	result, err := c.db.Exec(query)
	if err != nil {
		return 0, fmt.Errorf("failed to flush table %s: %w", tableName, err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("failed to get rows affected for %s: %w", tableName, err)
	}

	return rowsAffected, nil
}

// FlushSnapshotTable flushes a snapshot table (uses ledger_sequence instead of last_modified_ledger)
func (c *DuckDBClient) FlushSnapshotTable(tableName string, watermark int64, pgConnStr string) (int64, error) {
	query := fmt.Sprintf(`
		INSERT INTO %s.%s.%s
		SELECT * FROM postgres_scan('%s', 'public', '%s')
		WHERE ledger_sequence <= %d
	`, c.config.CatalogName, c.config.SchemaName, tableName, pgConnStr, tableName, watermark)

	result, err := c.db.Exec(query)
	if err != nil {
		return 0, fmt.Errorf("failed to flush snapshot table %s: %w", tableName, err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("failed to get rows affected for %s: %w", tableName, err)
	}

	return rowsAffected, nil
}

// VerifyTableExists checks if a table exists in the DuckLake catalog
func (c *DuckDBClient) VerifyTableExists(tableName string) error {
	query := fmt.Sprintf(`
		SELECT COUNT(*) FROM information_schema.tables
		WHERE table_catalog = '%s'
		  AND table_schema = '%s'
		  AND table_name = '%s'
	`, c.config.CatalogName, c.config.SchemaName, tableName)

	var count int
	err := c.db.QueryRow(query).Scan(&count)
	if err != nil {
		return fmt.Errorf("failed to verify table %s: %w", tableName, err)
	}

	if count == 0 {
		return fmt.Errorf("table %s does not exist in %s.%s", tableName, c.config.CatalogName, c.config.SchemaName)
	}

	return nil
}

// Close closes the DuckDB connection
func (c *DuckDBClient) Close() error {
	return c.db.Close()
}
