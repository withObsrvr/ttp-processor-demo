package main

import (
	"database/sql"
	"fmt"
	"log"
	"strings"

	_ "github.com/duckdb/duckdb-go/v2"
)

// DuckDBClient manages DuckDB connection and operations
type DuckDBClient struct {
	db      *sql.DB
	config  *DuckLakeConfig
	flusher *Flusher // Reference to parent Flusher for mutex coordination
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
		(DATA_PATH '%s', METADATA_SCHEMA '%s', DATA_INLINING_ROW_LIMIT 250, AUTOMATIC_MIGRATION TRUE, OVERRIDE_DATA_PATH TRUE)
	`, c.config.CatalogPath, c.config.CatalogName, c.config.DataPath, c.config.MetadataSchema)

	if _, err := c.db.Exec(attachQuery); err != nil {
		errStr := err.Error()
		// If metadata schema doesn't exist (after a full reset), create with TYPE ducklake
		if strings.Contains(errStr, "does not exist") || strings.Contains(errStr, "Cannot open") || strings.Contains(errStr, "No snapshot found") {
			log.Printf("⚠️  DuckLake catalog not found, creating fresh catalog...")
			createAttachQuery := fmt.Sprintf(`
				ATTACH 'ducklake:postgres:%s'
				AS %s
				(TYPE ducklake, DATA_PATH '%s', METADATA_SCHEMA '%s', DATA_INLINING_ROW_LIMIT 250, AUTOMATIC_MIGRATION TRUE, OVERRIDE_DATA_PATH TRUE)
			`, c.config.CatalogPath, c.config.CatalogName, c.config.DataPath, c.config.MetadataSchema)
			if _, err := c.db.Exec(createAttachQuery); err != nil {
				return fmt.Errorf("failed to create and attach DuckLake catalog: %w", err)
			}
			log.Println("✅ Created and attached new DuckLake catalog")
		} else {
			return fmt.Errorf("failed to attach DuckLake catalog: %w", err)
		}
	} else {
		log.Println("✅ DuckLake catalog attached successfully")
	}

	// Create Silver tables if they don't exist
	if err := c.createSilverTables(); err != nil {
		return fmt.Errorf("failed to create Silver tables: %w", err)
	}

	log.Println("✅ DuckDB initialized successfully")
	return nil
}

// describeColumns returns the column names, in order, of a DuckDB relation or query.
func (c *DuckDBClient) describeColumns(target string) ([]string, error) {
	rows, err := c.db.Query("DESCRIBE " + target)
	if err != nil {
		return nil, fmt.Errorf("describe %s: %w", target, err)
	}
	defer rows.Close()

	var cols []string
	for rows.Next() {
		var name string
		var ctype, null, key, def, extra sql.NullString
		if err := rows.Scan(&name, &ctype, &null, &key, &def, &extra); err != nil {
			return nil, fmt.Errorf("scan describe row: %w", err)
		}
		cols = append(cols, name)
	}
	return cols, rows.Err()
}

// flushOverrides maps table -> cold column -> SELECT expression, for the few columns that must be
// computed or cast during flush rather than copied straight from silver_hot.
var flushOverrides = map[string]map[string]string{
	"token_transfers_raw": {
		"amount":       "CAST(amount AS DOUBLE)",
		"ledger_range": "FLOOR(ledger_sequence / 100000)",
	},
}

// buildFlushColumns computes the INSERT column list and matching SELECT expressions for a flush,
// using only columns present in BOTH the cold and hot schemas (matched by name), plus any computed
// overrides. Cold-only columns without an override are omitted (they take their column default), and
// hot-only columns are ignored. This is what keeps flushing correct as the cold schema gains columns
// the hot table doesn't have yet (era_id / version_label / inserted_at / ...). Pure, for testability.
func buildFlushColumns(coldCols, hotCols []string, overrides map[string]string) (insertCols, selectExprs []string) {
	hotSet := make(map[string]bool, len(hotCols))
	for _, h := range hotCols {
		hotSet[h] = true
	}
	for _, col := range coldCols {
		switch {
		case overrides[col] != "":
			insertCols = append(insertCols, col)
			selectExprs = append(selectExprs, overrides[col]+" AS "+col)
		case hotSet[col]:
			insertCols = append(insertCols, col)
			selectExprs = append(selectExprs, col)
		}
	}
	return insertCols, selectExprs
}

// buildIntersectionFlush builds an INSERT copying watermark-bounded rows from silver_hot into the
// cold DuckLake table, projecting only the columns the two schemas share (plus computed overrides).
func (c *DuckDBClient) buildIntersectionFlush(tableName, watermarkCol string, watermark, lastFlushed int64, pgConnStr string) (string, error) {
	coldCols, err := c.describeColumns(fmt.Sprintf("%s.%s.%s", c.config.CatalogName, c.config.SchemaName, tableName))
	if err != nil {
		return "", fmt.Errorf("describe cold %s: %w", tableName, err)
	}
	hotCols, err := c.describeColumns(fmt.Sprintf("SELECT * FROM postgres_scan('%s', 'public', '%s')", pgConnStr, tableName))
	if err != nil {
		return "", fmt.Errorf("describe hot %s: %w", tableName, err)
	}
	insertCols, selectExprs := buildFlushColumns(coldCols, hotCols, flushOverrides[tableName])
	if len(insertCols) == 0 {
		return "", fmt.Errorf("no shared columns between hot and cold for %s", tableName)
	}
	return fmt.Sprintf(`
		INSERT INTO %s.%s.%s (%s)
		SELECT %s
		FROM postgres_scan('%s', 'public', '%s')
		WHERE %s > %d AND %s <= %d
	`, c.config.CatalogName, c.config.SchemaName, tableName,
		strings.Join(insertCols, ", "), strings.Join(selectExprs, ", "),
		pgConnStr, tableName, watermarkCol, lastFlushed, watermarkCol, watermark), nil
}

// execFlush runs a flush INSERT and returns the affected row count.
func (c *DuckDBClient) execFlush(tableName, query string) (int64, error) {
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

// FlushTable flushes a current-state table to DuckLake (watermark column last_modified_ledger).
func (c *DuckDBClient) FlushTable(tableName string, watermark int64, pgConnStr string, lastFlushed int64) (int64, error) {
	query, err := c.buildIntersectionFlush(tableName, "last_modified_ledger", watermark, lastFlushed, pgConnStr)
	if err != nil {
		return 0, err
	}
	return c.execFlush(tableName, query)
}

// FlushSnapshotTable flushes a snapshot/event table to DuckLake (watermark column ledger_sequence).
func (c *DuckDBClient) FlushSnapshotTable(tableName string, watermark int64, pgConnStr string, lastFlushed int64) (int64, error) {
	if tableName == "claimable_balances_snapshot" {
		// Source table does not currently exist in silver_hot; keep this explicit so a future
		// re-enablement is intentional.
		return 0, fmt.Errorf("source table %s does not exist in silver_hot", tableName)
	}
	query, err := c.buildIntersectionFlush(tableName, "ledger_sequence", watermark, lastFlushed, pgConnStr)
	if err != nil {
		return 0, err
	}
	return c.execFlush(tableName, query)
}

// FlushTableWithColumn flushes a table to DuckLake using a custom watermark column.
func (c *DuckDBClient) FlushTableWithColumn(tableName string, watermark int64, pgConnStr string, column string, lastFlushed int64) (int64, error) {
	query, err := c.buildIntersectionFlush(tableName, column, watermark, lastFlushed, pgConnStr)
	if err != nil {
		return 0, err
	}
	return c.execFlush(tableName, query)
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

// SetFlusher sets the reference to the parent Flusher for mutex coordination
func (c *DuckDBClient) SetFlusher(f *Flusher) {
	c.flusher = f
}

// Close closes the DuckDB connection
func (c *DuckDBClient) Close() error {
	return c.db.Close()
}
