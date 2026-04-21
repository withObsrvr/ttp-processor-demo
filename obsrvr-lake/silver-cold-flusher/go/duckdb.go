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

	if _, err := c.db.Exec("INSTALL ducklake FROM core_nightly"); err != nil {
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
		(DATA_PATH '%s', METADATA_SCHEMA '%s', DATA_INLINING_ROW_LIMIT 250)
	`, c.config.CatalogPath, c.config.CatalogName, c.config.DataPath, c.config.MetadataSchema)

	if _, err := c.db.Exec(attachQuery); err != nil {
		errStr := err.Error()
		// If metadata schema doesn't exist (after a full reset), create with TYPE ducklake
		if strings.Contains(errStr, "does not exist") || strings.Contains(errStr, "Cannot open") || strings.Contains(errStr, "No snapshot found") {
			log.Printf("⚠️  DuckLake catalog not found, creating fresh catalog...")
			createAttachQuery := fmt.Sprintf(`
				ATTACH 'ducklake:postgres:%s'
				AS %s
				(TYPE ducklake, DATA_PATH '%s', METADATA_SCHEMA '%s', DATA_INLINING_ROW_LIMIT 250)
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

// FlushTable flushes a table from PostgreSQL to DuckLake using postgres_scan
func (c *DuckDBClient) FlushTable(tableName string, watermark int64, pgConnStr string, lastFlushed int64) (int64, error) {
	var query string

	switch tableName {
	case "accounts_current":
		// PostgreSQL silver_hot.accounts_current currently contains additional legacy
		// and derived columns. Use an explicit projection to map into the 24-column
		// DuckLake schema in a stable order.
		query = fmt.Sprintf(`
			INSERT INTO %s.%s.%s
			SELECT
				account_id,
				balance,
				sequence_number,
				num_subentries,
				num_sponsoring,
				num_sponsored,
				home_domain,
				master_weight,
				low_threshold,
				med_threshold,
				high_threshold,
				flags,
				auth_required,
				auth_revocable,
				auth_immutable,
				auth_clawback_enabled,
				signers,
				sponsor_account,
				created_at,
				updated_at,
				last_modified_ledger,
				ledger_range,
				era_id,
				version_label
			FROM postgres_scan('%s', 'public', '%s')
			WHERE last_modified_ledger > %d AND last_modified_ledger <= %d
		`, c.config.CatalogName, c.config.SchemaName, tableName, pgConnStr, tableName, lastFlushed, watermark)
	case "trustlines_current":
		// Versioning columns may be appended at the tail of silver_hot via
		// ALTER TABLE on existing deployments, while fresh schemas can place
		// them before inserted_at/updated_at. Use an explicit projection so
		// hot/cold flushing is stable across both layouts.
		query = fmt.Sprintf(`
			INSERT INTO %s.%s.%s (
				account_id,
				asset_type,
				asset_issuer,
				asset_code,
				liquidity_pool_id,
				balance,
				trust_line_limit,
				buying_liabilities,
				selling_liabilities,
				flags,
				last_modified_ledger,
				ledger_sequence,
				created_at,
				sponsor,
				ledger_range,
				era_id,
				version_label,
				inserted_at,
				updated_at
			)
			SELECT
				account_id,
				asset_type,
				asset_issuer,
				asset_code,
				liquidity_pool_id,
				balance,
				trust_line_limit,
				buying_liabilities,
				selling_liabilities,
				flags,
				last_modified_ledger,
				ledger_sequence,
				created_at,
				sponsor,
				ledger_range,
				era_id,
				version_label,
				inserted_at,
				updated_at
			FROM postgres_scan('%s', 'public', '%s')
			WHERE last_modified_ledger > %d AND last_modified_ledger <= %d
		`, c.config.CatalogName, c.config.SchemaName, tableName, pgConnStr, tableName, lastFlushed, watermark)
	case "offers_current":
		// Same column-order issue as trustlines_current.
		query = fmt.Sprintf(`
			INSERT INTO %s.%s.%s (
				offer_id,
				seller_id,
				selling_asset_type,
				selling_asset_code,
				selling_asset_issuer,
				buying_asset_type,
				buying_asset_code,
				buying_asset_issuer,
				amount,
				price_n,
				price_d,
				price,
				flags,
				last_modified_ledger,
				ledger_sequence,
				created_at,
				sponsor,
				ledger_range,
				era_id,
				version_label,
				inserted_at,
				updated_at
			)
			SELECT
				offer_id,
				seller_id,
				selling_asset_type,
				selling_asset_code,
				selling_asset_issuer,
				buying_asset_type,
				buying_asset_code,
				buying_asset_issuer,
				amount,
				price_n,
				price_d,
				price,
				flags,
				last_modified_ledger,
				ledger_sequence,
				created_at,
				sponsor,
				ledger_range,
				era_id,
				version_label,
				inserted_at,
				updated_at
			FROM postgres_scan('%s', 'public', '%s')
			WHERE last_modified_ledger > %d AND last_modified_ledger <= %d
		`, c.config.CatalogName, c.config.SchemaName, tableName, pgConnStr, tableName, lastFlushed, watermark)
	case "claimable_balances_current":
		// Source table in silver_hot has 15 columns, while DuckLake carries two
		// additional compatibility columns (claimants, asset). Project explicitly.
		query = fmt.Sprintf(`
			INSERT INTO %s.%s.%s
			SELECT
				balance_id,
				NULL AS claimants,
				asset_type,
				asset_code,
				asset_issuer,
				NULL AS asset,
				amount,
				sponsor,
				flags,
				last_modified_ledger,
				ledger_sequence,
				created_at,
				ledger_range,
				inserted_at,
				updated_at,
				claimants_count,
				closed_at
			FROM postgres_scan('%s', 'public', '%s')
			WHERE last_modified_ledger > %d AND last_modified_ledger <= %d
		`, c.config.CatalogName, c.config.SchemaName, tableName, pgConnStr, tableName, lastFlushed, watermark)
	default:
		query = fmt.Sprintf(`
			INSERT INTO %s.%s.%s
			SELECT * FROM postgres_scan('%s', 'public', '%s')
			WHERE last_modified_ledger > %d AND last_modified_ledger <= %d
		`, c.config.CatalogName, c.config.SchemaName, tableName, pgConnStr, tableName, lastFlushed, watermark)
	}

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
func (c *DuckDBClient) FlushSnapshotTable(tableName string, watermark int64, pgConnStr string, lastFlushed int64) (int64, error) {
	var query string

	// token_transfers_raw needs ledger_range computed, explicit column ordering
	// (PG column order differs from DuckLake schema: inserted_at/event_index swapped),
	// and amount cast to DOUBLE to handle arbitrarily large Soroban token amounts on testnet
	if tableName == "token_transfers_raw" {
		query = fmt.Sprintf(`
			INSERT INTO %s.%s.%s
			SELECT timestamp, transaction_hash, ledger_sequence, source_type,
			       from_account, to_account, asset_code, asset_issuer,
			       CAST(amount AS DOUBLE) AS amount,
			       token_contract_id, operation_type, transaction_successful,
			       event_index, inserted_at, FLOOR(ledger_sequence / 100000) AS ledger_range
			FROM postgres_scan('%s', 'public', '%s')
			WHERE ledger_sequence > %d AND ledger_sequence <= %d
		`, c.config.CatalogName, c.config.SchemaName, tableName, pgConnStr, tableName, lastFlushed, watermark)
	} else if tableName == "contract_invocations_raw" {
		query = fmt.Sprintf(`
			INSERT INTO %s.%s.%s (
				ledger_sequence,
				transaction_index,
				operation_index,
				transaction_hash,
				source_account,
				contract_id,
				function_name,
				arguments_json,
				successful,
				closed_at,
				ledger_range,
				inserted_at,
				era_id,
				version_label
			)
			SELECT ledger_sequence, transaction_index, operation_index, transaction_hash,
			       source_account, contract_id, function_name, arguments_json,
			       successful, closed_at, ledger_range, inserted_at,
			       era_id, version_label
			FROM postgres_scan('%s', 'public', '%s')
			WHERE ledger_sequence > %d AND ledger_sequence <= %d
		`, c.config.CatalogName, c.config.SchemaName, tableName, pgConnStr, tableName, lastFlushed, watermark)
	} else if tableName == "claimable_balances_snapshot" {
		// Source table does not currently exist in silver_hot. Keep explicit handling
		// here so future re-enablement is intentional.
		return 0, fmt.Errorf("source table %s does not exist in silver_hot", tableName)
	} else {
		// Other snapshot tables use SELECT * (columns match)
		query = fmt.Sprintf(`
			INSERT INTO %s.%s.%s
			SELECT * FROM postgres_scan('%s', 'public', '%s')
			WHERE ledger_sequence > %d AND ledger_sequence <= %d
		`, c.config.CatalogName, c.config.SchemaName, tableName, pgConnStr, tableName, lastFlushed, watermark)
	}

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

// FlushTableWithColumn flushes a table using a custom watermark column
func (c *DuckDBClient) FlushTableWithColumn(tableName string, watermark int64, pgConnStr string, column string, lastFlushed int64) (int64, error) {
	var query string

	if tableName == "contract_metadata" {
		query = fmt.Sprintf(`
			INSERT INTO %s.%s.%s (
				contract_id,
				creator_address,
				wasm_hash,
				created_ledger,
				created_at,
				inserted_at,
				era_id,
				version_label
			)
			SELECT contract_id, creator_address, wasm_hash, created_ledger,
			       created_at, inserted_at, era_id, version_label
			FROM postgres_scan('%s', 'public', '%s')
			WHERE %s > %d AND %s <= %d
		`, c.config.CatalogName, c.config.SchemaName, tableName, pgConnStr, tableName, column, lastFlushed, column, watermark)
	} else {
		query = fmt.Sprintf(`
			INSERT INTO %s.%s.%s
			SELECT * FROM postgres_scan('%s', 'public', '%s')
			WHERE %s > %d AND %s <= %d
		`, c.config.CatalogName, c.config.SchemaName, tableName, pgConnStr, tableName, column, lastFlushed, column, watermark)
	}

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
