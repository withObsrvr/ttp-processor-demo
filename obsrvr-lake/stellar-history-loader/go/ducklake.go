package main

import (
	"context"
	"database/sql"
	_ "embed"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"

	_ "github.com/duckdb/duckdb-go/v2"
)

// embeddedBronzeSchema is the canonical bronze schema, kept in sync with
// postgres-ducklake-flusher/v3_bronze_schema.sql. The file is copied into
// this package and embedded so the loader creates DuckLake tables with the
// exact same column set the streaming flusher expects. Without this match,
// whichever writer ran first would define a different schema and the other
// would get "table has N columns but M values supplied" errors on insert.
//
//go:embed v3_bronze_schema.sql
var embeddedBronzeSchema string

// DuckLakeConfig holds configuration for pushing Parquet to DuckLake.
type DuckLakeConfig struct {
	CatalogDSN      string // PostgreSQL catalog connection string
	DataPath        string // S3/B2 bucket path (e.g., "s3://obsrvr-lake-testnet/")
	MetadataSchema  string // Catalog schema (e.g., "bronze_meta")
	CatalogName     string // DuckLake catalog name (e.g., "lake")
	SchemaName      string // Schema within catalog (e.g., "bronze")
	S3KeyID         string
	S3KeySecret     string
	S3Endpoint      string
	S3Region        string
	BronzeSchemaSQL string // Path to v3_bronze_schema.sql (optional, embedded fallback)
}

// DuckLakePusher pushes local Parquet files into DuckLake.
type DuckLakePusher struct {
	db     *sql.DB
	config DuckLakeConfig
}

func NewDuckLakePusher(config DuckLakeConfig) (*DuckLakePusher, error) {
	if config.CatalogName == "" {
		config.CatalogName = "lake"
	}
	if config.SchemaName == "" {
		config.SchemaName = "bronze"
	}
	if config.MetadataSchema == "" {
		config.MetadataSchema = "bronze_meta"
	}

	db, err := sql.Open("duckdb", "")
	if err != nil {
		return nil, fmt.Errorf("open duckdb: %w", err)
	}

	return &DuckLakePusher{db: db, config: config}, nil
}

func (p *DuckLakePusher) Close() error {
	return p.db.Close()
}

// Push reads local Parquet files and inserts them into DuckLake.
func (p *DuckLakePusher) Push(ctx context.Context, outputDir string) error {
	// Step 1: Load extensions
	log.Println("[DuckLake] Loading extensions...")
	if _, err := p.db.ExecContext(ctx, "INSTALL ducklake FROM core_nightly; LOAD ducklake;"); err != nil {
		return fmt.Errorf("load extension ducklake: %w", err)
	}
	if _, err := p.db.ExecContext(ctx, "INSTALL httpfs; LOAD httpfs;"); err != nil {
		return fmt.Errorf("load extension httpfs: %w", err)
	}

	// Step 2: Configure S3 credentials
	log.Println("[DuckLake] Configuring S3 credentials...")
	endpoint := strings.TrimPrefix(p.config.S3Endpoint, "https://")
	endpoint = strings.TrimPrefix(endpoint, "http://")

	p.db.ExecContext(ctx, "DROP SECRET IF EXISTS __default_s3")
	createSecretSQL := fmt.Sprintf(`
		CREATE SECRET (
			TYPE S3,
			KEY_ID '%s',
			SECRET '%s',
			REGION '%s',
			ENDPOINT '%s',
			URL_STYLE 'path',
			URL_COMPATIBILITY_MODE true
		)
	`, p.config.S3KeyID, p.config.S3KeySecret, p.config.S3Region, endpoint)

	if _, err := p.db.ExecContext(ctx, createSecretSQL); err != nil {
		return fmt.Errorf("create S3 secret: %w", err)
	}

	// Step 3: Attach DuckLake catalog
	log.Printf("[DuckLake] Attaching catalog: %s...", p.config.CatalogName)
	catalogPath := fmt.Sprintf("ducklake:postgres:%s", p.config.CatalogDSN)

	// Try attaching existing catalog first, then create if needed
	attachSQL := fmt.Sprintf(
		"ATTACH '%s' AS %s (DATA_PATH '%s', METADATA_SCHEMA '%s', AUTOMATIC_MIGRATION TRUE, OVERRIDE_DATA_PATH TRUE);",
		catalogPath, p.config.CatalogName, p.config.DataPath, p.config.MetadataSchema)

	if _, err := p.db.ExecContext(ctx, attachSQL); err != nil {
		createAttachSQL := fmt.Sprintf(
			"ATTACH '%s' AS %s (TYPE ducklake, DATA_PATH '%s', METADATA_SCHEMA '%s', AUTOMATIC_MIGRATION TRUE, OVERRIDE_DATA_PATH TRUE);",
			catalogPath, p.config.CatalogName, p.config.DataPath, p.config.MetadataSchema)
		if _, err := p.db.ExecContext(ctx, createAttachSQL); err != nil {
			return fmt.Errorf("attach catalog: %w", err)
		}
		log.Println("[DuckLake] Created new catalog")
	}

	// Step 4: Create schema
	createSchemaSQL := fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s.%s", p.config.CatalogName, p.config.SchemaName)
	if _, err := p.db.ExecContext(ctx, createSchemaSQL); err != nil {
		return fmt.Errorf("create schema: %w", err)
	}

	// Step 5: Create tables
	if err := p.createTables(ctx); err != nil {
		return fmt.Errorf("create tables: %w", err)
	}

	// Step 6: Push each bronze table
	bronzeDir := filepath.Join(outputDir, "bronze")
	entries, err := os.ReadDir(bronzeDir)
	if err != nil {
		return fmt.Errorf("read bronze dir: %w", err)
	}

	totalRows := int64(0)
	for _, entry := range entries {
		if !entry.IsDir() || strings.HasPrefix(entry.Name(), "_meta") {
			continue
		}

		tableName := entry.Name()
		duckTableName := mapToDuckLakeTable(tableName)
		if duckTableName == "" {
			log.Printf("[DuckLake] Skipping unmapped table: %s", tableName)
			continue
		}

		parquetGlob := filepath.Join(bronzeDir, tableName, "**", "*.parquet")
		fullTableName := fmt.Sprintf("%s.%s.%s", p.config.CatalogName, p.config.SchemaName, duckTableName)

		// Get Parquet columns
		parquetColSQL := fmt.Sprintf("SELECT column_name FROM (DESCRIBE SELECT * FROM read_parquet('%s'))", parquetGlob)
		parquetColRows, err := p.db.QueryContext(ctx, parquetColSQL)
		var parquetCols []string
		if err == nil {
			for parquetColRows.Next() {
				var col string
				if parquetColRows.Scan(&col) == nil {
					parquetCols = append(parquetCols, col)
				}
			}
			parquetColRows.Close()
		}

		// Get DuckLake table columns to find the intersection
		// (Parquet may have extra columns the table doesn't, e.g. list types)
		tableColSQL := fmt.Sprintf("SELECT column_name FROM (DESCRIBE %s)", fullTableName)
		tableColRows, err := p.db.QueryContext(ctx, tableColSQL)
		tableColSet := map[string]bool{}
		if err == nil {
			for tableColRows.Next() {
				var col string
				if tableColRows.Scan(&col) == nil {
					tableColSet[col] = true
				}
			}
			tableColRows.Close()
		}

		// Use only columns that exist in BOTH Parquet and DuckLake table
		var cols []string
		if len(tableColSet) > 0 {
			for _, col := range parquetCols {
				if tableColSet[col] {
					cols = append(cols, col)
				}
			}
		} else {
			cols = parquetCols
		}

		var insertSQL string
		if len(cols) > 0 {
			colList := strings.Join(cols, ", ")
			insertSQL = fmt.Sprintf("INSERT INTO %s (%s) SELECT %s FROM read_parquet('%s')", fullTableName, colList, colList, parquetGlob)
		} else {
			insertSQL = fmt.Sprintf("INSERT INTO %s SELECT * FROM read_parquet('%s')", fullTableName, parquetGlob)
		}

		// Delete-before-insert for idempotent multi-push: remove any existing rows
		// in the same ledger range before inserting, so re-pushing the same range
		// doesn't create duplicates.
		seqCol := ledgerSequenceColumn(duckTableName)
		if seqCol != "" {
			rangeSQL := fmt.Sprintf("SELECT MIN(%s), MAX(%s) FROM read_parquet('%s')", seqCol, seqCol, parquetGlob)
			var minSeq, maxSeq sql.NullInt64
			if err := p.db.QueryRowContext(ctx, rangeSQL).Scan(&minSeq, &maxSeq); err == nil && minSeq.Valid && maxSeq.Valid {
				deleteSQL := fmt.Sprintf("DELETE FROM %s WHERE %s BETWEEN %d AND %d",
					fullTableName, seqCol, minSeq.Int64, maxSeq.Int64)
				if delResult, err := p.db.ExecContext(ctx, deleteSQL); err == nil {
					deleted, _ := delResult.RowsAffected()
					if deleted > 0 {
						log.Printf("[DuckLake] %s: deleted %d existing rows in range %d-%d (idempotent push)",
							duckTableName, deleted, minSeq.Int64, maxSeq.Int64)
					}
				}
			}
		}

		log.Printf("[DuckLake] Pushing %s...", duckTableName)
		result, err := p.db.ExecContext(ctx, insertSQL)
		if err != nil {
			log.Printf("[DuckLake] FAILED %s: %v", duckTableName, err)
			continue
		}

		rows, _ := result.RowsAffected()
		totalRows += rows
		log.Printf("[DuckLake] %s: %d rows", duckTableName, rows)
	}

	log.Printf("[DuckLake] Push complete: %d total rows", totalRows)
	return nil
}

// createTables creates DuckLake tables from the bronze schema SQL.
func (p *DuckLakePusher) createTables(ctx context.Context) error {
	schemaSQL := embeddedBronzeSchema
	if p.config.BronzeSchemaSQL != "" {
		if data, err := os.ReadFile(p.config.BronzeSchemaSQL); err == nil {
			schemaSQL = string(data)
		}
	}

	schemaSQL = strings.ReplaceAll(schemaSQL, "bronze.", fmt.Sprintf("%s.%s.", p.config.CatalogName, p.config.SchemaName))

	for _, stmt := range strings.Split(schemaSQL, ";") {
		stmt = strings.TrimSpace(stmt)
		if stmt == "" || strings.HasPrefix(stmt, "--") {
			continue
		}
		if _, err := p.db.ExecContext(ctx, stmt+";"); err != nil {
			log.Printf("[DuckLake] Schema warning: %v", err)
		}
	}
	return nil
}

// mapToDuckLakeTable maps Parquet directory names to DuckLake table names.
func mapToDuckLakeTable(dirName string) string {
	mapping := map[string]string{
		"ledgers":                     "ledgers_row_v2",
		"transactions":               "transactions_row_v2",
		"operations":                 "operations_row_v2",
		"effects":                    "effects_row_v1",
		"trades":                     "trades_row_v1",
		"accounts_snapshot":          "accounts_snapshot_v1",
		"offers_snapshot":            "offers_snapshot_v1",
		"trustlines_snapshot":        "trustlines_snapshot_v1",
		"account_signers_snapshot":   "account_signers_snapshot_v1",
		"claimable_balances_snapshot": "claimable_balances_snapshot_v1",
		"liquidity_pools_snapshot":   "liquidity_pools_snapshot_v1",
		"config_settings":           "config_settings_snapshot_v1",
		"ttl_snapshot":              "ttl_snapshot_v1",
		"evicted_keys":              "evicted_keys_state_v1",
		"contract_events":           "contract_events_stream_v1",
		"contract_data_snapshot":    "contract_data_snapshot_v1",
		"contract_code_snapshot":    "contract_code_snapshot_v1",
		"native_balances":           "native_balances_snapshot_v1",
		"restored_keys":             "restored_keys_state_v1",
		"contract_creations":        "contract_creations_v1",
	}
	return mapping[dirName]
}

// ledgerSequenceColumn returns the ledger sequence column name for a given table.
// Returns empty string for tables without a clear ledger sequence column.
func ledgerSequenceColumn(tableName string) string {
	switch tableName {
	case "ledgers_row_v2":
		return "sequence"
	case "contract_creations_v1":
		return "created_ledger"
	default:
		return "ledger_sequence"
	}
}
