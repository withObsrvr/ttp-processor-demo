package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"

	_ "github.com/duckdb/duckdb-go/v2"
)

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
	for _, ext := range []string{"ducklake", "httpfs"} {
		if _, err := p.db.ExecContext(ctx, fmt.Sprintf("INSTALL %s; LOAD %s;", ext, ext)); err != nil {
			return fmt.Errorf("load extension %s: %w", ext, err)
		}
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

		// Use explicit column list to avoid positional mismatches when the
		// DuckLake schema has more columns than our Parquet output.
		colSQL := fmt.Sprintf("SELECT column_name FROM (DESCRIBE SELECT * FROM read_parquet('%s'))", parquetGlob)
		colRows, err := p.db.QueryContext(ctx, colSQL)
		var cols []string
		if err == nil {
			for colRows.Next() {
				var col string
				if colRows.Scan(&col) == nil {
					cols = append(cols, col)
				}
			}
			colRows.Close()
		}

		var insertSQL string
		if len(cols) > 0 {
			colList := strings.Join(cols, ", ")
			insertSQL = fmt.Sprintf("INSERT INTO %s (%s) SELECT %s FROM read_parquet('%s')", fullTableName, colList, colList, parquetGlob)
		} else {
			insertSQL = fmt.Sprintf("INSERT INTO %s SELECT * FROM read_parquet('%s')", fullTableName, parquetGlob)
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

// embeddedBronzeSchema is a minimal schema for creating DuckLake tables.
// It only creates tables that the history loader outputs — production should
// use the full v3_bronze_schema.sql from postgres-ducklake-flusher.
const embeddedBronzeSchema = `
CREATE TABLE IF NOT EXISTS bronze.ledgers_row_v2 (
    sequence BIGINT, ledger_hash TEXT, previous_ledger_hash TEXT,
    closed_at TIMESTAMP, protocol_version INTEGER,
    total_coins BIGINT, fee_pool BIGINT, base_fee INTEGER,
    base_reserve INTEGER, max_tx_set_size INTEGER,
    successful_tx_count INTEGER, failed_tx_count INTEGER,
    ingestion_timestamp TIMESTAMP, ledger_range BIGINT,
    transaction_count INTEGER, operation_count INTEGER,
    tx_set_operation_count INTEGER, pipeline_version TEXT
);

CREATE TABLE IF NOT EXISTS bronze.transactions_row_v2 (
    ledger_sequence BIGINT, transaction_hash TEXT, source_account TEXT,
    fee_charged BIGINT, max_fee BIGINT, successful BOOLEAN,
    transaction_result_code TEXT, operation_count INTEGER,
    memo_type TEXT, memo TEXT, created_at TIMESTAMP,
    account_sequence BIGINT, ledger_range BIGINT,
    signatures_count INTEGER, new_account BOOLEAN,
    rent_fee_charged BIGINT,
    soroban_resources_instructions BIGINT,
    soroban_resources_read_bytes BIGINT,
    soroban_resources_write_bytes BIGINT,
    pipeline_version TEXT
);

CREATE TABLE IF NOT EXISTS bronze.operations_row_v2 (
    transaction_hash TEXT, transaction_index INTEGER, operation_index INTEGER,
    ledger_sequence BIGINT, source_account TEXT, op_type INTEGER,
    type_string TEXT, created_at TIMESTAMP, transaction_successful BOOLEAN,
    operation_result_code TEXT, ledger_range BIGINT,
    amount BIGINT, asset TEXT, destination TEXT,
    soroban_operation TEXT, soroban_contract_id TEXT,
    soroban_function TEXT, soroban_arguments_json TEXT,
    contract_calls_json TEXT, max_call_depth INTEGER,
    pipeline_version TEXT
);

CREATE TABLE IF NOT EXISTS bronze.effects_row_v1 (
    ledger_sequence BIGINT, transaction_hash TEXT, operation_index INTEGER,
    effect_index INTEGER, effect_type INTEGER, effect_type_string TEXT,
    account_id TEXT, amount TEXT, asset_code TEXT, asset_issuer TEXT,
    asset_type TEXT, created_at TIMESTAMP, ledger_range BIGINT,
    pipeline_version TEXT
);

CREATE TABLE IF NOT EXISTS bronze.trades_row_v1 (
    ledger_sequence BIGINT, transaction_hash TEXT, operation_index INTEGER,
    trade_index INTEGER, trade_type TEXT, trade_timestamp TIMESTAMP,
    seller_account TEXT, selling_asset_code TEXT, selling_asset_issuer TEXT,
    selling_amount TEXT, buyer_account TEXT, buying_asset_code TEXT,
    buying_asset_issuer TEXT, buying_amount TEXT, price TEXT,
    created_at TIMESTAMP, ledger_range BIGINT, pipeline_version TEXT
);

CREATE TABLE IF NOT EXISTS bronze.accounts_snapshot_v1 (
    account_id TEXT, ledger_sequence BIGINT, closed_at TIMESTAMP,
    balance TEXT, sequence_number BIGINT, num_subentries INTEGER,
    num_sponsoring INTEGER, num_sponsored INTEGER, home_domain TEXT,
    master_weight INTEGER, low_threshold INTEGER, med_threshold INTEGER,
    high_threshold INTEGER, flags INTEGER, auth_required BOOLEAN,
    auth_revocable BOOLEAN, auth_immutable BOOLEAN,
    auth_clawback_enabled BOOLEAN, signers TEXT, sponsor_account TEXT,
    ledger_range BIGINT, pipeline_version TEXT
);

CREATE TABLE IF NOT EXISTS bronze.contract_events_stream_v1 (
    event_id TEXT, contract_id TEXT, ledger_sequence BIGINT,
    transaction_hash TEXT, closed_at TIMESTAMP, event_type TEXT,
    in_successful_contract_call BOOLEAN, topics_json TEXT,
    topics_decoded TEXT, data_xdr TEXT, data_decoded TEXT,
    topic_count INTEGER, topic0_decoded TEXT, topic1_decoded TEXT,
    topic2_decoded TEXT, topic3_decoded TEXT,
    operation_index INTEGER, event_index INTEGER,
    ledger_range BIGINT, pipeline_version TEXT
);

CREATE TABLE IF NOT EXISTS bronze.native_balances_snapshot_v1 (
    account_id TEXT, balance BIGINT, buying_liabilities BIGINT,
    selling_liabilities BIGINT, num_subentries INTEGER,
    num_sponsoring INTEGER, num_sponsored INTEGER,
    sequence_number BIGINT, last_modified_ledger BIGINT,
    ledger_sequence BIGINT, ledger_range BIGINT, pipeline_version TEXT
);

CREATE TABLE IF NOT EXISTS bronze.offers_snapshot_v1 (
    offer_id BIGINT, seller_account TEXT, ledger_sequence BIGINT,
    closed_at TIMESTAMP, selling_asset_type TEXT, selling_asset_code TEXT,
    selling_asset_issuer TEXT, buying_asset_type TEXT, buying_asset_code TEXT,
    buying_asset_issuer TEXT, amount TEXT, price TEXT, flags INTEGER,
    ledger_range BIGINT, pipeline_version TEXT
);

CREATE TABLE IF NOT EXISTS bronze.trustlines_snapshot_v1 (
    account_id TEXT, asset_code TEXT, asset_issuer TEXT, asset_type TEXT,
    balance TEXT, trust_limit TEXT, buying_liabilities TEXT,
    selling_liabilities TEXT, authorized BOOLEAN,
    authorized_to_maintain_liabilities BOOLEAN, clawback_enabled BOOLEAN,
    ledger_sequence BIGINT, ledger_range BIGINT, pipeline_version TEXT
);

CREATE TABLE IF NOT EXISTS bronze.account_signers_snapshot_v1 (
    account_id TEXT, signer TEXT, ledger_sequence BIGINT,
    weight INTEGER, sponsor TEXT, deleted BOOLEAN,
    closed_at TIMESTAMP, ledger_range BIGINT, pipeline_version TEXT
);

CREATE TABLE IF NOT EXISTS bronze.claimable_balances_snapshot_v1 (
    balance_id TEXT, sponsor TEXT, ledger_sequence BIGINT,
    closed_at TIMESTAMP, asset_type TEXT, asset_code TEXT,
    asset_issuer TEXT, amount BIGINT, claimants_count INTEGER,
    flags INTEGER, ledger_range BIGINT, pipeline_version TEXT
);

CREATE TABLE IF NOT EXISTS bronze.liquidity_pools_snapshot_v1 (
    liquidity_pool_id TEXT, ledger_sequence BIGINT, closed_at TIMESTAMP,
    pool_type TEXT, fee INTEGER, trustline_count INTEGER,
    total_pool_shares BIGINT, asset_a_type TEXT, asset_a_code TEXT,
    asset_a_issuer TEXT, asset_a_amount BIGINT, asset_b_type TEXT,
    asset_b_code TEXT, asset_b_issuer TEXT, asset_b_amount BIGINT,
    ledger_range BIGINT, pipeline_version TEXT
);

CREATE TABLE IF NOT EXISTS bronze.config_settings_snapshot_v1 (
    config_setting_id INTEGER, ledger_sequence BIGINT,
    last_modified_ledger INTEGER, deleted BOOLEAN,
    closed_at TIMESTAMP, config_setting_xdr TEXT,
    ledger_range BIGINT, pipeline_version TEXT
);

CREATE TABLE IF NOT EXISTS bronze.ttl_snapshot_v1 (
    key_hash TEXT, ledger_sequence BIGINT, live_until_ledger_seq BIGINT,
    ttl_remaining BIGINT, expired BOOLEAN, last_modified_ledger INTEGER,
    deleted BOOLEAN, closed_at TIMESTAMP, ledger_range BIGINT,
    pipeline_version TEXT
);

CREATE TABLE IF NOT EXISTS bronze.evicted_keys_state_v1 (
    key_hash TEXT, ledger_sequence BIGINT, contract_id TEXT,
    key_type TEXT, durability TEXT, closed_at TIMESTAMP,
    ledger_range BIGINT, pipeline_version TEXT
);

CREATE TABLE IF NOT EXISTS bronze.contract_data_snapshot_v1 (
    contract_id TEXT, ledger_sequence BIGINT, ledger_key_hash TEXT,
    contract_key_type TEXT, contract_durability TEXT,
    asset_code TEXT, asset_issuer TEXT, asset_type TEXT,
    balance_holder TEXT, balance TEXT,
    last_modified_ledger INTEGER, ledger_entry_change INTEGER,
    deleted BOOLEAN, closed_at TIMESTAMP, contract_data_xdr TEXT,
    token_name TEXT, token_symbol TEXT, token_decimals INTEGER,
    ledger_range BIGINT, pipeline_version TEXT
);

CREATE TABLE IF NOT EXISTS bronze.contract_code_snapshot_v1 (
    contract_code_hash TEXT, ledger_key_hash TEXT,
    contract_code_ext_v INTEGER, last_modified_ledger INTEGER,
    ledger_entry_change INTEGER, deleted BOOLEAN,
    closed_at TIMESTAMP, ledger_sequence BIGINT,
    n_instructions BIGINT, n_functions BIGINT, n_globals BIGINT,
    n_table_entries BIGINT, n_types BIGINT, n_data_segments BIGINT,
    n_elem_segments BIGINT, n_imports BIGINT, n_exports BIGINT,
    n_data_segment_bytes BIGINT, ledger_range BIGINT, pipeline_version TEXT
);

CREATE TABLE IF NOT EXISTS bronze.restored_keys_state_v1 (
    key_hash TEXT, ledger_sequence BIGINT, contract_id TEXT,
    key_type TEXT, durability TEXT, restored_from_ledger BIGINT,
    closed_at TIMESTAMP, ledger_range BIGINT, pipeline_version TEXT
);

CREATE TABLE IF NOT EXISTS bronze.contract_creations_v1 (
    contract_id TEXT, creator_address TEXT, wasm_hash TEXT,
    created_ledger BIGINT, created_at TIMESTAMP,
    ledger_range BIGINT, pipeline_version TEXT
)
`
