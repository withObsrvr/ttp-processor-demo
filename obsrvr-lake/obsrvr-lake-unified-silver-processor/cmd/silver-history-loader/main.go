package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	_ "github.com/duckdb/duckdb-go/v2"
	"gopkg.in/yaml.v3"
)

type Config struct {
	DuckLake DuckLakeConfig `yaml:"ducklake"`
	S3       S3Config       `yaml:"s3"`
}

type DuckLakeConfig struct {
	CatalogPath    string `yaml:"catalog_path"`
	DataPath       string `yaml:"data_path"`
	CatalogName    string `yaml:"catalog_name"`
	BronzeSchema   string `yaml:"bronze_schema"`
	SilverSchema   string `yaml:"silver_schema"`
	MetadataSchema string `yaml:"metadata_schema"`
	InliningLimit  int    `yaml:"inlining_row_limit"`
}

type S3Config struct {
	KeyID     string `yaml:"key_id"`
	KeySecret string `yaml:"key_secret"`
	Region    string `yaml:"region"`
	Endpoint  string `yaml:"endpoint"`
}

func main() {
	configPath := flag.String("config", "", "Path to config YAML")
	startLedger := flag.Int64("start", 0, "Start ledger sequence")
	endLedger := flag.Int64("end", 0, "End ledger sequence")
	outputDir := flag.String("output", "/tmp/silver-history", "Output directory for Parquet files")
	pushOnly := flag.Bool("push-only", false, "Skip extraction, push existing Parquet to DuckLake")
	flag.Parse()

	if *startLedger == 0 || *endLedger == 0 {
		fmt.Fprintf(os.Stderr, "Usage: silver-history-loader --config config.yaml --start 3 --end 200000\n")
		os.Exit(1)
	}

	cfg, err := loadConfig(*configPath)
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	log.Printf("Silver History Loader: ledgers %d to %d", *startLedger, *endLedger)
	totalStart := time.Now()

	ctx := context.Background()

	if !*pushOnly {
		// Phase 1: Read from bronze DuckLake, write to local Parquet
		log.Println("=== Phase 1: Bronze → Local Parquet ===")
		if err := extractToParquet(ctx, cfg, *startLedger, *endLedger, *outputDir); err != nil {
			log.Fatalf("Phase 1 failed: %v", err)
		}
	}

	// Phase 2: Push local Parquet to DuckLake silver
	log.Println("=== Phase 2: Local Parquet → DuckLake Silver ===")
	if err := pushToDuckLake(ctx, cfg, *outputDir); err != nil {
		log.Fatalf("Phase 2 failed: %v", err)
	}

	log.Printf("Complete in %s", time.Since(totalStart).Round(time.Second))
}

// ---------------------------------------------------------------------------
// Phase 1: Extract bronze → local Parquet files
// ---------------------------------------------------------------------------

func extractToParquet(ctx context.Context, cfg *Config, startSeq, endSeq int64, outputDir string) error {
	db, err := initDuckLake(ctx, cfg)
	if err != nil {
		return fmt.Errorf("init ducklake: %w", err)
	}
	defer db.Close()

	os.MkdirAll(outputDir, 0755)

	cat := cfg.DuckLake.CatalogName
	bronze := cfg.DuckLake.BronzeSchema

	transforms := []struct {
		name  string
		query string
	}{
		{"enriched_history_operations", enrichedOpsSQL(cat, bronze, startSeq, endSeq)},
		{"enriched_history_operations_soroban", enrichedOpsSorobanSQL(cat, bronze, startSeq, endSeq)},
		{"token_transfers_raw", tokenTransfersClassicSQL(cat, bronze, startSeq, endSeq)},
		{"token_transfers_soroban", tokenTransfersSorobanSQL(cat, bronze, startSeq, endSeq)},
		{"contract_invocations_raw", contractInvocationsSQL(cat, bronze, startSeq, endSeq)},
		{"contract_metadata", contractMetadataSQL(cat, bronze, startSeq, endSeq)},
		{"trades", tradesSQL(cat, bronze, startSeq, endSeq)},
		{"effects", effectsSQL(cat, bronze, startSeq, endSeq)},
		{"evicted_keys", evictedKeysSQL(cat, bronze, startSeq, endSeq)},
		{"restored_keys", restoredKeysSQL(cat, bronze, startSeq, endSeq)},
		{"accounts_current", accountsCurrentSQL(cat, bronze, startSeq, endSeq)},
		{"trustlines_current", trustlinesCurrentSQL(cat, bronze, startSeq, endSeq)},
		{"offers_current", offersCurrentSQL(cat, bronze, startSeq, endSeq)},
		{"claimable_balances_current", claimableBalancesCurrentSQL(cat, bronze, startSeq, endSeq)},
		{"liquidity_pools_current", liquidityPoolsCurrentSQL(cat, bronze, startSeq, endSeq)},
		{"native_balances_current", nativeBalancesCurrentSQL(cat, bronze, startSeq, endSeq)},
		{"contract_data_current", contractDataCurrentSQL(cat, bronze, startSeq, endSeq)},
		{"contract_code_current", contractCodeCurrentSQL(cat, bronze, startSeq, endSeq)},
		{"ttl_current", ttlCurrentSQL(cat, bronze, startSeq, endSeq)},
		{"config_settings_current", configSettingsCurrentSQL(cat, bronze, startSeq, endSeq)},
		{"token_registry", tokenRegistrySQL(cat, bronze, startSeq, endSeq)},
		{"accounts_snapshot", accountsSnapshotSQL(cat, bronze, startSeq, endSeq)},
		{"trustlines_snapshot", trustlinesSnapshotSQL(cat, bronze, startSeq, endSeq)},
		{"offers_snapshot", offersSnapshotSQL(cat, bronze, startSeq, endSeq)},
		{"account_signers_snapshot", accountSignersSnapshotSQL(cat, bronze, startSeq, endSeq)},
	}

	totalRows := int64(0)
	for _, t := range transforms {
		parquetPath := filepath.Join(outputDir, t.name+".parquet")
		copySQL := fmt.Sprintf("COPY (%s) TO '%s' (FORMAT PARQUET, COMPRESSION ZSTD)", t.query, parquetPath)

		startTime := time.Now()
		_, err := db.ExecContext(ctx, copySQL)
		if err != nil {
			log.Printf("  WARNING: %s: %v", t.name, err)
			continue
		}

		// Count rows
		var count int64
		if err := db.QueryRowContext(ctx, fmt.Sprintf("SELECT COUNT(*) FROM read_parquet('%s')", parquetPath)).Scan(&count); err == nil {
			totalRows += count
			log.Printf("  %s: %d rows in %s", t.name, count, time.Since(startTime).Round(time.Millisecond))
		}
	}

	// Phase B: Semantic transforms (read from the Parquet files we just wrote)
	log.Println("  --- Semantic transforms (from local Parquet) ---")

	semanticTransforms := []struct {
		name  string
		query string
	}{
		{"semantic_activities", semanticActivitiesSQL(outputDir)},
		{"semantic_flows_value", semanticFlowsSQL(outputDir)},
		{"semantic_entities_contracts", semanticEntitiesSQL(outputDir)},
		{"semantic_contract_functions", semanticContractFunctionsSQL(outputDir)},
		{"semantic_asset_stats", semanticAssetStatsSQL(outputDir)},
		{"semantic_dex_pairs", semanticDexPairsSQL(outputDir)},
		{"semantic_account_summary", semanticAccountSummarySQL(outputDir)},
	}

	for _, t := range semanticTransforms {
		parquetPath := filepath.Join(outputDir, t.name+".parquet")
		copySQL := fmt.Sprintf("COPY (%s) TO '%s' (FORMAT PARQUET, COMPRESSION ZSTD)", t.query, parquetPath)

		startTime := time.Now()
		_, err := db.ExecContext(ctx, copySQL)
		if err != nil {
			log.Printf("  WARNING: %s: %v", t.name, err)
			continue
		}

		var count int64
		if err := db.QueryRowContext(ctx, fmt.Sprintf("SELECT COUNT(*) FROM read_parquet('%s')", parquetPath)).Scan(&count); err == nil {
			totalRows += count
			log.Printf("  %s: %d rows in %s", t.name, count, time.Since(startTime).Round(time.Millisecond))
		}
	}

	log.Printf("Phase 1 complete: %d total rows extracted", totalRows)
	return nil
}

// ---------------------------------------------------------------------------
// Phase 2: Push local Parquet to DuckLake silver schema
// ---------------------------------------------------------------------------

func pushToDuckLake(ctx context.Context, cfg *Config, outputDir string) error {
	db, err := initDuckLake(ctx, cfg)
	if err != nil {
		return fmt.Errorf("init ducklake: %w", err)
	}
	defer db.Close()

	// Disable data inlining for bulk push — write directly to Parquet on B2
	// instead of storing in PostgreSQL metadata tables. Much faster for large loads.
	db.ExecContext(ctx, "SET ducklake_default_data_inlining_row_limit = 0")
	db.ExecContext(ctx, "SET ducklake_max_retry_count = 100")

	cat := cfg.DuckLake.CatalogName
	silver := cfg.DuckLake.SilverSchema

	// Map Parquet file names to silver table names
	tableMap := map[string]string{
		"enriched_history_operations":        "enriched_history_operations",
		"enriched_history_operations_soroban": "enriched_history_operations_soroban",
		"token_transfers_raw":                "token_transfers_raw",
		"token_transfers_soroban":            "token_transfers_raw", // appends to same table
		"contract_invocations_raw":           "contract_invocations_raw",
		"contract_metadata":                  "contract_metadata",
		"trades":                             "trades",
		"effects":                            "effects",
		"evicted_keys":                       "evicted_keys",
		"restored_keys":                      "restored_keys",
		"accounts_current":                   "accounts_current",
		"trustlines_current":                 "trustlines_current",
		"offers_current":                     "offers_current",
		"claimable_balances_current":         "claimable_balances_current",
		"liquidity_pools_current":            "liquidity_pools_current",
		"native_balances_current":            "native_balances_current",
		"contract_data_current":              "contract_data_current",
		"contract_code_current":              "contract_code_current",
		"ttl_current":                        "ttl_current",
		"config_settings_current":            "config_settings_current",
		"token_registry":                     "token_registry",
		"accounts_snapshot":                  "accounts_snapshot",
		"trustlines_snapshot":                "trustlines_snapshot",
		"offers_snapshot":                    "offers_snapshot",
		"account_signers_snapshot":           "account_signers_snapshot",
		"semantic_activities":                "semantic_activities",
		"semantic_flows_value":               "semantic_flows_value",
		"semantic_entities_contracts":        "semantic_entities_contracts",
		"semantic_contract_functions":        "semantic_contract_functions",
		"semantic_asset_stats":               "semantic_asset_stats",
		"semantic_dex_pairs":                 "semantic_dex_pairs",
		"semantic_account_summary":           "semantic_account_summary",
	}

	totalRows := int64(0)
	for parquetName, tableName := range tableMap {
		parquetPath := filepath.Join(outputDir, parquetName+".parquet")
		if _, err := os.Stat(parquetPath); os.IsNotExist(err) {
			continue
		}

		fullTable := fmt.Sprintf("%s.%s.%s", cat, silver, tableName)

		// Column intersection (same as bronze history loader)
		parquetCols := getParquetColumns(ctx, db, parquetPath)
		tableCols := getTableColumns(ctx, db, fullTable)

		var cols []string
		for _, col := range parquetCols {
			if tableCols[col] {
				cols = append(cols, col)
			}
		}

		if len(cols) == 0 {
			log.Printf("  WARNING: %s: no column intersection", tableName)
			continue
		}

		colList := strings.Join(cols, ", ")
		insertSQL := fmt.Sprintf("INSERT INTO %s (%s) SELECT %s FROM read_parquet('%s')",
			fullTable, colList, colList, parquetPath)

		startTime := time.Now()
		result, err := db.ExecContext(ctx, insertSQL)
		if err != nil {
			log.Printf("  FAILED: %s: %v", tableName, err)
			continue
		}

		rows, _ := result.RowsAffected()
		totalRows += rows
		log.Printf("  %s: %d rows in %s", tableName, rows, time.Since(startTime).Round(time.Millisecond))
	}

	log.Printf("Phase 2 complete: %d total rows pushed to DuckLake", totalRows)
	return nil
}

func getParquetColumns(ctx context.Context, db *sql.DB, path string) []string {
	rows, err := db.QueryContext(ctx, fmt.Sprintf("SELECT column_name FROM (DESCRIBE SELECT * FROM read_parquet('%s'))", path))
	if err != nil {
		return nil
	}
	defer rows.Close()
	var cols []string
	for rows.Next() {
		var col string
		if rows.Scan(&col) == nil {
			cols = append(cols, col)
		}
	}
	return cols
}

func getTableColumns(ctx context.Context, db *sql.DB, table string) map[string]bool {
	rows, err := db.QueryContext(ctx, fmt.Sprintf("SELECT column_name FROM (DESCRIBE %s)", table))
	if err != nil {
		return nil
	}
	defer rows.Close()
	cols := map[string]bool{}
	for rows.Next() {
		var col string
		if rows.Scan(&col) == nil {
			cols[col] = true
		}
	}
	return cols
}

// ---------------------------------------------------------------------------
// Phase A: Bronze → Silver SELECT queries (output to Parquet)
// These read from DuckLake bronze and produce silver-shaped rows.
// ---------------------------------------------------------------------------

func enrichedOpsSQL(cat, bronze string, start, end int64) string {
	return fmt.Sprintf(`
		SELECT
			o.transaction_hash, o.operation_index, o.ledger_sequence,
			o.source_account, o.type, o.type_string, o.created_at,
			o.transaction_successful, o.operation_result_code, o.operation_trace_code,
			o.ledger_range, o.source_account_muxed,
			o.asset, o.asset_type, o.asset_code, o.asset_issuer,
			o.source_asset, o.source_asset_type, o.source_asset_code, o.source_asset_issuer,
			o.destination, o.amount, o.source_amount, o.destination_min,
			o.starting_balance, o.trustline_limit AS limit_amount,
			o.offer_id,
			o.selling_asset, o.selling_asset_type, o.selling_asset_code, o.selling_asset_issuer,
			o.buying_asset, o.buying_asset_type, o.buying_asset_code, o.buying_asset_issuer,
			TRY_CAST(o.price AS DOUBLE) AS price,
			o.home_domain,
			o.set_flags, o.clear_flags,
			o.master_weight AS master_key_weight,
			o.low_threshold, o.medium_threshold AS med_threshold, o.high_threshold,
			o.data_name, o.data_value,
			o.soroban_operation AS host_function_type,
			o.soroban_contract_id AS contract_id,
			o.soroban_function AS function_name,
			o.balance_id, o.sponsored_id,
			o.trustor, o.bump_to,
			t.successful AS tx_successful,
			t.fee_charged AS tx_fee_charged,
			t.max_fee AS tx_max_fee,
			t.operation_count AS tx_operation_count,
			t.memo_type AS tx_memo_type,
			t.memo AS tx_memo,
			o.created_at AS ledger_closed_at,
			CASE WHEN o.type IN (1, 2, 13) THEN TRUE ELSE FALSE END AS is_payment_op,
			CASE WHEN o.type = 24 THEN TRUE ELSE FALSE END AS is_soroban_op,
			'silver-history-loader' AS era_id,
			'v1.0.0' AS version_label
		FROM %[1]s.%[2]s.operations_row_v2 o
		LEFT JOIN %[1]s.%[2]s.transactions_row_v2 t
			ON o.transaction_hash = t.transaction_hash AND o.ledger_sequence = t.ledger_sequence
		WHERE o.ledger_sequence BETWEEN %[3]d AND %[4]d
	`, cat, bronze, start, end)
}

func enrichedOpsSorobanSQL(cat, bronze string, start, end int64) string {
	return fmt.Sprintf(`
		SELECT
			o.transaction_hash, o.operation_index, o.ledger_sequence,
			o.source_account, o.type, o.type_string, o.created_at,
			o.transaction_successful, o.operation_result_code, o.ledger_range,
			o.soroban_operation AS host_function_type,
			o.soroban_contract_id AS contract_id,
			o.soroban_function AS function_name,
			o.soroban_arguments_json,
			o.contract_calls_json, o.max_call_depth,
			t.fee_charged AS tx_fee_charged,
			TRUE AS is_soroban_op,
			'silver-history-loader' AS era_id,
			'v1.0.0' AS version_label
		FROM %[1]s.%[2]s.operations_row_v2 o
		LEFT JOIN %[1]s.%[2]s.transactions_row_v2 t
			ON o.transaction_hash = t.transaction_hash AND o.ledger_sequence = t.ledger_sequence
		WHERE o.ledger_sequence BETWEEN %[3]d AND %[4]d AND o.type = 24
	`, cat, bronze, start, end)
}

func tokenTransfersClassicSQL(cat, bronze string, start, end int64) string {
	return fmt.Sprintf(`
		SELECT
			o.created_at AS timestamp, o.transaction_hash, o.ledger_sequence,
			'classic' AS source_type,
			o.source_account AS from_account, o.destination AS to_account,
			o.asset_code, o.asset_issuer,
			CAST(o.amount AS DOUBLE) / 10000000.0 AS amount,
			o.type AS operation_type, o.transaction_successful
		FROM %[1]s.%[2]s.operations_row_v2 o
		WHERE o.ledger_sequence BETWEEN %[3]d AND %[4]d
			AND o.type IN (1, 2, 13) AND o.amount IS NOT NULL
	`, cat, bronze, start, end)
}

func tokenTransfersSorobanSQL(cat, bronze string, start, end int64) string {
	return fmt.Sprintf(`
		SELECT
			e.closed_at AS timestamp, e.transaction_hash, e.ledger_sequence,
			'soroban' AS source_type,
			e.contract_id AS token_contract_id,
			e.event_index, e.in_successful_contract_call AS transaction_successful
		FROM %[1]s.%[2]s.contract_events_stream_v1 e
		WHERE e.ledger_sequence BETWEEN %[3]d AND %[4]d
			AND e.event_type = 'contract'
			AND (e.topic0_decoded LIKE '%%transfer%%'
				OR e.topic0_decoded LIKE '%%mint%%'
				OR e.topic0_decoded LIKE '%%burn%%')
	`, cat, bronze, start, end)
}

func contractInvocationsSQL(cat, bronze string, start, end int64) string {
	return fmt.Sprintf(`
		SELECT
			o.ledger_sequence, o.transaction_index, o.operation_index, o.transaction_hash,
			o.source_account, o.soroban_contract_id AS contract_id,
			o.soroban_function AS function_name, o.soroban_arguments_json AS arguments_json,
			o.transaction_successful AS successful, o.created_at AS closed_at,
			o.ledger_range, 'silver-history-loader' AS era_id, 'v1.0.0' AS version_label
		FROM %[1]s.%[2]s.operations_row_v2 o
		WHERE o.ledger_sequence BETWEEN %[3]d AND %[4]d
			AND o.type = 24 AND o.soroban_contract_id IS NOT NULL
	`, cat, bronze, start, end)
}

func contractMetadataSQL(cat, bronze string, start, end int64) string {
	return fmt.Sprintf(`
		SELECT contract_id, creator_address, wasm_hash, created_ledger, created_at
		FROM %[1]s.%[2]s.contract_creations_v1
		WHERE created_ledger BETWEEN %[3]d AND %[4]d
	`, cat, bronze, start, end)
}

func tradesSQL(cat, bronze string, start, end int64) string {
	return fmt.Sprintf(`
		SELECT
			ledger_sequence, transaction_hash, operation_index, trade_index,
			trade_type, trade_timestamp, seller_account,
			selling_asset_code, selling_asset_issuer,
			TRY_CAST(selling_amount AS BIGINT) AS selling_amount,
			buyer_account, buying_asset_code, buying_asset_issuer,
			TRY_CAST(buying_amount AS BIGINT) AS buying_amount,
			TRY_CAST(price AS DOUBLE) AS price,
			created_at, ledger_range,
			'silver-history-loader' AS era_id, 'v1.0.0' AS version_label
		FROM %[1]s.%[2]s.trades_row_v1
		WHERE ledger_sequence BETWEEN %[3]d AND %[4]d
	`, cat, bronze, start, end)
}

func effectsSQL(cat, bronze string, start, end int64) string {
	return fmt.Sprintf(`
		SELECT
			ledger_sequence, transaction_hash, operation_index, effect_index,
			effect_type, effect_type_string, account_id,
			amount, asset_code, asset_issuer, asset_type,
			trustline_limit, authorize_flag, clawback_flag,
			signer_account, signer_weight, offer_id, seller_account,
			created_at, ledger_range,
			'silver-history-loader' AS era_id, 'v1.0.0' AS version_label
		FROM %[1]s.%[2]s.effects_row_v1
		WHERE ledger_sequence BETWEEN %[3]d AND %[4]d
	`, cat, bronze, start, end)
}

func evictedKeysSQL(cat, bronze string, start, end int64) string {
	return fmt.Sprintf(`
		SELECT contract_id, key_hash, ledger_sequence, closed_at, created_at, ledger_range,
			'silver-history-loader' AS era_id, 'v1.0.0' AS version_label
		FROM %[1]s.%[2]s.evicted_keys_state_v1
		WHERE ledger_sequence BETWEEN %[3]d AND %[4]d
	`, cat, bronze, start, end)
}

func restoredKeysSQL(cat, bronze string, start, end int64) string {
	return fmt.Sprintf(`
		SELECT contract_id, key_hash, ledger_sequence, closed_at, created_at, ledger_range,
			'silver-history-loader' AS era_id, 'v1.0.0' AS version_label
		FROM %[1]s.%[2]s.restored_keys_state_v1
		WHERE ledger_sequence BETWEEN %[3]d AND %[4]d
	`, cat, bronze, start, end)
}

func accountsCurrentSQL(cat, bronze string, start, end int64) string {
	return fmt.Sprintf(`
		SELECT DISTINCT ON (account_id)
			account_id, CAST(balance AS BIGINT) AS balance, sequence_number, num_subentries,
			flags, home_domain, master_weight AS master_weight,
			low_threshold AS threshold_low, med_threshold AS threshold_medium,
			high_threshold AS threshold_high,
			sponsor_account, num_sponsored, num_sponsoring,
			ledger_sequence, closed_at, ledger_range,
			updated_at AS updated_at,
			'silver-history-loader' AS era_id, 'v1.0.0' AS version_label
		FROM %[1]s.%[2]s.accounts_snapshot_v1
		WHERE ledger_sequence BETWEEN %[3]d AND %[4]d
		ORDER BY account_id, ledger_sequence DESC
	`, cat, bronze, start, end)
}

func trustlinesCurrentSQL(cat, bronze string, start, end int64) string {
	return fmt.Sprintf(`
		SELECT DISTINCT ON (account_id, asset_code, asset_issuer)
			account_id, asset_type, asset_issuer, asset_code,
			CAST(balance AS BIGINT) AS balance,
			CAST(trust_limit AS BIGINT) AS trust_line_limit,
			CAST(buying_liabilities AS BIGINT) AS buying_liabilities,
			CAST(selling_liabilities AS BIGINT) AS selling_liabilities,
			ledger_sequence, created_at, ledger_range,
			'silver-history-loader' AS era_id, 'v1.0.0' AS version_label
		FROM %[1]s.%[2]s.trustlines_snapshot_v1
		WHERE ledger_sequence BETWEEN %[3]d AND %[4]d
		ORDER BY account_id, asset_code, asset_issuer, ledger_sequence DESC
	`, cat, bronze, start, end)
}

func offersCurrentSQL(cat, bronze string, start, end int64) string {
	return fmt.Sprintf(`
		SELECT DISTINCT ON (offer_id)
			offer_id, seller_account AS seller_id,
			selling_asset_type, selling_asset_code, selling_asset_issuer,
			buying_asset_type, buying_asset_code, buying_asset_issuer,
			CAST(amount AS BIGINT) AS amount,
			TRY_CAST(price AS DOUBLE) AS price,
			flags, ledger_sequence, created_at, ledger_range,
			'silver-history-loader' AS era_id, 'v1.0.0' AS version_label
		FROM %[1]s.%[2]s.offers_snapshot_v1
		WHERE ledger_sequence BETWEEN %[3]d AND %[4]d
		ORDER BY offer_id, ledger_sequence DESC
	`, cat, bronze, start, end)
}

func claimableBalancesCurrentSQL(cat, bronze string, start, end int64) string {
	return fmt.Sprintf(`
		SELECT DISTINCT ON (balance_id)
			balance_id, sponsor, asset_type, asset_code, asset_issuer,
			amount, claimants_count, flags, ledger_sequence,
			closed_at, created_at, ledger_range,
			'silver-history-loader' AS era_id, 'v1.0.0' AS version_label
		FROM %[1]s.%[2]s.claimable_balances_snapshot_v1
		WHERE ledger_sequence BETWEEN %[3]d AND %[4]d
		ORDER BY balance_id, ledger_sequence DESC
	`, cat, bronze, start, end)
}

func liquidityPoolsCurrentSQL(cat, bronze string, start, end int64) string {
	return fmt.Sprintf(`
		SELECT DISTINCT ON (liquidity_pool_id)
			liquidity_pool_id, pool_type, fee, trustline_count, total_pool_shares,
			asset_a_type, asset_a_code, asset_a_issuer, asset_a_amount,
			asset_b_type, asset_b_code, asset_b_issuer, asset_b_amount,
			ledger_sequence, closed_at, created_at, ledger_range,
			'silver-history-loader' AS era_id, 'v1.0.0' AS version_label
		FROM %[1]s.%[2]s.liquidity_pools_snapshot_v1
		WHERE ledger_sequence BETWEEN %[3]d AND %[4]d
		ORDER BY liquidity_pool_id, ledger_sequence DESC
	`, cat, bronze, start, end)
}

func nativeBalancesCurrentSQL(cat, bronze string, start, end int64) string {
	return fmt.Sprintf(`
		SELECT DISTINCT ON (account_id)
			account_id, balance, buying_liabilities, selling_liabilities,
			num_subentries, num_sponsoring, num_sponsored,
			sequence_number, last_modified_ledger, ledger_sequence, ledger_range,
			'silver-history-loader' AS era_id, 'v1.0.0' AS version_label
		FROM %[1]s.%[2]s.native_balances_snapshot_v1
		WHERE ledger_sequence BETWEEN %[3]d AND %[4]d
		ORDER BY account_id, ledger_sequence DESC
	`, cat, bronze, start, end)
}

func contractDataCurrentSQL(cat, bronze string, start, end int64) string {
	return fmt.Sprintf(`
		SELECT DISTINCT ON (contract_id, ledger_key_hash)
			contract_id, ledger_key_hash AS key_hash, contract_durability AS durability,
			asset_type, asset_code, asset_issuer,
			contract_data_xdr AS data_value,
			last_modified_ledger, ledger_sequence, closed_at, created_at, ledger_range,
			'silver-history-loader' AS era_id, 'v1.0.0' AS version_label
		FROM %[1]s.%[2]s.contract_data_snapshot_v1
		WHERE ledger_sequence BETWEEN %[3]d AND %[4]d AND deleted = FALSE
		ORDER BY contract_id, ledger_key_hash, ledger_sequence DESC
	`, cat, bronze, start, end)
}

func contractCodeCurrentSQL(cat, bronze string, start, end int64) string {
	return fmt.Sprintf(`
		SELECT DISTINCT ON (contract_code_hash)
			contract_code_hash, contract_code_ext_v,
			n_data_segment_bytes, n_data_segments, n_elem_segments, n_exports,
			n_functions, n_globals, n_imports, n_instructions, n_table_entries, n_types,
			last_modified_ledger, ledger_sequence, closed_at, created_at, ledger_range,
			'silver-history-loader' AS era_id, 'v1.0.0' AS version_label
		FROM %[1]s.%[2]s.contract_code_snapshot_v1
		WHERE ledger_sequence BETWEEN %[3]d AND %[4]d AND deleted = FALSE
		ORDER BY contract_code_hash, ledger_sequence DESC
	`, cat, bronze, start, end)
}

func ttlCurrentSQL(cat, bronze string, start, end int64) string {
	return fmt.Sprintf(`
		SELECT DISTINCT ON (key_hash)
			key_hash, live_until_ledger_seq, ttl_remaining, expired,
			last_modified_ledger, ledger_sequence, closed_at, created_at, ledger_range,
			'silver-history-loader' AS era_id, 'v1.0.0' AS version_label
		FROM %[1]s.%[2]s.ttl_snapshot_v1
		WHERE ledger_sequence BETWEEN %[3]d AND %[4]d AND deleted = FALSE
		ORDER BY key_hash, ledger_sequence DESC
	`, cat, bronze, start, end)
}

func configSettingsCurrentSQL(cat, bronze string, start, end int64) string {
	return fmt.Sprintf(`
		SELECT DISTINCT ON (config_setting_id)
			config_setting_id, config_setting_xdr,
			last_modified_ledger, ledger_sequence, closed_at, created_at, ledger_range,
			'silver-history-loader' AS era_id, 'v1.0.0' AS version_label
		FROM %[1]s.%[2]s.config_settings_snapshot_v1
		WHERE ledger_sequence BETWEEN %[3]d AND %[4]d AND deleted = FALSE
		ORDER BY config_setting_id, ledger_sequence DESC
	`, cat, bronze, start, end)
}

func tokenRegistrySQL(cat, bronze string, start, end int64) string {
	return fmt.Sprintf(`
		SELECT DISTINCT ON (contract_id)
			contract_id, token_name, token_symbol, token_decimals,
			asset_code, asset_issuer,
			ledger_sequence AS first_seen_ledger, ledger_sequence AS last_updated_ledger,
			closed_at AS updated_at,
			'silver-history-loader' AS era_id, 'v1.0.0' AS version_label
		FROM %[1]s.%[2]s.contract_data_snapshot_v1
		WHERE ledger_sequence BETWEEN %[3]d AND %[4]d AND token_name IS NOT NULL
		ORDER BY contract_id, ledger_sequence DESC
	`, cat, bronze, start, end)
}

func accountsSnapshotSQL(cat, bronze string, start, end int64) string {
	return fmt.Sprintf(`
		SELECT
			account_id, ledger_sequence, closed_at, balance, sequence_number,
			num_subentries, num_sponsoring, num_sponsored, home_domain,
			master_weight, low_threshold AS threshold_low,
			med_threshold AS threshold_medium, high_threshold AS threshold_high,
			flags, auth_required, auth_revocable, auth_immutable, auth_clawback_enabled,
			signers, sponsor_account, ledger_range,
			'silver-history-loader' AS era_id, 'v1.0.0' AS version_label
		FROM %[1]s.%[2]s.accounts_snapshot_v1
		WHERE ledger_sequence BETWEEN %[3]d AND %[4]d
	`, cat, bronze, start, end)
}

func trustlinesSnapshotSQL(cat, bronze string, start, end int64) string {
	return fmt.Sprintf(`
		SELECT
			account_id, asset_code, asset_issuer, asset_type,
			balance, trust_limit AS trust_line_limit,
			buying_liabilities, selling_liabilities,
			authorized, authorized_to_maintain_liabilities, clawback_enabled,
			ledger_sequence, ledger_range,
			'silver-history-loader' AS era_id, 'v1.0.0' AS version_label
		FROM %[1]s.%[2]s.trustlines_snapshot_v1
		WHERE ledger_sequence BETWEEN %[3]d AND %[4]d
	`, cat, bronze, start, end)
}

func offersSnapshotSQL(cat, bronze string, start, end int64) string {
	return fmt.Sprintf(`
		SELECT
			offer_id, seller_account AS seller_id, ledger_sequence, closed_at,
			selling_asset_type, selling_asset_code, selling_asset_issuer,
			buying_asset_type, buying_asset_code, buying_asset_issuer,
			amount, TRY_CAST(price AS DOUBLE) AS price, flags,
			ledger_range,
			'silver-history-loader' AS era_id, 'v1.0.0' AS version_label
		FROM %[1]s.%[2]s.offers_snapshot_v1
		WHERE ledger_sequence BETWEEN %[3]d AND %[4]d
	`, cat, bronze, start, end)
}

func accountSignersSnapshotSQL(cat, bronze string, start, end int64) string {
	return fmt.Sprintf(`
		SELECT
			account_id, signer, ledger_sequence, closed_at,
			weight, sponsor, deleted, ledger_range,
			'silver-history-loader' AS era_id, 'v1.0.0' AS version_label
		FROM %[1]s.%[2]s.account_signers_snapshot_v1
		WHERE ledger_sequence BETWEEN %[3]d AND %[4]d
	`, cat, bronze, start, end)
}

// ---------------------------------------------------------------------------
// Phase B: Semantic transforms (read from local Parquet, not DuckLake)
// ---------------------------------------------------------------------------

func semanticActivitiesSQL(outputDir string) string {
	src := filepath.Join(outputDir, "enriched_history_operations.parquet")
	return fmt.Sprintf(`
		SELECT
			CAST(ledger_sequence AS TEXT) || '-' || CAST(operation_index AS TEXT) AS id,
			ledger_sequence, created_at AS timestamp,
			CASE type
				WHEN 0 THEN 'account_created' WHEN 1 THEN 'payment'
				WHEN 2 THEN 'path_payment_strict_receive' WHEN 3 THEN 'manage_sell_offer'
				WHEN 5 THEN 'set_options' WHEN 6 THEN 'change_trust'
				WHEN 8 THEN 'account_merge' WHEN 12 THEN 'manage_buy_offer'
				WHEN 13 THEN 'path_payment_strict_send' WHEN 24 THEN 'invoke_host_function'
				ELSE 'other'
			END AS activity_type,
			type_string AS description,
			source_account, destination AS destination_account,
			contract_id, asset_code, asset_issuer,
			CAST(amount AS DOUBLE) AS amount,
			CASE WHEN type = 24 THEN TRUE ELSE FALSE END AS is_soroban,
			function_name AS soroban_function_name,
			transaction_hash, operation_index,
			transaction_successful AS successful,
			tx_fee_charged AS fee_charged,
			created_at
		FROM read_parquet('%s')
	`, src)
}

func semanticFlowsSQL(outputDir string) string {
	src := filepath.Join(outputDir, "token_transfers_raw.parquet")
	return fmt.Sprintf(`
		SELECT
			CAST(ledger_sequence AS TEXT) || '-' || CAST(ROW_NUMBER() OVER () AS TEXT) AS id,
			ledger_sequence, timestamp,
			CASE
				WHEN from_account IS NULL OR from_account = '' THEN 'mint'
				WHEN to_account IS NULL OR to_account = '' THEN 'burn'
				ELSE 'transfer'
			END AS flow_type,
			from_account, to_account, NULL AS contract_id,
			asset_code, asset_issuer, amount,
			transaction_hash, operation_type, transaction_successful AS successful,
			timestamp AS created_at
		FROM read_parquet('%s')
	`, src)
}

func semanticEntitiesSQL(outputDir string) string {
	invocations := filepath.Join(outputDir, "contract_invocations_raw.parquet")
	registry := filepath.Join(outputDir, "token_registry.parquet")
	metadata := filepath.Join(outputDir, "contract_metadata.parquet")
	return fmt.Sprintf(`
		SELECT
			ci.contract_id,
			CASE WHEN tr.token_name IS NOT NULL THEN 'token' ELSE 'contract' END AS contract_type,
			tr.token_name, tr.token_symbol, tr.token_decimals,
			cm.creator_address AS deployer_account,
			cm.created_at AS deployed_at, cm.created_ledger AS deployed_ledger,
			COUNT(*) AS total_invocations,
			MAX(ci.closed_at) AS last_activity,
			COUNT(DISTINCT ci.source_account) AS unique_callers,
			NOW() AS updated_at,
			'silver-history-loader' AS era_id, 'v1.0.0' AS version_label
		FROM read_parquet('%[1]s') ci
		LEFT JOIN read_parquet('%[2]s') tr ON ci.contract_id = tr.contract_id
		LEFT JOIN read_parquet('%[3]s') cm ON ci.contract_id = cm.contract_id
		GROUP BY ci.contract_id, tr.token_name, tr.token_symbol, tr.token_decimals,
				 cm.creator_address, cm.created_at, cm.created_ledger
	`, invocations, registry, metadata)
}

func semanticContractFunctionsSQL(outputDir string) string {
	src := filepath.Join(outputDir, "contract_invocations_raw.parquet")
	return fmt.Sprintf(`
		SELECT
			contract_id, function_name,
			COUNT(*) AS total_calls,
			COUNT(*) FILTER (WHERE successful = TRUE) AS successful_calls,
			COUNT(*) FILTER (WHERE successful = FALSE) AS failed_calls,
			COUNT(DISTINCT source_account) AS unique_callers,
			MIN(closed_at) AS first_called, MAX(closed_at) AS last_called,
			NOW() AS updated_at,
			'silver-history-loader' AS era_id, 'v1.0.0' AS version_label
		FROM read_parquet('%s')
		GROUP BY contract_id, function_name
	`, src)
}

func semanticAssetStatsSQL(outputDir string) string {
	src := filepath.Join(outputDir, "token_transfers_raw.parquet")
	return fmt.Sprintf(`
		SELECT
			COALESCE(asset_code, '') || ':' || COALESCE(asset_issuer, '') AS asset_key,
			asset_code, asset_issuer,
			COUNT(*) AS transfer_count_24h,
			SUM(COALESCE(amount, 0)) AS transfer_volume_24h,
			MIN(timestamp) AS first_seen, MAX(timestamp) AS last_transfer,
			NOW() AS updated_at,
			'silver-history-loader' AS era_id, 'v1.0.0' AS version_label
		FROM read_parquet('%s')
		WHERE asset_code IS NOT NULL
		GROUP BY asset_code, asset_issuer
	`, src)
}

func semanticDexPairsSQL(outputDir string) string {
	src := filepath.Join(outputDir, "trades.parquet")
	return fmt.Sprintf(`
		SELECT
			COALESCE(selling_asset_code, 'XLM') || '/' || COALESCE(buying_asset_code, 'XLM') AS pair_key,
			selling_asset_code, selling_asset_issuer,
			buying_asset_code, buying_asset_issuer,
			COUNT(*) AS trade_count,
			LAST(price ORDER BY ledger_sequence, trade_index) AS last_price,
			COUNT(DISTINCT seller_account) AS unique_sellers,
			COUNT(DISTINCT buyer_account) AS unique_buyers,
			MIN(trade_timestamp) AS first_trade, MAX(trade_timestamp) AS last_trade,
			NOW() AS updated_at,
			'silver-history-loader' AS era_id, 'v1.0.0' AS version_label
		FROM read_parquet('%s')
		GROUP BY selling_asset_code, selling_asset_issuer, buying_asset_code, buying_asset_issuer
	`, src)
}

func semanticAccountSummarySQL(outputDir string) string {
	src := filepath.Join(outputDir, "enriched_history_operations.parquet")
	return fmt.Sprintf(`
		SELECT
			source_account AS account_id,
			COUNT(*) AS total_operations,
			COUNT(*) FILTER (WHERE type IN (1, 2, 13)) AS total_payments_sent,
			COUNT(*) FILTER (WHERE type = 24) AS total_contract_calls,
			COUNT(DISTINCT contract_id) FILTER (WHERE type = 24) AS unique_contracts_called,
			MIN(created_at) AS first_activity, MAX(created_at) AS last_activity,
			NOW() AS updated_at,
			'silver-history-loader' AS era_id, 'v1.0.0' AS version_label
		FROM read_parquet('%s')
		GROUP BY source_account
	`, src)
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func initDuckLake(ctx context.Context, cfg *Config) (*sql.DB, error) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		return nil, fmt.Errorf("open duckdb: %w", err)
	}
	db.SetMaxOpenConns(1)

	for _, ext := range []string{"ducklake", "httpfs"} {
		if _, err := db.ExecContext(ctx, fmt.Sprintf("INSTALL %s FROM core_nightly", ext)); err != nil {
			if _, err2 := db.ExecContext(ctx, fmt.Sprintf("INSTALL %s", ext)); err2 != nil {
				return nil, fmt.Errorf("install %s: %w", ext, err)
			}
		}
		if _, err := db.ExecContext(ctx, fmt.Sprintf("LOAD %s", ext)); err != nil {
			return nil, fmt.Errorf("load %s: %w", ext, err)
		}
	}
	log.Println("Extensions loaded: ducklake, httpfs")

	s3SQL := fmt.Sprintf(`
		CREATE SECRET s3_secret (
			TYPE S3, KEY_ID '%s', SECRET '%s', REGION '%s', ENDPOINT '%s',
			URL_COMPATIBILITY_MODE true
		)`, cfg.S3.KeyID, cfg.S3.KeySecret, cfg.S3.Region, cfg.S3.Endpoint)
	if _, err := db.ExecContext(ctx, s3SQL); err != nil {
		return nil, fmt.Errorf("configure S3: %w", err)
	}

	metaSchema := cfg.DuckLake.MetadataSchema
	if metaSchema == "" {
		metaSchema = "lake_meta"
	}
	attachSQL := fmt.Sprintf(
		"ATTACH '%s' AS %s (DATA_PATH '%s', METADATA_SCHEMA '%s', AUTOMATIC_MIGRATION TRUE, OVERRIDE_DATA_PATH TRUE)",
		cfg.DuckLake.CatalogPath, cfg.DuckLake.CatalogName, cfg.DuckLake.DataPath, metaSchema)
	if _, err := db.ExecContext(ctx, attachSQL); err != nil {
		return nil, fmt.Errorf("attach catalog: %w", err)
	}
	log.Printf("DuckLake catalog '%s' attached", cfg.DuckLake.CatalogName)

	return db, nil
}

func loadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}
	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}
	if cfg.DuckLake.CatalogName == "" {
		cfg.DuckLake.CatalogName = "testnet_catalog"
	}
	if cfg.DuckLake.BronzeSchema == "" {
		cfg.DuckLake.BronzeSchema = "bronze"
	}
	if cfg.DuckLake.SilverSchema == "" {
		cfg.DuckLake.SilverSchema = "silver"
	}
	return &cfg, nil
}
