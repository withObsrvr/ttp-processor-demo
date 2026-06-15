package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"

	_ "github.com/duckdb/duckdb-go/v2"
)

var Version = "dev"

const schemaVersion = "silver-history-loader/v1"

type Config struct {
	Network string
	Start   int64
	End     int64
	Chunk   int64
	Resume  bool
	Verify  bool

	BronzeCatalog string
	BronzeData    string
	BronzeAlias   string
	BronzeSchema  string
	BronzeMeta    string

	SilverCatalog string
	SilverData    string
	SilverAlias   string
	SilverSchema  string
	SilverMeta    string

	S3KeyID    string
	S3Secret   string
	S3Region   string
	S3Endpoint string
}

type Loader struct {
	db  *sql.DB
	cfg Config
}

type Transform struct {
	Table     string
	RangeCol  string
	SelectSQL func(*Loader, int64, int64) string
}

func main() {
	cfg := parseFlags()
	ctx := context.Background()

	loader, err := NewLoader(ctx, cfg)
	if err != nil {
		log.Fatalf("setup failed: %v", err)
	}
	defer loader.Close()

	if cfg.Verify {
		if err := loader.Verify(ctx); err != nil {
			log.Fatalf("verify failed: %v", err)
		}
		return
	}

	if err := loader.Run(ctx); err != nil {
		log.Fatalf("load failed: %v", err)
	}
}

func parseFlags() Config {
	var cfg Config
	flag.StringVar(&cfg.Network, "network", "mainnet", "network label written to silver rows")
	flag.Int64Var(&cfg.Start, "start-ledger", 0, "start ledger sequence (required)")
	flag.Int64Var(&cfg.End, "end-ledger", 0, "end ledger sequence (required)")
	flag.Int64Var(&cfg.Chunk, "chunk-size", 100000, "ledgers per chunk")
	flag.BoolVar(&cfg.Resume, "resume", false, "skip chunks whose manifest entries are already complete")
	flag.BoolVar(&cfg.Verify, "verify", false, "verify source coverage, manifests, and silver output")

	flag.StringVar(&cfg.BronzeCatalog, "bronze-ducklake-catalog", "", "Bronze DuckLake catalog DSN/path (required)")
	flag.StringVar(&cfg.BronzeData, "bronze-data-path", "", "Bronze DuckLake data path (required)")
	flag.StringVar(&cfg.BronzeAlias, "bronze-catalog-name", "bronze_catalog", "DuckDB alias for Bronze catalog")
	flag.StringVar(&cfg.BronzeSchema, "bronze-schema", "bronze", "Bronze schema name")
	flag.StringVar(&cfg.BronzeMeta, "bronze-metadata-schema", "bronze_meta", "Bronze DuckLake metadata schema")

	flag.StringVar(&cfg.SilverCatalog, "silver-ducklake-catalog", "", "Silver DuckLake catalog DSN/path (required)")
	flag.StringVar(&cfg.SilverData, "silver-data-path", "", "Silver DuckLake data path (required)")
	flag.StringVar(&cfg.SilverAlias, "silver-catalog-name", "silver_catalog", "DuckDB alias for Silver catalog")
	flag.StringVar(&cfg.SilverSchema, "silver-schema", "silver", "Silver schema name")
	flag.StringVar(&cfg.SilverMeta, "silver-metadata-schema", "silver_meta", "Silver DuckLake metadata schema")

	flag.StringVar(&cfg.S3KeyID, "s3-key-id", "", "S3/B2 access key ID")
	flag.StringVar(&cfg.S3Secret, "s3-secret", "", "S3/B2 secret access key")
	flag.StringVar(&cfg.S3Region, "s3-region", "us-west-004", "S3/B2 region")
	flag.StringVar(&cfg.S3Endpoint, "s3-endpoint", "s3.us-west-004.backblazeb2.com", "S3/B2 endpoint")
	flag.Parse()

	if cfg.S3KeyID == "" {
		cfg.S3KeyID = os.Getenv("S3_KEY_ID")
	}
	if cfg.S3Secret == "" {
		cfg.S3Secret = os.Getenv("S3_SECRET")
	}
	if cfg.S3Region == "" {
		cfg.S3Region = os.Getenv("S3_REGION")
	}
	if cfg.S3Endpoint == "" {
		cfg.S3Endpoint = os.Getenv("S3_ENDPOINT")
	}

	if cfg.Start <= 0 || cfg.End <= 0 || cfg.End < cfg.Start {
		log.Fatal("--start-ledger and --end-ledger are required and end must be >= start")
	}
	if cfg.Chunk <= 0 {
		log.Fatal("--chunk-size must be > 0")
	}
	if cfg.BronzeCatalog == "" || cfg.BronzeData == "" || cfg.SilverCatalog == "" || cfg.SilverData == "" {
		log.Fatal("--bronze-ducklake-catalog, --bronze-data-path, --silver-ducklake-catalog, and --silver-data-path are required")
	}
	return cfg
}

func NewLoader(ctx context.Context, cfg Config) (*Loader, error) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		return nil, err
	}
	l := &Loader{db: db, cfg: cfg}
	for _, stmt := range []string{"INSTALL ducklake FROM core_nightly", "LOAD ducklake", "INSTALL httpfs", "LOAD httpfs"} {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			db.Close()
			return nil, fmt.Errorf("%s: %w", stmt, err)
		}
	}
	if cfg.S3KeyID != "" || cfg.S3Secret != "" {
		endpoint := strings.TrimPrefix(strings.TrimPrefix(cfg.S3Endpoint, "https://"), "http://")
		stmt := fmt.Sprintf("CREATE OR REPLACE SECRET (TYPE S3, KEY_ID %s, SECRET %s, REGION %s, ENDPOINT %s, URL_STYLE 'path')", q(cfg.S3KeyID), q(cfg.S3Secret), q(cfg.S3Region), q(endpoint))
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			db.Close()
			return nil, fmt.Errorf("create s3 secret: %w", err)
		}
	}
	if err := l.attach(ctx, cfg.BronzeAlias, cfg.BronzeCatalog, cfg.BronzeData, cfg.BronzeMeta, true); err != nil {
		db.Close()
		return nil, err
	}
	if err := l.attach(ctx, cfg.SilverAlias, cfg.SilverCatalog, cfg.SilverData, cfg.SilverMeta, false); err != nil {
		db.Close()
		return nil, err
	}
	if _, err := db.ExecContext(ctx, "CREATE SCHEMA IF NOT EXISTS "+l.schema()); err != nil {
		db.Close()
		return nil, fmt.Errorf("create silver schema: %w", err)
	}
	if err := l.ensureManifest(ctx); err != nil {
		db.Close()
		return nil, err
	}
	return l, nil
}

func (l *Loader) attach(ctx context.Context, alias, catalog, data, meta string, readOnly bool) error {
	// DuckLake attach syntax is kept aligned with the existing readers/flushers.
	// The loader never issues writes to the Bronze catalog; readOnly is retained
	// in the signature to document intent without relying on DuckLake-specific
	// READ_ONLY option support across versions.
	_ = readOnly
	stmt := fmt.Sprintf("ATTACH %s AS %s (DATA_PATH %s, METADATA_SCHEMA %s, AUTOMATIC_MIGRATION TRUE, OVERRIDE_DATA_PATH TRUE)", q(catalog), ident(alias), q(data), q(meta))
	if _, err := l.db.ExecContext(ctx, stmt); err != nil {
		return fmt.Errorf("attach %s: %w", alias, err)
	}
	return nil
}

func (l *Loader) Close() error { return l.db.Close() }

func (l *Loader) Run(ctx context.Context) error {
	log.Printf("silver-history-loader %s: network=%s range=%d..%d chunk=%d", Version, l.cfg.Network, l.cfg.Start, l.cfg.End, l.cfg.Chunk)
	for start := l.cfg.Start; start <= l.cfg.End; start += l.cfg.Chunk {
		end := start + l.cfg.Chunk - 1
		if end > l.cfg.End {
			end = l.cfg.End
		}
		if l.cfg.Resume {
			complete, err := l.chunkComplete(ctx, start, end)
			if err != nil {
				return err
			}
			if complete {
				log.Printf("skip completed chunk %d..%d", start, end)
				continue
			}
		}
		if err := l.processChunk(ctx, start, end); err != nil {
			return err
		}
	}
	return nil
}

func (l *Loader) processChunk(ctx context.Context, start, end int64) error {
	if err := l.verifyBronzeCoverage(ctx, start, end); err != nil {
		return err
	}
	log.Printf("processing chunk %d..%d", start, end)
	for _, t := range transforms() {
		if err := l.markManifest(ctx, start, end, t.Table, "running", "", 0, ""); err != nil {
			return err
		}
		if err := l.ensureTable(ctx, t, start, end); err != nil {
			_ = l.markManifest(ctx, start, end, t.Table, "failed", err.Error(), 0, "")
			return err
		}
		if _, err := l.db.ExecContext(ctx, "BEGIN TRANSACTION"); err != nil {
			_ = l.markManifest(ctx, start, end, t.Table, "failed", err.Error(), 0, "")
			return fmt.Errorf("begin %s %d..%d: %w", t.Table, start, end, err)
		}
		deleteSQL := fmt.Sprintf("DELETE FROM %s WHERE network = %s AND %s BETWEEN %d AND %d", l.table(t.Table), q(l.cfg.Network), ident(t.RangeCol), start, end)
		insertSQL := fmt.Sprintf("INSERT INTO %s %s", l.table(t.Table), t.SelectSQL(l, start, end))
		if _, err := l.db.ExecContext(ctx, deleteSQL); err != nil {
			_, _ = l.db.ExecContext(ctx, "ROLLBACK")
			_ = l.markManifest(ctx, start, end, t.Table, "failed", err.Error(), 0, "")
			return fmt.Errorf("delete %s %d..%d: %w", t.Table, start, end, err)
		}
		if _, err := l.db.ExecContext(ctx, insertSQL); err != nil {
			_, _ = l.db.ExecContext(ctx, "ROLLBACK")
			_ = l.markManifest(ctx, start, end, t.Table, "failed", err.Error(), 0, "")
			return fmt.Errorf("insert %s %d..%d: %w", t.Table, start, end, err)
		}
		if _, err := l.db.ExecContext(ctx, "COMMIT"); err != nil {
			_, _ = l.db.ExecContext(ctx, "ROLLBACK")
			_ = l.markManifest(ctx, start, end, t.Table, "failed", err.Error(), 0, "")
			return fmt.Errorf("commit %s %d..%d: %w", t.Table, start, end, err)
		}
		rowCount, checksum, err := l.tableStats(ctx, t, start, end)
		if err != nil {
			_ = l.markManifest(ctx, start, end, t.Table, "failed", err.Error(), 0, "")
			return err
		}
		if err := l.markManifest(ctx, start, end, t.Table, "completed", "", rowCount, checksum); err != nil {
			return err
		}
		log.Printf("  %s: %d rows", t.Table, rowCount)
	}
	return nil
}

func (l *Loader) ensureTable(ctx context.Context, t Transform, start, end int64) error {
	stmt := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s AS SELECT * FROM (%s) seed WHERE false", l.table(t.Table), t.SelectSQL(l, start, end))
	_, err := l.db.ExecContext(ctx, stmt)
	if err != nil {
		return fmt.Errorf("create %s: %w", t.Table, err)
	}
	return nil
}

func (l *Loader) verifyBronzeCoverage(ctx context.Context, start, end int64) error {
	var count int64
	query := fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE sequence BETWEEN %d AND %d", l.bronze("ledgers_row_v2"), start, end)
	if err := l.db.QueryRowContext(ctx, query).Scan(&count); err != nil {
		return fmt.Errorf("bronze coverage query: %w", err)
	}
	expected := end - start + 1
	if count != expected {
		return fmt.Errorf("bronze coverage incomplete for %d..%d: got %d ledgers, expected %d", start, end, count, expected)
	}
	return nil
}

func (l *Loader) Verify(ctx context.Context) error {
	if err := l.verifyBronzeCoverage(ctx, l.cfg.Start, l.cfg.End); err != nil {
		return err
	}
	for start := l.cfg.Start; start <= l.cfg.End; start += l.cfg.Chunk {
		end := start + l.cfg.Chunk - 1
		if end > l.cfg.End {
			end = l.cfg.End
		}
		complete, err := l.chunkComplete(ctx, start, end)
		if err != nil {
			return err
		}
		if !complete {
			return fmt.Errorf("manifest incomplete for chunk %d..%d", start, end)
		}
	}
	var min, max, cnt int64
	if err := l.db.QueryRowContext(ctx, fmt.Sprintf("SELECT COALESCE(MIN(ledger_sequence),0), COALESCE(MAX(ledger_sequence),0), COUNT(*) FROM %s", l.table("enriched_ledgers"))).Scan(&min, &max, &cnt); err != nil {
		return fmt.Errorf("read enriched_ledgers: %w", err)
	}
	log.Printf("enriched_ledgers: min=%d max=%d count=%d", min, max, cnt)
	var gaps int64
	gapSQL := fmt.Sprintf(`WITH ordered AS (SELECT ledger_sequence, lag(ledger_sequence) OVER (ORDER BY ledger_sequence) prev FROM %s WHERE ledger_sequence BETWEEN %d AND %d) SELECT COUNT(*) FROM ordered WHERE prev IS NOT NULL AND ledger_sequence <> prev + 1`, l.table("enriched_ledgers"), l.cfg.Start, l.cfg.End)
	if err := l.db.QueryRowContext(ctx, gapSQL).Scan(&gaps); err != nil {
		return err
	}
	if gaps != 0 {
		return fmt.Errorf("enriched_ledgers has %d positive gaps", gaps)
	}
	log.Printf("verify ok for %d..%d", l.cfg.Start, l.cfg.End)
	return nil
}

func (l *Loader) chunkComplete(ctx context.Context, start, end int64) (bool, error) {
	var count int64
	query := fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE network = %s AND start_ledger = %d AND end_ledger = %d AND status = 'completed'", l.table("silver_load_manifest"), q(l.cfg.Network), start, end)
	if err := l.db.QueryRowContext(ctx, query).Scan(&count); err != nil {
		return false, err
	}
	return count == int64(len(transforms())), nil
}

func (l *Loader) ensureManifest(ctx context.Context) error {
	stmt := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
		network VARCHAR, start_ledger BIGINT, end_ledger BIGINT, table_name VARCHAR,
		output_path VARCHAR, row_count BIGINT, checksum VARCHAR, schema_version VARCHAR,
		status VARCHAR, error_message VARCHAR, started_at TIMESTAMP, completed_at TIMESTAMP
	)`, l.table("silver_load_manifest"))
	_, err := l.db.ExecContext(ctx, stmt)
	return err
}

func (l *Loader) markManifest(ctx context.Context, start, end int64, table, status, message string, rows int64, checksum string) error {
	if _, err := l.db.ExecContext(ctx, fmt.Sprintf("DELETE FROM %s WHERE network=%s AND start_ledger=%d AND end_ledger=%d AND table_name=%s", l.table("silver_load_manifest"), q(l.cfg.Network), start, end, q(table))); err != nil {
		return err
	}
	completed := "NULL"
	if status == "completed" || status == "failed" {
		completed = "current_timestamp"
	}
	stmt := fmt.Sprintf(`INSERT INTO %s VALUES (%s, %d, %d, %s, %s, %d, %s, %s, %s, %s, current_timestamp, %s)`,
		l.table("silver_load_manifest"), q(l.cfg.Network), start, end, q(table), q(l.table(table)), rows, q(checksum), q(schemaVersion), q(status), q(message), completed)
	_, err := l.db.ExecContext(ctx, stmt)
	return err
}

func (l *Loader) tableStats(ctx context.Context, t Transform, start, end int64) (int64, string, error) {
	var count int64
	query := fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE network = %s AND %s BETWEEN %d AND %d", l.table(t.Table), q(l.cfg.Network), ident(t.RangeCol), start, end)
	if err := l.db.QueryRowContext(ctx, query).Scan(&count); err != nil {
		return 0, "", err
	}
	return count, fmt.Sprintf("rows=%d;range=%d-%d;schema=%s", count, start, end, schemaVersion), nil
}

func transforms() []Transform {
	return []Transform{
		{Table: "enriched_ledgers", RangeCol: "ledger_sequence", SelectSQL: selectLedgers},
		{Table: "enriched_history_operations", RangeCol: "ledger_sequence", SelectSQL: selectEnrichedOperations},
		{Table: "enriched_history_operations_soroban", RangeCol: "ledger_sequence", SelectSQL: selectEnrichedOperationsSoroban},
		{Table: "token_transfers_raw", RangeCol: "ledger_sequence", SelectSQL: selectClassicTokenTransfers},
		{Table: "contract_invocations_raw", RangeCol: "ledger_sequence", SelectSQL: selectContractInvocations},
		{Table: "contract_metadata", RangeCol: "created_ledger", SelectSQL: selectContractMetadata},
		{Table: "semantic_activities", RangeCol: "ledger_sequence", SelectSQL: selectSemanticActivities},
		{Table: "semantic_flows_value", RangeCol: "ledger_sequence", SelectSQL: selectSemanticFlowsValue},
		{Table: "accounts_snapshot", RangeCol: "ledger_sequence", SelectSQL: selectAccountsSnapshotAll},
		{Table: "trustlines_snapshot", RangeCol: "ledger_sequence", SelectSQL: selectTrustlinesSnapshotAll},
		{Table: "offers_snapshot", RangeCol: "ledger_sequence", SelectSQL: selectOffersSnapshotAll},
		{Table: "account_signers_snapshot", RangeCol: "ledger_sequence", SelectSQL: selectAccountSignersSnapshotAll},
		{Table: "effects", RangeCol: "ledger_sequence", SelectSQL: selectEffects},
		{Table: "trades", RangeCol: "ledger_sequence", SelectSQL: selectTrades},
		{Table: "evicted_keys", RangeCol: "ledger_sequence", SelectSQL: selectEvictedKeys},
		{Table: "restored_keys", RangeCol: "ledger_sequence", SelectSQL: selectRestoredKeys},
		{Table: "contract_data_changes", RangeCol: "ledger_sequence", SelectSQL: selectContractDataChanges},
		{Table: "balance_changes", RangeCol: "ledger_sequence", SelectSQL: selectBalanceChanges},
	}
}

func selectLedgers(l *Loader, start, end int64) string {
	return fmt.Sprintf(`SELECT %s AS network, sequence AS ledger_sequence, closed_at AS ledger_closed_at,
		ledger_hash, previous_ledger_hash, transaction_count, operation_count, successful_tx_count, failed_tx_count,
		total_coins, fee_pool, base_fee, base_reserve, protocol_version AS ledger_version,
		%s AS _schema_version, current_timestamp AS _loaded_at, %d::BIGINT AS _source_bronze_start_ledger, %d::BIGINT AS _source_bronze_end_ledger
		FROM %s WHERE sequence BETWEEN %d AND %d`, q(l.cfg.Network), q(schemaVersion), start, end, l.bronze("ledgers_row_v2"), start, end)
}

func selectEnrichedOperations(l *Loader, start, end int64) string {
	return fmt.Sprintf(`SELECT %s AS network,
		o.transaction_hash, o.operation_index, o.ledger_sequence, o.source_account, o.type, o.type_string,
		o.created_at, o.transaction_successful, o.operation_result_code, NULL AS operation_trace_code, o.ledger_range,
		o.source_account_muxed, o.asset, o.asset_type, o.asset_code, o.asset_issuer, o.source_asset,
		o.source_asset_type, o.source_asset_code, o.source_asset_issuer, o.destination, NULL AS destination_muxed,
		o.amount, o.source_amount, NULL AS from_account, NULL AS from_muxed, NULL AS to_address, NULL AS to_muxed,
		o.trustline_limit AS limit_amount, o.offer_id, o.selling_asset, o.selling_asset_type, o.selling_asset_code,
		o.selling_asset_issuer, o.buying_asset, o.buying_asset_type, o.buying_asset_code, o.buying_asset_issuer,
		CASE WHEN o.price_r IS NOT NULL THEN TRY_CAST(json_extract_string(o.price_r, '$.n') AS INTEGER) ELSE NULL END AS price_n,
		CASE WHEN o.price_r IS NOT NULL THEN TRY_CAST(json_extract_string(o.price_r, '$.d') AS INTEGER) ELSE NULL END AS price_d,
		CASE WHEN o.price IS NULL THEN NULL WHEN contains(CAST(o.price AS VARCHAR), '/') THEN TRY_CAST(split_part(CAST(o.price AS VARCHAR), '/', 1) AS DOUBLE) / NULLIF(TRY_CAST(split_part(CAST(o.price AS VARCHAR), '/', 2) AS DOUBLE), 0) ELSE TRY_CAST(o.price AS DOUBLE) END AS price,
		o.starting_balance, o.home_domain, NULL AS inflation_dest, o.set_flags, NULL AS set_flags_s, o.clear_flags, NULL AS clear_flags_s,
		o.master_weight AS master_key_weight, o.low_threshold, o.medium_threshold AS med_threshold, o.high_threshold,
		NULL AS signer_account_id, NULL AS signer_key, NULL AS signer_weight, o.data_name, o.data_value,
		o.soroban_operation AS host_function_type, NULL AS parameters, NULL AS address, o.soroban_contract_id AS contract_id,
		o.soroban_function AS function_name, o.balance_id, NULL AS claimant, NULL AS claimant_muxed, NULL AS predicate,
		NULL AS liquidity_pool_id, NULL AS reserve_a_asset, NULL AS reserve_a_amount, NULL AS reserve_b_asset, NULL AS reserve_b_amount, NULL AS shares, NULL AS shares_received,
		NULL AS into_account, NULL AS into_muxed, NULL AS sponsor, o.sponsored_id, NULL AS begin_sponsor,
		COALESCE(t.successful, o.transaction_successful) AS tx_successful, t.fee_charged AS tx_fee_charged, t.max_fee AS tx_max_fee, t.operation_count AS tx_operation_count, t.memo_type AS tx_memo_type, t.memo AS tx_memo,
		l.closed_at AS ledger_closed_at, l.total_coins AS ledger_total_coins, l.fee_pool AS ledger_fee_pool, l.base_fee AS ledger_base_fee, l.base_reserve AS ledger_base_reserve,
		l.transaction_count AS ledger_transaction_count, l.operation_count AS ledger_operation_count, l.successful_tx_count AS ledger_successful_tx_count, l.failed_tx_count AS ledger_failed_tx_count,
		CASE WHEN o.type IN (1, 2, 13) THEN true ELSE false END AS is_payment_op, CASE WHEN o.type = 24 THEN true ELSE false END AS is_soroban_op,
		%s AS _schema_version, current_timestamp AS _loaded_at, %d::BIGINT AS _source_bronze_start_ledger, %d::BIGINT AS _source_bronze_end_ledger
		FROM %s o LEFT JOIN %s t ON o.transaction_hash=t.transaction_hash AND o.ledger_sequence=t.ledger_sequence JOIN %s l ON o.ledger_sequence=l.sequence
		WHERE o.ledger_sequence BETWEEN %d AND %d`, q(l.cfg.Network), q(schemaVersion), start, end, l.bronze("operations_row_v2"), l.bronze("transactions_row_v2"), l.bronze("ledgers_row_v2"), start, end)
}

func selectEnrichedOperationsSoroban(l *Loader, start, end int64) string {
	return selectEnrichedOperations(l, start, end) + " AND o.type = 24"
}

func selectClassicTokenTransfers(l *Loader, start, end int64) string {
	return fmt.Sprintf(`SELECT * FROM (
		SELECT %s AS network, l.closed_at AS timestamp, o.transaction_hash, o.ledger_sequence, 'classic' AS source_type,
			o.source_account AS from_account, o.destination AS to_account,
			CASE WHEN o.asset IS NULL OR o.asset = 'native' THEN 'XLM' WHEN o.asset = 'liquidity_pool_shares' THEN NULL ELSE split_part(o.asset, ':', 1) END AS asset_code,
			CASE WHEN o.asset IS NULL OR o.asset = 'native' THEN NULL WHEN o.asset = 'liquidity_pool_shares' THEN NULL ELSE split_part(o.asset, ':', 2) END AS asset_issuer,
			CAST(o.amount AS VARCHAR) AS amount, NULL::VARCHAR AS token_contract_id, o.type AS operation_type, COALESCE(t.successful, o.transaction_successful) AS transaction_successful, NULL::INTEGER AS event_index,
			l.closed_at AS ledger_closed_at, %s AS _schema_version, current_timestamp AS _loaded_at, %d::BIGINT AS _source_bronze_start_ledger, %d::BIGINT AS _source_bronze_end_ledger
			FROM %s o LEFT JOIN %s t ON o.transaction_hash=t.transaction_hash AND o.ledger_sequence=t.ledger_sequence JOIN %s l ON o.ledger_sequence=l.sequence
			WHERE o.ledger_sequence BETWEEN %d AND %d AND o.type IN (1,2,13) AND COALESCE(t.successful, o.transaction_successful, true) = true
		UNION ALL
		SELECT %s AS network, e.closed_at AS timestamp, e.transaction_hash, e.ledger_sequence, 'soroban' AS source_type,
			CASE WHEN json_extract_string(e.topics_decoded, '$[0]') = 'transfer' THEN json_extract_string(e.topics_decoded, '$[1].address') WHEN json_extract_string(e.topics_decoded, '$[0]') IN ('burn','clawback') THEN json_extract_string(e.topics_decoded, '$[1].address') END AS from_account,
			CASE WHEN json_extract_string(e.topics_decoded, '$[0]') = 'transfer' THEN json_extract_string(e.topics_decoded, '$[2].address') WHEN json_extract_string(e.topics_decoded, '$[0]') = 'mint' AND json_type(e.topics_decoded, '$[2]') = 'OBJECT' THEN json_extract_string(e.topics_decoded, '$[2].address') WHEN json_extract_string(e.topics_decoded, '$[0]') = 'mint' AND json_type(e.topics_decoded, '$[1]') = 'OBJECT' AND json_extract_string(e.topics_decoded, '$[1].type') = 'account' THEN json_extract_string(e.topics_decoded, '$[1].address') END AS to_account,
			NULL::VARCHAR AS asset_code, NULL::VARCHAR AS asset_issuer,
			COALESCE(json_extract_string(e.data_decoded, '$.value'), json_extract_string(e.data_decoded, '$.entries.amount.value')) AS amount,
			e.contract_id AS token_contract_id, 24 AS operation_type, e.successful AS transaction_successful, e.event_index,
			e.closed_at AS ledger_closed_at, %s AS _schema_version, current_timestamp AS _loaded_at, %d::BIGINT AS _source_bronze_start_ledger, %d::BIGINT AS _source_bronze_end_ledger
			FROM %s e
			WHERE e.ledger_sequence BETWEEN %d AND %d AND e.event_type = 'contract' AND e.topic_count >= 2
			AND json_extract_string(e.topics_decoded, '$[0]') IN ('transfer','mint','burn','clawback')
		)`, q(l.cfg.Network), q(schemaVersion), start, end, l.bronze("operations_row_v2"), l.bronze("transactions_row_v2"), l.bronze("ledgers_row_v2"), start, end, q(l.cfg.Network), q(schemaVersion), start, end, l.bronze("contract_events_stream_v1"), start, end)
}

func (l *Loader) bronze(table string) string {
	return fmt.Sprintf("%s.%s.%s", ident(l.cfg.BronzeAlias), ident(l.cfg.BronzeSchema), ident(table))
}
func (l *Loader) table(table string) string { return fmt.Sprintf("%s.%s", l.schema(), ident(table)) }
func (l *Loader) schema() string {
	return fmt.Sprintf("%s.%s", ident(l.cfg.SilverAlias), ident(l.cfg.SilverSchema))
}

func ident(s string) string { return `"` + strings.ReplaceAll(s, `"`, `""`) + `"` }
func q(s string) string     { return `'` + strings.ReplaceAll(s, `'`, `''`) + `'` }
