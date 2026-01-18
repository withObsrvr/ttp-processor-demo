package main

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	_ "github.com/duckdb/duckdb-go/v2"
)

// BronzeColdReader reads data from Bronze cold storage (DuckLake)
// Used for backfilling when data is no longer available in Bronze Hot
type BronzeColdReader struct {
	db          *sql.DB
	config      *DuckLakeConfig
	s3Config    *S3Config
	catalogName string
	schemaName  string
}

// NewBronzeColdReader creates a new Bronze cold storage reader
func NewBronzeColdReader(config *DuckLakeConfig, s3Config *S3Config) (*BronzeColdReader, error) {
	// Open DuckDB connection (in-memory)
	db, err := sql.Open("duckdb", "")
	if err != nil {
		return nil, fmt.Errorf("failed to open DuckDB: %w", err)
	}

	catalogName := config.CatalogName
	if catalogName == "" {
		catalogName = "bronze_catalog"
	}

	schemaName := config.SchemaName
	if schemaName == "" {
		schemaName = "bronze"
	}

	reader := &BronzeColdReader{
		db:          db,
		config:      config,
		s3Config:    s3Config,
		catalogName: catalogName,
		schemaName:  schemaName,
	}

	// Initialize DuckLake connection
	if err := reader.initialize(context.Background()); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to initialize DuckLake: %w", err)
	}

	return reader, nil
}

func (r *BronzeColdReader) initialize(ctx context.Context) error {
	// Install required extensions
	if _, err := r.db.ExecContext(ctx, "INSTALL ducklake;"); err != nil {
		return fmt.Errorf("failed to install ducklake extension: %w", err)
	}
	if _, err := r.db.ExecContext(ctx, "LOAD ducklake;"); err != nil {
		return fmt.Errorf("failed to load ducklake extension: %w", err)
	}
	if _, err := r.db.ExecContext(ctx, "INSTALL httpfs;"); err != nil {
		return fmt.Errorf("failed to install httpfs extension: %w", err)
	}
	if _, err := r.db.ExecContext(ctx, "LOAD httpfs;"); err != nil {
		return fmt.Errorf("failed to load httpfs extension: %w", err)
	}

	// Remove https:// prefix from endpoint (DuckDB adds it automatically)
	endpoint := r.s3Config.Endpoint
	endpoint = strings.TrimPrefix(endpoint, "https://")
	endpoint = strings.TrimPrefix(endpoint, "http://")

	// Configure S3 credentials for Backblaze B2
	s3ConfigSQL := fmt.Sprintf(`
		CREATE SECRET (
			TYPE S3,
			KEY_ID '%s',
			SECRET '%s',
			REGION '%s',
			ENDPOINT '%s',
			URL_STYLE 'path'
		);
	`, r.s3Config.KeyID, r.s3Config.Secret, r.s3Config.Region, endpoint)

	if _, err := r.db.ExecContext(ctx, s3ConfigSQL); err != nil {
		return fmt.Errorf("failed to configure S3 credentials: %w", err)
	}

	// Attach DuckLake catalog
	attachSQL := fmt.Sprintf(`
		ATTACH '%s'
		AS %s
		(DATA_PATH '%s', METADATA_SCHEMA '%s');
	`, r.config.CatalogPath, r.catalogName, r.config.DataPath, r.config.MetadataSchema)

	if _, err := r.db.ExecContext(ctx, attachSQL); err != nil {
		return fmt.Errorf("failed to attach DuckLake catalog: %w", err)
	}

	return nil
}

// Close closes the DuckDB connection
func (r *BronzeColdReader) Close() error {
	if r.db != nil {
		return r.db.Close()
	}
	return nil
}

// tableName returns the fully qualified table name
func (r *BronzeColdReader) tableName(table string) string {
	return fmt.Sprintf("%s.%s.%s", r.catalogName, r.schemaName, table)
}

// =============================================================================
// Ledger Range Methods
// =============================================================================

// GetMaxLedgerSequence returns the maximum ledger sequence available in Bronze Cold
func (r *BronzeColdReader) GetMaxLedgerSequence(ctx context.Context) (int64, error) {
	var maxSeq sql.NullInt64

	query := fmt.Sprintf(`
		SELECT MAX(sequence)
		FROM %s
	`, r.tableName("ledgers_row_v2"))

	err := r.db.QueryRowContext(ctx, query).Scan(&maxSeq)
	if err != nil {
		return 0, fmt.Errorf("failed to get max ledger sequence from cold: %w", err)
	}

	if !maxSeq.Valid {
		return 0, nil
	}

	return maxSeq.Int64, nil
}

// GetMinLedgerSequence returns the minimum ledger sequence available in Bronze Cold
func (r *BronzeColdReader) GetMinLedgerSequence(ctx context.Context) (int64, error) {
	var minSeq sql.NullInt64

	query := fmt.Sprintf(`
		SELECT MIN(sequence)
		FROM %s
	`, r.tableName("ledgers_row_v2"))

	err := r.db.QueryRowContext(ctx, query).Scan(&minSeq)
	if err != nil {
		return 0, fmt.Errorf("failed to get min ledger sequence from cold: %w", err)
	}

	if !minSeq.Valid {
		return 0, nil
	}

	return minSeq.Int64, nil
}

// =============================================================================
// Phase 1: Core Tables (Operations, Transactions, Token Transfers)
// =============================================================================

// QueryEnrichedOperations reads enriched operations from Bronze Cold for a ledger range
func (r *BronzeColdReader) QueryEnrichedOperations(ctx context.Context, startLedger, endLedger int64) (*sql.Rows, error) {
	query := fmt.Sprintf(`
		SELECT
			-- Core operation fields
			o.transaction_hash,
			o.operation_index,
			o.ledger_sequence,
			o.source_account,
			o.type,
			o.type_string,
			o.created_at,
			o.transaction_successful,
			o.operation_result_code,
			o.operation_trace_code,
			o.ledger_range,
			o.source_account_muxed,

			-- Asset fields
			o.asset,
			o.asset_type,
			o.asset_code,
			o.asset_issuer,
			o.source_asset,
			o.source_asset_type,
			o.source_asset_code,
			o.source_asset_issuer,

			-- Payment/path payment fields
			o.destination,
			NULL AS destination_muxed,
			o.amount,
			o.source_amount,

			-- Path payment specific
			NULL AS from_account,
			NULL AS from_muxed,
			NULL AS to_address,
			NULL AS to_muxed,

			-- Trust line fields
			o.trustline_limit AS limit_amount,

			-- Offer fields
			o.offer_id,
			o.selling_asset,
			o.selling_asset_type,
			o.selling_asset_code,
			o.selling_asset_issuer,
			o.buying_asset,
			o.buying_asset_type,
			o.buying_asset_code,
			o.buying_asset_issuer,

			-- Price fields
			CASE WHEN o.price_r IS NOT NULL THEN CAST(json_extract(o.price_r, '$.n') AS INTEGER) ELSE NULL END AS price_n,
			CASE WHEN o.price_r IS NOT NULL THEN CAST(json_extract(o.price_r, '$.d') AS INTEGER) ELSE NULL END AS price_d,
			o.price,

			-- Account creation
			o.starting_balance,

			-- Account management
			o.home_domain,
			NULL AS inflation_dest,

			-- Flags
			o.set_flags,
			NULL AS set_flags_s,
			o.clear_flags,
			NULL AS clear_flags_s,

			-- Thresholds
			o.master_weight AS master_key_weight,
			o.low_threshold,
			o.medium_threshold AS med_threshold,
			o.high_threshold,

			-- Signer fields
			NULL AS signer_account_id,
			NULL AS signer_key,
			NULL AS signer_weight,

			-- Data entry
			o.data_name,
			o.data_value,

			-- Soroban fields
			o.soroban_operation AS host_function_type,
			NULL AS parameters,
			NULL AS address,
			o.soroban_contract_id AS contract_id,
			o.soroban_function AS function_name,

			-- Claimable balance
			o.balance_id,
			NULL AS claimant,
			NULL AS claimant_muxed,
			NULL AS predicate,

			-- Liquidity pool
			NULL AS liquidity_pool_id,
			NULL AS reserve_a_asset,
			NULL AS reserve_a_amount,
			NULL AS reserve_b_asset,
			NULL AS reserve_b_amount,
			NULL AS shares,
			NULL AS shares_received,

			-- Account merge
			NULL AS into_account,
			NULL AS into_muxed,

			-- Sponsorship
			NULL AS sponsor,
			o.sponsored_id,
			NULL AS begin_sponsor,

			-- Transaction fields (enriched from JOIN)
			t.successful AS tx_successful,
			t.fee_charged AS tx_fee_charged,
			t.max_fee AS tx_max_fee,
			t.operation_count AS tx_operation_count,
			t.memo_type AS tx_memo_type,
			t.memo AS tx_memo,

			-- Ledger fields (enriched from JOIN)
			l.closed_at AS ledger_closed_at,
			l.total_coins AS ledger_total_coins,
			l.fee_pool AS ledger_fee_pool,
			l.base_fee AS ledger_base_fee,
			l.base_reserve AS ledger_base_reserve,
			l.transaction_count AS ledger_transaction_count,
			l.operation_count AS ledger_operation_count,
			l.successful_tx_count AS ledger_successful_tx_count,
			l.failed_tx_count AS ledger_failed_tx_count,

			-- Derived fields
			CASE WHEN o.type IN (1, 2, 13) THEN true ELSE false END AS is_payment_op,
			CASE WHEN o.type = 24 THEN true ELSE false END AS is_soroban_op
		FROM %s o
		INNER JOIN %s t
			ON o.transaction_hash = t.transaction_hash
			AND o.ledger_sequence = t.ledger_sequence
		INNER JOIN %s l
			ON t.ledger_sequence = l.sequence
		WHERE o.ledger_sequence BETWEEN $1 AND $2
		ORDER BY o.ledger_sequence, o.operation_index
	`, r.tableName("operations_row_v2"), r.tableName("transactions_row_v2"), r.tableName("ledgers_row_v2"))

	rows, err := r.db.QueryContext(ctx, query, startLedger, endLedger)
	if err != nil {
		return nil, fmt.Errorf("failed to query enriched operations from cold: %w", err)
	}

	return rows, nil
}

// QueryTokenTransfers reads token transfers from Bronze Cold for a ledger range
func (r *BronzeColdReader) QueryTokenTransfers(ctx context.Context, startLedger, endLedger int64) (*sql.Rows, error) {
	query := fmt.Sprintf(`
		-- Classic Stellar Payments
		SELECT
			l.closed_at AS timestamp,
			o.transaction_hash,
			o.ledger_sequence,
			'classic' AS source_type,
			o.source_account AS from_account,
			CASE
				WHEN o.type = 1 THEN o.destination
				WHEN o.type = 2 THEN o.destination
				WHEN o.type = 13 THEN o.destination
			END AS to_account,
			CASE
				WHEN o.asset_type = 'native' THEN 'XLM'
				ELSE o.asset_code
			END AS asset_code,
			CASE
				WHEN o.asset_type = 'native' THEN NULL
				ELSE o.asset_issuer
			END AS asset_issuer,
			CASE
				WHEN o.type = 1 THEN o.amount
				WHEN o.type = 2 THEN o.amount
				WHEN o.type = 13 THEN o.source_amount
			END AS amount,
			NULL AS token_contract_id,
			o.type AS operation_type,
			t.successful AS transaction_successful
		FROM %s o
		INNER JOIN %s t
			ON o.transaction_hash = t.transaction_hash
			AND o.ledger_sequence = t.ledger_sequence
		INNER JOIN %s l
			ON o.ledger_sequence = l.sequence
		WHERE o.type IN (1, 2, 13)
		  AND o.ledger_sequence BETWEEN $1 AND $2

		UNION ALL

		-- Soroban Token Transfers
		SELECT
			l.closed_at AS timestamp,
			e.transaction_hash,
			e.ledger_sequence,
			'soroban' AS source_type,
			NULL AS from_account,
			NULL AS to_account,
			NULL AS asset_code,
			NULL AS asset_issuer,
			NULL AS amount,
			e.contract_id AS token_contract_id,
			24 AS operation_type,
			t.successful AS transaction_successful
		FROM %s e
		INNER JOIN %s t
			ON e.transaction_hash = t.transaction_hash
			AND e.ledger_sequence = t.ledger_sequence
		INNER JOIN %s l
			ON e.ledger_sequence = l.sequence
		WHERE e.ledger_sequence BETWEEN $1 AND $2

		ORDER BY ledger_sequence, transaction_hash
	`, r.tableName("operations_row_v2"), r.tableName("transactions_row_v2"), r.tableName("ledgers_row_v2"),
		r.tableName("contract_events_stream_v1"), r.tableName("transactions_row_v2"), r.tableName("ledgers_row_v2"))

	rows, err := r.db.QueryContext(ctx, query, startLedger, endLedger)
	if err != nil {
		return nil, fmt.Errorf("failed to query token transfers from cold: %w", err)
	}

	return rows, nil
}

// =============================================================================
// Phase 2: State Tables (Accounts, Trustlines, Offers)
// =============================================================================

// QueryAccountsSnapshot reads accounts from Bronze Cold snapshot for a ledger range (deduplicated)
func (r *BronzeColdReader) QueryAccountsSnapshot(ctx context.Context, startLedger, endLedger int64) (*sql.Rows, error) {
	query := fmt.Sprintf(`
		WITH ranked AS (
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
				ledger_sequence,
				ledger_range,
				era_id,
				version_label,
				ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY ledger_sequence DESC) as rn
			FROM %s
			WHERE ledger_sequence BETWEEN $1 AND $2
		)
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
			ledger_sequence,
			ledger_range,
			era_id,
			version_label
		FROM ranked
		WHERE rn = 1
		ORDER BY account_id
	`, r.tableName("accounts_snapshot_v1"))

	rows, err := r.db.QueryContext(ctx, query, startLedger, endLedger)
	if err != nil {
		return nil, fmt.Errorf("failed to query accounts snapshot from cold: %w", err)
	}

	return rows, nil
}

// QueryAccountsSnapshotAll reads all account snapshot changes (not deduplicated)
func (r *BronzeColdReader) QueryAccountsSnapshotAll(ctx context.Context, startLedger, endLedger int64) (*sql.Rows, error) {
	query := fmt.Sprintf(`
		SELECT
			account_id,
			ledger_sequence,
			closed_at,
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
			ledger_range,
			era_id,
			version_label
		FROM %s
		WHERE ledger_sequence BETWEEN $1 AND $2
		ORDER BY account_id, ledger_sequence
	`, r.tableName("accounts_snapshot_v1"))

	rows, err := r.db.QueryContext(ctx, query, startLedger, endLedger)
	if err != nil {
		return nil, fmt.Errorf("failed to query accounts snapshot all from cold: %w", err)
	}

	return rows, nil
}

// QueryTrustlinesSnapshotAll reads all trustline snapshot changes (not deduplicated)
func (r *BronzeColdReader) QueryTrustlinesSnapshotAll(ctx context.Context, startLedger, endLedger int64) (*sql.Rows, error) {
	query := fmt.Sprintf(`
		SELECT
			t.account_id,
			t.asset_code,
			t.asset_issuer,
			t.asset_type,
			t.balance,
			t.trust_limit,
			t.buying_liabilities,
			t.selling_liabilities,
			t.authorized,
			t.authorized_to_maintain_liabilities,
			t.clawback_enabled,
			t.ledger_sequence,
			l.closed_at,
			t.created_at,
			t.ledger_range,
			t.era_id,
			t.version_label
		FROM %s t
		INNER JOIN %s l ON t.ledger_sequence = l.sequence
		WHERE t.ledger_sequence BETWEEN $1 AND $2
		ORDER BY t.account_id, t.asset_code, t.asset_issuer, t.asset_type, t.ledger_sequence
	`, r.tableName("trustlines_snapshot_v1"), r.tableName("ledgers_row_v2"))

	rows, err := r.db.QueryContext(ctx, query, startLedger, endLedger)
	if err != nil {
		return nil, fmt.Errorf("failed to query trustlines snapshot all from cold: %w", err)
	}

	return rows, nil
}

// QueryTrustlinesSnapshot reads trustline snapshots (deduplicated)
func (r *BronzeColdReader) QueryTrustlinesSnapshot(ctx context.Context, startLedger, endLedger int64) (*sql.Rows, error) {
	query := fmt.Sprintf(`
		WITH ranked AS (
			SELECT
				account_id,
				asset_type,
				asset_issuer,
				asset_code,
				balance,
				trust_limit,
				buying_liabilities,
				selling_liabilities,
				authorized,
				authorized_to_maintain_liabilities,
				clawback_enabled,
				ledger_sequence,
				created_at,
				ledger_range,
				ROW_NUMBER() OVER (PARTITION BY account_id, asset_type, asset_code, asset_issuer ORDER BY ledger_sequence DESC) as rn
			FROM %s
			WHERE ledger_sequence BETWEEN $1 AND $2
		)
		SELECT
			account_id,
			asset_type,
			asset_issuer,
			asset_code,
			balance,
			trust_limit,
			buying_liabilities,
			selling_liabilities,
			authorized,
			authorized_to_maintain_liabilities,
			clawback_enabled,
			ledger_sequence,
			created_at,
			ledger_range
		FROM ranked
		WHERE rn = 1
	`, r.tableName("trustlines_snapshot_v1"))

	rows, err := r.db.QueryContext(ctx, query, startLedger, endLedger)
	if err != nil {
		return nil, fmt.Errorf("failed to query trustlines snapshot from cold: %w", err)
	}

	return rows, nil
}

// QueryOffersSnapshot reads offer snapshots (deduplicated)
func (r *BronzeColdReader) QueryOffersSnapshot(ctx context.Context, startLedger, endLedger int64) (*sql.Rows, error) {
	query := fmt.Sprintf(`
		WITH ranked AS (
			SELECT
				offer_id,
				seller_account,
				selling_asset_type,
				selling_asset_code,
				selling_asset_issuer,
				buying_asset_type,
				buying_asset_code,
				buying_asset_issuer,
				amount,
				price,
				flags,
				ledger_sequence,
				created_at,
				ledger_range,
				ROW_NUMBER() OVER (PARTITION BY offer_id ORDER BY ledger_sequence DESC) as rn
			FROM %s
			WHERE ledger_sequence BETWEEN $1 AND $2
		)
		SELECT
			offer_id,
			seller_account,
			selling_asset_type,
			selling_asset_code,
			selling_asset_issuer,
			buying_asset_type,
			buying_asset_code,
			buying_asset_issuer,
			amount,
			price,
			flags,
			ledger_sequence,
			created_at,
			ledger_range
		FROM ranked
		WHERE rn = 1
	`, r.tableName("offers_snapshot_v1"))

	rows, err := r.db.QueryContext(ctx, query, startLedger, endLedger)
	if err != nil {
		return nil, fmt.Errorf("failed to query offers snapshot from cold: %w", err)
	}

	return rows, nil
}

// QueryOffersSnapshotAll reads all offer snapshot changes (not deduplicated)
func (r *BronzeColdReader) QueryOffersSnapshotAll(ctx context.Context, startLedger, endLedger int64) (*sql.Rows, error) {
	query := fmt.Sprintf(`
		SELECT
			offer_id,
			seller_account,
			ledger_sequence,
			closed_at,
			selling_asset_type,
			selling_asset_code,
			selling_asset_issuer,
			buying_asset_type,
			buying_asset_code,
			buying_asset_issuer,
			amount,
			price,
			flags,
			created_at,
			ledger_range,
			era_id,
			version_label
		FROM %s
		WHERE ledger_sequence BETWEEN $1 AND $2
		ORDER BY offer_id, ledger_sequence
	`, r.tableName("offers_snapshot_v1"))

	rows, err := r.db.QueryContext(ctx, query, startLedger, endLedger)
	if err != nil {
		return nil, fmt.Errorf("failed to query offers snapshot all from cold: %w", err)
	}

	return rows, nil
}

// QueryAccountSignersSnapshotAll reads all account signer snapshot changes (not deduplicated)
func (r *BronzeColdReader) QueryAccountSignersSnapshotAll(ctx context.Context, startLedger, endLedger int64) (*sql.Rows, error) {
	query := fmt.Sprintf(`
		SELECT
			account_id,
			signer,
			ledger_sequence,
			closed_at,
			weight,
			sponsor,
			ledger_range,
			era_id,
			version_label
		FROM %s
		WHERE ledger_sequence BETWEEN $1 AND $2
		  AND deleted = false
		ORDER BY account_id, signer, ledger_sequence
	`, r.tableName("account_signers_snapshot_v1"))

	rows, err := r.db.QueryContext(ctx, query, startLedger, endLedger)
	if err != nil {
		return nil, fmt.Errorf("failed to query account signers snapshot all from cold: %w", err)
	}

	return rows, nil
}

// =============================================================================
// Contract Tables
// =============================================================================

// QueryContractInvocations reads contract invocations from Bronze Cold
func (r *BronzeColdReader) QueryContractInvocations(ctx context.Context, startLedger, endLedger int64) (*sql.Rows, error) {
	query := fmt.Sprintf(`
		SELECT
			o.ledger_sequence,
			o.transaction_index,
			o.operation_index,
			o.transaction_hash,
			o.source_account,
			o.soroban_contract_id,
			o.soroban_function,
			o.soroban_arguments_json,
			o.transaction_successful,
			o.created_at,
			o.ledger_range
		FROM %s o
		WHERE o.ledger_sequence BETWEEN $1 AND $2
		  AND o.type = 24
		  AND o.soroban_contract_id IS NOT NULL
		  AND o.soroban_function IS NOT NULL
		  AND o.soroban_arguments_json IS NOT NULL
		ORDER BY o.ledger_sequence, o.transaction_index, o.operation_index
	`, r.tableName("operations_row_v2"))

	rows, err := r.db.QueryContext(ctx, query, startLedger, endLedger)
	if err != nil {
		return nil, fmt.Errorf("failed to query contract invocations from cold: %w", err)
	}

	return rows, nil
}

// QueryContractCallGraphs reads operations with call graph data from Bronze Cold
func (r *BronzeColdReader) QueryContractCallGraphs(ctx context.Context, startLedger, endLedger int64) (*sql.Rows, error) {
	query := fmt.Sprintf(`
		SELECT
			o.ledger_sequence,
			o.transaction_index,
			o.operation_index,
			o.transaction_hash,
			o.source_account,
			o.soroban_contract_id,
			o.soroban_function,
			o.soroban_arguments_json,
			o.contract_calls_json,
			o.contracts_involved,
			o.max_call_depth,
			o.transaction_successful,
			o.created_at,
			o.ledger_range
		FROM %s o
		WHERE o.ledger_sequence BETWEEN $1 AND $2
		  AND o.type = 24
		  AND o.contract_calls_json IS NOT NULL
		  AND o.max_call_depth > 0
		ORDER BY o.ledger_sequence, o.transaction_index, o.operation_index
	`, r.tableName("operations_row_v2"))

	rows, err := r.db.QueryContext(ctx, query, startLedger, endLedger)
	if err != nil {
		return nil, fmt.Errorf("failed to query contract call graphs from cold: %w", err)
	}

	return rows, nil
}

// =============================================================================
// Additional State Tables
// =============================================================================

// QueryLiquidityPoolsSnapshot reads liquidity pool snapshots (deduplicated)
func (r *BronzeColdReader) QueryLiquidityPoolsSnapshot(ctx context.Context, startLedger, endLedger int64) (*sql.Rows, error) {
	query := fmt.Sprintf(`
		WITH ranked AS (
			SELECT
				liquidity_pool_id,
				pool_type,
				fee,
				trustline_count,
				total_pool_shares,
				asset_a_type,
				asset_a_code,
				asset_a_issuer,
				asset_a_amount,
				asset_b_type,
				asset_b_code,
				asset_b_issuer,
				asset_b_amount,
				ledger_sequence,
				closed_at,
				created_at,
				ledger_range,
				ROW_NUMBER() OVER (PARTITION BY liquidity_pool_id ORDER BY ledger_sequence DESC) as rn
			FROM %s
			WHERE ledger_sequence BETWEEN $1 AND $2
		)
		SELECT
			liquidity_pool_id,
			pool_type,
			fee,
			trustline_count,
			total_pool_shares,
			asset_a_type,
			asset_a_code,
			asset_a_issuer,
			asset_a_amount,
			asset_b_type,
			asset_b_code,
			asset_b_issuer,
			asset_b_amount,
			ledger_sequence,
			closed_at,
			created_at,
			ledger_range
		FROM ranked
		WHERE rn = 1
	`, r.tableName("liquidity_pools_snapshot_v1"))

	rows, err := r.db.QueryContext(ctx, query, startLedger, endLedger)
	if err != nil {
		return nil, fmt.Errorf("failed to query liquidity pools snapshot from cold: %w", err)
	}

	return rows, nil
}

// QueryClaimableBalancesSnapshot reads claimable balance snapshots (deduplicated)
func (r *BronzeColdReader) QueryClaimableBalancesSnapshot(ctx context.Context, startLedger, endLedger int64) (*sql.Rows, error) {
	query := fmt.Sprintf(`
		WITH ranked AS (
			SELECT
				balance_id,
				sponsor,
				asset_type,
				asset_code,
				asset_issuer,
				amount,
				claimants_count,
				flags,
				ledger_sequence,
				closed_at,
				created_at,
				ledger_range,
				ROW_NUMBER() OVER (PARTITION BY balance_id ORDER BY ledger_sequence DESC) as rn
			FROM %s
			WHERE ledger_sequence BETWEEN $1 AND $2
		)
		SELECT
			balance_id,
			sponsor,
			asset_type,
			asset_code,
			asset_issuer,
			amount,
			claimants_count,
			flags,
			ledger_sequence,
			closed_at,
			created_at,
			ledger_range
		FROM ranked
		WHERE rn = 1
	`, r.tableName("claimable_balances_snapshot_v1"))

	rows, err := r.db.QueryContext(ctx, query, startLedger, endLedger)
	if err != nil {
		return nil, fmt.Errorf("failed to query claimable balances snapshot from cold: %w", err)
	}

	return rows, nil
}

// QueryNativeBalancesSnapshot reads native balance snapshots (deduplicated)
func (r *BronzeColdReader) QueryNativeBalancesSnapshot(ctx context.Context, startLedger, endLedger int64) (*sql.Rows, error) {
	query := fmt.Sprintf(`
		WITH ranked AS (
			SELECT
				account_id,
				balance,
				buying_liabilities,
				selling_liabilities,
				num_subentries,
				num_sponsoring,
				num_sponsored,
				sequence_number,
				last_modified_ledger,
				ledger_sequence,
				ledger_range,
				ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY ledger_sequence DESC) as rn
			FROM %s
			WHERE ledger_sequence BETWEEN $1 AND $2
		)
		SELECT
			account_id,
			balance,
			buying_liabilities,
			selling_liabilities,
			num_subentries,
			num_sponsoring,
			num_sponsored,
			sequence_number,
			last_modified_ledger,
			ledger_sequence,
			ledger_range
		FROM ranked
		WHERE rn = 1
	`, r.tableName("native_balances_snapshot_v1"))

	rows, err := r.db.QueryContext(ctx, query, startLedger, endLedger)
	if err != nil {
		return nil, fmt.Errorf("failed to query native balances snapshot from cold: %w", err)
	}

	return rows, nil
}

// =============================================================================
// Event Stream Tables
// =============================================================================

// QueryTrades reads trade events from Bronze Cold
func (r *BronzeColdReader) QueryTrades(ctx context.Context, startLedger, endLedger int64) (*sql.Rows, error) {
	query := fmt.Sprintf(`
		SELECT
			ledger_sequence,
			transaction_hash,
			operation_index,
			trade_index,
			trade_type,
			trade_timestamp,
			seller_account,
			selling_asset_code,
			selling_asset_issuer,
			selling_amount,
			buyer_account,
			buying_asset_code,
			buying_asset_issuer,
			buying_amount,
			price,
			created_at,
			ledger_range
		FROM %s
		WHERE ledger_sequence BETWEEN $1 AND $2
		ORDER BY ledger_sequence, operation_index, trade_index
	`, r.tableName("trades_row_v1"))

	rows, err := r.db.QueryContext(ctx, query, startLedger, endLedger)
	if err != nil {
		return nil, fmt.Errorf("failed to query trades from cold: %w", err)
	}

	return rows, nil
}

// QueryEffects reads effect events from Bronze Cold
func (r *BronzeColdReader) QueryEffects(ctx context.Context, startLedger, endLedger int64) (*sql.Rows, error) {
	query := fmt.Sprintf(`
		SELECT
			ledger_sequence,
			transaction_hash,
			operation_index,
			effect_index,
			effect_type,
			effect_type_string,
			account_id,
			amount,
			asset_code,
			asset_issuer,
			asset_type,
			trustline_limit,
			authorize_flag,
			clawback_flag,
			signer_account,
			signer_weight,
			offer_id,
			seller_account,
			created_at,
			ledger_range
		FROM %s
		WHERE ledger_sequence BETWEEN $1 AND $2
		ORDER BY ledger_sequence, operation_index, effect_index
	`, r.tableName("effects_row_v1"))

	rows, err := r.db.QueryContext(ctx, query, startLedger, endLedger)
	if err != nil {
		return nil, fmt.Errorf("failed to query effects from cold: %w", err)
	}

	return rows, nil
}

// =============================================================================
// Soroban Tables
// =============================================================================

// QueryContractDataSnapshot reads contract data snapshots (deduplicated)
func (r *BronzeColdReader) QueryContractDataSnapshot(ctx context.Context, startLedger, endLedger int64) (*sql.Rows, error) {
	query := fmt.Sprintf(`
		WITH ranked AS (
			SELECT
				contract_id,
				ledger_key_hash AS key_hash,
				contract_durability AS durability,
				asset_type,
				asset_code,
				asset_issuer,
				contract_data_xdr AS data_value,
				last_modified_ledger,
				ledger_sequence,
				closed_at,
				created_at,
				ledger_range,
				ROW_NUMBER() OVER (PARTITION BY contract_id, ledger_key_hash ORDER BY ledger_sequence DESC) as rn
			FROM %s
			WHERE ledger_sequence BETWEEN $1 AND $2
			  AND deleted = false
		)
		SELECT
			contract_id,
			key_hash,
			durability,
			asset_type,
			asset_code,
			asset_issuer,
			data_value,
			last_modified_ledger,
			ledger_sequence,
			closed_at,
			created_at,
			ledger_range
		FROM ranked
		WHERE rn = 1
	`, r.tableName("contract_data_snapshot_v1"))

	rows, err := r.db.QueryContext(ctx, query, startLedger, endLedger)
	if err != nil {
		return nil, fmt.Errorf("failed to query contract data snapshot from cold: %w", err)
	}

	return rows, nil
}

// QueryContractCodeSnapshot reads contract code snapshots (deduplicated)
func (r *BronzeColdReader) QueryContractCodeSnapshot(ctx context.Context, startLedger, endLedger int64) (*sql.Rows, error) {
	query := fmt.Sprintf(`
		WITH ranked AS (
			SELECT
				contract_code_hash,
				contract_code_ext_v,
				n_data_segment_bytes,
				n_data_segments,
				n_elem_segments,
				n_exports,
				n_functions,
				n_globals,
				n_imports,
				n_instructions,
				n_table_entries,
				n_types,
				last_modified_ledger,
				ledger_sequence,
				closed_at,
				created_at,
				ledger_range,
				ROW_NUMBER() OVER (PARTITION BY contract_code_hash ORDER BY ledger_sequence DESC) as rn
			FROM %s
			WHERE ledger_sequence BETWEEN $1 AND $2
			  AND deleted = false
		)
		SELECT
			contract_code_hash,
			contract_code_ext_v,
			n_data_segment_bytes,
			n_data_segments,
			n_elem_segments,
			n_exports,
			n_functions,
			n_globals,
			n_imports,
			n_instructions,
			n_table_entries,
			n_types,
			last_modified_ledger,
			ledger_sequence,
			closed_at,
			created_at,
			ledger_range
		FROM ranked
		WHERE rn = 1
	`, r.tableName("contract_code_snapshot_v1"))

	rows, err := r.db.QueryContext(ctx, query, startLedger, endLedger)
	if err != nil {
		return nil, fmt.Errorf("failed to query contract code snapshot from cold: %w", err)
	}

	return rows, nil
}

// QueryTTLSnapshot reads TTL snapshots (deduplicated)
func (r *BronzeColdReader) QueryTTLSnapshot(ctx context.Context, startLedger, endLedger int64) (*sql.Rows, error) {
	query := fmt.Sprintf(`
		WITH ranked AS (
			SELECT
				key_hash,
				live_until_ledger_seq,
				ttl_remaining,
				expired,
				last_modified_ledger,
				ledger_sequence,
				closed_at,
				created_at,
				ledger_range,
				ROW_NUMBER() OVER (PARTITION BY key_hash ORDER BY ledger_sequence DESC) as rn
			FROM %s
			WHERE ledger_sequence BETWEEN $1 AND $2
			  AND deleted = false
		)
		SELECT
			key_hash,
			live_until_ledger_seq,
			ttl_remaining,
			expired,
			last_modified_ledger,
			ledger_sequence,
			closed_at,
			created_at,
			ledger_range
		FROM ranked
		WHERE rn = 1
	`, r.tableName("ttl_snapshot_v1"))

	rows, err := r.db.QueryContext(ctx, query, startLedger, endLedger)
	if err != nil {
		return nil, fmt.Errorf("failed to query TTL snapshot from cold: %w", err)
	}

	return rows, nil
}

// QueryEvictedKeys reads evicted key events from Bronze Cold
func (r *BronzeColdReader) QueryEvictedKeys(ctx context.Context, startLedger, endLedger int64) (*sql.Rows, error) {
	query := fmt.Sprintf(`
		SELECT
			contract_id,
			key_hash,
			ledger_sequence,
			closed_at,
			created_at,
			ledger_range
		FROM %s
		WHERE ledger_sequence BETWEEN $1 AND $2
		ORDER BY ledger_sequence, contract_id, key_hash
	`, r.tableName("evicted_keys_state_v1"))

	rows, err := r.db.QueryContext(ctx, query, startLedger, endLedger)
	if err != nil {
		return nil, fmt.Errorf("failed to query evicted keys from cold: %w", err)
	}

	return rows, nil
}

// QueryRestoredKeys reads restored key events from Bronze Cold
func (r *BronzeColdReader) QueryRestoredKeys(ctx context.Context, startLedger, endLedger int64) (*sql.Rows, error) {
	query := fmt.Sprintf(`
		SELECT
			contract_id,
			key_hash,
			ledger_sequence,
			closed_at,
			created_at,
			ledger_range
		FROM %s
		WHERE ledger_sequence BETWEEN $1 AND $2
		ORDER BY ledger_sequence, contract_id, key_hash
	`, r.tableName("restored_keys_state_v1"))

	rows, err := r.db.QueryContext(ctx, query, startLedger, endLedger)
	if err != nil {
		return nil, fmt.Errorf("failed to query restored keys from cold: %w", err)
	}

	return rows, nil
}

// =============================================================================
// Config Settings
// =============================================================================

// QueryConfigSettingsSnapshot reads config settings snapshots (deduplicated)
func (r *BronzeColdReader) QueryConfigSettingsSnapshot(ctx context.Context, startLedger, endLedger int64) (*sql.Rows, error) {
	query := fmt.Sprintf(`
		WITH ranked AS (
			SELECT
				config_setting_id,
				ledger_max_instructions,
				tx_max_instructions,
				fee_rate_per_instructions_increment,
				tx_memory_limit,
				ledger_max_read_ledger_entries,
				ledger_max_read_bytes,
				ledger_max_write_ledger_entries,
				ledger_max_write_bytes,
				tx_max_read_ledger_entries,
				tx_max_read_bytes,
				tx_max_write_ledger_entries,
				tx_max_write_bytes,
				contract_max_size_bytes,
				config_setting_xdr,
				last_modified_ledger,
				ledger_sequence,
				closed_at,
				created_at,
				ledger_range,
				ROW_NUMBER() OVER (PARTITION BY config_setting_id ORDER BY ledger_sequence DESC) as rn
			FROM %s
			WHERE ledger_sequence BETWEEN $1 AND $2
			  AND deleted = false
		)
		SELECT
			config_setting_id,
			ledger_max_instructions,
			tx_max_instructions,
			fee_rate_per_instructions_increment,
			tx_memory_limit,
			ledger_max_read_ledger_entries,
			ledger_max_read_bytes,
			ledger_max_write_ledger_entries,
			ledger_max_write_bytes,
			tx_max_read_ledger_entries,
			tx_max_read_bytes,
			tx_max_write_ledger_entries,
			tx_max_write_bytes,
			contract_max_size_bytes,
			config_setting_xdr,
			last_modified_ledger,
			ledger_sequence,
			closed_at,
			created_at,
			ledger_range
		FROM ranked
		WHERE rn = 1
	`, r.tableName("config_settings_snapshot_v1"))

	rows, err := r.db.QueryContext(ctx, query, startLedger, endLedger)
	if err != nil {
		return nil, fmt.Errorf("failed to query config settings snapshot from cold: %w", err)
	}

	return rows, nil
}
