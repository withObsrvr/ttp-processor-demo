package main

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	_ "github.com/duckdb/duckdb-go/v2"
)

type ColdReader struct {
	db     *sql.DB
	config DuckLakeConfig
}

func NewColdReader(config DuckLakeConfig) (*ColdReader, error) {
	// Open DuckDB connection (in-memory)
	db, err := sql.Open("duckdb", "")
	if err != nil {
		return nil, fmt.Errorf("failed to open DuckDB: %w", err)
	}

	reader := &ColdReader{
		db:     db,
		config: config,
	}

	// Initialize DuckLake connection
	if err := reader.initialize(context.Background()); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to initialize DuckLake: %w", err)
	}

	return reader, nil
}

func (c *ColdReader) initialize(ctx context.Context) error {
	// Install required extensions
	if _, err := c.db.ExecContext(ctx, "INSTALL ducklake;"); err != nil {
		return fmt.Errorf("failed to install ducklake extension: %w", err)
	}
	if _, err := c.db.ExecContext(ctx, "LOAD ducklake;"); err != nil {
		return fmt.Errorf("failed to load ducklake extension: %w", err)
	}
	if _, err := c.db.ExecContext(ctx, "INSTALL httpfs;"); err != nil {
		return fmt.Errorf("failed to install httpfs extension: %w", err)
	}
	if _, err := c.db.ExecContext(ctx, "LOAD httpfs;"); err != nil {
		return fmt.Errorf("failed to load httpfs extension: %w", err)
	}

	// Remove https:// prefix from endpoint (DuckDB adds it automatically)
	endpoint := c.config.AWSEndpoint
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
	`, c.config.AWSAccessKeyID, c.config.AWSSecretAccessKey, c.config.AWSRegion, endpoint)

	if _, err := c.db.ExecContext(ctx, s3ConfigSQL); err != nil {
		return fmt.Errorf("failed to configure S3 credentials: %w", err)
	}

	// Attach DuckLake catalog
	attachSQL := fmt.Sprintf(`
		ATTACH '%s'
		AS %s
		(DATA_PATH '%s', METADATA_SCHEMA '%s');
	`, c.config.CatalogPath, c.config.CatalogName, c.config.DataPath, c.config.MetadataSchema)

	if _, err := c.db.ExecContext(ctx, attachSQL); err != nil {
		return fmt.Errorf("failed to attach DuckLake catalog: %w", err)
	}

	return nil
}

func (c *ColdReader) QueryLedgers(ctx context.Context, start, end int64, limit int, sort string) (*sql.Rows, error) {
	// Map sort parameter to ORDER BY clause
	orderBy := "sequence ASC" // default
	switch sort {
	case "sequence_desc":
		orderBy = "sequence DESC"
	case "closed_at_asc":
		orderBy = "closed_at ASC"
	case "closed_at_desc":
		orderBy = "closed_at DESC"
	case "tx_count_desc":
		orderBy = "transaction_count DESC, sequence DESC"
	}

	query := fmt.Sprintf(`
		SELECT
			sequence,
			ledger_hash,
			previous_ledger_hash,
			transaction_count,
			operation_count,
			successful_tx_count,
			failed_tx_count,
			tx_set_operation_count,
			closed_at,
			total_coins,
			fee_pool,
			base_fee,
			base_reserve,
			max_tx_set_size,
			protocol_version,
			ledger_header,
			soroban_fee_write1kb as soroban_fee_write_1kb,
			node_id,
			signature,
			ledger_range,
			era_id,
			version_label,
			ingestion_timestamp as created_at
		FROM %s.%s.ledgers_row_v2
		WHERE sequence >= ? AND sequence <= ?
		ORDER BY %s
		LIMIT ?
	`, c.config.CatalogName, c.config.SchemaName, orderBy)

	rows, err := c.db.QueryContext(ctx, query, start, end, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to query ledgers from DuckLake: %w", err)
	}

	return rows, nil
}

func (c *ColdReader) QueryTransactions(ctx context.Context, start, end int64, limit int) (*sql.Rows, error) {
	query := fmt.Sprintf(`
		SELECT
			ledger_sequence,
			transaction_hash,
			source_account,
			source_account_muxed,
			account_sequence,
			max_fee,
			operation_count,
			created_at,
			ledger_range,
			era_id,
			version_label,
			successful
		FROM %s.%s.transactions_row_v2
		WHERE ledger_sequence >= ? AND ledger_sequence <= ?
		ORDER BY ledger_sequence ASC, transaction_hash ASC
		LIMIT ?
	`, c.config.CatalogName, c.config.SchemaName)

	rows, err := c.db.QueryContext(ctx, query, start, end, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to query transactions from DuckLake: %w", err)
	}

	return rows, nil
}

func (c *ColdReader) QueryOperations(ctx context.Context, start, end int64, limit int) (*sql.Rows, error) {
	query := fmt.Sprintf(`
		SELECT
			ledger_sequence,
			transaction_hash,
			operation_index,
			type,
			source_account,
			source_account_muxed,
			created_at,
			ledger_range,
			era_id,
			version_label
		FROM %s.%s.operations_row_v2
		WHERE ledger_sequence >= ? AND ledger_sequence <= ?
		ORDER BY ledger_sequence ASC, transaction_hash ASC, operation_index ASC
		LIMIT ?
	`, c.config.CatalogName, c.config.SchemaName)

	rows, err := c.db.QueryContext(ctx, query, start, end, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to query operations from DuckLake: %w", err)
	}

	return rows, nil
}

func (c *ColdReader) QueryEffects(ctx context.Context, start, end int64, limit int) (*sql.Rows, error) {
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
			ledger_range,
			era_id,
			version_label
		FROM %s.%s.effects_row_v1
		WHERE ledger_sequence >= ? AND ledger_sequence <= ?
		ORDER BY ledger_sequence ASC, transaction_hash ASC, operation_index ASC, effect_index ASC
		LIMIT ?
	`, c.config.CatalogName, c.config.SchemaName)

	rows, err := c.db.QueryContext(ctx, query, start, end, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to query effects from DuckLake: %w", err)
	}

	return rows, nil
}

func (c *ColdReader) QueryTrades(ctx context.Context, start, end int64, limit int) (*sql.Rows, error) {
	query := fmt.Sprintf(`
		SELECT
			ledger_sequence,
			transaction_hash,
			operation_index,
			trade_index,
			seller_account,
			selling_asset_code,
			selling_asset_issuer,
			selling_amount,
			buyer_account,
			buying_asset_code,
			buying_asset_issuer,
			buying_amount,
			price,
			ledger_range,
			era_id,
			version_label
		FROM %s.%s.trades_row_v1
		WHERE ledger_sequence >= ? AND ledger_sequence <= ?
		ORDER BY ledger_sequence ASC, transaction_hash ASC, operation_index ASC, trade_index ASC
		LIMIT ?
	`, c.config.CatalogName, c.config.SchemaName)

	rows, err := c.db.QueryContext(ctx, query, start, end, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to query trades from DuckLake: %w", err)
	}

	return rows, nil
}

func (c *ColdReader) QueryAccounts(ctx context.Context, accountID string, limit int) (*sql.Rows, error) {
	query := fmt.Sprintf(`
		SELECT
			account_id,
			balance,
			sequence_number,
			num_subentries,
			flags,
			home_domain,
			master_weight,
			low_threshold,
			med_threshold,
			high_threshold,
			ledger_sequence,
			num_sponsoring,
			num_sponsored,
			sponsor_account,
			signers,
			ledger_range,
			era_id,
			version_label,
			created_at
		FROM %s.%s.accounts_snapshot_v1
		WHERE account_id = ?
		ORDER BY ledger_sequence DESC
		LIMIT ?
	`, c.config.CatalogName, c.config.SchemaName)

	rows, err := c.db.QueryContext(ctx, query, accountID, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to query accounts from DuckLake: %w", err)
	}

	return rows, nil
}

func (c *ColdReader) QueryTrustlines(ctx context.Context, accountID string, limit int) (*sql.Rows, error) {
	query := fmt.Sprintf(`
		SELECT
			account_id,
			asset_code,
			asset_issuer,
			asset_type,
			balance,
			trust_limit,
			buying_liabilities,
			selling_liabilities,
			ledger_sequence,
			ledger_range,
			era_id,
			version_label,
			created_at
		FROM %s.%s.trustlines_snapshot_v1
		WHERE account_id = ?
		ORDER BY ledger_sequence DESC
		LIMIT ?
	`, c.config.CatalogName, c.config.SchemaName)

	rows, err := c.db.QueryContext(ctx, query, accountID, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to query trustlines from DuckLake: %w", err)
	}

	return rows, nil
}

func (c *ColdReader) QueryOffers(ctx context.Context, sellerID string, limit int) (*sql.Rows, error) {
	query := fmt.Sprintf(`
		SELECT
			seller_account,
			offer_id,
			selling_asset_code,
			selling_asset_issuer,
			selling_asset_type,
			buying_asset_code,
			buying_asset_issuer,
			buying_asset_type,
			amount,
			price,
			flags,
			ledger_sequence,
			ledger_range,
			era_id,
			version_label,
			created_at
		FROM %s.%s.offers_snapshot_v1
		WHERE seller_account = ?
		ORDER BY ledger_sequence DESC
		LIMIT ?
	`, c.config.CatalogName, c.config.SchemaName)

	rows, err := c.db.QueryContext(ctx, query, sellerID, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to query offers from DuckLake: %w", err)
	}

	return rows, nil
}

func (c *ColdReader) QueryContractEvents(ctx context.Context, start, end int64, limit int) (*sql.Rows, error) {
	query := fmt.Sprintf(`
		SELECT
			event_id,
			contract_id,
			ledger_sequence,
			transaction_hash,
			operation_index,
			event_index,
			event_type,
			topics_json,
			data_decoded,
			closed_at,
			ledger_range,
			era_id,
			version_label
		FROM %s.%s.contract_events_stream_v1
		WHERE ledger_sequence >= ? AND ledger_sequence <= ?
		ORDER BY ledger_sequence ASC, transaction_hash ASC, operation_index ASC, event_index ASC
		LIMIT ?
	`, c.config.CatalogName, c.config.SchemaName)

	rows, err := c.db.QueryContext(ctx, query, start, end, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to query contract events from DuckLake: %w", err)
	}

	return rows, nil
}

// GetDistinctAccountCount returns the count of unique accounts in Bronze storage
func (c *ColdReader) GetDistinctAccountCount(ctx context.Context) (int64, error) {
	query := fmt.Sprintf(`
		SELECT COUNT(DISTINCT account_id)
		FROM %s.%s.accounts_snapshot_v1
	`, c.config.CatalogName, c.config.SchemaName)

	var count int64
	err := c.db.QueryRowContext(ctx, query).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("failed to count distinct accounts: %w", err)
	}

	return count, nil
}

func (c *ColdReader) Close() error {
	return c.db.Close()
}
