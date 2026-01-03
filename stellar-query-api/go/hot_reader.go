package main

import (
	"context"
	"database/sql"
	"fmt"

	_ "github.com/lib/pq"
)

type HotReader struct {
	db *sql.DB
}

func NewHotReader(config PostgresConfig) (*HotReader, error) {
	dsn := config.DSN()
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open PostgreSQL connection: %w", err)
	}

	if config.MaxConnections > 0 {
		db.SetMaxOpenConns(config.MaxConnections)
	}

	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping PostgreSQL: %w", err)
	}

	return &HotReader{db: db}, nil
}

func (h *HotReader) GetHighWatermark() (int64, error) {
	var watermark sql.NullInt64
	err := h.db.QueryRow("SELECT MAX(sequence) FROM ledgers_row_v2").Scan(&watermark)
	if err != nil {
		return 0, fmt.Errorf("failed to get high watermark: %w", err)
	}
	if !watermark.Valid {
		return 0, nil // No data yet
	}
	return watermark.Int64, nil
}

func (h *HotReader) GetLowWatermark() (int64, error) {
	var watermark sql.NullInt64
	err := h.db.QueryRow("SELECT MIN(sequence) FROM ledgers_row_v2").Scan(&watermark)
	if err != nil {
		return 0, fmt.Errorf("failed to get low watermark: %w", err)
	}
	if !watermark.Valid {
		return 0, nil // No data yet
	}
	return watermark.Int64, nil
}

func (h *HotReader) QueryLedgers(ctx context.Context, start, end int64, limit int) (*sql.Rows, error) {
	query := `
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
			created_at
		FROM ledgers_row_v2
		WHERE sequence >= $1 AND sequence <= $2
		ORDER BY sequence ASC
		LIMIT $3
	`

	rows, err := h.db.QueryContext(ctx, query, start, end, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to query ledgers: %w", err)
	}

	return rows, nil
}

func (h *HotReader) QueryTransactions(ctx context.Context, start, end int64, limit int) (*sql.Rows, error) {
	query := `
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
		FROM transactions_row_v2
		WHERE ledger_sequence >= $1 AND ledger_sequence <= $2
		ORDER BY ledger_sequence ASC, transaction_hash ASC
		LIMIT $3
	`

	rows, err := h.db.QueryContext(ctx, query, start, end, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to query transactions: %w", err)
	}

	return rows, nil
}

func (h *HotReader) QueryOperations(ctx context.Context, start, end int64, limit int) (*sql.Rows, error) {
	query := `
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
		FROM operations_row_v2
		WHERE ledger_sequence >= $1 AND ledger_sequence <= $2
		ORDER BY ledger_sequence ASC, transaction_hash ASC, operation_index ASC
		LIMIT $3
	`

	rows, err := h.db.QueryContext(ctx, query, start, end, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to query operations: %w", err)
	}

	return rows, nil
}

func (h *HotReader) QueryEffects(ctx context.Context, start, end int64, limit int) (*sql.Rows, error) {
	query := `
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
		FROM effects_row_v1
		WHERE ledger_sequence >= $1 AND ledger_sequence <= $2
		ORDER BY ledger_sequence ASC, transaction_hash ASC, operation_index ASC, effect_index ASC
		LIMIT $3
	`

	rows, err := h.db.QueryContext(ctx, query, start, end, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to query effects: %w", err)
	}

	return rows, nil
}

func (h *HotReader) QueryTrades(ctx context.Context, start, end int64, limit int) (*sql.Rows, error) {
	query := `
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
		FROM trades_row_v1
		WHERE ledger_sequence >= $1 AND ledger_sequence <= $2
		ORDER BY ledger_sequence ASC, transaction_hash ASC, operation_index ASC, trade_index ASC
		LIMIT $3
	`

	rows, err := h.db.QueryContext(ctx, query, start, end, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to query trades: %w", err)
	}

	return rows, nil
}

func (h *HotReader) QueryAccounts(ctx context.Context, accountID string, limit int) (*sql.Rows, error) {
	query := `
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
		FROM accounts_snapshot_v1
		WHERE account_id = $1
		ORDER BY ledger_sequence DESC
		LIMIT $2
	`

	rows, err := h.db.QueryContext(ctx, query, accountID, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to query accounts: %w", err)
	}

	return rows, nil
}

func (h *HotReader) QueryTrustlines(ctx context.Context, accountID string, limit int) (*sql.Rows, error) {
	query := `
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
		FROM trustlines_snapshot_v1
		WHERE account_id = $1
		ORDER BY ledger_sequence DESC
		LIMIT $2
	`

	rows, err := h.db.QueryContext(ctx, query, accountID, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to query trustlines: %w", err)
	}

	return rows, nil
}

func (h *HotReader) QueryOffers(ctx context.Context, sellerID string, limit int) (*sql.Rows, error) {
	query := `
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
		FROM offers_snapshot_v1
		WHERE seller_account = $1
		ORDER BY ledger_sequence DESC
		LIMIT $2
	`

	rows, err := h.db.QueryContext(ctx, query, sellerID, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to query offers: %w", err)
	}

	return rows, nil
}

func (h *HotReader) QueryContractEvents(ctx context.Context, start, end int64, limit int) (*sql.Rows, error) {
	query := `
		SELECT
			event_id,
			contract_id,
			ledger_sequence,
			transaction_hash,
			operation_index,
			event_index,
			event_type,
			topics_json,
			data_json,
			closed_at,
			ledger_range,
			era_id,
			version_label
		FROM contract_events_stream_v1
		WHERE ledger_sequence >= $1 AND ledger_sequence <= $2
		ORDER BY ledger_sequence ASC, transaction_hash ASC, operation_index ASC, event_index ASC
		LIMIT $3
	`

	rows, err := h.db.QueryContext(ctx, query, start, end, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to query contract events: %w", err)
	}

	return rows, nil
}

func (h *HotReader) Close() error {
	return h.db.Close()
}
