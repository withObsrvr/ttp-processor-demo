package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
)

// SilverHotReader reads normalized Silver participant tables for account indexes.
type SilverHotReader struct {
	db *sql.DB
}

func NewSilverHotReader(db *sql.DB) *SilverHotReader {
	return &SilverHotReader{db: db}
}

func (sr *SilverHotReader) GetLedgerBounds(ctx context.Context) (int64, int64, error) {
	var minSeq sql.NullInt64
	var maxSeq sql.NullInt64
	query := `
		SELECT MIN(ledger_sequence), MAX(ledger_sequence)
		FROM enriched_history_operations
	`
	if err := sr.db.QueryRowContext(ctx, query).Scan(&minSeq, &maxSeq); err != nil {
		return 0, 0, fmt.Errorf("failed to get silver ledger bounds: %w", err)
	}
	if !minSeq.Valid || !maxSeq.Valid {
		return 0, 0, nil
	}
	return minSeq.Int64, maxSeq.Int64, nil
}

func (sr *SilverHotReader) ReadAccountLedgerRanges(ctx context.Context, startLedger, endLedger, partitionSize int64, bucketCount int) ([]AccountLedgerIndex, error) {
	query := buildAccountLedgerRangesQuery(partitionSize)

	rows, err := sr.db.QueryContext(ctx, query, startLedger, endLedger)
	if err != nil {
		return nil, fmt.Errorf("failed to query account ledger ranges: %w", err)
	}
	defer rows.Close()

	var out []AccountLedgerIndex
	for rows.Next() {
		var item AccountLedgerIndex
		if err := rows.Scan(&item.AccountID, &item.LedgerRange); err != nil {
			return nil, fmt.Errorf("failed to scan account ledger range: %w", err)
		}
		item.AccountBucket = AccountBucket(item.AccountID, bucketCount)
		out = append(out, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating account ledger ranges: %w", err)
	}
	log.Printf("📊 Read %d account ledger-range rows from silver_hot (ledgers %d-%d)", len(out), startLedger, endLedger)
	return out, nil
}

func buildAccountLedgerRangesQuery(partitionSize int64) string {
	return fmt.Sprintf(`
		SELECT DISTINCT account_id, CAST(ledger_sequence / %d AS BIGINT) AS ledger_range
		FROM (
			SELECT source_account AS account_id, ledger_sequence
			FROM enriched_history_operations
			WHERE ledger_sequence > $1 AND ledger_sequence <= $2 AND source_account IS NOT NULL AND source_account <> ''
			UNION ALL
			SELECT destination AS account_id, ledger_sequence
			FROM enriched_history_operations
			WHERE ledger_sequence > $1 AND ledger_sequence <= $2 AND destination IS NOT NULL AND destination <> ''
			UNION ALL
			SELECT from_account AS account_id, ledger_sequence
			FROM enriched_history_operations
			WHERE ledger_sequence > $1 AND ledger_sequence <= $2 AND from_account IS NOT NULL AND from_account <> ''
			UNION ALL
			SELECT to_address AS account_id, ledger_sequence
			FROM enriched_history_operations
			WHERE ledger_sequence > $1 AND ledger_sequence <= $2 AND to_address IS NOT NULL AND to_address <> ''
			UNION ALL
			SELECT address AS account_id, ledger_sequence
			FROM enriched_history_operations
			WHERE ledger_sequence > $1 AND ledger_sequence <= $2 AND address IS NOT NULL AND address <> ''
			UNION ALL
			SELECT from_account AS account_id, ledger_sequence
			FROM token_transfers_raw
			WHERE ledger_sequence > $1 AND ledger_sequence <= $2 AND from_account IS NOT NULL AND from_account <> ''
			UNION ALL
			SELECT to_account AS account_id, ledger_sequence
			FROM token_transfers_raw
			WHERE ledger_sequence > $1 AND ledger_sequence <= $2 AND to_account IS NOT NULL AND to_account <> ''
			UNION ALL
			SELECT source_account AS account_id, ledger_sequence
			FROM contract_invocations_raw
			WHERE ledger_sequence > $1 AND ledger_sequence <= $2 AND source_account IS NOT NULL AND source_account <> ''
		) participants
	`, partitionSize)
}

func (sr *SilverHotReader) ReadAccountFeedRows(ctx context.Context, startLedger, endLedger int64) ([]AccountFeedRow, error) {
	query := buildAccountFeedRowsQuery()
	rows, err := sr.db.QueryContext(ctx, query, startLedger, endLedger)
	if err != nil {
		return nil, fmt.Errorf("failed to query account feed rows: %w", err)
	}
	defer rows.Close()

	var out []AccountFeedRow
	for rows.Next() {
		var row AccountFeedRow
		if err := rows.Scan(
			&row.AccountID,
			&row.SourceMask,
			&row.TOID,
			&row.OperationTOID,
			&row.TxHash,
			&row.LedgerSequence,
			&row.LedgerClosedAt,
			&row.Successful,
			&row.ActivityType,
			&row.SourceAccount,
			&row.DestinationAccount,
			&row.PrimaryContractID,
			&row.OperationCount,
			&row.FeeCharged,
			&row.MemoType,
			&row.MemoValue,
			&row.OperationIndex,
			&row.OperationType,
			&row.OperationTypeName,
			&row.AssetKey,
			&row.AmountStroops,
			&row.ContractID,
			&row.FunctionName,
			&row.IsPaymentOp,
			&row.IsSorobanOp,
		); err != nil {
			return nil, fmt.Errorf("failed to scan account feed row: %w", err)
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating account feed rows: %w", err)
	}
	log.Printf("📊 Read %d account serving-feed rows from silver_hot (ledgers %d-%d)", len(out), startLedger, endLedger)
	return out, nil
}

func buildAccountFeedRowsQuery() string {
	return `
		WITH enriched AS (
			SELECT
				e.*
			FROM enriched_history_operations e
			WHERE e.ledger_sequence > $1 AND e.ledger_sequence <= $2
				AND e.transaction_id IS NOT NULL
				AND e.operation_id IS NOT NULL
		),
		participants AS (
			SELECT source_account AS account_id, 1::SMALLINT AS source_mask, * FROM enriched
			WHERE source_account IS NOT NULL AND source_account <> ''
			UNION ALL
			SELECT destination AS account_id, 2::SMALLINT AS source_mask, * FROM enriched
			WHERE destination IS NOT NULL AND destination <> ''
			UNION ALL
			SELECT from_account AS account_id, 4::SMALLINT AS source_mask, * FROM enriched
			WHERE from_account IS NOT NULL AND from_account <> ''
			UNION ALL
			SELECT to_address AS account_id, 8::SMALLINT AS source_mask, * FROM enriched
			WHERE to_address IS NOT NULL AND to_address <> ''
			UNION ALL
			SELECT address AS account_id, 16::SMALLINT AS source_mask, * FROM enriched
			WHERE address IS NOT NULL AND address <> ''
			UNION ALL
			SELECT into_account AS account_id, 32::SMALLINT AS source_mask, * FROM enriched
			WHERE into_account IS NOT NULL AND into_account <> ''
		)
		SELECT DISTINCT ON (account_id, transaction_hash, operation_index)
			account_id,
			source_mask,
			transaction_id AS toid,
			operation_id AS operation_toid,
			transaction_hash,
			ledger_sequence,
			COALESCE(ledger_closed_at, created_at) AS ledger_closed_at,
			COALESCE(tx_successful, transaction_successful, false) AS successful,
			CASE
				WHEN is_soroban_op THEN 'contract'
				WHEN is_payment_op THEN 'payment'
				WHEN type_string IS NOT NULL AND type_string <> '' THEN type_string
				ELSE 'operation'
			END AS activity_type,
			source_account,
			COALESCE(destination, to_address, into_account) AS destination_account,
			contract_id AS primary_contract_id,
			tx_operation_count AS operation_count,
			tx_fee_charged AS fee_charged,
			tx_memo_type AS memo_type,
			tx_memo AS memo_value,
			operation_index::BIGINT AS operation_index,
			type::BIGINT AS operation_type,
			type_string AS operation_type_name,
			COALESCE(asset, source_asset, selling_asset, buying_asset) AS asset_key,
			COALESCE(amount, source_amount) AS amount_stroops,
			contract_id,
			function_name,
			COALESCE(is_payment_op, false) AS is_payment_op,
			COALESCE(is_soroban_op, false) AS is_soroban_op
		FROM participants
		ORDER BY account_id, transaction_hash, operation_index, source_mask
	`
}

func (sr *SilverHotReader) Close() error {
	if sr.db != nil {
		return sr.db.Close()
	}
	return nil
}
