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

func (sr *SilverHotReader) Close() error {
	if sr.db != nil {
		return sr.db.Close()
	}
	return nil
}
