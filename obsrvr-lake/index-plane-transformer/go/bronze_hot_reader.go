package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
)

// BronzeHotReader reads transactions from Bronze Hot (stellar_hot PostgreSQL)
type BronzeHotReader struct {
	db *sql.DB
}

// NewBronzeHotReader creates a new Bronze Hot reader
func NewBronzeHotReader(db *sql.DB) *BronzeHotReader {
	return &BronzeHotReader{db: db}
}

// GetMaxLedgerSequence returns the maximum ledger sequence available in Bronze Hot
func (br *BronzeHotReader) GetMaxLedgerSequence(ctx context.Context) (int64, error) {
	var maxSeq sql.NullInt64

	query := `
		SELECT MAX(ledger_sequence)
		FROM transactions_row_v2
	`

	err := br.db.QueryRowContext(ctx, query).Scan(&maxSeq)
	if err != nil {
		return 0, fmt.Errorf("failed to get max ledger sequence: %w", err)
	}

	if !maxSeq.Valid {
		return 0, nil // No transactions yet
	}

	return maxSeq.Int64, nil
}

// ReadTransactions reads transactions from Bronze Hot for a ledger range
func (br *BronzeHotReader) ReadTransactions(ctx context.Context, startLedger, endLedger int64) ([]TransactionIndex, error) {
	query := `
		SELECT
			transaction_hash,
			ledger_sequence,
			operation_count,
			successful,
			created_at
		FROM transactions_row_v2
		WHERE ledger_sequence > $1 AND ledger_sequence <= $2
		ORDER BY ledger_sequence, transaction_hash
	`

	rows, err := br.db.QueryContext(ctx, query, startLedger, endLedger)
	if err != nil {
		return nil, fmt.Errorf("failed to query transactions: %w", err)
	}
	defer rows.Close()

	var transactions []TransactionIndex
	for rows.Next() {
		var tx TransactionIndex
		err := rows.Scan(
			&tx.TxHash,
			&tx.LedgerSequence,
			&tx.OperationCount,
			&tx.Successful,
			&tx.ClosedAt,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan transaction: %w", err)
		}

		// Calculate ledger range partition
		tx.LedgerRange = tx.LedgerSequence / 100000

		transactions = append(transactions, tx)
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating transactions: %w", err)
	}

	log.Printf("📊 Read %d transactions from stellar_hot (ledgers %d-%d)", len(transactions), startLedger, endLedger)
	return transactions, nil
}

// Close closes the database connection
func (br *BronzeHotReader) Close() error {
	if br.db != nil {
		return br.db.Close()
	}
	return nil
}
