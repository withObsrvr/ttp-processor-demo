package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
)

// ContractEventSummary represents aggregated contract events for a ledger
type ContractEventSummary struct {
	ContractID     string
	LedgerSequence int64
	EventCount     int32
}

// BronzeHotReader reads contract events from Bronze Hot (PostgreSQL)
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
		FROM contract_events_stream_v1
	`

	err := br.db.QueryRowContext(ctx, query).Scan(&maxSeq)
	if err != nil {
		return 0, fmt.Errorf("failed to get max ledger sequence: %w", err)
	}

	if !maxSeq.Valid {
		return 0, nil // No contract events yet
	}

	return maxSeq.Int64, nil
}

// ReadContractEventSummaries reads aggregated contract event data for a ledger range
// Groups by (contract_id, ledger_sequence) and counts events per combination
func (br *BronzeHotReader) ReadContractEventSummaries(ctx context.Context, startLedger, endLedger int64) ([]ContractEventSummary, error) {
	query := `
		SELECT
			contract_id,
			ledger_sequence,
			COUNT(*) as event_count
		FROM contract_events_stream_v1
		WHERE ledger_sequence > $1 AND ledger_sequence <= $2
			AND contract_id IS NOT NULL
		GROUP BY contract_id, ledger_sequence
		ORDER BY ledger_sequence, contract_id
	`

	rows, err := br.db.QueryContext(ctx, query, startLedger, endLedger)
	if err != nil {
		return nil, fmt.Errorf("failed to query contract events: %w", err)
	}
	defer rows.Close()

	var summaries []ContractEventSummary
	for rows.Next() {
		var summary ContractEventSummary
		err := rows.Scan(
			&summary.ContractID,
			&summary.LedgerSequence,
			&summary.EventCount,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan contract event summary: %w", err)
		}

		summaries = append(summaries, summary)
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating contract event summaries: %w", err)
	}

	log.Printf("📊 Read %d contract-ledger pairs from Bronze Hot (ledgers %d-%d)", len(summaries), startLedger, endLedger)
	return summaries, nil
}

// Close closes the database connection
func (br *BronzeHotReader) Close() error {
	if br.db != nil {
		return br.db.Close()
	}
	return nil
}
