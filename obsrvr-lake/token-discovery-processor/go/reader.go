package main

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/lib/pq"
)

// SilverReader reads contract activity from the Silver layer
type SilverReader struct {
	db *sql.DB
}

// NewSilverReader creates a new silver layer reader
func NewSilverReader(db *sql.DB) *SilverReader {
	return &SilverReader{db: db}
}

// GetContractActivity returns contract activity since the last checkpoint
// Groups by contract_id and aggregates function names and transfer counts
func (r *SilverReader) GetContractActivity(ctx context.Context, startLedger, endLedger int64, batchSize int) ([]ContractActivity, error) {
	// Query contract_events_stream_v1 for contract calls
	// This table contains Soroban contract invocations with function names
	query := `
		SELECT
			contract_id,
			ARRAY_AGG(DISTINCT function_name) FILTER (WHERE function_name IS NOT NULL) as functions,
			MIN(ledger_sequence) as first_seen,
			MAX(ledger_sequence) as last_seen,
			COUNT(*) as call_count
		FROM contract_events_stream_v1
		WHERE ledger_sequence > $1
		  AND ledger_sequence <= $2
		  AND contract_id IS NOT NULL
		GROUP BY contract_id
		LIMIT $3
	`

	rows, err := r.db.QueryContext(ctx, query, startLedger, endLedger, batchSize)
	if err != nil {
		return nil, fmt.Errorf("failed to query contract activity: %w", err)
	}
	defer rows.Close()

	var activities []ContractActivity
	for rows.Next() {
		var activity ContractActivity
		var functions pq.StringArray

		if err := rows.Scan(
			&activity.ContractID,
			&functions,
			&activity.FirstSeen,
			&activity.LastSeen,
			&activity.CallCount,
		); err != nil {
			return nil, fmt.Errorf("failed to scan contract activity: %w", err)
		}

		activity.Functions = []string(functions)
		activities = append(activities, activity)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating contract activity: %w", err)
	}

	return activities, nil
}

// GetTransferCounts returns transfer counts for contracts from token_transfers table
func (r *SilverReader) GetTransferCounts(ctx context.Context, contractIDs []string) (map[string]int64, error) {
	if len(contractIDs) == 0 {
		return make(map[string]int64), nil
	}

	query := `
		SELECT
			token_contract_id,
			COUNT(*) as transfer_count
		FROM token_transfers_raw
		WHERE token_contract_id = ANY($1)
		GROUP BY token_contract_id
	`

	rows, err := r.db.QueryContext(ctx, query, pq.Array(contractIDs))
	if err != nil {
		return nil, fmt.Errorf("failed to query transfer counts: %w", err)
	}
	defer rows.Close()

	counts := make(map[string]int64)
	for rows.Next() {
		var contractID string
		var count int64
		if err := rows.Scan(&contractID, &count); err != nil {
			return nil, fmt.Errorf("failed to scan transfer count: %w", err)
		}
		counts[contractID] = count
	}

	return counts, nil
}

// GetMaxLedger returns the maximum ledger sequence in the source database
func (r *SilverReader) GetMaxLedger(ctx context.Context) (int64, error) {
	query := `SELECT COALESCE(MAX(ledger_sequence), 0) FROM contract_events_stream_v1`

	var maxLedger int64
	err := r.db.QueryRowContext(ctx, query).Scan(&maxLedger)
	if err != nil {
		return 0, fmt.Errorf("failed to get max ledger: %w", err)
	}

	return maxLedger, nil
}

// GetHolderCounts returns holder counts for contracts (unique addresses with balance)
func (r *SilverReader) GetHolderCounts(ctx context.Context, contractIDs []string) (map[string]int64, error) {
	if len(contractIDs) == 0 {
		return make(map[string]int64), nil
	}

	// Count unique addresses with positive balance from transfers
	query := `
		WITH balances AS (
			SELECT
				token_contract_id,
				COALESCE(to_account, '') as account,
				SUM(CASE
					WHEN to_account IS NOT NULL THEN CAST(amount AS NUMERIC)
					ELSE 0
				END) -
				SUM(CASE
					WHEN from_account = COALESCE(to_account, '') THEN CAST(amount AS NUMERIC)
					ELSE 0
				END) as balance
			FROM token_transfers_raw
			WHERE token_contract_id = ANY($1)
			GROUP BY token_contract_id, COALESCE(to_account, '')
		)
		SELECT
			token_contract_id,
			COUNT(DISTINCT account) as holder_count
		FROM balances
		WHERE balance > 0
		GROUP BY token_contract_id
	`

	rows, err := r.db.QueryContext(ctx, query, pq.Array(contractIDs))
	if err != nil {
		return nil, fmt.Errorf("failed to query holder counts: %w", err)
	}
	defer rows.Close()

	counts := make(map[string]int64)
	for rows.Next() {
		var contractID string
		var count int64
		if err := rows.Scan(&contractID, &count); err != nil {
			return nil, fmt.Errorf("failed to scan holder count: %w", err)
		}
		counts[contractID] = count
	}

	return counts, nil
}
