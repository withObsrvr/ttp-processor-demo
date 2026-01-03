package main

import (
	"context"
	"database/sql"
	"fmt"

	_ "github.com/lib/pq"
)

// SilverHotReader queries PostgreSQL silver_hot for recent analytics data
type SilverHotReader struct {
	db *sql.DB
}

// NewSilverHotReader creates a new Silver hot layer reader
func NewSilverHotReader(config PostgresConfig) (*SilverHotReader, error) {
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

	return &SilverHotReader{db: db}, nil
}

// Close closes the database connection
func (h *SilverHotReader) Close() error {
	if h.db != nil {
		return h.db.Close()
	}
	return nil
}

// ============================================
// ACCOUNT QUERIES (Current State + History)
// ============================================

// GetAccountCurrent returns the current state of an account from hot buffer
func (h *SilverHotReader) GetAccountCurrent(ctx context.Context, accountID string) (*AccountCurrent, error) {
	query := `
		SELECT
			account_id,
			balance,
			sequence_number,
			num_subentries,
			last_modified_ledger,
			updated_at
		FROM accounts_current
		WHERE account_id = $1
	`

	var acc AccountCurrent
	err := h.db.QueryRowContext(ctx, query, accountID).Scan(
		&acc.AccountID, &acc.Balance, &acc.SequenceNumber,
		&acc.NumSubentries, &acc.LastModifiedLedger, &acc.UpdatedAt,
	)

	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	return &acc, nil
}

// GetAccountHistory returns historical snapshots from hot buffer
func (h *SilverHotReader) GetAccountHistory(ctx context.Context, accountID string, limit int) ([]AccountSnapshot, error) {
	query := `
		SELECT
			account_id,
			balance,
			sequence_number,
			ledger_sequence,
			closed_at,
			valid_to
		FROM accounts_snapshot
		WHERE account_id = $1
		ORDER BY ledger_sequence DESC
		LIMIT $2
	`

	rows, err := h.db.QueryContext(ctx, query, accountID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var snapshots []AccountSnapshot
	for rows.Next() {
		var snap AccountSnapshot
		if err := rows.Scan(&snap.AccountID, &snap.Balance, &snap.SequenceNumber,
			&snap.LedgerSequence, &snap.ClosedAt, &snap.ValidTo); err != nil {
			return nil, err
		}
		snapshots = append(snapshots, snap)
	}

	return snapshots, nil
}

// GetTopAccounts returns top accounts by balance from hot buffer
func (h *SilverHotReader) GetTopAccounts(ctx context.Context, limit int) ([]AccountCurrent, error) {
	query := `
		SELECT
			account_id,
			balance,
			sequence_number,
			num_subentries,
			last_modified_ledger,
			updated_at
		FROM accounts_current
		ORDER BY CAST(balance AS BIGINT) DESC
		LIMIT $1
	`

	rows, err := h.db.QueryContext(ctx, query, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var accounts []AccountCurrent
	for rows.Next() {
		var acc AccountCurrent
		if err := rows.Scan(&acc.AccountID, &acc.Balance, &acc.SequenceNumber,
			&acc.NumSubentries, &acc.LastModifiedLedger, &acc.UpdatedAt); err != nil {
			return nil, err
		}
		accounts = append(accounts, acc)
	}

	return accounts, nil
}

// ============================================
// ENRICHED OPERATIONS QUERIES
// ============================================

// GetEnrichedOperations returns enriched operations from hot buffer
func (h *SilverHotReader) GetEnrichedOperations(ctx context.Context, filters OperationFilters) ([]EnrichedOperation, error) {
	query := `
		SELECT
			transaction_hash,
			operation_index,
			ledger_sequence,
			ledger_closed_at,
			source_account,
			type,
			destination,
			asset_code,
			asset_issuer,
			amount,
			tx_successful,
			tx_fee_charged,
			is_payment_op,
			is_soroban_op
		FROM enriched_history_operations
		WHERE 1=1
	`

	args := []interface{}{}

	if filters.AccountID != "" {
		query += " AND source_account = $" + fmt.Sprint(len(args)+1)
		args = append(args, filters.AccountID)
	}

	if filters.TxHash != "" {
		query += " AND transaction_hash = $" + fmt.Sprint(len(args)+1)
		args = append(args, filters.TxHash)
	}

	if filters.StartLedger > 0 {
		query += " AND ledger_sequence >= $" + fmt.Sprint(len(args)+1)
		args = append(args, filters.StartLedger)
	}

	if filters.EndLedger > 0 {
		query += " AND ledger_sequence <= $" + fmt.Sprint(len(args)+1)
		args = append(args, filters.EndLedger)
	}

	if filters.PaymentsOnly {
		query += " AND is_payment_op = true"
	}

	if filters.SorobanOnly {
		query += " AND is_soroban_op = true"
	}

	query += " ORDER BY ledger_sequence DESC, operation_index DESC LIMIT $" + fmt.Sprint(len(args)+1)
	args = append(args, filters.Limit)

	rows, err := h.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var operations []EnrichedOperation
	for rows.Next() {
		var op EnrichedOperation
		if err := rows.Scan(&op.TransactionHash, &op.OperationID, &op.LedgerSequence,
			&op.LedgerClosedAt, &op.SourceAccount, &op.Type,
			&op.Destination, &op.AssetCode, &op.AssetIssuer, &op.Amount,
			&op.TxSuccessful, &op.TxFeeCharged, &op.IsPaymentOp, &op.IsSorobanOp); err != nil {
			return nil, err
		}
		op.TypeName = operationTypeName(op.Type)
		operations = append(operations, op)
	}

	return operations, nil
}

// ============================================
// TOKEN TRANSFERS QUERIES
// ============================================

// GetTokenTransfers returns token transfers from hot buffer
func (h *SilverHotReader) GetTokenTransfers(ctx context.Context, filters TransferFilters) ([]TokenTransfer, error) {
	query := `
		SELECT
			timestamp,
			transaction_hash,
			ledger_sequence,
			source_type,
			from_account,
			to_account,
			asset_code,
			asset_issuer,
			amount,
			token_contract_id,
			transaction_successful
		FROM token_transfers_raw
		WHERE transaction_successful = true
	`

	args := []interface{}{}

	if filters.SourceType != "" {
		query += " AND source_type = $" + fmt.Sprint(len(args)+1)
		args = append(args, filters.SourceType)
	}

	if filters.AssetCode != "" {
		query += " AND asset_code = $" + fmt.Sprint(len(args)+1)
		args = append(args, filters.AssetCode)
	}

	if filters.FromAccount != "" {
		query += " AND from_account = $" + fmt.Sprint(len(args)+1)
		args = append(args, filters.FromAccount)
	}

	if filters.ToAccount != "" {
		query += " AND to_account = $" + fmt.Sprint(len(args)+1)
		args = append(args, filters.ToAccount)
	}

	if !filters.StartTime.IsZero() {
		query += " AND timestamp >= $" + fmt.Sprint(len(args)+1)
		args = append(args, filters.StartTime)
	}

	if !filters.EndTime.IsZero() {
		query += " AND timestamp <= $" + fmt.Sprint(len(args)+1)
		args = append(args, filters.EndTime)
	}

	query += " ORDER BY timestamp DESC LIMIT $" + fmt.Sprint(len(args)+1)
	args = append(args, filters.Limit)

	rows, err := h.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var transfers []TokenTransfer
	for rows.Next() {
		var t TokenTransfer
		if err := rows.Scan(&t.Timestamp, &t.TransactionHash, &t.LedgerSequence,
			&t.SourceType, &t.FromAccount, &t.ToAccount, &t.AssetCode, &t.AssetIssuer,
			&t.Amount, &t.TokenContractID, &t.TransactionSuccessful); err != nil {
			return nil, err
		}
		transfers = append(transfers, t)
	}

	return transfers, nil
}
