package main

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/lib/pq"
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
func (h *SilverHotReader) GetAccountHistory(ctx context.Context, accountID string, limit int, cursor *AccountCursor) ([]AccountSnapshot, error) {
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
	`

	args := []interface{}{accountID}

	// Cursor-based pagination: filter for records before the cursor position
	if cursor != nil {
		query += " AND ledger_sequence < $2"
		args = append(args, cursor.LedgerSequence)
	}

	query += " ORDER BY ledger_sequence DESC LIMIT $" + fmt.Sprint(len(args)+1)
	args = append(args, limit)

	rows, err := h.db.QueryContext(ctx, query, args...)
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

// AccountListFilters contains filters for listing accounts
type AccountListFilters struct {
	SortBy     string             // "balance", "last_modified", "account_id"
	SortOrder  string             // "asc" or "desc"
	MinBalance *int64             // Optional minimum balance filter
	Limit      int                // Max results to return
	Cursor     *AccountListCursor // Cursor for pagination
}

// GetAccountsList returns a paginated list of all accounts
func (h *SilverHotReader) GetAccountsList(ctx context.Context, filters AccountListFilters) ([]AccountCurrent, error) {
	// Build base query
	query := `
		SELECT
			account_id,
			balance,
			sequence_number,
			num_subentries,
			last_modified_ledger,
			updated_at
		FROM accounts_current
		WHERE 1=1
	`

	args := []interface{}{}

	// Apply minimum balance filter (balance is stored as decimal string in XLM)
	if filters.MinBalance != nil {
		// Convert stroops to XLM for comparison (divide by 10^7)
		minBalXLM := float64(*filters.MinBalance) / 10000000.0
		query += " AND CAST(balance AS DECIMAL) >= $" + fmt.Sprint(len(args)+1)
		args = append(args, minBalXLM)
	}

	// Cursor-based pagination (for balance DESC order)
	if filters.Cursor != nil {
		// For descending balance order: get records with lower balance,
		// or same balance but account_id comes after (for tie-breaking)
		// Convert cursor balance (stroops) to XLM for comparison
		cursorBalXLM := float64(filters.Cursor.Balance) / 10000000.0
		query += " AND (CAST(balance AS DECIMAL) < $" + fmt.Sprint(len(args)+1) +
			" OR (CAST(balance AS DECIMAL) = $" + fmt.Sprint(len(args)+2) +
			" AND account_id > $" + fmt.Sprint(len(args)+3) + "))"
		args = append(args, cursorBalXLM, cursorBalXLM, filters.Cursor.AccountID)
	}

	// Default sort by balance descending (balance is stored as decimal string)
	sortBy := "CAST(balance AS DECIMAL)"
	sortOrder := "DESC"

	switch filters.SortBy {
	case "last_modified":
		sortBy = "last_modified_ledger"
	case "account_id":
		sortBy = "account_id"
	}

	if filters.SortOrder == "asc" {
		sortOrder = "ASC"
	}

	// Add secondary sort by account_id for stable ordering
	query += fmt.Sprintf(" ORDER BY %s %s, account_id ASC LIMIT $%d", sortBy, sortOrder, len(args)+1)
	args = append(args, filters.Limit)

	rows, err := h.db.QueryContext(ctx, query, args...)
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
		query += " AND (source_account = $" + fmt.Sprint(len(args)+1) + " OR destination = $" + fmt.Sprint(len(args)+2) + ")"
		args = append(args, filters.AccountID, filters.AccountID)
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

	// Cursor-based pagination: filter for records before the cursor position
	if filters.Cursor != nil {
		query += " AND (ledger_sequence < $" + fmt.Sprint(len(args)+1) +
			" OR (ledger_sequence = $" + fmt.Sprint(len(args)+2) +
			" AND operation_index < $" + fmt.Sprint(len(args)+3) + "))"
		args = append(args, filters.Cursor.LedgerSequence, filters.Cursor.LedgerSequence, filters.Cursor.OperationIndex)
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

	// Cursor-based pagination: filter for records before the cursor position
	if filters.Cursor != nil {
		query += " AND (ledger_sequence < $" + fmt.Sprint(len(args)+1) +
			" OR (ledger_sequence = $" + fmt.Sprint(len(args)+2) +
			" AND timestamp < $" + fmt.Sprint(len(args)+3) + "))"
		args = append(args, filters.Cursor.LedgerSequence, filters.Cursor.LedgerSequence, filters.Cursor.Timestamp)
	}

	query += " ORDER BY ledger_sequence DESC, timestamp DESC LIMIT $" + fmt.Sprint(len(args)+1)
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

// ============================================
// CONTRACT CALL QUERIES (Freighter Support)
// ============================================

// GetContractsInvolved returns all unique contracts involved in a transaction
func (h *SilverHotReader) GetContractsInvolved(ctx context.Context, txHash string) ([]string, error) {
	query := `
		SELECT DISTINCT contract_id
		FROM (
			SELECT from_contract AS contract_id FROM contract_invocation_calls WHERE transaction_hash = $1
			UNION
			SELECT to_contract AS contract_id FROM contract_invocation_calls WHERE transaction_hash = $1
		) contracts
		ORDER BY contract_id
	`

	rows, err := h.db.QueryContext(ctx, query, txHash)
	if err != nil {
		return nil, fmt.Errorf("failed to query contracts involved: %w", err)
	}
	defer rows.Close()

	var contracts []string
	for rows.Next() {
		var contractID string
		if err := rows.Scan(&contractID); err != nil {
			return nil, err
		}
		contracts = append(contracts, contractID)
	}

	return contracts, nil
}

// GetTransactionCallGraph returns the call graph for a transaction
func (h *SilverHotReader) GetTransactionCallGraph(ctx context.Context, txHash string) ([]ContractCall, error) {
	query := `
		SELECT
			from_contract,
			to_contract,
			function_name,
			call_depth,
			execution_order,
			successful,
			transaction_hash,
			ledger_sequence,
			closed_at
		FROM contract_invocation_calls
		WHERE transaction_hash = $1
		ORDER BY execution_order
	`

	rows, err := h.db.QueryContext(ctx, query, txHash)
	if err != nil {
		return nil, fmt.Errorf("failed to query call graph: %w", err)
	}
	defer rows.Close()

	var calls []ContractCall
	for rows.Next() {
		var call ContractCall
		var closedAt sql.NullTime
		if err := rows.Scan(
			&call.FromContract, &call.ToContract, &call.FunctionName,
			&call.CallDepth, &call.ExecutionOrder, &call.Successful,
			&call.TransactionHash, &call.LedgerSequence, &closedAt,
		); err != nil {
			return nil, err
		}
		if closedAt.Valid {
			call.ClosedAt = closedAt.Time.Format("2006-01-02T15:04:05Z")
		}
		calls = append(calls, call)
	}

	return calls, nil
}

// GetTransactionHierarchy returns the contract hierarchy for a transaction
func (h *SilverHotReader) GetTransactionHierarchy(ctx context.Context, txHash string) ([]ContractHierarchy, error) {
	query := `
		SELECT
			root_contract,
			child_contract,
			path_depth,
			full_path
		FROM contract_invocation_hierarchy
		WHERE transaction_hash = $1
		ORDER BY path_depth
	`

	rows, err := h.db.QueryContext(ctx, query, txHash)
	if err != nil {
		return nil, fmt.Errorf("failed to query hierarchy: %w", err)
	}
	defer rows.Close()

	var hierarchies []ContractHierarchy
	for rows.Next() {
		var h ContractHierarchy
		var pathArray []string
		if err := rows.Scan(&h.RootContract, &h.ChildContract, &h.PathDepth, pq.Array(&pathArray)); err != nil {
			return nil, err
		}
		h.FullPath = pathArray
		hierarchies = append(hierarchies, h)
	}

	return hierarchies, nil
}

// GetContractRecentCalls returns recent calls for a contract
func (h *SilverHotReader) GetContractRecentCalls(ctx context.Context, contractID string, limit int) ([]ContractCall, int, int, error) {
	query := `
		SELECT
			from_contract,
			to_contract,
			function_name,
			call_depth,
			execution_order,
			successful,
			transaction_hash,
			ledger_sequence,
			closed_at
		FROM contract_invocation_calls
		WHERE from_contract = $1 OR to_contract = $1
		ORDER BY closed_at DESC
		LIMIT $2
	`

	rows, err := h.db.QueryContext(ctx, query, contractID, limit)
	if err != nil {
		return nil, 0, 0, fmt.Errorf("failed to query recent calls: %w", err)
	}
	defer rows.Close()

	var calls []ContractCall
	asCaller := 0
	asCallee := 0

	for rows.Next() {
		var call ContractCall
		var closedAt sql.NullTime
		if err := rows.Scan(
			&call.FromContract, &call.ToContract, &call.FunctionName,
			&call.CallDepth, &call.ExecutionOrder, &call.Successful,
			&call.TransactionHash, &call.LedgerSequence, &closedAt,
		); err != nil {
			return nil, 0, 0, err
		}
		if closedAt.Valid {
			call.ClosedAt = closedAt.Time.Format("2006-01-02T15:04:05Z")
		}
		calls = append(calls, call)

		if call.FromContract == contractID {
			asCaller++
		}
		if call.ToContract == contractID {
			asCallee++
		}
	}

	return calls, asCaller, asCallee, nil
}

// GetContractCallers returns contracts that call a specific contract
func (h *SilverHotReader) GetContractCallers(ctx context.Context, contractID string, limit int) ([]ContractRelationship, error) {
	query := `
		SELECT
			from_contract AS contract_id,
			COUNT(*) AS call_count,
			array_agg(DISTINCT function_name) AS functions,
			MAX(closed_at) AS last_call
		FROM contract_invocation_calls
		WHERE to_contract = $1
		GROUP BY from_contract
		ORDER BY call_count DESC
		LIMIT $2
	`

	rows, err := h.db.QueryContext(ctx, query, contractID, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to query callers: %w", err)
	}
	defer rows.Close()

	var callers []ContractRelationship
	for rows.Next() {
		var r ContractRelationship
		var functionsArray []string
		var lastCall sql.NullTime
		if err := rows.Scan(&r.ContractID, &r.CallCount, pq.Array(&functionsArray), &lastCall); err != nil {
			return nil, err
		}
		r.Functions = functionsArray
		if lastCall.Valid {
			r.LastCall = lastCall.Time.Format("2006-01-02T15:04:05Z")
		}
		callers = append(callers, r)
	}

	return callers, nil
}

// GetContractCallees returns contracts called by a specific contract
func (h *SilverHotReader) GetContractCallees(ctx context.Context, contractID string, limit int) ([]ContractRelationship, error) {
	query := `
		SELECT
			to_contract AS contract_id,
			COUNT(*) AS call_count,
			array_agg(DISTINCT function_name) AS functions,
			MAX(closed_at) AS last_call
		FROM contract_invocation_calls
		WHERE from_contract = $1
		GROUP BY to_contract
		ORDER BY call_count DESC
		LIMIT $2
	`

	rows, err := h.db.QueryContext(ctx, query, contractID, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to query callees: %w", err)
	}
	defer rows.Close()

	var callees []ContractRelationship
	for rows.Next() {
		var r ContractRelationship
		var functionsArray []string
		var lastCall sql.NullTime
		if err := rows.Scan(&r.ContractID, &r.CallCount, pq.Array(&functionsArray), &lastCall); err != nil {
			return nil, err
		}
		r.Functions = functionsArray
		if lastCall.Valid {
			r.LastCall = lastCall.Time.Format("2006-01-02T15:04:05Z")
		}
		callees = append(callees, r)
	}

	return callees, nil
}

// GetContractCallSummary returns aggregated call statistics for a contract
func (h *SilverHotReader) GetContractCallSummary(ctx context.Context, contractID string) (*ContractCallSummary, error) {
	query := `
		WITH caller_stats AS (
			SELECT COUNT(*) as total_as_caller, COUNT(DISTINCT to_contract) as unique_callees
			FROM contract_invocation_calls WHERE from_contract = $1
		),
		callee_stats AS (
			SELECT COUNT(*) as total_as_callee, COUNT(DISTINCT from_contract) as unique_callers
			FROM contract_invocation_calls WHERE to_contract = $1
		),
		time_stats AS (
			SELECT MIN(closed_at) as first_seen, MAX(closed_at) as last_seen
			FROM contract_invocation_calls WHERE from_contract = $1 OR to_contract = $1
		)
		SELECT
			$1 as contract_id,
			COALESCE((SELECT total_as_caller FROM caller_stats), 0),
			COALESCE((SELECT total_as_callee FROM callee_stats), 0),
			COALESCE((SELECT unique_callers FROM callee_stats), 0),
			COALESCE((SELECT unique_callees FROM caller_stats), 0),
			(SELECT first_seen FROM time_stats),
			(SELECT last_seen FROM time_stats)
	`

	var summary ContractCallSummary
	var firstSeen, lastSeen sql.NullTime

	err := h.db.QueryRowContext(ctx, query, contractID).Scan(
		&summary.ContractID,
		&summary.TotalCallsAsCaller,
		&summary.TotalCallsAsCallee,
		&summary.UniqueCallers,
		&summary.UniqueCallees,
		&firstSeen,
		&lastSeen,
	)

	if err != nil {
		return nil, fmt.Errorf("failed to get call summary: %w", err)
	}

	if firstSeen.Valid {
		summary.FirstSeen = firstSeen.Time.Format("2006-01-02T15:04:05Z")
	}
	if lastSeen.Valid {
		summary.LastSeen = lastSeen.Time.Format("2006-01-02T15:04:05Z")
	}

	return &summary, nil
}
