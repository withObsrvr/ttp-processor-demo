package main

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"
	"time"

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
		ORDER BY CAST(balance AS DECIMAL) DESC
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

	// Cursor-based pagination - respects current sort field and order
	if filters.Cursor != nil {
		// Determine effective sort settings
		sortBy := filters.SortBy
		if sortBy == "" {
			sortBy = "balance"
		}
		sortOrder := filters.SortOrder
		if sortOrder == "" {
			sortOrder = "desc"
		}
		isAsc := sortOrder == "asc"

		switch sortBy {
		case "last_modified":
			// Paginate based on last_modified_ledger, tie-break by account_id
			if isAsc {
				query += " AND (last_modified_ledger > $" + fmt.Sprint(len(args)+1) +
					" OR (last_modified_ledger = $" + fmt.Sprint(len(args)+2) +
					" AND account_id > $" + fmt.Sprint(len(args)+3) + "))"
			} else {
				query += " AND (last_modified_ledger < $" + fmt.Sprint(len(args)+1) +
					" OR (last_modified_ledger = $" + fmt.Sprint(len(args)+2) +
					" AND account_id > $" + fmt.Sprint(len(args)+3) + "))"
			}
			args = append(args, filters.Cursor.LastModifiedLedger, filters.Cursor.LastModifiedLedger, filters.Cursor.AccountID)

		case "account_id":
			// Paginate directly on account_id
			if isAsc {
				query += " AND account_id > $" + fmt.Sprint(len(args)+1)
			} else {
				query += " AND account_id < $" + fmt.Sprint(len(args)+1)
			}
			args = append(args, filters.Cursor.AccountID)

		default: // "balance" or empty
			// Paginate based on balance, tie-break by account_id
			cursorBalXLM := float64(filters.Cursor.Balance) / 10000000.0
			if isAsc {
				query += " AND (CAST(balance AS DECIMAL) > $" + fmt.Sprint(len(args)+1) +
					" OR (CAST(balance AS DECIMAL) = $" + fmt.Sprint(len(args)+2) +
					" AND account_id > $" + fmt.Sprint(len(args)+3) + "))"
			} else {
				query += " AND (CAST(balance AS DECIMAL) < $" + fmt.Sprint(len(args)+1) +
					" OR (CAST(balance AS DECIMAL) = $" + fmt.Sprint(len(args)+2) +
					" AND account_id > $" + fmt.Sprint(len(args)+3) + "))"
			}
			args = append(args, cursorBalXLM, cursorBalXLM, filters.Cursor.AccountID)
		}
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

// ============================================
// PHASE 6: STATE TABLE QUERIES (Offers, Liquidity Pools, Claimable Balances)
// ============================================

// GetOffers returns offers with optional filters
func (h *SilverHotReader) GetOffers(ctx context.Context, filters OfferFilters) ([]OfferCurrent, error) {
	query := `
		SELECT
			offer_id,
			seller_id,
			selling_asset_type,
			selling_asset_code,
			selling_asset_issuer,
			buying_asset_type,
			buying_asset_code,
			buying_asset_issuer,
			amount,
			price_n,
			price_d,
			price,
			last_modified_ledger,
			sponsor
		FROM offers_current
		WHERE 1=1
	`

	args := []interface{}{}

	if filters.SellerID != "" {
		query += " AND seller_id = $" + fmt.Sprint(len(args)+1)
		args = append(args, filters.SellerID)
	}

	if filters.SellingAssetCode != "" {
		if filters.SellingAssetCode == "XLM" {
			query += " AND selling_asset_type = 'native'"
		} else {
			query += " AND selling_asset_code = $" + fmt.Sprint(len(args)+1)
			args = append(args, filters.SellingAssetCode)
			if filters.SellingAssetIssuer != "" {
				query += " AND selling_asset_issuer = $" + fmt.Sprint(len(args)+1)
				args = append(args, filters.SellingAssetIssuer)
			}
		}
	}

	if filters.BuyingAssetCode != "" {
		if filters.BuyingAssetCode == "XLM" {
			query += " AND buying_asset_type = 'native'"
		} else {
			query += " AND buying_asset_code = $" + fmt.Sprint(len(args)+1)
			args = append(args, filters.BuyingAssetCode)
			if filters.BuyingAssetIssuer != "" {
				query += " AND buying_asset_issuer = $" + fmt.Sprint(len(args)+1)
				args = append(args, filters.BuyingAssetIssuer)
			}
		}
	}

	// Cursor-based pagination
	if filters.Cursor != nil {
		query += " AND offer_id > $" + fmt.Sprint(len(args)+1)
		args = append(args, filters.Cursor.OfferID)
	}

	query += " ORDER BY offer_id ASC LIMIT $" + fmt.Sprint(len(args)+1)
	args = append(args, filters.Limit)

	rows, err := h.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to query offers: %w", err)
	}
	defer rows.Close()

	var offers []OfferCurrent
	for rows.Next() {
		var o OfferCurrent
		var sellingType, sellingCode, buyingType, buyingCode sql.NullString
		var sellingIssuer, buyingIssuer, sponsor sql.NullString
		var amount int64
		var priceN, priceD int
		var price float64

		if err := rows.Scan(
			&o.OfferID, &o.SellerID,
			&sellingType, &sellingCode, &sellingIssuer,
			&buyingType, &buyingCode, &buyingIssuer,
			&amount, &priceN, &priceD, &price,
			&o.LastModifiedLedger, &sponsor,
		); err != nil {
			return nil, err
		}

		// Build selling asset
		o.Selling = buildAssetInfo(sellingType.String, sellingCode.String, sellingIssuer.String)
		// Build buying asset
		o.Buying = buildAssetInfo(buyingType.String, buyingCode.String, buyingIssuer.String)
		// Format amount from stroops
		o.Amount = formatStroops(amount)
		// Set price
		o.Price = fmt.Sprintf("%.7f", price)
		o.PriceR = PriceR{N: priceN, D: priceD}
		// Set sponsor
		if sponsor.Valid {
			o.Sponsor = &sponsor.String
		}

		offers = append(offers, o)
	}

	return offers, nil
}

// GetOfferByID returns a single offer by ID
func (h *SilverHotReader) GetOfferByID(ctx context.Context, offerID int64) (*OfferCurrent, error) {
	query := `
		SELECT
			offer_id,
			seller_id,
			selling_asset_type,
			selling_asset_code,
			selling_asset_issuer,
			buying_asset_type,
			buying_asset_code,
			buying_asset_issuer,
			amount,
			price_n,
			price_d,
			price,
			last_modified_ledger,
			sponsor
		FROM offers_current
		WHERE offer_id = $1
	`

	var o OfferCurrent
	var sellingType, sellingCode, buyingType, buyingCode sql.NullString
	var sellingIssuer, buyingIssuer, sponsor sql.NullString
	var amount int64
	var priceN, priceD int
	var price float64

	err := h.db.QueryRowContext(ctx, query, offerID).Scan(
		&o.OfferID, &o.SellerID,
		&sellingType, &sellingCode, &sellingIssuer,
		&buyingType, &buyingCode, &buyingIssuer,
		&amount, &priceN, &priceD, &price,
		&o.LastModifiedLedger, &sponsor,
	)

	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	o.Selling = buildAssetInfo(sellingType.String, sellingCode.String, sellingIssuer.String)
	o.Buying = buildAssetInfo(buyingType.String, buyingCode.String, buyingIssuer.String)
	o.Amount = formatStroops(amount)
	o.Price = fmt.Sprintf("%.7f", price)
	o.PriceR = PriceR{N: priceN, D: priceD}
	if sponsor.Valid {
		o.Sponsor = &sponsor.String
	}

	return &o, nil
}

// GetLiquidityPools returns liquidity pools with optional filters
func (h *SilverHotReader) GetLiquidityPools(ctx context.Context, filters LiquidityPoolFilters) ([]LiquidityPoolCurrent, error) {
	query := `
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
			last_modified_ledger
		FROM liquidity_pools_current
		WHERE 1=1
	`

	args := []interface{}{}

	// Filter by asset (matches either asset_a or asset_b)
	if filters.AssetCode != "" {
		if filters.AssetCode == "XLM" {
			query += " AND (asset_a_type = 'native' OR asset_b_type = 'native')"
		} else {
			query += " AND ((asset_a_code = $" + fmt.Sprint(len(args)+1) + " AND asset_a_issuer = $" + fmt.Sprint(len(args)+2) + ")"
			query += " OR (asset_b_code = $" + fmt.Sprint(len(args)+1) + " AND asset_b_issuer = $" + fmt.Sprint(len(args)+2) + "))"
			args = append(args, filters.AssetCode, filters.AssetIssuer)
		}
	}

	// Cursor-based pagination
	if filters.Cursor != nil {
		query += " AND liquidity_pool_id > $" + fmt.Sprint(len(args)+1)
		args = append(args, filters.Cursor.PoolID)
	}

	query += " ORDER BY liquidity_pool_id ASC LIMIT $" + fmt.Sprint(len(args)+1)
	args = append(args, filters.Limit)

	rows, err := h.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to query liquidity pools: %w", err)
	}
	defer rows.Close()

	var pools []LiquidityPoolCurrent
	for rows.Next() {
		var p LiquidityPoolCurrent
		var assetAType, assetACode, assetBType, assetBCode sql.NullString
		var assetAIssuer, assetBIssuer sql.NullString
		var assetAAmount, assetBAmount, totalShares int64

		if err := rows.Scan(
			&p.PoolID, &p.PoolType, &p.FeeBP, &p.TrustlineCount, &totalShares,
			&assetAType, &assetACode, &assetAIssuer, &assetAAmount,
			&assetBType, &assetBCode, &assetBIssuer, &assetBAmount,
			&p.LastModifiedLedger,
		); err != nil {
			return nil, err
		}

		p.TotalShares = formatStroops(totalShares)
		p.Reserves = []PoolReserve{
			{
				Asset:  buildAssetInfo(assetAType.String, assetACode.String, assetAIssuer.String),
				Amount: formatStroops(assetAAmount),
			},
			{
				Asset:  buildAssetInfo(assetBType.String, assetBCode.String, assetBIssuer.String),
				Amount: formatStroops(assetBAmount),
			},
		}

		pools = append(pools, p)
	}

	return pools, nil
}

// GetLiquidityPoolByID returns a single liquidity pool by ID
func (h *SilverHotReader) GetLiquidityPoolByID(ctx context.Context, poolID string) (*LiquidityPoolCurrent, error) {
	query := `
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
			last_modified_ledger
		FROM liquidity_pools_current
		WHERE liquidity_pool_id = $1
	`

	var p LiquidityPoolCurrent
	var assetAType, assetACode, assetBType, assetBCode sql.NullString
	var assetAIssuer, assetBIssuer sql.NullString
	var assetAAmount, assetBAmount, totalShares int64

	err := h.db.QueryRowContext(ctx, query, poolID).Scan(
		&p.PoolID, &p.PoolType, &p.FeeBP, &p.TrustlineCount, &totalShares,
		&assetAType, &assetACode, &assetAIssuer, &assetAAmount,
		&assetBType, &assetBCode, &assetBIssuer, &assetBAmount,
		&p.LastModifiedLedger,
	)

	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	p.TotalShares = formatStroops(totalShares)
	p.Reserves = []PoolReserve{
		{
			Asset:  buildAssetInfo(assetAType.String, assetACode.String, assetAIssuer.String),
			Amount: formatStroops(assetAAmount),
		},
		{
			Asset:  buildAssetInfo(assetBType.String, assetBCode.String, assetBIssuer.String),
			Amount: formatStroops(assetBAmount),
		},
	}

	return &p, nil
}

// GetClaimableBalances returns claimable balances with optional filters
func (h *SilverHotReader) GetClaimableBalances(ctx context.Context, filters ClaimableBalanceFilters) ([]ClaimableBalanceCurrent, error) {
	query := `
		SELECT
			balance_id,
			sponsor,
			asset_type,
			asset_code,
			asset_issuer,
			amount,
			COALESCE(jsonb_array_length(claimants), 0) as claimants_count,
			flags,
			last_modified_ledger
		FROM claimable_balances_current
		WHERE 1=1
	`

	args := []interface{}{}

	if filters.Sponsor != "" {
		query += " AND sponsor = $" + fmt.Sprint(len(args)+1)
		args = append(args, filters.Sponsor)
	}

	if filters.AssetCode != "" {
		if filters.AssetCode == "XLM" {
			query += " AND asset_type = 'native'"
		} else {
			query += " AND asset_code = $" + fmt.Sprint(len(args)+1)
			args = append(args, filters.AssetCode)
			if filters.AssetIssuer != "" {
				query += " AND asset_issuer = $" + fmt.Sprint(len(args)+1)
				args = append(args, filters.AssetIssuer)
			}
		}
	}

	// Cursor-based pagination
	if filters.Cursor != nil {
		query += " AND balance_id > $" + fmt.Sprint(len(args)+1)
		args = append(args, filters.Cursor.BalanceID)
	}

	query += " ORDER BY balance_id ASC LIMIT $" + fmt.Sprint(len(args)+1)
	args = append(args, filters.Limit)

	rows, err := h.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to query claimable balances: %w", err)
	}
	defer rows.Close()

	var balances []ClaimableBalanceCurrent
	for rows.Next() {
		var b ClaimableBalanceCurrent
		var sponsor, assetType, assetCode, assetIssuer sql.NullString
		var amount int64

		if err := rows.Scan(
			&b.BalanceID, &sponsor,
			&assetType, &assetCode, &assetIssuer,
			&amount, &b.ClaimantsCount, &b.Flags, &b.LastModifiedLedger,
		); err != nil {
			return nil, err
		}

		if sponsor.Valid {
			b.Sponsor = &sponsor.String
		}
		b.Asset = buildAssetInfo(assetType.String, assetCode.String, assetIssuer.String)
		b.Amount = formatStroops(amount)

		balances = append(balances, b)
	}

	return balances, nil
}

// GetClaimableBalanceByID returns a single claimable balance by ID
func (h *SilverHotReader) GetClaimableBalanceByID(ctx context.Context, balanceID string) (*ClaimableBalanceCurrent, error) {
	query := `
		SELECT
			balance_id,
			sponsor,
			asset_type,
			asset_code,
			asset_issuer,
			amount,
			COALESCE(jsonb_array_length(claimants), 0) as claimants_count,
			flags,
			last_modified_ledger
		FROM claimable_balances_current
		WHERE balance_id = $1
	`

	var b ClaimableBalanceCurrent
	var sponsor, assetType, assetCode, assetIssuer sql.NullString
	var amount int64

	err := h.db.QueryRowContext(ctx, query, balanceID).Scan(
		&b.BalanceID, &sponsor,
		&assetType, &assetCode, &assetIssuer,
		&amount, &b.ClaimantsCount, &b.Flags, &b.LastModifiedLedger,
	)

	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	if sponsor.Valid {
		b.Sponsor = &sponsor.String
	}
	b.Asset = buildAssetInfo(assetType.String, assetCode.String, assetIssuer.String)
	b.Amount = formatStroops(amount)

	return &b, nil
}

// Helper functions for Phase 6

// buildAssetInfo creates an AssetInfo from type, code, and issuer
func buildAssetInfo(assetType, assetCode, assetIssuer string) AssetInfo {
	if assetType == "native" || assetCode == "" {
		return AssetInfo{
			Type: "native",
			Code: "XLM",
		}
	}
	info := AssetInfo{
		Type: assetType,
		Code: assetCode,
	}
	if assetIssuer != "" {
		info.Issuer = &assetIssuer
	}
	return info
}

// formatStroops converts stroops (int64) to XLM string with 7 decimal places
func formatStroops(stroops int64) string {
	return fmt.Sprintf("%.7f", float64(stroops)/10000000.0)
}

// ============================================
// PHASE 7: EVENT TABLE METHODS
// ============================================

// GetTrades returns trades from the hot buffer with filters
func (h *SilverHotReader) GetTrades(ctx context.Context, filters TradeFilters) ([]SilverTrade, string, bool, error) {
	var conditions []string
	var args []interface{}
	argNum := 1

	// Time range filter (default to last 24 hours)
	if filters.StartTime.IsZero() {
		filters.StartTime = time.Now().Add(-24 * time.Hour)
	}
	if filters.EndTime.IsZero() {
		filters.EndTime = time.Now()
	}
	conditions = append(conditions, fmt.Sprintf("trade_timestamp >= $%d AND trade_timestamp <= $%d", argNum, argNum+1))
	args = append(args, filters.StartTime, filters.EndTime)
	argNum += 2

	// Account filters
	if filters.AccountID != "" {
		conditions = append(conditions, fmt.Sprintf("(seller_account = $%d OR buyer_account = $%d)", argNum, argNum))
		args = append(args, filters.AccountID)
		argNum++
	}
	if filters.SellerAccount != "" {
		conditions = append(conditions, fmt.Sprintf("seller_account = $%d", argNum))
		args = append(args, filters.SellerAccount)
		argNum++
	}
	if filters.BuyerAccount != "" {
		conditions = append(conditions, fmt.Sprintf("buyer_account = $%d", argNum))
		args = append(args, filters.BuyerAccount)
		argNum++
	}

	// Asset pair filters
	if filters.SellingAssetCode != "" {
		if filters.SellingAssetCode == "XLM" {
			conditions = append(conditions, "(selling_asset_code IS NULL OR selling_asset_code = '')")
		} else {
			conditions = append(conditions, fmt.Sprintf("selling_asset_code = $%d", argNum))
			args = append(args, filters.SellingAssetCode)
			argNum++
			if filters.SellingAssetIssuer != "" {
				conditions = append(conditions, fmt.Sprintf("selling_asset_issuer = $%d", argNum))
				args = append(args, filters.SellingAssetIssuer)
				argNum++
			}
		}
	}
	if filters.BuyingAssetCode != "" {
		if filters.BuyingAssetCode == "XLM" {
			conditions = append(conditions, "(buying_asset_code IS NULL OR buying_asset_code = '')")
		} else {
			conditions = append(conditions, fmt.Sprintf("buying_asset_code = $%d", argNum))
			args = append(args, filters.BuyingAssetCode)
			argNum++
			if filters.BuyingAssetIssuer != "" {
				conditions = append(conditions, fmt.Sprintf("buying_asset_issuer = $%d", argNum))
				args = append(args, filters.BuyingAssetIssuer)
				argNum++
			}
		}
	}

	// Cursor pagination
	if filters.Cursor != nil {
		conditions = append(conditions, fmt.Sprintf(`
			(ledger_sequence, transaction_hash, operation_index, trade_index) > ($%d, $%d, $%d, $%d)
		`, argNum, argNum+1, argNum+2, argNum+3))
		args = append(args, filters.Cursor.LedgerSequence, filters.Cursor.TransactionHash,
			filters.Cursor.OperationIndex, filters.Cursor.TradeIndex)
		argNum += 4
	}

	whereClause := "WHERE " + strings.Join(conditions, " AND ")

	limit := filters.Limit
	if limit <= 0 {
		limit = 100
	}

	query := fmt.Sprintf(`
		SELECT ledger_sequence, transaction_hash, operation_index, trade_index,
			   COALESCE(trade_type, 'orderbook') as trade_type, trade_timestamp,
			   seller_account, selling_asset_code, selling_asset_issuer, selling_amount,
			   buyer_account, buying_asset_code, buying_asset_issuer, buying_amount,
			   price
		FROM trades
		%s
		ORDER BY ledger_sequence ASC, transaction_hash ASC, operation_index ASC, trade_index ASC
		LIMIT $%d
	`, whereClause, argNum)
	args = append(args, limit+1)

	rows, err := h.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, "", false, err
	}
	defer rows.Close()

	var trades []SilverTrade
	for rows.Next() {
		var t SilverTrade
		var sellingCode, sellingIssuer, buyingCode, buyingIssuer sql.NullString
		var sellingAmount, buyingAmount int64
		var priceDecimal float64

		err := rows.Scan(
			&t.LedgerSequence, &t.TransactionHash, &t.OperationIndex, &t.TradeIndex,
			&t.TradeType, &t.Timestamp,
			&t.Seller.AccountID, &sellingCode, &sellingIssuer, &sellingAmount,
			&t.Buyer.AccountID, &buyingCode, &buyingIssuer, &buyingAmount,
			&priceDecimal,
		)
		if err != nil {
			return nil, "", false, err
		}

		t.Selling.Asset = buildAssetInfo("", sellingCode.String, sellingIssuer.String)
		t.Selling.Amount = formatStroops(sellingAmount)
		t.Buying.Asset = buildAssetInfo("", buyingCode.String, buyingIssuer.String)
		t.Buying.Amount = formatStroops(buyingAmount)
		t.Price = fmt.Sprintf("%.7f", priceDecimal)

		trades = append(trades, t)
	}

	hasMore := len(trades) > limit
	if hasMore {
		trades = trades[:limit]
	}

	var nextCursor string
	if hasMore && len(trades) > 0 {
		last := trades[len(trades)-1]
		nextCursor = TradeCursor{
			LedgerSequence:  last.LedgerSequence,
			TransactionHash: last.TransactionHash,
			OperationIndex:  last.OperationIndex,
			TradeIndex:      last.TradeIndex,
		}.Encode()
	}

	return trades, nextCursor, hasMore, nil
}

// GetTradeStats returns aggregated trade statistics
func (h *SilverHotReader) GetTradeStats(ctx context.Context, groupBy string, startTime, endTime time.Time) ([]TradeStats, error) {
	var groupExpr, selectGroup string
	switch groupBy {
	case "asset_pair":
		groupExpr = "COALESCE(selling_asset_code, 'XLM') || '/' || COALESCE(buying_asset_code, 'XLM')"
		selectGroup = groupExpr + " as group_key"
	case "hour":
		groupExpr = "date_trunc('hour', trade_timestamp)"
		selectGroup = "to_char(" + groupExpr + ", 'YYYY-MM-DD HH24:00') as group_key"
	case "day":
		groupExpr = "date_trunc('day', trade_timestamp)"
		selectGroup = "to_char(" + groupExpr + ", 'YYYY-MM-DD') as group_key"
	default:
		groupExpr = "COALESCE(selling_asset_code, 'XLM') || '/' || COALESCE(buying_asset_code, 'XLM')"
		selectGroup = groupExpr + " as group_key"
	}

	query := fmt.Sprintf(`
		SELECT %s,
			   COUNT(*) as trade_count,
			   SUM(selling_amount) as volume_selling,
			   SUM(buying_amount) as volume_buying,
			   COUNT(DISTINCT seller_account) as unique_sellers,
			   COUNT(DISTINCT buyer_account) as unique_buyers,
			   AVG(price) as avg_price
		FROM trades
		WHERE trade_timestamp >= $1 AND trade_timestamp <= $2
		GROUP BY %s
		ORDER BY trade_count DESC
		LIMIT 100
	`, selectGroup, groupExpr)

	rows, err := h.db.QueryContext(ctx, query, startTime, endTime)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var stats []TradeStats
	for rows.Next() {
		var s TradeStats
		var volSelling, volBuying int64
		var avgPrice sql.NullFloat64

		err := rows.Scan(&s.Group, &s.TradeCount, &volSelling, &volBuying,
			&s.UniqueSellers, &s.UniqueBuyers, &avgPrice)
		if err != nil {
			return nil, err
		}

		s.VolumeSelling = formatStroops(volSelling)
		s.VolumeBuying = formatStroops(volBuying)
		if avgPrice.Valid {
			avgStr := fmt.Sprintf("%.7f", avgPrice.Float64)
			s.AvgPrice = &avgStr
		}

		stats = append(stats, s)
	}

	return stats, nil
}

// GetEffects returns effects from the hot buffer with filters
func (h *SilverHotReader) GetEffects(ctx context.Context, filters EffectFilters) ([]SilverEffect, string, bool, error) {
	var conditions []string
	var args []interface{}
	argNum := 1

	// Time range filter
	if !filters.StartTime.IsZero() {
		conditions = append(conditions, fmt.Sprintf("created_at >= $%d", argNum))
		args = append(args, filters.StartTime)
		argNum++
	}
	if !filters.EndTime.IsZero() {
		conditions = append(conditions, fmt.Sprintf("created_at <= $%d", argNum))
		args = append(args, filters.EndTime)
		argNum++
	}

	// Account filter
	if filters.AccountID != "" {
		conditions = append(conditions, fmt.Sprintf("account_id = $%d", argNum))
		args = append(args, filters.AccountID)
		argNum++
	}

	// Effect type filter (int or string)
	if filters.EffectType != "" {
		if typeInt, err := strconv.Atoi(filters.EffectType); err == nil {
			conditions = append(conditions, fmt.Sprintf("effect_type = $%d", argNum))
			args = append(args, typeInt)
		} else {
			conditions = append(conditions, fmt.Sprintf("effect_type_string = $%d", argNum))
			args = append(args, filters.EffectType)
		}
		argNum++
	}

	// Ledger filter
	if filters.LedgerSequence > 0 {
		conditions = append(conditions, fmt.Sprintf("ledger_sequence = $%d", argNum))
		args = append(args, filters.LedgerSequence)
		argNum++
	}

	// Transaction filter
	if filters.TransactionHash != "" {
		conditions = append(conditions, fmt.Sprintf("transaction_hash = $%d", argNum))
		args = append(args, filters.TransactionHash)
		argNum++
	}

	// Cursor pagination
	if filters.Cursor != nil {
		conditions = append(conditions, fmt.Sprintf(`
			(ledger_sequence, transaction_hash, operation_index, effect_index) > ($%d, $%d, $%d, $%d)
		`, argNum, argNum+1, argNum+2, argNum+3))
		args = append(args, filters.Cursor.LedgerSequence, filters.Cursor.TransactionHash,
			filters.Cursor.OperationIndex, filters.Cursor.EffectIndex)
		argNum += 4
	}

	whereClause := ""
	if len(conditions) > 0 {
		whereClause = "WHERE " + strings.Join(conditions, " AND ")
	}

	limit := filters.Limit
	if limit <= 0 {
		limit = 100
	}

	query := fmt.Sprintf(`
		SELECT ledger_sequence, transaction_hash, operation_index, effect_index,
			   effect_type, effect_type_string, account_id,
			   amount, asset_code, asset_issuer, asset_type,
			   trustline_limit, authorize_flag, clawback_flag,
			   signer_account, signer_weight, offer_id, seller_account,
			   created_at
		FROM effects
		%s
		ORDER BY ledger_sequence ASC, transaction_hash ASC, operation_index ASC, effect_index ASC
		LIMIT $%d
	`, whereClause, argNum)
	args = append(args, limit+1)

	rows, err := h.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, "", false, err
	}
	defer rows.Close()

	var effects []SilverEffect
	for rows.Next() {
		var e SilverEffect
		var accountID, amount, assetCode, assetIssuer, assetType sql.NullString
		var trustlineLimit, signerAccount, sellerAccount sql.NullString
		var authorizeFlag, clawbackFlag sql.NullBool
		var signerWeight sql.NullInt32
		var offerID sql.NullInt64

		err := rows.Scan(
			&e.LedgerSequence, &e.TransactionHash, &e.OperationIndex, &e.EffectIndex,
			&e.EffectType, &e.EffectTypeString, &accountID,
			&amount, &assetCode, &assetIssuer, &assetType,
			&trustlineLimit, &authorizeFlag, &clawbackFlag,
			&signerAccount, &signerWeight, &offerID, &sellerAccount,
			&e.Timestamp,
		)
		if err != nil {
			return nil, "", false, err
		}

		if accountID.Valid {
			e.AccountID = &accountID.String
		}
		if amount.Valid {
			e.Amount = &amount.String
		}
		if assetCode.Valid || assetType.Valid {
			asset := buildAssetInfo(assetType.String, assetCode.String, assetIssuer.String)
			e.Asset = &asset
		}
		if trustlineLimit.Valid {
			e.TrustlineLimit = &trustlineLimit.String
		}
		if authorizeFlag.Valid {
			e.AuthorizeFlag = &authorizeFlag.Bool
		}
		if clawbackFlag.Valid {
			e.ClawbackFlag = &clawbackFlag.Bool
		}
		if signerAccount.Valid {
			e.SignerAccount = &signerAccount.String
		}
		if signerWeight.Valid {
			sw := int(signerWeight.Int32)
			e.SignerWeight = &sw
		}
		if offerID.Valid {
			e.OfferID = &offerID.Int64
		}
		if sellerAccount.Valid {
			e.SellerAccount = &sellerAccount.String
		}

		effects = append(effects, e)
	}

	hasMore := len(effects) > limit
	if hasMore {
		effects = effects[:limit]
	}

	var nextCursor string
	if hasMore && len(effects) > 0 {
		last := effects[len(effects)-1]
		nextCursor = EffectCursor{
			LedgerSequence:  last.LedgerSequence,
			TransactionHash: last.TransactionHash,
			OperationIndex:  last.OperationIndex,
			EffectIndex:     last.EffectIndex,
		}.Encode()
	}

	return effects, nextCursor, hasMore, nil
}

// GetEffectTypes returns counts of each effect type
func (h *SilverHotReader) GetEffectTypes(ctx context.Context) ([]EffectTypeCount, int64, error) {
	query := `
		SELECT effect_type, effect_type_string, COUNT(*) as count
		FROM effects
		GROUP BY effect_type, effect_type_string
		ORDER BY count DESC
	`

	rows, err := h.db.QueryContext(ctx, query)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()

	var types []EffectTypeCount
	var total int64
	for rows.Next() {
		var t EffectTypeCount
		err := rows.Scan(&t.Type, &t.Name, &t.Count)
		if err != nil {
			return nil, 0, err
		}
		total += t.Count
		types = append(types, t)
	}

	return types, total, nil
}

// ============================================
// PHASE 8: SOROBAN TABLE METHODS
// ============================================

// GetContractCode returns contract code metadata by hash
func (h *SilverHotReader) GetContractCode(ctx context.Context, hash string) (*ContractCode, error) {
	query := `
		SELECT contract_code_hash, n_functions, n_instructions, n_data_segments,
			   n_data_segment_bytes, n_elem_segments, n_exports, n_globals,
			   n_imports, n_table_entries, n_types, last_modified_ledger, created_at
		FROM contract_code_current
		WHERE contract_code_hash = $1
	`

	var cc ContractCode
	var nFunctions, nInstructions, nDataSegments, nDataSegmentBytes sql.NullInt32
	var nElemSegments, nExports, nGlobals, nImports, nTableEntries, nTypes sql.NullInt32

	err := h.db.QueryRowContext(ctx, query, hash).Scan(
		&cc.Hash, &nFunctions, &nInstructions, &nDataSegments,
		&nDataSegmentBytes, &nElemSegments, &nExports, &nGlobals,
		&nImports, &nTableEntries, &nTypes, &cc.LastModifiedLedger, &cc.CreatedAt,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	cc.Metrics = ContractCodeMetrics{
		NFunctions:        int(nFunctions.Int32),
		NInstructions:     int(nInstructions.Int32),
		NDataSegments:     int(nDataSegments.Int32),
		NDataSegmentBytes: int(nDataSegmentBytes.Int32),
		NElemSegments:     int(nElemSegments.Int32),
		NExports:          int(nExports.Int32),
		NGlobals:          int(nGlobals.Int32),
		NImports:          int(nImports.Int32),
		NTableEntries:     int(nTableEntries.Int32),
		NTypes:            int(nTypes.Int32),
	}

	return &cc, nil
}

// GetTTL returns TTL entry for a specific key
func (h *SilverHotReader) GetTTL(ctx context.Context, keyHash string) (*TTLEntry, error) {
	query := `
		SELECT key_hash, live_until_ledger_seq, expired, last_modified_ledger, closed_at
		FROM ttl_current
		WHERE key_hash = $1
	`

	var entry TTLEntry
	err := h.db.QueryRowContext(ctx, query, keyHash).Scan(
		&entry.KeyHash, &entry.LiveUntilLedger, &entry.Expired,
		&entry.LastModifiedLedger, &entry.ClosedAt,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	return &entry, nil
}

// GetTTLExpiring returns TTL entries expiring within N ledgers
func (h *SilverHotReader) GetTTLExpiring(ctx context.Context, currentLedger int64, filters TTLFilters) ([]TTLEntry, string, bool, error) {
	var conditions []string
	var args []interface{}
	argNum := 1

	// Calculate expiration threshold
	expirationThreshold := currentLedger + filters.WithinLedgers
	conditions = append(conditions, fmt.Sprintf("live_until_ledger_seq <= $%d", argNum))
	args = append(args, expirationThreshold)
	argNum++

	// Only non-expired entries
	conditions = append(conditions, "expired = false")

	// Cursor pagination
	if filters.Cursor != nil {
		conditions = append(conditions, fmt.Sprintf("(live_until_ledger_seq, key_hash) > ($%d, $%d)", argNum, argNum+1))
		args = append(args, filters.Cursor.LiveUntilLedger, filters.Cursor.KeyHash)
		argNum += 2
	}

	whereClause := strings.Join(conditions, " AND ")

	limit := filters.Limit
	if limit <= 0 {
		limit = 100
	}

	query := fmt.Sprintf(`
		SELECT key_hash, live_until_ledger_seq, expired, last_modified_ledger, closed_at
		FROM ttl_current
		WHERE %s
		ORDER BY live_until_ledger_seq ASC, key_hash ASC
		LIMIT $%d
	`, whereClause, argNum)
	args = append(args, limit+1)

	rows, err := h.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, "", false, err
	}
	defer rows.Close()

	var entries []TTLEntry
	for rows.Next() {
		var e TTLEntry
		err := rows.Scan(&e.KeyHash, &e.LiveUntilLedger, &e.Expired,
			&e.LastModifiedLedger, &e.ClosedAt)
		if err != nil {
			return nil, "", false, err
		}
		e.LedgersRemaining = e.LiveUntilLedger - currentLedger
		entries = append(entries, e)
	}

	hasMore := len(entries) > limit
	if hasMore {
		entries = entries[:limit]
	}

	var nextCursor string
	if hasMore && len(entries) > 0 {
		last := entries[len(entries)-1]
		nextCursor = TTLCursor{
			LiveUntilLedger: last.LiveUntilLedger,
			KeyHash:         last.KeyHash,
		}.Encode()
	}

	return entries, nextCursor, hasMore, nil
}

// GetTTLExpired returns expired TTL entries
func (h *SilverHotReader) GetTTLExpired(ctx context.Context, filters TTLFilters) ([]TTLEntry, string, bool, error) {
	var conditions []string
	var args []interface{}
	argNum := 1

	conditions = append(conditions, "expired = true")

	// Cursor pagination
	if filters.Cursor != nil {
		conditions = append(conditions, fmt.Sprintf("(live_until_ledger_seq, key_hash) > ($%d, $%d)", argNum, argNum+1))
		args = append(args, filters.Cursor.LiveUntilLedger, filters.Cursor.KeyHash)
		argNum += 2
	}

	whereClause := strings.Join(conditions, " AND ")

	limit := filters.Limit
	if limit <= 0 {
		limit = 100
	}

	query := fmt.Sprintf(`
		SELECT key_hash, live_until_ledger_seq, expired, last_modified_ledger, closed_at
		FROM ttl_current
		WHERE %s
		ORDER BY live_until_ledger_seq DESC, key_hash ASC
		LIMIT $%d
	`, whereClause, argNum)
	args = append(args, limit+1)

	rows, err := h.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, "", false, err
	}
	defer rows.Close()

	var entries []TTLEntry
	for rows.Next() {
		var e TTLEntry
		err := rows.Scan(&e.KeyHash, &e.LiveUntilLedger, &e.Expired,
			&e.LastModifiedLedger, &e.ClosedAt)
		if err != nil {
			return nil, "", false, err
		}
		entries = append(entries, e)
	}

	hasMore := len(entries) > limit
	if hasMore {
		entries = entries[:limit]
	}

	var nextCursor string
	if hasMore && len(entries) > 0 {
		last := entries[len(entries)-1]
		nextCursor = TTLCursor{
			LiveUntilLedger: last.LiveUntilLedger,
			KeyHash:         last.KeyHash,
		}.Encode()
	}

	return entries, nextCursor, hasMore, nil
}

// GetEvictedKeys returns evicted storage keys, optionally filtered by contract
func (h *SilverHotReader) GetEvictedKeys(ctx context.Context, filters EvictionFilters) ([]EvictedKey, string, bool, error) {
	var conditions []string
	var args []interface{}
	argNum := 1

	if filters.ContractID != "" {
		conditions = append(conditions, fmt.Sprintf("contract_id = $%d", argNum))
		args = append(args, filters.ContractID)
		argNum++
	}

	// Cursor pagination
	if filters.Cursor != nil {
		conditions = append(conditions, fmt.Sprintf("(contract_id, key_hash, ledger_sequence) > ($%d, $%d, $%d)", argNum, argNum+1, argNum+2))
		args = append(args, filters.Cursor.ContractID, filters.Cursor.KeyHash, filters.Cursor.LedgerSequence)
		argNum += 3
	}

	whereClause := "1=1"
	if len(conditions) > 0 {
		whereClause = strings.Join(conditions, " AND ")
	}

	limit := filters.Limit
	if limit <= 0 {
		limit = 100
	}

	query := fmt.Sprintf(`
		SELECT contract_id, key_hash, ledger_sequence, closed_at
		FROM evicted_keys
		WHERE %s
		ORDER BY closed_at DESC, contract_id ASC, key_hash ASC, ledger_sequence ASC
		LIMIT $%d
	`, whereClause, argNum)
	args = append(args, limit+1)

	rows, err := h.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, "", false, err
	}
	defer rows.Close()

	var keys []EvictedKey
	for rows.Next() {
		var k EvictedKey
		err := rows.Scan(&k.ContractID, &k.KeyHash, &k.LedgerSequence, &k.ClosedAt)
		if err != nil {
			return nil, "", false, err
		}
		keys = append(keys, k)
	}

	hasMore := len(keys) > limit
	if hasMore {
		keys = keys[:limit]
	}

	var nextCursor string
	if hasMore && len(keys) > 0 {
		last := keys[len(keys)-1]
		nextCursor = EvictionCursor{
			ContractID:     last.ContractID,
			KeyHash:        last.KeyHash,
			LedgerSequence: last.LedgerSequence,
		}.Encode()
	}

	return keys, nextCursor, hasMore, nil
}

// GetRestoredKeys returns restored storage keys, optionally filtered by contract
func (h *SilverHotReader) GetRestoredKeys(ctx context.Context, filters EvictionFilters) ([]RestoredKey, string, bool, error) {
	var conditions []string
	var args []interface{}
	argNum := 1

	if filters.ContractID != "" {
		conditions = append(conditions, fmt.Sprintf("contract_id = $%d", argNum))
		args = append(args, filters.ContractID)
		argNum++
	}

	// Cursor pagination
	if filters.Cursor != nil {
		conditions = append(conditions, fmt.Sprintf("(contract_id, key_hash, ledger_sequence) > ($%d, $%d, $%d)", argNum, argNum+1, argNum+2))
		args = append(args, filters.Cursor.ContractID, filters.Cursor.KeyHash, filters.Cursor.LedgerSequence)
		argNum += 3
	}

	whereClause := "1=1"
	if len(conditions) > 0 {
		whereClause = strings.Join(conditions, " AND ")
	}

	limit := filters.Limit
	if limit <= 0 {
		limit = 100
	}

	query := fmt.Sprintf(`
		SELECT contract_id, key_hash, ledger_sequence, closed_at
		FROM restored_keys
		WHERE %s
		ORDER BY closed_at DESC, contract_id ASC, key_hash ASC, ledger_sequence ASC
		LIMIT $%d
	`, whereClause, argNum)
	args = append(args, limit+1)

	rows, err := h.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, "", false, err
	}
	defer rows.Close()

	var keys []RestoredKey
	for rows.Next() {
		var k RestoredKey
		err := rows.Scan(&k.ContractID, &k.KeyHash, &k.LedgerSequence, &k.ClosedAt)
		if err != nil {
			return nil, "", false, err
		}
		keys = append(keys, k)
	}

	hasMore := len(keys) > limit
	if hasMore {
		keys = keys[:limit]
	}

	var nextCursor string
	if hasMore && len(keys) > 0 {
		last := keys[len(keys)-1]
		nextCursor = EvictionCursor{
			ContractID:     last.ContractID,
			KeyHash:        last.KeyHash,
			LedgerSequence: last.LedgerSequence,
		}.Encode()
	}

	return keys, nextCursor, hasMore, nil
}

// GetSorobanConfig returns current Soroban network configuration
func (h *SilverHotReader) GetSorobanConfig(ctx context.Context) (*SorobanConfig, error) {
	query := `
		SELECT ledger_max_instructions, tx_max_instructions, fee_rate_per_instructions_increment,
			   tx_memory_limit, ledger_max_read_ledger_entries, ledger_max_read_bytes,
			   ledger_max_write_ledger_entries, ledger_max_write_bytes,
			   tx_max_read_ledger_entries, tx_max_read_bytes,
			   tx_max_write_ledger_entries, tx_max_write_bytes,
			   contract_max_size_bytes, last_modified_ledger, closed_at
		FROM config_settings_current
		WHERE config_setting_id = 1
	`

	var cfg SorobanConfig
	var ledgerMaxInstr, txMaxInstr, feeRate, txMemLimit sql.NullInt64
	var ledgerMaxReadEntries, ledgerMaxReadBytes, ledgerMaxWriteEntries, ledgerMaxWriteBytes sql.NullInt64
	var txMaxReadEntries, txMaxReadBytes, txMaxWriteEntries, txMaxWriteBytes sql.NullInt64
	var contractMaxSize sql.NullInt64

	err := h.db.QueryRowContext(ctx, query).Scan(
		&ledgerMaxInstr, &txMaxInstr, &feeRate, &txMemLimit,
		&ledgerMaxReadEntries, &ledgerMaxReadBytes, &ledgerMaxWriteEntries, &ledgerMaxWriteBytes,
		&txMaxReadEntries, &txMaxReadBytes, &txMaxWriteEntries, &txMaxWriteBytes,
		&contractMaxSize, &cfg.LastModifiedLedger, &cfg.UpdatedAt,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	cfg.Instructions = SorobanInstructionLimits{
		LedgerMax:           ledgerMaxInstr.Int64,
		TxMax:               txMaxInstr.Int64,
		FeeRatePerIncrement: feeRate.Int64,
	}
	cfg.Memory = SorobanMemoryLimits{
		TxLimitBytes: txMemLimit.Int64,
	}
	cfg.LedgerLimits = SorobanIOLimits{
		MaxReadEntries:  ledgerMaxReadEntries.Int64,
		MaxReadBytes:    ledgerMaxReadBytes.Int64,
		MaxWriteEntries: ledgerMaxWriteEntries.Int64,
		MaxWriteBytes:   ledgerMaxWriteBytes.Int64,
	}
	cfg.TxLimits = SorobanIOLimits{
		MaxReadEntries:  txMaxReadEntries.Int64,
		MaxReadBytes:    txMaxReadBytes.Int64,
		MaxWriteEntries: txMaxWriteEntries.Int64,
		MaxWriteBytes:   txMaxWriteBytes.Int64,
	}
	cfg.Contract = SorobanContractLimits{
		MaxSizeBytes: contractMaxSize.Int64,
	}

	return &cfg, nil
}

// GetContractData returns contract storage entries
func (h *SilverHotReader) GetContractData(ctx context.Context, filters ContractDataFilters) ([]ContractData, string, bool, error) {
	var conditions []string
	var args []interface{}
	argNum := 1

	// Contract ID is required
	if filters.ContractID != "" {
		conditions = append(conditions, fmt.Sprintf("contract_id = $%d", argNum))
		args = append(args, filters.ContractID)
		argNum++
	}

	// Durability filter
	if filters.Durability != "" {
		conditions = append(conditions, fmt.Sprintf("durability = $%d", argNum))
		args = append(args, filters.Durability)
		argNum++
	}

	// Single key lookup
	if filters.KeyHash != "" {
		conditions = append(conditions, fmt.Sprintf("key_hash = $%d", argNum))
		args = append(args, filters.KeyHash)
		argNum++
	}

	// Cursor pagination
	if filters.Cursor != nil {
		conditions = append(conditions, fmt.Sprintf("(contract_id, key_hash) > ($%d, $%d)", argNum, argNum+1))
		args = append(args, filters.Cursor.ContractID, filters.Cursor.KeyHash)
		argNum += 2
	}

	whereClause := "1=1"
	if len(conditions) > 0 {
		whereClause = strings.Join(conditions, " AND ")
	}

	limit := filters.Limit
	if limit <= 0 {
		limit = 100
	}

	query := fmt.Sprintf(`
		SELECT contract_id, key_hash, durability, data_value,
			   asset_type, asset_code, asset_issuer, last_modified_ledger
		FROM contract_data_current
		WHERE %s
		ORDER BY contract_id ASC, key_hash ASC
		LIMIT $%d
	`, whereClause, argNum)
	args = append(args, limit+1)

	rows, err := h.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, "", false, err
	}
	defer rows.Close()

	var data []ContractData
	for rows.Next() {
		var d ContractData
		var dataValue, assetType, assetCode, assetIssuer sql.NullString

		err := rows.Scan(&d.ContractID, &d.KeyHash, &d.Durability, &dataValue,
			&assetType, &assetCode, &assetIssuer, &d.LastModifiedLedger)
		if err != nil {
			return nil, "", false, err
		}

		if dataValue.Valid && dataValue.String != "" {
			d.DataValueXDR = &dataValue.String
		}
		if assetCode.Valid && assetCode.String != "" {
			issuer := assetIssuer.String
			d.Asset = &AssetInfo{
				Type:   assetType.String,
				Code:   assetCode.String,
				Issuer: &issuer,
			}
		}

		data = append(data, d)
	}

	hasMore := len(data) > limit
	if hasMore {
		data = data[:limit]
	}

	var nextCursor string
	if hasMore && len(data) > 0 {
		last := data[len(data)-1]
		nextCursor = ContractDataCursor{
			ContractID: last.ContractID,
			KeyHash:    last.KeyHash,
		}.Encode()
	}

	return data, nextCursor, hasMore, nil
}

// GetCurrentLedger returns the current ledger sequence from hot storage
func (h *SilverHotReader) GetCurrentLedger(ctx context.Context) (int64, error) {
	query := `SELECT MAX(ledger_sequence) FROM ttl_current`
	var ledger sql.NullInt64
	err := h.db.QueryRowContext(ctx, query).Scan(&ledger)
	if err != nil {
		return 0, err
	}
	return ledger.Int64, nil
}
