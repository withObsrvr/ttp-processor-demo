package main

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	_ "github.com/duckdb/duckdb-go/v2"
)

// SilverColdReader queries the analytics-ready Silver layer from DuckLake (cold storage)
type SilverColdReader struct {
	db          *sql.DB
	catalogName string
	schemaName  string
}

// NewSilverColdReader creates a new Silver cold layer reader
func NewSilverColdReader(config DuckLakeConfig) (*SilverColdReader, error) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		return nil, fmt.Errorf("failed to open duckdb: %w", err)
	}

	// Install extensions
	if _, err := db.Exec("INSTALL ducklake"); err != nil {
		return nil, fmt.Errorf("failed to install ducklake: %w", err)
	}
	if _, err := db.Exec("LOAD ducklake"); err != nil {
		return nil, fmt.Errorf("failed to load ducklake: %w", err)
	}
	if _, err := db.Exec("INSTALL httpfs"); err != nil {
		return nil, fmt.Errorf("failed to install httpfs: %w", err)
	}
	if _, err := db.Exec("LOAD httpfs"); err != nil {
		return nil, fmt.Errorf("failed to load httpfs: %w", err)
	}

	// Configure S3 credentials
	secretSQL := fmt.Sprintf(`CREATE SECRET (
		TYPE S3,
		KEY_ID '%s',
		SECRET '%s',
		REGION '%s',
		ENDPOINT '%s',
		URL_STYLE 'path'
	)`, config.AWSAccessKeyID, config.AWSSecretAccessKey, config.AWSRegion, config.AWSEndpoint)

	if _, err := db.Exec(secretSQL); err != nil {
		return nil, fmt.Errorf("failed to configure S3: %w", err)
	}

	// Attach DuckLake catalog
	attachSQL := fmt.Sprintf(`ATTACH '%s' AS %s (DATA_PATH '%s', METADATA_SCHEMA '%s')`,
		config.CatalogPath, config.CatalogName, config.DataPath, config.MetadataSchema)

	if _, err := db.Exec(attachSQL); err != nil {
		return nil, fmt.Errorf("failed to attach catalog: %w", err)
	}

	return &SilverColdReader{
		db:          db,
		catalogName: config.CatalogName,
		schemaName:  config.SchemaName,
	}, nil
}

// Close closes the database connection
func (r *SilverColdReader) Close() error {
	if r.db != nil {
		return r.db.Close()
	}
	return nil
}

// ============================================
// ACCOUNT QUERIES (Current State + History)
// ============================================

type AccountCurrent struct {
	AccountID            string `json:"account_id"`
	Balance              string `json:"balance"`
	SequenceNumber       string `json:"sequence_number"`
	NumSubentries        int64  `json:"num_subentries"`
	LastModifiedLedger   int64  `json:"last_modified_ledger"`
	UpdatedAt            string `json:"updated_at"`
}

type AccountSnapshot struct {
	AccountID      string  `json:"account_id"`
	Balance        string  `json:"balance"`
	SequenceNumber string  `json:"sequence_number"`
	LedgerSequence int64   `json:"ledger_sequence"`
	ClosedAt       string  `json:"closed_at"`
	ValidTo        *string `json:"valid_to,omitempty"`
}

// AccountSigner represents a signer on a Stellar account (matches Horizon format)
type AccountSigner struct {
	Key     string `json:"key"`
	Weight  int    `json:"weight"`
	Type    string `json:"type"`
	Sponsor string `json:"sponsor,omitempty"`
}

// AccountSignersResponse contains account signer information
type AccountSignersResponse struct {
	AccountID  string          `json:"account_id"`
	Signers    []AccountSigner `json:"signers"`
	Thresholds struct {
		LowThreshold  int `json:"low_threshold"`
		MedThreshold  int `json:"med_threshold"`
		HighThreshold int `json:"high_threshold"`
	} `json:"thresholds"`
}

// Balance represents a single balance (XLM or trustline)
type Balance struct {
	AssetType     string  `json:"asset_type"`
	AssetCode     string  `json:"asset_code"`
	AssetIssuer   *string `json:"asset_issuer,omitempty"`
	Balance       string  `json:"balance"`
	BalanceStroops int64  `json:"balance_stroops"`
	Limit         *string `json:"limit,omitempty"`
	IsAuthorized  *bool   `json:"is_authorized,omitempty"`
}

// AccountBalancesResponse contains all balances for an account
type AccountBalancesResponse struct {
	AccountID     string    `json:"account_id"`
	Balances      []Balance `json:"balances"`
	TotalBalances int       `json:"total_balances"`
}

// TokenHolder represents an account holding a specific token
type TokenHolder struct {
	AccountID      string `json:"account_id"`
	Balance        string `json:"balance"`
	BalanceStroops int64  `json:"balance_stroops"`
	Rank           int    `json:"rank"`
}

// AssetInfo contains asset identification information
type AssetInfo struct {
	Code   string  `json:"code"`
	Issuer *string `json:"issuer,omitempty"`
	Type   string  `json:"type"`
}

// TokenHoldersResponse contains holders of a specific token
type TokenHoldersResponse struct {
	Asset        AssetInfo     `json:"asset"`
	Holders      []TokenHolder `json:"holders"`
	TotalHolders int           `json:"total_holders"`
	Cursor       string        `json:"cursor,omitempty"`
	HasMore      bool          `json:"has_more"`
}

// TokenHoldersFilters contains filter options for token holders query
type TokenHoldersFilters struct {
	AssetCode   string
	AssetIssuer string
	MinBalance  *int64 // Minimum balance in stroops
	Limit       int
	Cursor      *TokenHoldersCursor
}

// TokenStats contains aggregated statistics for a token
type TokenStats struct {
	TotalHolders        int64   `json:"total_holders"`         // Accounts with balance > 0
	TotalTrustlines     int64   `json:"total_trustlines"`      // All trustlines (including 0 balance)
	CirculatingSupply   string  `json:"circulating_supply"`    // Total supply in circulation
	Top10Concentration  float64 `json:"top_10_concentration"`  // Fraction held by top 10 holders
	Transfers24h        int64   `json:"transfers_24h"`         // Transfer count in last 24h
	Volume24h           string  `json:"volume_24h"`            // Transfer volume in last 24h
	UniqueAccounts24h   int64   `json:"unique_accounts_24h"`   // Unique accounts in last 24h
}

// TokenStatsResponse contains token statistics with asset info
type TokenStatsResponse struct {
	Asset       AssetInfo  `json:"asset"`
	Stats       TokenStats `json:"stats"`
	GeneratedAt string     `json:"generated_at"`
}

// GetAccountCurrent returns the current state of an account
func (r *SilverColdReader) GetAccountCurrent(ctx context.Context, accountID string) (*AccountCurrent, error) {
	query := fmt.Sprintf(`
		SELECT
			account_id,
			balance,
			sequence_number,
			num_subentries,
			last_modified_ledger,
			updated_at
		FROM %s.%s.accounts_current
		WHERE account_id = ?
	`, r.catalogName, r.schemaName)

	var acc AccountCurrent
	err := r.db.QueryRowContext(ctx, query, accountID).Scan(
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

// GetAccountHistory returns historical snapshots of an account (backward compatible)
func (r *SilverColdReader) GetAccountHistory(ctx context.Context, accountID string, limit int) ([]AccountSnapshot, error) {
	return r.GetAccountHistoryWithCursor(ctx, accountID, limit, nil)
}

// GetAccountHistoryWithCursor returns historical snapshots with cursor-based pagination
func (r *SilverColdReader) GetAccountHistoryWithCursor(ctx context.Context, accountID string, limit int, cursor *AccountCursor) ([]AccountSnapshot, error) {
	query := fmt.Sprintf(`
		SELECT
			account_id,
			balance,
			sequence_number,
			ledger_sequence,
			closed_at,
			valid_to
		FROM %s.%s.accounts_snapshot
		WHERE account_id = ?
	`, r.catalogName, r.schemaName)

	args := []interface{}{accountID}

	// Cursor-based pagination: filter for records before the cursor position
	if cursor != nil {
		query += " AND ledger_sequence < ?"
		args = append(args, cursor.LedgerSequence)
	}

	query += " ORDER BY ledger_sequence DESC LIMIT ?"
	args = append(args, limit)

	rows, err := r.db.QueryContext(ctx, query, args...)
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

// GetTopAccounts returns top accounts by balance
func (r *SilverColdReader) GetTopAccounts(ctx context.Context, limit int) ([]AccountCurrent, error) {
	query := fmt.Sprintf(`
		SELECT
			account_id,
			balance,
			sequence_number,
			num_subentries,
			last_modified_ledger,
			updated_at
		FROM %s.%s.accounts_current
		ORDER BY CAST(balance AS DECIMAL) DESC
		LIMIT ?
	`, r.catalogName, r.schemaName)

	rows, err := r.db.QueryContext(ctx, query, limit)
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

// GetAccountsListWithCursor returns a paginated list of all accounts with cursor support
// Uses database-level pagination for scalability (handles millions of accounts)
func (r *SilverColdReader) GetAccountsListWithCursor(ctx context.Context, filters AccountListFilters) ([]AccountCurrent, error) {
	// Build base query with deduplication (accounts_current may have multiple versions per account)
	// Use ROW_NUMBER to get only the latest record per account_id
	query := fmt.Sprintf(`
		WITH latest_accounts AS (
			SELECT
				account_id,
				balance,
				sequence_number,
				num_subentries,
				last_modified_ledger,
				updated_at,
				ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY last_modified_ledger DESC) as rn
			FROM %s.%s.accounts_current
		)
		SELECT
			account_id,
			balance,
			sequence_number,
			num_subentries,
			last_modified_ledger,
			updated_at
		FROM latest_accounts
		WHERE rn = 1
	`, r.catalogName, r.schemaName)

	args := []interface{}{}

	// Apply minimum balance filter (balance is stored as decimal string in XLM)
	if filters.MinBalance != nil {
		// Convert stroops to XLM for comparison (divide by 10^7)
		minBalXLM := float64(*filters.MinBalance) / 10000000.0
		query += " AND CAST(balance AS DECIMAL) >= ?"
		args = append(args, minBalXLM)
	}

	// Cursor-based pagination - respects current sort field and order
	if filters.Cursor != nil {
		// Determine effective sort settings (use cursor's sort if available, otherwise use filters)
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
				query += " AND (last_modified_ledger > ? OR (last_modified_ledger = ? AND account_id > ?))"
			} else {
				query += " AND (last_modified_ledger < ? OR (last_modified_ledger = ? AND account_id > ?))"
			}
			args = append(args, filters.Cursor.LastModifiedLedger, filters.Cursor.LastModifiedLedger, filters.Cursor.AccountID)

		case "account_id":
			// Paginate directly on account_id
			if isAsc {
				query += " AND account_id > ?"
			} else {
				query += " AND account_id < ?"
			}
			args = append(args, filters.Cursor.AccountID)

		default: // "balance" or empty
			// Paginate based on balance, tie-break by account_id
			cursorBalXLM := float64(filters.Cursor.Balance) / 10000000.0
			if isAsc {
				query += " AND (CAST(balance AS DECIMAL) > ? OR (CAST(balance AS DECIMAL) = ? AND account_id > ?))"
			} else {
				query += " AND (CAST(balance AS DECIMAL) < ? OR (CAST(balance AS DECIMAL) = ? AND account_id > ?))"
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
	query += fmt.Sprintf(" ORDER BY %s %s, account_id ASC LIMIT ?", sortBy, sortOrder)
	args = append(args, filters.Limit)

	rows, err := r.db.QueryContext(ctx, query, args...)
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

type EnrichedOperation struct {
	TransactionHash    string  `json:"transaction_hash"`
	OperationID        int64   `json:"operation_id"`
	LedgerSequence     int64   `json:"ledger_sequence"`
	LedgerClosedAt     string  `json:"ledger_closed_at"`
	SourceAccount      string  `json:"source_account"`
	Type               int32   `json:"type"`
	TypeName           string  `json:"type_name"`
	Destination        *string `json:"destination,omitempty"`
	AssetCode          *string `json:"asset_code,omitempty"`
	AssetIssuer        *string `json:"asset_issuer,omitempty"`
	Amount             *string `json:"amount,omitempty"`
	TxSuccessful       bool    `json:"tx_successful"`
	TxFeeCharged       int64   `json:"tx_fee_charged"`
	IsPaymentOp        bool    `json:"is_payment_op"`
	IsSorobanOp        bool    `json:"is_soroban_op"`
}

// GetEnrichedOperations returns enriched operations with filters
func (r *SilverColdReader) GetEnrichedOperations(ctx context.Context, filters OperationFilters) ([]EnrichedOperation, error) {
	query := fmt.Sprintf(`
		SELECT
			transaction_hash,
			operation_index AS operation_id,
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
		FROM %s.%s.enriched_history_operations
		WHERE 1=1
	`, r.catalogName, r.schemaName)

	args := []interface{}{}

	if filters.AccountID != "" {
		query += " AND source_account = ?"
		args = append(args, filters.AccountID)
	}

	if filters.TxHash != "" {
		query += " AND transaction_hash = ?"
		args = append(args, filters.TxHash)
	}

	if filters.StartLedger > 0 {
		query += " AND ledger_sequence >= ?"
		args = append(args, filters.StartLedger)
	}

	if filters.EndLedger > 0 {
		query += " AND ledger_sequence <= ?"
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
		query += " AND (ledger_sequence < ? OR (ledger_sequence = ? AND operation_index < ?))"
		args = append(args, filters.Cursor.LedgerSequence, filters.Cursor.LedgerSequence, filters.Cursor.OperationIndex)
	}

	query += " ORDER BY ledger_sequence DESC, operation_index DESC LIMIT ?"
	args = append(args, filters.Limit)

	rows, err := r.db.QueryContext(ctx, query, args...)
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

type TokenTransfer struct {
	Timestamp             string  `json:"timestamp"`
	TransactionHash       string  `json:"transaction_hash"`
	LedgerSequence        int64   `json:"ledger_sequence"`
	SourceType            string  `json:"source_type"` // "classic" or "soroban"
	FromAccount           *string `json:"from_account,omitempty"`
	ToAccount             *string `json:"to_account,omitempty"`
	AssetCode             *string `json:"asset_code,omitempty"`
	AssetIssuer           *string `json:"asset_issuer,omitempty"`
	Amount                *string `json:"amount,omitempty"`
	TokenContractID       *string `json:"token_contract_id,omitempty"`
	TransactionSuccessful bool    `json:"transaction_successful"`
}

// GetTokenTransfers returns token transfers with filters
func (r *SilverColdReader) GetTokenTransfers(ctx context.Context, filters TransferFilters) ([]TokenTransfer, error) {
	query := fmt.Sprintf(`
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
		FROM %s.%s.token_transfers_raw
		WHERE transaction_successful = true
	`, r.catalogName, r.schemaName)

	args := []interface{}{}

	if filters.SourceType != "" {
		query += " AND source_type = ?"
		args = append(args, filters.SourceType)
	}

	if filters.AssetCode != "" {
		query += " AND asset_code = ?"
		args = append(args, filters.AssetCode)
	}

	if filters.FromAccount != "" {
		query += " AND from_account = ?"
		args = append(args, filters.FromAccount)
	}

	if filters.ToAccount != "" {
		query += " AND to_account = ?"
		args = append(args, filters.ToAccount)
	}

	if !filters.StartTime.IsZero() {
		query += " AND timestamp >= ?"
		args = append(args, filters.StartTime.Format(time.RFC3339))
	}

	if !filters.EndTime.IsZero() {
		query += " AND timestamp <= ?"
		args = append(args, filters.EndTime.Format(time.RFC3339))
	}

	// Cursor-based pagination: filter for records before the cursor position
	if filters.Cursor != nil {
		query += " AND (ledger_sequence < ? OR (ledger_sequence = ? AND timestamp < ?))"
		args = append(args, filters.Cursor.LedgerSequence, filters.Cursor.LedgerSequence, filters.Cursor.Timestamp.Format(time.RFC3339Nano))
	}

	query += " ORDER BY ledger_sequence DESC, timestamp DESC LIMIT ?"
	args = append(args, filters.Limit)

	rows, err := r.db.QueryContext(ctx, query, args...)
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

// GetTokenTransferStats returns aggregated transfer statistics
func (r *SilverColdReader) GetTokenTransferStats(ctx context.Context, groupBy string, startTime, endTime time.Time) ([]TransferStats, error) {
	var groupByClause string
	switch groupBy {
	case "asset":
		groupByClause = "asset_code, source_type"
	case "source_type":
		groupByClause = "source_type"
	case "hour":
		groupByClause = "DATE_TRUNC('hour', timestamp)"
	case "day":
		groupByClause = "DATE_TRUNC('day', timestamp)"
	default:
		return nil, fmt.Errorf("invalid group_by: %s", groupBy)
	}

	query := fmt.Sprintf(`
		SELECT
			%s,
			COUNT(*) as transfer_count,
			COUNT(DISTINCT from_account) as unique_senders,
			COUNT(DISTINCT to_account) as unique_receivers,
			SUM(CAST(amount AS DOUBLE)) as total_volume
		FROM %s.%s.token_transfers_raw
		WHERE transaction_successful = true
			AND timestamp >= ?
			AND timestamp <= ?
			AND amount IS NOT NULL
		GROUP BY %s
		ORDER BY total_volume DESC
		LIMIT 100
	`, groupByClause, r.catalogName, r.schemaName, groupByClause)

	rows, err := r.db.QueryContext(ctx, query, startTime.Format(time.RFC3339), endTime.Format(time.RFC3339))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var stats []TransferStats
	for rows.Next() {
		var s TransferStats
		// Scan based on group_by type
		if groupBy == "asset" {
			if err := rows.Scan(&s.AssetCode, &s.SourceType, &s.TransferCount,
				&s.UniqueSenders, &s.UniqueReceivers, &s.TotalVolume); err != nil {
				return nil, err
			}
		} else if groupBy == "source_type" {
			if err := rows.Scan(&s.SourceType, &s.TransferCount,
				&s.UniqueSenders, &s.UniqueReceivers, &s.TotalVolume); err != nil {
				return nil, err
			}
		} else {
			if err := rows.Scan(&s.TimeBucket, &s.TransferCount,
				&s.UniqueSenders, &s.UniqueReceivers, &s.TotalVolume); err != nil {
				return nil, err
			}
		}
		stats = append(stats, s)
	}

	return stats, nil
}

// ============================================
// HELPER TYPES AND FUNCTIONS
// ============================================

type OperationFilters struct {
	AccountID    string
	TxHash       string
	StartLedger  int64
	EndLedger    int64
	PaymentsOnly bool
	SorobanOnly  bool
	Limit        int
	Cursor       *OperationCursor // Decoded cursor for WHERE clause (pagination)
}

type TransferFilters struct {
	SourceType  string // "classic" or "soroban"
	AssetCode   string
	FromAccount string
	ToAccount   string
	StartTime   time.Time
	EndTime     time.Time
	Limit       int
	Cursor      *TransferCursor // Decoded cursor for WHERE clause (pagination)
}

type TransferStats struct {
	AssetCode       *string  `json:"asset_code,omitempty"`
	SourceType      *string  `json:"source_type,omitempty"`
	TimeBucket      *string  `json:"time_bucket,omitempty"`
	TransferCount   int64    `json:"transfer_count"`
	UniqueSenders   int64    `json:"unique_senders"`
	UniqueReceivers int64    `json:"unique_receivers"`
	TotalVolume     *float64 `json:"total_volume,omitempty"`
}

// ============================================
// NETWORK STATS QUERIES
// ============================================

// GetTotalAccountCount returns the total number of accounts from cold storage
func (r *SilverColdReader) GetTotalAccountCount(ctx context.Context) (int64, error) {
	query := fmt.Sprintf(`SELECT COUNT(*) FROM %s.%s.accounts_current`, r.catalogName, r.schemaName)
	var count int64
	err := r.db.QueryRowContext(ctx, query).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("failed to count accounts: %w", err)
	}
	return count, nil
}

// GetOperationStats24h returns 24h operation counts grouped by type from cold storage
func (r *SilverColdReader) GetOperationStats24h(ctx context.Context) (map[int32]int64, error) {
	query := fmt.Sprintf(`
		SELECT type, COUNT(*) as count
		FROM %s.%s.enriched_history_operations
		WHERE ledger_closed_at > NOW() - INTERVAL '24 hours'
		GROUP BY type
	`, r.catalogName, r.schemaName)

	rows, err := r.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to get operation stats: %w", err)
	}
	defer rows.Close()

	stats := make(map[int32]int64)
	for rows.Next() {
		var opType int32
		var count int64
		if err := rows.Scan(&opType, &count); err != nil {
			continue
		}
		stats[opType] = count
	}
	return stats, nil
}

// GetActiveAccounts24h returns count of accounts with activity in last 24h from cold storage
func (r *SilverColdReader) GetActiveAccounts24h(ctx context.Context) (int64, error) {
	query := fmt.Sprintf(`
		SELECT COUNT(DISTINCT source_account)
		FROM %s.%s.enriched_history_operations
		WHERE ledger_closed_at > NOW() - INTERVAL '24 hours'
	`, r.catalogName, r.schemaName)

	var count int64
	err := r.db.QueryRowContext(ctx, query).Scan(&count)
	if err != nil {
		return 0, err
	}
	return count, nil
}

func operationTypeName(opType int32) string {
	names := map[int32]string{
		0:  "CREATE_ACCOUNT",
		1:  "PAYMENT",
		2:  "PATH_PAYMENT_STRICT_RECEIVE",
		3:  "MANAGE_SELL_OFFER",
		4:  "CREATE_PASSIVE_SELL_OFFER",
		5:  "SET_OPTIONS",
		6:  "CHANGE_TRUST",
		7:  "ALLOW_TRUST",
		8:  "ACCOUNT_MERGE",
		9:  "INFLATION",
		10: "MANAGE_DATA",
		11: "BUMP_SEQUENCE",
		12: "MANAGE_BUY_OFFER",
		13: "PATH_PAYMENT_STRICT_SEND",
		14: "CREATE_CLAIMABLE_BALANCE",
		15: "CLAIM_CLAIMABLE_BALANCE",
		16: "BEGIN_SPONSORING_FUTURE_RESERVES",
		17: "END_SPONSORING_FUTURE_RESERVES",
		18: "REVOKE_SPONSORSHIP",
		19: "CLAWBACK",
		20: "CLAWBACK_CLAIMABLE_BALANCE",
		21: "SET_TRUST_LINE_FLAGS",
		22: "LIQUIDITY_POOL_DEPOSIT",
		23: "LIQUIDITY_POOL_WITHDRAW",
		24: "INVOKE_HOST_FUNCTION",
		25: "EXTEND_FOOTPRINT_TTL",
		26: "RESTORE_FOOTPRINT",
	}
	if name, ok := names[opType]; ok {
		return name
	}
	return fmt.Sprintf("UNKNOWN_%d", opType)
}

// ============================================
// PHASE 6: STATE TABLE TYPES (Offers, Liquidity Pools, Claimable Balances)
// ============================================

// OfferCurrent represents the current state of a DEX offer
type OfferCurrent struct {
	OfferID            int64    `json:"offer_id"`
	SellerID           string   `json:"seller_id"`
	Selling            AssetInfo `json:"selling"`
	Buying             AssetInfo `json:"buying"`
	Amount             string   `json:"amount"`
	Price              string   `json:"price"`
	PriceR             PriceR   `json:"price_r"`
	LastModifiedLedger int64    `json:"last_modified_ledger"`
	Sponsor            *string  `json:"sponsor,omitempty"`
}

// PriceR represents a rational price (numerator/denominator)
type PriceR struct {
	N int `json:"n"`
	D int `json:"d"`
}

// OfferFilters contains filter options for offer queries
type OfferFilters struct {
	SellerID           string
	SellingAssetCode   string
	SellingAssetIssuer string
	BuyingAssetCode    string
	BuyingAssetIssuer  string
	Limit              int
	Cursor             *OfferCursor
}

// LiquidityPoolCurrent represents the current state of a liquidity pool
type LiquidityPoolCurrent struct {
	PoolID             string            `json:"pool_id"`
	PoolType           string            `json:"pool_type"`
	FeeBP              int               `json:"fee_bp"`
	TrustlineCount     int               `json:"trustline_count"`
	TotalShares        string            `json:"total_shares"`
	Reserves           []PoolReserve     `json:"reserves"`
	LastModifiedLedger int64             `json:"last_modified_ledger"`
}

// PoolReserve represents a reserve in a liquidity pool
type PoolReserve struct {
	Asset  AssetInfo `json:"asset"`
	Amount string    `json:"amount"`
}

// LiquidityPoolFilters contains filter options for liquidity pool queries
type LiquidityPoolFilters struct {
	AssetCode   string
	AssetIssuer string
	Limit       int
	Cursor      *LiquidityPoolCursor
}

// ClaimableBalanceCurrent represents the current state of a claimable balance
type ClaimableBalanceCurrent struct {
	BalanceID          string    `json:"balance_id"`
	Sponsor            *string   `json:"sponsor,omitempty"`
	Asset              AssetInfo `json:"asset"`
	Amount             string    `json:"amount"`
	ClaimantsCount     int       `json:"claimants_count"`
	Flags              int       `json:"flags"`
	LastModifiedLedger int64     `json:"last_modified_ledger"`
}

// ClaimableBalanceFilters contains filter options for claimable balance queries
type ClaimableBalanceFilters struct {
	Sponsor     string
	AssetCode   string
	AssetIssuer string
	Limit       int
	Cursor      *ClaimableBalanceCursor
}

// ============================================
// PHASE 7: EVENT TABLE TYPES
// ============================================

// SilverTrade represents a trade from the Silver trades table
type SilverTrade struct {
	LedgerSequence  int64     `json:"ledger_sequence"`
	TransactionHash string    `json:"transaction_hash"`
	OperationIndex  int       `json:"operation_index"`
	TradeIndex      int       `json:"trade_index"`
	TradeType       string    `json:"trade_type"` // "orderbook" or "liquidity_pool"
	Timestamp       time.Time `json:"timestamp"`
	Seller          struct {
		AccountID string `json:"account_id"`
	} `json:"seller"`
	Selling struct {
		Asset  AssetInfo `json:"asset"`
		Amount string    `json:"amount"`
	} `json:"selling"`
	Buyer struct {
		AccountID string `json:"account_id"`
	} `json:"buyer"`
	Buying struct {
		Asset  AssetInfo `json:"asset"`
		Amount string    `json:"amount"`
	} `json:"buying"`
	Price string `json:"price"`
}

// TradeFilters contains filter options for trade queries
type TradeFilters struct {
	SellerAccount      string
	BuyerAccount       string
	AccountID          string // Either seller OR buyer
	SellingAssetCode   string
	SellingAssetIssuer string
	BuyingAssetCode    string
	BuyingAssetIssuer  string
	StartTime          time.Time
	EndTime            time.Time
	Limit              int
	Cursor             *TradeCursor
}

// TradeStats represents aggregated trade statistics
type TradeStats struct {
	Group         string  `json:"group"`
	TradeCount    int64   `json:"trade_count"`
	VolumeSelling string  `json:"volume_selling"`
	VolumeBuying  string  `json:"volume_buying"`
	UniqueSellers int64   `json:"unique_sellers"`
	UniqueBuyers  int64   `json:"unique_buyers"`
	AvgPrice      *string `json:"avg_price,omitempty"`
}

// SilverEffect represents an effect from the Silver effects table
type SilverEffect struct {
	LedgerSequence   int64     `json:"ledger_sequence"`
	TransactionHash  string    `json:"transaction_hash"`
	OperationIndex   int       `json:"operation_index"`
	EffectIndex      int       `json:"effect_index"`
	EffectType       int       `json:"effect_type"`
	EffectTypeString string    `json:"effect_type_string"`
	AccountID        *string   `json:"account_id,omitempty"`
	Asset            *AssetInfo `json:"asset,omitempty"`
	Amount           *string   `json:"amount,omitempty"`
	TrustlineLimit   *string   `json:"trustline_limit,omitempty"`
	AuthorizeFlag    *bool     `json:"authorize_flag,omitempty"`
	ClawbackFlag     *bool     `json:"clawback_flag,omitempty"`
	SignerAccount    *string   `json:"signer_account,omitempty"`
	SignerWeight     *int      `json:"signer_weight,omitempty"`
	OfferID          *int64    `json:"offer_id,omitempty"`
	SellerAccount    *string   `json:"seller_account,omitempty"`
	Timestamp        time.Time `json:"timestamp"`
}

// EffectFilters contains filter options for effect queries
type EffectFilters struct {
	AccountID       string
	EffectType      string // Can be int (as string) or effect type name
	LedgerSequence  int64
	TransactionHash string
	StartTime       time.Time
	EndTime         time.Time
	Limit           int
	Cursor          *EffectCursor
}

// EffectTypeCount represents an effect type with its count
type EffectTypeCount struct {
	Type  int    `json:"type"`
	Name  string `json:"name"`
	Count int64  `json:"count"`
}

// ============================================
// PHASE 8: SOROBAN TABLE TYPES
// ============================================

// ContractCode represents a deployed Soroban WASM contract
type ContractCode struct {
	Hash    string              `json:"hash"`
	Metrics ContractCodeMetrics `json:"metrics"`
	LastModifiedLedger int64   `json:"last_modified_ledger"`
	CreatedAt          time.Time `json:"created_at"`
}

// ContractCodeMetrics contains WASM complexity metrics
type ContractCodeMetrics struct {
	NFunctions        int `json:"n_functions"`
	NInstructions     int `json:"n_instructions"`
	NDataSegments     int `json:"n_data_segments"`
	NDataSegmentBytes int `json:"n_data_segment_bytes"`
	NElemSegments     int `json:"n_elem_segments"`
	NExports          int `json:"n_exports"`
	NGlobals          int `json:"n_globals"`
	NImports          int `json:"n_imports"`
	NTableEntries     int `json:"n_table_entries"`
	NTypes            int `json:"n_types"`
}

// TTLEntry represents a TTL entry for a Soroban storage key
type TTLEntry struct {
	KeyHash            string    `json:"key_hash"`
	LiveUntilLedger    int64     `json:"live_until_ledger"`
	LedgersRemaining   int64     `json:"ledgers_remaining,omitempty"`
	Expired            bool      `json:"expired"`
	LastModifiedLedger int64     `json:"last_modified_ledger"`
	ClosedAt           time.Time `json:"closed_at"`
}

// TTLFilters contains filter options for TTL queries
type TTLFilters struct {
	KeyHash       string
	WithinLedgers int64  // Entries expiring within N ledgers
	ExpiredOnly   bool   // Only show expired entries
	Limit         int
	Cursor        *TTLCursor
}

// EvictedKey represents a storage key that was evicted
type EvictedKey struct {
	ContractID     string    `json:"contract_id"`
	KeyHash        string    `json:"key_hash"`
	LedgerSequence int64     `json:"ledger_sequence"`
	ClosedAt       time.Time `json:"closed_at"`
}

// EvictionFilters contains filter options for eviction/restoration queries
type EvictionFilters struct {
	ContractID string
	Limit      int
	Cursor     *EvictionCursor
}

// RestoredKey represents a storage key that was restored
type RestoredKey struct {
	ContractID     string    `json:"contract_id"`
	KeyHash        string    `json:"key_hash"`
	LedgerSequence int64     `json:"ledger_sequence"`
	ClosedAt       time.Time `json:"closed_at"`
}

// SorobanConfig represents Soroban network configuration
type SorobanConfig struct {
	Instructions  SorobanInstructionLimits `json:"instructions"`
	Memory        SorobanMemoryLimits      `json:"memory"`
	LedgerLimits  SorobanIOLimits          `json:"ledger_limits"`
	TxLimits      SorobanIOLimits          `json:"tx_limits"`
	Contract      SorobanContractLimits    `json:"contract"`
	LastModifiedLedger int64               `json:"last_modified_ledger"`
	UpdatedAt     time.Time                `json:"updated_at"`
}

// SorobanInstructionLimits contains instruction-related limits
type SorobanInstructionLimits struct {
	LedgerMax          int64 `json:"ledger_max"`
	TxMax              int64 `json:"tx_max"`
	FeeRatePerIncrement int64 `json:"fee_rate_per_increment"`
}

// SorobanMemoryLimits contains memory-related limits
type SorobanMemoryLimits struct {
	TxLimitBytes int64 `json:"tx_limit_bytes"`
}

// SorobanIOLimits contains read/write limits
type SorobanIOLimits struct {
	MaxReadEntries  int64 `json:"max_read_entries"`
	MaxReadBytes    int64 `json:"max_read_bytes"`
	MaxWriteEntries int64 `json:"max_write_entries"`
	MaxWriteBytes   int64 `json:"max_write_bytes"`
}

// SorobanContractLimits contains contract-related limits
type SorobanContractLimits struct {
	MaxSizeBytes int64 `json:"max_size_bytes"`
}

// ContractData represents a contract storage entry
type ContractData struct {
	ContractID         string     `json:"contract_id"`
	KeyHash            string     `json:"key_hash"`
	Durability         string     `json:"durability"`
	DataValueXDR       *string    `json:"data_value_xdr,omitempty"`
	Asset              *AssetInfo `json:"asset,omitempty"`
	LastModifiedLedger int64      `json:"last_modified_ledger"`
}

// ContractDataFilters contains filter options for contract data queries
type ContractDataFilters struct {
	ContractID string
	KeyHash    string
	Durability string // "persistent" or "temporary"
	Limit      int
	Cursor     *ContractDataCursor
}
