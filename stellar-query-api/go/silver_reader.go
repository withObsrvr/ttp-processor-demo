package main

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	_ "github.com/duckdb/duckdb-go/v2"
)

// SilverReader queries the analytics-ready Silver layer
type SilverReader struct {
	db          *sql.DB
	catalogName string
	schemaName  string
}

// NewSilverReader creates a new Silver layer reader
func NewSilverReader(config DuckLakeConfig) (*SilverReader, error) {
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

	return &SilverReader{
		db:          db,
		catalogName: config.CatalogName,
		schemaName:  config.SchemaName,
	}, nil
}

// Close closes the database connection
func (r *SilverReader) Close() error {
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

// GetAccountCurrent returns the current state of an account
func (r *SilverReader) GetAccountCurrent(ctx context.Context, accountID string) (*AccountCurrent, error) {
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

// GetAccountHistory returns historical snapshots of an account
func (r *SilverReader) GetAccountHistory(ctx context.Context, accountID string, limit int) ([]AccountSnapshot, error) {
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
		ORDER BY ledger_sequence DESC
		LIMIT ?
	`, r.catalogName, r.schemaName)

	rows, err := r.db.QueryContext(ctx, query, accountID, limit)
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
func (r *SilverReader) GetTopAccounts(ctx context.Context, limit int) ([]AccountCurrent, error) {
	query := fmt.Sprintf(`
		SELECT
			account_id,
			balance,
			sequence_number,
			num_subentries,
			last_modified_ledger,
			updated_at
		FROM %s.%s.accounts_current
		ORDER BY CAST(balance AS BIGINT) DESC
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
func (r *SilverReader) GetEnrichedOperations(ctx context.Context, filters OperationFilters) ([]EnrichedOperation, error) {
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
func (r *SilverReader) GetTokenTransfers(ctx context.Context, filters TransferFilters) ([]TokenTransfer, error) {
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

	query += " ORDER BY timestamp DESC LIMIT ?"
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
func (r *SilverReader) GetTokenTransferStats(ctx context.Context, groupBy string, startTime, endTime time.Time) ([]TransferStats, error) {
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
}

type TransferFilters struct {
	SourceType  string // "classic" or "soroban"
	AssetCode   string
	FromAccount string
	ToAccount   string
	StartTime   time.Time
	EndTime     time.Time
	Limit       int
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
