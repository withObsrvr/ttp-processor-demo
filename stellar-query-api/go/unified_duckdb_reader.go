package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"strings"
	"time"

	_ "github.com/duckdb/duckdb-go/v2"
)

// UnifiedDuckDBReader queries both hot (PostgreSQL) and cold (DuckLake) data
// through a single DuckDB connection using ATTACH mechanism.
// This eliminates the need for Go-layer merging of results.
type UnifiedDuckDBReader struct {
	db         *sql.DB
	hotSchema  string // e.g., "hot_db" or "hot_db.public"
	coldSchema string // e.g., "cold_db.silver"
	config     UnifiedReaderConfig
}

// NewUnifiedDuckDBReader creates a new unified reader that ATTACHes both
// PostgreSQL (hot) and DuckLake (cold) databases to a single DuckDB instance.
func NewUnifiedDuckDBReader(config UnifiedReaderConfig) (*UnifiedDuckDBReader, error) {
	// Open in-memory DuckDB instance
	db, err := sql.Open("duckdb", "")
	if err != nil {
		return nil, fmt.Errorf("failed to open DuckDB: %w", err)
	}

	// Install and load required extensions
	extensions := []struct {
		name    string
		install string
		load    string
	}{
		{"postgres", "INSTALL postgres", "LOAD postgres"},
		{"ducklake", "INSTALL ducklake", "LOAD ducklake"},
		{"httpfs", "INSTALL httpfs", "LOAD httpfs"},
	}

	for _, ext := range extensions {
		if _, err := db.Exec(ext.install); err != nil {
			db.Close()
			return nil, fmt.Errorf("failed to install %s extension: %w", ext.name, err)
		}
		if _, err := db.Exec(ext.load); err != nil {
			db.Close()
			return nil, fmt.Errorf("failed to load %s extension: %w", ext.name, err)
		}
		log.Printf("✅ Loaded DuckDB extension: %s", ext.name)
	}

	// Configure S3 credentials for DuckLake
	s3Secret := fmt.Sprintf(`CREATE SECRET s3_secret (
		TYPE S3,
		KEY_ID '%s',
		SECRET '%s',
		REGION '%s',
		ENDPOINT '%s',
		URL_STYLE 'path'
	)`, config.S3.KeyID, config.S3.Secret, config.S3.Region, config.S3.Endpoint)

	if _, err := db.Exec(s3Secret); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to create S3 secret: %w", err)
	}
	log.Println("✅ Configured S3 credentials for DuckLake")

	// ATTACH PostgreSQL (hot buffer)
	// Build connection string for postgres extension
	pgConnStr := fmt.Sprintf("dbname=%s host=%s port=%d user=%s password=%s",
		config.Postgres.Database, config.Postgres.Host, config.Postgres.Port,
		config.Postgres.User, config.Postgres.Password)

	if config.Postgres.SSLMode != "" {
		pgConnStr += fmt.Sprintf(" sslmode=%s", config.Postgres.SSLMode)
	}

	pgAttach := fmt.Sprintf(`ATTACH '%s' AS hot_db (TYPE POSTGRES)`, pgConnStr)
	if _, err := db.Exec(pgAttach); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to attach PostgreSQL: %w", err)
	}
	log.Println("✅ Attached PostgreSQL as hot_db")

	// ATTACH DuckLake (cold storage)
	dlAttach := fmt.Sprintf(`ATTACH '%s' AS cold_db (DATA_PATH '%s', METADATA_SCHEMA '%s')`,
		config.DuckLake.CatalogPath, config.DuckLake.DataPath, config.DuckLake.MetadataSchema)

	if _, err := db.Exec(dlAttach); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to attach DuckLake: %w", err)
	}
	log.Println("✅ Attached DuckLake as cold_db")

	// Determine schema paths
	hotSchema := "hot_db"
	if config.Postgres.Schema != "" {
		hotSchema = fmt.Sprintf("hot_db.%s", config.Postgres.Schema)
	}

	coldSchema := fmt.Sprintf("cold_db.%s", config.DuckLake.SchemaName)

	return &UnifiedDuckDBReader{
		db:         db,
		hotSchema:  hotSchema,
		coldSchema: coldSchema,
		config:     config,
	}, nil
}

// Close closes the DuckDB connection (which detaches all databases)
func (r *UnifiedDuckDBReader) Close() error {
	if r.db != nil {
		return r.db.Close()
	}
	return nil
}

// HealthCheck verifies both hot and cold databases are accessible
func (r *UnifiedDuckDBReader) HealthCheck(ctx context.Context) (*UnifiedHealthStatus, error) {
	status := &UnifiedHealthStatus{
		HotDB:  "unknown",
		ColdDB: "unknown",
	}

	// Check hot_db (PostgreSQL)
	var hotOK int
	hotQuery := fmt.Sprintf("SELECT 1 FROM %s.accounts_current LIMIT 1", r.hotSchema)
	if err := r.db.QueryRowContext(ctx, hotQuery).Scan(&hotOK); err != nil {
		if strings.Contains(err.Error(), "no rows") || err == sql.ErrNoRows {
			status.HotDB = "connected" // Table exists but empty
		} else {
			status.HotDB = fmt.Sprintf("error: %v", err)
		}
	} else {
		status.HotDB = "connected"
	}

	// Check cold_db (DuckLake)
	var coldOK int
	coldQuery := fmt.Sprintf("SELECT 1 FROM %s.accounts_current LIMIT 1", r.coldSchema)
	if err := r.db.QueryRowContext(ctx, coldQuery).Scan(&coldOK); err != nil {
		if strings.Contains(err.Error(), "no rows") || err == sql.ErrNoRows {
			status.ColdDB = "connected" // Table exists but empty
		} else {
			status.ColdDB = fmt.Sprintf("error: %v", err)
		}
	} else {
		status.ColdDB = "connected"
	}

	return status, nil
}

// UnifiedHealthStatus represents the health of both attached databases
type UnifiedHealthStatus struct {
	HotDB  string `json:"hot_db"`
	ColdDB string `json:"cold_db"`
}

// ============================================
// ACCOUNT QUERIES (Cycle 2 - placeholders for now)
// These will be implemented in Cycle 2
// ============================================

// GetAccountCurrent returns the current state of an account
// Queries hot first, falls back to cold if not found (via SQL UNION)
func (r *UnifiedDuckDBReader) GetAccountCurrent(ctx context.Context, accountID string) (*AccountCurrent, error) {
	// Priority-based UNION: hot (priority 1) wins over cold (priority 2)
	query := fmt.Sprintf(`
		SELECT account_id, balance, sequence_number, num_subentries,
		       last_modified_ledger, updated_at
		FROM (
			SELECT account_id, balance, sequence_number, num_subentries,
			       last_modified_ledger, updated_at, 1 as priority
			FROM %s.accounts_current WHERE account_id = $1
			UNION ALL
			SELECT account_id, balance, sequence_number, num_subentries,
			       last_modified_ledger, updated_at, 2 as priority
			FROM %s.accounts_current WHERE account_id = $1
		) combined
		ORDER BY priority ASC
		LIMIT 1
	`, r.hotSchema, r.coldSchema)

	var acc AccountCurrent
	err := r.db.QueryRowContext(ctx, query, accountID).Scan(
		&acc.AccountID, &acc.Balance, &acc.SequenceNumber,
		&acc.NumSubentries, &acc.LastModifiedLedger, &acc.UpdatedAt,
	)

	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("unified GetAccountCurrent: %w", err)
	}

	return &acc, nil
}

// GetAccountHistoryWithCursor returns historical snapshots with cursor-based pagination
// Merges hot and cold data via SQL UNION with deduplication
// Returns: snapshots, nextCursor, hasMore, error (matching UnifiedSilverReader interface)
func (r *UnifiedDuckDBReader) GetAccountHistoryWithCursor(ctx context.Context, accountID string, limit int, cursor *AccountCursor) ([]AccountSnapshot, string, bool, error) {
	// Request one extra to detect has_more
	requestLimit := limit + 1

	query := fmt.Sprintf(`
		WITH combined AS (
			SELECT account_id, balance, sequence_number, ledger_sequence,
			       closed_at, valid_to, 1 as source
			FROM %s.accounts_snapshot WHERE account_id = $1
			UNION ALL
			SELECT account_id, balance, sequence_number, ledger_sequence,
			       closed_at, valid_to, 2 as source
			FROM %s.accounts_snapshot WHERE account_id = $1
		)
		SELECT DISTINCT ON (ledger_sequence)
		       account_id, balance, sequence_number, ledger_sequence,
		       closed_at, valid_to
		FROM combined
		WHERE ($2::bigint IS NULL OR ledger_sequence < $2)
		ORDER BY ledger_sequence DESC
		LIMIT $3
	`, r.hotSchema, r.coldSchema)

	var cursorLedger *int64
	if cursor != nil {
		cursorLedger = &cursor.LedgerSequence
	}

	rows, err := r.db.QueryContext(ctx, query, accountID, cursorLedger, requestLimit)
	if err != nil {
		return nil, "", false, fmt.Errorf("unified GetAccountHistoryWithCursor: %w", err)
	}
	defer rows.Close()

	var snapshots []AccountSnapshot
	for rows.Next() {
		var snap AccountSnapshot
		if err := rows.Scan(&snap.AccountID, &snap.Balance, &snap.SequenceNumber,
			&snap.LedgerSequence, &snap.ClosedAt, &snap.ValidTo); err != nil {
			return nil, "", false, err
		}
		snapshots = append(snapshots, snap)
	}

	// Determine has_more and trim to requested limit
	hasMore := len(snapshots) > limit
	if hasMore {
		snapshots = snapshots[:limit]
	}

	// Generate next cursor from last result
	var nextCursor string
	if len(snapshots) > 0 && hasMore {
		last := snapshots[len(snapshots)-1]
		c := AccountCursor{LedgerSequence: last.LedgerSequence}
		nextCursor = c.Encode()
	}

	return snapshots, nextCursor, hasMore, nil
}

// GetTopAccounts returns top accounts by balance
// Merges hot and cold with deduplication (hot takes precedence)
func (r *UnifiedDuckDBReader) GetTopAccounts(ctx context.Context, limit int) ([]AccountCurrent, error) {
	query := fmt.Sprintf(`
		WITH combined AS (
			SELECT account_id, balance, sequence_number, num_subentries,
			       last_modified_ledger, updated_at, 1 as source
			FROM %s.accounts_current
			UNION ALL
			SELECT account_id, balance, sequence_number, num_subentries,
			       last_modified_ledger, updated_at, 2 as source
			FROM %s.accounts_current
		),
		deduplicated AS (
			SELECT DISTINCT ON (account_id)
			       account_id, balance, sequence_number, num_subentries,
			       last_modified_ledger, updated_at
			FROM combined
			ORDER BY account_id, source ASC
		)
		SELECT account_id, balance, sequence_number, num_subentries,
		       last_modified_ledger, updated_at
		FROM deduplicated
		ORDER BY CAST(balance AS DECIMAL) DESC
		LIMIT $1
	`, r.hotSchema, r.coldSchema)

	rows, err := r.db.QueryContext(ctx, query, limit)
	if err != nil {
		return nil, fmt.Errorf("unified GetTopAccounts: %w", err)
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

// GetAccountsListWithCursor returns a paginated list of all accounts
// Uses cold as source of truth, hot overlays recent updates
// Returns: accounts, nextCursor, hasMore, error (matching UnifiedSilverReader interface)
func (r *UnifiedDuckDBReader) GetAccountsListWithCursor(ctx context.Context, filters AccountListFilters) ([]AccountCurrent, string, bool, error) {
	// Request one extra to detect has_more
	requestLimit := filters.Limit + 1

	// Build query with deduplication - hot takes precedence over cold
	query := fmt.Sprintf(`
		WITH combined AS (
			SELECT account_id, balance, sequence_number, num_subentries,
			       last_modified_ledger, updated_at, 1 as source
			FROM %s.accounts_current
			UNION ALL
			SELECT account_id, balance, sequence_number, num_subentries,
			       last_modified_ledger, updated_at, 2 as source
			FROM %s.accounts_current
		),
		deduplicated AS (
			SELECT DISTINCT ON (account_id)
			       account_id, balance, sequence_number, num_subentries,
			       last_modified_ledger, updated_at
			FROM combined
			ORDER BY account_id, source ASC
		)
		SELECT account_id, balance, sequence_number, num_subentries,
		       last_modified_ledger, updated_at
		FROM deduplicated
		WHERE 1=1
	`, r.hotSchema, r.coldSchema)

	args := []interface{}{}
	argNum := 1

	// Apply minimum balance filter
	if filters.MinBalance != nil {
		minBalXLM := float64(*filters.MinBalance) / 10000000.0
		query += fmt.Sprintf(" AND CAST(balance AS DECIMAL) >= $%d", argNum)
		args = append(args, minBalXLM)
		argNum++
	}

	// Cursor-based pagination
	if filters.Cursor != nil {
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
			if isAsc {
				query += fmt.Sprintf(" AND (last_modified_ledger > $%d OR (last_modified_ledger = $%d AND account_id > $%d))", argNum, argNum, argNum+1)
			} else {
				query += fmt.Sprintf(" AND (last_modified_ledger < $%d OR (last_modified_ledger = $%d AND account_id > $%d))", argNum, argNum, argNum+1)
			}
			args = append(args, filters.Cursor.LastModifiedLedger, filters.Cursor.AccountID)
			argNum += 2

		case "account_id":
			if isAsc {
				query += fmt.Sprintf(" AND account_id > $%d", argNum)
			} else {
				query += fmt.Sprintf(" AND account_id < $%d", argNum)
			}
			args = append(args, filters.Cursor.AccountID)
			argNum++

		default: // "balance"
			cursorBalXLM := float64(filters.Cursor.Balance) / 10000000.0
			if isAsc {
				query += fmt.Sprintf(" AND (CAST(balance AS DECIMAL) > $%d OR (CAST(balance AS DECIMAL) = $%d AND account_id > $%d))", argNum, argNum, argNum+1)
			} else {
				query += fmt.Sprintf(" AND (CAST(balance AS DECIMAL) < $%d OR (CAST(balance AS DECIMAL) = $%d AND account_id > $%d))", argNum, argNum, argNum+1)
			}
			args = append(args, cursorBalXLM, filters.Cursor.AccountID)
			argNum += 2
		}
	}

	// Sorting
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

	query += fmt.Sprintf(" ORDER BY %s %s, account_id ASC LIMIT $%d", sortBy, sortOrder, argNum)
	args = append(args, requestLimit)

	rows, err := r.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, "", false, fmt.Errorf("unified GetAccountsListWithCursor: %w", err)
	}
	defer rows.Close()

	var accounts []AccountCurrent
	for rows.Next() {
		var acc AccountCurrent
		if err := rows.Scan(&acc.AccountID, &acc.Balance, &acc.SequenceNumber,
			&acc.NumSubentries, &acc.LastModifiedLedger, &acc.UpdatedAt); err != nil {
			return nil, "", false, err
		}
		accounts = append(accounts, acc)
	}

	// Determine has_more and trim to requested limit
	hasMore := len(accounts) > filters.Limit
	if hasMore {
		accounts = accounts[:filters.Limit]
	}

	// Generate next cursor from last result
	var nextCursor string
	if len(accounts) > 0 && hasMore {
		last := accounts[len(accounts)-1]
		balanceStroops, err := parseBalanceToStroops(last.Balance)
		if err != nil {
			log.Printf("Warning: failed to parse balance '%s' for cursor: %v", last.Balance, err)
			balanceStroops = 0
		}

		// Determine effective sort settings for cursor
		effSortBy := filters.SortBy
		if effSortBy == "" {
			effSortBy = "balance"
		}
		effSortOrder := filters.SortOrder
		if effSortOrder == "" {
			effSortOrder = "desc"
		}

		cursor := AccountListCursor{
			Balance:            balanceStroops,
			LastModifiedLedger: last.LastModifiedLedger,
			AccountID:          last.AccountID,
			SortBy:             effSortBy,
			SortOrder:          effSortOrder,
		}
		nextCursor = cursor.Encode()
	}

	return accounts, nextCursor, hasMore, nil
}

// ============================================
// OPERATIONS QUERIES
// ============================================

// GetEnrichedOperations returns enriched operations without cursor (backward compatible)
func (r *UnifiedDuckDBReader) GetEnrichedOperations(ctx context.Context, filters OperationFilters) ([]EnrichedOperation, error) {
	operations, _, _, err := r.GetEnrichedOperationsWithCursor(ctx, filters)
	return operations, err
}

// GetEnrichedOperationsWithCursor returns enriched operations with filters and cursor pagination
// Returns: operations, nextCursor, hasMore, error (matching UnifiedSilverReader interface)
func (r *UnifiedDuckDBReader) GetEnrichedOperationsWithCursor(ctx context.Context, filters OperationFilters) ([]EnrichedOperation, string, bool, error) {
	// Request one extra to detect has_more
	requestLimit := filters.Limit + 1
	// Build WHERE clause for filters
	whereClause := "WHERE 1=1"
	args := []interface{}{}
	argNum := 1

	if filters.AccountID != "" {
		whereClause += fmt.Sprintf(" AND source_account = $%d", argNum)
		args = append(args, filters.AccountID)
		argNum++
	}

	if filters.TxHash != "" {
		whereClause += fmt.Sprintf(" AND transaction_hash = $%d", argNum)
		args = append(args, filters.TxHash)
		argNum++
	}

	if filters.StartLedger > 0 {
		whereClause += fmt.Sprintf(" AND ledger_sequence >= $%d", argNum)
		args = append(args, filters.StartLedger)
		argNum++
	}

	if filters.EndLedger > 0 {
		whereClause += fmt.Sprintf(" AND ledger_sequence <= $%d", argNum)
		args = append(args, filters.EndLedger)
		argNum++
	}

	if filters.PaymentsOnly {
		whereClause += " AND is_payment_op = true"
	}

	if filters.SorobanOnly {
		whereClause += " AND is_soroban_op = true"
	}

	// Cursor pagination
	cursorClause := ""
	if filters.Cursor != nil {
		cursorClause = fmt.Sprintf(" AND (ledger_sequence < $%d OR (ledger_sequence = $%d AND operation_index < $%d))",
			argNum, argNum, argNum+1)
		args = append(args, filters.Cursor.LedgerSequence, filters.Cursor.OperationIndex)
		argNum += 2
	}

	query := fmt.Sprintf(`
		WITH combined AS (
			SELECT transaction_hash, operation_index AS operation_id, ledger_sequence,
			       ledger_closed_at, source_account, type, destination,
			       asset_code, asset_issuer, amount, tx_successful,
			       tx_fee_charged, is_payment_op, is_soroban_op, 1 as source
			FROM %s.enriched_history_operations
			%s
			UNION ALL
			SELECT transaction_hash, operation_index AS operation_id, ledger_sequence,
			       ledger_closed_at, source_account, type, destination,
			       asset_code, asset_issuer, amount, tx_successful,
			       tx_fee_charged, is_payment_op, is_soroban_op, 2 as source
			FROM %s.enriched_history_operations
			%s
		)
		SELECT DISTINCT ON (ledger_sequence, operation_id)
		       transaction_hash, operation_id, ledger_sequence,
		       ledger_closed_at, source_account, type, destination,
		       asset_code, asset_issuer, amount, tx_successful,
		       tx_fee_charged, is_payment_op, is_soroban_op
		FROM combined
		WHERE 1=1 %s
		ORDER BY ledger_sequence DESC, operation_id DESC
		LIMIT $%d
	`, r.hotSchema, whereClause, r.coldSchema, whereClause, cursorClause, argNum)

	args = append(args, requestLimit)

	rows, err := r.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, "", false, fmt.Errorf("unified GetEnrichedOperations: %w", err)
	}
	defer rows.Close()

	var operations []EnrichedOperation
	for rows.Next() {
		var op EnrichedOperation
		if err := rows.Scan(&op.TransactionHash, &op.OperationID, &op.LedgerSequence,
			&op.LedgerClosedAt, &op.SourceAccount, &op.Type,
			&op.Destination, &op.AssetCode, &op.AssetIssuer, &op.Amount,
			&op.TxSuccessful, &op.TxFeeCharged, &op.IsPaymentOp, &op.IsSorobanOp); err != nil {
			return nil, "", false, err
		}
		op.TypeName = operationTypeName(op.Type)
		operations = append(operations, op)
	}

	// Determine has_more and trim to requested limit
	hasMore := len(operations) > filters.Limit
	if hasMore {
		operations = operations[:filters.Limit]
	}

	// Generate next cursor from last result
	var nextCursor string
	if len(operations) > 0 && hasMore {
		last := operations[len(operations)-1]
		cursor := OperationCursor{
			LedgerSequence: last.LedgerSequence,
			OperationIndex: last.OperationID,
		}
		nextCursor = cursor.Encode()
	}

	return operations, nextCursor, hasMore, nil
}

// ============================================
// TOKEN TRANSFERS QUERIES
// ============================================

// GetTokenTransfers returns token transfers without cursor (backward compatible)
func (r *UnifiedDuckDBReader) GetTokenTransfers(ctx context.Context, filters TransferFilters) ([]TokenTransfer, error) {
	transfers, _, _, err := r.GetTokenTransfersWithCursor(ctx, filters)
	return transfers, err
}

// GetTokenTransfersWithCursor returns token transfers with cursor pagination
// Returns: transfers, nextCursor, hasMore, error (matching UnifiedSilverReader interface)
func (r *UnifiedDuckDBReader) GetTokenTransfersWithCursor(ctx context.Context, filters TransferFilters) ([]TokenTransfer, string, bool, error) {
	// Request one extra to detect has_more
	requestLimit := filters.Limit + 1

	whereClause := "WHERE transaction_successful = true"
	args := []interface{}{}
	argNum := 1

	if filters.SourceType != "" {
		whereClause += fmt.Sprintf(" AND source_type = $%d", argNum)
		args = append(args, filters.SourceType)
		argNum++
	}

	if filters.AssetCode != "" {
		whereClause += fmt.Sprintf(" AND asset_code = $%d", argNum)
		args = append(args, filters.AssetCode)
		argNum++
	}

	if filters.FromAccount != "" {
		whereClause += fmt.Sprintf(" AND from_account = $%d", argNum)
		args = append(args, filters.FromAccount)
		argNum++
	}

	if filters.ToAccount != "" {
		whereClause += fmt.Sprintf(" AND to_account = $%d", argNum)
		args = append(args, filters.ToAccount)
		argNum++
	}

	if !filters.StartTime.IsZero() {
		whereClause += fmt.Sprintf(" AND timestamp >= $%d", argNum)
		args = append(args, filters.StartTime)
		argNum++
	}

	if !filters.EndTime.IsZero() {
		whereClause += fmt.Sprintf(" AND timestamp <= $%d", argNum)
		args = append(args, filters.EndTime)
		argNum++
	}

	// Cursor pagination
	cursorClause := ""
	if filters.Cursor != nil {
		cursorClause = fmt.Sprintf(" AND (ledger_sequence < $%d OR (ledger_sequence = $%d AND timestamp < $%d))",
			argNum, argNum, argNum+1)
		args = append(args, filters.Cursor.LedgerSequence, filters.Cursor.Timestamp)
		argNum += 2
	}

	query := fmt.Sprintf(`
		WITH combined AS (
			SELECT timestamp, transaction_hash, ledger_sequence, source_type,
			       from_account, to_account, asset_code, asset_issuer,
			       amount, token_contract_id, transaction_successful, 1 as source
			FROM %s.token_transfers_raw
			%s
			UNION ALL
			SELECT timestamp, transaction_hash, ledger_sequence, source_type,
			       from_account, to_account, asset_code, asset_issuer,
			       amount, token_contract_id, transaction_successful, 2 as source
			FROM %s.token_transfers_raw
			%s
		)
		SELECT DISTINCT ON (ledger_sequence, timestamp, transaction_hash)
		       timestamp, transaction_hash, ledger_sequence, source_type,
		       from_account, to_account, asset_code, asset_issuer,
		       amount, token_contract_id, transaction_successful
		FROM combined
		WHERE 1=1 %s
		ORDER BY ledger_sequence DESC, timestamp DESC
		LIMIT $%d
	`, r.hotSchema, whereClause, r.coldSchema, whereClause, cursorClause, argNum)

	args = append(args, requestLimit)

	rows, err := r.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, "", false, fmt.Errorf("unified GetTokenTransfers: %w", err)
	}
	defer rows.Close()

	var transfers []TokenTransfer
	for rows.Next() {
		var t TokenTransfer
		if err := rows.Scan(&t.Timestamp, &t.TransactionHash, &t.LedgerSequence,
			&t.SourceType, &t.FromAccount, &t.ToAccount, &t.AssetCode, &t.AssetIssuer,
			&t.Amount, &t.TokenContractID, &t.TransactionSuccessful); err != nil {
			return nil, "", false, err
		}
		transfers = append(transfers, t)
	}

	// Determine has_more and trim to requested limit
	hasMore := len(transfers) > filters.Limit
	if hasMore {
		transfers = transfers[:filters.Limit]
	}

	// Generate next cursor from last result
	var nextCursor string
	if len(transfers) > 0 && hasMore {
		last := transfers[len(transfers)-1]
		// Parse timestamp string to time.Time for cursor
		ts, err := time.Parse(time.RFC3339Nano, last.Timestamp)
		if err != nil {
			ts, err = time.Parse(time.RFC3339, last.Timestamp)
			if err != nil {
				log.Printf("Warning: failed to parse timestamp '%s' for cursor: %v", last.Timestamp, err)
			}
		}
		cursor := TransferCursor{
			LedgerSequence: last.LedgerSequence,
			Timestamp:      ts,
		}
		nextCursor = cursor.Encode()
	}

	return transfers, nextCursor, hasMore, nil
}

// GetTokenTransferStats returns aggregated transfer statistics (cold-only)
// Aggregations are expensive on hot buffer, so we only query cold
func (r *UnifiedDuckDBReader) GetTokenTransferStats(ctx context.Context, groupBy string, startTime, endTime time.Time) ([]TransferStats, error) {
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

	// Cold-only query - aggregations are too expensive on hot buffer
	query := fmt.Sprintf(`
		SELECT
			%s,
			COUNT(*) as transfer_count,
			COUNT(DISTINCT from_account) as unique_senders,
			COUNT(DISTINCT to_account) as unique_receivers,
			SUM(CAST(amount AS DOUBLE)) as total_volume
		FROM %s.token_transfers_raw
		WHERE transaction_successful = true
			AND timestamp >= $1
			AND timestamp <= $2
			AND amount IS NOT NULL
		GROUP BY %s
		ORDER BY total_volume DESC
		LIMIT 100
	`, groupByClause, r.coldSchema, groupByClause)

	rows, err := r.db.QueryContext(ctx, query, startTime, endTime)
	if err != nil {
		return nil, fmt.Errorf("unified GetTokenTransferStats: %w", err)
	}
	defer rows.Close()

	var stats []TransferStats
	for rows.Next() {
		var s TransferStats
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
// NETWORK STATS (cold-only for aggregations)
// ============================================

// GetTotalAccountCount returns total accounts from cold storage
func (r *UnifiedDuckDBReader) GetTotalAccountCount(ctx context.Context) (int64, error) {
	query := fmt.Sprintf(`SELECT COUNT(*) FROM %s.accounts_current`, r.coldSchema)
	var count int64
	err := r.db.QueryRowContext(ctx, query).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("unified GetTotalAccountCount: %w", err)
	}
	return count, nil
}

// GetOperationStats24h returns 24h operation counts grouped by type
func (r *UnifiedDuckDBReader) GetOperationStats24h(ctx context.Context) (map[int32]int64, error) {
	query := fmt.Sprintf(`
		SELECT type, COUNT(*) as count
		FROM %s.enriched_history_operations
		WHERE ledger_closed_at > NOW() - INTERVAL '24 hours'
		GROUP BY type
	`, r.coldSchema)

	rows, err := r.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("unified GetOperationStats24h: %w", err)
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

// GetActiveAccounts24h returns count of accounts with activity in last 24h
func (r *UnifiedDuckDBReader) GetActiveAccounts24h(ctx context.Context) (int64, error) {
	query := fmt.Sprintf(`
		SELECT COUNT(DISTINCT source_account)
		FROM %s.enriched_history_operations
		WHERE ledger_closed_at > NOW() - INTERVAL '24 hours'
	`, r.coldSchema)

	var count int64
	err := r.db.QueryRowContext(ctx, query).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("unified GetActiveAccounts24h: %w", err)
	}
	return count, nil
}
