package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"strconv"
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
// Queries both hot and cold, returns the one with highest last_modified_ledger
func (r *UnifiedDuckDBReader) GetAccountCurrent(ctx context.Context, accountID string) (*AccountCurrent, error) {
	// Return the row with highest last_modified_ledger (most recent state)
	query := fmt.Sprintf(`
		SELECT account_id, balance, sequence_number, num_subentries,
		       last_modified_ledger, updated_at
		FROM (
			SELECT account_id, balance, sequence_number, num_subentries,
			       last_modified_ledger, updated_at
			FROM %s.accounts_current WHERE account_id = $1
			UNION ALL
			SELECT account_id, balance, sequence_number, num_subentries,
			       last_modified_ledger, updated_at
			FROM %s.accounts_current WHERE account_id = $1
		) combined
		ORDER BY last_modified_ledger DESC
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

// GetAccountBalances returns all balances (XLM + trustlines) for an account
// Queries accounts_current for XLM and trustlines_current for all trustlines
func (r *UnifiedDuckDBReader) GetAccountBalances(ctx context.Context, accountID string) (*AccountBalancesResponse, error) {
	// First get XLM balance from accounts_current
	// Note: hot uses BIGINT for balance, cold uses VARCHAR (pre-formatted)
	// We cast to VARCHAR to handle both cases uniformly, then parse
	xlmQuery := fmt.Sprintf(`
		WITH combined AS (
			SELECT account_id, CAST(balance AS VARCHAR) as balance_str, last_modified_ledger, 1 as source
			FROM %s.accounts_current WHERE account_id = $1
			UNION ALL
			SELECT account_id, CAST(balance AS VARCHAR) as balance_str, last_modified_ledger, 2 as source
			FROM %s.accounts_current WHERE account_id = $1
		)
		SELECT account_id, balance_str
		FROM combined
		ORDER BY last_modified_ledger DESC
		LIMIT 1
	`, r.hotSchema, r.coldSchema)

	var xlmBalanceStr string
	err := r.db.QueryRowContext(ctx, xlmQuery, accountID).Scan(&accountID, &xlmBalanceStr)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("account not found: %s", accountID)
		}
		return nil, fmt.Errorf("failed to get XLM balance: %w", err)
	}

	// Start building balances response
	var balances []Balance

	// Parse the XLM balance - may be stroops (integer) or formatted (decimal string)
	var xlmStroops int64
	var xlmBalanceFormatted string
	if strings.Contains(xlmBalanceStr, ".") {
		// Already formatted as decimal, parse and convert back to stroops
		parts := strings.Split(xlmBalanceStr, ".")
		whole, _ := strconv.ParseInt(parts[0], 10, 64)
		frac := int64(0)
		if len(parts) > 1 {
			// Pad or trim to 7 digits
			fracStr := parts[1]
			for len(fracStr) < 7 {
				fracStr += "0"
			}
			if len(fracStr) > 7 {
				fracStr = fracStr[:7]
			}
			frac, _ = strconv.ParseInt(fracStr, 10, 64)
		}
		xlmStroops = whole*10000000 + frac
		xlmBalanceFormatted = xlmBalanceStr
	} else {
		// Raw stroops integer
		xlmStroops, _ = strconv.ParseInt(xlmBalanceStr, 10, 64)
		xlmBalanceFormatted = formatStroopsToDecimal(xlmStroops)
	}

	balances = append(balances, Balance{
		AssetType:      "native",
		AssetCode:      "XLM",
		Balance:        xlmBalanceFormatted,
		BalanceStroops: xlmStroops,
	})

	// Get trustline balances
	trustlineQuery := fmt.Sprintf(`
		WITH combined AS (
			SELECT account_id, asset_type, asset_code, asset_issuer,
			       balance, trust_line_limit, flags, last_modified_ledger, 1 as source
			FROM %s.trustlines_current WHERE account_id = $1
			UNION ALL
			SELECT account_id, asset_type, asset_code, asset_issuer,
			       balance, trust_line_limit, flags, last_modified_ledger, 2 as source
			FROM %s.trustlines_current WHERE account_id = $1
		),
		deduplicated AS (
			SELECT DISTINCT ON (account_id, asset_code, asset_issuer)
			       asset_type, asset_code, asset_issuer, balance, trust_line_limit, flags
			FROM combined
			ORDER BY account_id, asset_code, asset_issuer, last_modified_ledger DESC
		)
		SELECT asset_type, asset_code, asset_issuer, balance, trust_line_limit, flags
		FROM deduplicated
	`, r.hotSchema, r.coldSchema)

	rows, err := r.db.QueryContext(ctx, trustlineQuery, accountID)
	if err != nil {
		return nil, fmt.Errorf("failed to get trustlines: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var assetType, assetCode string
		var assetIssuer sql.NullString
		var balanceStroops, limit sql.NullInt64
		var flags sql.NullInt64

		if err := rows.Scan(&assetType, &assetCode, &assetIssuer, &balanceStroops, &limit, &flags); err != nil {
			return nil, fmt.Errorf("failed to scan trustline: %w", err)
		}

		var issuer *string
		if assetIssuer.Valid {
			issuer = &assetIssuer.String
		}

		var limitStr *string
		if limit.Valid {
			ls := formatStroopsToDecimal(limit.Int64)
			limitStr = &ls
		}

		var isAuthorized *bool
		if flags.Valid {
			// Flag 1 = authorized (see Stellar SDK)
			authorized := (flags.Int64 & 1) != 0
			isAuthorized = &authorized
		}

		bal := balanceStroops.Int64
		balances = append(balances, Balance{
			AssetType:      assetType,
			AssetCode:      assetCode,
			AssetIssuer:    issuer,
			Balance:        formatStroopsToDecimal(bal),
			BalanceStroops: bal,
			Limit:          limitStr,
			IsAuthorized:   isAuthorized,
		})
	}

	return &AccountBalancesResponse{
		AccountID:     accountID,
		Balances:      balances,
		TotalBalances: len(balances),
	}, nil
}

// formatStroopsToDecimal converts stroops to decimal string with 7 decimal places
func formatStroopsToDecimal(stroops int64) string {
	whole := stroops / 10000000
	frac := stroops % 10000000
	if frac < 0 {
		frac = -frac
	}
	return fmt.Sprintf("%d.%07d", whole, frac)
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
// NOTE: This query derives transfers from enriched_history_operations (where is_payment_op=true)
// to ensure asset_code and asset_issuer are always populated correctly.
func (r *UnifiedDuckDBReader) GetTokenTransfersWithCursor(ctx context.Context, filters TransferFilters) ([]TokenTransfer, string, bool, error) {
	// Request one extra to detect has_more
	requestLimit := filters.Limit + 1

	// Build WHERE clause for enriched_history_operations
	whereClause := "WHERE transaction_successful = true AND is_payment_op = true"
	args := []interface{}{}
	argNum := 1

	if filters.SourceType != "" {
		if filters.SourceType == "classic" {
			whereClause += " AND (is_soroban_op = false OR is_soroban_op IS NULL)"
		} else if filters.SourceType == "soroban" {
			whereClause += " AND is_soroban_op = true"
		}
	}

	if filters.AssetCode != "" {
		// Handle XLM special case
		if filters.AssetCode == "XLM" {
			whereClause += " AND (asset_type = 'native' OR asset_code = 'XLM')"
		} else {
			whereClause += fmt.Sprintf(" AND asset_code = $%d", argNum)
			args = append(args, filters.AssetCode)
			argNum++
		}
	}

	if filters.FromAccount != "" {
		whereClause += fmt.Sprintf(" AND source_account = $%d", argNum)
		args = append(args, filters.FromAccount)
		argNum++
	}

	if filters.ToAccount != "" {
		whereClause += fmt.Sprintf(" AND destination = $%d", argNum)
		args = append(args, filters.ToAccount)
		argNum++
	}

	if !filters.StartTime.IsZero() {
		whereClause += fmt.Sprintf(" AND created_at >= $%d", argNum)
		args = append(args, filters.StartTime)
		argNum++
	}

	if !filters.EndTime.IsZero() {
		whereClause += fmt.Sprintf(" AND created_at <= $%d", argNum)
		args = append(args, filters.EndTime)
		argNum++
	}

	// Cursor pagination
	cursorClause := ""
	if filters.Cursor != nil {
		cursorClause = fmt.Sprintf(" AND (ledger_sequence < $%d OR (ledger_sequence = $%d AND created_at < $%d))",
			argNum, argNum, argNum+1)
		args = append(args, filters.Cursor.LedgerSequence, filters.Cursor.Timestamp)
		argNum += 2
	}

	// Query from enriched_history_operations which has correct asset info
	// Map columns to match TokenTransfer struct
	// NOTE: We handle NULL asset_type by defaulting to 'XLM' for classic payments
	// and parsing the combined 'asset' field if available
	query := fmt.Sprintf(`
		WITH combined AS (
			SELECT
				created_at as timestamp,
				transaction_hash,
				ledger_sequence,
				CASE WHEN is_soroban_op = true THEN 'soroban' ELSE 'classic' END as source_type,
				source_account as from_account,
				destination as to_account,
				CASE
					WHEN asset_code IS NOT NULL THEN asset_code
					WHEN asset_type = 'native' OR asset = 'native' THEN 'XLM'
					WHEN asset IS NOT NULL AND starts_with(asset, 'credit_alphanum') THEN
						SPLIT_PART(asset, ':', 2)
					WHEN is_soroban_op = false OR is_soroban_op IS NULL THEN 'XLM'
					ELSE NULL
				END as asset_code,
				CASE
					WHEN asset_issuer IS NOT NULL THEN asset_issuer
					WHEN asset_type = 'native' OR asset = 'native' THEN NULL
					WHEN asset IS NOT NULL AND starts_with(asset, 'credit_alphanum') THEN
						SPLIT_PART(asset, ':', 3)
					ELSE NULL
				END as asset_issuer,
				amount,
				contract_id as token_contract_id,
				transaction_successful,
				1 as source
			FROM %s.enriched_history_operations
			%s
			UNION ALL
			SELECT
				created_at as timestamp,
				transaction_hash,
				ledger_sequence,
				CASE WHEN is_soroban_op = true THEN 'soroban' ELSE 'classic' END as source_type,
				source_account as from_account,
				destination as to_account,
				CASE
					WHEN asset_code IS NOT NULL THEN asset_code
					WHEN asset_type = 'native' OR asset = 'native' THEN 'XLM'
					WHEN asset IS NOT NULL AND starts_with(asset, 'credit_alphanum') THEN
						SPLIT_PART(asset, ':', 2)
					WHEN is_soroban_op = false OR is_soroban_op IS NULL THEN 'XLM'
					ELSE NULL
				END as asset_code,
				CASE
					WHEN asset_issuer IS NOT NULL THEN asset_issuer
					WHEN asset_type = 'native' OR asset = 'native' THEN NULL
					WHEN asset IS NOT NULL AND starts_with(asset, 'credit_alphanum') THEN
						SPLIT_PART(asset, ':', 3)
					ELSE NULL
				END as asset_issuer,
				amount,
				contract_id as token_contract_id,
				transaction_successful,
				2 as source
			FROM %s.enriched_history_operations
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

// GetTokenHolders returns holders of a specific token, ranked by balance
// For XLM (native asset), queries accounts_current
// For other assets, queries trustlines_current
func (r *UnifiedDuckDBReader) GetTokenHolders(ctx context.Context, filters TokenHoldersFilters) (*TokenHoldersResponse, error) {
	// Request one extra to detect has_more
	requestLimit := filters.Limit + 1

	isNative := filters.AssetCode == "XLM" || filters.AssetCode == "native"

	var query string
	var args []interface{}
	argNum := 1

	if isNative {
		// XLM holders - query accounts_current
		// Cast balance to VARCHAR to handle both BIGINT (hot) and VARCHAR (cold) formats
		query = fmt.Sprintf(`
			WITH combined AS (
				SELECT account_id, CAST(balance AS VARCHAR) as balance_str, last_modified_ledger, 1 as source
				FROM %s.accounts_current
				UNION ALL
				SELECT account_id, CAST(balance AS VARCHAR) as balance_str, last_modified_ledger, 2 as source
				FROM %s.accounts_current
			),
			deduplicated AS (
				SELECT DISTINCT ON (account_id)
				       account_id, balance_str, last_modified_ledger
				FROM combined
				ORDER BY account_id, last_modified_ledger DESC, source ASC
			)
			SELECT account_id, balance_str
			FROM deduplicated
			WHERE 1=1
		`, r.hotSchema, r.coldSchema)
	} else {
		// Non-XLM holders - query trustlines_current
		query = fmt.Sprintf(`
			WITH combined AS (
				SELECT account_id, balance, last_modified_ledger, 1 as source
				FROM %s.trustlines_current
				WHERE asset_code = $1 AND asset_issuer = $2
				UNION ALL
				SELECT account_id, balance, last_modified_ledger, 2 as source
				FROM %s.trustlines_current
				WHERE asset_code = $1 AND asset_issuer = $2
			),
			deduplicated AS (
				SELECT DISTINCT ON (account_id)
				       account_id, balance, last_modified_ledger
				FROM combined
				ORDER BY account_id, last_modified_ledger DESC, source ASC
			)
			SELECT account_id, CAST(balance AS VARCHAR) as balance_str
			FROM deduplicated
			WHERE 1=1
		`, r.hotSchema, r.coldSchema)
		args = append(args, filters.AssetCode, filters.AssetIssuer)
		argNum = 3
	}

	// Apply min_balance filter
	if filters.MinBalance != nil {
		if isNative {
			// For XLM, balance is in stroops or decimal format
			// Convert min_balance stroops to decimal for comparison
			minBalDecimal := float64(*filters.MinBalance) / 10000000.0
			query += fmt.Sprintf(` AND (
				CASE
					WHEN balance_str LIKE '%%.%%' THEN CAST(balance_str AS DOUBLE)
					ELSE CAST(balance_str AS BIGINT) / 10000000.0
				END
			) >= $%d`, argNum)
			args = append(args, minBalDecimal)
			argNum++
		} else {
			// For trustlines, balance is typically stored as integer
			query += fmt.Sprintf(` AND CAST(balance AS BIGINT) >= $%d`, argNum)
			args = append(args, *filters.MinBalance)
			argNum++
		}
	}

	// Apply cursor pagination (paginating by balance DESC, account_id for tie-breaking)
	if filters.Cursor != nil {
		cursorBalDecimal := float64(filters.Cursor.Balance) / 10000000.0
		if isNative {
			query += fmt.Sprintf(` AND (
				(CASE
					WHEN balance_str LIKE '%%.%%' THEN CAST(balance_str AS DOUBLE)
					ELSE CAST(balance_str AS BIGINT) / 10000000.0
				END) < $%d
				OR (
					(CASE
						WHEN balance_str LIKE '%%.%%' THEN CAST(balance_str AS DOUBLE)
						ELSE CAST(balance_str AS BIGINT) / 10000000.0
					END) = $%d
					AND account_id > $%d
				)
			)`, argNum, argNum, argNum+1)
			args = append(args, cursorBalDecimal, filters.Cursor.AccountID)
			argNum += 2
		} else {
			query += fmt.Sprintf(` AND (CAST(balance AS BIGINT) < $%d OR (CAST(balance AS BIGINT) = $%d AND account_id > $%d))`,
				argNum, argNum, argNum+1)
			args = append(args, filters.Cursor.Balance, filters.Cursor.AccountID)
			argNum += 2
		}
	}

	// Order by balance DESC, account_id for stable ordering
	if isNative {
		query += fmt.Sprintf(` ORDER BY (
			CASE
				WHEN balance_str LIKE '%%.%%' THEN CAST(balance_str AS DOUBLE)
				ELSE CAST(balance_str AS BIGINT) / 10000000.0
			END
		) DESC, account_id ASC LIMIT $%d`, argNum)
	} else {
		query += fmt.Sprintf(` ORDER BY CAST(balance AS BIGINT) DESC, account_id ASC LIMIT $%d`, argNum)
	}
	args = append(args, requestLimit)

	rows, err := r.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("unified GetTokenHolders: %w", err)
	}
	defer rows.Close()

	var holders []TokenHolder
	rank := 1
	if filters.Cursor != nil {
		// If we have a cursor, we don't know the exact starting rank
		// This is a simplification - in production you might track offset
		rank = 0 // Will be 0-indexed after cursor
	}

	for rows.Next() {
		var accountID, balanceStr string
		if err := rows.Scan(&accountID, &balanceStr); err != nil {
			return nil, fmt.Errorf("failed to scan holder: %w", err)
		}

		// Parse balance to stroops
		var balanceStroops int64
		var balanceFormatted string
		if strings.Contains(balanceStr, ".") {
			// Decimal format - parse and convert to stroops
			parts := strings.Split(balanceStr, ".")
			whole, _ := strconv.ParseInt(parts[0], 10, 64)
			frac := int64(0)
			if len(parts) > 1 {
				fracStr := parts[1]
				for len(fracStr) < 7 {
					fracStr += "0"
				}
				if len(fracStr) > 7 {
					fracStr = fracStr[:7]
				}
				frac, _ = strconv.ParseInt(fracStr, 10, 64)
			}
			balanceStroops = whole*10000000 + frac
			balanceFormatted = balanceStr
		} else {
			// Integer stroops format
			balanceStroops, _ = strconv.ParseInt(balanceStr, 10, 64)
			balanceFormatted = formatStroopsToDecimal(balanceStroops)
		}

		if filters.Cursor == nil {
			holders = append(holders, TokenHolder{
				AccountID:      accountID,
				Balance:        balanceFormatted,
				BalanceStroops: balanceStroops,
				Rank:           rank,
			})
			rank++
		} else {
			// With cursor, don't include rank (it would be incorrect)
			holders = append(holders, TokenHolder{
				AccountID:      accountID,
				Balance:        balanceFormatted,
				BalanceStroops: balanceStroops,
				Rank:           0, // Unknown when using cursor pagination
			})
		}
	}

	// Determine has_more and trim to requested limit
	hasMore := len(holders) > filters.Limit
	if hasMore {
		holders = holders[:filters.Limit]
	}

	// Generate next cursor from last result
	var nextCursor string
	if len(holders) > 0 && hasMore {
		last := holders[len(holders)-1]
		cursor := TokenHoldersCursor{
			Balance:   last.BalanceStroops,
			AccountID: last.AccountID,
		}
		nextCursor = cursor.Encode()
	}

	// Build asset info
	asset := AssetInfo{
		Code: filters.AssetCode,
	}
	if isNative {
		asset.Type = "native"
	} else {
		asset.Issuer = &filters.AssetIssuer
		if len(filters.AssetCode) <= 4 {
			asset.Type = "credit_alphanum4"
		} else {
			asset.Type = "credit_alphanum12"
		}
	}

	return &TokenHoldersResponse{
		Asset:        asset,
		Holders:      holders,
		TotalHolders: len(holders),
		Cursor:       nextCursor,
		HasMore:      hasMore,
	}, nil
}

// GetTokenStats returns aggregated statistics for a token
// For XLM (native asset), queries accounts_current
// For other assets, queries trustlines_current
func (r *UnifiedDuckDBReader) GetTokenStats(ctx context.Context, assetCode, assetIssuer string) (*TokenStatsResponse, error) {
	isNative := assetCode == "XLM" || assetCode == "native"

	var stats TokenStats
	var totalSupply float64

	if isNative {
		// XLM stats - query accounts_current
		// Get holder count and total supply
		query := fmt.Sprintf(`
			WITH combined AS (
				SELECT account_id, CAST(balance AS VARCHAR) as balance_str, last_modified_ledger, 1 as source
				FROM %s.accounts_current
				UNION ALL
				SELECT account_id, CAST(balance AS VARCHAR) as balance_str, last_modified_ledger, 2 as source
				FROM %s.accounts_current
			),
			deduplicated AS (
				SELECT DISTINCT ON (account_id)
				       account_id, balance_str
				FROM combined
				ORDER BY account_id, last_modified_ledger DESC, source ASC
			),
			parsed AS (
				SELECT
					account_id,
					CASE
						WHEN balance_str LIKE '%%.%%' THEN CAST(balance_str AS DOUBLE)
						ELSE CAST(balance_str AS BIGINT) / 10000000.0
					END as balance_xlm
				FROM deduplicated
			)
			SELECT
				COUNT(*) as total_accounts,
				COUNT(*) FILTER (WHERE balance_xlm > 0) as total_holders,
				COALESCE(SUM(balance_xlm), 0) as total_supply
			FROM parsed
		`, r.hotSchema, r.coldSchema)

		var totalAccounts int64
		err := r.db.QueryRowContext(ctx, query).Scan(&totalAccounts, &stats.TotalHolders, &totalSupply)
		if err != nil {
			return nil, fmt.Errorf("failed to get XLM stats: %w", err)
		}
		stats.TotalTrustlines = totalAccounts // For XLM, trustlines = accounts

		// Get top 10 concentration for XLM
		top10Query := fmt.Sprintf(`
			WITH combined AS (
				SELECT account_id, CAST(balance AS VARCHAR) as balance_str, last_modified_ledger, 1 as source
				FROM %s.accounts_current
				UNION ALL
				SELECT account_id, CAST(balance AS VARCHAR) as balance_str, last_modified_ledger, 2 as source
				FROM %s.accounts_current
			),
			deduplicated AS (
				SELECT DISTINCT ON (account_id)
				       account_id, balance_str
				FROM combined
				ORDER BY account_id, last_modified_ledger DESC, source ASC
			),
			parsed AS (
				SELECT
					CASE
						WHEN balance_str LIKE '%%.%%' THEN CAST(balance_str AS DOUBLE)
						ELSE CAST(balance_str AS BIGINT) / 10000000.0
					END as balance_xlm
				FROM deduplicated
				ORDER BY balance_xlm DESC
				LIMIT 10
			)
			SELECT COALESCE(SUM(balance_xlm), 0) as top_10_total
			FROM parsed
		`, r.hotSchema, r.coldSchema)

		var top10Total float64
		if err := r.db.QueryRowContext(ctx, top10Query).Scan(&top10Total); err != nil {
			log.Printf("Warning: failed to get top 10 concentration: %v", err)
		} else if totalSupply > 0 {
			stats.Top10Concentration = top10Total / totalSupply
		}

	} else {
		// Non-XLM stats - query trustlines_current
		query := fmt.Sprintf(`
			WITH combined AS (
				SELECT account_id, balance, last_modified_ledger, 1 as source
				FROM %s.trustlines_current
				WHERE asset_code = $1 AND asset_issuer = $2
				UNION ALL
				SELECT account_id, balance, last_modified_ledger, 2 as source
				FROM %s.trustlines_current
				WHERE asset_code = $1 AND asset_issuer = $2
			),
			deduplicated AS (
				SELECT DISTINCT ON (account_id)
				       account_id, balance
				FROM combined
				ORDER BY account_id, last_modified_ledger DESC, source ASC
			)
			SELECT
				COUNT(*) as total_trustlines,
				COUNT(*) FILTER (WHERE CAST(balance AS BIGINT) > 0) as total_holders,
				COALESCE(SUM(CAST(balance AS BIGINT)), 0) as total_supply_stroops
			FROM deduplicated
		`, r.hotSchema, r.coldSchema)

		var totalSupplyStroops int64
		err := r.db.QueryRowContext(ctx, query, assetCode, assetIssuer).Scan(
			&stats.TotalTrustlines, &stats.TotalHolders, &totalSupplyStroops)
		if err != nil {
			return nil, fmt.Errorf("failed to get token stats: %w", err)
		}
		totalSupply = float64(totalSupplyStroops) / 10000000.0

		// Get top 10 concentration
		top10Query := fmt.Sprintf(`
			WITH combined AS (
				SELECT account_id, balance, last_modified_ledger, 1 as source
				FROM %s.trustlines_current
				WHERE asset_code = $1 AND asset_issuer = $2
				UNION ALL
				SELECT account_id, balance, last_modified_ledger, 2 as source
				FROM %s.trustlines_current
				WHERE asset_code = $1 AND asset_issuer = $2
			),
			deduplicated AS (
				SELECT DISTINCT ON (account_id)
				       account_id, balance
				FROM combined
				ORDER BY account_id, last_modified_ledger DESC, source ASC
			),
			top10 AS (
				SELECT CAST(balance AS BIGINT) as balance_stroops
				FROM deduplicated
				ORDER BY balance_stroops DESC
				LIMIT 10
			)
			SELECT COALESCE(SUM(balance_stroops), 0) as top_10_total
			FROM top10
		`, r.hotSchema, r.coldSchema)

		var top10TotalStroops int64
		if err := r.db.QueryRowContext(ctx, top10Query, assetCode, assetIssuer).Scan(&top10TotalStroops); err != nil {
			log.Printf("Warning: failed to get top 10 concentration: %v", err)
		} else if totalSupply > 0 {
			stats.Top10Concentration = float64(top10TotalStroops) / 10000000.0 / totalSupply
		}
	}

	stats.CirculatingSupply = fmt.Sprintf("%.7f", totalSupply)

	// Get 24h transfer stats from enriched_history_operations
	// This works for both XLM and other assets
	// Note: amount is stored in stroops, so we divide by 10^7 to get human-readable values
	transferQuery := fmt.Sprintf(`
		WITH combined AS (
			SELECT source_account, destination, amount, created_at, 1 as source
			FROM %s.enriched_history_operations
			WHERE is_payment_op = true
				AND transaction_successful = true
				AND created_at > NOW() - INTERVAL '24 hours'
				AND (
					($1 = 'XLM' AND (asset_type = 'native' OR asset_code = 'XLM' OR (asset_code IS NULL AND is_soroban_op = false)))
					OR ($1 != 'XLM' AND asset_code = $1 AND ($2 = '' OR asset_issuer = $2))
				)
			UNION ALL
			SELECT source_account, destination, amount, created_at, 2 as source
			FROM %s.enriched_history_operations
			WHERE is_payment_op = true
				AND transaction_successful = true
				AND created_at > NOW() - INTERVAL '24 hours'
				AND (
					($1 = 'XLM' AND (asset_type = 'native' OR asset_code = 'XLM' OR (asset_code IS NULL AND is_soroban_op = false)))
					OR ($1 != 'XLM' AND asset_code = $1 AND ($2 = '' OR asset_issuer = $2))
				)
		)
		SELECT
			COUNT(*) as transfer_count,
			COALESCE(SUM(CAST(amount AS DOUBLE)) / 10000000.0, 0) as total_volume,
			COUNT(DISTINCT source_account) + COUNT(DISTINCT destination) as unique_accounts
		FROM combined
	`, r.hotSchema, r.coldSchema)

	var volume24h float64
	err := r.db.QueryRowContext(ctx, transferQuery, assetCode, assetIssuer).Scan(
		&stats.Transfers24h, &volume24h, &stats.UniqueAccounts24h)
	if err != nil {
		// Don't fail if transfer stats query fails - it's less critical
		log.Printf("Warning: failed to get 24h transfer stats: %v", err)
		stats.Transfers24h = 0
		stats.Volume24h = "0.0000000"
		stats.UniqueAccounts24h = 0
	} else {
		stats.Volume24h = fmt.Sprintf("%.7f", volume24h)
	}

	// Build asset info
	asset := AssetInfo{
		Code: assetCode,
	}
	if isNative {
		asset.Type = "native"
	} else {
		asset.Issuer = &assetIssuer
		if len(assetCode) <= 4 {
			asset.Type = "credit_alphanum4"
		} else {
			asset.Type = "credit_alphanum12"
		}
	}

	return &TokenStatsResponse{
		Asset:       asset,
		Stats:       stats,
		GeneratedAt: time.Now().UTC().Format(time.RFC3339),
	}, nil
}

// GetAccountSigners returns the current signers for an account (Horizon-compatible format)
func (r *UnifiedDuckDBReader) GetAccountSigners(ctx context.Context, accountID string) (*AccountSignersResponse, error) {
	// Query from hot first (PostgreSQL), then cold (DuckLake) if not found
	query := fmt.Sprintf(`
		SELECT
			account_id,
			COALESCE(signers, '[]') as signers,
			COALESCE(master_weight, 1) as master_weight,
			COALESCE(low_threshold, 0) as low_threshold,
			COALESCE(med_threshold, 0) as med_threshold,
			COALESCE(high_threshold, 0) as high_threshold
		FROM %s.accounts_current
		WHERE account_id = $1
		UNION ALL
		SELECT
			account_id,
			COALESCE(signers, '[]') as signers,
			COALESCE(master_weight, 1) as master_weight,
			COALESCE(low_threshold, 0) as low_threshold,
			COALESCE(med_threshold, 0) as med_threshold,
			COALESCE(high_threshold, 0) as high_threshold
		FROM %s.accounts_current
		WHERE account_id = $1
			AND NOT EXISTS (
				SELECT 1 FROM %s.accounts_current WHERE account_id = $1
			)
		LIMIT 1
	`, r.hotSchema, r.coldSchema, r.hotSchema)

	var accID string
	var signersJSON string
	var masterWeight, lowThreshold, medThreshold, highThreshold int

	err := r.db.QueryRowContext(ctx, query, accountID).Scan(
		&accID, &signersJSON, &masterWeight, &lowThreshold, &medThreshold, &highThreshold,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("unified GetAccountSigners: %w", err)
	}

	// Parse signers JSON
	var signers []AccountSigner

	// The signers column may be stored as a JSON array of signer objects
	// Format: [{"key": "G...", "weight": 1, "type": "ed25519_public_key"}, ...]
	// Or it could be empty/null
	if signersJSON != "" && signersJSON != "[]" {
		if err := json.Unmarshal([]byte(signersJSON), &signers); err != nil {
			// If JSON parsing fails, log but continue with empty signers
			log.Printf("Warning: failed to parse signers JSON for %s: %v", accountID, err)
			signers = []AccountSigner{}
		}
	}

	// Always include the master key as a signer if it has weight > 0
	if masterWeight > 0 {
		// Check if master key is already in signers list
		hasMaster := false
		for _, s := range signers {
			if s.Key == accountID {
				hasMaster = true
				break
			}
		}
		if !hasMaster {
			// Prepend master key
			masterSigner := AccountSigner{
				Key:    accountID,
				Weight: masterWeight,
				Type:   "ed25519_public_key",
			}
			signers = append([]AccountSigner{masterSigner}, signers...)
		}
	}

	response := &AccountSignersResponse{
		AccountID: accID,
		Signers:   signers,
	}
	response.Thresholds.LowThreshold = lowThreshold
	response.Thresholds.MedThreshold = medThreshold
	response.Thresholds.HighThreshold = highThreshold

	return response, nil
}
