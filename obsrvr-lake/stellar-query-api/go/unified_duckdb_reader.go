package main

import (
	"context"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/stellar/go/strkey"
)

// ErrTxNotFound is returned when a transaction hash is not found in any data source.
var ErrTxNotFound = fmt.Errorf("transaction not found")

// UnifiedDuckDBReader queries both hot (PostgreSQL) and cold (DuckLake) data
// through a single DuckDB connection using ATTACH mechanism.
// This eliminates the need for Go-layer merging of results.
type UnifiedDuckDBReader struct {
	db               *sql.DB
	hotSchema        string // e.g., "hot_db" or "hot_db.public"
	coldSchema       string // e.g., "cold_db.silver"
	bronzeHotSchema  string // e.g., "bronze_hot_db.public" (bronze hot PostgreSQL)
	bronzeColdSchema string // e.g., "bronze_cold_db.bronze" (DuckLake bronze cold)
	config           UnifiedReaderConfig
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

	// ATTACH Bronze PostgreSQL (hot buffer) if configured
	bronzeHotSchema := ""
	if config.BronzePostgres != nil {
		bronzePgConnStr := fmt.Sprintf("dbname=%s host=%s port=%d user=%s password=%s",
			config.BronzePostgres.Database, config.BronzePostgres.Host, config.BronzePostgres.Port,
			config.BronzePostgres.User, config.BronzePostgres.Password)
		if config.BronzePostgres.SSLMode != "" {
			bronzePgConnStr += fmt.Sprintf(" sslmode=%s", config.BronzePostgres.SSLMode)
		}
		bronzePgAttach := fmt.Sprintf(`ATTACH '%s' AS bronze_hot_db (TYPE POSTGRES, READ_ONLY)`, bronzePgConnStr)
		if _, err := db.Exec(bronzePgAttach); err != nil {
			log.Printf("⚠️  Failed to attach Bronze PostgreSQL (connection error redacted to protect credentials)")
			log.Println("     Bronze hot endpoints (generic events, tx diffs) will be limited")
		} else {
			bronzeHotSchema = "bronze_hot_db"
			if config.BronzePostgres.Schema != "" {
				bronzeHotSchema = fmt.Sprintf("bronze_hot_db.%s", config.BronzePostgres.Schema)
			}
			log.Println("✅ Attached Bronze PostgreSQL as bronze_hot_db")
		}
	}

	// ATTACH Bronze DuckLake (cold storage) if configured
	bronzeColdSchema := ""
	if config.BronzeDuckLake != nil {
		bronzeDlAttach := fmt.Sprintf(`ATTACH '%s' AS bronze_cold_db (DATA_PATH '%s', METADATA_SCHEMA '%s')`,
			config.BronzeDuckLake.CatalogPath, config.BronzeDuckLake.DataPath, config.BronzeDuckLake.MetadataSchema)
		if _, err := db.Exec(bronzeDlAttach); err != nil {
			log.Printf("⚠️  Failed to attach Bronze DuckLake (connection error redacted to protect credentials)")
			log.Println("     Bronze cold data will not be available (only recent hot data)")
		} else {
			bronzeColdSchema = fmt.Sprintf("bronze_cold_db.%s", config.BronzeDuckLake.SchemaName)
			log.Println("✅ Attached Bronze DuckLake as bronze_cold_db")
		}
	}

	return &UnifiedDuckDBReader{
		db:               db,
		hotSchema:        hotSchema,
		coldSchema:       coldSchema,
		bronzeHotSchema:  bronzeHotSchema,
		bronzeColdSchema: bronzeColdSchema,
		config:           config,
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

// GetAvailableLedgers returns the range of ledgers available across hot and cold storage
// This is used for RPC v2-style data boundary indicators
func (r *UnifiedDuckDBReader) GetAvailableLedgers(ctx context.Context) (*LedgerRange, error) {
	// Query min/max ledger sequence from both hot and cold enriched_history_operations tables
	// Note: Using enriched_history_operations which exists in Silver layer
	query := fmt.Sprintf(`
		SELECT MIN(min_seq) as oldest, MAX(max_seq) as latest
		FROM (
			SELECT MIN(ledger_sequence) as min_seq, MAX(ledger_sequence) as max_seq FROM %s.enriched_history_operations
			UNION ALL
			SELECT MIN(ledger_sequence) as min_seq, MAX(ledger_sequence) as max_seq FROM %s.enriched_history_operations
		) combined
	`, r.hotSchema, r.coldSchema)

	var oldest, latest sql.NullInt64
	if err := r.db.QueryRowContext(ctx, query).Scan(&oldest, &latest); err != nil {
		return nil, fmt.Errorf("GetAvailableLedgers: %w", err)
	}

	// If no data found, return nil
	if !oldest.Valid || !latest.Valid {
		return nil, nil
	}

	return &LedgerRange{
		Oldest: oldest.Int64,
		Latest: latest.Int64,
	}, nil
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
		       last_modified_ledger, updated_at, home_domain
		FROM (
			SELECT account_id, balance, sequence_number, num_subentries,
			       last_modified_ledger, updated_at, home_domain
			FROM %s.accounts_current WHERE account_id = $1
			UNION ALL
			SELECT account_id, balance, sequence_number, num_subentries,
			       last_modified_ledger, updated_at, home_domain
			FROM %s.accounts_current WHERE account_id = $1
		) combined
		ORDER BY last_modified_ledger DESC
		LIMIT 1
	`, r.hotSchema, r.coldSchema)

	var acc AccountCurrent
	var homeDomain sql.NullString
	err := r.db.QueryRowContext(ctx, query, accountID).Scan(
		&acc.AccountID, &acc.Balance, &acc.SequenceNumber,
		&acc.NumSubentries, &acc.LastModifiedLedger, &acc.UpdatedAt,
		&homeDomain,
	)

	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("unified GetAccountCurrent: %w", err)
	}

	if homeDomain.Valid && homeDomain.String != "" {
		acc.HomeDomain = &homeDomain.String
	}

	return &acc, nil
}

// GetAccountCreatedAt returns the earliest closed_at for an account
func (r *UnifiedDuckDBReader) GetAccountCreatedAt(ctx context.Context, accountID string) (*string, error) {
	query := fmt.Sprintf(`
		SELECT MIN(closed_at)::text
		FROM (
			SELECT closed_at FROM %s.accounts_snapshot WHERE account_id = $1
			UNION ALL
			SELECT closed_at FROM %s.accounts_snapshot WHERE account_id = $1
		) combined
	`, r.hotSchema, r.coldSchema)

	var createdAt sql.NullString
	err := r.db.QueryRowContext(ctx, query, accountID).Scan(&createdAt)
	if err != nil {
		return nil, err
	}
	if !createdAt.Valid {
		return nil, sql.ErrNoRows
	}
	return &createdAt.String, nil
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

	if filters.SorobanFunction != "" {
		whereClause += fmt.Sprintf(" AND function_name = $%d", argNum)
		args = append(args, filters.SorobanFunction)
		argNum++
	}

	if filters.ContractID != "" {
		whereClause += fmt.Sprintf(" AND contract_id = $%d", argNum)
		args = append(args, filters.ContractID)
		argNum++
	}

	// Determine order direction (default: desc for backward compatibility)
	orderDir := "DESC"
	cursorOp := "<"
	if filters.Order == "asc" {
		orderDir = "ASC"
		cursorOp = ">"
	}

	// Cursor pagination - direction depends on order
	cursorClause := ""
	if filters.Cursor != nil {
		cursorClause = fmt.Sprintf(" AND (ledger_sequence %s $%d OR (ledger_sequence = $%d AND operation_id %s $%d))",
			cursorOp, argNum, argNum, cursorOp, argNum+1)
		args = append(args, filters.Cursor.LedgerSequence, filters.Cursor.OperationIndex)
		argNum += 2
	}

	query := fmt.Sprintf(`
		WITH combined AS (
			SELECT transaction_hash, operation_index AS operation_id, ledger_sequence,
			       ledger_closed_at, source_account, type, destination,
			       asset_code, asset_issuer, amount, tx_successful,
			       tx_fee_charged, is_payment_op, is_soroban_op,
			       contract_id, function_name, parameters,
			       1 as source
			FROM %s.enriched_history_operations
			%s
			UNION ALL
			SELECT transaction_hash, operation_index AS operation_id, ledger_sequence,
			       ledger_closed_at, source_account, type, destination,
			       asset_code, asset_issuer, amount, tx_successful,
			       tx_fee_charged, is_payment_op, is_soroban_op,
			       contract_id, function_name, parameters,
			       2 as source
			FROM %s.enriched_history_operations
			%s
		)
		SELECT DISTINCT ON (ledger_sequence, operation_id)
		       transaction_hash, operation_id, ledger_sequence,
		       ledger_closed_at, source_account, type, destination,
		       asset_code, asset_issuer, amount, tx_successful,
		       tx_fee_charged, is_payment_op, is_soroban_op,
		       contract_id, function_name, parameters
		FROM combined
		WHERE 1=1 %s
		ORDER BY ledger_sequence %s, operation_id %s
		LIMIT $%d
	`, r.hotSchema, whereClause, r.coldSchema, whereClause, cursorClause, orderDir, orderDir, argNum)

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
			&op.TxSuccessful, &op.TxFeeCharged, &op.IsPaymentOp, &op.IsSorobanOp,
			&op.SorobanContractID, &op.SorobanFunction, &op.SorobanArgsJSON); err != nil {
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
			Order:          filters.Order,
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

	// Determine order direction (default: desc for backward compatibility)
	orderDir := "DESC"
	cursorOp := "<"
	if filters.Order == "asc" {
		orderDir = "ASC"
		cursorOp = ">"
	}

	// Cursor pagination - direction depends on order
	cursorClause := ""
	if filters.Cursor != nil {
		cursorClause = fmt.Sprintf(" AND (ledger_sequence %s $%d OR (ledger_sequence = $%d AND timestamp %s $%d))",
			cursorOp, argNum, argNum, cursorOp, argNum+1)
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
		ORDER BY ledger_sequence %s, timestamp %s
		LIMIT $%d
	`, r.hotSchema, whereClause, r.coldSchema, whereClause, cursorClause, orderDir, orderDir, argNum)

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
			Order:          filters.Order,
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

// ============================================
// PHASE 6: STATE TABLE QUERIES (Offers, Liquidity Pools, Claimable Balances)
// ============================================

// GetOffers returns offers with optional filters, merging hot and cold data
func (r *UnifiedDuckDBReader) GetOffers(ctx context.Context, filters OfferFilters) ([]OfferCurrent, string, bool, error) {
	// Build WHERE clause conditions
	whereConditions := []string{}
	args := []interface{}{}
	argNum := 1

	if filters.SellerID != "" {
		whereConditions = append(whereConditions, fmt.Sprintf("seller_id = $%d", argNum))
		args = append(args, filters.SellerID)
		argNum++
	}

	if filters.SellingAssetCode != "" {
		if filters.SellingAssetCode == "XLM" {
			whereConditions = append(whereConditions, "selling_asset_type = 'native'")
		} else {
			whereConditions = append(whereConditions, fmt.Sprintf("selling_asset_code = $%d", argNum))
			args = append(args, filters.SellingAssetCode)
			argNum++
			if filters.SellingAssetIssuer != "" {
				whereConditions = append(whereConditions, fmt.Sprintf("selling_asset_issuer = $%d", argNum))
				args = append(args, filters.SellingAssetIssuer)
				argNum++
			}
		}
	}

	if filters.BuyingAssetCode != "" {
		if filters.BuyingAssetCode == "XLM" {
			whereConditions = append(whereConditions, "buying_asset_type = 'native'")
		} else {
			whereConditions = append(whereConditions, fmt.Sprintf("buying_asset_code = $%d", argNum))
			args = append(args, filters.BuyingAssetCode)
			argNum++
			if filters.BuyingAssetIssuer != "" {
				whereConditions = append(whereConditions, fmt.Sprintf("buying_asset_issuer = $%d", argNum))
				args = append(args, filters.BuyingAssetIssuer)
				argNum++
			}
		}
	}

	if filters.Cursor != nil {
		whereConditions = append(whereConditions, fmt.Sprintf("offer_id > $%d", argNum))
		args = append(args, filters.Cursor.OfferID)
		argNum++
	}

	whereClause := "1=1"
	if len(whereConditions) > 0 {
		whereClause = strings.Join(whereConditions, " AND ")
	}

	// Query limit+1 to detect hasMore
	queryLimit := filters.Limit + 1
	args = append(args, queryLimit)

	// Build unified query with deduplication
	query := fmt.Sprintf(`
		WITH combined AS (
			SELECT offer_id, seller_id, selling_asset_type, selling_asset_code, selling_asset_issuer,
			       buying_asset_type, buying_asset_code, buying_asset_issuer,
			       amount, price_n, price_d, price, last_modified_ledger, sponsor,
			       1 as source
			FROM %s.offers_current
			WHERE %s
			UNION ALL
			SELECT offer_id, seller_id, selling_asset_type, selling_asset_code, selling_asset_issuer,
			       buying_asset_type, buying_asset_code, buying_asset_issuer,
			       amount, price_n, price_d, price, last_modified_ledger, sponsor,
			       2 as source
			FROM %s.offers_current
			WHERE %s
		),
		deduplicated AS (
			SELECT DISTINCT ON (offer_id)
			       offer_id, seller_id, selling_asset_type, selling_asset_code, selling_asset_issuer,
			       buying_asset_type, buying_asset_code, buying_asset_issuer,
			       amount, price_n, price_d, price, last_modified_ledger, sponsor
			FROM combined
			ORDER BY offer_id, source ASC, last_modified_ledger DESC
		)
		SELECT * FROM deduplicated
		ORDER BY offer_id ASC
		LIMIT $%d
	`, r.hotSchema, whereClause, r.coldSchema, whereClause, argNum)

	rows, err := r.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, "", false, fmt.Errorf("unified GetOffers: %w", err)
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
			return nil, "", false, err
		}

		o.Selling = buildAssetInfo(sellingType.String, sellingCode.String, sellingIssuer.String)
		o.Buying = buildAssetInfo(buyingType.String, buyingCode.String, buyingIssuer.String)
		o.Amount = formatStroops(amount)
		o.Price = fmt.Sprintf("%.7f", price)
		o.PriceR = PriceR{N: priceN, D: priceD}
		if sponsor.Valid {
			o.Sponsor = &sponsor.String
		}

		offers = append(offers, o)
	}

	// Check for hasMore and generate cursor
	hasMore := len(offers) > filters.Limit
	var nextCursor string
	if hasMore {
		offers = offers[:filters.Limit]
		lastOffer := offers[len(offers)-1]
		nextCursor = OfferCursor{OfferID: lastOffer.OfferID}.Encode()
	}

	return offers, nextCursor, hasMore, nil
}

// GetOfferByID returns a single offer by ID, checking both hot and cold
func (r *UnifiedDuckDBReader) GetOfferByID(ctx context.Context, offerID int64) (*OfferCurrent, error) {
	query := fmt.Sprintf(`
		SELECT offer_id, seller_id, selling_asset_type, selling_asset_code, selling_asset_issuer,
		       buying_asset_type, buying_asset_code, buying_asset_issuer,
		       amount, price_n, price_d, price, last_modified_ledger, sponsor
		FROM (
			SELECT offer_id, seller_id, selling_asset_type, selling_asset_code, selling_asset_issuer,
			       buying_asset_type, buying_asset_code, buying_asset_issuer,
			       amount, price_n, price_d, price, last_modified_ledger, sponsor, 1 as source
			FROM %s.offers_current WHERE offer_id = $1
			UNION ALL
			SELECT offer_id, seller_id, selling_asset_type, selling_asset_code, selling_asset_issuer,
			       buying_asset_type, buying_asset_code, buying_asset_issuer,
			       amount, price_n, price_d, price, last_modified_ledger, sponsor, 2 as source
			FROM %s.offers_current WHERE offer_id = $1
		) combined
		ORDER BY source ASC, last_modified_ledger DESC
		LIMIT 1
	`, r.hotSchema, r.coldSchema)

	var o OfferCurrent
	var sellingType, sellingCode, buyingType, buyingCode sql.NullString
	var sellingIssuer, buyingIssuer, sponsor sql.NullString
	var amount int64
	var priceN, priceD int
	var price float64

	err := r.db.QueryRowContext(ctx, query, offerID).Scan(
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
		return nil, fmt.Errorf("unified GetOfferByID: %w", err)
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

// GetLiquidityPools returns liquidity pools with optional filters, merging hot and cold
func (r *UnifiedDuckDBReader) GetLiquidityPools(ctx context.Context, filters LiquidityPoolFilters) ([]LiquidityPoolCurrent, string, bool, error) {
	whereConditions := []string{}
	args := []interface{}{}
	argNum := 1

	if filters.AssetCode != "" {
		if filters.AssetCode == "XLM" {
			whereConditions = append(whereConditions, "(asset_a_type = 'native' OR asset_b_type = 'native')")
		} else {
			whereConditions = append(whereConditions,
				fmt.Sprintf("((asset_a_code = $%d AND asset_a_issuer = $%d) OR (asset_b_code = $%d AND asset_b_issuer = $%d))",
					argNum, argNum+1, argNum, argNum+1))
			args = append(args, filters.AssetCode, filters.AssetIssuer)
			argNum += 2
		}
	}

	if filters.Cursor != nil {
		whereConditions = append(whereConditions, fmt.Sprintf("liquidity_pool_id > $%d", argNum))
		args = append(args, filters.Cursor.PoolID)
		argNum++
	}

	whereClause := "1=1"
	if len(whereConditions) > 0 {
		whereClause = strings.Join(whereConditions, " AND ")
	}

	queryLimit := filters.Limit + 1
	args = append(args, queryLimit)

	query := fmt.Sprintf(`
		WITH combined AS (
			SELECT liquidity_pool_id, pool_type, fee, trustline_count, total_pool_shares,
			       asset_a_type, asset_a_code, asset_a_issuer, asset_a_amount,
			       asset_b_type, asset_b_code, asset_b_issuer, asset_b_amount,
			       last_modified_ledger, 1 as source
			FROM %s.liquidity_pools_current
			WHERE %s
			UNION ALL
			SELECT liquidity_pool_id, pool_type, fee, trustline_count, total_pool_shares,
			       asset_a_type, asset_a_code, asset_a_issuer, asset_a_amount,
			       asset_b_type, asset_b_code, asset_b_issuer, asset_b_amount,
			       last_modified_ledger, 2 as source
			FROM %s.liquidity_pools_current
			WHERE %s
		),
		deduplicated AS (
			SELECT DISTINCT ON (liquidity_pool_id)
			       liquidity_pool_id, pool_type, fee, trustline_count, total_pool_shares,
			       asset_a_type, asset_a_code, asset_a_issuer, asset_a_amount,
			       asset_b_type, asset_b_code, asset_b_issuer, asset_b_amount,
			       last_modified_ledger
			FROM combined
			ORDER BY liquidity_pool_id, source ASC, last_modified_ledger DESC
		)
		SELECT * FROM deduplicated
		ORDER BY liquidity_pool_id ASC
		LIMIT $%d
	`, r.hotSchema, whereClause, r.coldSchema, whereClause, argNum)

	rows, err := r.db.QueryContext(ctx, query, args...)
	if err != nil {
		// Check if the error is because cold table doesn't exist - fall back to hot only
		errStr := err.Error()
		if strings.Contains(errStr, "does not exist") || strings.Contains(errStr, "not found in FROM clause") {
			hotOnlyQuery := fmt.Sprintf(`
				SELECT liquidity_pool_id, pool_type, fee, trustline_count, total_pool_shares,
				       asset_a_type, asset_a_code, asset_a_issuer, asset_a_amount,
				       asset_b_type, asset_b_code, asset_b_issuer, asset_b_amount,
				       last_modified_ledger
				FROM %s.liquidity_pools_current
				WHERE %s
				ORDER BY liquidity_pool_id ASC
				LIMIT $%d
			`, r.hotSchema, whereClause, argNum)
			rows, err = r.db.QueryContext(ctx, hotOnlyQuery, args...)
			if err != nil {
				return nil, "", false, fmt.Errorf("unified GetLiquidityPools (hot-only): %w", err)
			}
		} else {
			return nil, "", false, fmt.Errorf("unified GetLiquidityPools: %w", err)
		}
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
			return nil, "", false, err
		}

		p.TotalShares = formatStroops(totalShares)
		p.Reserves = []PoolReserve{
			{Asset: buildAssetInfo(assetAType.String, assetACode.String, assetAIssuer.String), Amount: formatStroops(assetAAmount)},
			{Asset: buildAssetInfo(assetBType.String, assetBCode.String, assetBIssuer.String), Amount: formatStroops(assetBAmount)},
		}

		pools = append(pools, p)
	}

	hasMore := len(pools) > filters.Limit
	var nextCursor string
	if hasMore {
		pools = pools[:filters.Limit]
		lastPool := pools[len(pools)-1]
		nextCursor = LiquidityPoolCursor{PoolID: lastPool.PoolID}.Encode()
	}

	return pools, nextCursor, hasMore, nil
}

// GetLiquidityPoolByID returns a single liquidity pool by ID
func (r *UnifiedDuckDBReader) GetLiquidityPoolByID(ctx context.Context, poolID string) (*LiquidityPoolCurrent, error) {
	query := fmt.Sprintf(`
		SELECT liquidity_pool_id, pool_type, fee, trustline_count, total_pool_shares,
		       asset_a_type, asset_a_code, asset_a_issuer, asset_a_amount,
		       asset_b_type, asset_b_code, asset_b_issuer, asset_b_amount,
		       last_modified_ledger
		FROM (
			SELECT liquidity_pool_id, pool_type, fee, trustline_count, total_pool_shares,
			       asset_a_type, asset_a_code, asset_a_issuer, asset_a_amount,
			       asset_b_type, asset_b_code, asset_b_issuer, asset_b_amount,
			       last_modified_ledger, 1 as source
			FROM %s.liquidity_pools_current WHERE liquidity_pool_id = $1
			UNION ALL
			SELECT liquidity_pool_id, pool_type, fee, trustline_count, total_pool_shares,
			       asset_a_type, asset_a_code, asset_a_issuer, asset_a_amount,
			       asset_b_type, asset_b_code, asset_b_issuer, asset_b_amount,
			       last_modified_ledger, 2 as source
			FROM %s.liquidity_pools_current WHERE liquidity_pool_id = $1
		) combined
		ORDER BY source ASC, last_modified_ledger DESC
		LIMIT 1
	`, r.hotSchema, r.coldSchema)

	var p LiquidityPoolCurrent
	var assetAType, assetACode, assetBType, assetBCode sql.NullString
	var assetAIssuer, assetBIssuer sql.NullString
	var assetAAmount, assetBAmount, totalShares int64

	err := r.db.QueryRowContext(ctx, query, poolID).Scan(
		&p.PoolID, &p.PoolType, &p.FeeBP, &p.TrustlineCount, &totalShares,
		&assetAType, &assetACode, &assetAIssuer, &assetAAmount,
		&assetBType, &assetBCode, &assetBIssuer, &assetBAmount,
		&p.LastModifiedLedger,
	)

	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		// Check if the error is because cold table doesn't exist - fall back to hot only
		errStr := err.Error()
		if strings.Contains(errStr, "does not exist") || strings.Contains(errStr, "not found in FROM clause") {
			hotOnlyQuery := fmt.Sprintf(`
				SELECT liquidity_pool_id, pool_type, fee, trustline_count, total_pool_shares,
				       asset_a_type, asset_a_code, asset_a_issuer, asset_a_amount,
				       asset_b_type, asset_b_code, asset_b_issuer, asset_b_amount,
				       last_modified_ledger
				FROM %s.liquidity_pools_current
				WHERE liquidity_pool_id = $1
			`, r.hotSchema)
			err = r.db.QueryRowContext(ctx, hotOnlyQuery, poolID).Scan(
				&p.PoolID, &p.PoolType, &p.FeeBP, &p.TrustlineCount, &totalShares,
				&assetAType, &assetACode, &assetAIssuer, &assetAAmount,
				&assetBType, &assetBCode, &assetBIssuer, &assetBAmount,
				&p.LastModifiedLedger,
			)
			if err == sql.ErrNoRows {
				return nil, nil
			}
			if err != nil {
				return nil, fmt.Errorf("unified GetLiquidityPoolByID (hot-only): %w", err)
			}
		} else {
			return nil, fmt.Errorf("unified GetLiquidityPoolByID: %w", err)
		}
	}

	p.TotalShares = formatStroops(totalShares)
	p.Reserves = []PoolReserve{
		{Asset: buildAssetInfo(assetAType.String, assetACode.String, assetAIssuer.String), Amount: formatStroops(assetAAmount)},
		{Asset: buildAssetInfo(assetBType.String, assetBCode.String, assetBIssuer.String), Amount: formatStroops(assetBAmount)},
	}

	return &p, nil
}

// GetClaimableBalances returns claimable balances with optional filters
func (r *UnifiedDuckDBReader) GetClaimableBalances(ctx context.Context, filters ClaimableBalanceFilters) ([]ClaimableBalanceCurrent, string, bool, error) {
	whereConditions := []string{}
	args := []interface{}{}
	argNum := 1

	if filters.Sponsor != "" {
		whereConditions = append(whereConditions, fmt.Sprintf("sponsor = $%d", argNum))
		args = append(args, filters.Sponsor)
		argNum++
	}

	if filters.AssetCode != "" {
		if filters.AssetCode == "XLM" {
			whereConditions = append(whereConditions, "asset_type = 'native'")
		} else {
			whereConditions = append(whereConditions, fmt.Sprintf("asset_code = $%d", argNum))
			args = append(args, filters.AssetCode)
			argNum++
			if filters.AssetIssuer != "" {
				whereConditions = append(whereConditions, fmt.Sprintf("asset_issuer = $%d", argNum))
				args = append(args, filters.AssetIssuer)
				argNum++
			}
		}
	}

	if filters.Cursor != nil {
		whereConditions = append(whereConditions, fmt.Sprintf("balance_id > $%d", argNum))
		args = append(args, filters.Cursor.BalanceID)
		argNum++
	}

	whereClause := "1=1"
	if len(whereConditions) > 0 {
		whereClause = strings.Join(whereConditions, " AND ")
	}

	queryLimit := filters.Limit + 1
	args = append(args, queryLimit)

	query := fmt.Sprintf(`
		WITH combined AS (
			SELECT balance_id, sponsor, asset_type, asset_code, asset_issuer,
			       amount, COALESCE(json_array_length(claimants), 0) as claimants_count, flags, last_modified_ledger, 1 as source
			FROM %s.claimable_balances_current
			WHERE %s
			UNION ALL
			SELECT balance_id, sponsor, asset_type, asset_code, asset_issuer,
			       amount, COALESCE(json_array_length(claimants), 0) as claimants_count, flags, last_modified_ledger, 2 as source
			FROM %s.claimable_balances_current
			WHERE %s
		),
		deduplicated AS (
			SELECT DISTINCT ON (balance_id)
			       balance_id, sponsor, asset_type, asset_code, asset_issuer,
			       amount, claimants_count, flags, last_modified_ledger
			FROM combined
			ORDER BY balance_id, source ASC, last_modified_ledger DESC
		)
		SELECT * FROM deduplicated
		ORDER BY balance_id ASC
		LIMIT $%d
	`, r.hotSchema, whereClause, r.coldSchema, whereClause, argNum)

	rows, err := r.db.QueryContext(ctx, query, args...)
	if err != nil {
		// Check if the error is because cold table doesn't exist - fall back to hot only
		errStr := err.Error()
		if strings.Contains(errStr, "does not exist") || strings.Contains(errStr, "not found in FROM clause") {
			hotOnlyQuery := fmt.Sprintf(`
				SELECT balance_id, sponsor, asset_type, asset_code, asset_issuer,
				       amount, COALESCE(json_array_length(claimants), 0) as claimants_count, flags, last_modified_ledger
				FROM %s.claimable_balances_current
				WHERE %s
				ORDER BY balance_id ASC
				LIMIT $%d
			`, r.hotSchema, whereClause, argNum)
			rows, err = r.db.QueryContext(ctx, hotOnlyQuery, args...)
			if err != nil {
				return nil, "", false, fmt.Errorf("unified GetClaimableBalances (hot-only): %w", err)
			}
		} else {
			return nil, "", false, fmt.Errorf("unified GetClaimableBalances: %w", err)
		}
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
			return nil, "", false, err
		}

		if sponsor.Valid {
			b.Sponsor = &sponsor.String
		}
		b.Asset = buildAssetInfo(assetType.String, assetCode.String, assetIssuer.String)
		b.Amount = formatStroops(amount)

		balances = append(balances, b)
	}

	hasMore := len(balances) > filters.Limit
	var nextCursor string
	if hasMore {
		balances = balances[:filters.Limit]
		lastBalance := balances[len(balances)-1]
		nextCursor = ClaimableBalanceCursor{BalanceID: lastBalance.BalanceID}.Encode()
	}

	return balances, nextCursor, hasMore, nil
}

// GetClaimableBalanceByID returns a single claimable balance by ID
func (r *UnifiedDuckDBReader) GetClaimableBalanceByID(ctx context.Context, balanceID string) (*ClaimableBalanceCurrent, error) {
	query := fmt.Sprintf(`
		SELECT balance_id, sponsor, asset_type, asset_code, asset_issuer,
		       amount, claimants_count, flags, last_modified_ledger
		FROM (
			SELECT balance_id, sponsor, asset_type, asset_code, asset_issuer,
			       amount, COALESCE(json_array_length(claimants), 0) as claimants_count, flags, last_modified_ledger, 1 as source
			FROM %s.claimable_balances_current WHERE balance_id = $1
			UNION ALL
			SELECT balance_id, sponsor, asset_type, asset_code, asset_issuer,
			       amount, COALESCE(json_array_length(claimants), 0) as claimants_count, flags, last_modified_ledger, 2 as source
			FROM %s.claimable_balances_current WHERE balance_id = $1
		) combined
		ORDER BY source ASC, last_modified_ledger DESC
		LIMIT 1
	`, r.hotSchema, r.coldSchema)

	var b ClaimableBalanceCurrent
	var sponsor, assetType, assetCode, assetIssuer sql.NullString
	var amount int64

	err := r.db.QueryRowContext(ctx, query, balanceID).Scan(
		&b.BalanceID, &sponsor,
		&assetType, &assetCode, &assetIssuer,
		&amount, &b.ClaimantsCount, &b.Flags, &b.LastModifiedLedger,
	)

	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		// Check if the error is because cold table doesn't exist - fall back to hot only
		errStr := err.Error()
		if strings.Contains(errStr, "does not exist") || strings.Contains(errStr, "not found in FROM clause") {
			hotOnlyQuery := fmt.Sprintf(`
				SELECT balance_id, sponsor, asset_type, asset_code, asset_issuer,
				       amount, COALESCE(json_array_length(claimants), 0) as claimants_count, flags, last_modified_ledger
				FROM %s.claimable_balances_current
				WHERE balance_id = $1
			`, r.hotSchema)
			err = r.db.QueryRowContext(ctx, hotOnlyQuery, balanceID).Scan(
				&b.BalanceID, &sponsor,
				&assetType, &assetCode, &assetIssuer,
				&amount, &b.ClaimantsCount, &b.Flags, &b.LastModifiedLedger,
			)
			if err == sql.ErrNoRows {
				return nil, nil
			}
			if err != nil {
				return nil, fmt.Errorf("unified GetClaimableBalanceByID (hot-only): %w", err)
			}
		} else {
			return nil, fmt.Errorf("unified GetClaimableBalanceByID: %w", err)
		}
	}

	if sponsor.Valid {
		b.Sponsor = &sponsor.String
	}
	b.Asset = buildAssetInfo(assetType.String, assetCode.String, assetIssuer.String)
	b.Amount = formatStroops(amount)

	return &b, nil
}

// ============================================
// PHASE 7: EVENT TABLE METHODS
// ============================================

// GetTrades returns trades from unified hot+cold storage
func (r *UnifiedDuckDBReader) GetTrades(ctx context.Context, filters TradeFilters) ([]SilverTrade, string, bool, error) {
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

	// Determine order direction (default: asc for backward compatibility)
	orderDir := "ASC"
	cursorOp := ">"
	if filters.Order == "desc" {
		orderDir = "DESC"
		cursorOp = "<"
	}

	// Cursor pagination - direction depends on order
	if filters.Cursor != nil {
		conditions = append(conditions, fmt.Sprintf(`
			(ledger_sequence, transaction_hash, operation_index, trade_index) %s ($%d, $%d, $%d, $%d)
		`, cursorOp, argNum, argNum+1, argNum+2, argNum+3))
		args = append(args, filters.Cursor.LedgerSequence, filters.Cursor.TransactionHash,
			filters.Cursor.OperationIndex, filters.Cursor.TradeIndex)
		argNum += 4
	}

	whereClause := strings.Join(conditions, " AND ")

	limit := filters.Limit
	if limit <= 0 {
		limit = 100
	}

	// Unified query with hot+cold merge - trades are append-only so no dedup needed
	query := fmt.Sprintf(`
		SELECT ledger_sequence, transaction_hash, operation_index, trade_index,
			   COALESCE(trade_type, 'orderbook') as trade_type, trade_timestamp,
			   seller_account, selling_asset_code, selling_asset_issuer, selling_amount,
			   buyer_account, buying_asset_code, buying_asset_issuer, buying_amount,
			   price
		FROM (
			SELECT ledger_sequence, transaction_hash, operation_index, trade_index,
				   trade_type, trade_timestamp,
				   seller_account, selling_asset_code, selling_asset_issuer, selling_amount,
				   buyer_account, buying_asset_code, buying_asset_issuer, buying_amount,
				   price
			FROM %s.trades WHERE %s
			UNION ALL
			SELECT ledger_sequence, transaction_hash, operation_index, trade_index,
				   trade_type, trade_timestamp,
				   seller_account, selling_asset_code, selling_asset_issuer, selling_amount,
				   buyer_account, buying_asset_code, buying_asset_issuer, buying_amount,
				   price
			FROM %s.trades WHERE %s
		) combined
		ORDER BY ledger_sequence %s, transaction_hash %s, operation_index %s, trade_index %s
		LIMIT $%d
	`, r.hotSchema, whereClause, r.coldSchema, whereClause, orderDir, orderDir, orderDir, orderDir, argNum)
	args = append(args, limit+1)

	rows, err := r.db.QueryContext(ctx, query, args...)
	if err != nil {
		// Check if cold table doesn't exist, fall back to hot-only
		if strings.Contains(err.Error(), "does not exist") && strings.Contains(err.Error(), "trades") {
			hotOnlyQuery := fmt.Sprintf(`
				SELECT ledger_sequence, transaction_hash, operation_index, trade_index,
					   COALESCE(trade_type, 'orderbook') as trade_type, trade_timestamp,
					   seller_account, selling_asset_code, selling_asset_issuer, selling_amount,
					   buyer_account, buying_asset_code, buying_asset_issuer, buying_amount,
					   price
				FROM %s.trades WHERE %s
				ORDER BY ledger_sequence %s, transaction_hash %s, operation_index %s, trade_index %s
				LIMIT $%d
			`, r.hotSchema, whereClause, orderDir, orderDir, orderDir, orderDir, argNum)
			rows, err = r.db.QueryContext(ctx, hotOnlyQuery, args...)
			if err != nil {
				return nil, "", false, fmt.Errorf("unified GetTrades (hot-only fallback): %w", err)
			}
		} else {
			return nil, "", false, fmt.Errorf("unified GetTrades: %w", err)
		}
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
			return nil, "", false, fmt.Errorf("unified GetTrades scan: %w", err)
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
			Order:           filters.Order,
		}.Encode()
	}

	return trades, nextCursor, hasMore, nil
}

// GetTradeStats returns aggregated trade statistics from unified storage
func (r *UnifiedDuckDBReader) GetTradeStats(ctx context.Context, groupBy string, startTime, endTime time.Time) ([]TradeStats, error) {
	var groupExpr, selectGroup string
	switch groupBy {
	case "asset_pair":
		groupExpr = "COALESCE(selling_asset_code, 'XLM') || '/' || COALESCE(buying_asset_code, 'XLM')"
		selectGroup = groupExpr + " as group_key"
	case "hour":
		groupExpr = "date_trunc('hour', trade_timestamp)"
		selectGroup = "strftime(" + groupExpr + ", '%Y-%m-%d %H:00') as group_key"
	case "day":
		groupExpr = "date_trunc('day', trade_timestamp)"
		selectGroup = "strftime(" + groupExpr + ", '%Y-%m-%d') as group_key"
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
		FROM (
			SELECT selling_asset_code, buying_asset_code, trade_timestamp,
				   selling_amount, buying_amount, seller_account, buyer_account, price
			FROM %s.trades WHERE trade_timestamp >= $1 AND trade_timestamp <= $2
			UNION ALL
			SELECT selling_asset_code, buying_asset_code, trade_timestamp,
				   selling_amount, buying_amount, seller_account, buyer_account, price
			FROM %s.trades WHERE trade_timestamp >= $1 AND trade_timestamp <= $2
		) combined
		GROUP BY %s
		ORDER BY trade_count DESC
		LIMIT 100
	`, selectGroup, r.hotSchema, r.coldSchema, groupExpr)

	rows, err := r.db.QueryContext(ctx, query, startTime, endTime)
	if err != nil {
		// Check if cold table doesn't exist, fall back to hot-only
		if strings.Contains(err.Error(), "does not exist") && strings.Contains(err.Error(), "trades") {
			hotOnlyQuery := fmt.Sprintf(`
				SELECT %s,
					   COUNT(*) as trade_count,
					   SUM(selling_amount) as volume_selling,
					   SUM(buying_amount) as volume_buying,
					   COUNT(DISTINCT seller_account) as unique_sellers,
					   COUNT(DISTINCT buyer_account) as unique_buyers,
					   AVG(price) as avg_price
				FROM %s.trades WHERE trade_timestamp >= $1 AND trade_timestamp <= $2
				GROUP BY %s
				ORDER BY trade_count DESC
				LIMIT 100
			`, selectGroup, r.hotSchema, groupExpr)
			rows, err = r.db.QueryContext(ctx, hotOnlyQuery, startTime, endTime)
			if err != nil {
				return nil, fmt.Errorf("unified GetTradeStats (hot-only fallback): %w", err)
			}
		} else {
			return nil, fmt.Errorf("unified GetTradeStats: %w", err)
		}
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
			return nil, fmt.Errorf("unified GetTradeStats scan: %w", err)
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

// GetEffects returns effects from unified hot+cold storage
func (r *UnifiedDuckDBReader) GetEffects(ctx context.Context, filters EffectFilters) ([]SilverEffect, string, bool, error) {
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

	// Determine order direction (default: asc for backward compatibility)
	orderDir := "ASC"
	cursorOp := ">"
	if filters.Order == "desc" {
		orderDir = "DESC"
		cursorOp = "<"
	}

	// Cursor pagination - direction depends on order
	if filters.Cursor != nil {
		conditions = append(conditions, fmt.Sprintf(`
			(ledger_sequence, transaction_hash, operation_index, effect_index) %s ($%d, $%d, $%d, $%d)
		`, cursorOp, argNum, argNum+1, argNum+2, argNum+3))
		args = append(args, filters.Cursor.LedgerSequence, filters.Cursor.TransactionHash,
			filters.Cursor.OperationIndex, filters.Cursor.EffectIndex)
		argNum += 4
	}

	whereClause := "1=1"
	if len(conditions) > 0 {
		whereClause = strings.Join(conditions, " AND ")
	}

	limit := filters.Limit
	if limit <= 0 {
		limit = 100
	}

	// Unified query with hot+cold merge - effects are append-only so no dedup needed
	query := fmt.Sprintf(`
		SELECT ledger_sequence, transaction_hash, operation_index, effect_index,
			   effect_type, effect_type_string, account_id,
			   amount, asset_code, asset_issuer, asset_type,
			   trustline_limit, authorize_flag, clawback_flag,
			   signer_account, signer_weight, offer_id, seller_account,
			   created_at
		FROM (
			SELECT ledger_sequence, transaction_hash, operation_index, effect_index,
				   effect_type, effect_type_string, account_id,
				   amount, asset_code, asset_issuer, asset_type,
				   trustline_limit, authorize_flag, clawback_flag,
				   signer_account, signer_weight, offer_id, seller_account,
				   created_at
			FROM %s.effects WHERE %s
			UNION ALL
			SELECT ledger_sequence, transaction_hash, operation_index, effect_index,
				   effect_type, effect_type_string, account_id,
				   amount, asset_code, asset_issuer, asset_type,
				   trustline_limit, authorize_flag, clawback_flag,
				   signer_account, signer_weight, offer_id, seller_account,
				   created_at
			FROM %s.effects WHERE %s
		) combined
		ORDER BY ledger_sequence %s, transaction_hash %s, operation_index %s, effect_index %s
		LIMIT $%d
	`, r.hotSchema, whereClause, r.coldSchema, whereClause, orderDir, orderDir, orderDir, orderDir, argNum)
	args = append(args, limit+1)

	rows, err := r.db.QueryContext(ctx, query, args...)
	if err != nil {
		// Check if cold table doesn't exist, fall back to hot-only
		if strings.Contains(err.Error(), "does not exist") && strings.Contains(err.Error(), "effects") {
			hotOnlyQuery := fmt.Sprintf(`
				SELECT ledger_sequence, transaction_hash, operation_index, effect_index,
					   effect_type, effect_type_string, account_id,
					   amount, asset_code, asset_issuer, asset_type,
					   trustline_limit, authorize_flag, clawback_flag,
					   signer_account, signer_weight, offer_id, seller_account,
					   created_at
				FROM %s.effects WHERE %s
				ORDER BY ledger_sequence %s, transaction_hash %s, operation_index %s, effect_index %s
				LIMIT $%d
			`, r.hotSchema, whereClause, orderDir, orderDir, orderDir, orderDir, argNum)
			rows, err = r.db.QueryContext(ctx, hotOnlyQuery, args...)
			if err != nil {
				return nil, "", false, fmt.Errorf("unified GetEffects (hot-only fallback): %w", err)
			}
		} else {
			return nil, "", false, fmt.Errorf("unified GetEffects: %w", err)
		}
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
			return nil, "", false, fmt.Errorf("unified GetEffects scan: %w", err)
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
			Order:           filters.Order,
		}.Encode()
	}

	return effects, nextCursor, hasMore, nil
}

// GetEffectTypes returns counts of each effect type from unified storage
func (r *UnifiedDuckDBReader) GetEffectTypes(ctx context.Context) ([]EffectTypeCount, int64, error) {
	query := fmt.Sprintf(`
		SELECT effect_type, effect_type_string, SUM(cnt) as count
		FROM (
			SELECT effect_type, effect_type_string, COUNT(*) as cnt
			FROM %s.effects
			GROUP BY effect_type, effect_type_string
			UNION ALL
			SELECT effect_type, effect_type_string, COUNT(*) as cnt
			FROM %s.effects
			GROUP BY effect_type, effect_type_string
		) combined
		GROUP BY effect_type, effect_type_string
		ORDER BY count DESC
	`, r.hotSchema, r.coldSchema)

	rows, err := r.db.QueryContext(ctx, query)
	if err != nil {
		// Check if cold table doesn't exist, fall back to hot-only
		if strings.Contains(err.Error(), "does not exist") && strings.Contains(err.Error(), "effects") {
			hotOnlyQuery := fmt.Sprintf(`
				SELECT effect_type, effect_type_string, COUNT(*) as count
				FROM %s.effects
				GROUP BY effect_type, effect_type_string
				ORDER BY count DESC
			`, r.hotSchema)
			rows, err = r.db.QueryContext(ctx, hotOnlyQuery)
			if err != nil {
				return nil, 0, fmt.Errorf("unified GetEffectTypes (hot-only fallback): %w", err)
			}
		} else {
			return nil, 0, fmt.Errorf("unified GetEffectTypes: %w", err)
		}
	}
	defer rows.Close()

	var types []EffectTypeCount
	var total int64
	for rows.Next() {
		var t EffectTypeCount
		err := rows.Scan(&t.Type, &t.Name, &t.Count)
		if err != nil {
			return nil, 0, fmt.Errorf("unified GetEffectTypes scan: %w", err)
		}
		total += t.Count
		types = append(types, t)
	}

	return types, total, nil
}

// ============================================
// PHASE 8: SOROBAN TABLE METHODS
// ============================================

// GetContractCode returns contract code metadata by hash from unified storage
func (r *UnifiedDuckDBReader) GetContractCode(ctx context.Context, hash string) (*ContractCode, error) {
	query := fmt.Sprintf(`
		SELECT contract_code_hash, n_functions, n_instructions, n_data_segments,
			   n_data_segment_bytes, n_elem_segments, n_exports, n_globals,
			   n_imports, n_table_entries, n_types, last_modified_ledger, created_at
		FROM (
			SELECT * FROM %s.contract_code_current WHERE contract_code_hash = $1
			UNION ALL
			SELECT * FROM %s.contract_code_current WHERE contract_code_hash = $1
		) combined
		LIMIT 1
	`, r.hotSchema, r.coldSchema)

	var cc ContractCode
	var nFunctions, nInstructions, nDataSegments, nDataSegmentBytes sql.NullInt32
	var nElemSegments, nExports, nGlobals, nImports, nTableEntries, nTypes sql.NullInt32

	err := r.db.QueryRowContext(ctx, query, hash).Scan(
		&cc.Hash, &nFunctions, &nInstructions, &nDataSegments,
		&nDataSegmentBytes, &nElemSegments, &nExports, &nGlobals,
		&nImports, &nTableEntries, &nTypes, &cc.LastModifiedLedger, &cc.CreatedAt,
	)
	if err != nil {
		// Check if cold table doesn't exist, fall back to hot-only
		if strings.Contains(err.Error(), "does not exist") && strings.Contains(err.Error(), "contract_code") {
			hotOnlyQuery := fmt.Sprintf(`
				SELECT contract_code_hash, n_functions, n_instructions, n_data_segments,
					   n_data_segment_bytes, n_elem_segments, n_exports, n_globals,
					   n_imports, n_table_entries, n_types, last_modified_ledger, created_at
				FROM %s.contract_code_current WHERE contract_code_hash = $1
			`, r.hotSchema)
			err = r.db.QueryRowContext(ctx, hotOnlyQuery, hash).Scan(
				&cc.Hash, &nFunctions, &nInstructions, &nDataSegments,
				&nDataSegmentBytes, &nElemSegments, &nExports, &nGlobals,
				&nImports, &nTableEntries, &nTypes, &cc.LastModifiedLedger, &cc.CreatedAt,
			)
			if err == sql.ErrNoRows {
				return nil, nil
			}
			if err != nil {
				return nil, fmt.Errorf("unified GetContractCode (hot-only): %w", err)
			}
		} else if err == sql.ErrNoRows {
			return nil, nil
		} else {
			return nil, fmt.Errorf("unified GetContractCode: %w", err)
		}
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

// GetTTL returns TTL entry for a specific key from unified storage
func (r *UnifiedDuckDBReader) GetTTL(ctx context.Context, keyHash string) (*TTLEntry, error) {
	query := fmt.Sprintf(`
		SELECT key_hash, live_until_ledger_seq, expired, last_modified_ledger, closed_at
		FROM (
			SELECT * FROM %s.ttl_current WHERE key_hash = $1
			UNION ALL
			SELECT * FROM %s.ttl_current WHERE key_hash = $1
		) combined
		LIMIT 1
	`, r.hotSchema, r.coldSchema)

	var entry TTLEntry
	err := r.db.QueryRowContext(ctx, query, keyHash).Scan(
		&entry.KeyHash, &entry.LiveUntilLedger, &entry.Expired,
		&entry.LastModifiedLedger, &entry.ClosedAt,
	)
	if err != nil {
		// Check if cold table doesn't exist, fall back to hot-only
		if strings.Contains(err.Error(), "does not exist") && strings.Contains(err.Error(), "ttl") {
			hotOnlyQuery := fmt.Sprintf(`
				SELECT key_hash, live_until_ledger_seq, expired, last_modified_ledger, closed_at
				FROM %s.ttl_current WHERE key_hash = $1
			`, r.hotSchema)
			err = r.db.QueryRowContext(ctx, hotOnlyQuery, keyHash).Scan(
				&entry.KeyHash, &entry.LiveUntilLedger, &entry.Expired,
				&entry.LastModifiedLedger, &entry.ClosedAt,
			)
			if err == sql.ErrNoRows {
				return nil, nil
			}
			if err != nil {
				return nil, fmt.Errorf("unified GetTTL (hot-only): %w", err)
			}
		} else if err == sql.ErrNoRows {
			return nil, nil
		} else {
			return nil, fmt.Errorf("unified GetTTL: %w", err)
		}
	}

	return &entry, nil
}

// GetTTLExpiring returns TTL entries expiring within N ledgers from unified storage
func (r *UnifiedDuckDBReader) GetTTLExpiring(ctx context.Context, currentLedger int64, filters TTLFilters) ([]TTLEntry, string, bool, error) {
	var conditions []string
	var args []interface{}
	argNum := 1

	expirationThreshold := currentLedger + filters.WithinLedgers
	conditions = append(conditions, fmt.Sprintf("live_until_ledger_seq <= $%d", argNum))
	args = append(args, expirationThreshold)
	argNum++

	conditions = append(conditions, "expired = false")

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
		FROM (
			SELECT * FROM %s.ttl_current WHERE %s
			UNION ALL
			SELECT * FROM %s.ttl_current WHERE %s
		) combined
		ORDER BY live_until_ledger_seq ASC, key_hash ASC
		LIMIT $%d
	`, r.hotSchema, whereClause, r.coldSchema, whereClause, argNum)
	args = append(args, limit+1)

	rows, err := r.db.QueryContext(ctx, query, args...)
	if err != nil {
		// Check if cold table doesn't exist, fall back to hot-only
		if strings.Contains(err.Error(), "does not exist") && strings.Contains(err.Error(), "ttl") {
			hotOnlyQuery := fmt.Sprintf(`
				SELECT key_hash, live_until_ledger_seq, expired, last_modified_ledger, closed_at
				FROM %s.ttl_current WHERE %s
				ORDER BY live_until_ledger_seq ASC, key_hash ASC
				LIMIT $%d
			`, r.hotSchema, whereClause, argNum)
			rows, err = r.db.QueryContext(ctx, hotOnlyQuery, args...)
			if err != nil {
				return nil, "", false, fmt.Errorf("unified GetTTLExpiring (hot-only): %w", err)
			}
		} else {
			return nil, "", false, fmt.Errorf("unified GetTTLExpiring: %w", err)
		}
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
		e.Expired = e.LedgersRemaining <= 0
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

// GetTTLExpired returns expired TTL entries from unified storage
func (r *UnifiedDuckDBReader) GetTTLExpired(ctx context.Context, filters TTLFilters) ([]TTLEntry, string, bool, error) {
	var conditions []string
	var args []interface{}
	argNum := 1

	conditions = append(conditions, "expired = true")

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
		FROM (
			SELECT * FROM %s.ttl_current WHERE %s
			UNION ALL
			SELECT * FROM %s.ttl_current WHERE %s
		) combined
		ORDER BY live_until_ledger_seq DESC, key_hash ASC
		LIMIT $%d
	`, r.hotSchema, whereClause, r.coldSchema, whereClause, argNum)
	args = append(args, limit+1)

	rows, err := r.db.QueryContext(ctx, query, args...)
	if err != nil {
		if strings.Contains(err.Error(), "does not exist") && strings.Contains(err.Error(), "ttl") {
			hotOnlyQuery := fmt.Sprintf(`
				SELECT key_hash, live_until_ledger_seq, expired, last_modified_ledger, closed_at
				FROM %s.ttl_current WHERE %s
				ORDER BY live_until_ledger_seq DESC, key_hash ASC
				LIMIT $%d
			`, r.hotSchema, whereClause, argNum)
			rows, err = r.db.QueryContext(ctx, hotOnlyQuery, args...)
			if err != nil {
				return nil, "", false, fmt.Errorf("unified GetTTLExpired (hot-only): %w", err)
			}
		} else {
			return nil, "", false, fmt.Errorf("unified GetTTLExpired: %w", err)
		}
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

// GetEvictedKeys returns evicted storage keys from unified storage
func (r *UnifiedDuckDBReader) GetEvictedKeys(ctx context.Context, filters EvictionFilters) ([]EvictedKey, string, bool, error) {
	var conditions []string
	var args []interface{}
	argNum := 1

	if filters.ContractID != "" {
		conditions = append(conditions, fmt.Sprintf("contract_id = $%d", argNum))
		args = append(args, filters.ContractID)
		argNum++
	}

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
		FROM (
			SELECT * FROM %s.evicted_keys WHERE %s
			UNION ALL
			SELECT * FROM %s.evicted_keys WHERE %s
		) combined
		ORDER BY closed_at DESC, contract_id ASC, key_hash ASC, ledger_sequence ASC
		LIMIT $%d
	`, r.hotSchema, whereClause, r.coldSchema, whereClause, argNum)
	args = append(args, limit+1)

	rows, err := r.db.QueryContext(ctx, query, args...)
	if err != nil {
		if strings.Contains(err.Error(), "does not exist") && strings.Contains(err.Error(), "evicted") {
			hotOnlyQuery := fmt.Sprintf(`
				SELECT contract_id, key_hash, ledger_sequence, closed_at
				FROM %s.evicted_keys WHERE %s
				ORDER BY closed_at DESC, contract_id ASC, key_hash ASC, ledger_sequence ASC
				LIMIT $%d
			`, r.hotSchema, whereClause, argNum)
			rows, err = r.db.QueryContext(ctx, hotOnlyQuery, args...)
			if err != nil {
				return nil, "", false, fmt.Errorf("unified GetEvictedKeys (hot-only): %w", err)
			}
		} else {
			return nil, "", false, fmt.Errorf("unified GetEvictedKeys: %w", err)
		}
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

// GetRestoredKeys returns restored storage keys from unified storage
func (r *UnifiedDuckDBReader) GetRestoredKeys(ctx context.Context, filters EvictionFilters) ([]RestoredKey, string, bool, error) {
	var conditions []string
	var args []interface{}
	argNum := 1

	if filters.ContractID != "" {
		conditions = append(conditions, fmt.Sprintf("contract_id = $%d", argNum))
		args = append(args, filters.ContractID)
		argNum++
	}

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
		FROM (
			SELECT * FROM %s.restored_keys WHERE %s
			UNION ALL
			SELECT * FROM %s.restored_keys WHERE %s
		) combined
		ORDER BY closed_at DESC, contract_id ASC, key_hash ASC, ledger_sequence ASC
		LIMIT $%d
	`, r.hotSchema, whereClause, r.coldSchema, whereClause, argNum)
	args = append(args, limit+1)

	rows, err := r.db.QueryContext(ctx, query, args...)
	if err != nil {
		if strings.Contains(err.Error(), "does not exist") && strings.Contains(err.Error(), "restored") {
			hotOnlyQuery := fmt.Sprintf(`
				SELECT contract_id, key_hash, ledger_sequence, closed_at
				FROM %s.restored_keys WHERE %s
				ORDER BY closed_at DESC, contract_id ASC, key_hash ASC, ledger_sequence ASC
				LIMIT $%d
			`, r.hotSchema, whereClause, argNum)
			rows, err = r.db.QueryContext(ctx, hotOnlyQuery, args...)
			if err != nil {
				return nil, "", false, fmt.Errorf("unified GetRestoredKeys (hot-only): %w", err)
			}
		} else {
			return nil, "", false, fmt.Errorf("unified GetRestoredKeys: %w", err)
		}
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

// GetSorobanConfig returns current Soroban network configuration from unified storage
func (r *UnifiedDuckDBReader) GetSorobanConfig(ctx context.Context) (*SorobanConfig, error) {
	query := fmt.Sprintf(`
		SELECT ledger_max_instructions, tx_max_instructions, fee_rate_per_instructions_increment,
			   tx_memory_limit, ledger_max_read_ledger_entries, ledger_max_read_bytes,
			   ledger_max_write_ledger_entries, ledger_max_write_bytes,
			   tx_max_read_ledger_entries, tx_max_read_bytes,
			   tx_max_write_ledger_entries, tx_max_write_bytes,
			   contract_max_size_bytes, last_modified_ledger, closed_at
		FROM (
			SELECT * FROM %s.config_settings_current WHERE config_setting_id = 1
			UNION ALL
			SELECT * FROM %s.config_settings_current WHERE config_setting_id = 1
		) combined
		ORDER BY last_modified_ledger DESC
		LIMIT 1
	`, r.hotSchema, r.coldSchema)

	var cfg SorobanConfig
	var ledgerMaxInstr, txMaxInstr, feeRate, txMemLimit sql.NullInt64
	var ledgerMaxReadEntries, ledgerMaxReadBytes, ledgerMaxWriteEntries, ledgerMaxWriteBytes sql.NullInt64
	var txMaxReadEntries, txMaxReadBytes, txMaxWriteEntries, txMaxWriteBytes sql.NullInt64
	var contractMaxSize sql.NullInt64

	err := r.db.QueryRowContext(ctx, query).Scan(
		&ledgerMaxInstr, &txMaxInstr, &feeRate, &txMemLimit,
		&ledgerMaxReadEntries, &ledgerMaxReadBytes, &ledgerMaxWriteEntries, &ledgerMaxWriteBytes,
		&txMaxReadEntries, &txMaxReadBytes, &txMaxWriteEntries, &txMaxWriteBytes,
		&contractMaxSize, &cfg.LastModifiedLedger, &cfg.UpdatedAt,
	)
	if err != nil {
		if strings.Contains(err.Error(), "does not exist") && strings.Contains(err.Error(), "config") {
			hotOnlyQuery := fmt.Sprintf(`
				SELECT ledger_max_instructions, tx_max_instructions, fee_rate_per_instructions_increment,
					   tx_memory_limit, ledger_max_read_ledger_entries, ledger_max_read_bytes,
					   ledger_max_write_ledger_entries, ledger_max_write_bytes,
					   tx_max_read_ledger_entries, tx_max_read_bytes,
					   tx_max_write_ledger_entries, tx_max_write_bytes,
					   contract_max_size_bytes, last_modified_ledger, closed_at
				FROM %s.config_settings_current WHERE config_setting_id = 1
			`, r.hotSchema)
			err = r.db.QueryRowContext(ctx, hotOnlyQuery).Scan(
				&ledgerMaxInstr, &txMaxInstr, &feeRate, &txMemLimit,
				&ledgerMaxReadEntries, &ledgerMaxReadBytes, &ledgerMaxWriteEntries, &ledgerMaxWriteBytes,
				&txMaxReadEntries, &txMaxReadBytes, &txMaxWriteEntries, &txMaxWriteBytes,
				&contractMaxSize, &cfg.LastModifiedLedger, &cfg.UpdatedAt,
			)
			if err == sql.ErrNoRows {
				return nil, nil
			}
			if err != nil {
				return nil, fmt.Errorf("unified GetSorobanConfig (hot-only): %w", err)
			}
		} else if err == sql.ErrNoRows {
			return nil, nil
		} else {
			return nil, fmt.Errorf("unified GetSorobanConfig: %w", err)
		}
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

// GetContractData returns contract storage entries from unified storage
func (r *UnifiedDuckDBReader) GetContractData(ctx context.Context, filters ContractDataFilters) ([]ContractData, string, bool, error) {
	var conditions []string
	var args []interface{}
	argNum := 1

	if filters.ContractID != "" {
		conditions = append(conditions, fmt.Sprintf("contract_id = $%d", argNum))
		args = append(args, filters.ContractID)
		argNum++
	}

	if filters.Durability != "" {
		conditions = append(conditions, fmt.Sprintf("durability = $%d", argNum))
		args = append(args, filters.Durability)
		argNum++
	}

	if filters.KeyHash != "" {
		conditions = append(conditions, fmt.Sprintf("key_hash = $%d", argNum))
		args = append(args, filters.KeyHash)
		argNum++
	}

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
		FROM (
			SELECT contract_id, key_hash, durability, data_value,
				   asset_type, asset_code, asset_issuer, last_modified_ledger
			FROM %s.contract_data_current WHERE %s
			UNION ALL
			SELECT contract_id, key_hash, durability, data_value,
				   asset_type, asset_code, asset_issuer, last_modified_ledger
			FROM %s.contract_data_current WHERE %s
		) combined
		ORDER BY contract_id ASC, key_hash ASC
		LIMIT $%d
	`, r.hotSchema, whereClause, r.coldSchema, whereClause, argNum)
	args = append(args, limit+1)

	rows, err := r.db.QueryContext(ctx, query, args...)
	if err != nil {
		// Fall back to hot-only if cold table doesn't exist or has schema mismatch
		errStr := err.Error()
		if strings.Contains(errStr, "does not exist") ||
			strings.Contains(errStr, "not found in FROM clause") ||
			strings.Contains(errStr, "contract_data") {
			hotOnlyQuery := fmt.Sprintf(`
				SELECT contract_id, key_hash, durability, data_value,
					   asset_type, asset_code, asset_issuer, last_modified_ledger
				FROM %s.contract_data_current WHERE %s
				ORDER BY contract_id ASC, key_hash ASC
				LIMIT $%d
			`, r.hotSchema, whereClause, argNum)
			rows, err = r.db.QueryContext(ctx, hotOnlyQuery, args...)
			if err != nil {
				return nil, "", false, fmt.Errorf("unified GetContractData (hot-only): %w", err)
			}
		} else {
			return nil, "", false, fmt.Errorf("unified GetContractData: %w", err)
		}
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

// GetCurrentLedger returns the current ledger sequence from unified storage
func (r *UnifiedDuckDBReader) GetCurrentLedger(ctx context.Context) (int64, error) {
	query := fmt.Sprintf(`SELECT MAX(ledger_sequence) FROM %s.ttl_current`, r.hotSchema)
	var ledger sql.NullInt64
	err := r.db.QueryRowContext(ctx, query).Scan(&ledger)
	if err != nil {
		return 0, err
	}
	return ledger.Int64, nil
}

// GetAssetList returns a paginated list of all assets on the network
// Queries both hot and cold trustlines_current tables, deduplicates, then aggregates by asset
func (r *UnifiedDuckDBReader) GetAssetList(ctx context.Context, filters AssetListFilters) (*AssetListResponse, error) {
	requestedLimit := filters.Limit
	if requestedLimit <= 0 {
		requestedLimit = 100
	}
	if requestedLimit > 1000 {
		requestedLimit = 1000
	}

	// Determine sort field and order
	sortBy := filters.SortBy
	if sortBy == "" {
		sortBy = "holder_count"
	}
	sortOrder := filters.SortOrder
	if sortOrder == "" {
		sortOrder = "desc"
	}

	// Validate sort field
	validSortFields := map[string]string{
		"holder_count":       "holder_count",
		"volume_24h":         "volume_24h",
		"transfers_24h":      "transfers_24h",
		"circulating_supply": "circulating_supply",
	}
	dbSortField, ok := validSortFields[sortBy]
	if !ok {
		dbSortField = "holder_count"
		sortBy = "holder_count"
	}

	// Validate sort order
	if sortOrder != "asc" && sortOrder != "desc" {
		sortOrder = "desc"
	}

	// Build the unified query with hot + cold trustlines
	// Step 1: UNION ALL hot and cold trustlines
	// Step 2: Deduplicate by (account_id, asset_code, asset_issuer)
	// Step 3: Aggregate by asset to get holder counts and supply
	// Step 4: Join with transfer stats (hot only - recent data)
	query := fmt.Sprintf(`
		WITH combined_trustlines AS (
			SELECT account_id, asset_code, asset_issuer, asset_type,
			       CAST(balance AS BIGINT) as balance, last_modified_ledger,
			       created_at, updated_at, 1 as source
			FROM %s.trustlines_current
			WHERE asset_code IS NOT NULL AND asset_code != ''
			UNION ALL
			SELECT account_id, asset_code, asset_issuer, asset_type,
			       CAST(balance AS BIGINT) as balance, last_modified_ledger,
			       created_at, updated_at, 2 as source
			FROM %s.trustlines_current
			WHERE asset_code IS NOT NULL AND asset_code != ''
		),
		deduplicated_trustlines AS (
			SELECT DISTINCT ON (account_id, asset_code, asset_issuer)
			       account_id, asset_code, asset_issuer, asset_type, balance,
			       created_at, updated_at
			FROM combined_trustlines
			ORDER BY account_id, asset_code, asset_issuer, last_modified_ledger DESC, source ASC
		),
		asset_stats AS (
			SELECT
				asset_code,
				asset_issuer,
				asset_type,
				COUNT(*) FILTER (WHERE balance > 0) as holder_count,
				SUM(balance) as circulating_supply,
				MIN(created_at) as first_seen,
				MAX(updated_at) as last_activity
			FROM deduplicated_trustlines
			GROUP BY asset_code, asset_issuer, asset_type
		),
		transfer_stats AS (
			SELECT
				asset_code,
				asset_issuer,
				COUNT(*) as transfers_24h,
				COALESCE(SUM(amount), 0) as volume_24h
			FROM %s.token_transfers_raw
			WHERE timestamp >= NOW() - INTERVAL '24 hours'
			  AND transaction_successful = true
			GROUP BY asset_code, asset_issuer
		)
		SELECT
			a.asset_code,
			COALESCE(a.asset_issuer, '') as asset_issuer,
			COALESCE(a.asset_type, 'credit_alphanum4') as asset_type,
			a.holder_count,
			COALESCE(a.circulating_supply::text, '0') as circulating_supply,
			COALESCE(t.transfers_24h, 0) as transfers_24h,
			COALESCE(t.volume_24h, 0) as volume_24h,
			a.first_seen,
			a.last_activity
		FROM asset_stats a
		LEFT JOIN transfer_stats t
			ON a.asset_code = t.asset_code
			AND COALESCE(a.asset_issuer, '') = COALESCE(t.asset_issuer, '')
		WHERE a.holder_count > 0
	`, r.hotSchema, r.coldSchema, r.hotSchema)

	args := []interface{}{}
	argIndex := 1

	// Apply filters
	if filters.MinHolders != nil && *filters.MinHolders > 0 {
		query += fmt.Sprintf(" AND a.holder_count >= $%d", argIndex)
		args = append(args, *filters.MinHolders)
		argIndex++
	}

	if filters.MinVolume24h != nil && *filters.MinVolume24h > 0 {
		query += fmt.Sprintf(" AND COALESCE(t.volume_24h, 0) >= $%d", argIndex)
		args = append(args, *filters.MinVolume24h)
		argIndex++
	}

	if filters.AssetType != "" {
		query += fmt.Sprintf(" AND a.asset_type = $%d", argIndex)
		args = append(args, filters.AssetType)
		argIndex++
	}

	if filters.Search != "" {
		query += fmt.Sprintf(" AND UPPER(a.asset_code) LIKE UPPER($%d)", argIndex)
		args = append(args, filters.Search+"%")
		argIndex++
	}

	// Apply cursor for pagination
	if filters.Cursor != nil {
		// Validate cursor matches current sort settings
		if filters.Cursor.SortBy != sortBy || filters.Cursor.SortOrder != sortOrder {
			return nil, fmt.Errorf("cursor was created with different sort settings")
		}

		if sortOrder == "desc" {
			switch sortBy {
			case "holder_count":
				query += fmt.Sprintf(" AND (a.holder_count < $%d OR (a.holder_count = $%d AND (a.asset_code > $%d OR (a.asset_code = $%d AND COALESCE(a.asset_issuer, '') > $%d))))",
					argIndex, argIndex, argIndex+1, argIndex+1, argIndex+2)
				args = append(args, filters.Cursor.HolderCount, filters.Cursor.AssetCode, filters.Cursor.AssetIssuer)
				argIndex += 3
			case "volume_24h":
				query += fmt.Sprintf(" AND (COALESCE(t.volume_24h, 0) < $%d OR (COALESCE(t.volume_24h, 0) = $%d AND (a.asset_code > $%d OR (a.asset_code = $%d AND COALESCE(a.asset_issuer, '') > $%d))))",
					argIndex, argIndex, argIndex+1, argIndex+1, argIndex+2)
				args = append(args, filters.Cursor.Volume24h, filters.Cursor.AssetCode, filters.Cursor.AssetIssuer)
				argIndex += 3
			default:
				query += fmt.Sprintf(" AND (a.holder_count < $%d OR (a.holder_count = $%d AND (a.asset_code > $%d OR (a.asset_code = $%d AND COALESCE(a.asset_issuer, '') > $%d))))",
					argIndex, argIndex, argIndex+1, argIndex+1, argIndex+2)
				args = append(args, filters.Cursor.HolderCount, filters.Cursor.AssetCode, filters.Cursor.AssetIssuer)
				argIndex += 3
			}
		} else {
			// Ascending order
			switch sortBy {
			case "holder_count":
				query += fmt.Sprintf(" AND (a.holder_count > $%d OR (a.holder_count = $%d AND (a.asset_code > $%d OR (a.asset_code = $%d AND COALESCE(a.asset_issuer, '') > $%d))))",
					argIndex, argIndex, argIndex+1, argIndex+1, argIndex+2)
				args = append(args, filters.Cursor.HolderCount, filters.Cursor.AssetCode, filters.Cursor.AssetIssuer)
				argIndex += 3
			case "volume_24h":
				query += fmt.Sprintf(" AND (COALESCE(t.volume_24h, 0) > $%d OR (COALESCE(t.volume_24h, 0) = $%d AND (a.asset_code > $%d OR (a.asset_code = $%d AND COALESCE(a.asset_issuer, '') > $%d))))",
					argIndex, argIndex, argIndex+1, argIndex+1, argIndex+2)
				args = append(args, filters.Cursor.Volume24h, filters.Cursor.AssetCode, filters.Cursor.AssetIssuer)
				argIndex += 3
			default:
				query += fmt.Sprintf(" AND (a.holder_count > $%d OR (a.holder_count = $%d AND (a.asset_code > $%d OR (a.asset_code = $%d AND COALESCE(a.asset_issuer, '') > $%d))))",
					argIndex, argIndex, argIndex+1, argIndex+1, argIndex+2)
				args = append(args, filters.Cursor.HolderCount, filters.Cursor.AssetCode, filters.Cursor.AssetIssuer)
				argIndex += 3
			}
		}
	}

	// Add ORDER BY
	orderDir := "DESC"
	if sortOrder == "asc" {
		orderDir = "ASC"
	}
	query += fmt.Sprintf(" ORDER BY %s %s, a.asset_code ASC, a.asset_issuer ASC", dbSortField, orderDir)

	// Request one extra to detect has_more
	query += fmt.Sprintf(" LIMIT $%d", argIndex)
	args = append(args, requestedLimit+1)

	rows, err := r.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to query asset list: %w", err)
	}
	defer rows.Close()

	var assets []AssetSummary
	for rows.Next() {
		var a AssetSummary
		var circulatingSupplyStr sql.NullString
		var volume24h int64
		var firstSeen, lastActivity sql.NullTime

		err := rows.Scan(
			&a.AssetCode,
			&a.AssetIssuer,
			&a.AssetType,
			&a.HolderCount,
			&circulatingSupplyStr,
			&a.Transfers24h,
			&volume24h,
			&firstSeen,
			&lastActivity,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan asset row: %w", err)
		}

		// Handle large circulating supply values that overflow int64
		a.CirculatingSupply = formatBigNumericStroops(circulatingSupplyStr.String)
		a.Volume24h = formatStroopsLocal(volume24h)

		if firstSeen.Valid {
			a.FirstSeen = firstSeen.Time.Format(time.RFC3339)
		}
		if lastActivity.Valid {
			a.LastActivity = lastActivity.Time.Format(time.RFC3339)
		}

		assets = append(assets, a)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating asset rows: %w", err)
	}

	// Determine has_more and trim to requested limit
	hasMore := len(assets) > requestedLimit
	if hasMore {
		assets = assets[:requestedLimit]
	}

	// Generate next cursor from last result
	var nextCursor string
	if len(assets) > 0 && hasMore {
		last := assets[len(assets)-1]
		// Parse volume back to stroops for cursor
		volume24hStroops := parseFormattedToStroopsLocal(last.Volume24h)

		cursor := AssetListCursor{
			HolderCount: last.HolderCount,
			Volume24h:   volume24hStroops,
			AssetCode:   last.AssetCode,
			AssetIssuer: last.AssetIssuer,
			SortBy:      sortBy,
			SortOrder:   sortOrder,
		}
		nextCursor = cursor.Encode()
	}

	// Get total count of assets matching filters (excluding pagination)
	totalCount, err := r.GetAssetCount(ctx, filters)
	if err != nil {
		log.Printf("Warning: failed to get asset count: %v", err)
		// Fall back to page count if count query fails
		totalCount = len(assets)
	}

	return &AssetListResponse{
		Assets:      assets,
		TotalAssets: totalCount,
		Cursor:      nextCursor,
		HasMore:     hasMore,
		GeneratedAt: time.Now().UTC().Format(time.RFC3339),
	}, nil
}

// GetAssetCount returns the total count of assets matching the given filters (ignoring pagination)
func (r *UnifiedDuckDBReader) GetAssetCount(ctx context.Context, filters AssetListFilters) (int, error) {
	// Build count query with same filters as main query, but no pagination/sorting
	query := fmt.Sprintf(`
		WITH combined_trustlines AS (
			SELECT account_id, asset_code, asset_issuer, asset_type,
			       CAST(balance AS BIGINT) as balance, last_modified_ledger, 1 as source
			FROM %s.trustlines_current
			WHERE asset_code IS NOT NULL AND asset_code != ''
			UNION ALL
			SELECT account_id, asset_code, asset_issuer, asset_type,
			       CAST(balance AS BIGINT) as balance, last_modified_ledger, 2 as source
			FROM %s.trustlines_current
			WHERE asset_code IS NOT NULL AND asset_code != ''
		),
		deduplicated_trustlines AS (
			SELECT DISTINCT ON (account_id, asset_code, asset_issuer)
			       account_id, asset_code, asset_issuer, asset_type, balance
			FROM combined_trustlines
			ORDER BY account_id, asset_code, asset_issuer, last_modified_ledger DESC, source ASC
		),
		asset_stats AS (
			SELECT
				asset_code,
				asset_issuer,
				asset_type,
				COUNT(*) FILTER (WHERE balance > 0) as holder_count
			FROM deduplicated_trustlines
			GROUP BY asset_code, asset_issuer, asset_type
		),
		transfer_stats AS (
			SELECT
				asset_code,
				asset_issuer,
				COALESCE(SUM(amount), 0) as volume_24h
			FROM %s.token_transfers_raw
			WHERE timestamp >= NOW() - INTERVAL '24 hours'
			  AND transaction_successful = true
			GROUP BY asset_code, asset_issuer
		)
		SELECT COUNT(*)
		FROM asset_stats a
		LEFT JOIN transfer_stats t
			ON a.asset_code = t.asset_code
			AND COALESCE(a.asset_issuer, '') = COALESCE(t.asset_issuer, '')
		WHERE a.holder_count > 0
	`, r.hotSchema, r.coldSchema, r.hotSchema)

	args := []interface{}{}
	argIndex := 1

	// Apply same filters as main query (excluding pagination/cursor)
	if filters.MinHolders != nil && *filters.MinHolders > 0 {
		query += fmt.Sprintf(" AND a.holder_count >= $%d", argIndex)
		args = append(args, *filters.MinHolders)
		argIndex++
	}

	if filters.MinVolume24h != nil && *filters.MinVolume24h > 0 {
		query += fmt.Sprintf(" AND COALESCE(t.volume_24h, 0) >= $%d", argIndex)
		args = append(args, *filters.MinVolume24h)
		argIndex++
	}

	if filters.AssetType != "" {
		query += fmt.Sprintf(" AND a.asset_type = $%d", argIndex)
		args = append(args, filters.AssetType)
		argIndex++
	}

	if filters.Search != "" {
		query += fmt.Sprintf(" AND UPPER(a.asset_code) LIKE UPPER($%d)", argIndex)
		args = append(args, filters.Search+"%")
		argIndex++
	}

	var count int
	err := r.db.QueryRowContext(ctx, query, args...).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("failed to count assets: %w", err)
	}

	return count, nil
}

// formatStroopsLocal converts stroops to formatted string (7 decimal places)
// Local version to avoid redeclaration with other files
func formatStroopsLocal(stroops int64) string {
	whole := stroops / 10000000
	frac := stroops % 10000000
	if frac < 0 {
		frac = -frac
	}
	return fmt.Sprintf("%d.%07d", whole, frac)
}

// parseFormattedToStroopsLocal converts formatted string back to stroops
func parseFormattedToStroopsLocal(formatted string) int64 {
	var whole, frac int64
	_, err := fmt.Sscanf(formatted, "%d.%d", &whole, &frac)
	if err != nil {
		return 0
	}
	return whole*10000000 + frac
}

// ============================================
// CONTRACT CALL QUERIES (with cold fallback)
// ============================================

// ContractCallRecord represents a contract invocation from enriched_history_operations (hot+cold)
type ContractCallRecord struct {
	TransactionHash string  `json:"transaction_hash"`
	LedgerSequence  int64   `json:"ledger_sequence"`
	ClosedAt        string  `json:"closed_at"`
	ContractID      *string `json:"contract_id,omitempty"`
	FunctionName    *string `json:"function_name,omitempty"`
	SourceAccount   string  `json:"source_account"`
	Successful      bool    `json:"successful"`
	OperationIndex  int64   `json:"operation_index"`
}

// GetRecentContractCallsWithCursor returns recent contract invocations using UNION ALL hot+cold
// on enriched_history_operations (which exists in both hot and cold schemas).
func (r *UnifiedDuckDBReader) GetRecentContractCallsWithCursor(ctx context.Context, contractID string, limit int, cursor *OperationCursor, order string) ([]ContractCallRecord, string, bool, error) {
	requestLimit := limit + 1
	whereClause := "WHERE is_soroban_op = true AND contract_id = $1"
	args := []interface{}{contractID}
	argNum := 2

	orderDir := "DESC"
	cursorOp := "<"
	if order == "asc" {
		orderDir = "ASC"
		cursorOp = ">"
	}

	cursorClause := ""
	if cursor != nil {
		cursorClause = fmt.Sprintf(" AND (ledger_sequence %s $%d OR (ledger_sequence = $%d AND operation_index %s $%d))",
			cursorOp, argNum, argNum, cursorOp, argNum+1)
		args = append(args, cursor.LedgerSequence, cursor.OperationIndex)
		argNum += 2
	}

	query := fmt.Sprintf(`
		WITH combined AS (
			SELECT transaction_hash, ledger_sequence, ledger_closed_at, contract_id,
				   function_name, source_account, tx_successful, operation_index, 1 as source
			FROM %s.enriched_history_operations
			%s
			UNION ALL
			SELECT transaction_hash, ledger_sequence, ledger_closed_at, contract_id,
				   function_name, source_account, tx_successful, operation_index, 2 as source
			FROM %s.enriched_history_operations
			%s
		)
		SELECT DISTINCT ON (ledger_sequence, operation_index)
			transaction_hash, ledger_sequence, ledger_closed_at, contract_id,
			function_name, source_account, tx_successful, operation_index
		FROM combined
		WHERE 1=1 %s
		ORDER BY ledger_sequence %s, operation_index %s
		LIMIT $%d
	`, r.hotSchema, whereClause, r.coldSchema, whereClause, cursorClause, orderDir, orderDir, argNum)

	args = append(args, requestLimit)

	rows, err := r.db.QueryContext(ctx, query, args...)
	if err != nil {
		errStr := err.Error()
		if strings.Contains(errStr, "does not exist") || strings.Contains(errStr, "not found in FROM clause") {
			hotOnlyQuery := fmt.Sprintf(`
				SELECT transaction_hash, ledger_sequence, ledger_closed_at, contract_id,
					   function_name, source_account, tx_successful, operation_index
				FROM %s.enriched_history_operations
				%s %s
				ORDER BY ledger_sequence %s, operation_index %s
				LIMIT $%d
			`, r.hotSchema, whereClause, cursorClause, orderDir, orderDir, argNum)
			rows, err = r.db.QueryContext(ctx, hotOnlyQuery, args...)
			if err != nil {
				return nil, "", false, fmt.Errorf("hot-only fallback GetRecentContractCalls: %w", err)
			}
		} else {
			return nil, "", false, fmt.Errorf("GetRecentContractCalls: %w", err)
		}
	}
	defer rows.Close()

	var calls []ContractCallRecord
	for rows.Next() {
		var c ContractCallRecord
		if err := rows.Scan(&c.TransactionHash, &c.LedgerSequence, &c.ClosedAt,
			&c.ContractID, &c.FunctionName, &c.SourceAccount, &c.Successful,
			&c.OperationIndex); err != nil {
			return nil, "", false, err
		}
		calls = append(calls, c)
	}

	hasMore := len(calls) > limit
	if hasMore {
		calls = calls[:limit]
	}

	var nextCursor string
	if len(calls) > 0 && hasMore {
		last := calls[len(calls)-1]
		c := OperationCursor{
			LedgerSequence: last.LedgerSequence,
			OperationIndex: last.OperationIndex,
			Order:          order,
		}
		nextCursor = c.Encode()
	}

	return calls, nextCursor, hasMore, nil
}

// ============================================
// CAP-67 UNIFIED EVENT QUERIES
// ============================================

// GetUnifiedEvents returns CAP-67 unified events from token_transfers_raw with cursor pagination.
// Event type is derived: NULL from_account = mint, NULL to_account = burn, else transfer.
func (r *UnifiedDuckDBReader) GetUnifiedEvents(ctx context.Context, filters UnifiedEventFilters) ([]UnifiedEvent, string, bool, error) {
	whereClause := "WHERE transaction_successful = true"
	args := []interface{}{}
	argNum := 1

	if filters.ContractID != "" {
		whereClause += fmt.Sprintf(" AND token_contract_id = $%d", argNum)
		args = append(args, filters.ContractID)
		argNum++
	}

	if filters.Address != "" {
		whereClause += fmt.Sprintf(" AND (from_account = $%d OR to_account = $%d)", argNum, argNum)
		args = append(args, filters.Address)
		argNum++
	}

	if filters.TxHash != "" {
		whereClause += fmt.Sprintf(" AND transaction_hash = $%d", argNum)
		args = append(args, filters.TxHash)
		argNum++
	}

	if filters.EventType != "" {
		switch filters.EventType {
		case "mint":
			whereClause += " AND from_account IS NULL AND to_account IS NOT NULL"
		case "burn":
			whereClause += " AND to_account IS NULL AND from_account IS NOT NULL"
		case "transfer":
			whereClause += " AND from_account IS NOT NULL AND to_account IS NOT NULL"
		}
	}

	if filters.SourceType != "" {
		whereClause += fmt.Sprintf(" AND source_type = $%d", argNum)
		args = append(args, filters.SourceType)
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

	orderDir := "DESC"
	cursorOp := "<"
	if filters.Order == "asc" {
		orderDir = "ASC"
		cursorOp = ">"
	}

	// Use inclusive comparison on transaction_hash (<=/>= instead of </>)
	// to avoid skipping events when a page break falls mid-transaction.
	// Already-seen events are filtered out in code below.
	cursorClause := ""
	if filters.Cursor != nil {
		cursorClause = fmt.Sprintf(" AND (ledger_sequence %s $%d OR (ledger_sequence = $%d AND transaction_hash %s= $%d))",
			cursorOp, argNum, argNum, cursorOp, argNum+1)
		args = append(args, filters.Cursor.LedgerSequence, filters.Cursor.TxHash)
		argNum += 2
	}

	// Request extra rows to compensate for skipped cursor-overlap events
	requestLimit := filters.Limit + 1
	if filters.Cursor != nil {
		requestLimit += filters.Cursor.EventIndex + 1
	}

	query := fmt.Sprintf(`
		WITH combined AS (
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
				operation_type,
				ROW_NUMBER() OVER (PARTITION BY transaction_hash, ledger_sequence, from_account, to_account, amount ORDER BY timestamp) as rn,
				1 as source
			FROM %s.token_transfers_raw
			%s
			UNION ALL
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
				operation_type,
				ROW_NUMBER() OVER (PARTITION BY transaction_hash, ledger_sequence, from_account, to_account, amount ORDER BY timestamp) as rn,
				2 as source
			FROM %s.token_transfers_raw
			%s
		)
		SELECT DISTINCT ON (ledger_sequence, transaction_hash, from_account, to_account, amount)
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
			operation_type
		FROM combined
		WHERE rn = 1 %s
		ORDER BY ledger_sequence %s, transaction_hash %s
		LIMIT $%d
	`, r.hotSchema, whereClause, r.coldSchema, whereClause, cursorClause, orderDir, orderDir, argNum)

	args = append(args, requestLimit)

	rows, err := r.db.QueryContext(ctx, query, args...)
	if err != nil {
		errStr := err.Error()
		if strings.Contains(errStr, "does not exist") || strings.Contains(errStr, "not found in FROM clause") {
			hotOnlyQuery := fmt.Sprintf(`
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
					operation_type
				FROM %s.token_transfers_raw
				%s %s
				ORDER BY ledger_sequence %s, transaction_hash %s
				LIMIT $%d
			`, r.hotSchema, whereClause, cursorClause, orderDir, orderDir, argNum)
			rows, err = r.db.QueryContext(ctx, hotOnlyQuery, args...)
			if err != nil {
				return nil, "", false, fmt.Errorf("hot-only fallback GetUnifiedEvents: %w", err)
			}
		} else {
			return nil, "", false, fmt.Errorf("unified GetUnifiedEvents: %w", err)
		}
	}
	defer rows.Close()

	var events []UnifiedEvent
	var lastLedger int64
	var lastTxHash string
	txEventCounter := 0
	for rows.Next() {
		var e UnifiedEvent
		var timestamp string
		var amount *int64
		if err := rows.Scan(&timestamp, &e.TxHash, &e.LedgerSequence,
			&e.SourceType, &e.From, &e.To, &e.AssetCode, &e.AssetIssuer,
			&amount, &e.ContractID, &e.OperationType); err != nil {
			return nil, "", false, err
		}

		// Track event position within each (ledger, tx) group
		if e.LedgerSequence != lastLedger || e.TxHash != lastTxHash {
			txEventCounter = 0
			lastLedger = e.LedgerSequence
			lastTxHash = e.TxHash
		}

		// Skip events already seen on the previous page
		if filters.Cursor != nil &&
			e.LedgerSequence == filters.Cursor.LedgerSequence &&
			e.TxHash == filters.Cursor.TxHash &&
			txEventCounter <= filters.Cursor.EventIndex {
			txEventCounter++
			continue
		}

		e.ClosedAt = timestamp
		e.EventIndex = txEventCounter
		txEventCounter++

		// Derive event type from from/to nullity
		switch {
		case e.From == nil && e.To != nil:
			e.EventType = "mint"
		case e.To == nil && e.From != nil:
			e.EventType = "burn"
		default:
			e.EventType = "transfer"
		}

		// Format amount from stroops
		if amount != nil {
			formatted := formatStroopsLocal(*amount)
			e.Amount = &formatted
		}

		// Generate event ID
		e.EventID = fmt.Sprintf("%d-%s-%d", e.LedgerSequence, e.TxHash[:8], e.EventIndex)

		events = append(events, e)
	}

	hasMore := len(events) > filters.Limit
	if hasMore {
		events = events[:filters.Limit]
	}

	var nextCursor string
	if len(events) > 0 && hasMore {
		last := events[len(events)-1]
		cursor := UnifiedEventCursor{
			LedgerSequence: last.LedgerSequence,
			TxHash:         last.TxHash,
			EventIndex:     last.EventIndex,
			Order:          filters.Order,
		}
		nextCursor = cursor.Encode()
	}

	return events, nextCursor, hasMore, nil
}

// GetTransactionEvents returns all events for a single transaction hash
func (r *UnifiedDuckDBReader) GetTransactionEvents(ctx context.Context, txHash string) ([]UnifiedEvent, error) {
	filters := UnifiedEventFilters{
		TxHash: txHash,
		Limit:  1000,
		Order:  "asc",
	}
	events, _, _, err := r.GetUnifiedEvents(ctx, filters)
	return events, err
}

// GetAddressEvents returns events where the address is either the sender or receiver
func (r *UnifiedDuckDBReader) GetAddressEvents(ctx context.Context, addr string, filters UnifiedEventFilters) ([]UnifiedEvent, string, bool, error) {
	filters.Address = addr
	return r.GetUnifiedEvents(ctx, filters)
}

// ============================================
// SEP-41 TOKEN QUERIES
// ============================================

// GetSEP41TokenMetadata returns metadata for a token identified by contract_id.
// Derives metadata by aggregating from token_transfers_raw.
func (r *UnifiedDuckDBReader) GetSEP41TokenMetadata(ctx context.Context, contractID string) (*SEP41TokenMetadata, error) {
	query := fmt.Sprintf(`
		WITH raw AS (
			SELECT source_type, asset_code, asset_issuer, timestamp, from_account, to_account,
				transaction_hash, ledger_sequence, amount, COALESCE(event_index, -1) as evt_idx
			FROM %s.token_transfers_raw
			WHERE token_contract_id = $1 AND transaction_successful = true
			UNION ALL
			SELECT source_type, asset_code, asset_issuer, timestamp, from_account, to_account,
				transaction_hash, ledger_sequence, amount, -1 as evt_idx
			FROM %s.token_transfers_raw
			WHERE token_contract_id = $1 AND transaction_successful = true
		),
		combined AS (
			SELECT DISTINCT ON (transaction_hash, ledger_sequence, from_account, to_account, amount, evt_idx)
				source_type, asset_code, asset_issuer, timestamp, from_account, to_account
			FROM raw
		),
		unique_addresses AS (
			SELECT from_account as address FROM combined WHERE from_account IS NOT NULL
			UNION
			SELECT to_account as address FROM combined WHERE to_account IS NOT NULL
		)
		SELECT
			COALESCE(MAX(c.asset_code), '') as asset_code,
			MAX(c.asset_issuer) as asset_issuer,
			COALESCE(MAX(c.source_type), 'soroban') as source_type,
			(SELECT COUNT(*) FROM unique_addresses) as holder_count,
			COUNT(*) as transfer_count,
			MIN(c.timestamp) as first_seen,
			MAX(c.timestamp) as last_activity
		FROM combined c
	`, r.hotSchema, r.coldSchema)

	var meta SEP41TokenMetadata
	meta.ContractID = contractID
	var assetCode string
	var firstSeen, lastActivity sql.NullString
	err := r.db.QueryRowContext(ctx, query, contractID).Scan(
		&assetCode, &meta.AssetIssuer, &meta.SourceType,
		&meta.HolderCount, &meta.TransferCount, &firstSeen, &lastActivity,
	)
	if firstSeen.Valid {
		meta.FirstSeen = firstSeen.String
	}
	if lastActivity.Valid {
		meta.LastActivity = lastActivity.String
	}
	if err != nil {
		return nil, fmt.Errorf("GetSEP41TokenMetadata: %w", err)
	}
	if assetCode != "" {
		meta.AssetCode = &assetCode
	}

	// Enrich from token_registry for name, symbol, decimals, token_type (hot + cold)
	var regName, regSymbol, regTokenType sql.NullString
	var regDecimals sql.NullInt32
	regQuery := fmt.Sprintf(`SELECT token_name, token_symbol, token_decimals, token_type FROM (
		SELECT token_name, token_symbol, token_decimals, token_type FROM %s.token_registry WHERE contract_id = $1
		UNION ALL
		SELECT token_name, token_symbol, token_decimals, token_type FROM %s.token_registry WHERE contract_id = $1
	) combined LIMIT 1`, r.hotSchema, r.coldSchema)
	regErr := r.db.QueryRowContext(ctx, regQuery, contractID).Scan(&regName, &regSymbol, &regDecimals, &regTokenType)
	if regErr == nil {
		if regName.Valid && regName.String != "" {
			meta.Name = &regName.String
		}
		if regSymbol.Valid && regSymbol.String != "" {
			meta.Symbol = &regSymbol.String
		}
		if regDecimals.Valid {
			meta.Decimals = int(regDecimals.Int32)
		} else {
			meta.Decimals = 7
		}
		if regTokenType.Valid {
			meta.TokenType = regTokenType.String
		}
	} else {
		meta.Decimals = 7 // fallback default
		if meta.AssetCode != nil {
			meta.TokenType = "sac"
		} else {
			meta.TokenType = "custom_soroban"
		}
	}

	return &meta, nil
}

// GetSEP41Balances returns computed balances for all holders of a given token contract.
// Balances are derived from transfer history: received minus sent.
func (r *UnifiedDuckDBReader) GetSEP41Balances(ctx context.Context, filters SEP41BalanceFilters) ([]SEP41Balance, string, bool, error) {
	requestLimit := filters.Limit + 1

	cursorClause := ""
	args := []interface{}{filters.ContractID}
	argNum := 2

	if filters.Cursor != nil {
		cursorClause = fmt.Sprintf(" HAVING net_balance < $%d OR (net_balance = $%d AND address > $%d)", argNum, argNum, argNum+1)
		args = append(args, filters.Cursor.Balance, filters.Cursor.Balance, filters.Cursor.Address)
		argNum += 2
	}

	if filters.MinBalance > 0 && cursorClause == "" {
		cursorClause = fmt.Sprintf(" HAVING net_balance >= $%d", argNum)
		args = append(args, filters.MinBalance)
		argNum++
	} else if filters.MinBalance > 0 {
		cursorClause += fmt.Sprintf(" AND net_balance >= $%d", argNum)
		args = append(args, filters.MinBalance)
		argNum++
	}

	query := fmt.Sprintf(`
		WITH raw_transfers AS (
			SELECT from_account, to_account, amount, ledger_sequence, timestamp, transaction_hash, COALESCE(event_index, -1) as evt_idx
			FROM %s.token_transfers_raw
			WHERE token_contract_id = $1 AND transaction_successful = true
			UNION ALL
			SELECT from_account, to_account, amount, ledger_sequence, timestamp, transaction_hash, -1 as evt_idx
			FROM %s.token_transfers_raw
			WHERE token_contract_id = $1 AND transaction_successful = true
		),
		transfers AS (
			SELECT DISTINCT ON (transaction_hash, ledger_sequence, from_account, to_account, amount, evt_idx)
				from_account, to_account, amount, ledger_sequence, timestamp
			FROM raw_transfers
		),
		balances AS (
			SELECT
				address,
				SUM(received) as total_received,
				SUM(sent) as total_sent,
				SUM(received) - SUM(sent) as net_balance,
				COUNT(*) as tx_count,
				MAX(ledger_sequence) as last_ledger,
				MAX(timestamp) as last_seen
			FROM (
				SELECT to_account as address, amount as received, 0 as sent, ledger_sequence, timestamp
				FROM transfers WHERE to_account IS NOT NULL
				UNION ALL
				SELECT from_account as address, 0 as received, amount as sent, ledger_sequence, timestamp
				FROM transfers WHERE from_account IS NOT NULL
			) addr_amounts
			GROUP BY address
			%s
		)
		SELECT address, net_balance, total_received, total_sent, tx_count, last_ledger, last_seen
		FROM balances
		WHERE net_balance > 0
		ORDER BY net_balance DESC, address ASC
		LIMIT $%d
	`, r.hotSchema, r.coldSchema, cursorClause, argNum)

	args = append(args, requestLimit)

	rows, err := r.db.QueryContext(ctx, query, args...)
	if err != nil {
		errStr := err.Error()
		if strings.Contains(errStr, "does not exist") || strings.Contains(errStr, "not found in FROM clause") {
			hotOnlyQuery := fmt.Sprintf(`
				WITH transfers AS (
					SELECT from_account, to_account, amount, ledger_sequence, timestamp
					FROM %s.token_transfers_raw
					WHERE token_contract_id = $1 AND transaction_successful = true
				),
				balances AS (
					SELECT
						address,
						SUM(received) as total_received,
						SUM(sent) as total_sent,
						SUM(received) - SUM(sent) as net_balance,
						COUNT(*) as tx_count,
						MAX(ledger_sequence) as last_ledger,
						MAX(timestamp) as last_seen
					FROM (
						SELECT to_account as address, amount as received, 0 as sent, ledger_sequence, timestamp
						FROM transfers WHERE to_account IS NOT NULL
						UNION ALL
						SELECT from_account as address, 0 as received, amount as sent, ledger_sequence, timestamp
						FROM transfers WHERE from_account IS NOT NULL
					) addr_amounts
					GROUP BY address
					%s
				)
				SELECT address, net_balance, total_received, total_sent, tx_count, last_ledger, last_seen
				FROM balances
				WHERE net_balance > 0
				ORDER BY net_balance DESC, address ASC
				LIMIT $%d
			`, r.hotSchema, cursorClause, argNum)
			rows, err = r.db.QueryContext(ctx, hotOnlyQuery, args...)
			if err != nil {
				return nil, "", false, fmt.Errorf("hot-only fallback GetSEP41Balances: %w", err)
			}
		} else {
			return nil, "", false, fmt.Errorf("GetSEP41Balances: %w", err)
		}
	}
	defer rows.Close()

	var balances []SEP41Balance
	for rows.Next() {
		var b SEP41Balance
		if err := rows.Scan(&b.Address, &b.BalanceRaw, &b.Received, &b.Sent,
			&b.TxCount, &b.LastLedger, &b.LastSeen); err != nil {
			return nil, "", false, err
		}
		b.Balance = formatStroopsLocal(b.BalanceRaw)
		balances = append(balances, b)
	}

	hasMore := len(balances) > filters.Limit
	if hasMore {
		balances = balances[:filters.Limit]
	}

	var nextCursor string
	if len(balances) > 0 && hasMore {
		last := balances[len(balances)-1]
		cursor := SEP41BalanceCursor{
			Balance: last.BalanceRaw,
			Address: last.Address,
		}
		nextCursor = cursor.Encode()
	}

	return balances, nextCursor, hasMore, nil
}

// GetSEP41SingleBalance returns the computed balance for a single address on a token contract
func (r *UnifiedDuckDBReader) GetSEP41SingleBalance(ctx context.Context, contractID, address string) (*SEP41Balance, error) {
	query := fmt.Sprintf(`
		WITH raw_transfers AS (
			SELECT from_account, to_account, amount, ledger_sequence, timestamp, transaction_hash, COALESCE(event_index, -1) as evt_idx
			FROM %s.token_transfers_raw
			WHERE token_contract_id = $1 AND transaction_successful = true
				AND (from_account = $2 OR to_account = $2)
			UNION ALL
			SELECT from_account, to_account, amount, ledger_sequence, timestamp, transaction_hash, -1 as evt_idx
			FROM %s.token_transfers_raw
			WHERE token_contract_id = $1 AND transaction_successful = true
				AND (from_account = $2 OR to_account = $2)
		),
		transfers AS (
			SELECT DISTINCT ON (transaction_hash, ledger_sequence, from_account, to_account, amount, evt_idx)
				from_account, to_account, amount, ledger_sequence, timestamp
			FROM raw_transfers
		)
		SELECT
			COALESCE(SUM(CASE WHEN to_account = $2 THEN amount ELSE 0 END), 0) as total_received,
			COALESCE(SUM(CASE WHEN from_account = $2 THEN amount ELSE 0 END), 0) as total_sent,
			COUNT(*) as tx_count,
			COALESCE(MAX(ledger_sequence), 0) as last_ledger,
			COALESCE(MAX(timestamp), '1970-01-01'::timestamp) as last_seen
		FROM transfers
	`, r.hotSchema, r.coldSchema)

	var b SEP41Balance
	b.Address = address
	err := r.db.QueryRowContext(ctx, query, contractID, address).Scan(
		&b.Received, &b.Sent, &b.TxCount, &b.LastLedger, &b.LastSeen,
	)
	if err != nil {
		return nil, fmt.Errorf("GetSEP41SingleBalance: %w", err)
	}
	b.BalanceRaw = b.Received - b.Sent
	b.Balance = formatStroopsLocal(b.BalanceRaw)

	return &b, nil
}

// GetSEP41Transfers returns transfer events for a specific token contract
func (r *UnifiedDuckDBReader) GetSEP41Transfers(ctx context.Context, contractID string, filters UnifiedEventFilters) ([]UnifiedEvent, string, bool, error) {
	filters.ContractID = contractID
	return r.GetUnifiedEvents(ctx, filters)
}

// GetSEP41TokenStats returns aggregate statistics for a token contract
func (r *UnifiedDuckDBReader) GetSEP41TokenStats(ctx context.Context, contractID string) (*SEP41TokenStats, error) {
	query := fmt.Sprintf(`
		WITH raw_transfers AS (
			SELECT from_account, to_account, amount, timestamp, asset_code, transaction_hash, ledger_sequence, COALESCE(event_index, -1) as evt_idx
			FROM %s.token_transfers_raw
			WHERE token_contract_id = $1 AND transaction_successful = true
			UNION ALL
			SELECT from_account, to_account, amount, timestamp, asset_code, transaction_hash, ledger_sequence, -1 as evt_idx
			FROM %s.token_transfers_raw
			WHERE token_contract_id = $1 AND transaction_successful = true
		),
		transfers AS (
			SELECT DISTINCT ON (transaction_hash, ledger_sequence, from_account, to_account, amount, evt_idx)
				from_account, to_account, amount, timestamp, asset_code
			FROM raw_transfers
		),
		holders AS (
			SELECT address, SUM(received) - SUM(sent) as net_balance
			FROM (
				SELECT to_account as address, amount as received, 0 as sent FROM transfers WHERE to_account IS NOT NULL
				UNION ALL
				SELECT from_account as address, 0 as received, amount as sent FROM transfers WHERE from_account IS NOT NULL
			) addr_amounts
			GROUP BY address
			HAVING SUM(received) - SUM(sent) > 0
		)
		SELECT
			(SELECT COUNT(*) FROM holders) as holder_count,
			COALESCE((SELECT SUM(net_balance) FROM holders), 0) as total_supply,
			(SELECT COUNT(*) FROM transfers WHERE timestamp > NOW() - INTERVAL '24 hours') as transfers_24h,
			COALESCE((SELECT SUM(amount) FROM transfers WHERE timestamp > NOW() - INTERVAL '24 hours'), 0) as volume_24h,
			(SELECT MAX(asset_code) FROM transfers) as asset_code
		FROM (SELECT 1) dummy
	`, r.hotSchema, r.coldSchema)

	var stats SEP41TokenStats
	stats.ContractID = contractID
	err := r.db.QueryRowContext(ctx, query, contractID).Scan(
		&stats.HolderCount, &stats.TotalSupplyRaw, &stats.Transfers24h,
		&stats.Volume24hRaw, &stats.AssetCode,
	)
	if err != nil {
		return nil, fmt.Errorf("GetSEP41TokenStats: %w", err)
	}
	stats.TotalSupply = formatStroopsLocal(stats.TotalSupplyRaw)
	stats.Volume24h = formatStroopsLocal(stats.Volume24hRaw)

	// Enrich from token_registry (hot + cold)
	var regName, regSymbol, regTokenType sql.NullString
	var regDecimals sql.NullInt32
	regQuery := fmt.Sprintf(`SELECT token_name, token_symbol, token_decimals, token_type FROM (
		SELECT token_name, token_symbol, token_decimals, token_type FROM %s.token_registry WHERE contract_id = $1
		UNION ALL
		SELECT token_name, token_symbol, token_decimals, token_type FROM %s.token_registry WHERE contract_id = $1
	) combined LIMIT 1`, r.hotSchema, r.coldSchema)
	regErr := r.db.QueryRowContext(ctx, regQuery, contractID).Scan(&regName, &regSymbol, &regDecimals, &regTokenType)
	if regErr == nil {
		if regName.Valid && regName.String != "" {
			stats.Name = &regName.String
		}
		if regSymbol.Valid && regSymbol.String != "" {
			stats.Symbol = &regSymbol.String
		}
		if regDecimals.Valid {
			stats.Decimals = int(regDecimals.Int32)
		} else {
			stats.Decimals = 7
		}
		if regTokenType.Valid {
			stats.TokenType = regTokenType.String
		}
	} else {
		stats.Decimals = 7
		if stats.AssetCode != nil {
			stats.TokenType = "sac"
		} else {
			stats.TokenType = "custom_soroban"
		}
	}

	return &stats, nil
}

// GetAddressTokenPortfolio returns all token holdings for a given address
func (r *UnifiedDuckDBReader) GetAddressTokenPortfolio(ctx context.Context, address string) ([]TokenHolding, error) {
	query := fmt.Sprintf(`
		WITH raw_transfers AS (
			SELECT from_account, to_account, amount, token_contract_id, asset_code, asset_issuer, source_type, timestamp,
				transaction_hash, ledger_sequence, COALESCE(event_index, -1) as evt_idx
			FROM %s.token_transfers_raw
			WHERE (from_account = $1 OR to_account = $1) AND transaction_successful = true
			UNION ALL
			SELECT from_account, to_account, amount, token_contract_id, asset_code, asset_issuer, source_type, timestamp,
				transaction_hash, ledger_sequence, -1 as evt_idx
			FROM %s.token_transfers_raw
			WHERE (from_account = $1 OR to_account = $1) AND transaction_successful = true
		),
		transfers AS (
			SELECT DISTINCT ON (transaction_hash, ledger_sequence, from_account, to_account, amount, token_contract_id, evt_idx)
				from_account, to_account, amount, token_contract_id, asset_code, asset_issuer, source_type, timestamp
			FROM raw_transfers
		)
		SELECT
			token_contract_id,
			MAX(asset_code) as asset_code,
			MAX(asset_issuer) as asset_issuer,
			MAX(source_type) as source_type,
			SUM(CASE WHEN to_account = $1 THEN amount ELSE 0 END) -
			SUM(CASE WHEN from_account = $1 THEN amount ELSE 0 END) as net_balance,
			COUNT(*) as tx_count,
			MAX(timestamp) as last_seen
		FROM transfers
		GROUP BY token_contract_id
		HAVING SUM(CASE WHEN to_account = $1 THEN amount ELSE 0 END) -
			   SUM(CASE WHEN from_account = $1 THEN amount ELSE 0 END) > 0
		ORDER BY net_balance DESC
		LIMIT 100
	`, r.hotSchema, r.coldSchema)

	rows, err := r.db.QueryContext(ctx, query, address)
	if err != nil {
		errStr := err.Error()
		if strings.Contains(errStr, "does not exist") || strings.Contains(errStr, "not found in FROM clause") {
			hotOnlyQuery := fmt.Sprintf(`
				WITH transfers AS (
					SELECT from_account, to_account, amount, token_contract_id, asset_code, asset_issuer, source_type, timestamp
					FROM %s.token_transfers_raw
					WHERE (from_account = $1 OR to_account = $1) AND transaction_successful = true
				)
				SELECT
					token_contract_id,
					MAX(asset_code) as asset_code,
					MAX(asset_issuer) as asset_issuer,
					MAX(source_type) as source_type,
					SUM(CASE WHEN to_account = $1 THEN amount ELSE 0 END) -
					SUM(CASE WHEN from_account = $1 THEN amount ELSE 0 END) as net_balance,
					COUNT(*) as tx_count,
					MAX(timestamp) as last_seen
				FROM transfers
				GROUP BY token_contract_id
				HAVING SUM(CASE WHEN to_account = $1 THEN amount ELSE 0 END) -
					   SUM(CASE WHEN from_account = $1 THEN amount ELSE 0 END) > 0
				ORDER BY net_balance DESC
				LIMIT 100
			`, r.hotSchema)
			rows, err = r.db.QueryContext(ctx, hotOnlyQuery, address)
			if err != nil {
				return nil, fmt.Errorf("hot-only fallback GetAddressTokenPortfolio: %w", err)
			}
		} else {
			return nil, fmt.Errorf("GetAddressTokenPortfolio: %w", err)
		}
	}
	defer rows.Close()

	var holdings []TokenHolding
	for rows.Next() {
		var h TokenHolding
		if err := rows.Scan(&h.ContractID, &h.AssetCode, &h.AssetIssuer,
			&h.SourceType, &h.BalanceRaw, &h.TxCount, &h.LastSeen); err != nil {
			return nil, err
		}
		h.Balance = formatStroopsLocal(h.BalanceRaw)
		// Set defaults
		h.Decimals = 7
		if h.AssetCode != nil && *h.AssetCode != "" {
			h.TokenType = "sac"
		} else {
			h.TokenType = "custom_soroban"
		}
		holdings = append(holdings, h)
	}

	// Enrich holdings from token_registry (batch query, hot + cold)
	if len(holdings) > 0 {
		// Collect contract IDs for batch lookup
		var contractIDs []string
		idxMap := map[string][]int{} // contract_id -> indices in holdings
		for i := range holdings {
			if holdings[i].ContractID == nil {
				continue
			}
			cid := *holdings[i].ContractID
			if _, exists := idxMap[cid]; !exists {
				contractIDs = append(contractIDs, cid)
			}
			idxMap[cid] = append(idxMap[cid], i)
		}

		if len(contractIDs) > 0 {
			// Build parameterized IN clauses for hot and cold halves of UNION
			n := len(contractIDs)
			hotPlaceholders := make([]string, n)
			coldPlaceholders := make([]string, n)
			allArgs := make([]interface{}, 0, n*2)
			for i, cid := range contractIDs {
				hotPlaceholders[i] = fmt.Sprintf("$%d", i+1)
				coldPlaceholders[i] = fmt.Sprintf("$%d", n+i+1)
				allArgs = append(allArgs, cid)
			}
			// Append again for the cold half
			for _, cid := range contractIDs {
				allArgs = append(allArgs, cid)
			}
			hotInClause := strings.Join(hotPlaceholders, ", ")
			coldInClause := strings.Join(coldPlaceholders, ", ")

			batchQuery := fmt.Sprintf(`SELECT contract_id, token_name, token_symbol, token_decimals, token_type FROM (
				SELECT contract_id, token_name, token_symbol, token_decimals, token_type FROM %s.token_registry WHERE contract_id IN (%s)
				UNION ALL
				SELECT contract_id, token_name, token_symbol, token_decimals, token_type FROM %s.token_registry WHERE contract_id IN (%s)
			) combined`, r.hotSchema, hotInClause, r.coldSchema, coldInClause)

			regRows, regErr := r.db.QueryContext(ctx, batchQuery, allArgs...)
			if regErr == nil {
				defer regRows.Close()
				seen := map[string]bool{}
				for regRows.Next() {
					var cid string
					var regName, regSymbol, regTokenType sql.NullString
					var regDecimals sql.NullInt32
					if err := regRows.Scan(&cid, &regName, &regSymbol, &regDecimals, &regTokenType); err != nil {
						continue
					}
					if seen[cid] {
						continue // skip duplicates from UNION ALL
					}
					seen[cid] = true
					for _, idx := range idxMap[cid] {
						if regName.Valid && regName.String != "" {
							holdings[idx].Name = &regName.String
						}
						if regSymbol.Valid && regSymbol.String != "" {
							holdings[idx].Symbol = &regSymbol.String
						}
						if regDecimals.Valid {
							holdings[idx].Decimals = int(regDecimals.Int32)
						}
						if regTokenType.Valid {
							holdings[idx].TokenType = regTokenType.String
						}
					}
				}
			}
		}
	}

	return holdings, nil
}

// ============================================
// TRANSACTION DECODE QUERIES
// ============================================

// GetTransactionForDecode retrieves a transaction's operations and events for human-readable decoding
func (r *UnifiedDuckDBReader) GetTransactionForDecode(ctx context.Context, txHash string) (*DecodedTransaction, error) {
	// Fetch operations from enriched_history_operations
	opsQuery := fmt.Sprintf(`
		SELECT
			operation_index,
			type,
			source_account,
			contract_id,
			function_name,
			destination,
			asset_code,
			amount,
			is_soroban_op,
			tx_successful,
			tx_fee_charged,
			ledger_sequence,
			ledger_closed_at
		FROM (
			SELECT operation_index, type, source_account, contract_id, function_name,
				   destination, asset_code, amount, is_soroban_op, tx_successful,
				   tx_fee_charged, ledger_sequence, ledger_closed_at
			FROM %s.enriched_history_operations WHERE transaction_hash = $1
			UNION ALL
			SELECT operation_index, type, source_account, contract_id, function_name,
				   destination, asset_code, amount, is_soroban_op, tx_successful,
				   tx_fee_charged, ledger_sequence, ledger_closed_at
			FROM %s.enriched_history_operations WHERE transaction_hash = $1
		) combined
		ORDER BY operation_index ASC
	`, r.hotSchema, r.coldSchema)

	rows, err := r.db.QueryContext(ctx, opsQuery, txHash)
	if err != nil {
		errStr := err.Error()
		if strings.Contains(errStr, "does not exist") || strings.Contains(errStr, "not found in FROM clause") {
			hotOnlyQuery := fmt.Sprintf(`
				SELECT operation_index, type, source_account, contract_id, function_name,
					   destination, asset_code, amount, is_soroban_op, tx_successful,
					   tx_fee_charged, ledger_sequence, ledger_closed_at
				FROM %s.enriched_history_operations WHERE transaction_hash = $1
				ORDER BY operation_index ASC
			`, r.hotSchema)
			rows, err = r.db.QueryContext(ctx, hotOnlyQuery, txHash)
			if err != nil {
				return nil, fmt.Errorf("hot-only fallback GetTransactionForDecode: %w", err)
			}
		} else {
			return nil, fmt.Errorf("GetTransactionForDecode: %w", err)
		}
	}
	defer rows.Close()

	tx := &DecodedTransaction{TxHash: txHash}
	seen := make(map[int]bool)

	for rows.Next() {
		var op DecodedOperation
		var txSuccessful bool
		var txFee int64
		var ledgerSeq int64
		var closedAt *string
		var isSorobanOp *bool

		if err := rows.Scan(&op.Index, &op.Type, &op.SourceAccount,
			&op.ContractID, &op.FunctionName, &op.Destination, &op.AssetCode, &op.Amount,
			&isSorobanOp, &txSuccessful, &txFee, &ledgerSeq, &closedAt); err != nil {
			return nil, err
		}

		if isSorobanOp != nil {
			op.IsSorobanOp = *isSorobanOp
		}
		op.TypeName = operationTypeNameDecode(op.Type)

		// Deduplicate by operation_index (from UNION ALL)
		if !seen[op.Index] {
			seen[op.Index] = true
			tx.Operations = append(tx.Operations, op)
		}

		// Set tx-level fields from first row
		if tx.LedgerSeq == 0 {
			tx.Successful = txSuccessful
			tx.Fee = txFee
			tx.LedgerSeq = ledgerSeq
			if closedAt != nil {
				tx.ClosedAt = *closedAt
			}
		}
	}

	tx.OpCount = len(tx.Operations)

	// Fetch events from token_transfers_raw
	events, err := r.GetTransactionEvents(ctx, txHash)
	if err != nil {
		// Events are optional, log but don't fail
		log.Printf("Warning: failed to fetch events for tx %s: %v", txHash, err)
	} else {
		tx.Events = events
	}

	// Generate human-readable summary
	tx.Summary = GenerateTxSummary(tx.Operations, tx.Events)

	// Fetch supplementary bronze data (source_account, account_sequence, soroban resources)
	r.enrichTransactionFromBronze(ctx, tx, txHash)

	return tx, nil
}

// enrichTransactionFromBronze adds supplementary fields from bronze transactions_row_v2
func (r *UnifiedDuckDBReader) enrichTransactionFromBronze(ctx context.Context, tx *DecodedTransaction, txHash string) {
	if r.bronzeHotSchema == "" && r.bronzeColdSchema == "" {
		return
	}

	// Try bronze hot first, then cold
	schemas := []string{}
	if r.bronzeHotSchema != "" {
		schemas = append(schemas, r.bronzeHotSchema)
	}
	if r.bronzeColdSchema != "" {
		schemas = append(schemas, r.bronzeColdSchema)
	}

	for _, schema := range schemas {
		query := fmt.Sprintf(`
			SELECT source_account, account_sequence,
			       soroban_resources_instructions, soroban_resources_read_bytes,
			       soroban_resources_write_bytes
			FROM %s.transactions_row_v2
			WHERE transaction_hash = $1
			LIMIT 1
		`, schema)

		var sourceAccount sql.NullString
		var accountSequence sql.NullInt64
		var instructions, readBytes, writeBytes sql.NullInt64

		err := r.db.QueryRowContext(ctx, query, txHash).Scan(
			&sourceAccount, &accountSequence,
			&instructions, &readBytes, &writeBytes,
		)
		if err != nil {
			continue
		}

		if sourceAccount.Valid {
			tx.SourceAccount = &sourceAccount.String
		}
		if accountSequence.Valid {
			tx.AccountSequence = &accountSequence.Int64
		}
		if instructions.Valid {
			tx.SorobanResourcesInstructions = &instructions.Int64
		}
		if readBytes.Valid {
			tx.SorobanResourcesReadBytes = &readBytes.Int64
		}
		if writeBytes.Valid {
			tx.SorobanResourcesWriteBytes = &writeBytes.Int64
		}
		// If we got basic fields but soroban resources are null, try next schema
		if tx.SorobanResourcesInstructions == nil && tx.SorobanResourcesReadBytes == nil {
			continue
		}
		return
	}
}

// GetContractFunctions returns distinct function names observed for a contract
func (r *UnifiedDuckDBReader) GetContractFunctions(ctx context.Context, contractID string) ([]string, error) {
	query := fmt.Sprintf(`
		SELECT DISTINCT function_name FROM (
			SELECT function_name FROM %s.enriched_history_operations
			WHERE contract_id = $1 AND function_name IS NOT NULL
			UNION
			SELECT function_name FROM %s.enriched_history_operations
			WHERE contract_id = $1 AND function_name IS NOT NULL
		) combined
		ORDER BY function_name
	`, r.hotSchema, r.coldSchema)

	rows, err := r.db.QueryContext(ctx, query, contractID)
	if err != nil {
		errStr := err.Error()
		if strings.Contains(errStr, "does not exist") || strings.Contains(errStr, "not found in FROM clause") {
			hotOnlyQuery := fmt.Sprintf(`
				SELECT DISTINCT function_name
				FROM %s.enriched_history_operations
				WHERE contract_id = $1 AND function_name IS NOT NULL
				ORDER BY function_name
			`, r.hotSchema)
			rows, err = r.db.QueryContext(ctx, hotOnlyQuery, contractID)
			if err != nil {
				return nil, fmt.Errorf("hot-only fallback GetContractFunctions: %w", err)
			}
		} else {
			return nil, fmt.Errorf("GetContractFunctions: %w", err)
		}
	}
	defer rows.Close()

	var functions []string
	for rows.Next() {
		var fn string
		if err := rows.Scan(&fn); err != nil {
			return nil, err
		}
		functions = append(functions, fn)
	}
	return functions, nil
}

// ============================================
// BRONZE QUERY HELPERS
// ============================================

// hasBronze returns true if at least one bronze source is available
func (r *UnifiedDuckDBReader) hasBronze() bool {
	return r.bronzeHotSchema != "" || r.bronzeColdSchema != ""
}

// bronzeUnionQuery builds a UNION ALL query across available bronze hot+cold sources.
// selectCols is the column list, table is the table name (without schema prefix),
// whereClause includes "WHERE ..." or is empty.
func (r *UnifiedDuckDBReader) bronzeUnionQuery(selectCols, table, whereClause string) string {
	var parts []string
	if r.bronzeHotSchema != "" {
		parts = append(parts, fmt.Sprintf("SELECT %s FROM %s.%s %s", selectCols, r.bronzeHotSchema, table, whereClause))
	}
	if r.bronzeColdSchema != "" {
		parts = append(parts, fmt.Sprintf("SELECT %s FROM %s.%s %s", selectCols, r.bronzeColdSchema, table, whereClause))
	}
	return strings.Join(parts, " UNION ALL ")
}

// ============================================
// FEATURE 1: GENERIC CAP-67 EVENT STREAM
// ============================================

// GetGenericEvents returns generic contract events from bronze (hot + cold)
func (r *UnifiedDuckDBReader) GetGenericEvents(ctx context.Context, filters GenericEventFilters) ([]GenericEvent, string, bool, error) {
	if !r.hasBronze() {
		return nil, "", false, fmt.Errorf("no bronze database attached")
	}

	limit := filters.Limit
	if limit <= 0 {
		limit = 20
	}
	if limit > 200 {
		limit = 200
	}
	requestLimit := limit + 1

	order := "DESC"
	if filters.Order == "asc" {
		order = "ASC"
	}

	var conditions []string
	var args []any
	argIdx := 1

	if filters.ContractID != nil && *filters.ContractID != "" {
		conditions = append(conditions, fmt.Sprintf("contract_id = $%d", argIdx))
		args = append(args, *filters.ContractID)
		argIdx++
	}
	if filters.EventType != nil && *filters.EventType != "" {
		conditions = append(conditions, fmt.Sprintf("event_type = $%d", argIdx))
		args = append(args, *filters.EventType)
		argIdx++
	}
	if filters.TopicMatch != nil && *filters.TopicMatch != "" {
		conditions = append(conditions, fmt.Sprintf("topics_decoded ILIKE '%%' || $%d || '%%'", argIdx))
		args = append(args, *filters.TopicMatch)
		argIdx++
	}
	if filters.Topic0 != nil && *filters.Topic0 != "" {
		conditions = append(conditions, fmt.Sprintf("topic0_decoded = $%d", argIdx))
		args = append(args, *filters.Topic0)
		argIdx++
	}
	if filters.Topic1 != nil && *filters.Topic1 != "" {
		conditions = append(conditions, fmt.Sprintf("topic1_decoded = $%d", argIdx))
		args = append(args, *filters.Topic1)
		argIdx++
	}
	if filters.Topic2 != nil && *filters.Topic2 != "" {
		conditions = append(conditions, fmt.Sprintf("topic2_decoded = $%d", argIdx))
		args = append(args, *filters.Topic2)
		argIdx++
	}
	if filters.Topic3 != nil && *filters.Topic3 != "" {
		conditions = append(conditions, fmt.Sprintf("topic3_decoded = $%d", argIdx))
		args = append(args, *filters.Topic3)
		argIdx++
	}
	if filters.StartLedger != nil {
		conditions = append(conditions, fmt.Sprintf("ledger_sequence >= $%d", argIdx))
		args = append(args, *filters.StartLedger)
		argIdx++
	}
	if filters.EndLedger != nil {
		conditions = append(conditions, fmt.Sprintf("ledger_sequence <= $%d", argIdx))
		args = append(args, *filters.EndLedger)
		argIdx++
	}
	if filters.Cursor != nil && *filters.Cursor != "" {
		// Cursor format: "ledger_seq:event_index"
		parts := strings.SplitN(*filters.Cursor, ":", 2)
		if len(parts) == 2 {
			cursorLedger, err := strconv.ParseInt(parts[0], 10, 64)
			if err != nil {
				return nil, "", false, fmt.Errorf("invalid cursor ledger value %q: %w", parts[0], err)
			}
			cursorEvent, err := strconv.ParseInt(parts[1], 10, 64)
			if err != nil {
				return nil, "", false, fmt.Errorf("invalid cursor event_index value %q: %w", parts[1], err)
			}
			if order == "DESC" {
				conditions = append(conditions, fmt.Sprintf("(ledger_sequence < $%d OR (ledger_sequence = $%d AND event_index < $%d))", argIdx, argIdx, argIdx+1))
			} else {
				conditions = append(conditions, fmt.Sprintf("(ledger_sequence > $%d OR (ledger_sequence = $%d AND event_index > $%d))", argIdx, argIdx, argIdx+1))
			}
			args = append(args, cursorLedger, cursorEvent)
			argIdx += 2
		}
	}

	whereClause := ""
	if len(conditions) > 0 {
		whereClause = "WHERE " + strings.Join(conditions, " AND ")
	}

	selectCols := `event_id, contract_id, ledger_sequence, transaction_hash, closed_at,
		event_type, in_successful_contract_call, topics_json, topics_decoded, data_decoded,
		topic_count, operation_index, event_index,
		topic0_decoded, topic1_decoded, topic2_decoded, topic3_decoded`

	innerQuery := r.bronzeUnionQuery(selectCols, "contract_events_stream_v1", whereClause)

	query := fmt.Sprintf(`
		SELECT event_id, contract_id, ledger_sequence, transaction_hash, closed_at,
		       event_type, in_successful_contract_call, topics_json, topics_decoded, data_decoded,
		       topic_count, operation_index, event_index,
		       topic0_decoded, topic1_decoded, topic2_decoded, topic3_decoded
		FROM (%s) combined
		ORDER BY ledger_sequence %s, event_index %s
		LIMIT $%d
	`, innerQuery, order, order, argIdx)
	args = append(args, requestLimit)

	rows, err := r.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, "", false, fmt.Errorf("GetGenericEvents: %w", err)
	}
	defer rows.Close()

	var events []GenericEvent
	for rows.Next() {
		var e GenericEvent
		if err := rows.Scan(&e.EventID, &e.ContractID, &e.LedgerSeq, &e.TxHash, &e.ClosedAt,
			&e.EventType, &e.Successful, &e.TopicsJSON, &e.TopicsDecoded, &e.DataDecoded,
			&e.TopicCount, &e.OpIndex, &e.EventIndex,
			&e.Topic0Decoded, &e.Topic1Decoded, &e.Topic2Decoded, &e.Topic3Decoded); err != nil {
			return nil, "", false, err
		}
		events = append(events, e)
	}

	hasMore := len(events) > limit
	if hasMore {
		events = events[:limit]
	}

	var nextCursor string
	if len(events) > 0 && hasMore {
		last := events[len(events)-1]
		nextCursor = fmt.Sprintf("%d:%d", last.LedgerSeq, last.EventIndex)
	}

	return events, nextCursor, hasMore, nil
}

// GetContractGenericEvents returns generic events for a specific contract
func (r *UnifiedDuckDBReader) GetContractGenericEvents(ctx context.Context, contractID string, filters GenericEventFilters) ([]GenericEvent, string, bool, error) {
	filters.ContractID = &contractID
	return r.GetGenericEvents(ctx, filters)
}

// ============================================
// FEATURE 2: UNIFIED SEARCH
// ============================================

// UnifiedSearch performs a search across multiple data types
func (r *UnifiedDuckDBReader) UnifiedSearch(ctx context.Context, query string) (*SearchResults, error) {
	results := &SearchResults{
		Query:   query,
		Results: []SearchResult{},
	}

	q := strings.TrimSpace(query)
	if q == "" {
		return results, nil
	}

	queryType := detectQueryType(q)

	switch queryType {
	case "account":
		r.searchAccount(ctx, q, results)
	case "contract":
		r.searchContract(ctx, q, results)
	case "tx_hash":
		r.searchTransaction(ctx, q, results)
	case "ledger":
		r.searchLedger(ctx, q, results)
	default:
		r.searchAssetCode(ctx, q, results)
	}

	return results, nil
}

func detectQueryType(q string) string {
	if strings.HasPrefix(q, "G") && len(q) == 56 {
		return "account"
	}
	if strings.HasPrefix(q, "C") && len(q) == 56 {
		return "contract"
	}
	if len(q) == 64 {
		return "tx_hash"
	}
	if _, err := strconv.ParseInt(q, 10, 64); err == nil {
		return "ledger"
	}
	return "asset_code"
}

func (r *UnifiedDuckDBReader) searchAccount(ctx context.Context, accountID string, results *SearchResults) {
	// Check token_transfers_raw for activity (hot + cold)
	countQuery := fmt.Sprintf(`
		SELECT COUNT(*) FROM (
			SELECT 1 FROM %s.token_transfers_raw WHERE from_account = $1
			UNION ALL
			SELECT 1 FROM %s.token_transfers_raw WHERE to_account = $1
			UNION ALL
			SELECT 1 FROM %s.token_transfers_raw WHERE from_account = $1
			UNION ALL
			SELECT 1 FROM %s.token_transfers_raw WHERE to_account = $1
		) t
	`, r.hotSchema, r.hotSchema, r.coldSchema, r.coldSchema)

	var count int64
	if err := r.db.QueryRowContext(ctx, countQuery, accountID).Scan(&count); err != nil {
		count = 0
	}

	label := "Stellar Account"
	if count > 0 {
		label = fmt.Sprintf("Stellar Account (%d transfers)", count)
	}

	results.Results = append(results.Results, SearchResult{
		Type:  "account",
		ID:    accountID,
		Label: label,
		Details: map[string]any{
			"transfer_count": count,
		},
	})
}

func (r *UnifiedDuckDBReader) searchContract(ctx context.Context, contractID string, results *SearchResults) {
	// Check token_registry for name/symbol
	regQuery := fmt.Sprintf(`
		SELECT token_name, token_symbol FROM (
			SELECT token_name, token_symbol FROM %s.token_registry WHERE contract_id = $1
			UNION ALL
			SELECT token_name, token_symbol FROM %s.token_registry WHERE contract_id = $1
		) combined LIMIT 1
	`, r.hotSchema, r.coldSchema)

	var name, symbol sql.NullString
	_ = r.db.QueryRowContext(ctx, regQuery, contractID).Scan(&name, &symbol)

	label := "Smart Contract"
	if name.Valid && name.String != "" {
		label = name.String
		if symbol.Valid && symbol.String != "" {
			label = fmt.Sprintf("%s (%s)", name.String, symbol.String)
		}
	}

	details := map[string]any{}
	if name.Valid {
		details["token_name"] = name.String
	}
	if symbol.Valid {
		details["token_symbol"] = symbol.String
	}

	results.Results = append(results.Results, SearchResult{
		Type:    "contract",
		ID:      contractID,
		Label:   label,
		Details: details,
	})
}

func (r *UnifiedDuckDBReader) searchTransaction(ctx context.Context, txHash string, results *SearchResults) {
	// Check enriched_history_operations for tx existence
	txQuery := fmt.Sprintf(`
		SELECT ledger_sequence, COUNT(*) as op_count FROM (
			SELECT ledger_sequence FROM %s.enriched_history_operations WHERE transaction_hash = $1
			UNION ALL
			SELECT ledger_sequence FROM %s.enriched_history_operations WHERE transaction_hash = $1
		) combined
		GROUP BY ledger_sequence
		LIMIT 1
	`, r.hotSchema, r.coldSchema)

	var ledgerSeq int64
	var opCount int64
	if err := r.db.QueryRowContext(ctx, txQuery, txHash).Scan(&ledgerSeq, &opCount); err != nil {
		// Not found - still return as potential result
		results.Results = append(results.Results, SearchResult{
			Type:  "transaction",
			ID:    txHash,
			Label: "Transaction (not found in current data)",
		})
		return
	}

	results.Results = append(results.Results, SearchResult{
		Type:  "transaction",
		ID:    txHash,
		Label: fmt.Sprintf("Transaction in ledger %d (%d ops)", ledgerSeq, opCount),
		Details: map[string]any{
			"ledger_sequence": ledgerSeq,
			"operation_count": opCount,
		},
	})
}

func (r *UnifiedDuckDBReader) searchLedger(ctx context.Context, ledgerStr string, results *SearchResults) {
	ledgerSeq, _ := strconv.ParseInt(ledgerStr, 10, 64)

	// Check enriched_history_operations for any tx at that sequence
	txQuery := fmt.Sprintf(`
		SELECT COUNT(DISTINCT transaction_hash) FROM (
			SELECT transaction_hash FROM %s.enriched_history_operations WHERE ledger_sequence = $1
			UNION ALL
			SELECT transaction_hash FROM %s.enriched_history_operations WHERE ledger_sequence = $1
		) combined
	`, r.hotSchema, r.coldSchema)

	var txCount int64
	if err := r.db.QueryRowContext(ctx, txQuery, ledgerSeq).Scan(&txCount); err != nil {
		txCount = 0
	}

	label := fmt.Sprintf("Ledger #%d", ledgerSeq)
	if txCount > 0 {
		label = fmt.Sprintf("Ledger #%d (%d transactions)", ledgerSeq, txCount)
	}

	results.Results = append(results.Results, SearchResult{
		Type:  "ledger",
		ID:    ledgerStr,
		Label: label,
		Details: map[string]any{
			"ledger_sequence":   ledgerSeq,
			"transaction_count": txCount,
		},
	})
}

func (r *UnifiedDuckDBReader) searchAssetCode(ctx context.Context, assetCode string, results *SearchResults) {
	// Search token_registry by symbol or name
	regQuery := fmt.Sprintf(`
		SELECT contract_id, token_name, token_symbol FROM (
			SELECT contract_id, token_name, token_symbol FROM %s.token_registry
			WHERE token_symbol ILIKE $1 OR token_name ILIKE $1
			UNION
			SELECT contract_id, token_name, token_symbol FROM %s.token_registry
			WHERE token_symbol ILIKE $1 OR token_name ILIKE $1
		) combined
		LIMIT 10
	`, r.hotSchema, r.coldSchema)

	searchTerm := "%" + assetCode + "%"
	rows, err := r.db.QueryContext(ctx, regQuery, searchTerm)
	if err != nil {
		return
	}
	defer rows.Close()

	for rows.Next() {
		var contractID string
		var name, symbol sql.NullString
		if err := rows.Scan(&contractID, &name, &symbol); err != nil {
			continue
		}
		label := "Token"
		if symbol.Valid && symbol.String != "" {
			label = symbol.String
		}
		if name.Valid && name.String != "" {
			if label != "Token" {
				label = fmt.Sprintf("%s - %s", label, name.String)
			} else {
				label = name.String
			}
		}
		results.Results = append(results.Results, SearchResult{
			Type:  "token",
			ID:    contractID,
			Label: label,
			Details: map[string]any{
				"contract_id":  contractID,
				"token_name":   name.String,
				"token_symbol": symbol.String,
			},
		})
	}

	// Also search classic assets in trustlines_current
	assetQuery := fmt.Sprintf(`
		SELECT DISTINCT asset_code, asset_issuer FROM (
			SELECT asset_code, asset_issuer FROM %s.trustlines_current
			WHERE asset_code ILIKE $1
			UNION
			SELECT asset_code, asset_issuer FROM %s.trustlines_current
			WHERE asset_code ILIKE $1
		) combined
		LIMIT 10
	`, r.hotSchema, r.coldSchema)

	rows2, err := r.db.QueryContext(ctx, assetQuery, searchTerm)
	if err != nil {
		return
	}
	defer rows2.Close()

	for rows2.Next() {
		var code string
		var issuer sql.NullString
		if err := rows2.Scan(&code, &issuer); err != nil {
			continue
		}
		issuerStr := ""
		if issuer.Valid {
			issuerStr = issuer.String
		}
		results.Results = append(results.Results, SearchResult{
			Type:  "asset",
			ID:    code + ":" + issuerStr,
			Label: fmt.Sprintf("%s (Classic Asset)", code),
			Details: map[string]any{
				"asset_code":   code,
				"asset_issuer": issuerStr,
			},
		})
	}
}

// ============================================
// FEATURE 4: TRANSACTION DIFFS (Balance/State Changes)
// ============================================

// GetTransactionDiffs retrieves tx_meta XDR and extracts balance/state changes
func (r *UnifiedDuckDBReader) GetTransactionDiffs(ctx context.Context, txHash string) (*TxDiffs, error) {
	if !r.hasBronze() {
		return nil, fmt.Errorf("no bronze database attached - tx diffs require bronze data")
	}

	// Fetch tx_meta from bronze transactions_row_v2 (hot + cold)
	innerQuery := r.bronzeUnionQuery("ledger_sequence, tx_meta", "transactions_row_v2", "WHERE transaction_hash = $1")
	query := fmt.Sprintf(`
		SELECT ledger_sequence, tx_meta
		FROM (%s) combined
		LIMIT 1
	`, innerQuery)

	var ledgerSeq int64
	var txMetaXDR sql.NullString
	err := r.db.QueryRowContext(ctx, query, txHash).Scan(&ledgerSeq, &txMetaXDR)
	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("%w: %s", ErrTxNotFound, txHash)
	}
	if err != nil {
		return nil, fmt.Errorf("GetTransactionDiffs query: %w", err)
	}

	diffs := &TxDiffs{
		TxHash:         txHash,
		LedgerSequence: ledgerSeq,
		BalanceChanges: []TxBalanceChange{},
		StateChanges:   []TxStateChange{},
	}

	if !txMetaXDR.Valid || txMetaXDR.String == "" {
		return diffs, nil
	}

	// Decode XDR and extract changes
	balanceChanges, stateChanges, err := decodeTxMetaXDR(txMetaXDR.String)
	if err != nil {
		// Return partial result with error note
		log.Printf("Warning: failed to decode tx_meta XDR for %s: %v", txHash, err)
		return diffs, nil
	}

	diffs.BalanceChanges = balanceChanges
	diffs.StateChanges = stateChanges
	return diffs, nil
}

// ============================================
// FEATURE 5: SMART WALLET DETECTION
// ============================================

// contractIDToHex converts a C... strkey contract ID to its hex-encoded 32-byte hash.
// Bronze tables store contract_id as hex hashes, while APIs receive C... strkey format.
func contractIDToHex(contractID string) (string, error) {
	raw, err := strkey.Decode(strkey.VersionByteContract, contractID)
	if err != nil {
		return "", fmt.Errorf("failed to decode contract strkey: %w", err)
	}
	return hex.EncodeToString(raw), nil
}

// GetSmartWalletInfo detects if a contract is a SEP-50 smart wallet
func (r *UnifiedDuckDBReader) GetSmartWalletInfo(ctx context.Context, contractID string) (*SmartWalletInfo, error) {
	info := &SmartWalletInfo{
		ContractID:    contractID,
		IsSmartWallet: false,
		Signers:       []string{},
	}

	// Check for __check_auth in contract events topics (bronze hot + cold)
	// Bronze stores contract_id as hex hashes, so convert from C... strkey
	if r.hasBronze() {
		hexContractID, err := contractIDToHex(contractID)
		if err != nil {
			log.Printf("Warning: could not convert contract ID to hex for bronze query: %v", err)
		} else {
			innerQuery := r.bronzeUnionQuery("1",
				"contract_events_stream_v1",
				"WHERE contract_id = $1 AND topics_decoded LIKE '%__check_auth%'")
			checkAuthQuery := fmt.Sprintf(`SELECT COUNT(*) FROM (%s) t LIMIT 1`, innerQuery)

			var checkAuthCount int64
			if err := r.db.QueryRowContext(ctx, checkAuthQuery, hexContractID).Scan(&checkAuthCount); err == nil {
				info.HasCheckAuth = checkAuthCount > 0
			}
		}
	}

	// Check contract_data for signer-related keys
	signerQuery := fmt.Sprintf(`
		SELECT data_value FROM (
			SELECT data_value FROM %s.contract_data_current WHERE contract_id = $1
				AND durability = 'instance'
			UNION ALL
			SELECT data_value FROM %s.contract_data_current WHERE contract_id = $1
				AND durability = 'instance'
		) combined
	`, r.hotSchema, r.coldSchema)

	rows, err := r.db.QueryContext(ctx, signerQuery, contractID)
	if err != nil {
		// Table may not exist, that's ok
		if !strings.Contains(err.Error(), "does not exist") {
			return nil, fmt.Errorf("GetSmartWalletInfo: %w", err)
		}
		return info, nil
	}
	defer rows.Close()

	for rows.Next() {
		var dataValue sql.NullString
		if err := rows.Scan(&dataValue); err != nil {
			continue
		}
		if dataValue.Valid {
			// Look for signer-related keys in the data value
			val := dataValue.String
			if strings.Contains(val, "Signer") || strings.Contains(val, "signer") || strings.Contains(val, "Policy") {
				info.Signers = append(info.Signers, val)
			}
		}
	}

	info.SignerCount = len(info.Signers)
	info.IsSmartWallet = info.HasCheckAuth || info.SignerCount > 0

	return info, nil
}

// ============================================
// FEATURE 3: PRICE DATA (OHLC & Latest)
// ============================================

// GetOHLCCandles returns OHLC price candles for a trading pair
func (r *UnifiedDuckDBReader) GetOHLCCandles(ctx context.Context, baseCode, baseIssuer, counterCode, counterIssuer, interval string, startTime, endTime time.Time) ([]OHLCCandle, error) {
	// Validate interval
	intervalSQL := "1 hour"
	switch interval {
	case "1m":
		intervalSQL = "1 minute"
	case "5m":
		intervalSQL = "5 minutes"
	case "15m":
		intervalSQL = "15 minutes"
	case "1h":
		intervalSQL = "1 hour"
	case "4h":
		intervalSQL = "4 hours"
	case "1d":
		intervalSQL = "1 day"
	case "1w":
		intervalSQL = "7 days"
	}

	// Use DuckDB time_bucket on silver trades (hot-only, cold may not have trades table)
	query := fmt.Sprintf(`
		SELECT
			time_bucket(INTERVAL '%s', trade_timestamp) as bucket,
			arg_min(CAST(price AS DOUBLE), trade_timestamp) as open_price,
			MAX(CAST(price AS DOUBLE)) as high_price,
			MIN(CAST(price AS DOUBLE)) as low_price,
			arg_max(CAST(price AS DOUBLE), trade_timestamp) as close_price,
			SUM(CAST(selling_amount AS DOUBLE)) as volume,
			COUNT(*) as trade_count
		FROM %s.trades
		WHERE (($1 = 'XLM' AND (selling_asset_code IS NULL OR selling_asset_code = '')) OR selling_asset_code = $1)
		  AND (($2 = 'XLM' AND (buying_asset_code IS NULL OR buying_asset_code = '')) OR buying_asset_code = $2)
		  AND ($3::text IS NULL OR $3 = '' OR selling_asset_issuer = $3)
		  AND ($4::text IS NULL OR $4 = '' OR buying_asset_issuer = $4)
		  AND trade_timestamp >= $5 AND trade_timestamp <= $6
		GROUP BY bucket
		ORDER BY bucket ASC
		LIMIT 500
	`, intervalSQL, r.hotSchema)

	rows, err := r.db.QueryContext(ctx, query, baseCode, counterCode, baseIssuer, counterIssuer, startTime, endTime)
	if err != nil {
		return nil, fmt.Errorf("GetOHLCCandles: %w", err)
	}
	defer rows.Close()

	var candles []OHLCCandle
	for rows.Next() {
		var c OHLCCandle
		var ts time.Time
		if err := rows.Scan(&ts, &c.Open, &c.High, &c.Low, &c.Close, &c.Volume, &c.TradeCount); err != nil {
			return nil, err
		}
		c.Timestamp = ts.Format(time.RFC3339)
		candles = append(candles, c)
	}

	return candles, nil
}

// GetLatestPrice returns the most recent price for a trading pair
func (r *UnifiedDuckDBReader) GetLatestPrice(ctx context.Context, baseCode, baseIssuer, counterCode, counterIssuer string) (*LatestPrice, error) {
	// Hot-only query (cold may not have trades table)
	query := fmt.Sprintf(`
		WITH latest AS (
			SELECT CAST(price AS DOUBLE) as price, trade_timestamp
			FROM %s.trades
			WHERE (($1 = 'XLM' AND (selling_asset_code IS NULL OR selling_asset_code = '')) OR selling_asset_code = $1)
			  AND (($2 = 'XLM' AND (buying_asset_code IS NULL OR buying_asset_code = '')) OR buying_asset_code = $2)
			  AND ($3::text IS NULL OR $3 = '' OR selling_asset_issuer = $3)
			  AND ($4::text IS NULL OR $4 = '' OR buying_asset_issuer = $4)
			ORDER BY trade_timestamp DESC
			LIMIT 1
		),
		stats_24h AS (
			SELECT SUM(CAST(selling_amount AS DOUBLE)) as vol, COUNT(*) as cnt
			FROM %s.trades
			WHERE (($1 = 'XLM' AND (selling_asset_code IS NULL OR selling_asset_code = '')) OR selling_asset_code = $1)
			  AND (($2 = 'XLM' AND (buying_asset_code IS NULL OR buying_asset_code = '')) OR buying_asset_code = $2)
			  AND ($3::text IS NULL OR $3 = '' OR selling_asset_issuer = $3)
			  AND ($4::text IS NULL OR $4 = '' OR buying_asset_issuer = $4)
			  AND trade_timestamp >= NOW() - INTERVAL '24 hours'
		)
		SELECT l.price, l.trade_timestamp, COALESCE(s.vol, 0), COALESCE(s.cnt, 0)
		FROM latest l, stats_24h s
	`, r.hotSchema, r.hotSchema)

	var lp LatestPrice
	var ts time.Time
	err := r.db.QueryRowContext(ctx, query, baseCode, counterCode, baseIssuer, counterIssuer).Scan(
		&lp.Price, &ts, &lp.Volume24h, &lp.TradeCount24h)
	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("no trades found for pair %s/%s", baseCode, counterCode)
	}
	if err != nil {
		return nil, fmt.Errorf("GetLatestPrice: %w", err)
	}

	lp.BaseAsset = baseCode
	lp.CounterAsset = counterCode
	lp.Timestamp = ts.Format(time.RFC3339)
	return &lp, nil
}

// GetTradePairs returns available trading pairs
func (r *UnifiedDuckDBReader) GetTradePairs(ctx context.Context) ([]TradePair, error) {
	// Hot-only query (cold may not have trades table)
	query := fmt.Sprintf(`
		SELECT COALESCE(NULLIF(selling_asset_code, ''), 'XLM') as selling_asset_code,
		       selling_asset_issuer,
		       COALESCE(NULLIF(buying_asset_code, ''), 'XLM') as buying_asset_code,
		       buying_asset_issuer,
		       COUNT(*) as trade_count
		FROM %s.trades
		GROUP BY selling_asset_code, selling_asset_issuer, buying_asset_code, buying_asset_issuer
		HAVING COUNT(*) >= 5
		ORDER BY trade_count DESC
		LIMIT 100
	`, r.hotSchema)

	rows, err := r.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("GetTradePairs: %w", err)
	}
	defer rows.Close()

	var pairs []TradePair
	for rows.Next() {
		var p TradePair
		var sellingCode, sellingIssuer, buyingCode, buyingIssuer sql.NullString
		if err := rows.Scan(&sellingCode, &sellingIssuer, &buyingCode, &buyingIssuer, &p.TradeCount); err != nil {
			return nil, err
		}
		if sellingCode.Valid {
			p.BaseAssetCode = sellingCode.String
		}
		if sellingIssuer.Valid {
			p.BaseAssetIssuer = sellingIssuer.String
		}
		if buyingCode.Valid {
			p.CounterAssetCode = buyingCode.String
		}
		if buyingIssuer.Valid {
			p.CounterAssetIssuer = buyingIssuer.String
		}
		pairs = append(pairs, p)
	}
	return pairs, nil
}

// enrichContractMetadata adds creator info from contract_metadata (hot + cold)
func (r *UnifiedDuckDBReader) enrichContractMetadata(ctx context.Context, resp *ContractMetadataResponse) {
	query := fmt.Sprintf(`
		SELECT creator_address, wasm_hash, created_ledger, created_at::text
		FROM (
			SELECT creator_address, wasm_hash, created_ledger, created_at
			FROM %s.contract_metadata WHERE contract_id = $1
			UNION ALL
			SELECT creator_address, wasm_hash, created_ledger, created_at
			FROM %s.contract_metadata WHERE contract_id = $1
		) combined
		LIMIT 1
	`, r.hotSchema, r.coldSchema)

	var creator, wasmHash, createdAt sql.NullString
	var createdLedger sql.NullInt64
	err := r.db.QueryRowContext(ctx, query, resp.ContractID).Scan(
		&creator, &wasmHash, &createdLedger, &createdAt,
	)
	if err != nil {
		return
	}
	if creator.Valid {
		resp.CreatorAddress = &creator.String
	}
	if wasmHash.Valid {
		resp.WasmHash = &wasmHash.String
	}
	if createdLedger.Valid {
		resp.CreatedLedger = &createdLedger.Int64
	}
	if createdAt.Valid {
		resp.CreatedAt = &createdAt.String
	}
}

// enrichContractFunctions adds observed function names from contract_invocations_raw (hot + cold)
func (r *UnifiedDuckDBReader) enrichContractFunctions(ctx context.Context, resp *ContractMetadataResponse) {
	query := fmt.Sprintf(`
		SELECT function_name, SUM(call_count) as total_calls
		FROM (
			SELECT function_name, COUNT(*) as call_count
			FROM %s.contract_invocations_raw
			WHERE contract_id = $1 AND function_name != ''
			GROUP BY function_name
			UNION ALL
			SELECT function_name, COUNT(*) as call_count
			FROM %s.contract_invocations_raw
			WHERE contract_id = $1 AND function_name != ''
			GROUP BY function_name
		) combined
		GROUP BY function_name
		ORDER BY total_calls DESC
	`, r.hotSchema, r.coldSchema)

	rows, err := r.db.QueryContext(ctx, query, resp.ContractID)
	if err != nil {
		return
	}
	defer rows.Close()

	for rows.Next() {
		var fc FunctionCallCount
		if err := rows.Scan(&fc.Name, &fc.CallCount); err != nil {
			break
		}
		resp.ExportedFunctions = append(resp.ExportedFunctions, fc)
	}
}
