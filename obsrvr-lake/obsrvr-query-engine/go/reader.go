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

// DuckLakeReader provides read access to the DuckLake catalog.
type DuckLakeReader struct {
	db     *sql.DB
	bronze string // fully qualified bronze schema, e.g. "testnet_catalog.bronze"
	silver string // fully qualified silver schema, e.g. "testnet_catalog.silver"
}

// NewDuckLakeReader opens a DuckDB connection, installs extensions,
// configures S3, and ATTACHes the DuckLake catalog.
func NewDuckLakeReader(cfg *Config) (*DuckLakeReader, error) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		return nil, fmt.Errorf("failed to open DuckDB: %w", err)
	}
	db.SetMaxOpenConns(1)

	r := &DuckLakeReader{
		db:     db,
		bronze: fmt.Sprintf("%s.%s", cfg.DuckLake.CatalogName, cfg.DuckLake.BronzeSchema),
		silver: fmt.Sprintf("%s.%s", cfg.DuckLake.CatalogName, cfg.DuckLake.SilverSchema),
	}

	if err := r.initialize(context.Background(), cfg); err != nil {
		db.Close()
		return nil, err
	}

	return r, nil
}

func (r *DuckLakeReader) initialize(ctx context.Context, cfg *Config) error {
	// Install and load extensions (ducklake from nightly, httpfs from stable)
	stmts := []string{
		"FORCE INSTALL ducklake FROM core_nightly;",
		"LOAD ducklake;",
		"INSTALL httpfs;",
		"LOAD httpfs;",
	}
	for _, s := range stmts {
		if _, err := r.db.ExecContext(ctx, s); err != nil {
			return fmt.Errorf("extension setup (%s): %w", s, err)
		}
	}

	// Configure S3 secret
	endpoint := cfg.S3.Endpoint
	endpoint = strings.TrimPrefix(endpoint, "https://")
	endpoint = strings.TrimPrefix(endpoint, "http://")

	secretSQL := fmt.Sprintf(`
		CREATE SECRET (
			TYPE S3,
			KEY_ID '%s',
			SECRET '%s',
			REGION '%s',
			ENDPOINT '%s',
			URL_STYLE 'path',
			URL_COMPATIBILITY_MODE true
		);
	`, cfg.S3.KeyID, cfg.S3.KeySecret, cfg.S3.Region, endpoint)

	if _, err := r.db.ExecContext(ctx, secretSQL); err != nil {
		return fmt.Errorf("failed to create S3 secret: %w", err)
	}

	// Attach the DuckLake catalog
	attachSQL := fmt.Sprintf(`
		ATTACH '%s'
		AS %s
		(DATA_PATH '%s', METADATA_SCHEMA '%s', AUTOMATIC_MIGRATION TRUE, OVERRIDE_DATA_PATH TRUE);
	`, cfg.DuckLake.CatalogPath, cfg.DuckLake.CatalogName, cfg.DuckLake.DataPath, cfg.DuckLake.MetadataSchema)

	if _, err := r.db.ExecContext(ctx, attachSQL); err != nil {
		return fmt.Errorf("failed to attach DuckLake catalog: %w", err)
	}

	log.Printf("DuckLake catalog %q attached (bronze=%s, silver=%s)", cfg.DuckLake.CatalogName, r.bronze, r.silver)
	return nil
}

// GetDataBoundaries returns the MIN and MAX ledger sequence from bronze.ledgers_row_v2.
func (r *DuckLakeReader) GetDataBoundaries(ctx context.Context) (*DataBoundaries, error) {
	query := fmt.Sprintf(`
		SELECT
			COALESCE(MIN(sequence), 0),
			COALESCE(MAX(sequence), 0)
		FROM %s.ledgers_row_v2
	`, r.bronze)

	var b DataBoundaries
	if err := r.db.QueryRowContext(ctx, query).Scan(&b.MinLedger, &b.MaxLedger); err != nil {
		return nil, fmt.Errorf("failed to query data boundaries: %w", err)
	}
	return &b, nil
}

// GetNetworkStats returns aggregate network statistics in the nested format
// matching stellar-query-api's BronzeNetworkStats/NetworkStats schema.
func (r *DuckLakeReader) GetNetworkStats(ctx context.Context) (map[string]interface{}, error) {
	now := time.Now().UTC().Format(time.RFC3339)

	// Ledger stats
	ledgerQuery := fmt.Sprintf(`
		SELECT
			COALESCE(MAX(sequence), 0) AS latest_sequence,
			MAX(ledger_hash) AS latest_hash,
			MAX(protocol_version) AS protocol_version,
			MAX(closed_at) AS closed_at
		FROM %s.ledgers_row_v2
		WHERE sequence = (SELECT MAX(sequence) FROM %s.ledgers_row_v2)
	`, r.bronze, r.bronze)

	ledger := map[string]interface{}{}
	var latestSeq int64
	var latestHash, closedAt sql.NullString
	var protocolVersion sql.NullInt64
	if err := r.db.QueryRowContext(ctx, ledgerQuery).Scan(&latestSeq, &latestHash, &protocolVersion, &closedAt); err == nil {
		ledger["latest_sequence"] = latestSeq
		if latestHash.Valid {
			ledger["latest_hash"] = latestHash.String
		}
		if protocolVersion.Valid {
			ledger["protocol_version"] = protocolVersion.Int64
		}
		if closedAt.Valid {
			ledger["closed_at"] = closedAt.String
		}
	}

	// Avg close time from last 100 ledgers
	avgCloseQuery := fmt.Sprintf(`
		SELECT AVG(diff) FROM (
			SELECT EPOCH(closed_at - LAG(closed_at) OVER (ORDER BY sequence)) AS diff
			FROM %s.ledgers_row_v2
			ORDER BY sequence DESC LIMIT 100
		) sub WHERE diff IS NOT NULL AND diff > 0
	`, r.bronze)
	var avgClose sql.NullFloat64
	if err := r.db.QueryRowContext(ctx, avgCloseQuery).Scan(&avgClose); err == nil && avgClose.Valid {
		ledger["avg_close_time_seconds"] = avgClose.Float64
	}

	// Transaction stats (24h approximation — use all data if limited)
	txQuery := fmt.Sprintf(`
		SELECT
			COUNT(*) AS total,
			COUNT(*) FILTER (WHERE successful = true) AS successful,
			COUNT(*) FILTER (WHERE successful = false) AS failed,
			COUNT(*) FILTER (WHERE soroban_resources_instructions IS NOT NULL) AS soroban_count,
			COALESCE(SUM(fee_charged), 0) AS total_fees_charged
		FROM %s.transactions_row_v2
	`, r.bronze)

	tx24h := map[string]interface{}{}
	var total, successful, failed, sorobanCount, totalFees int64
	if err := r.db.QueryRowContext(ctx, txQuery).Scan(&total, &successful, &failed, &sorobanCount, &totalFees); err == nil {
		tx24h["total"] = total
		tx24h["successful"] = successful
		tx24h["failed"] = failed
		tx24h["soroban_count"] = sorobanCount
		tx24h["total_fees_charged"] = totalFees
	}

	// Operation stats
	opQuery := fmt.Sprintf(`
		SELECT
			COUNT(*) AS total,
			COUNT(*) FILTER (WHERE soroban_contract_id IS NOT NULL) AS soroban_op_count
		FROM %s.operations_row_v2
	`, r.bronze)

	ops24h := map[string]interface{}{}
	var opTotal, sorobanOps int64
	if err := r.db.QueryRowContext(ctx, opQuery).Scan(&opTotal, &sorobanOps); err == nil {
		ops24h["total"] = opTotal
		ops24h["soroban_op_count"] = sorobanOps
	}

	return map[string]interface{}{
		"generated_at":    now,
		"data_freshness":  closedAt.String,
		"ledger":          ledger,
		"transactions_24h": tx24h,
		"operations_24h":  ops24h,
	}, nil
}

// GetSilverNetworkStats returns the full silver NetworkStats schema including
// accounts, fees_24h, and soroban sections beyond what bronze provides.
func (r *DuckLakeReader) GetSilverNetworkStats(ctx context.Context) (map[string]interface{}, error) {
	// Start with bronze stats as base
	result, err := r.GetNetworkStats(ctx)
	if err != nil {
		return nil, err
	}

	// Accounts section
	accounts := map[string]interface{}{}
	accountQuery := fmt.Sprintf(`
		SELECT COUNT(DISTINCT account_id) AS total
		FROM %s.accounts_snapshot_v1
	`, r.bronze)
	var totalAccounts int64
	if err := r.db.QueryRowContext(ctx, accountQuery).Scan(&totalAccounts); err == nil {
		accounts["total"] = totalAccounts
	}
	result["accounts"] = accounts

	// Ledger section — enhance with current_sequence
	if ledger, ok := result["ledger"].(map[string]interface{}); ok {
		if seq, ok := ledger["latest_sequence"]; ok {
			ledger["current_sequence"] = seq
		}
	}

	// Operations breakdown by type
	opsQuery := fmt.Sprintf(`
		SELECT
			COUNT(*) AS total,
			COUNT(*) FILTER (WHERE type IN (1, 2, 13)) AS payments,
			COUNT(*) FILTER (WHERE type = 2) AS path_payments,
			COUNT(*) FILTER (WHERE type = 0) AS create_account,
			COUNT(*) FILTER (WHERE type = 8) AS account_merge,
			COUNT(*) FILTER (WHERE type = 6) AS change_trust,
			COUNT(*) FILTER (WHERE type IN (3, 4, 12)) AS manage_offer,
			COUNT(*) FILTER (WHERE type = 24) AS contract_invoke,
			COUNT(*) FILTER (WHERE type NOT IN (0, 1, 2, 3, 4, 6, 8, 12, 13, 24)) AS other
		FROM %s.enriched_history_operations
	`, r.silver)
	opsRows, err := r.db.QueryContext(ctx, opsQuery)
	if err == nil {
		defer opsRows.Close()
		ops, _ := scanRowsToMaps(opsRows)
		if len(ops) > 0 {
			result["operations_24h"] = ops[0]
		}
	}

	// Fees section
	feeQuery := fmt.Sprintf(`
		SELECT
			APPROX_QUANTILE(fee_charged, 0.50) AS median_stroops,
			APPROX_QUANTILE(fee_charged, 0.99) AS p99_stroops,
			COALESCE(SUM(fee_charged), 0) AS daily_total_stroops
		FROM %s.transactions_row_v2
	`, r.bronze)
	var medianFee, p99Fee sql.NullFloat64
	var dailyTotal sql.NullInt64
	if err := r.db.QueryRowContext(ctx, feeQuery).Scan(&medianFee, &p99Fee, &dailyTotal); err == nil {
		fees := map[string]interface{}{}
		if medianFee.Valid {
			fees["median_stroops"] = int64(medianFee.Float64)
		}
		if p99Fee.Valid {
			fees["p99_stroops"] = int64(p99Fee.Float64)
		}
		if dailyTotal.Valid {
			fees["daily_total_stroops"] = dailyTotal.Int64
		}
		fees["surge_active"] = medianFee.Valid && int64(medianFee.Float64) > 100
		result["fees_24h"] = fees
	}

	// Soroban section
	sorobanQuery := fmt.Sprintf(`
		SELECT
			COUNT(DISTINCT soroban_contract_id) AS active_contracts_24h,
			AVG(soroban_resources_instructions) AS avg_cpu_insns,
			COALESCE(SUM(rent_fee_charged), 0) AS rent_burned_24h_stroops
		FROM %s.transactions_row_v2
		WHERE soroban_resources_instructions IS NOT NULL
	`, r.bronze)
	var activeContracts int64
	var avgCPU sql.NullFloat64
	var rentBurned sql.NullInt64
	if err := r.db.QueryRowContext(ctx, sorobanQuery).Scan(&activeContracts, &avgCPU, &rentBurned); err == nil {
		soroban := map[string]interface{}{
			"active_contracts_24h": activeContracts,
		}
		if avgCPU.Valid {
			soroban["avg_cpu_insns"] = int64(avgCPU.Float64)
		}
		if rentBurned.Valid {
			soroban["rent_burned_24h_stroops"] = rentBurned.Int64
		}
		result["soroban"] = soroban
	}

	return result, nil
}

// GetLedgers returns ledger rows within a sequence range.
func (r *DuckLakeReader) GetLedgers(ctx context.Context, start, end int64, limit int) ([]Ledger, error) {
	if limit <= 0 || limit > 1000 {
		limit = 100
	}

	query := fmt.Sprintf(`
		SELECT
			sequence,
			ledger_hash,
			previous_ledger_hash,
			COALESCE(transaction_count, 0),
			COALESCE(operation_count, 0),
			COALESCE(successful_tx_count, 0),
			COALESCE(failed_tx_count, 0),
			closed_at,
			total_coins,
			fee_pool,
			base_fee,
			base_reserve,
			max_tx_set_size,
			protocol_version,
			COALESCE(tx_set_operation_count, 0)
		FROM %s.ledgers_row_v2
		WHERE sequence >= ? AND sequence <= ?
		ORDER BY sequence ASC
		LIMIT ?
	`, r.bronze)

	rows, err := r.db.QueryContext(ctx, query, start, end, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to query ledgers: %w", err)
	}
	defer rows.Close()

	var ledgers []Ledger
	for rows.Next() {
		var l Ledger
		if err := rows.Scan(
			&l.Sequence,
			&l.LedgerHash,
			&l.PreviousLedgerHash,
			&l.TransactionCount,
			&l.OperationCount,
			&l.SuccessfulTxCount,
			&l.FailedTxCount,
			&l.ClosedAt,
			&l.TotalCoins,
			&l.FeePool,
			&l.BaseFee,
			&l.BaseReserve,
			&l.MaxTxSetSize,
			&l.ProtocolVersion,
			&l.TxSetOperationCount,
		); err != nil {
			return nil, fmt.Errorf("failed to scan ledger row: %w", err)
		}
		ledgers = append(ledgers, l)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("ledger rows iteration error: %w", err)
	}

	return ledgers, nil
}

// GetTransactions returns transaction rows within a ledger sequence range.
func (r *DuckLakeReader) GetTransactions(ctx context.Context, start, end int64, limit int) ([]Transaction, error) {
	if limit <= 0 || limit > 1000 {
		limit = 100
	}

	query := fmt.Sprintf(`
		SELECT
			ledger_sequence,
			transaction_hash,
			source_account,
			account_sequence,
			max_fee,
			operation_count,
			successful,
			created_at
		FROM %s.transactions_row_v2
		WHERE ledger_sequence >= ? AND ledger_sequence <= ?
		ORDER BY ledger_sequence ASC, transaction_hash ASC
		LIMIT ?
	`, r.bronze)

	rows, err := r.db.QueryContext(ctx, query, start, end, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to query transactions: %w", err)
	}
	defer rows.Close()

	var txns []Transaction
	for rows.Next() {
		var t Transaction
		if err := rows.Scan(
			&t.LedgerSequence,
			&t.TransactionHash,
			&t.SourceAccount,
			&t.AccountSequence,
			&t.MaxFee,
			&t.OperationCount,
			&t.Successful,
			&t.CreatedAt,
		); err != nil {
			return nil, fmt.Errorf("failed to scan transaction row: %w", err)
		}
		txns = append(txns, t)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("transaction rows iteration error: %w", err)
	}

	return txns, nil
}

// GetEnrichedOperations returns operations from the silver enriched table with optional filters.
func (r *DuckLakeReader) GetEnrichedOperations(ctx context.Context, filters OperationFilters) ([]EnrichedOperation, error) {
	if filters.Limit <= 0 || filters.Limit > 1000 {
		filters.Limit = 100
	}

	var conditions []string
	var args []interface{}

	if filters.StartLedger > 0 {
		conditions = append(conditions, "ledger_sequence >= ?")
		args = append(args, filters.StartLedger)
	}
	if filters.EndLedger > 0 {
		conditions = append(conditions, "ledger_sequence <= ?")
		args = append(args, filters.EndLedger)
	}
	if filters.OpType != "" {
		conditions = append(conditions, "type_string = ?")
		args = append(args, filters.OpType)
	}
	if filters.Account != "" {
		conditions = append(conditions, "source_account = ?")
		args = append(args, filters.Account)
	}

	whereClause := ""
	if len(conditions) > 0 {
		whereClause = "WHERE " + strings.Join(conditions, " AND ")
	}

	args = append(args, filters.Limit)

	query := fmt.Sprintf(`
		SELECT
			ledger_sequence,
			transaction_hash,
			operation_index,
			type_string AS op_type,
			source_account,
			closed_at AS created_at
		FROM %s.enriched_history_operations
		%s
		ORDER BY ledger_sequence ASC, transaction_hash ASC, operation_index ASC
		LIMIT ?
	`, r.silver, whereClause)

	rows, err := r.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to query enriched operations: %w", err)
	}
	defer rows.Close()

	var ops []EnrichedOperation
	for rows.Next() {
		var o EnrichedOperation
		if err := rows.Scan(
			&o.LedgerSequence,
			&o.TransactionHash,
			&o.OperationIndex,
			&o.OpType,
			&o.SourceAccount,
			&o.CreatedAt,
		); err != nil {
			return nil, fmt.Errorf("failed to scan enriched operation row: %w", err)
		}
		ops = append(ops, o)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("enriched operations rows iteration error: %w", err)
	}

	return ops, nil
}

// GetTopAccounts returns the top accounts by balance from bronze.accounts_snapshot_v1.
func (r *DuckLakeReader) GetTopAccounts(ctx context.Context, limit int) ([]TopAccount, error) {
	if limit <= 0 || limit > 100 {
		limit = 20
	}

	// Use a subquery to get the latest snapshot per account, then sort by balance.
	query := fmt.Sprintf(`
		SELECT account_id, balance
		FROM (
			SELECT account_id, balance,
				ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY ledger_sequence DESC) AS rn
			FROM %s.accounts_snapshot_v1
		) sub
		WHERE rn = 1
		ORDER BY CAST(balance AS BIGINT) DESC
		LIMIT ?
	`, r.bronze)

	rows, err := r.db.QueryContext(ctx, query, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to query top accounts: %w", err)
	}
	defer rows.Close()

	var accounts []TopAccount
	for rows.Next() {
		var a TopAccount
		if err := rows.Scan(&a.AccountID, &a.Balance); err != nil {
			return nil, fmt.Errorf("failed to scan top account row: %w", err)
		}
		accounts = append(accounts, a)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("top accounts rows iteration error: %w", err)
	}

	return accounts, nil
}

// Close shuts down the DuckDB connection.
func (r *DuckLakeReader) Close() error {
	return r.db.Close()
}
