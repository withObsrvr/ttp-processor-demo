package main

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	_ "github.com/duckdb/duckdb-go/v2"
	_ "github.com/lib/pq"
)

// ContractIndexReader provides fast contract event lookups using Contract Event Index
type ContractIndexReader struct {
	db          *sql.DB // DuckDB with DuckLake catalog attached
	catalogName string
	schemaName  string
	tableName   string
}

// ContractLedgerInfo represents a ledger containing events from a contract
type ContractLedgerInfo struct {
	ContractID     string    `json:"contract_id"`
	LedgerSequence int64     `json:"ledger_sequence"`
	EventCount     int32     `json:"event_count"`
	FirstSeenAt    time.Time `json:"first_seen_at"`
	LedgerRange    int64     `json:"ledger_range"`
}

// NewContractIndexReader creates a new Contract Event Index reader
func NewContractIndexReader(config ContractIndexConfig) (*ContractIndexReader, error) {
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
	)`, config.S3AccessKeyID, config.S3SecretAccessKey, config.S3Region, config.S3Endpoint)

	if _, err := db.Exec(secretSQL); err != nil {
		return nil, fmt.Errorf("failed to configure S3: %w", err)
	}

	// Build catalog path (PostgreSQL connection string)
	catalogPath := fmt.Sprintf("ducklake:postgres:postgresql://%s:%s@%s:%d/%s?sslmode=require",
		config.CatalogUser, config.CatalogPassword,
		config.CatalogHost, config.CatalogPort, config.CatalogDatabase)

	// Build data path (S3 bucket)
	dataPath := fmt.Sprintf("s3://%s/", config.S3Bucket)

	// Attach DuckLake catalog - use same metadata schema as contract-event-index-transformer
	attachSQL := fmt.Sprintf(`ATTACH '%s' AS testnet_catalog (DATA_PATH '%s', METADATA_SCHEMA 'index')`,
		catalogPath, dataPath)

	if _, err := db.Exec(attachSQL); err != nil {
		return nil, fmt.Errorf("failed to attach catalog: %w", err)
	}

	return &ContractIndexReader{
		db:          db,
		catalogName: "testnet_catalog",
		schemaName:  "index",
		tableName:   "contract_events_index",
	}, nil
}

// GetLedgersForContract returns all ledgers containing events from a specific contract
func (cir *ContractIndexReader) GetLedgersForContract(ctx context.Context, contractID string, startLedger, endLedger int64, limit int) ([]int64, error) {
	fullTableName := fmt.Sprintf("%s.%s.%s", cir.catalogName, cir.schemaName, cir.tableName)

	// Build query with optional range filters
	query := fmt.Sprintf(`
		SELECT DISTINCT ledger_sequence
		FROM %s
		WHERE contract_id = ?
	`, fullTableName)

	args := []interface{}{contractID}

	if startLedger > 0 {
		query += " AND ledger_sequence >= ?"
		args = append(args, startLedger)
	}

	if endLedger > 0 {
		query += " AND ledger_sequence <= ?"
		args = append(args, endLedger)
	}

	query += " ORDER BY ledger_sequence"

	if limit > 0 {
		query += fmt.Sprintf(" LIMIT %d", limit)
	}

	rows, err := cir.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to query contract index: %w", err)
	}
	defer rows.Close()

	var ledgers []int64
	for rows.Next() {
		var ledger int64
		if err := rows.Scan(&ledger); err != nil {
			return nil, err
		}
		ledgers = append(ledgers, ledger)
	}

	if err = rows.Err(); err != nil {
		return nil, err
	}

	return ledgers, nil
}

// GetContractEventSummary returns detailed event information for a contract in a ledger range
func (cir *ContractIndexReader) GetContractEventSummary(ctx context.Context, contractID string, startLedger, endLedger int64) ([]ContractLedgerInfo, error) {
	fullTableName := fmt.Sprintf("%s.%s.%s", cir.catalogName, cir.schemaName, cir.tableName)

	query := fmt.Sprintf(`
		SELECT
			contract_id,
			ledger_sequence,
			event_count,
			first_seen_at,
			ledger_range
		FROM %s
		WHERE contract_id = ?
	`, fullTableName)

	args := []interface{}{contractID}

	if startLedger > 0 {
		query += " AND ledger_sequence >= ?"
		args = append(args, startLedger)
	}

	if endLedger > 0 {
		query += " AND ledger_sequence <= ?"
		args = append(args, endLedger)
	}

	query += " ORDER BY ledger_sequence"

	rows, err := cir.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to query contract event summary: %w", err)
	}
	defer rows.Close()

	var results []ContractLedgerInfo
	for rows.Next() {
		var info ContractLedgerInfo
		if err := rows.Scan(&info.ContractID, &info.LedgerSequence,
			&info.EventCount, &info.FirstSeenAt, &info.LedgerRange); err != nil {
			return nil, err
		}
		results = append(results, info)
	}

	if err = rows.Err(); err != nil {
		return nil, err
	}

	return results, nil
}

// GetLedgersForContracts performs batch lookup for multiple contracts
func (cir *ContractIndexReader) GetLedgersForContracts(ctx context.Context, contractIDs []string) (map[string][]int64, error) {
	if len(contractIDs) == 0 {
		return map[string][]int64{}, nil
	}

	// Build contract ID list for WHERE IN clause
	idList := "("
	for i, id := range contractIDs {
		if i > 0 {
			idList += ", "
		}
		idList += fmt.Sprintf("'%s'", id)
	}
	idList += ")"

	query := fmt.Sprintf(`
		SELECT
			contract_id,
			ledger_sequence
		FROM %s.%s.%s
		WHERE contract_id IN %s
		ORDER BY contract_id, ledger_sequence
	`, cir.catalogName, cir.schemaName, cir.tableName, idList)

	rows, err := cir.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query contract index: %w", err)
	}
	defer rows.Close()

	results := make(map[string][]int64)
	for rows.Next() {
		var contractID string
		var ledgerSeq int64
		if err := rows.Scan(&contractID, &ledgerSeq); err != nil {
			return nil, err
		}
		results[contractID] = append(results[contractID], ledgerSeq)
	}

	if err = rows.Err(); err != nil {
		return nil, err
	}

	return results, nil
}

// GetIndexStats returns statistics about the Contract Event Index coverage
func (cir *ContractIndexReader) GetIndexStats(ctx context.Context) (map[string]interface{}, error) {
	fullTableName := fmt.Sprintf("%s.%s.%s", cir.catalogName, cir.schemaName, cir.tableName)

	query := fmt.Sprintf(`
		SELECT
			COUNT(*) as total_contract_ledger_pairs,
			COUNT(DISTINCT contract_id) as unique_contracts,
			COALESCE(MIN(ledger_sequence), 0) as min_ledger,
			COALESCE(MAX(ledger_sequence), 0) as max_ledger,
			MAX(created_at) as last_updated
		FROM %s
	`, fullTableName)

	var totalPairs, uniqueContracts, minLedger, maxLedger int64
	var lastUpdated time.Time

	err := cir.db.QueryRowContext(ctx, query).Scan(
		&totalPairs, &uniqueContracts, &minLedger, &maxLedger, &lastUpdated,
	)

	if err == sql.ErrNoRows || totalPairs == 0 {
		return map[string]interface{}{
			"status":  "empty",
			"message": "Contract Event Index not yet populated",
		}, nil
	}

	if err != nil {
		return nil, fmt.Errorf("failed to get index stats: %w", err)
	}

	return map[string]interface{}{
		"total_contract_ledger_pairs": totalPairs,
		"unique_contracts":             uniqueContracts,
		"min_ledger":                   minLedger,
		"max_ledger":                   maxLedger,
		"ledger_coverage":              maxLedger - minLedger + 1,
		"last_updated":                 lastUpdated.Format(time.RFC3339),
	}, nil
}

// Close closes the database connection
func (cir *ContractIndexReader) Close() error {
	if cir.db != nil {
		return cir.db.Close()
	}
	return nil
}
