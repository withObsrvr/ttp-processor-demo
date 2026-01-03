package main

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	_ "github.com/duckdb/duckdb-go/v2"
	_ "github.com/lib/pq"
)

// IndexReader provides fast transaction hash lookups using Index Plane
type IndexReader struct {
	db          *sql.DB // DuckDB with DuckLake catalog attached
	catalogName string
	schemaName  string
	tableName   string
}

// TxLocation represents a transaction location in the Index Plane
type TxLocation struct {
	TxHash         string    `json:"tx_hash"`
	LedgerSequence int64     `json:"ledger_sequence"`
	OperationCount int32     `json:"operation_count"`
	Successful     bool      `json:"successful"`
	ClosedAt       time.Time `json:"closed_at"`
	LedgerRange    int64     `json:"ledger_range"`
	IndexedAt      time.Time `json:"indexed_at"`
}

// NewIndexReader creates a new Index Plane reader
func NewIndexReader(config IndexConfig) (*IndexReader, error) {
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

	// Attach DuckLake catalog - use same metadata schema as index-plane-transformer
	// IMPORTANT: Must match METADATA_SCHEMA in index-plane-transformer (use 'index' not 'index_meta')
	attachSQL := fmt.Sprintf(`ATTACH '%s' AS testnet_catalog (DATA_PATH '%s', METADATA_SCHEMA 'index')`,
		catalogPath, dataPath)

	if _, err := db.Exec(attachSQL); err != nil {
		return nil, fmt.Errorf("failed to attach catalog: %w", err)
	}

	return &IndexReader{
		db:          db,
		catalogName: "testnet_catalog",
		schemaName:  "index",
		tableName:   "tx_hash_index",
	}, nil
}

// LookupTransactionHash performs fast O(1) hash → ledger lookup
func (ir *IndexReader) LookupTransactionHash(ctx context.Context, txHash string) (*TxLocation, error) {
	// Query the DuckLake table directly
	fullTableName := fmt.Sprintf("%s.%s.%s", ir.catalogName, ir.schemaName, ir.tableName)

	query := fmt.Sprintf(`
		SELECT
			tx_hash,
			ledger_sequence,
			operation_count,
			successful,
			closed_at,
			ledger_range,
			created_at
		FROM %s
		WHERE tx_hash = ?
		LIMIT 1
	`, fullTableName)

	var location TxLocation
	err := ir.db.QueryRowContext(ctx, query, txHash).Scan(
		&location.TxHash,
		&location.LedgerSequence,
		&location.OperationCount,
		&location.Successful,
		&location.ClosedAt,
		&location.LedgerRange,
		&location.IndexedAt,
	)

	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	return &location, nil
}

// LookupTransactionHashes performs batch lookup for multiple hashes
func (ir *IndexReader) LookupTransactionHashes(ctx context.Context, txHashes []string) ([]TxLocation, error) {
	if len(txHashes) == 0 {
		return []TxLocation{}, nil
	}

	// Build hash list for WHERE IN clause
	hashList := "("
	for i, hash := range txHashes {
		if i > 0 {
			hashList += ", "
		}
		hashList += fmt.Sprintf("'%s'", hash)
	}
	hashList += ")"

	query := fmt.Sprintf(`
		SELECT
			tx_hash,
			ledger_sequence,
			operation_count,
			successful,
			closed_at,
			ledger_range,
			created_at
		FROM %s.%s.%s
		WHERE tx_hash IN %s
	`, ir.catalogName, ir.schemaName, ir.tableName, hashList)

	rows, err := ir.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query index: %w", err)
	}
	defer rows.Close()

	var results []TxLocation
	for rows.Next() {
		var location TxLocation
		if err := rows.Scan(&location.TxHash, &location.LedgerSequence,
			&location.OperationCount, &location.Successful,
			&location.ClosedAt, &location.LedgerRange, &location.IndexedAt); err != nil {
			return nil, err
		}
		results = append(results, location)
	}

	return results, nil
}

// GetIndexStats returns statistics about the Index Plane coverage
func (ir *IndexReader) GetIndexStats(ctx context.Context) (map[string]interface{}, error) {
	fullTableName := fmt.Sprintf("%s.%s.%s", ir.catalogName, ir.schemaName, ir.tableName)

	query := fmt.Sprintf(`
		SELECT
			COUNT(*) as total_transactions,
			MIN(ledger_sequence) as min_ledger,
			MAX(ledger_sequence) as max_ledger,
			MAX(created_at) as last_updated
		FROM %s
	`, fullTableName)

	var totalTx, minLedger, maxLedger int64
	var lastUpdated time.Time

	err := ir.db.QueryRowContext(ctx, query).Scan(
		&totalTx, &minLedger, &maxLedger, &lastUpdated,
	)

	if err == sql.ErrNoRows {
		return map[string]interface{}{
			"status":  "empty",
			"message": "Index Plane not yet populated",
		}, nil
	}

	if err != nil {
		return nil, fmt.Errorf("failed to get index stats: %w", err)
	}

	return map[string]interface{}{
		"total_transactions": totalTx,
		"min_ledger":         minLedger,
		"max_ledger":         maxLedger,
		"ledger_coverage":    maxLedger - minLedger + 1,
		"last_updated":       lastUpdated.Format(time.RFC3339),
	}, nil
}

// Close closes the database connection
func (ir *IndexReader) Close() error {
	if ir.db != nil {
		return ir.db.Close()
	}
	return nil
}
