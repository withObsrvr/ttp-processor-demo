package main

import (
	"context"
	"database/sql"
	"fmt"
	"hash/crc32"
	"log"
	"strconv"
	"strings"
	"time"
)

const defaultAccountLedgerIndexBuckets = 256

// AccountLedgerIndexReader reads the account_id -> ledger_range index produced
// by account-index-transformer. It shares the unified DuckDB connection so
// account history queries can prune cold Silver without opening another DuckDB.
type AccountLedgerIndexReader struct {
	db          *sql.DB
	catalogName string
	schemaName  string
	tableName   string
	bucketCount int
	canPrune    bool
}

func AccountLedgerBucket(accountID string, bucketCount int) int64 {
	if bucketCount <= 0 {
		bucketCount = defaultAccountLedgerIndexBuckets
	}
	return int64(crc32.ChecksumIEEE([]byte(accountID)) % uint32(bucketCount))
}

func (r *UnifiedDuckDBReader) AttachAccountLedgerIndex(config IndexConfig) error {
	if r == nil || r.db == nil {
		return fmt.Errorf("unified reader is not initialized")
	}

	secretSQL := fmt.Sprintf(`CREATE OR REPLACE SECRET account_index_s3_secret (
		TYPE S3,
		KEY_ID '%s',
		SECRET '%s',
		REGION '%s',
		ENDPOINT '%s',
		URL_STYLE 'path'
	)`, config.S3AccessKeyID, config.S3SecretAccessKey, config.S3Region, config.S3Endpoint)
	if _, err := r.db.Exec(secretSQL); err != nil {
		return fmt.Errorf("failed to configure account index S3 credentials: %w", err)
	}

	catalogPath := fmt.Sprintf("ducklake:postgres:postgresql://%s:%s@%s:%d/%s?sslmode=disable",
		config.CatalogUser, config.CatalogPassword,
		config.CatalogHost, config.CatalogPort, config.CatalogDatabase)
	dataPath := fmt.Sprintf("s3://%s/", config.S3Bucket)
	attachSQL := fmt.Sprintf(`ATTACH '%s' AS account_index_db (DATA_PATH '%s', METADATA_SCHEMA 'index', AUTOMATIC_MIGRATION TRUE, OVERRIDE_DATA_PATH TRUE)`,
		catalogPath, dataPath)

	if _, err := r.db.Exec(attachSQL); err != nil {
		return fmt.Errorf("failed to attach account index catalog: %w", err)
	}

	reader := &AccountLedgerIndexReader{
		db:          r.db,
		catalogName: "account_index_db",
		schemaName:  "index",
		tableName:   "account_ledger_index",
		bucketCount: defaultAccountLedgerIndexBuckets,
		canPrune:    accountLedgerIndexPruningEnabled(),
	}

	warmCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var cnt int64
	if err := r.db.QueryRowContext(warmCtx, "SELECT COUNT(*) FROM account_index_db.index.account_ledger_index").Scan(&cnt); err != nil {
		log.Printf("Account ledger index warm-up query failed (non-fatal): %v", err)
	} else {
		log.Printf("Account ledger index reader warmed up (account_ledger_index: %d rows)", cnt)
	}
	if !reader.canPrune {
		log.Printf("Account ledger index attached, but pruning is disabled; set ACCOUNT_LEDGER_INDEX_ENABLE_PRUNING=true after backfill coverage is complete")
	}

	r.accountIndex = reader
	return nil
}

func accountLedgerIndexPruningEnabled() bool {
	value := strings.TrimSpace(envOrDefault("ACCOUNT_LEDGER_INDEX_ENABLE_PRUNING", "false"))
	enabled, err := strconv.ParseBool(value)
	return err == nil && enabled
}

func (air *AccountLedgerIndexReader) CanPrune() bool {
	return air != nil && air.canPrune
}

func (air *AccountLedgerIndexReader) LookupLedgerRanges(ctx context.Context, accountID string, startLedger, endLedger int64) ([]int64, error) {
	if air == nil || air.db == nil {
		return nil, nil
	}

	query := fmt.Sprintf(`
		SELECT DISTINCT ledger_range
		FROM %s.%s.%s
		WHERE account_bucket = ?
		  AND account_id = ?
	`, air.catalogName, air.schemaName, air.tableName)
	args := []any{AccountLedgerBucket(accountID, air.bucketCount), accountID}

	if startLedger > 0 {
		query += " AND ledger_range >= ?"
		args = append(args, startLedger/100000)
	}
	if endLedger > 0 {
		query += " AND ledger_range <= ?"
		args = append(args, endLedger/100000)
	}
	query += " ORDER BY ledger_range"

	rows, err := air.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to query account ledger index: %w", err)
	}
	defer rows.Close()

	var ranges []int64
	for rows.Next() {
		var ledgerRange int64
		if err := rows.Scan(&ledgerRange); err != nil {
			return nil, err
		}
		ranges = append(ranges, ledgerRange)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return ranges, nil
}
