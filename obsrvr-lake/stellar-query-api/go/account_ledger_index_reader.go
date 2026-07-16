package main

import (
	"context"
	"database/sql"
	"fmt"
	"hash/crc32"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"
)

const defaultAccountLedgerIndexBuckets = 256
const defaultAccountLedgerIndexPartitionSize = int64(100000)
const accountLedgerCoverageTTL = 30 * time.Second
const defaultAccountLedgerIndexLookupTimeout = 1500 * time.Millisecond

// AccountLedgerIndexReader reads the account_id -> ledger_range index produced
// by account-index-transformer. It shares the unified DuckDB connection so
// account history queries can prune cold Silver without opening another DuckDB.
type AccountLedgerIndexReader struct {
	db                       *sql.DB
	catalogDB                *sql.DB
	source                   string
	catalogName              string
	schemaName               string
	tableName                string
	transformerCheckpoint    string
	backfillCheckpoint       string
	postgresCheckpoint       string
	bucketCount              int
	partitionSize            int64
	canPrune                 bool
	coverageMu               sync.Mutex
	coverageCache            AccountLedgerIndexCoverage
	coverageCacheRefreshedAt time.Time
	warmup                   *OptionalWarmupStatus
}

type AccountLedgerIndexCoverage struct {
	Enabled           bool    `json:"enabled"`
	PruningEnabled    bool    `json:"pruning_enabled"`
	Status            string  `json:"status"`
	BackfillStatus    string  `json:"backfill_status,omitempty"`
	BackfillLedger    int64   `json:"backfill_ledger,omitempty"`
	IncrementalLedger int64   `json:"incremental_ledger,omitempty"`
	CoveredStart      int64   `json:"covered_start,omitempty"`
	CoveredEnd        int64   `json:"covered_end,omitempty"`
	Complete          bool    `json:"complete"`
	Used              bool    `json:"used"`
	SkippedCold       bool    `json:"skipped_cold"`
	Uncovered         bool    `json:"uncovered"`
	LookupFailed      bool    `json:"lookup_failed"`
	PrunedRanges      []int64 `json:"pruned_ranges,omitempty"`
}

func AccountLedgerBucket(accountID string, bucketCount int) int64 {
	if bucketCount <= 0 {
		bucketCount = defaultAccountLedgerIndexBuckets
	}
	return int64(crc32.ChecksumIEEE([]byte(accountID)) % uint32(bucketCount))
}

func AccountLedgerRangeForLedger(ledgerSequence, partitionSize int64) int64 {
	if partitionSize <= 0 {
		partitionSize = defaultAccountLedgerIndexPartitionSize
	}
	if ledgerSequence <= 0 {
		return 0
	}
	return ledgerSequence / partitionSize
}

func (r *UnifiedDuckDBReader) AttachAccountLedgerIndex(config IndexConfig) error {
	if r == nil || r.db == nil {
		return fmt.Errorf("unified reader is not initialized")
	}

	catalogDB, err := sql.Open("postgres", fmt.Sprintf(
		"host=%s port=%d dbname=%s user=%s password=%s sslmode=disable",
		config.CatalogHost, config.CatalogPort, config.CatalogDatabase, config.CatalogUser, config.CatalogPassword,
	))
	if err != nil {
		return fmt.Errorf("failed to open account index catalog db: %w", err)
	}
	if err := catalogDB.Ping(); err != nil {
		catalogDB.Close()
		return fmt.Errorf("failed to ping account index catalog db: %w", err)
	}

	source := accountLedgerIndexSource()
	if source == "ducklake" {
		secretSQL := fmt.Sprintf(`CREATE OR REPLACE SECRET account_index_s3_secret (
			TYPE S3,
			KEY_ID '%s',
			SECRET '%s',
			REGION '%s',
			ENDPOINT '%s',
			URL_STYLE 'path'
		)`, config.S3AccessKeyID, config.S3SecretAccessKey, config.S3Region, config.S3Endpoint)
		if _, err := r.db.Exec(secretSQL); err != nil {
			catalogDB.Close()
			return fmt.Errorf("failed to configure account index S3 credentials: %w", err)
		}

		catalogPath := fmt.Sprintf("ducklake:postgres:postgresql://%s:%s@%s:%d/%s?sslmode=disable",
			config.CatalogUser, config.CatalogPassword,
			config.CatalogHost, config.CatalogPort, config.CatalogDatabase)
		dataPath := fmt.Sprintf("s3://%s/", config.S3Bucket)
		attachSQL := fmt.Sprintf(`ATTACH '%s' AS account_index_db (DATA_PATH '%s', METADATA_SCHEMA 'index', AUTOMATIC_MIGRATION TRUE, OVERRIDE_DATA_PATH TRUE)`,
			catalogPath, dataPath)

		if _, err := r.db.Exec(attachSQL); err != nil {
			catalogDB.Close()
			return fmt.Errorf("failed to attach account index catalog: %w", err)
		}
	}

	reader := &AccountLedgerIndexReader{
		db:                    r.db,
		catalogDB:             catalogDB,
		source:                source,
		catalogName:           "account_index_db",
		schemaName:            "index",
		tableName:             "account_ledger_index",
		transformerCheckpoint: "index.account_ledger_transformer_checkpoint",
		backfillCheckpoint:    "index.account_ledger_backfill_checkpoint",
		postgresCheckpoint:    "index.account_ledger_postgres_checkpoint",
		bucketCount:           defaultAccountLedgerIndexBuckets,
		partitionSize:         accountLedgerIndexPartitionSize(),
		canPrune:              accountLedgerIndexPruningEnabled(),
	}
	warmupEnabled := reader.canPrune || accountTransactionFeedEnabled()
	reader.warmup = newOptionalWarmupStatus(warmupEnabled)
	if warmupEnabled {
		warmQuery := "SELECT 1 FROM account_index_db.index.account_ledger_index LIMIT 1"
		warmDB := r.db
		if source == "postgres" {
			warmQuery = "SELECT 1 FROM index.account_ledger_index LIMIT 1"
			warmDB = catalogDB
		}
		startOptionalWarmup("Account ledger index", true, optionalIndexWarmupTimeout, reader.warmup, func(ctx context.Context) error {
			var marker int
			return warmDB.QueryRowContext(ctx, warmQuery).Scan(&marker)
		})
	} else {
		log.Printf("Account ledger index warm-up skipped because pruning and serving feed are disabled")
	}
	if !reader.canPrune {
		log.Printf("Account ledger index attached, but pruning is disabled; set ACCOUNT_LEDGER_INDEX_ENABLE_PRUNING=true after backfill coverage is complete")
	}

	r.accountIndex = reader
	return nil
}

func accountLedgerIndexSource() string {
	value := strings.ToLower(strings.TrimSpace(envOrDefault("ACCOUNT_LEDGER_INDEX_SOURCE", "ducklake")))
	switch value {
	case "postgres", "pg":
		return "postgres"
	default:
		return "ducklake"
	}
}

func accountLedgerIndexPruningEnabled() bool {
	value := strings.TrimSpace(envOrDefault("ACCOUNT_LEDGER_INDEX_ENABLE_PRUNING", "false"))
	enabled, err := strconv.ParseBool(value)
	return err == nil && enabled
}

func (air *AccountLedgerIndexReader) CanPrune() bool {
	return air != nil && air.canPrune
}

func (air *AccountLedgerIndexReader) WarmupStatus() *OptionalWarmupStatus {
	if air == nil {
		return nil
	}
	return air.warmup
}

// initialAccountIndexCoverage seeds per-request coverage metadata before any
// index lookup runs, distinguishing "index not configured" from "configured
// but not consulted for this request".
func initialAccountIndexCoverage(idx *AccountLedgerIndexReader) AccountLedgerIndexCoverage {
	if idx == nil {
		return AccountLedgerIndexCoverage{Status: "not_configured"}
	}
	return AccountLedgerIndexCoverage{Enabled: true, PruningEnabled: idx.CanPrune(), Status: "not_used"}
}

func accountLedgerIndexPartitionSize() int64 {
	// Must match account-index-transformer index_cold.partition_size.
	// Divergence silently maps ledgers to the wrong ledger_range buckets.
	value := strings.TrimSpace(envOrDefault("ACCOUNT_LEDGER_INDEX_PARTITION_SIZE", ""))
	if value == "" {
		return defaultAccountLedgerIndexPartitionSize
	}
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil || parsed <= 0 {
		return defaultAccountLedgerIndexPartitionSize
	}
	return parsed
}

func accountLedgerIndexLookupTimeout() time.Duration {
	value := strings.TrimSpace(envOrDefault("ACCOUNT_LEDGER_INDEX_LOOKUP_TIMEOUT", ""))
	if value == "" {
		return defaultAccountLedgerIndexLookupTimeout
	}
	parsed, err := time.ParseDuration(value)
	if err != nil || parsed <= 0 {
		return defaultAccountLedgerIndexLookupTimeout
	}
	return parsed
}

func (air *AccountLedgerIndexReader) LoadCoverage(ctx context.Context) AccountLedgerIndexCoverage {
	if air == nil {
		return AccountLedgerIndexCoverage{Enabled: false, PruningEnabled: false, Status: "not_configured"}
	}
	now := time.Now()
	air.coverageMu.Lock()
	if !air.coverageCacheRefreshedAt.IsZero() && now.Sub(air.coverageCacheRefreshedAt) < accountLedgerCoverageTTL {
		cached := air.coverageCache
		air.coverageMu.Unlock()
		return cached
	}
	air.coverageMu.Unlock()

	coverage := AccountLedgerIndexCoverage{
		Enabled:        true,
		PruningEnabled: air.canPrune,
		Status:         "unknown",
	}
	if air.catalogDB == nil {
		coverage.Status = "catalog_unavailable"
		return coverage
	}
	if air.source == "postgres" {
		return air.loadPostgresCoverage(ctx, coverage, now)
	}

	queryCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	var backfillLedger sql.NullInt64
	var backfillStatus sql.NullString
	if err := air.catalogDB.QueryRowContext(queryCtx,
		fmt.Sprintf("SELECT last_ledger_sequence, status FROM %s WHERE id = 1", air.backfillCheckpoint),
	).Scan(&backfillLedger, &backfillStatus); err != nil && err != sql.ErrNoRows {
		coverage.Status = "checkpoint_error"
		return coverage
	}
	if backfillLedger.Valid {
		coverage.BackfillLedger = backfillLedger.Int64
	}
	if backfillStatus.Valid {
		coverage.BackfillStatus = strings.ToLower(strings.TrimSpace(backfillStatus.String))
	}

	var incrementalLedger sql.NullInt64
	if err := air.catalogDB.QueryRowContext(queryCtx,
		fmt.Sprintf("SELECT last_ledger_sequence FROM %s WHERE id = 1", air.transformerCheckpoint),
	).Scan(&incrementalLedger); err != nil && err != sql.ErrNoRows {
		coverage.Status = "checkpoint_error"
		return coverage
	}
	if incrementalLedger.Valid {
		coverage.IncrementalLedger = incrementalLedger.Int64
	}

	switch coverage.BackfillStatus {
	case "complete", "done":
		if coverage.IncrementalLedger > 0 {
			coverage.Status = "complete"
			coverage.Complete = true
			coverage.CoveredStart = 1
			coverage.CoveredEnd = coverage.IncrementalLedger
		} else if coverage.BackfillLedger > 0 {
			coverage.Status = "backfill_complete"
			coverage.CoveredStart = 1
			coverage.CoveredEnd = coverage.BackfillLedger
		}
	case "running":
		coverage.Status = "backfill_running"
		if coverage.BackfillLedger > 0 {
			coverage.CoveredStart = 1
			coverage.CoveredEnd = coverage.BackfillLedger
		}
	default:
		if coverage.BackfillLedger > 0 {
			coverage.Status = "partial"
			coverage.CoveredStart = 1
			coverage.CoveredEnd = coverage.BackfillLedger
		} else {
			coverage.Status = "uncovered"
		}
	}

	air.coverageMu.Lock()
	air.coverageCache = coverage
	air.coverageCacheRefreshedAt = now
	air.coverageMu.Unlock()
	return coverage
}

func (air *AccountLedgerIndexReader) loadPostgresCoverage(ctx context.Context, coverage AccountLedgerIndexCoverage, now time.Time) AccountLedgerIndexCoverage {
	queryCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	var checkpoint sql.NullInt64
	if err := air.catalogDB.QueryRowContext(queryCtx,
		fmt.Sprintf("SELECT checkpoint FROM %s WHERE sink = $1", air.postgresCheckpoint),
		fmt.Sprintf("%s.%s", air.schemaName, air.tableName),
	).Scan(&checkpoint); err != nil && err != sql.ErrNoRows {
		coverage.Status = "checkpoint_error"
		return coverage
	}
	if checkpoint.Valid {
		coverage.IncrementalLedger = checkpoint.Int64
	}

	var minFrom, maxTo sql.NullInt64
	if err := air.catalogDB.QueryRowContext(queryCtx,
		fmt.Sprintf("SELECT MIN(ledger_from), MAX(ledger_to) FROM %s.%s", air.schemaName, air.tableName),
	).Scan(&minFrom, &maxTo); err != nil {
		coverage.Status = "checkpoint_error"
		return coverage
	}
	if minFrom.Valid {
		coverage.CoveredStart = minFrom.Int64
	}
	if maxTo.Valid {
		coverage.CoveredEnd = maxTo.Int64
	}

	switch {
	case !minFrom.Valid || !maxTo.Valid:
		coverage.Status = "uncovered"
	case minFrom.Int64 <= 1 && checkpoint.Valid && maxTo.Int64 >= checkpoint.Int64:
		coverage.Status = "complete"
		coverage.BackfillStatus = "postgres_mirror_complete"
		coverage.Complete = true
		coverage.CoveredStart = 1
		coverage.CoveredEnd = checkpoint.Int64
	default:
		coverage.Status = "partial"
		coverage.BackfillStatus = "postgres_mirror_partial"
	}

	air.coverageMu.Lock()
	air.coverageCache = coverage
	air.coverageCacheRefreshedAt = now
	air.coverageMu.Unlock()
	return coverage
}

func (coverage AccountLedgerIndexCoverage) Covers(startLedger, endLedger int64) bool {
	if !coverage.Enabled || coverage.CoveredEnd <= 0 {
		return false
	}
	if startLedger <= 0 {
		startLedger = 1
	}
	if endLedger <= 0 {
		return coverage.Complete
	}
	return coverage.CoveredStart <= startLedger && coverage.CoveredEnd >= endLedger
}

func (air *AccountLedgerIndexReader) LookupLedgerRanges(ctx context.Context, accountID string, startLedger, endLedger int64) ([]int64, error) {
	if air == nil {
		return nil, nil
	}
	if air.source == "postgres" {
		return air.lookupLedgerRangesPostgres(ctx, accountID, startLedger, endLedger)
	}
	if air.db == nil {
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
		args = append(args, AccountLedgerRangeForLedger(startLedger, air.partitionSize))
	}
	if endLedger > 0 {
		query += " AND ledger_range <= ?"
		args = append(args, AccountLedgerRangeForLedger(endLedger, air.partitionSize))
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

func (air *AccountLedgerIndexReader) lookupLedgerRangesPostgres(ctx context.Context, accountID string, startLedger, endLedger int64) ([]int64, error) {
	if air.catalogDB == nil {
		return nil, nil
	}
	query := fmt.Sprintf(`
		SELECT DISTINCT ledger_range
		FROM %s.%s
		WHERE account_bucket = $1
		  AND account_id = $2
	`, air.schemaName, air.tableName)
	args := []any{AccountLedgerBucket(accountID, air.bucketCount), accountID}
	arg := 3
	if startLedger > 0 {
		query += fmt.Sprintf(" AND ledger_range >= $%d", arg)
		args = append(args, AccountLedgerRangeForLedger(startLedger, air.partitionSize))
		arg++
	}
	if endLedger > 0 {
		query += fmt.Sprintf(" AND ledger_range <= $%d", arg)
		args = append(args, AccountLedgerRangeForLedger(endLedger, air.partitionSize))
	}
	query += " ORDER BY ledger_range"

	rows, err := air.catalogDB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to query Postgres account ledger index: %w", err)
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
