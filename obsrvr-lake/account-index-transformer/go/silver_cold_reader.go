package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"

	_ "github.com/duckdb/duckdb-go/v2"
)

type SilverColdReader struct {
	db     *sql.DB
	config *SilverColdConfig
}

func NewSilverColdReader(config *SilverColdConfig) (*SilverColdReader, error) {
	db, err := sql.Open("duckdb", config.DuckDBPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open DuckDB: %w", err)
	}
	reader := &SilverColdReader{db: db, config: config}
	if err := reader.initialize(); err != nil {
		_ = db.Close()
		return nil, err
	}
	return reader, nil
}

func (sr *SilverColdReader) initialize() error {
	for _, stmt := range []string{
		"INSTALL ducklake",
		"LOAD ducklake",
		"INSTALL httpfs",
		"LOAD httpfs",
	} {
		if _, err := sr.db.Exec(stmt); err != nil {
			return fmt.Errorf("%s: %w", stmt, err)
		}
	}

	secretSQL := fmt.Sprintf(`CREATE SECRET IF NOT EXISTS (
		TYPE S3,
		KEY_ID '%s',
		SECRET '%s',
		REGION '%s',
		ENDPOINT '%s',
		URL_STYLE 'path'
	)`, sqlString(sr.config.S3AccessKeyID), sqlString(sr.config.S3SecretAccessKey), sqlString(sr.config.S3Region), sqlString(sr.config.S3Endpoint))
	if _, err := sr.db.Exec(secretSQL); err != nil {
		return fmt.Errorf("failed to configure S3 secret: %w", err)
	}

	catalogPath := fmt.Sprintf("ducklake:postgres:postgresql://%s:%s@%s:%d/%s?sslmode=%s",
		sr.config.CatalogUser,
		sr.config.CatalogPassword,
		sr.config.CatalogHost,
		sr.config.CatalogPort,
		sr.config.CatalogDatabase,
		sr.config.CatalogSSLMode,
	)
	attachSQL := fmt.Sprintf(`ATTACH '%s' AS %s (DATA_PATH '%s', METADATA_SCHEMA '%s', AUTOMATIC_MIGRATION TRUE, OVERRIDE_DATA_PATH TRUE)`,
		catalogPath,
		sr.config.CatalogName,
		sr.config.DataPath,
		sr.config.MetadataSchema,
	)
	if _, err := sr.db.Exec(attachSQL); err != nil {
		return fmt.Errorf("failed to attach cold Silver DuckLake catalog: %w", err)
	}
	log.Printf("✅ Cold Silver catalog attached: %s.%s", sr.config.CatalogName, sr.config.SchemaName)
	return nil
}

func (sr *SilverColdReader) GetMaxLedger(ctx context.Context) (int64, error) {
	query := fmt.Sprintf(`
		SELECT COALESCE(MAX(ledger_sequence), 0)
		FROM %s.%s.enriched_history_operations
	`, sr.config.CatalogName, sr.config.SchemaName)
	var maxLedger int64
	if err := sr.db.QueryRowContext(ctx, query).Scan(&maxLedger); err != nil {
		return 0, fmt.Errorf("failed to get cold Silver max ledger: %w", err)
	}
	return maxLedger, nil
}

func (sr *SilverColdReader) ReadAccountLedgerRanges(ctx context.Context, startLedger, endLedger, partitionSize int64, bucketCount int) ([]AccountLedgerIndex, error) {
	query := buildColdAccountLedgerRangesQuery(sr.config.CatalogName, sr.config.SchemaName, partitionSize)
	rows, err := sr.db.QueryContext(ctx, query,
		startLedger, endLedger,
		startLedger, endLedger,
		startLedger, endLedger,
		startLedger, endLedger,
		startLedger, endLedger,
	)
	if err != nil {
		return nil, fmt.Errorf("cold account ledger query: %w", err)
	}
	defer rows.Close()

	var out []AccountLedgerIndex
	for rows.Next() {
		var item AccountLedgerIndex
		if err := rows.Scan(&item.AccountID, &item.LedgerRange); err != nil {
			return nil, fmt.Errorf("scan cold account ledger row: %w", err)
		}
		item.AccountBucket = AccountBucket(item.AccountID, bucketCount)
		out = append(out, item)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	log.Printf("📊 Read %d account ledger-range rows from cold Silver (ledgers %d-%d)", len(out), startLedger, endLedger)
	return out, nil
}

func buildColdAccountLedgerRangesQuery(catalogName, schemaName string, partitionSize int64) string {
	prefix := fmt.Sprintf("%s.%s", catalogName, schemaName)
	return fmt.Sprintf(`
		WITH participants AS (
			SELECT source_account AS account_id, ledger_sequence
			FROM %s.enriched_history_operations
			WHERE ledger_sequence >= ? AND ledger_sequence <= ?
			  AND source_account IS NOT NULL AND source_account <> ''

			UNION ALL
			SELECT destination AS account_id, ledger_sequence
			FROM %s.enriched_history_operations
			WHERE ledger_sequence >= ? AND ledger_sequence <= ?
			  AND destination IS NOT NULL AND destination <> ''

			UNION ALL
			SELECT from_account AS account_id, ledger_sequence
			FROM %s.token_transfers_raw
			WHERE ledger_sequence >= ? AND ledger_sequence <= ?
			  AND from_account IS NOT NULL AND from_account <> ''

			UNION ALL
			SELECT to_account AS account_id, ledger_sequence
			FROM %s.token_transfers_raw
			WHERE ledger_sequence >= ? AND ledger_sequence <= ?
			  AND to_account IS NOT NULL AND to_account <> ''

			UNION ALL
			SELECT source_account AS account_id, ledger_sequence
			FROM %s.contract_invocations_raw
			WHERE ledger_sequence >= ? AND ledger_sequence <= ?
			  AND source_account IS NOT NULL AND source_account <> ''
		)
		SELECT DISTINCT
			account_id,
			CAST(ledger_sequence / %d AS BIGINT) AS ledger_range
		FROM participants
		ORDER BY ledger_range, account_id
	`, prefix, prefix, prefix, prefix, prefix, partitionSize)
}

func (sr *SilverColdReader) Close() error {
	if sr.db != nil {
		return sr.db.Close()
	}
	return nil
}
