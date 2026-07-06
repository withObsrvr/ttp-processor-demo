package main

import (
	"context"
	"database/sql"
	"fmt"
)

type PostgresIndexWriter struct {
	db            *sql.DB
	config        IndexPostgresConfig
	partitionSize int64
}

func NewPostgresIndexWriter(db *sql.DB, config IndexPostgresConfig, partitionSize int64) *PostgresIndexWriter {
	if partitionSize <= 0 {
		partitionSize = 100000
	}
	return &PostgresIndexWriter{
		db:            db,
		config:        config,
		partitionSize: partitionSize,
	}
}

func (w *PostgresIndexWriter) Ensure(ctx context.Context) error {
	for _, stmt := range []string{
		`CREATE SCHEMA IF NOT EXISTS ` + ident(w.config.Schema),
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
			account_id text not null,
			account_bucket integer not null,
			ledger_range bigint not null,
			ledger_from bigint not null,
			ledger_to bigint not null,
			updated_at timestamptz not null default now(),
			primary key (account_id, ledger_range)
		)`, w.indexTable()),
		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS %s ON %s (account_bucket, account_id, ledger_range)`,
			ident(w.config.Table+"_bucket_account_range_idx"), w.indexTable()),
		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS %s ON %s (account_id, ledger_to desc)`,
			ident(w.config.Table+"_account_ledger_to_idx"), w.indexTable()),
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
			sink text primary key,
			checkpoint bigint not null,
			updated_at timestamptz not null default now()
		)`, w.config.CheckpointTable),
	} {
		if _, err := w.db.ExecContext(ctx, stmt); err != nil {
			return err
		}
	}
	return nil
}

func (w *PostgresIndexWriter) LoadCheckpoint(ctx context.Context) (int64, error) {
	if err := w.Ensure(ctx); err != nil {
		return 0, err
	}
	var checkpoint int64
	err := w.db.QueryRowContext(ctx, fmt.Sprintf(`
		SELECT checkpoint
		FROM %s
		WHERE sink = $1
	`, w.config.CheckpointTable), w.indexTableName()).Scan(&checkpoint)
	if err == sql.ErrNoRows {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	return checkpoint, nil
}

func (w *PostgresIndexWriter) WriteAccountLedgerRanges(ctx context.Context, rows []AccountLedgerIndex, endLedger int64) (int64, error) {
	if err := w.Ensure(ctx); err != nil {
		return 0, err
	}
	tx, err := w.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()

	stmt, err := tx.PrepareContext(ctx, fmt.Sprintf(`
		INSERT INTO %s AS existing (
			account_id, account_bucket, ledger_range, ledger_from, ledger_to, updated_at
		) VALUES ($1, $2, $3, $4, $5, now())
		ON CONFLICT (account_id, ledger_range) DO UPDATE SET
			account_bucket = EXCLUDED.account_bucket,
			ledger_from = LEAST(existing.ledger_from, EXCLUDED.ledger_from),
			ledger_to = GREATEST(existing.ledger_to, EXCLUDED.ledger_to),
			updated_at = now()
	`, w.indexTable()))
	if err != nil {
		return 0, err
	}
	defer stmt.Close()

	seen := make(map[string]struct{}, len(rows))
	var written int64
	for _, row := range rows {
		if row.AccountID == "" {
			continue
		}
		key := fmt.Sprintf("%s|%d", row.AccountID, row.LedgerRange)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		ledgerFrom, ledgerTo := AccountLedgerRangeBounds(row.LedgerRange, w.partitionSize)
		if _, err := stmt.ExecContext(ctx,
			row.AccountID, row.AccountBucket, row.LedgerRange, ledgerFrom, ledgerTo,
		); err != nil {
			return 0, err
		}
		written++
	}
	if err := w.saveCheckpoint(ctx, tx, endLedger); err != nil {
		return 0, err
	}
	if err := tx.Commit(); err != nil {
		return 0, err
	}
	return written, nil
}

func (w *PostgresIndexWriter) saveCheckpoint(ctx context.Context, tx *sql.Tx, ledger int64) error {
	_, err := tx.ExecContext(ctx, fmt.Sprintf(`
		INSERT INTO %s AS existing (sink, checkpoint, updated_at)
		VALUES ($1, $2, now())
		ON CONFLICT (sink) DO UPDATE SET
			checkpoint = GREATEST(existing.checkpoint, EXCLUDED.checkpoint),
			updated_at = now()
	`, w.config.CheckpointTable), w.indexTableName(), ledger)
	return err
}

func (w *PostgresIndexWriter) indexTable() string {
	return qualifiedTable(w.config.Schema, w.config.Table)
}

func (w *PostgresIndexWriter) indexTableName() string {
	return w.config.Schema + "." + w.config.Table
}

func AccountLedgerRangeBounds(ledgerRange, partitionSize int64) (int64, int64) {
	if partitionSize <= 0 {
		partitionSize = 100000
	}
	ledgerFrom := ledgerRange * partitionSize
	return ledgerFrom, ledgerFrom + partitionSize - 1
}
