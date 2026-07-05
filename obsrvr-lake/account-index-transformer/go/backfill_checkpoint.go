package main

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

type BackfillCheckpointManager struct {
	db    *sql.DB
	table string
}

func NewBackfillCheckpointManager(db *sql.DB, config *CheckpointConfig) (*BackfillCheckpointManager, error) {
	manager := &BackfillCheckpointManager{db: db, table: config.Table}
	if err := manager.ensureTable(context.Background()); err != nil {
		return nil, err
	}
	return manager, nil
}

func (m *BackfillCheckpointManager) ensureTable(ctx context.Context) error {
	schema := "index"
	if parts := strings.Split(m.table, "."); len(parts) == 2 {
		schema = parts[0]
	}
	if _, err := m.db.ExecContext(ctx, fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s", schema)); err != nil {
		return fmt.Errorf("create backfill checkpoint schema: %w", err)
	}
	query := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id INTEGER PRIMARY KEY,
			last_ledger_sequence BIGINT NOT NULL DEFAULT 0,
			last_processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			status TEXT NOT NULL DEFAULT 'pending',
			error_text TEXT NOT NULL DEFAULT ''
		)
	`, m.table)
	if _, err := m.db.ExecContext(ctx, query); err != nil {
		return fmt.Errorf("create backfill checkpoint table: %w", err)
	}
	_, err := m.db.ExecContext(ctx, fmt.Sprintf(`
		INSERT INTO %s (id, last_ledger_sequence, status)
		VALUES (1, 0, 'pending')
		ON CONFLICT (id) DO NOTHING
	`, m.table))
	if err != nil {
		return fmt.Errorf("seed backfill checkpoint: %w", err)
	}
	return nil
}

func (m *BackfillCheckpointManager) Load(ctx context.Context) (int64, string, error) {
	query := fmt.Sprintf("SELECT last_ledger_sequence, status FROM %s WHERE id = 1", m.table)
	var ledger int64
	var status string
	if err := m.db.QueryRowContext(ctx, query).Scan(&ledger, &status); err != nil {
		return 0, "", fmt.Errorf("load backfill checkpoint: %w", err)
	}
	return ledger, status, nil
}

func (m *BackfillCheckpointManager) Save(ctx context.Context, ledger int64, status, errorText string) error {
	query := fmt.Sprintf(`
		UPDATE %s
		SET last_ledger_sequence = $1,
		    last_processed_at = NOW(),
		    updated_at = NOW(),
		    status = $2,
		    error_text = $3
		WHERE id = 1
	`, m.table)
	if _, err := m.db.ExecContext(ctx, query, ledger, status, errorText); err != nil {
		return fmt.Errorf("save backfill checkpoint: %w", err)
	}
	return nil
}
