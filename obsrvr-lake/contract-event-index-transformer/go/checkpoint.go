package main

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

// CheckpointManager handles PostgreSQL-backed checkpoint tracking.
type CheckpointManager struct {
	db    *sql.DB
	table string
}

// NewCheckpointManager creates a new checkpoint manager.
func NewCheckpointManager(db *sql.DB, table string) (*CheckpointManager, error) {
	cm := &CheckpointManager{db: db, table: table}
	if err := cm.ensure(context.Background()); err != nil {
		return nil, err
	}
	return cm, nil
}

func (cm *CheckpointManager) ensure(ctx context.Context) error {
	query := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id INTEGER PRIMARY KEY DEFAULT 1,
			last_ledger_sequence BIGINT NOT NULL,
			last_processed_at TIMESTAMP NOT NULL,
			transformer_version VARCHAR(50),
			CONSTRAINT single_checkpoint CHECK (id = 1)
		)
	`, cm.table)
	if _, err := cm.db.ExecContext(ctx, query); err != nil {
		return fmt.Errorf("ensure checkpoint table: %w", err)
	}

	seed := fmt.Sprintf(`
		INSERT INTO %s (id, last_ledger_sequence, last_processed_at, transformer_version)
		VALUES (1, 0, NOW(), 'v1.0.0')
		ON CONFLICT (id) DO NOTHING
	`, cm.table)
	if _, err := cm.db.ExecContext(ctx, seed); err != nil {
		return fmt.Errorf("seed checkpoint table: %w", err)
	}
	return nil
}

// Load retrieves the last processed ledger sequence.
func (cm *CheckpointManager) Load(ctx context.Context) (int64, error) {
	var lastLedger int64
	query := fmt.Sprintf(`SELECT last_ledger_sequence FROM %s WHERE id = 1`, cm.table)
	err := cm.db.QueryRowContext(ctx, query).Scan(&lastLedger)
	if err != nil {
		if err == sql.ErrNoRows {
			return 0, nil
		}
		return 0, fmt.Errorf("load checkpoint: %w", err)
	}
	return lastLedger, nil
}

// Save updates the checkpoint with the last processed ledger.
func (cm *CheckpointManager) Save(ctx context.Context, ledgerSequence int64) error {
	query := fmt.Sprintf(`
		UPDATE %s
		SET last_ledger_sequence = $1,
		    last_processed_at = $2
		WHERE id = 1
	`, cm.table)
	result, err := cm.db.ExecContext(ctx, query, ledgerSequence, time.Now().UTC())
	if err != nil {
		return fmt.Errorf("save checkpoint: %w", err)
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("save checkpoint rows affected: %w", err)
	}
	if rowsAffected == 0 {
		return fmt.Errorf("no checkpoint row found to update")
	}
	return nil
}

// GetStatus returns checkpoint status information.
func (cm *CheckpointManager) GetStatus() (lastLedger int64, lastProcessed time.Time, err error) {
	query := fmt.Sprintf(`SELECT last_ledger_sequence, last_processed_at FROM %s WHERE id = 1`, cm.table)
	err = cm.db.QueryRow(query).Scan(&lastLedger, &lastProcessed)
	if err != nil {
		if err == sql.ErrNoRows {
			return 0, time.Time{}, nil
		}
		return 0, time.Time{}, fmt.Errorf("get checkpoint status: %w", err)
	}
	return lastLedger, lastProcessed, nil
}

// Close closes the checkpoint manager (DB is owned by transformer).
func (cm *CheckpointManager) Close() error {
	return nil
}
