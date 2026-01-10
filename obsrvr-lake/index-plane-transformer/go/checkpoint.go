package main

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	_ "github.com/lib/pq"
)

// CheckpointManager handles checkpoint tracking for the index plane transformer
type CheckpointManager struct {
	db     *sql.DB
	config *CheckpointConfig
}

// NewCheckpointManager creates a new checkpoint manager
func NewCheckpointManager(db *sql.DB, config *CheckpointConfig) (*CheckpointManager, error) {
	return &CheckpointManager{
		db:     db,
		config: config,
	}, nil
}

// Load retrieves the last processed ledger sequence
func (cm *CheckpointManager) Load(ctx context.Context) (int64, error) {
	var lastLedger int64

	query := fmt.Sprintf(`
		SELECT last_ledger_sequence
		FROM %s
		WHERE id = 1
	`, cm.config.Table)

	err := cm.db.QueryRowContext(ctx, query).Scan(&lastLedger)
	if err != nil {
		if err == sql.ErrNoRows {
			return 0, nil // No checkpoint yet, start from beginning
		}
		return 0, fmt.Errorf("failed to load checkpoint: %w", err)
	}

	return lastLedger, nil
}

// Save updates the checkpoint with the last processed ledger
func (cm *CheckpointManager) Save(ctx context.Context, ledgerSequence int64) error {
	query := fmt.Sprintf(`
		UPDATE %s
		SET last_ledger_sequence = $1,
		    last_processed_at = $2
		WHERE id = 1
	`, cm.config.Table)

	result, err := cm.db.ExecContext(ctx, query, ledgerSequence, time.Now())
	if err != nil {
		return fmt.Errorf("failed to save checkpoint: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to check rows affected: %w", err)
	}

	if rowsAffected == 0 {
		return fmt.Errorf("no checkpoint row found to update")
	}

	return nil
}

// GetStatus returns checkpoint status information
func (cm *CheckpointManager) GetStatus() (lastLedger int64, lastProcessed time.Time, err error) {
	query := fmt.Sprintf(`
		SELECT last_ledger_sequence, last_processed_at
		FROM %s
		WHERE id = 1
	`, cm.config.Table)

	err = cm.db.QueryRow(query).Scan(&lastLedger, &lastProcessed)
	if err != nil {
		if err == sql.ErrNoRows {
			return 0, time.Time{}, nil
		}
		return 0, time.Time{}, fmt.Errorf("failed to get checkpoint status: %w", err)
	}

	return lastLedger, lastProcessed, nil
}

// Close closes the checkpoint manager (DB is owned by main, not closed here)
func (cm *CheckpointManager) Close() error {
	// DB connection is shared, so we don't close it here
	return nil
}
