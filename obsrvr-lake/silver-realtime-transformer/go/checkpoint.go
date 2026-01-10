package main

import (
	"database/sql"
	"fmt"
	"time"
)

// CheckpointManager handles checkpoint tracking for the real-time transformer
type CheckpointManager struct {
	db        *sql.DB
	tableName string
}

// NewCheckpointManager creates a new checkpoint manager
func NewCheckpointManager(db *sql.DB, tableName string) *CheckpointManager {
	return &CheckpointManager{
		db:        db,
		tableName: tableName,
	}
}

// Load retrieves the last processed ledger sequence
func (cm *CheckpointManager) Load() (int64, error) {
	var lastLedger int64

	query := fmt.Sprintf(`
		SELECT last_ledger_sequence
		FROM %s
		WHERE id = 1
	`, cm.tableName)

	err := cm.db.QueryRow(query).Scan(&lastLedger)
	if err != nil {
		if err == sql.ErrNoRows {
			return 0, nil // No checkpoint yet, start from beginning
		}
		return 0, fmt.Errorf("failed to load checkpoint: %w", err)
	}

	return lastLedger, nil
}

// Save updates the checkpoint with the last processed ledger
func (cm *CheckpointManager) Save(ledgerSequence int64) error {
	query := fmt.Sprintf(`
		UPDATE %s
		SET last_ledger_sequence = $1,
		    last_processed_at = $2
		WHERE id = 1
	`, cm.tableName)

	result, err := cm.db.Exec(query, ledgerSequence, time.Now())
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

// SaveWithTx updates the checkpoint within a transaction
func (cm *CheckpointManager) SaveWithTx(tx *sql.Tx, ledgerSequence int64) error {
	query := fmt.Sprintf(`
		UPDATE %s
		SET last_ledger_sequence = $1,
		    last_processed_at = $2
		WHERE id = 1
	`, cm.tableName)

	result, err := tx.Exec(query, ledgerSequence, time.Now())
	if err != nil {
		return fmt.Errorf("failed to save checkpoint in transaction: %w", err)
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
	`, cm.tableName)

	err = cm.db.QueryRow(query).Scan(&lastLedger, &lastProcessed)
	if err != nil {
		if err == sql.ErrNoRows {
			return 0, time.Time{}, nil
		}
		return 0, time.Time{}, fmt.Errorf("failed to get checkpoint status: %w", err)
	}

	return lastLedger, lastProcessed, nil
}
