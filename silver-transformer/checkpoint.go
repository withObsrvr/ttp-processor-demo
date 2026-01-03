package main

import (
	"database/sql"
	"fmt"
	"log"
)

// CheckpointManager handles checkpoint persistence for resumability
type CheckpointManager struct {
	db          *sql.DB
	serviceName string
}

// NewCheckpointManager creates a new checkpoint manager
func NewCheckpointManager(db *sql.DB, serviceName string) *CheckpointManager {
	return &CheckpointManager{
		db:          db,
		serviceName: serviceName,
	}
}

// InitCheckpointTable creates the checkpoint table if it doesn't exist
func (cm *CheckpointManager) InitCheckpointTable() error {
	log.Println("📋 Initializing checkpoint table...")

	_, err := cm.db.Exec(`
		CREATE TABLE IF NOT EXISTS _checkpoint (
			service_name VARCHAR PRIMARY KEY,
			last_processed_sequence BIGINT NOT NULL,
			last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			ledgers_processed BIGINT DEFAULT 0,
			transactions_processed BIGINT DEFAULT 0,
			operations_processed BIGINT DEFAULT 0
		)
	`)

	if err != nil {
		return fmt.Errorf("failed to create checkpoint table: %w", err)
	}

	log.Println("✅ Checkpoint table ready")
	return nil
}

// Load retrieves the last processed sequence number
func (cm *CheckpointManager) Load() (int64, error) {
	var sequence int64
	err := cm.db.QueryRow(`
		SELECT last_processed_sequence
		FROM _checkpoint
		WHERE service_name = ?
	`, cm.serviceName).Scan(&sequence)

	if err == sql.ErrNoRows {
		return 0, nil // Start from beginning
	}

	return sequence, err
}

// Save updates the checkpoint with the latest sequence
func (cm *CheckpointManager) Save(sequence int64) error {
	_, err := cm.db.Exec(`
		INSERT INTO _checkpoint (
			service_name,
			last_processed_sequence
		)
		VALUES (?, ?)
		ON CONFLICT (service_name) DO UPDATE SET
			last_processed_sequence = EXCLUDED.last_processed_sequence,
			last_updated = now()
	`, cm.serviceName, sequence)

	return err
}

// UpdateStats updates processing statistics in checkpoint
func (cm *CheckpointManager) UpdateStats(ledgers, transactions, operations int64) error {
	_, err := cm.db.Exec(`
		UPDATE _checkpoint
		SET
			ledgers_processed = ledgers_processed + ?,
			transactions_processed = transactions_processed + ?,
			operations_processed = operations_processed + ?
		WHERE service_name = ?
	`, ledgers, transactions, operations, cm.serviceName)

	return err
}

// GetStats retrieves current processing statistics
func (cm *CheckpointManager) GetStats() (ledgers, transactions, operations int64, err error) {
	err = cm.db.QueryRow(`
		SELECT
			COALESCE(ledgers_processed, 0),
			COALESCE(transactions_processed, 0),
			COALESCE(operations_processed, 0)
		FROM _checkpoint
		WHERE service_name = ?
	`, cm.serviceName).Scan(&ledgers, &transactions, &operations)

	if err == sql.ErrNoRows {
		return 0, 0, 0, nil
	}

	return ledgers, transactions, operations, err
}
