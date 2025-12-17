package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
)

// CheckpointManager manages transformation progress checkpoints
type CheckpointManager struct {
	db     *sql.DB
	config *CheckpointConfig
	catalogName string
}

// NewCheckpointManager creates a new checkpoint manager
func NewCheckpointManager(db *sql.DB, config *CheckpointConfig, catalogName string) *CheckpointManager {
	return &CheckpointManager{
		db:     db,
		config: config,
		catalogName: catalogName,
	}
}

// InitCheckpointTable creates the checkpoint table if it doesn't exist
func (cm *CheckpointManager) InitCheckpointTable() error {
	ctx := context.Background()

	// Create checkpoint table in the Bronze schema (not Silver)
	// This tracks which ledgers have been transformed to Silver
	// Note: DuckLake doesn't support PRIMARY KEY constraints
	createTableSQL := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s.%s.%s (
			id INTEGER,
			last_ledger_sequence BIGINT,
			updated_at TIMESTAMP
		)
	`, cm.catalogName, cm.config.Schema, cm.config.Table)

	if _, err := cm.db.ExecContext(ctx, createTableSQL); err != nil {
		return fmt.Errorf("failed to create checkpoint table: %w", err)
	}

	// Insert initial row if table is empty
	insertSQL := fmt.Sprintf(`
		INSERT INTO %s.%s.%s (id, last_ledger_sequence, updated_at)
		SELECT 1, 0, CURRENT_TIMESTAMP
		WHERE NOT EXISTS (
			SELECT 1 FROM %s.%s.%s WHERE id = 1
		)
	`, cm.catalogName, cm.config.Schema, cm.config.Table,
		cm.catalogName, cm.config.Schema, cm.config.Table)

	if _, err := cm.db.ExecContext(ctx, insertSQL); err != nil {
		return fmt.Errorf("failed to initialize checkpoint: %w", err)
	}

	log.Println("✅ Checkpoint table initialized")
	return nil
}

// Load retrieves the last processed ledger sequence
func (cm *CheckpointManager) Load() (int64, error) {
	ctx := context.Background()

	query := fmt.Sprintf(
		"SELECT last_ledger_sequence FROM %s.%s.%s WHERE id = 1",
		cm.catalogName, cm.config.Schema, cm.config.Table,
	)

	var lastSeq int64
	err := cm.db.QueryRowContext(ctx, query).Scan(&lastSeq)
	if err != nil {
		return 0, fmt.Errorf("failed to load checkpoint: %w", err)
	}

	return lastSeq, nil
}

// Save updates the checkpoint with the latest processed ledger sequence
func (cm *CheckpointManager) Save(ledgerSequence int64) error {
	ctx := context.Background()

	updateSQL := fmt.Sprintf(`
		UPDATE %s.%s.%s
		SET last_ledger_sequence = %d, updated_at = CURRENT_TIMESTAMP
		WHERE id = 1
	`, cm.catalogName, cm.config.Schema, cm.config.Table, ledgerSequence)

	if _, err := cm.db.ExecContext(ctx, updateSQL); err != nil {
		return fmt.Errorf("failed to save checkpoint: %w", err)
	}

	return nil
}
