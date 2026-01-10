package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"
)

// Checkpoint represents the saved state
type Checkpoint struct {
	LastLedger  int64     `json:"last_ledger"`
	LastUpdated time.Time `json:"last_updated"`
}

// CheckpointManager handles file-based checkpoint tracking
type CheckpointManager struct {
	filePath string
}

// NewCheckpointManager creates a new checkpoint manager
func NewCheckpointManager(filePath string) (*CheckpointManager, error) {
	return &CheckpointManager{
		filePath: filePath,
	}, nil
}

// Load retrieves the last processed ledger sequence from file
func (cm *CheckpointManager) Load(ctx context.Context) (int64, error) {
	// Check if file exists
	if _, err := os.Stat(cm.filePath); os.IsNotExist(err) {
		return 0, nil // No checkpoint yet, start from beginning
	}

	// Read file
	data, err := os.ReadFile(cm.filePath)
	if err != nil {
		return 0, fmt.Errorf("failed to read checkpoint file: %w", err)
	}

	// Parse JSON
	var checkpoint Checkpoint
	if err := json.Unmarshal(data, &checkpoint); err != nil {
		return 0, fmt.Errorf("failed to parse checkpoint JSON: %w", err)
	}

	return checkpoint.LastLedger, nil
}

// Save updates the checkpoint file with the last processed ledger
func (cm *CheckpointManager) Save(ctx context.Context, ledgerSequence int64) error {
	checkpoint := Checkpoint{
		LastLedger:  ledgerSequence,
		LastUpdated: time.Now().UTC(),
	}

	// Marshal to JSON
	data, err := json.MarshalIndent(checkpoint, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal checkpoint: %w", err)
	}

	// Write to file
	if err := os.WriteFile(cm.filePath, data, 0644); err != nil {
		return fmt.Errorf("failed to write checkpoint file: %w", err)
	}

	return nil
}

// GetStatus returns checkpoint status information
func (cm *CheckpointManager) GetStatus() (lastLedger int64, lastProcessed time.Time, err error) {
	// Check if file exists
	if _, err := os.Stat(cm.filePath); os.IsNotExist(err) {
		return 0, time.Time{}, nil
	}

	// Read file
	data, err := os.ReadFile(cm.filePath)
	if err != nil {
		return 0, time.Time{}, fmt.Errorf("failed to read checkpoint file: %w", err)
	}

	// Parse JSON
	var checkpoint Checkpoint
	if err := json.Unmarshal(data, &checkpoint); err != nil {
		return 0, time.Time{}, fmt.Errorf("failed to parse checkpoint JSON: %w", err)
	}

	return checkpoint.LastLedger, checkpoint.LastUpdated, nil
}

// Close closes the checkpoint manager (no-op for file-based)
func (cm *CheckpointManager) Close() error {
	return nil
}
