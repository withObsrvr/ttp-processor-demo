package server

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"go.uber.org/zap"
)

// Checkpoint represents the processing state for crash recovery
type Checkpoint struct {
	LastProcessedLedger uint32    `json:"last_processed_ledger"`
	Timestamp           time.Time `json:"timestamp"`
	TotalProcessed      int64     `json:"total_processed"`
	TotalBytesProcessed int64     `json:"total_bytes_processed"`
	ErrorCount          int       `json:"error_count"`
	LastError           string    `json:"last_error,omitempty"`
}

// SaveCheckpoint saves the current processing state to disk
func (s *RawLedgerServer) SaveCheckpoint(lastSeq uint32) error {
	if s.config.CheckpointPath == "" {
		return nil // Checkpointing disabled
	}

	// Ensure checkpoint directory exists
	dir := filepath.Dir(s.config.CheckpointPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create checkpoint directory: %w", err)
	}

	// Get current metrics
	s.metrics.mu.RLock()
	checkpoint := Checkpoint{
		LastProcessedLedger: lastSeq,
		Timestamp:           time.Now(),
		TotalProcessed:      s.metrics.TotalProcessed,
		TotalBytesProcessed: s.metrics.TotalBytesProcessed,
		ErrorCount:          s.metrics.ErrorCount,
	}
	if s.metrics.LastError != nil {
		checkpoint.LastError = s.metrics.LastError.Error()
	}
	s.metrics.mu.RUnlock()

	// Marshal to JSON
	data, err := json.MarshalIndent(checkpoint, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal checkpoint: %w", err)
	}

	// Write atomically using temp file + rename
	tmpPath := s.config.CheckpointPath + ".tmp"
	if err := os.WriteFile(tmpPath, data, 0644); err != nil {
		return fmt.Errorf("failed to write checkpoint file: %w", err)
	}

	if err := os.Rename(tmpPath, s.config.CheckpointPath); err != nil {
		return fmt.Errorf("failed to rename checkpoint file: %w", err)
	}

	s.logger.Debug("Checkpoint saved",
		zap.Uint32("last_sequence", lastSeq),
		zap.Int64("total_processed", checkpoint.TotalProcessed),
	)

	return nil
}

// LoadCheckpoint loads the processing state from disk
func (s *RawLedgerServer) LoadCheckpoint() (*Checkpoint, error) {
	if s.config.CheckpointPath == "" {
		return nil, nil // Checkpointing disabled
	}

	data, err := os.ReadFile(s.config.CheckpointPath)
	if err != nil {
		if os.IsNotExist(err) {
			s.logger.Info("No checkpoint file found - starting fresh")
			return nil, nil // No checkpoint exists yet
		}
		return nil, fmt.Errorf("failed to read checkpoint file: %w", err)
	}

	var checkpoint Checkpoint
	if err := json.Unmarshal(data, &checkpoint); err != nil {
		return nil, fmt.Errorf("failed to unmarshal checkpoint: %w", err)
	}

	s.logger.Info("Loaded checkpoint from disk",
		zap.Uint32("last_processed_ledger", checkpoint.LastProcessedLedger),
		zap.Time("checkpoint_time", checkpoint.Timestamp),
		zap.Int64("total_processed", checkpoint.TotalProcessed),
	)

	return &checkpoint, nil
}

// SaveCheckpointAsync saves checkpoint asynchronously to avoid blocking the stream
func (s *RawLedgerServer) SaveCheckpointAsync(lastSeq uint32) {
	go func() {
		if err := s.SaveCheckpoint(lastSeq); err != nil {
			s.logger.Warn("Failed to save checkpoint",
				zap.Error(err),
				zap.Uint32("last_sequence", lastSeq),
			)
		}
	}()
}

// GetCheckpointInfo returns current checkpoint information for health endpoint
func (s *RawLedgerServer) GetCheckpointInfo() map[string]interface{} {
	checkpoint, err := s.LoadCheckpoint()
	if err != nil {
		return map[string]interface{}{
			"enabled": s.config.CheckpointPath != "",
			"error":   err.Error(),
		}
	}

	if checkpoint == nil {
		return map[string]interface{}{
			"enabled": s.config.CheckpointPath != "",
			"exists":  false,
		}
	}

	return map[string]interface{}{
		"enabled":                s.config.CheckpointPath != "",
		"exists":                 true,
		"last_processed_ledger":  checkpoint.LastProcessedLedger,
		"checkpoint_time":        checkpoint.Timestamp.Format(time.RFC3339),
		"checkpoint_age_seconds": time.Since(checkpoint.Timestamp).Seconds(),
		"total_processed":        checkpoint.TotalProcessed,
		"total_bytes_processed":  checkpoint.TotalBytesProcessed,
	}
}
