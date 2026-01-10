package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// Checkpoint tracks ingestion progress
type Checkpoint struct {
	mu               sync.RWMutex
	filePath         string
	LastLedger       uint32 `json:"last_ledger"`
	LastLedgerHash   string `json:"last_ledger_hash"`
	LastLedgerRange  uint32 `json:"last_ledger_range"`
	LastUpdateTime   string `json:"last_update_time"`
	TotalLedgers     uint64 `json:"total_ledgers"`
	TotalTransactions uint64 `json:"total_transactions"`
	TotalOperations  uint64 `json:"total_operations"`
}

// NewCheckpoint creates a new checkpoint manager
func NewCheckpoint(filePath string) (*Checkpoint, error) {
	cp := &Checkpoint{
		filePath: filePath,
	}

	// Try to load existing checkpoint
	if err := cp.Load(); err != nil {
		// If file doesn't exist, that's OK - we'll start fresh
		if !os.IsNotExist(err) {
			return nil, fmt.Errorf("failed to load checkpoint: %w", err)
		}
	}

	return cp, nil
}

// Load reads the checkpoint from disk
func (cp *Checkpoint) Load() error {
	cp.mu.Lock()
	defer cp.mu.Unlock()

	data, err := os.ReadFile(cp.filePath)
	if err != nil {
		return err
	}

	return json.Unmarshal(data, cp)
}

// Save writes the checkpoint to disk
func (cp *Checkpoint) Save() error {
	cp.mu.RLock()
	defer cp.mu.RUnlock()

	// Ensure directory exists
	dir := filepath.Dir(cp.filePath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create checkpoint directory: %w", err)
	}

	data, err := json.MarshalIndent(cp, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal checkpoint: %w", err)
	}

	// Write to temp file first, then rename (atomic)
	tempPath := cp.filePath + ".tmp"
	if err := os.WriteFile(tempPath, data, 0644); err != nil {
		return fmt.Errorf("failed to write checkpoint: %w", err)
	}

	if err := os.Rename(tempPath, cp.filePath); err != nil {
		return fmt.Errorf("failed to rename checkpoint: %w", err)
	}

	return nil
}

// Update updates the checkpoint with new values
func (cp *Checkpoint) Update(ledgerSeq uint32, ledgerHash string, ledgerRange uint32, txCount, opCount uint64) error {
	cp.mu.Lock()
	defer cp.mu.Unlock()

	cp.LastLedger = ledgerSeq
	cp.LastLedgerHash = ledgerHash
	cp.LastLedgerRange = ledgerRange
	cp.LastUpdateTime = formatTimestamp()
	cp.TotalLedgers++
	cp.TotalTransactions += txCount
	cp.TotalOperations += opCount

	return nil
}

// GetLastLedger returns the last processed ledger sequence
func (cp *Checkpoint) GetLastLedger() uint32 {
	cp.mu.RLock()
	defer cp.mu.RUnlock()
	return cp.LastLedger
}

// GetStats returns checkpoint statistics
func (cp *Checkpoint) GetStats() (uint32, uint64, uint64, uint64) {
	cp.mu.RLock()
	defer cp.mu.RUnlock()
	return cp.LastLedger, cp.TotalLedgers, cp.TotalTransactions, cp.TotalOperations
}

// formatTimestamp returns current time in ISO 8601 format
func formatTimestamp() string {
	return timeNow().UTC().Format("2006-01-02T15:04:05Z07:00")
}

// timeNow is a variable to allow mocking in tests
var timeNow = time.Now
