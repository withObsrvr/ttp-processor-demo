package checkpoint

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// Checkpoint tracks the last processed scan ID for resumable streaming.
type Checkpoint struct {
	mu       sync.RWMutex
	filePath string

	LastScanID     uint32 `json:"last_scan_id"`
	LastUpdateTime string `json:"last_update_time"`
	TotalScans     uint64 `json:"total_scans"`
}

func New(filePath string) (*Checkpoint, error) {
	cp := &Checkpoint{filePath: filePath}
	if err := cp.Load(); err != nil {
		if !os.IsNotExist(err) {
			return nil, fmt.Errorf("failed to load checkpoint: %w", err)
		}
	}
	return cp, nil
}

func (cp *Checkpoint) Load() error {
	cp.mu.Lock()
	defer cp.mu.Unlock()

	data, err := os.ReadFile(cp.filePath)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, cp)
}

func (cp *Checkpoint) Save() error {
	cp.mu.RLock()
	defer cp.mu.RUnlock()

	dir := filepath.Dir(cp.filePath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create checkpoint directory: %w", err)
	}

	data, err := json.MarshalIndent(cp, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal checkpoint: %w", err)
	}

	tempPath := cp.filePath + ".tmp"
	if err := os.WriteFile(tempPath, data, 0644); err != nil {
		return fmt.Errorf("failed to write checkpoint: %w", err)
	}
	return os.Rename(tempPath, cp.filePath)
}

func (cp *Checkpoint) Update(scanID uint32) error {
	cp.mu.Lock()
	defer cp.mu.Unlock()

	cp.LastScanID = scanID
	cp.LastUpdateTime = time.Now().UTC().Format(time.RFC3339)
	cp.TotalScans++
	return nil
}

func (cp *Checkpoint) GetLastScanID() uint32 {
	cp.mu.RLock()
	defer cp.mu.RUnlock()
	return cp.LastScanID
}

func (cp *Checkpoint) GetStats() (uint32, uint64) {
	cp.mu.RLock()
	defer cp.mu.RUnlock()
	return cp.LastScanID, cp.TotalScans
}
