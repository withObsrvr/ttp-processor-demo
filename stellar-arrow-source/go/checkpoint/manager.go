package checkpoint

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/withObsrvr/ttp-processor-demo/stellar-arrow-source/logging"
)

// CheckpointManager manages streaming checkpoints for resume capability
type CheckpointManager struct {
	logger         *logging.ComponentLogger
	checkpointDir  string
	checkpointFile string
	mu             sync.RWMutex
	
	// Current state
	currentState   *StreamState
	
	// Auto-save configuration
	autoSaveInterval time.Duration
	autoSaveEnabled  bool
	stopChan         chan struct{}
	wg               sync.WaitGroup
}

// StreamState represents the current state of a stream
type StreamState struct {
	StreamID        string    `json:"stream_id"`
	StartLedger     uint32    `json:"start_ledger"`
	EndLedger       uint32    `json:"end_ledger"`
	LastProcessed   uint32    `json:"last_processed"`
	ProcessedCount  int64     `json:"processed_count"`
	BytesProcessed  int64     `json:"bytes_processed"`
	LastCheckpoint  time.Time `json:"last_checkpoint"`
	Metadata        map[string]interface{} `json:"metadata,omitempty"`
}

// NewCheckpointManager creates a new checkpoint manager
func NewCheckpointManager(logger *logging.ComponentLogger, checkpointDir string) (*CheckpointManager, error) {
	// Ensure checkpoint directory exists
	if err := os.MkdirAll(checkpointDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create checkpoint directory: %w", err)
	}
	
	cm := &CheckpointManager{
		logger:           logger,
		checkpointDir:    checkpointDir,
		autoSaveInterval: 30 * time.Second,
		autoSaveEnabled:  true,
		stopChan:         make(chan struct{}),
	}
	
	logger.Info().
		Str("checkpoint_dir", checkpointDir).
		Msg("Checkpoint manager initialized")
	
	return cm, nil
}

// StartStream initializes a new stream or resumes from checkpoint
func (cm *CheckpointManager) StartStream(streamID string, startLedger, endLedger uint32) (*StreamState, error) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	
	// Set checkpoint file path
	cm.checkpointFile = filepath.Join(cm.checkpointDir, fmt.Sprintf("stream_%s.checkpoint", streamID))
	
	// Try to load existing checkpoint
	if state, err := cm.loadCheckpoint(); err == nil && state.StreamID == streamID {
		cm.logger.Info().
			Str("stream_id", streamID).
			Uint32("last_processed", state.LastProcessed).
			Int64("processed_count", state.ProcessedCount).
			Time("last_checkpoint", state.LastCheckpoint).
			Msg("Resuming from checkpoint")
		
		// Update state with new parameters if provided
		if startLedger > 0 && state.LastProcessed < startLedger {
			state.LastProcessed = startLedger - 1
		}
		if endLedger > 0 {
			state.EndLedger = endLedger
		}
		
		cm.currentState = state
	} else {
		// Create new state
		cm.currentState = &StreamState{
			StreamID:       streamID,
			StartLedger:    startLedger,
			EndLedger:      endLedger,
			LastProcessed:  startLedger - 1,
			ProcessedCount: 0,
			BytesProcessed: 0,
			LastCheckpoint: time.Now(),
			Metadata:       make(map[string]interface{}),
		}
		
		cm.logger.Info().
			Str("stream_id", streamID).
			Uint32("start_ledger", startLedger).
			Uint32("end_ledger", endLedger).
			Msg("Starting new stream")
	}
	
	// Start auto-save if enabled
	if cm.autoSaveEnabled {
		cm.startAutoSave()
	}
	
	return cm.currentState, nil
}

// UpdateProgress updates the stream progress
func (cm *CheckpointManager) UpdateProgress(lastProcessed uint32, bytesProcessed int64) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	
	if cm.currentState == nil {
		return fmt.Errorf("no active stream")
	}
	
	// Only update if we've actually progressed
	if lastProcessed > cm.currentState.LastProcessed {
		cm.currentState.LastProcessed = lastProcessed
		cm.currentState.ProcessedCount++
		cm.currentState.BytesProcessed += bytesProcessed
		
		cm.logger.Debug().
			Uint32("last_processed", lastProcessed).
			Int64("total_processed", cm.currentState.ProcessedCount).
			Msg("Updated checkpoint progress")
	}
	
	return nil
}

// SaveCheckpoint saves the current state to disk
func (cm *CheckpointManager) SaveCheckpoint() error {
	cm.mu.RLock()
	state := cm.currentState
	checkpointFile := cm.checkpointFile
	cm.mu.RUnlock()
	
	if state == nil {
		return fmt.Errorf("no active stream to checkpoint")
	}
	
	// Update checkpoint time
	state.LastCheckpoint = time.Now()
	
	// Marshal state to JSON
	data, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal checkpoint: %w", err)
	}
	
	// Write atomically using temp file
	tempFile := checkpointFile + ".tmp"
	if err := ioutil.WriteFile(tempFile, data, 0644); err != nil {
		return fmt.Errorf("failed to write checkpoint: %w", err)
	}
	
	// Rename to final location
	if err := os.Rename(tempFile, checkpointFile); err != nil {
		os.Remove(tempFile) // Clean up
		return fmt.Errorf("failed to rename checkpoint: %w", err)
	}
	
	cm.logger.Debug().
		Str("checkpoint_file", checkpointFile).
		Uint32("last_processed", state.LastProcessed).
		Int64("processed_count", state.ProcessedCount).
		Msg("Checkpoint saved")
	
	return nil
}

// loadCheckpoint loads a checkpoint from disk
func (cm *CheckpointManager) loadCheckpoint() (*StreamState, error) {
	data, err := ioutil.ReadFile(cm.checkpointFile)
	if err != nil {
		return nil, err
	}
	
	var state StreamState
	if err := json.Unmarshal(data, &state); err != nil {
		return nil, fmt.Errorf("failed to unmarshal checkpoint: %w", err)
	}
	
	return &state, nil
}

// GetResumePoint returns the ledger to resume from
func (cm *CheckpointManager) GetResumePoint() uint32 {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	
	if cm.currentState == nil {
		return 0
	}
	
	// Resume from the next ledger after the last processed
	return cm.currentState.LastProcessed + 1
}

// SetMetadata sets custom metadata in the checkpoint
func (cm *CheckpointManager) SetMetadata(key string, value interface{}) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	
	if cm.currentState != nil && cm.currentState.Metadata != nil {
		cm.currentState.Metadata[key] = value
	}
}

// GetMetadata retrieves custom metadata from the checkpoint
func (cm *CheckpointManager) GetMetadata(key string) (interface{}, bool) {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	
	if cm.currentState != nil && cm.currentState.Metadata != nil {
		value, exists := cm.currentState.Metadata[key]
		return value, exists
	}
	
	return nil, false
}

// startAutoSave starts the automatic checkpoint saving
func (cm *CheckpointManager) startAutoSave() {
	cm.wg.Add(1)
	go func() {
		defer cm.wg.Done()
		
		ticker := time.NewTicker(cm.autoSaveInterval)
		defer ticker.Stop()
		
		for {
			select {
			case <-ticker.C:
				if err := cm.SaveCheckpoint(); err != nil {
					cm.logger.Error().
						Err(err).
						Msg("Auto-save checkpoint failed")
				}
			case <-cm.stopChan:
				// Save final checkpoint before stopping
				cm.SaveCheckpoint()
				return
			}
		}
	}()
	
	cm.logger.Info().
		Dur("interval", cm.autoSaveInterval).
		Msg("Started auto-save checkpoint")
}

// Stop stops the checkpoint manager
func (cm *CheckpointManager) Stop() {
	cm.logger.Info().
		Msg("Stopping checkpoint manager")
	
	// Signal stop
	close(cm.stopChan)
	
	// Wait for auto-save to finish
	cm.wg.Wait()
	
	// Save final checkpoint
	if err := cm.SaveCheckpoint(); err != nil {
		cm.logger.Error().
			Err(err).
			Msg("Failed to save final checkpoint")
	}
	
	cm.logger.Info().
		Msg("Checkpoint manager stopped")
}

// StreamCheckpoint provides stream-specific checkpoint operations
type StreamCheckpoint struct {
	manager  *CheckpointManager
	streamID string
}

// NewStreamCheckpoint creates a checkpoint for a specific stream
func (cm *CheckpointManager) NewStreamCheckpoint(streamID string) *StreamCheckpoint {
	return &StreamCheckpoint{
		manager:  cm,
		streamID: streamID,
	}
}

// RecordBatch records a batch of processed ledgers
func (sc *StreamCheckpoint) RecordBatch(startSeq, endSeq uint32, recordCount int, bytesProcessed int64) error {
	// Update for each ledger in the batch
	for seq := startSeq; seq <= endSeq; seq++ {
		if err := sc.manager.UpdateProgress(seq, bytesProcessed/int64(endSeq-startSeq+1)); err != nil {
			return err
		}
	}
	
	// Set batch metadata
	sc.manager.SetMetadata("last_batch_size", recordCount)
	sc.manager.SetMetadata("last_batch_end", endSeq)
	
	return nil
}

// CheckpointStore provides persistent storage for checkpoints
type CheckpointStore struct {
	basePath string
	logger   *logging.ComponentLogger
}

// NewCheckpointStore creates a new checkpoint store
func NewCheckpointStore(basePath string, logger *logging.ComponentLogger) (*CheckpointStore, error) {
	if err := os.MkdirAll(basePath, 0755); err != nil {
		return nil, fmt.Errorf("failed to create checkpoint store: %w", err)
	}
	
	return &CheckpointStore{
		basePath: basePath,
		logger:   logger,
	}, nil
}

// ListCheckpoints returns all available checkpoints
func (cs *CheckpointStore) ListCheckpoints() ([]string, error) {
	files, err := filepath.Glob(filepath.Join(cs.basePath, "stream_*.checkpoint"))
	if err != nil {
		return nil, err
	}
	
	checkpoints := make([]string, 0, len(files))
	for _, file := range files {
		base := filepath.Base(file)
		// Extract stream ID from filename
		if len(base) > 20 {
			streamID := base[7 : len(base)-11] // Remove "stream_" prefix and ".checkpoint" suffix
			checkpoints = append(checkpoints, streamID)
		}
	}
	
	return checkpoints, nil
}

// DeleteCheckpoint removes a checkpoint
func (cs *CheckpointStore) DeleteCheckpoint(streamID string) error {
	checkpointFile := filepath.Join(cs.basePath, fmt.Sprintf("stream_%s.checkpoint", streamID))
	return os.Remove(checkpointFile)
}

// CleanupOldCheckpoints removes checkpoints older than the specified duration
func (cs *CheckpointStore) CleanupOldCheckpoints(maxAge time.Duration) error {
	files, err := filepath.Glob(filepath.Join(cs.basePath, "stream_*.checkpoint"))
	if err != nil {
		return err
	}
	
	cutoff := time.Now().Add(-maxAge)
	removed := 0
	
	for _, file := range files {
		info, err := os.Stat(file)
		if err != nil {
			continue
		}
		
		if info.ModTime().Before(cutoff) {
			if err := os.Remove(file); err != nil {
				cs.logger.Error().
					Err(err).
					Str("file", file).
					Msg("Failed to remove old checkpoint")
			} else {
				removed++
			}
		}
	}
	
	if removed > 0 {
		cs.logger.Info().
			Int("removed", removed).
			Dur("max_age", maxAge).
			Msg("Cleaned up old checkpoints")
	}
	
	return nil
}