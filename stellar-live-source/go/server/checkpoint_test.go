package server

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"go.uber.org/zap"
)

func TestCheckpointSaveLoad(t *testing.T) {
	// Create temp directory for test checkpoints
	tmpDir, err := os.MkdirTemp("", "checkpoint-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	checkpointPath := filepath.Join(tmpDir, "test-checkpoint.json")

	// Create test server with checkpoint config
	config := &Config{
		RPCEndpoint:    "http://localhost:8000",
		CheckpointPath: checkpointPath,
	}

	server := &RawLedgerServer{
		config:  config,
		metrics: NewDataSourceMetrics(),
	}

	// Initialize logger (required for checkpoint operations)
	logger, err := NewTestLogger()
	if err != nil {
		t.Fatalf("Failed to create logger: %v", err)
	}
	server.logger = logger

	// Save a checkpoint
	testSeq := uint32(12345)
	err = server.SaveCheckpoint(testSeq)
	if err != nil {
		t.Fatalf("Failed to save checkpoint: %v", err)
	}

	// Verify checkpoint file exists
	if _, err := os.Stat(checkpointPath); os.IsNotExist(err) {
		t.Fatal("Checkpoint file was not created")
	}

	// Load the checkpoint
	checkpoint, err := server.LoadCheckpoint()
	if err != nil {
		t.Fatalf("Failed to load checkpoint: %v", err)
	}

	if checkpoint == nil {
		t.Fatal("Expected checkpoint to be loaded, got nil")
	}

	// Verify checkpoint data
	if checkpoint.LastProcessedLedger != testSeq {
		t.Errorf("Expected last processed ledger %d, got %d",
			testSeq, checkpoint.LastProcessedLedger)
	}

	if time.Since(checkpoint.Timestamp) > 5*time.Second {
		t.Error("Checkpoint timestamp is too old")
	}
}

func TestCheckpointNoFile(t *testing.T) {
	// Create temp directory
	tmpDir, err := os.MkdirTemp("", "checkpoint-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	checkpointPath := filepath.Join(tmpDir, "nonexistent-checkpoint.json")

	config := &Config{
		RPCEndpoint:    "http://localhost:8000",
		CheckpointPath: checkpointPath,
	}

	server := &RawLedgerServer{
		config:  config,
		metrics: NewDataSourceMetrics(),
	}

	logger, err := NewTestLogger()
	if err != nil {
		t.Fatalf("Failed to create logger: %v", err)
	}
	server.logger = logger

	// Load checkpoint when file doesn't exist should return nil, nil
	checkpoint, err := server.LoadCheckpoint()
	if err != nil {
		t.Fatalf("Expected no error loading non-existent checkpoint, got: %v", err)
	}

	if checkpoint != nil {
		t.Error("Expected nil checkpoint when file doesn't exist")
	}
}

func TestCheckpointDisabled(t *testing.T) {
	// Create server with checkpointing disabled (empty path)
	config := &Config{
		RPCEndpoint:    "http://localhost:8000",
		CheckpointPath: "", // Disabled
	}

	server := &RawLedgerServer{
		config:  config,
		metrics: NewDataSourceMetrics(),
	}

	logger, err := NewTestLogger()
	if err != nil {
		t.Fatalf("Failed to create logger: %v", err)
	}
	server.logger = logger

	// Save checkpoint should be a no-op
	err = server.SaveCheckpoint(12345)
	if err != nil {
		t.Fatalf("Expected no error when checkpointing disabled, got: %v", err)
	}

	// Load checkpoint should return nil
	checkpoint, err := server.LoadCheckpoint()
	if err != nil {
		t.Fatalf("Expected no error loading checkpoint when disabled, got: %v", err)
	}

	if checkpoint != nil {
		t.Error("Expected nil checkpoint when checkpointing disabled")
	}
}

func TestCheckpointAtomicWrite(t *testing.T) {
	// Create temp directory
	tmpDir, err := os.MkdirTemp("", "checkpoint-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	checkpointPath := filepath.Join(tmpDir, "atomic-checkpoint.json")

	config := &Config{
		RPCEndpoint:    "http://localhost:8000",
		CheckpointPath: checkpointPath,
	}

	server := &RawLedgerServer{
		config:  config,
		metrics: NewDataSourceMetrics(),
	}

	logger, err := NewTestLogger()
	if err != nil {
		t.Fatalf("Failed to create logger: %v", err)
	}
	server.logger = logger

	// Save initial checkpoint
	err = server.SaveCheckpoint(1000)
	if err != nil {
		t.Fatalf("Failed to save initial checkpoint: %v", err)
	}

	// Save another checkpoint (should overwrite atomically)
	err = server.SaveCheckpoint(2000)
	if err != nil {
		t.Fatalf("Failed to save second checkpoint: %v", err)
	}

	// Verify temp file doesn't exist
	tmpPath := checkpointPath + ".tmp"
	if _, err := os.Stat(tmpPath); !os.IsNotExist(err) {
		t.Error("Temporary checkpoint file should not exist after atomic rename")
	}

	// Load checkpoint and verify it has the latest data
	checkpoint, err := server.LoadCheckpoint()
	if err != nil {
		t.Fatalf("Failed to load checkpoint: %v", err)
	}

	if checkpoint.LastProcessedLedger != 2000 {
		t.Errorf("Expected last processed ledger 2000, got %d",
			checkpoint.LastProcessedLedger)
	}
}

func TestGetCheckpointInfo(t *testing.T) {
	// Create temp directory
	tmpDir, err := os.MkdirTemp("", "checkpoint-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	checkpointPath := filepath.Join(tmpDir, "info-checkpoint.json")

	config := &Config{
		RPCEndpoint:    "http://localhost:8000",
		CheckpointPath: checkpointPath,
	}

	server := &RawLedgerServer{
		config:  config,
		metrics: NewDataSourceMetrics(),
	}

	logger, err := NewTestLogger()
	if err != nil {
		t.Fatalf("Failed to create logger: %v", err)
	}
	server.logger = logger

	// Get info when no checkpoint exists
	info := server.GetCheckpointInfo()
	if info["enabled"].(bool) != true {
		t.Error("Expected checkpoint to be enabled")
	}
	if info["exists"].(bool) != false {
		t.Error("Expected checkpoint to not exist yet")
	}

	// Save a checkpoint
	err = server.SaveCheckpoint(5000)
	if err != nil {
		t.Fatalf("Failed to save checkpoint: %v", err)
	}

	// Get info after checkpoint exists
	info = server.GetCheckpointInfo()
	if info["enabled"].(bool) != true {
		t.Error("Expected checkpoint to be enabled")
	}
	if info["exists"].(bool) != true {
		t.Error("Expected checkpoint to exist")
	}
	if info["last_processed_ledger"].(uint32) != 5000 {
		t.Errorf("Expected last processed ledger 5000, got %v",
			info["last_processed_ledger"])
	}
}

// NewTestLogger creates a logger for testing
func NewTestLogger() (*zap.Logger, error) {
	// Use a development logger for tests
	return zap.NewDevelopment()
}
