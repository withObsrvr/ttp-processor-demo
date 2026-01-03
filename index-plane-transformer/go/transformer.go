package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"sync"
	"time"
)

// Transformer manages the Index Plane transformation pipeline
type Transformer struct {
	config       *Config
	bronzeReader *BronzeHotReader
	indexWriter  *IndexWriter
	checkpoint   *CheckpointManager
	healthServer *HealthServer
	stats        TransformerStats
	mu           sync.RWMutex
	stopChan     chan struct{}
}

// NewTransformer creates a new Index Plane transformer
func NewTransformer(config *Config, bronzeDB *sql.DB, catalogDB *sql.DB) (*Transformer, error) {
	// Create Bronze Hot reader (reads from PostgreSQL stellar_hot)
	bronzeReader := NewBronzeHotReader(bronzeDB)

	// Create Index writer (writes to DuckLake)
	indexWriter, err := NewIndexWriter(&config.IndexCold, catalogDB)
	if err != nil {
		bronzeReader.Close()
		return nil, fmt.Errorf("failed to create Index writer: %w", err)
	}

	// Create checkpoint manager
	checkpoint, err := NewCheckpointManager(catalogDB, &config.Checkpoint)
	if err != nil {
		bronzeReader.Close()
		indexWriter.Close()
		return nil, fmt.Errorf("failed to create checkpoint manager: %w", err)
	}

	// Create health server
	healthServer := NewHealthServer(config.Health.Port)

	return &Transformer{
		config:       config,
		bronzeReader: bronzeReader,
		indexWriter:  indexWriter,
		checkpoint:   checkpoint,
		healthServer: healthServer,
		stopChan:     make(chan struct{}),
	}, nil
}

// Start starts the transformation loop
func (t *Transformer) Start() error {
	log.Println("🚀 Starting Index Plane Transformer")
	log.Printf("Poll Interval: %v", t.config.GetPollInterval())
	log.Printf("Batch Size: %d ledgers", t.config.Service.BatchSize)
	log.Println("Source: Bronze Hot (PostgreSQL stellar_hot)")
	log.Println("Target: Index Plane (DuckLake)")

	// Start health server
	t.healthServer.SetIndexWriter(t.indexWriter)
	if err := t.healthServer.Start(); err != nil {
		return fmt.Errorf("failed to start health server: %w", err)
	}

	ctx := context.Background()

	// Load checkpoint
	lastLedger, err := t.checkpoint.Load(ctx)
	if err != nil {
		return fmt.Errorf("failed to load checkpoint: %w", err)
	}

	if lastLedger == 0 {
		log.Println("⚠️  No checkpoint found, starting from beginning")
	} else {
		log.Printf("📍 Resuming from ledger: %d", lastLedger)
	}

	// Update initial stats
	t.updateStats(lastLedger, time.Now(), 0)

	// Run initial transformation check
	log.Println("🔍 Running initial transformation check...")
	if err := t.runTransformationCycle(); err != nil {
		log.Printf("⚠️  Initial transformation error: %v", err)
	}

	log.Println("✅ Transformer ready - polling for new data...")
	log.Println("📋 Maintenance endpoints (⚠️  STOP transformer before using):")
	log.Println("   POST /maintenance/merge    - Merge small files")
	log.Println("   POST /maintenance/expire   - Expire old snapshots")
	log.Println("   POST /maintenance/cleanup  - Remove orphaned files")
	log.Println("   POST /maintenance/full     - Full cycle (merge+expire+cleanup)")
	log.Println("   POST /maintenance/recreate - ⚠️  Drop & recreate table (DELETES ALL DATA)")

	// Start polling loop
	ticker := time.NewTicker(t.config.GetPollInterval())
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if err := t.runTransformationCycle(); err != nil {
				log.Printf("❌ Transformation error: %v", err)
				t.incrementErrors()
			}
		case <-t.stopChan:
			log.Println("🛑 Transformer stopping...")
			return nil
		}
	}
}

// runTransformationCycle executes one transformation cycle
func (t *Transformer) runTransformationCycle() error {
	startTime := time.Now()
	ctx := context.Background()

	// Get last processed ledger from checkpoint
	lastLedger, err := t.checkpoint.Load(ctx)
	if err != nil {
		return fmt.Errorf("failed to load checkpoint: %w", err)
	}

	// Get max ledger in Bronze Hot
	maxLedger, err := t.bronzeReader.GetMaxLedgerSequence(ctx)
	if err != nil {
		return fmt.Errorf("failed to get max ledger from Bronze Hot: %w", err)
	}

	// Check if there's new data to process
	if maxLedger <= lastLedger {
		// No new data, update stats and return
		t.updateStats(lastLedger, time.Now(), 0)
		return nil
	}

	// Calculate batch end (process up to batch_size ledgers)
	endLedger := lastLedger + t.config.Service.BatchSize
	if endLedger > maxLedger {
		endLedger = maxLedger
	}

	log.Printf("📊 New data available (ledgers %d to %d)", lastLedger+1, endLedger)

	// Read transactions from Bronze Hot (PostgreSQL stellar_hot)
	transactions, err := t.bronzeReader.ReadTransactions(ctx, lastLedger, endLedger)
	if err != nil {
		return fmt.Errorf("failed to read transactions: %w", err)
	}

	if len(transactions) == 0 {
		log.Printf("ℹ️  No transactions found in ledger range %d-%d", lastLedger+1, endLedger)
	} else {
		// Write transactions to Index Plane (DuckLake)
		rowsWritten, err := t.indexWriter.WriteTransactions(ctx, transactions)
		if err != nil {
			return fmt.Errorf("failed to write transactions: %w", err)
		}

		log.Printf("✅ Indexed %d transactions (ledgers %d→%d)", rowsWritten, lastLedger+1, endLedger)
	}

	// Save checkpoint
	if err := t.checkpoint.Save(ctx, endLedger); err != nil {
		return fmt.Errorf("failed to save checkpoint: %w", err)
	}

	// Update stats
	duration := time.Since(startTime)
	t.updateStats(endLedger, time.Now(), duration)
	t.incrementTotal()

	return nil
}

// updateStats updates transformer statistics
func (t *Transformer) updateStats(lastLedger int64, processedAt time.Time, duration time.Duration) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.stats.LastLedgerProcessed = lastLedger
	t.stats.LastProcessedAt = processedAt
	t.stats.LastTransformDuration = duration

	// Calculate lag (simple: time since last processed)
	t.stats.LagSeconds = int64(time.Since(processedAt).Seconds())

	// Update health server
	t.healthServer.UpdateStats(t.stats)
}

// incrementTotal increments total transformations count
func (t *Transformer) incrementTotal() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.stats.TransformationsTotal++
}

// incrementErrors increments error count
func (t *Transformer) incrementErrors() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.stats.TransformationErrors++
}

// GetStats returns a copy of current stats
func (t *Transformer) GetStats() TransformerStats {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.stats
}

// Stop stops the transformer gracefully
func (t *Transformer) Stop() error {
	log.Println("🛑 Stopping transformer...")

	close(t.stopChan)

	// Stop health server
	if err := t.healthServer.Stop(); err != nil {
		log.Printf("Error stopping health server: %v", err)
	}

	// Close connections
	if err := t.bronzeReader.Close(); err != nil {
		log.Printf("Error closing Bronze Hot reader: %v", err)
	}

	if err := t.indexWriter.Close(); err != nil {
		log.Printf("Error closing Index writer: %v", err)
	}

	if err := t.checkpoint.Close(); err != nil {
		log.Printf("Error closing checkpoint manager: %v", err)
	}

	log.Println("✅ Transformer stopped")
	return nil
}
