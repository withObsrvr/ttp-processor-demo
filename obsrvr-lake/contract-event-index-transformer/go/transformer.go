package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"
)

// Transformer manages the Contract Event Index transformation pipeline
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

// NewTransformer creates a new Contract Event Index transformer
func NewTransformer(config *Config) (*Transformer, error) {
	// Create Bronze Hot reader (reads from PostgreSQL Bronze Hot)
	bronzeReader, err := connectBronzeHot(config)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to Bronze Hot: %w", err)
	}

	// Create Index writer (writes to DuckLake)
	indexWriter, err := NewIndexWriter(config)
	if err != nil {
		bronzeReader.Close()
		return nil, fmt.Errorf("failed to create Index writer: %w", err)
	}

	// Create checkpoint manager (file-based)
	checkpoint, err := NewCheckpointManager(config.Indexing.CheckpointFile)
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
	log.Println("🚀 Starting Contract Event Index Transformer")

	pollInterval, err := t.config.GetPollInterval()
	if err != nil {
		return fmt.Errorf("invalid poll interval: %w", err)
	}

	log.Printf("Poll Interval: %v", pollInterval)
	log.Printf("Batch Size: %d ledgers", t.config.Indexing.BatchSize)
	log.Println("Source: Bronze Hot (PostgreSQL contract_events_stream_v1)")
	log.Println("Target: Contract Event Index (DuckLake)")

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

	log.Println("✅ Transformer ready - polling for new contract events...")

	// Start polling loop
	ticker := time.NewTicker(pollInterval)
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
	endLedger := lastLedger + t.config.Indexing.BatchSize
	if endLedger > maxLedger {
		endLedger = maxLedger
	}

	log.Printf("📊 New data available (ledgers %d to %d)", lastLedger+1, endLedger)

	// Read contract event summaries from Bronze Hot
	summaries, err := t.bronzeReader.ReadContractEventSummaries(ctx, lastLedger, endLedger)
	if err != nil {
		return fmt.Errorf("failed to read contract event summaries: %w", err)
	}

	if len(summaries) == 0 {
		log.Printf("ℹ️  No contract events found in ledger range %d-%d", lastLedger+1, endLedger)
	} else {
		// Transform to index rows
		indexRows := make([]ContractEventIndexRow, len(summaries))
		firstSeenAt := time.Now()

		for i, summary := range summaries {
			indexRows[i] = ContractEventIndexRow{
				ContractID:     summary.ContractID,
				LedgerSequence: summary.LedgerSequence,
				EventCount:     summary.EventCount,
				FirstSeenAt:    firstSeenAt,
				LedgerRange:    summary.LedgerSequence / 100000,
			}
		}

		// Write to DuckLake
		rowsWritten, err := t.indexWriter.WriteBatch(ctx, indexRows)
		if err != nil {
			return fmt.Errorf("failed to write index rows: %w", err)
		}

		// CRITICAL: CHECKPOINT to flush data to Parquet files
		if err := t.indexWriter.Checkpoint(); err != nil {
			return fmt.Errorf("failed to checkpoint DuckLake: %w", err)
		}

		log.Printf("✅ Indexed %d contract-ledger pairs (ledgers %d→%d)", rowsWritten, lastLedger+1, endLedger)
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

// connectBronzeHot creates a Bronze Hot database connection
func connectBronzeHot(config *Config) (*BronzeHotReader, error) {
	db, err := connectPostgres(&config.BronzeHot)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to Bronze Hot: %w", err)
	}

	return NewBronzeHotReader(db), nil
}
