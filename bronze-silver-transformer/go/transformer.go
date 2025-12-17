package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/withobsrvr/bronze-silver-transformer/transformations"
)

// Transformer handles Bronze → Silver transformations
type Transformer struct {
	config     *Config
	duckdb     *DuckDBClient
	checkpoint *CheckpointManager
	stopChan   chan struct{}

	// Stats
	mu                          sync.RWMutex
	transformationsTotal        int64
	transformationErrors        int64
	lastLedgerSequence          int64
	lastTransformationTime      time.Time
	lastTransformationDuration  time.Duration
}

// TransformerStats holds transformation statistics
type TransformerStats struct {
	TransformationsTotal       int64
	TransformationErrors       int64
	LastLedgerSequence         int64
	LastTransformationTime     time.Time
	LastTransformationDuration time.Duration
}

// NewTransformer creates a new transformer instance
func NewTransformer(config *Config) (*Transformer, error) {
	if err := config.Validate(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}

	// Create DuckDB client with catalog attached
	duckdb, err := NewDuckDBClient(&config.DuckLake)
	if err != nil {
		return nil, fmt.Errorf("failed to create DuckDB client: %w", err)
	}

	// Create checkpoint manager
	checkpoint := NewCheckpointManager(duckdb.db, &config.Checkpoint, config.DuckLake.CatalogName)

	// Initialize checkpoint table
	if err := checkpoint.InitCheckpointTable(); err != nil {
		return nil, fmt.Errorf("failed to initialize checkpoint: %w", err)
	}

	// Load last processed ledger
	lastSeq, err := checkpoint.Load()
	if err != nil {
		return nil, fmt.Errorf("failed to load checkpoint: %w", err)
	}

	if lastSeq == 0 {
		log.Println("⚠️  No checkpoint found, starting from beginning")
	} else {
		log.Printf("📍 Resuming from ledger sequence: %d", lastSeq)
	}

	// Log catalog info
	log.Printf("📋 Connected to DuckLake catalog with %s.%s (Bronze) and %s.%s (Silver)",
		config.DuckLake.CatalogName, config.DuckLake.BronzeSchema,
		config.DuckLake.CatalogName, config.DuckLake.SilverSchema)

	return &Transformer{
		config:             config,
		duckdb:             duckdb,
		checkpoint:         checkpoint,
		stopChan:           make(chan struct{}),
		lastLedgerSequence: lastSeq,
	}, nil
}

// Start begins the transformation scheduler
func (t *Transformer) Start() error {
	log.Println("🚀 Starting Bronze → Silver Transformer")
	log.Printf("Transform Interval: %v", t.config.TransformInterval())
	log.Printf("Bronze Schema: %s.%s", t.config.DuckLake.CatalogName, t.config.DuckLake.BronzeSchema)
	log.Printf("Silver Schema: %s.%s", t.config.DuckLake.CatalogName, t.config.DuckLake.SilverSchema)

	// Run immediate check on startup
	log.Println("🔍 Running initial transformation check...")
	if err := t.runTransformationCycle(); err != nil {
		log.Printf("⚠️  Initial transformation error: %v", err)
		t.incrementErrors()
	}

	// Start transformation ticker
	ticker := time.NewTicker(t.config.TransformInterval())
	defer ticker.Stop()

	log.Println("✅ Transformer ready - polling for new data...")

	for {
		select {
		case <-ticker.C:
			if err := t.runTransformationCycle(); err != nil {
				log.Printf("❌ Transformation error: %v", err)
				t.incrementErrors()
			}
		case <-t.stopChan:
			log.Println("🛑 Stopping transformer...")
			return nil
		}
	}
}

// Stop gracefully stops the transformer
func (t *Transformer) Stop() {
	close(t.stopChan)
	if t.duckdb != nil {
		t.duckdb.Close()
	}
}

// runTransformationCycle executes a full transformation cycle
func (t *Transformer) runTransformationCycle() error {
	startTime := time.Now()
	ctx := context.Background()

	// Check if there's new data in Bronze
	maxBronzeSeq, err := t.duckdb.QueryMaxLedgerSequence(ctx)
	if err != nil {
		return fmt.Errorf("failed to query max ledger sequence: %w", err)
	}

	if maxBronzeSeq <= t.lastLedgerSequence {
		// No new data
		return nil
	}

	log.Printf("📊 New Bronze data available (current: %d, last processed: %d)", maxBronzeSeq, t.lastLedgerSequence)

	// Run transformations
	log.Printf("🔄 Running transformations for ledgers %d to %d...", t.lastLedgerSequence+1, maxBronzeSeq)

	// Phase 1: Current State Tables (10 tables)
	currentTransformer := transformations.NewCurrentStateTransformer(
		t.duckdb.db,
		t.config.DuckLake.CatalogName,
		t.config.DuckLake.BronzeSchema,
		t.config.DuckLake.SilverSchema,
		t.config.DuckLake.SilverDataPath,
	)
	if err := currentTransformer.TransformAll(ctx, t.lastLedgerSequence+1, maxBronzeSeq); err != nil {
		return fmt.Errorf("failed to transform current state tables: %w", err)
	}

	// Phase 2: Snapshot Tables with SCD Type 2 (5 tables)
	snapshotTransformer := transformations.NewSnapshotTransformer(
		t.duckdb.db,
		t.config.DuckLake.CatalogName,
		t.config.DuckLake.BronzeSchema,
		t.config.DuckLake.SilverSchema,
		t.config.DuckLake.SilverDataPath,
	)
	if err := snapshotTransformer.TransformAll(ctx, t.lastLedgerSequence+1, maxBronzeSeq); err != nil {
		return fmt.Errorf("failed to transform snapshot tables: %w", err)
	}

	// Phase 3: Enriched Operations with JOINs (2 tables)
	enrichedTransformer := transformations.NewEnrichedTransformer(
		t.duckdb.db,
		t.config.DuckLake.CatalogName,
		t.config.DuckLake.BronzeSchema,
		t.config.DuckLake.SilverSchema,
		t.config.DuckLake.SilverDataPath,
	)
	if err := enrichedTransformer.TransformAll(ctx, t.lastLedgerSequence+1, maxBronzeSeq); err != nil {
		return fmt.Errorf("failed to transform enriched operations: %w", err)
	}

	// Phase 4: Analytics - Token Transfers (1 table)
	analyticsTransformer := transformations.NewAnalyticsTransformer(
		t.duckdb.db,
		t.config.DuckLake.CatalogName,
		t.config.DuckLake.BronzeSchema,
		t.config.DuckLake.SilverSchema,
		t.config.DuckLake.SilverDataPath,
	)
	if err := analyticsTransformer.TransformAll(ctx, t.lastLedgerSequence+1, maxBronzeSeq); err != nil {
		return fmt.Errorf("failed to transform analytics tables: %w", err)
	}

	// Update checkpoint
	if err := t.checkpoint.Save(maxBronzeSeq); err != nil {
		log.Printf("⚠️  Failed to save checkpoint: %v", err)
	}

	// Update stats
	duration := time.Since(startTime)
	t.updateStats(maxBronzeSeq, duration)

	log.Printf("✅ Transformation complete in %v", duration)

	// Update Prometheus metrics
	transformationsTotal.Inc()
	transformationDuration.Observe(duration.Seconds())
	lastLedgerSequence.Set(float64(maxBronzeSeq))

	return nil
}

// GetStats returns current transformation statistics
func (t *Transformer) GetStats() TransformerStats {
	t.mu.RLock()
	defer t.mu.RUnlock()

	return TransformerStats{
		TransformationsTotal:       t.transformationsTotal,
		TransformationErrors:       t.transformationErrors,
		LastLedgerSequence:         t.lastLedgerSequence,
		LastTransformationTime:     t.lastTransformationTime,
		LastTransformationDuration: t.lastTransformationDuration,
	}
}

// updateStats updates internal statistics
func (t *Transformer) updateStats(ledgerSeq int64, duration time.Duration) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.transformationsTotal++
	t.lastLedgerSequence = ledgerSeq
	t.lastTransformationTime = time.Now()
	t.lastTransformationDuration = duration
}

// incrementErrors increments the error counter
func (t *Transformer) incrementErrors() {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.transformationErrors++
	transformationErrors.Inc()
}
