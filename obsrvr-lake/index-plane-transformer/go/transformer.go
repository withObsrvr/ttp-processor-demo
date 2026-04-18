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
	advisoryLock *AdvisoryLock
	healthServer *HealthServer
	stats        TransformerStats
	mu           sync.RWMutex
	stopChan     chan struct{}
	writeCount   int
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

	lockResource := fmt.Sprintf("%s|%s|%s|%s", config.Service.Name, config.Catalog.Database, config.IndexCold.SchemaName, config.IndexCold.TableName)
	advisoryLock := NewAdvisoryLock(catalogDB, "index-plane-transformer", lockResource)

	// Create health server
	healthServer := NewHealthServer(config.Health.Port)

	return &Transformer{
		config:       config,
		bronzeReader: bronzeReader,
		indexWriter:  indexWriter,
		checkpoint:   checkpoint,
		advisoryLock: advisoryLock,
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

	ctx := context.Background()

	if err := t.advisoryLock.Acquire(ctx); err != nil {
		return fmt.Errorf("failed to acquire singleton advisory lock: %w", err)
	}
	log.Println("🔒 Acquired advisory lock; singleton writer active")

	// Start health server
	t.healthServer.SetIndexWriter(t.indexWriter)
	if err := t.healthServer.Start(); err != nil {
		_ = t.advisoryLock.Release(ctx)
		return fmt.Errorf("failed to start health server: %w", err)
	}

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
	t.updateStats(lastLedger, 0, 0, false, "", time.Now(), 0)

	// Run initial transformation check
	log.Println("🔍 Running initial transformation check...")
	if err := t.runTransformationCycle(); err != nil {
		log.Printf("⚠️  Initial transformation error: %v", err)
	}

	log.Println("📋 Maintenance endpoints (⚠️  STOP transformer before using):")
	log.Println("   POST /maintenance/merge    - Merge small files")
	log.Println("   POST /maintenance/expire   - Expire old snapshots")
	log.Println("   POST /maintenance/cleanup  - Remove orphaned files")
	log.Println("   POST /maintenance/full     - Full cycle (merge+expire+cleanup)")
	log.Println("   POST /maintenance/recreate - ⚠️  Drop & recreate table (DELETES ALL DATA)")

	if t.config.BronzeSource.Mode == "grpc" {
		return t.startGRPC()
	}
	return t.startPolling()
}

func (t *Transformer) startPolling() error {
	log.Printf("✅ Transformer ready - polling for new data (interval: %v)...", t.config.GetPollInterval())

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

// startGRPC connects to the bronze ingester's flowctl SourceService and triggers
// transformation cycles on each received ledger-committed event.
func (t *Transformer) startGRPC() error {
	endpoint := t.config.BronzeSource.Endpoint
	log.Printf("✅ Transformer ready - gRPC streaming from %s", endpoint)

	client, err := NewBronzeStreamClient(endpoint)
	if err != nil {
		return fmt.Errorf("failed to create bronze stream client: %w", err)
	}
	defer client.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		<-t.stopChan
		cancel()
	}()

	lastLedger, err := t.checkpoint.Load(ctx)
	if err != nil {
		return fmt.Errorf("failed to load checkpoint: %w", err)
	}
	eventCh := client.StreamLedgerEvents(ctx, lastLedger)

	var pendingEnd int64
	var cycleRunning bool
	cycleDone := make(chan struct{}, 1)

	for {
		select {
		case event, ok := <-eventCh:
			if !ok {
				log.Println("Stopping transformer (stream closed)...")
				return nil
			}
			endLedger := int64(event.EndLedger)
			if cycleRunning {
				if endLedger > pendingEnd {
					pendingEnd = endLedger
				}
				continue
			}
			cycleRunning = true
			pendingEnd = 0
			go func() {
				if err := t.runTransformationCycle(); err != nil {
					log.Printf("❌ Transformation error: %v", err)
					t.incrementErrors()
				}
				select {
				case cycleDone <- struct{}{}:
				case <-ctx.Done():
				}
			}()

		case <-cycleDone:
			cycleRunning = false
			if pendingEnd > 0 {
				pendingEnd = 0
				cycleRunning = true
				go func() {
					if err := t.runTransformationCycle(); err != nil {
						log.Printf("❌ Transformation error: %v", err)
						t.incrementErrors()
					}
					select {
					case cycleDone <- struct{}{}:
					case <-ctx.Done():
					}
				}()
			}

		case <-ctx.Done():
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

	// Get current source ledger bounds in Bronze Hot.
	minLedger, maxLedger, err := t.bronzeReader.GetLedgerBounds(ctx)
	if err != nil {
		return fmt.Errorf("failed to get ledger bounds from Bronze Hot: %w", err)
	}

	retentionGapDetected, retentionGapMessage := describeRetentionGap(lastLedger, minLedger)
	if retentionGapDetected {
		if !t.config.Service.AllowRetentionGapStart {
			return fmt.Errorf("retention gap detected and allow_retention_gap_start=false: %s", retentionGapMessage)
		}
		log.Printf("⚠️  retention gap detected but continuing because allow_retention_gap_start=true: %s", retentionGapMessage)
	}

	// Check if there's new data to process
	if maxLedger <= lastLedger {
		// No new data, update stats and return
		t.updateStats(lastLedger, minLedger, maxLedger, retentionGapDetected, retentionGapMessage, time.Now(), 0)
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

		// Increment write count and flush inlined data + run maintenance if due
		t.writeCount++
		if t.config.Maintenance.Enabled && t.writeCount >= t.config.Maintenance.EveryNWrites {
			log.Printf("🔧 Flushing inlined data + maintenance (write #%d)...", t.writeCount)
			if _, err := t.indexWriter.FlushInlinedData(); err != nil {
				log.Printf("⚠️  Flush inlined data failed (non-fatal): %v", err)
			}
			if err := t.indexWriter.RunCheckpoint(ctx, t.config.Maintenance.MaxCompactedFiles); err != nil {
				log.Printf("⚠️  DuckLake maintenance failed (non-fatal): %v", err)
			}
			t.writeCount = 0
		}
	}

	// Save checkpoint
	if err := t.checkpoint.Save(ctx, endLedger); err != nil {
		return fmt.Errorf("failed to save checkpoint: %w", err)
	}

	// Update stats
	duration := time.Since(startTime)
	t.updateStats(endLedger, minLedger, maxLedger, retentionGapDetected, retentionGapMessage, time.Now(), duration)
	t.incrementTotal()

	if retentionGapDetected {
		log.Printf("⚠️  indexed forward despite retention gap; historical tx-hash coverage is incomplete until backfilled from cold: %s", retentionGapMessage)
	}

	return nil
}

// updateStats updates transformer statistics.
func (t *Transformer) updateStats(lastLedger, minLedger, maxLedger int64, retentionGapDetected bool, retentionGapMessage string, processedAt time.Time, duration time.Duration) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.stats.LastLedgerProcessed = lastLedger
	t.stats.LastProcessedAt = processedAt
	t.stats.LastTransformDuration = duration
	t.stats.SourceMinLedger = minLedger
	t.stats.SourceMaxLedger = maxLedger
	t.stats.RetentionGapDetected = retentionGapDetected
	t.stats.RetentionGapMessage = retentionGapMessage
	if maxLedger > lastLedger {
		t.stats.CheckpointGapLedgers = maxLedger - lastLedger
	} else {
		t.stats.CheckpointGapLedgers = 0
	}

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

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := t.advisoryLock.Release(ctx); err != nil {
		log.Printf("Error releasing advisory lock: %v", err)
	} else {
		log.Println("🔓 Released advisory lock")
	}

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

func describeRetentionGap(lastLedger, minLedger int64) (bool, string) {
	if minLedger <= 0 {
		return false, ""
	}
	if lastLedger == 0 && minLedger > 1 {
		return true, fmt.Sprintf("no checkpoint exists and Bronze Hot begins at ledger %d; earlier ledgers are not available from hot storage", minLedger)
	}
	if lastLedger > 0 && lastLedger < minLedger-1 {
		return true, fmt.Sprintf("checkpoint ledger %d is behind oldest hot ledger %d", lastLedger, minLedger)
	}
	return false, ""
}
