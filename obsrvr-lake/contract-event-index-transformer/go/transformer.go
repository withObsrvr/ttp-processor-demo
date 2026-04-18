package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"sync"
	"time"
)

// Transformer manages the Contract Event Index transformation pipeline
type Transformer struct {
	config       *Config
	bronzeReader *BronzeHotReader
	catalogDB    *sql.DB
	indexWriter  *IndexWriter
	checkpoint   *CheckpointManager
	advisoryLock *AdvisoryLock
	healthServer *HealthServer
	stats        TransformerStats
	mu           sync.RWMutex
	stopChan     chan struct{}
	writeCount   int
}

// NewTransformer creates a new Contract Event Index transformer
func NewTransformer(config *Config) (*Transformer, error) {
	// Create Bronze Hot reader (reads from PostgreSQL Bronze Hot)
	bronzeReader, err := connectBronzeHot(config)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to Bronze Hot: %w", err)
	}

	// Connect to catalog DB for advisory locking and catalog-backed DuckLake access.
	catalogDB, err := connectPostgres(&config.Catalog)
	if err != nil {
		bronzeReader.Close()
		return nil, fmt.Errorf("failed to connect to Catalog DB: %w", err)
	}

	// Create Index writer (writes to DuckLake)
	indexWriter, err := NewIndexWriter(config)
	if err != nil {
		bronzeReader.Close()
		catalogDB.Close()
		return nil, fmt.Errorf("failed to create Index writer: %w", err)
	}

	// Create checkpoint manager (PostgreSQL-backed)
	checkpoint, err := NewCheckpointManager(catalogDB, config.Indexing.CheckpointTable)
	if err != nil {
		bronzeReader.Close()
		indexWriter.Close()
		catalogDB.Close()
		return nil, fmt.Errorf("failed to create checkpoint manager: %w", err)
	}

	lockResource := fmt.Sprintf("%s|%s|contract_events_index", config.Service.Name, config.Catalog.Database)
	advisoryLock := NewAdvisoryLock(catalogDB, "contract-event-index-transformer", lockResource)

	// Create health server
	healthServer := NewHealthServer(config.Health.Port)

	return &Transformer{
		config:       config,
		bronzeReader: bronzeReader,
		catalogDB:    catalogDB,
		indexWriter:  indexWriter,
		checkpoint:   checkpoint,
		advisoryLock: advisoryLock,
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

	if t.config.BronzeSource.Mode == "grpc" {
		return t.startGRPC()
	}
	return t.startPolling(pollInterval)
}

func (t *Transformer) startPolling(pollInterval time.Duration) error {
	log.Printf("✅ Transformer ready - polling for new contract events (interval: %v)...", pollInterval)

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

		log.Printf("✅ Indexed %d contract-ledger pairs (ledgers %d→%d)", rowsWritten, lastLedger+1, endLedger)

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
		log.Printf("⚠️  indexed forward despite retention gap; historical contract-event coverage is incomplete until backfilled from cold: %s", retentionGapMessage)
	}

	return nil
}

// updateStats updates transformer statistics
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

	if err := t.catalogDB.Close(); err != nil {
		log.Printf("Error closing Catalog DB: %v", err)
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
