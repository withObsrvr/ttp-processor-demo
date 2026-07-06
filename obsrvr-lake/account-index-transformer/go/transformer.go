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
	config        *Config
	silverReader  *SilverHotReader
	indexWriter   *IndexWriter
	pgIndexWriter *PostgresIndexWriter
	feedWriter    *ServingFeedWriter
	checkpoint    *CheckpointManager
	advisoryLock  *AdvisoryLock
	healthServer  *HealthServer
	stats         TransformerStats
	mu            sync.RWMutex
	stopChan      chan struct{}
	writeCount    int
}

// NewTransformer creates a new Index Plane transformer
func NewTransformer(config *Config, silverDB *sql.DB, catalogDB *sql.DB) (*Transformer, error) {
	if silverDB == nil {
		return nil, fmt.Errorf("silver_hot connection is nil")
	}
	silverReader := NewSilverHotReader(silverDB)

	indexConfig := config.IndexConfig()
	indexWriter, err := NewIndexWriter(&indexConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create account Index writer: %w", err)
	}
	var pgIndexWriter *PostgresIndexWriter
	if config.IndexPostgres.Enabled {
		pgIndexWriter = NewPostgresIndexWriter(catalogDB, config.IndexPostgres, config.IndexCold.PartitionSize)
	}
	var feedWriter *ServingFeedWriter
	if config.ServingFeed.Enabled {
		feedWriter = NewServingFeedWriter(silverDB, config.ServingFeed)
	}

	checkpoint, err := NewCheckpointManager(catalogDB, &config.Checkpoint)
	if err != nil {
		_ = indexWriter.Close()
		return nil, fmt.Errorf("failed to create account checkpoint manager: %w", err)
	}

	lockResource := fmt.Sprintf("%s|%s|%s|%s", config.Service.Name, config.Catalog.Database, config.IndexCold.SchemaName, config.IndexCold.TableName)
	advisoryLock := NewAdvisoryLock(catalogDB, "account-index-transformer", lockResource)

	// Create health server
	healthServer := NewHealthServer(config.Health.Port)

	return &Transformer{
		config:        config,
		silverReader:  silverReader,
		indexWriter:   indexWriter,
		pgIndexWriter: pgIndexWriter,
		feedWriter:    feedWriter,
		checkpoint:    checkpoint,
		advisoryLock:  advisoryLock,
		healthServer:  healthServer,
		stopChan:      make(chan struct{}),
	}, nil
}

// Start starts the transformation loop
func (t *Transformer) Start() error {
	log.Println("🚀 Starting Account Index Transformer")
	log.Printf("Poll Interval: %v", t.config.GetPollInterval())
	log.Printf("Batch Size: %d ledgers", t.config.Service.BatchSize)
	log.Printf("Account buckets: %d", t.config.AccountBucketCount())
	log.Println("Target: Index Plane (DuckLake)")
	if t.pgIndexWriter != nil {
		log.Printf("Postgres account index sink: enabled mode=%s (%s.%s)", t.config.IndexPostgres.Mode, t.config.IndexPostgres.Schema, t.config.IndexPostgres.Table)
	}
	if t.feedWriter != nil {
		log.Printf("Serving feed sink: enabled (%s.%s)", t.config.ServingFeed.Schema, t.config.ServingFeed.TransactionsTable)
	}

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

	lastLedger, err := t.checkpoint.Load(ctx)
	if err != nil {
		return fmt.Errorf("failed to load account checkpoint: %w", err)
	}
	if lastLedger == 0 {
		log.Println("⚠️  No account checkpoint found, starting from beginning")
	} else {
		log.Printf("📍 Resuming account index from ledger: %d", lastLedger)
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
	ctx := context.Background()
	err := t.runAccountLedgerCycle(ctx)
	if t.feedWriter != nil {
		t.runServingFeedCycle(ctx)
	}
	return err
}

func (t *Transformer) runAccountLedgerCycle(ctx context.Context) error {
	startTime := time.Now()
	lastLedger, err := t.checkpoint.Load(ctx)
	if err != nil {
		return fmt.Errorf("failed to load account ledger checkpoint: %w", err)
	}
	minLedger, maxLedger, err := t.silverReader.GetLedgerBounds(ctx)
	if err != nil {
		return fmt.Errorf("failed to get account ledger bounds from Silver Hot: %w", err)
	}
	retentionGapDetected, retentionGapMessage := describeRetentionGap(lastLedger, minLedger)
	if retentionGapDetected {
		if !t.config.Service.AllowRetentionGapStart {
			return fmt.Errorf("account ledger retention gap detected and allow_retention_gap_start=false: %s", retentionGapMessage)
		}
		log.Printf("⚠️  account ledger retention gap detected but continuing because allow_retention_gap_start=true: %s", retentionGapMessage)
		if minLedger > 1 && lastLedger < minLedger-1 {
			lastLedger = minLedger - 1
			if err := t.checkpoint.Save(ctx, lastLedger); err != nil {
				return fmt.Errorf("failed to advance account checkpoint over hot retention gap: %w", err)
			}
			log.Printf("📍 Advanced account checkpoint to oldest hot ledger boundary: %d", lastLedger)
		}
	}
	if maxLedger <= lastLedger {
		t.updateStats(lastLedger, minLedger, maxLedger, retentionGapDetected, retentionGapMessage, time.Now(), time.Since(startTime))
		return nil
	}
	endLedger := lastLedger + t.config.Service.BatchSize
	if endLedger > maxLedger {
		endLedger = maxLedger
	}
	log.Printf("📊 Account ledger data available (ledgers %d to %d)", lastLedger+1, endLedger)

	rows, err := t.silverReader.ReadAccountLedgerRanges(ctx, lastLedger, endLedger, t.config.IndexCold.PartitionSize, t.config.AccountBucketCount())
	if err != nil {
		return fmt.Errorf("failed to read account ledger ranges: %w", err)
	}
	if len(rows) > 0 {
		rowsWritten, err := t.writeAccountLedgerRangesWithRecovery(ctx, rows)
		if err != nil {
			return fmt.Errorf("failed to write account ledger ranges: %w", err)
		}
		log.Printf("✅ Indexed %d account ledger-range rows (ledgers %d→%d)", rowsWritten, lastLedger+1, endLedger)
		if err := t.writePostgresAccountIndex(ctx, rows, endLedger); err != nil {
			if t.config.IndexPostgres.Mode == "primary" {
				return err
			}
			log.Printf("⚠️  Postgres account index mirror skipped: %v", err)
		}
		t.writeCount++
		if t.config.Maintenance.Enabled && t.writeCount >= t.config.Maintenance.EveryNWrites {
			log.Printf("🔧 Account index flush + maintenance (write #%d)...", t.writeCount)
			if _, err := t.indexWriter.FlushInlinedData(); err != nil {
				log.Printf("⚠️  Account index flush failed (non-fatal): %v", err)
			}
			if err := t.indexWriter.RunCheckpoint(ctx, t.config.Maintenance.MaxCompactedFiles); err != nil {
				log.Printf("⚠️  Account index maintenance failed (non-fatal): %v", err)
			}
			t.writeCount = 0
		}
	}
	if err := t.checkpoint.Save(ctx, endLedger); err != nil {
		return fmt.Errorf("failed to save account ledger checkpoint: %w", err)
	}
	duration := time.Since(startTime)
	t.updateStats(endLedger, minLedger, maxLedger, retentionGapDetected, retentionGapMessage, time.Now(), duration)
	t.incrementTotal()
	log.Printf("✅ Account ledger cycle completed in %s", duration.Round(time.Millisecond))
	return nil
}

func (t *Transformer) writePostgresAccountIndex(ctx context.Context, rows []AccountLedgerIndex, endLedger int64) error {
	if t.pgIndexWriter == nil {
		return nil
	}
	written, err := t.pgIndexWriter.WriteAccountLedgerRanges(ctx, rows, endLedger)
	if err != nil {
		return fmt.Errorf("failed to write Postgres account index mirror: %w", err)
	}
	log.Printf("✅ Mirrored %d account ledger-range rows to Postgres index (through ledger %d)", written, endLedger)
	return nil
}

func (t *Transformer) runServingFeedCycle(ctx context.Context) {
	minLedger, maxLedger, err := t.silverReader.GetLedgerBounds(ctx)
	if err != nil {
		log.Printf("⚠️  Serving feed skipped: failed to get Silver Hot bounds: %v", err)
		return
	}
	if maxLedger == 0 {
		return
	}
	fallback := int64(0)
	if minLedger > 1 {
		fallback = minLedger - 1
	}
	lastLedger, err := t.feedWriter.LoadCheckpoint(ctx, fallback)
	if err != nil {
		log.Printf("⚠️  Serving feed skipped: failed to load checkpoint: %v", err)
		return
	}
	if lastLedger < fallback {
		lastLedger = fallback
	}
	if maxLedger <= lastLedger {
		return
	}
	endLedger := lastLedger + t.config.Service.BatchSize
	if endLedger > maxLedger {
		endLedger = maxLedger
	}
	rows, err := t.silverReader.ReadAccountFeedRows(ctx, lastLedger, endLedger)
	if err != nil {
		log.Printf("⚠️  Serving feed skipped: failed to read rows for ledgers %d-%d: %v", lastLedger+1, endLedger, err)
		return
	}
	written, err := t.feedWriter.Write(ctx, rows, lastLedger+1, endLedger)
	if err != nil {
		log.Printf("⚠️  Serving feed skipped: failed to write rows for ledgers %d-%d: %v", lastLedger+1, endLedger, err)
		return
	}
	log.Printf("✅ Serving feed wrote %d account operation rows (ledgers %d→%d)", written, lastLedger+1, endLedger)
}

func (t *Transformer) writeAccountLedgerRangesWithRecovery(ctx context.Context, rows []AccountLedgerIndex) (int64, error) {
	rowsWritten, err := t.indexWriter.WriteAccountLedgerRanges(ctx, rows)
	if err == nil || !IsFatalDuckDBWriterError(err) {
		return rowsWritten, err
	}

	log.Printf("⚠️  Account index writer hit fatal DuckDB state; reopening writer and retrying batch once: %v", err)
	if reopenErr := t.reopenIndexWriter(); reopenErr != nil {
		return 0, fmt.Errorf("%w; additionally failed to reopen account index writer: %v", err, reopenErr)
	}
	return t.indexWriter.WriteAccountLedgerRanges(ctx, rows)
}

func (t *Transformer) reopenIndexWriter() error {
	if t.indexWriter != nil {
		if err := t.indexWriter.Close(); err != nil {
			log.Printf("⚠️  Failed to close poisoned account index writer: %v", err)
		}
	}

	indexConfig := t.config.IndexConfig()
	writer, err := NewIndexWriter(&indexConfig)
	if err != nil {
		return err
	}
	t.indexWriter = writer
	t.healthServer.SetIndexWriter(writer)
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
	if t.silverReader != nil {
		if err := t.silverReader.Close(); err != nil {
			log.Printf("Error closing Silver Hot reader: %v", err)
		}
	}

	if t.indexWriter != nil {
		if err := t.indexWriter.Close(); err != nil {
			log.Printf("Error closing Index writer: %v", err)
		}
	}

	if t.checkpoint != nil {
		if err := t.checkpoint.Close(); err != nil {
			log.Printf("Error closing checkpoint manager: %v", err)
		}
	}
	log.Println("✅ Transformer stopped")
	return nil
}

func describeRetentionGap(lastLedger, minLedger int64) (bool, string) {
	if minLedger <= 0 {
		return false, ""
	}
	if lastLedger == 0 && minLedger > 1 {
		return true, fmt.Sprintf("no checkpoint exists and Silver Hot begins at ledger %d; earlier ledgers are not available from hot storage", minLedger)
	}
	if lastLedger > 0 && lastLedger < minLedger-1 {
		return true, fmt.Sprintf("checkpoint ledger %d is behind oldest hot ledger %d", lastLedger, minLedger)
	}
	return false, ""
}
