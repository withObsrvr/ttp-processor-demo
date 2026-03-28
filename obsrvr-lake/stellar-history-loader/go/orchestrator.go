package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"
)

// OrchestratorConfig holds configuration for the orchestrator.
type OrchestratorConfig struct {
	StartLedger       uint32
	EndLedger         uint32
	NumWorkers        int
	OutputDir         string
	BatchSize         int
	NetworkPassphrase string
	StorageType       string
	Bucket            string
	LedgersPerFile    uint32
	FilesPerPartition uint32
}

// Orchestrator manages parallel workers for history loading.
type Orchestrator struct {
	config     OrchestratorConfig
	progress   *ProgressTracker
	checkpoint *CheckpointManager
	lineage    *LineageWriter
}

// Shard represents a range of ledgers assigned to a worker.
type Shard struct {
	ID          int
	StartLedger uint32
	EndLedger   uint32
}

// ProgressTracker tracks overall and per-worker progress.
type ProgressTracker struct {
	mu                sync.Mutex
	totalLedgers      uint64
	processedByWorker map[int]uint64
	startTime         time.Time
	lastReport        time.Time
}

// NewOrchestrator creates a new Orchestrator with the given configuration.
func NewOrchestrator(config OrchestratorConfig) *Orchestrator {
	checkpoint, _ := NewCheckpointManager(config.OutputDir, config)

	return &Orchestrator{
		config: config,
		progress: &ProgressTracker{
			processedByWorker: make(map[int]uint64),
		},
		checkpoint: checkpoint,
		lineage:    NewLineageWriter(config.OutputDir, Version),
	}
}

// Run splits the ledger range into shards and runs workers in parallel.
func (o *Orchestrator) Run() error {
	shards := o.splitIntoShards()

	log.Printf("Splitting range [%d, %d] into %d shards", o.config.StartLedger, o.config.EndLedger, len(shards))
	for _, s := range shards {
		shardSize := s.EndLedger - s.StartLedger + 1
		log.Printf("  Shard %d: ledgers %d - %d (%d ledgers)", s.ID, s.StartLedger, s.EndLedger, shardSize)
	}

	// Create shared ledger source (for XDR/FS modes).
	// GCS/S3 create per-worker sources since the archive backend is sequential.
	var source LedgerSource
	if o.config.StorageType != "GCS" && o.config.StorageType != "S3" {
		var err error
		source, err = NewLedgerSourceFromConfig(o.config)
		if err != nil {
			return fmt.Errorf("create ledger source: %w", err)
		}
		defer source.Close()

		// If the source needs range preparation (RPC), do it now
		type rangePreparer interface {
			PrepareRange(ctx context.Context, start, end uint32) error
		}
		if p, ok := source.(rangePreparer); ok {
			log.Printf("Preparing range [%d, %d]...", o.config.StartLedger, o.config.EndLedger)
			if err := p.PrepareRange(context.Background(), o.config.StartLedger, o.config.EndLedger); err != nil {
				return fmt.Errorf("prepare range: %w", err)
			}
		}
	}

	o.progress.startTime = time.Now()
	o.progress.lastReport = time.Now()

	var wg sync.WaitGroup
	errCh := make(chan error, len(shards))

	// Start progress reporter
	stopProgress := make(chan struct{})
	var progressWg sync.WaitGroup
	progressWg.Add(1)
	go func() {
		defer progressWg.Done()
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				o.progress.PrintStatus()
			case <-stopProgress:
				return
			}
		}
	}()

	// Launch workers
	for _, shard := range shards {
		wg.Add(1)
		go func(s Shard) {
			defer wg.Done()
			if err := o.runWorker(s, source); err != nil {
				errCh <- fmt.Errorf("worker %d failed: %w", s.ID, err)
			}
		}(shard)
	}

	// Wait for all workers to finish
	wg.Wait()
	close(stopProgress)
	progressWg.Wait()
	close(errCh)

	// Collect errors
	var errs []error
	for err := range errCh {
		errs = append(errs, err)
	}

	// Flush lineage records
	if err := o.lineage.Flush(); err != nil {
		log.Printf("Warning: lineage flush failed: %v", err)
	}

	// Print final status
	o.progress.PrintStatus()

	if len(errs) > 0 {
		for _, err := range errs {
			log.Printf("ERROR: %v", err)
		}
		return fmt.Errorf("%d worker(s) failed", len(errs))
	}

	return nil
}

// splitIntoShards divides the ledger range evenly across workers.
func (o *Orchestrator) splitIntoShards() []Shard {
	totalLedgers := uint64(o.config.EndLedger-o.config.StartLedger) + 1
	numWorkers := o.config.NumWorkers

	// Don't create more shards than ledgers
	if uint64(numWorkers) > totalLedgers {
		numWorkers = int(totalLedgers)
	}

	shards := make([]Shard, 0, numWorkers)
	ledgersPerShard := totalLedgers / uint64(numWorkers)
	remainder := totalLedgers % uint64(numWorkers)

	current := o.config.StartLedger
	for i := 0; i < numWorkers; i++ {
		shardSize := ledgersPerShard
		// Distribute remainder across the first shards
		if uint64(i) < remainder {
			shardSize++
		}

		shard := Shard{
			ID:          i,
			StartLedger: current,
			EndLedger:   current + uint32(shardSize) - 1,
		}
		shards = append(shards, shard)
		current = shard.EndLedger + 1
	}

	return shards
}

// runWorker creates a Worker and runs it for the given shard.
func (o *Orchestrator) runWorker(shard Shard, source LedgerSource) error {
	log.Printf("[Worker %d] Starting: ledgers %d - %d", shard.ID, shard.StartLedger, shard.EndLedger)

	writer, err := NewParquetWriter(o.config.OutputDir, shard.ID, Version)
	if err != nil {
		return fmt.Errorf("create parquet writer: %w", err)
	}
	defer writer.Close()

	worker := NewWorker(shard.ID, shard, o.config, o.progress, writer, source, o.checkpoint, o.lineage)
	ctx := context.Background()

	return worker.Run(ctx)
}

// ReportProgress records that a worker has processed additional ledgers.
func (pt *ProgressTracker) ReportProgress(workerID int, ledgersProcessed uint64) {
	pt.mu.Lock()
	defer pt.mu.Unlock()

	pt.totalLedgers += ledgersProcessed
	pt.processedByWorker[workerID] += ledgersProcessed
}

// PrintStatus prints the current progress, throughput, and per-worker breakdown.
func (pt *ProgressTracker) PrintStatus() {
	pt.mu.Lock()
	defer pt.mu.Unlock()

	elapsed := time.Since(pt.startTime)
	if elapsed == 0 {
		return
	}

	throughput := float64(pt.totalLedgers) / elapsed.Seconds()

	fmt.Printf("[Progress] %d ledgers processed | %.1f ledgers/sec | elapsed %s",
		pt.totalLedgers, throughput, elapsed.Round(time.Second))

	// Per-worker breakdown
	if len(pt.processedByWorker) > 0 {
		fmt.Print(" | per-worker: ")
		first := true
		for id := 0; id < len(pt.processedByWorker); id++ {
			count, ok := pt.processedByWorker[id]
			if !ok {
				continue
			}
			if !first {
				fmt.Print(", ")
			}
			fmt.Printf("w%d=%d", id, count)
			first = false
		}
	}
	fmt.Println()

	pt.lastReport = time.Now()
}
