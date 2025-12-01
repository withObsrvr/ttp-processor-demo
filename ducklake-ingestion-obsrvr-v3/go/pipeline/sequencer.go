package pipeline

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/withObsrvr/ttp-processor-demo/ducklake-ingestion-obsrvr-v3/go/metrics"
)

// CommitFunc is called when a batch is ready to be committed.
// This allows the main ingester to handle audit layer commits.
type CommitFunc func(result *BatchResult) error

// Sequencer ensures batches are committed in order even if they complete out of order.
// This is critical for PAS hash chaining and checkpoint consistency.
type Sequencer struct {
	mu sync.Mutex

	// pending holds out-of-order results waiting to be committed
	pending map[uint32]*BatchResult

	// nextExpected is the next batch start sequence we expect to commit
	nextExpected uint32

	// commitFunc is called for each batch ready to commit
	commitFunc CommitFunc

	// metrics for observability
	metrics *metrics.Metrics

	// stats
	totalCommitted int
	lastCommitTime time.Time
}

// NewSequencer creates a new sequencer.
func NewSequencer(startLedger uint32, commitFunc CommitFunc, m *metrics.Metrics) *Sequencer {
	return &Sequencer{
		pending:      make(map[uint32]*BatchResult),
		nextExpected: startLedger,
		commitFunc:   commitFunc,
		metrics:      m,
	}
}

// Submit adds a batch result to the sequencer.
// If this batch or subsequent batches can be committed in order, they will be.
func (s *Sequencer) Submit(result *BatchResult) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	batchStart := result.Batch.Start

	// Check if this is an error result
	if result.Error != nil {
		log.Printf("[sequencer] Received error result for batch %d-%d: %v",
			result.Batch.Start, result.Batch.End, result.Error)
		// Store it anyway - we might want to handle partial failures
		s.pending[batchStart] = result
		return result.Error
	}

	// Store the result
	s.pending[batchStart] = result

	// Try to commit consecutive batches
	return s.commitReady()
}

// commitReady commits all consecutive ready batches starting from nextExpected.
// Must be called with mu held.
func (s *Sequencer) commitReady() error {
	for {
		result, ok := s.pending[s.nextExpected]
		if !ok {
			// No result for next expected batch yet
			break
		}

		// Skip error batches
		if result.Error != nil {
			log.Printf("[sequencer] Skipping failed batch %d-%d",
				result.Batch.Start, result.Batch.End)
			delete(s.pending, s.nextExpected)
			s.nextExpected = result.Batch.End + 1
			continue
		}

		// Commit this batch
		start := time.Now()
		if err := s.commitFunc(result); err != nil {
			return fmt.Errorf("failed to commit batch %d-%d: %w",
				result.Batch.Start, result.Batch.End, err)
		}

		commitDuration := time.Since(start)
		log.Printf("[sequencer] Committed batch %d-%d (%d rows) in %v",
			result.Batch.Start, result.Batch.End, result.TotalRows, commitDuration)

		// Update state
		delete(s.pending, s.nextExpected)
		s.nextExpected = result.Batch.End + 1
		s.totalCommitted++
		s.lastCommitTime = time.Now()

		// Update metrics
		if s.metrics != nil {
			s.metrics.SetPendingBatches(len(s.pending))
			s.metrics.SetCurrentLedger(result.Batch.End)
		}
	}

	return nil
}

// PendingCount returns the number of out-of-order results waiting.
func (s *Sequencer) PendingCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.pending)
}

// NextExpected returns the next expected batch start sequence.
func (s *Sequencer) NextExpected() uint32 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.nextExpected
}

// TotalCommitted returns the total number of batches committed.
func (s *Sequencer) TotalCommitted() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.totalCommitted
}

// LastCommitTime returns the time of the last commit.
func (s *Sequencer) LastCommitTime() time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastCommitTime
}

// Flush commits all remaining pending batches if they are consecutive.
// Returns the number of batches that couldn't be committed due to gaps.
func (s *Sequencer) Flush() (uncommitted int, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.commitReady(); err != nil {
		return len(s.pending), err
	}

	return len(s.pending), nil
}

// Pipeline orchestrates the parallel processing pipeline.
type Pipeline struct {
	pool      *Pool
	sequencer *Sequencer
	metrics   *metrics.Metrics

	// Batch tracking
	batchIDCounter uint64
	batchSize      int

	// Stats
	startTime      time.Time
	ledgersTotal   int
	ledgersStarted int
}

// PipelineConfig holds configuration for the pipeline.
type PipelineConfig struct {
	Pool      PoolConfig
	BatchSize int `yaml:"batch_size"` // Ledgers per batch (default: 10)
}

// ApplyDefaults sets default values for pipeline config.
func (c *PipelineConfig) ApplyDefaults() {
	c.Pool.ApplyDefaults()
	if c.BatchSize <= 0 {
		c.BatchSize = 10
	}
}

// NewPipeline creates a new processing pipeline.
func NewPipeline(cfg PipelineConfig, workFunc WorkerFunc, commitFunc CommitFunc, startLedger uint32, m *metrics.Metrics) *Pipeline {
	cfg.ApplyDefaults()

	pool := NewPool(cfg.Pool, workFunc, m)
	sequencer := NewSequencer(startLedger, commitFunc, m)

	return &Pipeline{
		pool:      pool,
		sequencer: sequencer,
		metrics:   m,
		batchSize: cfg.BatchSize,
		startTime: time.Now(),
	}
}

// Start launches the pipeline.
func (p *Pipeline) Start(ctx context.Context) {
	p.pool.Start(ctx)

	// Start result processor goroutine
	go p.processResults(ctx)
}

// processResults reads results from the pool and submits to sequencer.
func (p *Pipeline) processResults(ctx context.Context) {
	for result := range p.pool.Results() {
		if err := p.sequencer.Submit(result); err != nil {
			log.Printf("[pipeline] Error submitting result to sequencer: %v", err)
		}
	}
}

// SubmitLedgerRange breaks a range into batches and submits them.
func (p *Pipeline) SubmitLedgerRange(ctx context.Context, start, end uint32) error {
	p.ledgersTotal = int(end - start + 1)

	// Break into batches
	for batchStart := start; batchStart <= end; batchStart += uint32(p.batchSize) {
		batchEnd := batchStart + uint32(p.batchSize) - 1
		if batchEnd > end {
			batchEnd = end
		}

		p.batchIDCounter++
		batch := &Batch{
			Start:     batchStart,
			End:       batchEnd,
			ID:        p.batchIDCounter,
			CreatedAt: time.Now(),
		}

		if err := p.pool.Submit(ctx, batch); err != nil {
			return fmt.Errorf("failed to submit batch %d-%d: %w", batchStart, batchEnd, err)
		}

		p.ledgersStarted += batch.Size()
	}

	return nil
}

// Shutdown gracefully stops the pipeline.
func (p *Pipeline) Shutdown() (uncommitted int, err error) {
	p.pool.Shutdown()
	return p.sequencer.Flush()
}

// Stats returns pipeline statistics.
func (p *Pipeline) Stats() PipelineStats {
	elapsed := time.Since(p.startTime)
	committed := p.sequencer.TotalCommitted()

	return PipelineStats{
		Workers:          p.pool.WorkerCount(),
		BatchSize:        p.batchSize,
		TotalLedgers:     p.ledgersTotal,
		LedgersStarted:   p.ledgersStarted,
		BatchesCommitted: committed,
		PendingBatches:   p.sequencer.PendingCount(),
		QueuedBatches:    p.pool.QueuedBatches(),
		ElapsedTime:      elapsed,
		LedgersPerSec:    float64(p.ledgersStarted) / elapsed.Seconds(),
	}
}

// PipelineStats holds pipeline statistics.
type PipelineStats struct {
	Workers          int
	BatchSize        int
	TotalLedgers     int
	LedgersStarted   int
	BatchesCommitted int
	PendingBatches   int
	QueuedBatches    int
	ElapsedTime      time.Duration
	LedgersPerSec    float64
}
