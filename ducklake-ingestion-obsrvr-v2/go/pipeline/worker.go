package pipeline

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/withObsrvr/ttp-processor-demo/ducklake-ingestion-obsrvr-v2/go/metrics"
)

// WorkerFunc is the function signature for processing a batch.
// This allows the main ingester to provide its own processing logic.
type WorkerFunc func(ctx context.Context, batch *Batch) (*BatchResult, error)

// Worker processes batches in parallel.
type Worker struct {
	id       int
	workFunc WorkerFunc
	input    <-chan *Batch
	output   chan<- *BatchResult
	metrics  *metrics.Metrics
	wg       *sync.WaitGroup
}

// NewWorker creates a new worker.
func NewWorker(id int, workFunc WorkerFunc, input <-chan *Batch, output chan<- *BatchResult, m *metrics.Metrics, wg *sync.WaitGroup) *Worker {
	return &Worker{
		id:       id,
		workFunc: workFunc,
		input:    input,
		output:   output,
		metrics:  m,
		wg:       wg,
	}
}

// Run starts the worker processing loop.
func (w *Worker) Run(ctx context.Context) {
	defer w.wg.Done()

	log.Printf("[worker-%d] Started", w.id)

	for {
		select {
		case <-ctx.Done():
			log.Printf("[worker-%d] Shutting down: %v", w.id, ctx.Err())
			return

		case batch, ok := <-w.input:
			if !ok {
				log.Printf("[worker-%d] Input channel closed, exiting", w.id)
				return
			}

			// Update metrics
			if w.metrics != nil {
				w.metrics.SetActiveWorkers(1) // Will be summed across workers
			}

			// Process the batch
			start := time.Now()
			result, err := w.workFunc(ctx, batch)

			if err != nil {
				log.Printf("[worker-%d] Error processing batch %d-%d: %v",
					w.id, batch.Start, batch.End, err)

				// Create error result
				result = &BatchResult{
					Batch:          batch,
					Error:          err,
					ProcessingTime: time.Since(start),
				}

				if w.metrics != nil {
					w.metrics.RecordError("extraction")
				}
			} else {
				result.ProcessingTime = time.Since(start)
				log.Printf("[worker-%d] Completed batch %d-%d (%d rows) in %v",
					w.id, batch.Start, batch.End, result.TotalRows, result.ProcessingTime)
			}

			// Record metrics
			if w.metrics != nil {
				w.metrics.RecordBatchDuration(result.ProcessingTime)
				w.metrics.RecordBatchCompleted(err == nil)
			}

			// Send result
			select {
			case w.output <- result:
			case <-ctx.Done():
				return
			}
		}
	}
}

// Pool manages a pool of workers.
type Pool struct {
	workers    int
	input      chan *Batch
	output     chan *BatchResult
	metrics    *metrics.Metrics
	wg         sync.WaitGroup
	activeWg   sync.WaitGroup
	workerList []*Worker
}

// PoolConfig holds configuration for the worker pool.
type PoolConfig struct {
	Workers   int `yaml:"workers"`    // Number of parallel workers (default: 4)
	QueueSize int `yaml:"queue_size"` // Size of input queue (default: workers * 2)
}

// ApplyDefaults sets default values for pool config.
func (c *PoolConfig) ApplyDefaults() {
	if c.Workers <= 0 {
		c.Workers = 4
	}
	if c.QueueSize <= 0 {
		c.QueueSize = c.Workers * 2
	}
}

// NewPool creates a new worker pool.
func NewPool(cfg PoolConfig, workFunc WorkerFunc, m *metrics.Metrics) *Pool {
	cfg.ApplyDefaults()

	p := &Pool{
		workers: cfg.Workers,
		input:   make(chan *Batch, cfg.QueueSize),
		output:  make(chan *BatchResult, cfg.QueueSize),
		metrics: m,
	}

	// Create workers
	for i := 0; i < cfg.Workers; i++ {
		worker := NewWorker(i, workFunc, p.input, p.output, m, &p.wg)
		p.workerList = append(p.workerList, worker)
	}

	return p
}

// Start launches all workers.
func (p *Pool) Start(ctx context.Context) {
	p.wg.Add(len(p.workerList))

	for _, worker := range p.workerList {
		go worker.Run(ctx)
	}

	log.Printf("[pool] Started %d workers", len(p.workerList))

	// Update metrics
	if p.metrics != nil {
		p.metrics.SetActiveWorkers(p.workers)
	}
}

// Submit sends a batch to the pool for processing.
// Blocks if the queue is full.
func (p *Pool) Submit(ctx context.Context, batch *Batch) error {
	select {
	case p.input <- batch:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Results returns the channel of batch results.
func (p *Pool) Results() <-chan *BatchResult {
	return p.output
}

// Shutdown stops all workers and waits for them to finish.
func (p *Pool) Shutdown() {
	// Close input to signal workers to stop
	close(p.input)

	// Wait for all workers to finish
	p.wg.Wait()

	// Close output channel
	close(p.output)

	log.Printf("[pool] All workers stopped")

	// Update metrics
	if p.metrics != nil {
		p.metrics.SetActiveWorkers(0)
	}
}

// DrainResults drains all remaining results from the output channel.
// Returns results in the order received (may not be in ledger order).
func (p *Pool) DrainResults() []*BatchResult {
	var results []*BatchResult
	for result := range p.output {
		results = append(results, result)
	}
	return results
}

// WorkerCount returns the number of workers in the pool.
func (p *Pool) WorkerCount() int {
	return p.workers
}

// QueuedBatches returns the number of batches waiting in the queue.
func (p *Pool) QueuedBatches() int {
	return len(p.input)
}
