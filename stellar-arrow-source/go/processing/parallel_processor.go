package processing

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/stellar/go/xdr"
	"github.com/withObsrvr/ttp-processor-demo/stellar-arrow-source/logging"
	"github.com/withObsrvr/ttp-processor-demo/stellar-arrow-source/metrics"
	"github.com/withObsrvr/ttp-processor-demo/stellar-arrow-source/schema"
	rawledger "github.com/withObsrvr/ttp-processor-demo/stellar-live-source-datalake/gen/raw_ledger_service"
)

// ParallelProcessor processes XDR to Arrow conversions in parallel
type ParallelProcessor struct {
	allocator     memory.Allocator
	schemaManager *schema.SchemaManager
	logger        *logging.ComponentLogger
	metrics       *metrics.Collector
	
	// Worker pool configuration
	numWorkers    int
	workerPool    *WorkerPool
	
	// Processing pipeline
	inputChan     chan *ProcessingTask
	outputChan    chan *ProcessingResult
	errorChan     chan error
	
	// Synchronization
	wg            sync.WaitGroup
	ctx           context.Context
	cancel        context.CancelFunc
	
	// Performance tracking
	tasksProcessed int64
	tasksErrored   int64
}

// ProcessingTask represents a ledger to process
type ProcessingTask struct {
	ID        string
	RawLedger *rawledger.RawLedger
	Priority  int
}

// ProcessingResult contains the converted Arrow data
type ProcessingResult struct {
	TaskID    string
	Record    arrow.Record
	Sequence  uint32
	Duration  time.Duration
	Error     error
}

// WorkerPool manages parallel workers
type WorkerPool struct {
	workers   []*Worker
	taskQueue chan *ProcessingTask
	results   chan *ProcessingResult
	wg        sync.WaitGroup
}

// Worker processes tasks
type Worker struct {
	id            int
	allocator     memory.Allocator
	schemaManager *schema.SchemaManager
	logger        *logging.ComponentLogger
	metrics       *metrics.Collector
	taskQueue     chan *ProcessingTask
	results       chan *ProcessingResult
}

// NewParallelProcessor creates a new parallel processor
func NewParallelProcessor(allocator memory.Allocator, schemaManager *schema.SchemaManager,
	logger *logging.ComponentLogger, metrics *metrics.Collector, numWorkers int) *ParallelProcessor {
	
	if numWorkers <= 0 {
		numWorkers = runtime.NumCPU()
	}
	
	ctx, cancel := context.WithCancel(context.Background())
	
	p := &ParallelProcessor{
		allocator:     allocator,
		schemaManager: schemaManager,
		logger:        logger,
		metrics:       metrics,
		numWorkers:    numWorkers,
		inputChan:     make(chan *ProcessingTask, numWorkers*2),
		outputChan:    make(chan *ProcessingResult, numWorkers),
		errorChan:     make(chan error, numWorkers),
		ctx:           ctx,
		cancel:        cancel,
	}
	
	// Create worker pool
	p.workerPool = &WorkerPool{
		taskQueue: p.inputChan,
		results:   p.outputChan,
		workers:   make([]*Worker, numWorkers),
	}
	
	// Initialize workers
	for i := 0; i < numWorkers; i++ {
		worker := &Worker{
			id:            i,
			allocator:     allocator,
			schemaManager: schemaManager,
			logger:        logger,
			metrics:       metrics,
			taskQueue:     p.inputChan,
			results:       p.outputChan,
		}
		p.workerPool.workers[i] = worker
	}
	
	logger.Info().
		Int("num_workers", numWorkers).
		Msg("Created parallel processor")
	
	return p
}

// Start starts the parallel processing pipeline
func (p *ParallelProcessor) Start() {
	// Start all workers
	for _, worker := range p.workerPool.workers {
		p.workerPool.wg.Add(1)
		go worker.run(p.ctx, &p.workerPool.wg)
	}
	
	p.logger.Info().
		Int("workers", p.numWorkers).
		Msg("Started parallel processing pipeline")
}

// Stop stops the parallel processor
func (p *ParallelProcessor) Stop() {
	p.logger.Info().
		Msg("Stopping parallel processor")
	
	// Cancel context to signal workers to stop
	p.cancel()
	
	// Close input channel
	close(p.inputChan)
	
	// Wait for all workers to finish
	p.workerPool.wg.Wait()
	
	// Close output channels
	close(p.outputChan)
	close(p.errorChan)
	
	p.logger.Info().
		Int64("tasks_processed", atomic.LoadInt64(&p.tasksProcessed)).
		Int64("tasks_errored", atomic.LoadInt64(&p.tasksErrored)).
		Msg("Parallel processor stopped")
}

// SubmitTask submits a task for processing
func (p *ParallelProcessor) SubmitTask(task *ProcessingTask) error {
	select {
	case p.inputChan <- task:
		return nil
	case <-p.ctx.Done():
		return fmt.Errorf("processor is shutting down")
	default:
		return fmt.Errorf("task queue is full")
	}
}

// GetResult retrieves a processing result
func (p *ParallelProcessor) GetResult() (*ProcessingResult, error) {
	timer := time.NewTimer(100 * time.Millisecond)
	defer timer.Stop()
	
	select {
	case result := <-p.outputChan:
		if result.Error != nil {
			atomic.AddInt64(&p.tasksErrored, 1)
		} else {
			atomic.AddInt64(&p.tasksProcessed, 1)
		}
		return result, nil
	case err := <-p.errorChan:
		return nil, err
	case <-timer.C:
		return nil, nil // No result available
	}
}

// ProcessBatch processes a batch of ledgers in parallel
func (p *ParallelProcessor) ProcessBatch(ctx context.Context, ledgers []*rawledger.RawLedger) ([]arrow.Record, error) {
	numLedgers := len(ledgers)
	results := make([]arrow.Record, 0, numLedgers)
	resultMap := make(map[uint32]*ProcessingResult)
	
	// Submit all tasks
	for i, ledger := range ledgers {
		task := &ProcessingTask{
			ID:        fmt.Sprintf("batch-%d", i),
			RawLedger: ledger,
			Priority:  numLedgers - i, // Higher priority for earlier ledgers
		}
		
		if err := p.SubmitTask(task); err != nil {
			return nil, fmt.Errorf("failed to submit task: %w", err)
		}
	}
	
	// Collect results
	received := 0
	timer := time.NewTimer(30 * time.Second)
	defer timer.Stop()
	
	for received < numLedgers {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-timer.C:
			return nil, fmt.Errorf("timeout processing batch")
		default:
			result, err := p.GetResult()
			if err != nil {
				return nil, err
			}
			if result != nil {
				if result.Error != nil {
					p.logger.Error().
						Err(result.Error).
						Str("task_id", result.TaskID).
						Msg("Task processing failed")
					// Continue processing other tasks
				} else {
					resultMap[result.Sequence] = result
				}
				received++
			}
		}
	}
	
	// Sort results by sequence
	for _, ledger := range ledgers {
		if result, exists := resultMap[ledger.Sequence]; exists {
			results = append(results, result.Record)
		}
	}
	
	return results, nil
}

// Worker implementation

func (w *Worker) run(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	
	w.logger.Debug().
		Int("worker_id", w.id).
		Msg("Worker started")
	
	for {
		select {
		case <-ctx.Done():
			w.logger.Debug().
				Int("worker_id", w.id).
				Msg("Worker stopping due to context cancellation")
			return
			
		case task, ok := <-w.taskQueue:
			if !ok {
				w.logger.Debug().
					Int("worker_id", w.id).
					Msg("Worker stopping - task queue closed")
				return
			}
			
			w.processTask(task)
		}
	}
}

func (w *Worker) processTask(task *ProcessingTask) {
	start := time.Now()
	
	result := &ProcessingResult{
		TaskID:   task.ID,
		Sequence: task.RawLedger.Sequence,
	}
	
	// Convert XDR to Arrow with timing
	w.metrics.TimeXDRConversion(func() {
		record, err := w.convertToArrow(task.RawLedger)
		if err != nil {
			result.Error = fmt.Errorf("conversion failed: %w", err)
			w.metrics.RecordError()
		} else {
			result.Record = record
			w.metrics.RecordLedgerProcessed()
		}
	})
	
	result.Duration = time.Since(start)
	
	// Send result
	select {
	case w.results <- result:
		// Success
	default:
		// Result channel full, log and drop
		w.logger.Warn().
			Int("worker_id", w.id).
			Str("task_id", task.ID).
			Msg("Result channel full, dropping result")
		if result.Record != nil {
			result.Record.Release()
		}
	}
}

func (w *Worker) convertToArrow(rawLedger *rawledger.RawLedger) (arrow.Record, error) {
	// Create a single-record batch builder
	arrowSchema := w.schemaManager.GetStellarLedgerSchema()
	builder := array.NewRecordBuilder(w.allocator, arrowSchema)
	defer builder.Release()
	
	// Unmarshal XDR
	var lcm xdr.LedgerCloseMeta
	if err := xdr.SafeUnmarshal(rawLedger.LedgerCloseMetaXdr, &lcm); err != nil {
		return nil, fmt.Errorf("failed to unmarshal XDR: %w", err)
	}
	
	// Extract ledger data (simplified for parallel processing)
	ledgerHeader := lcm.LedgerHeaderHistoryEntry().Header
	
	// Fill builders - matching the schema field order
	builder.Field(0).(*array.Uint32Builder).Append(rawLedger.Sequence)
	
	closeTime := time.Unix(int64(ledgerHeader.ScpValue.CloseTime), 0)
	builder.Field(1).(*array.TimestampBuilder).Append(arrow.Timestamp(closeTime.UnixMicro()))
	
	ledgerHash := lcm.LedgerHash()
	builder.Field(2).(*array.FixedSizeBinaryBuilder).Append(ledgerHash[:])
	
	builder.Field(3).(*array.FixedSizeBinaryBuilder).Append(ledgerHeader.PreviousLedgerHash[:])
	
	// Transaction counts (simplified)
	var txCount, opCount, successCount, failedCount uint32
	switch lcm.V {
	case 0:
		txCount = uint32(len(lcm.MustV0().TxProcessing))
	case 1:
		txCount = uint32(len(lcm.MustV1().TxProcessing))
	case 2:
		txCount = uint32(len(lcm.MustV2().TxProcessing))
	}
	
	builder.Field(4).(*array.Uint32Builder).Append(txCount)
	builder.Field(5).(*array.Uint32Builder).Append(opCount)
	builder.Field(6).(*array.Uint32Builder).Append(successCount)
	builder.Field(7).(*array.Uint32Builder).Append(failedCount)
	
	// Protocol info
	builder.Field(8).(*array.Uint32Builder).Append(uint32(ledgerHeader.LedgerVersion))
	builder.Field(9).(*array.Uint32Builder).Append(uint32(ledgerHeader.BaseFee))
	builder.Field(10).(*array.Uint32Builder).Append(uint32(ledgerHeader.BaseReserve))
	builder.Field(11).(*array.Uint32Builder).Append(uint32(ledgerHeader.MaxTxSetSize))
	builder.Field(12).(*array.Uint32Builder).Append(1) // close time resolution
	
	// Raw XDR
	builder.Field(13).(*array.BinaryBuilder).Append(rawLedger.LedgerCloseMetaXdr)
	
	// Build record
	return builder.NewRecord(), nil
}

// ParallelBatchBuilder builds Arrow batches using parallel processing
type ParallelBatchBuilder struct {
	processor     *ParallelProcessor
	logger        *logging.ComponentLogger
	batchSize     int
	pendingTasks  []*ProcessingTask
	mu            sync.Mutex
}

// NewParallelBatchBuilder creates a new parallel batch builder
func NewParallelBatchBuilder(processor *ParallelProcessor, logger *logging.ComponentLogger, batchSize int) *ParallelBatchBuilder {
	return &ParallelBatchBuilder{
		processor:    processor,
		logger:       logger,
		batchSize:    batchSize,
		pendingTasks: make([]*ProcessingTask, 0, batchSize),
	}
}

// AddLedger adds a ledger for parallel processing
func (pbb *ParallelBatchBuilder) AddLedger(rawLedger *rawledger.RawLedger) error {
	pbb.mu.Lock()
	defer pbb.mu.Unlock()
	
	task := &ProcessingTask{
		ID:        fmt.Sprintf("ledger-%d", rawLedger.Sequence),
		RawLedger: rawLedger,
	}
	
	pbb.pendingTasks = append(pbb.pendingTasks, task)
	return nil
}

// GetBatch returns a batch if ready
func (pbb *ParallelBatchBuilder) GetBatch(ctx context.Context) ([]arrow.Record, error) {
	pbb.mu.Lock()
	defer pbb.mu.Unlock()
	
	if len(pbb.pendingTasks) < pbb.batchSize {
		return nil, nil
	}
	
	// Process batch in parallel
	tasks := pbb.pendingTasks[:pbb.batchSize]
	ledgers := make([]*rawledger.RawLedger, len(tasks))
	for i, task := range tasks {
		ledgers[i] = task.RawLedger
	}
	
	records, err := pbb.processor.ProcessBatch(ctx, ledgers)
	if err != nil {
		return nil, err
	}
	
	// Clear processed tasks
	pbb.pendingTasks = pbb.pendingTasks[pbb.batchSize:]
	
	return records, nil
}