package server

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/stellar/go/xdr"
	"github.com/withObsrvr/ttp-processor-demo/contract-data-processor/config"
	"github.com/withObsrvr/ttp-processor-demo/contract-data-processor/logging"
	rawledger "github.com/withObsrvr/ttp-processor-demo/stellar-live-source-datalake/gen/raw_ledger_service"
)

// StreamManager manages the ledger streaming and processing pipeline
type StreamManager struct {
	config       *config.Config
	logger       *logging.ComponentLogger
	dataSource   *DataSourceClient
	
	// Processing pipeline
	ledgerChan   chan *ProcessingTask
	resultChan   chan *ProcessingResult
	errorChan    chan error
	
	// Worker management
	workers      []*Worker
	wg           sync.WaitGroup
	
	// State
	mu           sync.RWMutex
	running      bool
	currentLedger uint32
	startLedger   uint32
	
	// Metrics
	processedCount  atomic.Uint64
	errorCount      atomic.Uint64
	contractsFound  atomic.Uint64
	lastProcessTime atomic.Int64
}

// ProcessingTask represents a ledger to be processed
type ProcessingTask struct {
	Sequence   uint32
	XDRData    []byte
	ReceivedAt time.Time
}

// ProcessingResult represents the result of processing a ledger
type ProcessingResult struct {
	Sequence       uint32
	ContractsFound int
	ProcessingTime time.Duration
	Error          error
}

// Worker processes ledgers in parallel
type Worker struct {
	id         int
	manager    *StreamManager
	processor  LedgerProcessor
}

// LedgerProcessor is the interface for processing ledgers
type LedgerProcessor interface {
	ProcessLedger(ctx context.Context, ledgerMeta xdr.LedgerCloseMeta) (int, error)
}

// NewStreamManager creates a new stream manager
func NewStreamManager(cfg *config.Config, logger *logging.ComponentLogger, dataSource *DataSourceClient) *StreamManager {
	return &StreamManager{
		config:       cfg,
		logger:       logger,
		dataSource:   dataSource,
		ledgerChan:   make(chan *ProcessingTask, cfg.ChannelBufferSize),
		resultChan:   make(chan *ProcessingResult, cfg.ChannelBufferSize),
		errorChan:    make(chan error, 10),
		workers:      make([]*Worker, 0, cfg.WorkerCount),
	}
}

// Start begins streaming and processing ledgers
func (sm *StreamManager) Start(ctx context.Context, startLedger uint32) error {
	sm.mu.Lock()
	if sm.running {
		sm.mu.Unlock()
		return fmt.Errorf("stream manager already running")
	}
	sm.running = true
	sm.startLedger = startLedger
	sm.currentLedger = startLedger
	sm.mu.Unlock()
	
	sm.logger.Info().
		Uint32("start_ledger", startLedger).
		Int("workers", sm.config.WorkerCount).
		Msg("Starting stream manager")
	
	// Start workers
	for i := 0; i < sm.config.WorkerCount; i++ {
		worker := &Worker{
			id:      i,
			manager: sm,
			// Note: processor will be set in Phase 3
		}
		sm.workers = append(sm.workers, worker)
		sm.wg.Add(1)
		go worker.run(ctx)
	}
	
	// Start result collector
	sm.wg.Add(1)
	go sm.collectResults(ctx)
	
	// Start streaming with automatic reconnection
	sm.wg.Add(1)
	go sm.streamWithReconnect(ctx)
	
	return nil
}

// Stop gracefully stops the stream manager
func (sm *StreamManager) Stop() {
	sm.mu.Lock()
	if !sm.running {
		sm.mu.Unlock()
		return
	}
	sm.running = false
	sm.mu.Unlock()
	
	sm.logger.Info().Msg("Stopping stream manager")
	
	// Close channels to signal workers to stop
	close(sm.ledgerChan)
	
	// Wait for all workers to finish
	sm.wg.Wait()
	
	// Close result channels
	close(sm.resultChan)
	close(sm.errorChan)
	
	sm.logger.Info().
		Uint64("processed", sm.processedCount.Load()).
		Uint64("errors", sm.errorCount.Load()).
		Uint64("contracts", sm.contractsFound.Load()).
		Msg("Stream manager stopped")
}

// GetMetrics returns current processing metrics
func (sm *StreamManager) GetMetrics() StreamMetrics {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	
	lastProcess := time.Unix(0, sm.lastProcessTime.Load())
	var processingLag time.Duration
	if !lastProcess.IsZero() {
		processingLag = time.Since(lastProcess)
	}
	
	return StreamMetrics{
		Running:        sm.running,
		CurrentLedger:  sm.currentLedger,
		ProcessedCount: sm.processedCount.Load(),
		ErrorCount:     sm.errorCount.Load(),
		ContractsFound: sm.contractsFound.Load(),
		ProcessingLag:  processingLag,
		WorkerCount:    len(sm.workers),
		QueueDepth:     len(sm.ledgerChan),
	}
}

// StreamMetrics contains metrics about stream processing
type StreamMetrics struct {
	Running        bool
	CurrentLedger  uint32
	ProcessedCount uint64
	ErrorCount     uint64
	ContractsFound uint64
	ProcessingLag  time.Duration
	WorkerCount    int
	QueueDepth     int
}

// Private methods

func (sm *StreamManager) streamWithReconnect(ctx context.Context) {
	defer sm.wg.Done()
	
	for {
		select {
		case <-ctx.Done():
			return
		default:
			// Get current position
			sm.mu.RLock()
			startFrom := sm.currentLedger
			sm.mu.RUnlock()
			
			// Start streaming
			err := sm.dataSource.StreamLedgers(ctx, startFrom, sm.handleLedger)
			if err != nil {
				if ctx.Err() != nil {
					return // Context cancelled
				}
				
				sm.logger.Error().
					Err(err).
					Msg("Ledger stream error, will reconnect")
				
				// Attempt reconnection
				reconnectCtx, cancel := context.WithTimeout(ctx, 5*time.Minute)
				if err := sm.dataSource.Reconnect(reconnectCtx); err != nil {
					cancel()
					sm.logger.Error().
						Err(err).
						Msg("Failed to reconnect to data source")
					
					select {
					case sm.errorChan <- fmt.Errorf("data source reconnection failed: %w", err):
					default:
					}
					
					// Wait before retrying
					select {
					case <-ctx.Done():
						return
					case <-time.After(10 * time.Second):
						continue
					}
				}
				cancel()
				
				sm.logger.Info().Msg("Reconnected to data source, resuming stream")
			}
		}
	}
}

func (sm *StreamManager) handleLedger(ctx context.Context, ledger *rawledger.RawLedger) error {
	// Update current ledger
	sm.mu.Lock()
	sm.currentLedger = ledger.Sequence
	sm.mu.Unlock()
	
	// Create processing task
	task := &ProcessingTask{
		Sequence:   ledger.Sequence,
		XDRData:    ledger.LedgerCloseMetaXdr,
		ReceivedAt: time.Now(),
	}
	
	// Send to workers
	select {
	case sm.ledgerChan <- task:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	default:
		// Channel full, log and skip
		sm.logger.Warn().
			Uint32("ledger", ledger.Sequence).
			Int("queue_depth", len(sm.ledgerChan)).
			Msg("Processing queue full, skipping ledger")
		sm.errorCount.Add(1)
		return nil
	}
}

func (sm *StreamManager) collectResults(ctx context.Context) {
	defer sm.wg.Done()
	
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	
	for {
		select {
		case <-ctx.Done():
			return
			
		case result, ok := <-sm.resultChan:
			if !ok {
				return
			}
			
			if result.Error != nil {
				sm.errorCount.Add(1)
				sm.logger.Error().
					Err(result.Error).
					Uint32("ledger", result.Sequence).
					Msg("Error processing ledger")
			} else {
				sm.processedCount.Add(1)
				sm.contractsFound.Add(uint64(result.ContractsFound))
				sm.lastProcessTime.Store(time.Now().UnixNano())
				
				// Log progress periodically
				if sm.processedCount.Load()%100 == 0 {
					sm.logger.LogProcessing(logging.ProcessingMetrics{
						LedgerSequence:     result.Sequence,
						ContractsFound:     result.ContractsFound,
						RecordsProcessed:   1,
						ProcessingDuration: result.ProcessingTime,
					})
				}
			}
			
		case <-ticker.C:
			// Log periodic metrics
			metrics := sm.GetMetrics()
			sm.logger.Info().
				Uint32("current_ledger", metrics.CurrentLedger).
				Uint64("processed", metrics.ProcessedCount).
				Uint64("contracts", metrics.ContractsFound).
				Uint64("errors", metrics.ErrorCount).
				Int("queue_depth", metrics.QueueDepth).
				Msg("Stream processing metrics")
		}
	}
}

// Worker implementation

func (w *Worker) run(ctx context.Context) {
	defer w.manager.wg.Done()
	
	w.manager.logger.Debug().
		Int("worker_id", w.id).
		Msg("Worker started")
	
	for task := range w.manager.ledgerChan {
		select {
		case <-ctx.Done():
			return
		default:
			w.processTask(ctx, task)
		}
	}
	
	w.manager.logger.Debug().
		Int("worker_id", w.id).
		Msg("Worker stopped")
}

func (w *Worker) processTask(ctx context.Context, task *ProcessingTask) {
	start := time.Now()
	
	// For Phase 2, we'll just deserialize and count
	// Phase 3 will add actual contract processing
	var ledgerMeta xdr.LedgerCloseMeta
	if err := ledgerMeta.UnmarshalBinary(task.XDRData); err != nil {
		result := &ProcessingResult{
			Sequence:       task.Sequence,
			ProcessingTime: time.Since(start),
			Error:          fmt.Errorf("failed to unmarshal XDR: %w", err),
		}
		
		select {
		case w.manager.resultChan <- result:
		case <-ctx.Done():
		}
		return
	}
	
	// TODO: In Phase 3, process with stellar/go contract processor
	contractsFound := 0
	
	result := &ProcessingResult{
		Sequence:       task.Sequence,
		ContractsFound: contractsFound,
		ProcessingTime: time.Since(start),
	}
	
	select {
	case w.manager.resultChan <- result:
	case <-ctx.Done():
	}
}