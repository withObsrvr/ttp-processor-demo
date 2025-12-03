package main

import (
	"context"
	"fmt"
	"sync"
	"time"

	flowctlv1 "github.com/withObsrvr/flow-proto/go/gen/flowctl/v1"
	stellarv1 "github.com/withObsrvr/flow-proto/go/gen/stellar/v1"

	"github.com/stellar/go-stellar-sdk/processors/token_transfer"
	"github.com/stellar/go-stellar-sdk/xdr"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

// BatchConfig defines batch processing configuration
type BatchConfig struct {
	// Enable batch processing
	Enabled bool

	// Maximum batch size (number of ledgers)
	MaxBatchSize int

	// Batch timeout - flush batch after this duration even if not full
	BatchTimeout time.Duration

	// Process batches in parallel
	ParallelProcessing bool

	// Maximum concurrent batch processors
	MaxConcurrency int
}

// LoadBatchConfig loads batch processing configuration from environment
func LoadBatchConfig(logger *zap.Logger) *BatchConfig {
	config := &BatchConfig{
		Enabled:            parseBool(getEnv("ENABLE_BATCHING", "false")),
		MaxBatchSize:       parseInt(getEnv("BATCH_SIZE", "10")),
		BatchTimeout:       parseDuration(getEnv("BATCH_TIMEOUT", "5s")),
		ParallelProcessing: parseBool(getEnv("BATCH_PARALLEL", "false")),
		MaxConcurrency:     parseInt(getEnv("BATCH_CONCURRENCY", "4")),
	}

	if config.Enabled {
		logger.Info("Batch processing enabled",
			zap.Int("max_batch_size", config.MaxBatchSize),
			zap.Duration("batch_timeout", config.BatchTimeout),
			zap.Bool("parallel", config.ParallelProcessing),
			zap.Int("max_concurrency", config.MaxConcurrency))
	}

	return config
}

// BatchProcessor handles batch processing of ledgers
type BatchProcessor struct {
	config        *BatchConfig
	filterConfig  *FilterConfig
	ttpProcessor  *token_transfer.EventsProcessor
	logger        *zap.Logger
	componentID   string
	version       string
	metrics       *ProcessorMetrics
	networkName   string

	mu             sync.Mutex
	currentBatch   []*stellarv1.RawLedger
	batchTimer     *time.Timer
	processingChan chan []*stellarv1.RawLedger
	doneChan       chan struct{}
	wg             sync.WaitGroup
}

// NewBatchProcessor creates a new batch processor
func NewBatchProcessor(
	config *BatchConfig,
	filterConfig *FilterConfig,
	ttpProcessor *token_transfer.EventsProcessor,
	logger *zap.Logger,
	componentID string,
	version string,
	metrics *ProcessorMetrics,
	networkName string,
) *BatchProcessor {
	bp := &BatchProcessor{
		config:         config,
		filterConfig:   filterConfig,
		ttpProcessor:   ttpProcessor,
		logger:         logger,
		componentID:    componentID,
		version:        version,
		metrics:        metrics,
		networkName:    networkName,
		currentBatch:   make([]*stellarv1.RawLedger, 0, config.MaxBatchSize),
		processingChan: make(chan []*stellarv1.RawLedger, config.MaxConcurrency),
		doneChan:       make(chan struct{}),
	}

	// Start batch processors
	if config.ParallelProcessing {
		for i := 0; i < config.MaxConcurrency; i++ {
			bp.wg.Add(1)
			go bp.batchWorker(i)
		}
	}

	return bp
}

// AddLedger adds a ledger to the current batch
func (bp *BatchProcessor) AddLedger(ctx context.Context, rawLedger *stellarv1.RawLedger) (*flowctlv1.Event, error) {
	bp.mu.Lock()

	// Add to current batch
	bp.currentBatch = append(bp.currentBatch, rawLedger)

	// Reset timer
	if bp.batchTimer != nil {
		bp.batchTimer.Stop()
	}
	bp.batchTimer = time.AfterFunc(bp.config.BatchTimeout, func() {
		bp.flushBatch(ctx)
	})

	// Check if batch is full
	shouldFlush := len(bp.currentBatch) >= bp.config.MaxBatchSize

	bp.mu.Unlock()

	if shouldFlush {
		return bp.flushBatch(ctx)
	}

	return nil, nil // Batch not ready yet
}

// flushBatch processes the current batch
func (bp *BatchProcessor) flushBatch(ctx context.Context) (*flowctlv1.Event, error) {
	bp.mu.Lock()
	if len(bp.currentBatch) == 0 {
		bp.mu.Unlock()
		return nil, nil
	}

	// Take current batch
	batch := bp.currentBatch
	bp.currentBatch = make([]*stellarv1.RawLedger, 0, bp.config.MaxBatchSize)

	// Stop timer
	if bp.batchTimer != nil {
		bp.batchTimer.Stop()
		bp.batchTimer = nil
	}

	bp.mu.Unlock()

	startTime := time.Now()

	if bp.config.ParallelProcessing {
		// Send to worker pool
		select {
		case bp.processingChan <- batch:
			// Batch queued for processing
			return nil, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	// Process synchronously
	return bp.processBatch(ctx, batch, startTime)
}

// batchWorker processes batches from the queue
func (bp *BatchProcessor) batchWorker(workerID int) {
	defer bp.wg.Done()

	for {
		select {
		case batch := <-bp.processingChan:
			startTime := time.Now()
			if _, err := bp.processBatch(context.Background(), batch, startTime); err != nil {
				bp.logger.Error("Batch processing failed",
					zap.Int("worker_id", workerID),
					zap.Int("batch_size", len(batch)),
					zap.Error(err))
			}
		case <-bp.doneChan:
			return
		}
	}
}

// processBatch processes a batch of ledgers
func (bp *BatchProcessor) processBatch(ctx context.Context, batch []*stellarv1.RawLedger, startTime time.Time) (*flowctlv1.Event, error) {
	if len(batch) == 0 {
		return nil, nil
	}

	batchStats := &BatchStats{
		LedgerCount: len(batch),
		StartTime:   startTime,
		Errors:      NewErrorCollector(),
	}

	allEvents := make([]*stellarv1.TokenTransferEvent, 0)
	var firstLedger, lastLedger uint32

	for i, rawLedger := range batch {
		if i == 0 {
			firstLedger = rawLedger.Sequence
		}
		lastLedger = rawLedger.Sequence

		// Record ledger processed
		bp.metrics.RecordLedgerProcessed(bp.networkName, true)

		// Decode XDR
		var ledgerCloseMeta xdr.LedgerCloseMeta
		if err := xdr.SafeUnmarshal(rawLedger.LedgerCloseMetaXdr, &ledgerCloseMeta); err != nil {
			batchStats.Errors.AddError(
				SeverityError,
				"Failed to unmarshal XDR",
				err,
				map[string]string{
					"ledger_sequence": fmt.Sprintf("%d", rawLedger.Sequence),
					"stage":           "xdr_decode",
				},
			)
			bp.metrics.RecordProcessingError(string(SeverityError), "xdr_decode", bp.networkName)
			bp.logger.Error("Failed to unmarshal XDR in batch",
				zap.Uint32("sequence", rawLedger.Sequence),
				zap.Error(err))
			batchStats.ErrorCount++
			continue
		}

		// Extract events
		ttpEvents, err := bp.ttpProcessor.EventsFromLedger(ledgerCloseMeta)
		if err != nil {
			batchStats.Errors.AddError(
				SeverityError,
				"Failed to extract token transfer events",
				err,
				map[string]string{
					"ledger_sequence": fmt.Sprintf("%d", rawLedger.Sequence),
					"stage":           "event_extraction",
				},
			)
			bp.metrics.RecordProcessingError(string(SeverityError), "event_extraction", bp.networkName)
			bp.logger.Error("Failed to extract events in batch",
				zap.Uint32("sequence", rawLedger.Sequence),
				zap.Error(err))
			batchStats.ErrorCount++
			continue
		}

		batchStats.EventsExtracted += len(ttpEvents)

		// Apply filtering and convert
		for _, ttpEvent := range ttpEvents {
			// Record event extracted
			eventType := getEventType(ttpEvent)
			bp.metrics.RecordEventExtracted(eventType, bp.networkName, true)

			// Apply filter
			if !bp.filterConfig.ShouldIncludeEvent(ttpEvent, bp.logger) {
				// Determine filter reason
				filterReason := "event_type"
				if bp.filterConfig.MinAmount != nil && getEventAmount(ttpEvent) < *bp.filterConfig.MinAmount {
					filterReason = "min_amount"
				}
				bp.metrics.RecordEventFiltered(eventType, filterReason, bp.networkName)
				batchStats.EventsFiltered++
				continue
			}

			// Convert to proto
			converted := convertTokenTransferEvent(ttpEvent)
			if converted != nil {
				allEvents = append(allEvents, converted)
				bp.metrics.RecordEventEmitted(eventType, bp.networkName, true)
			}
		}
	}

	// Record batch size and processing latency
	bp.metrics.RecordBatchSize(bp.networkName, float64(batchStats.LedgerCount))
	bp.metrics.RecordProcessingLatency("batch", bp.networkName, float64(time.Since(startTime).Milliseconds()), true)

	batchStats.EventsPassed = len(allEvents)
	batchStats.ProcessingTime = time.Since(startTime)

	// Log batch stats with error summary
	errorSummary := batchStats.Errors.Summary()
	bp.logger.Info("Processed batch",
		zap.Uint32("first_ledger", firstLedger),
		zap.Uint32("last_ledger", lastLedger),
		zap.Int("ledgers", batchStats.LedgerCount),
		zap.Int("extracted", batchStats.EventsExtracted),
		zap.Int("filtered", batchStats.EventsFiltered),
		zap.Int("passed", batchStats.EventsPassed),
		zap.Int("errors", batchStats.ErrorCount),
		zap.Int("warnings", errorSummary["warnings"]),
		zap.Int("fatal_errors", errorSummary["fatal"]),
		zap.Duration("processing_time", batchStats.ProcessingTime))

	// If no events after filtering, return nil
	if len(allEvents) == 0 {
		return nil, nil
	}

	// Marshal events
	eventsData, err := proto.Marshal(&stellarv1.TokenTransferBatch{
		Events: allEvents,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to marshal batch: %w", err)
	}

	// Create output event
	outputEvent := &flowctlv1.Event{
		Id:                fmt.Sprintf("token-transfers-batch-%d-%d", firstLedger, lastLedger),
		Type:              "stellar.token.transfer.v1",
		Payload:           eventsData,
		Metadata:          make(map[string]string),
		SourceComponentId: bp.componentID,
		ContentType:       "application/protobuf",
		StellarCursor: &flowctlv1.StellarCursor{
			LedgerSequence: uint64(lastLedger),
		},
	}

	// Add batch metadata
	outputEvent.Metadata["batch_mode"] = "true"
	outputEvent.Metadata["batch_size"] = fmt.Sprintf("%d", batchStats.LedgerCount)
	outputEvent.Metadata["first_ledger"] = fmt.Sprintf("%d", firstLedger)
	outputEvent.Metadata["last_ledger"] = fmt.Sprintf("%d", lastLedger)
	outputEvent.Metadata["events_extracted"] = fmt.Sprintf("%d", batchStats.EventsExtracted)
	outputEvent.Metadata["events_filtered"] = fmt.Sprintf("%d", batchStats.EventsFiltered)
	outputEvent.Metadata["events_count"] = fmt.Sprintf("%d", batchStats.EventsPassed)
	outputEvent.Metadata["batch_errors"] = fmt.Sprintf("%d", batchStats.ErrorCount)
	outputEvent.Metadata["error_warnings"] = fmt.Sprintf("%d", errorSummary["warnings"])
	outputEvent.Metadata["error_errors"] = fmt.Sprintf("%d", errorSummary["errors"])
	outputEvent.Metadata["error_fatal"] = fmt.Sprintf("%d", errorSummary["fatal"])
	outputEvent.Metadata["partial_success"] = fmt.Sprintf("%t", batchStats.EventsPassed > 0 && batchStats.ErrorCount > 0)
	outputEvent.Metadata["processing_time_ms"] = fmt.Sprintf("%d", batchStats.ProcessingTime.Milliseconds())
	outputEvent.Metadata["processor_version"] = bp.version
	outputEvent.Metadata["filter_enabled"] = fmt.Sprintf("%t", bp.filterConfig.Enabled)

	return outputEvent, nil
}

// Stop stops the batch processor
func (bp *BatchProcessor) Stop(ctx context.Context) error {
	// Flush any remaining batch
	_, err := bp.flushBatch(ctx)

	// Stop workers
	if bp.config.ParallelProcessing {
		close(bp.doneChan)
		bp.wg.Wait()
	}

	return err
}

// BatchStats tracks statistics for a batch
type BatchStats struct {
	LedgerCount      int
	EventsExtracted  int
	EventsFiltered   int
	EventsPassed     int
	ErrorCount       int
	StartTime        time.Time
	ProcessingTime   time.Duration
	Errors           *ErrorCollector
}

// Helper function
func parseBool(s string) bool {
	return s == "true" || s == "1" || s == "yes"
}
