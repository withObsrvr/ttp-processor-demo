package server

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/stellar/go/xdr"
	"github.com/withObsrvr/ttp-processor-demo/contract-data-processor/config"
	"github.com/withObsrvr/ttp-processor-demo/contract-data-processor/flight"
	"github.com/withObsrvr/ttp-processor-demo/contract-data-processor/logging"
	"github.com/withObsrvr/ttp-processor-demo/contract-data-processor/processor"
)

// ProcessingCoordinator coordinates data processing between control and data planes
type ProcessingCoordinator struct {
	config        *config.Config
	logger        *logging.ComponentLogger
	dataSource    *DataSourceClient
	flightServer  *flight.ContractDataFlightServer
	
	// Processor
	processor     *processor.ContractDataProcessor
	allocator     memory.Allocator
	
	// Worker pool
	workerCount   int
	workerWG      sync.WaitGroup
	
	// Metrics
	metrics       atomic.Value // ProcessingMetrics
}

// MetricsCallback is called periodically with processing metrics
type MetricsCallback func(ProcessingMetrics)

// NewProcessingCoordinator creates a new processing coordinator
func NewProcessingCoordinator(
	cfg *config.Config,
	logger *logging.ComponentLogger,
	dataSource *DataSourceClient,
	flightServer *flight.ContractDataFlightServer,
) *ProcessingCoordinator {
	allocator := memory.NewGoAllocator()
	contractProcessor := processor.NewContractDataProcessor(cfg, logger)
	
	c := &ProcessingCoordinator{
		config:       cfg,
		logger:       logger,
		dataSource:   dataSource,
		flightServer: flightServer,
		processor:    contractProcessor,
		allocator:    allocator,
		workerCount:  cfg.WorkerCount,
	}
	
	if c.workerCount == 0 {
		c.workerCount = 4 // Default worker count
	}
	
	// Initialize metrics
	c.metrics.Store(ProcessingMetrics{})
	
	return c
}

// ProcessLedgers processes ledgers and streams results via Arrow Flight
func (c *ProcessingCoordinator) ProcessLedgers(
	ctx context.Context,
	startLedger, endLedger uint32,
	metricsCallback MetricsCallback,
) error {
	c.logger.Info().
		Uint32("start_ledger", startLedger).
		Uint32("end_ledger", endLedger).
		Int("worker_count", c.workerCount).
		Msg("Starting ledger processing")
	
	// Create channels
	ledgerChan := make(chan xdr.LedgerCloseMeta, c.workerCount*2)
	entryChan := make(chan *processor.ContractDataEntry, 1000)
	errorChan := make(chan error, c.workerCount+1)
	
	// Start metrics reporter
	metricsTicker := time.NewTicker(5 * time.Second)
	defer metricsTicker.Stop()
	
	go func() {
		for {
			select {
			case <-metricsTicker.C:
				if metricsCallback != nil {
					metrics := c.metrics.Load().(ProcessingMetrics)
					metricsCallback(metrics)
				}
			case <-ctx.Done():
				return
			}
		}
	}()
	
	// Start ledger stream reader
	go c.streamLedgers(ctx, startLedger, endLedger, ledgerChan, errorChan)
	
	// Start workers
	for i := 0; i < c.workerCount; i++ {
		c.workerWG.Add(1)
		go c.processWorker(ctx, i, ledgerChan, entryChan, errorChan)
	}
	
	// Start batch builder
	batchDone := make(chan struct{})
	go c.batchBuilder(ctx, entryChan, errorChan, batchDone)
	
	// Wait for completion or error
	select {
	case err := <-errorChan:
		c.logger.Error().Err(err).Msg("Processing error occurred")
		return err
		
	case <-ctx.Done():
		c.logger.Info().Msg("Processing cancelled")
		return ctx.Err()
		
	case <-batchDone:
		c.logger.Info().Msg("Processing completed")
		return nil
	}
}

// streamLedgers reads ledgers from the data source
func (c *ProcessingCoordinator) streamLedgers(
	ctx context.Context,
	startLedger, endLedger uint32,
	ledgerChan chan<- xdr.LedgerCloseMeta,
	errorChan chan<- error,
) {
	defer close(ledgerChan)
	
	err := c.dataSource.StreamLedgers(ctx, startLedger, func(ledger xdr.LedgerCloseMeta) error {
		// Check if we've reached the end
		ledgerSeq := uint32(ledger.LedgerSequence())
		if endLedger > 0 && ledgerSeq > endLedger {
			return nil // Stop processing
		}
		
		// Update current ledger metric
		metrics := c.metrics.Load().(ProcessingMetrics)
		metrics.CurrentLedger = ledgerSeq
		c.metrics.Store(metrics)
		
		select {
		case ledgerChan <- ledger:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})
	
	if err != nil && err != context.Canceled {
		errorChan <- fmt.Errorf("ledger stream error: %w", err)
	}
}

// processWorker processes ledgers in parallel
func (c *ProcessingCoordinator) processWorker(
	ctx context.Context,
	workerID int,
	ledgerChan <-chan xdr.LedgerCloseMeta,
	entryChan chan<- *processor.ContractDataEntry,
	errorChan chan<- error,
) {
	defer c.workerWG.Done()
	
	c.logger.Debug().
		Int("worker_id", workerID).
		Msg("Worker started")
	
	for {
		select {
		case ledger, ok := <-ledgerChan:
			if !ok {
				c.logger.Debug().
					Int("worker_id", workerID).
					Msg("Worker finished - no more ledgers")
				return
			}
			
			// Process ledger
			entries, err := c.processor.ProcessLedger(ctx, ledger)
			if err != nil {
				c.logger.Error().
					Err(err).
					Int("worker_id", workerID).
					Uint32("ledger", uint32(ledger.LedgerSequence())).
					Msg("Failed to process ledger")
				continue
			}
			
			// Send entries to batch builder
			for _, entry := range entries {
				select {
				case entryChan <- entry:
				case <-ctx.Done():
					return
				}
			}
			
			// Update metrics
			processorMetrics := c.processor.GetMetrics()
			metrics := c.metrics.Load().(ProcessingMetrics)
			metrics.EntriesProcessed = processorMetrics.ContractsProcessed
			metrics.EntriesSkipped = processorMetrics.EntriesSkipped
			c.metrics.Store(metrics)
			
		case <-ctx.Done():
			c.logger.Debug().
				Int("worker_id", workerID).
				Msg("Worker cancelled")
			return
		}
	}
}

// batchBuilder builds Arrow batches from entries
func (c *ProcessingCoordinator) batchBuilder(
	ctx context.Context,
	entryChan <-chan *processor.ContractDataEntry,
	errorChan chan<- error,
	done chan<- struct{},
) {
	defer close(done)
	
	transformer := processor.NewArrowTransformer(c.allocator)
	defer transformer.Release()
	
	batchSize := c.config.BatchSize
	if batchSize == 0 {
		batchSize = 1000
	}
	
	streamID := "contract-data-stream"
	
	for {
		select {
		case entry, ok := <-entryChan:
			if !ok {
				// No more entries, flush final batch
				if transformer.GetCurrentSize() > 0 {
					if err := c.flushBatch(transformer, streamID); err != nil {
						errorChan <- err
						return
					}
				}
				
				// Wait for workers to finish
				c.workerWG.Wait()
				return
			}
			
			// Add entry to transformer
			if err := transformer.AddEntry(entry); err != nil {
				c.logger.Error().
					Err(err).
					Msg("Failed to add entry to transformer")
				continue
			}
			
			// Check if batch is full
			if transformer.GetCurrentSize() >= int(batchSize) {
				if err := c.flushBatch(transformer, streamID); err != nil {
					errorChan <- err
					return
				}
			}
			
		case <-ctx.Done():
			// Flush any remaining entries
			if transformer.GetCurrentSize() > 0 {
				c.flushBatch(transformer, streamID)
			}
			return
		}
	}
}

// flushBatch builds and sends a batch to the Flight server
func (c *ProcessingCoordinator) flushBatch(transformer *processor.ArrowTransformer, streamID string) error {
	// Build Arrow record
	record, err := transformer.BuildRecord()
	if err != nil {
		return fmt.Errorf("failed to build record: %w", err)
	}
	
	// Add to Flight server
	if err := c.flightServer.AddBatch(streamID, record); err != nil {
		record.Release()
		return fmt.Errorf("failed to add batch to flight server: %w", err)
	}
	
	// Update metrics
	metrics := c.metrics.Load().(ProcessingMetrics)
	metrics.BatchesCreated++
	metrics.BytesProcessed += estimateRecordSize(record)
	c.metrics.Store(metrics)
	
	// Reset transformer for next batch
	transformer.Reset()
	
	c.logger.Debug().
		Int64("rows", record.NumRows()).
		Uint64("batches", metrics.BatchesCreated).
		Msg("Created Arrow batch")
	
	return nil
}

// UpdateFilters updates the processor filters
func (c *ProcessingCoordinator) UpdateFilters(cfg *config.Config) error {
	// Create new processor with updated config
	newProcessor := processor.NewContractDataProcessor(cfg, c.logger)
	
	// Atomic swap
	c.processor = newProcessor
	
	return nil
}

// estimateRecordSize estimates the size of an Arrow record
func estimateRecordSize(record arrow.Record) uint64 {
	var size uint64
	for i := 0; i < int(record.NumCols()); i++ {
		col := record.Column(i)
		// Rough estimate based on column type and length
		size += uint64(col.Len()) * 8 // Assume 8 bytes average per value
	}
	return size
}