package server

import (
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/withObsrvr/ttp-processor-demo/stellar-arrow-source/logging"
	"github.com/withObsrvr/ttp-processor-demo/stellar-arrow-source/schema"
	rawledger "github.com/withObsrvr/ttp-processor-demo/stellar-live-source-datalake/gen/raw_ledger_service"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// realArrowSourceClient connects to real Stellar data sources and converts to Arrow format
type realArrowSourceClient struct {
	allocator        memory.Allocator
	logger           *logging.ComponentLogger
	schemaManager    *schema.SchemaManager
	endpoint         string
	conn             *grpc.ClientConn
	client           rawledger.RawLedgerServiceClient
	isHealthy        bool
	healthMutex      sync.RWMutex
	lastError        error
	lastErrorTime    time.Time
	totalProcessed   int64
	totalErrors      int64
	batchSize        int
}

// newRealArrowSourceClient creates a client that connects to real Stellar data
func newRealArrowSourceClient(endpoint string, logger *logging.ComponentLogger, batchSize int) (ArrowSourceClient, error) {
	allocator := memory.NewGoAllocator()
	schemaManager := schema.NewSchemaManager()

	logger.Info().
		Str("operation", "real_client_init").
		Str("endpoint", endpoint).
		Bool("mock_data", false).
		Msg("Initializing real Arrow source client")

	// Use provided batch size or default to 1
	if batchSize <= 0 {
		batchSize = 1
	}

	client := &realArrowSourceClient{
		allocator:     allocator,
		logger:        logger,
		schemaManager: schemaManager,
		endpoint:      endpoint,
		isHealthy:     false,
		batchSize:     batchSize,
	}

	// Connect to the raw ledger service
	if err := client.connect(); err != nil {
		return nil, fmt.Errorf("failed to connect to real data source: %w", err)
	}

	logger.Info().
		Str("operation", "real_client_connected").
		Str("endpoint", endpoint).
		Bool("native_arrow", true).
		Int("batch_size", batchSize).
		Msg("Successfully connected to real Stellar data source")

	return client, nil
}

// connect establishes gRPC connection to the raw ledger service
func (r *realArrowSourceClient) connect() error {
	r.logger.Info().
		Str("operation", "grpc_connect").
		Str("endpoint", r.endpoint).
		Msg("Connecting to raw ledger service")

	// Set up gRPC connection with insecure credentials for development
	conn, err := grpc.Dial(r.endpoint, 
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
		grpc.WithTimeout(10*time.Second))
	if err != nil {
		r.recordError(fmt.Errorf("failed to dial raw ledger service: %w", err))
		return err
	}

	r.conn = conn
	r.client = rawledger.NewRawLedgerServiceClient(conn)
	
	r.healthMutex.Lock()
	r.isHealthy = true
	r.healthMutex.Unlock()

	r.logger.Info().
		Str("operation", "grpc_connected").
		Str("endpoint", r.endpoint).
		Msg("Successfully connected to raw ledger service")

	return nil
}

// StreamArrowRecords streams real Stellar data converted to Arrow format
func (r *realArrowSourceClient) StreamArrowRecords(ctx context.Context, startLedger, endLedger uint32) (<-chan arrow.Record, <-chan error) {
	recordChan := make(chan arrow.Record, 10)
	errorChan := make(chan error, 1)

	r.logger.Info().
		Str("operation", "real_stream_start").
		Uint32("start_ledger", startLedger).
		Uint32("end_ledger", endLedger).
		Bool("mock_data", false).
		Msg("Starting real Arrow record stream from Stellar data")

	go func() {
		defer close(recordChan)
		defer close(errorChan)

		// Create the raw ledger stream request
		request := &rawledger.StreamLedgersRequest{
			StartLedger: startLedger,
		}

		// Start streaming from the raw ledger service
		stream, err := r.client.StreamRawLedgers(ctx, request)
		if err != nil {
			r.recordError(fmt.Errorf("failed to start raw ledger stream: %w", err))
			errorChan <- err
			return
		}

		r.logger.Info().
			Str("operation", "raw_stream_started").
			Uint32("start_ledger", startLedger).
			Uint32("end_ledger", endLedger).
			Bool("end_ledger_specified", endLedger > 0).
			Msg("Raw ledger stream started successfully")

		// Process incoming raw ledgers and convert to Arrow
		batchBuilder := NewXDRBatchBuilder(r.allocator, r.schemaManager, r.logger, r.batchSize)
		ledgerCount := 0

		for {
			select {
			case <-ctx.Done():
				r.logger.Info().
					Str("operation", "real_stream_cancelled").
					Int("ledgers_processed", ledgerCount).
					Msg("Real Arrow stream cancelled")
				return
			default:
				// Receive next raw ledger
				rawLedger, err := stream.Recv()
				if err == io.EOF {
					// Stream completed - send final batch if any
					if record := batchBuilder.Finalize(); record != nil {
						select {
						case recordChan <- record:
							r.logger.Debug().
								Int64("final_batch_rows", record.NumRows()).
								Msg("Sent final Arrow batch")
						case <-ctx.Done():
							record.Release()
							return
						}
					}
					
					r.logger.Info().
						Str("operation", "real_stream_completed").
						Int("total_ledgers", ledgerCount).
						Msg("Real Arrow stream completed successfully")
					return
				}
				if err != nil {
					r.recordError(fmt.Errorf("stream receive error: %w", err))
					errorChan <- err
					return
				}

				// Log each received ledger for debugging
				r.logger.Debug().
					Str("operation", "ledger_received").
					Uint32("ledger_sequence", rawLedger.Sequence).
					Uint32("end_ledger", endLedger).
					Bool("should_stop", endLedger > 0 && rawLedger.Sequence > endLedger).
					Msg("Received ledger from datalake")

				// Check if we've reached the end ledger (if specified)
				if endLedger > 0 && rawLedger.Sequence > endLedger {
					r.logger.Info().
						Str("operation", "end_ledger_reached").
						Uint32("end_ledger", endLedger).
						Uint32("current_ledger", rawLedger.Sequence).
						Int("ledgers_processed", ledgerCount).
						Msg("Reached end ledger, stopping stream")
					
					// Send final batch if any
					if record := batchBuilder.Finalize(); record != nil {
						select {
						case recordChan <- record:
							r.logger.Debug().
								Int64("final_batch_rows", record.NumRows()).
								Msg("Sent final Arrow batch")
						case <-ctx.Done():
							record.Release()
							return
						}
					}
					return
				}

				// Convert raw XDR to Stellar objects and add to batch
				if err := batchBuilder.AddRawLedger(rawLedger); err != nil {
					r.logger.Error().
						Err(err).
						Uint32("ledger_sequence", rawLedger.Sequence).
						Msg("Failed to process raw ledger - skipping")
					r.recordError(err)
					continue // Skip malformed ledgers but continue processing
				}

				ledgerCount++
				r.totalProcessed++

				// Send completed batches
				if record := batchBuilder.GetBatch(); record != nil {
					select {
					case recordChan <- record:
						r.logger.Debug().
							Int64("batch_rows", record.NumRows()).
							Int("ledgers_in_batch", ledgerCount%100).
							Msg("Sent Arrow batch from real data")
					case <-ctx.Done():
						record.Release()
						return
					}
				}

				// Log progress periodically
				if ledgerCount%1000 == 0 {
					r.logger.Info().
						Str("operation", "stream_progress").
						Int("ledgers_processed", ledgerCount).
						Int64("total_errors", r.totalErrors).
						Msg("Real data streaming progress")
				}
			}
		}
	}()

	return recordChan, errorChan
}

// GetLedgerRecord retrieves a single real ledger as Arrow record
func (r *realArrowSourceClient) GetLedgerRecord(ctx context.Context, sequence uint32) (arrow.Record, error) {
	r.logger.Debug().
		Str("operation", "get_real_ledger").
		Uint32("sequence", sequence).
		Bool("mock_data", false).
		Msg("Retrieving real ledger as Arrow record")

	// Use streaming with single ledger range
	recordChan, errChan := r.StreamArrowRecords(ctx, sequence, sequence)

	select {
	case record := <-recordChan:
		if record != nil {
			return record, nil
		}
		return nil, fmt.Errorf("no record received for ledger %d", sequence)
	case err := <-errChan:
		return nil, fmt.Errorf("failed to get ledger %d: %w", sequence, err)
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// IsHealthy returns the current health status
func (r *realArrowSourceClient) IsHealthy() bool {
	r.healthMutex.RLock()
	defer r.healthMutex.RUnlock()
	return r.isHealthy
}

// recordError updates error tracking and health status
func (r *realArrowSourceClient) recordError(err error) {
	r.healthMutex.Lock()
	defer r.healthMutex.Unlock()
	
	r.lastError = err
	r.lastErrorTime = time.Now()
	r.totalErrors++
	
	// Mark unhealthy if too many recent errors
	if r.totalErrors > 10 {
		r.isHealthy = false
	}
	
	r.logger.Error().
		Err(err).
		Int64("total_errors", r.totalErrors).
		Msg("Recorded error in real Arrow source client")
}

// Close cleanup connections
func (r *realArrowSourceClient) Close() error {
	if r.conn != nil {
		return r.conn.Close()
	}
	return nil
}