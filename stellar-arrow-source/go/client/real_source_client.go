package client

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"time"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/stellar/go/xdr"
	"github.com/withObsrvr/ttp-processor-demo/stellar-arrow-source/converter"
	"github.com/withObsrvr/ttp-processor-demo/stellar-arrow-source/logging"
	"github.com/withObsrvr/ttp-processor-demo/stellar-arrow-source/schema"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	// Import the raw ledger service client
	rawledger "github.com/withObsrvr/ttp-processor-demo/stellar-live-source-datalake/gen/raw_ledger_service"
)

// RealArrowSourceClient provides native Arrow data streaming from real Stellar sources
type RealArrowSourceClient struct {
	allocator     memory.Allocator
	logger        *logging.ComponentLogger
	schemaManager *schema.SchemaManager
	converter     *converter.XDRToArrowConverter
	
	// gRPC connection to stellar data source
	sourceConn   *grpc.ClientConn
	sourceClient rawledger.RawLedgerServiceClient
	endpoint     string
	
	// Performance tracking
	lastHealthCheck time.Time
	isHealthy       bool
	
	// Statistics
	recordsProcessed int64
	conversionErrors int64
	lastError        error
}

// NewRealArrowSourceClient creates a new real Arrow source client
func NewRealArrowSourceClient(endpoint string, logger *logging.ComponentLogger) (*RealArrowSourceClient, error) {
	allocator := memory.NewGoAllocator()
	schemaManager := schema.NewSchemaManager()
	
	logger.Info().
		Str("operation", "real_source_client_init").
		Str("endpoint", endpoint).
		Bool("native_arrow", true).
		Msg("Initializing real Arrow source client")

	// Connect to stellar data source service
	conn, err := grpc.Dial(endpoint, 
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		logger.Error().
			Err(err).
			Str("endpoint", endpoint).
			Msg("Failed to connect to stellar data source")
		return nil, fmt.Errorf("failed to connect to stellar source: %w", err)
	}

	// Create gRPC client
	sourceClient := rawledger.NewRawLedgerServiceClient(conn)

	// Create XDR to Arrow converter
	arrowConverter := converter.NewXDRToArrowConverter(allocator, schemaManager)

	client := &RealArrowSourceClient{
		allocator:     allocator,
		logger:        logger,
		schemaManager: schemaManager,
		converter:     arrowConverter,
		sourceConn:    conn,
		sourceClient:  sourceClient,
		endpoint:      endpoint,
		isHealthy:     true,
	}

	// Perform initial health check
	if err := client.healthCheck(); err != nil {
		logger.Warn().
			Err(err).
			Str("endpoint", endpoint).
			Msg("Initial health check failed, but continuing")
	}

	logger.Info().
		Str("operation", "real_source_client_ready").
		Bool("connection_healthy", client.isHealthy).
		Msg("Real Arrow source client initialized successfully")

	return client, nil
}

// StreamArrowRecords streams native Arrow records from the real stellar source
func (c *RealArrowSourceClient) StreamArrowRecords(ctx context.Context, startLedger, endLedger uint32) (<-chan arrow.Record, <-chan error) {
	recordChan := make(chan arrow.Record, 10)
	errorChan := make(chan error, 1)

	c.logger.Info().
		Str("operation", "real_arrow_stream_start").
		Uint32("start_ledger", startLedger).
		Uint32("end_ledger", endLedger).
		Bool("native_conversion", true).
		Msg("Starting real Arrow record stream from stellar source")

	go func() {
		defer close(recordChan)
		defer close(errorChan)

		err := c.streamFromRealSource(ctx, startLedger, endLedger, recordChan)
		if err != nil {
			c.logger.Error().
				Err(err).
				Msg("Real source streaming failed")
			errorChan <- err
		}
	}()

	return recordChan, errorChan
}

// streamFromRealSource handles streaming from real stellar gRPC source
func (c *RealArrowSourceClient) streamFromRealSource(
	ctx context.Context, 
	startLedger, endLedger uint32, 
	recordChan chan<- arrow.Record) error {

	c.logger.Info().
		Str("operation", "stellar_grpc_stream").
		Uint32("start_ledger", startLedger).
		Uint32("end_ledger", endLedger).
		Str("endpoint", c.endpoint).
		Msg("Connecting to real stellar data source")

	// Create request for stellar data source
	sourceReq := &rawledger.StreamLedgersRequest{
		StartLedger: startLedger,
	}

	// Start streaming from stellar source
	rawLedgerStream, err := c.sourceClient.StreamRawLedgers(ctx, sourceReq)
	if err != nil {
		c.logger.Error().
			Err(err).
			Str("endpoint", c.endpoint).
			Msg("Failed to start stellar ledger stream")
		return fmt.Errorf("failed to start stellar stream: %w", err)
	}

	c.logger.Info().
		Str("operation", "stellar_stream_connected").
		Msg("Successfully connected to stellar data stream")

	batchCount := 0
	recordsInBatch := 0
	var batchBuilder *XDRBatchBuilder
	batchBuilder = NewXDRBatchBuilder(c.allocator, c.schemaManager.GetStellarLedgerSchema(), 100) // 100 ledgers per batch

	for {
		select {
		case <-ctx.Done():
			c.logger.Info().
				Str("operation", "real_stream_cancelled").
				Int("batches_processed", batchCount).
				Msg("Real stellar stream cancelled")
			if batchBuilder != nil {
				batchBuilder.Release()
			}
			return ctx.Err()

		default:
			// Receive raw ledger from stellar source
			rawLedgerMsg, err := rawLedgerStream.Recv()
			if err == io.EOF {
				c.logger.Info().
					Str("operation", "stellar_stream_ended").
					Int("batches_processed", batchCount).
					Msg("Stellar source stream ended")
				
				// Send any remaining records in batch
				if recordsInBatch > 0 {
					if record, err := batchBuilder.Build(); err == nil {
						recordChan <- record
						batchCount++
					}
				}
				batchBuilder.Release()
				return nil
			}
			if err != nil {
				c.logger.Error().
					Err(err).
					Msg("Error receiving from stellar source stream")
				c.conversionErrors++
				c.lastError = err
				batchBuilder.Release()
				return fmt.Errorf("stellar stream error: %w", err)
			}

			// Check if we should stop based on endLedger
			if endLedger > 0 && rawLedgerMsg.Sequence > endLedger {
				c.logger.Info().
					Str("operation", "end_ledger_reached").
					Uint32("end_ledger", endLedger).
					Uint32("last_sequence", rawLedgerMsg.Sequence).
					Msg("Reached end ledger, stopping stream")
				
				// Send any remaining records in batch
				if recordsInBatch > 0 {
					if record, err := batchBuilder.Build(); err == nil {
						recordChan <- record
						batchCount++
					}
				}
				batchBuilder.Release()
				return nil
			}

			// Convert XDR to Arrow and add to batch
			if err := c.addLedgerToBatch(batchBuilder, rawLedgerMsg); err != nil {
				c.logger.Error().
					Err(err).
					Uint32("ledger_sequence", rawLedgerMsg.Sequence).
					Msg("Failed to convert XDR ledger to Arrow")
				c.conversionErrors++
				c.lastError = err
				continue // Skip this ledger but continue processing
			}

			recordsInBatch++
			c.recordsProcessed++

			// Send batch when full
			if recordsInBatch >= 100 {
				record, err := batchBuilder.Build()
				if err != nil {
					c.logger.Error().
						Err(err).
						Msg("Failed to build Arrow batch")
					c.conversionErrors++
					continue
				}

				select {
				case recordChan <- record:
					c.logger.Debug().
						Str("operation", "arrow_batch_sent").
						Int("batch_number", batchCount).
						Int("records_in_batch", recordsInBatch).
						Int64("total_records", c.recordsProcessed).
						Msg("Sent Arrow batch from real data")
					batchCount++
					recordsInBatch = 0
					batchBuilder.Reset()
				case <-ctx.Done():
					record.Release()
					batchBuilder.Release()
					return ctx.Err()
				}
			}
		}
	}
}

// addLedgerToBatch converts a raw ledger XDR to Arrow format and adds it to the batch
func (c *RealArrowSourceClient) addLedgerToBatch(builder *XDRBatchBuilder, rawLedger *rawledger.RawLedger) error {
	// Unmarshal XDR to LedgerCloseMeta
	var lcm xdr.LedgerCloseMeta
	_, err := xdr.Unmarshal(bytes.NewReader(rawLedger.LedgerCloseMetaXdr), &lcm)
	if err != nil {
		return fmt.Errorf("failed to unmarshal XDR for ledger %d: %w", rawLedger.Sequence, err)
	}

	// Add to Arrow batch
	return builder.AddLedger(&lcm)
}

// GetLedgerRecord retrieves a single ledger as an Arrow record
func (c *RealArrowSourceClient) GetLedgerRecord(ctx context.Context, sequence uint32) (arrow.Record, error) {
	c.logger.Debug().
		Str("operation", "get_single_real_ledger").
		Uint32("sequence", sequence).
		Msg("Retrieving single ledger from real source")

	// For single ledger requests, we can stream just that one ledger
	recordChan, errorChan := c.StreamArrowRecords(ctx, sequence, sequence)

	select {
	case record := <-recordChan:
		return record, nil
	case err := <-errorChan:
		return nil, err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// IsHealthy returns the health status of the real source client
func (c *RealArrowSourceClient) IsHealthy() bool {
	// Perform health check if it's been more than 30 seconds
	if time.Since(c.lastHealthCheck) > 30*time.Second {
		c.healthCheck()
	}
	return c.isHealthy
}

// healthCheck performs a health check on the real source connection
func (c *RealArrowSourceClient) healthCheck() error {
	c.lastHealthCheck = time.Now()
	
	if c.sourceConn == nil {
		c.isHealthy = false
		return fmt.Errorf("no source connection")
	}

	// Check connection state
	state := c.sourceConn.GetState()
	if state.String() == "SHUTDOWN" || state.String() == "TRANSIENT_FAILURE" {
		c.isHealthy = false
		return fmt.Errorf("connection in bad state: %s", state.String())
	}

	c.isHealthy = true
	
	c.logger.Debug().
		Str("operation", "health_check").
		Bool("healthy", c.isHealthy).
		Str("endpoint", c.endpoint).
		Str("connection_state", state.String()).
		Int64("records_processed", c.recordsProcessed).
		Int64("conversion_errors", c.conversionErrors).
		Msg("Real source client health check completed")

	return nil
}

// GetStats returns processing statistics
func (c *RealArrowSourceClient) GetStats() map[string]interface{} {
	return map[string]interface{}{
		"records_processed": c.recordsProcessed,
		"conversion_errors": c.conversionErrors,
		"error_rate":        float64(c.conversionErrors) / float64(max(c.recordsProcessed, 1)),
		"last_error":        fmt.Sprintf("%v", c.lastError),
		"endpoint":          c.endpoint,
		"healthy":           c.isHealthy,
	}
}

// Close closes the real source client and cleans up resources
func (c *RealArrowSourceClient) Close() error {
	c.logger.Info().
		Str("operation", "real_source_client_close").
		Int64("total_records_processed", c.recordsProcessed).
		Int64("total_conversion_errors", c.conversionErrors).
		Msg("Closing real Arrow source client")

	if c.sourceConn != nil {
		if err := c.sourceConn.Close(); err != nil {
			c.logger.Error().
				Err(err).
				Msg("Failed to close stellar source connection")
			return err
		}
	}

	c.logger.Info().
		Str("operation", "real_source_client_closed").
		Msg("Real Arrow source client closed successfully")

	return nil
}

// max returns the maximum of two int64 values
func max(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}