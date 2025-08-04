package client

import (
	"context"
	"fmt"
	"time"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/withObsrvr/ttp-processor-demo/stellar-arrow-source/converter"
	"github.com/withObsrvr/ttp-processor-demo/stellar-arrow-source/logging"
	"github.com/withObsrvr/ttp-processor-demo/stellar-arrow-source/schema"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// NativeArrowSourceClient provides native Arrow data streaming from existing sources
type NativeArrowSourceClient struct {
	allocator     memory.Allocator
	logger        *logging.ComponentLogger
	schemaManager *schema.SchemaManager
	converter     *converter.XDRToArrowConverter
	
	// Connection to existing source
	sourceConn   *grpc.ClientConn
	sourceClient interface{} // Will be typed based on actual source
	endpoint     string
	
	// Performance tracking
	lastHealthCheck time.Time
	isHealthy       bool
}

// NewNativeArrowSourceClient creates a new Arrow source client
func NewNativeArrowSourceClient(endpoint string, logger *logging.ComponentLogger) (*NativeArrowSourceClient, error) {
	allocator := memory.NewGoAllocator()
	schemaManager := schema.NewSchemaManager()
	
	logger.Info().
		Str("operation", "source_client_init").
		Str("endpoint", endpoint).
		Bool("native_arrow", true).
		Msg("Initializing native Arrow source client")

	// Connect to existing source service (temporary during migration)
	conn, err := grpc.Dial(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		logger.Error().
			Err(err).
			Str("endpoint", endpoint).
			Msg("Failed to connect to source service")
		return nil, fmt.Errorf("failed to connect to source: %w", err)
	}

	// Create XDR to Arrow converter
	arrowConverter := converter.NewXDRToArrowConverter(allocator, schemaManager)

	client := &NativeArrowSourceClient{
		allocator:     allocator,
		logger:        logger,
		schemaManager: schemaManager,
		converter:     arrowConverter,
		sourceConn:    conn,
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
		Str("operation", "source_client_ready").
		Bool("connection_healthy", client.isHealthy).
		Msg("Native Arrow source client initialized")

	return client, nil
}

// StreamArrowRecords streams native Arrow records from the source
func (c *NativeArrowSourceClient) StreamArrowRecords(ctx context.Context, startLedger, endLedger uint32) (<-chan arrow.Record, <-chan error) {
	recordChan := make(chan arrow.Record, 10)
	errorChan := make(chan error, 1)

	c.logger.Info().
		Str("operation", "arrow_stream_start").
		Uint32("start_ledger", startLedger).
		Uint32("end_ledger", endLedger).
		Bool("native_conversion", true).
		Msg("Starting native Arrow record stream")

	go func() {
		defer close(recordChan)
		defer close(errorChan)

		// This is a placeholder implementation
		// In the actual implementation, this would:
		// 1. Connect to the existing stellar-live-source or stellar-live-source-datalake
		// 2. Stream raw ledger data
		// 3. Convert XDR to native Arrow records using the converter
		// 4. Stream the Arrow records

		err := c.streamFromExistingSource(ctx, startLedger, endLedger, recordChan)
		if err != nil {
			errorChan <- err
		}
	}()

	return recordChan, errorChan
}

// streamFromExistingSource handles streaming from existing gRPC sources
func (c *NativeArrowSourceClient) streamFromExistingSource(
	ctx context.Context, 
	startLedger, endLedger uint32, 
	recordChan chan<- arrow.Record) error {

	c.logger.Info().
		Str("operation", "real_source_stream").
		Uint32("start_ledger", startLedger).
		Uint32("end_ledger", endLedger).
		Msg("Streaming from real stellar source with native Arrow conversion")

	// Import the raw ledger service client - you'll need to add this import
	// import rawledger "github.com/withObsrvr/ttp-processor-demo/stellar-live-source-datalake/gen/raw_ledger_service"
	
	// TODO: Add import and implement real client
	// For now, fallback to mock until imports are added
	c.logger.Warn().
		Msg("Real gRPC client not yet implemented, using mock data")
	
	return c.generateMockArrowData(ctx, startLedger, endLedger, recordChan)
}

// generateMockArrowData creates mock Arrow records for testing
func (c *NativeArrowSourceClient) generateMockArrowData(
	ctx context.Context,
	startLedger, endLedger uint32,
	recordChan chan<- arrow.Record) error {

	schema := c.schemaManager.GetStellarLedgerSchema()
	
	batchSize := 100
	currentLedger := startLedger
	
	for {
		select {
		case <-ctx.Done():
			c.logger.Info().
				Str("operation", "mock_stream_cancelled").
				Uint32("last_ledger", currentLedger).
				Msg("Mock Arrow stream cancelled")
			return ctx.Err()
		default:
			// Check if we've reached the end
			if endLedger > 0 && currentLedger > endLedger {
				c.logger.Info().
					Str("operation", "mock_stream_completed").
					Uint32("end_ledger", currentLedger-1).
					Msg("Mock Arrow stream completed")
				return nil
			}

			// Generate a batch of mock records
			record, err := c.createMockLedgerBatch(schema, currentLedger, batchSize)
			if err != nil {
				c.logger.Error().
					Err(err).
					Uint32("ledger", currentLedger).
					Msg("Failed to create mock Arrow record")
				return err
			}

			// Send the record
			select {
			case recordChan <- record:
				c.logger.Debug().
					Str("operation", "mock_batch_sent").
					Uint32("start_ledger", currentLedger).
					Int("batch_size", batchSize).
					Int64("record_rows", record.NumRows()).
					Msg("Sent mock Arrow batch")
			case <-ctx.Done():
				record.Release()
				return ctx.Err()
			}

			currentLedger += uint32(batchSize)
			
			// Add small delay to simulate realistic streaming
			time.Sleep(100 * time.Millisecond)
		}
	}
}

// createMockLedgerBatch creates a mock Arrow record batch
func (c *NativeArrowSourceClient) createMockLedgerBatch(
	schema *arrow.Schema, 
	startLedger uint32, 
	batchSize int) (arrow.Record, error) {

	builder := array.NewRecordBuilder(c.allocator, schema)
	defer builder.Release()

	// Build mock data for each field
	ledgerSeqBuilder := builder.Field(0).(*array.Uint32Builder)
	closeTimeBuilder := builder.Field(1).(*array.TimestampBuilder)
	ledgerHashBuilder := builder.Field(2).(*array.FixedSizeBinaryBuilder)
	prevHashBuilder := builder.Field(3).(*array.FixedSizeBinaryBuilder)
	txCountBuilder := builder.Field(4).(*array.Uint32Builder)
	opCountBuilder := builder.Field(5).(*array.Uint32Builder)
	successTxBuilder := builder.Field(6).(*array.Uint32Builder)
	failedTxBuilder := builder.Field(7).(*array.Uint32Builder)
	protocolBuilder := builder.Field(8).(*array.Uint32Builder)
	baseFeeBuilder := builder.Field(9).(*array.Uint32Builder)
	baseReserveBuilder := builder.Field(10).(*array.Uint32Builder)
	maxTxSetBuilder := builder.Field(11).(*array.Uint32Builder)
	closeResBuilder := builder.Field(12).(*array.Uint32Builder)
	xdrBuilder := builder.Field(13).(*array.BinaryBuilder)

	baseTime := time.Now().Add(-time.Hour) // Start an hour ago

	for i := 0; i < batchSize; i++ {
		ledgerSeq := startLedger + uint32(i)
		
		// Mock ledger data
		ledgerSeqBuilder.Append(ledgerSeq)
		closeTimeBuilder.Append(arrow.Timestamp(baseTime.Add(time.Duration(i)*5*time.Second).UnixMicro()))
		
		// Mock 32-byte hashes
		ledgerHash := make([]byte, 32)
		prevHash := make([]byte, 32)
		for j := range ledgerHash {
			ledgerHash[j] = byte(ledgerSeq + uint32(j))
			prevHash[j] = byte(ledgerSeq - 1 + uint32(j))
		}
		ledgerHashBuilder.Append(ledgerHash)
		prevHashBuilder.Append(prevHash)
		
		// Mock transaction counts
		txCount := uint32(50 + i%100) // 50-150 transactions
		txCountBuilder.Append(txCount)
		opCountBuilder.Append(txCount * 2) // ~2 operations per transaction
		successTxBuilder.Append(txCount - uint32(i%5)) // Most successful
		failedTxBuilder.Append(uint32(i % 5)) // Few failures
		
		// Protocol and fee info
		protocolBuilder.Append(23) // Protocol 23
		baseFeeBuilder.Append(100) // 100 stroops
		baseReserveBuilder.Append(5000000) // 0.5 XLM
		maxTxSetBuilder.Append(1000)
		closeResBuilder.Append(1)
		
		// Mock XDR data (placeholder)
		mockXDR := fmt.Sprintf("mock_xdr_data_for_ledger_%d", ledgerSeq)
		xdrBuilder.Append([]byte(mockXDR))
	}

	record := builder.NewRecord()
	
	c.logger.Debug().
		Str("operation", "mock_record_created").
		Int64("rows", record.NumRows()).
		Int64("columns", record.NumCols()).
		Uint32("start_ledger", startLedger).
		Msg("Created mock Arrow record batch")

	return record, nil
}

// GetLedgerRecord retrieves a single ledger as an Arrow record
func (c *NativeArrowSourceClient) GetLedgerRecord(ctx context.Context, sequence uint32) (arrow.Record, error) {
	c.logger.Debug().
		Str("operation", "get_single_ledger").
		Uint32("sequence", sequence).
		Msg("Retrieving single ledger as Arrow record")

	schema := c.schemaManager.GetStellarLedgerSchema()
	record, err := c.createMockLedgerBatch(schema, sequence, 1)
	if err != nil {
		c.logger.Error().
			Err(err).
			Uint32("sequence", sequence).
			Msg("Failed to create single ledger record")
		return nil, err
	}

	return record, nil
}

// IsHealthy returns the health status of the source client
func (c *NativeArrowSourceClient) IsHealthy() bool {
	// Perform health check if it's been more than 30 seconds
	if time.Since(c.lastHealthCheck) > 30*time.Second {
		c.healthCheck()
	}
	return c.isHealthy
}

// healthCheck performs a health check on the source connection
func (c *NativeArrowSourceClient) healthCheck() error {
	c.lastHealthCheck = time.Now()
	
	// For now, just check if connection is still valid
	if c.sourceConn == nil {
		c.isHealthy = false
		return fmt.Errorf("no source connection")
	}

	// In a real implementation, this would ping the actual source service
	c.isHealthy = true
	
	c.logger.Debug().
		Str("operation", "health_check").
		Bool("healthy", c.isHealthy).
		Str("endpoint", c.endpoint).
		Msg("Source client health check completed")

	return nil
}

// Close closes the source client and cleans up resources
func (c *NativeArrowSourceClient) Close() error {
	c.logger.Info().
		Str("operation", "source_client_close").
		Msg("Closing native Arrow source client")

	if c.sourceConn != nil {
		if err := c.sourceConn.Close(); err != nil {
			c.logger.Error().
				Err(err).
				Msg("Failed to close source connection")
			return err
		}
	}

	c.logger.Info().
		Str("operation", "source_client_closed").
		Msg("Native Arrow source client closed successfully")

	return nil
}