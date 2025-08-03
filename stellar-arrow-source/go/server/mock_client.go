package server

import (
	"context"
	"fmt"
	"time"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/withObsrvr/ttp-processor-demo/stellar-arrow-source/logging"
	"github.com/withObsrvr/ttp-processor-demo/stellar-arrow-source/schema"
)

// mockArrowSourceClient provides mock Arrow data for Phase 1 testing
type mockArrowSourceClient struct {
	allocator     memory.Allocator
	logger        *logging.ComponentLogger
	schemaManager *schema.SchemaManager
	endpoint      string
	isHealthy     bool
}

// newMockArrowSourceClient creates a mock source client for Phase 1
func newMockArrowSourceClient(endpoint string, logger *logging.ComponentLogger) (ArrowSourceClient, error) {
	allocator := memory.NewGoAllocator()
	schemaManager := schema.NewSchemaManager()

	logger.Info().
		Str("operation", "mock_client_init").
		Str("endpoint", endpoint).
		Bool("mock_data", true).
		Msg("Initializing mock Arrow source client for Phase 1")

	return &mockArrowSourceClient{
		allocator:     allocator,
		logger:        logger,
		schemaManager: schemaManager,
		endpoint:      endpoint,
		isHealthy:     true,
	}, nil
}

// StreamArrowRecords streams mock Arrow records
func (m *mockArrowSourceClient) StreamArrowRecords(ctx context.Context, startLedger, endLedger uint32) (<-chan arrow.Record, <-chan error) {
	recordChan := make(chan arrow.Record, 10)
	errorChan := make(chan error, 1)

	m.logger.Info().
		Str("operation", "mock_stream_start").
		Uint32("start_ledger", startLedger).
		Uint32("end_ledger", endLedger).
		Bool("mock_data", true).
		Msg("Starting mock Arrow record stream")

	go func() {
		defer close(recordChan)
		defer close(errorChan)

		schema := m.schemaManager.GetStellarLedgerSchema()
		batchSize := 100
		currentLedger := startLedger

		for {
			select {
			case <-ctx.Done():
				m.logger.Info().
					Str("operation", "mock_stream_cancelled").
					Uint32("last_ledger", currentLedger).
					Msg("Mock Arrow stream cancelled")
				return
			default:
				// Check if we've reached the end
				if endLedger > 0 && currentLedger > endLedger {
					m.logger.Info().
						Str("operation", "mock_stream_completed").
						Uint32("end_ledger", currentLedger-1).
						Msg("Mock Arrow stream completed")
					return
				}

				// Generate a batch of mock records
				record, err := m.createMockLedgerBatch(schema, currentLedger, batchSize)
				if err != nil {
					m.logger.Error().
						Err(err).
						Uint32("ledger", currentLedger).
						Msg("Failed to create mock Arrow record")
					errorChan <- err
					return
				}

				// Send the record
				select {
				case recordChan <- record:
					m.logger.Debug().
						Str("operation", "mock_batch_sent").
						Uint32("start_ledger", currentLedger).
						Int("batch_size", batchSize).
						Int64("record_rows", record.NumRows()).
						Msg("Sent mock Arrow batch")
				case <-ctx.Done():
					record.Release()
					return
				}

				currentLedger += uint32(batchSize)

				// Add small delay to simulate realistic streaming
				time.Sleep(100 * time.Millisecond)
			}
		}
	}()

	return recordChan, errorChan
}

// GetLedgerRecord retrieves a single mock ledger record
func (m *mockArrowSourceClient) GetLedgerRecord(ctx context.Context, sequence uint32) (arrow.Record, error) {
	m.logger.Debug().
		Str("operation", "get_mock_ledger").
		Uint32("sequence", sequence).
		Bool("mock_data", true).
		Msg("Retrieving mock ledger as Arrow record")

	schema := m.schemaManager.GetStellarLedgerSchema()
	record, err := m.createMockLedgerBatch(schema, sequence, 1)
	if err != nil {
		m.logger.Error().
			Err(err).
			Uint32("sequence", sequence).
			Msg("Failed to create mock ledger record")
		return nil, err
	}

	return record, nil
}

// IsHealthy returns mock health status
func (m *mockArrowSourceClient) IsHealthy() bool {
	return m.isHealthy
}

// createMockLedgerBatch creates a mock Arrow record batch
func (m *mockArrowSourceClient) createMockLedgerBatch(
	schema *arrow.Schema,
	startLedger uint32,
	batchSize int) (arrow.Record, error) {

	builder := array.NewRecordBuilder(m.allocator, schema)
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

	m.logger.Debug().
		Str("operation", "mock_record_created").
		Int64("rows", record.NumRows()).
		Int64("columns", record.NumCols()).
		Uint32("start_ledger", startLedger).
		Bool("mock_data", true).
		Msg("Created mock Arrow record batch")

	return record, nil
}