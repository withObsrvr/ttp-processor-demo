package server

import (
	"fmt"
	"time"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/stellar/go/xdr"
	"github.com/withObsrvr/ttp-processor-demo/stellar-arrow-source/logging"
	"github.com/withObsrvr/ttp-processor-demo/stellar-arrow-source/schema"
	rawledger "github.com/withObsrvr/ttp-processor-demo/stellar-live-source-datalake/gen/raw_ledger_service"
)

// XDRBatchBuilder converts Stellar XDR data to Arrow batches efficiently
type XDRBatchBuilder struct {
	allocator     memory.Allocator
	schemaManager *schema.SchemaManager
	logger        *logging.ComponentLogger
	builder       *array.RecordBuilder
	schema        *arrow.Schema
	batchSize     int
	currentCount  int
}

// NewXDRBatchBuilder creates a new batch builder for XDR to Arrow conversion
func NewXDRBatchBuilder(allocator memory.Allocator, schemaManager *schema.SchemaManager, logger *logging.ComponentLogger, batchSize int) *XDRBatchBuilder {
	schema := schemaManager.GetStellarLedgerSchema()
	builder := array.NewRecordBuilder(allocator, schema)

	// Default to 1 for real-time streaming if not specified
	if batchSize <= 0 {
		batchSize = 1
	}

	return &XDRBatchBuilder{
		allocator:     allocator,
		schemaManager: schemaManager,
		logger:        logger,
		builder:       builder,
		schema:        schema,
		batchSize:     batchSize,
		currentCount:  0,
	}
}

// AddRawLedger adds a raw ledger to the current batch
func (b *XDRBatchBuilder) AddRawLedger(rawLedger *rawledger.RawLedger) error {
	// Unmarshal the XDR data
	var ledgerCloseMeta xdr.LedgerCloseMeta
	if err := xdr.SafeUnmarshal(rawLedger.LedgerCloseMetaXdr, &ledgerCloseMeta); err != nil {
		return fmt.Errorf("failed to unmarshal LedgerCloseMeta XDR: %w", err)
	}

	// Extract ledger data and add to Arrow builders
	if err := b.addLedgerToBuilders(rawLedger.Sequence, &ledgerCloseMeta, rawLedger.LedgerCloseMetaXdr); err != nil {
		return fmt.Errorf("failed to add ledger to Arrow builders: %w", err)
	}

	b.currentCount++
	return nil
}

// GetBatch returns a completed batch if ready, nil otherwise
func (b *XDRBatchBuilder) GetBatch() arrow.Record {
	if b.currentCount >= b.batchSize {
		return b.buildAndReset()
	}
	return nil
}

// Finalize returns the final batch with any remaining data
func (b *XDRBatchBuilder) Finalize() arrow.Record {
	if b.currentCount > 0 {
		return b.buildAndReset()
	}
	return nil
}

// addLedgerToBuilders adds ledger data to the Arrow column builders
func (b *XDRBatchBuilder) addLedgerToBuilders(sequence uint32, lcm *xdr.LedgerCloseMeta, rawXDR []byte) error {
	// Get the field builders - must match schema field order exactly
	ledgerSeqBuilder := b.builder.Field(0).(*array.Uint32Builder)
	closeTimeBuilder := b.builder.Field(1).(*array.TimestampBuilder)
	ledgerHashBuilder := b.builder.Field(2).(*array.FixedSizeBinaryBuilder)
	prevHashBuilder := b.builder.Field(3).(*array.FixedSizeBinaryBuilder)
	txCountBuilder := b.builder.Field(4).(*array.Uint32Builder)
	opCountBuilder := b.builder.Field(5).(*array.Uint32Builder)
	successTxBuilder := b.builder.Field(6).(*array.Uint32Builder)
	failedTxBuilder := b.builder.Field(7).(*array.Uint32Builder)
	protocolBuilder := b.builder.Field(8).(*array.Uint32Builder)
	baseFeeBuilder := b.builder.Field(9).(*array.Uint32Builder)
	baseReserveBuilder := b.builder.Field(10).(*array.Uint32Builder)
	maxTxSetBuilder := b.builder.Field(11).(*array.Uint32Builder)
	closeResBuilder := b.builder.Field(12).(*array.Uint32Builder)
	xdrBuilder := b.builder.Field(13).(*array.BinaryBuilder)

	// Extract ledger header and transaction processing data
	ledgerHeaderEntry := lcm.LedgerHeaderHistoryEntry()
	ledgerHeader := ledgerHeaderEntry.Header
	ledgerHash := lcm.LedgerHash()
	
	// Process transactions based on version (different types for V2)
	var txCount uint32
	var opCount, successTxCount, failedTxCount uint32
	
	switch lcm.V {
	case 0:
		txProcessing := lcm.MustV0().TxProcessing
		txCount, opCount, successTxCount, failedTxCount = b.processTxResults(txProcessing)
	case 1:
		txProcessing := lcm.MustV1().TxProcessing
		txCount, opCount, successTxCount, failedTxCount = b.processTxResults(txProcessing)
	case 2:
		// V2 has different type - process separately
		txProcessingV2 := lcm.MustV2().TxProcessing
		txCount, opCount, successTxCount, failedTxCount = b.processTxResultsV2(txProcessingV2)
	default:
		return fmt.Errorf("unsupported LedgerCloseMeta version: %d", lcm.V)
	}

	// Add basic ledger information
	ledgerSeqBuilder.Append(sequence)
	
	// Convert Stellar timestamp (seconds since epoch) to Arrow timestamp (microseconds)
	closeTime := time.Unix(int64(ledgerHeader.ScpValue.CloseTime), 0)
	closeTimeBuilder.Append(arrow.Timestamp(closeTime.UnixMicro()))

	// Add ledger hash (32 bytes)
	ledgerHashBuilder.Append(ledgerHash[:])

	// Add previous ledger hash (32 bytes)  
	prevHash := ledgerHeader.PreviousLedgerHash
	prevHashBuilder.Append(prevHash[:])

	// Transaction statistics are now calculated above in version-specific processing

	txCountBuilder.Append(txCount)
	opCountBuilder.Append(opCount)
	successTxBuilder.Append(successTxCount)
	failedTxBuilder.Append(failedTxCount)

	// Add protocol and fee information
	protocolBuilder.Append(uint32(ledgerHeader.LedgerVersion))
	baseFeeBuilder.Append(uint32(ledgerHeader.BaseFee))
	baseReserveBuilder.Append(uint32(ledgerHeader.BaseReserve))
	maxTxSetBuilder.Append(uint32(ledgerHeader.MaxTxSetSize))
	
	// Close time resolution is always 1 for Stellar
	closeResBuilder.Append(1)

	// Add raw XDR data for complete compatibility
	xdrBuilder.Append(rawXDR)

	return nil
}

// buildAndReset creates an Arrow record from current data and resets builders
func (b *XDRBatchBuilder) buildAndReset() arrow.Record {
	if b.currentCount == 0 {
		return nil
	}

	// Build the record
	record := b.builder.NewRecord()
	
	b.logger.Debug().
		Str("operation", "arrow_batch_built").
		Int64("rows", record.NumRows()).
		Int64("columns", record.NumCols()).
		Int("ledgers_converted", b.currentCount).
		Bool("native_arrow", true).
		Msg("Built Arrow batch from real Stellar data")

	// Reset for next batch
	b.builder.Release()
	b.builder = array.NewRecordBuilder(b.allocator, b.schema)
	b.currentCount = 0

	return record
}

// processTxResults processes TransactionResultMeta for V0/V1
func (b *XDRBatchBuilder) processTxResults(txProcessing []xdr.TransactionResultMeta) (txCount, opCount, successTxCount, failedTxCount uint32) {
	txCount = uint32(len(txProcessing))
	
	for _, txMeta := range txProcessing {
		// Count operations in this transaction based on TransactionMeta version
		txApplyMeta := txMeta.TxApplyProcessing
		switch txApplyMeta.V {
		case 0, 1:
			opCount += uint32(len(txApplyMeta.MustV1().Operations))
		case 2:
			opCount += uint32(len(txApplyMeta.MustV2().Operations))
		case 3:
			// V3 has operations in different structure - skip for now
		case 4:
			opCount += uint32(len(txApplyMeta.MustV4().Operations))
		}

		// Check if transaction was successful
		if txMeta.Result.Successful() {
			successTxCount++
		} else {
			failedTxCount++
		}
	}
	
	return
}

// processTxResultsV2 processes TransactionResultMetaV1 for V2
func (b *XDRBatchBuilder) processTxResultsV2(txProcessing []xdr.TransactionResultMetaV1) (txCount, opCount, successTxCount, failedTxCount uint32) {
	txCount = uint32(len(txProcessing))
	
	for _, txMeta := range txProcessing {
		// Count operations in this transaction based on TransactionMeta version
		txApplyMeta := txMeta.TxApplyProcessing
		switch txApplyMeta.V {
		case 0, 1:
			opCount += uint32(len(txApplyMeta.MustV1().Operations))
		case 2:
			opCount += uint32(len(txApplyMeta.MustV2().Operations))
		case 3:
			// V3 has operations in different structure - skip for now
		case 4:
			opCount += uint32(len(txApplyMeta.MustV4().Operations))
		}

		// Check if transaction was successful (V2 uses same Result structure)
		if txMeta.Result.Successful() {
			successTxCount++
		} else {
			failedTxCount++
		}
	}
	
	return
}

// Release cleans up the builder resources
func (b *XDRBatchBuilder) Release() {
	if b.builder != nil {
		b.builder.Release()
	}
}