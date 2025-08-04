package client

import (
	"fmt"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/stellar/go/xdr"
)

// XDRBatchBuilder efficiently builds Arrow record batches from XDR data
type XDRBatchBuilder struct {
	allocator memory.Allocator
	schema    *arrow.Schema
	builder   *array.RecordBuilder
	batchSize int
	count     int
}

// NewXDRBatchBuilder creates a new XDR batch builder
func NewXDRBatchBuilder(allocator memory.Allocator, schema *arrow.Schema, batchSize int) *XDRBatchBuilder {
	builder := array.NewRecordBuilder(allocator, schema)
	
	return &XDRBatchBuilder{
		allocator: allocator,
		schema:    schema,
		builder:   builder,
		batchSize: batchSize,
		count:     0,
	}
}

// AddLedger adds a Stellar ledger to the batch
func (b *XDRBatchBuilder) AddLedger(lcm *xdr.LedgerCloseMeta) error {
	if b.count >= b.batchSize {
		return fmt.Errorf("batch is full, cannot add more ledgers")
	}

	// Extract fields based on Stellar ledger schema
	// Schema fields (from stellar-arrow-source/go/schema/stellar_schema.go):
	// 0: ledger_sequence (uint32)
	// 1: ledger_close_time (timestamp[us])
	// 2: ledger_hash (fixed_size_binary[32])
	// 3: previous_ledger_hash (fixed_size_binary[32])
	// 4: transaction_count (uint32)
	// 5: operation_count (uint32)
	// 6: successful_transaction_count (uint32)
	// 7: failed_transaction_count (uint32)
	// 8: protocol_version (uint32)
	// 9: base_fee (uint32) 
	// 10: base_reserve (uint32)
	// 11: max_tx_set_size (uint32)
	// 12: close_time_resolution (uint32)
	// 13: ledger_close_meta_xdr (binary)

	// Get builders for each field
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

	// Use the built-in helper methods from LedgerCloseMeta
	ledgerHeader := lcm.LedgerHeaderHistoryEntry().Header
	txCount := uint32(lcm.CountTransactions())
	
	// Count operations and success/failure rates
	var opCount, successTxCount, failedTxCount uint32
	
	switch lcm.V {
	case 0:
		for _, result := range lcm.MustV0().TxProcessing {
			if result.Result.Successful() {
				successTxCount++
				if opResults, ok := result.Result.OperationResults(); ok {
					opCount += uint32(len(opResults))
				}
			} else {
				failedTxCount++
			}
		}
	case 1:
		for _, result := range lcm.MustV1().TxProcessing {
			if result.Result.Successful() {
				successTxCount++
				if opResults, ok := result.Result.OperationResults(); ok {
					opCount += uint32(len(opResults))
				}
			} else {
				failedTxCount++
			}
		}
	case 2:
		for _, result := range lcm.MustV2().TxProcessing {
			if result.Result.Successful() {
				successTxCount++
				if opResults, ok := result.Result.OperationResults(); ok {
					opCount += uint32(len(opResults))
				}
			} else {
				failedTxCount++
			}
		}
	default:
		return fmt.Errorf("unsupported LedgerCloseMeta version: %d", lcm.V)
	}

	// Convert to Arrow fields using helper methods
	ledgerSeqBuilder.Append(lcm.LedgerSequence())
	
	// Convert Stellar timestamp to Arrow timestamp (microseconds)
	closeTime := lcm.ClosedAt()
	closeTimeBuilder.Append(arrow.Timestamp(closeTime.UnixMicro()))
	
	// Hash fields (32 bytes each) - use helper methods
	ledgerHash := lcm.LedgerHash()
	prevHash := lcm.PreviousLedgerHash()
	ledgerHashBuilder.Append(ledgerHash[:])
	prevHashBuilder.Append(prevHash[:])
	
	// Transaction and operation counts
	txCountBuilder.Append(txCount)
	opCountBuilder.Append(opCount)
	successTxBuilder.Append(successTxCount)
	failedTxBuilder.Append(failedTxCount)
	
	// Protocol and network info
	protocolBuilder.Append(lcm.ProtocolVersion())
	baseFeeBuilder.Append(uint32(ledgerHeader.BaseFee))
	baseReserveBuilder.Append(uint32(ledgerHeader.BaseReserve))
	maxTxSetBuilder.Append(uint32(ledgerHeader.MaxTxSetSize))
	closeResBuilder.Append(1) // closeTimeResolution - default to 1 second
	
	// Store original XDR bytes
	xdrBytes, err := lcm.MarshalBinary()
	if err != nil {
		return fmt.Errorf("failed to marshal LedgerCloseMeta to XDR: %w", err)
	}
	xdrBuilder.Append(xdrBytes)

	b.count++
	return nil
}

// Build creates an Arrow record from the current batch
func (b *XDRBatchBuilder) Build() (arrow.Record, error) {
	if b.count == 0 {
		return nil, fmt.Errorf("no ledgers in batch")
	}

	record := b.builder.NewRecord()
	b.count = 0 // Reset count but keep builder for reuse
	return record, nil
}

// Reset clears the batch builder for reuse
func (b *XDRBatchBuilder) Reset() {
	// Note: We don't release the builder, just reset its state
	// The builder will be reused for the next batch
	b.count = 0
}

// Release releases all resources
func (b *XDRBatchBuilder) Release() {
	if b.builder != nil {
		b.builder.Release()
		b.builder = nil
	}
}

// Count returns the number of ledgers currently in the batch
func (b *XDRBatchBuilder) Count() int {
	return b.count
}

// IsFull returns true if the batch is at capacity
func (b *XDRBatchBuilder) IsFull() bool {
	return b.count >= b.batchSize
}