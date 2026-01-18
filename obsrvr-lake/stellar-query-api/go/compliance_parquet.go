package main

import (
	"bytes"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
)

// ============================================
// PARQUET EXPORT UTILITIES
// ============================================

// TransactionsToParquet converts transaction data to Parquet format
func TransactionsToParquet(transactions []ComplianceTransaction) ([]byte, error) {
	pool := memory.NewGoAllocator()

	// Define schema
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "ledger_sequence", Type: arrow.PrimitiveTypes.Int64},
		{Name: "closed_at", Type: arrow.BinaryTypes.String},
		{Name: "transaction_hash", Type: arrow.BinaryTypes.String},
		{Name: "operation_index", Type: arrow.PrimitiveTypes.Int32},
		{Name: "operation_type", Type: arrow.BinaryTypes.String},
		{Name: "from_account", Type: arrow.BinaryTypes.String},
		{Name: "to_account", Type: arrow.BinaryTypes.String},
		{Name: "amount", Type: arrow.BinaryTypes.String},
		{Name: "successful", Type: arrow.FixedWidthTypes.Boolean},
	}, nil)

	// Build arrays
	ledgerSeqBuilder := array.NewInt64Builder(pool)
	closedAtBuilder := array.NewStringBuilder(pool)
	txHashBuilder := array.NewStringBuilder(pool)
	opIndexBuilder := array.NewInt32Builder(pool)
	opTypeBuilder := array.NewStringBuilder(pool)
	fromAccountBuilder := array.NewStringBuilder(pool)
	toAccountBuilder := array.NewStringBuilder(pool)
	amountBuilder := array.NewStringBuilder(pool)
	successfulBuilder := array.NewBooleanBuilder(pool)

	defer ledgerSeqBuilder.Release()
	defer closedAtBuilder.Release()
	defer txHashBuilder.Release()
	defer opIndexBuilder.Release()
	defer opTypeBuilder.Release()
	defer fromAccountBuilder.Release()
	defer toAccountBuilder.Release()
	defer amountBuilder.Release()
	defer successfulBuilder.Release()

	for _, tx := range transactions {
		ledgerSeqBuilder.Append(tx.LedgerSequence)
		closedAtBuilder.Append(tx.ClosedAt)
		txHashBuilder.Append(tx.TransactionHash)
		opIndexBuilder.Append(int32(tx.OperationIndex))
		opTypeBuilder.Append(tx.OperationType)
		fromAccountBuilder.Append(tx.FromAccount)
		toAccountBuilder.Append(tx.ToAccount)
		amountBuilder.Append(tx.Amount)
		successfulBuilder.Append(tx.Successful)
	}

	// Create record batch
	record := array.NewRecord(schema, []arrow.Array{
		ledgerSeqBuilder.NewArray(),
		closedAtBuilder.NewArray(),
		txHashBuilder.NewArray(),
		opIndexBuilder.NewArray(),
		opTypeBuilder.NewArray(),
		fromAccountBuilder.NewArray(),
		toAccountBuilder.NewArray(),
		amountBuilder.NewArray(),
		successfulBuilder.NewArray(),
	}, int64(len(transactions)))
	defer record.Release()

	// Write to parquet
	var buf bytes.Buffer
	writer, err := pqarrow.NewFileWriter(schema, &buf, nil, pqarrow.DefaultWriterProps())
	if err != nil {
		return nil, fmt.Errorf("failed to create parquet writer: %w", err)
	}

	if err := writer.Write(record); err != nil {
		writer.Close()
		return nil, fmt.Errorf("failed to write parquet record: %w", err)
	}

	if err := writer.Close(); err != nil {
		return nil, fmt.Errorf("failed to close parquet writer: %w", err)
	}

	return buf.Bytes(), nil
}

// BalancesToParquet converts balance holder data to Parquet format
func BalancesToParquet(holders []ComplianceHolder) ([]byte, error) {
	pool := memory.NewGoAllocator()

	// Define schema
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "account_id", Type: arrow.BinaryTypes.String},
		{Name: "balance", Type: arrow.BinaryTypes.String},
		{Name: "percent_of_supply", Type: arrow.BinaryTypes.String},
	}, nil)

	// Build arrays
	accountBuilder := array.NewStringBuilder(pool)
	balanceBuilder := array.NewStringBuilder(pool)
	percentBuilder := array.NewStringBuilder(pool)

	defer accountBuilder.Release()
	defer balanceBuilder.Release()
	defer percentBuilder.Release()

	for _, h := range holders {
		accountBuilder.Append(h.AccountID)
		balanceBuilder.Append(h.Balance)
		percentBuilder.Append(h.PercentOfSupply)
	}

	// Create record batch
	record := array.NewRecord(schema, []arrow.Array{
		accountBuilder.NewArray(),
		balanceBuilder.NewArray(),
		percentBuilder.NewArray(),
	}, int64(len(holders)))
	defer record.Release()

	// Write to parquet
	var buf bytes.Buffer
	writer, err := pqarrow.NewFileWriter(schema, &buf, nil, pqarrow.DefaultWriterProps())
	if err != nil {
		return nil, fmt.Errorf("failed to create parquet writer: %w", err)
	}

	if err := writer.Write(record); err != nil {
		writer.Close()
		return nil, fmt.Errorf("failed to write parquet record: %w", err)
	}

	if err := writer.Close(); err != nil {
		return nil, fmt.Errorf("failed to close parquet writer: %w", err)
	}

	return buf.Bytes(), nil
}

// SupplyTimelineToParquet converts supply timeline data to Parquet format
func SupplyTimelineToParquet(timeline []SupplyDataPoint) ([]byte, error) {
	pool := memory.NewGoAllocator()

	// Define schema
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "timestamp", Type: arrow.BinaryTypes.String},
		{Name: "ledger_sequence", Type: arrow.PrimitiveTypes.Int64},
		{Name: "total_supply", Type: arrow.BinaryTypes.String},
		{Name: "circulating_supply", Type: arrow.BinaryTypes.String},
		{Name: "issuer_balance", Type: arrow.BinaryTypes.String},
		{Name: "holder_count", Type: arrow.PrimitiveTypes.Int32},
		{Name: "supply_change", Type: arrow.BinaryTypes.String},
		{Name: "supply_change_percent", Type: arrow.BinaryTypes.String},
	}, nil)

	// Build arrays
	timestampBuilder := array.NewStringBuilder(pool)
	ledgerSeqBuilder := array.NewInt64Builder(pool)
	totalSupplyBuilder := array.NewStringBuilder(pool)
	circSupplyBuilder := array.NewStringBuilder(pool)
	issuerBalBuilder := array.NewStringBuilder(pool)
	holderCountBuilder := array.NewInt32Builder(pool)
	supplyChangeBuilder := array.NewStringBuilder(pool)
	supplyChangePctBuilder := array.NewStringBuilder(pool)

	defer timestampBuilder.Release()
	defer ledgerSeqBuilder.Release()
	defer totalSupplyBuilder.Release()
	defer circSupplyBuilder.Release()
	defer issuerBalBuilder.Release()
	defer holderCountBuilder.Release()
	defer supplyChangeBuilder.Release()
	defer supplyChangePctBuilder.Release()

	for _, dp := range timeline {
		timestampBuilder.Append(dp.Timestamp)
		ledgerSeqBuilder.Append(dp.LedgerSequence)
		totalSupplyBuilder.Append(dp.TotalSupply)
		circSupplyBuilder.Append(dp.CirculatingSupply)
		issuerBalBuilder.Append(dp.IssuerBalance)
		holderCountBuilder.Append(int32(dp.HolderCount))

		if dp.SupplyChange != nil {
			supplyChangeBuilder.Append(*dp.SupplyChange)
		} else {
			supplyChangeBuilder.Append("")
		}

		if dp.SupplyChangePercent != nil {
			supplyChangePctBuilder.Append(*dp.SupplyChangePercent)
		} else {
			supplyChangePctBuilder.Append("")
		}
	}

	// Create record batch
	record := array.NewRecord(schema, []arrow.Array{
		timestampBuilder.NewArray(),
		ledgerSeqBuilder.NewArray(),
		totalSupplyBuilder.NewArray(),
		circSupplyBuilder.NewArray(),
		issuerBalBuilder.NewArray(),
		holderCountBuilder.NewArray(),
		supplyChangeBuilder.NewArray(),
		supplyChangePctBuilder.NewArray(),
	}, int64(len(timeline)))
	defer record.Release()

	// Write to parquet
	var buf bytes.Buffer
	writer, err := pqarrow.NewFileWriter(schema, &buf, nil, pqarrow.DefaultWriterProps())
	if err != nil {
		return nil, fmt.Errorf("failed to create parquet writer: %w", err)
	}

	if err := writer.Write(record); err != nil {
		writer.Close()
		return nil, fmt.Errorf("failed to write parquet record: %w", err)
	}

	if err := writer.Close(); err != nil {
		return nil, fmt.Errorf("failed to close parquet writer: %w", err)
	}

	return buf.Bytes(), nil
}
