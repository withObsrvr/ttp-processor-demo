package processor

import (
	"fmt"
	"strconv"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/withObsrvr/ttp-processor-demo/contract-data-processor/schema"
)

// ArrowTransformer converts contract data entries to Arrow records
type ArrowTransformer struct {
	allocator     memory.Allocator
	schemaManager *schema.SchemaManager
	schema        *schema.ContractDataSchema
	
	// Builders for each column
	builders struct {
		contractID              *array.StringBuilder
		contractKeyType         *array.StringBuilder
		contractDurability      *array.StringBuilder
		assetCode               *array.StringBuilder
		assetIssuer             *array.StringBuilder
		assetType               *array.StringBuilder
		balanceHolder           *array.StringBuilder
		balance                 *array.Int64Builder
		lastModifiedLedger      *array.Uint32Builder
		deleted                 *array.BooleanBuilder
		ledgerSequence          *array.Uint32Builder
		closedAt                *array.TimestampBuilder
		ledgerKeyHash           *array.StringBuilder
		keyXdr                  *array.StringBuilder
		valXdr                  *array.StringBuilder
		contractInstanceType    *array.StringBuilder
		contractInstanceWasmHash *array.StringBuilder
		expirationLedgerSeq     *array.Uint32Builder
	}
	
	// Current batch size
	currentSize int
}

// NewArrowTransformer creates a new transformer
func NewArrowTransformer(allocator memory.Allocator) *ArrowTransformer {
	schemaManager := schema.NewSchemaManager(allocator)
	contractSchema := schemaManager.GetContractDataSchema()
	
	t := &ArrowTransformer{
		allocator:     allocator,
		schemaManager: schemaManager,
		schema:        contractSchema,
	}
	
	// Initialize builders
	t.initBuilders()
	
	return t
}

// initBuilders initializes all Arrow builders
func (t *ArrowTransformer) initBuilders() {
	t.builders.contractID = array.NewStringBuilder(t.allocator)
	t.builders.contractKeyType = array.NewStringBuilder(t.allocator)
	t.builders.contractDurability = array.NewStringBuilder(t.allocator)
	t.builders.assetCode = array.NewStringBuilder(t.allocator)
	t.builders.assetIssuer = array.NewStringBuilder(t.allocator)
	t.builders.assetType = array.NewStringBuilder(t.allocator)
	t.builders.balanceHolder = array.NewStringBuilder(t.allocator)
	t.builders.balance = array.NewInt64Builder(t.allocator)
	t.builders.lastModifiedLedger = array.NewUint32Builder(t.allocator)
	t.builders.deleted = array.NewBooleanBuilder(t.allocator)
	t.builders.ledgerSequence = array.NewUint32Builder(t.allocator)
	t.builders.closedAt = array.NewTimestampBuilder(t.allocator, &arrow.TimestampType{
		Unit: arrow.Microsecond,
	})
	t.builders.ledgerKeyHash = array.NewStringBuilder(t.allocator)
	t.builders.keyXdr = array.NewStringBuilder(t.allocator)
	t.builders.valXdr = array.NewStringBuilder(t.allocator)
	t.builders.contractInstanceType = array.NewStringBuilder(t.allocator)
	t.builders.contractInstanceWasmHash = array.NewStringBuilder(t.allocator)
	t.builders.expirationLedgerSeq = array.NewUint32Builder(t.allocator)
}

// AddEntry adds a contract data entry to the current batch
func (t *ArrowTransformer) AddEntry(entry *ContractDataEntry) error {
	// Append to each builder
	t.builders.contractID.Append(entry.ContractId)
	t.builders.contractKeyType.Append(entry.ContractKeyType)
	t.builders.contractDurability.Append(entry.ContractDurability)
	
	// Handle nullable fields
	if entry.ContractDataAssetCode != "" {
		t.builders.assetCode.Append(entry.ContractDataAssetCode)
	} else {
		t.builders.assetCode.AppendNull()
	}
	
	if entry.ContractDataAssetIssuer != "" {
		t.builders.assetIssuer.Append(entry.ContractDataAssetIssuer)
	} else {
		t.builders.assetIssuer.AppendNull()
	}
	
	if entry.ContractDataAssetType != "" {
		t.builders.assetType.Append(entry.ContractDataAssetType)
	} else {
		t.builders.assetType.AppendNull()
	}
	
	if entry.ContractDataBalanceHolder != "" {
		t.builders.balanceHolder.Append(entry.ContractDataBalanceHolder)
	} else {
		t.builders.balanceHolder.AppendNull()
	}
	
	// Convert balance string to int64
	if entry.ContractDataBalance != "" {
		balance, err := strconv.ParseInt(entry.ContractDataBalance, 10, 64)
		if err != nil {
			t.builders.balance.AppendNull()
		} else {
			t.builders.balance.Append(balance)
		}
	} else {
		t.builders.balance.AppendNull()
	}
	
	t.builders.lastModifiedLedger.Append(entry.LastModifiedLedger)
	t.builders.deleted.Append(entry.Deleted)
	t.builders.ledgerSequence.Append(entry.LedgerSequence)
	t.builders.closedAt.Append(arrow.Timestamp(entry.ClosedAt))
	
	if entry.LedgerKeyHash != "" {
		t.builders.ledgerKeyHash.Append(entry.LedgerKeyHash)
	} else {
		t.builders.ledgerKeyHash.AppendNull()
	}
	
	if entry.KeyXdr != "" {
		t.builders.keyXdr.Append(entry.KeyXdr)
	} else {
		t.builders.keyXdr.AppendNull()
	}
	
	if entry.ValXdr != "" {
		t.builders.valXdr.Append(entry.ValXdr)
	} else {
		t.builders.valXdr.AppendNull()
	}
	
	if entry.ContractInstanceType != "" {
		t.builders.contractInstanceType.Append(entry.ContractInstanceType)
	} else {
		t.builders.contractInstanceType.AppendNull()
	}
	
	if entry.ContractInstanceWasmHash != "" {
		t.builders.contractInstanceWasmHash.Append(entry.ContractInstanceWasmHash)
	} else {
		t.builders.contractInstanceWasmHash.AppendNull()
	}
	
	if entry.ExpirationLedgerSeq > 0 {
		t.builders.expirationLedgerSeq.Append(entry.ExpirationLedgerSeq)
	} else {
		t.builders.expirationLedgerSeq.AppendNull()
	}
	
	t.currentSize++
	return nil
}

// BuildRecord creates an Arrow record from the current batch
func (t *ArrowTransformer) BuildRecord() (arrow.Record, error) {
	if t.currentSize == 0 {
		return nil, fmt.Errorf("no entries to build record from")
	}
	
	// Create arrays from builders
	arrays := make([]arrow.Array, 0, 18)
	
	arrays = append(arrays, t.builders.contractID.NewArray())
	arrays = append(arrays, t.builders.contractKeyType.NewArray())
	arrays = append(arrays, t.builders.contractDurability.NewArray())
	arrays = append(arrays, t.builders.assetCode.NewArray())
	arrays = append(arrays, t.builders.assetIssuer.NewArray())
	arrays = append(arrays, t.builders.assetType.NewArray())
	arrays = append(arrays, t.builders.balanceHolder.NewArray())
	arrays = append(arrays, t.builders.balance.NewArray())
	arrays = append(arrays, t.builders.lastModifiedLedger.NewArray())
	arrays = append(arrays, t.builders.deleted.NewArray())
	arrays = append(arrays, t.builders.ledgerSequence.NewArray())
	arrays = append(arrays, t.builders.closedAt.NewArray())
	arrays = append(arrays, t.builders.ledgerKeyHash.NewArray())
	arrays = append(arrays, t.builders.keyXdr.NewArray())
	arrays = append(arrays, t.builders.valXdr.NewArray())
	arrays = append(arrays, t.builders.contractInstanceType.NewArray())
	arrays = append(arrays, t.builders.contractInstanceWasmHash.NewArray())
	arrays = append(arrays, t.builders.expirationLedgerSeq.NewArray())
	
	// Create record
	record := array.NewRecord(t.schema.Schema, arrays, int64(t.currentSize))
	
	// Release arrays (record holds references)
	for _, arr := range arrays {
		arr.Release()
	}
	
	return record, nil
}

// Reset clears the transformer for a new batch
func (t *ArrowTransformer) Reset() {
	// Release old builders
	t.releaseBuilders()
	
	// Create new builders
	t.initBuilders()
	
	t.currentSize = 0
}

// releaseBuilders releases all builders
func (t *ArrowTransformer) releaseBuilders() {
	t.builders.contractID.Release()
	t.builders.contractKeyType.Release()
	t.builders.contractDurability.Release()
	t.builders.assetCode.Release()
	t.builders.assetIssuer.Release()
	t.builders.assetType.Release()
	t.builders.balanceHolder.Release()
	t.builders.balance.Release()
	t.builders.lastModifiedLedger.Release()
	t.builders.deleted.Release()
	t.builders.ledgerSequence.Release()
	t.builders.closedAt.Release()
	t.builders.ledgerKeyHash.Release()
	t.builders.keyXdr.Release()
	t.builders.valXdr.Release()
	t.builders.contractInstanceType.Release()
	t.builders.contractInstanceWasmHash.Release()
	t.builders.expirationLedgerSeq.Release()
}

// Release cleans up resources
func (t *ArrowTransformer) Release() {
	t.releaseBuilders()
}

// GetSchema returns the Arrow schema
func (t *ArrowTransformer) GetSchema() *arrow.Schema {
	return t.schema.Schema
}

// GetCurrentSize returns the current batch size
func (t *ArrowTransformer) GetCurrentSize() int {
	return t.currentSize
}