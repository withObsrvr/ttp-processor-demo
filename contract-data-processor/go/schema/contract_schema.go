package schema

import (
	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/memory"
)

// ContractDataSchema defines the Arrow schema for contract data
type ContractDataSchema struct {
	Schema *arrow.Schema
	
	// Field indices for fast access
	ContractIDIdx              int
	ContractKeyTypeIdx         int
	ContractDurabilityIdx      int
	AssetCodeIdx               int
	AssetIssuerIdx             int
	AssetTypeIdx               int
	BalanceHolderIdx           int
	BalanceIdx                 int
	LastModifiedLedgerIdx      int
	DeletedIdx                 int
	LedgerSequenceIdx          int
	ClosedAtIdx                int
	LedgerKeyHashIdx           int
	KeyXdrIdx                  int
	ValXdrIdx                  int
	ContractInstanceTypeIdx    int
	ContractInstanceWasmHashIdx int
	ExpirationLedgerSeqIdx     int
}

// NewContractDataSchema creates a new Arrow schema for contract data
func NewContractDataSchema() *ContractDataSchema {
	// Define fields
	fields := []arrow.Field{
		{Name: "contract_id", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "contract_key_type", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "contract_durability", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "asset_code", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "asset_issuer", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "asset_type", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "balance_holder", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "balance", Type: arrow.PrimitiveTypes.Int64, Nullable: true}, // Store as int64 for precision
		{Name: "last_modified_ledger", Type: arrow.PrimitiveTypes.Uint32, Nullable: false},
		{Name: "deleted", Type: arrow.FixedWidthTypes.Boolean, Nullable: false},
		{Name: "ledger_sequence", Type: arrow.PrimitiveTypes.Uint32, Nullable: false},
		{Name: "closed_at", Type: arrow.FixedWidthTypes.Timestamp_us, Nullable: false}, // Microsecond precision
		{Name: "ledger_key_hash", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "key_xdr", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "val_xdr", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "contract_instance_type", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "contract_instance_wasm_hash", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "expiration_ledger_seq", Type: arrow.PrimitiveTypes.Uint32, Nullable: true},
	}
	
	// Create schema
	schema := arrow.NewSchema(fields, nil)
	
	// Build field index map
	s := &ContractDataSchema{
		Schema: schema,
	}
	
	// Set field indices
	for i, field := range fields {
		switch field.Name {
		case "contract_id":
			s.ContractIDIdx = i
		case "contract_key_type":
			s.ContractKeyTypeIdx = i
		case "contract_durability":
			s.ContractDurabilityIdx = i
		case "asset_code":
			s.AssetCodeIdx = i
		case "asset_issuer":
			s.AssetIssuerIdx = i
		case "asset_type":
			s.AssetTypeIdx = i
		case "balance_holder":
			s.BalanceHolderIdx = i
		case "balance":
			s.BalanceIdx = i
		case "last_modified_ledger":
			s.LastModifiedLedgerIdx = i
		case "deleted":
			s.DeletedIdx = i
		case "ledger_sequence":
			s.LedgerSequenceIdx = i
		case "closed_at":
			s.ClosedAtIdx = i
		case "ledger_key_hash":
			s.LedgerKeyHashIdx = i
		case "key_xdr":
			s.KeyXdrIdx = i
		case "val_xdr":
			s.ValXdrIdx = i
		case "contract_instance_type":
			s.ContractInstanceTypeIdx = i
		case "contract_instance_wasm_hash":
			s.ContractInstanceWasmHashIdx = i
		case "expiration_ledger_seq":
			s.ExpirationLedgerSeqIdx = i
		}
	}
	
	return s
}

// GetFieldByName returns the field index by name
func (s *ContractDataSchema) GetFieldByName(name string) (int, bool) {
	for i, field := range s.Schema.Fields() {
		if field.Name == name {
			return i, true
		}
	}
	return -1, false
}

// Metadata returns schema metadata
func (s *ContractDataSchema) Metadata() map[string]string {
	return map[string]string{
		"schema_version": "1.0",
		"schema_type":    "contract_data",
		"description":    "Stellar smart contract data changes",
	}
}

// SchemaManager manages Arrow schemas
type SchemaManager struct {
	contractSchema *ContractDataSchema
	allocator      memory.Allocator
}

// NewSchemaManager creates a new schema manager
func NewSchemaManager(allocator memory.Allocator) *SchemaManager {
	return &SchemaManager{
		contractSchema: NewContractDataSchema(),
		allocator:      allocator,
	}
}

// GetContractDataSchema returns the contract data schema
func (m *SchemaManager) GetContractDataSchema() *ContractDataSchema {
	return m.contractSchema
}

// GetAllocator returns the memory allocator
func (m *SchemaManager) GetAllocator() memory.Allocator {
	return m.allocator
}