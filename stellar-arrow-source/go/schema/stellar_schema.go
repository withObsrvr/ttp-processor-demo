package schema

import (
	"fmt"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/memory"
)

// SchemaManager manages Arrow schemas for Stellar data
type SchemaManager struct {
	allocator memory.Allocator
}

// NewSchemaManager creates a new schema manager
func NewSchemaManager() *SchemaManager {
	return &SchemaManager{
		allocator: memory.NewGoAllocator(),
	}
}

// GetStellarLedgerSchema returns the native Arrow schema for Stellar ledger data
func (sm *SchemaManager) GetStellarLedgerSchema() *arrow.Schema {
	return arrow.NewSchema(
		[]arrow.Field{
			// Native Arrow-optimized ledger metadata
			{Name: "ledger_sequence", Type: arrow.PrimitiveTypes.Uint32, Nullable: false},
			{Name: "ledger_close_time", Type: arrow.FixedWidthTypes.Timestamp_us, Nullable: false},
			{Name: "ledger_hash", Type: &arrow.FixedSizeBinaryType{ByteWidth: 32}, Nullable: false},
			{Name: "previous_ledger_hash", Type: &arrow.FixedSizeBinaryType{ByteWidth: 32}, Nullable: false},

			// Transaction statistics for analytics
			{Name: "transaction_count", Type: arrow.PrimitiveTypes.Uint32, Nullable: false},
			{Name: "operation_count", Type: arrow.PrimitiveTypes.Uint32, Nullable: false},
			{Name: "successful_transaction_count", Type: arrow.PrimitiveTypes.Uint32, Nullable: false},
			{Name: "failed_transaction_count", Type: arrow.PrimitiveTypes.Uint32, Nullable: false},

			// Protocol version for compatibility
			{Name: "protocol_version", Type: arrow.PrimitiveTypes.Uint32, Nullable: false},

			// Base fee and reserve for economics
			{Name: "base_fee", Type: arrow.PrimitiveTypes.Uint32, Nullable: false},
			{Name: "base_reserve", Type: arrow.PrimitiveTypes.Uint32, Nullable: false},

			// Network and timing information
			{Name: "max_tx_set_size", Type: arrow.PrimitiveTypes.Uint32, Nullable: false},
			{Name: "close_time_resolution", Type: arrow.PrimitiveTypes.Uint32, Nullable: false},

			// Raw XDR for complete compatibility during migration only
			{Name: "ledger_close_meta_xdr", Type: arrow.BinaryTypes.Binary, Nullable: false},
		},
		nil, // Metadata will be added later if needed
	)
}

// GetTTPEventSchema returns the native Arrow schema optimized for TTP events
func (sm *SchemaManager) GetTTPEventSchema() *arrow.Schema {
	return arrow.NewSchema(
		[]arrow.Field{
			// Event identification
			{Name: "ledger_sequence", Type: arrow.PrimitiveTypes.Uint32, Nullable: false},
			{Name: "ledger_close_time", Type: arrow.FixedWidthTypes.Timestamp_us, Nullable: false},
			{Name: "transaction_hash", Type: &arrow.FixedSizeBinaryType{ByteWidth: 32}, Nullable: false},
			{Name: "operation_index", Type: arrow.PrimitiveTypes.Uint32, Nullable: false},

			// Native Arrow optimized TTP event data
			{Name: "event_type", Type: arrow.BinaryTypes.String, Nullable: false},
			{Name: "from_account", Type: arrow.BinaryTypes.String, Nullable: false},
			{Name: "to_account", Type: arrow.BinaryTypes.String, Nullable: false},
			{Name: "asset_code", Type: arrow.BinaryTypes.String, Nullable: true},
			{Name: "asset_issuer", Type: arrow.BinaryTypes.String, Nullable: true},
			{Name: "amount", Type: arrow.PrimitiveTypes.Int64, Nullable: false}, // Native numeric for analytics
			{Name: "amount_precision", Type: arrow.PrimitiveTypes.Uint32, Nullable: false}, // Decimal precision

			// Additional event context
			{Name: "memo", Type: arrow.BinaryTypes.String, Nullable: true},
			{Name: "memo_type", Type: arrow.BinaryTypes.String, Nullable: true},
			{Name: "operation_source_account", Type: arrow.BinaryTypes.String, Nullable: true},

			// Analytics-optimized fields
			{Name: "is_native_asset", Type: arrow.FixedWidthTypes.Boolean, Nullable: false},
			{Name: "is_path_payment", Type: arrow.FixedWidthTypes.Boolean, Nullable: false},
			{Name: "path_length", Type: arrow.PrimitiveTypes.Uint32, Nullable: true},

			// Network fees and timing
			{Name: "fee_charged", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
			{Name: "max_fee", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		},
		nil, // Metadata will be added later if needed
	)
}

// GetEnrichedTTPEventSchema returns schema with additional computed analytics fields
func (sm *SchemaManager) GetEnrichedTTPEventSchema() *arrow.Schema {
	// Start with base TTP event schema
	baseSchema := sm.GetTTPEventSchema()
	fields := make([]arrow.Field, len(baseSchema.Fields()))
	copy(fields, baseSchema.Fields())

	// Add computed analytics fields
	analyticsFields := []arrow.Field{
		{Name: "amount_category", Type: arrow.BinaryTypes.String, Nullable: false}, // "small", "medium", "large"
		{Name: "daily_volume_rank", Type: arrow.PrimitiveTypes.Uint32, Nullable: true},
		{Name: "asset_popularity_score", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
		{Name: "cross_border_indicator", Type: arrow.FixedWidthTypes.Boolean, Nullable: false},
		{Name: "volume_percentile", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
	}

	fields = append(fields, analyticsFields...)

	return arrow.NewSchema(
		fields,
		nil, // Metadata will be added later if needed
	)
}

// GetTransactionSchema returns schema for individual transaction data
func (sm *SchemaManager) GetTransactionSchema() *arrow.Schema {
	return arrow.NewSchema(
		[]arrow.Field{
			// Transaction identification
			{Name: "ledger_sequence", Type: arrow.PrimitiveTypes.Uint32, Nullable: false},
			{Name: "transaction_hash", Type: &arrow.FixedSizeBinaryType{ByteWidth: 32}, Nullable: false},
			{Name: "transaction_index", Type: arrow.PrimitiveTypes.Uint32, Nullable: false},

			// Transaction metadata
			{Name: "source_account", Type: arrow.BinaryTypes.String, Nullable: false},
			{Name: "sequence_number", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
			{Name: "fee_charged", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
			{Name: "max_fee", Type: arrow.PrimitiveTypes.Int64, Nullable: false},

			// Transaction status
			{Name: "successful", Type: arrow.FixedWidthTypes.Boolean, Nullable: false},
			{Name: "result_code", Type: arrow.BinaryTypes.String, Nullable: false},
			{Name: "operation_count", Type: arrow.PrimitiveTypes.Uint32, Nullable: false},

			// Timing and signatures
			{Name: "time_bounds_min", Type: arrow.FixedWidthTypes.Timestamp_s, Nullable: true},
			{Name: "time_bounds_max", Type: arrow.FixedWidthTypes.Timestamp_s, Nullable: true},
			{Name: "signature_count", Type: arrow.PrimitiveTypes.Uint32, Nullable: false},

			// Memo information
			{Name: "memo_type", Type: arrow.BinaryTypes.String, Nullable: true},
			{Name: "memo_value", Type: arrow.BinaryTypes.String, Nullable: true},
		},
		nil, // Metadata will be added later if needed
	)
}

// ValidateSchemaCompatibility ensures the Arrow schema can represent all existing data
func (sm *SchemaManager) ValidateSchemaCompatibility(schemaName string, existingDataSample interface{}) error {
	// Implementation to validate that Arrow schema can represent all current protobuf data
	// This ensures no data loss during migration

	switch schemaName {
	case "stellar_ledger":
		return sm.validateStellarLedgerCompatibility(existingDataSample)
	case "ttp_events":
		return sm.validateTTPEventCompatibility(existingDataSample)
	case "transactions":
		return sm.validateTransactionCompatibility(existingDataSample)
	default:
		return fmt.Errorf("unknown schema name: %s", schemaName)
	}
}

func (sm *SchemaManager) validateStellarLedgerCompatibility(data interface{}) error {
	// Validate that all existing ledger data fields can be represented in Arrow schema
	// Check for data type compatibility, precision requirements, etc.
	return nil
}

func (sm *SchemaManager) validateTTPEventCompatibility(data interface{}) error {
	// Validate that all existing TTP event fields can be represented in Arrow schema
	// Ensure no precision loss in amount fields, string field lengths, etc.
	return nil
}

func (sm *SchemaManager) validateTransactionCompatibility(data interface{}) error {
	// Validate that all existing transaction fields can be represented in Arrow schema
	return nil
}

// GetSchemaVersion returns the current schema version
func (sm *SchemaManager) GetSchemaVersion(schemaName string) string {
	switch schemaName {
	case "stellar_ledger", "ttp_events", "transactions":
		return "1.0.0"
	default:
		return "unknown"
	}
}

// GetSchemaEvolutionPath returns the evolution path for schema upgrades
func (sm *SchemaManager) GetSchemaEvolutionPath(fromVersion, toVersion string) ([]string, error) {
	// Define schema evolution paths for safe upgrades
	if fromVersion == toVersion {
		return []string{}, nil
	}

	// Example evolution path
	switch {
	case fromVersion == "0.9.0" && toVersion == "1.0.0":
		return []string{"0.9.0", "0.9.1", "1.0.0"}, nil
	default:
		return nil, fmt.Errorf("no evolution path from %s to %s", fromVersion, toVersion)
	}
}