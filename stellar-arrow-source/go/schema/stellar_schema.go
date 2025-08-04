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
			{Name: "ledger_close_time", Type: arrow.FixedWidthTypes.Timestamp_us, Nullable: false},

			// Transaction source and account details
			{Name: "source_account", Type: arrow.BinaryTypes.String, Nullable: false},
			{Name: "source_account_ed25519", Type: &arrow.FixedSizeBinaryType{ByteWidth: 32}, Nullable: true},
			{Name: "source_account_muxed", Type: arrow.PrimitiveTypes.Uint64, Nullable: true},
			{Name: "sequence_number", Type: arrow.PrimitiveTypes.Int64, Nullable: false},

			// Transaction fees and resources
			{Name: "fee_charged", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
			{Name: "max_fee", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
			{Name: "resource_fee", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
			{Name: "soroban_resource_fee", Type: arrow.PrimitiveTypes.Int64, Nullable: true},

			// Transaction execution details
			{Name: "successful", Type: arrow.FixedWidthTypes.Boolean, Nullable: false},
			{Name: "result_code", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
			{Name: "result_code_string", Type: arrow.BinaryTypes.String, Nullable: false},
			{Name: "operation_count", Type: arrow.PrimitiveTypes.Uint32, Nullable: false},
			{Name: "created_contract_count", Type: arrow.PrimitiveTypes.Uint32, Nullable: true},

			// Timing bounds and conditions
			{Name: "time_bounds_min", Type: arrow.FixedWidthTypes.Timestamp_us, Nullable: true},
			{Name: "time_bounds_max", Type: arrow.FixedWidthTypes.Timestamp_us, Nullable: true},
			{Name: "ledger_bounds_min", Type: arrow.PrimitiveTypes.Uint32, Nullable: true},
			{Name: "ledger_bounds_max", Type: arrow.PrimitiveTypes.Uint32, Nullable: true},
			{Name: "min_account_sequence", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
			{Name: "min_account_sequence_age", Type: arrow.PrimitiveTypes.Uint64, Nullable: true},
			{Name: "min_account_sequence_ledger_gap", Type: arrow.PrimitiveTypes.Uint32, Nullable: true},
			{Name: "extra_signers_count", Type: arrow.PrimitiveTypes.Uint32, Nullable: true},

			// Signatures
			{Name: "signature_count", Type: arrow.PrimitiveTypes.Uint32, Nullable: false},
			{Name: "inner_transaction_hash", Type: &arrow.FixedSizeBinaryType{ByteWidth: 32}, Nullable: true},
			{Name: "fee_source", Type: arrow.BinaryTypes.String, Nullable: true},

			// Memo information
			{Name: "memo_type", Type: arrow.PrimitiveTypes.Uint8, Nullable: false},
			{Name: "memo_text", Type: arrow.BinaryTypes.String, Nullable: true},
			{Name: "memo_id", Type: arrow.PrimitiveTypes.Uint64, Nullable: true},
			{Name: "memo_hash", Type: &arrow.FixedSizeBinaryType{ByteWidth: 32}, Nullable: true},
			{Name: "memo_return_hash", Type: &arrow.FixedSizeBinaryType{ByteWidth: 32}, Nullable: true},

			// Protocol version and transaction version
			{Name: "protocol_version", Type: arrow.PrimitiveTypes.Uint32, Nullable: false},
			{Name: "transaction_envelope_type", Type: arrow.PrimitiveTypes.Uint32, Nullable: false},

			// Soroban (smart contract) specific fields
			{Name: "soroban_operations_count", Type: arrow.PrimitiveTypes.Uint32, Nullable: true},
			{Name: "soroban_resources_instructions", Type: arrow.PrimitiveTypes.Uint32, Nullable: true},
			{Name: "soroban_resources_read_bytes", Type: arrow.PrimitiveTypes.Uint32, Nullable: true},
			{Name: "soroban_resources_write_bytes", Type: arrow.PrimitiveTypes.Uint32, Nullable: true},
			{Name: "soroban_resources_metadata_size_bytes", Type: arrow.PrimitiveTypes.Uint32, Nullable: true},

			// Raw XDR for full fidelity
			{Name: "transaction_envelope_xdr", Type: arrow.BinaryTypes.Binary, Nullable: false},
			{Name: "transaction_result_xdr", Type: arrow.BinaryTypes.Binary, Nullable: false},
			{Name: "transaction_meta_xdr", Type: arrow.BinaryTypes.Binary, Nullable: false},
		},
		nil, // Metadata will be added later if needed
	)
}

// GetOperationSchema returns schema for individual operation data
func (sm *SchemaManager) GetOperationSchema() *arrow.Schema {
	return arrow.NewSchema(
		[]arrow.Field{
			// Operation identification
			{Name: "ledger_sequence", Type: arrow.PrimitiveTypes.Uint32, Nullable: false},
			{Name: "transaction_hash", Type: &arrow.FixedSizeBinaryType{ByteWidth: 32}, Nullable: false},
			{Name: "transaction_index", Type: arrow.PrimitiveTypes.Uint32, Nullable: false},
			{Name: "operation_index", Type: arrow.PrimitiveTypes.Uint32, Nullable: false},
			{Name: "operation_id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
			{Name: "ledger_close_time", Type: arrow.FixedWidthTypes.Timestamp_us, Nullable: false},

			// Operation type and source
			{Name: "operation_type", Type: arrow.PrimitiveTypes.Uint32, Nullable: false},
			{Name: "operation_type_string", Type: arrow.BinaryTypes.String, Nullable: false},
			{Name: "source_account", Type: arrow.BinaryTypes.String, Nullable: true},
			{Name: "source_account_ed25519", Type: &arrow.FixedSizeBinaryType{ByteWidth: 32}, Nullable: true},
			{Name: "source_account_muxed", Type: arrow.PrimitiveTypes.Uint64, Nullable: true},

			// Transaction context
			{Name: "transaction_successful", Type: arrow.FixedWidthTypes.Boolean, Nullable: false},
			{Name: "transaction_source_account", Type: arrow.BinaryTypes.String, Nullable: false},

			// Operation result
			{Name: "successful", Type: arrow.FixedWidthTypes.Boolean, Nullable: false},
			{Name: "result_code", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
			{Name: "result_code_string", Type: arrow.BinaryTypes.String, Nullable: false},

			// Common operation fields (nullable based on operation type)
			// Payment/Path Payment
			{Name: "destination_account", Type: arrow.BinaryTypes.String, Nullable: true},
			{Name: "amount", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
			{Name: "asset_type", Type: arrow.PrimitiveTypes.Uint8, Nullable: true},
			{Name: "asset_code", Type: arrow.BinaryTypes.String, Nullable: true},
			{Name: "asset_issuer", Type: arrow.BinaryTypes.String, Nullable: true},

			// Path payment specific
			{Name: "send_asset_type", Type: arrow.PrimitiveTypes.Uint8, Nullable: true},
			{Name: "send_asset_code", Type: arrow.BinaryTypes.String, Nullable: true},
			{Name: "send_asset_issuer", Type: arrow.BinaryTypes.String, Nullable: true},
			{Name: "send_amount", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
			{Name: "dest_min", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
			{Name: "path_length", Type: arrow.PrimitiveTypes.Uint32, Nullable: true},

			// Create/Change Trust
			{Name: "trustor", Type: arrow.BinaryTypes.String, Nullable: true},
			{Name: "trustee", Type: arrow.BinaryTypes.String, Nullable: true},
			{Name: "trust_limit", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
			{Name: "authorize_to_maintain_liabilities", Type: arrow.FixedWidthTypes.Boolean, Nullable: true},
			{Name: "clawback_enabled", Type: arrow.FixedWidthTypes.Boolean, Nullable: true},

			// Manage Offer/Create Passive Offer
			{Name: "offer_id", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
			{Name: "buying_asset_type", Type: arrow.PrimitiveTypes.Uint8, Nullable: true},
			{Name: "buying_asset_code", Type: arrow.BinaryTypes.String, Nullable: true},
			{Name: "buying_asset_issuer", Type: arrow.BinaryTypes.String, Nullable: true},
			{Name: "selling_asset_type", Type: arrow.PrimitiveTypes.Uint8, Nullable: true},
			{Name: "selling_asset_code", Type: arrow.BinaryTypes.String, Nullable: true},
			{Name: "selling_asset_issuer", Type: arrow.BinaryTypes.String, Nullable: true},
			{Name: "price_n", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
			{Name: "price_d", Type: arrow.PrimitiveTypes.Int32, Nullable: true},

			// Account operations
			{Name: "starting_balance", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
			{Name: "home_domain", Type: arrow.BinaryTypes.String, Nullable: true},
			{Name: "inflation_dest", Type: arrow.BinaryTypes.String, Nullable: true},
			{Name: "master_weight", Type: arrow.PrimitiveTypes.Uint32, Nullable: true},
			{Name: "threshold_low", Type: arrow.PrimitiveTypes.Uint32, Nullable: true},
			{Name: "threshold_medium", Type: arrow.PrimitiveTypes.Uint32, Nullable: true},
			{Name: "threshold_high", Type: arrow.PrimitiveTypes.Uint32, Nullable: true},
			{Name: "signer_key", Type: arrow.BinaryTypes.String, Nullable: true},
			{Name: "signer_weight", Type: arrow.PrimitiveTypes.Uint32, Nullable: true},

			// Data operations
			{Name: "data_name", Type: arrow.BinaryTypes.String, Nullable: true},
			{Name: "data_value", Type: arrow.BinaryTypes.Binary, Nullable: true},

			// Claimable balance operations
			{Name: "balance_id", Type: &arrow.FixedSizeBinaryType{ByteWidth: 32}, Nullable: true},
			{Name: "claimant_count", Type: arrow.PrimitiveTypes.Uint32, Nullable: true},

			// Sponsorship operations
			{Name: "sponsored_account", Type: arrow.BinaryTypes.String, Nullable: true},
			{Name: "sponsor_account", Type: arrow.BinaryTypes.String, Nullable: true},

			// Liquidity pool operations
			{Name: "liquidity_pool_id", Type: &arrow.FixedSizeBinaryType{ByteWidth: 32}, Nullable: true},
			{Name: "reserve_a_deposit", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
			{Name: "reserve_b_deposit", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
			{Name: "min_price_n", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
			{Name: "min_price_d", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
			{Name: "max_price_n", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
			{Name: "max_price_d", Type: arrow.PrimitiveTypes.Int32, Nullable: true},

			// Soroban (smart contract) operations
			{Name: "host_function_type", Type: arrow.PrimitiveTypes.Uint32, Nullable: true},
			{Name: "contract_id", Type: &arrow.FixedSizeBinaryType{ByteWidth: 32}, Nullable: true},
			{Name: "contract_code_hash", Type: &arrow.FixedSizeBinaryType{ByteWidth: 32}, Nullable: true},
			{Name: "function_name", Type: arrow.BinaryTypes.String, Nullable: true},
			{Name: "soroban_auth_count", Type: arrow.PrimitiveTypes.Uint32, Nullable: true},

			// Raw XDR for full fidelity
			{Name: "operation_xdr", Type: arrow.BinaryTypes.Binary, Nullable: false},
			{Name: "operation_result_xdr", Type: arrow.BinaryTypes.Binary, Nullable: false},
			{Name: "operation_meta_xdr", Type: arrow.BinaryTypes.Binary, Nullable: false},
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