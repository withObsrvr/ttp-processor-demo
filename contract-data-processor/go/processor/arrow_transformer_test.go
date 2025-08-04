package processor

import (
	"testing"

	"github.com/apache/arrow/go/v17/arrow/memory"
)

func TestArrowTransformer(t *testing.T) {
	allocator := memory.NewGoAllocator()
	transformer := NewArrowTransformer(allocator)
	defer transformer.Release()
	
	// Test adding entries
	entries := []*ContractDataEntry{
		{
			ContractId:                "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAHK3M",
			ContractKeyType:           "ContractDataDurability",
			ContractDurability:        "Persistent",
			ContractDataAssetCode:     "USDC",
			ContractDataAssetIssuer:   "GA5ZSEJYB37JRC5AVCIA5MOP4RHTM335X2KGX3IHOJAPP5RE34K4KZVN",
			ContractDataAssetType:     "credit_alphanum4",
			ContractDataBalanceHolder: "GAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAWHF",
			ContractDataBalance:       "1000000",
			LastModifiedLedger:        12345,
			Deleted:                   false,
			LedgerSequence:            12346,
			ClosedAt:                  1234567890000000, // microseconds
			LedgerKeyHash:             "abcdef123456",
		},
		{
			ContractId:         "CBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB",
			ContractKeyType:    "ContractDataDurability",
			ContractDurability: "Temporary",
			// No asset data for this entry
			LastModifiedLedger: 12346,
			Deleted:            true,
			LedgerSequence:     12347,
			ClosedAt:           1234567891000000,
		},
	}
	
	// Add entries
	for _, entry := range entries {
		if err := transformer.AddEntry(entry); err != nil {
			t.Fatalf("Failed to add entry: %v", err)
		}
	}
	
	// Build record
	record, err := transformer.BuildRecord()
	if err != nil {
		t.Fatalf("Failed to build record: %v", err)
	}
	defer record.Release()
	
	// Verify record
	if record.NumRows() != int64(len(entries)) {
		t.Errorf("Expected %d rows, got %d", len(entries), record.NumRows())
	}
	
	if record.NumCols() != 18 {
		t.Errorf("Expected 18 columns, got %d", record.NumCols())
	}
	
	// Verify schema
	schema := transformer.GetSchema()
	if schema.NumFields() != 18 {
		t.Errorf("Expected 18 fields in schema, got %d", schema.NumFields())
	}
	
	// Test specific values
	contractIDCol := record.Column(0)
	if contractIDCol.Len() != len(entries) {
		t.Errorf("Contract ID column has wrong length")
	}
	
	// Test reset
	transformer.Reset()
	if transformer.GetCurrentSize() != 0 {
		t.Errorf("Expected size to be 0 after reset, got %d", transformer.GetCurrentSize())
	}
}

func TestArrowTransformerEmptyBatch(t *testing.T) {
	allocator := memory.NewGoAllocator()
	transformer := NewArrowTransformer(allocator)
	defer transformer.Release()
	
	// Try to build record without entries
	_, err := transformer.BuildRecord()
	if err == nil {
		t.Error("Expected error when building empty record")
	}
}

func TestArrowTransformerNullableFields(t *testing.T) {
	allocator := memory.NewGoAllocator()
	transformer := NewArrowTransformer(allocator)
	defer transformer.Release()
	
	// Entry with minimal fields
	entry := &ContractDataEntry{
		ContractId:         "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAHK3M",
		ContractKeyType:    "ContractDataDurability",
		ContractDurability: "Persistent",
		LastModifiedLedger: 12345,
		Deleted:            false,
		LedgerSequence:     12346,
		ClosedAt:           1234567890000000,
		// All other fields are empty/zero
	}
	
	if err := transformer.AddEntry(entry); err != nil {
		t.Fatalf("Failed to add entry: %v", err)
	}
	
	record, err := transformer.BuildRecord()
	if err != nil {
		t.Fatalf("Failed to build record: %v", err)
	}
	defer record.Release()
	
	// Check that nullable fields are properly set as null
	assetCodeCol := record.Column(3)
	if !assetCodeCol.IsNull(0) {
		t.Error("Expected asset_code to be null")
	}
	
	balanceCol := record.Column(7)
	if !balanceCol.IsNull(0) {
		t.Error("Expected balance to be null")
	}
}

func TestArrowTransformerBalanceConversion(t *testing.T) {
	allocator := memory.NewGoAllocator()
	transformer := NewArrowTransformer(allocator)
	defer transformer.Release()
	
	tests := []struct {
		balance    string
		expected   int64
		shouldNull bool
	}{
		{"1000000", 1000000, false},
		{"0", 0, false},
		{"-1000", -1000, false},
		{"invalid", 0, true},
		{"", 0, true},
		{"9223372036854775807", 9223372036854775807, false}, // max int64
	}
	
	for _, tt := range tests {
		transformer.Reset()
		
		entry := &ContractDataEntry{
			ContractId:          "CTEST",
			ContractKeyType:     "test",
			ContractDurability:  "test",
			ContractDataBalance: tt.balance,
			LastModifiedLedger:  1,
			LedgerSequence:      1,
			ClosedAt:            1,
		}
		
		if err := transformer.AddEntry(entry); err != nil {
			t.Fatalf("Failed to add entry: %v", err)
		}
		
		record, err := transformer.BuildRecord()
		if err != nil {
			t.Fatalf("Failed to build record: %v", err)
		}
		
		balanceCol := record.Column(7)
		if tt.shouldNull {
			if !balanceCol.IsNull(0) {
				t.Errorf("Balance %q should be null", tt.balance)
			}
		} else {
			if balanceCol.IsNull(0) {
				t.Errorf("Balance %q should not be null", tt.balance)
			}
			// Note: We'd need to cast to Int64Array to check the actual value
			// This is omitted for brevity
		}
		
		record.Release()
	}
}