package main

import (
	"testing"
	"time"
)

func TestOperationCursorEncodeDecode(t *testing.T) {
	tests := []struct {
		name           string
		ledgerSequence int64
		operationIndex int64
	}{
		{"basic", 2137918, 5},
		{"large ledger", 999999999, 12345},
		{"zero op index", 1000000, 0},
		{"small values", 1, 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cursor := OperationCursor{
				LedgerSequence: tt.ledgerSequence,
				OperationIndex: tt.operationIndex,
			}

			// Encode
			encoded := cursor.Encode()
			if encoded == "" {
				t.Fatal("Encode returned empty string")
			}

			// Decode
			decoded, err := DecodeOperationCursor(encoded)
			if err != nil {
				t.Fatalf("DecodeOperationCursor failed: %v", err)
			}

			// Verify round-trip
			if decoded.LedgerSequence != tt.ledgerSequence {
				t.Errorf("LedgerSequence mismatch: got %d, want %d", decoded.LedgerSequence, tt.ledgerSequence)
			}
			if decoded.OperationIndex != tt.operationIndex {
				t.Errorf("OperationIndex mismatch: got %d, want %d", decoded.OperationIndex, tt.operationIndex)
			}
		})
	}
}

func TestDecodeOperationCursorEmpty(t *testing.T) {
	cursor, err := DecodeOperationCursor("")
	if err != nil {
		t.Fatalf("DecodeOperationCursor failed for empty string: %v", err)
	}
	if cursor != nil {
		t.Error("Expected nil cursor for empty string")
	}
}

func TestDecodeOperationCursorInvalid(t *testing.T) {
	tests := []struct {
		name   string
		cursor string
	}{
		{"not base64", "not-valid-base64!!!"},
		{"wrong format", "bm90OmNvbG9uOmZvcm1hdA=="}, // "not:colon:format" base64
		{"missing op index", "MTIzNDU2"},              // "123456" base64
		{"non-numeric ledger", "YWJjOjEyMw=="},        // "abc:123" base64
		{"non-numeric op", "MTIzOmFiYw=="},            // "123:abc" base64
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := DecodeOperationCursor(tt.cursor)
			if err == nil {
				t.Error("Expected error for invalid cursor")
			}
		})
	}
}

func TestTransferCursorEncodeDecode(t *testing.T) {
	tests := []struct {
		name           string
		ledgerSequence int64
		timestamp      time.Time
	}{
		{"basic", 2137918, time.Date(2024, 12, 16, 10, 0, 0, 0, time.UTC)},
		{"with nanoseconds", 1000000, time.Date(2024, 1, 15, 12, 30, 45, 123456789, time.UTC)},
		{"epoch", 1, time.Unix(0, 0).UTC()},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cursor := TransferCursor{
				LedgerSequence: tt.ledgerSequence,
				Timestamp:      tt.timestamp,
			}

			// Encode
			encoded := cursor.Encode()
			if encoded == "" {
				t.Fatal("Encode returned empty string")
			}

			// Decode
			decoded, err := DecodeTransferCursor(encoded)
			if err != nil {
				t.Fatalf("DecodeTransferCursor failed: %v", err)
			}

			// Verify round-trip
			if decoded.LedgerSequence != tt.ledgerSequence {
				t.Errorf("LedgerSequence mismatch: got %d, want %d", decoded.LedgerSequence, tt.ledgerSequence)
			}
			if !decoded.Timestamp.Equal(tt.timestamp) {
				t.Errorf("Timestamp mismatch: got %v, want %v", decoded.Timestamp, tt.timestamp)
			}
		})
	}
}

func TestDecodeTransferCursorEmpty(t *testing.T) {
	cursor, err := DecodeTransferCursor("")
	if err != nil {
		t.Fatalf("DecodeTransferCursor failed for empty string: %v", err)
	}
	if cursor != nil {
		t.Error("Expected nil cursor for empty string")
	}
}

func TestDecodeTransferCursorInvalid(t *testing.T) {
	tests := []struct {
		name   string
		cursor string
	}{
		{"not base64", "not-valid-base64!!!"},
		{"missing timestamp", "MTIzNDU2"}, // "123456" base64
		{"invalid timestamp", "MTIzNDU2Om5vdC1hLXRpbWVzdGFtcA=="}, // "123456:not-a-timestamp" base64
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := DecodeTransferCursor(tt.cursor)
			if err == nil {
				t.Error("Expected error for invalid cursor")
			}
		})
	}
}

func TestAccountCursorEncodeDecode(t *testing.T) {
	tests := []struct {
		name           string
		ledgerSequence int64
	}{
		{"basic", 2137918},
		{"large", 999999999},
		{"small", 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cursor := AccountCursor{
				LedgerSequence: tt.ledgerSequence,
			}

			// Encode
			encoded := cursor.Encode()
			if encoded == "" {
				t.Fatal("Encode returned empty string")
			}

			// Decode
			decoded, err := DecodeAccountCursor(encoded)
			if err != nil {
				t.Fatalf("DecodeAccountCursor failed: %v", err)
			}

			// Verify round-trip
			if decoded.LedgerSequence != tt.ledgerSequence {
				t.Errorf("LedgerSequence mismatch: got %d, want %d", decoded.LedgerSequence, tt.ledgerSequence)
			}
		})
	}
}

func TestDecodeAccountCursorEmpty(t *testing.T) {
	cursor, err := DecodeAccountCursor("")
	if err != nil {
		t.Fatalf("DecodeAccountCursor failed for empty string: %v", err)
	}
	if cursor != nil {
		t.Error("Expected nil cursor for empty string")
	}
}

func TestCursorIsOpaque(t *testing.T) {
	// Verify that cursors are base64 encoded (opaque to clients)
	cursor := OperationCursor{
		LedgerSequence: 2137918,
		OperationIndex: 5,
	}
	encoded := cursor.Encode()

	// Should not contain raw numbers or colons (should be base64)
	if encoded == "2137918:5" {
		t.Error("Cursor should be base64 encoded, not plain text")
	}

	// Should be valid base64 (already tested by decode, but explicit check)
	decoded, err := DecodeOperationCursor(encoded)
	if err != nil {
		t.Fatalf("Encoded cursor should be valid: %v", err)
	}
	if decoded.LedgerSequence != 2137918 || decoded.OperationIndex != 5 {
		t.Error("Decoded values don't match original")
	}
}
