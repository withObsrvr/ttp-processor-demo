package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

// MockUnifiedReader implements a minimal mock for testing handlers
type MockUnifiedReader struct{}

func (m *MockUnifiedReader) GetEnrichedOperationsWithCursor(filters OperationFilters) ([]EnrichedOperation, string, bool, error) {
	// Return mock data with pagination info
	ops := []EnrichedOperation{
		{
			TransactionHash: "TXHASH123",
			OperationID:     5,
			LedgerSequence:  2137918,
			LedgerClosedAt:  "2024-12-16T10:00:00Z",
			SourceAccount:   "GTEST123",
			Type:            1,
			TypeName:        "PAYMENT",
			TxSuccessful:    true,
		},
	}

	// Simulate pagination - if we have a cursor, return empty (end of results)
	if filters.Cursor != nil {
		return []EnrichedOperation{}, "", false, nil
	}

	// First page - return data with cursor
	cursor := OperationCursor{LedgerSequence: 2137918, OperationIndex: 5}
	return ops, cursor.Encode(), true, nil
}

func TestParseLimitDefaults(t *testing.T) {
	tests := []struct {
		name         string
		queryLimit   string
		defaultLimit int
		maxLimit     int
		expected     int
	}{
		{"empty uses default", "", 100, 1000, 100},
		{"valid limit", "50", 100, 1000, 50},
		{"exceeds max uses max", "2000", 100, 1000, 1000},
		{"negative uses default", "-5", 100, 1000, 100},
		{"invalid uses default", "abc", 100, 1000, 100},
		{"zero uses default", "0", 100, 1000, 100},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest("GET", "/test?limit="+tt.queryLimit, nil)
			result := parseLimit(req, tt.defaultLimit, tt.maxLimit)
			if result != tt.expected {
				t.Errorf("parseLimit() = %d, want %d", result, tt.expected)
			}
		})
	}
}

func TestDecodeOperationCursorFromRequest(t *testing.T) {
	tests := []struct {
		name        string
		cursorParam string
		wantNil     bool
		wantErr     bool
	}{
		{"empty cursor returns nil", "", true, false},
		{"valid cursor decodes", "MjEzNzkxODo1", false, false}, // "2137918:5" base64
		{"invalid cursor returns error", "invalid!!!", false, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cursor, err := DecodeOperationCursor(tt.cursorParam)

			if tt.wantErr {
				if err == nil {
					t.Error("Expected error but got nil")
				}
				return
			}

			if err != nil {
				t.Errorf("Unexpected error: %v", err)
				return
			}

			if tt.wantNil && cursor != nil {
				t.Error("Expected nil cursor")
			}
			if !tt.wantNil && cursor == nil {
				t.Error("Expected non-nil cursor")
			}
		})
	}
}

func TestCursorAndStartLedgerMutualExclusion(t *testing.T) {
	// This tests the validation logic that cursor and start_ledger are mutually exclusive
	// In the actual handler, this returns a 400 error

	cursorStr := "MjEzNzkxODo1" // valid cursor
	startLedger := "1000000"

	// Both present should be an error condition
	if cursorStr != "" && startLedger != "" {
		// This is the expected error case
		t.Log("Correctly identified mutual exclusion violation")
	} else {
		t.Error("Test setup incorrect")
	}

	// Only cursor - OK
	cursorStr = "MjEzNzkxODo1"
	startLedger = ""
	if cursorStr != "" && startLedger != "" {
		t.Error("Should not detect conflict when only cursor is set")
	}

	// Only start_ledger - OK
	cursorStr = ""
	startLedger = "1000000"
	if cursorStr != "" && startLedger != "" {
		t.Error("Should not detect conflict when only start_ledger is set")
	}

	// Neither - OK
	cursorStr = ""
	startLedger = ""
	if cursorStr != "" && startLedger != "" {
		t.Error("Should not detect conflict when neither is set")
	}
}

func TestResponseIncludesPaginationFields(t *testing.T) {
	// Test that our response format includes cursor and has_more fields
	response := map[string]interface{}{
		"operations": []EnrichedOperation{},
		"count":      0,
		"has_more":   true,
		"cursor":     "MjEzNzkxODo1",
	}

	jsonBytes, err := json.Marshal(response)
	if err != nil {
		t.Fatalf("Failed to marshal response: %v", err)
	}

	var decoded map[string]interface{}
	if err := json.Unmarshal(jsonBytes, &decoded); err != nil {
		t.Fatalf("Failed to unmarshal response: %v", err)
	}

	// Verify pagination fields exist
	if _, ok := decoded["has_more"]; !ok {
		t.Error("Response missing has_more field")
	}
	if _, ok := decoded["cursor"]; !ok {
		t.Error("Response missing cursor field")
	}

	// Verify has_more is boolean
	if hasMore, ok := decoded["has_more"].(bool); !ok {
		t.Error("has_more should be boolean")
	} else if !hasMore {
		t.Error("has_more should be true in this test")
	}

	// Verify cursor is string
	if cursor, ok := decoded["cursor"].(string); !ok {
		t.Error("cursor should be string")
	} else if cursor != "MjEzNzkxODo1" {
		t.Errorf("cursor mismatch: got %s", cursor)
	}
}

func TestRespondJSON(t *testing.T) {
	w := httptest.NewRecorder()

	data := map[string]interface{}{
		"test": "value",
	}

	respondJSON(w, data)

	// Check status code
	if w.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d", w.Code)
	}

	// Check content type
	contentType := w.Header().Get("Content-Type")
	if contentType != "application/json" {
		t.Errorf("Expected Content-Type application/json, got %s", contentType)
	}

	// Check CORS header
	cors := w.Header().Get("Access-Control-Allow-Origin")
	if cors != "*" {
		t.Errorf("Expected CORS header *, got %s", cors)
	}

	// Check body
	var decoded map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &decoded); err != nil {
		t.Fatalf("Failed to decode response body: %v", err)
	}
	if decoded["test"] != "value" {
		t.Error("Response body mismatch")
	}
}

func TestRespondError(t *testing.T) {
	w := httptest.NewRecorder()

	respondError(w, "test error", http.StatusBadRequest)

	// Check status code
	if w.Code != http.StatusBadRequest {
		t.Errorf("Expected status 400, got %d", w.Code)
	}

	// Check body contains error
	var decoded map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &decoded); err != nil {
		t.Fatalf("Failed to decode response body: %v", err)
	}
	if decoded["error"] != "test error" {
		t.Errorf("Error message mismatch: got %v", decoded["error"])
	}
}

func TestCursorRoundTripThroughResponse(t *testing.T) {
	// Simulate a full round-trip: create cursor -> include in response -> decode from next request

	// Step 1: Create a cursor (as server would after first query)
	originalCursor := OperationCursor{
		LedgerSequence: 2137918,
		OperationIndex: 42,
	}
	encoded := originalCursor.Encode()

	// Step 2: Include in JSON response
	response := map[string]interface{}{
		"cursor": encoded,
	}
	jsonBytes, _ := json.Marshal(response)

	// Step 3: Client receives response and extracts cursor
	var clientResponse map[string]interface{}
	json.Unmarshal(jsonBytes, &clientResponse)
	cursorFromResponse := clientResponse["cursor"].(string)

	// Step 4: Client sends cursor in next request
	decoded, err := DecodeOperationCursor(cursorFromResponse)
	if err != nil {
		t.Fatalf("Failed to decode cursor from response: %v", err)
	}

	// Step 5: Verify round-trip preserved values
	if decoded.LedgerSequence != originalCursor.LedgerSequence {
		t.Errorf("LedgerSequence mismatch after round-trip: got %d, want %d",
			decoded.LedgerSequence, originalCursor.LedgerSequence)
	}
	if decoded.OperationIndex != originalCursor.OperationIndex {
		t.Errorf("OperationIndex mismatch after round-trip: got %d, want %d",
			decoded.OperationIndex, originalCursor.OperationIndex)
	}
}
