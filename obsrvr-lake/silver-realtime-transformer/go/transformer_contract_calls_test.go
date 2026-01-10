package main

import (
	"testing"
	"time"
)

func TestParseContractCallGraph_ValidJSON(t *testing.T) {
	// Simulate a Soroswap swap transaction call graph
	callsJSON := `[
		{"from_contract": "CABC123", "to_contract": "CDEF456", "function": "swap", "call_depth": 1, "execution_order": 0, "successful": true},
		{"from_contract": "CDEF456", "to_contract": "CGHI789", "function": "do_swap", "call_depth": 2, "execution_order": 1, "successful": true},
		{"from_contract": "CGHI789", "to_contract": "CJKL012", "function": "transfer", "call_depth": 3, "execution_order": 2, "successful": true},
		{"from_contract": "CGHI789", "to_contract": "CMNO345", "function": "transfer", "call_depth": 3, "execution_order": 3, "successful": true}
	]`

	contractsInvolved := []string{"CABC123", "CDEF456", "CGHI789", "CJKL012", "CMNO345"}
	txHash := "abc123def456"
	ledgerSeq := int64(12345678)
	txIndex := 1
	opIndex := 0
	closedAt := time.Now()
	ledgerRange := int64(123456)

	callRows, hierarchyRows, err := parseContractCallGraph(
		callsJSON,
		txHash,
		ledgerSeq,
		txIndex,
		opIndex,
		closedAt,
		ledgerRange,
		contractsInvolved,
	)

	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}

	// Verify call rows
	if len(callRows) != 4 {
		t.Errorf("Expected 4 call rows, got %d", len(callRows))
	}

	// Verify first call
	if callRows[0].FromContract != "CABC123" {
		t.Errorf("Expected FromContract 'CABC123', got '%s'", callRows[0].FromContract)
	}
	if callRows[0].ToContract != "CDEF456" {
		t.Errorf("Expected ToContract 'CDEF456', got '%s'", callRows[0].ToContract)
	}
	if callRows[0].FunctionName != "swap" {
		t.Errorf("Expected FunctionName 'swap', got '%s'", callRows[0].FunctionName)
	}
	if callRows[0].CallDepth != 1 {
		t.Errorf("Expected CallDepth 1, got %d", callRows[0].CallDepth)
	}

	// Verify hierarchy rows
	if len(hierarchyRows) != 4 {
		t.Errorf("Expected 4 hierarchy rows, got %d", len(hierarchyRows))
	}

	// Verify first hierarchy (root to first child)
	if hierarchyRows[0].RootContract != "CABC123" {
		t.Errorf("Expected RootContract 'CABC123', got '%s'", hierarchyRows[0].RootContract)
	}
	if hierarchyRows[0].ChildContract != "CDEF456" {
		t.Errorf("Expected ChildContract 'CDEF456', got '%s'", hierarchyRows[0].ChildContract)
	}
	if hierarchyRows[0].PathDepth != 1 {
		t.Errorf("Expected PathDepth 1, got %d", hierarchyRows[0].PathDepth)
	}
}

func TestParseContractCallGraph_EmptyJSON(t *testing.T) {
	callsJSON := `[]`
	contractsInvolved := []string{"CABC123"}

	callRows, hierarchyRows, err := parseContractCallGraph(
		callsJSON,
		"txhash",
		12345678,
		1,
		0,
		time.Now(),
		123456,
		contractsInvolved,
	)

	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}

	if len(callRows) != 0 {
		t.Errorf("Expected 0 call rows for empty JSON, got %d", len(callRows))
	}

	// Single contract = no hierarchy
	if len(hierarchyRows) != 0 {
		t.Errorf("Expected 0 hierarchy rows for single contract, got %d", len(hierarchyRows))
	}
}

func TestParseContractCallGraph_InvalidJSON(t *testing.T) {
	callsJSON := `{invalid json`

	_, _, err := parseContractCallGraph(
		callsJSON,
		"txhash",
		12345678,
		1,
		0,
		time.Now(),
		123456,
		[]string{},
	)

	if err == nil {
		t.Error("Expected error for invalid JSON, got nil")
	}
}

func TestParseContractCallGraph_SingleDirectCall(t *testing.T) {
	// Single direct call with no nested calls
	callsJSON := `[
		{"from_contract": "CABC123", "to_contract": "CDEF456", "function": "transfer", "call_depth": 1, "execution_order": 0, "successful": true}
	]`

	contractsInvolved := []string{"CABC123", "CDEF456"}

	callRows, hierarchyRows, err := parseContractCallGraph(
		callsJSON,
		"txhash",
		12345678,
		1,
		0,
		time.Now(),
		123456,
		contractsInvolved,
	)

	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}

	if len(callRows) != 1 {
		t.Errorf("Expected 1 call row, got %d", len(callRows))
	}

	if len(hierarchyRows) != 1 {
		t.Errorf("Expected 1 hierarchy row, got %d", len(hierarchyRows))
	}
}

func TestParseContractCallGraph_FailedCall(t *testing.T) {
	callsJSON := `[
		{"from_contract": "CABC123", "to_contract": "CDEF456", "function": "swap", "call_depth": 1, "execution_order": 0, "successful": false}
	]`

	contractsInvolved := []string{"CABC123", "CDEF456"}

	callRows, _, err := parseContractCallGraph(
		callsJSON,
		"txhash",
		12345678,
		1,
		0,
		time.Now(),
		123456,
		contractsInvolved,
	)

	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}

	if callRows[0].Successful != false {
		t.Error("Expected Successful to be false for failed call")
	}
}

func TestParseContractCallGraph_PreservesMetadata(t *testing.T) {
	callsJSON := `[
		{"from_contract": "CABC123", "to_contract": "CDEF456", "function": "swap", "call_depth": 1, "execution_order": 0, "successful": true}
	]`

	txHash := "unique_tx_hash_123"
	ledgerSeq := int64(99999999)
	txIndex := 42
	opIndex := 7
	closedAt := time.Date(2026, 1, 4, 12, 0, 0, 0, time.UTC)
	ledgerRange := int64(999999)
	contractsInvolved := []string{"CABC123", "CDEF456"}

	callRows, hierarchyRows, err := parseContractCallGraph(
		callsJSON,
		txHash,
		ledgerSeq,
		txIndex,
		opIndex,
		closedAt,
		ledgerRange,
		contractsInvolved,
	)

	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}

	// Verify call row preserves all metadata
	call := callRows[0]
	if call.TransactionHash != txHash {
		t.Errorf("Expected TransactionHash '%s', got '%s'", txHash, call.TransactionHash)
	}
	if call.LedgerSequence != ledgerSeq {
		t.Errorf("Expected LedgerSequence %d, got %d", ledgerSeq, call.LedgerSequence)
	}
	if call.TransactionIndex != txIndex {
		t.Errorf("Expected TransactionIndex %d, got %d", txIndex, call.TransactionIndex)
	}
	if call.OperationIndex != opIndex {
		t.Errorf("Expected OperationIndex %d, got %d", opIndex, call.OperationIndex)
	}
	if !call.ClosedAt.Equal(closedAt) {
		t.Errorf("Expected ClosedAt %v, got %v", closedAt, call.ClosedAt)
	}
	if call.LedgerRange != ledgerRange {
		t.Errorf("Expected LedgerRange %d, got %d", ledgerRange, call.LedgerRange)
	}

	// Verify hierarchy row preserves metadata
	hierarchy := hierarchyRows[0]
	if hierarchy.TransactionHash != txHash {
		t.Errorf("Expected hierarchy TransactionHash '%s', got '%s'", txHash, hierarchy.TransactionHash)
	}
	if hierarchy.LedgerRange != ledgerRange {
		t.Errorf("Expected hierarchy LedgerRange %d, got %d", ledgerRange, hierarchy.LedgerRange)
	}
}

func TestParseContractCallGraph_DeepNesting(t *testing.T) {
	// Test deep nesting scenario (5 levels deep)
	callsJSON := `[
		{"from_contract": "C1", "to_contract": "C2", "function": "fn1", "call_depth": 1, "execution_order": 0, "successful": true},
		{"from_contract": "C2", "to_contract": "C3", "function": "fn2", "call_depth": 2, "execution_order": 1, "successful": true},
		{"from_contract": "C3", "to_contract": "C4", "function": "fn3", "call_depth": 3, "execution_order": 2, "successful": true},
		{"from_contract": "C4", "to_contract": "C5", "function": "fn4", "call_depth": 4, "execution_order": 3, "successful": true},
		{"from_contract": "C5", "to_contract": "C6", "function": "fn5", "call_depth": 5, "execution_order": 4, "successful": true}
	]`

	contractsInvolved := []string{"C1", "C2", "C3", "C4", "C5", "C6"}

	callRows, hierarchyRows, err := parseContractCallGraph(
		callsJSON,
		"txhash",
		12345678,
		1,
		0,
		time.Now(),
		123456,
		contractsInvolved,
	)

	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}

	if len(callRows) != 5 {
		t.Errorf("Expected 5 call rows, got %d", len(callRows))
	}

	// Verify call depths
	for i, call := range callRows {
		expectedDepth := i + 1
		if call.CallDepth != expectedDepth {
			t.Errorf("Call %d: Expected CallDepth %d, got %d", i, expectedDepth, call.CallDepth)
		}
	}

	// Should have 5 hierarchy rows (C1→C2, C1→C3, C1→C4, C1→C5, C1→C6)
	if len(hierarchyRows) != 5 {
		t.Errorf("Expected 5 hierarchy rows, got %d", len(hierarchyRows))
	}

	// All hierarchy rows should have C1 as root
	for _, h := range hierarchyRows {
		if h.RootContract != "C1" {
			t.Errorf("Expected RootContract 'C1', got '%s'", h.RootContract)
		}
	}
}
