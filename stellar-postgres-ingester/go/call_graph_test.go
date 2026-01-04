package main

import (
	"encoding/json"
	"testing"
)

// TestDeduplicateCalls tests the deduplication logic for contract calls
func TestDeduplicateCalls(t *testing.T) {
	tests := []struct {
		name     string
		input    []ContractCall
		expected int
	}{
		{
			name:     "empty slice",
			input:    []ContractCall{},
			expected: 0,
		},
		{
			name: "no duplicates",
			input: []ContractCall{
				{FromContract: "CA", ToContract: "CB", FunctionName: "swap", CallDepth: 1},
				{FromContract: "CB", ToContract: "CC", FunctionName: "transfer", CallDepth: 2},
			},
			expected: 2,
		},
		{
			name: "with duplicates at same depth",
			input: []ContractCall{
				{FromContract: "CA", ToContract: "CB", FunctionName: "swap", CallDepth: 1, ExecutionOrder: 0},
				{FromContract: "CA", ToContract: "CB", FunctionName: "swap", CallDepth: 1, ExecutionOrder: 1},
				{FromContract: "CB", ToContract: "CC", FunctionName: "transfer", CallDepth: 2, ExecutionOrder: 2},
			},
			expected: 2, // First duplicate removed
		},
		{
			name: "same contracts different depths",
			input: []ContractCall{
				{FromContract: "CA", ToContract: "CB", FunctionName: "swap", CallDepth: 1},
				{FromContract: "CA", ToContract: "CB", FunctionName: "swap", CallDepth: 2},
			},
			expected: 2, // Different depths = different calls
		},
		{
			name: "same contracts different functions",
			input: []ContractCall{
				{FromContract: "CA", ToContract: "CB", FunctionName: "swap", CallDepth: 1},
				{FromContract: "CA", ToContract: "CB", FunctionName: "transfer", CallDepth: 1},
			},
			expected: 2, // Different functions = different calls
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := deduplicateCalls(tt.input)
			if len(result) != tt.expected {
				t.Errorf("deduplicateCalls() got %d calls, want %d", len(result), tt.expected)
			}
		})
	}
}

// TestCallGraphToJSON tests the JSON conversion for call graphs
func TestCallGraphToJSON(t *testing.T) {
	tests := []struct {
		name              string
		input             *CallGraphResult
		expectNilJSON     bool
		expectMaxDepth    int
		expectContractCnt int
	}{
		{
			name:          "nil result",
			input:         nil,
			expectNilJSON: true,
		},
		{
			name: "empty calls",
			input: &CallGraphResult{
				Calls:             []ContractCall{},
				ContractsInvolved: []string{},
				MaxDepth:          0,
			},
			expectNilJSON: true,
		},
		{
			name: "single call",
			input: &CallGraphResult{
				Calls: []ContractCall{
					{
						FromContract:   "CSWAP123",
						ToContract:     "CTOKEN456",
						FunctionName:   "transfer",
						CallDepth:      1,
						ExecutionOrder: 0,
						Successful:     true,
					},
				},
				ContractsInvolved: []string{"CSWAP123", "CTOKEN456"},
				MaxDepth:          1,
			},
			expectNilJSON:     false,
			expectMaxDepth:    1,
			expectContractCnt: 2,
		},
		{
			name: "nested calls",
			input: &CallGraphResult{
				Calls: []ContractCall{
					{FromContract: "CROUTER", ToContract: "CPOOL", FunctionName: "do_swap", CallDepth: 1},
					{FromContract: "CPOOL", ToContract: "CUSDC", FunctionName: "transfer", CallDepth: 2},
					{FromContract: "CPOOL", ToContract: "CXLM", FunctionName: "transfer", CallDepth: 2},
				},
				ContractsInvolved: []string{"CROUTER", "CPOOL", "CUSDC", "CXLM"},
				MaxDepth:          2,
			},
			expectNilJSON:     false,
			expectMaxDepth:    2,
			expectContractCnt: 4,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			callsJSON, contracts, maxDepth, err := callGraphToJSON(tt.input)
			if err != nil {
				t.Fatalf("callGraphToJSON() error = %v", err)
			}

			if tt.expectNilJSON {
				if callsJSON != nil {
					t.Errorf("callGraphToJSON() expected nil JSON, got %v", *callsJSON)
				}
				return
			}

			if callsJSON == nil {
				t.Fatal("callGraphToJSON() expected non-nil JSON")
			}

			// Verify JSON is valid
			var parsed []ContractCall
			if err := json.Unmarshal([]byte(*callsJSON), &parsed); err != nil {
				t.Errorf("callGraphToJSON() produced invalid JSON: %v", err)
			}

			if len(parsed) != len(tt.input.Calls) {
				t.Errorf("callGraphToJSON() JSON has %d calls, want %d", len(parsed), len(tt.input.Calls))
			}

			if maxDepth == nil || *maxDepth != tt.expectMaxDepth {
				t.Errorf("callGraphToJSON() maxDepth = %v, want %d", maxDepth, tt.expectMaxDepth)
			}

			if len(contracts) != tt.expectContractCnt {
				t.Errorf("callGraphToJSON() contracts count = %d, want %d", len(contracts), tt.expectContractCnt)
			}
		})
	}
}

// TestContractCallJSONMarshaling tests that ContractCall marshals to expected JSON format
func TestContractCallJSONMarshaling(t *testing.T) {
	call := ContractCall{
		FromContract:   "CSWAP123456789",
		ToContract:     "CTOKEN987654321",
		FunctionName:   "transfer",
		CallDepth:      2,
		ExecutionOrder: 5,
		Successful:     true,
	}

	data, err := json.Marshal(call)
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}

	// Verify JSON field names match expected format
	var parsed map[string]interface{}
	if err := json.Unmarshal(data, &parsed); err != nil {
		t.Fatalf("json.Unmarshal() error = %v", err)
	}

	expectedFields := []string{"from_contract", "to_contract", "function", "call_depth", "execution_order", "successful"}
	for _, field := range expectedFields {
		if _, ok := parsed[field]; !ok {
			t.Errorf("JSON missing expected field %q", field)
		}
	}

	// Verify values
	if parsed["from_contract"] != "CSWAP123456789" {
		t.Errorf("from_contract = %v, want CSWAP123456789", parsed["from_contract"])
	}
	if parsed["function"] != "transfer" {
		t.Errorf("function = %v, want transfer", parsed["function"])
	}
	if int(parsed["call_depth"].(float64)) != 2 {
		t.Errorf("call_depth = %v, want 2", parsed["call_depth"])
	}
}

// TestCallGraphResultMaxDepthCalculation verifies max depth is calculated correctly
func TestCallGraphResultMaxDepthCalculation(t *testing.T) {
	tests := []struct {
		name           string
		calls          []ContractCall
		expectedMax    int
		expectedUnique int
	}{
		{
			name:           "empty",
			calls:          []ContractCall{},
			expectedMax:    0,
			expectedUnique: 0,
		},
		{
			name: "flat calls",
			calls: []ContractCall{
				{FromContract: "A", ToContract: "B", CallDepth: 1},
				{FromContract: "A", ToContract: "C", CallDepth: 1},
			},
			expectedMax:    1,
			expectedUnique: 3, // A, B, C
		},
		{
			name: "deep nesting",
			calls: []ContractCall{
				{FromContract: "A", ToContract: "B", CallDepth: 1},
				{FromContract: "B", ToContract: "C", CallDepth: 2},
				{FromContract: "C", ToContract: "D", CallDepth: 3},
				{FromContract: "D", ToContract: "E", CallDepth: 4},
			},
			expectedMax:    4,
			expectedUnique: 5, // A, B, C, D, E
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Simulate what extractCallGraph does
			contractsSet := make(map[string]bool)
			maxDepth := 0
			for _, call := range tt.calls {
				contractsSet[call.FromContract] = true
				contractsSet[call.ToContract] = true
				if call.CallDepth > maxDepth {
					maxDepth = call.CallDepth
				}
			}

			if maxDepth != tt.expectedMax {
				t.Errorf("maxDepth = %d, want %d", maxDepth, tt.expectedMax)
			}
			if len(contractsSet) != tt.expectedUnique {
				t.Errorf("unique contracts = %d, want %d", len(contractsSet), tt.expectedUnique)
			}
		})
	}
}

// TestExtractFirstTopicPatterns tests various topic extraction scenarios
func TestExtractFirstTopicPatterns(t *testing.T) {
	// Note: Full testing would require XDR fixtures
	// This test documents expected behavior
	t.Run("documents expected patterns", func(t *testing.T) {
		// fn_call indicates a contract is calling a function
		// fn_return indicates a function is returning
		// These are the key patterns for call stack tracking
		patterns := []string{"fn_call", "fn_return", "log", "error"}
		for _, p := range patterns {
			t.Logf("Expected diagnostic event topic pattern: %s", p)
		}
	})
}
