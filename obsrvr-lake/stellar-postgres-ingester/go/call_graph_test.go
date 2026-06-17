package main

import (
	"encoding/json"
	"testing"

	"github.com/stellar/go-stellar-sdk/strkey"
	"github.com/stellar/go-stellar-sdk/xdr"
)

func TestExtractCallsFromAuthInvocationSkipsNonContractAddress(t *testing.T) {
	invocation := &xdr.SorobanAuthorizedInvocation{
		Function: xdr.SorobanAuthorizedFunction{
			Type: xdr.SorobanAuthorizedFunctionTypeSorobanAuthorizedFunctionTypeContractFn,
			ContractFn: &xdr.InvokeContractArgs{
				// ContractFn exists, but ContractAddress.ContractId is nil because the
				// SCAddress arm is not a contract address.
				ContractAddress: xdr.ScAddress{Type: xdr.ScAddressTypeScAddressTypeAccount},
				FunctionName:    xdr.ScSymbol("transfer"),
			},
		},
	}

	executionOrder := 0
	calls := extractCallsFromAuthInvocation(invocation, "CMAIN", 1, &executionOrder)
	if len(calls) != 0 {
		t.Fatalf("expected non-contract auth invocation to be skipped, got %d calls", len(calls))
	}
	if executionOrder != 0 {
		t.Fatalf("expected execution order to remain 0, got %d", executionOrder)
	}
}

func TestExtractCallsFromAuthInvocationContractAddress(t *testing.T) {
	var contractID xdr.ContractId
	contractID[0] = 1
	toContract, err := strkey.Encode(strkey.VersionByteContract, contractID[:])
	if err != nil {
		t.Fatalf("encode contract: %v", err)
	}

	invocation := &xdr.SorobanAuthorizedInvocation{
		Function: xdr.SorobanAuthorizedFunction{
			Type: xdr.SorobanAuthorizedFunctionTypeSorobanAuthorizedFunctionTypeContractFn,
			ContractFn: &xdr.InvokeContractArgs{
				ContractAddress: xdr.ScAddress{
					Type:       xdr.ScAddressTypeScAddressTypeContract,
					ContractId: &contractID,
				},
				FunctionName: xdr.ScSymbol("swap"),
			},
		},
	}

	executionOrder := 0
	calls := extractCallsFromAuthInvocation(invocation, "CMAIN", 1, &executionOrder)
	if len(calls) != 1 {
		t.Fatalf("expected one contract call, got %d", len(calls))
	}
	if calls[0].ToContract != toContract {
		t.Fatalf("expected to contract %s, got %s", toContract, calls[0].ToContract)
	}
	if calls[0].FunctionName != "swap" {
		t.Fatalf("expected function swap, got %s", calls[0].FunctionName)
	}
	if executionOrder != 1 {
		t.Fatalf("expected execution order 1, got %d", executionOrder)
	}
}

func TestDeduplicateCalls(t *testing.T) {
	calls := []ContractCall{
		{FromContract: "CA", ToContract: "CB", FunctionName: "swap", CallDepth: 1, ExecutionOrder: 1},
		{FromContract: "CA", ToContract: "CB", FunctionName: "swap", CallDepth: 1, ExecutionOrder: 2},
		{FromContract: "CA", ToContract: "CC", FunctionName: "swap", CallDepth: 1, ExecutionOrder: 3},
	}

	unique := deduplicateCalls(calls)
	if len(unique) != 2 {
		t.Fatalf("expected 2 unique calls, got %d", len(unique))
	}
	if unique[0].ExecutionOrder != 1 {
		t.Fatalf("expected first duplicate to be retained, got execution order %d", unique[0].ExecutionOrder)
	}
}

func TestCallGraphToJSON(t *testing.T) {
	result := &CallGraphResult{
		Calls:             []ContractCall{{FromContract: "CA", ToContract: "CB", FunctionName: "swap", CallDepth: 1, ExecutionOrder: 1, Successful: true}},
		ContractsInvolved: []string{"CA", "CB"},
		MaxDepth:          1,
	}

	callsJSON, contracts, maxDepth, err := callGraphToJSON(result)
	if err != nil {
		t.Fatalf("callGraphToJSON failed: %v", err)
	}
	if callsJSON == nil || *callsJSON == "" {
		t.Fatal("expected calls JSON")
	}
	if len(contracts) != 2 || contracts[0] != "CA" || contracts[1] != "CB" {
		t.Fatalf("unexpected contracts: %#v", contracts)
	}
	if maxDepth == nil || *maxDepth != 1 {
		t.Fatalf("unexpected max depth: %v", maxDepth)
	}
}

// TestDeduplicateCallsBoundaries pins the dedup-key semantics: calls are the
// same only when from/to/function/depth all match. Different depth or function
// must be treated as distinct edges.
func TestDeduplicateCallsBoundaries(t *testing.T) {
	if got := deduplicateCalls(nil); len(got) != 0 {
		t.Fatalf("empty input: expected 0, got %d", len(got))
	}

	differentDepth := []ContractCall{
		{FromContract: "CA", ToContract: "CB", FunctionName: "swap", CallDepth: 1, ExecutionOrder: 1},
		{FromContract: "CA", ToContract: "CB", FunctionName: "swap", CallDepth: 2, ExecutionOrder: 2},
	}
	if got := deduplicateCalls(differentDepth); len(got) != 2 {
		t.Fatalf("different depth should be distinct: expected 2, got %d", len(got))
	}

	differentFunction := []ContractCall{
		{FromContract: "CA", ToContract: "CB", FunctionName: "swap", CallDepth: 1, ExecutionOrder: 1},
		{FromContract: "CA", ToContract: "CB", FunctionName: "deposit", CallDepth: 1, ExecutionOrder: 2},
	}
	if got := deduplicateCalls(differentFunction); len(got) != 2 {
		t.Fatalf("different function should be distinct: expected 2, got %d", len(got))
	}
}

// TestContractCallJSONMarshaling guards the on-wire JSON field names, which are
// a serialization contract consumed downstream. A struct-tag rename must fail here.
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

	var parsed map[string]interface{}
	if err := json.Unmarshal(data, &parsed); err != nil {
		t.Fatalf("json.Unmarshal() error = %v", err)
	}

	for _, field := range []string{"from_contract", "to_contract", "function", "call_depth", "execution_order", "successful"} {
		if _, ok := parsed[field]; !ok {
			t.Errorf("JSON missing expected field %q", field)
		}
	}
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
