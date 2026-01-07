package main

import (
	"encoding/json"
	"fmt"
	"log"

	"github.com/stellar/go-stellar-sdk/ingest"
	"github.com/stellar/go-stellar-sdk/strkey"
	"github.com/stellar/go-stellar-sdk/xdr"
)

// extractCallGraph extracts the cross-contract call graph from a transaction's operation
// This is the main entry point for call graph extraction, combining:
// 1. Diagnostic events (fn_call/fn_return patterns)
// 2. Authorization sub-invocations
// Returns a CallGraphResult with all calls, contracts involved, and max depth
func extractCallGraph(
	tx ingest.LedgerTransaction,
	opIndex int,
	op xdr.Operation,
) (*CallGraphResult, error) {
	// Only process InvokeHostFunction operations (type 24)
	invokeOp, ok := op.Body.GetInvokeHostFunctionOp()
	if !ok {
		return nil, nil
	}

	// Only process InvokeContract host function
	if invokeOp.HostFunction.Type != xdr.HostFunctionTypeHostFunctionTypeInvokeContract {
		return nil, nil
	}

	// Get the main contract being invoked
	if invokeOp.HostFunction.InvokeContract == nil {
		return nil, nil
	}

	invokeContract := invokeOp.HostFunction.InvokeContract
	mainContractID, err := invokeContract.ContractAddress.String()
	if err != nil {
		return nil, fmt.Errorf("failed to get main contract ID: %w", err)
	}

	var allCalls []ContractCall
	contractsSet := make(map[string]bool)
	contractsSet[mainContractID] = true

	// Source 1: Extract from diagnostic events (fn_call/fn_return patterns)
	diagnosticCalls := extractCrossContractCallsFromDiagnosticEvents(tx, opIndex, mainContractID)
	allCalls = append(allCalls, diagnosticCalls...)

	// Source 2: Extract from authorization sub-invocations
	authCalls := extractCallsFromAuthEntries(invokeOp.Auth, mainContractID)
	allCalls = append(allCalls, authCalls...)

	// Deduplicate calls (same from->to->function at same depth)
	allCalls = deduplicateCalls(allCalls)

	// Build contracts involved set and find max depth
	maxDepth := 0
	for _, call := range allCalls {
		contractsSet[call.FromContract] = true
		contractsSet[call.ToContract] = true
		if call.CallDepth > maxDepth {
			maxDepth = call.CallDepth
		}
	}

	// Convert set to slice
	var contractsInvolved []string
	for contract := range contractsSet {
		contractsInvolved = append(contractsInvolved, contract)
	}

	// If no cross-contract calls, return nil (no call graph to store)
	if len(allCalls) == 0 {
		return nil, nil
	}

	return &CallGraphResult{
		Calls:             allCalls,
		ContractsInvolved: contractsInvolved,
		MaxDepth:          maxDepth,
	}, nil
}

// extractCrossContractCallsFromDiagnosticEvents extracts cross-contract calls from diagnostic events
// Uses fn_call/fn_return patterns to track the call stack
// Ported from cdp-pipeline-workflow/processor/processor_contract_invocation_v3.go lines 803-926
func extractCrossContractCallsFromDiagnosticEvents(
	tx ingest.LedgerTransaction,
	opIndex int,
	mainContractID string,
) []ContractCall {
	var calls []ContractCall

	// Get diagnostic events
	txEvents, err := tx.GetTransactionEvents()
	if err != nil {
		return calls
	}

	diagnosticEvents := txEvents.DiagnosticEvents
	if len(diagnosticEvents) == 0 {
		return calls
	}

	executionOrder := 0
	currentDepth := 0

	// Track call stack for determining "from" contract
	callStack := []string{mainContractID}

	for i, diagEvent := range diagnosticEvents {
		// Skip events with nil ContractId
		if diagEvent.Event.ContractId == nil {
			continue
		}

		// Get the contract that emitted this event
		eventContractID, err := strkey.Encode(strkey.VersionByteContract, diagEvent.Event.ContractId[:])
		if err != nil {
			continue
		}

		// Get first topic if available (to detect fn_call/fn_return)
		firstTopic := extractFirstTopic(diagEvent.Event)

		// Pattern 1: fn_call from main contract followed by event from different contract
		if firstTopic == "fn_call" && eventContractID == mainContractID && i+1 < len(diagnosticEvents) {
			nextEvent := diagnosticEvents[i+1]
			if nextEvent.Event.ContractId != nil {
				nextContractID, err := strkey.Encode(strkey.VersionByteContract, nextEvent.Event.ContractId[:])
				if err == nil && nextContractID != eventContractID {
					// This fn_call is calling another contract
					// Try to get function name from callee's fn_call event first (more accurate)
					var functionName string
					nextFirstTopic := extractFirstTopic(nextEvent.Event)
					if nextFirstTopic == "fn_call" {
						functionName = extractFunctionNameFromDiagnosticEvent(nextEvent.Event)
					}
					// Fallback to caller's event if callee's doesn't have it
					if functionName == "" || functionName == "unknown" {
						functionName = extractFunctionNameFromDiagnosticEvent(diagEvent.Event)
					}
					arguments := extractArgumentsFromDiagnosticEvent(diagEvent.Event)

					// Final fallback: extract fn_name from arguments (auth entries)
					if functionName == "" || functionName == "unknown" {
						if argFnName := extractFunctionNameFromArguments(arguments); argFnName != "" {
							functionName = argFnName
						}
					}

					call := ContractCall{
						FromContract:   eventContractID,
						ToContract:     nextContractID,
						FunctionName:   functionName,
						Arguments:      arguments,
						CallDepth:      currentDepth + 1,
						ExecutionOrder: executionOrder,
						Successful:     diagEvent.InSuccessfulContractCall,
					}

					calls = append(calls, call)
					executionOrder++
				}
			}
		}

		// Pattern 2: Detect cross-contract call when contract changes in call stack
		if firstTopic == "fn_call" && eventContractID != mainContractID {
			// A different contract is making a fn_call
			fromContract := mainContractID
			if len(callStack) > 0 {
				fromContract = callStack[len(callStack)-1]
			}

			// Only record if it's actually a different contract calling
			if fromContract != eventContractID {
				functionName := extractFunctionNameFromDiagnosticEvent(diagEvent.Event)
				arguments := extractArgumentsFromDiagnosticEvent(diagEvent.Event)

				// Fallback: extract fn_name from arguments (auth entries)
				if functionName == "" || functionName == "unknown" {
					if argFnName := extractFunctionNameFromArguments(arguments); argFnName != "" {
						functionName = argFnName
					}
				}

				call := ContractCall{
					FromContract:   fromContract,
					ToContract:     eventContractID,
					FunctionName:   functionName,
					Arguments:      arguments,
					CallDepth:      currentDepth + 1,
					ExecutionOrder: executionOrder,
					Successful:     diagEvent.InSuccessfulContractCall,
				}

				calls = append(calls, call)
				executionOrder++
			}
		}

		// Update call stack based on fn_call/fn_return
		if firstTopic == "fn_call" {
			// Push to call stack
			if eventContractID != "" && (len(callStack) == 0 || callStack[len(callStack)-1] != eventContractID) {
				callStack = append(callStack, eventContractID)
				currentDepth++
			}
		} else if firstTopic == "fn_return" && len(callStack) > 1 {
			// Pop from call stack
			callStack = callStack[:len(callStack)-1]
			if currentDepth > 0 {
				currentDepth--
			}
		}
	}

	return calls
}

// extractCallsFromAuthEntries extracts cross-contract calls from authorization entries
// Ported from cdp-pipeline-workflow/processor/processor_contract_invocation_v2.go
func extractCallsFromAuthEntries(
	authEntries []xdr.SorobanAuthorizationEntry,
	mainContractID string,
) []ContractCall {
	var calls []ContractCall
	executionOrder := 0

	for _, authEntry := range authEntries {
		authCalls := extractCallsFromAuthInvocation(
			&authEntry.RootInvocation,
			mainContractID,
			1,
			&executionOrder,
		)
		calls = append(calls, authCalls...)
	}

	return calls
}

// extractCallsFromAuthInvocation recursively extracts calls from auth invocations
func extractCallsFromAuthInvocation(
	invocation *xdr.SorobanAuthorizedInvocation,
	fromContract string,
	depth int,
	executionOrder *int,
) []ContractCall {
	var calls []ContractCall

	if invocation == nil || depth > 10 { // Max depth protection
		return calls
	}

	// First, capture this invocation's function call (the root or current node)
	// This was previously missing - we only captured sub-invocations
	if invocation.Function.Type == xdr.SorobanAuthorizedFunctionTypeSorobanAuthorizedFunctionTypeContractFn {
		contractFn := invocation.Function.ContractFn
		if contractFn != nil {
			toContractID, err := strkey.Encode(strkey.VersionByteContract, contractFn.ContractAddress.ContractId[:])
			if err == nil && toContractID != fromContract {
				// Only record if it's a cross-contract call (different from/to)
				functionName := string(contractFn.FunctionName)
				call := ContractCall{
					FromContract:   fromContract,
					ToContract:     toContractID,
					FunctionName:   functionName,
					CallDepth:      depth,
					ExecutionOrder: *executionOrder,
					Successful:     true,
				}
				calls = append(calls, call)
				*executionOrder++

				// Update fromContract for sub-invocations
				fromContract = toContractID
			}
		}
	}

	// Process sub-invocations recursively
	// The recursive call will capture each sub-invocation's function via the code above
	for _, subInvocation := range invocation.SubInvocations {
		subCalls := extractCallsFromAuthInvocation(
			&subInvocation,
			fromContract, // Pass current contract as the caller
			depth+1,
			executionOrder,
		)
		calls = append(calls, subCalls...)
	}

	return calls
}

// extractFirstTopic extracts the first topic from a contract event as a string
func extractFirstTopic(event xdr.ContractEvent) string {
	if event.Body.V != 0 || event.Body.V0 == nil {
		return ""
	}

	topics := event.Body.V0.Topics
	if len(topics) == 0 {
		return ""
	}

	// Convert first topic to string
	decoded, err := ConvertScValToJSON(topics[0])
	if err != nil {
		return ""
	}

	// Return as string if it is one
	if str, ok := decoded.(string); ok {
		return str
	}

	return fmt.Sprintf("%v", decoded)
}

// extractFunctionNameFromDiagnosticEvent extracts function name from the second topic of a fn_call event
// For fn_call diagnostic events, topics are: [0] = "fn_call", [1] = function_name (symbol)
func extractFunctionNameFromDiagnosticEvent(event xdr.ContractEvent) string {
	if event.Body.V != 0 || event.Body.V0 == nil {
		return "unknown"
	}

	topics := event.Body.V0.Topics
	if len(topics) < 2 {
		return "unknown"
	}

	// Use the dedicated function that handles symbols, strings, and bytes
	name := GetFunctionNameFromScVal(topics[1])
	if name == "" {
		return "unknown"
	}
	return name
}

// extractArgumentsFromDiagnosticEvent extracts function arguments from the Data field of a fn_call event
// For fn_call diagnostic events, the Data field contains the function arguments as ScVal
func extractArgumentsFromDiagnosticEvent(event xdr.ContractEvent) interface{} {
	if event.Body.V != 0 || event.Body.V0 == nil {
		return nil
	}

	// Arguments are in the Data field
	decoded, err := ConvertScValToJSON(event.Body.V0.Data)
	if err != nil {
		return nil
	}

	return decoded
}

// extractFunctionNameFromArguments searches the arguments structure for fn_name
// This is a fallback when diagnostic event extraction fails
// The fn_name is typically found in authorization entries nested in the arguments
func extractFunctionNameFromArguments(arguments interface{}) string {
	if arguments == nil {
		return ""
	}

	// Recursively search for fn_name in the structure
	return findFnNameRecursive(arguments)
}

// findFnNameRecursive recursively searches a JSON-like structure for fn_name
func findFnNameRecursive(v interface{}) string {
	switch val := v.(type) {
	case map[string]interface{}:
		// Check if this map has fn_name
		if fnName, ok := val["fn_name"]; ok {
			if str, ok := fnName.(string); ok && str != "" {
				return str
			}
		}
		// Recursively search all values
		for _, v := range val {
			if result := findFnNameRecursive(v); result != "" {
				return result
			}
		}
	case []interface{}:
		// Search through array elements
		for _, item := range val {
			if result := findFnNameRecursive(item); result != "" {
				return result
			}
		}
	}
	return ""
}

// deduplicateCalls removes duplicate calls (same from->to->function at same depth)
func deduplicateCalls(calls []ContractCall) []ContractCall {
	seen := make(map[string]bool)
	var unique []ContractCall

	for _, call := range calls {
		key := fmt.Sprintf("%s->%s:%s@%d", call.FromContract, call.ToContract, call.FunctionName, call.CallDepth)
		if !seen[key] {
			seen[key] = true
			unique = append(unique, call)
		}
	}

	return unique
}

// callGraphToJSON converts a CallGraphResult to JSON strings for database storage
func callGraphToJSON(result *CallGraphResult) (*string, []string, *int, error) {
	if result == nil || len(result.Calls) == 0 {
		return nil, nil, nil, nil
	}

	// Convert calls to JSON
	callsJSON, err := json.Marshal(result.Calls)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to marshal calls to JSON: %w", err)
	}

	callsStr := string(callsJSON)
	maxDepth := result.MaxDepth

	return &callsStr, result.ContractsInvolved, &maxDepth, nil
}

// integrateCallGraph integrates call graph extraction into operation data extraction
// Call this from the main extraction loop after extracting basic operation details
func integrateCallGraph(
	tx ingest.LedgerTransaction,
	opIndex int,
	op xdr.Operation,
	opData *OperationData,
) error {
	// Extract call graph
	callGraph, err := extractCallGraph(tx, opIndex, op)
	if err != nil {
		log.Printf("Warning: Failed to extract call graph for op %d: %v", opIndex, err)
		return nil // Don't fail the whole operation for call graph errors
	}

	if callGraph == nil {
		return nil // No call graph to integrate
	}

	// Convert to JSON and integrate into operation data
	callsJSON, contractsInvolved, maxDepth, err := callGraphToJSON(callGraph)
	if err != nil {
		log.Printf("Warning: Failed to convert call graph to JSON for op %d: %v", opIndex, err)
		return nil
	}

	opData.ContractCallsJSON = callsJSON
	opData.ContractsInvolved = contractsInvolved
	opData.MaxCallDepth = maxDepth

	return nil
}
