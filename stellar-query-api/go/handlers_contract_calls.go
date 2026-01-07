package main

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/gorilla/mux"
)

// ContractCallHandlers contains HTTP handlers for contract call graph queries
// These endpoints are designed for Freighter wallet's "Contracts Involved" feature
type ContractCallHandlers struct {
	reader *UnifiedSilverReader
}

// NewContractCallHandlers creates new contract call API handlers
func NewContractCallHandlers(reader *UnifiedSilverReader) *ContractCallHandlers {
	return &ContractCallHandlers{reader: reader}
}

// ============================================
// TRANSACTION-CENTRIC ENDPOINTS
// ============================================

// HandleContractsInvolved returns all contracts involved in a transaction
// GET /api/v1/silver/tx/{hash}/contracts-involved
// Response: { "contracts_involved": ["CXXX...", "CYYY...", "CZZZ..."], "count": 3 }
func (h *ContractCallHandlers) HandleContractsInvolved(w http.ResponseWriter, r *http.Request) {
	// Extract transaction hash from path using gorilla/mux
	vars := mux.Vars(r)
	txHash := vars["hash"]
	if txHash == "" {
		respondError(w, "transaction hash required", http.StatusBadRequest)
		return
	}

	contracts, err := h.reader.GetContractsInvolved(r.Context(), txHash)
	if err != nil {
		respondError(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if len(contracts) == 0 {
		respondError(w, "no contracts found for transaction", http.StatusNotFound)
		return
	}

	respondJSON(w, map[string]interface{}{
		"transaction_hash":   txHash,
		"contracts_involved": contracts,
		"count":              len(contracts),
	})
}

// HandleCallGraph returns the call graph for a transaction
// GET /api/v1/silver/tx/{hash}/call-graph
// Response: { "calls": [{ "from": "CXXX", "to": "CYYY", "function": "swap", "depth": 1, "order": 0 }, ...] }
func (h *ContractCallHandlers) HandleCallGraph(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	txHash := vars["hash"]
	if txHash == "" {
		respondError(w, "transaction hash required", http.StatusBadRequest)
		return
	}

	calls, err := h.reader.GetTransactionCallGraph(r.Context(), txHash)
	if err != nil {
		respondError(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Transform to Freighter-friendly format
	callsResponse := make([]map[string]interface{}, len(calls))
	for i, call := range calls {
		callsResponse[i] = map[string]interface{}{
			"from":       call.FromContract,
			"to":         call.ToContract,
			"function":   call.FunctionName,
			"depth":      call.CallDepth,
			"order":      call.ExecutionOrder,
			"successful": call.Successful,
		}
	}

	respondJSON(w, map[string]interface{}{
		"transaction_hash": txHash,
		"calls":            callsResponse,
		"count":            len(callsResponse),
	})
}

// HandleTransactionHierarchy returns the contract hierarchy for a transaction
// GET /api/v1/silver/tx/{hash}/hierarchy
// Response: { "hierarchies": [{ "root": "CXXX", "child": "CYYY", "path": ["CXXX", "CYYY"], "depth": 1 }, ...] }
func (h *ContractCallHandlers) HandleTransactionHierarchy(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	txHash := vars["hash"]
	if txHash == "" {
		respondError(w, "transaction hash required", http.StatusBadRequest)
		return
	}

	hierarchies, err := h.reader.GetTransactionHierarchy(r.Context(), txHash)
	if err != nil {
		respondError(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Transform to response format
	hierarchyResponse := make([]map[string]interface{}, len(hierarchies))
	for i, h := range hierarchies {
		hierarchyResponse[i] = map[string]interface{}{
			"root":  h.RootContract,
			"child": h.ChildContract,
			"path":  h.FullPath,
			"depth": h.PathDepth,
		}
	}

	respondJSON(w, map[string]interface{}{
		"transaction_hash": txHash,
		"hierarchies":      hierarchyResponse,
		"count":            len(hierarchyResponse),
	})
}

// ============================================
// CONTRACT-CENTRIC ENDPOINTS
// ============================================

// HandleRecentCalls returns recent calls for a specific contract
// GET /api/v1/silver/contracts/{id}/recent-calls?limit=100
// Response: { "calls": [...], "as_caller": 45, "as_callee": 55 }
func (h *ContractCallHandlers) HandleRecentCalls(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	contractID := vars["id"]
	if contractID == "" {
		respondError(w, "contract_id required", http.StatusBadRequest)
		return
	}

	limit := parseLimit(r, 100, 1000)

	calls, asCaller, asCallee, err := h.reader.GetContractRecentCalls(r.Context(), contractID, limit)
	if err != nil {
		respondError(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Transform calls to response format
	callsResponse := make([]map[string]interface{}, len(calls))
	for i, call := range calls {
		callsResponse[i] = map[string]interface{}{
			"from":             call.FromContract,
			"to":               call.ToContract,
			"function":         call.FunctionName,
			"depth":            call.CallDepth,
			"transaction_hash": call.TransactionHash,
			"ledger_sequence":  call.LedgerSequence,
			"successful":       call.Successful,
			"closed_at":        call.ClosedAt,
		}
	}

	respondJSON(w, map[string]interface{}{
		"contract_id": contractID,
		"calls":       callsResponse,
		"as_caller":   asCaller,
		"as_callee":   asCallee,
		"total":       len(callsResponse),
	})
}

// HandleContractCallers returns contracts that call a specific contract
// GET /api/v1/silver/contracts/{id}/callers?limit=50
// Response: { "callers": [{ "contract_id": "CXXX", "call_count": 100, "functions": ["swap", "quote"] }] }
func (h *ContractCallHandlers) HandleContractCallers(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	contractID := vars["id"]
	if contractID == "" {
		respondError(w, "contract_id required", http.StatusBadRequest)
		return
	}

	limit := parseLimit(r, 50, 200)

	callers, err := h.reader.GetContractCallers(r.Context(), contractID, limit)
	if err != nil {
		respondError(w, err.Error(), http.StatusInternalServerError)
		return
	}

	respondJSON(w, map[string]interface{}{
		"contract_id": contractID,
		"callers":     callers,
		"count":       len(callers),
	})
}

// HandleContractCallees returns contracts called by a specific contract
// GET /api/v1/silver/contracts/{id}/callees?limit=50
// Response: { "callees": [{ "contract_id": "CYYY", "call_count": 80, "functions": ["transfer"] }] }
func (h *ContractCallHandlers) HandleContractCallees(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	contractID := vars["id"]
	if contractID == "" {
		respondError(w, "contract_id required", http.StatusBadRequest)
		return
	}

	limit := parseLimit(r, 50, 200)

	callees, err := h.reader.GetContractCallees(r.Context(), contractID, limit)
	if err != nil {
		respondError(w, err.Error(), http.StatusInternalServerError)
		return
	}

	respondJSON(w, map[string]interface{}{
		"contract_id": contractID,
		"callees":     callees,
		"count":       len(callees),
	})
}

// HandleContractCallSummary returns aggregated call statistics for a contract
// GET /api/v1/silver/contracts/{id}/call-summary
// Response: { "total_calls_as_caller": 150, "total_calls_as_callee": 200, "unique_callers": 10, "unique_callees": 5 }
func (h *ContractCallHandlers) HandleContractCallSummary(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	contractID := vars["id"]
	if contractID == "" {
		respondError(w, "contract_id required", http.StatusBadRequest)
		return
	}

	summary, err := h.reader.GetContractCallSummary(r.Context(), contractID)
	if err != nil {
		respondError(w, err.Error(), http.StatusInternalServerError)
		return
	}

	respondJSON(w, summary)
}

// ============================================
// WALLET-FRIENDLY CONTRACTS SUMMARY
// ============================================

// HandleContractsSummary returns contracts in wallet-friendly format
// GET /api/v1/silver/tx/{hash}/contracts-summary
// Response optimized for wallet display (Freighter, etc.)
func (h *ContractCallHandlers) HandleContractsSummary(w http.ResponseWriter, r *http.Request) {
	// Extract transaction hash from path using gorilla/mux
	vars := mux.Vars(r)
	txHash := vars["hash"]
	if txHash == "" {
		respondError(w, "transaction hash required", http.StatusBadRequest)
		return
	}

	// Get contracts involved
	contracts, err := h.reader.GetContractsInvolved(r.Context(), txHash)
	if err != nil {
		respondError(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Get call graph for display purposes
	calls, _ := h.reader.GetTransactionCallGraph(r.Context(), txHash)

	// Build Freighter-optimized response
	contractDetails := make([]map[string]interface{}, len(contracts))
	for i, contractID := range contracts {
		// Count calls for this contract
		callsAsFrom := 0
		callsAsTo := 0
		var functions []string
		functionSet := make(map[string]bool)

		for _, call := range calls {
			if call.FromContract == contractID {
				callsAsFrom++
			}
			if call.ToContract == contractID {
				callsAsTo++
				if call.FunctionName != "" && !functionSet[call.FunctionName] {
					functions = append(functions, call.FunctionName)
					functionSet[call.FunctionName] = true
				}
			}
		}

		contractDetails[i] = map[string]interface{}{
			"contract_id":    contractID,
			"calls_made":     callsAsFrom,
			"calls_received": callsAsTo,
			"functions":      functions,
			"is_root":        i == 0, // First contract is the root caller
		}
	}

	respondJSON(w, map[string]interface{}{
		"transaction_hash":   txHash,
		"contracts_involved": contractDetails,
		"total_contracts":    len(contracts),
		"total_calls":        len(calls),
		"display_format":     "wallet_v1",
	})
}

// ============================================
// CONTRACT ANALYTICS ENDPOINTS
// ============================================

// HandleContractAnalyticsSummary returns comprehensive analytics for a contract
// GET /api/v1/silver/contracts/{id}/analytics
// Response: Full analytics including stats, top functions, and daily trend
func (h *ContractCallHandlers) HandleContractAnalyticsSummary(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	contractID := vars["id"]
	if contractID == "" {
		respondError(w, "contract_id required", http.StatusBadRequest)
		return
	}

	summary, err := h.reader.GetContractAnalyticsSummary(r.Context(), contractID)
	if err != nil {
		respondError(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if summary.Stats.TotalCallsAsCaller == 0 && summary.Stats.TotalCallsAsCallee == 0 {
		respondError(w, "no activity found for contract", http.StatusNotFound)
		return
	}

	respondJSON(w, summary)
}

// HandleTopContracts returns the most active contracts for a given period
// GET /api/v1/silver/contracts/top?period=24h&limit=20
// Response: List of top contracts ranked by call count
func (h *ContractCallHandlers) HandleTopContracts(w http.ResponseWriter, r *http.Request) {
	// Parse query parameters
	period := r.URL.Query().Get("period")
	if period == "" {
		period = "24h"
	}

	// Validate period
	validPeriods := map[string]bool{"24h": true, "7d": true, "30d": true, "all": true}
	if !validPeriods[period] {
		respondError(w, "invalid period: must be 24h, 7d, 30d, or all", http.StatusBadRequest)
		return
	}

	limit := parseLimit(r, 20, 100)

	contracts, err := h.reader.GetTopContracts(r.Context(), period, limit)
	if err != nil {
		respondError(w, err.Error(), http.StatusInternalServerError)
		return
	}

	respondJSON(w, map[string]interface{}{
		"period":       period,
		"contracts":    contracts,
		"count":        len(contracts),
		"generated_at": time.Now().UTC().Format(time.RFC3339),
	})
}

// ============================================
// TYPES
// ============================================

// ContractCall represents a single contract-to-contract call
type ContractCall struct {
	FromContract    string
	ToContract      string
	FunctionName    string
	CallDepth       int
	ExecutionOrder  int
	Successful      bool
	TransactionHash string
	LedgerSequence  int64
	ClosedAt        string
}

// ContractHierarchy represents a contract hierarchy relationship
type ContractHierarchy struct {
	RootContract  string
	ChildContract string
	PathDepth     int
	FullPath      []string
}

// ContractRelationship represents aggregated call relationship stats
type ContractRelationship struct {
	ContractID string   `json:"contract_id"`
	CallCount  int      `json:"call_count"`
	Functions  []string `json:"functions"`
	LastCall   string   `json:"last_call,omitempty"`
}

// ContractCallSummary represents aggregated call statistics
type ContractCallSummary struct {
	ContractID         string `json:"contract_id"`
	TotalCallsAsCaller int    `json:"total_calls_as_caller"`
	TotalCallsAsCallee int    `json:"total_calls_as_callee"`
	UniqueCallers      int    `json:"unique_callers"`
	UniqueCallees      int    `json:"unique_callees"`
	FirstSeen          string `json:"first_seen,omitempty"`
	LastSeen           string `json:"last_seen,omitempty"`
}

// Helper function for JSON encoding (mirrors handlers_silver.go)
func respondJSONContractCall(w http.ResponseWriter, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(data)
}
