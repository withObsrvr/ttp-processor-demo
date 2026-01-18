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
// @Summary Get contracts involved in transaction
// @Description Returns list of all contract addresses involved in a transaction (for Freighter wallet)
// @Tags Contracts
// @Accept json
// @Produce json
// @Param hash path string true "Transaction hash"
// @Success 200 {object} map[string]interface{} "List of contracts involved"
// @Failure 400 {object} map[string]interface{} "Invalid parameters"
// @Failure 404 {object} map[string]interface{} "No contracts found"
// @Failure 500 {object} map[string]interface{} "Internal server error"
// @Router /api/v1/silver/tx/{hash}/contracts-involved [get]
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
// @Summary Get transaction call graph
// @Description Returns contract-to-contract call graph showing execution flow
// @Tags Contracts
// @Accept json
// @Produce json
// @Param hash path string true "Transaction hash"
// @Success 200 {object} map[string]interface{} "Call graph with from/to/function/depth/order"
// @Failure 400 {object} map[string]interface{} "Invalid parameters"
// @Failure 500 {object} map[string]interface{} "Internal server error"
// @Router /api/v1/silver/tx/{hash}/call-graph [get]
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
// @Summary Get transaction contract hierarchy
// @Description Returns hierarchical view of contract calls with root/child relationships
// @Tags Contracts
// @Accept json
// @Produce json
// @Param hash path string true "Transaction hash"
// @Success 200 {object} map[string]interface{} "Contract hierarchy with root/child/path/depth"
// @Failure 400 {object} map[string]interface{} "Invalid parameters"
// @Failure 500 {object} map[string]interface{} "Internal server error"
// @Router /api/v1/silver/tx/{hash}/hierarchy [get]
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
// @Summary Get recent contract calls
// @Description Returns recent calls involving a specific contract (as caller and callee)
// @Tags Contracts
// @Accept json
// @Produce json
// @Param id path string true "Contract ID (C... address)"
// @Param limit query int false "Maximum results to return (default: 100, max: 1000)"
// @Success 200 {object} map[string]interface{} "Recent calls with as_caller and as_callee counts"
// @Failure 400 {object} map[string]interface{} "Invalid parameters"
// @Failure 500 {object} map[string]interface{} "Internal server error"
// @Router /api/v1/silver/contracts/{id}/recent-calls [get]
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
// @Summary Get contract callers
// @Description Returns list of contracts that have called a specific contract
// @Tags Contracts
// @Accept json
// @Produce json
// @Param id path string true "Contract ID (C... address)"
// @Param limit query int false "Maximum results to return (default: 50, max: 200)"
// @Success 200 {object} map[string]interface{} "List of caller contracts with call counts and functions"
// @Failure 400 {object} map[string]interface{} "Invalid parameters"
// @Failure 500 {object} map[string]interface{} "Internal server error"
// @Router /api/v1/silver/contracts/{id}/callers [get]
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
// @Summary Get contract callees
// @Description Returns list of contracts that have been called by a specific contract
// @Tags Contracts
// @Accept json
// @Produce json
// @Param id path string true "Contract ID (C... address)"
// @Param limit query int false "Maximum results to return (default: 50, max: 200)"
// @Success 200 {object} map[string]interface{} "List of callee contracts with call counts and functions"
// @Failure 400 {object} map[string]interface{} "Invalid parameters"
// @Failure 500 {object} map[string]interface{} "Internal server error"
// @Router /api/v1/silver/contracts/{id}/callees [get]
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
// @Summary Get contract call summary
// @Description Returns aggregated call statistics for a specific contract
// @Tags Contracts
// @Accept json
// @Produce json
// @Param id path string true "Contract ID (C... address)"
// @Success 200 {object} ContractCallSummary "Call summary with totals and unique counts"
// @Failure 400 {object} map[string]interface{} "Invalid parameters"
// @Failure 500 {object} map[string]interface{} "Internal server error"
// @Router /api/v1/silver/contracts/{id}/call-summary [get]
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
// @Summary Get wallet-friendly contracts summary
// @Description Returns contracts involved in a transaction optimized for wallet display (Freighter, etc.)
// @Tags Contracts
// @Accept json
// @Produce json
// @Param hash path string true "Transaction hash"
// @Success 200 {object} map[string]interface{} "Wallet-friendly contract summary"
// @Failure 400 {object} map[string]interface{} "Invalid parameters"
// @Failure 500 {object} map[string]interface{} "Internal server error"
// @Router /api/v1/silver/tx/{hash}/contracts-summary [get]
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
// @Summary Get contract analytics
// @Description Returns comprehensive analytics including stats, top functions, and daily trends
// @Tags Contracts
// @Accept json
// @Produce json
// @Param id path string true "Contract ID (C... address)"
// @Success 200 {object} map[string]interface{} "Full analytics with stats, top functions, and trends"
// @Failure 400 {object} map[string]interface{} "Invalid parameters"
// @Failure 404 {object} map[string]interface{} "No activity found for contract"
// @Failure 500 {object} map[string]interface{} "Internal server error"
// @Router /api/v1/silver/contracts/{id}/analytics [get]
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
// @Summary Get top contracts by activity
// @Description Returns most active contracts ranked by call count for a given period
// @Tags Contracts
// @Accept json
// @Produce json
// @Param period query string false "Time period: 24h (default), 7d, 30d, all"
// @Param limit query int false "Maximum results to return (default: 20, max: 100)"
// @Success 200 {object} map[string]interface{} "List of top contracts with call counts"
// @Failure 400 {object} map[string]interface{} "Invalid parameters"
// @Failure 500 {object} map[string]interface{} "Internal server error"
// @Router /api/v1/silver/contracts/top [get]
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
