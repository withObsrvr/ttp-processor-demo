package main

import (
	"encoding/hex"
	"encoding/json"
	"net/http"
	"strconv"
	"strings"

	"github.com/stellar/go/strkey"
)

// ContractIndexHandlers contains HTTP handlers for Contract Event Index queries
type ContractIndexHandlers struct {
	reader *ContractIndexReader
}

// NewContractIndexHandlers creates new Contract Index API handlers
func NewContractIndexHandlers(reader *ContractIndexReader) *ContractIndexHandlers {
	return &ContractIndexHandlers{reader: reader}
}

// normalizeContractID converts a contract address to hex format
// Accepts both:
//   - Stellar StrKey contract address (starts with 'C', like CBLJ2...)
//   - Hex-encoded contract hash (64 chars)
// Returns hex format (64 chars) for querying the index
func normalizeContractID(input string) (string, error) {
	// If it starts with 'C', decode from StrKey
	if strings.HasPrefix(input, "C") {
		decoded, err := strkey.Decode(strkey.VersionByteContract, input)
		if err != nil {
			return "", err
		}
		return hex.EncodeToString(decoded), nil
	}

	// Otherwise, validate as hex
	if len(input) != 64 {
		return "", strkey.ErrInvalidVersionByte
	}

	// Validate it's valid hex
	_, err := hex.DecodeString(input)
	if err != nil {
		return "", err
	}

	return strings.ToLower(input), nil
}

// hexToStrKey converts a hex contract ID to Stellar StrKey contract address
func hexToStrKey(hexID string) (string, error) {
	hashBytes, err := hex.DecodeString(hexID)
	if err != nil {
		return "", err
	}

	address, err := strkey.Encode(strkey.VersionByteContract, hashBytes)
	if err != nil {
		return "", err
	}

	return address, nil
}

// HandleContractLedgers returns list of ledgers containing events from a specific contract
// GET /api/v1/index/contracts/{contract_id}/ledgers
// Query params:
//   - start_ledger: optional minimum ledger sequence
//   - end_ledger: optional maximum ledger sequence
//   - limit: optional result limit (default: 1000)
func (h *ContractIndexHandlers) HandleContractLedgers(w http.ResponseWriter, r *http.Request) {
	// Extract contract ID from URL path
	path := r.URL.Path
	var contractID string

	if strings.HasPrefix(path, "/api/v1/index/contracts/") {
		// Remove prefix and "/ledgers" suffix
		contractID = strings.TrimPrefix(path, "/api/v1/index/contracts/")
		contractID = strings.TrimSuffix(contractID, "/ledgers")
		contractID = strings.TrimSuffix(contractID, "/")
	} else {
		respondError(w, "invalid path", http.StatusBadRequest)
		return
	}

	if contractID == "" {
		respondError(w, "contract_id required", http.StatusBadRequest)
		return
	}

	// Normalize contract ID (convert StrKey to hex if needed)
	normalizedID, err := normalizeContractID(contractID)
	if err != nil {
		respondError(w, "invalid contract_id: must be Stellar contract address (C...) or 64-char hex hash", http.StatusBadRequest)
		return
	}
	contractID = normalizedID

	// Parse query parameters
	startLedger := int64(0)
	endLedger := int64(0)
	limit := 1000

	if startStr := r.URL.Query().Get("start_ledger"); startStr != "" {
		val, err := strconv.ParseInt(startStr, 10, 64)
		if err != nil {
			respondError(w, "invalid start_ledger", http.StatusBadRequest)
			return
		}
		startLedger = val
	}

	if endStr := r.URL.Query().Get("end_ledger"); endStr != "" {
		val, err := strconv.ParseInt(endStr, 10, 64)
		if err != nil {
			respondError(w, "invalid end_ledger", http.StatusBadRequest)
			return
		}
		endLedger = val
	}

	if limitStr := r.URL.Query().Get("limit"); limitStr != "" {
		val, err := strconv.Atoi(limitStr)
		if err != nil {
			respondError(w, "invalid limit", http.StatusBadRequest)
			return
		}
		if val > 10000 {
			respondError(w, "limit cannot exceed 10000", http.StatusBadRequest)
			return
		}
		limit = val
	}

	// Lookup in Contract Event Index
	ledgers, err := h.reader.GetLedgersForContract(r.Context(), contractID, startLedger, endLedger, limit)
	if err != nil {
		respondError(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Convert hex to StrKey for response
	contractAddress, err := hexToStrKey(contractID)
	if err != nil {
		// If conversion fails, just use hex
		contractAddress = ""
	}

	if len(ledgers) == 0 {
		// Return empty array instead of 404
		respondJSON(w, map[string]interface{}{
			"contract_id":      contractID,
			"contract_address": contractAddress,
			"ledgers":          []int64{},
			"total":            0,
		})
		return
	}

	respondJSON(w, map[string]interface{}{
		"contract_id":      contractID,
		"contract_address": contractAddress,
		"ledgers":          ledgers,
		"total":            len(ledgers),
	})
}

// HandleContractEventSummary returns detailed event information for a contract
// GET /api/v1/index/contracts/{contract_id}/summary
// Query params:
//   - start_ledger: optional minimum ledger sequence
//   - end_ledger: optional maximum ledger sequence
func (h *ContractIndexHandlers) HandleContractEventSummary(w http.ResponseWriter, r *http.Request) {
	// Extract contract ID from URL path
	path := r.URL.Path
	var contractID string

	if strings.HasPrefix(path, "/api/v1/index/contracts/") {
		contractID = strings.TrimPrefix(path, "/api/v1/index/contracts/")
		contractID = strings.TrimSuffix(contractID, "/summary")
		contractID = strings.TrimSuffix(contractID, "/")
	} else {
		respondError(w, "invalid path", http.StatusBadRequest)
		return
	}

	if contractID == "" {
		respondError(w, "contract_id required", http.StatusBadRequest)
		return
	}

	// Normalize contract ID (convert StrKey to hex if needed)
	normalizedID, err := normalizeContractID(contractID)
	if err != nil {
		respondError(w, "invalid contract_id: must be Stellar contract address (C...) or 64-char hex hash", http.StatusBadRequest)
		return
	}
	contractID = normalizedID

	// Parse query parameters
	startLedger := int64(0)
	endLedger := int64(0)

	if startStr := r.URL.Query().Get("start_ledger"); startStr != "" {
		val, err := strconv.ParseInt(startStr, 10, 64)
		if err != nil {
			respondError(w, "invalid start_ledger", http.StatusBadRequest)
			return
		}
		startLedger = val
	}

	if endStr := r.URL.Query().Get("end_ledger"); endStr != "" {
		val, err := strconv.ParseInt(endStr, 10, 64)
		if err != nil {
			respondError(w, "invalid end_ledger", http.StatusBadRequest)
			return
		}
		endLedger = val
	}

	// Get summary from Contract Event Index
	summary, err := h.reader.GetContractEventSummary(r.Context(), contractID, startLedger, endLedger)
	if err != nil {
		respondError(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Convert hex to StrKey for response
	contractAddress, err := hexToStrKey(contractID)
	if err != nil {
		// If conversion fails, just use hex
		contractAddress = ""
	}

	respondJSON(w, map[string]interface{}{
		"contract_id":      contractID,
		"contract_address": contractAddress,
		"summary":          summary,
		"total":            len(summary),
	})
}

// HandleBatchContractLookup performs batch lookup for multiple contracts
// POST /api/v1/index/contracts/lookup
// Body: {"contract_ids": ["CABC...", "CDEF...", ...]}
func (h *ContractIndexHandlers) HandleBatchContractLookup(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		respondError(w, "POST required", http.StatusMethodNotAllowed)
		return
	}

	var request struct {
		ContractIDs []string `json:"contract_ids"`
	}

	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		respondError(w, "invalid JSON body", http.StatusBadRequest)
		return
	}

	if len(request.ContractIDs) == 0 {
		respondError(w, "contract_ids array required", http.StatusBadRequest)
		return
	}

	if len(request.ContractIDs) > 100 {
		respondError(w, "maximum 100 contract_ids per request", http.StatusBadRequest)
		return
	}

	// Normalize all contract IDs (convert StrKey to hex if needed)
	normalizedIDs := make([]string, len(request.ContractIDs))
	for i, contractID := range request.ContractIDs {
		normalizedID, err := normalizeContractID(contractID)
		if err != nil {
			respondError(w, "invalid contract_id at index "+strconv.Itoa(i)+": must be Stellar contract address (C...) or 64-char hex hash", http.StatusBadRequest)
			return
		}
		normalizedIDs[i] = normalizedID
	}

	// Batch lookup
	results, err := h.reader.GetLedgersForContracts(r.Context(), normalizedIDs)
	if err != nil {
		respondError(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Build response with all requested contracts
	type Result struct {
		ContractID      string  `json:"contract_id"`
		ContractAddress string  `json:"contract_address"`
		Ledgers         []int64 `json:"ledgers"`
		Total           int     `json:"total"`
	}

	responseResults := make([]Result, 0, len(request.ContractIDs))
	for i := range request.ContractIDs {
		// Use normalized ID (hex) to lookup results
		normalizedID := normalizedIDs[i]
		ledgers, ok := results[normalizedID]
		if !ok {
			ledgers = []int64{}
		}

		// Convert hex to StrKey for response
		contractAddress, err := hexToStrKey(normalizedID)
		if err != nil {
			// If conversion fails, just use empty string
			contractAddress = ""
		}

		responseResults = append(responseResults, Result{
			ContractID:      normalizedID,
			ContractAddress: contractAddress,
			Ledgers:         ledgers,
			Total:           len(ledgers),
		})
	}

	respondJSON(w, map[string]interface{}{
		"results": responseResults,
		"total":   len(request.ContractIDs),
	})
}

// HandleContractIndexHealth returns Contract Event Index health and coverage statistics
// GET /api/v1/index/contracts/health
func (h *ContractIndexHandlers) HandleContractIndexHealth(w http.ResponseWriter, r *http.Request) {
	stats, err := h.reader.GetIndexStats(r.Context())
	if err != nil {
		respondError(w, err.Error(), http.StatusInternalServerError)
		return
	}

	response := map[string]interface{}{
		"status": "healthy",
		"index":  stats,
	}

	respondJSON(w, response)
}
