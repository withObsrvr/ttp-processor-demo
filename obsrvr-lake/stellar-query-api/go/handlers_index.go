package main

import (
	"encoding/json"
	"net/http"
	"strings"
)

// IndexHandlers contains HTTP handlers for Index Plane queries
type IndexHandlers struct {
	reader *IndexReader
}

// NewIndexHandlers creates new Index API handlers
func NewIndexHandlers(reader *IndexReader) *IndexHandlers {
	return &IndexHandlers{reader: reader}
}

// HandleTransactionLookup performs fast transaction hash lookup
// @Summary Lookup transaction by hash
// @Description Fast O(1) lookup of transaction location by hash using the Index Plane
// @Tags Index
// @Accept json
// @Produce json
// @Param hash path string true "Transaction hash"
// @Success 200 {object} TxLocation "Transaction location data"
// @Failure 400 {object} map[string]interface{} "Invalid parameters"
// @Failure 404 {object} map[string]interface{} "Transaction not found"
// @Failure 500 {object} map[string]interface{} "Internal server error"
// @Router /api/v1/index/transactions/{hash} [get]
func (h *IndexHandlers) HandleTransactionLookup(w http.ResponseWriter, r *http.Request) {
	// Extract hash from URL path
	path := r.URL.Path
	var txHash string

	// Support both /transactions/{hash} and /api/v1/index/transactions/{hash}
	if strings.HasPrefix(path, "/api/v1/index/transactions/") {
		txHash = strings.TrimPrefix(path, "/api/v1/index/transactions/")
	} else if strings.HasPrefix(path, "/transactions/") {
		txHash = strings.TrimPrefix(path, "/transactions/")
	} else {
		respondError(w, "invalid path", http.StatusBadRequest)
		return
	}

	// Remove any trailing slash
	txHash = strings.TrimSuffix(txHash, "/")

	if txHash == "" {
		respondError(w, "transaction hash required", http.StatusBadRequest)
		return
	}

	// Lookup in Index Plane
	location, err := h.reader.LookupTransactionHash(r.Context(), txHash)
	if err != nil {
		respondError(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if location == nil {
		respondError(w, "transaction not found in index", http.StatusNotFound)
		return
	}

	respondJSON(w, location)
}

// HandleBatchTransactionLookup performs batch lookup for multiple transaction hashes
// @Summary Batch lookup transactions by hash
// @Description Lookup up to 1000 transaction hashes in a single request
// @Tags Index
// @Accept json
// @Produce json
// @Param request body object true "Request body with hashes array" example({"hashes": ["abc123", "def456"]})
// @Success 200 {object} map[string]interface{} "Batch lookup results with found/total counts"
// @Failure 400 {object} map[string]interface{} "Invalid parameters"
// @Failure 405 {object} map[string]interface{} "Method not allowed (POST required)"
// @Failure 500 {object} map[string]interface{} "Internal server error"
// @Router /api/v1/index/transactions/lookup [post]
func (h *IndexHandlers) HandleBatchTransactionLookup(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		respondError(w, "POST required", http.StatusMethodNotAllowed)
		return
	}

	var request struct {
		Hashes []string `json:"hashes"`
	}

	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		respondError(w, "invalid JSON body", http.StatusBadRequest)
		return
	}

	if len(request.Hashes) == 0 {
		respondError(w, "hashes array required", http.StatusBadRequest)
		return
	}

	if len(request.Hashes) > 1000 {
		respondError(w, "maximum 1000 hashes per request", http.StatusBadRequest)
		return
	}

	// Batch lookup
	locations, err := h.reader.LookupTransactionHashes(r.Context(), request.Hashes)
	if err != nil {
		respondError(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Build results map for easy lookup
	resultsMap := make(map[string]*TxLocation)
	for i := range locations {
		resultsMap[locations[i].TxHash] = &locations[i]
	}

	// Build response with all requested hashes
	type Result struct {
		TxHash   string       `json:"tx_hash"`
		Location *TxLocation  `json:"location,omitempty"`
		Error    *string      `json:"error,omitempty"`
	}

	results := make([]Result, len(request.Hashes))
	notFound := "not found"

	for i, hash := range request.Hashes {
		if location, ok := resultsMap[hash]; ok {
			results[i] = Result{
				TxHash:   hash,
				Location: location,
			}
		} else {
			results[i] = Result{
				TxHash: hash,
				Error:  &notFound,
			}
		}
	}

	respondJSON(w, map[string]interface{}{
		"results": results,
		"found":   len(locations),
		"total":   len(request.Hashes),
	})
}

// HandleIndexHealth returns Index Plane health and coverage statistics
// @Summary Get Index Plane health
// @Description Returns Index Plane health status and coverage statistics
// @Tags Index
// @Accept json
// @Produce json
// @Success 200 {object} map[string]interface{} "Index health status and statistics"
// @Failure 500 {object} map[string]interface{} "Internal server error"
// @Router /api/v1/index/health [get]
func (h *IndexHandlers) HandleIndexHealth(w http.ResponseWriter, r *http.Request) {
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
