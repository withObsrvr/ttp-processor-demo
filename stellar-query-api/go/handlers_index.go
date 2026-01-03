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
// GET /transactions/{hash}
// GET /api/v1/index/transactions/{hash}
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
// POST /api/v1/index/transactions/lookup
// Body: {"hashes": ["abc123", "def456", ...]}
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
// GET /api/v1/index/health
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
