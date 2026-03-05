package main

import (
	"net/http"
)

// SearchHandlers contains HTTP handlers for unified search
type SearchHandlers struct {
	reader *UnifiedDuckDBReader
}

// NewSearchHandlers creates new search API handlers
func NewSearchHandlers(reader *UnifiedDuckDBReader) *SearchHandlers {
	return &SearchHandlers{reader: reader}
}

// HandleSearch performs a unified search across all data types
// @Summary Unified search
// @Description Detects query type (account, contract, transaction, ledger, asset) and returns categorized results
// @Tags Search
// @Produce json
// @Param q query string true "Search query (account address, contract ID, tx hash, ledger number, or asset code/name)"
// @Success 200 {object} SearchResults "Search results"
// @Failure 400 {object} map[string]interface{} "Missing query"
// @Failure 500 {object} map[string]interface{} "Internal server error"
// @Router /api/v1/silver/search [get]
func (h *SearchHandlers) HandleSearch(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query().Get("q")
	if q == "" {
		respondError(w, "q query parameter required", http.StatusBadRequest)
		return
	}

	results, err := h.reader.UnifiedSearch(r.Context(), q)
	if err != nil {
		respondError(w, err.Error(), http.StatusInternalServerError)
		return
	}

	respondJSON(w, results)
}
