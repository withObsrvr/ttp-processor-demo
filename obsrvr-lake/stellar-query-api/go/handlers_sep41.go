package main

import (
	"net/http"

	"github.com/gorilla/mux"
)

// SEP41Handlers contains HTTP handlers for SEP-41 token queries
type SEP41Handlers struct {
	reader *UnifiedDuckDBReader
}

// NewSEP41Handlers creates new SEP-41 token API handlers
func NewSEP41Handlers(reader *UnifiedDuckDBReader) *SEP41Handlers {
	return &SEP41Handlers{reader: reader}
}

// HandleTokenMetadata returns metadata for a SEP-41 token
// GET /api/v1/silver/tokens/{contract_id}
func (h *SEP41Handlers) HandleTokenMetadata(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	contractID := vars["contract_id"]
	if contractID == "" {
		respondError(w, "contract_id required", http.StatusBadRequest)
		return
	}

	meta, err := h.reader.GetSEP41TokenMetadata(r.Context(), contractID)
	if err != nil {
		respondError(w, err.Error(), http.StatusInternalServerError)
		return
	}

	respondJSON(w, meta)
}

// HandleTokenBalances returns computed balances for all holders of a token
// GET /api/v1/silver/tokens/{contract_id}/balances
func (h *SEP41Handlers) HandleTokenBalances(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	contractID := vars["contract_id"]
	if contractID == "" {
		respondError(w, "contract_id required", http.StatusBadRequest)
		return
	}

	filters := SEP41BalanceFilters{
		ContractID: contractID,
		Limit:      parseLimit(r, 20, 200),
		Order:      "desc",
	}

	if cursorStr := r.URL.Query().Get("cursor"); cursorStr != "" {
		cursor, err := DecodeSEP41BalanceCursor(cursorStr)
		if err != nil {
			respondError(w, "invalid cursor: "+err.Error(), http.StatusBadRequest)
			return
		}
		filters.Cursor = cursor
	}

	balances, nextCursor, hasMore, err := h.reader.GetSEP41Balances(r.Context(), filters)
	if err != nil {
		respondError(w, err.Error(), http.StatusInternalServerError)
		return
	}

	respondJSON(w, map[string]interface{}{
		"contract_id": contractID,
		"balances":    balances,
		"count":       len(balances),
		"has_more":    hasMore,
		"next_cursor": nextCursor,
	})
}

// HandleSingleBalance returns the balance of a specific address for a token
// GET /api/v1/silver/tokens/{contract_id}/balance/{address}
func (h *SEP41Handlers) HandleSingleBalance(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	contractID := vars["contract_id"]
	address := vars["address"]
	if contractID == "" || address == "" {
		respondError(w, "contract_id and address required", http.StatusBadRequest)
		return
	}

	balance, err := h.reader.GetSEP41SingleBalance(r.Context(), contractID, address)
	if err != nil {
		respondError(w, err.Error(), http.StatusInternalServerError)
		return
	}

	respondJSON(w, map[string]interface{}{
		"contract_id": contractID,
		"balance":     balance,
	})
}

// HandleTokenTransfers returns transfer history for a specific token
// GET /api/v1/silver/tokens/{contract_id}/transfers
func (h *SEP41Handlers) HandleTokenTransfers(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	contractID := vars["contract_id"]
	if contractID == "" {
		respondError(w, "contract_id required", http.StatusBadRequest)
		return
	}

	filters, err := parseEventFilters(r)
	if err != nil {
		respondError(w, err.Error(), http.StatusBadRequest)
		return
	}

	events, nextCursor, hasMore, err := h.reader.GetSEP41Transfers(r.Context(), contractID, filters)
	if err != nil {
		respondError(w, err.Error(), http.StatusInternalServerError)
		return
	}

	respondJSON(w, map[string]interface{}{
		"contract_id": contractID,
		"transfers":   events,
		"count":       len(events),
		"has_more":    hasMore,
		"next_cursor": nextCursor,
	})
}

// HandleTokenStats returns aggregate statistics for a token
// GET /api/v1/silver/tokens/{contract_id}/stats
func (h *SEP41Handlers) HandleTokenStats(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	contractID := vars["contract_id"]
	if contractID == "" {
		respondError(w, "contract_id required", http.StatusBadRequest)
		return
	}

	stats, err := h.reader.GetSEP41TokenStats(r.Context(), contractID)
	if err != nil {
		respondError(w, err.Error(), http.StatusInternalServerError)
		return
	}

	respondJSON(w, stats)
}

// HandleAddressTokenPortfolio returns all token holdings for an address
// GET /api/v1/silver/address/{addr}/token-balances
func (h *SEP41Handlers) HandleAddressTokenPortfolio(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	addr := vars["addr"]
	if addr == "" {
		respondError(w, "address required", http.StatusBadRequest)
		return
	}

	holdings, err := h.reader.GetAddressTokenPortfolio(r.Context(), addr)
	if err != nil {
		respondError(w, err.Error(), http.StatusInternalServerError)
		return
	}

	respondJSON(w, map[string]interface{}{
		"address":  addr,
		"holdings": holdings,
		"count":    len(holdings),
	})
}
