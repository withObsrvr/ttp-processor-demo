package main

import (
	"net/http"

	"github.com/gorilla/mux"
)

// SEP41Handlers contains HTTP handlers for SEP-41 token queries
type SEP41Handlers struct {
	reader *SilverColdReader
}

// NewSEP41Handlers creates new SEP-41 token API handlers
func NewSEP41Handlers(reader *SilverColdReader) *SEP41Handlers {
	return &SEP41Handlers{reader: reader}
}

// HandleTokenMetadata returns metadata for a SEP-41 token
// @Summary Get SEP-41 token metadata
// @Description Returns metadata for a SEP-41 token including asset code, holder count, and transfer count
// @Tags Tokens
// @Accept json
// @Produce json
// @Param contract_id path string true "Token contract ID (C...)"
// @Success 200 {object} SEP41TokenMetadata "Token metadata"
// @Failure 400 {object} map[string]interface{} "Missing contract_id"
// @Failure 500 {object} map[string]interface{} "Internal server error"
// @Router /api/v1/silver/tokens/{contract_id} [get]
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
// @Summary Get SEP-41 token holder balances
// @Description Returns computed balances for all holders of a token, derived from transfer history (received minus sent), ranked by balance
// @Tags Tokens
// @Accept json
// @Produce json
// @Param contract_id path string true "Token contract ID (C...)"
// @Param limit query int false "Max results (default: 20, max: 200)" default(20)
// @Param cursor query string false "Pagination cursor"
// @Success 200 {object} map[string]interface{} "Token balances with pagination"
// @Failure 400 {object} map[string]interface{} "Missing contract_id or invalid cursor"
// @Failure 500 {object} map[string]interface{} "Internal server error"
// @Router /api/v1/silver/tokens/{contract_id}/balances [get]
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
// @Summary Get single address balance for a token
// @Description Returns the computed balance (received minus sent) of a specific address for a SEP-41 token
// @Tags Tokens
// @Accept json
// @Produce json
// @Param contract_id path string true "Token contract ID (C...)"
// @Param address path string true "Holder address (G... or C...)"
// @Success 200 {object} map[string]interface{} "Single address balance"
// @Failure 400 {object} map[string]interface{} "Missing contract_id or address"
// @Failure 500 {object} map[string]interface{} "Internal server error"
// @Router /api/v1/silver/tokens/{contract_id}/balance/{address} [get]
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
// @Summary Get SEP-41 token transfer history
// @Description Returns transfer history (transfers, mints, burns) for a specific SEP-41 token
// @Tags Tokens
// @Accept json
// @Produce json
// @Param contract_id path string true "Token contract ID (C...)"
// @Param event_type query string false "Filter by type: transfer, mint, burn"
// @Param limit query int false "Max results (default: 20, max: 200)" default(20)
// @Param cursor query string false "Pagination cursor"
// @Param order query string false "Sort order" default(desc) Enums(asc, desc)
// @Success 200 {object} map[string]interface{} "Token transfers with pagination"
// @Failure 400 {object} map[string]interface{} "Missing contract_id"
// @Failure 500 {object} map[string]interface{} "Internal server error"
// @Router /api/v1/silver/tokens/{contract_id}/transfers [get]
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
// @Summary Get SEP-41 token statistics
// @Description Returns aggregate statistics for a token including holder count, total supply, 24h transfer count and volume
// @Tags Tokens
// @Accept json
// @Produce json
// @Param contract_id path string true "Token contract ID (C...)"
// @Success 200 {object} SEP41TokenStats "Token statistics"
// @Failure 400 {object} map[string]interface{} "Missing contract_id"
// @Failure 500 {object} map[string]interface{} "Internal server error"
// @Router /api/v1/silver/tokens/{contract_id}/stats [get]
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
// @Summary Get address token portfolio
// @Description Returns all SEP-41 token holdings for an address across all tokens, with computed balances from transfer history
// @Tags Tokens
// @Accept json
// @Produce json
// @Param addr path string true "Stellar account or contract address"
// @Success 200 {object} map[string]interface{} "Token holdings for the address"
// @Failure 400 {object} map[string]interface{} "Missing address"
// @Failure 500 {object} map[string]interface{} "Internal server error"
// @Router /api/v1/silver/address/{addr}/token-balances [get]
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
