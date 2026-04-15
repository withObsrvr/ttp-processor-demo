package main

import (
	"net/http"
	"strconv"

	"github.com/gorilla/mux"
)

// EventHandlers contains HTTP handlers for CAP-67 unified event stream queries
type EventHandlers struct {
	reader        *SilverColdReader
	hotPathReader *TxHotPathReader
}

// NewEventHandlers creates new CAP-67 event API handlers
func NewEventHandlers(reader *SilverColdReader, hotPathReader *TxHotPathReader) *EventHandlers {
	return &EventHandlers{reader: reader, hotPathReader: hotPathReader}
}

// HandleUnifiedEvents returns the unified CAP-67 event stream with filters
// @Summary Get unified CAP-67 event stream
// @Description Returns the unified CAP-67 event stream with optional filters for contract, event type, source type, and ledger range. Events are derived from token_transfers_raw with type inferred from from/to nullity.
// @Tags Events
// @Accept json
// @Produce json
// @Param contract_id query string false "Filter by token contract ID (C...)"
// @Param event_type query string false "Filter by event type: transfer, mint, burn"
// @Param source_type query string false "Filter by source: classic or soroban"
// @Param start_ledger query int false "Start of ledger range"
// @Param end_ledger query int false "End of ledger range"
// @Param limit query int false "Max results (default: 20, max: 200)" default(20)
// @Param cursor query string false "Pagination cursor from previous response"
// @Param order query string false "Sort order" default(desc) Enums(asc, desc)
// @Success 200 {object} map[string]interface{} "Unified events with pagination"
// @Failure 400 {object} map[string]interface{} "Invalid parameters"
// @Failure 500 {object} map[string]interface{} "Internal server error"
// @Router /api/v1/silver/events [get]
func (h *EventHandlers) HandleUnifiedEvents(w http.ResponseWriter, r *http.Request) {
	filters, err := parseEventFilters(r)
	if err != nil {
		respondError(w, err.Error(), http.StatusBadRequest)
		return
	}

	events, nextCursor, hasMore, err := h.reader.GetUnifiedEvents(r.Context(), filters)
	if err != nil {
		respondError(w, err.Error(), http.StatusInternalServerError)
		return
	}

	respondJSON(w, map[string]interface{}{
		"events":      events,
		"count":       len(events),
		"has_more":    hasMore,
		"next_cursor": nextCursor,
	})
}

// HandleContractEvents returns events for a specific contract
// @Summary Get events by contract
// @Description Returns CAP-67 events filtered to a specific token contract
// @Tags Events
// @Accept json
// @Produce json
// @Param contract_id query string true "Token contract ID (C...)"
// @Param event_type query string false "Filter by event type: transfer, mint, burn"
// @Param source_type query string false "Filter by source: classic or soroban"
// @Param limit query int false "Max results (default: 20, max: 200)" default(20)
// @Param cursor query string false "Pagination cursor"
// @Param order query string false "Sort order" default(desc) Enums(asc, desc)
// @Success 200 {object} map[string]interface{} "Contract events with pagination"
// @Failure 400 {object} map[string]interface{} "Missing contract_id"
// @Failure 500 {object} map[string]interface{} "Internal server error"
// @Router /api/v1/silver/events/by-contract [get]
func (h *EventHandlers) HandleContractEvents(w http.ResponseWriter, r *http.Request) {
	contractID := r.URL.Query().Get("contract_id")
	if contractID == "" {
		respondError(w, "contract_id query parameter required", http.StatusBadRequest)
		return
	}

	filters, err := parseEventFilters(r)
	if err != nil {
		respondError(w, err.Error(), http.StatusBadRequest)
		return
	}
	filters.ContractID = contractID

	events, nextCursor, hasMore, err := h.reader.GetUnifiedEvents(r.Context(), filters)
	if err != nil {
		respondError(w, err.Error(), http.StatusInternalServerError)
		return
	}

	respondJSON(w, map[string]interface{}{
		"contract_id": contractID,
		"events":      events,
		"count":       len(events),
		"has_more":    hasMore,
		"next_cursor": nextCursor,
	})
}

// HandleAddressEvents returns events where the address is sender or receiver
// @Summary Get events for an address
// @Description Returns CAP-67 events where the address appears as sender (from) or receiver (to)
// @Tags Events
// @Accept json
// @Produce json
// @Param addr path string true "Stellar account or contract address"
// @Param event_type query string false "Filter by event type: transfer, mint, burn"
// @Param source_type query string false "Filter by source: classic or soroban"
// @Param limit query int false "Max results (default: 20, max: 200)" default(20)
// @Param cursor query string false "Pagination cursor"
// @Param order query string false "Sort order" default(desc) Enums(asc, desc)
// @Success 200 {object} map[string]interface{} "Address events with pagination"
// @Failure 400 {object} map[string]interface{} "Missing address"
// @Failure 500 {object} map[string]interface{} "Internal server error"
// @Router /api/v1/silver/address/{addr}/events [get]
func (h *EventHandlers) HandleAddressEvents(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	addr := vars["addr"]
	if addr == "" {
		respondError(w, "address required", http.StatusBadRequest)
		return
	}

	filters, err := parseEventFilters(r)
	if err != nil {
		respondError(w, err.Error(), http.StatusBadRequest)
		return
	}

	events, nextCursor, hasMore, err := h.reader.GetAddressEvents(r.Context(), addr, filters)
	if err != nil {
		respondError(w, err.Error(), http.StatusInternalServerError)
		return
	}

	respondJSON(w, map[string]interface{}{
		"address":     addr,
		"events":      events,
		"count":       len(events),
		"has_more":    hasMore,
		"next_cursor": nextCursor,
	})
}

// HandleTransactionEvents returns all events for a specific transaction
// @Summary Get events for a transaction
// @Description Returns all CAP-67 events (transfers, mints, burns) for a specific transaction
// @Tags Events
// @Accept json
// @Produce json
// @Param hash path string true "Transaction hash"
// @Success 200 {object} map[string]interface{} "Transaction events"
// @Failure 400 {object} map[string]interface{} "Missing transaction hash"
// @Failure 500 {object} map[string]interface{} "Internal server error"
// @Router /api/v1/silver/tx/{hash}/events [get]
func (h *EventHandlers) HandleTransactionEvents(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	txHash := vars["hash"]
	if txHash == "" {
		respondError(w, "transaction hash required", http.StatusBadRequest)
		return
	}

	var events []UnifiedEvent
	var err error
	if h.hotPathReader != nil {
		events, err = h.hotPathReader.GetTransactionEvents(r.Context(), txHash)
	}
	if err != nil || events == nil {
		events, err = h.reader.GetTransactionEvents(r.Context(), txHash)
	}
	if err != nil {
		respondError(w, err.Error(), http.StatusInternalServerError)
		return
	}

	respondJSON(w, map[string]interface{}{
		"transaction_hash": txHash,
		"events":           events,
		"count":            len(events),
	})
}

// parseEventFilters extracts event filter parameters from the HTTP request
func parseEventFilters(r *http.Request) (UnifiedEventFilters, error) {
	filters := UnifiedEventFilters{
		ContractID: r.URL.Query().Get("contract_id"),
		EventType:  r.URL.Query().Get("event_type"),
		SourceType: r.URL.Query().Get("source_type"),
		Limit:      parseLimit(r, 20, 200),
		Order:      "desc",
	}

	if order := r.URL.Query().Get("order"); order == "asc" {
		filters.Order = "asc"
	}

	if startLedger := r.URL.Query().Get("start_ledger"); startLedger != "" {
		val, err := strconv.ParseInt(startLedger, 10, 64)
		if err != nil {
			return filters, err
		}
		filters.StartLedger = val
	}

	if endLedger := r.URL.Query().Get("end_ledger"); endLedger != "" {
		val, err := strconv.ParseInt(endLedger, 10, 64)
		if err != nil {
			return filters, err
		}
		filters.EndLedger = val
	}

	if cursorStr := r.URL.Query().Get("cursor"); cursorStr != "" {
		cursor, err := DecodeUnifiedEventCursor(cursorStr)
		if err != nil {
			return filters, err
		}
		filters.Cursor = cursor
	}

	return filters, nil
}
