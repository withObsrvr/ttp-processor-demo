package main

import (
	"net/http"
	"strconv"

	"github.com/gorilla/mux"
)

// EventHandlers contains HTTP handlers for CAP-67 unified event stream queries
type EventHandlers struct {
	reader *UnifiedDuckDBReader
}

// NewEventHandlers creates new CAP-67 event API handlers
func NewEventHandlers(reader *UnifiedDuckDBReader) *EventHandlers {
	return &EventHandlers{reader: reader}
}

// HandleUnifiedEvents returns the unified CAP-67 event stream with filters
// GET /api/v1/silver/events
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
// GET /api/v1/silver/events/by-contract
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
// GET /api/v1/silver/address/{addr}/events
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
// GET /api/v1/silver/tx/{hash}/events
func (h *EventHandlers) HandleTransactionEvents(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	txHash := vars["hash"]
	if txHash == "" {
		respondError(w, "transaction hash required", http.StatusBadRequest)
		return
	}

	events, err := h.reader.GetTransactionEvents(r.Context(), txHash)
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
