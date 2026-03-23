package main

import (
	"net/http"
	"strconv"

	"github.com/gorilla/mux"
)

// GenericEventHandlers contains HTTP handlers for generic CAP-67 contract events
type GenericEventHandlers struct {
	reader *UnifiedDuckDBReader
}

// NewGenericEventHandlers creates new generic event API handlers
func NewGenericEventHandlers(reader *UnifiedDuckDBReader) *GenericEventHandlers {
	return &GenericEventHandlers{reader: reader}
}

// HandleGenericEvents returns all contract events with filters
// @Summary Get generic contract events
// @Description Returns raw contract events from bronze layer with optional filters
// @Tags Events
// @Produce json
// @Param contract_id query string false "Filter by contract ID (64-character hex contract hash)"
// @Param event_type query string false "Filter by event type: contract, system, diagnostic"
// @Param topic_match query string false "Search within topics_decoded (ILIKE substring match)"
// @Param topic0 query string false "Exact match on topic position 0 (e.g. transfer)"
// @Param topic1 query string false "Exact match on topic position 1 (e.g. sender address)"
// @Param topic2 query string false "Exact match on topic position 2 (e.g. receiver address)"
// @Param topic3 query string false "Exact match on topic position 3"
// @Param start_ledger query int false "Start of ledger range"
// @Param end_ledger query int false "End of ledger range"
// @Param limit query int false "Max results (default: 20, max: 200)" default(20)
// @Param cursor query string false "Pagination cursor"
// @Param order query string false "Sort order" default(desc) Enums(asc, desc)
// @Success 200 {object} map[string]interface{} "Generic events with pagination"
// @Failure 500 {object} map[string]interface{} "Internal server error"
// @Router /api/v1/silver/events/generic [get]
func (h *GenericEventHandlers) HandleGenericEvents(w http.ResponseWriter, r *http.Request) {
	filters := parseGenericEventFilters(r)

	events, nextCursor, hasMore, err := h.reader.GetGenericEvents(r.Context(), filters)
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

// HandleContractGenericEvents returns events for a specific contract
// @Summary Get contract events by contract ID
// @Description Returns raw contract events for a specific contract from bronze layer
// @Tags Events
// @Produce json
// @Param contract_id path string true "Contract ID (hex format)"
// @Param event_type query string false "Filter by event type"
// @Param limit query int false "Max results (default: 20, max: 200)" default(20)
// @Param cursor query string false "Pagination cursor"
// @Param order query string false "Sort order" default(desc) Enums(asc, desc)
// @Success 200 {object} map[string]interface{} "Contract events with pagination"
// @Failure 400 {object} map[string]interface{} "Missing contract_id"
// @Failure 500 {object} map[string]interface{} "Internal server error"
// @Router /api/v1/silver/events/contract/{contract_id} [get]
func (h *GenericEventHandlers) HandleContractGenericEvents(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	contractID := vars["contract_id"]
	if contractID == "" {
		respondError(w, "contract_id required", http.StatusBadRequest)
		return
	}

	filters := parseGenericEventFilters(r)

	events, nextCursor, hasMore, err := h.reader.GetContractGenericEvents(r.Context(), contractID, filters)
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

func parseGenericEventFilters(r *http.Request) GenericEventFilters {
	filters := GenericEventFilters{
		Limit: parseLimit(r, 20, 200),
		Order: "desc",
	}

	if order := r.URL.Query().Get("order"); order == "asc" {
		filters.Order = "asc"
	}

	if v := r.URL.Query().Get("contract_id"); v != "" {
		filters.ContractID = &v
	}
	if v := r.URL.Query().Get("event_type"); v != "" {
		filters.EventType = &v
	}
	if v := r.URL.Query().Get("topic_match"); v != "" {
		filters.TopicMatch = &v
	}
	if v := r.URL.Query().Get("topic0"); v != "" {
		filters.Topic0 = &v
	}
	if v := r.URL.Query().Get("topic1"); v != "" {
		filters.Topic1 = &v
	}
	if v := r.URL.Query().Get("topic2"); v != "" {
		filters.Topic2 = &v
	}
	if v := r.URL.Query().Get("topic3"); v != "" {
		filters.Topic3 = &v
	}
	if v := r.URL.Query().Get("start_ledger"); v != "" {
		if val, err := strconv.ParseInt(v, 10, 64); err == nil {
			filters.StartLedger = &val
		}
	}
	if v := r.URL.Query().Get("end_ledger"); v != "" {
		if val, err := strconv.ParseInt(v, 10, 64); err == nil {
			filters.EndLedger = &val
		}
	}
	if v := r.URL.Query().Get("cursor"); v != "" {
		filters.Cursor = &v
	}

	return filters
}
