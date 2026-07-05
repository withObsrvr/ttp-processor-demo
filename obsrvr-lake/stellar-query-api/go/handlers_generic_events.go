package main

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/gorilla/mux"
)

// GenericEventHandlers contains HTTP handlers for generic CAP-67 contract events
type GenericEventHandlers struct {
	reader    *ColdReader
	hotReader *SilverHotReader
}

// NewGenericEventHandlers creates new generic event API handlers
func NewGenericEventHandlers(reader *ColdReader, hotReader *SilverHotReader) *GenericEventHandlers {
	return &GenericEventHandlers{reader: reader, hotReader: hotReader}
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
	filters, err := parseGenericEventFilters(r)
	if err != nil {
		respondError(w, err.Error(), http.StatusBadRequest)
		return
	}

	defaulted, err := h.defaultRecentWindow(r.Context(), &filters)
	if err != nil {
		respondError(w, err.Error(), http.StatusServiceUnavailable)
		return
	}

	if h.hotReader != nil {
		ctx, cancel := withInteractiveQueryTimeout(r.Context())
		events, nextCursor, hasMore, err := h.hotReader.GetServingGenericEvents(ctx, filters)
		cancel()
		if err == nil && len(events) > 0 {
			response := map[string]interface{}{
				"events":      events,
				"count":       len(events),
				"has_more":    hasMore,
				"next_cursor": nextCursor,
				"coverage":    genericEventCoverage(),
			}
			if defaulted {
				response["_meta"] = genericDefaultWindowMeta(filters)
			}
			respondJSON(w, response)
			return
		} else if err != nil && isQueryTimeout(err) {
			respondQueryTimeout(w, "generic events")
			return
		}
	}

	ctx, cancel := withInteractiveQueryTimeout(r.Context())
	defer cancel()
	events, nextCursor, hasMore, err := h.reader.GetGenericEvents(ctx, filters)
	if err != nil {
		if isQueryTimeout(err) {
			respondQueryTimeout(w, "generic events")
			return
		}
		respondError(w, err.Error(), http.StatusInternalServerError)
		return
	}

	response := map[string]interface{}{
		"events":      events,
		"count":       len(events),
		"has_more":    hasMore,
		"next_cursor": nextCursor,
		"coverage":    genericEventCoverage(),
	}
	if defaulted {
		response["_meta"] = genericDefaultWindowMeta(filters)
	}
	respondJSON(w, response)
}

func (h *GenericEventHandlers) defaultRecentWindow(ctx context.Context, filters *GenericEventFilters) (bool, error) {
	if filters == nil || filters.StartLedger != nil || filters.EndLedger != nil || filters.Cursor != nil {
		return false, nil
	}
	if h.hotReader == nil {
		return false, nil
	}
	lookupCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()
	latest, err := h.hotReader.GetServingLatestLedgerSequence(lookupCtx)
	if err != nil || latest <= 0 {
		if err != nil && isQueryTimeout(err) {
			return false, err
		}
		return false, nil
	}
	start, end := defaultLedgerWindow(latest)
	filters.StartLedger = &start
	filters.EndLedger = &end
	return true, nil
}

func genericDefaultWindowMeta(filters GenericEventFilters) map[string]interface{} {
	meta := map[string]interface{}{"default_recent_window": true}
	if filters.StartLedger != nil {
		meta["start_ledger"] = *filters.StartLedger
	}
	if filters.EndLedger != nil {
		meta["end_ledger"] = *filters.EndLedger
	}
	return meta
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

	filters, err := parseGenericEventFilters(r)
	if err != nil {
		respondError(w, err.Error(), http.StatusBadRequest)
		return
	}

	filters.ContractID = &contractID
	if h.hotReader != nil {
		ctx, cancel := withInteractiveQueryTimeout(r.Context())
		events, nextCursor, hasMore, err := h.hotReader.GetServingGenericEvents(ctx, filters)
		cancel()
		if err == nil && len(events) > 0 {
			respondJSON(w, map[string]interface{}{
				"contract_id": contractID,
				"events":      events,
				"count":       len(events),
				"has_more":    hasMore,
				"next_cursor": nextCursor,
				"coverage":    genericEventCoverage(),
			})
			return
		} else if err != nil && isQueryTimeout(err) {
			respondQueryTimeout(w, "contract generic events")
			return
		}
	}

	ctx, cancel := withInteractiveQueryTimeout(r.Context())
	defer cancel()
	events, nextCursor, hasMore, err := h.reader.GetContractGenericEvents(ctx, contractID, filters)
	if err != nil {
		if isQueryTimeout(err) {
			respondQueryTimeout(w, "contract generic events")
			return
		}
		respondError(w, err.Error(), http.StatusInternalServerError)
		return
	}

	respondJSON(w, map[string]interface{}{
		"contract_id": contractID,
		"events":      events,
		"count":       len(events),
		"has_more":    hasMore,
		"next_cursor": nextCursor,
		"coverage":    genericEventCoverage(),
	})
}

func parseGenericEventFilters(r *http.Request) (GenericEventFilters, error) {
	filters := GenericEventFilters{
		Limit: parseLimit(r, 20, 200),
		Order: "desc",
	}

	if order := r.URL.Query().Get("order"); order != "" {
		if order != "asc" && order != "desc" {
			return filters, fmt.Errorf("order must be asc or desc")
		}
		filters.Order = order
	}

	if v := r.URL.Query().Get("contract_id"); v != "" {
		filters.ContractID = &v
	}
	if v := r.URL.Query().Get("tx_hash"); v != "" {
		filters.TxHash = &v
	}
	if v := r.URL.Query().Get("event_type"); v != "" {
		switch v {
		case "contract", "system", "diagnostic":
		default:
			return filters, fmt.Errorf("event_type must be contract, system, or diagnostic")
		}
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
		val, err := strconv.ParseInt(v, 10, 64)
		if err != nil || val <= 0 {
			return filters, fmt.Errorf("invalid start_ledger")
		}
		filters.StartLedger = &val
	}
	if v := r.URL.Query().Get("end_ledger"); v != "" {
		val, err := strconv.ParseInt(v, 10, 64)
		if err != nil || val <= 0 {
			return filters, fmt.Errorf("invalid end_ledger")
		}
		filters.EndLedger = &val
	}
	if filters.StartLedger != nil && filters.EndLedger != nil && *filters.StartLedger > *filters.EndLedger {
		return filters, fmt.Errorf("start_ledger must be <= end_ledger")
	}
	if v := r.URL.Query().Get("cursor"); v != "" {
		filters.Cursor = &v
	}

	return filters, nil
}
