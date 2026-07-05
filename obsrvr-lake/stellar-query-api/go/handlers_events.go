package main

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/gorilla/mux"
)

// EventHandlers contains HTTP handlers for CAP-67 unified event stream queries
type EventHandlers struct {
	reader        *SilverColdReader
	hotPathReader *TxHotPathReader
	hotReader     *SilverHotReader
}

type EventAPICoverage struct {
	Version     string   `json:"version"`
	Includes    []string `json:"includes"`
	Limitations []string `json:"limitations"`
}

func unifiedEventCoverage() EventAPICoverage {
	return EventAPICoverage{
		Version: "cap67_transfer_events_v1",
		Includes: []string{
			"CAP-67-like transfer, mint, and burn events derived from token_transfers_raw",
			"classic Stellar payments/path payments and decoded Soroban token transfers where present in token_transfers_raw",
			"filters for contract_id, event_type, source_type, address, transaction, ledger range, order, and cursor pagination",
		},
		Limitations: []string{
			"this endpoint is a semantic token-transfer stream, not the complete raw Soroban event stream",
			"non-transfer contract events, diagnostic events, and raw topic/data payloads are exposed through /api/v1/silver/events/generic",
			"amounts are raw token units; callers should use token metadata for display decimals",
		},
	}
}

func genericEventCoverage() EventAPICoverage {
	return EventAPICoverage{
		Version: "raw_contract_events_v1",
		Includes: []string{
			"raw Soroban contract_events_stream_v1 rows with decoded topic/data fields where available",
			"contract, system, and diagnostic event filters",
			"topic0-topic3 exact filters, topic substring search, transaction hash, ledger range, order, and cursor pagination",
		},
		Limitations: []string{
			"raw generic events are not guaranteed to be classified into business actions",
			"topic_match is substring search over decoded topics and should not be treated as complete address extraction",
			"for Prism-style semantic classification use /api/v1/explorer/events",
		},
	}
}

// NewEventHandlers creates new CAP-67 event API handlers
func NewEventHandlers(reader *SilverColdReader, hotPathReader *TxHotPathReader, hotReader *SilverHotReader) *EventHandlers {
	return &EventHandlers{reader: reader, hotPathReader: hotPathReader, hotReader: hotReader}
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

	defaulted, err := h.defaultRecentWindow(r.Context(), &filters)
	if err != nil {
		respondError(w, err.Error(), http.StatusServiceUnavailable)
		return
	}

	ctx, cancel := withInteractiveQueryTimeout(r.Context())
	defer cancel()
	events, nextCursor, hasMore, err := h.reader.GetUnifiedEvents(ctx, filters)
	if err != nil {
		if isQueryTimeout(err) {
			respondQueryTimeout(w, "events")
			return
		}
		respondError(w, err.Error(), http.StatusInternalServerError)
		return
	}

	coverage := unifiedEventCoverage()
	response := map[string]interface{}{
		"events":      events,
		"count":       len(events),
		"has_more":    hasMore,
		"next_cursor": nextCursor,
		"coverage":    coverage,
	}
	if defaulted {
		response["_meta"] = map[string]interface{}{
			"default_recent_window": true,
			"start_ledger":          filters.StartLedger,
			"end_ledger":            filters.EndLedger,
		}
	}

	respondJSON(w, response)
}

func (h *EventHandlers) defaultRecentWindow(ctx context.Context, filters *UnifiedEventFilters) (bool, error) {
	if filters == nil || filters.StartLedger > 0 || filters.EndLedger > 0 || filters.Cursor != nil {
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
	filters.StartLedger = start
	filters.EndLedger = end
	return true, nil
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

	ctx, cancel := withInteractiveQueryTimeout(r.Context())
	defer cancel()
	events, nextCursor, hasMore, err := h.reader.GetUnifiedEvents(ctx, filters)
	if err != nil {
		if isQueryTimeout(err) {
			respondQueryTimeout(w, "contract events")
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
		"coverage":    unifiedEventCoverage(),
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
// @Failure 400 {object} map[string]interface{} "Invalid address or parameters"
// @Failure 500 {object} map[string]interface{} "Internal server error"
// @Router /api/v1/silver/address/{addr}/events [get]
func (h *EventHandlers) HandleAddressEvents(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	addr := vars["addr"]
	if !isValidStellarAddress(addr) {
		respondError(w, "addr must be a valid Stellar account (G...) or contract (C...) address", http.StatusBadRequest)
		return
	}

	filters, err := parseEventFilters(r)
	if err != nil {
		respondError(w, err.Error(), http.StatusBadRequest)
		return
	}

	ctx, cancel := withInteractiveQueryTimeout(r.Context())
	defer cancel()
	events, nextCursor, hasMore, err := h.reader.GetAddressEvents(ctx, addr, filters)
	if err != nil {
		if isQueryTimeout(err) {
			respondQueryTimeout(w, "address events")
			return
		}
		respondError(w, err.Error(), http.StatusInternalServerError)
		return
	}

	respondJSON(w, map[string]interface{}{
		"address":     addr,
		"events":      events,
		"count":       len(events),
		"has_more":    hasMore,
		"next_cursor": nextCursor,
		"coverage":    unifiedEventCoverage(),
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
		hotCtx, cancel := context.WithTimeout(r.Context(), 1200*time.Millisecond)
		events, err = h.hotPathReader.GetTransactionEvents(hotCtx, txHash)
		cancel()
	}
	if err != nil || len(events) == 0 {
		coldCtx, cancel := withInteractiveQueryTimeout(r.Context())
		events, err = h.reader.GetTransactionEvents(coldCtx, txHash)
		cancel()
	}
	if err != nil {
		if isQueryTimeout(err) {
			respondQueryTimeout(w, "transaction events")
			return
		}
		respondError(w, err.Error(), http.StatusInternalServerError)
		return
	}

	respondJSON(w, map[string]interface{}{
		"transaction_hash": txHash,
		"events":           events,
		"count":            len(events),
		"coverage":         unifiedEventCoverage(),
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

	if order := r.URL.Query().Get("order"); order != "" {
		if order != "asc" && order != "desc" {
			return filters, fmt.Errorf("order must be asc or desc")
		}
		filters.Order = order
	}

	if filters.EventType != "" {
		switch filters.EventType {
		case "transfer", "mint", "burn":
		default:
			return filters, fmt.Errorf("event_type must be transfer, mint, or burn")
		}
	}
	if filters.SourceType != "" {
		switch filters.SourceType {
		case "classic", "soroban":
		default:
			return filters, fmt.Errorf("source_type must be classic or soroban")
		}
	}

	if startLedger := r.URL.Query().Get("start_ledger"); startLedger != "" {
		val, err := strconv.ParseInt(startLedger, 10, 64)
		if err != nil || val <= 0 {
			return filters, fmt.Errorf("invalid start_ledger")
		}
		filters.StartLedger = val
	}

	if endLedger := r.URL.Query().Get("end_ledger"); endLedger != "" {
		val, err := strconv.ParseInt(endLedger, 10, 64)
		if err != nil || val <= 0 {
			return filters, fmt.Errorf("invalid end_ledger")
		}
		filters.EndLedger = val
	}

	if filters.StartLedger > 0 && filters.EndLedger > 0 && filters.StartLedger > filters.EndLedger {
		return filters, fmt.Errorf("start_ledger must be <= end_ledger")
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
