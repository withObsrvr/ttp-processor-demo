package main

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"strings"
)

// ExplorerEventHandlers contains HTTP handlers for the Prism explorer events endpoint
type ExplorerEventHandlers struct {
	reader     *UnifiedDuckDBReader
	classifier *EventClassifier
}

// NewExplorerEventHandlers creates new explorer event API handlers
func NewExplorerEventHandlers(reader *UnifiedDuckDBReader, classifier *EventClassifier) *ExplorerEventHandlers {
	return &ExplorerEventHandlers{reader: reader, classifier: classifier}
}

// tabToTypes maps UI tab shortcuts to type filter values
var tabToTypes = map[string][]string{
	"transfers":      {"transfer"},
	"swaps":          {"swap"},
	"mints_burns":    {"mint", "burn"},
	"contract_calls": {"contract_call"},
}

// HandleExplorerEvents returns enriched contract events for the Prism block explorer
// @Summary Get explorer events
// @Description Returns paginated Soroban contract events enriched with contract names and semantic type classification
// @Tags Explorer
// @Produce json
// @Param type query string false "Comma-separated event types (dynamically derived from classification rules)"
// @Param tab query string false "UI tab shortcut: transfers, swaps, mints_burns, contract_calls"
// @Param contract_id query string false "Filter by contract ID (C... address)"
// @Param contract_name query string false "Search by contract/token name (case-insensitive substring)"
// @Param tx_hash query string false "Filter by transaction hash"
// @Param topic_match query string false "Substring match across all decoded topics (case-insensitive)"
// @Param topic0 query string false "Exact match on topic position 0"
// @Param topic1 query string false "Exact match on topic position 1"
// @Param topic2 query string false "Exact match on topic position 2"
// @Param topic3 query string false "Exact match on topic position 3"
// @Param start_ledger query int false "Start of ledger range"
// @Param end_ledger query int false "End of ledger range"
// @Param limit query int false "Max results (default: 20, max: 200)"
// @Param cursor query string false "Pagination cursor from previous response"
// @Param order query string false "Sort order: asc or desc (default: desc)"
// @Success 200 {object} map[string]interface{} "Explorer events with metadata and pagination"
// @Failure 400 {object} map[string]interface{} "Invalid parameters"
// @Failure 500 {object} map[string]interface{} "Internal server error"
// @Router /api/v1/explorer/events [get]
func (h *ExplorerEventHandlers) HandleExplorerEvents(w http.ResponseWriter, r *http.Request) {
	filters, err := h.parseExplorerEventFilters(r)
	if err != nil {
		respondError(w, err.Error(), http.StatusBadRequest)
		return
	}

	events, meta, nextCursor, hasMore, err := h.reader.GetExplorerEvents(r.Context(), filters, h.classifier)
	if err != nil {
		respondError(w, err.Error(), http.StatusInternalServerError)
		return
	}

	response := map[string]interface{}{
		"meta":     meta,
		"events":   events,
		"count":    len(events),
		"has_more": hasMore,
	}
	if nextCursor != "" {
		response["next_cursor"] = nextCursor
	}

	respondJSON(w, response)
}

// HandleExplorerEventRules returns the current classification rules
// @Summary Get event classification rules
// @Description Returns the currently loaded classification rules
// @Tags Explorer
// @Produce json
// @Success 200 {object} map[string]interface{} "Classification rules"
// @Router /api/v1/explorer/events/rules [get]
func (h *ExplorerEventHandlers) HandleExplorerEventRules(w http.ResponseWriter, r *http.Request) {
	respondJSON(w, map[string]interface{}{
		"rules": h.classifier.Rules(),
		"count": len(h.classifier.Rules()),
	})
}

// HandleExplorerEventRulesReload hot-reloads classification rules from the database
// @Summary Reload event classification rules
// @Description Reloads classification rules from the database without restarting
// @Tags Explorer
// @Produce json
// @Success 200 {object} map[string]interface{} "Reload result"
// @Failure 500 {object} map[string]interface{} "Reload failed"
// @Router /api/v1/explorer/events/rules/reload [post]
func (h *ExplorerEventHandlers) HandleExplorerEventRulesReload(w http.ResponseWriter, r *http.Request) {
	if err := h.classifier.LoadRules(context.Background()); err != nil {
		respondError(w, "failed to reload rules: "+err.Error(), http.StatusInternalServerError)
		return
	}

	rules := h.classifier.Rules()
	respondJSON(w, map[string]interface{}{
		"status": "reloaded",
		"count":  len(rules),
		"rules":  rules,
	})
}

func (h *ExplorerEventHandlers) parseExplorerEventFilters(r *http.Request) (ExplorerEventFilters, error) {
	filters := ExplorerEventFilters{
		Limit: parseLimit(r, 20, 200),
		Order: "desc",
	}

	if order := r.URL.Query().Get("order"); order == "asc" {
		filters.Order = "asc"
	}

	// Valid types are dynamically derived from loaded classification rules
	validTypes := h.classifier.KnownEventTypes()

	// Type filter: explicit type param takes precedence over tab
	if typeStr := r.URL.Query().Get("type"); typeStr != "" {
		types := strings.Split(typeStr, ",")
		for _, t := range types {
			t = strings.TrimSpace(t)
			if !validTypes[t] {
				return filters, fmt.Errorf("invalid type filter: %q is not a recognized event type", t)
			}
			filters.Types = append(filters.Types, t)
		}
	} else if tab := r.URL.Query().Get("tab"); tab != "" {
		if types, ok := tabToTypes[tab]; ok {
			filters.Types = types
		} else {
			return filters, fmt.Errorf("invalid tab: %q (valid: transfers, swaps, mints_burns, contract_calls)", tab)
		}
	}

	if v := r.URL.Query().Get("contract_id"); v != "" {
		filters.ContractID = &v
	}
	if v := r.URL.Query().Get("contract_name"); v != "" {
		filters.ContractName = &v
	}
	if v := r.URL.Query().Get("tx_hash"); v != "" {
		filters.TxHash = &v
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

	return filters, nil
}
