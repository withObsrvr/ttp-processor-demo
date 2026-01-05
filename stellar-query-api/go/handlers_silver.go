package main

import (
	"encoding/json"
	"log"
	"net/http"
	"reflect"
	"strconv"
	"time"
)

// SilverHandlers contains HTTP handlers for Silver layer queries
type SilverHandlers struct {
	legacyReader  *UnifiedSilverReader
	unifiedReader *UnifiedDuckDBReader
	readerMode    ReaderMode
}

// NewSilverHandlers creates new Silver API handlers with reader mode support
func NewSilverHandlers(legacyReader *UnifiedSilverReader, unifiedReader *UnifiedDuckDBReader, readerMode ReaderMode) *SilverHandlers {
	return &SilverHandlers{
		legacyReader:  legacyReader,
		unifiedReader: unifiedReader,
		readerMode:    readerMode,
	}
}

// logMismatch logs when legacy and unified reader results differ (for hybrid mode validation)
func logMismatch(endpoint string, legacyCount, unifiedCount int, details string) {
	log.Printf("⚠️ HYBRID MISMATCH [%s]: legacy=%d, unified=%d | %s", endpoint, legacyCount, unifiedCount, details)
}

// logHybridMatch logs when legacy and unified reader results match (for hybrid mode validation)
func logHybridMatch(endpoint string, count int) {
	log.Printf("✅ HYBRID MATCH [%s]: count=%d", endpoint, count)
}

// ============================================
// ACCOUNT ENDPOINTS
// ============================================

// HandleAccountCurrent returns current state of an account
// GET /api/v1/silver/accounts/current?account_id=GXXXXX
func (h *SilverHandlers) HandleAccountCurrent(w http.ResponseWriter, r *http.Request) {
	accountID := r.URL.Query().Get("account_id")
	if accountID == "" {
		respondError(w, "account_id required", http.StatusBadRequest)
		return
	}

	var account *AccountCurrent
	var err error

	switch h.readerMode {
	case ReaderModeUnified:
		account, err = h.unifiedReader.GetAccountCurrent(r.Context(), accountID)
	case ReaderModeHybrid:
		// Run both, compare, return legacy
		legacyAccount, legacyErr := h.legacyReader.GetAccountCurrent(r.Context(), accountID)
		unifiedAccount, unifiedErr := h.unifiedReader.GetAccountCurrent(r.Context(), accountID)

		if legacyErr != nil && unifiedErr != nil {
			log.Printf("⚠️ HYBRID [HandleAccountCurrent]: both failed - legacy: %v, unified: %v", legacyErr, unifiedErr)
		} else if legacyErr != nil {
			log.Printf("⚠️ HYBRID [HandleAccountCurrent]: legacy failed: %v, unified succeeded", legacyErr)
		} else if unifiedErr != nil {
			log.Printf("⚠️ HYBRID [HandleAccountCurrent]: unified failed: %v, legacy succeeded", unifiedErr)
		} else {
			// Both succeeded, compare results
			legacyNil := legacyAccount == nil
			unifiedNil := unifiedAccount == nil
			if legacyNil != unifiedNil {
				logMismatch("HandleAccountCurrent", boolToInt(!legacyNil), boolToInt(!unifiedNil), "account_id="+accountID)
			} else if !legacyNil && !reflect.DeepEqual(legacyAccount, unifiedAccount) {
				logMismatch("HandleAccountCurrent", 1, 1, "account_id="+accountID+" (content differs)")
			} else {
				logHybridMatch("HandleAccountCurrent", boolToInt(!legacyNil))
			}
		}
		account, err = legacyAccount, legacyErr
	default: // ReaderModeLegacy
		account, err = h.legacyReader.GetAccountCurrent(r.Context(), accountID)
	}

	if err != nil {
		respondError(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if account == nil {
		respondError(w, "account not found", http.StatusNotFound)
		return
	}

	respondJSON(w, map[string]interface{}{
		"account": account,
	})
}

// HandleAccountHistory returns historical snapshots of an account
// GET /api/v1/silver/accounts/history?account_id=GXXXXX&limit=50
// GET /api/v1/silver/accounts/history?cursor=xxx (cursor-based pagination)
func (h *SilverHandlers) HandleAccountHistory(w http.ResponseWriter, r *http.Request) {
	accountID := r.URL.Query().Get("account_id")
	if accountID == "" {
		respondError(w, "account_id required", http.StatusBadRequest)
		return
	}

	// Parse cursor for pagination
	cursorStr := r.URL.Query().Get("cursor")
	cursor, err := DecodeAccountCursor(cursorStr)
	if err != nil {
		respondError(w, "invalid cursor: "+err.Error(), http.StatusBadRequest)
		return
	}

	limit := parseLimit(r, 50, 500)

	var history []AccountSnapshot
	var nextCursor string
	var hasMore bool

	switch h.readerMode {
	case ReaderModeUnified:
		history, nextCursor, hasMore, err = h.unifiedReader.GetAccountHistoryWithCursor(r.Context(), accountID, limit, cursor)
	case ReaderModeHybrid:
		legacyHistory, legacyNextCursor, legacyHasMore, legacyErr := h.legacyReader.GetAccountHistoryWithCursor(r.Context(), accountID, limit, cursor)
		unifiedHistory, _, _, unifiedErr := h.unifiedReader.GetAccountHistoryWithCursor(r.Context(), accountID, limit, cursor)

		if legacyErr != nil && unifiedErr != nil {
			log.Printf("⚠️ HYBRID [HandleAccountHistory]: both failed - legacy: %v, unified: %v", legacyErr, unifiedErr)
		} else if legacyErr != nil {
			log.Printf("⚠️ HYBRID [HandleAccountHistory]: legacy failed: %v, unified succeeded with %d results", legacyErr, len(unifiedHistory))
		} else if unifiedErr != nil {
			log.Printf("⚠️ HYBRID [HandleAccountHistory]: unified failed: %v, legacy succeeded with %d results", unifiedErr, len(legacyHistory))
		} else {
			if len(legacyHistory) != len(unifiedHistory) {
				logMismatch("HandleAccountHistory", len(legacyHistory), len(unifiedHistory), "account_id="+accountID)
			} else {
				logHybridMatch("HandleAccountHistory", len(legacyHistory))
			}
		}
		history, nextCursor, hasMore, err = legacyHistory, legacyNextCursor, legacyHasMore, legacyErr
	default: // ReaderModeLegacy
		history, nextCursor, hasMore, err = h.legacyReader.GetAccountHistoryWithCursor(r.Context(), accountID, limit, cursor)
	}

	if err != nil {
		respondError(w, err.Error(), http.StatusInternalServerError)
		return
	}

	response := map[string]interface{}{
		"account_id": accountID,
		"history":    history,
		"count":      len(history),
		"has_more":   hasMore,
	}
	if nextCursor != "" {
		response["cursor"] = nextCursor
	}

	respondJSON(w, response)
}

// HandleTopAccounts returns top accounts by balance (for leaderboards)
// GET /api/v1/silver/accounts/top?limit=100
func (h *SilverHandlers) HandleTopAccounts(w http.ResponseWriter, r *http.Request) {
	limit := parseLimit(r, 100, 1000)

	var accounts []AccountCurrent
	var err error

	switch h.readerMode {
	case ReaderModeUnified:
		accounts, err = h.unifiedReader.GetTopAccounts(r.Context(), limit)
	case ReaderModeHybrid:
		legacyAccounts, legacyErr := h.legacyReader.GetTopAccounts(r.Context(), limit)
		unifiedAccounts, unifiedErr := h.unifiedReader.GetTopAccounts(r.Context(), limit)

		if legacyErr != nil && unifiedErr != nil {
			log.Printf("⚠️ HYBRID [HandleTopAccounts]: both failed - legacy: %v, unified: %v", legacyErr, unifiedErr)
		} else if legacyErr != nil {
			log.Printf("⚠️ HYBRID [HandleTopAccounts]: legacy failed: %v, unified succeeded with %d results", legacyErr, len(unifiedAccounts))
		} else if unifiedErr != nil {
			log.Printf("⚠️ HYBRID [HandleTopAccounts]: unified failed: %v, legacy succeeded with %d results", unifiedErr, len(legacyAccounts))
		} else {
			if len(legacyAccounts) != len(unifiedAccounts) {
				logMismatch("HandleTopAccounts", len(legacyAccounts), len(unifiedAccounts), "limit="+strconv.Itoa(limit))
			} else {
				logHybridMatch("HandleTopAccounts", len(legacyAccounts))
			}
		}
		accounts, err = legacyAccounts, legacyErr
	default: // ReaderModeLegacy
		accounts, err = h.legacyReader.GetTopAccounts(r.Context(), limit)
	}

	if err != nil {
		respondError(w, err.Error(), http.StatusInternalServerError)
		return
	}

	respondJSON(w, map[string]interface{}{
		"accounts": accounts,
		"count":    len(accounts),
	})
}

// HandleListAccounts returns a paginated list of all accounts
// GET /api/v1/silver/accounts?limit=100
// GET /api/v1/silver/accounts?cursor=xxx (cursor-based pagination)
// GET /api/v1/silver/accounts?sort_by=balance&order=desc
// GET /api/v1/silver/accounts?min_balance=1000000000 (filter by minimum balance in stroops)
func (h *SilverHandlers) HandleListAccounts(w http.ResponseWriter, r *http.Request) {
	// Parse cursor for pagination
	cursorStr := r.URL.Query().Get("cursor")
	cursor, err := DecodeAccountListCursor(cursorStr)
	if err != nil {
		respondError(w, "invalid cursor: "+err.Error(), http.StatusBadRequest)
		return
	}

	// Build filters
	filters := AccountListFilters{
		SortBy:    r.URL.Query().Get("sort_by"),
		SortOrder: r.URL.Query().Get("order"),
		Limit:     parseLimit(r, 100, 1000),
		Cursor:    cursor,
	}

	// Parse minimum balance filter
	if minBalStr := r.URL.Query().Get("min_balance"); minBalStr != "" {
		minBal, err := strconv.ParseInt(minBalStr, 10, 64)
		if err == nil && minBal >= 0 {
			filters.MinBalance = &minBal
		}
	}

	// Default sort by balance descending
	if filters.SortBy == "" {
		filters.SortBy = "balance"
	}
	if filters.SortOrder == "" {
		filters.SortOrder = "desc"
	}

	// Validate cursor sort params match request sort params
	// This prevents incorrect pagination when sort order changes between requests
	if cursor != nil && cursor.SortBy != "" {
		if cursor.SortBy != filters.SortBy {
			respondError(w, "cursor was created with sort_by='"+cursor.SortBy+"' but request uses sort_by='"+filters.SortBy+"'. Cannot change sort order while paginating.", http.StatusBadRequest)
			return
		}
		if cursor.SortOrder != filters.SortOrder {
			respondError(w, "cursor was created with order='"+cursor.SortOrder+"' but request uses order='"+filters.SortOrder+"'. Cannot change sort order while paginating.", http.StatusBadRequest)
			return
		}
	}

	var accounts []AccountCurrent
	var nextCursor string
	var hasMore bool

	switch h.readerMode {
	case ReaderModeUnified:
		accounts, nextCursor, hasMore, err = h.unifiedReader.GetAccountsListWithCursor(r.Context(), filters)
	case ReaderModeHybrid:
		legacyAccounts, legacyNextCursor, legacyHasMore, legacyErr := h.legacyReader.GetAccountsListWithCursor(r.Context(), filters)
		unifiedAccounts, _, _, unifiedErr := h.unifiedReader.GetAccountsListWithCursor(r.Context(), filters)

		if legacyErr != nil && unifiedErr != nil {
			log.Printf("⚠️ HYBRID [HandleListAccounts]: both failed - legacy: %v, unified: %v", legacyErr, unifiedErr)
		} else if legacyErr != nil {
			log.Printf("⚠️ HYBRID [HandleListAccounts]: legacy failed: %v, unified succeeded with %d results", legacyErr, len(unifiedAccounts))
		} else if unifiedErr != nil {
			log.Printf("⚠️ HYBRID [HandleListAccounts]: unified failed: %v, legacy succeeded with %d results", unifiedErr, len(legacyAccounts))
		} else {
			if len(legacyAccounts) != len(unifiedAccounts) {
				logMismatch("HandleListAccounts", len(legacyAccounts), len(unifiedAccounts), "sort="+filters.SortBy+":"+filters.SortOrder)
			} else {
				logHybridMatch("HandleListAccounts", len(legacyAccounts))
			}
		}
		accounts, nextCursor, hasMore, err = legacyAccounts, legacyNextCursor, legacyHasMore, legacyErr
	default: // ReaderModeLegacy
		accounts, nextCursor, hasMore, err = h.legacyReader.GetAccountsListWithCursor(r.Context(), filters)
	}

	if err != nil {
		respondError(w, err.Error(), http.StatusInternalServerError)
		return
	}

	response := map[string]interface{}{
		"accounts": accounts,
		"count":    len(accounts),
		"has_more": hasMore,
	}
	if nextCursor != "" {
		response["cursor"] = nextCursor
	}

	respondJSON(w, response)
}

// ============================================
// OPERATIONS ENDPOINTS (Enriched)
// ============================================

// HandleEnrichedOperations returns enriched operations with full context
// GET /api/v1/silver/operations/enriched?account_id=GXXXXX&limit=100
// GET /api/v1/silver/operations/enriched?tx_hash=TXXXXX
// GET /api/v1/silver/operations/enriched?payments_only=true
// GET /api/v1/silver/operations/enriched?cursor=xxx (cursor-based pagination)
func (h *SilverHandlers) HandleEnrichedOperations(w http.ResponseWriter, r *http.Request) {
	// Parse cursor for pagination
	cursorStr := r.URL.Query().Get("cursor")
	cursor, err := DecodeOperationCursor(cursorStr)
	if err != nil {
		respondError(w, "invalid cursor: "+err.Error(), http.StatusBadRequest)
		return
	}

	// Validate cursor and start_ledger are mutually exclusive
	startLedgerStr := r.URL.Query().Get("start_ledger")
	if cursorStr != "" && startLedgerStr != "" {
		respondError(w, "cursor and start_ledger are mutually exclusive", http.StatusBadRequest)
		return
	}

	filters := OperationFilters{
		AccountID:    r.URL.Query().Get("account_id"),
		TxHash:       r.URL.Query().Get("tx_hash"),
		PaymentsOnly: r.URL.Query().Get("payments_only") == "true",
		SorobanOnly:  r.URL.Query().Get("soroban_only") == "true",
		Limit:        parseLimit(r, 100, 1000),
		Cursor:       cursor,
	}

	// Parse ledger range (only if no cursor)
	if cursor == nil {
		if startLedgerStr != "" {
			if start, err := strconv.ParseInt(startLedgerStr, 10, 64); err == nil {
				filters.StartLedger = start
			}
		}
		if endStr := r.URL.Query().Get("end_ledger"); endStr != "" {
			if end, err := strconv.ParseInt(endStr, 10, 64); err == nil {
				filters.EndLedger = end
			}
		}
	}

	var operations []EnrichedOperation
	var nextCursor string
	var hasMore bool

	switch h.readerMode {
	case ReaderModeUnified:
		operations, nextCursor, hasMore, err = h.unifiedReader.GetEnrichedOperationsWithCursor(r.Context(), filters)
	case ReaderModeHybrid:
		legacyOps, legacyNextCursor, legacyHasMore, legacyErr := h.legacyReader.GetEnrichedOperationsWithCursor(r.Context(), filters)
		unifiedOps, _, _, unifiedErr := h.unifiedReader.GetEnrichedOperationsWithCursor(r.Context(), filters)

		if legacyErr != nil && unifiedErr != nil {
			log.Printf("⚠️ HYBRID [HandleEnrichedOperations]: both failed - legacy: %v, unified: %v", legacyErr, unifiedErr)
		} else if legacyErr != nil {
			log.Printf("⚠️ HYBRID [HandleEnrichedOperations]: legacy failed: %v, unified succeeded with %d results", legacyErr, len(unifiedOps))
		} else if unifiedErr != nil {
			log.Printf("⚠️ HYBRID [HandleEnrichedOperations]: unified failed: %v, legacy succeeded with %d results", unifiedErr, len(legacyOps))
		} else {
			if len(legacyOps) != len(unifiedOps) {
				logMismatch("HandleEnrichedOperations", len(legacyOps), len(unifiedOps), "account_id="+filters.AccountID)
			} else {
				logHybridMatch("HandleEnrichedOperations", len(legacyOps))
			}
		}
		operations, nextCursor, hasMore, err = legacyOps, legacyNextCursor, legacyHasMore, legacyErr
	default: // ReaderModeLegacy
		operations, nextCursor, hasMore, err = h.legacyReader.GetEnrichedOperationsWithCursor(r.Context(), filters)
	}

	if err != nil {
		respondError(w, err.Error(), http.StatusInternalServerError)
		return
	}

	response := map[string]interface{}{
		"operations": operations,
		"count":      len(operations),
		"filters":    filters,
		"has_more":   hasMore,
	}
	if nextCursor != "" {
		response["cursor"] = nextCursor
	}

	respondJSON(w, response)
}

// HandlePayments is a convenience endpoint for payments only
// GET /api/v1/silver/payments?account_id=GXXXXX&limit=50
// GET /api/v1/silver/payments?cursor=xxx (cursor-based pagination)
func (h *SilverHandlers) HandlePayments(w http.ResponseWriter, r *http.Request) {
	// Parse cursor for pagination
	cursorStr := r.URL.Query().Get("cursor")
	cursor, err := DecodeOperationCursor(cursorStr)
	if err != nil {
		respondError(w, "invalid cursor: "+err.Error(), http.StatusBadRequest)
		return
	}

	filters := OperationFilters{
		AccountID:    r.URL.Query().Get("account_id"),
		PaymentsOnly: true,
		Limit:        parseLimit(r, 50, 500),
		Cursor:       cursor,
	}

	var operations []EnrichedOperation
	var nextCursor string
	var hasMore bool

	switch h.readerMode {
	case ReaderModeUnified:
		operations, nextCursor, hasMore, err = h.unifiedReader.GetEnrichedOperationsWithCursor(r.Context(), filters)
	case ReaderModeHybrid:
		// Use legacy for convenience endpoints in hybrid mode (primary routing is done in main handlers)
		operations, nextCursor, hasMore, err = h.legacyReader.GetEnrichedOperationsWithCursor(r.Context(), filters)
	default:
		operations, nextCursor, hasMore, err = h.legacyReader.GetEnrichedOperationsWithCursor(r.Context(), filters)
	}

	if err != nil {
		respondError(w, err.Error(), http.StatusInternalServerError)
		return
	}

	response := map[string]interface{}{
		"payments": operations,
		"count":    len(operations),
		"has_more": hasMore,
	}
	if nextCursor != "" {
		response["cursor"] = nextCursor
	}

	respondJSON(w, response)
}

// HandleSorobanOperations is a convenience endpoint for Soroban operations only
// GET /api/v1/silver/operations/soroban?account_id=GXXXXX&limit=50
// GET /api/v1/silver/operations/soroban?cursor=xxx (cursor-based pagination)
func (h *SilverHandlers) HandleSorobanOperations(w http.ResponseWriter, r *http.Request) {
	// Parse cursor for pagination
	cursorStr := r.URL.Query().Get("cursor")
	cursor, err := DecodeOperationCursor(cursorStr)
	if err != nil {
		respondError(w, "invalid cursor: "+err.Error(), http.StatusBadRequest)
		return
	}

	filters := OperationFilters{
		AccountID:   r.URL.Query().Get("account_id"),
		SorobanOnly: true,
		Limit:       parseLimit(r, 50, 500),
		Cursor:      cursor,
	}

	var operations []EnrichedOperation
	var nextCursor string
	var hasMore bool

	switch h.readerMode {
	case ReaderModeUnified:
		operations, nextCursor, hasMore, err = h.unifiedReader.GetEnrichedOperationsWithCursor(r.Context(), filters)
	case ReaderModeHybrid:
		operations, nextCursor, hasMore, err = h.legacyReader.GetEnrichedOperationsWithCursor(r.Context(), filters)
	default:
		operations, nextCursor, hasMore, err = h.legacyReader.GetEnrichedOperationsWithCursor(r.Context(), filters)
	}

	if err != nil {
		respondError(w, err.Error(), http.StatusInternalServerError)
		return
	}

	response := map[string]interface{}{
		"soroban_operations": operations,
		"count":              len(operations),
		"has_more":           hasMore,
	}
	if nextCursor != "" {
		response["cursor"] = nextCursor
	}

	respondJSON(w, response)
}

// ============================================
// TOKEN TRANSFERS ENDPOINTS
// ============================================

// HandleTokenTransfers returns token transfers (classic + Soroban unified)
// GET /api/v1/silver/transfers?asset_code=USDC&limit=100
// GET /api/v1/silver/transfers?from_account=GXXXXX
// GET /api/v1/silver/transfers?source_type=classic
// GET /api/v1/silver/transfers?cursor=xxx (cursor-based pagination)
func (h *SilverHandlers) HandleTokenTransfers(w http.ResponseWriter, r *http.Request) {
	// Parse cursor for pagination
	cursorStr := r.URL.Query().Get("cursor")
	cursor, err := DecodeTransferCursor(cursorStr)
	if err != nil {
		respondError(w, "invalid cursor: "+err.Error(), http.StatusBadRequest)
		return
	}

	filters := TransferFilters{
		SourceType:  r.URL.Query().Get("source_type"), // "classic" or "soroban"
		AssetCode:   r.URL.Query().Get("asset_code"),
		FromAccount: r.URL.Query().Get("from_account"),
		ToAccount:   r.URL.Query().Get("to_account"),
		Limit:       parseLimit(r, 100, 1000),
		Cursor:      cursor,
	}

	// Parse time range
	if startStr := r.URL.Query().Get("start_time"); startStr != "" {
		if start, err := time.Parse(time.RFC3339, startStr); err == nil {
			filters.StartTime = start
		}
	} else {
		// Default to last 24 hours
		filters.StartTime = time.Now().Add(-24 * time.Hour)
	}

	if endStr := r.URL.Query().Get("end_time"); endStr != "" {
		if end, err := time.Parse(time.RFC3339, endStr); err == nil {
			filters.EndTime = end
		}
	} else {
		filters.EndTime = time.Now()
	}

	var transfers []TokenTransfer
	var nextCursor string
	var hasMore bool

	switch h.readerMode {
	case ReaderModeUnified:
		transfers, nextCursor, hasMore, err = h.unifiedReader.GetTokenTransfersWithCursor(r.Context(), filters)
	case ReaderModeHybrid:
		legacyTransfers, legacyNextCursor, legacyHasMore, legacyErr := h.legacyReader.GetTokenTransfersWithCursor(r.Context(), filters)
		unifiedTransfers, _, _, unifiedErr := h.unifiedReader.GetTokenTransfersWithCursor(r.Context(), filters)

		if legacyErr != nil && unifiedErr != nil {
			log.Printf("⚠️ HYBRID [HandleTokenTransfers]: both failed - legacy: %v, unified: %v", legacyErr, unifiedErr)
		} else if legacyErr != nil {
			log.Printf("⚠️ HYBRID [HandleTokenTransfers]: legacy failed: %v, unified succeeded with %d results", legacyErr, len(unifiedTransfers))
		} else if unifiedErr != nil {
			log.Printf("⚠️ HYBRID [HandleTokenTransfers]: unified failed: %v, legacy succeeded with %d results", unifiedErr, len(legacyTransfers))
		} else {
			if len(legacyTransfers) != len(unifiedTransfers) {
				logMismatch("HandleTokenTransfers", len(legacyTransfers), len(unifiedTransfers), "asset="+filters.AssetCode)
			} else {
				logHybridMatch("HandleTokenTransfers", len(legacyTransfers))
			}
		}
		transfers, nextCursor, hasMore, err = legacyTransfers, legacyNextCursor, legacyHasMore, legacyErr
	default: // ReaderModeLegacy
		transfers, nextCursor, hasMore, err = h.legacyReader.GetTokenTransfersWithCursor(r.Context(), filters)
	}

	if err != nil {
		respondError(w, err.Error(), http.StatusInternalServerError)
		return
	}

	response := map[string]interface{}{
		"transfers": transfers,
		"count":     len(transfers),
		"filters":   filters,
		"has_more":  hasMore,
	}
	if nextCursor != "" {
		response["cursor"] = nextCursor
	}

	respondJSON(w, response)
}

// HandleTokenTransferStats returns aggregated transfer statistics
// GET /api/v1/silver/transfers/stats?group_by=asset
// GET /api/v1/silver/transfers/stats?group_by=hour&start_time=2024-01-01T00:00:00Z
func (h *SilverHandlers) HandleTokenTransferStats(w http.ResponseWriter, r *http.Request) {
	groupBy := r.URL.Query().Get("group_by")
	if groupBy == "" {
		groupBy = "asset"
	}

	// Validate group_by
	validGroupBy := map[string]bool{"asset": true, "source_type": true, "hour": true, "day": true}
	if !validGroupBy[groupBy] {
		respondError(w, "invalid group_by, must be: asset, source_type, hour, or day", http.StatusBadRequest)
		return
	}

	// Parse time range
	startTime := time.Now().Add(-24 * time.Hour)
	if startStr := r.URL.Query().Get("start_time"); startStr != "" {
		if start, err := time.Parse(time.RFC3339, startStr); err == nil {
			startTime = start
		}
	}

	endTime := time.Now()
	if endStr := r.URL.Query().Get("end_time"); endStr != "" {
		if end, err := time.Parse(time.RFC3339, endStr); err == nil {
			endTime = end
		}
	}

	var stats []TransferStats
	var err error

	switch h.readerMode {
	case ReaderModeUnified:
		stats, err = h.unifiedReader.GetTokenTransferStats(r.Context(), groupBy, startTime, endTime)
	case ReaderModeHybrid:
		legacyStats, legacyErr := h.legacyReader.GetTokenTransferStats(r.Context(), groupBy, startTime, endTime)
		unifiedStats, unifiedErr := h.unifiedReader.GetTokenTransferStats(r.Context(), groupBy, startTime, endTime)

		if legacyErr != nil && unifiedErr != nil {
			log.Printf("⚠️ HYBRID [HandleTokenTransferStats]: both failed - legacy: %v, unified: %v", legacyErr, unifiedErr)
		} else if legacyErr != nil {
			log.Printf("⚠️ HYBRID [HandleTokenTransferStats]: legacy failed: %v, unified succeeded with %d results", legacyErr, len(unifiedStats))
		} else if unifiedErr != nil {
			log.Printf("⚠️ HYBRID [HandleTokenTransferStats]: unified failed: %v, legacy succeeded with %d results", unifiedErr, len(legacyStats))
		} else {
			if len(legacyStats) != len(unifiedStats) {
				logMismatch("HandleTokenTransferStats", len(legacyStats), len(unifiedStats), "group_by="+groupBy)
			} else {
				logHybridMatch("HandleTokenTransferStats", len(legacyStats))
			}
		}
		stats, err = legacyStats, legacyErr
	default: // ReaderModeLegacy
		stats, err = h.legacyReader.GetTokenTransferStats(r.Context(), groupBy, startTime, endTime)
	}

	if err != nil {
		respondError(w, err.Error(), http.StatusInternalServerError)
		return
	}

	respondJSON(w, map[string]interface{}{
		"stats":      stats,
		"count":      len(stats),
		"group_by":   groupBy,
		"start_time": startTime.Format(time.RFC3339),
		"end_time":   endTime.Format(time.RFC3339),
	})
}

// ============================================
// BLOCK EXPLORER SPECIFIC ENDPOINTS
// ============================================

// HandleAccountOverview returns comprehensive account overview for block explorer
// GET /api/v1/silver/explorer/account?account_id=GXXXXX
func (h *SilverHandlers) HandleAccountOverview(w http.ResponseWriter, r *http.Request) {
	accountID := r.URL.Query().Get("account_id")
	if accountID == "" {
		respondError(w, "account_id required", http.StatusBadRequest)
		return
	}

	var account *AccountCurrent
	var operations []EnrichedOperation
	var transfersFrom, transfersTo []TokenTransfer
	var err error

	// Select reader based on mode
	switch h.readerMode {
	case ReaderModeUnified:
		account, err = h.unifiedReader.GetAccountCurrent(r.Context(), accountID)
		if err != nil {
			respondError(w, err.Error(), http.StatusInternalServerError)
			return
		}
		if account == nil {
			respondError(w, "account not found", http.StatusNotFound)
			return
		}
		operations, err = h.unifiedReader.GetEnrichedOperations(r.Context(), OperationFilters{
			AccountID: accountID,
			Limit:     10,
		})
		if err != nil {
			respondError(w, err.Error(), http.StatusInternalServerError)
			return
		}
		transfersFrom, _ = h.unifiedReader.GetTokenTransfers(r.Context(), TransferFilters{
			FromAccount: accountID,
			StartTime:   time.Now().Add(-7 * 24 * time.Hour),
			EndTime:     time.Now(),
			Limit:       10,
		})
		transfersTo, _ = h.unifiedReader.GetTokenTransfers(r.Context(), TransferFilters{
			ToAccount: accountID,
			StartTime: time.Now().Add(-7 * 24 * time.Hour),
			EndTime:   time.Now(),
			Limit:     10,
		})

	case ReaderModeHybrid:
		// Run both for validation, return legacy
		legacyAccount, legacyErr := h.legacyReader.GetAccountCurrent(r.Context(), accountID)
		unifiedAccount, unifiedErr := h.unifiedReader.GetAccountCurrent(r.Context(), accountID)
		if legacyErr == nil && unifiedErr == nil {
			legacyNil := legacyAccount == nil
			unifiedNil := unifiedAccount == nil
			if legacyNil != unifiedNil {
				logMismatch("HandleAccountOverview", boolToInt(!legacyNil), boolToInt(!unifiedNil), "account_id="+accountID)
			} else {
				logHybridMatch("HandleAccountOverview", boolToInt(!legacyNil))
			}
		}
		account, err = legacyAccount, legacyErr
		if err != nil {
			respondError(w, err.Error(), http.StatusInternalServerError)
			return
		}
		if account == nil {
			respondError(w, "account not found", http.StatusNotFound)
			return
		}
		operations, err = h.legacyReader.GetEnrichedOperations(r.Context(), OperationFilters{
			AccountID: accountID,
			Limit:     10,
		})
		if err != nil {
			respondError(w, err.Error(), http.StatusInternalServerError)
			return
		}
		transfersFrom, _ = h.legacyReader.GetTokenTransfers(r.Context(), TransferFilters{
			FromAccount: accountID,
			StartTime:   time.Now().Add(-7 * 24 * time.Hour),
			EndTime:     time.Now(),
			Limit:       10,
		})
		transfersTo, _ = h.legacyReader.GetTokenTransfers(r.Context(), TransferFilters{
			ToAccount: accountID,
			StartTime: time.Now().Add(-7 * 24 * time.Hour),
			EndTime:   time.Now(),
			Limit:     10,
		})

	default: // ReaderModeLegacy
		account, err = h.legacyReader.GetAccountCurrent(r.Context(), accountID)
		if err != nil {
			respondError(w, err.Error(), http.StatusInternalServerError)
			return
		}
		if account == nil {
			respondError(w, "account not found", http.StatusNotFound)
			return
		}
		operations, err = h.legacyReader.GetEnrichedOperations(r.Context(), OperationFilters{
			AccountID: accountID,
			Limit:     10,
		})
		if err != nil {
			respondError(w, err.Error(), http.StatusInternalServerError)
			return
		}
		transfersFrom, _ = h.legacyReader.GetTokenTransfers(r.Context(), TransferFilters{
			FromAccount: accountID,
			StartTime:   time.Now().Add(-7 * 24 * time.Hour),
			EndTime:     time.Now(),
			Limit:       10,
		})
		transfersTo, _ = h.legacyReader.GetTokenTransfers(r.Context(), TransferFilters{
			ToAccount: accountID,
			StartTime: time.Now().Add(-7 * 24 * time.Hour),
			EndTime:   time.Now(),
			Limit:     10,
		})
	}

	// Combine transfers
	transfers := append(transfersFrom, transfersTo...)

	respondJSON(w, map[string]interface{}{
		"account":           account,
		"recent_operations": operations,
		"recent_transfers":  transfers,
		"operations_count":  len(operations),
		"transfers_count":   len(transfers),
	})
}

// HandleTransactionDetails returns full transaction details with operations
// GET /api/v1/silver/explorer/transaction?tx_hash=TXXXXX
func (h *SilverHandlers) HandleTransactionDetails(w http.ResponseWriter, r *http.Request) {
	txHash := r.URL.Query().Get("tx_hash")
	if txHash == "" {
		respondError(w, "tx_hash required", http.StatusBadRequest)
		return
	}

	var operations []EnrichedOperation
	var transfers []TokenTransfer
	var err error

	// Select reader based on mode
	switch h.readerMode {
	case ReaderModeUnified:
		operations, err = h.unifiedReader.GetEnrichedOperations(r.Context(), OperationFilters{
			TxHash: txHash,
			Limit:  100,
		})
		if err != nil {
			respondError(w, err.Error(), http.StatusInternalServerError)
			return
		}
		if len(operations) == 0 {
			respondError(w, "transaction not found", http.StatusNotFound)
			return
		}
		transfers, _ = h.unifiedReader.GetTokenTransfers(r.Context(), TransferFilters{
			Limit: 100,
		})

	case ReaderModeHybrid:
		// Run both for validation, return legacy
		legacyOps, legacyErr := h.legacyReader.GetEnrichedOperations(r.Context(), OperationFilters{
			TxHash: txHash,
			Limit:  100,
		})
		unifiedOps, unifiedErr := h.unifiedReader.GetEnrichedOperations(r.Context(), OperationFilters{
			TxHash: txHash,
			Limit:  100,
		})
		if legacyErr == nil && unifiedErr == nil {
			if len(legacyOps) != len(unifiedOps) {
				logMismatch("HandleTransactionDetails", len(legacyOps), len(unifiedOps), "tx_hash="+txHash)
			} else {
				logHybridMatch("HandleTransactionDetails", len(legacyOps))
			}
		}
		operations, err = legacyOps, legacyErr
		if err != nil {
			respondError(w, err.Error(), http.StatusInternalServerError)
			return
		}
		if len(operations) == 0 {
			respondError(w, "transaction not found", http.StatusNotFound)
			return
		}
		transfers, _ = h.legacyReader.GetTokenTransfers(r.Context(), TransferFilters{
			Limit: 100,
		})

	default: // ReaderModeLegacy
		operations, err = h.legacyReader.GetEnrichedOperations(r.Context(), OperationFilters{
			TxHash: txHash,
			Limit:  100,
		})
		if err != nil {
			respondError(w, err.Error(), http.StatusInternalServerError)
			return
		}
		if len(operations) == 0 {
			respondError(w, "transaction not found", http.StatusNotFound)
			return
		}
		transfers, _ = h.legacyReader.GetTokenTransfers(r.Context(), TransferFilters{
			Limit: 100,
		})
	}

	// Filter transfers for this transaction
	var txTransfers []TokenTransfer
	for _, t := range transfers {
		if t.TransactionHash == txHash {
			txTransfers = append(txTransfers, t)
		}
	}

	// Extract transaction info from first operation
	txInfo := map[string]interface{}{
		"transaction_hash": txHash,
		"ledger_sequence":  operations[0].LedgerSequence,
		"ledger_closed_at": operations[0].LedgerClosedAt,
		"successful":       operations[0].TxSuccessful,
		"fee_charged":      operations[0].TxFeeCharged,
		"operation_count":  len(operations),
	}

	respondJSON(w, map[string]interface{}{
		"transaction": txInfo,
		"operations":  operations,
		"transfers":   txTransfers,
	})
}

// HandleAssetOverview returns asset statistics and recent transfers
// GET /api/v1/silver/explorer/asset?asset_code=USDC&asset_issuer=GXXXXX
func (h *SilverHandlers) HandleAssetOverview(w http.ResponseWriter, r *http.Request) {
	assetCode := r.URL.Query().Get("asset_code")
	if assetCode == "" {
		respondError(w, "asset_code required", http.StatusBadRequest)
		return
	}

	var transfers []TokenTransfer
	var stats []TransferStats
	var err error

	// Select reader based on mode
	switch h.readerMode {
	case ReaderModeUnified:
		transfers, err = h.unifiedReader.GetTokenTransfers(r.Context(), TransferFilters{
			AssetCode: assetCode,
			StartTime: time.Now().Add(-24 * time.Hour),
			EndTime:   time.Now(),
			Limit:     100,
		})
		if err != nil {
			respondError(w, err.Error(), http.StatusInternalServerError)
			return
		}
		stats, err = h.unifiedReader.GetTokenTransferStats(r.Context(), "asset",
			time.Now().Add(-24*time.Hour), time.Now())
		if err != nil {
			respondError(w, err.Error(), http.StatusInternalServerError)
			return
		}

	case ReaderModeHybrid:
		// Run both for validation, return legacy
		legacyTransfers, legacyErr := h.legacyReader.GetTokenTransfers(r.Context(), TransferFilters{
			AssetCode: assetCode,
			StartTime: time.Now().Add(-24 * time.Hour),
			EndTime:   time.Now(),
			Limit:     100,
		})
		unifiedTransfers, unifiedErr := h.unifiedReader.GetTokenTransfers(r.Context(), TransferFilters{
			AssetCode: assetCode,
			StartTime: time.Now().Add(-24 * time.Hour),
			EndTime:   time.Now(),
			Limit:     100,
		})
		if legacyErr == nil && unifiedErr == nil {
			if len(legacyTransfers) != len(unifiedTransfers) {
				logMismatch("HandleAssetOverview", len(legacyTransfers), len(unifiedTransfers), "asset_code="+assetCode)
			} else {
				logHybridMatch("HandleAssetOverview", len(legacyTransfers))
			}
		}
		transfers, err = legacyTransfers, legacyErr
		if err != nil {
			respondError(w, err.Error(), http.StatusInternalServerError)
			return
		}
		stats, err = h.legacyReader.GetTokenTransferStats(r.Context(), "asset",
			time.Now().Add(-24*time.Hour), time.Now())
		if err != nil {
			respondError(w, err.Error(), http.StatusInternalServerError)
			return
		}

	default: // ReaderModeLegacy
		transfers, err = h.legacyReader.GetTokenTransfers(r.Context(), TransferFilters{
			AssetCode: assetCode,
			StartTime: time.Now().Add(-24 * time.Hour),
			EndTime:   time.Now(),
			Limit:     100,
		})
		if err != nil {
			respondError(w, err.Error(), http.StatusInternalServerError)
			return
		}
		stats, err = h.legacyReader.GetTokenTransferStats(r.Context(), "asset",
			time.Now().Add(-24*time.Hour), time.Now())
		if err != nil {
			respondError(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}

	// Filter stats for this asset
	var assetStats *TransferStats
	for _, s := range stats {
		if s.AssetCode != nil && *s.AssetCode == assetCode {
			assetStats = &s
			break
		}
	}

	respondJSON(w, map[string]interface{}{
		"asset_code":       assetCode,
		"recent_transfers": transfers,
		"stats_24h":        assetStats,
		"transfer_count":   len(transfers),
	})
}

// ============================================
// HELPER FUNCTIONS
// ============================================

func parseLimit(r *http.Request, defaultLimit, maxLimit int) int {
	limitStr := r.URL.Query().Get("limit")
	if limitStr == "" {
		return defaultLimit
	}

	limit, err := strconv.Atoi(limitStr)
	if err != nil || limit <= 0 {
		return defaultLimit
	}

	if limit > maxLimit {
		return maxLimit
	}

	return limit
}

func respondJSON(w http.ResponseWriter, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*") // CORS for block explorer
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(data)
}

func respondError(w http.ResponseWriter, message string, statusCode int) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.WriteHeader(statusCode)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"error": message,
	})
}

// boolToInt converts bool to int for logging purposes
func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}
