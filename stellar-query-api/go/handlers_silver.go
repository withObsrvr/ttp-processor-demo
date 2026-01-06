package main

import (
	"encoding/json"
	"log"
	"net/http"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/mux"
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

// HandleAccountSigners returns the signers for an account (Horizon-compatible format)
// GET /api/v1/silver/accounts/{id}/signers
func (h *SilverHandlers) HandleAccountSigners(w http.ResponseWriter, r *http.Request) {
	// Extract account_id from path or query param
	accountID := r.URL.Query().Get("account_id")
	if accountID == "" {
		// Try to get from path variable (for /accounts/{id}/signers pattern)
		// The mux should have extracted it
		respondError(w, "account_id required", http.StatusBadRequest)
		return
	}

	var response *AccountSignersResponse
	var err error

	switch h.readerMode {
	case ReaderModeUnified, ReaderModeHybrid:
		// Use unified reader for both unified and hybrid modes
		response, err = h.unifiedReader.GetAccountSigners(r.Context(), accountID)
	default:
		// Legacy mode - unified reader still works, just use it
		if h.unifiedReader != nil {
			response, err = h.unifiedReader.GetAccountSigners(r.Context(), accountID)
		} else {
			respondError(w, "signers endpoint requires unified reader", http.StatusInternalServerError)
			return
		}
	}

	if err != nil {
		respondError(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if response == nil {
		respondError(w, "account not found", http.StatusNotFound)
		return
	}

	respondJSON(w, response)
}

// HandleTokenStats returns aggregated statistics for a specific token
// GET /api/v1/silver/assets/{asset}/stats
// Asset format: XLM (for native) or CODE:ISSUER (for credit assets)
func (h *SilverHandlers) HandleTokenStats(w http.ResponseWriter, r *http.Request) {
	// Extract asset from path parameter
	vars := mux.Vars(r)
	assetParam := vars["asset"]
	if assetParam == "" {
		respondError(w, "asset parameter required (format: CODE:ISSUER or XLM)", http.StatusBadRequest)
		return
	}

	// Parse asset parameter
	var assetCode, assetIssuer string
	if assetParam == "XLM" || assetParam == "native" {
		assetCode = "XLM"
	} else {
		// Expected format: CODE:ISSUER
		parts := strings.SplitN(assetParam, ":", 2)
		if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
			respondError(w, "invalid asset format, expected CODE:ISSUER or XLM", http.StatusBadRequest)
			return
		}
		assetCode = parts[0]
		assetIssuer = parts[1]
	}

	var response *TokenStatsResponse
	var err error

	// Use unified reader
	if h.unifiedReader != nil {
		response, err = h.unifiedReader.GetTokenStats(r.Context(), assetCode, assetIssuer)
	} else {
		respondError(w, "stats endpoint requires unified reader", http.StatusInternalServerError)
		return
	}

	if err != nil {
		respondError(w, err.Error(), http.StatusInternalServerError)
		return
	}

	respondJSON(w, response)
}

// HandleTokenHolders returns holders of a specific token
// GET /api/v1/silver/assets/{asset}/holders
// Asset format: XLM (for native) or CODE:ISSUER (for credit assets)
// Query params: limit, cursor, min_balance
func (h *SilverHandlers) HandleTokenHolders(w http.ResponseWriter, r *http.Request) {
	// Extract asset from path parameter
	vars := mux.Vars(r)
	assetParam := vars["asset"]
	if assetParam == "" {
		respondError(w, "asset parameter required (format: CODE:ISSUER or XLM)", http.StatusBadRequest)
		return
	}

	// Parse asset parameter
	var assetCode, assetIssuer string
	if assetParam == "XLM" || assetParam == "native" {
		assetCode = "XLM"
	} else {
		// Expected format: CODE:ISSUER
		parts := strings.SplitN(assetParam, ":", 2)
		if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
			respondError(w, "invalid asset format, expected CODE:ISSUER or XLM", http.StatusBadRequest)
			return
		}
		assetCode = parts[0]
		assetIssuer = parts[1]
	}

	// Parse cursor for pagination
	cursorStr := r.URL.Query().Get("cursor")
	cursor, err := DecodeTokenHoldersCursor(cursorStr)
	if err != nil {
		respondError(w, "invalid cursor: "+err.Error(), http.StatusBadRequest)
		return
	}

	// Build filters
	filters := TokenHoldersFilters{
		AssetCode:   assetCode,
		AssetIssuer: assetIssuer,
		Limit:       parseLimit(r, 100, 1000),
		Cursor:      cursor,
	}

	// Parse minimum balance filter (in stroops)
	if minBalStr := r.URL.Query().Get("min_balance"); minBalStr != "" {
		minBal, err := strconv.ParseInt(minBalStr, 10, 64)
		if err == nil && minBal >= 0 {
			filters.MinBalance = &minBal
		}
	}

	var response *TokenHoldersResponse

	// Use unified reader
	if h.unifiedReader != nil {
		response, err = h.unifiedReader.GetTokenHolders(r.Context(), filters)
	} else {
		respondError(w, "holders endpoint requires unified reader", http.StatusInternalServerError)
		return
	}

	if err != nil {
		respondError(w, err.Error(), http.StatusInternalServerError)
		return
	}

	respondJSON(w, response)
}

// HandleAccountBalances returns all balances (XLM + trustlines) for an account
// GET /api/v1/silver/accounts/{id}/balances
func (h *SilverHandlers) HandleAccountBalances(w http.ResponseWriter, r *http.Request) {
	// Extract account_id from path parameter
	vars := mux.Vars(r)
	accountID := vars["id"]
	if accountID == "" {
		// Fallback to query param
		accountID = r.URL.Query().Get("account_id")
	}
	if accountID == "" {
		respondError(w, "account_id required", http.StatusBadRequest)
		return
	}

	var response *AccountBalancesResponse
	var err error

	// Use unified reader
	if h.unifiedReader != nil {
		response, err = h.unifiedReader.GetAccountBalances(r.Context(), accountID)
	} else {
		respondError(w, "balances endpoint requires unified reader", http.StatusInternalServerError)
		return
	}

	if err != nil {
		if strings.Contains(err.Error(), "not found") {
			respondError(w, "account not found", http.StatusNotFound)
			return
		}
		respondError(w, err.Error(), http.StatusInternalServerError)
		return
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
// PHASE 6: STATE TABLE ENDPOINTS
// ============================================

// HandleOffers returns paginated list of offers
// GET /api/v1/silver/offers?seller_id=GXXXXX&limit=100
// GET /api/v1/silver/offers?cursor=xxx (cursor-based pagination)
func (h *SilverHandlers) HandleOffers(w http.ResponseWriter, r *http.Request) {
	// Parse cursor for pagination
	cursorStr := r.URL.Query().Get("cursor")
	cursor, err := DecodeOfferCursor(cursorStr)
	if err != nil {
		respondError(w, "invalid cursor: "+err.Error(), http.StatusBadRequest)
		return
	}

	// Build filters
	filters := OfferFilters{
		SellerID: r.URL.Query().Get("seller_id"),
		Limit:    parseLimit(r, 100, 1000),
		Cursor:   cursor,
	}

	var offers []OfferCurrent
	var nextCursor string
	var hasMore bool

	// Use unified reader (state tables are unified-only)
	if h.unifiedReader != nil {
		offers, nextCursor, hasMore, err = h.unifiedReader.GetOffers(r.Context(), filters)
	} else {
		respondError(w, "offers endpoint requires unified reader", http.StatusInternalServerError)
		return
	}

	if err != nil {
		respondError(w, err.Error(), http.StatusInternalServerError)
		return
	}

	response := map[string]interface{}{
		"offers":   offers,
		"count":    len(offers),
		"has_more": hasMore,
	}
	if nextCursor != "" {
		response["cursor"] = nextCursor
	}

	respondJSON(w, response)
}

// HandleOfferByID returns a single offer by ID
// GET /api/v1/silver/offers/{id}
func (h *SilverHandlers) HandleOfferByID(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	offerIDStr := vars["id"]
	if offerIDStr == "" {
		respondError(w, "offer_id required", http.StatusBadRequest)
		return
	}

	offerID, err := strconv.ParseInt(offerIDStr, 10, 64)
	if err != nil {
		respondError(w, "invalid offer_id: must be a number", http.StatusBadRequest)
		return
	}

	var offer *OfferCurrent

	if h.unifiedReader != nil {
		offer, err = h.unifiedReader.GetOfferByID(r.Context(), offerID)
	} else {
		respondError(w, "offers endpoint requires unified reader", http.StatusInternalServerError)
		return
	}

	if err != nil {
		respondError(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if offer == nil {
		respondError(w, "offer not found", http.StatusNotFound)
		return
	}

	respondJSON(w, map[string]interface{}{
		"offer": offer,
	})
}

// HandleOffersByPair returns offers for a trading pair
// GET /api/v1/silver/offers/pair?selling=XLM&buying=USDC:GXXXXX&limit=100
func (h *SilverHandlers) HandleOffersByPair(w http.ResponseWriter, r *http.Request) {
	sellingParam := r.URL.Query().Get("selling")
	buyingParam := r.URL.Query().Get("buying")

	if sellingParam == "" || buyingParam == "" {
		respondError(w, "selling and buying parameters required (format: CODE:ISSUER or XLM)", http.StatusBadRequest)
		return
	}

	// Parse selling asset
	sellingCode, sellingIssuer := parseAssetParam(sellingParam)
	if sellingCode == "" {
		respondError(w, "invalid selling asset format", http.StatusBadRequest)
		return
	}

	// Parse buying asset
	buyingCode, buyingIssuer := parseAssetParam(buyingParam)
	if buyingCode == "" {
		respondError(w, "invalid buying asset format", http.StatusBadRequest)
		return
	}

	// Parse cursor for pagination
	cursorStr := r.URL.Query().Get("cursor")
	cursor, err := DecodeOfferCursor(cursorStr)
	if err != nil {
		respondError(w, "invalid cursor: "+err.Error(), http.StatusBadRequest)
		return
	}

	filters := OfferFilters{
		SellingAssetCode:   sellingCode,
		SellingAssetIssuer: sellingIssuer,
		BuyingAssetCode:    buyingCode,
		BuyingAssetIssuer:  buyingIssuer,
		Limit:              parseLimit(r, 100, 1000),
		Cursor:             cursor,
	}

	var offers []OfferCurrent
	var nextCursor string
	var hasMore bool

	if h.unifiedReader != nil {
		offers, nextCursor, hasMore, err = h.unifiedReader.GetOffers(r.Context(), filters)
	} else {
		respondError(w, "offers endpoint requires unified reader", http.StatusInternalServerError)
		return
	}

	if err != nil {
		respondError(w, err.Error(), http.StatusInternalServerError)
		return
	}

	response := map[string]interface{}{
		"offers":   offers,
		"count":    len(offers),
		"has_more": hasMore,
		"pair": map[string]interface{}{
			"selling": sellingParam,
			"buying":  buyingParam,
		},
	}
	if nextCursor != "" {
		response["cursor"] = nextCursor
	}

	respondJSON(w, response)
}

// HandleLiquidityPools returns paginated list of liquidity pools
// GET /api/v1/silver/liquidity-pools?limit=100
// GET /api/v1/silver/liquidity-pools?cursor=xxx (cursor-based pagination)
func (h *SilverHandlers) HandleLiquidityPools(w http.ResponseWriter, r *http.Request) {
	// Parse cursor for pagination
	cursorStr := r.URL.Query().Get("cursor")
	cursor, err := DecodeLiquidityPoolCursor(cursorStr)
	if err != nil {
		respondError(w, "invalid cursor: "+err.Error(), http.StatusBadRequest)
		return
	}

	filters := LiquidityPoolFilters{
		Limit:  parseLimit(r, 100, 1000),
		Cursor: cursor,
	}

	var pools []LiquidityPoolCurrent
	var nextCursor string
	var hasMore bool

	if h.unifiedReader != nil {
		pools, nextCursor, hasMore, err = h.unifiedReader.GetLiquidityPools(r.Context(), filters)
	} else {
		respondError(w, "liquidity-pools endpoint requires unified reader", http.StatusInternalServerError)
		return
	}

	if err != nil {
		respondError(w, err.Error(), http.StatusInternalServerError)
		return
	}

	response := map[string]interface{}{
		"liquidity_pools": pools,
		"count":           len(pools),
		"has_more":        hasMore,
	}
	if nextCursor != "" {
		response["cursor"] = nextCursor
	}

	respondJSON(w, response)
}

// HandleLiquidityPoolByID returns a single liquidity pool by ID
// GET /api/v1/silver/liquidity-pools/{id}
func (h *SilverHandlers) HandleLiquidityPoolByID(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	poolID := vars["id"]
	if poolID == "" {
		respondError(w, "pool_id required", http.StatusBadRequest)
		return
	}

	var pool *LiquidityPoolCurrent
	var err error

	if h.unifiedReader != nil {
		pool, err = h.unifiedReader.GetLiquidityPoolByID(r.Context(), poolID)
	} else {
		respondError(w, "liquidity-pools endpoint requires unified reader", http.StatusInternalServerError)
		return
	}

	if err != nil {
		respondError(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if pool == nil {
		respondError(w, "liquidity pool not found", http.StatusNotFound)
		return
	}

	respondJSON(w, map[string]interface{}{
		"liquidity_pool": pool,
	})
}

// HandleLiquidityPoolsByAsset returns liquidity pools containing an asset
// GET /api/v1/silver/liquidity-pools/asset?asset=USDC:GXXXXX&limit=100
func (h *SilverHandlers) HandleLiquidityPoolsByAsset(w http.ResponseWriter, r *http.Request) {
	assetParam := r.URL.Query().Get("asset")
	if assetParam == "" {
		respondError(w, "asset parameter required (format: CODE:ISSUER or XLM)", http.StatusBadRequest)
		return
	}

	assetCode, assetIssuer := parseAssetParam(assetParam)
	if assetCode == "" {
		respondError(w, "invalid asset format", http.StatusBadRequest)
		return
	}

	// Parse cursor for pagination
	cursorStr := r.URL.Query().Get("cursor")
	cursor, err := DecodeLiquidityPoolCursor(cursorStr)
	if err != nil {
		respondError(w, "invalid cursor: "+err.Error(), http.StatusBadRequest)
		return
	}

	filters := LiquidityPoolFilters{
		AssetCode:   assetCode,
		AssetIssuer: assetIssuer,
		Limit:       parseLimit(r, 100, 1000),
		Cursor:      cursor,
	}

	var pools []LiquidityPoolCurrent
	var nextCursor string
	var hasMore bool

	if h.unifiedReader != nil {
		pools, nextCursor, hasMore, err = h.unifiedReader.GetLiquidityPools(r.Context(), filters)
	} else {
		respondError(w, "liquidity-pools endpoint requires unified reader", http.StatusInternalServerError)
		return
	}

	if err != nil {
		respondError(w, err.Error(), http.StatusInternalServerError)
		return
	}

	response := map[string]interface{}{
		"liquidity_pools": pools,
		"count":           len(pools),
		"has_more":        hasMore,
		"asset":           assetParam,
	}
	if nextCursor != "" {
		response["cursor"] = nextCursor
	}

	respondJSON(w, response)
}

// HandleClaimableBalances returns paginated list of claimable balances
// GET /api/v1/silver/claimable-balances?sponsor=GXXXXX&limit=100
// GET /api/v1/silver/claimable-balances?cursor=xxx (cursor-based pagination)
func (h *SilverHandlers) HandleClaimableBalances(w http.ResponseWriter, r *http.Request) {
	// Parse cursor for pagination
	cursorStr := r.URL.Query().Get("cursor")
	cursor, err := DecodeClaimableBalanceCursor(cursorStr)
	if err != nil {
		respondError(w, "invalid cursor: "+err.Error(), http.StatusBadRequest)
		return
	}

	filters := ClaimableBalanceFilters{
		Sponsor: r.URL.Query().Get("sponsor"),
		Limit:   parseLimit(r, 100, 1000),
		Cursor:  cursor,
	}

	var balances []ClaimableBalanceCurrent
	var nextCursor string
	var hasMore bool

	if h.unifiedReader != nil {
		balances, nextCursor, hasMore, err = h.unifiedReader.GetClaimableBalances(r.Context(), filters)
	} else {
		respondError(w, "claimable-balances endpoint requires unified reader", http.StatusInternalServerError)
		return
	}

	if err != nil {
		respondError(w, err.Error(), http.StatusInternalServerError)
		return
	}

	response := map[string]interface{}{
		"claimable_balances": balances,
		"count":              len(balances),
		"has_more":           hasMore,
	}
	if nextCursor != "" {
		response["cursor"] = nextCursor
	}

	respondJSON(w, response)
}

// HandleClaimableBalanceByID returns a single claimable balance by ID
// GET /api/v1/silver/claimable-balances/{id}
func (h *SilverHandlers) HandleClaimableBalanceByID(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	balanceID := vars["id"]
	if balanceID == "" {
		respondError(w, "balance_id required", http.StatusBadRequest)
		return
	}

	var balance *ClaimableBalanceCurrent
	var err error

	if h.unifiedReader != nil {
		balance, err = h.unifiedReader.GetClaimableBalanceByID(r.Context(), balanceID)
	} else {
		respondError(w, "claimable-balances endpoint requires unified reader", http.StatusInternalServerError)
		return
	}

	if err != nil {
		respondError(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if balance == nil {
		respondError(w, "claimable balance not found", http.StatusNotFound)
		return
	}

	respondJSON(w, map[string]interface{}{
		"claimable_balance": balance,
	})
}

// HandleClaimableBalancesByAsset returns claimable balances for an asset
// GET /api/v1/silver/claimable-balances/asset?asset=USDC:GXXXXX&limit=100
func (h *SilverHandlers) HandleClaimableBalancesByAsset(w http.ResponseWriter, r *http.Request) {
	assetParam := r.URL.Query().Get("asset")
	if assetParam == "" {
		respondError(w, "asset parameter required (format: CODE:ISSUER or XLM)", http.StatusBadRequest)
		return
	}

	assetCode, assetIssuer := parseAssetParam(assetParam)
	if assetCode == "" {
		respondError(w, "invalid asset format", http.StatusBadRequest)
		return
	}

	// Parse cursor for pagination
	cursorStr := r.URL.Query().Get("cursor")
	cursor, err := DecodeClaimableBalanceCursor(cursorStr)
	if err != nil {
		respondError(w, "invalid cursor: "+err.Error(), http.StatusBadRequest)
		return
	}

	filters := ClaimableBalanceFilters{
		AssetCode:   assetCode,
		AssetIssuer: assetIssuer,
		Limit:       parseLimit(r, 100, 1000),
		Cursor:      cursor,
	}

	var balances []ClaimableBalanceCurrent
	var nextCursor string
	var hasMore bool

	if h.unifiedReader != nil {
		balances, nextCursor, hasMore, err = h.unifiedReader.GetClaimableBalances(r.Context(), filters)
	} else {
		respondError(w, "claimable-balances endpoint requires unified reader", http.StatusInternalServerError)
		return
	}

	if err != nil {
		respondError(w, err.Error(), http.StatusInternalServerError)
		return
	}

	response := map[string]interface{}{
		"claimable_balances": balances,
		"count":              len(balances),
		"has_more":           hasMore,
		"asset":              assetParam,
	}
	if nextCursor != "" {
		response["cursor"] = nextCursor
	}

	respondJSON(w, response)
}

// ============================================
// PHASE 7: EVENT TABLE ENDPOINTS
// ============================================

// HandleTrades returns paginated list of trades
// GET /api/v1/silver/trades?account_id=GXXXXX&limit=100
// GET /api/v1/silver/trades?seller_account=GXXXXX
// GET /api/v1/silver/trades?buyer_account=GXXXXX
// GET /api/v1/silver/trades?start_time=2026-01-01T00:00:00Z&end_time=2026-01-06T00:00:00Z
func (h *SilverHandlers) HandleTrades(w http.ResponseWriter, r *http.Request) {
	// Parse cursor for pagination
	cursorStr := r.URL.Query().Get("cursor")
	cursor, err := DecodeTradeCursor(cursorStr)
	if err != nil {
		respondError(w, "invalid cursor: "+err.Error(), http.StatusBadRequest)
		return
	}

	// Build filters
	filters := TradeFilters{
		AccountID:     r.URL.Query().Get("account_id"),
		SellerAccount: r.URL.Query().Get("seller_account"),
		BuyerAccount:  r.URL.Query().Get("buyer_account"),
		Limit:         parseLimit(r, 100, 1000),
		Cursor:        cursor,
	}

	// Parse time range
	if startStr := r.URL.Query().Get("start_time"); startStr != "" {
		if start, err := time.Parse(time.RFC3339, startStr); err == nil {
			filters.StartTime = start
		}
	}
	if endStr := r.URL.Query().Get("end_time"); endStr != "" {
		if end, err := time.Parse(time.RFC3339, endStr); err == nil {
			filters.EndTime = end
		}
	}

	var trades []SilverTrade
	var nextCursor string
	var hasMore bool

	if h.unifiedReader != nil {
		trades, nextCursor, hasMore, err = h.unifiedReader.GetTrades(r.Context(), filters)
	} else {
		respondError(w, "trades endpoint requires unified reader", http.StatusInternalServerError)
		return
	}

	if err != nil {
		respondError(w, err.Error(), http.StatusInternalServerError)
		return
	}

	response := map[string]interface{}{
		"trades":   trades,
		"count":    len(trades),
		"has_more": hasMore,
	}
	if nextCursor != "" {
		response["cursor"] = nextCursor
	}

	respondJSON(w, response)
}

// HandleTradesByPair returns trades for a specific trading pair
// GET /api/v1/silver/trades/by-pair?selling_asset=XLM&buying_asset=USDC:GXXXXX
func (h *SilverHandlers) HandleTradesByPair(w http.ResponseWriter, r *http.Request) {
	sellingParam := r.URL.Query().Get("selling_asset")
	buyingParam := r.URL.Query().Get("buying_asset")

	if sellingParam == "" || buyingParam == "" {
		respondError(w, "selling_asset and buying_asset parameters required (format: CODE:ISSUER or XLM)", http.StatusBadRequest)
		return
	}

	sellingCode, sellingIssuer := parseAssetParam(sellingParam)
	if sellingCode == "" {
		respondError(w, "invalid selling_asset format", http.StatusBadRequest)
		return
	}

	buyingCode, buyingIssuer := parseAssetParam(buyingParam)
	if buyingCode == "" {
		respondError(w, "invalid buying_asset format", http.StatusBadRequest)
		return
	}

	// Parse cursor for pagination
	cursorStr := r.URL.Query().Get("cursor")
	cursor, err := DecodeTradeCursor(cursorStr)
	if err != nil {
		respondError(w, "invalid cursor: "+err.Error(), http.StatusBadRequest)
		return
	}

	filters := TradeFilters{
		SellingAssetCode:   sellingCode,
		SellingAssetIssuer: sellingIssuer,
		BuyingAssetCode:    buyingCode,
		BuyingAssetIssuer:  buyingIssuer,
		Limit:              parseLimit(r, 100, 1000),
		Cursor:             cursor,
	}

	// Parse time range
	if startStr := r.URL.Query().Get("start_time"); startStr != "" {
		if start, err := time.Parse(time.RFC3339, startStr); err == nil {
			filters.StartTime = start
		}
	}
	if endStr := r.URL.Query().Get("end_time"); endStr != "" {
		if end, err := time.Parse(time.RFC3339, endStr); err == nil {
			filters.EndTime = end
		}
	}

	var trades []SilverTrade
	var nextCursor string
	var hasMore bool

	if h.unifiedReader != nil {
		trades, nextCursor, hasMore, err = h.unifiedReader.GetTrades(r.Context(), filters)
	} else {
		respondError(w, "trades endpoint requires unified reader", http.StatusInternalServerError)
		return
	}

	if err != nil {
		respondError(w, err.Error(), http.StatusInternalServerError)
		return
	}

	response := map[string]interface{}{
		"trades":   trades,
		"count":    len(trades),
		"has_more": hasMore,
		"pair": map[string]interface{}{
			"selling_asset": sellingParam,
			"buying_asset":  buyingParam,
		},
	}
	if nextCursor != "" {
		response["cursor"] = nextCursor
	}

	respondJSON(w, response)
}

// HandleTradeStats returns aggregated trade statistics
// GET /api/v1/silver/trades/stats?group_by=asset_pair&start_time=2026-01-01T00:00:00Z
func (h *SilverHandlers) HandleTradeStats(w http.ResponseWriter, r *http.Request) {
	groupBy := r.URL.Query().Get("group_by")
	if groupBy == "" {
		groupBy = "asset_pair"
	}

	// Validate group_by
	validGroupBy := map[string]bool{"asset_pair": true, "hour": true, "day": true}
	if !validGroupBy[groupBy] {
		respondError(w, "invalid group_by, must be: asset_pair, hour, or day", http.StatusBadRequest)
		return
	}

	// Parse time range (default to last 24 hours)
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

	var stats []TradeStats
	var err error

	if h.unifiedReader != nil {
		stats, err = h.unifiedReader.GetTradeStats(r.Context(), groupBy, startTime, endTime)
	} else {
		respondError(w, "trades stats endpoint requires unified reader", http.StatusInternalServerError)
		return
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

// HandleEffects returns paginated list of effects
// GET /api/v1/silver/effects?account_id=GXXXXX&limit=100
// GET /api/v1/silver/effects?effect_type=account_credited
// GET /api/v1/silver/effects?effect_type=2
func (h *SilverHandlers) HandleEffects(w http.ResponseWriter, r *http.Request) {
	// Parse cursor for pagination
	cursorStr := r.URL.Query().Get("cursor")
	cursor, err := DecodeEffectCursor(cursorStr)
	if err != nil {
		respondError(w, "invalid cursor: "+err.Error(), http.StatusBadRequest)
		return
	}

	filters := EffectFilters{
		AccountID:  r.URL.Query().Get("account_id"),
		EffectType: r.URL.Query().Get("effect_type"),
		Limit:      parseLimit(r, 100, 1000),
		Cursor:     cursor,
	}

	// Parse ledger_sequence
	if ledgerStr := r.URL.Query().Get("ledger_sequence"); ledgerStr != "" {
		if ledger, err := strconv.ParseInt(ledgerStr, 10, 64); err == nil {
			filters.LedgerSequence = ledger
		}
	}

	// Parse time range
	if startStr := r.URL.Query().Get("start_time"); startStr != "" {
		if start, err := time.Parse(time.RFC3339, startStr); err == nil {
			filters.StartTime = start
		}
	}
	if endStr := r.URL.Query().Get("end_time"); endStr != "" {
		if end, err := time.Parse(time.RFC3339, endStr); err == nil {
			filters.EndTime = end
		}
	}

	var effects []SilverEffect
	var nextCursor string
	var hasMore bool

	if h.unifiedReader != nil {
		effects, nextCursor, hasMore, err = h.unifiedReader.GetEffects(r.Context(), filters)
	} else {
		respondError(w, "effects endpoint requires unified reader", http.StatusInternalServerError)
		return
	}

	if err != nil {
		respondError(w, err.Error(), http.StatusInternalServerError)
		return
	}

	response := map[string]interface{}{
		"effects":  effects,
		"count":    len(effects),
		"has_more": hasMore,
	}
	if nextCursor != "" {
		response["cursor"] = nextCursor
	}

	respondJSON(w, response)
}

// HandleEffectTypes returns list of all effect types with counts
// GET /api/v1/silver/effects/types
func (h *SilverHandlers) HandleEffectTypes(w http.ResponseWriter, r *http.Request) {
	var types []EffectTypeCount
	var total int64
	var err error

	if h.unifiedReader != nil {
		types, total, err = h.unifiedReader.GetEffectTypes(r.Context())
	} else {
		respondError(w, "effect types endpoint requires unified reader", http.StatusInternalServerError)
		return
	}

	if err != nil {
		respondError(w, err.Error(), http.StatusInternalServerError)
		return
	}

	respondJSON(w, map[string]interface{}{
		"effect_types":  types,
		"total_effects": total,
		"generated_at":  time.Now().Format(time.RFC3339),
	})
}

// HandleEffectsByTransaction returns all effects for a specific transaction
// GET /api/v1/silver/effects/transaction/{tx_hash}
func (h *SilverHandlers) HandleEffectsByTransaction(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	txHash := vars["tx_hash"]
	if txHash == "" {
		respondError(w, "tx_hash required", http.StatusBadRequest)
		return
	}

	filters := EffectFilters{
		TransactionHash: txHash,
		Limit:           1000, // Get all effects for the transaction
	}

	var effects []SilverEffect
	var err error

	if h.unifiedReader != nil {
		effects, _, _, err = h.unifiedReader.GetEffects(r.Context(), filters)
	} else {
		respondError(w, "effects endpoint requires unified reader", http.StatusInternalServerError)
		return
	}

	if err != nil {
		respondError(w, err.Error(), http.StatusInternalServerError)
		return
	}

	respondJSON(w, map[string]interface{}{
		"transaction_hash": txHash,
		"effects":          effects,
		"count":            len(effects),
	})
}

// ============================================
// PHASE 8: SOROBAN TABLE ENDPOINTS
// ============================================

// HandleContractCode returns contract code metadata by hash
// GET /api/v1/silver/soroban/contract-code?hash=...
func (h *SilverHandlers) HandleContractCode(w http.ResponseWriter, r *http.Request) {
	hash := r.URL.Query().Get("hash")
	if hash == "" {
		respondError(w, "hash parameter required", http.StatusBadRequest)
		return
	}

	var code *ContractCode
	var err error

	if h.unifiedReader != nil {
		code, err = h.unifiedReader.GetContractCode(r.Context(), hash)
	} else {
		respondError(w, "contract-code endpoint requires unified reader", http.StatusInternalServerError)
		return
	}

	if err != nil {
		respondError(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if code == nil {
		respondError(w, "contract code not found", http.StatusNotFound)
		return
	}

	respondJSON(w, map[string]interface{}{
		"contract_code": code,
	})
}

// HandleTTL returns TTL entry for a specific key hash
// GET /api/v1/silver/soroban/ttl?key_hash=...
func (h *SilverHandlers) HandleTTL(w http.ResponseWriter, r *http.Request) {
	keyHash := r.URL.Query().Get("key_hash")
	if keyHash == "" {
		respondError(w, "key_hash parameter required", http.StatusBadRequest)
		return
	}

	var ttl *TTLEntry
	var err error

	if h.unifiedReader != nil {
		ttl, err = h.unifiedReader.GetTTL(r.Context(), keyHash)
	} else {
		respondError(w, "ttl endpoint requires unified reader", http.StatusInternalServerError)
		return
	}

	if err != nil {
		respondError(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if ttl == nil {
		respondError(w, "TTL entry not found", http.StatusNotFound)
		return
	}

	respondJSON(w, map[string]interface{}{
		"ttl": ttl,
	})
}

// HandleTTLExpiring returns TTL entries expiring within a given number of ledgers
// GET /api/v1/silver/soroban/ttl/expiring?within_ledgers=10000&limit=100
func (h *SilverHandlers) HandleTTLExpiring(w http.ResponseWriter, r *http.Request) {
	// Parse within_ledgers parameter
	withinLedgersStr := r.URL.Query().Get("within_ledgers")
	if withinLedgersStr == "" {
		respondError(w, "within_ledgers parameter required", http.StatusBadRequest)
		return
	}

	withinLedgers, err := strconv.ParseInt(withinLedgersStr, 10, 64)
	if err != nil || withinLedgers <= 0 {
		respondError(w, "within_ledgers must be a positive integer", http.StatusBadRequest)
		return
	}

	// Parse cursor for pagination
	cursorStr := r.URL.Query().Get("cursor")
	cursor, err := DecodeTTLCursor(cursorStr)
	if err != nil {
		respondError(w, "invalid cursor: "+err.Error(), http.StatusBadRequest)
		return
	}

	filters := TTLFilters{
		WithinLedgers: withinLedgers,
		Limit:         parseLimit(r, 100, 1000),
		Cursor:        cursor,
	}

	var entries []TTLEntry
	var nextCursor string
	var hasMore bool

	if h.unifiedReader != nil {
		// Get current ledger first
		currentLedger, err := h.unifiedReader.GetCurrentLedger(r.Context())
		if err != nil {
			respondError(w, "failed to get current ledger: "+err.Error(), http.StatusInternalServerError)
			return
		}

		entries, nextCursor, hasMore, err = h.unifiedReader.GetTTLExpiring(r.Context(), currentLedger, filters)
		if err != nil {
			respondError(w, err.Error(), http.StatusInternalServerError)
			return
		}
	} else {
		respondError(w, "ttl endpoint requires unified reader", http.StatusInternalServerError)
		return
	}

	response := map[string]interface{}{
		"ttl_entries":    entries,
		"count":          len(entries),
		"within_ledgers": withinLedgers,
		"has_more":       hasMore,
	}
	if nextCursor != "" {
		response["cursor"] = nextCursor
	}

	respondJSON(w, response)
}

// HandleTTLExpired returns already expired TTL entries
// GET /api/v1/silver/soroban/ttl/expired?limit=100
func (h *SilverHandlers) HandleTTLExpired(w http.ResponseWriter, r *http.Request) {
	// Parse cursor for pagination
	cursorStr := r.URL.Query().Get("cursor")
	cursor, err := DecodeTTLCursor(cursorStr)
	if err != nil {
		respondError(w, "invalid cursor: "+err.Error(), http.StatusBadRequest)
		return
	}

	filters := TTLFilters{
		ExpiredOnly: true,
		Limit:       parseLimit(r, 100, 1000),
		Cursor:      cursor,
	}

	var entries []TTLEntry
	var nextCursor string
	var hasMore bool

	if h.unifiedReader != nil {
		entries, nextCursor, hasMore, err = h.unifiedReader.GetTTLExpired(r.Context(), filters)
	} else {
		respondError(w, "ttl endpoint requires unified reader", http.StatusInternalServerError)
		return
	}

	if err != nil {
		respondError(w, err.Error(), http.StatusInternalServerError)
		return
	}

	response := map[string]interface{}{
		"ttl_entries": entries,
		"count":       len(entries),
		"has_more":    hasMore,
	}
	if nextCursor != "" {
		response["cursor"] = nextCursor
	}

	respondJSON(w, response)
}

// HandleEvictedKeys returns evicted contract keys
// GET /api/v1/silver/soroban/evicted-keys?contract_id=C...&limit=100
func (h *SilverHandlers) HandleEvictedKeys(w http.ResponseWriter, r *http.Request) {
	// Parse cursor for pagination
	cursorStr := r.URL.Query().Get("cursor")
	cursor, err := DecodeEvictionCursor(cursorStr)
	if err != nil {
		respondError(w, "invalid cursor: "+err.Error(), http.StatusBadRequest)
		return
	}

	filters := EvictionFilters{
		ContractID: r.URL.Query().Get("contract_id"),
		Limit:      parseLimit(r, 100, 1000),
		Cursor:     cursor,
	}

	var keys []EvictedKey
	var nextCursor string
	var hasMore bool

	if h.unifiedReader != nil {
		keys, nextCursor, hasMore, err = h.unifiedReader.GetEvictedKeys(r.Context(), filters)
	} else {
		respondError(w, "evicted-keys endpoint requires unified reader", http.StatusInternalServerError)
		return
	}

	if err != nil {
		respondError(w, err.Error(), http.StatusInternalServerError)
		return
	}

	response := map[string]interface{}{
		"evicted_keys": keys,
		"count":        len(keys),
		"has_more":     hasMore,
	}
	if nextCursor != "" {
		response["cursor"] = nextCursor
	}
	if filters.ContractID != "" {
		response["contract_id"] = filters.ContractID
	}

	respondJSON(w, response)
}

// HandleRestoredKeys returns restored contract keys
// GET /api/v1/silver/soroban/restored-keys?contract_id=C...&limit=100
func (h *SilverHandlers) HandleRestoredKeys(w http.ResponseWriter, r *http.Request) {
	// Parse cursor for pagination
	cursorStr := r.URL.Query().Get("cursor")
	cursor, err := DecodeEvictionCursor(cursorStr)
	if err != nil {
		respondError(w, "invalid cursor: "+err.Error(), http.StatusBadRequest)
		return
	}

	filters := EvictionFilters{
		ContractID: r.URL.Query().Get("contract_id"),
		Limit:      parseLimit(r, 100, 1000),
		Cursor:     cursor,
	}

	var keys []RestoredKey
	var nextCursor string
	var hasMore bool

	if h.unifiedReader != nil {
		keys, nextCursor, hasMore, err = h.unifiedReader.GetRestoredKeys(r.Context(), filters)
	} else {
		respondError(w, "restored-keys endpoint requires unified reader", http.StatusInternalServerError)
		return
	}

	if err != nil {
		respondError(w, err.Error(), http.StatusInternalServerError)
		return
	}

	response := map[string]interface{}{
		"restored_keys": keys,
		"count":         len(keys),
		"has_more":      hasMore,
	}
	if nextCursor != "" {
		response["cursor"] = nextCursor
	}
	if filters.ContractID != "" {
		response["contract_id"] = filters.ContractID
	}

	respondJSON(w, response)
}

// HandleSorobanConfig returns current Soroban network configuration
// GET /api/v1/silver/soroban/config
func (h *SilverHandlers) HandleSorobanConfig(w http.ResponseWriter, r *http.Request) {
	var config *SorobanConfig
	var err error

	if h.unifiedReader != nil {
		config, err = h.unifiedReader.GetSorobanConfig(r.Context())
	} else {
		respondError(w, "config endpoint requires unified reader", http.StatusInternalServerError)
		return
	}

	if err != nil {
		respondError(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if config == nil {
		respondError(w, "Soroban configuration not found", http.StatusNotFound)
		return
	}

	respondJSON(w, map[string]interface{}{
		"config": config,
	})
}

// HandleSorobanConfigLimits returns a simplified view of Soroban limits
// GET /api/v1/silver/soroban/config/limits
func (h *SilverHandlers) HandleSorobanConfigLimits(w http.ResponseWriter, r *http.Request) {
	var config *SorobanConfig
	var err error

	if h.unifiedReader != nil {
		config, err = h.unifiedReader.GetSorobanConfig(r.Context())
	} else {
		respondError(w, "config endpoint requires unified reader", http.StatusInternalServerError)
		return
	}

	if err != nil {
		respondError(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if config == nil {
		respondError(w, "Soroban configuration not found", http.StatusNotFound)
		return
	}

	// Return simplified limits view
	respondJSON(w, map[string]interface{}{
		"instructions": config.Instructions,
		"memory":       config.Memory,
		"ledger":       config.LedgerLimits,
		"transaction":  config.TxLimits,
		"contract":     config.Contract,
		"updated_at":   config.UpdatedAt,
	})
}

// HandleContractData returns contract data entries
// GET /api/v1/silver/soroban/contract-data?contract_id=C...&limit=100
// GET /api/v1/silver/soroban/contract-data?contract_id=C...&durability=persistent
func (h *SilverHandlers) HandleContractData(w http.ResponseWriter, r *http.Request) {
	contractID := r.URL.Query().Get("contract_id")
	if contractID == "" {
		respondError(w, "contract_id parameter required", http.StatusBadRequest)
		return
	}

	// Parse cursor for pagination
	cursorStr := r.URL.Query().Get("cursor")
	cursor, err := DecodeContractDataCursor(cursorStr)
	if err != nil {
		respondError(w, "invalid cursor: "+err.Error(), http.StatusBadRequest)
		return
	}

	// Validate durability if provided
	durability := r.URL.Query().Get("durability")
	if durability != "" && durability != "persistent" && durability != "temporary" {
		respondError(w, "durability must be 'persistent' or 'temporary'", http.StatusBadRequest)
		return
	}

	filters := ContractDataFilters{
		ContractID: contractID,
		KeyHash:    r.URL.Query().Get("key_hash"),
		Durability: durability,
		Limit:      parseLimit(r, 100, 1000),
		Cursor:     cursor,
	}

	var data []ContractData
	var nextCursor string
	var hasMore bool

	if h.unifiedReader != nil {
		data, nextCursor, hasMore, err = h.unifiedReader.GetContractData(r.Context(), filters)
	} else {
		respondError(w, "contract-data endpoint requires unified reader", http.StatusInternalServerError)
		return
	}

	if err != nil {
		respondError(w, err.Error(), http.StatusInternalServerError)
		return
	}

	response := map[string]interface{}{
		"contract_data": data,
		"count":         len(data),
		"has_more":      hasMore,
		"contract_id":   contractID,
	}
	if nextCursor != "" {
		response["cursor"] = nextCursor
	}
	if durability != "" {
		response["durability"] = durability
	}

	respondJSON(w, response)
}

// HandleContractDataEntry returns a single contract data entry by contract_id and key_hash
// GET /api/v1/silver/soroban/contract-data/entry?contract_id=C...&key_hash=...
func (h *SilverHandlers) HandleContractDataEntry(w http.ResponseWriter, r *http.Request) {
	contractID := r.URL.Query().Get("contract_id")
	keyHash := r.URL.Query().Get("key_hash")

	if contractID == "" || keyHash == "" {
		respondError(w, "contract_id and key_hash parameters required", http.StatusBadRequest)
		return
	}

	filters := ContractDataFilters{
		ContractID: contractID,
		KeyHash:    keyHash,
		Limit:      1,
	}

	var data []ContractData
	var err error

	if h.unifiedReader != nil {
		data, _, _, err = h.unifiedReader.GetContractData(r.Context(), filters)
	} else {
		respondError(w, "contract-data endpoint requires unified reader", http.StatusInternalServerError)
		return
	}

	if err != nil {
		respondError(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if len(data) == 0 {
		respondError(w, "contract data entry not found", http.StatusNotFound)
		return
	}

	respondJSON(w, map[string]interface{}{
		"contract_data": data[0],
	})
}

// ============================================
// HELPER FUNCTIONS
// ============================================

// parseAssetParam parses an asset parameter in format CODE:ISSUER or XLM/native
func parseAssetParam(assetParam string) (code string, issuer string) {
	if assetParam == "XLM" || assetParam == "native" {
		return "XLM", ""
	}
	parts := strings.SplitN(assetParam, ":", 2)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return "", ""
	}
	return parts[0], parts[1]
}

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
