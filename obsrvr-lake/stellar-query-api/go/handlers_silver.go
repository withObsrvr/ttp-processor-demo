package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"net/http"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/mux"
	"github.com/stellar/go/strkey"
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

func normalizeTTLEntryForCurrentLedger(entry *TTLEntry, currentLedger int64) {
	if entry == nil {
		return
	}
	entry.LedgersRemaining = entry.LiveUntilLedger - currentLedger
	entry.Expired = entry.LedgersRemaining <= 0
}

func normalizeTTLEntriesForCurrentLedger(entries []TTLEntry, currentLedger int64) {
	for i := range entries {
		normalizeTTLEntryForCurrentLedger(&entries[i], currentLedger)
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
// @Summary Get current account state
// @Description Returns the current state of a Stellar account including balance, sequence number, and subentries
// @Tags Accounts
// @Accept json
// @Produce json
// @Param account_id query string true "Stellar account ID (G...)"
// @Success 200 {object} map[string]interface{} "Account data"
// @Failure 400 {object} map[string]interface{} "Missing or invalid account_id"
// @Failure 404 {object} map[string]interface{} "Account not found"
// @Failure 500 {object} map[string]interface{} "Internal server error"
// @Router /api/v1/silver/accounts/current [get]
func (h *SilverHandlers) HandleAccountCurrent(w http.ResponseWriter, r *http.Request) {
	accountID := r.URL.Query().Get("account_id")
	if accountID == "" {
		respondError(w, "account_id required", http.StatusBadRequest)
		return
	}

	var account *AccountCurrent
	var err error

	// Fast path: serving projection in silver_hot. This avoids DuckDB federation
	// and broad silver scans for the explorer's hottest account lookup path.
	if h.legacyReader != nil && h.legacyReader.hot != nil {
		account, err = h.legacyReader.hot.GetServingAccountCurrent(r.Context(), accountID)
		if err == nil && account != nil {
			respondJSON(w, map[string]interface{}{"account": account})
			return
		}
	}

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
// @Summary Get account history
// @Description Returns historical snapshots of an account's state over time with cursor-based pagination
// @Tags Accounts
// @Accept json
// @Produce json
// @Param account_id query string true "Stellar account ID (G...)"
// @Param limit query int false "Maximum results to return (default: 50, max: 500)"
// @Param cursor query string false "Pagination cursor from previous response"
// @Success 200 {object} map[string]interface{} "Account history with pagination info"
// @Failure 400 {object} map[string]interface{} "Missing or invalid parameters"
// @Failure 500 {object} map[string]interface{} "Internal server error"
// @Router /api/v1/silver/accounts/history [get]
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
// @Summary Get top accounts by balance
// @Description Returns accounts with the highest XLM balances, useful for leaderboards
// @Tags Accounts
// @Accept json
// @Produce json
// @Param limit query int false "Maximum results to return (default: 100, max: 1000)"
// @Success 200 {object} map[string]interface{} "List of top accounts"
// @Failure 500 {object} map[string]interface{} "Internal server error"
// @Router /api/v1/silver/accounts/top [get]
func (h *SilverHandlers) HandleTopAccounts(w http.ResponseWriter, r *http.Request) {
	limit := parseLimit(r, 100, 1000)

	var accounts []AccountCurrent
	var err error

	// Fast path: serving projection in silver_hot. Top accounts is a leaderboard
	// query that should be an indexed read, not a federated hot+cold query.
	if h.legacyReader != nil && h.legacyReader.hot != nil {
		accounts, err = h.legacyReader.hot.GetServingTopAccounts(r.Context(), limit)
		if err == nil && len(accounts) > 0 {
			respondJSON(w, map[string]interface{}{
				"accounts": accounts,
				"count":    len(accounts),
			})
			return
		}
	}

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
// @Summary List all accounts
// @Description Returns a paginated list of all Stellar accounts with sorting and filtering options
// @Tags Accounts
// @Accept json
// @Produce json
// @Param limit query int false "Maximum results to return (default: 100, max: 1000)"
// @Param cursor query string false "Pagination cursor from previous response"
// @Param sort_by query string false "Sort field: balance (default)"
// @Param order query string false "Sort order: asc, desc (default)"
// @Param min_balance query int false "Minimum balance filter in stroops"
// @Success 200 {object} map[string]interface{} "List of accounts with pagination info"
// @Failure 400 {object} map[string]interface{} "Invalid parameters or cursor mismatch"
// @Failure 500 {object} map[string]interface{} "Internal server error"
// @Router /api/v1/silver/accounts [get]
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
// @Summary Get account signers
// @Description Returns the signers and thresholds for a Stellar account in Horizon-compatible format
// @Tags Accounts
// @Accept json
// @Produce json
// @Param account_id query string true "Stellar account ID (G...)"
// @Success 200 {object} AccountSignersResponse "Account signers and thresholds"
// @Failure 400 {object} map[string]interface{} "Missing account_id"
// @Failure 404 {object} map[string]interface{} "Account not found"
// @Failure 500 {object} map[string]interface{} "Internal server error"
// @Router /api/v1/silver/accounts/signers [get]
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

// HandleAssetList returns a paginated list of all assets on the network
// @Summary List all assets
// @Description Returns a paginated list of all assets on the Stellar network with holder counts, supply, and 24h activity
// @Tags Assets
// @Accept json
// @Produce json
// @Param limit query int false "Maximum results to return (default: 100, max: 1000)"
// @Param cursor query string false "Pagination cursor from previous response"
// @Param sort_by query string false "Sort field: holder_count (default), volume_24h, transfers_24h, circulating_supply"
// @Param order query string false "Sort order: asc, desc (default)"
// @Param min_holders query int false "Minimum holder count filter"
// @Param min_volume_24h query int false "Minimum 24h volume in stroops"
// @Param asset_type query string false "Asset type: credit_alphanum4, credit_alphanum12, native"
// @Param search query string false "Search by asset code prefix"
// @Success 200 {object} AssetListResponse "List of assets with pagination info"
// @Failure 400 {object} map[string]interface{} "Invalid parameters or cursor mismatch"
// @Failure 500 {object} map[string]interface{} "Internal server error"
// @Router /api/v1/silver/assets [get]
func (h *SilverHandlers) HandleAssetList(w http.ResponseWriter, r *http.Request) {
	// Parse cursor for pagination
	cursorStr := r.URL.Query().Get("cursor")
	cursor, err := DecodeAssetListCursor(cursorStr)
	if err != nil {
		respondError(w, "invalid cursor: "+err.Error(), http.StatusBadRequest)
		return
	}

	// Build filters
	filters := AssetListFilters{
		SortBy:    r.URL.Query().Get("sort_by"),
		SortOrder: r.URL.Query().Get("order"),
		AssetType: r.URL.Query().Get("asset_type"),
		Search:    r.URL.Query().Get("search"),
		Limit:     parseLimit(r, 100, 1000),
		Cursor:    cursor,
	}

	// Parse minimum holders filter
	if minHoldersStr := r.URL.Query().Get("min_holders"); minHoldersStr != "" {
		minHolders, err := strconv.ParseInt(minHoldersStr, 10, 64)
		if err == nil && minHolders >= 0 {
			filters.MinHolders = &minHolders
		}
	}

	// Parse minimum 24h volume filter (in stroops)
	if minVolStr := r.URL.Query().Get("min_volume_24h"); minVolStr != "" {
		minVol, err := strconv.ParseInt(minVolStr, 10, 64)
		if err == nil && minVol >= 0 {
			filters.MinVolume24h = &minVol
		}
	}

	// Default sort by holder_count descending
	if filters.SortBy == "" {
		filters.SortBy = "holder_count"
	}
	if filters.SortOrder == "" {
		filters.SortOrder = "desc"
	}

	// Validate cursor sort params match request sort params
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

	// Fast path: serving projection first. Fall back to the pre-existing legacy
	// hot PG path if serving asset tables are not populated yet.
	var response *AssetListResponse
	var queryErr error

	if h.legacyReader != nil && h.legacyReader.hot != nil {
		response, queryErr = h.legacyReader.hot.GetServingAssetList(r.Context(), filters)
		if queryErr == nil && response != nil && len(response.Assets) > 0 {
			respondJSON(w, response)
			return
		}
	}

	if h.legacyReader != nil {
		response, queryErr = h.legacyReader.GetAssetList(r.Context(), filters)
	} else {
		response, queryErr = h.unifiedReader.GetAssetList(r.Context(), filters)
	}

	if queryErr != nil {
		respondError(w, queryErr.Error(), http.StatusInternalServerError)
		return
	}

	respondJSON(w, response)
}

// HandleAssetDetail returns a composite asset detail response for classic
// assets, XLM, and token contract IDs that can be served with currently
// available query-api data sources.
func (h *SilverHandlers) HandleAssetDetail(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	slug := vars["asset"]
	if slug == "" {
		respondError(w, "asset parameter required", http.StatusBadRequest)
		return
	}

	ref, err := parseAssetSlug(slug)
	if err != nil {
		respondError(w, err.Error(), http.StatusBadRequest)
		return
	}

	resp := &AssetDetailResponse{
		Asset:         ref.AssetInfo(),
		CanonicalSlug: ref.CanonicalSlug(),
		GeneratedAt:   time.Now().UTC().Format(time.RFC3339),
	}

	if ref.IsContract {
		if h.unifiedReader == nil {
			respondError(w, "contract asset detail requires unified reader", http.StatusServiceUnavailable)
			return
		}

		meta, err := h.unifiedReader.GetSEP41TokenMetadata(r.Context(), ref.ContractID)
		if err != nil {
			respondError(w, err.Error(), http.StatusInternalServerError)
			return
		}
		stats, err := h.unifiedReader.GetSEP41TokenStats(r.Context(), ref.ContractID)
		if err != nil {
			respondError(w, err.Error(), http.StatusInternalServerError)
			return
		}
		balances, _, _, err := h.unifiedReader.GetSEP41Balances(r.Context(), SEP41BalanceFilters{ContractID: ref.ContractID, Limit: 10, Order: "desc"})
		if err != nil {
			respondError(w, err.Error(), http.StatusInternalServerError)
			return
		}

		resp.DisplayName = meta.Name
		if resp.DisplayName == nil {
			resp.DisplayName = meta.Symbol
		}
		resp.Symbol = meta.Symbol
		resp.TokenType = &meta.TokenType
		resp.Decimals = &meta.Decimals
		resp.Stats = stats
		resp.TopHolders = balances
		resp.RecentTransfers, _ = h.queryRecentAssetTransfers(r.Context(), ref, 20)

		if meta.AssetCode != nil {
			classicRef := assetRef{AssetCode: *meta.AssetCode, IsNative: *meta.AssetCode == "XLM" || *meta.AssetCode == "native"}
			if meta.AssetIssuer != nil {
				classicRef.AssetIssuer = *meta.AssetIssuer
			}
			resp.LinkedContractID = &ref.ContractID
			if meta.AssetIssuer != nil || classicRef.IsNative {
				resp.Asset = ref.AssetInfo()
				resp.LinkedTokens = []LinkedTokenSummary{{ContractID: ref.ContractID, TokenType: meta.TokenType, TokenName: meta.Name, TokenSymbol: meta.Symbol, TokenDecimals: &meta.Decimals}}
			}
			if meta.AssetIssuer != nil {
				pairs, _ := h.queryTopAssetPairs(r.Context(), assetRef{AssetCode: *meta.AssetCode, AssetIssuer: *meta.AssetIssuer}, 5)
				resp.TopPairs = pairs
			}
		}

		respondJSON(w, resp)
		return
	}

	stats, err := h.getClassicAssetStats(r.Context(), ref)
	if err != nil {
		respondError(w, err.Error(), http.StatusInternalServerError)
		return
	}
	resp.Stats = stats

	holders, err := h.getClassicAssetTopHolders(r.Context(), ref, 10)
	if err != nil {
		respondError(w, err.Error(), http.StatusInternalServerError)
		return
	}
	resp.TopHolders = holders
	resp.RecentTransfers, _ = h.queryRecentAssetTransfers(r.Context(), ref, 20)
	resp.Issuer, _ = h.queryIssuerMetadata(r.Context(), ref.AssetIssuer)
	resp.LinkedTokens, _ = h.queryLinkedTokens(r.Context(), ref)
	if len(resp.LinkedTokens) > 0 {
		resp.LinkedContractID = &resp.LinkedTokens[0].ContractID
		resp.TokenType = &resp.LinkedTokens[0].TokenType
		if resp.DisplayName == nil {
			resp.DisplayName = resp.LinkedTokens[0].TokenName
		}
		if resp.Symbol == nil {
			resp.Symbol = resp.LinkedTokens[0].TokenSymbol
		}
		if resp.Decimals == nil {
			resp.Decimals = resp.LinkedTokens[0].TokenDecimals
		}
	}
	resp.TopPairs, _ = h.queryTopAssetPairs(r.Context(), ref, 5)
	if h.unifiedReader != nil {
		pools, _, _, err := h.unifiedReader.GetLiquidityPools(r.Context(), LiquidityPoolFilters{AssetCode: ref.AssetCode, AssetIssuer: ref.AssetIssuer, Limit: 5})
		if err == nil {
			resp.LiquidityPools = pools
		}
	}

	respondJSON(w, resp)
}

// HandleAssetLinks returns observed classic↔token linkage information that can
// be derived from token_registry today.
func (h *SilverHandlers) HandleAssetLinks(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	slug := vars["asset"]
	if slug == "" {
		respondError(w, "asset parameter required", http.StatusBadRequest)
		return
	}

	ref, err := parseAssetSlug(slug)
	if err != nil {
		respondError(w, err.Error(), http.StatusBadRequest)
		return
	}

	response := map[string]any{
		"asset":          ref.AssetInfo(),
		"canonical_slug": ref.CanonicalSlug(),
		"generated_at":   time.Now().UTC().Format(time.RFC3339),
	}

	if ref.IsContract {
		if h.unifiedReader == nil {
			respondError(w, "contract asset links require unified reader", http.StatusServiceUnavailable)
			return
		}
		meta, err := h.unifiedReader.GetSEP41TokenMetadata(r.Context(), ref.ContractID)
		if err != nil {
			respondError(w, err.Error(), http.StatusInternalServerError)
			return
		}
		response["linked_tokens"] = []LinkedTokenSummary{{
			ContractID:    ref.ContractID,
			TokenType:     meta.TokenType,
			TokenName:     meta.Name,
			TokenSymbol:   meta.Symbol,
			TokenDecimals: &meta.Decimals,
		}}
		if meta.AssetCode != nil {
			classic := map[string]any{"asset_code": *meta.AssetCode, "canonical_slug": *meta.AssetCode}
			if meta.AssetIssuer != nil && *meta.AssetIssuer != "" {
				classic["asset_issuer"] = *meta.AssetIssuer
				classic["canonical_slug"] = *meta.AssetCode + ":" + *meta.AssetIssuer
			}
			response["linked_classic_asset"] = classic
		}
		respondJSON(w, response)
		return
	}

	links, err := h.queryLinkedTokens(r.Context(), ref)
	if err != nil {
		respondError(w, err.Error(), http.StatusInternalServerError)
		return
	}
	response["linked_tokens"] = links
	respondJSON(w, response)
}

// HandleAssetPairs returns pair/trade and liquidity-pool relationships for an asset.
func (h *SilverHandlers) HandleAssetPairs(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	slug := vars["asset"]
	if slug == "" {
		respondError(w, "asset parameter required", http.StatusBadRequest)
		return
	}

	ref, err := parseAssetSlug(slug)
	if err != nil {
		respondError(w, err.Error(), http.StatusBadRequest)
		return
	}

	limit := parseLimit(r, 10, 100)
	pairs, err := h.queryTopAssetPairs(r.Context(), ref, limit)
	if err != nil {
		respondError(w, err.Error(), http.StatusInternalServerError)
		return
	}

	response := map[string]any{
		"asset":          ref.AssetInfo(),
		"canonical_slug": ref.CanonicalSlug(),
		"pairs":          pairs,
		"pair_count":     len(pairs),
		"generated_at":   time.Now().UTC().Format(time.RFC3339),
	}

	if !ref.IsContract && h.unifiedReader != nil {
		if pools, _, _, err := h.unifiedReader.GetLiquidityPools(r.Context(), LiquidityPoolFilters{
			AssetCode: ref.AssetCode, AssetIssuer: ref.AssetIssuer, Limit: limit,
		}); err == nil {
			response["liquidity_pools"] = pools
			response["liquidity_pool_count"] = len(pools)
		}
	}

	respondJSON(w, response)
}

// HandleTokenStats returns aggregated statistics for a specific token
// @Summary Get token statistics
// @Description Returns aggregated statistics for a specific token including holder count, supply, and 24h activity
// @Tags Assets
// @Accept json
// @Produce json
// @Param asset path string true "Asset identifier: XLM or CODE:ISSUER"
// @Success 200 {object} TokenStatsResponse "Token statistics"
// @Failure 400 {object} map[string]interface{} "Invalid asset format"
// @Failure 500 {object} map[string]interface{} "Internal server error"
// @Router /api/v1/silver/assets/{asset}/stats [get]
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

	// Fast path: serving projection first.
	if h.legacyReader != nil && h.legacyReader.hot != nil {
		response, err = h.legacyReader.hot.GetServingTokenStats(r.Context(), assetCode, assetIssuer)
		if err == nil && response != nil {
			respondJSON(w, response)
			return
		}
	}

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
// @Summary Get token holders
// @Description Returns a paginated list of accounts holding a specific token, sorted by balance
// @Tags Assets
// @Accept json
// @Produce json
// @Param asset path string true "Asset identifier: XLM or CODE:ISSUER"
// @Param limit query int false "Maximum results to return (default: 100, max: 1000)"
// @Param cursor query string false "Pagination cursor from previous response"
// @Param min_balance query int false "Minimum balance filter in stroops"
// @Success 200 {object} TokenHoldersResponse "Token holders with pagination info"
// @Failure 400 {object} map[string]interface{} "Invalid asset format or cursor"
// @Failure 500 {object} map[string]interface{} "Internal server error"
// @Router /api/v1/silver/assets/{asset}/holders [get]
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

	// Fast path: serving projection first.
	if h.legacyReader != nil && h.legacyReader.hot != nil {
		response, err = h.legacyReader.hot.GetServingTokenHolders(r.Context(), filters)
		if err == nil && response != nil {
			respondJSON(w, response)
			return
		}
	}

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
// @Summary Get account balances
// @Description Returns all balances including XLM and trustlines for a Stellar account
// @Tags Accounts
// @Accept json
// @Produce json
// @Param id path string true "Stellar account ID (G...)"
// @Success 200 {object} AccountBalancesResponse "Account balances"
// @Failure 400 {object} map[string]interface{} "Missing account_id"
// @Failure 404 {object} map[string]interface{} "Account not found"
// @Failure 500 {object} map[string]interface{} "Internal server error"
// @Router /api/v1/silver/accounts/{id}/balances [get]
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

	// Fast path: serving projection in silver_hot. Account balances is one of the
	// primary endpoints that timed out through unified DuckDB hot+cold federation.
	if h.legacyReader != nil && h.legacyReader.hot != nil {
		response, err = h.legacyReader.hot.GetServingAccountBalances(r.Context(), accountID)
		if err == nil && response != nil {
			respondJSON(w, response)
			return
		}
	}

	// Fall back to legacy unified DuckDB path until all serving projections exist.
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

// HandleAccountContracts returns contract interactions for a specific account
// @Summary Get account contract interactions
// @Description Returns contracts an account has interacted with, including call counts and token metadata
// @Tags Accounts
// @Accept json
// @Produce json
// @Param id path string true "Stellar account ID (G...)"
// @Param limit query int false "Maximum results to return (default: 50, max: 200)"
// @Success 200 {object} map[string]interface{} "Account contract interactions"
// @Failure 400 {object} map[string]interface{} "Missing account_id"
// @Failure 500 {object} map[string]interface{} "Internal server error"
// @Router /api/v1/silver/accounts/{id}/contracts [get]
func (h *SilverHandlers) HandleAccountContracts(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	accountID := vars["id"]
	if accountID == "" {
		accountID = r.URL.Query().Get("account_id")
	}
	if accountID == "" {
		respondError(w, "account_id required", http.StatusBadRequest)
		return
	}

	limit := parseLimit(r, 50, 200)

	// Query silver_hot via the legacy reader's hot connection
	if h.legacyReader == nil || h.legacyReader.hot == nil {
		respondError(w, "silver hot reader not available", http.StatusInternalServerError)
		return
	}

	ctx := r.Context()
	query := `
		SELECT
			ci.contract_id,
			COUNT(*) as total_calls,
			(SELECT ci2.function_name FROM contract_invocations_raw ci2
			 WHERE ci2.contract_id = ci.contract_id AND ci2.source_account = ci.source_account
			 GROUP BY ci2.function_name ORDER BY COUNT(*) DESC LIMIT 1) as top_function,
			MAX(ci.closed_at)::text as last_called,
			tr.token_name
		FROM contract_invocations_raw ci
		LEFT JOIN token_registry tr ON ci.contract_id = tr.contract_id
		WHERE ci.source_account = $1
		GROUP BY ci.contract_id, ci.source_account, tr.token_name
		ORDER BY total_calls DESC
		LIMIT $2
	`

	rows, err := h.legacyReader.hot.db.QueryContext(ctx, query, accountID, limit)
	if err != nil {
		respondError(w, "query failed: "+err.Error(), http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	var contracts []map[string]interface{}
	for rows.Next() {
		var contractID string
		var totalCalls int64
		var topFunction, lastCalled, tokenName sql.NullString

		if err := rows.Scan(&contractID, &totalCalls, &topFunction, &lastCalled, &tokenName); err != nil {
			respondError(w, "scan failed: "+err.Error(), http.StatusInternalServerError)
			return
		}

		entry := map[string]interface{}{
			"contract_id": contractID,
			"total_calls": totalCalls,
		}
		if topFunction.Valid {
			entry["top_function"] = topFunction.String
		}
		if lastCalled.Valid {
			entry["last_called"] = lastCalled.String
		}
		if tokenName.Valid {
			entry["token_name"] = tokenName.String
		}

		contracts = append(contracts, entry)
	}
	if err := rows.Err(); err != nil {
		respondError(w, "rows error: "+err.Error(), http.StatusInternalServerError)
		return
	}

	if contracts == nil {
		contracts = []map[string]interface{}{}
	}

	respondJSON(w, map[string]interface{}{
		"account_id":      accountID,
		"contracts":       contracts,
		"total_contracts": len(contracts),
	})
}

// ============================================
// OPERATIONS ENDPOINTS (Enriched)
// ============================================

// HandleEnrichedOperations returns enriched operations with full context
// @Summary Get enriched operations
// @Description Returns operations with full context including transaction details, account info, and asset metadata
// @Tags Operations
// @Accept json
// @Produce json
// @Param account_id query string false "Filter by account ID"
// @Param tx_hash query string false "Filter by transaction hash"
// @Param payments_only query bool false "Filter for payment operations only"
// @Param soroban_only query bool false "Filter for Soroban operations only"
// @Param start_ledger query int false "Start ledger sequence (mutually exclusive with cursor)"
// @Param end_ledger query int false "End ledger sequence"
// @Param limit query int false "Maximum results to return (default: 100, max: 1000)"
// @Param cursor query string false "Pagination cursor from previous response"
// @Param order query string false "Sort order: asc or desc (default: desc)"
// @Success 200 {object} map[string]interface{} "Enriched operations with pagination info"
// @Failure 400 {object} map[string]interface{} "Invalid parameters"
// @Failure 500 {object} map[string]interface{} "Internal server error"
// @Router /api/v1/silver/operations/enriched [get]
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

	// Parse and validate order parameter (default: desc for backward compatibility)
	order := strings.ToLower(r.URL.Query().Get("order"))
	if order == "" {
		order = "desc"
	}
	if order != "asc" && order != "desc" {
		respondError(w, "order must be 'asc' or 'desc'", http.StatusBadRequest)
		return
	}

	// Validate cursor order matches request order (cannot change order while paginating)
	if cursor != nil && cursor.Order != "" && cursor.Order != order {
		respondError(w, "cursor was created with order='"+cursor.Order+"' but request uses order='"+order+"'. Cannot change order while paginating.", http.StatusBadRequest)
		return
	}

	filters := OperationFilters{
		AccountID:    r.URL.Query().Get("account_id"),
		TxHash:       r.URL.Query().Get("tx_hash"),
		PaymentsOnly: r.URL.Query().Get("payments_only") == "true",
		SorobanOnly:  r.URL.Query().Get("soroban_only") == "true",
		Limit:        parseLimit(r, 100, 1000),
		Cursor:       cursor,
		Order:        order,
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

	// Build _meta for RPC v2 compatibility
	meta := ResponseMeta{}
	if len(operations) > 0 {
		// scanned_ledger is the max ledger sequence in the result set
		var maxLedger int64
		for _, op := range operations {
			if op.LedgerSequence > maxLedger {
				maxLedger = op.LedgerSequence
			}
		}
		meta.ScannedLedger = &maxLedger
	}
	// Get available ledgers (only if unified reader is available)
	if h.unifiedReader != nil {
		if availableLedgers, err := h.unifiedReader.GetAvailableLedgers(r.Context()); err == nil {
			meta.AvailableLedgers = availableLedgers
		}
	}

	response := map[string]interface{}{
		"operations": operations,
		"count":      len(operations),
		"filters":    filters,
		"has_more":   hasMore,
		"_meta":      meta,
	}
	if nextCursor != "" {
		response["cursor"] = nextCursor
	}

	respondJSON(w, response)
}

// HandlePayments is a convenience endpoint for payments only
// @Summary Get payment operations
// @Description Returns payment operations (payment, path_payment, etc.) filtered by account
// @Tags Operations
// @Accept json
// @Produce json
// @Param account_id query string false "Filter by account ID"
// @Param limit query int false "Maximum results to return (default: 50, max: 500)"
// @Param cursor query string false "Pagination cursor from previous response"
// @Param order query string false "Sort order: asc or desc (default: desc)"
// @Success 200 {object} map[string]interface{} "Payment operations with pagination info"
// @Failure 400 {object} map[string]interface{} "Invalid cursor"
// @Failure 500 {object} map[string]interface{} "Internal server error"
// @Router /api/v1/silver/payments [get]
func (h *SilverHandlers) HandlePayments(w http.ResponseWriter, r *http.Request) {
	// Parse cursor for pagination
	cursorStr := r.URL.Query().Get("cursor")
	cursor, err := DecodeOperationCursor(cursorStr)
	if err != nil {
		respondError(w, "invalid cursor: "+err.Error(), http.StatusBadRequest)
		return
	}

	// Parse and validate order parameter (default: desc for backward compatibility)
	order := strings.ToLower(r.URL.Query().Get("order"))
	if order == "" {
		order = "desc"
	}
	if order != "asc" && order != "desc" {
		respondError(w, "order must be 'asc' or 'desc'", http.StatusBadRequest)
		return
	}

	// Validate cursor order matches request order
	if cursor != nil && cursor.Order != "" && cursor.Order != order {
		respondError(w, "cursor was created with order='"+cursor.Order+"' but request uses order='"+order+"'. Cannot change order while paginating.", http.StatusBadRequest)
		return
	}

	filters := OperationFilters{
		AccountID:    r.URL.Query().Get("account_id"),
		PaymentsOnly: true,
		Limit:        parseLimit(r, 50, 500),
		Cursor:       cursor,
		Order:        order,
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

	// Build _meta for RPC v2 compatibility
	meta := ResponseMeta{}
	if len(operations) > 0 {
		var maxLedger int64
		for _, op := range operations {
			if op.LedgerSequence > maxLedger {
				maxLedger = op.LedgerSequence
			}
		}
		meta.ScannedLedger = &maxLedger
	}
	if h.unifiedReader != nil {
		if availableLedgers, err := h.unifiedReader.GetAvailableLedgers(r.Context()); err == nil {
			meta.AvailableLedgers = availableLedgers
		}
	}

	response := map[string]interface{}{
		"payments": operations,
		"count":    len(operations),
		"has_more": hasMore,
		"_meta":    meta,
	}
	if nextCursor != "" {
		response["cursor"] = nextCursor
	}

	respondJSON(w, response)
}

// HandleSorobanOperations is a convenience endpoint for Soroban operations only
// @Summary Get Soroban operations
// @Description Returns Soroban smart contract operations (invoke_host_function, etc.)
// @Tags Operations
// @Accept json
// @Produce json
// @Param account_id query string false "Filter by account ID"
// @Param limit query int false "Maximum results to return (default: 50, max: 500)"
// @Param cursor query string false "Pagination cursor from previous response"
// @Param order query string false "Sort order: asc or desc (default: desc)"
// @Success 200 {object} map[string]interface{} "Soroban operations with pagination info"
// @Failure 400 {object} map[string]interface{} "Invalid cursor"
// @Failure 500 {object} map[string]interface{} "Internal server error"
// @Router /api/v1/silver/operations/soroban [get]
func (h *SilverHandlers) HandleSorobanOperations(w http.ResponseWriter, r *http.Request) {
	// Parse cursor for pagination
	cursorStr := r.URL.Query().Get("cursor")
	cursor, err := DecodeOperationCursor(cursorStr)
	if err != nil {
		respondError(w, "invalid cursor: "+err.Error(), http.StatusBadRequest)
		return
	}

	// Parse and validate order parameter (default: desc for backward compatibility)
	order := strings.ToLower(r.URL.Query().Get("order"))
	if order == "" {
		order = "desc"
	}
	if order != "asc" && order != "desc" {
		respondError(w, "order must be 'asc' or 'desc'", http.StatusBadRequest)
		return
	}

	// Validate cursor order matches request order
	if cursor != nil && cursor.Order != "" && cursor.Order != order {
		respondError(w, "cursor was created with order='"+cursor.Order+"' but request uses order='"+order+"'. Cannot change order while paginating.", http.StatusBadRequest)
		return
	}

	filters := OperationFilters{
		AccountID:   r.URL.Query().Get("account_id"),
		SorobanOnly: true,
		Limit:       parseLimit(r, 50, 500),
		Cursor:      cursor,
		Order:       order,
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

	// Build _meta for RPC v2 compatibility
	meta := ResponseMeta{}
	if len(operations) > 0 {
		var maxLedger int64
		for _, op := range operations {
			if op.LedgerSequence > maxLedger {
				maxLedger = op.LedgerSequence
			}
		}
		meta.ScannedLedger = &maxLedger
	}
	if h.unifiedReader != nil {
		if availableLedgers, err := h.unifiedReader.GetAvailableLedgers(r.Context()); err == nil {
			meta.AvailableLedgers = availableLedgers
		}
	}

	response := map[string]interface{}{
		"soroban_operations": operations,
		"count":              len(operations),
		"has_more":           hasMore,
		"_meta":              meta,
	}
	if nextCursor != "" {
		response["cursor"] = nextCursor
	}

	respondJSON(w, response)
}

// HandleSorobanOpsByFunction returns Soroban operations filtered by contract and function name
// @Summary Get Soroban operations by function
// @Description Returns Soroban invoke_host_function operations filtered by contract_id and/or function_name
// @Tags Operations
// @Accept json
// @Produce json
// @Param contract_id query string false "Contract ID to filter by"
// @Param function_name query string false "Function name to filter by"
// @Param limit query int false "Number of results" default(50)
// @Param cursor query string false "Pagination cursor"
// @Param order query string false "Sort order" default(desc) Enums(asc, desc)
// @Success 200 {object} map[string]interface{} "Filtered Soroban operations"
// @Failure 400 {object} map[string]interface{} "Invalid parameters"
// @Failure 500 {object} map[string]interface{} "Internal server error"
// @Router /api/v1/silver/operations/soroban/by-function [get]
// @Router /api/v1/silver/calls [get]
func (h *SilverHandlers) HandleSorobanOpsByFunction(w http.ResponseWriter, r *http.Request) {
	contractID := r.URL.Query().Get("contract_id")
	functionName := r.URL.Query().Get("function_name")

	if contractID == "" && functionName == "" {
		respondError(w, "at least one of contract_id or function_name is required", http.StatusBadRequest)
		return
	}

	cursorStr := r.URL.Query().Get("cursor")
	cursor, err := DecodeOperationCursor(cursorStr)
	if err != nil {
		respondError(w, "invalid cursor: "+err.Error(), http.StatusBadRequest)
		return
	}

	order := strings.ToLower(r.URL.Query().Get("order"))
	if order == "" {
		order = "desc"
	}
	if order != "asc" && order != "desc" {
		respondError(w, "order must be 'asc' or 'desc'", http.StatusBadRequest)
		return
	}

	if cursor != nil && cursor.Order != "" && cursor.Order != order {
		respondError(w, "cursor was created with order='"+cursor.Order+"' but request uses order='"+order+"'. Cannot change order while paginating.", http.StatusBadRequest)
		return
	}

	filters := OperationFilters{
		SorobanOnly:     true,
		ContractID:      contractID,
		SorobanFunction: functionName,
		Limit:           parseLimit(r, 50, 500),
		Cursor:          cursor,
		Order:           order,
	}

	var operations []EnrichedOperation
	var nextCursor string
	var hasMore bool

	switch h.readerMode {
	case ReaderModeUnified:
		operations, nextCursor, hasMore, err = h.unifiedReader.GetEnrichedOperationsWithCursor(r.Context(), filters)
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
	if contractID != "" {
		response["contract_id"] = contractID
	}
	if functionName != "" {
		response["function_name"] = functionName
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
// @Summary Get token transfers
// @Description Returns unified token transfers from both classic payments and Soroban contract transfers
// @Tags Transfers
// @Accept json
// @Produce json
// @Param asset_code query string false "Filter by asset code"
// @Param from_account query string false "Filter by sender account"
// @Param to_account query string false "Filter by recipient account"
// @Param source_type query string false "Filter by source type: classic, soroban"
// @Param start_time query string false "Start time in RFC3339 format (default: 24h ago)"
// @Param end_time query string false "End time in RFC3339 format (default: now)"
// @Param limit query int false "Maximum results to return (default: 100, max: 1000)"
// @Param cursor query string false "Pagination cursor from previous response"
// @Param order query string false "Sort order: asc or desc (default: desc)"
// @Success 200 {object} map[string]interface{} "Token transfers with pagination info"
// @Failure 400 {object} map[string]interface{} "Invalid cursor"
// @Failure 500 {object} map[string]interface{} "Internal server error"
// @Router /api/v1/silver/transfers [get]
func (h *SilverHandlers) HandleTokenTransfers(w http.ResponseWriter, r *http.Request) {
	// Parse cursor for pagination
	cursorStr := r.URL.Query().Get("cursor")
	cursor, err := DecodeTransferCursor(cursorStr)
	if err != nil {
		respondError(w, "invalid cursor: "+err.Error(), http.StatusBadRequest)
		return
	}

	// Parse and validate order parameter (default: desc for backward compatibility)
	order := strings.ToLower(r.URL.Query().Get("order"))
	if order == "" {
		order = "desc"
	}
	if order != "asc" && order != "desc" {
		respondError(w, "order must be 'asc' or 'desc'", http.StatusBadRequest)
		return
	}

	// Validate cursor order matches request order
	if cursor != nil && cursor.Order != "" && cursor.Order != order {
		respondError(w, "cursor was created with order='"+cursor.Order+"' but request uses order='"+order+"'. Cannot change order while paginating.", http.StatusBadRequest)
		return
	}

	filters := TransferFilters{
		SourceType:  r.URL.Query().Get("source_type"), // "classic" or "soroban"
		AssetCode:   r.URL.Query().Get("asset_code"),
		FromAccount: r.URL.Query().Get("from_account"),
		ToAccount:   r.URL.Query().Get("to_account"),
		Limit:       parseLimit(r, 100, 1000),
		Cursor:      cursor,
		Order:       order,
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

	// Explorer/default transfer queries are usually recent and/or selective.
	// Prefer direct hot PG via the legacy reader for those cases instead of
	// unified DuckDB federation over enriched_history_operations.
	recentQuery := (!filters.StartTime.IsZero() && filters.StartTime.After(time.Now().Add(-48*time.Hour))) ||
		filters.FromAccount != "" || filters.ToAccount != "" || filters.Cursor != nil

	switch h.readerMode {
	case ReaderModeUnified:
		if recentQuery && h.legacyReader != nil {
			transfers, nextCursor, hasMore, err = h.legacyReader.GetTokenTransfersWithCursor(r.Context(), filters)
		} else {
			transfers, nextCursor, hasMore, err = h.unifiedReader.GetTokenTransfersWithCursor(r.Context(), filters)
		}
	case ReaderModeHybrid:
		legacyTransfers, legacyNextCursor, legacyHasMore, legacyErr := h.legacyReader.GetTokenTransfersWithCursor(r.Context(), filters)
		var unifiedTransfers []TokenTransfer
		var unifiedErr error
		if !recentQuery {
			unifiedTransfers, _, _, unifiedErr = h.unifiedReader.GetTokenTransfersWithCursor(r.Context(), filters)
		}

		if !recentQuery {
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
		}
		transfers, nextCursor, hasMore, err = legacyTransfers, legacyNextCursor, legacyHasMore, legacyErr
	default: // ReaderModeLegacy
		transfers, nextCursor, hasMore, err = h.legacyReader.GetTokenTransfersWithCursor(r.Context(), filters)
	}

	if err != nil {
		respondError(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Build _meta for RPC v2 compatibility
	meta := ResponseMeta{}
	if len(transfers) > 0 {
		var maxLedger int64
		for _, t := range transfers {
			if t.LedgerSequence > maxLedger {
				maxLedger = t.LedgerSequence
			}
		}
		meta.ScannedLedger = &maxLedger
	}
	if !recentQuery && h.unifiedReader != nil {
		if availableLedgers, err := h.unifiedReader.GetAvailableLedgers(r.Context()); err == nil {
			meta.AvailableLedgers = availableLedgers
		}
	}

	response := map[string]interface{}{
		"transfers": transfers,
		"count":     len(transfers),
		"filters":   filters,
		"has_more":  hasMore,
		"_meta":     meta,
	}
	if nextCursor != "" {
		response["cursor"] = nextCursor
	}

	respondJSON(w, response)
}

// HandleTokenTransferStats returns aggregated transfer statistics
// @Summary Get transfer statistics
// @Description Returns aggregated transfer statistics grouped by asset, source type, hour, or day
// @Tags Transfers
// @Accept json
// @Produce json
// @Param group_by query string false "Group by: asset (default), source_type, hour, day"
// @Param start_time query string false "Start time in RFC3339 format (default: 24h ago)"
// @Param end_time query string false "End time in RFC3339 format (default: now)"
// @Success 200 {object} map[string]interface{} "Transfer statistics"
// @Failure 400 {object} map[string]interface{} "Invalid group_by value"
// @Failure 500 {object} map[string]interface{} "Internal server error"
// @Router /api/v1/silver/transfers/stats [get]
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
// @Summary Get account overview
// @Description Returns comprehensive account overview including current state, recent operations, and transfers
// @Tags Explorer
// @Accept json
// @Produce json
// @Param account_id query string true "Stellar account ID (G...)"
// @Success 200 {object} map[string]interface{} "Account overview with recent activity"
// @Failure 400 {object} map[string]interface{} "Missing account_id"
// @Failure 404 {object} map[string]interface{} "Account not found"
// @Failure 500 {object} map[string]interface{} "Internal server error"
// @Router /api/v1/silver/explorer/account [get]
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

	// Fetch created_at (best-effort, non-blocking)
	if account.CreatedAt == nil {
		switch h.readerMode {
		case ReaderModeUnified:
			if h.unifiedReader != nil {
				createdAt, _ := h.unifiedReader.GetAccountCreatedAt(r.Context(), accountID)
				account.CreatedAt = createdAt
			}
		default:
			if h.legacyReader != nil {
				createdAt, _ := h.legacyReader.hot.GetAccountCreatedAt(r.Context(), accountID)
				account.CreatedAt = createdAt
			}
		}
	}

	respondJSON(w, map[string]interface{}{
		"account":           account,
		"recent_operations": operations,
		"recent_transfers":  transfers,
		"operations_count":  len(operations),
		"transfers_count":   len(transfers),
	})
}

// HandleAccountOffers returns offers for a specific account
// @Summary Get account offers
// @Description Returns all open offers for a given account
// @Tags Accounts
// @Accept json
// @Produce json
// @Param id path string true "Account ID (G... address)"
// @Param limit query int false "Maximum results to return (default: 100, max: 1000)"
// @Param cursor query string false "Pagination cursor"
// @Success 200 {object} map[string]interface{} "Account offers"
// @Failure 400 {object} map[string]interface{} "Invalid parameters"
// @Failure 500 {object} map[string]interface{} "Internal server error"
// @Router /api/v1/silver/accounts/{id}/offers [get]
func (h *SilverHandlers) HandleAccountOffers(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	accountID := vars["id"]
	if accountID == "" {
		respondError(w, "account_id required", http.StatusBadRequest)
		return
	}

	cursorStr := r.URL.Query().Get("cursor")
	cursor, err := DecodeOfferCursor(cursorStr)
	if err != nil {
		respondError(w, "invalid cursor: "+err.Error(), http.StatusBadRequest)
		return
	}

	filters := OfferFilters{
		SellerID: accountID,
		Limit:    parseLimit(r, 100, 1000),
		Cursor:   cursor,
	}

	if h.unifiedReader == nil {
		respondError(w, "offers endpoint requires unified reader", http.StatusInternalServerError)
		return
	}

	offers, nextCursor, hasMore, err := h.unifiedReader.GetOffers(r.Context(), filters)
	if err != nil {
		respondError(w, err.Error(), http.StatusInternalServerError)
		return
	}

	response := map[string]interface{}{
		"account_id": accountID,
		"offers":     offers,
		"count":      len(offers),
		"has_more":   hasMore,
	}
	if nextCursor != "" {
		response["cursor"] = nextCursor
	}

	respondJSON(w, response)
}

// HandleTransactionDetails returns full transaction details with operations
// @Summary Get transaction details
// @Description Returns full transaction details including all operations and transfers
// @Tags Explorer
// @Accept json
// @Produce json
// @Param tx_hash query string true "Transaction hash"
// @Success 200 {object} map[string]interface{} "Transaction details with operations"
// @Failure 400 {object} map[string]interface{} "Missing tx_hash"
// @Failure 404 {object} map[string]interface{} "Transaction not found"
// @Failure 500 {object} map[string]interface{} "Internal server error"
// @Router /api/v1/silver/explorer/transaction [get]
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
// @Summary Get asset overview
// @Description Returns asset statistics including holder count, supply, and recent transfers
// @Tags Explorer
// @Accept json
// @Produce json
// @Param asset_code query string true "Asset code"
// @Param asset_issuer query string false "Asset issuer (omit for XLM)"
// @Success 200 {object} map[string]interface{} "Asset overview with statistics and transfers"
// @Failure 400 {object} map[string]interface{} "Missing asset_code"
// @Failure 500 {object} map[string]interface{} "Internal server error"
// @Router /api/v1/silver/explorer/asset [get]
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
// @Summary List offers
// @Description Returns a paginated list of DEX offers with optional seller filter
// @Tags DEX
// @Accept json
// @Produce json
// @Param seller_id query string false "Filter by seller account ID"
// @Param limit query int false "Maximum results to return (default: 100, max: 1000)"
// @Param cursor query string false "Pagination cursor from previous response"
// @Success 200 {object} map[string]interface{} "List of offers with pagination info"
// @Failure 400 {object} map[string]interface{} "Invalid cursor"
// @Failure 500 {object} map[string]interface{} "Internal server error"
// @Router /api/v1/silver/offers [get]
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
// @Summary List liquidity pools
// @Description Returns a paginated list of AMM liquidity pools
// @Tags DEX
// @Accept json
// @Produce json
// @Param limit query int false "Maximum results to return (default: 100, max: 1000)"
// @Param cursor query string false "Pagination cursor from previous response"
// @Success 200 {object} map[string]interface{} "List of liquidity pools with pagination info"
// @Failure 400 {object} map[string]interface{} "Invalid cursor"
// @Failure 500 {object} map[string]interface{} "Internal server error"
// @Router /api/v1/silver/liquidity-pools [get]
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
// @Summary List claimable balances
// @Description Returns a paginated list of claimable balances with optional sponsor filter
// @Tags State
// @Accept json
// @Produce json
// @Param sponsor query string false "Filter by sponsor account ID"
// @Param limit query int false "Maximum results to return (default: 100, max: 1000)"
// @Param cursor query string false "Pagination cursor from previous response"
// @Success 200 {object} map[string]interface{} "List of claimable balances with pagination info"
// @Failure 400 {object} map[string]interface{} "Invalid cursor"
// @Failure 500 {object} map[string]interface{} "Internal server error"
// @Router /api/v1/silver/claimable-balances [get]
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
// GET /api/v1/silver/trades?order=desc (default: asc for backward compatibility)
func (h *SilverHandlers) HandleTrades(w http.ResponseWriter, r *http.Request) {
	// Parse cursor for pagination
	cursorStr := r.URL.Query().Get("cursor")
	cursor, err := DecodeTradeCursor(cursorStr)
	if err != nil {
		respondError(w, "invalid cursor: "+err.Error(), http.StatusBadRequest)
		return
	}

	// Parse and validate order parameter (default: asc for backward compatibility - trades historically sorted ASC)
	order := strings.ToLower(r.URL.Query().Get("order"))
	if order == "" {
		order = "asc"
	}
	if order != "asc" && order != "desc" {
		respondError(w, "order must be 'asc' or 'desc'", http.StatusBadRequest)
		return
	}

	// Validate cursor order matches request order
	if cursor != nil && cursor.Order != "" && cursor.Order != order {
		respondError(w, "cursor was created with order='"+cursor.Order+"' but request uses order='"+order+"'. Cannot change order while paginating.", http.StatusBadRequest)
		return
	}

	// Build filters
	filters := TradeFilters{
		AccountID:     r.URL.Query().Get("account_id"),
		SellerAccount: r.URL.Query().Get("seller_account"),
		BuyerAccount:  r.URL.Query().Get("buyer_account"),
		Limit:         parseLimit(r, 100, 1000),
		Cursor:        cursor,
		Order:         order,
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

	// Build _meta for RPC v2 compatibility
	meta := ResponseMeta{}
	if len(trades) > 0 {
		var maxLedger int64
		for _, t := range trades {
			if t.LedgerSequence > maxLedger {
				maxLedger = t.LedgerSequence
			}
		}
		meta.ScannedLedger = &maxLedger
	}
	if h.unifiedReader != nil {
		if availableLedgers, err := h.unifiedReader.GetAvailableLedgers(r.Context()); err == nil {
			meta.AvailableLedgers = availableLedgers
		}
	}

	response := map[string]interface{}{
		"trades":   trades,
		"count":    len(trades),
		"has_more": hasMore,
		"_meta":    meta,
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
// GET /api/v1/silver/effects?order=desc (default: asc for backward compatibility)
func (h *SilverHandlers) HandleEffects(w http.ResponseWriter, r *http.Request) {
	// Parse cursor for pagination
	cursorStr := r.URL.Query().Get("cursor")
	cursor, err := DecodeEffectCursor(cursorStr)
	if err != nil {
		respondError(w, "invalid cursor: "+err.Error(), http.StatusBadRequest)
		return
	}

	// Parse and validate order parameter (default: desc for most-recent-first)
	order := strings.ToLower(r.URL.Query().Get("order"))
	if order == "" {
		order = "desc"
	}
	if order != "asc" && order != "desc" {
		respondError(w, "order must be 'asc' or 'desc'", http.StatusBadRequest)
		return
	}

	// Validate cursor order matches request order
	if cursor != nil && cursor.Order != "" && cursor.Order != order {
		respondError(w, "cursor was created with order='"+cursor.Order+"' but request uses order='"+order+"'. Cannot change order while paginating.", http.StatusBadRequest)
		return
	}

	filters := EffectFilters{
		AccountID:  r.URL.Query().Get("account_id"),
		EffectType: r.URL.Query().Get("effect_type"),
		Limit:      parseLimit(r, 100, 1000),
		Cursor:     cursor,
		Order:      order,
	}

	// Parse ledger_sequence
	if ledgerStr := r.URL.Query().Get("ledger_sequence"); ledgerStr != "" {
		if ledger, err := strconv.ParseInt(ledgerStr, 10, 64); err == nil {
			filters.LedgerSequence = ledger
		}
	}

	// Parse time range — default to last 24h to avoid full-table scan
	if startStr := r.URL.Query().Get("start_time"); startStr != "" {
		if start, err := time.Parse(time.RFC3339, startStr); err == nil {
			filters.StartTime = start
		}
	} else if filters.LedgerSequence == 0 && filters.AccountID == "" && filters.Cursor == nil {
		// No filter at all — default to recent data to prevent scanning entire history
		filters.StartTime = time.Now().Add(-24 * time.Hour)
	}
	if endStr := r.URL.Query().Get("end_time"); endStr != "" {
		if end, err := time.Parse(time.RFC3339, endStr); err == nil {
			filters.EndTime = end
		}
	}

	var effects []SilverEffect
	var nextCursor string
	var hasMore bool

	// For recent queries (default 24h), query silver_hot PG directly to avoid
	// DuckDB federation overhead. Fall back to unified reader for historical queries.
	// Use the fast hot PG path for recent queries or queries with selective filters
	// (account_id, transaction_hash). Only fall through to DuckDB for historical
	// ledger-range queries that need cold data.
	recentQuery := filters.LedgerSequence == 0 && ((!filters.StartTime.IsZero() && filters.StartTime.After(time.Now().Add(-48*time.Hour))) ||
		filters.AccountID != "" ||
		filters.TransactionHash != "")
	if recentQuery && h.legacyReader != nil && h.legacyReader.hot != nil {
		effects, nextCursor, hasMore, err = h.legacyReader.hot.GetEffects(r.Context(), filters)
	} else if h.unifiedReader != nil {
		effects, nextCursor, hasMore, err = h.unifiedReader.GetEffects(r.Context(), filters)
	} else {
		respondError(w, "effects endpoint requires unified reader", http.StatusInternalServerError)
		return
	}

	if err != nil {
		respondError(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Build _meta for RPC v2 compatibility
	meta := ResponseMeta{}
	if len(effects) > 0 {
		var maxLedger int64
		for _, e := range effects {
			if e.LedgerSequence > maxLedger {
				maxLedger = e.LedgerSequence
			}
		}
		meta.ScannedLedger = &maxLedger
	}
	// Only fetch available ledgers from DuckDB if we didn't use the fast hot path.
	// GetAvailableLedgers goes through DuckDB which can be slow (30s+).
	if !recentQuery && h.unifiedReader != nil {
		if availableLedgers, err := h.unifiedReader.GetAvailableLedgers(r.Context()); err == nil {
			meta.AvailableLedgers = availableLedgers
		}
	}

	response := map[string]interface{}{
		"effects":  effects,
		"count":    len(effects),
		"has_more": hasMore,
		"_meta":    meta,
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

	if h.legacyReader != nil && h.legacyReader.hot != nil {
		effects, err = h.legacyReader.hot.GetEffectsByTransactionFast(r.Context(), txHash)
		if err != nil {
			log.Printf("effects-by-tx hot-path fallback tx=%s err=%v", txHash, err)
		} else {
			respondJSON(w, map[string]interface{}{
				"transaction_hash": txHash,
				"effects":          effects,
				"count":            len(effects),
			})
			return
		}
	}
	if err != nil || effects == nil {
		if h.unifiedReader != nil {
			effects, _, _, err = h.unifiedReader.GetEffects(r.Context(), filters)
		} else {
			respondError(w, "effects endpoint requires unified reader", http.StatusInternalServerError)
			return
		}
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

// HandleEffectsByTransactionAlias provides Prism-compatible route shape.
// GET /api/v1/silver/tx/{hash}/effects
func (h *SilverHandlers) HandleEffectsByTransactionAlias(w http.ResponseWriter, r *http.Request) {
	txHash := mux.Vars(r)["hash"]
	if txHash == "" {
		respondError(w, "hash required", http.StatusBadRequest)
		return
	}

	filters := EffectFilters{
		TransactionHash: txHash,
		Limit:           1000,
	}

	var effects []SilverEffect
	var err error
	if h.legacyReader != nil && h.legacyReader.hot != nil {
		effects, err = h.legacyReader.hot.GetEffectsByTransactionFast(r.Context(), txHash)
		if err == nil {
			respondJSON(w, map[string]interface{}{
				"transaction_hash": txHash,
				"effects":          effects,
				"count":            len(effects),
			})
			return
		}
		log.Printf("effects-by-tx alias hot-path fallback tx=%s err=%v", txHash, err)
	}
	if h.unifiedReader == nil {
		respondError(w, "effects endpoint requires unified reader", http.StatusInternalServerError)
		return
	}
	effects, _, _, err = h.unifiedReader.GetEffects(r.Context(), filters)
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

// HandleTTL returns TTL entries.
//
// Lookup modes:
//   - key_hash=<hex>            → single TTL entry by hash
//   - contract_id=C...          → all TTL entries for the contract's storage (paginated)
//
// GET /api/v1/silver/soroban/ttl?key_hash=...
// GET /api/v1/silver/soroban/ttl?contract_id=C...&limit=100&cursor=...
func (h *SilverHandlers) HandleTTL(w http.ResponseWriter, r *http.Request) {
	if h.unifiedReader == nil {
		respondError(w, "ttl endpoint requires unified reader", http.StatusInternalServerError)
		return
	}

	keyHash := r.URL.Query().Get("key_hash")
	contractID := r.URL.Query().Get("contract_id")

	if keyHash == "" && contractID == "" {
		respondError(w, "key_hash or contract_id parameter required", http.StatusBadRequest)
		return
	}
	if keyHash != "" && contractID != "" {
		respondError(w, "specify key_hash or contract_id, not both", http.StatusBadRequest)
		return
	}

	currentLedger, err := h.unifiedReader.GetCurrentLedger(r.Context())
	if err != nil {
		respondError(w, "failed to get current ledger: "+err.Error(), http.StatusInternalServerError)
		return
	}

	if keyHash != "" {
		ttl, err := h.unifiedReader.GetTTL(r.Context(), keyHash)
		if err != nil {
			respondError(w, err.Error(), http.StatusInternalServerError)
			return
		}
		if ttl == nil {
			respondError(w, "TTL entry not found", http.StatusNotFound)
			return
		}
		normalizeTTLEntryForCurrentLedger(ttl, currentLedger)
		respondJSON(w, map[string]interface{}{"ttl": ttl})
		return
	}

	cursor, err := DecodeTTLCursor(r.URL.Query().Get("cursor"))
	if err != nil {
		respondError(w, "invalid cursor: "+err.Error(), http.StatusBadRequest)
		return
	}
	filters := TTLFilters{
		ContractID: contractID,
		Limit:      parseLimit(r, 100, 1000),
		Cursor:     cursor,
	}

	entries, nextCursor, hasMore, err := h.unifiedReader.GetTTLByContract(r.Context(), contractID, filters)
	if err != nil {
		respondError(w, err.Error(), http.StatusInternalServerError)
		return
	}
	normalizeTTLEntriesForCurrentLedger(entries, currentLedger)

	resp := map[string]interface{}{
		"contract_id": contractID,
		"ttl_entries": entries,
		"count":       len(entries),
		"has_more":    hasMore,
	}
	if nextCursor != "" {
		resp["cursor"] = nextCursor
	}
	respondJSON(w, resp)
}

// HandleTTLResolve computes the ledger-key hash for a (contract_id, durability, ScVal key)
// tuple server-side and returns the matching TTL entry. Removes the need for clients to
// XDR-marshal the LedgerKey themselves.
//
// GET /api/v1/silver/soroban/ttl/resolve?contract_id=C...&durability=persistent&key=<base64-ScVal>
// GET /api/v1/silver/soroban/ttl/resolve?contract_id=C...&durability=persistent&key_type=instance
func (h *SilverHandlers) HandleTTLResolve(w http.ResponseWriter, r *http.Request) {
	if h.unifiedReader == nil {
		respondError(w, "ttl endpoint requires unified reader", http.StatusInternalServerError)
		return
	}

	contractID := r.URL.Query().Get("contract_id")
	durabilityParam := strings.ToLower(r.URL.Query().Get("durability"))
	keyB64 := r.URL.Query().Get("key")
	keyType := strings.ToLower(r.URL.Query().Get("key_type"))

	if contractID == "" {
		respondError(w, "contract_id required", http.StatusBadRequest)
		return
	}
	if keyB64 == "" && keyType == "" {
		respondError(w, "key (base64 ScVal) or key_type=instance required", http.StatusBadRequest)
		return
	}
	if keyB64 != "" && keyType != "" {
		respondError(w, "specify key or key_type, not both", http.StatusBadRequest)
		return
	}

	keyHash, err := computeContractDataKeyHash(contractID, durabilityParam, keyB64, keyType)
	if err != nil {
		respondError(w, err.Error(), http.StatusBadRequest)
		return
	}

	ttl, err := h.unifiedReader.GetTTL(r.Context(), keyHash)
	if err != nil {
		respondError(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if ttl == nil {
		respondError(w, "TTL entry not found", http.StatusNotFound)
		return
	}

	currentLedger, err := h.unifiedReader.GetCurrentLedger(r.Context())
	if err != nil {
		respondError(w, "failed to get current ledger: "+err.Error(), http.StatusInternalServerError)
		return
	}
	normalizeTTLEntryForCurrentLedger(ttl, currentLedger)

	respondJSON(w, map[string]interface{}{
		"key_hash": keyHash,
		"ttl":      ttl,
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
// TRANSACTION SUMMARIES ENDPOINT
// ============================================

// TransactionSummaryItem represents a summary of a single transaction
type TransactionSummaryItem struct {
	TxHash          string  `json:"tx_hash"`
	LedgerSequence  int64   `json:"ledger_sequence"`
	ClosedAt        string  `json:"closed_at"`
	SourceAccount   string  `json:"source_account"`
	FeeCharged      int64   `json:"fee_charged"`
	OpCount         int64   `json:"op_count"`
	Successful      bool    `json:"successful"`
	HasSoroban      bool    `json:"has_soroban"`
	PrimaryContract *string `json:"primary_contract,omitempty"`
	TxType          string  `json:"tx_type"`
}

// HandleRecentLedgers returns the most recent ledgers from the serving layer.
// @Summary Get recent ledgers
// @Description Returns the newest ledgers for latest-ledgers UIs from serving projections
// @Tags Ledgers
// @Accept json
// @Produce json
// @Param limit query int false "Maximum results (default: 6, max: 25)"
// @Success 200 {object} map[string]interface{} "Recent ledgers"
// @Failure 500 {object} map[string]interface{} "Internal server error"
// @Router /api/v1/silver/ledgers/recent [get]
func (h *SilverHandlers) HandleRecentLedgers(w http.ResponseWriter, r *http.Request) {
	if h.legacyReader == nil || h.legacyReader.hot == nil {
		respondError(w, "recent ledgers endpoint requires silver hot reader", http.StatusInternalServerError)
		return
	}

	limit := parseLimit(r, 6, 25)
	latestSequence, ledgers, err := h.legacyReader.hot.GetServingRecentLedgers(r.Context(), limit)
	if err != nil {
		respondError(w, err.Error(), http.StatusInternalServerError)
		return
	}

	respondJSON(w, map[string]interface{}{
		"latest_sequence": latestSequence,
		"count":           len(ledgers),
		"ledgers":         ledgers,
	})
}

// HandleRecentTransactions returns the most recent transactions from the serving layer.
// @Summary Get recent transactions
// @Description Returns the newest transactions with serving-backed summaries for latest-activity UIs
// @Tags Transactions
// @Accept json
// @Produce json
// @Param limit query int false "Maximum results (default: 6, max: 25)"
// @Success 200 {object} map[string]interface{} "Recent transactions"
// @Failure 500 {object} map[string]interface{} "Internal server error"
// @Router /api/v1/silver/transactions/recent [get]
func (h *SilverHandlers) HandleRecentTransactions(w http.ResponseWriter, r *http.Request) {
	if h.legacyReader == nil || h.legacyReader.hot == nil {
		respondError(w, "recent transactions endpoint requires silver hot reader", http.StatusInternalServerError)
		return
	}

	limit := parseLimit(r, 6, 25)
	latestSequence, transactions, err := h.legacyReader.hot.GetServingRecentTransactions(r.Context(), limit)
	if err != nil {
		respondError(w, err.Error(), http.StatusInternalServerError)
		return
	}

	respondJSON(w, map[string]interface{}{
		"latest_sequence": latestSequence,
		"count":           len(transactions),
		"transactions":    transactions,
	})
}

// HandleTransactionSummaries returns batch transaction summaries
// @Summary Get transaction summaries
// @Description Returns summarized transaction data for a batch of hashes or a ledger
// @Tags Transactions
// @Accept json
// @Produce json
// @Param hashes query string false "Comma-separated transaction hashes (max 25)"
// @Param ledger query int false "Ledger sequence to get all transactions from"
// @Param limit query int false "Maximum results (default: 10, max: 100)"
// @Success 200 {object} map[string]interface{} "Transaction summaries"
// @Failure 400 {object} map[string]interface{} "Invalid parameters"
// @Failure 500 {object} map[string]interface{} "Internal server error"
// @Router /api/v1/silver/transactions/summaries [get]
func (h *SilverHandlers) HandleTransactionSummaries(w http.ResponseWriter, r *http.Request) {
	if h.unifiedReader == nil {
		respondError(w, "transaction summaries requires unified reader", http.StatusInternalServerError)
		return
	}

	ctx := r.Context()
	hashesStr := r.URL.Query().Get("hashes")
	ledgerStr := r.URL.Query().Get("ledger")

	if hashesStr == "" && ledgerStr == "" {
		respondError(w, "either 'hashes' or 'ledger' parameter required", http.StatusBadRequest)
		return
	}

	var query string
	var args []interface{}

	if hashesStr != "" {
		hashes := strings.Split(hashesStr, ",")
		if len(hashes) > 25 {
			respondError(w, "maximum 25 hashes allowed", http.StatusBadRequest)
			return
		}

		// Build query with individual placeholders (DuckDB doesn't support pq.Array)
		placeholders := make([]string, len(hashes))
		args = make([]interface{}, len(hashes))
		for i, h := range hashes {
			placeholders[i] = fmt.Sprintf("$%d", i+1)
			args[i] = strings.TrimSpace(h)
		}

		query = fmt.Sprintf(`
			SELECT transaction_hash,
			       MIN(ledger_sequence) as ledger_seq,
			       MIN(ledger_closed_at) as closed_at,
			       MIN(source_account) as source_account,
			       MIN(tx_fee_charged) as fee_charged,
			       COUNT(*) as op_count,
			       BOOL_AND(tx_successful) as successful,
			       BOOL_OR(is_soroban_op) as has_soroban,
			       MIN(contract_id) FILTER (WHERE contract_id IS NOT NULL) as primary_contract
			FROM %s.enriched_history_operations
			WHERE transaction_hash IN (%s)
			GROUP BY transaction_hash
		`, h.unifiedReader.hotSchema, strings.Join(placeholders, ", "))
	} else {
		ledgerSeq, err := strconv.ParseInt(ledgerStr, 10, 64)
		if err != nil {
			respondError(w, "invalid ledger sequence", http.StatusBadRequest)
			return
		}
		limit := parseLimit(r, 10, 100)

		query = fmt.Sprintf(`
			SELECT transaction_hash,
			       MIN(ledger_sequence) as ledger_seq,
			       MIN(ledger_closed_at) as closed_at,
			       MIN(source_account) as source_account,
			       MIN(tx_fee_charged) as fee_charged,
			       COUNT(*) as op_count,
			       BOOL_AND(tx_successful) as successful,
			       BOOL_OR(is_soroban_op) as has_soroban,
			       MIN(contract_id) FILTER (WHERE contract_id IS NOT NULL) as primary_contract
			FROM %s.enriched_history_operations
			WHERE ledger_sequence = $1
			GROUP BY transaction_hash
			ORDER BY MIN(operation_index)
			LIMIT $2
		`, h.unifiedReader.hotSchema)
		args = []interface{}{ledgerSeq, limit}
	}

	rows, err := h.unifiedReader.db.QueryContext(ctx, query, args...)
	if err != nil {
		respondError(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	var summaries []TransactionSummaryItem
	for rows.Next() {
		var item TransactionSummaryItem
		var closedAt sql.NullString
		var sourceAccount sql.NullString
		var feeCharged sql.NullInt64
		var hasSoroban sql.NullBool
		var primaryContract sql.NullString

		if err := rows.Scan(
			&item.TxHash, &item.LedgerSequence, &closedAt,
			&sourceAccount, &feeCharged, &item.OpCount,
			&item.Successful, &hasSoroban, &primaryContract,
		); err != nil {
			respondError(w, err.Error(), http.StatusInternalServerError)
			return
		}

		if closedAt.Valid {
			item.ClosedAt = closedAt.String
		}
		if sourceAccount.Valid {
			item.SourceAccount = sourceAccount.String
		}
		if feeCharged.Valid {
			item.FeeCharged = feeCharged.Int64
		}
		if hasSoroban.Valid {
			item.HasSoroban = hasSoroban.Bool
		}
		if primaryContract.Valid {
			item.PrimaryContract = &primaryContract.String
		}

		// Derive tx_type
		if item.HasSoroban {
			item.TxType = "soroban"
		} else {
			item.TxType = "classic"
		}

		summaries = append(summaries, item)
	}

	respondJSON(w, map[string]interface{}{
		"summaries": summaries,
		"count":     len(summaries),
	})
}

// ============================================
// CONTRACT STORAGE ENDPOINT
// ============================================

// ContractStorageEntry represents a contract storage entry with TTL info
type ContractStorageEntry struct {
	ContractID         string  `json:"contract_id"`
	Key                string  `json:"key"`
	KeyHash            string  `json:"key_hash"`
	Type               string  `json:"type"`
	Durability         string  `json:"durability"`
	SizeBytes          *int    `json:"size_bytes,omitempty"`
	DataValue          *string `json:"data_value,omitempty"`
	LastModifiedLedger int64   `json:"last_modified_ledger"`
	ClosedAt           string  `json:"closed_at"`
	LiveUntilLedgerSeq *int64  `json:"live_until_ledger_seq,omitempty"`
	TTLRemaining       *int    `json:"ttl_remaining,omitempty"`
	Expired            *bool   `json:"expired,omitempty"`
}

// HandleContractStorage returns storage entries for a contract
// @Summary Get contract storage entries
// @Description Returns contract storage entries with TTL information
// @Tags Contracts
// @Accept json
// @Produce json
// @Param id path string true "Contract ID (C... address)"
// @Param limit query int false "Maximum results (default: 100, max: 1000)"
// @Param offset query int false "Offset for pagination (default: 0)"
// @Param durability query string false "Filter by durability: persistent, temporary, instance"
// @Success 200 {object} map[string]interface{} "Contract storage entries"
// @Failure 400 {object} map[string]interface{} "Invalid parameters"
// @Failure 500 {object} map[string]interface{} "Internal server error"
// @Router /api/v1/silver/contracts/{id}/storage [get]
func (h *SilverHandlers) HandleContractStorage(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	contractID := vars["id"]
	if contractID == "" {
		respondError(w, "contract_id required", http.StatusBadRequest)
		return
	}

	if h.unifiedReader == nil {
		respondError(w, "contract storage requires unified reader", http.StatusInternalServerError)
		return
	}

	limit := parseLimit(r, 100, 1000)
	offsetStr := r.URL.Query().Get("offset")
	offset := 0
	if offsetStr != "" {
		if o, err := strconv.Atoi(offsetStr); err == nil && o >= 0 {
			offset = o
		}
	}
	durability := r.URL.Query().Get("durability")

	ctx := r.Context()

	// Build query against silver hot
	query := fmt.Sprintf(`
		SELECT cd.contract_id, cd.key_hash, cd.durability,
		       LENGTH(cd.data_value), cd.data_value, cd.last_modified_ledger, cd.closed_at,
		       t.live_until_ledger_seq, t.ttl_remaining, t.expired
		FROM %s.contract_data_current cd
		LEFT JOIN %s.ttl_current t ON cd.key_hash = t.key_hash
		WHERE cd.contract_id = $1
	`, h.unifiedReader.hotSchema, h.unifiedReader.hotSchema)

	args := []interface{}{contractID}
	argIdx := 2

	if durability != "" {
		query += fmt.Sprintf(" AND cd.durability = $%d", argIdx)
		args = append(args, durability)
		argIdx++
	}

	query += " ORDER BY cd.key_hash"
	query += fmt.Sprintf(" LIMIT $%d OFFSET $%d", argIdx, argIdx+1)
	args = append(args, limit, offset)

	rows, err := h.unifiedReader.db.QueryContext(ctx, query, args...)
	if err != nil {
		respondError(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	var entries []ContractStorageEntry
	for rows.Next() {
		var entry ContractStorageEntry
		var sizeBytes sql.NullInt32
		var dataValue sql.NullString
		var closedAt sql.NullString
		var liveUntil sql.NullInt64
		var ttlRemaining sql.NullInt32
		var expired sql.NullBool

		if err := rows.Scan(
			&entry.ContractID, &entry.KeyHash, &entry.Durability,
			&sizeBytes, &dataValue, &entry.LastModifiedLedger, &closedAt,
			&liveUntil, &ttlRemaining, &expired,
		); err != nil {
			respondError(w, err.Error(), http.StatusInternalServerError)
			return
		}

		entry.Key = entry.KeyHash
		entry.Type = normalizeContractStorageType(entry.Durability)
		if sizeBytes.Valid {
			sz := int(sizeBytes.Int32)
			entry.SizeBytes = &sz
		}
		if dataValue.Valid {
			entry.DataValue = &dataValue.String
		}
		if closedAt.Valid {
			entry.ClosedAt = closedAt.String
		}
		if liveUntil.Valid {
			entry.LiveUntilLedgerSeq = &liveUntil.Int64
		}
		if ttlRemaining.Valid {
			rem := int(ttlRemaining.Int32)
			entry.TTLRemaining = &rem
		}
		if expired.Valid {
			entry.Expired = &expired.Bool
		}

		entries = append(entries, entry)
	}

	respondJSON(w, map[string]interface{}{
		"contract_id": contractID,
		"entries":     entries,
		"count":       len(entries),
		"limit":       limit,
		"offset":      offset,
	})
}

// ============================================
// HELPER FUNCTIONS
// ============================================

func normalizeContractStorageType(durability string) string {
	switch strings.ToLower(durability) {
	case "contractdatadurabilitypersistent", "persistent":
		return "persistent"
	case "contractdatadurabilitytemporary", "temporary":
		return "temporary"
	case "contractdatadurabilityinstance", "instance":
		return "instance"
	default:
		return durability
	}
}

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

type assetRef struct {
	IsContract  bool
	ContractID  string
	AssetCode   string
	AssetIssuer string
	IsNative    bool
}

func (a assetRef) CanonicalSlug() string {
	if a.IsContract {
		return a.ContractID
	}
	if a.IsNative {
		return "XLM"
	}
	return a.AssetCode + ":" + a.AssetIssuer
}

func (a assetRef) AssetInfo() AssetInfo {
	if a.IsContract {
		return AssetInfo{Code: a.ContractID, Type: "soroban_token"}
	}
	if a.IsNative {
		return AssetInfo{Code: "XLM", Type: "native"}
	}
	issuer := a.AssetIssuer
	assetType := "credit_alphanum4"
	if len(a.AssetCode) > 4 {
		assetType = "credit_alphanum12"
	}
	return AssetInfo{Code: a.AssetCode, Issuer: &issuer, Type: assetType}
}

func parseAssetSlug(assetParam string) (assetRef, error) {
	if assetParam == "XLM" || assetParam == "native" {
		return assetRef{AssetCode: "XLM", IsNative: true}, nil
	}
	if strings.HasPrefix(assetParam, "C") {
		if _, err := strkey.Decode(strkey.VersionByteContract, assetParam); err == nil {
			return assetRef{IsContract: true, ContractID: assetParam}, nil
		}
	}
	if code, issuer := parseAssetParam(assetParam); code != "" {
		return assetRef{AssetCode: code, AssetIssuer: issuer}, nil
	}
	if idx := strings.LastIndex(assetParam, "-"); idx > 0 {
		code := assetParam[:idx]
		issuer := assetParam[idx+1:]
		if strings.HasPrefix(issuer, "G") {
			return assetRef{AssetCode: code, AssetIssuer: issuer}, nil
		}
	}
	return assetRef{}, fmt.Errorf("invalid asset format, expected XLM, CODE:ISSUER, CODE-G..., or C...")
}

func stellarFlagSet(flags int64, mask int64) bool { return flags&mask == mask }

func (h *SilverHandlers) queryIssuerMetadata(ctx context.Context, issuer string) (*AssetIssuerMetadata, error) {
	if issuer == "" {
		return nil, nil
	}

	if h.unifiedReader != nil {
		query := fmt.Sprintf(`
			WITH combined AS (
				SELECT account_id, home_domain, flags, last_modified_ledger, 1 as source
				FROM %s.accounts_current WHERE account_id = $1
				UNION ALL
				SELECT account_id, home_domain, flags, last_modified_ledger, 2 as source
				FROM %s.accounts_current WHERE account_id = $1
			)
			SELECT account_id, home_domain, COALESCE(flags, 0)
			FROM combined
			ORDER BY last_modified_ledger DESC, source ASC
			LIMIT 1
		`, h.unifiedReader.hotSchema, h.unifiedReader.coldSchema)
		var meta AssetIssuerMetadata
		var home sql.NullString
		var flags int64
		if err := h.unifiedReader.db.QueryRowContext(ctx, query, issuer).Scan(&meta.AccountID, &home, &flags); err != nil {
			if err == sql.ErrNoRows {
				return nil, nil
			}
			return nil, err
		}
		if home.Valid && home.String != "" {
			meta.HomeDomain = &home.String
		}
		meta.AuthRequired = stellarFlagSet(flags, 1)
		meta.AuthRevocable = stellarFlagSet(flags, 2)
		meta.AuthImmutable = stellarFlagSet(flags, 4)
		meta.AuthClawbackEnabled = stellarFlagSet(flags, 8)
		return &meta, nil
	}

	if h.legacyReader != nil && h.legacyReader.hot != nil {
		var meta AssetIssuerMetadata
		var home sql.NullString
		var flags int64
		if err := h.legacyReader.hot.db.QueryRowContext(ctx, `
			SELECT account_id, home_domain, COALESCE(flags, 0)
			FROM accounts_current WHERE account_id = $1
		`, issuer).Scan(&meta.AccountID, &home, &flags); err != nil {
			if err == sql.ErrNoRows {
				return nil, nil
			}
			return nil, err
		}
		if home.Valid && home.String != "" {
			meta.HomeDomain = &home.String
		}
		meta.AuthRequired = stellarFlagSet(flags, 1)
		meta.AuthRevocable = stellarFlagSet(flags, 2)
		meta.AuthImmutable = stellarFlagSet(flags, 4)
		meta.AuthClawbackEnabled = stellarFlagSet(flags, 8)
		return &meta, nil
	}

	return nil, nil
}

func (h *SilverHandlers) queryLinkedTokens(ctx context.Context, ref assetRef) ([]LinkedTokenSummary, error) {
	if ref.IsContract {
		return nil, nil
	}

	var rows *sql.Rows
	var err error
	if h.unifiedReader != nil {
		query := fmt.Sprintf(`
			SELECT DISTINCT ON (contract_id) contract_id, token_name, token_symbol, token_decimals, token_type
			FROM (
				SELECT contract_id, token_name, token_symbol, token_decimals, token_type FROM %s.token_registry WHERE asset_code = $1 AND COALESCE(asset_issuer, '') = $2
				UNION ALL
				SELECT contract_id, token_name, token_symbol, token_decimals, token_type FROM %s.token_registry WHERE asset_code = $1 AND COALESCE(asset_issuer, '') = $2
			) t
			ORDER BY contract_id
		`, h.unifiedReader.hotSchema, h.unifiedReader.coldSchema)
		rows, err = h.unifiedReader.db.QueryContext(ctx, query, ref.AssetCode, ref.AssetIssuer)
	} else if h.legacyReader != nil && h.legacyReader.hot != nil {
		rows, err = h.legacyReader.hot.db.QueryContext(ctx, `
			SELECT contract_id, token_name, token_symbol, token_decimals, token_type
			FROM token_registry WHERE asset_code = $1 AND COALESCE(asset_issuer, '') = $2
			ORDER BY contract_id
		`, ref.AssetCode, ref.AssetIssuer)
	}
	if err != nil || rows == nil {
		return nil, err
	}
	defer rows.Close()

	var links []LinkedTokenSummary
	for rows.Next() {
		var link LinkedTokenSummary
		var name, symbol, tokenType sql.NullString
		var decimals sql.NullInt32
		if err := rows.Scan(&link.ContractID, &name, &symbol, &decimals, &tokenType); err != nil {
			return nil, err
		}
		if name.Valid && name.String != "" {
			link.TokenName = &name.String
		}
		if symbol.Valid && symbol.String != "" {
			link.TokenSymbol = &symbol.String
		}
		if decimals.Valid {
			d := int(decimals.Int32)
			link.TokenDecimals = &d
		}
		if tokenType.Valid {
			link.TokenType = tokenType.String
		}
		links = append(links, link)
	}
	return links, rows.Err()
}

func (h *SilverHandlers) queryRecentAssetTransfers(ctx context.Context, ref assetRef, limit int) ([]TokenTransfer, error) {
	if limit <= 0 {
		limit = 20
	}
	var rows *sql.Rows
	var err error
	if h.unifiedReader != nil {
		query := fmt.Sprintf(`
			SELECT timestamp, transaction_hash, ledger_sequence, source_type, from_account, to_account,
			       asset_code, asset_issuer, amount, token_contract_id, transaction_successful
			FROM (
				SELECT timestamp, transaction_hash, ledger_sequence, source_type, from_account, to_account,
				       asset_code, asset_issuer, amount, token_contract_id, transaction_successful
				FROM %s.token_transfers_raw
				UNION ALL
				SELECT timestamp, transaction_hash, ledger_sequence, source_type, from_account, to_account,
				       asset_code, asset_issuer, amount, token_contract_id, transaction_successful
				FROM %s.token_transfers_raw
			) t
			WHERE transaction_successful = true AND %s
			ORDER BY ledger_sequence DESC, timestamp DESC
			LIMIT %d
		`, h.unifiedReader.hotSchema, h.unifiedReader.coldSchema, assetTransferPredicate(ref, "$1", "$2"), limit)
		if ref.IsContract {
			rows, err = h.unifiedReader.db.QueryContext(ctx, query, ref.ContractID, "")
		} else {
			rows, err = h.unifiedReader.db.QueryContext(ctx, query, ref.AssetCode, ref.AssetIssuer)
		}
	} else if h.legacyReader != nil && h.legacyReader.hot != nil {
		query := fmt.Sprintf(`
			SELECT timestamp, transaction_hash, ledger_sequence, source_type, from_account, to_account,
			       asset_code, asset_issuer, amount, token_contract_id, transaction_successful
			FROM token_transfers_raw
			WHERE transaction_successful = true AND %s
			ORDER BY ledger_sequence DESC, timestamp DESC
			LIMIT %d
		`, assetTransferPredicate(ref, "$1", "$2"), limit)
		if ref.IsContract {
			rows, err = h.legacyReader.hot.db.QueryContext(ctx, query, ref.ContractID, "")
		} else {
			rows, err = h.legacyReader.hot.db.QueryContext(ctx, query, ref.AssetCode, ref.AssetIssuer)
		}
	}
	if err != nil || rows == nil {
		return nil, err
	}
	defer rows.Close()

	var transfers []TokenTransfer
	for rows.Next() {
		var t TokenTransfer
		if err := rows.Scan(&t.Timestamp, &t.TransactionHash, &t.LedgerSequence, &t.SourceType, &t.FromAccount, &t.ToAccount, &t.AssetCode, &t.AssetIssuer, &t.Amount, &t.TokenContractID, &t.TransactionSuccessful); err != nil {
			return nil, err
		}
		transfers = append(transfers, t)
	}
	return transfers, rows.Err()
}

func assetTransferPredicate(ref assetRef, p1, p2 string) string {
	if ref.IsContract {
		return "token_contract_id = " + p1
	}
	if ref.IsNative {
		return "(asset_code = 'XLM' OR asset_code IS NULL OR asset_code = '')"
	}
	return "asset_code = " + p1 + " AND COALESCE(asset_issuer, '') = " + p2
}

func (h *SilverHandlers) queryTopAssetPairs(ctx context.Context, ref assetRef, limit int) ([]AssetPairSummary, error) {
	if ref.IsContract || limit <= 0 {
		return nil, nil
	}
	if h.legacyReader == nil || h.legacyReader.hot == nil {
		return nil, nil
	}
	query := `
		WITH matched AS (
			SELECT buying_asset_code AS counter_code,
			       buying_asset_issuer AS counter_issuer,
			       selling_amount::numeric AS base_volume,
			       buying_amount::numeric AS counter_volume,
			       price::text AS last_price,
			       trade_timestamp
			FROM trades
			WHERE trade_timestamp >= NOW() - INTERVAL '24 hours'
			  AND (( $1 = 'XLM' AND (selling_asset_code IS NULL OR selling_asset_code = ''))
			       OR (selling_asset_code = $1 AND COALESCE(selling_asset_issuer, '') = $2))
			UNION ALL
			SELECT selling_asset_code AS counter_code,
			       selling_asset_issuer AS counter_issuer,
			       buying_amount::numeric AS base_volume,
			       selling_amount::numeric AS counter_volume,
			       CASE WHEN price IS NULL OR price = 0 THEN NULL ELSE (1 / price)::text END AS last_price,
			       trade_timestamp
			FROM trades
			WHERE trade_timestamp >= NOW() - INTERVAL '24 hours'
			  AND (( $1 = 'XLM' AND (buying_asset_code IS NULL OR buying_asset_code = ''))
			       OR (buying_asset_code = $1 AND COALESCE(buying_asset_issuer, '') = $2))
		)
		SELECT COALESCE(counter_code, 'XLM') AS counter_code,
		       COALESCE(counter_issuer, '') AS counter_issuer,
		       COUNT(*) AS trade_count_24h,
		       COALESCE(SUM(base_volume), 0)::text,
		       COALESCE(SUM(counter_volume), 0)::text,
		       (ARRAY_AGG(last_price ORDER BY trade_timestamp DESC))[1]
		FROM matched
		GROUP BY counter_code, counter_issuer
		ORDER BY trade_count_24h DESC, counter_code ASC
		LIMIT $3
	`
	rows, err := h.legacyReader.hot.db.QueryContext(ctx, query, ref.AssetCode, ref.AssetIssuer, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var pairs []AssetPairSummary
	for rows.Next() {
		var p AssetPairSummary
		var counterCode, counterIssuer, baseVol, counterVol sql.NullString
		var lastPrice sql.NullString
		if err := rows.Scan(&counterCode, &counterIssuer, &p.TradeCount24h, &baseVol, &counterVol, &lastPrice); err != nil {
			return nil, err
		}
		p.CounterAsset = buildAssetInfo("", counterCode.String, counterIssuer.String)
		p.BaseVolume24h = baseVol.String
		p.CounterVolume24h = counterVol.String
		if lastPrice.Valid && lastPrice.String != "" {
			p.LastPrice = &lastPrice.String
		}
		pairs = append(pairs, p)
	}
	return pairs, rows.Err()
}

func (h *SilverHandlers) getClassicAssetStats(ctx context.Context, ref assetRef) (any, error) {
	if h.legacyReader != nil && h.legacyReader.hot != nil {
		if resp, err := h.legacyReader.hot.GetServingTokenStats(ctx, ref.AssetCode, ref.AssetIssuer); err == nil && resp != nil {
			return resp, nil
		}
	}
	if h.unifiedReader != nil {
		return h.unifiedReader.GetTokenStats(ctx, ref.AssetCode, ref.AssetIssuer)
	}
	return nil, fmt.Errorf("asset stats require unified reader")
}

func (h *SilverHandlers) getClassicAssetTopHolders(ctx context.Context, ref assetRef, limit int) (any, error) {
	filters := TokenHoldersFilters{AssetCode: ref.AssetCode, AssetIssuer: ref.AssetIssuer, Limit: limit}
	if h.legacyReader != nil && h.legacyReader.hot != nil {
		if resp, err := h.legacyReader.hot.GetServingTokenHolders(ctx, filters); err == nil && resp != nil {
			return resp.Holders, nil
		}
	}
	if h.unifiedReader != nil {
		resp, err := h.unifiedReader.GetTokenHolders(ctx, filters)
		if err != nil {
			return nil, err
		}
		if resp != nil {
			return resp.Holders, nil
		}
	}
	return nil, fmt.Errorf("asset holders require unified reader")
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

// ============================================
// DATA BOUNDARIES ENDPOINT (RPC v2 Compatibility)
// ============================================

// HandleDataBoundaries returns the available ledger range in the data store
// This provides data freshness information for RPC v2 compatibility
// @Summary Get data boundaries
// @Description Returns the range of ledgers available in the data store (oldest and latest)
// @Tags Data
// @Accept json
// @Produce json
// @Success 200 {object} map[string]interface{} "Data boundaries with ledger range"
// @Failure 500 {object} map[string]interface{} "Internal server error"
// @Router /api/v1/silver/data-boundaries [get]
func (h *SilverHandlers) HandleDataBoundaries(w http.ResponseWriter, r *http.Request) {
	if h.unifiedReader == nil {
		respondError(w, "data boundaries endpoint requires unified reader", http.StatusInternalServerError)
		return
	}

	availableLedgers, err := h.unifiedReader.GetAvailableLedgers(r.Context())
	if err != nil {
		respondError(w, err.Error(), http.StatusInternalServerError)
		return
	}

	response := map[string]interface{}{
		"available_ledgers": availableLedgers,
		"generated_at":      time.Now().Format(time.RFC3339),
	}

	respondJSON(w, response)
}

func respondJSON(w http.ResponseWriter, data interface{}) {
	if err := writeJSON(w, http.StatusOK, data, nil); err != nil {
		writeError(w, http.StatusInternalServerError, "failed to encode JSON response")
	}
}

func respondError(w http.ResponseWriter, message string, statusCode int) {
	writeError(w, statusCode, message)
}

// boolToInt converts bool to int for logging purposes
func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}
