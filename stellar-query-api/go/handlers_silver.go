package main

import (
	"encoding/json"
	"net/http"
	"strconv"
	"time"
)

// SilverHandlers contains HTTP handlers for Silver layer queries
type SilverHandlers struct {
	reader *UnifiedSilverReader
}

// NewSilverHandlers creates new Silver API handlers
func NewSilverHandlers(reader *UnifiedSilverReader) *SilverHandlers {
	return &SilverHandlers{reader: reader}
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

	account, err := h.reader.GetAccountCurrent(r.Context(), accountID)
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

	history, nextCursor, hasMore, err := h.reader.GetAccountHistoryWithCursor(r.Context(), accountID, limit, cursor)
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

	accounts, err := h.reader.GetTopAccounts(r.Context(), limit)
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

	accounts, nextCursor, hasMore, err := h.reader.GetAccountsListWithCursor(r.Context(), filters)
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

	operations, nextCursor, hasMore, err := h.reader.GetEnrichedOperationsWithCursor(r.Context(), filters)
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

	operations, nextCursor, hasMore, err := h.reader.GetEnrichedOperationsWithCursor(r.Context(), filters)
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

	operations, nextCursor, hasMore, err := h.reader.GetEnrichedOperationsWithCursor(r.Context(), filters)
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

	transfers, nextCursor, hasMore, err := h.reader.GetTokenTransfersWithCursor(r.Context(), filters)
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

	stats, err := h.reader.GetTokenTransferStats(r.Context(), groupBy, startTime, endTime)
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

	// Get current account state
	account, err := h.reader.GetAccountCurrent(r.Context(), accountID)
	if err != nil {
		respondError(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if account == nil {
		respondError(w, "account not found", http.StatusNotFound)
		return
	}

	// Get recent operations
	operations, err := h.reader.GetEnrichedOperations(r.Context(), OperationFilters{
		AccountID: accountID,
		Limit:     10,
	})
	if err != nil {
		respondError(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Get recent transfers (both sent and received)
	transfersFrom, _ := h.reader.GetTokenTransfers(r.Context(), TransferFilters{
		FromAccount: accountID,
		StartTime:   time.Now().Add(-7 * 24 * time.Hour),
		EndTime:     time.Now(),
		Limit:       10,
	})
	transfersTo, _ := h.reader.GetTokenTransfers(r.Context(), TransferFilters{
		ToAccount: accountID,
		StartTime: time.Now().Add(-7 * 24 * time.Hour),
		EndTime:   time.Now(),
		Limit:     10,
	})
	// Combine and dedupe transfers
	transfers := append(transfersFrom, transfersTo...)

	respondJSON(w, map[string]interface{}{
		"account":             account,
		"recent_operations":   operations,
		"recent_transfers":    transfers,
		"operations_count":    len(operations),
		"transfers_count":     len(transfers),
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

	// Get all operations for this transaction
	operations, err := h.reader.GetEnrichedOperations(r.Context(), OperationFilters{
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

	// Get transfers for this transaction
	transfers, err := h.reader.GetTokenTransfers(r.Context(), TransferFilters{
		Limit: 100,
	})
	if err != nil {
		// Non-fatal, just log
		transfers = []TokenTransfer{}
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

	// Get recent transfers for this asset
	transfers, err := h.reader.GetTokenTransfers(r.Context(), TransferFilters{
		AssetCode: assetCode,
		StartTime: time.Now().Add(-24 * time.Hour),
		EndTime:   time.Now(),
		Limit:     100,
	})
	if err != nil {
		respondError(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Get stats
	stats, err := h.reader.GetTokenTransferStats(r.Context(), "asset",
		time.Now().Add(-24*time.Hour), time.Now())
	if err != nil {
		respondError(w, err.Error(), http.StatusInternalServerError)
		return
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
		"asset_code":      assetCode,
		"recent_transfers": transfers,
		"stats_24h":       assetStats,
		"transfer_count":  len(transfers),
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
