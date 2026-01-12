package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"
)

// GoldHandlers contains HTTP handlers for Gold layer queries (Snapshot API)
type GoldHandlers struct {
	reader *UnifiedSilverReader
}

// NewGoldHandlers creates new Gold API handlers
func NewGoldHandlers(reader *UnifiedSilverReader) *GoldHandlers {
	return &GoldHandlers{
		reader: reader,
	}
}

// parseSnapshotTimestamp parses timestamp from request, supporting RFC3339 and date-only formats
func parseSnapshotTimestamp(r *http.Request) (time.Time, error) {
	timestampStr := r.URL.Query().Get("timestamp")
	if timestampStr == "" {
		return time.Time{}, fmt.Errorf("timestamp required")
	}

	// Try RFC3339 first (2025-12-31T23:59:59Z)
	if t, err := time.Parse(time.RFC3339, timestampStr); err == nil {
		return t, nil
	}

	// Try date-only format (2025-12-31) - interpret as end of day
	if t, err := time.Parse("2006-01-02", timestampStr); err == nil {
		// Set to 23:59:59.999 UTC for end of day
		return t.Add(23*time.Hour + 59*time.Minute + 59*time.Second + 999*time.Millisecond), nil
	}

	return time.Time{}, fmt.Errorf("invalid timestamp format, use RFC3339 (2025-12-31T23:59:59Z) or date (2025-12-31)")
}

// ============================================
// GOLD SNAPSHOT API - Handlers
// ============================================

// HandleAccountSnapshot returns account state at a specific point in time
// GET /api/v1/gold/snapshots/account?account_id=GXXXXX&timestamp=2025-12-31T23:59:59Z
func (h *GoldHandlers) HandleAccountSnapshot(w http.ResponseWriter, r *http.Request) {
	accountID := r.URL.Query().Get("account_id")
	if accountID == "" {
		respondError(w, "account_id required", http.StatusBadRequest)
		return
	}

	timestamp, err := parseSnapshotTimestamp(r)
	if err != nil {
		respondError(w, err.Error(), http.StatusBadRequest)
		return
	}

	log.Printf("Gold API: GetAccountAtTimestamp account=%s timestamp=%s", accountID, timestamp.Format(time.RFC3339))

	snapshot, err := h.reader.GetAccountAtTimestamp(r.Context(), accountID, timestamp)
	if err != nil {
		log.Printf("Gold API error: %v", err)
		respondError(w, err.Error(), http.StatusInternalServerError)
		return
	}

	respondJSON(w, snapshot)
}

// HandlePortfolioSnapshot returns all balances for an account at a specific point in time
// GET /api/v1/gold/snapshots/portfolio?account_id=GXXXXX&timestamp=2025-12-31T23:59:59Z
func (h *GoldHandlers) HandlePortfolioSnapshot(w http.ResponseWriter, r *http.Request) {
	accountID := r.URL.Query().Get("account_id")
	if accountID == "" {
		respondError(w, "account_id required", http.StatusBadRequest)
		return
	}

	timestamp, err := parseSnapshotTimestamp(r)
	if err != nil {
		respondError(w, err.Error(), http.StatusBadRequest)
		return
	}

	log.Printf("Gold API: GetPortfolioAtTimestamp account=%s timestamp=%s", accountID, timestamp.Format(time.RFC3339))

	portfolio, err := h.reader.GetPortfolioAtTimestamp(r.Context(), accountID, timestamp)
	if err != nil {
		log.Printf("Gold API error: %v", err)
		respondError(w, err.Error(), http.StatusInternalServerError)
		return
	}

	respondJSON(w, portfolio)
}

// HandleAssetHolders returns all holders of an asset at a specific point in time
// GET /api/v1/gold/snapshots/balance?asset_code=USDC&asset_issuer=GA5ZSE...&timestamp=2025-12-31T23:59:59Z
func (h *GoldHandlers) HandleAssetHolders(w http.ResponseWriter, r *http.Request) {
	assetCode := r.URL.Query().Get("asset_code")
	if assetCode == "" {
		respondError(w, "asset_code required", http.StatusBadRequest)
		return
	}

	assetIssuer := r.URL.Query().Get("asset_issuer")
	// asset_issuer is required for non-XLM assets
	if assetCode != "XLM" && assetIssuer == "" {
		respondError(w, "asset_issuer required for non-native assets", http.StatusBadRequest)
		return
	}

	timestamp, err := parseSnapshotTimestamp(r)
	if err != nil {
		respondError(w, err.Error(), http.StatusBadRequest)
		return
	}

	limit := parseLimit(r, 1000, 10000)
	minBalance := r.URL.Query().Get("min_balance")

	log.Printf("Gold API: GetAssetHoldersAtTimestamp asset=%s:%s timestamp=%s limit=%d",
		assetCode, assetIssuer, timestamp.Format(time.RFC3339), limit)

	holders, err := h.reader.GetAssetHoldersAtTimestamp(r.Context(), assetCode, assetIssuer, timestamp, limit, minBalance)
	if err != nil {
		log.Printf("Gold API error: %v", err)
		respondError(w, err.Error(), http.StatusInternalServerError)
		return
	}

	respondJSON(w, holders)
}

// HandleBatchAccounts returns state for multiple accounts at a specific point in time
// POST /api/v1/gold/snapshots/accounts/batch
// Request body: {"account_ids": ["GABC...", "GDEF..."], "timestamp": "2025-12-31T23:59:59Z"}
func (h *GoldHandlers) HandleBatchAccounts(w http.ResponseWriter, r *http.Request) {
	// Parse request body
	var req BatchAccountsRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		respondError(w, "invalid request body: "+err.Error(), http.StatusBadRequest)
		return
	}

	if len(req.AccountIDs) == 0 {
		respondError(w, "account_ids required", http.StatusBadRequest)
		return
	}

	if len(req.AccountIDs) > 100 {
		respondError(w, "maximum 100 accounts per batch request", http.StatusBadRequest)
		return
	}

	if req.Timestamp == "" {
		respondError(w, "timestamp required", http.StatusBadRequest)
		return
	}

	// Parse timestamp
	timestamp, err := time.Parse(time.RFC3339, req.Timestamp)
	if err != nil {
		// Try date-only format
		timestamp, err = time.Parse("2006-01-02", req.Timestamp)
		if err != nil {
			respondError(w, "invalid timestamp format", http.StatusBadRequest)
			return
		}
		// Set to end of day
		timestamp = timestamp.Add(23*time.Hour + 59*time.Minute + 59*time.Second + 999*time.Millisecond)
	}

	log.Printf("Gold API: GetBatchAccountsAtTimestamp count=%d timestamp=%s",
		len(req.AccountIDs), timestamp.Format(time.RFC3339))

	result, err := h.reader.GetBatchAccountsAtTimestamp(r.Context(), req.AccountIDs, timestamp)
	if err != nil {
		log.Printf("Gold API error: %v", err)
		respondError(w, err.Error(), http.StatusInternalServerError)
		return
	}

	respondJSON(w, result)
}
