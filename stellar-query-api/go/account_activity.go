package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"

	"github.com/gorilla/mux"
)

// ============================================
// ACCOUNT ACTIVITY TYPES
// ============================================

// ActivityItem represents a single activity in the unified feed
type ActivityItem struct {
	Type           string                 `json:"type"`
	Timestamp      string                 `json:"timestamp"`
	LedgerSequence int64                  `json:"ledger_sequence"`
	TxHash         string                 `json:"tx_hash"`
	Details        map[string]interface{} `json:"details"`
}

// ActivityResponse is the response for account activity endpoint
type ActivityResponse struct {
	AccountID string         `json:"account_id"`
	Activity  []ActivityItem `json:"activity"`
	Cursor    string         `json:"cursor,omitempty"`
	HasMore   bool           `json:"has_more"`
	Count     int            `json:"count"`
}

// ActivityCursor for pagination
type ActivityCursor struct {
	LedgerSequence int64  `json:"l"`
	Timestamp      string `json:"t"`
}

func (c *ActivityCursor) Encode() string {
	data, _ := json.Marshal(c)
	return base64.StdEncoding.EncodeToString(data)
}

func decodeActivityCursor(s string) (*ActivityCursor, error) {
	if s == "" {
		return nil, nil
	}
	data, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		return nil, fmt.Errorf("invalid cursor: %w", err)
	}
	var cursor ActivityCursor
	if err := json.Unmarshal(data, &cursor); err != nil {
		return nil, fmt.Errorf("invalid cursor format: %w", err)
	}
	return &cursor, nil
}

// ============================================
// HOT READER METHODS
// ============================================

// GetAccountActivity returns unified activity feed for an account
func (h *SilverHotReader) GetAccountActivity(ctx context.Context, accountID string, limit int, cursor *ActivityCursor) ([]ActivityItem, error) {
	// Query multiple sources in parallel and merge
	// For simplicity, we'll query sequentially and merge

	var allActivity []ActivityItem

	// 1. Get payments sent/received from enriched_operations
	payments, err := h.getPaymentActivity(ctx, accountID, limit*2, cursor)
	if err != nil {
		return nil, fmt.Errorf("failed to get payments: %w", err)
	}
	allActivity = append(allActivity, payments...)

	// 2. Get contract calls (as caller) from contract_invocation_calls
	contractCalls, err := h.getContractCallActivity(ctx, accountID, limit*2, cursor)
	if err != nil {
		// Non-fatal - account may not have contract activity
		contractCalls = nil
	}
	allActivity = append(allActivity, contractCalls...)

	// Sort by timestamp DESC
	sort.Slice(allActivity, func(i, j int) bool {
		return allActivity[i].Timestamp > allActivity[j].Timestamp
	})

	// Apply limit
	if len(allActivity) > limit {
		allActivity = allActivity[:limit]
	}

	return allActivity, nil
}

// getPaymentActivity retrieves payment-related activity
func (h *SilverHotReader) getPaymentActivity(ctx context.Context, accountID string, limit int, cursor *ActivityCursor) ([]ActivityItem, error) {
	query := `
		SELECT
			CASE WHEN source_account = $1 THEN 'payment_sent' ELSE 'payment_received' END as activity_type,
			ledger_closed_at,
			ledger_sequence,
			transaction_hash,
			source_account,
			destination,
			asset_code,
			asset_issuer,
			amount
		FROM enriched_history_operations
		WHERE (source_account = $1 OR destination = $1)
		  AND is_payment_op = true
	`

	args := []interface{}{accountID}

	if cursor != nil {
		query += " AND (ledger_sequence < $2 OR (ledger_sequence = $2 AND ledger_closed_at < $3))"
		args = append(args, cursor.LedgerSequence, cursor.Timestamp)
	}

	query += " ORDER BY ledger_sequence DESC, ledger_closed_at DESC LIMIT $" + strconv.Itoa(len(args)+1)
	args = append(args, limit)

	rows, err := h.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var activities []ActivityItem
	for rows.Next() {
		var activityType, timestamp, txHash string
		var sourceAccount, destination, assetCode, assetIssuer *string
		var amount *string
		var ledgerSeq int64

		err := rows.Scan(&activityType, &timestamp, &ledgerSeq, &txHash,
			&sourceAccount, &destination, &assetCode, &assetIssuer, &amount)
		if err != nil {
			continue
		}

		details := make(map[string]interface{})
		if activityType == "payment_sent" {
			if destination != nil {
				details["to"] = *destination
			}
		} else {
			if sourceAccount != nil {
				details["from"] = *sourceAccount
			}
		}
		if amount != nil {
			details["amount"] = *amount
		}
		if assetCode != nil && *assetCode != "" {
			details["asset_code"] = *assetCode
		} else {
			details["asset_code"] = "XLM"
		}
		if assetIssuer != nil && *assetIssuer != "" {
			details["asset_issuer"] = *assetIssuer
		}

		activities = append(activities, ActivityItem{
			Type:           activityType,
			Timestamp:      timestamp,
			LedgerSequence: ledgerSeq,
			TxHash:         txHash,
			Details:        details,
		})
	}

	return activities, nil
}

// getContractCallActivity retrieves contract call activity for an account
// Note: This checks if the account appears in contract calls (via source_account join)
func (h *SilverHotReader) getContractCallActivity(ctx context.Context, accountID string, limit int, cursor *ActivityCursor) ([]ActivityItem, error) {
	// For accounts, we need to check if they initiated contract calls
	// This requires joining with enriched_history_operations to find transactions
	// where the source_account matches and the operation is INVOKE_HOST_FUNCTION

	query := `
		SELECT
			'contract_call' as activity_type,
			c.closed_at,
			c.ledger_sequence,
			c.transaction_hash,
			c.from_contract,
			c.to_contract,
			c.function_name
		FROM contract_invocation_calls c
		INNER JOIN enriched_history_operations o
			ON c.transaction_hash = o.transaction_hash
		WHERE o.source_account = $1
		  AND o.type = 24
		  AND c.call_depth = 0
	`

	args := []interface{}{accountID}

	if cursor != nil {
		query += " AND (c.ledger_sequence < $2 OR (c.ledger_sequence = $2 AND c.closed_at < $3))"
		args = append(args, cursor.LedgerSequence, cursor.Timestamp)
	}

	query += " ORDER BY c.ledger_sequence DESC, c.closed_at DESC LIMIT $" + strconv.Itoa(len(args)+1)
	args = append(args, limit)

	rows, err := h.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var activities []ActivityItem
	for rows.Next() {
		var activityType, timestamp, txHash string
		var fromContract, toContract, functionName *string
		var ledgerSeq int64

		err := rows.Scan(&activityType, &timestamp, &ledgerSeq, &txHash,
			&fromContract, &toContract, &functionName)
		if err != nil {
			continue
		}

		details := make(map[string]interface{})
		if toContract != nil {
			details["contract_id"] = *toContract
		}
		if functionName != nil && *functionName != "" {
			details["function"] = *functionName
		}
		details["role"] = "caller"

		activities = append(activities, ActivityItem{
			Type:           activityType,
			Timestamp:      timestamp,
			LedgerSequence: ledgerSeq,
			TxHash:         txHash,
			Details:        details,
		})
	}

	return activities, nil
}

// ============================================
// UNIFIED READER METHODS
// ============================================

// GetAccountActivity returns unified activity feed
func (u *UnifiedSilverReader) GetAccountActivity(ctx context.Context, accountID string, limit int, cursor *ActivityCursor) ([]ActivityItem, bool, error) {
	// Query hot storage for recent activity
	// For full history, we'd query cold too, but hot covers the main use case
	activity, err := u.hot.GetAccountActivity(ctx, accountID, limit+1, cursor)
	if err != nil {
		return nil, false, err
	}

	hasMore := len(activity) > limit
	if hasMore {
		activity = activity[:limit]
	}

	return activity, hasMore, nil
}

// ============================================
// HTTP HANDLERS
// ============================================

// AccountActivityHandler handles account activity requests
type AccountActivityHandler struct {
	reader *UnifiedSilverReader
}

// NewAccountActivityHandler creates a new account activity handler
func NewAccountActivityHandler(reader *UnifiedSilverReader) *AccountActivityHandler {
	return &AccountActivityHandler{reader: reader}
}

// HandleAccountActivity returns unified activity feed for an account
// GET /api/v1/silver/accounts/{id}/activity?limit=50&cursor=...
func (h *AccountActivityHandler) HandleAccountActivity(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	accountID := vars["id"]
	if accountID == "" {
		respondError(w, "account_id required", http.StatusBadRequest)
		return
	}

	// Validate account ID format (should start with G for accounts)
	if !strings.HasPrefix(accountID, "G") {
		respondError(w, "invalid account_id: must start with G", http.StatusBadRequest)
		return
	}

	// Parse limit
	limit := 50
	if limitStr := r.URL.Query().Get("limit"); limitStr != "" {
		if parsed, err := strconv.Atoi(limitStr); err == nil && parsed > 0 && parsed <= 200 {
			limit = parsed
		}
	}

	// Parse cursor
	cursor, err := decodeActivityCursor(r.URL.Query().Get("cursor"))
	if err != nil {
		respondError(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Get activity
	activity, hasMore, err := h.reader.GetAccountActivity(r.Context(), accountID, limit, cursor)
	if err != nil {
		respondError(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Generate next cursor
	var nextCursor string
	if hasMore && len(activity) > 0 {
		lastItem := activity[len(activity)-1]
		nextCursor = (&ActivityCursor{
			LedgerSequence: lastItem.LedgerSequence,
			Timestamp:      lastItem.Timestamp,
		}).Encode()
	}

	respondJSON(w, ActivityResponse{
		AccountID: accountID,
		Activity:  activity,
		Cursor:    nextCursor,
		HasMore:   hasMore,
		Count:     len(activity),
	})
}
