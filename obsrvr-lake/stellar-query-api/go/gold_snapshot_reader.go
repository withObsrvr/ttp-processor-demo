package main

import (
	"context"
	"fmt"
	"time"
)

// ============================================
// GOLD SNAPSHOT API - Response Types
// ============================================

// AccountState represents the state of an account at a point in time
type AccountState struct {
	Balance        string          `json:"balance"`
	BalanceStroops int64           `json:"balance_stroops"`
	SequenceNumber string          `json:"sequence_number"`
	NumSubentries  int64           `json:"num_subentries"`
	HomeDomain     *string         `json:"home_domain,omitempty"`
	Thresholds     *ThresholdState `json:"thresholds,omitempty"`
	Flags          *FlagState      `json:"flags,omitempty"`
}

// ThresholdState represents account thresholds
type ThresholdState struct {
	Low  int `json:"low"`
	Med  int `json:"med"`
	High int `json:"high"`
}

// FlagState represents account flags
type FlagState struct {
	AuthRequired  bool `json:"auth_required"`
	AuthRevocable bool `json:"auth_revocable"`
	AuthImmutable bool `json:"auth_immutable"`
}

// GoldAccountSnapshotResponse for GET /gold/snapshots/account
type GoldAccountSnapshotResponse struct {
	AccountID      string        `json:"account_id"`
	SnapshotAt     string        `json:"snapshot_at"`
	SnapshotLedger int64         `json:"snapshot_ledger"`
	State          *AccountState `json:"state,omitempty"`
	GeneratedAt    string        `json:"generated_at"`
}

// BalanceSnapshot represents a single balance at a point in time
type BalanceSnapshot struct {
	AssetType   string  `json:"asset_type"`
	AssetCode   string  `json:"asset_code"`
	AssetIssuer *string `json:"asset_issuer,omitempty"`
	Balance     string  `json:"balance"`
}

// GoldPortfolioSnapshotResponse for GET /gold/snapshots/portfolio
type GoldPortfolioSnapshotResponse struct {
	AccountID      string            `json:"account_id"`
	SnapshotAt     string            `json:"snapshot_at"`
	SnapshotLedger int64             `json:"snapshot_ledger"`
	Balances       []BalanceSnapshot `json:"balances"`
	TotalAssets    int               `json:"total_assets"`
	GeneratedAt    string            `json:"generated_at"`
}

// HolderSnapshot represents an account holding an asset at a point in time
type HolderSnapshot struct {
	AccountID string `json:"account_id"`
	Balance   string `json:"balance"`
}

// HoldersSummary provides aggregate info about asset holders
type HoldersSummary struct {
	TotalHolders  int    `json:"total_holders"`
	TotalSupply   string `json:"total_supply"`
	ReturnedCount int    `json:"returned_count"`
}

// GoldAssetHoldersResponse for GET /gold/snapshots/balance
type GoldAssetHoldersResponse struct {
	Asset          AssetInfo        `json:"asset"`
	SnapshotAt     string           `json:"snapshot_at"`
	SnapshotLedger int64            `json:"snapshot_ledger"`
	Holders        []HolderSnapshot `json:"holders"`
	Summary        HoldersSummary   `json:"summary"`
	Cursor         string           `json:"cursor,omitempty"`
	HasMore        bool             `json:"has_more"`
	GeneratedAt    string           `json:"generated_at"`
}

// BatchAccountResult represents a single account in a batch request
type BatchAccountResult struct {
	AccountID string        `json:"account_id"`
	Found     bool          `json:"found"`
	State     *AccountState `json:"state,omitempty"`
	Error     string        `json:"error,omitempty"`
}

// GoldBatchAccountsResponse for POST /gold/snapshots/accounts/batch
type GoldBatchAccountsResponse struct {
	SnapshotAt     string               `json:"snapshot_at"`
	SnapshotLedger int64                `json:"snapshot_ledger"`
	Accounts       []BatchAccountResult `json:"accounts"`
	Requested      int                  `json:"requested"`
	Found          int                  `json:"found"`
	GeneratedAt    string               `json:"generated_at"`
}

// BatchAccountsRequest represents the request body for batch account lookup
type BatchAccountsRequest struct {
	AccountIDs []string `json:"account_ids"`
	Timestamp  string   `json:"timestamp"`
}

// ============================================
// GOLD SNAPSHOT API - Reader Methods
// ============================================

// GetAccountAtTimestamp returns account state at a specific timestamp
// Uses SCD Type 2 query pattern against accounts_snapshot
func (r *UnifiedSilverReader) GetAccountAtTimestamp(ctx context.Context, accountID string, timestamp time.Time) (*GoldAccountSnapshotResponse, error) {
	// Query hot reader first (PostgreSQL silver_hot)
	query := `
		SELECT
			account_id,
			balance,
			sequence_number,
			num_subentries,
			home_domain,
			ledger_sequence,
			closed_at
		FROM accounts_snapshot
		WHERE account_id = $1
		  AND closed_at <= $2
		  AND (valid_to IS NULL OR valid_to > $2)
		ORDER BY closed_at DESC
		LIMIT 1
	`

	var accountID_result string
	var balance string // Stored as text with decimal format (e.g., "100.9982800")
	var sequenceNumber int64
	var numSubentries int64
	var homeDomain *string
	var ledgerSequence int64
	var closedAt time.Time

	err := r.hot.db.QueryRowContext(ctx, query, accountID, timestamp).Scan(
		&accountID_result,
		&balance,
		&sequenceNumber,
		&numSubentries,
		&homeDomain,
		&ledgerSequence,
		&closedAt,
	)

	if err != nil {
		// Check if it's a "not found" error
		if err.Error() == "sql: no rows in result set" {
			// Return response with nil state (account didn't exist)
			return &GoldAccountSnapshotResponse{
				AccountID:      accountID,
				SnapshotAt:     timestamp.Format(time.RFC3339),
				SnapshotLedger: 0,
				State:          nil,
				GeneratedAt:    time.Now().UTC().Format(time.RFC3339),
			}, nil
		}
		return nil, fmt.Errorf("failed to query account snapshot: %w", err)
	}

	// Parse balance string to stroops (multiply by 10^7)
	balanceStroops, _ := parseBalanceToStroops(balance)

	response := &GoldAccountSnapshotResponse{
		AccountID:      accountID_result,
		SnapshotAt:     timestamp.Format(time.RFC3339),
		SnapshotLedger: ledgerSequence,
		State: &AccountState{
			Balance:        balance,
			BalanceStroops: balanceStroops,
			SequenceNumber: fmt.Sprintf("%d", sequenceNumber),
			NumSubentries:  numSubentries,
			HomeDomain:     homeDomain,
		},
		GeneratedAt: time.Now().UTC().Format(time.RFC3339),
	}

	return response, nil
}

// GetPortfolioAtTimestamp returns all balances for an account at a specific timestamp
// Combines XLM balance from accounts_snapshot and trustline balances from trustlines_snapshot
func (r *UnifiedSilverReader) GetPortfolioAtTimestamp(ctx context.Context, accountID string, timestamp time.Time) (*GoldPortfolioSnapshotResponse, error) {
	var balances []BalanceSnapshot
	var snapshotLedger int64

	// 1. Get XLM balance from accounts_snapshot
	xlmQuery := `
		SELECT balance, ledger_sequence
		FROM accounts_snapshot
		WHERE account_id = $1
		  AND closed_at <= $2
		  AND (valid_to IS NULL OR valid_to > $2)
		ORDER BY closed_at DESC
		LIMIT 1
	`

	var xlmBalance string // Balance stored as text with decimal format
	err := r.hot.db.QueryRowContext(ctx, xlmQuery, accountID, timestamp).Scan(&xlmBalance, &snapshotLedger)
	if err != nil && err.Error() != "sql: no rows in result set" {
		return nil, fmt.Errorf("failed to query XLM balance: %w", err)
	}

	if err == nil {
		// Add XLM balance
		balances = append(balances, BalanceSnapshot{
			AssetType:   "native",
			AssetCode:   "XLM",
			AssetIssuer: nil,
			Balance:     xlmBalance,
		})
	}

	// 2. Get trustline balances from trustlines_snapshot
	trustlineQuery := `
		SELECT
			asset_type,
			asset_code,
			asset_issuer,
			balance,
			ledger_sequence
		FROM trustlines_snapshot
		WHERE account_id = $1
		  AND created_at <= $2
		  AND (valid_to IS NULL OR valid_to > $2)
		  AND CAST(balance AS NUMERIC) > 0
		ORDER BY CAST(balance AS NUMERIC) DESC
	`

	rows, err := r.hot.db.QueryContext(ctx, trustlineQuery, accountID, timestamp)
	if err != nil {
		return nil, fmt.Errorf("failed to query trustline balances: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var assetType string
		var assetCode string
		var assetIssuer string
		var balance string
		var ledgerSeq int64

		if err := rows.Scan(&assetType, &assetCode, &assetIssuer, &balance, &ledgerSeq); err != nil {
			return nil, fmt.Errorf("failed to scan trustline row: %w", err)
		}

		// Update snapshot ledger to max
		if ledgerSeq > snapshotLedger {
			snapshotLedger = ledgerSeq
		}

		balances = append(balances, BalanceSnapshot{
			AssetType:   assetType,
			AssetCode:   assetCode,
			AssetIssuer: &assetIssuer,
			Balance:     balance,
		})
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating trustline rows: %w", err)
	}

	response := &GoldPortfolioSnapshotResponse{
		AccountID:      accountID,
		SnapshotAt:     timestamp.Format(time.RFC3339),
		SnapshotLedger: snapshotLedger,
		Balances:       balances,
		TotalAssets:    len(balances),
		GeneratedAt:    time.Now().UTC().Format(time.RFC3339),
	}

	return response, nil
}

// GetAssetHoldersAtTimestamp returns all holders of an asset at a specific timestamp
func (r *UnifiedSilverReader) GetAssetHoldersAtTimestamp(ctx context.Context, assetCode string, assetIssuer string, timestamp time.Time, limit int, minBalance string) (*GoldAssetHoldersResponse, error) {
	// Determine asset type
	assetType := "credit_alphanum4"
	if len(assetCode) > 4 {
		assetType = "credit_alphanum12"
	}
	if assetCode == "XLM" && assetIssuer == "" {
		assetType = "native"
	}

	// For native XLM, we need to query accounts_snapshot
	if assetType == "native" {
		return r.getXLMHoldersAtTimestamp(ctx, timestamp, limit, minBalance)
	}

	// For issued assets, query trustlines_snapshot
	query := `
		SELECT
			account_id,
			balance,
			ledger_sequence
		FROM trustlines_snapshot
		WHERE asset_code = $1
		  AND asset_issuer = $2
		  AND created_at <= $3
		  AND (valid_to IS NULL OR valid_to > $3)
		  AND CAST(balance AS NUMERIC) > 0
		ORDER BY CAST(balance AS NUMERIC) DESC
		LIMIT $4
	`

	rows, err := r.hot.db.QueryContext(ctx, query, assetCode, assetIssuer, timestamp, limit+1)
	if err != nil {
		return nil, fmt.Errorf("failed to query asset holders: %w", err)
	}
	defer rows.Close()

	var holders []HolderSnapshot
	var snapshotLedger int64

	for rows.Next() {
		var accountID string
		var balance string
		var ledgerSeq int64

		if err := rows.Scan(&accountID, &balance, &ledgerSeq); err != nil {
			return nil, fmt.Errorf("failed to scan holder row: %w", err)
		}

		if ledgerSeq > snapshotLedger {
			snapshotLedger = ledgerSeq
		}

		holders = append(holders, HolderSnapshot{
			AccountID: accountID,
			Balance:   balance,
		})
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating holder rows: %w", err)
	}

	// Check if there are more results
	hasMore := len(holders) > limit
	if hasMore {
		holders = holders[:limit]
	}

	// Get total count and supply (separate query for summary)
	summaryQuery := `
		SELECT
			COUNT(*) as total_holders,
			COALESCE(SUM(CAST(balance AS NUMERIC)), 0) as total_supply
		FROM trustlines_snapshot
		WHERE asset_code = $1
		  AND asset_issuer = $2
		  AND created_at <= $3
		  AND (valid_to IS NULL OR valid_to > $3)
		  AND CAST(balance AS NUMERIC) > 0
	`

	var totalHolders int
	var totalSupply string
	err = r.hot.db.QueryRowContext(ctx, summaryQuery, assetCode, assetIssuer, timestamp).Scan(&totalHolders, &totalSupply)
	if err != nil {
		// Non-fatal, just use zeros
		totalHolders = len(holders)
		totalSupply = "0"
	}

	response := &GoldAssetHoldersResponse{
		Asset: AssetInfo{
			Code:   assetCode,
			Issuer: &assetIssuer,
			Type:   assetType,
		},
		SnapshotAt:     timestamp.Format(time.RFC3339),
		SnapshotLedger: snapshotLedger,
		Holders:        holders,
		Summary: HoldersSummary{
			TotalHolders:  totalHolders,
			TotalSupply:   totalSupply,
			ReturnedCount: len(holders),
		},
		HasMore:     hasMore,
		GeneratedAt: time.Now().UTC().Format(time.RFC3339),
	}

	return response, nil
}

// getXLMHoldersAtTimestamp returns XLM holders from accounts_snapshot
func (r *UnifiedSilverReader) getXLMHoldersAtTimestamp(ctx context.Context, timestamp time.Time, limit int, minBalance string) (*GoldAssetHoldersResponse, error) {
	query := `
		SELECT
			account_id,
			balance,
			ledger_sequence
		FROM accounts_snapshot
		WHERE closed_at <= $1
		  AND (valid_to IS NULL OR valid_to > $1)
		  AND CAST(balance AS NUMERIC) > 0
		ORDER BY CAST(balance AS NUMERIC) DESC
		LIMIT $2
	`

	rows, err := r.hot.db.QueryContext(ctx, query, timestamp, limit+1)
	if err != nil {
		return nil, fmt.Errorf("failed to query XLM holders: %w", err)
	}
	defer rows.Close()

	var holders []HolderSnapshot
	var snapshotLedger int64

	for rows.Next() {
		var accountID string
		var balance string // Balance stored as text
		var ledgerSeq int64

		if err := rows.Scan(&accountID, &balance, &ledgerSeq); err != nil {
			return nil, fmt.Errorf("failed to scan XLM holder row: %w", err)
		}

		if ledgerSeq > snapshotLedger {
			snapshotLedger = ledgerSeq
		}

		holders = append(holders, HolderSnapshot{
			AccountID: accountID,
			Balance:   balance,
		})
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating XLM holder rows: %w", err)
	}

	hasMore := len(holders) > limit
	if hasMore {
		holders = holders[:limit]
	}

	response := &GoldAssetHoldersResponse{
		Asset: AssetInfo{
			Code:   "XLM",
			Issuer: nil,
			Type:   "native",
		},
		SnapshotAt:     timestamp.Format(time.RFC3339),
		SnapshotLedger: snapshotLedger,
		Holders:        holders,
		Summary: HoldersSummary{
			TotalHolders:  len(holders),
			TotalSupply:   "0", // XLM total supply is complex to calculate
			ReturnedCount: len(holders),
		},
		HasMore:     hasMore,
		GeneratedAt: time.Now().UTC().Format(time.RFC3339),
	}

	return response, nil
}

// GetBatchAccountsAtTimestamp returns state for multiple accounts at a timestamp
func (r *UnifiedSilverReader) GetBatchAccountsAtTimestamp(ctx context.Context, accountIDs []string, timestamp time.Time) (*GoldBatchAccountsResponse, error) {
	results := make([]BatchAccountResult, 0, len(accountIDs))
	var snapshotLedger int64
	found := 0

	for _, accountID := range accountIDs {
		// Get account snapshot
		snapshot, err := r.GetAccountAtTimestamp(ctx, accountID, timestamp)
		if err != nil {
			results = append(results, BatchAccountResult{
				AccountID: accountID,
				Found:     false,
				Error:     err.Error(),
			})
			continue
		}

		if snapshot.State != nil {
			found++
			if snapshot.SnapshotLedger > snapshotLedger {
				snapshotLedger = snapshot.SnapshotLedger
			}
			results = append(results, BatchAccountResult{
				AccountID: accountID,
				Found:     true,
				State:     snapshot.State,
			})
		} else {
			results = append(results, BatchAccountResult{
				AccountID: accountID,
				Found:     false,
				Error:     "account did not exist at this timestamp",
			})
		}
	}

	response := &GoldBatchAccountsResponse{
		SnapshotAt:     timestamp.Format(time.RFC3339),
		SnapshotLedger: snapshotLedger,
		Accounts:       results,
		Requested:      len(accountIDs),
		Found:          found,
		GeneratedAt:    time.Now().UTC().Format(time.RFC3339),
	}

	return response, nil
}
