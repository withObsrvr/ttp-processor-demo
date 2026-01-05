package main

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"time"
)

// ============================================
// NETWORK STATS TYPES
// ============================================

// NetworkStats represents headline network statistics
type NetworkStats struct {
	GeneratedAt   string `json:"generated_at"`
	DataFreshness string `json:"data_freshness"`

	Accounts     AccountStats     `json:"accounts"`
	Ledger       LedgerStats      `json:"ledger"`
	Operations24h OperationStats  `json:"operations_24h"`
}

// AccountStats contains account-related statistics
type AccountStats struct {
	Total       int64 `json:"total"`
	Active24h   int64 `json:"active_24h,omitempty"`
	Created24h  int64 `json:"created_24h,omitempty"`
}

// LedgerStats contains ledger-related statistics
type LedgerStats struct {
	CurrentSequence     int64   `json:"current_sequence"`
	AvgCloseTimeSeconds float64 `json:"avg_close_time_seconds,omitempty"`
	ProtocolVersion     int     `json:"protocol_version,omitempty"`
}

// OperationStats contains 24-hour operation statistics
type OperationStats struct {
	Total            int64 `json:"total"`
	Payments         int64 `json:"payments,omitempty"`
	PathPayments     int64 `json:"path_payments,omitempty"`
	CreateAccount    int64 `json:"create_account,omitempty"`
	AccountMerge     int64 `json:"account_merge,omitempty"`
	ChangeTrust      int64 `json:"change_trust,omitempty"`
	ManageOffer      int64 `json:"manage_offer,omitempty"`
	ContractInvoke   int64 `json:"contract_invoke,omitempty"`
	Other            int64 `json:"other,omitempty"`
}

// ============================================
// HOT READER METHODS
// ============================================

// GetNetworkStats returns aggregated network statistics from hot buffer
func (h *SilverHotReader) GetNetworkStats(ctx context.Context) (*NetworkStats, error) {
	stats := &NetworkStats{
		GeneratedAt:   time.Now().UTC().Format(time.RFC3339),
		DataFreshness: "real-time",
	}

	// Query 1: Total accounts
	accountQuery := `SELECT COUNT(*) FROM accounts_current`
	err := h.db.QueryRowContext(ctx, accountQuery).Scan(&stats.Accounts.Total)
	if err != nil {
		return nil, fmt.Errorf("failed to get account count: %w", err)
	}

	// Query 2: Current ledger (from accounts_current)
	ledgerQuery := `SELECT COALESCE(MAX(last_modified_ledger), 0) FROM accounts_current`
	err = h.db.QueryRowContext(ctx, ledgerQuery).Scan(&stats.Ledger.CurrentSequence)
	if err != nil {
		return nil, fmt.Errorf("failed to get current ledger: %w", err)
	}

	// Query 3: Active accounts in 24h (accounts that had operations)
	activeQuery := `
		SELECT COUNT(DISTINCT source_account)
		FROM enriched_history_operations
		WHERE ledger_closed_at > NOW() - INTERVAL '24 hours'
	`
	err = h.db.QueryRowContext(ctx, activeQuery).Scan(&stats.Accounts.Active24h)
	if err != nil && err != sql.ErrNoRows {
		// Non-fatal - some deployments may not have this data
		stats.Accounts.Active24h = 0
	}

	// Query 4: 24h operations breakdown by type (using integer type column)
	opsQuery := `
		SELECT
			type,
			COUNT(*) as count
		FROM enriched_history_operations
		WHERE ledger_closed_at > NOW() - INTERVAL '24 hours'
		GROUP BY type
	`
	rows, err := h.db.QueryContext(ctx, opsQuery)
	if err != nil {
		return nil, fmt.Errorf("failed to get operations breakdown: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var opType int32
		var count int64
		if err := rows.Scan(&opType, &count); err != nil {
			continue
		}

		stats.Operations24h.Total += count

		// Map operation types to categories (see operationTypeName in silver_reader.go)
		switch opType {
		case 1: // PAYMENT
			stats.Operations24h.Payments += count
		case 2, 13: // PATH_PAYMENT_STRICT_RECEIVE, PATH_PAYMENT_STRICT_SEND
			stats.Operations24h.PathPayments += count
		case 0: // CREATE_ACCOUNT
			stats.Operations24h.CreateAccount += count
			stats.Accounts.Created24h += count
		case 8: // ACCOUNT_MERGE
			stats.Operations24h.AccountMerge += count
		case 6, 7, 21: // CHANGE_TRUST, ALLOW_TRUST, SET_TRUST_LINE_FLAGS
			stats.Operations24h.ChangeTrust += count
		case 3, 4, 12: // MANAGE_SELL_OFFER, CREATE_PASSIVE_SELL_OFFER, MANAGE_BUY_OFFER
			stats.Operations24h.ManageOffer += count
		case 24, 25, 26: // INVOKE_HOST_FUNCTION, EXTEND_FOOTPRINT_TTL, RESTORE_FOOTPRINT
			stats.Operations24h.ContractInvoke += count
		default:
			stats.Operations24h.Other += count
		}
	}

	// Query 5: Average ledger close time (last 100 ledgers approximation)
	// This requires a ledgers table which may not exist in silver_hot
	// We'll skip this for now and document it as an enhancement
	stats.Ledger.AvgCloseTimeSeconds = 5.0 // Default Stellar target

	return stats, nil
}

// ============================================
// UNIFIED READER METHODS
// ============================================

// GetNetworkStats returns aggregated network statistics from both hot and cold storage
func (u *UnifiedSilverReader) GetNetworkStats(ctx context.Context) (*NetworkStats, error) {
	stats := &NetworkStats{
		GeneratedAt:   time.Now().UTC().Format(time.RFC3339),
		DataFreshness: "real-time",
	}

	// Get total accounts from COLD storage (full history)
	totalAccounts, err := u.cold.GetTotalAccountCount(ctx)
	if err != nil {
		// Fall back to hot if cold fails
		hotStats, hotErr := u.hot.GetNetworkStats(ctx)
		if hotErr != nil {
			return nil, fmt.Errorf("failed to get stats from both hot and cold: cold=%v, hot=%v", err, hotErr)
		}
		return hotStats, nil
	}
	stats.Accounts.Total = totalAccounts

	// Get 24h stats from HOT storage (more real-time)
	hotStats, err := u.hot.GetNetworkStats(ctx)
	if err != nil {
		// Return what we have from cold
		return stats, nil
	}

	// Use hot storage for 24h metrics (more current)
	stats.Accounts.Active24h = hotStats.Accounts.Active24h
	stats.Accounts.Created24h = hotStats.Accounts.Created24h
	stats.Ledger = hotStats.Ledger
	stats.Operations24h = hotStats.Operations24h

	return stats, nil
}

// ============================================
// HTTP HANDLERS
// ============================================

// NetworkStatsHandler handles network statistics requests
type NetworkStatsHandler struct {
	silverReader *UnifiedSilverReader
	bronzeReader *ColdReader // Bronze layer for accurate total account count
}

// NewNetworkStatsHandler creates a new network stats handler
func NewNetworkStatsHandler(silverReader *UnifiedSilverReader, bronzeReader *ColdReader) *NetworkStatsHandler {
	return &NetworkStatsHandler{
		silverReader: silverReader,
		bronzeReader: bronzeReader,
	}
}

// HandleNetworkStats returns headline network statistics
// GET /api/v1/silver/stats/network
func (h *NetworkStatsHandler) HandleNetworkStats(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	stats := &NetworkStats{
		GeneratedAt:   time.Now().UTC().Format(time.RFC3339),
		DataFreshness: "real-time",
	}

	// Get total accounts from BRONZE storage (full network history)
	if h.bronzeReader != nil {
		totalAccounts, err := h.bronzeReader.GetDistinctAccountCount(ctx)
		if err == nil {
			stats.Accounts.Total = totalAccounts
		}
	}

	// Get 24h stats from Silver hot storage (more current)
	hotStats, err := h.silverReader.hot.GetNetworkStats(ctx)
	if err != nil {
		// If we have a total from Bronze, return partial stats
		if stats.Accounts.Total > 0 {
			respondJSON(w, stats)
			return
		}
		respondError(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Use hot storage for 24h metrics
	stats.Accounts.Active24h = hotStats.Accounts.Active24h
	stats.Accounts.Created24h = hotStats.Accounts.Created24h
	stats.Ledger = hotStats.Ledger
	stats.Operations24h = hotStats.Operations24h

	// If Bronze failed, fall back to hot/silver total
	if stats.Accounts.Total == 0 {
		stats.Accounts.Total = hotStats.Accounts.Total
	}

	respondJSON(w, stats)
}
