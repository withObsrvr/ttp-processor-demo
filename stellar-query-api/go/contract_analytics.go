package main

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

// ============================================
// CONTRACT ANALYTICS TYPES
// ============================================

// ContractAnalyticsSummary extends ContractCallSummary with analytics data
type ContractAnalyticsSummary struct {
	ContractID string `json:"contract_id"`

	// Call statistics
	Stats ContractStats `json:"stats"`

	// Timeline info
	Timeline ContractTimeline `json:"timeline"`

	// Top functions called (on this contract as callee)
	TopFunctions []FunctionCount `json:"top_functions"`

	// Daily call counts for last 7 days
	DailyCalls7d []DailyCount `json:"daily_calls_7d"`
}

// ContractStats holds call statistics
type ContractStats struct {
	TotalCallsAsCaller int `json:"total_calls_as_caller"`
	TotalCallsAsCallee int `json:"total_calls_as_callee"`
	UniqueCallers      int `json:"unique_callers"`
	UniqueCallees      int `json:"unique_callees"`
}

// ContractTimeline holds first/last activity timestamps
type ContractTimeline struct {
	FirstSeen    string `json:"first_seen,omitempty"`
	LastActivity string `json:"last_activity,omitempty"`
}

// FunctionCount represents a function and its call count
type FunctionCount struct {
	Name  string `json:"name"`
	Count int    `json:"count"`
}

// DailyCount represents call count for a single day
type DailyCount struct {
	Date  string `json:"date"`
	Count int    `json:"count"`
}

// TopContract represents a contract in the top contracts ranking
type TopContract struct {
	ContractID    string `json:"contract_id"`
	TotalCalls    int    `json:"total_calls"`
	UniqueCallers int    `json:"unique_callers"`
	TopFunction   string `json:"top_function,omitempty"`
	LastActivity  string `json:"last_activity,omitempty"`
}

// ============================================
// HOT READER METHODS
// ============================================

// GetContractAnalyticsSummary returns comprehensive analytics for a contract
func (h *SilverHotReader) GetContractAnalyticsSummary(ctx context.Context, contractID string) (*ContractAnalyticsSummary, error) {
	// Get basic stats (reuse existing query logic)
	summary := &ContractAnalyticsSummary{
		ContractID: contractID,
	}

	// Query 1: Basic stats
	statsQuery := `
		WITH caller_stats AS (
			SELECT COUNT(*) as total_as_caller, COUNT(DISTINCT to_contract) as unique_callees
			FROM contract_invocation_calls WHERE from_contract = $1
		),
		callee_stats AS (
			SELECT COUNT(*) as total_as_callee, COUNT(DISTINCT from_contract) as unique_callers
			FROM contract_invocation_calls WHERE to_contract = $1
		),
		time_stats AS (
			SELECT MIN(closed_at) as first_seen, MAX(closed_at) as last_seen
			FROM contract_invocation_calls WHERE from_contract = $1 OR to_contract = $1
		)
		SELECT
			COALESCE((SELECT total_as_caller FROM caller_stats), 0),
			COALESCE((SELECT total_as_callee FROM callee_stats), 0),
			COALESCE((SELECT unique_callers FROM callee_stats), 0),
			COALESCE((SELECT unique_callees FROM caller_stats), 0),
			(SELECT first_seen FROM time_stats),
			(SELECT last_seen FROM time_stats)
	`

	var firstSeen, lastSeen sql.NullTime
	err := h.db.QueryRowContext(ctx, statsQuery, contractID).Scan(
		&summary.Stats.TotalCallsAsCaller,
		&summary.Stats.TotalCallsAsCallee,
		&summary.Stats.UniqueCallers,
		&summary.Stats.UniqueCallees,
		&firstSeen,
		&lastSeen,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to get contract stats: %w", err)
	}

	if firstSeen.Valid {
		summary.Timeline.FirstSeen = firstSeen.Time.Format(time.RFC3339)
	}
	if lastSeen.Valid {
		summary.Timeline.LastActivity = lastSeen.Time.Format(time.RFC3339)
	}

	// Query 2: Top functions (where this contract is the callee)
	topFunctionsQuery := `
		SELECT function_name, COUNT(*) as call_count
		FROM contract_invocation_calls
		WHERE to_contract = $1 AND function_name IS NOT NULL AND function_name != ''
		GROUP BY function_name
		ORDER BY call_count DESC
		LIMIT 10
	`

	rows, err := h.db.QueryContext(ctx, topFunctionsQuery, contractID)
	if err != nil {
		return nil, fmt.Errorf("failed to get top functions: %w", err)
	}
	defer rows.Close()

	summary.TopFunctions = []FunctionCount{}
	for rows.Next() {
		var fc FunctionCount
		if err := rows.Scan(&fc.Name, &fc.Count); err != nil {
			return nil, err
		}
		summary.TopFunctions = append(summary.TopFunctions, fc)
	}

	// Query 3: Daily call counts for last 7 days
	dailyQuery := `
		SELECT DATE(closed_at) as day, COUNT(*) as call_count
		FROM contract_invocation_calls
		WHERE (from_contract = $1 OR to_contract = $1)
		  AND closed_at >= NOW() - INTERVAL '7 days'
		GROUP BY DATE(closed_at)
		ORDER BY day DESC
	`

	rows, err = h.db.QueryContext(ctx, dailyQuery, contractID)
	if err != nil {
		return nil, fmt.Errorf("failed to get daily counts: %w", err)
	}
	defer rows.Close()

	summary.DailyCalls7d = []DailyCount{}
	for rows.Next() {
		var dc DailyCount
		var day time.Time
		if err := rows.Scan(&day, &dc.Count); err != nil {
			return nil, err
		}
		dc.Date = day.Format("2006-01-02")
		summary.DailyCalls7d = append(summary.DailyCalls7d, dc)
	}

	return summary, nil
}

// GetTopContracts returns the most active contracts for a given period
func (h *SilverHotReader) GetTopContracts(ctx context.Context, period string, limit int) ([]TopContract, error) {
	// Determine time interval based on period
	var interval string
	switch period {
	case "24h":
		interval = "1 day"
	case "7d":
		interval = "7 days"
	case "30d":
		interval = "30 days"
	default:
		interval = "1 day" // default to 24h
	}

	query := fmt.Sprintf(`
		WITH contract_calls AS (
			SELECT
				contract_id,
				COUNT(*) as total_calls,
				COUNT(DISTINCT caller) as unique_callers,
				MAX(closed_at) as last_activity
			FROM (
				SELECT to_contract as contract_id, from_contract as caller, closed_at
				FROM contract_invocation_calls
				WHERE closed_at >= NOW() - INTERVAL '%s'
			) sub
			GROUP BY contract_id
		),
		top_functions AS (
			SELECT DISTINCT ON (to_contract)
				to_contract as contract_id,
				function_name as top_function
			FROM (
				SELECT to_contract, function_name, COUNT(*) as cnt
				FROM contract_invocation_calls
				WHERE closed_at >= NOW() - INTERVAL '%s'
				  AND function_name IS NOT NULL AND function_name != ''
				GROUP BY to_contract, function_name
				ORDER BY to_contract, cnt DESC
			) ranked
		)
		SELECT
			cc.contract_id,
			cc.total_calls,
			cc.unique_callers,
			COALESCE(tf.top_function, '') as top_function,
			cc.last_activity
		FROM contract_calls cc
		LEFT JOIN top_functions tf ON cc.contract_id = tf.contract_id
		ORDER BY cc.total_calls DESC
		LIMIT $1
	`, interval, interval)

	rows, err := h.db.QueryContext(ctx, query, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to get top contracts: %w", err)
	}
	defer rows.Close()

	var contracts []TopContract
	for rows.Next() {
		var tc TopContract
		var lastActivity sql.NullTime
		if err := rows.Scan(&tc.ContractID, &tc.TotalCalls, &tc.UniqueCallers, &tc.TopFunction, &lastActivity); err != nil {
			return nil, err
		}
		if lastActivity.Valid {
			tc.LastActivity = lastActivity.Time.Format(time.RFC3339)
		}
		contracts = append(contracts, tc)
	}

	return contracts, nil
}

// ============================================
// UNIFIED READER METHODS
// ============================================

// GetContractAnalyticsSummary returns comprehensive analytics for a contract
func (u *UnifiedSilverReader) GetContractAnalyticsSummary(ctx context.Context, contractID string) (*ContractAnalyticsSummary, error) {
	// Query hot storage (recent data is most relevant for analytics)
	summary, err := u.hot.GetContractAnalyticsSummary(ctx, contractID)
	if err != nil {
		return nil, err
	}

	// TODO: Merge with cold storage for historical completeness
	return summary, nil
}

// GetTopContracts returns the most active contracts for a given period
func (u *UnifiedSilverReader) GetTopContracts(ctx context.Context, period string, limit int) ([]TopContract, error) {
	// Query hot storage (top contracts are about recent activity)
	contracts, err := u.hot.GetTopContracts(ctx, period, limit)
	if err != nil {
		return nil, err
	}

	// TODO: Consider merging with cold for longer periods (30d)
	return contracts, nil
}
