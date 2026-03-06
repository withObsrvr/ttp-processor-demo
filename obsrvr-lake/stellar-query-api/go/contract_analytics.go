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

	// Enhanced analytics
	SuccessRate  float64 `json:"success_rate"`
	TotalCalls7d  int64  `json:"total_calls_7d"`
	TotalCalls30d int64  `json:"total_calls_30d"`

	// Timeline info
	Timeline ContractTimeline `json:"timeline"`

	// Top functions called (on this contract as callee)
	TopFunctions []FunctionCount `json:"top_functions"`

	// Daily call counts for last 7 days
	DailyCalls7d []DailyCount `json:"daily_calls_7d"`

	// Daily call counts for last 30 days
	DailyCalls30d []DailyCount `json:"daily_calls_30d,omitempty"`
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
	Name        string  `json:"name"`
	Count       int     `json:"count"`
	Calls7d     int64   `json:"calls_7d,omitempty"`
	Calls30d    int64   `json:"calls_30d,omitempty"`
	SuccessRate float64 `json:"success_rate,omitempty"`
	LastCalled  string  `json:"last_called,omitempty"`
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
	UnknownCalls  int    `json:"unknown_calls"` // Calls where function name couldn't be extracted (typically depth-1 diagnostic events)
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

	// Query 4: Success rate and time-windowed counts from contract_invocations_raw
	enhancedQuery := `
		SELECT
			COUNT(*) FILTER (WHERE successful) as success_count,
			COUNT(*) as total_count,
			COUNT(*) FILTER (WHERE closed_at >= NOW() - INTERVAL '7 days') as calls_7d,
			COUNT(*) FILTER (WHERE closed_at >= NOW() - INTERVAL '30 days') as calls_30d
		FROM contract_invocations_raw
		WHERE contract_id = $1
	`
	var successCount, totalCount sql.NullInt64
	err = h.db.QueryRowContext(ctx, enhancedQuery, contractID).Scan(
		&successCount, &totalCount,
		&summary.TotalCalls7d, &summary.TotalCalls30d,
	)
	if err == nil && totalCount.Valid && totalCount.Int64 > 0 {
		summary.SuccessRate = float64(successCount.Int64) / float64(totalCount.Int64)
	}

	// Query 5: Enhanced top functions with 7d/30d counts and success rates
	enhancedFunctionsQuery := `
		SELECT
			function_name,
			COUNT(*) as total_count,
			COUNT(*) FILTER (WHERE closed_at >= NOW() - INTERVAL '7 days') as calls_7d,
			COUNT(*) FILTER (WHERE closed_at >= NOW() - INTERVAL '30 days') as calls_30d,
			COUNT(*) FILTER (WHERE successful)::float / NULLIF(COUNT(*), 0) as success_rate,
			MAX(closed_at) as last_called
		FROM contract_invocations_raw
		WHERE contract_id = $1 AND function_name != ''
		GROUP BY function_name
		ORDER BY total_count DESC
		LIMIT 10
	`
	funcRows, err := h.db.QueryContext(ctx, enhancedFunctionsQuery, contractID)
	if err == nil {
		defer funcRows.Close()
		enhancedFunctions := []FunctionCount{}
		for funcRows.Next() {
			var fc FunctionCount
			var successRate sql.NullFloat64
			var lastCalled sql.NullTime
			if err := funcRows.Scan(&fc.Name, &fc.Count, &fc.Calls7d, &fc.Calls30d, &successRate, &lastCalled); err != nil {
				break
			}
			if successRate.Valid {
				fc.SuccessRate = successRate.Float64
			}
			if lastCalled.Valid {
				fc.LastCalled = lastCalled.Time.Format(time.RFC3339)
			}
			enhancedFunctions = append(enhancedFunctions, fc)
		}
		if len(enhancedFunctions) > 0 {
			summary.TopFunctions = enhancedFunctions
		}
	}

	// Query 6: Daily calls for last 30 days
	daily30dQuery := `
		SELECT DATE(closed_at) as day, COUNT(*) as call_count
		FROM contract_invocations_raw
		WHERE contract_id = $1
		  AND closed_at >= NOW() - INTERVAL '30 days'
		GROUP BY DATE(closed_at)
		ORDER BY day DESC
	`
	daily30dRows, err := h.db.QueryContext(ctx, daily30dQuery, contractID)
	if err == nil {
		defer daily30dRows.Close()
		summary.DailyCalls30d = []DailyCount{}
		for daily30dRows.Next() {
			var dc DailyCount
			var day time.Time
			if err := daily30dRows.Scan(&day, &dc.Count); err != nil {
				break
			}
			dc.Date = day.Format("2006-01-02")
			summary.DailyCalls30d = append(summary.DailyCalls30d, dc)
		}
	}

	return summary, nil
}

// GetTopContracts returns the most active contracts for a given period
func (h *SilverHotReader) GetTopContracts(ctx context.Context, period string, limit int) ([]TopContract, error) {
	var query string

	if period == "all" {
		// Query all data without time filter
		query = `
			WITH contract_calls AS (
				SELECT
					contract_id,
					COUNT(*) as total_calls,
					COUNT(DISTINCT caller) as unique_callers,
					MAX(closed_at) as last_activity
				FROM (
					SELECT to_contract as contract_id, from_contract as caller, closed_at
					FROM contract_invocation_calls
				) sub
				GROUP BY contract_id
			),
			unknown_counts AS (
				SELECT to_contract as contract_id, COUNT(*) as unknown_calls
				FROM contract_invocation_calls
				WHERE function_name = 'unknown'
				GROUP BY to_contract
			),
			top_functions AS (
				SELECT DISTINCT ON (to_contract)
					to_contract as contract_id,
					function_name as top_function
				FROM (
					SELECT to_contract, function_name, COUNT(*) as cnt
					FROM contract_invocation_calls
					WHERE function_name IS NOT NULL AND function_name != '' AND function_name != 'unknown'
					GROUP BY to_contract, function_name
					ORDER BY to_contract, cnt DESC
				) ranked
			)
			SELECT
				cc.contract_id,
				cc.total_calls,
				cc.unique_callers,
				COALESCE(tf.top_function, '') as top_function,
				COALESCE(uc.unknown_calls, 0) as unknown_calls,
				cc.last_activity
			FROM contract_calls cc
			LEFT JOIN top_functions tf ON cc.contract_id = tf.contract_id
			LEFT JOIN unknown_counts uc ON cc.contract_id = uc.contract_id
			ORDER BY cc.total_calls DESC
			LIMIT $1
		`
	} else {
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

		query = fmt.Sprintf(`
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
			unknown_counts AS (
				SELECT to_contract as contract_id, COUNT(*) as unknown_calls
				FROM contract_invocation_calls
				WHERE closed_at >= NOW() - INTERVAL '%s'
				  AND function_name = 'unknown'
				GROUP BY to_contract
			),
			top_functions AS (
				SELECT DISTINCT ON (to_contract)
					to_contract as contract_id,
					function_name as top_function
				FROM (
					SELECT to_contract, function_name, COUNT(*) as cnt
					FROM contract_invocation_calls
					WHERE closed_at >= NOW() - INTERVAL '%s'
					  AND function_name IS NOT NULL AND function_name != '' AND function_name != 'unknown'
					GROUP BY to_contract, function_name
					ORDER BY to_contract, cnt DESC
				) ranked
			)
			SELECT
				cc.contract_id,
				cc.total_calls,
				cc.unique_callers,
				COALESCE(tf.top_function, '') as top_function,
				COALESCE(uc.unknown_calls, 0) as unknown_calls,
				cc.last_activity
			FROM contract_calls cc
			LEFT JOIN top_functions tf ON cc.contract_id = tf.contract_id
			LEFT JOIN unknown_counts uc ON cc.contract_id = uc.contract_id
			ORDER BY cc.total_calls DESC
			LIMIT $1
		`, interval, interval, interval)
	}

	rows, err := h.db.QueryContext(ctx, query, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to get top contracts: %w", err)
	}
	defer rows.Close()

	var contracts []TopContract
	for rows.Next() {
		var tc TopContract
		var lastActivity sql.NullTime
		if err := rows.Scan(&tc.ContractID, &tc.TotalCalls, &tc.UniqueCallers, &tc.TopFunction, &tc.UnknownCalls, &lastActivity); err != nil {
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
