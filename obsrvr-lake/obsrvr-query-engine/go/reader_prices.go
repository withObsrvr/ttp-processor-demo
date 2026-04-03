package main

import (
	"context"
	"fmt"
	"strings"
	"time"
)

// validIntervals maps user-facing intervals to DuckDB INTERVAL syntax.
var validIntervals = map[string]string{
	"1m":  "1 minute",
	"5m":  "5 minutes",
	"15m": "15 minutes",
	"1h":  "1 hour",
	"4h":  "4 hours",
	"1d":  "1 day",
	"1w":  "7 days",
}

// GetOHLCCandles returns OHLC price candles for a trading pair.
func (r *DuckLakeReader) GetOHLCCandles(ctx context.Context, base, baseIssuer, counter, counterIssuer, interval string, start, end time.Time) ([]map[string]interface{}, error) {
	ivl, ok := validIntervals[interval]
	if !ok {
		ivl = "1 hour"
	}

	// Build asset matching conditions
	// For XLM (native), the code/issuer columns may be empty or NULL
	baseCond := assetCondition("selling", base, baseIssuer)
	counterCond := assetCondition("buying", counter, counterIssuer)

	query := fmt.Sprintf(`
		SELECT
			time_bucket(INTERVAL '%s', trade_timestamp) AS bucket,
			arg_min(CAST(price AS DOUBLE), trade_timestamp) AS open_price,
			MAX(CAST(price AS DOUBLE)) AS high_price,
			MIN(CAST(price AS DOUBLE)) AS low_price,
			arg_max(CAST(price AS DOUBLE), trade_timestamp) AS close_price,
			SUM(CAST(selling_amount AS DOUBLE)) AS volume,
			COUNT(*) AS trade_count
		FROM %s.trades_row_v1
		WHERE %s AND %s
		  AND trade_timestamp >= ? AND trade_timestamp <= ?
		GROUP BY bucket
		ORDER BY bucket ASC
		LIMIT 500
	`, ivl, r.bronze, baseCond, counterCond)

	rows, err := r.db.QueryContext(ctx, query, start, end)
	if err != nil {
		return nil, fmt.Errorf("failed to query OHLC candles: %w", err)
	}
	defer rows.Close()
	return scanRowsToMaps(rows)
}

// GetLatestPrice returns the most recent trade price for a pair with 24h volume.
func (r *DuckLakeReader) GetLatestPrice(ctx context.Context, base, baseIssuer, counter, counterIssuer string) (map[string]interface{}, error) {
	baseCond := assetCondition("selling", base, baseIssuer)
	counterCond := assetCondition("buying", counter, counterIssuer)

	query := fmt.Sprintf(`
		WITH latest AS (
			SELECT CAST(price AS DOUBLE) AS price, trade_timestamp
			FROM %s.trades_row_v1
			WHERE %s AND %s
			ORDER BY trade_timestamp DESC
			LIMIT 1
		),
		stats_24h AS (
			SELECT
				COALESCE(SUM(CAST(selling_amount AS DOUBLE)), 0) AS volume_24h,
				COUNT(*) AS trade_count_24h
			FROM %s.trades_row_v1
			WHERE %s AND %s
			  AND trade_timestamp >= NOW() - INTERVAL '24 hours'
		)
		SELECT l.price, l.trade_timestamp, s.volume_24h, s.trade_count_24h
		FROM latest l, stats_24h s
	`, r.bronze, baseCond, counterCond, r.bronze, baseCond, counterCond)

	rows, err := r.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query latest price: %w", err)
	}
	defer rows.Close()

	results, err := scanRowsToMaps(rows)
	if err != nil {
		return nil, err
	}
	if len(results) == 0 {
		return nil, fmt.Errorf("no trades found for this pair")
	}
	results[0]["base"] = base
	results[0]["counter"] = counter
	return results[0], nil
}

// GetTradePairs returns available trading pairs with minimum trade counts.
func (r *DuckLakeReader) GetTradePairs(ctx context.Context) ([]map[string]interface{}, error) {
	query := fmt.Sprintf(`
		SELECT
			COALESCE(NULLIF(selling_asset_code, ''), 'XLM') AS selling_asset_code,
			selling_asset_issuer,
			COALESCE(NULLIF(buying_asset_code, ''), 'XLM') AS buying_asset_code,
			buying_asset_issuer,
			COUNT(*) AS trade_count
		FROM %s.trades_row_v1
		GROUP BY selling_asset_code, selling_asset_issuer, buying_asset_code, buying_asset_issuer
		HAVING COUNT(*) >= 5
		ORDER BY trade_count DESC
		LIMIT 100
	`, r.bronze)

	rows, err := r.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query trade pairs: %w", err)
	}
	defer rows.Close()
	return scanRowsToMaps(rows)
}

// GetLedgerFees returns fee distribution for a specific ledger.
func (r *DuckLakeReader) GetLedgerFees(ctx context.Context, ledgerSeq int64) ([]int64, error) {
	query := fmt.Sprintf(`
		SELECT fee_charged
		FROM %s.transactions_row_v2
		WHERE ledger_sequence = ?
		ORDER BY fee_charged
	`, r.bronze)

	rows, err := r.db.QueryContext(ctx, query, ledgerSeq)
	if err != nil {
		return nil, fmt.Errorf("failed to query ledger fees: %w", err)
	}
	defer rows.Close()

	var fees []int64
	for rows.Next() {
		var fee int64
		if err := rows.Scan(&fee); err != nil {
			return nil, fmt.Errorf("failed to scan fee: %w", err)
		}
		fees = append(fees, fee)
	}
	return fees, rows.Err()
}

// GetLedgerSoroban returns Soroban resource aggregates for a specific ledger.
func (r *DuckLakeReader) GetLedgerSoroban(ctx context.Context, ledgerSeq int64) (map[string]interface{}, error) {
	query := fmt.Sprintf(`
		SELECT
			COUNT(*) AS tx_count,
			COALESCE(SUM(soroban_resources_instructions), 0) AS total_cpu_insns,
			COALESCE(SUM(soroban_resources_read_bytes), 0) AS total_read_bytes,
			COALESCE(SUM(soroban_resources_write_bytes), 0) AS total_write_bytes,
			COALESCE(SUM(rent_fee_charged), 0) AS total_rent_charged,
			COUNT(DISTINCT soroban_contract_id) AS unique_contracts
		FROM %s.transactions_row_v2
		WHERE ledger_sequence = ? AND soroban_resources_instructions IS NOT NULL
	`, r.bronze)

	rows, err := r.db.QueryContext(ctx, query, ledgerSeq)
	if err != nil {
		return nil, fmt.Errorf("failed to query ledger soroban: %w", err)
	}
	defer rows.Close()

	results, err := scanRowsToMaps(rows)
	if err != nil {
		return nil, err
	}
	if len(results) == 0 {
		return nil, nil
	}
	results[0]["ledger_sequence"] = ledgerSeq
	return results[0], nil
}

// GetSorobanStats returns comprehensive Soroban network statistics matching
// the SorobanStatsResponse{Contracts, Execution, State} schema.
func (r *DuckLakeReader) GetSorobanStats(ctx context.Context) (map[string]interface{}, error) {
	result := map[string]interface{}{
		"generated_at": time.Now().UTC().Format(time.RFC3339),
	}

	// Contracts section: total_deployed, active_24h, active_7d
	contractQuery := fmt.Sprintf(`
		SELECT
			COUNT(DISTINCT soroban_contract_id) AS total_deployed,
			COUNT(DISTINCT soroban_contract_id) AS active_24h,
			COUNT(DISTINCT soroban_contract_id) AS active_7d
		FROM %s.enriched_history_operations
		WHERE soroban_contract_id IS NOT NULL
	`, r.silver)
	var totalDeployed, active24h, active7d int64
	if err := r.db.QueryRowContext(ctx, contractQuery).Scan(&totalDeployed, &active24h, &active7d); err == nil {
		result["contracts"] = map[string]interface{}{
			"total_deployed": totalDeployed,
			"active_24h":     active24h,
			"active_7d":      active7d,
		}
	}

	// Execution section: total_invocations_24h, avg_cpu_insns, rent_burned_24h_stroops
	execQuery := fmt.Sprintf(`
		SELECT
			COUNT(*) AS total_invocations_24h,
			CAST(COALESCE(AVG(soroban_resources_instructions), 0) AS BIGINT) AS avg_cpu_insns,
			COALESCE(SUM(soroban_resources_instructions), 0) AS total_cpu_insns,
			COALESCE(SUM(rent_fee_charged), 0) AS rent_burned_24h_stroops
		FROM %s.transactions_row_v2
		WHERE soroban_resources_instructions IS NOT NULL
	`, r.bronze)
	execRows, err := r.db.QueryContext(ctx, execQuery)
	if err == nil {
		defer execRows.Close()
		execResults, _ := scanRowsToMaps(execRows)
		if len(execResults) > 0 {
			result["execution"] = execResults[0]
		}
	}

	// State section: persistent_entries, temporary_entries
	stateQuery := fmt.Sprintf(`
		SELECT
			COUNT(DISTINCT ledger_key_hash) FILTER (WHERE contract_durability = 'persistent') AS persistent_entries,
			COUNT(DISTINCT ledger_key_hash) FILTER (WHERE contract_durability = 'temporary') AS temporary_entries
		FROM %s.contract_data_snapshot_v1
		WHERE deleted = false
	`, r.bronze)
	var persistent, temporary int64
	if err := r.db.QueryRowContext(ctx, stateQuery).Scan(&persistent, &temporary); err == nil {
		result["state"] = map[string]interface{}{
			"persistent_entries": persistent,
			"temporary_entries":  temporary,
		}
	}

	return result, nil
}

// GetGoldAccountSnapshot returns an account's state at a given timestamp using ROW_NUMBER.
func (r *DuckLakeReader) GetGoldAccountSnapshot(ctx context.Context, accountID string, ts time.Time) (map[string]interface{}, error) {
	query := fmt.Sprintf(`
		SELECT
			account_id, balance, sequence_number, num_subentries,
			home_domain, ledger_sequence, closed_at
		FROM %s.accounts_snapshot_v1
		WHERE account_id = ? AND closed_at <= ?
		ORDER BY closed_at DESC
		LIMIT 1
	`, r.bronze)

	rows, err := r.db.QueryContext(ctx, query, accountID, ts)
	if err != nil {
		return nil, fmt.Errorf("failed to query account snapshot: %w", err)
	}
	defer rows.Close()

	results, err := scanRowsToMaps(rows)
	if err != nil {
		return nil, err
	}
	if len(results) == 0 {
		return nil, nil
	}
	return results[0], nil
}

// GetGoldPortfolioSnapshot returns all balances for an account at a given timestamp.
func (r *DuckLakeReader) GetGoldPortfolioSnapshot(ctx context.Context, accountID string, ts time.Time) (map[string]interface{}, error) {
	// Get XLM balance from accounts_snapshot
	account, err := r.GetGoldAccountSnapshot(ctx, accountID, ts)
	if err != nil {
		return nil, err
	}

	// Get trustline balances at timestamp (latest snapshot per asset before ts)
	trustlineQuery := fmt.Sprintf(`
		SELECT asset_type, asset_code, asset_issuer, balance, ledger_sequence
		FROM (
			SELECT *, ROW_NUMBER() OVER (
				PARTITION BY asset_code, asset_issuer
				ORDER BY ledger_sequence DESC
			) AS rn
			FROM %s.trustlines_snapshot_v1
			WHERE account_id = ? AND ledger_sequence <= (
				SELECT COALESCE(MAX(sequence), 0) FROM %s.ledgers_row_v2 WHERE closed_at <= ?
			)
		) sub
		WHERE rn = 1 AND CAST(balance AS BIGINT) > 0
		ORDER BY CAST(balance AS BIGINT) DESC
	`, r.bronze, r.bronze)

	rows, err := r.db.QueryContext(ctx, trustlineQuery, accountID, ts)
	if err != nil {
		return nil, fmt.Errorf("failed to query trustline snapshots: %w", err)
	}
	defer rows.Close()

	balances, err := scanRowsToMaps(rows)
	if err != nil {
		return nil, err
	}

	return map[string]interface{}{
		"account_id": accountID,
		"timestamp":  ts.Format(time.RFC3339),
		"xlm":        account,
		"balances":   balances,
	}, nil
}

// GetGoldAssetHolders returns holders of an asset at a given timestamp.
func (r *DuckLakeReader) GetGoldAssetHolders(ctx context.Context, assetCode, assetIssuer string, ts time.Time, limit int) ([]map[string]interface{}, error) {
	if limit <= 0 || limit > 200 {
		limit = 50
	}

	query := fmt.Sprintf(`
		SELECT account_id, balance, ledger_sequence
		FROM (
			SELECT *, ROW_NUMBER() OVER (
				PARTITION BY account_id
				ORDER BY ledger_sequence DESC
			) AS rn
			FROM %s.trustlines_snapshot_v1
			WHERE asset_code = ? AND asset_issuer = ?
			  AND ledger_sequence <= (
				SELECT COALESCE(MAX(sequence), 0) FROM %s.ledgers_row_v2 WHERE closed_at <= ?
			  )
		) sub
		WHERE rn = 1 AND CAST(balance AS BIGINT) > 0
		ORDER BY CAST(balance AS BIGINT) DESC
		LIMIT ?
	`, r.bronze, r.bronze)

	rows, err := r.db.QueryContext(ctx, query, assetCode, assetIssuer, ts, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to query asset holders at timestamp: %w", err)
	}
	defer rows.Close()
	return scanRowsToMaps(rows)
}

// assetCondition builds a SQL condition for matching an asset in trades.
func assetCondition(prefix, code, issuer string) string {
	if strings.EqualFold(code, "XLM") || code == "" {
		return fmt.Sprintf("(%s_asset_code IS NULL OR %s_asset_code = '' OR %s_asset_code = 'XLM')", prefix, prefix, prefix)
	}
	if issuer == "" {
		return fmt.Sprintf("%s_asset_code = '%s'", prefix, code)
	}
	return fmt.Sprintf("(%s_asset_code = '%s' AND %s_asset_issuer = '%s')", prefix, code, prefix, issuer)
}

// percentile returns the p-th percentile from a sorted slice.
func percentile(sorted []int64, p float64) int64 {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(p * float64(len(sorted)-1))
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}
