package main

import (
	"context"
	"fmt"
	"strings"
	"time"
)

// ── Search Endpoint ──────────────────────────────────────────

// Search performs a unified search across accounts, transactions, and contracts.
func (r *DuckLakeReader) Search(ctx context.Context, q string, limit int) (map[string]interface{}, error) {
	if limit <= 0 || limit > 20 {
		limit = 5
	}

	result := map[string]interface{}{}

	// Search accounts
	accountQuery := fmt.Sprintf(`
		SELECT account_id, balance FROM (
			SELECT *, ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY ledger_sequence DESC) AS rn
			FROM %s.accounts_snapshot_v1
			WHERE account_id = ?
		) sub WHERE rn = 1
		LIMIT ?
	`, r.bronze)
	if accountRows, err := r.db.QueryContext(ctx, accountQuery, q, limit); err == nil {
		defer accountRows.Close()
		accounts, _ := scanRowsToMaps(accountRows)
		result["accounts"] = accounts
	}

	// Search transactions
	txQuery := fmt.Sprintf(`
		SELECT transaction_hash, ledger_sequence, source_account FROM %s.transactions_row_v2
		WHERE transaction_hash = ? LIMIT ?
	`, r.bronze)
	if txRows, err := r.db.QueryContext(ctx, txQuery, q, limit); err == nil {
		defer txRows.Close()
		txs, _ := scanRowsToMaps(txRows)
		result["transactions"] = txs
	}

	// Search contracts
	contractQuery := fmt.Sprintf(`
		SELECT contract_id, creator_address, wasm_hash FROM %s.contract_creations_v1
		WHERE contract_id = ? LIMIT ?
	`, r.bronze)
	if contractRows, err := r.db.QueryContext(ctx, contractQuery, q, limit); err == nil {
		defer contractRows.Close()
		contracts, _ := scanRowsToMaps(contractRows)
		result["contracts"] = contracts
	}

	return result, nil
}

// ── Fee Stats Endpoints ──────────────────────────────────────

// GetFeeStats returns fee percentiles from bronze.transactions_row_v2 for a given period.
// Falls back to all-time stats if the time window returns no results (e.g., during historical catch-up).
func (r *DuckLakeReader) GetFeeStats(ctx context.Context, period string) (map[string]interface{}, error) {
	validPeriods := map[string]string{
		"1h":  "1 hour",
		"24h": "24 hours",
		"7d":  "7 days",
	}
	interval, ok := validPeriods[period]
	if !ok {
		interval = "24 hours"
		period = "24h"
	}

	// Try time-windowed query first
	result, err := r.queryFeeStats(ctx, fmt.Sprintf("WHERE created_at > NOW() - INTERVAL '%s' AND successful = true", interval))
	if err != nil {
		return nil, err
	}

	// If windowed query returned no data, fall back to all-time
	if txCount, _ := result["tx_count"].(int64); txCount == 0 {
		result, err = r.queryFeeStats(ctx, "WHERE successful = true")
		if err != nil {
			return nil, err
		}
		result["period"] = period + " (all-time fallback)"
	} else {
		result["period"] = period
	}

	result["generated_at"] = time.Now().UTC().Format(time.RFC3339)
	if medianFee, ok := result["median_fee"].(int64); ok {
		result["surge_active"] = medianFee > 100
	} else if medianFee, ok := result["median_fee"].(float64); ok {
		result["surge_active"] = int64(medianFee) > 100
	} else {
		result["surge_active"] = false
	}
	return result, nil
}

func (r *DuckLakeReader) queryFeeStats(ctx context.Context, whereClause string) (map[string]interface{}, error) {
	query := fmt.Sprintf(`
		SELECT
			COUNT(*) AS tx_count,
			APPROX_QUANTILE(fee_charged, 0.50) AS median_fee,
			APPROX_QUANTILE(fee_charged, 0.75) AS p75_fee,
			APPROX_QUANTILE(fee_charged, 0.90) AS p90_fee,
			APPROX_QUANTILE(fee_charged, 0.99) AS p99_fee,
			MIN(fee_charged) AS min_fee,
			MAX(fee_charged) AS max_fee,
			COALESCE(SUM(fee_charged), 0) AS total_fees
		FROM %s.transactions_row_v2
		%s
	`, r.bronze, whereClause)

	rows, err := r.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query fee stats: %w", err)
	}
	defer rows.Close()

	results, err := scanRowsToMaps(rows)
	if err != nil {
		return nil, err
	}
	if len(results) == 0 {
		return map[string]interface{}{}, nil
	}
	return results[0], nil
}

// GetTransactionSummaries returns batch transaction summaries from silver.enriched_history_operations.
func (r *DuckLakeReader) GetTransactionSummaries(ctx context.Context, startLedger, endLedger int64, limit int) ([]map[string]interface{}, error) {
	if limit <= 0 || limit > 500 {
		limit = 50
	}

	query := fmt.Sprintf(`
		SELECT
			transaction_hash, ledger_sequence, source_account,
			type_string, operation_index, closed_at,
			soroban_contract_id, soroban_function, amount, destination,
			transaction_successful, fee_charged
		FROM %s.enriched_history_operations
		WHERE ledger_sequence >= ? AND ledger_sequence <= ?
		ORDER BY ledger_sequence DESC, operation_index ASC
		LIMIT ?
	`, r.silver)

	rows, err := r.db.QueryContext(ctx, query, startLedger, endLedger, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to query transaction summaries: %w", err)
	}
	defer rows.Close()
	return scanRowsToMaps(rows)
}

// GetUnifiedEvents returns unified CAP-67 events (transfer/mint/burn) from token_transfers_raw.
func (r *DuckLakeReader) GetUnifiedEvents(ctx context.Context, limit int, startLedger, endLedger int64, contractID string) ([]map[string]interface{}, error) {
	if limit <= 0 || limit > 500 {
		limit = 50
	}
	var conditions []string
	var args []interface{}
	if startLedger > 0 {
		conditions = append(conditions, "ledger_sequence >= ?")
		args = append(args, startLedger)
	}
	if endLedger > 0 {
		conditions = append(conditions, "ledger_sequence <= ?")
		args = append(args, endLedger)
	}
	if contractID != "" {
		conditions = append(conditions, "token_contract_id = ?")
		args = append(args, contractID)
	}
	whereClause := ""
	if len(conditions) > 0 {
		whereClause = "WHERE " + strings.Join(conditions, " AND ")
	}
	args = append(args, limit)
	query := fmt.Sprintf(`
		SELECT
			CASE
				WHEN from_account IS NULL OR from_account = '' THEN 'mint'
				WHEN to_account IS NULL OR to_account = '' THEN 'burn'
				ELSE 'transfer'
			END AS event_type,
			from_account AS "from",
			to_account AS "to",
			CAST(amount AS TEXT) AS amount,
			asset_code,
			asset_issuer,
			token_contract_id AS contract_id,
			transaction_hash,
			ledger_sequence,
			timestamp AS closed_at,
			source_type,
			transaction_successful
		FROM %s.token_transfers_raw
		%s
		ORDER BY ledger_sequence DESC
		LIMIT ?
	`, r.silver, whereClause)
	uRows, err := r.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to query unified events: %w", err)
	}
	defer uRows.Close()
	return scanRowsToMaps(uRows)
}

// GetGenericEvents returns contract events from bronze.contract_events_stream_v1 with filtering.
func (r *DuckLakeReader) GetGenericEvents(ctx context.Context, limit int, startLedger, endLedger int64, contractID string) ([]map[string]interface{}, error) {
	if limit <= 0 || limit > 500 {
		limit = 50
	}

	var conditions []string
	var args []interface{}

	if startLedger > 0 {
		conditions = append(conditions, "ledger_sequence >= ?")
		args = append(args, startLedger)
	}
	if endLedger > 0 {
		conditions = append(conditions, "ledger_sequence <= ?")
		args = append(args, endLedger)
	}
	if contractID != "" {
		conditions = append(conditions, "contract_id = ?")
		args = append(args, contractID)
	}

	whereClause := ""
	if len(conditions) > 0 {
		whereClause = "WHERE " + strings.Join(conditions, " AND ")
	}
	args = append(args, limit)

	query := fmt.Sprintf(`
		SELECT
			event_id, contract_id, ledger_sequence, transaction_hash,
			closed_at, event_type, in_successful_contract_call,
			topics_decoded, data_decoded, topic_count,
			operation_index, event_index
		FROM %s.contract_events_stream_v1
		%s
		ORDER BY ledger_sequence DESC, event_index DESC
		LIMIT ?
	`, r.bronze, whereClause)

	rows, err := r.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to query generic events: %w", err)
	}
	defer rows.Close()
	return scanRowsToMaps(rows)
}

// GetAddressEvents returns events related to an address (as contract invoker).
func (r *DuckLakeReader) GetAddressEvents(ctx context.Context, address string, limit int) ([]map[string]interface{}, error) {
	if limit <= 0 || limit > 200 {
		limit = 50
	}

	// Find events from transactions where this address was the source
	query := fmt.Sprintf(`
		SELECT
			e.event_id, e.contract_id, e.ledger_sequence, e.transaction_hash,
			e.closed_at, e.event_type, e.topics_decoded, e.data_decoded,
			e.topic_count, e.operation_index, e.event_index
		FROM %s.contract_events_stream_v1 e
		JOIN %s.transactions_row_v2 t
			ON e.transaction_hash = t.transaction_hash
			AND e.ledger_sequence = t.ledger_sequence
		WHERE t.source_account = ?
		ORDER BY e.ledger_sequence DESC
		LIMIT ?
	`, r.bronze, r.bronze)

	rows, err := r.db.QueryContext(ctx, query, address, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to query address events: %w", err)
	}
	defer rows.Close()
	return scanRowsToMaps(rows)
}

// GetTransactionEvents returns events for a specific transaction.
func (r *DuckLakeReader) GetTransactionEvents(ctx context.Context, txHash string) ([]map[string]interface{}, error) {
	query := fmt.Sprintf(`
		SELECT
			event_id, contract_id, ledger_sequence, transaction_hash,
			closed_at, event_type, in_successful_contract_call,
			topics_decoded, data_decoded, topic_count,
			operation_index, event_index
		FROM %s.contract_events_stream_v1
		WHERE transaction_hash = ?
		ORDER BY operation_index ASC, event_index ASC
	`, r.bronze)

	rows, err := r.db.QueryContext(ctx, query, txHash)
	if err != nil {
		return nil, fmt.Errorf("failed to query transaction events: %w", err)
	}
	defer rows.Close()
	return scanRowsToMaps(rows)
}

// ── Semantic Layer Readers ──────────────────────────────────

func (r *DuckLakeReader) GetSemanticActivities(ctx context.Context, limit int, startLedger, endLedger int64, activityType, account string) ([]map[string]interface{}, error) {
	if limit <= 0 || limit > 500 {
		limit = 50
	}
	var conditions []string
	var args []interface{}
	if startLedger > 0 {
		conditions = append(conditions, "ledger_sequence >= ?")
		args = append(args, startLedger)
	}
	if endLedger > 0 {
		conditions = append(conditions, "ledger_sequence <= ?")
		args = append(args, endLedger)
	}
	if activityType != "" {
		conditions = append(conditions, "activity_type = ?")
		args = append(args, activityType)
	}
	if account != "" {
		conditions = append(conditions, "(source_account = ? OR destination_account = ?)")
		args = append(args, account, account)
	}
	whereClause := ""
	if len(conditions) > 0 {
		whereClause = "WHERE " + strings.Join(conditions, " AND ")
	}
	args = append(args, limit)
	query := fmt.Sprintf(`
		SELECT id, ledger_sequence, timestamp, activity_type, description,
			source_account, destination_account, contract_id,
			asset_code, asset_issuer, amount, is_soroban,
			soroban_function_name, transaction_hash, operation_index,
			successful, fee_charged
		FROM %s.semantic_activities %s
		ORDER BY ledger_sequence DESC LIMIT ?
	`, r.silver, whereClause)
	qRows, err := r.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to query semantic activities: %w", err)
	}
	defer qRows.Close()
	return scanRowsToMaps(qRows)
}

func (r *DuckLakeReader) GetSemanticContracts(ctx context.Context, limit int, contractType string) ([]map[string]interface{}, error) {
	if limit <= 0 || limit > 200 {
		limit = 50
	}
	whereClause := ""
	var args []interface{}
	if contractType != "" {
		whereClause = "WHERE contract_type = ?"
		args = append(args, contractType)
	}
	args = append(args, limit)
	query := fmt.Sprintf(`
		SELECT contract_id, contract_type, token_name, token_symbol, token_decimals,
			deployer_account, deployed_at, deployed_ledger,
			total_invocations, last_activity, unique_callers, observed_functions
		FROM %s.semantic_entities_contracts %s
		ORDER BY total_invocations DESC LIMIT ?
	`, r.silver, whereClause)
	qRows, err := r.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to query semantic contracts: %w", err)
	}
	defer qRows.Close()
	return scanRowsToMaps(qRows)
}

func (r *DuckLakeReader) GetSemanticFlows(ctx context.Context, limit int, startLedger, endLedger int64, flowType, account string) ([]map[string]interface{}, error) {
	if limit <= 0 || limit > 500 {
		limit = 50
	}
	var conditions []string
	var args []interface{}
	if startLedger > 0 {
		conditions = append(conditions, "ledger_sequence >= ?")
		args = append(args, startLedger)
	}
	if endLedger > 0 {
		conditions = append(conditions, "ledger_sequence <= ?")
		args = append(args, endLedger)
	}
	if flowType != "" {
		conditions = append(conditions, "flow_type = ?")
		args = append(args, flowType)
	}
	if account != "" {
		conditions = append(conditions, "(from_account = ? OR to_account = ?)")
		args = append(args, account, account)
	}
	whereClause := ""
	if len(conditions) > 0 {
		whereClause = "WHERE " + strings.Join(conditions, " AND ")
	}
	args = append(args, limit)
	query := fmt.Sprintf(`
		SELECT id, ledger_sequence, timestamp, flow_type, from_account, to_account,
			contract_id, asset_code, asset_issuer, asset_type, amount,
			transaction_hash, operation_type, successful
		FROM %s.semantic_flows_value %s
		ORDER BY ledger_sequence DESC LIMIT ?
	`, r.silver, whereClause)
	qRows, err := r.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to query semantic flows: %w", err)
	}
	defer qRows.Close()
	return scanRowsToMaps(qRows)
}

func (r *DuckLakeReader) GetSemanticContractFunctions(ctx context.Context, contractID string, limit int) ([]map[string]interface{}, error) {
	if limit <= 0 || limit > 200 {
		limit = 50
	}
	whereClause := ""
	var args []interface{}
	if contractID != "" {
		whereClause = "WHERE contract_id = ?"
		args = append(args, contractID)
	}
	args = append(args, limit)
	query := fmt.Sprintf(`
		SELECT contract_id, function_name, total_calls, successful_calls, failed_calls,
			unique_callers, first_called, last_called
		FROM %s.semantic_contract_functions %s
		ORDER BY total_calls DESC LIMIT ?
	`, r.silver, whereClause)
	qRows, err := r.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to query semantic contract functions: %w", err)
	}
	defer qRows.Close()
	return scanRowsToMaps(qRows)
}

func (r *DuckLakeReader) GetSemanticAssets(ctx context.Context, limit int, assetCode string) ([]map[string]interface{}, error) {
	if limit <= 0 || limit > 200 {
		limit = 50
	}
	whereClause := ""
	var args []interface{}
	if assetCode != "" {
		whereClause = "WHERE asset_code = ?"
		args = append(args, assetCode)
	}
	args = append(args, limit)
	query := fmt.Sprintf(`
		SELECT asset_key, asset_code, asset_issuer, asset_type,
			token_name, token_symbol, token_decimals, contract_id,
			transfer_count, transfer_volume, first_seen, last_transfer
		FROM %s.semantic_asset_stats %s
		ORDER BY transfer_count DESC LIMIT ?
	`, r.silver, whereClause)
	qRows, err := r.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to query semantic assets: %w", err)
	}
	defer qRows.Close()
	return scanRowsToMaps(qRows)
}

func (r *DuckLakeReader) GetSemanticDexPairs(ctx context.Context, limit int) ([]map[string]interface{}, error) {
	if limit <= 0 || limit > 200 {
		limit = 50
	}
	query := fmt.Sprintf(`
		SELECT pair_key, selling_asset_code, selling_asset_issuer,
			buying_asset_code, buying_asset_issuer,
			trade_count, selling_volume, buying_volume, last_price,
			unique_sellers, unique_buyers, first_trade, last_trade
		FROM %s.semantic_dex_pairs ORDER BY trade_count DESC LIMIT ?
	`, r.silver)
	qRows, err := r.db.QueryContext(ctx, query, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to query semantic dex pairs: %w", err)
	}
	defer qRows.Close()
	return scanRowsToMaps(qRows)
}

func (r *DuckLakeReader) GetSemanticAccountSummary(ctx context.Context, accountID string, limit int) ([]map[string]interface{}, error) {
	if limit <= 0 || limit > 200 {
		limit = 50
	}
	whereClause := ""
	var args []interface{}
	if accountID != "" {
		whereClause = "WHERE account_id = ?"
		args = append(args, accountID)
	}
	args = append(args, limit)
	query := fmt.Sprintf(`
		SELECT account_id, total_operations, total_payments_sent,
			total_payments_received, total_contract_calls,
			unique_contracts_called, top_contract_id, top_contract_function,
			is_contract_deployer, contracts_deployed, first_activity, last_activity
		FROM %s.semantic_account_summary %s
		ORDER BY total_operations DESC LIMIT ?
	`, r.silver, whereClause)
	qRows, err := r.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to query semantic account summary: %w", err)
	}
	defer qRows.Close()
	return scanRowsToMaps(qRows)
}

func (r *DuckLakeReader) GetSemanticTokenSummary(ctx context.Context, contractID string) (map[string]interface{}, error) {
	query := fmt.Sprintf(`
		SELECT contract_id, contract_type, token_name, token_symbol, token_decimals,
			deployer_account, deployed_at, deployed_ledger,
			total_invocations, last_activity, unique_callers, observed_functions
		FROM %s.semantic_entities_contracts WHERE contract_id = ? LIMIT 1
	`, r.silver)
	qRows, err := r.db.QueryContext(ctx, query, contractID)
	if err != nil {
		return nil, fmt.Errorf("failed to query semantic token summary: %w", err)
	}
	defer qRows.Close()
	results, err := scanRowsToMaps(qRows)
	if err != nil {
		return nil, err
	}
	if len(results) == 0 {
		return nil, nil
	}
	return results[0], nil
}

// ── SEP-41 Token Readers ──────────────────────────────────

func (r *DuckLakeReader) GetSEP41TokenMetadata(ctx context.Context, contractID string) (map[string]interface{}, error) {
	query := fmt.Sprintf(`
		SELECT contract_id, token_name, token_symbol, token_decimals,
			asset_code, asset_issuer, token_type,
			first_seen_ledger, last_updated_ledger, created_at, updated_at
		FROM %s.token_registry WHERE contract_id = ? LIMIT 1
	`, r.silver)
	qRows, err := r.db.QueryContext(ctx, query, contractID)
	if err != nil {
		return nil, fmt.Errorf("failed to query SEP-41 token metadata: %w", err)
	}
	defer qRows.Close()
	results, err := scanRowsToMaps(qRows)
	if err != nil {
		return nil, err
	}
	if len(results) == 0 {
		return nil, nil
	}
	return results[0], nil
}

func (r *DuckLakeReader) GetSEP41TokenBalances(ctx context.Context, contractID string, limit int) ([]map[string]interface{}, error) {
	if limit <= 0 || limit > 200 {
		limit = 50
	}
	query := fmt.Sprintf(`
		SELECT address, SUM(net) AS balance FROM (
			SELECT to_account AS address, amount AS net
			FROM %[1]s.token_transfers_raw
			WHERE token_contract_id = ? AND to_account IS NOT NULL AND to_account != ''
			UNION ALL
			SELECT from_account AS address, -amount AS net
			FROM %[1]s.token_transfers_raw
			WHERE token_contract_id = ? AND from_account IS NOT NULL AND from_account != ''
		) sub GROUP BY address HAVING SUM(net) > 0
		ORDER BY balance DESC LIMIT ?
	`, r.silver)
	qRows, err := r.db.QueryContext(ctx, query, contractID, contractID, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to query SEP-41 token balances: %w", err)
	}
	defer qRows.Close()
	return scanRowsToMaps(qRows)
}

func (r *DuckLakeReader) GetSEP41SingleBalance(ctx context.Context, contractID, address string) (map[string]interface{}, error) {
	query := fmt.Sprintf(`
		SELECT address, SUM(net) AS balance FROM (
			SELECT to_account AS address, amount AS net
			FROM %[1]s.token_transfers_raw WHERE token_contract_id = ? AND to_account = ?
			UNION ALL
			SELECT from_account AS address, -amount AS net
			FROM %[1]s.token_transfers_raw WHERE token_contract_id = ? AND from_account = ?
		) sub GROUP BY address
	`, r.silver)
	qRows, err := r.db.QueryContext(ctx, query, contractID, address, contractID, address)
	if err != nil {
		return nil, fmt.Errorf("failed to query SEP-41 single balance: %w", err)
	}
	defer qRows.Close()
	results, err := scanRowsToMaps(qRows)
	if err != nil {
		return nil, err
	}
	if len(results) == 0 {
		return map[string]interface{}{"address": address, "balance": 0}, nil
	}
	return results[0], nil
}

func (r *DuckLakeReader) GetSEP41TokenTransfers(ctx context.Context, contractID string, limit int) ([]map[string]interface{}, error) {
	if limit <= 0 || limit > 200 {
		limit = 50
	}
	query := fmt.Sprintf(`
		SELECT timestamp, transaction_hash, ledger_sequence,
			from_account, to_account, amount, transaction_successful
		FROM %s.token_transfers_raw WHERE token_contract_id = ?
		ORDER BY ledger_sequence DESC LIMIT ?
	`, r.silver)
	qRows, err := r.db.QueryContext(ctx, query, contractID, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to query SEP-41 token transfers: %w", err)
	}
	defer qRows.Close()
	return scanRowsToMaps(qRows)
}

func (r *DuckLakeReader) GetSEP41TokenStats(ctx context.Context, contractID string) (map[string]interface{}, error) {
	query := fmt.Sprintf(`
		SELECT COUNT(*) AS total_transfers,
			COUNT(DISTINCT from_account) AS unique_senders,
			COUNT(DISTINCT to_account) AS unique_receivers,
			SUM(amount) AS total_volume,
			MIN(timestamp) AS first_transfer, MAX(timestamp) AS last_transfer
		FROM %s.token_transfers_raw WHERE token_contract_id = ?
	`, r.silver)
	qRows, err := r.db.QueryContext(ctx, query, contractID)
	if err != nil {
		return nil, fmt.Errorf("failed to query SEP-41 token stats: %w", err)
	}
	defer qRows.Close()
	results, err := scanRowsToMaps(qRows)
	if err != nil {
		return nil, err
	}
	if len(results) == 0 {
		return map[string]interface{}{}, nil
	}
	return results[0], nil
}

func (r *DuckLakeReader) GetAddressTokenPortfolio(ctx context.Context, address string, limit int) ([]map[string]interface{}, error) {
	if limit <= 0 || limit > 200 {
		limit = 50
	}
	query := fmt.Sprintf(`
		SELECT sub.token_contract_id, sub.balance,
			tr.token_name, tr.token_symbol, tr.token_decimals
		FROM (
			SELECT token_contract_id, SUM(net) AS balance FROM (
				SELECT token_contract_id, amount AS net
				FROM %[1]s.token_transfers_raw WHERE to_account = ? AND token_contract_id IS NOT NULL
				UNION ALL
				SELECT token_contract_id, -amount AS net
				FROM %[1]s.token_transfers_raw WHERE from_account = ? AND token_contract_id IS NOT NULL
			) inner_sub GROUP BY token_contract_id HAVING SUM(net) > 0
		) sub
		LEFT JOIN %[1]s.token_registry tr ON tr.contract_id = sub.token_contract_id
		ORDER BY sub.balance DESC LIMIT ?
	`, r.silver)
	qRows, err := r.db.QueryContext(ctx, query, address, address, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to query address token portfolio: %w", err)
	}
	defer qRows.Close()
	return scanRowsToMaps(qRows)
}
