package main

import (
	"context"
	"fmt"
)

// GetOperations returns operation rows from bronze.operations_row_v2.
func (r *DuckLakeReader) GetOperations(ctx context.Context, start, end int64, limit int) ([]map[string]interface{}, error) {
	if limit <= 0 || limit > 1000 {
		limit = 100
	}

	query := fmt.Sprintf(`
		SELECT
			ledger_sequence, transaction_hash, operation_index,
			op_type, type_string, source_account, created_at,
			amount, asset_code, asset_issuer, destination,
			soroban_contract_id, soroban_function
		FROM %s.operations_row_v2
		WHERE ledger_sequence >= ? AND ledger_sequence <= ?
		ORDER BY ledger_sequence ASC, transaction_hash ASC, operation_index ASC
		LIMIT ?
	`, r.bronze)

	rows, err := r.db.QueryContext(ctx, query, start, end, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to query operations: %w", err)
	}
	defer rows.Close()
	return scanRowsToMaps(rows)
}

// GetEffects returns effect rows from bronze.effects_row_v1.
func (r *DuckLakeReader) GetEffects(ctx context.Context, start, end int64, limit int) ([]map[string]interface{}, error) {
	if limit <= 0 || limit > 1000 {
		limit = 100
	}

	query := fmt.Sprintf(`
		SELECT
			ledger_sequence, transaction_hash, operation_index,
			effect_index, effect_type, effect_type_string,
			account_id, amount, asset_code, asset_issuer, asset_type,
			trustline_limit, authorize_flag, clawback_flag,
			signer_account, signer_weight, offer_id, seller_account,
			created_at
		FROM %s.effects_row_v1
		WHERE ledger_sequence >= ? AND ledger_sequence <= ?
		ORDER BY ledger_sequence ASC, transaction_hash ASC, operation_index ASC, effect_index ASC
		LIMIT ?
	`, r.bronze)

	rows, err := r.db.QueryContext(ctx, query, start, end, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to query effects: %w", err)
	}
	defer rows.Close()
	return scanRowsToMaps(rows)
}

// GetTrades returns trade rows from bronze.trades_row_v1.
func (r *DuckLakeReader) GetTrades(ctx context.Context, start, end int64, limit int) ([]map[string]interface{}, error) {
	if limit <= 0 || limit > 1000 {
		limit = 100
	}

	query := fmt.Sprintf(`
		SELECT
			ledger_sequence, transaction_hash, operation_index,
			trade_index, trade_type, trade_timestamp,
			seller_account, selling_asset_code, selling_asset_issuer, selling_amount,
			buyer_account, buying_asset_code, buying_asset_issuer, buying_amount,
			price
		FROM %s.trades_row_v1
		WHERE ledger_sequence >= ? AND ledger_sequence <= ?
		ORDER BY ledger_sequence ASC, transaction_hash ASC, operation_index ASC, trade_index ASC
		LIMIT ?
	`, r.bronze)

	rows, err := r.db.QueryContext(ctx, query, start, end, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to query trades: %w", err)
	}
	defer rows.Close()
	return scanRowsToMaps(rows)
}

// GetBronzeAccounts returns account snapshots from bronze.accounts_snapshot_v1.
func (r *DuckLakeReader) GetBronzeAccounts(ctx context.Context, accountID string, limit int) ([]map[string]interface{}, error) {
	if limit <= 0 || limit > 1000 {
		limit = 100
	}

	query := fmt.Sprintf(`
		SELECT
			account_id, balance, sequence_number, num_subentries,
			num_sponsoring, num_sponsored, home_domain,
			master_weight, low_threshold, med_threshold, high_threshold,
			flags, auth_required, auth_revocable, auth_immutable, auth_clawback_enabled,
			signers, sponsor_account, ledger_sequence, closed_at
		FROM %s.accounts_snapshot_v1
		WHERE account_id = ?
		ORDER BY ledger_sequence DESC
		LIMIT ?
	`, r.bronze)

	rows, err := r.db.QueryContext(ctx, query, accountID, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to query bronze accounts: %w", err)
	}
	defer rows.Close()
	return scanRowsToMaps(rows)
}

// GetBronzeTrustlines returns trustline snapshots from bronze.trustlines_snapshot_v1.
func (r *DuckLakeReader) GetBronzeTrustlines(ctx context.Context, accountID string, limit int) ([]map[string]interface{}, error) {
	if limit <= 0 || limit > 1000 {
		limit = 100
	}

	query := fmt.Sprintf(`
		SELECT
			account_id, asset_code, asset_issuer, asset_type,
			balance, trust_limit, buying_liabilities, selling_liabilities,
			authorized, authorized_to_maintain_liabilities, clawback_enabled,
			ledger_sequence
		FROM %s.trustlines_snapshot_v1
		WHERE account_id = ?
		ORDER BY ledger_sequence DESC
		LIMIT ?
	`, r.bronze)

	rows, err := r.db.QueryContext(ctx, query, accountID, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to query bronze trustlines: %w", err)
	}
	defer rows.Close()
	return scanRowsToMaps(rows)
}

// GetBronzeOffers returns offer snapshots from bronze.offers_snapshot_v1.
func (r *DuckLakeReader) GetBronzeOffers(ctx context.Context, sellerID string, limit int) ([]map[string]interface{}, error) {
	if limit <= 0 || limit > 1000 {
		limit = 100
	}

	query := fmt.Sprintf(`
		SELECT
			offer_id, seller_account, ledger_sequence, closed_at,
			selling_asset_type, selling_asset_code, selling_asset_issuer,
			buying_asset_type, buying_asset_code, buying_asset_issuer,
			amount, price, flags
		FROM %s.offers_snapshot_v1
		WHERE seller_account = ?
		ORDER BY ledger_sequence DESC
		LIMIT ?
	`, r.bronze)

	rows, err := r.db.QueryContext(ctx, query, sellerID, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to query bronze offers: %w", err)
	}
	defer rows.Close()
	return scanRowsToMaps(rows)
}

// GetContractEvents returns contract events from bronze.contract_events_stream_v1.
func (r *DuckLakeReader) GetContractEvents(ctx context.Context, start, end int64, limit int) ([]map[string]interface{}, error) {
	if limit <= 0 || limit > 1000 {
		limit = 100
	}

	query := fmt.Sprintf(`
		SELECT
			event_id, contract_id, ledger_sequence, transaction_hash,
			closed_at, event_type, in_successful_contract_call,
			topics_json, topics_decoded, data_decoded,
			topic_count, operation_index, event_index
		FROM %s.contract_events_stream_v1
		WHERE ledger_sequence >= ? AND ledger_sequence <= ?
		ORDER BY ledger_sequence ASC, transaction_hash ASC, operation_index ASC, event_index ASC
		LIMIT ?
	`, r.bronze)

	rows, err := r.db.QueryContext(ctx, query, start, end, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to query contract events: %w", err)
	}
	defer rows.Close()
	return scanRowsToMaps(rows)
}

// GetBronzeNetworkStats returns network stats from bronze tables.
func (r *DuckLakeReader) GetBronzeNetworkStats(ctx context.Context) (map[string]interface{}, error) {
	return r.GetNetworkStats(ctx)
}

// GetLiquidityPools returns liquidity pool snapshots from bronze.liquidity_pools_snapshot_v1.
func (r *DuckLakeReader) GetLiquidityPools(ctx context.Context, limit int) ([]map[string]interface{}, error) {
	if limit <= 0 || limit > 200 {
		limit = 50
	}

	query := fmt.Sprintf(`
		SELECT
			liquidity_pool_id, ledger_sequence, closed_at,
			pool_type, fee, trustline_count, total_pool_shares,
			asset_a_type, asset_a_code, asset_a_issuer, asset_a_amount,
			asset_b_type, asset_b_code, asset_b_issuer, asset_b_amount
		FROM %s.liquidity_pools_snapshot_v1
		ORDER BY ledger_sequence DESC
		LIMIT ?
	`, r.bronze)

	rows, err := r.db.QueryContext(ctx, query, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to query liquidity pools: %w", err)
	}
	defer rows.Close()
	return scanRowsToMaps(rows)
}

// GetLiquidityPoolsByAsset returns liquidity pools containing a specific asset.
func (r *DuckLakeReader) GetLiquidityPoolsByAsset(ctx context.Context, assetCode, assetIssuer string, limit int) ([]map[string]interface{}, error) {
	if limit <= 0 || limit > 200 {
		limit = 50
	}

	query := fmt.Sprintf(`
		SELECT
			liquidity_pool_id, ledger_sequence, closed_at,
			pool_type, fee, trustline_count, total_pool_shares,
			asset_a_type, asset_a_code, asset_a_issuer, asset_a_amount,
			asset_b_type, asset_b_code, asset_b_issuer, asset_b_amount
		FROM %s.liquidity_pools_snapshot_v1
		WHERE (asset_a_code = ? AND asset_a_issuer = ?)
		   OR (asset_b_code = ? AND asset_b_issuer = ?)
		ORDER BY ledger_sequence DESC
		LIMIT ?
	`, r.bronze)

	rows, err := r.db.QueryContext(ctx, query, assetCode, assetIssuer, assetCode, assetIssuer, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to query liquidity pools by asset: %w", err)
	}
	defer rows.Close()
	return scanRowsToMaps(rows)
}

// GetLiquidityPoolByID returns a single liquidity pool by ID.
func (r *DuckLakeReader) GetLiquidityPoolByID(ctx context.Context, poolID string) (map[string]interface{}, error) {
	query := fmt.Sprintf(`
		SELECT
			liquidity_pool_id, ledger_sequence, closed_at,
			pool_type, fee, trustline_count, total_pool_shares,
			asset_a_type, asset_a_code, asset_a_issuer, asset_a_amount,
			asset_b_type, asset_b_code, asset_b_issuer, asset_b_amount
		FROM %s.liquidity_pools_snapshot_v1
		WHERE liquidity_pool_id = ?
		ORDER BY ledger_sequence DESC
		LIMIT 1
	`, r.bronze)

	rows, err := r.db.QueryContext(ctx, query, poolID)
	if err != nil {
		return nil, fmt.Errorf("failed to query liquidity pool: %w", err)
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

// GetTTLEntries returns TTL entries from bronze.ttl_snapshot_v1.
func (r *DuckLakeReader) GetTTLEntries(ctx context.Context, keyHash string, limit int) ([]map[string]interface{}, error) {
	if limit <= 0 || limit > 200 {
		limit = 50
	}

	whereClause := ""
	var args []interface{}
	if keyHash != "" {
		whereClause = "WHERE key_hash = ?"
		args = append(args, keyHash)
	}
	args = append(args, limit)

	query := fmt.Sprintf(`
		SELECT
			key_hash, ledger_sequence, live_until_ledger_seq,
			ttl_remaining, expired, last_modified_ledger, deleted, closed_at
		FROM %s.ttl_snapshot_v1
		%s
		ORDER BY ledger_sequence DESC
		LIMIT ?
	`, r.bronze, whereClause)

	rows, err := r.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to query TTL entries: %w", err)
	}
	defer rows.Close()
	return scanRowsToMaps(rows)
}

// GetTTLExpiring returns entries expiring within N ledgers.
func (r *DuckLakeReader) GetTTLExpiring(ctx context.Context, withinLedgers int64, limit int) ([]map[string]interface{}, error) {
	if limit <= 0 || limit > 200 {
		limit = 50
	}

	query := fmt.Sprintf(`
		SELECT
			t.key_hash, t.ledger_sequence, t.live_until_ledger_seq,
			t.ttl_remaining, t.expired, t.closed_at
		FROM %s.ttl_snapshot_v1 t
		INNER JOIN (
			SELECT key_hash, MAX(ledger_sequence) AS max_seq
			FROM %s.ttl_snapshot_v1
			GROUP BY key_hash
		) latest ON t.key_hash = latest.key_hash AND t.ledger_sequence = latest.max_seq
		WHERE t.expired = false AND t.ttl_remaining <= ? AND t.deleted = false
		ORDER BY t.ttl_remaining ASC
		LIMIT ?
	`, r.bronze, r.bronze)

	rows, err := r.db.QueryContext(ctx, query, withinLedgers, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to query expiring TTL entries: %w", err)
	}
	defer rows.Close()
	return scanRowsToMaps(rows)
}

// GetTTLExpired returns already-expired entries.
func (r *DuckLakeReader) GetTTLExpired(ctx context.Context, limit int) ([]map[string]interface{}, error) {
	if limit <= 0 || limit > 200 {
		limit = 50
	}

	query := fmt.Sprintf(`
		SELECT
			t.key_hash, t.ledger_sequence, t.live_until_ledger_seq,
			t.ttl_remaining, t.closed_at
		FROM %s.ttl_snapshot_v1 t
		INNER JOIN (
			SELECT key_hash, MAX(ledger_sequence) AS max_seq
			FROM %s.ttl_snapshot_v1
			GROUP BY key_hash
		) latest ON t.key_hash = latest.key_hash AND t.ledger_sequence = latest.max_seq
		WHERE t.expired = true AND t.deleted = false
		ORDER BY t.ledger_sequence DESC
		LIMIT ?
	`, r.bronze, r.bronze)

	rows, err := r.db.QueryContext(ctx, query, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to query expired TTL entries: %w", err)
	}
	defer rows.Close()
	return scanRowsToMaps(rows)
}

// GetEvictedKeys returns evicted contract keys from bronze.evicted_keys_state_v1.
func (r *DuckLakeReader) GetEvictedKeys(ctx context.Context, contractID string, limit int) ([]map[string]interface{}, error) {
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
		SELECT
			key_hash, ledger_sequence, contract_id,
			key_type, durability, closed_at
		FROM %s.evicted_keys_state_v1
		%s
		ORDER BY ledger_sequence DESC
		LIMIT ?
	`, r.bronze, whereClause)

	rows, err := r.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to query evicted keys: %w", err)
	}
	defer rows.Close()
	return scanRowsToMaps(rows)
}

// GetRestoredKeys returns restored contract keys from bronze.restored_keys_state_v1.
func (r *DuckLakeReader) GetRestoredKeys(ctx context.Context, contractID string, limit int) ([]map[string]interface{}, error) {
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
		SELECT
			key_hash, ledger_sequence, contract_id,
			key_type, durability, restored_from_ledger, closed_at
		FROM %s.restored_keys_state_v1
		%s
		ORDER BY ledger_sequence DESC
		LIMIT ?
	`, r.bronze, whereClause)

	rows, err := r.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to query restored keys: %w", err)
	}
	defer rows.Close()
	return scanRowsToMaps(rows)
}

// GetSorobanConfig returns Soroban config settings from bronze.config_settings_snapshot_v1.
func (r *DuckLakeReader) GetSorobanConfig(ctx context.Context) ([]map[string]interface{}, error) {
	query := fmt.Sprintf(`
		SELECT
			c.config_setting_id, c.ledger_sequence, c.last_modified_ledger,
			c.deleted, c.closed_at, c.config_setting_xdr
		FROM %s.config_settings_snapshot_v1 c
		INNER JOIN (
			SELECT config_setting_id, MAX(ledger_sequence) AS max_seq
			FROM %s.config_settings_snapshot_v1
			GROUP BY config_setting_id
		) latest ON c.config_setting_id = latest.config_setting_id AND c.ledger_sequence = latest.max_seq
		WHERE c.deleted = false
		ORDER BY c.config_setting_id ASC
	`, r.bronze, r.bronze)

	rows, err := r.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query soroban config: %w", err)
	}
	defer rows.Close()
	return scanRowsToMaps(rows)
}
