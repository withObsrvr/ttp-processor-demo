package main

import (
	"context"
	"fmt"
	"strings"
)

// ── Account Endpoints (from bronze snapshot tables) ──────────────────

// GetAccountsList returns a paginated list of accounts (latest snapshot per account).
func (r *DuckLakeReader) GetAccountsList(ctx context.Context, limit int, cursor string) ([]map[string]interface{}, error) {
	if limit <= 0 || limit > 500 {
		limit = 50
	}

	cursorClause := ""
	var args []interface{}
	if cursor != "" {
		cursorClause = "AND account_id > ?"
		args = append(args, cursor)
	}
	args = append(args, limit)

	query := fmt.Sprintf(`
		SELECT account_id, balance, sequence_number, num_subentries,
			num_sponsoring, num_sponsored, home_domain,
			master_weight, low_threshold, med_threshold, high_threshold,
			flags, ledger_sequence
		FROM (
			SELECT *, ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY ledger_sequence DESC) AS rn
			FROM %s.accounts_snapshot_v1
			WHERE 1=1 %s
		) sub
		WHERE rn = 1
		ORDER BY account_id ASC
		LIMIT ?
	`, r.bronze, cursorClause)

	rows, err := r.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to query accounts list: %w", err)
	}
	defer rows.Close()
	return scanRowsToMaps(rows)
}

// GetAccountCurrentSilver returns the latest snapshot for a single account.
func (r *DuckLakeReader) GetAccountCurrentSilver(ctx context.Context, accountID string) (map[string]interface{}, error) {
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
		LIMIT 1
	`, r.bronze)

	rows, err := r.db.QueryContext(ctx, query, accountID)
	if err != nil {
		return nil, fmt.Errorf("failed to query account current: %w", err)
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

// GetAccountHistory returns historical snapshots of an account.
func (r *DuckLakeReader) GetAccountHistory(ctx context.Context, accountID string, limit int, cursorLedger int64) ([]map[string]interface{}, error) {
	if limit <= 0 || limit > 500 {
		limit = 50
	}

	whereClause := "WHERE account_id = ?"
	args := []interface{}{accountID}
	if cursorLedger > 0 {
		whereClause += " AND ledger_sequence < ?"
		args = append(args, cursorLedger)
	}
	args = append(args, limit)

	query := fmt.Sprintf(`
		SELECT
			account_id, balance, sequence_number, num_subentries,
			num_sponsoring, num_sponsored, home_domain,
			master_weight, low_threshold, med_threshold, high_threshold,
			flags, ledger_sequence, closed_at
		FROM %s.accounts_snapshot_v1
		%s
		ORDER BY ledger_sequence DESC
		LIMIT ?
	`, r.bronze, whereClause)

	rows, err := r.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to query account history: %w", err)
	}
	defer rows.Close()
	return scanRowsToMaps(rows)
}

// GetAccountSigners returns signer data from bronze.account_signers_snapshot_v1.
func (r *DuckLakeReader) GetAccountSigners(ctx context.Context, accountID string) ([]map[string]interface{}, error) {
	query := fmt.Sprintf(`
		SELECT
			account_id, signer, weight, sponsor, deleted, ledger_sequence, closed_at
		FROM %s.account_signers_snapshot_v1
		WHERE account_id = ?
		ORDER BY ledger_sequence DESC
	`, r.bronze)

	rows, err := r.db.QueryContext(ctx, query, accountID)
	if err != nil {
		return nil, fmt.Errorf("failed to query account signers: %w", err)
	}
	defer rows.Close()
	return scanRowsToMaps(rows)
}

// GetAccountBalances returns trustline balances for an account.
func (r *DuckLakeReader) GetAccountBalances(ctx context.Context, accountID string) ([]map[string]interface{}, error) {
	// Get latest trustline snapshot per asset for this account
	query := fmt.Sprintf(`
		SELECT account_id, asset_type, asset_code, asset_issuer,
			balance, trust_limit, buying_liabilities, selling_liabilities,
			authorized, clawback_enabled, ledger_sequence
		FROM (
			SELECT *, ROW_NUMBER() OVER (
				PARTITION BY account_id, asset_code, asset_issuer
				ORDER BY ledger_sequence DESC
			) AS rn
			FROM %s.trustlines_snapshot_v1
			WHERE account_id = ?
		) sub
		WHERE rn = 1
		ORDER BY asset_code ASC
	`, r.bronze)

	rows, err := r.db.QueryContext(ctx, query, accountID)
	if err != nil {
		return nil, fmt.Errorf("failed to query account balances: %w", err)
	}
	defer rows.Close()
	return scanRowsToMaps(rows)
}

// GetAccountOffers returns active offers for an account from bronze.offers_snapshot_v1.
func (r *DuckLakeReader) GetAccountOffers(ctx context.Context, accountID string, limit int) ([]map[string]interface{}, error) {
	if limit <= 0 || limit > 200 {
		limit = 50
	}

	query := fmt.Sprintf(`
		SELECT
			offer_id, seller_account,
			selling_asset_type, selling_asset_code, selling_asset_issuer,
			buying_asset_type, buying_asset_code, buying_asset_issuer,
			amount, price, flags, ledger_sequence, closed_at
		FROM %s.offers_snapshot_v1
		WHERE seller_account = ?
		ORDER BY ledger_sequence DESC
		LIMIT ?
	`, r.bronze)

	rows, err := r.db.QueryContext(ctx, query, accountID, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to query account offers: %w", err)
	}
	defer rows.Close()
	return scanRowsToMaps(rows)
}

// GetAccountContracts returns contracts created by an account from bronze.contract_creations_v1.
func (r *DuckLakeReader) GetAccountContracts(ctx context.Context, accountID string, limit int) ([]map[string]interface{}, error) {
	if limit <= 0 || limit > 200 {
		limit = 50
	}

	query := fmt.Sprintf(`
		SELECT
			contract_id, creator_address, wasm_hash, created_ledger, created_at
		FROM %s.contract_creations_v1
		WHERE creator_address = ?
		ORDER BY created_ledger DESC
		LIMIT ?
	`, r.bronze)

	rows, err := r.db.QueryContext(ctx, query, accountID, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to query account contracts: %w", err)
	}
	defer rows.Close()
	return scanRowsToMaps(rows)
}

// GetAccountActivity returns a unified activity timeline from silver.enriched_history_operations.
func (r *DuckLakeReader) GetAccountActivity(ctx context.Context, accountID string, limit int, startLedger, endLedger int64) ([]map[string]interface{}, error) {
	if limit <= 0 || limit > 500 {
		limit = 50
	}

	var conditions []string
	var args []interface{}

	conditions = append(conditions, "(source_account = ? OR destination = ?)")
	args = append(args, accountID, accountID)

	if startLedger > 0 {
		conditions = append(conditions, "ledger_sequence >= ?")
		args = append(args, startLedger)
	}
	if endLedger > 0 {
		conditions = append(conditions, "ledger_sequence <= ?")
		args = append(args, endLedger)
	}
	args = append(args, limit)

	query := fmt.Sprintf(`
		SELECT
			id, ledger_sequence, closed_at, type_string,
			source_account, destination, soroban_contract_id,
			soroban_function, asset_code, asset_issuer, amount,
			transaction_hash, operation_index, transaction_successful, fee_charged
		FROM %s.enriched_history_operations
		WHERE %s
		ORDER BY ledger_sequence DESC, operation_index DESC
		LIMIT ?
	`, r.silver, strings.Join(conditions, " AND "))

	rows, err := r.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to query account activity: %w", err)
	}
	defer rows.Close()
	return scanRowsToMaps(rows)
}

// ── Asset Endpoints ──────────────────────────────────────────────

// GetAssetList returns a list of assets enriched with transfer stats and supply.
func (r *DuckLakeReader) GetAssetList(ctx context.Context, limit int, cursor string) ([]map[string]interface{}, error) {
	if limit <= 0 || limit > 200 {
		limit = 50
	}

	// Join trustlines with token_transfers_raw for volume/transfer stats
	query := fmt.Sprintf(`
		SELECT
			t.asset_code, t.asset_issuer, t.asset_type,
			COUNT(DISTINCT t.account_id) AS holder_count,
			CAST(COALESCE(SUM(CAST(t.balance AS BIGINT)), 0) AS TEXT) AS circulating_supply,
			COALESCE(tx.transfer_count, 0) AS transfers_24h,
			CAST(COALESCE(tx.transfer_volume, 0) AS TEXT) AS volume_24h
		FROM (
			SELECT *, ROW_NUMBER() OVER (
				PARTITION BY account_id, asset_code, asset_issuer
				ORDER BY ledger_sequence DESC
			) AS rn
			FROM %s.trustlines_snapshot_v1
			WHERE asset_type != 'native'
		) t
		LEFT JOIN (
			SELECT asset_code, asset_issuer,
				COUNT(*) AS transfer_count,
				COALESCE(SUM(amount), 0) AS transfer_volume
			FROM %s.token_transfers_raw
			GROUP BY asset_code, asset_issuer
		) tx ON t.asset_code = tx.asset_code AND t.asset_issuer = tx.asset_issuer
		WHERE t.rn = 1
		GROUP BY t.asset_code, t.asset_issuer, t.asset_type, tx.transfer_count, tx.transfer_volume
		ORDER BY holder_count DESC
		LIMIT ?
	`, r.bronze, r.silver)

	rows, err := r.db.QueryContext(ctx, query, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to query asset list: %w", err)
	}
	defer rows.Close()
	return scanRowsToMaps(rows)
}

// GetTokenHolders returns holders of a specific asset.
func (r *DuckLakeReader) GetTokenHolders(ctx context.Context, assetCode, assetIssuer string, limit int) ([]map[string]interface{}, error) {
	if limit <= 0 || limit > 200 {
		limit = 50
	}

	// Get latest trustline snapshot per account for this asset
	query := fmt.Sprintf(`
		SELECT account_id, balance, trust_limit, authorized, ledger_sequence
		FROM (
			SELECT *, ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY ledger_sequence DESC) AS rn
			FROM %s.trustlines_snapshot_v1
			WHERE asset_code = ? AND asset_issuer = ?
		) sub
		WHERE rn = 1
		ORDER BY CAST(balance AS BIGINT) DESC
		LIMIT ?
	`, r.bronze)

	rows, err := r.db.QueryContext(ctx, query, assetCode, assetIssuer, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to query token holders: %w", err)
	}
	defer rows.Close()
	return scanRowsToMaps(rows)
}

// GetTokenStats returns aggregate statistics for a specific asset including supply and volume.
func (r *DuckLakeReader) GetTokenStats(ctx context.Context, assetCode, assetIssuer string) (map[string]interface{}, error) {
	query := fmt.Sprintf(`
		SELECT
			COUNT(DISTINCT account_id) AS holder_count,
			COUNT(CASE WHEN CAST(balance AS BIGINT) > 0 THEN 1 END) AS active_holders,
			CAST(COALESCE(SUM(CASE WHEN CAST(balance AS BIGINT) > 0 THEN CAST(balance AS BIGINT) END), 0) AS TEXT) AS circulating_supply
		FROM (
			SELECT *, ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY ledger_sequence DESC) AS rn
			FROM %s.trustlines_snapshot_v1
			WHERE asset_code = ? AND asset_issuer = ?
		) sub
		WHERE rn = 1
	`, r.bronze)

	rows, err := r.db.QueryContext(ctx, query, assetCode, assetIssuer)
	if err != nil {
		return nil, fmt.Errorf("failed to query token stats: %w", err)
	}
	defer rows.Close()

	results, err := scanRowsToMaps(rows)
	if err != nil {
		return nil, err
	}
	if len(results) == 0 {
		return map[string]interface{}{}, nil
	}
	result := results[0]
	result["asset_code"] = assetCode
	result["asset_issuer"] = assetIssuer

	// Enrich with transfer volume from silver
	volQuery := fmt.Sprintf(`
		SELECT COUNT(*) AS transfers_24h,
			CAST(COALESCE(SUM(amount), 0) AS TEXT) AS volume_24h
		FROM %s.token_transfers_raw
		WHERE asset_code = ? AND asset_issuer = ?
	`, r.silver)
	volRows, err := r.db.QueryContext(ctx, volQuery, assetCode, assetIssuer)
	if err == nil {
		defer volRows.Close()
		volResults, _ := scanRowsToMaps(volRows)
		if len(volResults) > 0 {
			result["transfers_24h"] = volResults[0]["transfers_24h"]
			result["volume_24h"] = volResults[0]["volume_24h"]
		}
	}

	return result, nil
}

// ── Operations Endpoints ──────────────────────────────────────────

// GetSorobanOperations returns Soroban-specific operations from silver.enriched_history_operations.
func (r *DuckLakeReader) GetSorobanOperations(ctx context.Context, limit int, startLedger, endLedger int64, contractID, functionName string) ([]map[string]interface{}, error) {
	if limit <= 0 || limit > 500 {
		limit = 50
	}

	var conditions []string
	var args []interface{}

	conditions = append(conditions, "soroban_contract_id IS NOT NULL")

	if startLedger > 0 {
		conditions = append(conditions, "ledger_sequence >= ?")
		args = append(args, startLedger)
	}
	if endLedger > 0 {
		conditions = append(conditions, "ledger_sequence <= ?")
		args = append(args, endLedger)
	}
	if contractID != "" {
		conditions = append(conditions, "soroban_contract_id = ?")
		args = append(args, contractID)
	}
	if functionName != "" {
		conditions = append(conditions, "soroban_function = ?")
		args = append(args, functionName)
	}
	args = append(args, limit)

	query := fmt.Sprintf(`
		SELECT
			transaction_hash, operation_index, ledger_sequence,
			source_account, type_string, closed_at,
			soroban_contract_id, soroban_function, soroban_operation,
			transaction_successful
		FROM %s.enriched_history_operations
		WHERE %s
		ORDER BY ledger_sequence DESC, operation_index DESC
		LIMIT ?
	`, r.silver, strings.Join(conditions, " AND "))

	rows, err := r.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to query soroban operations: %w", err)
	}
	defer rows.Close()
	return scanRowsToMaps(rows)
}

// GetSorobanOpsByFunction returns Soroban operations filtered by contract/function.
func (r *DuckLakeReader) GetSorobanOpsByFunction(ctx context.Context, contractID, functionName string, limit int, startLedger, endLedger int64) ([]map[string]interface{}, error) {
	return r.GetSorobanOperations(ctx, limit, startLedger, endLedger, contractID, functionName)
}

// GetPayments returns payment-type operations from silver.enriched_history_operations.
func (r *DuckLakeReader) GetPayments(ctx context.Context, limit int, startLedger, endLedger int64, account string) ([]map[string]interface{}, error) {
	if limit <= 0 || limit > 500 {
		limit = 50
	}

	var conditions []string
	var args []interface{}

	// Payment types: payment (1), path_payment_strict_receive (2), path_payment_strict_send (13)
	conditions = append(conditions, "type IN (1, 2, 13)")

	if startLedger > 0 {
		conditions = append(conditions, "ledger_sequence >= ?")
		args = append(args, startLedger)
	}
	if endLedger > 0 {
		conditions = append(conditions, "ledger_sequence <= ?")
		args = append(args, endLedger)
	}
	if account != "" {
		conditions = append(conditions, "(source_account = ? OR destination = ?)")
		args = append(args, account, account)
	}
	args = append(args, limit)

	query := fmt.Sprintf(`
		SELECT
			transaction_hash, operation_index, ledger_sequence,
			source_account, type AS type, type_string AS type_name, closed_at,
			asset_code, asset_issuer, amount, destination,
			transaction_successful
		FROM %s.enriched_history_operations
		WHERE %s
		ORDER BY ledger_sequence DESC, operation_index DESC
		LIMIT ?
	`, r.silver, strings.Join(conditions, " AND "))

	rows, err := r.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to query payments: %w", err)
	}
	defer rows.Close()
	return scanRowsToMaps(rows)
}

// ── Transfer Endpoints ──────────────────────────────────────────
// Note: token_transfers_raw doesn't exist in the unified processor.
// We derive transfers from silver.enriched_history_operations payment types.

// GetTokenTransfers returns payment operations as token transfers.
func (r *DuckLakeReader) GetTokenTransfers(ctx context.Context, limit int, startLedger, endLedger int64, account, assetCode string) ([]map[string]interface{}, error) {
	if limit <= 0 || limit > 500 {
		limit = 50
	}

	var conditions []string
	var args []interface{}

	conditions = append(conditions, "type IN (1, 2, 13)")

	if startLedger > 0 {
		conditions = append(conditions, "ledger_sequence >= ?")
		args = append(args, startLedger)
	}
	if endLedger > 0 {
		conditions = append(conditions, "ledger_sequence <= ?")
		args = append(args, endLedger)
	}
	if account != "" {
		conditions = append(conditions, "(source_account = ? OR destination = ?)")
		args = append(args, account, account)
	}
	if assetCode != "" {
		conditions = append(conditions, "asset_code = ?")
		args = append(args, assetCode)
	}
	args = append(args, limit)

	query := fmt.Sprintf(`
		SELECT
			closed_at AS timestamp, transaction_hash, ledger_sequence,
			source_account AS from_account, destination AS to_account,
			asset_code, asset_issuer, amount,
			transaction_successful
		FROM %s.enriched_history_operations
		WHERE %s
		ORDER BY ledger_sequence DESC
		LIMIT ?
	`, r.silver, strings.Join(conditions, " AND "))

	rows, err := r.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to query token transfers: %w", err)
	}
	defer rows.Close()
	return scanRowsToMaps(rows)
}

// GetTokenTransferStats returns aggregate transfer statistics.
func (r *DuckLakeReader) GetTokenTransferStats(ctx context.Context, assetCode, assetIssuer string) (map[string]interface{}, error) {
	var conditions []string
	var args []interface{}

	conditions = append(conditions, "type IN (1, 2, 13)")

	if assetCode != "" {
		conditions = append(conditions, "asset_code = ?")
		args = append(args, assetCode)
	}
	if assetIssuer != "" {
		conditions = append(conditions, "asset_issuer = ?")
		args = append(args, assetIssuer)
	}

	query := fmt.Sprintf(`
		SELECT
			COUNT(*) AS total_transfers,
			COUNT(DISTINCT source_account) AS unique_senders,
			COUNT(DISTINCT destination) AS unique_receivers,
			MIN(closed_at) AS first_transfer,
			MAX(closed_at) AS last_transfer
		FROM %s.enriched_history_operations
		WHERE %s
	`, r.silver, strings.Join(conditions, " AND "))

	rows, err := r.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to query transfer stats: %w", err)
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

// ── Explorer Endpoints ──────────────────────────────────────────

// GetExplorerAccount returns an account overview combining account + balances.
func (r *DuckLakeReader) GetExplorerAccount(ctx context.Context, accountID string) (map[string]interface{}, error) {
	account, err := r.GetAccountCurrentSilver(ctx, accountID)
	if err != nil {
		return nil, err
	}
	if account == nil {
		return nil, nil
	}

	balances, err := r.GetAccountBalances(ctx, accountID)
	if err != nil {
		return nil, err
	}

	return map[string]interface{}{
		"account":  account,
		"balances": balances,
	}, nil
}

// GetExplorerTransaction returns a transaction overview from silver.enriched_history_operations.
func (r *DuckLakeReader) GetExplorerTransaction(ctx context.Context, txHash string) (map[string]interface{}, error) {
	query := fmt.Sprintf(`
		SELECT
			transaction_hash, ledger_sequence, source_account,
			type_string, operation_index, closed_at,
			asset_code, asset_issuer, amount, destination,
			soroban_contract_id, soroban_function,
			transaction_successful, fee_charged, memo_type, memo
		FROM %s.enriched_history_operations
		WHERE transaction_hash = ?
		ORDER BY operation_index ASC
	`, r.silver)

	rows, err := r.db.QueryContext(ctx, query, txHash)
	if err != nil {
		return nil, fmt.Errorf("failed to query transaction: %w", err)
	}
	defer rows.Close()

	ops, err := scanRowsToMaps(rows)
	if err != nil {
		return nil, err
	}
	if len(ops) == 0 {
		return nil, nil
	}

	return map[string]interface{}{
		"transaction_hash": txHash,
		"operations":       ops,
		"operation_count":  len(ops),
	}, nil
}

// GetExplorerAsset returns an asset overview combining stats + recent transfers.
func (r *DuckLakeReader) GetExplorerAsset(ctx context.Context, assetCode, assetIssuer string) (map[string]interface{}, error) {
	stats, err := r.GetTokenStats(ctx, assetCode, assetIssuer)
	if err != nil {
		return nil, err
	}

	transfers, err := r.GetTokenTransfers(ctx, 10, 0, 0, "", assetCode)
	if err != nil {
		return nil, err
	}

	return map[string]interface{}{
		"stats":            stats,
		"recent_transfers": transfers,
	}, nil
}

// ── Offers Endpoints (from bronze snapshots) ──────────────────────

// GetSilverOffers returns latest offers from bronze.offers_snapshot_v1.
func (r *DuckLakeReader) GetSilverOffers(ctx context.Context, sellerID string, limit int) ([]map[string]interface{}, error) {
	if limit <= 0 || limit > 200 {
		limit = 50
	}

	whereClause := ""
	var args []interface{}
	if sellerID != "" {
		whereClause = "WHERE seller_account = ?"
		args = append(args, sellerID)
	}
	args = append(args, limit)

	query := fmt.Sprintf(`
		SELECT
			offer_id, seller_account,
			selling_asset_type, selling_asset_code, selling_asset_issuer,
			buying_asset_type, buying_asset_code, buying_asset_issuer,
			amount, price, flags, ledger_sequence, closed_at
		FROM %s.offers_snapshot_v1
		%s
		ORDER BY ledger_sequence DESC
		LIMIT ?
	`, r.bronze, whereClause)

	rows, err := r.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to query offers: %w", err)
	}
	defer rows.Close()
	return scanRowsToMaps(rows)
}

// GetSilverOffersByPair returns offers for a trading pair.
func (r *DuckLakeReader) GetSilverOffersByPair(ctx context.Context, sellCode, sellIssuer, buyCode, buyIssuer string, limit int) ([]map[string]interface{}, error) {
	if limit <= 0 || limit > 200 {
		limit = 50
	}

	query := fmt.Sprintf(`
		SELECT
			offer_id, seller_account,
			selling_asset_type, selling_asset_code, selling_asset_issuer,
			buying_asset_type, buying_asset_code, buying_asset_issuer,
			amount, price, flags, ledger_sequence, closed_at
		FROM %s.offers_snapshot_v1
		WHERE selling_asset_code = ? AND selling_asset_issuer = ?
		  AND buying_asset_code = ? AND buying_asset_issuer = ?
		ORDER BY CAST(price AS DOUBLE) ASC
		LIMIT ?
	`, r.bronze)

	rows, err := r.db.QueryContext(ctx, query, sellCode, sellIssuer, buyCode, buyIssuer, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to query offers by pair: %w", err)
	}
	defer rows.Close()
	return scanRowsToMaps(rows)
}

// GetSilverOfferByID returns a single offer by ID (latest snapshot).
func (r *DuckLakeReader) GetSilverOfferByID(ctx context.Context, offerID int64) (map[string]interface{}, error) {
	query := fmt.Sprintf(`
		SELECT
			offer_id, seller_account,
			selling_asset_type, selling_asset_code, selling_asset_issuer,
			buying_asset_type, buying_asset_code, buying_asset_issuer,
			amount, price, flags, ledger_sequence, closed_at
		FROM %s.offers_snapshot_v1
		WHERE offer_id = ?
		ORDER BY ledger_sequence DESC
		LIMIT 1
	`, r.bronze)

	rows, err := r.db.QueryContext(ctx, query, offerID)
	if err != nil {
		return nil, fmt.Errorf("failed to query offer by ID: %w", err)
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

// ── Claimable Balances (from bronze snapshots) ──────────────────

// GetClaimableBalances returns claimable balances from bronze.claimable_balances_snapshot_v1.
func (r *DuckLakeReader) GetClaimableBalances(ctx context.Context, sponsor string, limit int) ([]map[string]interface{}, error) {
	if limit <= 0 || limit > 200 {
		limit = 50
	}

	whereClause := ""
	var args []interface{}
	if sponsor != "" {
		whereClause = "WHERE sponsor = ?"
		args = append(args, sponsor)
	}
	args = append(args, limit)

	query := fmt.Sprintf(`
		SELECT
			balance_id, sponsor, asset_type, asset_code, asset_issuer,
			amount, claimants_count, flags, ledger_sequence, closed_at
		FROM %s.claimable_balances_snapshot_v1
		%s
		ORDER BY ledger_sequence DESC
		LIMIT ?
	`, r.bronze, whereClause)

	rows, err := r.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to query claimable balances: %w", err)
	}
	defer rows.Close()
	return scanRowsToMaps(rows)
}

// GetClaimableBalancesByAsset returns claimable balances for a specific asset.
func (r *DuckLakeReader) GetClaimableBalancesByAsset(ctx context.Context, assetCode, assetIssuer string, limit int) ([]map[string]interface{}, error) {
	if limit <= 0 || limit > 200 {
		limit = 50
	}

	query := fmt.Sprintf(`
		SELECT
			balance_id, sponsor, asset_type, asset_code, asset_issuer,
			amount, claimants_count, flags, ledger_sequence, closed_at
		FROM %s.claimable_balances_snapshot_v1
		WHERE asset_code = ? AND asset_issuer = ?
		ORDER BY amount DESC
		LIMIT ?
	`, r.bronze)

	rows, err := r.db.QueryContext(ctx, query, assetCode, assetIssuer, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to query claimable balances by asset: %w", err)
	}
	defer rows.Close()
	return scanRowsToMaps(rows)
}

// GetClaimableBalanceByID returns a single claimable balance by ID.
func (r *DuckLakeReader) GetClaimableBalanceByID(ctx context.Context, balanceID string) (map[string]interface{}, error) {
	query := fmt.Sprintf(`
		SELECT
			balance_id, sponsor, asset_type, asset_code, asset_issuer,
			amount, claimants_count, flags, ledger_sequence, closed_at
		FROM %s.claimable_balances_snapshot_v1
		WHERE balance_id = ?
		ORDER BY ledger_sequence DESC
		LIMIT 1
	`, r.bronze)

	rows, err := r.db.QueryContext(ctx, query, balanceID)
	if err != nil {
		return nil, fmt.Errorf("failed to query claimable balance by ID: %w", err)
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

// ── Trades Endpoints (from silver enriched operations) ──────────

// GetSilverTrades returns trade-type operations from silver.enriched_history_operations.
func (r *DuckLakeReader) GetSilverTrades(ctx context.Context, limit int, startLedger, endLedger int64, account string) ([]map[string]interface{}, error) {
	if limit <= 0 || limit > 500 {
		limit = 50
	}

	var conditions []string
	var args []interface{}

	conditions = append(conditions, "type_string IN ('manage_sell_offer', 'manage_buy_offer', 'create_passive_sell_offer')")

	if startLedger > 0 {
		conditions = append(conditions, "ledger_sequence >= ?")
		args = append(args, startLedger)
	}
	if endLedger > 0 {
		conditions = append(conditions, "ledger_sequence <= ?")
		args = append(args, endLedger)
	}
	if account != "" {
		conditions = append(conditions, "source_account = ?")
		args = append(args, account)
	}
	args = append(args, limit)

	query := fmt.Sprintf(`
		SELECT
			transaction_hash, operation_index, ledger_sequence,
			source_account, type_string, closed_at,
			selling_asset_code, selling_asset_issuer,
			buying_asset_code, buying_asset_issuer,
			amount, price, offer_id, transaction_successful
		FROM %s.enriched_history_operations
		WHERE %s
		ORDER BY ledger_sequence DESC, operation_index DESC
		LIMIT ?
	`, r.silver, strings.Join(conditions, " AND "))

	rows, err := r.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to query trades: %w", err)
	}
	defer rows.Close()
	return scanRowsToMaps(rows)
}

// GetSilverTradesByPair returns trades for a specific asset pair.
func (r *DuckLakeReader) GetSilverTradesByPair(ctx context.Context, sellCode, sellIssuer, buyCode, buyIssuer string, limit int) ([]map[string]interface{}, error) {
	if limit <= 0 || limit > 500 {
		limit = 50
	}

	query := fmt.Sprintf(`
		SELECT
			transaction_hash, operation_index, ledger_sequence,
			source_account, type_string, closed_at,
			selling_asset_code, selling_asset_issuer,
			buying_asset_code, buying_asset_issuer,
			amount, price, offer_id, transaction_successful
		FROM %s.enriched_history_operations
		WHERE type_string IN ('manage_sell_offer', 'manage_buy_offer', 'create_passive_sell_offer')
		  AND selling_asset_code = ? AND selling_asset_issuer = ?
		  AND buying_asset_code = ? AND buying_asset_issuer = ?
		ORDER BY ledger_sequence DESC
		LIMIT ?
	`, r.silver)

	rows, err := r.db.QueryContext(ctx, query, sellCode, sellIssuer, buyCode, buyIssuer, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to query trades by pair: %w", err)
	}
	defer rows.Close()
	return scanRowsToMaps(rows)
}

// GetSilverTradeStats returns aggregate trade statistics.
func (r *DuckLakeReader) GetSilverTradeStats(ctx context.Context, sellCode, sellIssuer, buyCode, buyIssuer string) (map[string]interface{}, error) {
	var conditions []string
	var args []interface{}

	conditions = append(conditions, "type_string IN ('manage_sell_offer', 'manage_buy_offer', 'create_passive_sell_offer')")

	if sellCode != "" {
		conditions = append(conditions, "selling_asset_code = ?")
		args = append(args, sellCode)
		if sellIssuer != "" {
			conditions = append(conditions, "selling_asset_issuer = ?")
			args = append(args, sellIssuer)
		}
	}
	if buyCode != "" {
		conditions = append(conditions, "buying_asset_code = ?")
		args = append(args, buyCode)
		if buyIssuer != "" {
			conditions = append(conditions, "buying_asset_issuer = ?")
			args = append(args, buyIssuer)
		}
	}

	query := fmt.Sprintf(`
		SELECT
			COUNT(*) AS total_trades,
			COUNT(DISTINCT source_account) AS unique_traders,
			MIN(closed_at) AS first_trade,
			MAX(closed_at) AS last_trade
		FROM %s.enriched_history_operations
		WHERE %s
	`, r.silver, strings.Join(conditions, " AND "))

	rows, err := r.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to query trade stats: %w", err)
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

// ── Effects Endpoints (from bronze effects table) ──────────────

// GetSilverEffects returns effects from bronze.effects_row_v1 with filtering.
func (r *DuckLakeReader) GetSilverEffects(ctx context.Context, limit int, startLedger, endLedger int64, account, effectType string) ([]map[string]interface{}, error) {
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
	if account != "" {
		conditions = append(conditions, "account_id = ?")
		args = append(args, account)
	}
	if effectType != "" {
		conditions = append(conditions, "effect_type_string = ?")
		args = append(args, effectType)
	}

	whereClause := ""
	if len(conditions) > 0 {
		whereClause = "WHERE " + strings.Join(conditions, " AND ")
	}
	args = append(args, limit)

	query := fmt.Sprintf(`
		SELECT
			ledger_sequence, transaction_hash, operation_index,
			effect_index, effect_type, effect_type_string,
			account_id, amount, asset_code, asset_issuer, asset_type,
			created_at
		FROM %s.effects_row_v1
		%s
		ORDER BY ledger_sequence DESC, operation_index DESC, effect_index DESC
		LIMIT ?
	`, r.bronze, whereClause)

	rows, err := r.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to query effects: %w", err)
	}
	defer rows.Close()
	return scanRowsToMaps(rows)
}

// GetEffectTypes returns distinct effect types with counts.
func (r *DuckLakeReader) GetEffectTypes(ctx context.Context) ([]map[string]interface{}, error) {
	query := fmt.Sprintf(`
		SELECT effect_type, effect_type_string, COUNT(*) AS count
		FROM %s.effects_row_v1
		GROUP BY effect_type, effect_type_string
		ORDER BY count DESC
	`, r.bronze)

	rows, err := r.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query effect types: %w", err)
	}
	defer rows.Close()
	return scanRowsToMaps(rows)
}

// GetEffectsByTransaction returns effects for a specific transaction.
func (r *DuckLakeReader) GetEffectsByTransaction(ctx context.Context, txHash string) ([]map[string]interface{}, error) {
	query := fmt.Sprintf(`
		SELECT
			ledger_sequence, transaction_hash, operation_index,
			effect_index, effect_type, effect_type_string,
			account_id, amount, asset_code, asset_issuer, asset_type,
			created_at
		FROM %s.effects_row_v1
		WHERE transaction_hash = ?
		ORDER BY operation_index ASC, effect_index ASC
	`, r.bronze)

	rows, err := r.db.QueryContext(ctx, query, txHash)
	if err != nil {
		return nil, fmt.Errorf("failed to query effects by transaction: %w", err)
	}
	defer rows.Close()
	return scanRowsToMaps(rows)
}

// ── Soroban Endpoints ──────────────────────────────────────────

// GetContractCode returns contract code/WASM metadata from bronze.contract_code_snapshot_v1.
func (r *DuckLakeReader) GetContractCode(ctx context.Context, contractID string) (map[string]interface{}, error) {
	// First get the wasm hash from contract_creations_v1
	creationQuery := fmt.Sprintf(`
		SELECT contract_id, creator_address, wasm_hash, created_ledger, created_at
		FROM %s.contract_creations_v1
		WHERE contract_id = ?
		ORDER BY created_ledger DESC
		LIMIT 1
	`, r.bronze)

	rows, err := r.db.QueryContext(ctx, creationQuery, contractID)
	if err != nil {
		return nil, fmt.Errorf("failed to query contract code: %w", err)
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

// GetContractData returns contract data entries from bronze.contract_data_snapshot_v1.
func (r *DuckLakeReader) GetContractData(ctx context.Context, contractID string, limit int) ([]map[string]interface{}, error) {
	if limit <= 0 || limit > 200 {
		limit = 50
	}

	whereClause := ""
	var args []interface{}
	if contractID != "" {
		whereClause = "WHERE contract_id = ? AND deleted = false"
		args = append(args, contractID)
	}
	args = append(args, limit)

	query := fmt.Sprintf(`
		SELECT
			contract_id, ledger_key_hash, contract_key_type, contract_durability,
			asset_code, asset_issuer, asset_type, balance_holder, balance,
			last_modified_ledger, deleted, closed_at,
			token_name, token_symbol, token_decimals
		FROM %s.contract_data_snapshot_v1
		%s
		ORDER BY ledger_sequence DESC
		LIMIT ?
	`, r.bronze, whereClause)

	rows, err := r.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to query contract data: %w", err)
	}
	defer rows.Close()
	return scanRowsToMaps(rows)
}

// GetContractDataEntry returns a single contract data entry.
func (r *DuckLakeReader) GetContractDataEntry(ctx context.Context, contractID, keyHash string) (map[string]interface{}, error) {
	query := fmt.Sprintf(`
		SELECT
			contract_id, ledger_key_hash, contract_key_type, contract_durability,
			asset_code, asset_issuer, balance_holder, balance,
			last_modified_ledger, deleted, closed_at,
			token_name, token_symbol, token_decimals
		FROM %s.contract_data_snapshot_v1
		WHERE contract_id = ? AND ledger_key_hash = ?
		ORDER BY ledger_sequence DESC
		LIMIT 1
	`, r.bronze)

	rows, err := r.db.QueryContext(ctx, query, contractID, keyHash)
	if err != nil {
		return nil, fmt.Errorf("failed to query contract data entry: %w", err)
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

// GetContractStorage returns contract data entries (alias for GetContractData).
func (r *DuckLakeReader) GetContractStorage(ctx context.Context, contractID string, limit int) ([]map[string]interface{}, error) {
	return r.GetContractData(ctx, contractID, limit)
}

// ── Contract Analysis (from bronze operations + contract_creations) ──

// GetContractsInvolved returns contracts involved in a transaction.
func (r *DuckLakeReader) GetContractsInvolved(ctx context.Context, txHash string) ([]map[string]interface{}, error) {
	query := fmt.Sprintf(`
		SELECT DISTINCT
			soroban_contract_id AS contract_id, soroban_function AS function_name
		FROM %s.enriched_history_operations
		WHERE transaction_hash = ? AND soroban_contract_id IS NOT NULL
		ORDER BY soroban_contract_id
	`, r.silver)

	rows, err := r.db.QueryContext(ctx, query, txHash)
	if err != nil {
		return nil, fmt.Errorf("failed to query contracts involved: %w", err)
	}
	defer rows.Close()
	return scanRowsToMaps(rows)
}

// GetTopContracts returns the most active contracts with top function and unknown call count.
func (r *DuckLakeReader) GetTopContracts(ctx context.Context, limit int) ([]map[string]interface{}, error) {
	if limit <= 0 || limit > 100 {
		limit = 20
	}

	query := fmt.Sprintf(`
		SELECT
			soroban_contract_id AS contract_id,
			COUNT(*) AS total_calls,
			COUNT(DISTINCT source_account) AS unique_callers,
			MAX(closed_at) AS last_activity,
			MODE(soroban_function) AS top_function,
			COUNT(*) FILTER (WHERE soroban_function IS NULL OR soroban_function = '') AS unknown_calls
		FROM %s.enriched_history_operations
		WHERE soroban_contract_id IS NOT NULL
		GROUP BY soroban_contract_id
		ORDER BY total_calls DESC
		LIMIT ?
	`, r.silver)

	rows, err := r.db.QueryContext(ctx, query, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to query top contracts: %w", err)
	}
	defer rows.Close()
	return scanRowsToMaps(rows)
}

// GetContractRecentCalls returns recent invocations of a contract.
func (r *DuckLakeReader) GetContractRecentCalls(ctx context.Context, contractID string, limit int) ([]map[string]interface{}, error) {
	if limit <= 0 || limit > 200 {
		limit = 50
	}

	query := fmt.Sprintf(`
		SELECT
			ledger_sequence, transaction_hash, operation_index,
			source_account, soroban_function AS function_name,
			transaction_successful, closed_at
		FROM %s.enriched_history_operations
		WHERE soroban_contract_id = ?
		ORDER BY ledger_sequence DESC
		LIMIT ?
	`, r.silver)

	rows, err := r.db.QueryContext(ctx, query, contractID, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to query contract recent calls: %w", err)
	}
	defer rows.Close()
	return scanRowsToMaps(rows)
}

// GetContractCallers returns unique callers of a contract.
func (r *DuckLakeReader) GetContractCallers(ctx context.Context, contractID string, limit int) ([]map[string]interface{}, error) {
	if limit <= 0 || limit > 200 {
		limit = 50
	}

	query := fmt.Sprintf(`
		SELECT
			source_account,
			COUNT(*) AS call_count,
			MAX(closed_at) AS last_call
		FROM %s.enriched_history_operations
		WHERE soroban_contract_id = ?
		GROUP BY source_account
		ORDER BY call_count DESC
		LIMIT ?
	`, r.silver)

	rows, err := r.db.QueryContext(ctx, query, contractID, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to query contract callers: %w", err)
	}
	defer rows.Close()
	return scanRowsToMaps(rows)
}

// GetContractCallees returns other contracts called alongside a given contract.
func (r *DuckLakeReader) GetContractCallees(ctx context.Context, contractID string, limit int) ([]map[string]interface{}, error) {
	if limit <= 0 || limit > 200 {
		limit = 50
	}

	query := fmt.Sprintf(`
		SELECT
			e2.soroban_contract_id AS callee_contract_id,
			e2.soroban_function AS function_name,
			COUNT(*) AS call_count
		FROM %s.enriched_history_operations e1
		JOIN %s.enriched_history_operations e2
			ON e1.transaction_hash = e2.transaction_hash
			AND e2.soroban_contract_id IS NOT NULL
			AND e2.soroban_contract_id != ?
		WHERE e1.soroban_contract_id = ?
		GROUP BY e2.soroban_contract_id, e2.soroban_function
		ORDER BY call_count DESC
		LIMIT ?
	`, r.silver, r.silver)

	rows, err := r.db.QueryContext(ctx, query, contractID, contractID, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to query contract callees: %w", err)
	}
	defer rows.Close()
	return scanRowsToMaps(rows)
}

// GetContractCallSummary returns a summary of a contract's call patterns.
func (r *DuckLakeReader) GetContractCallSummary(ctx context.Context, contractID string) (map[string]interface{}, error) {
	query := fmt.Sprintf(`
		SELECT
			soroban_contract_id AS contract_id,
			COUNT(*) AS total_calls,
			COUNT(CASE WHEN transaction_successful THEN 1 END) AS successful_calls,
			COUNT(CASE WHEN NOT transaction_successful THEN 1 END) AS failed_calls,
			COUNT(DISTINCT source_account) AS unique_callers,
			COUNT(DISTINCT soroban_function) AS unique_functions,
			MIN(closed_at) AS first_call,
			MAX(closed_at) AS last_call
		FROM %s.enriched_history_operations
		WHERE soroban_contract_id = ?
		GROUP BY soroban_contract_id
	`, r.silver)

	rows, err := r.db.QueryContext(ctx, query, contractID)
	if err != nil {
		return nil, fmt.Errorf("failed to query contract call summary: %w", err)
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

// GetContractAnalytics returns comprehensive analytics for a contract.
func (r *DuckLakeReader) GetContractAnalytics(ctx context.Context, contractID string) (map[string]interface{}, error) {
	summary, err := r.GetContractCallSummary(ctx, contractID)
	if err != nil {
		return nil, err
	}

	metadata, err := r.GetContractCode(ctx, contractID)
	if err != nil {
		return nil, err
	}

	// Get function breakdown
	query := fmt.Sprintf(`
		SELECT
			soroban_function AS function_name,
			COUNT(*) AS call_count,
			COUNT(CASE WHEN transaction_successful THEN 1 END) AS success_count
		FROM %s.enriched_history_operations
		WHERE soroban_contract_id = ?
		GROUP BY soroban_function
		ORDER BY call_count DESC
		LIMIT 20
	`, r.silver)

	rows, err := r.db.QueryContext(ctx, query, contractID)
	if err != nil {
		return nil, fmt.Errorf("failed to query function breakdown: %w", err)
	}
	defer rows.Close()

	functions, err := scanRowsToMaps(rows)
	if err != nil {
		return nil, err
	}

	return map[string]interface{}{
		"summary":   summary,
		"metadata":  metadata,
		"functions": functions,
	}, nil
}

// GetContractMetadata returns metadata for a contract.
func (r *DuckLakeReader) GetContractMetadata(ctx context.Context, contractID string) (map[string]interface{}, error) {
	return r.GetContractCode(ctx, contractID)
}

// GetCallGraph returns the call graph of a transaction.
func (r *DuckLakeReader) GetCallGraph(ctx context.Context, txHash string) ([]map[string]interface{}, error) {
	query := fmt.Sprintf(`
		SELECT
			soroban_contract_id AS contract_id, soroban_function AS function_name,
			source_account, operation_index, transaction_successful, closed_at
		FROM %s.enriched_history_operations
		WHERE transaction_hash = ? AND soroban_contract_id IS NOT NULL
		ORDER BY operation_index ASC
	`, r.silver)

	rows, err := r.db.QueryContext(ctx, query, txHash)
	if err != nil {
		return nil, fmt.Errorf("failed to query call graph: %w", err)
	}
	defer rows.Close()
	return scanRowsToMaps(rows)
}

// GetTransactionHierarchy returns the operation hierarchy for a transaction.
func (r *DuckLakeReader) GetTransactionHierarchy(ctx context.Context, txHash string) (map[string]interface{}, error) {
	query := fmt.Sprintf(`
		SELECT
			operation_index, type_string, source_account,
			soroban_contract_id, soroban_function, amount, destination,
			asset_code, transaction_successful
		FROM %s.enriched_history_operations
		WHERE transaction_hash = ?
		ORDER BY operation_index ASC
	`, r.silver)

	rows, err := r.db.QueryContext(ctx, query, txHash)
	if err != nil {
		return nil, fmt.Errorf("failed to query transaction hierarchy: %w", err)
	}
	defer rows.Close()

	ops, err := scanRowsToMaps(rows)
	if err != nil {
		return nil, err
	}

	return map[string]interface{}{
		"transaction_hash": txHash,
		"operations":       ops,
	}, nil
}

// GetContractsSummary returns a wallet-friendly summary of contracts in a transaction.
func (r *DuckLakeReader) GetContractsSummary(ctx context.Context, txHash string) (map[string]interface{}, error) {
	contracts, err := r.GetContractsInvolved(ctx, txHash)
	if err != nil {
		return nil, err
	}

	return map[string]interface{}{
		"transaction_hash": txHash,
		"contracts":        contracts,
		"contract_count":   len(contracts),
	}, nil
}
