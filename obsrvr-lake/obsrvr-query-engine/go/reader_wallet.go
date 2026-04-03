package main

import (
	"context"
	"fmt"
	"strings"
)

// GatherWalletEvidence collects on-chain data needed for smart wallet detection.
func (r *DuckLakeReader) GatherWalletEvidence(ctx context.Context, contractID string) (*WalletEvidence, error) {
	evidence := &WalletEvidence{ContractID: contractID}

	// 1. Get observed function names from contract_invocations_raw
	fnQuery := fmt.Sprintf(`
		SELECT DISTINCT function_name FROM %s.contract_invocations_raw
		WHERE contract_id = ? AND function_name IS NOT NULL
	`, r.silver)
	fnRows, err := r.db.QueryContext(ctx, fnQuery, contractID)
	if err == nil {
		defer fnRows.Close()
		for fnRows.Next() {
			var fn string
			if fnRows.Scan(&fn) == nil {
				evidence.ObservedFunctions = append(evidence.ObservedFunctions, fn)
			}
		}
	}

	// 2. Check for __check_auth events in bronze contract_events
	authQuery := fmt.Sprintf(`
		SELECT COUNT(*) FROM %s.contract_events_stream_v1
		WHERE contract_id = ? AND topic0_decoded = '__check_auth'
		LIMIT 1
	`, r.bronze)
	var authCount int64
	if err := r.db.QueryRowContext(ctx, authQuery, contractID).Scan(&authCount); err == nil {
		evidence.HasCheckAuth = authCount > 0
	}

	// 3. Get instance storage entries from bronze contract_data
	storageQuery := fmt.Sprintf(`
		SELECT ledger_key_hash, COALESCE(contract_data_xdr, '') AS data_value
		FROM %s.contract_data_snapshot_v1
		WHERE contract_id = ? AND contract_durability = 'persistent'
		  AND deleted = false
		ORDER BY ledger_sequence DESC
		LIMIT 50
	`, r.bronze)
	storageRows, err := r.db.QueryContext(ctx, storageQuery, contractID)
	if err == nil {
		defer storageRows.Close()
		for storageRows.Next() {
			var keyHash, dataValue string
			if storageRows.Scan(&keyHash, &dataValue) == nil {
				evidence.InstanceStorage = append(evidence.InstanceStorage, StorageEntry{
					KeyHash: keyHash, DataValue: dataValue,
				})
			}
		}
	}

	return evidence, nil
}

// GetSmartWalletInfo detects smart wallet type and extracts signer info.
func (r *DuckLakeReader) GetSmartWalletInfo(ctx context.Context, contractID string) (map[string]interface{}, error) {
	evidence, err := r.GatherWalletEvidence(ctx, contractID)
	if err != nil {
		return nil, fmt.Errorf("failed to gather wallet evidence: %w", err)
	}

	registry := NewWalletDetectorRegistry()
	result := registry.Detect(*evidence)

	if result == nil {
		return map[string]interface{}{
			"contract_id":   contractID,
			"is_smart_wallet": false,
			"observed_functions": evidence.ObservedFunctions,
		}, nil
	}

	return map[string]interface{}{
		"contract_id":        contractID,
		"is_smart_wallet":    true,
		"wallet_type":        result.WalletType,
		"confidence":         result.Confidence,
		"signers":            result.Signers,
		"policies":           result.Policies,
		"observed_functions": evidence.ObservedFunctions,
	}, nil
}

// GetIndexTransactionLookup looks up a transaction by hash in bronze transactions.
func (r *DuckLakeReader) GetIndexTransactionLookup(ctx context.Context, txHash string) (map[string]interface{}, error) {
	query := fmt.Sprintf(`
		SELECT transaction_hash, ledger_sequence, operation_count, successful, created_at
		FROM %s.transactions_row_v2
		WHERE transaction_hash = ?
		LIMIT 1
	`, r.bronze)
	rows, err := r.db.QueryContext(ctx, query, txHash)
	if err != nil {
		return nil, fmt.Errorf("failed to lookup transaction: %w", err)
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

// GetIndexBatchTransactionLookup looks up multiple transactions by hash.
func (r *DuckLakeReader) GetIndexBatchTransactionLookup(ctx context.Context, hashes []string) ([]map[string]interface{}, error) {
	if len(hashes) == 0 {
		return nil, nil
	}
	if len(hashes) > 1000 {
		hashes = hashes[:1000]
	}
	placeholders := make([]string, len(hashes))
	args := make([]interface{}, len(hashes))
	for i, h := range hashes {
		placeholders[i] = "?"
		args[i] = h
	}
	query := fmt.Sprintf(`
		SELECT transaction_hash, ledger_sequence, operation_count, successful, created_at
		FROM %s.transactions_row_v2
		WHERE transaction_hash IN (%s)
	`, r.bronze, strings.Join(placeholders, ","))
	rows, err := r.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to batch lookup transactions: %w", err)
	}
	defer rows.Close()
	return scanRowsToMaps(rows)
}

// GetIndexHealth returns coverage statistics for the index.
func (r *DuckLakeReader) GetIndexHealth(ctx context.Context) (map[string]interface{}, error) {
	query := fmt.Sprintf(`
		SELECT
			COUNT(*) AS total_transactions,
			MIN(ledger_sequence) AS min_ledger,
			MAX(ledger_sequence) AS max_ledger,
			COUNT(DISTINCT ledger_sequence) AS total_ledgers
		FROM %s.transactions_row_v2
	`, r.bronze)
	rows, err := r.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query index health: %w", err)
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

// GetContractIndexLedgers returns ledgers where a contract has events.
func (r *DuckLakeReader) GetContractIndexLedgers(ctx context.Context, contractID string, limit int) ([]map[string]interface{}, error) {
	if limit <= 0 || limit > 200 {
		limit = 50
	}
	query := fmt.Sprintf(`
		SELECT ledger_sequence, COUNT(*) AS event_count, MIN(closed_at) AS first_event
		FROM %s.contract_events_stream_v1
		WHERE contract_id = ?
		GROUP BY ledger_sequence
		ORDER BY ledger_sequence DESC
		LIMIT ?
	`, r.bronze)
	rows, err := r.db.QueryContext(ctx, query, contractID, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to query contract ledgers: %w", err)
	}
	defer rows.Close()
	return scanRowsToMaps(rows)
}

// GetContractIndexSummary returns a summary of contract event activity.
func (r *DuckLakeReader) GetContractIndexSummary(ctx context.Context, contractID string) (map[string]interface{}, error) {
	query := fmt.Sprintf(`
		SELECT
			contract_id,
			COUNT(*) AS total_events,
			COUNT(DISTINCT ledger_sequence) AS active_ledgers,
			MIN(ledger_sequence) AS first_ledger,
			MAX(ledger_sequence) AS last_ledger,
			MIN(closed_at) AS first_seen,
			MAX(closed_at) AS last_seen
		FROM %s.contract_events_stream_v1
		WHERE contract_id = ?
		GROUP BY contract_id
	`, r.bronze)
	rows, err := r.db.QueryContext(ctx, query, contractID)
	if err != nil {
		return nil, fmt.Errorf("failed to query contract summary: %w", err)
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

// GetContractIndexHealth returns contract event index statistics.
func (r *DuckLakeReader) GetContractIndexHealth(ctx context.Context) (map[string]interface{}, error) {
	query := fmt.Sprintf(`
		SELECT
			COUNT(DISTINCT contract_id) AS total_contracts,
			COUNT(*) AS total_events,
			MIN(ledger_sequence) AS min_ledger,
			MAX(ledger_sequence) AS max_ledger
		FROM %s.contract_events_stream_v1
	`, r.bronze)
	rows, err := r.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query contract index health: %w", err)
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

// ── Gold Compliance Readers ──────────────────────────────────

// GetComplianceTransactions returns transaction archive for an asset in a date range.
func (r *DuckLakeReader) GetComplianceTransactions(ctx context.Context, assetCode, assetIssuer string, startLedger, endLedger int64, limit int) ([]map[string]interface{}, error) {
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	query := fmt.Sprintf(`
		SELECT transaction_hash, ledger_sequence, source_account, type_string,
			asset_code, asset_issuer, amount, destination, closed_at, transaction_successful
		FROM %s.enriched_history_operations
		WHERE asset_code = ? AND asset_issuer = ?
		  AND ledger_sequence BETWEEN ? AND ?
		ORDER BY ledger_sequence ASC
		LIMIT ?
	`, r.silver)
	rows, err := r.db.QueryContext(ctx, query, assetCode, assetIssuer, startLedger, endLedger, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to query compliance transactions: %w", err)
	}
	defer rows.Close()
	return scanRowsToMaps(rows)
}

// GetComplianceBalances returns point-in-time holder snapshot for an asset.
func (r *DuckLakeReader) GetComplianceBalances(ctx context.Context, assetCode, assetIssuer string, atLedger int64, limit int) ([]map[string]interface{}, error) {
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	query := fmt.Sprintf(`
		SELECT account_id, balance, ledger_sequence
		FROM (
			SELECT *, ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY ledger_sequence DESC) AS rn
			FROM %s.trustlines_snapshot_v1
			WHERE asset_code = ? AND asset_issuer = ? AND ledger_sequence <= ?
		) sub WHERE rn = 1 AND CAST(balance AS BIGINT) > 0
		ORDER BY CAST(balance AS BIGINT) DESC
		LIMIT ?
	`, r.bronze)
	rows, err := r.db.QueryContext(ctx, query, assetCode, assetIssuer, atLedger, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to query compliance balances: %w", err)
	}
	defer rows.Close()
	return scanRowsToMaps(rows)
}

// GetComplianceSupply returns supply timeline for an asset.
func (r *DuckLakeReader) GetComplianceSupply(ctx context.Context, assetCode, assetIssuer string, startLedger, endLedger int64) ([]map[string]interface{}, error) {
	query := fmt.Sprintf(`
		SELECT
			ledger_sequence,
			SUM(CAST(balance AS BIGINT)) AS total_supply,
			COUNT(DISTINCT account_id) AS holder_count
		FROM %s.trustlines_snapshot_v1
		WHERE asset_code = ? AND asset_issuer = ?
		  AND ledger_sequence BETWEEN ? AND ?
		GROUP BY ledger_sequence
		ORDER BY ledger_sequence ASC
	`, r.bronze)
	rows, err := r.db.QueryContext(ctx, query, assetCode, assetIssuer, startLedger, endLedger)
	if err != nil {
		return nil, fmt.Errorf("failed to query compliance supply: %w", err)
	}
	defer rows.Close()
	return scanRowsToMaps(rows)
}
