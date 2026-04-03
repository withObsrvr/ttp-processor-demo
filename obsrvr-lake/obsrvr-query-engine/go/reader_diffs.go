package main

import (
	"context"
	"fmt"
)

// GetTransactionDiffs returns balance/state change information for a transaction.
// Since full XDR decoding requires the stellar/go/xdr dependency (heavy),
// we return operations-level diffs derived from enriched_history_operations
// plus the raw tx_meta base64 for clients that want to decode themselves.
func (r *DuckLakeReader) GetTransactionDiffs(ctx context.Context, txHash string) (map[string]interface{}, error) {
	// Get raw tx_meta XDR from bronze (if available)
	var txMeta *string
	var ledgerSeq int64
	metaQuery := fmt.Sprintf(`
		SELECT ledger_sequence, tx_meta
		FROM %s.transactions_row_v2
		WHERE transaction_hash = ?
		LIMIT 1
	`, r.bronze)
	row := r.db.QueryRowContext(ctx, metaQuery, txHash)
	if err := row.Scan(&ledgerSeq, &txMeta); err != nil {
		// Transaction not found
		return nil, nil
	}

	// Get operation-level changes from enriched operations (approximate diffs)
	opsQuery := fmt.Sprintf(`
		SELECT
			operation_index, type_string, source_account,
			asset_code, asset_issuer, amount, destination,
			selling_asset_code, selling_asset_issuer,
			buying_asset_code, buying_asset_issuer,
			soroban_contract_id, soroban_function,
			transaction_successful
		FROM %s.enriched_history_operations
		WHERE transaction_hash = ?
		ORDER BY operation_index ASC
	`, r.silver)

	opsRows, err := r.db.QueryContext(ctx, opsQuery, txHash)
	if err != nil {
		return nil, fmt.Errorf("failed to query operations for diffs: %w", err)
	}
	defer opsRows.Close()

	ops, err := scanRowsToMaps(opsRows)
	if err != nil {
		return nil, err
	}

	result := map[string]interface{}{
		"transaction_hash": txHash,
		"ledger_sequence":  ledgerSeq,
		"operations":       ops,
		"operation_count":  len(ops),
	}

	if txMeta != nil && *txMeta != "" {
		result["tx_meta_xdr"] = *txMeta
		result["has_raw_meta"] = true
	} else {
		result["has_raw_meta"] = false
	}

	return result, nil
}
