package main

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"
)

func detectSearchQueryType(q string) string {
	if strings.HasPrefix(q, "G") && len(q) == 56 {
		return "account"
	}
	if strings.HasPrefix(q, "C") && len(q) == 56 {
		return "contract"
	}
	if len(q) == 64 {
		return "tx_hash"
	}
	if _, err := strconv.ParseInt(q, 10, 64); err == nil {
		return "ledger"
	}
	return "asset_code"
}

func (h *SearchHandlers) Search(ctx context.Context, query string) (*SearchResults, error) {
	results := &SearchResults{Query: query, Results: []SearchResult{}}
	q := strings.TrimSpace(query)
	if q == "" {
		return results, nil
	}

	switch detectSearchQueryType(q) {
	case "account":
		h.searchAccount(ctx, q, results)
	case "contract":
		h.searchContract(ctx, q, results)
	case "tx_hash":
		h.searchTransaction(ctx, q, results)
	case "ledger":
		h.searchLedger(ctx, q, results)
	default:
		h.searchAssetCode(ctx, q, results)
	}
	return results, nil
}

func (h *SearchHandlers) searchAccount(ctx context.Context, accountID string, results *SearchResults) {
	var count int64
	if h.hot != nil {
		_ = h.hot.db.QueryRowContext(ctx, `
			SELECT COUNT(*) FROM token_transfers_raw WHERE from_account = $1 OR to_account = $1
		`, accountID).Scan(&count)
	}
	if h.cold != nil {
		var coldCount int64
		_ = h.cold.db.QueryRowContext(ctx, fmt.Sprintf(`
			SELECT COUNT(*) FROM %s.%s.token_transfers_raw WHERE from_account = ? OR to_account = ?
		`, h.cold.catalogName, h.cold.schemaName), accountID, accountID).Scan(&coldCount)
		count += coldCount
	}

	label := "Stellar Account"
	if count > 0 {
		label = fmt.Sprintf("Stellar Account (%d transfers)", count)
	}
	results.Results = append(results.Results, SearchResult{
		Type:  "account",
		ID:    accountID,
		Label: label,
		Details: map[string]any{
			"transfer_count": count,
		},
	})
}

func (h *SearchHandlers) searchContract(ctx context.Context, contractID string, results *SearchResults) {
	var name, symbol sql.NullString
	if h.hot != nil {
		_ = h.hot.db.QueryRowContext(ctx, `
			SELECT token_name, token_symbol FROM token_registry WHERE contract_id = $1 LIMIT 1
		`, contractID).Scan(&name, &symbol)
	}
	if (!name.Valid || name.String == "") && h.cold != nil {
		_ = h.cold.db.QueryRowContext(ctx, fmt.Sprintf(`
			SELECT token_name, token_symbol FROM %s.%s.token_registry WHERE contract_id = ? LIMIT 1
		`, h.cold.catalogName, h.cold.schemaName), contractID).Scan(&name, &symbol)
	}

	label := "Smart Contract"
	if name.Valid && name.String != "" {
		label = name.String
		if symbol.Valid && symbol.String != "" {
			label = fmt.Sprintf("%s (%s)", name.String, symbol.String)
		}
	}
	details := map[string]any{}
	if name.Valid {
		details["token_name"] = name.String
	}
	if symbol.Valid {
		details["token_symbol"] = symbol.String
	}
	results.Results = append(results.Results, SearchResult{Type: "contract", ID: contractID, Label: label, Details: details})
}

func (h *SearchHandlers) searchTransaction(ctx context.Context, txHash string, results *SearchResults) {
	var ledgerSeq, opCount int64
	found := false
	if h.hot != nil {
		err := h.hot.db.QueryRowContext(ctx, `
			SELECT ledger_sequence, COUNT(*) as op_count
			FROM enriched_history_operations WHERE transaction_hash = $1
			GROUP BY ledger_sequence LIMIT 1
		`, txHash).Scan(&ledgerSeq, &opCount)
		found = err == nil
	}
	if !found && h.cold != nil {
		err := h.cold.db.QueryRowContext(ctx, fmt.Sprintf(`
			SELECT ledger_sequence, COUNT(*) as op_count
			FROM %s.%s.enriched_history_operations WHERE transaction_hash = ?
			GROUP BY ledger_sequence LIMIT 1
		`, h.cold.catalogName, h.cold.schemaName), txHash).Scan(&ledgerSeq, &opCount)
		found = err == nil
	}
	if !found {
		results.Results = append(results.Results, SearchResult{Type: "transaction", ID: txHash, Label: "Transaction (not found in current data)"})
		return
	}
	results.Results = append(results.Results, SearchResult{
		Type:  "transaction",
		ID:    txHash,
		Label: fmt.Sprintf("Transaction in ledger %d (%d ops)", ledgerSeq, opCount),
		Details: map[string]any{
			"ledger_sequence": ledgerSeq,
			"operation_count": opCount,
		},
	})
}

func (h *SearchHandlers) searchLedger(ctx context.Context, ledgerStr string, results *SearchResults) {
	ledgerSeq, _ := strconv.ParseInt(ledgerStr, 10, 64)
	var txCount int64
	if h.hot != nil {
		_ = h.hot.db.QueryRowContext(ctx, `
			SELECT COUNT(DISTINCT transaction_hash) FROM enriched_history_operations WHERE ledger_sequence = $1
		`, ledgerSeq).Scan(&txCount)
	}
	if h.cold != nil {
		var coldCount int64
		_ = h.cold.db.QueryRowContext(ctx, fmt.Sprintf(`
			SELECT COUNT(DISTINCT transaction_hash) FROM %s.%s.enriched_history_operations WHERE ledger_sequence = ?
		`, h.cold.catalogName, h.cold.schemaName), ledgerSeq).Scan(&coldCount)
		if coldCount > txCount {
			txCount = coldCount
		}
	}
	label := fmt.Sprintf("Ledger #%d", ledgerSeq)
	if txCount > 0 {
		label = fmt.Sprintf("Ledger #%d (%d transactions)", ledgerSeq, txCount)
	}
	results.Results = append(results.Results, SearchResult{
		Type:  "ledger",
		ID:    ledgerStr,
		Label: label,
		Details: map[string]any{
			"ledger_sequence":   ledgerSeq,
			"transaction_count": txCount,
		},
	})
}

func (h *SearchHandlers) searchAssetCode(ctx context.Context, assetCode string, results *SearchResults) {
	searchTerm := "%" + assetCode + "%"
	seen := map[string]bool{}

	appendTokenRows := func(rows *sql.Rows) {
		defer rows.Close()
		for rows.Next() {
			var contractID string
			var name, symbol sql.NullString
			if err := rows.Scan(&contractID, &name, &symbol); err != nil {
				continue
			}
			key := "token:" + contractID
			if seen[key] {
				continue
			}
			seen[key] = true
			label := "Token"
			if symbol.Valid && symbol.String != "" {
				label = symbol.String
			}
			if name.Valid && name.String != "" {
				if label != "Token" {
					label = fmt.Sprintf("%s - %s", label, name.String)
				} else {
					label = name.String
				}
			}
			results.Results = append(results.Results, SearchResult{
				Type:  "token",
				ID:    contractID,
				Label: label,
				Details: map[string]any{
					"contract_id":  contractID,
					"token_name":   name.String,
					"token_symbol": symbol.String,
				},
			})
		}
	}

	appendAssetRows := func(rows *sql.Rows) {
		defer rows.Close()
		for rows.Next() {
			var code string
			var issuer sql.NullString
			if err := rows.Scan(&code, &issuer); err != nil {
				continue
			}
			issuerStr := ""
			if issuer.Valid {
				issuerStr = issuer.String
			}
			key := "asset:" + code + ":" + issuerStr
			if seen[key] {
				continue
			}
			seen[key] = true
			results.Results = append(results.Results, SearchResult{
				Type:  "asset",
				ID:    code + ":" + issuerStr,
				Label: fmt.Sprintf("%s (Classic Asset)", code),
				Details: map[string]any{
					"asset_code":   code,
					"asset_issuer": issuerStr,
				},
			})
		}
	}

	if h.hot != nil {
		if rows, err := h.hot.db.QueryContext(ctx, `
			SELECT contract_id, token_name, token_symbol FROM token_registry
			WHERE token_symbol ILIKE $1 OR token_name ILIKE $1 LIMIT 10
		`, searchTerm); err == nil {
			appendTokenRows(rows)
		}
		if rows, err := h.hot.db.QueryContext(ctx, `
			SELECT DISTINCT asset_code, asset_issuer FROM trustlines_current
			WHERE asset_code ILIKE $1 LIMIT 10
		`, searchTerm); err == nil {
			appendAssetRows(rows)
		}
	}
	if h.cold != nil {
		if rows, err := h.cold.db.QueryContext(ctx, fmt.Sprintf(`
			SELECT contract_id, token_name, token_symbol FROM %s.%s.token_registry
			WHERE token_symbol ILIKE ? OR token_name ILIKE ? LIMIT 10
		`, h.cold.catalogName, h.cold.schemaName), searchTerm, searchTerm); err == nil {
			appendTokenRows(rows)
		}
		if rows, err := h.cold.db.QueryContext(ctx, fmt.Sprintf(`
			SELECT DISTINCT asset_code, asset_issuer FROM %s.%s.trustlines_current
			WHERE asset_code ILIKE ? LIMIT 10
		`, h.cold.catalogName, h.cold.schemaName), searchTerm); err == nil {
			appendAssetRows(rows)
		}
	}
}
