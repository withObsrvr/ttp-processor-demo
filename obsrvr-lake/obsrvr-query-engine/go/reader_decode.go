package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
)

// GetTransactionForDecode returns a decoded transaction matching Prism's DecodedTransaction type.
func (r *DuckLakeReader) GetTransactionForDecode(ctx context.Context, txHash string) (map[string]interface{}, error) {
	// Get operations from silver enriched
	opsQuery := fmt.Sprintf(`
		SELECT
			operation_index, type, type_string, source_account,
			soroban_contract_id, soroban_function,
			destination, asset_code, asset_issuer, amount,
			transaction_successful, fee_charged,
			ledger_sequence, closed_at, memo_type, memo
		FROM %s.enriched_history_operations
		WHERE transaction_hash = ?
		ORDER BY operation_index ASC
	`, r.silver)

	opsRows, err := r.db.QueryContext(ctx, opsQuery, txHash)
	if err != nil {
		return nil, fmt.Errorf("failed to query operations for decode: %w", err)
	}
	defer opsRows.Close()

	rawOps, err := scanRowsToMaps(opsRows)
	if err != nil {
		return nil, err
	}
	if len(rawOps) == 0 {
		return nil, nil
	}

	firstOp := rawOps[0]

	// Get transaction-level details from bronze
	var sourceAccountTx interface{}
	var sorobanInstructions, sorobanReadBytes, sorobanWriteBytes interface{}

	txQuery := fmt.Sprintf(`
		SELECT source_account, soroban_resources_instructions, soroban_resources_read_bytes, soroban_resources_write_bytes
		FROM %s.transactions_row_v2 WHERE transaction_hash = ? LIMIT 1
	`, r.bronze)
	txRows, err := r.db.QueryContext(ctx, txQuery, txHash)
	if err == nil {
		defer txRows.Close()
		txDetails, _ := scanRowsToMaps(txRows)
		if len(txDetails) > 0 {
			sourceAccountTx = txDetails[0]["source_account"]
			sorobanInstructions = txDetails[0]["soroban_resources_instructions"]
			sorobanReadBytes = txDetails[0]["soroban_resources_read_bytes"]
			sorobanWriteBytes = txDetails[0]["soroban_resources_write_bytes"]
		}
	}

	// Reshape operations to match Prism's DecodedOperation type
	operations := make([]map[string]interface{}, len(rawOps))
	for i, op := range rawOps {
		contractID, _ := op["soroban_contract_id"].(string)
		functionName, _ := op["soroban_function"].(string)
		isSoroban := contractID != ""

		operations[i] = map[string]interface{}{
			"index":          op["operation_index"],
			"type":           op["type"],
			"type_name":      op["type_string"],
			"source_account": strOrEmpty(op["source_account"]),
			"contract_id":    contractID,
			"function_name":  functionName,
			"is_soroban_op":  isSoroban,
			"destination":    strOrEmpty(op["destination"]),
			"amount":         strOrEmpty(op["amount"]),
			"asset_code":     strOrEmpty(op["asset_code"]),
			"asset_issuer":   strOrEmpty(op["asset_issuer"]),
		}
	}

	// Get events from bronze and reshape to unified format
	events := r.getUnifiedEventsForTx(ctx, txHash, firstOp)

	// Build summary with structured detail objects
	summary := buildTxSummary(rawOps, events)

	result := map[string]interface{}{
		"tx_hash":         txHash,
		"ledger_sequence": firstOp["ledger_sequence"],
		"closed_at":       firstOp["closed_at"],
		"successful":      firstOp["transaction_successful"],
		"fee":             firstOp["fee_charged"],
		"operation_count": len(operations),
		"source_account":  sourceAccountTx,
		"memo_type":       firstOp["memo_type"],
		"memo":            firstOp["memo"],
		"summary":         summary,
		"operations":      operations,
		"events":          events,
		"soroban_resources_instructions": sorobanInstructions,
		"soroban_resources_read_bytes":   sorobanReadBytes,
		"soroban_resources_write_bytes":  sorobanWriteBytes,
		"errors":          []string{},
	}

	return result, nil
}

// getUnifiedEventsForTx returns events reshaped to Prism's UnifiedEvent format.
func (r *DuckLakeReader) getUnifiedEventsForTx(ctx context.Context, txHash string, firstOp map[string]interface{}) []map[string]interface{} {
	eventsQuery := fmt.Sprintf(`
		SELECT
			event_id, contract_id, event_type,
			topic0_decoded, topic1_decoded, topic2_decoded,
			data_decoded, topic_count,
			operation_index, event_index, ledger_sequence, closed_at
		FROM %s.contract_events_stream_v1
		WHERE transaction_hash = ?
		ORDER BY operation_index ASC, event_index ASC
	`, r.bronze)

	eventRows, err := r.db.QueryContext(ctx, eventsQuery, txHash)
	if err != nil {
		return nil
	}
	defer eventRows.Close()

	rawEvents, _ := scanRowsToMaps(eventRows)

	var events []map[string]interface{}
	for _, ev := range rawEvents {
		topic0, _ := ev["topic0_decoded"].(string)
		contractID, _ := ev["contract_id"].(string)

		// Determine unified event type from topic0
		eventType := "contract"
		switch topic0 {
		case "transfer":
			eventType = "transfer"
		case "mint":
			eventType = "mint"
		case "burn":
			eventType = "burn"
		case "clawback":
			eventType = "clawback"
		case "approve":
			eventType = "approve"
		}

		// Parse from/to from topics
		from := extractAddress(ev["topic1_decoded"])
		to := extractAddress(ev["topic2_decoded"])
		amount := extractAmount(ev["data_decoded"])
		assetCode := "" // Soroban events don't carry asset_code directly

		events = append(events, map[string]interface{}{
			"event_id":        ev["event_id"],
			"event_type":      eventType,
			"ledger_sequence": ev["ledger_sequence"],
			"tx_hash":         txHash,
			"closed_at":       ev["closed_at"],
			"from":            from,
			"to":              to,
			"amount":          amount,
			"asset_code":      assetCode,
			"contract_id":     contractID,
		})
	}
	return events
}

// buildTxSummary creates the nested summary matching Prism's TxSummary type.
func buildTxSummary(ops []map[string]interface{}, events []map[string]interface{}) map[string]interface{} {
	summary := map[string]interface{}{
		"type":               "classic",
		"description":        "",
		"involved_contracts": []string{},
		"transfer":           nil,
		"swap":               nil,
		"mint":               nil,
		"burn":               nil,
	}

	// Collect involved contracts
	var contracts []string
	contractSet := map[string]bool{}
	var primaryContract, primaryFunction string

	for _, op := range ops {
		if cid, ok := op["soroban_contract_id"].(string); ok && cid != "" {
			if !contractSet[cid] {
				contracts = append(contracts, cid)
				contractSet[cid] = true
			}
			if primaryContract == "" {
				primaryContract = cid
			}
		}
		if fn, ok := op["soroban_function"].(string); ok && fn != "" && primaryFunction == "" {
			primaryFunction = fn
		}
	}
	summary["involved_contracts"] = contracts

	// Check events for mint/burn/transfer patterns
	var mintEvent, transferEvent map[string]interface{}
	for _, ev := range events {
		evType, _ := ev["event_type"].(string)
		switch evType {
		case "mint":
			mintEvent = ev
		case "transfer":
			transferEvent = ev
		}
	}

	contractShort := shortAddr(primaryContract)

	if len(contracts) > 0 && primaryFunction != "" {
		// Soroban contract call
		switch primaryFunction {
		case "transfer", "transfer_from":
			summary["type"] = "transfer"
			from := strOrEmpty(transferEvent["from"])
			to := strOrEmpty(transferEvent["to"])
			amount := strOrEmpty(transferEvent["amount"])
			assetCode := findAssetCode(ops)
			summary["transfer"] = map[string]interface{}{
				"from": from, "to": to, "amount": amount, "asset": assetCode,
			}
			summary["description"] = fmt.Sprintf("Transferred %s %s to %s", amount, assetCode, shortAddr(to))

		case "swap", "swap_exact_tokens_for_tokens", "swap_tokens_for_exact_tokens":
			summary["type"] = "swap"
			summary["description"] = fmt.Sprintf("Swapped via %s", contractShort)

		case "mint":
			summary["type"] = "mint"
			to := strOrEmpty(mintEvent["to"])
			amount := strOrEmpty(mintEvent["amount"])
			assetCode := findAssetCode(ops)
			summary["mint"] = map[string]interface{}{
				"to": to, "amount": amount, "asset": assetCode,
			}
			summary["description"] = fmt.Sprintf("Minted %s %s to %s", amount, assetCode, shortAddr(to))

		case "burn", "burn_from":
			summary["type"] = "burn"
			from := strOrEmpty(ops[0]["source_account"])
			amount := strOrEmpty(ops[0]["amount"])
			assetCode := findAssetCode(ops)
			summary["burn"] = map[string]interface{}{
				"from": from, "amount": amount, "asset": assetCode,
			}
			summary["description"] = fmt.Sprintf("Burned %s %s", amount, assetCode)

		case "approve":
			summary["type"] = "contract_call"
			summary["description"] = fmt.Sprintf("Approved %s via approve()", contractShort)

		default:
			summary["type"] = "contract_call"
			summary["description"] = fmt.Sprintf("Called %s() on %s", primaryFunction, contractShort)
		}
	} else {
		// Classic transaction — also check events for mint patterns
		if mintEvent != nil {
			to := strOrEmpty(mintEvent["to"])
			amount := strOrEmpty(mintEvent["amount"])
			assetCode := findAssetCode(ops)
			summary["mint"] = map[string]interface{}{
				"to": to, "amount": amount, "asset": assetCode,
			}
		}
		summary["type"] = "classic"
		summary["description"] = buildClassicDescription(ops, events)
	}

	return summary
}

func buildClassicDescription(ops []map[string]interface{}, events []map[string]interface{}) string {
	if len(ops) == 0 {
		return "Transaction"
	}

	// Build a multi-operation description
	var parts []string
	for _, op := range ops {
		typeStr, _ := op["type_string"].(string)
		switch typeStr {
		case "OperationTypePayment":
			amount, _ := op["amount"].(string)
			dest, _ := op["destination"].(string)
			assetCode, _ := op["asset_code"].(string)
			if assetCode == "" {
				assetCode = "XLM"
			}
			parts = append(parts, fmt.Sprintf("paid %s %s to %s", amount, assetCode, shortAddr(dest)))
		case "OperationTypeCreateAccount":
			dest, _ := op["destination"].(string)
			parts = append(parts, fmt.Sprintf("created account %s", shortAddr(dest)))
		case "OperationTypeChangeTrust":
			assetCode, _ := op["asset_code"].(string)
			parts = append(parts, fmt.Sprintf("changed trust for %s", assetCode))
		case "OperationTypeAccountMerge":
			parts = append(parts, "merged account")
		case "OperationTypeManageSellOffer", "OperationTypeManageBuyOffer", "OperationTypeCreatePassiveSellOffer":
			parts = append(parts, "DEX offer")
		default:
			parts = append(parts, typeStr)
		}
	}

	// Check events for additional context (e.g., mint events from SAC)
	for _, ev := range events {
		evType, _ := ev["event_type"].(string)
		if evType == "mint" {
			amount := strOrEmpty(ev["amount"])
			to := strOrEmpty(ev["to"])
			assetCode := findAssetCode(ops)
			if amount != "" {
				parts = append(parts, fmt.Sprintf("minted %s %s to %s", amount, assetCode, shortAddr(to)))
			}
		}
	}

	if len(parts) == 0 {
		return "Transaction"
	}
	// Capitalize first letter
	desc := strings.Join(parts, " and ")
	if len(desc) > 0 {
		desc = strings.ToUpper(desc[:1]) + desc[1:]
	}
	return desc
}

func findAssetCode(ops []map[string]interface{}) string {
	for _, op := range ops {
		if ac, ok := op["asset_code"].(string); ok && ac != "" {
			return ac
		}
	}
	return "XLM"
}

func shortAddr(addr interface{}) string {
	s, _ := addr.(string)
	if len(s) > 10 {
		return s[:4] + "..." + s[len(s)-4:]
	}
	return s
}

func strOrEmpty(v interface{}) string {
	if v == nil {
		return ""
	}
	s, _ := v.(string)
	return s
}

// extractAddress pulls an address from a topic decoded value.
// Topics can be a plain string address or a JSON object like {"address":"G...","type":"account"}
func extractAddress(v interface{}) string {
	s, ok := v.(string)
	if !ok || s == "" {
		return ""
	}
	// If it's a raw Stellar address
	if len(s) == 56 && (s[0] == 'G' || s[0] == 'C') {
		return s
	}
	// Try parsing as JSON object
	var obj map[string]interface{}
	if json.Unmarshal([]byte(s), &obj) == nil {
		if addr, ok := obj["address"].(string); ok {
			return addr
		}
	}
	return s
}

// extractAmount pulls an amount from a data_decoded value.
// data_decoded can be a JSON object like {"hi":0,"lo":1930000000,"type":"i128","value":"1930000000"}
func extractAmount(v interface{}) string {
	s, ok := v.(string)
	if !ok || s == "" {
		return ""
	}
	var obj map[string]interface{}
	if json.Unmarshal([]byte(s), &obj) == nil {
		if val, ok := obj["value"]; ok {
			return fmt.Sprintf("%v", val)
		}
		if lo, ok := obj["lo"]; ok {
			return fmt.Sprintf("%v", lo)
		}
	}
	return s
}

// GetContractFunctions returns observed function names for a contract.
func (r *DuckLakeReader) GetContractFunctions(ctx context.Context, contractID string) ([]string, error) {
	query := fmt.Sprintf(`
		SELECT DISTINCT function_name
		FROM %s.contract_invocations_raw
		WHERE contract_id = ? AND function_name IS NOT NULL AND function_name != ''
		ORDER BY function_name
	`, r.silver)

	rows, err := r.db.QueryContext(ctx, query, contractID)
	if err != nil {
		return nil, fmt.Errorf("failed to query contract functions: %w", err)
	}
	defer rows.Close()

	var functions []string
	for rows.Next() {
		var fn string
		if err := rows.Scan(&fn); err != nil {
			continue
		}
		functions = append(functions, fn)
	}
	return functions, rows.Err()
}

// GetTransactionHashesForLedger returns transaction hashes for a specific ledger.
func (r *DuckLakeReader) GetTransactionHashesForLedger(ctx context.Context, ledgerSeq int64, limit int) ([]string, error) {
	if limit <= 0 || limit > 100 {
		limit = 25
	}

	query := fmt.Sprintf(`
		SELECT DISTINCT transaction_hash
		FROM %s.transactions_row_v2
		WHERE ledger_sequence = ?
		LIMIT ?
	`, r.bronze)

	rows, err := r.db.QueryContext(ctx, query, ledgerSeq, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to query transaction hashes: %w", err)
	}
	defer rows.Close()

	var hashes []string
	for rows.Next() {
		var h string
		if err := rows.Scan(&h); err != nil {
			continue
		}
		hashes = append(hashes, h)
	}
	return hashes, rows.Err()
}
