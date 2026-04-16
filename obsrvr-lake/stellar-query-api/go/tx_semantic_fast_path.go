package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"math/big"
	"strings"
	"time"
)

func (h *DecodeHandlers) getTransactionForSemantic(ctx context.Context, txHash string, options SemanticBuildOptions) (*DecodedTransaction, error) {
	requestStart := time.Now()
	if h.hotPathReader != nil {
		hotStart := time.Now()
		hotCtx, cancel := context.WithTimeout(ctx, 1200*time.Millisecond)
		defer cancel()
		decoded, err := h.hotPathReader.GetTransactionForDecode(hotCtx, txHash)
		if err == nil {
			log.Printf("tx_semantic path=hot tx=%s duration_ms=%d total_ms=%d deep=%t", txHash, time.Since(hotStart).Milliseconds(), time.Since(requestStart).Milliseconds(), options.DeepEnrichment)
			return decoded, nil
		}
		log.Printf("tx_semantic path=hot_fallback tx=%s duration_ms=%d deep=%t err=%v", txHash, time.Since(hotStart).Milliseconds(), options.DeepEnrichment, err)
	}

	if h.coldReader != nil {
		coldTimeout := 4 * time.Second
		if options.DeepEnrichment {
			coldTimeout = 6 * time.Second
		}
		coldStart := time.Now()
		coldCtx, cancel := context.WithTimeout(ctx, coldTimeout)
		defer cancel()
		decoded, err := h.coldReader.GetTransactionForSemanticFast(coldCtx, txHash, h.bronzeCold)
		if err == nil {
			log.Printf("tx_semantic path=cold_fast tx=%s duration_ms=%d total_ms=%d deep=%t", txHash, time.Since(coldStart).Milliseconds(), time.Since(requestStart).Milliseconds(), options.DeepEnrichment)
			return decoded, nil
		}
		log.Printf("tx_semantic path=cold_fast_error tx=%s duration_ms=%d total_ms=%d deep=%t err=%v", txHash, time.Since(coldStart).Milliseconds(), time.Since(requestStart).Milliseconds(), options.DeepEnrichment, err)
		return nil, err
	}

	return nil, fmt.Errorf("transaction semantic decode requires cold or hot reader")
}

func (r *SilverColdReader) GetTransactionForSemanticFast(ctx context.Context, txHash string, bronzeCold *ColdReader) (*DecodedTransaction, error) {
	if r == nil || r.db == nil {
		return nil, fmt.Errorf("silver cold reader not configured")
	}

	query := fmt.Sprintf(`
		SELECT operation_index, type, source_account, contract_id, function_name,
		       destination, asset_code, amount, is_soroban_op, tx_successful,
		       tx_fee_charged, ledger_sequence, ledger_closed_at
		FROM %s.%s.enriched_history_operations
		WHERE transaction_hash = ?
		ORDER BY operation_index ASC
	`, r.catalogName, r.schemaName)

	rows, err := r.db.QueryContext(ctx, query, txHash)
	if err != nil {
		return nil, fmt.Errorf("semantic cold operations query: %w", err)
	}
	defer rows.Close()

	tx := &DecodedTransaction{TxHash: txHash}
	seen := make(map[int]bool)
	for rows.Next() {
		var op DecodedOperation
		var txSuccessful bool
		var txFee int64
		var ledgerSeq int64
		var closedAt sql.NullString
		var isSorobanOp sql.NullBool
		var sourceAccount sql.NullString
		var contractID sql.NullString
		var functionName sql.NullString
		var destination sql.NullString
		var assetCode sql.NullString
		var amount sql.NullString
		if err := rows.Scan(&op.Index, &op.Type, &sourceAccount, &contractID, &functionName, &destination, &assetCode, &amount, &isSorobanOp, &txSuccessful, &txFee, &ledgerSeq, &closedAt); err != nil {
			return nil, fmt.Errorf("semantic cold operations scan: %w", err)
		}
		op.SourceAccount = sourceAccount.String
		op.TypeName = operationTypeNameDecode(op.Type)
		if contractID.Valid {
			op.ContractID = &contractID.String
		}
		if functionName.Valid {
			op.FunctionName = &functionName.String
		}
		if destination.Valid {
			op.Destination = &destination.String
		}
		if assetCode.Valid {
			op.AssetCode = &assetCode.String
		}
		if amount.Valid {
			op.Amount = &amount.String
		}
		if isSorobanOp.Valid {
			op.IsSorobanOp = isSorobanOp.Bool
		}
		if !seen[op.Index] {
			seen[op.Index] = true
			tx.Operations = append(tx.Operations, op)
		}
		if tx.LedgerSeq == 0 {
			tx.Successful = txSuccessful
			tx.Fee = txFee
			tx.LedgerSeq = ledgerSeq
			if closedAt.Valid {
				tx.ClosedAt = closedAt.String
			}
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("semantic cold operations iterate: %w", err)
	}
	if len(tx.Operations) == 0 {
		return nil, fmt.Errorf("%w: %s", ErrTxNotFound, txHash)
	}
	tx.OpCount = len(tx.Operations)

	r.enrichOperationArgumentsCold(ctx, tx, txHash)
	if events, err := r.getTransactionEventsFast(ctx, txHash); err == nil {
		tx.Events = events
	}
	tx.Summary = GenerateTxSummary(tx.Operations, tx.Events)
	r.enrichTransactionFromBronzeColdFast(ctx, tx, txHash, bronzeCold)
	return tx, nil
}

func (r *SilverColdReader) getTransactionEventsFast(ctx context.Context, txHash string) ([]UnifiedEvent, error) {
	query := fmt.Sprintf(`
		SELECT
			timestamp,
			transaction_hash,
			ledger_sequence,
			source_type,
			from_account,
			to_account,
			asset_code,
			asset_issuer,
			amount::text,
			token_contract_id,
			operation_type
		FROM %s.%s.token_transfers_raw
		WHERE transaction_successful = true
		  AND transaction_hash = ?
		ORDER BY ledger_sequence ASC, transaction_hash ASC, timestamp ASC
		LIMIT 1000
	`, r.catalogName, r.schemaName)

	rows, err := r.db.QueryContext(ctx, query, txHash)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var events []UnifiedEvent
	txEventCounter := 0
	for rows.Next() {
		var e UnifiedEvent
		var ts sql.NullString
		var from sql.NullString
		var to sql.NullString
		var assetCode sql.NullString
		var assetIssuer sql.NullString
		var amountText sql.NullString
		var contractID sql.NullString
		var operationType sql.NullInt32

		if err := rows.Scan(&ts, &e.TxHash, &e.LedgerSequence, &e.SourceType, &from, &to, &assetCode, &assetIssuer, &amountText, &contractID, &operationType); err != nil {
			return nil, err
		}
		e.ClosedAt = ts.String
		e.EventIndex = txEventCounter
		txEventCounter++
		if from.Valid {
			e.From = &from.String
		}
		if to.Valid {
			e.To = &to.String
		}
		if assetCode.Valid {
			e.AssetCode = &assetCode.String
		}
		if assetIssuer.Valid {
			e.AssetIssuer = &assetIssuer.String
		}
		if amountText.Valid {
			if amt, ok := new(big.Int).SetString(amountText.String, 10); ok {
				s := amt.String()
				e.Amount = &s
			} else {
				s := amountText.String
				e.Amount = &s
			}
		}
		if contractID.Valid {
			e.ContractID = &contractID.String
		}
		if operationType.Valid {
			v := operationType.Int32
			e.OperationType = &v
		}
		switch {
		case e.From == nil && e.To != nil:
			e.EventType = "mint"
		case e.To == nil && e.From != nil:
			e.EventType = "burn"
		default:
			e.EventType = "transfer"
		}
		prefix := e.TxHash
		if len(prefix) > 8 {
			prefix = prefix[:8]
		}
		e.EventID = fmt.Sprintf("%d-%s-%d", e.LedgerSequence, prefix, e.EventIndex)
		events = append(events, e)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	r.enrichTransactionEventsFast(ctx, events)
	return events, nil
}

func (r *SilverColdReader) enrichTransactionEventsFast(ctx context.Context, events []UnifiedEvent) {
	if r == nil || r.db == nil || len(events) == 0 {
		return
	}

	contractIDs := make([]string, 0)
	seen := make(map[string]struct{})
	for _, e := range events {
		if e.ContractID == nil || *e.ContractID == "" {
			continue
		}
		if _, ok := seen[*e.ContractID]; ok {
			continue
		}
		seen[*e.ContractID] = struct{}{}
		contractIDs = append(contractIDs, *e.ContractID)
	}
	if len(contractIDs) == 0 {
		return
	}

	placeholders := make([]string, len(contractIDs))
	args := make([]any, len(contractIDs))
	for i, id := range contractIDs {
		placeholders[i] = "?"
		args[i] = id
	}

	query := fmt.Sprintf(`
		SELECT contract_id, token_name, token_symbol, token_decimals, token_type
		FROM %s.%s.token_registry
		WHERE contract_id IN (%s)
	`, r.catalogName, r.schemaName, strings.Join(placeholders, ","))
	rows, err := r.db.QueryContext(ctx, query, args...)
	if err != nil {
		return
	}
	defer rows.Close()

	infoByContract := make(map[string]tokenRegistryInfo, len(contractIDs))
	for rows.Next() {
		var contractID string
		var name, symbol, tokenType sql.NullString
		var decimals sql.NullInt32
		if err := rows.Scan(&contractID, &name, &symbol, &decimals, &tokenType); err != nil {
			continue
		}
		info := tokenRegistryInfo{}
		if name.Valid && name.String != "" {
			s := name.String
			info.Name = &s
		}
		if symbol.Valid && symbol.String != "" {
			s := symbol.String
			info.Symbol = &s
		}
		if decimals.Valid {
			d := int(decimals.Int32)
			info.Decimals = &d
		}
		if tokenType.Valid && tokenType.String != "" {
			s := tokenType.String
			info.TokenType = &s
		}
		info.Issuer = inferIssuerFromTokenInfo(info.Name, info.Symbol, info.TokenType)
		infoByContract[contractID] = info
	}

	for i := range events {
		if events[i].ContractID == nil {
			continue
		}
		info, ok := infoByContract[*events[i].ContractID]
		if !ok {
			continue
		}
		applyTokenRegistryInfoToEvent(&events[i], info)
	}
}

func (r *SilverColdReader) enrichTransactionFromBronzeColdFast(ctx context.Context, tx *DecodedTransaction, txHash string, bronzeCold *ColdReader) {
	if bronzeCold == nil {
		return
	}
	bronzeCtx, cancel := context.WithTimeout(ctx, 700*time.Millisecond)
	defer cancel()

	query := fmt.Sprintf(`
		SELECT source_account, account_sequence, max_fee,
		       soroban_resources_instructions, soroban_resources_read_bytes,
		       soroban_resources_write_bytes
		FROM %s.%s.transactions_row_v2
		WHERE transaction_hash = ?
		LIMIT 1
	`, bronzeCold.CatalogName(), bronzeCold.SchemaName())
	var sourceAccount sql.NullString
	var accountSequence, maxFee sql.NullInt64
	var instructions, readBytes, writeBytes sql.NullInt64
	if err := bronzeCold.DB().QueryRowContext(bronzeCtx, query, txHash).Scan(&sourceAccount, &accountSequence, &maxFee, &instructions, &readBytes, &writeBytes); err != nil {
		return
	}
	if sourceAccount.Valid {
		tx.SourceAccount = &sourceAccount.String
	}
	if accountSequence.Valid {
		tx.AccountSequence = &accountSequence.Int64
	}
	if maxFee.Valid {
		tx.MaxFee = &maxFee.Int64
	}
	if instructions.Valid {
		tx.SorobanResourcesInstructions = &instructions.Int64
	}
	if readBytes.Valid {
		tx.SorobanResourcesReadBytes = &readBytes.Int64
	}
	if writeBytes.Valid {
		tx.SorobanResourcesWriteBytes = &writeBytes.Int64
	}
}
