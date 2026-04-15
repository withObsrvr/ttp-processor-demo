package main

import (
	"context"
	"database/sql"
	"fmt"
	"math/big"
	"strings"
	"time"
)

// TxHotPathReader provides direct PostgreSQL hot-path reads for transaction
// receipt/detail endpoints so recent/selective tx queries do not have to go
// through DuckDB ATTACH POSTGRES federation.
type TxHotPathReader struct {
	bronze *sql.DB
	silver *sql.DB
}

func NewTxHotPathReader(bronze, silver *sql.DB) *TxHotPathReader {
	return &TxHotPathReader{bronze: bronze, silver: silver}
}

func (r *TxHotPathReader) GetTransactionForDecode(ctx context.Context, txHash string) (*DecodedTransaction, error) {
	if r == nil || r.silver == nil {
		return nil, fmt.Errorf("tx hot path reader not configured")
	}

	rows, err := r.silver.QueryContext(ctx, `
		SELECT
			operation_index,
			type,
			source_account,
			contract_id,
			function_name,
			destination,
			asset_code,
			amount,
			is_soroban_op,
			tx_successful,
			tx_fee_charged,
			ledger_sequence,
			ledger_closed_at
		FROM enriched_history_operations
		WHERE transaction_hash = $1
		ORDER BY operation_index ASC
	`, txHash)
	if err != nil {
		return nil, fmt.Errorf("hot decode operations query: %w", err)
	}
	defer rows.Close()

	tx := &DecodedTransaction{TxHash: txHash}
	for rows.Next() {
		var op DecodedOperation
		var txSuccessful bool
		var txFee int64
		var ledgerSeq int64
		var closedAt sql.NullTime
		var isSorobanOp sql.NullBool
		var sourceAccount sql.NullString
		var contractID sql.NullString
		var functionName sql.NullString
		var destination sql.NullString
		var assetCode sql.NullString
		var amount sql.NullString

		if err := rows.Scan(
			&op.Index,
			&op.Type,
			&sourceAccount,
			&contractID,
			&functionName,
			&destination,
			&assetCode,
			&amount,
			&isSorobanOp,
			&txSuccessful,
			&txFee,
			&ledgerSeq,
			&closedAt,
		); err != nil {
			return nil, fmt.Errorf("hot decode operations scan: %w", err)
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

		tx.Operations = append(tx.Operations, op)
		if tx.LedgerSeq == 0 {
			tx.Successful = txSuccessful
			tx.Fee = txFee
			tx.LedgerSeq = ledgerSeq
			if closedAt.Valid {
				tx.ClosedAt = closedAt.Time.UTC().Format(time.RFC3339)
			}
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("hot decode operations iterate: %w", err)
	}
	if len(tx.Operations) == 0 {
		return nil, fmt.Errorf("%w: %s", ErrTxNotFound, txHash)
	}

	tx.OpCount = len(tx.Operations)

	r.enrichOperationArguments(ctx, tx, txHash)

	events, err := r.GetTransactionEvents(ctx, txHash)
	if err != nil {
		return nil, fmt.Errorf("hot decode events: %w", err)
	}
	tx.Events = events
	tx.Summary = GenerateTxSummary(tx.Operations, tx.Events)

	r.enrichTransactionFromBronze(ctx, tx, txHash)
	return tx, nil
}

func (r *TxHotPathReader) enrichOperationArguments(ctx context.Context, tx *DecodedTransaction, txHash string) {
	if r == nil || r.silver == nil {
		return
	}

	argsByIndex := map[int]string{}
	rows, err := r.silver.QueryContext(ctx, `
		SELECT operation_index, arguments_json
		FROM contract_invocations_raw
		WHERE transaction_hash = $1
		ORDER BY operation_index
	`, txHash)
	if err != nil {
		return
	}
	defer rows.Close()

	for rows.Next() {
		var opIndex int
		var argsJSON sql.NullString
		if err := rows.Scan(&opIndex, &argsJSON); err != nil {
			continue
		}
		if argsJSON.Valid && argsJSON.String != "" && argsJSON.String != "[]" {
			argsByIndex[opIndex] = argsJSON.String
		}
	}

	for i := range tx.Operations {
		if args, ok := argsByIndex[tx.Operations[i].Index]; ok {
			tx.Operations[i].ArgumentsJSON = &args
		}
	}
}

func (r *TxHotPathReader) enrichTransactionFromBronze(ctx context.Context, tx *DecodedTransaction, txHash string) {
	if r == nil || r.bronze == nil {
		return
	}

	var sourceAccount sql.NullString
	var accountSequence, maxFee sql.NullInt64
	var instructions, readBytes, writeBytes sql.NullInt64

	err := r.bronze.QueryRowContext(ctx, `
		SELECT source_account, account_sequence, max_fee,
		       soroban_resources_instructions, soroban_resources_read_bytes,
		       soroban_resources_write_bytes
		FROM transactions_row_v2
		WHERE transaction_hash = $1
		LIMIT 1
	`, txHash).Scan(
		&sourceAccount,
		&accountSequence,
		&maxFee,
		&instructions,
		&readBytes,
		&writeBytes,
	)
	if err != nil {
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

func (r *TxHotPathReader) GetTransactionEvents(ctx context.Context, txHash string) ([]UnifiedEvent, error) {
	if r == nil || r.silver == nil {
		return nil, fmt.Errorf("tx hot path reader not configured")
	}

	rows, err := r.silver.QueryContext(ctx, `
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
		FROM token_transfers_raw
		WHERE transaction_successful = true
		  AND transaction_hash = $1
		ORDER BY ledger_sequence ASC, transaction_hash ASC, timestamp ASC
		LIMIT 1000
	`, txHash)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var events []UnifiedEvent
	txEventCounter := 0
	for rows.Next() {
		var e UnifiedEvent
		var ts time.Time
		var from sql.NullString
		var to sql.NullString
		var assetCode sql.NullString
		var assetIssuer sql.NullString
		var amountText sql.NullString
		var contractID sql.NullString
		var operationType sql.NullInt32

		if err := rows.Scan(
			&ts,
			&e.TxHash,
			&e.LedgerSequence,
			&e.SourceType,
			&from,
			&to,
			&assetCode,
			&assetIssuer,
			&amountText,
			&contractID,
			&operationType,
		); err != nil {
			return nil, err
		}

		e.ClosedAt = ts.UTC().Format(time.RFC3339)
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
			// Normalize potentially-large integer text exactly as a decimal string.
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
		e.EventID = fmt.Sprintf("%d-%s-%d", e.LedgerSequence, e.TxHash[:8], e.EventIndex)
		events = append(events, e)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	r.enrichTransactionEvents(ctx, events)
	return events, nil
}

func (r *TxHotPathReader) enrichTransactionEvents(ctx context.Context, events []UnifiedEvent) {
	if r == nil || r.silver == nil || len(events) == 0 {
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
		placeholders[i] = fmt.Sprintf("$%d", i+1)
		args[i] = id
	}

	rows, err := r.silver.QueryContext(ctx, fmt.Sprintf(`
		SELECT contract_id, token_name, token_symbol, token_decimals, token_type
		FROM token_registry
		WHERE contract_id IN (%s)
	`, strings.Join(placeholders, ",")), args...)
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

func (r *TxHotPathReader) GetTransactionDiffs(ctx context.Context, txHash string) (*TxDiffs, error) {
	if r == nil || r.bronze == nil {
		return nil, fmt.Errorf("tx hot path reader not configured")
	}

	var ledgerSeq int64
	var txMetaXDR sql.NullString
	err := r.bronze.QueryRowContext(ctx, `
		SELECT ledger_sequence, tx_meta
		FROM transactions_row_v2
		WHERE transaction_hash = $1
		LIMIT 1
	`, txHash).Scan(&ledgerSeq, &txMetaXDR)
	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("%w: %s", ErrTxNotFound, txHash)
	}
	if err != nil {
		return nil, fmt.Errorf("hot tx diffs query: %w", err)
	}

	diffs := &TxDiffs{
		TxHash:         txHash,
		LedgerSequence: ledgerSeq,
		BalanceChanges: []TxBalanceChange{},
		StateChanges:   []TxStateChange{},
	}
	if !txMetaXDR.Valid || txMetaXDR.String == "" {
		return diffs, nil
	}

	balanceChanges, stateChanges, err := decodeTxMetaXDR(txMetaXDR.String)
	if err != nil {
		return diffs, nil
	}
	diffs.BalanceChanges = balanceChanges
	diffs.StateChanges = stateChanges
	return diffs, nil
}

func (r *TxHotPathReader) ResolveHashesFromLedger(ctx context.Context, ledgerSeq int64, limit int) ([]string, error) {
	if r == nil || r.silver == nil {
		return nil, fmt.Errorf("tx hot path reader not configured")
	}

	rows, err := r.silver.QueryContext(ctx, `
		SELECT DISTINCT transaction_hash
		FROM enriched_history_operations
		WHERE ledger_sequence = $1
		ORDER BY transaction_hash
		LIMIT $2
	`, ledgerSeq, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var hashes []string
	for rows.Next() {
		var hash string
		if err := rows.Scan(&hash); err != nil {
			return nil, err
		}
		hashes = append(hashes, hash)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(hashes) == 0 {
		return nil, fmt.Errorf("no transactions found in ledger %d", ledgerSeq)
	}
	return hashes, nil
}
