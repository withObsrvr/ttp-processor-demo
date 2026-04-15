package main

import (
	"context"
	"database/sql"
	"fmt"
)

func (r *SilverColdReader) GetTransactionForDecode(ctx context.Context, txHash string, bronzeCold *ColdReader) (*DecodedTransaction, error) {
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
		return nil, fmt.Errorf("GetTransactionForDecode: %w", err)
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
			return nil, err
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
		return nil, err
	}
	if len(tx.Operations) == 0 {
		return nil, fmt.Errorf("%w: %s", ErrTxNotFound, txHash)
	}
	tx.OpCount = len(tx.Operations)

	r.enrichOperationArgumentsCold(ctx, tx, txHash)
	if events, err := r.GetTransactionEvents(ctx, txHash); err == nil {
		tx.Events = events
	}
	tx.Summary = GenerateTxSummary(tx.Operations, tx.Events)
	r.enrichTransactionFromBronzeCold(ctx, tx, txHash, bronzeCold)
	return tx, nil
}

func (r *SilverColdReader) enrichOperationArgumentsCold(ctx context.Context, tx *DecodedTransaction, txHash string) {
	hasSorobanOps := false
	for _, op := range tx.Operations {
		if op.IsSorobanOp && op.ArgumentsJSON == nil {
			hasSorobanOps = true
			break
		}
	}
	if !hasSorobanOps {
		return
	}
	query := fmt.Sprintf(`
		SELECT operation_index, arguments_json
		FROM %s.%s.contract_invocations_raw
		WHERE transaction_hash = ?
		ORDER BY operation_index
	`, r.catalogName, r.schemaName)
	rows, err := r.db.QueryContext(ctx, query, txHash)
	if err != nil {
		return
	}
	defer rows.Close()
	argsByIndex := map[int]string{}
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

func (r *SilverColdReader) enrichTransactionFromBronzeCold(ctx context.Context, tx *DecodedTransaction, txHash string, bronzeCold *ColdReader) {
	if bronzeCold == nil {
		return
	}
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
	if err := bronzeCold.DB().QueryRowContext(ctx, query, txHash).Scan(&sourceAccount, &accountSequence, &maxFee, &instructions, &readBytes, &writeBytes); err != nil {
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

func (r *SilverColdReader) GetContractFunctions(ctx context.Context, contractID string) ([]string, error) {
	query := fmt.Sprintf(`
		SELECT DISTINCT function_name
		FROM %s.%s.enriched_history_operations
		WHERE contract_id = ? AND function_name IS NOT NULL
		ORDER BY function_name
	`, r.catalogName, r.schemaName)
	rows, err := r.db.QueryContext(ctx, query, contractID)
	if err != nil {
		return nil, fmt.Errorf("GetContractFunctions: %w", err)
	}
	defer rows.Close()
	var functions []string
	for rows.Next() {
		var fn string
		if err := rows.Scan(&fn); err != nil {
			return nil, err
		}
		functions = append(functions, fn)
	}
	return functions, nil
}
