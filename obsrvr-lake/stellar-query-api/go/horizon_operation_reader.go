package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/stellar/go-stellar-sdk/toid"
)

var errHorizonOperationNotFound = errors.New("horizon operation not found")

type HorizonOperationReader struct {
	db         *sql.DB
	hotSchema  string
	coldSchema string
}

func NewHorizonOperationReader(reader *UnifiedDuckDBReader) *HorizonOperationReader {
	if reader == nil {
		return nil
	}
	return &HorizonOperationReader{
		db:         reader.db,
		hotSchema:  reader.hotSchema,
		coldSchema: reader.coldSchema,
	}
}

func (r *HorizonOperationReader) GetEnrichedOperationsWithCursor(ctx context.Context, filters OperationFilters) ([]EnrichedOperation, string, bool, error) {
	if r == nil || r.db == nil {
		return nil, "", false, fmt.Errorf("horizon operation reader unavailable")
	}

	limit := filters.Limit
	if limit <= 0 {
		limit = int(defaultHorizonLimit)
	}

	whereClause, args, nextArg := horizonOperationWhereClause(filters, 1)
	orderDir := "ASC"
	cursorOp := ">"
	if filters.Order == "desc" {
		orderDir = "DESC"
		cursorOp = "<"
	}

	cursorClause := ""
	if filters.Cursor != nil {
		cursorClause = fmt.Sprintf(" AND (ledger_sequence %s $%d OR (ledger_sequence = $%d AND operation_id %s $%d))",
			cursorOp, nextArg, nextArg, cursorOp, nextArg+1)
		args = append(args, filters.Cursor.LedgerSequence, filters.Cursor.OperationIndex)
		nextArg += 2
	}

	arms := make([]string, 0, 2)
	if strings.TrimSpace(r.hotSchema) != "" {
		arms = append(arms, horizonOperationArm(r.hotSchema, whereClause, 1))
	}
	if strings.TrimSpace(r.coldSchema) != "" {
		arms = append(arms, horizonOperationArm(r.coldSchema, whereClause, 2))
	}
	if len(arms) == 0 {
		return nil, "", false, fmt.Errorf("horizon operation reader requires transaction_id and operation_id columns")
	}

	query := fmt.Sprintf(`
		WITH combined AS (
			%s
		), ranked AS (
			SELECT *,
			       ROW_NUMBER() OVER (PARTITION BY operation_id ORDER BY source) AS rn
			FROM combined
			WHERE transaction_id IS NOT NULL
			  AND operation_id IS NOT NULL
			  %s
		)
		SELECT transaction_hash, operation_id, ledger_sequence,
		       ledger_closed_at, source_account, type, destination,
		       asset_code, asset_issuer, amount, tx_successful,
		       tx_fee_charged, is_payment_op, is_soroban_op,
		       contract_id, function_name, parameters
		FROM ranked
		WHERE rn = 1
		ORDER BY ledger_sequence %s, operation_id %s
		LIMIT $%d
	`, strings.Join(arms, "\nUNION ALL\n"), cursorClause, orderDir, orderDir, nextArg)
	args = append(args, limit+1)

	rows, err := r.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, "", false, fmt.Errorf("horizon GetEnrichedOperations: %w", err)
	}
	defer rows.Close()

	ops := make([]EnrichedOperation, 0, limit)
	for rows.Next() {
		var op EnrichedOperation
		if err := rows.Scan(&op.TransactionHash, &op.OperationID, &op.LedgerSequence,
			&op.LedgerClosedAt, &op.SourceAccount, &op.Type,
			&op.Destination, &op.AssetCode, &op.AssetIssuer, &op.Amount,
			&op.TxSuccessful, &op.TxFeeCharged, &op.IsPaymentOp, &op.IsSorobanOp,
			&op.SorobanContractID, &op.SorobanFunction, &op.SorobanArgsJSON); err != nil {
			return nil, "", false, err
		}
		op.TypeName = operationTypeName(op.Type)
		ops = append(ops, op)
	}
	if err := rows.Err(); err != nil {
		return nil, "", false, err
	}

	hasMore := len(ops) > limit
	if hasMore {
		ops = ops[:limit]
	}

	var nextCursor string
	if hasMore && len(ops) > 0 {
		last := ops[len(ops)-1]
		nextCursor = OperationCursor{
			LedgerSequence: last.LedgerSequence,
			OperationIndex: last.OperationID,
			Order:          filters.Order,
		}.Encode()
	}

	return ops, nextCursor, hasMore, nil
}

func (r *HorizonOperationReader) GetOperationByID(ctx context.Context, operationID int64) (*EnrichedOperation, error) {
	if r == nil || r.db == nil {
		return nil, fmt.Errorf("horizon operation reader unavailable")
	}

	whereClause := "WHERE operation_id = $1"
	args := []interface{}{operationID}
	if ledgerSequence := int64(toid.Parse(operationID).LedgerSequence); ledgerSequence > 0 {
		whereClause = "WHERE operation_id = $1 AND ledger_sequence = $2"
		args = append(args, ledgerSequence)
	}

	arms := make([]string, 0, 2)
	if strings.TrimSpace(r.hotSchema) != "" {
		arms = append(arms, horizonOperationArm(r.hotSchema, whereClause, 1))
	}
	if strings.TrimSpace(r.coldSchema) != "" {
		arms = append(arms, horizonOperationArm(r.coldSchema, whereClause, 2))
	}
	if len(arms) == 0 {
		return nil, fmt.Errorf("horizon operation reader requires transaction_id and operation_id columns")
	}

	query := fmt.Sprintf(`
		WITH combined AS (
			%s
		), ranked AS (
			SELECT *,
			       ROW_NUMBER() OVER (PARTITION BY operation_id ORDER BY source) AS rn
			FROM combined
			WHERE transaction_id IS NOT NULL
			  AND operation_id IS NOT NULL
		)
		SELECT transaction_hash, operation_id, ledger_sequence,
		       ledger_closed_at, source_account, type, destination,
		       asset_code, asset_issuer, amount, tx_successful,
		       tx_fee_charged, is_payment_op, is_soroban_op,
		       contract_id, function_name, parameters
		FROM ranked
		WHERE rn = 1
		LIMIT 1
	`, strings.Join(arms, "\nUNION ALL\n"))

	var op EnrichedOperation
	err := r.db.QueryRowContext(ctx, query, args...).Scan(
		&op.TransactionHash, &op.OperationID, &op.LedgerSequence,
		&op.LedgerClosedAt, &op.SourceAccount, &op.Type,
		&op.Destination, &op.AssetCode, &op.AssetIssuer, &op.Amount,
		&op.TxSuccessful, &op.TxFeeCharged, &op.IsPaymentOp, &op.IsSorobanOp,
		&op.SorobanContractID, &op.SorobanFunction, &op.SorobanArgsJSON,
	)
	if err == sql.ErrNoRows {
		return nil, errHorizonOperationNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("horizon GetOperationByID: %w", err)
	}
	op.TypeName = operationTypeName(op.Type)
	return &op, nil
}

func (r *HorizonOperationReader) schemaHasOperationTOIDs(ctx context.Context, schema string) bool {
	if strings.TrimSpace(schema) == "" {
		return false
	}
	rows, err := r.db.QueryContext(ctx, fmt.Sprintf(
		"SELECT transaction_id, operation_id FROM %s.enriched_history_operations LIMIT 0", schema))
	if err != nil {
		return false
	}
	_ = rows.Close()
	return true
}

func horizonOperationArm(schema, whereClause string, source int) string {
	return fmt.Sprintf(`
			SELECT transaction_hash, transaction_id, operation_id, operation_index, ledger_sequence,
			       ledger_closed_at, source_account, type, destination,
			       asset_code, asset_issuer, amount, tx_successful,
			       tx_fee_charged, is_payment_op, is_soroban_op,
			       contract_id, function_name, parameters,
			       %d as source
			FROM %s.enriched_history_operations
			%s`, source, schema, whereClause)
}

func horizonOperationWhereClause(filters OperationFilters, startArg int) (string, []interface{}, int) {
	var conditions []string
	args := []interface{}{}
	arg := startArg

	if filters.AccountID != "" {
		conditions = append(conditions, fmt.Sprintf(`(
			source_account = $%d OR destination = $%d OR from_account = $%d OR
			to_address = $%d OR address = $%d OR into_account = $%d
		)`, arg, arg+1, arg+2, arg+3, arg+4, arg+5))
		for i := 0; i < 6; i++ {
			args = append(args, filters.AccountID)
		}
		arg += 6
	}

	if filters.TxHash != "" {
		conditions = append(conditions, fmt.Sprintf("transaction_hash = $%d", arg))
		args = append(args, filters.TxHash)
		arg++
	}
	if filters.StartLedger > 0 {
		conditions = append(conditions, fmt.Sprintf("ledger_sequence >= $%d", arg))
		args = append(args, filters.StartLedger)
		arg++
	}
	if filters.EndLedger > 0 {
		conditions = append(conditions, fmt.Sprintf("ledger_sequence <= $%d", arg))
		args = append(args, filters.EndLedger)
		arg++
	}
	if filters.PaymentsOnly {
		conditions = append(conditions, "is_payment_op = true")
	}
	if filters.SorobanOnly {
		conditions = append(conditions, "is_soroban_op = true")
	}
	if filters.SorobanFunction != "" {
		conditions = append(conditions, fmt.Sprintf("function_name = $%d", arg))
		args = append(args, filters.SorobanFunction)
		arg++
	}
	if filters.ContractID != "" {
		conditions = append(conditions, fmt.Sprintf("contract_id = $%d", arg))
		args = append(args, filters.ContractID)
		arg++
	}

	if len(conditions) == 0 {
		return "WHERE 1=1", args, arg
	}
	return "WHERE " + strings.Join(conditions, " AND "), args, arg
}
