package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/stellar/go-stellar-sdk/toid"
)

var errHorizonOperationNotFound = errors.New("horizon operation not found")

type HorizonOperationReader struct {
	db           *sql.DB
	hotSchema    string
	coldSchema   string
	serving      *SilverHotReader
	schemaCapsMu sync.Mutex
	schemaCaps   map[string]bool
}

func NewHorizonOperationReader(reader *UnifiedDuckDBReader, serving ...*SilverHotReader) *HorizonOperationReader {
	if reader == nil {
		return nil
	}
	var servingReader *SilverHotReader
	if len(serving) > 0 {
		servingReader = serving[0]
	}
	return &HorizonOperationReader{
		db:         reader.db,
		hotSchema:  reader.hotSchema,
		coldSchema: reader.coldSchema,
		serving:    servingReader,
		schemaCaps: make(map[string]bool),
	}
}

func (r *HorizonOperationReader) GetEnrichedOperationsWithCursor(ctx context.Context, filters OperationFilters) ([]EnrichedOperation, string, bool, error) {
	if r == nil {
		return nil, "", false, fmt.Errorf("horizon operation reader unavailable")
	}
	if r.serving != nil {
		ops, next, hasMore, covered, err := r.serving.GetServingAccountOperations(ctx, filters)
		if err != nil {
			return nil, "", false, err
		}
		if covered {
			return ops, next, hasMore, nil
		}
	}
	if r.db == nil {
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
	if r.schemaHasOperationTOIDsCached(r.hotSchema) {
		arms = append(arms, horizonOperationArm(r.hotSchema, whereClause, 1))
	}
	if r.schemaHasOperationTOIDsCached(r.coldSchema) {
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

	ledgerSequence := int64(toid.Parse(operationID).LedgerSequence)
	attempted := false
	if r.schemaHasOperationTOIDsCached(r.hotSchema) {
		attempted = true
		op, err := r.getOperationByIDFromSchema(ctx, r.hotSchema, operationID, ledgerSequence)
		if err == nil {
			return op, nil
		}
		if !errors.Is(err, errHorizonOperationNotFound) {
			return nil, err
		}
	}
	if r.schemaHasOperationTOIDsCached(r.coldSchema) {
		attempted = true
		op, err := r.getOperationByIDFromSchema(ctx, r.coldSchema, operationID, ledgerSequence)
		if err == nil {
			return op, nil
		}
		if !errors.Is(err, errHorizonOperationNotFound) {
			return nil, err
		}
	}
	if !attempted {
		return nil, fmt.Errorf("horizon operation reader requires transaction_id and operation_id columns")
	}
	return nil, errHorizonOperationNotFound
}

func (r *HorizonOperationReader) getOperationByIDFromSchema(ctx context.Context, schema string, operationID, ledgerSequence int64) (*EnrichedOperation, error) {
	whereClause := "operation_id = $1"
	args := []interface{}{operationID}
	if ledgerSequence > 0 {
		whereClause = "operation_id = $1 AND ledger_sequence = $2"
		args = append(args, ledgerSequence)
	}

	query := fmt.Sprintf(`
		WITH ops AS (
			SELECT transaction_hash, transaction_id, %s AS operation_id, operation_index, ledger_sequence,
			       ledger_closed_at, source_account, type, destination,
			       asset_code, asset_issuer, amount, tx_successful,
			       tx_fee_charged, is_payment_op, is_soroban_op,
			       contract_id, function_name, parameters
			FROM %s.enriched_history_operations
		)
		SELECT transaction_hash, operation_id, ledger_sequence,
		       ledger_closed_at, source_account, type, destination,
		       asset_code, asset_issuer, amount, tx_successful,
		       tx_fee_charged, is_payment_op, is_soroban_op,
		       contract_id, function_name, parameters
		FROM ops
		WHERE %s
		  AND transaction_id IS NOT NULL
		  AND operation_id IS NOT NULL
		LIMIT 1
	`, horizonOperationIDExpr(), schema, whereClause)

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
		return nil, fmt.Errorf("horizon GetOperationByID %s: %w", schema, err)
	}
	op.TypeName = operationTypeName(op.Type)
	return &op, nil
}

func (r *HorizonOperationReader) schemaHasOperationTOIDsCached(schema string) bool {
	schema = strings.TrimSpace(schema)
	if schema == "" {
		return false
	}

	r.schemaCapsMu.Lock()
	if r.schemaCaps == nil {
		r.schemaCaps = make(map[string]bool)
	}
	available, ok := r.schemaCaps[schema]
	r.schemaCapsMu.Unlock()
	if ok {
		return available
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	available = r.schemaHasOperationTOIDs(ctx, schema)

	r.schemaCapsMu.Lock()
	r.schemaCaps[schema] = available
	r.schemaCapsMu.Unlock()
	return available
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
			SELECT transaction_hash, transaction_id, %s AS operation_id, operation_index, ledger_sequence,
			       ledger_closed_at, source_account, type, destination,
			       asset_code, asset_issuer, amount, tx_successful,
			       tx_fee_charged, is_payment_op, is_soroban_op,
			       contract_id, function_name, parameters,
			       %d as source
			FROM %s.enriched_history_operations
			%s`, horizonOperationIDExpr(), source, schema, whereClause)
}

func horizonOperationIDExpr() string {
	return `COALESCE(operation_id, CASE
		WHEN transaction_id IS NULL THEN NULL
		ELSE (transaction_id | ((operation_index + 1)::BIGINT & 4095))
	END)`
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
