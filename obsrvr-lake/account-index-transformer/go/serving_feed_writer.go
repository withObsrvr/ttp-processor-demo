package main

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

type ServingFeedWriter struct {
	db     *sql.DB
	config ServingFeedConfig
}

func NewServingFeedWriter(db *sql.DB, config ServingFeedConfig) *ServingFeedWriter {
	return &ServingFeedWriter{db: db, config: config}
}

// Column lists for the by-account feed tables. These MUST stay in lockstep with
// serving-projection-processor/go/schema/serving_schema.sql (sv_transactions_by_account
// and sv_operations_by_account): the serving schema init or serving-cold-backfill
// usually creates these tables first, and CREATE TABLE IF NOT EXISTS will not
// reconcile drift — a mismatched column name fails at prepare time and stalls the feed.
var servingFeedTxInsertColumns = []string{
	"account_id", "source_mask", "toid", "tx_hash", "ledger_sequence", "closed_at",
	"successful", "activity_type", "source_account", "destination_account", "primary_contract_id",
	"operation_count", "fee_charged_stroops", "memo_type", "memo_value",
}

var servingFeedOpInsertColumns = []string{
	"account_id", "source_mask", "tx_toid", "operation_toid", "tx_hash", "ledger_sequence", "closed_at",
	"op_index", "type_code", "type_name", "source_account", "destination_account", "asset_key",
	"amount_stroops", "contract_id", "function_name", "successful", "is_payment_op", "is_soroban_op",
}

func placeholders(n int) string {
	parts := make([]string, n)
	for i := range parts {
		parts[i] = fmt.Sprintf("$%d", i+1)
	}
	return strings.Join(parts, ",")
}

func (w *ServingFeedWriter) Ensure(ctx context.Context) error {
	for _, stmt := range []string{
		`CREATE SCHEMA IF NOT EXISTS ` + ident(w.config.Schema),
		`CREATE SCHEMA IF NOT EXISTS ops`,
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
			table_name text primary key,
			status text not null,
			complete_from bigint not null,
			complete_thru bigint not null,
			updated_at timestamptz not null
		)`, w.config.WatermarkTable),
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
			pipeline text not null,
			source_table text not null,
			checkpoint bigint not null,
			updated_at timestamptz not null,
			primary key (pipeline, source_table)
		)`, w.config.ConsumerTable),
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
			account_id text not null,
			toid bigint not null,
			tx_hash text not null,
			ledger_sequence bigint not null,
			closed_at timestamptz not null,
			successful boolean not null,
			activity_type text not null,
			source_account text,
			destination_account text,
			primary_contract_id text,
			operation_count integer,
			fee_charged_stroops bigint,
			memo_type text,
			memo_value text,
			source_mask smallint not null default 1,
			materialized_at timestamptz not null default now(),
			primary key (account_id, toid)
		)`, w.txTable()),
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
			account_id text not null,
			operation_toid bigint not null,
			tx_toid bigint not null,
			tx_hash text not null,
			ledger_sequence bigint not null,
			closed_at timestamptz not null,
			op_index integer not null,
			type_code integer,
			type_name text,
			source_account text,
			destination_account text,
			asset_key text,
			amount_stroops bigint,
			contract_id text,
			function_name text,
			successful boolean not null,
			is_payment_op boolean not null default false,
			is_soroban_op boolean not null default false,
			source_mask smallint not null default 1,
			materialized_at timestamptz not null default now(),
			primary key (account_id, operation_toid)
		)`, w.opsTable()),
		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS %s ON %s (account_id, toid DESC)`, ident(w.config.TransactionsTable+"_page_idx"), w.txTable()),
		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS %s ON %s (account_id, operation_toid DESC)`, ident(w.config.OperationsTable+"_page_idx"), w.opsTable()),
		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS %s ON %s (operation_toid)`, ident(w.config.OperationsTable+"_operation_idx"), w.opsTable()),
		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS %s ON %s (account_id, operation_toid DESC) WHERE is_payment_op = true`, ident(w.config.OperationsTable+"_payment_page_idx"), w.opsTable()),
	} {
		if _, err := w.db.ExecContext(ctx, stmt); err != nil {
			return err
		}
	}
	return nil
}

func (w *ServingFeedWriter) LoadCheckpoint(ctx context.Context, fallback int64) (int64, error) {
	if err := w.Ensure(ctx); err != nil {
		return 0, err
	}
	var checkpoint sql.NullInt64
	err := w.db.QueryRowContext(ctx, fmt.Sprintf(
		`SELECT checkpoint FROM %s WHERE pipeline = $1 AND source_table = $2`,
		w.config.ConsumerTable,
	), w.config.Pipeline, w.txTableName()).Scan(&checkpoint)
	if err != nil && err != sql.ErrNoRows {
		return 0, err
	}
	if checkpoint.Valid {
		return checkpoint.Int64, nil
	}

	var watermark sql.NullInt64
	err = w.db.QueryRowContext(ctx, fmt.Sprintf(
		`SELECT complete_thru FROM %s WHERE table_name = $1 AND status = 'complete'`,
		w.config.WatermarkTable,
	), w.txTableName()).Scan(&watermark)
	if err != nil && err != sql.ErrNoRows {
		return 0, err
	}
	if watermark.Valid {
		return watermark.Int64, nil
	}
	return fallback, nil
}

func (w *ServingFeedWriter) Write(ctx context.Context, rows []AccountFeedRow, startLedger, endLedger int64) (int64, error) {
	if err := w.Ensure(ctx); err != nil {
		return 0, err
	}
	tx, err := w.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()

	txStmt, err := tx.PrepareContext(ctx, fmt.Sprintf(`
		INSERT INTO %s AS target (%s) VALUES (%s)
		ON CONFLICT (account_id, toid) DO UPDATE SET
			source_mask = (target.source_mask | EXCLUDED.source_mask)::SMALLINT,
			successful = EXCLUDED.successful,
			activity_type = EXCLUDED.activity_type,
			source_account = COALESCE(target.source_account, EXCLUDED.source_account),
			destination_account = COALESCE(target.destination_account, EXCLUDED.destination_account),
			primary_contract_id = COALESCE(target.primary_contract_id, EXCLUDED.primary_contract_id),
			operation_count = EXCLUDED.operation_count,
			fee_charged_stroops = EXCLUDED.fee_charged_stroops,
			memo_type = EXCLUDED.memo_type,
			memo_value = EXCLUDED.memo_value
	`, w.txTable(), strings.Join(servingFeedTxInsertColumns, ", "), placeholders(len(servingFeedTxInsertColumns))))
	if err != nil {
		return 0, err
	}
	defer txStmt.Close()

	opStmt, err := tx.PrepareContext(ctx, fmt.Sprintf(`
		INSERT INTO %s AS target (%s) VALUES (%s)
		ON CONFLICT (account_id, operation_toid) DO UPDATE SET
			source_mask = (target.source_mask | EXCLUDED.source_mask)::SMALLINT,
			type_code = EXCLUDED.type_code,
			type_name = EXCLUDED.type_name,
			source_account = COALESCE(target.source_account, EXCLUDED.source_account),
			destination_account = COALESCE(target.destination_account, EXCLUDED.destination_account),
			asset_key = COALESCE(target.asset_key, EXCLUDED.asset_key),
			amount_stroops = EXCLUDED.amount_stroops,
			contract_id = COALESCE(target.contract_id, EXCLUDED.contract_id),
			function_name = COALESCE(target.function_name, EXCLUDED.function_name),
			successful = EXCLUDED.successful,
			is_payment_op = EXCLUDED.is_payment_op,
			is_soroban_op = EXCLUDED.is_soroban_op
	`, w.opsTable(), strings.Join(servingFeedOpInsertColumns, ", "), placeholders(len(servingFeedOpInsertColumns))))
	if err != nil {
		return 0, err
	}
	defer opStmt.Close()

	var written int64
	for _, row := range rows {
		if _, err := txStmt.ExecContext(ctx,
			row.AccountID, row.SourceMask, row.TOID, row.TxHash, row.LedgerSequence, row.LedgerClosedAt,
			row.Successful, row.ActivityType, nullableString(row.SourceAccount), nullableString(row.DestinationAccount), nullableString(row.PrimaryContractID),
			nullableInt(row.OperationCount), nullableInt(row.FeeCharged), nullableString(row.MemoType), nullableString(row.MemoValue),
		); err != nil {
			return 0, err
		}
		if _, err := opStmt.ExecContext(ctx,
			row.AccountID, row.SourceMask, row.TOID, row.OperationTOID, row.TxHash, row.LedgerSequence, row.LedgerClosedAt,
			row.OperationIndex, nullableInt(row.OperationType), nullableString(row.OperationTypeName), nullableString(row.SourceAccount), nullableString(row.DestinationAccount), nullableString(row.AssetKey),
			nullableInt(row.AmountStroops), nullableString(row.ContractID), nullableString(row.FunctionName), row.Successful, row.IsPaymentOp, row.IsSorobanOp,
		); err != nil {
			return 0, err
		}
		written++
	}
	if err := w.extendWatermarks(ctx, tx, startLedger, endLedger); err != nil {
		return 0, err
	}
	if err := w.saveCheckpoint(ctx, tx, endLedger); err != nil {
		return 0, err
	}
	if err := tx.Commit(); err != nil {
		return 0, err
	}
	return written, nil
}

func (w *ServingFeedWriter) extendWatermarks(ctx context.Context, tx *sql.Tx, startLedger, endLedger int64) error {
	for _, tableName := range []string{w.txTableName(), w.opsTableName()} {
		if _, err := tx.ExecContext(ctx, fmt.Sprintf(`
			UPDATE %s
			SET complete_thru = GREATEST(complete_thru, $1), updated_at = now()
			WHERE table_name = $2 AND status = 'complete' AND complete_thru >= $3
		`, w.config.WatermarkTable), endLedger, tableName, startLedger-1); err != nil {
			return err
		}
	}
	return nil
}

func (w *ServingFeedWriter) saveCheckpoint(ctx context.Context, tx *sql.Tx, ledger int64) error {
	for _, tableName := range []string{w.txTableName(), w.opsTableName()} {
		if _, err := tx.ExecContext(ctx, fmt.Sprintf(`
			INSERT INTO %s (pipeline, source_table, checkpoint, updated_at)
			VALUES ($1, $2, $3, now())
			ON CONFLICT (pipeline, source_table) DO UPDATE SET
				checkpoint = GREATEST(%s.checkpoint, EXCLUDED.checkpoint),
				updated_at = EXCLUDED.updated_at
		`, w.config.ConsumerTable, w.config.ConsumerTable), w.config.Pipeline, tableName, ledger); err != nil {
			return err
		}
	}
	return nil
}

func (w *ServingFeedWriter) txTable() string {
	return qualifiedTable(w.config.Schema, w.config.TransactionsTable)
}

func (w *ServingFeedWriter) opsTable() string {
	return qualifiedTable(w.config.Schema, w.config.OperationsTable)
}

func (w *ServingFeedWriter) txTableName() string {
	return w.config.Schema + "." + w.config.TransactionsTable
}

func (w *ServingFeedWriter) opsTableName() string {
	return w.config.Schema + "." + w.config.OperationsTable
}

func qualifiedTable(schema, table string) string {
	return ident(schema) + "." + ident(table)
}

func ident(value string) string {
	return `"` + strings.ReplaceAll(value, `"`, `""`) + `"`
}

func nullableString(value sql.NullString) any {
	if !value.Valid {
		return nil
	}
	return value.String
}

func nullableInt(value sql.NullInt64) any {
	if !value.Valid {
		return nil
	}
	return value.Int64
}
