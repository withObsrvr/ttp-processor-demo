package main

import (
	"context"
	"database/sql"
	"regexp"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/stellar/go-stellar-sdk/toid"
)

var horizonOperationColumns = []string{
	"transaction_hash", "operation_id", "ledger_sequence",
	"ledger_closed_at", "source_account", "type", "destination",
	"asset_code", "asset_issuer", "amount", "tx_successful",
	"tx_fee_charged", "is_payment_op", "is_soroban_op",
	"contract_id", "function_name", "parameters",
}

func TestHorizonOperationReaderGetOperationByIDUsesHotFirst(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	operationID := toid.New(123, 1, 1).ToInt64()
	reader := &HorizonOperationReader{
		db:         db,
		hotSchema:  "hot",
		coldSchema: "cold",
		schemaCaps: map[string]bool{"hot": true, "cold": true},
	}

	mock.ExpectQuery(regexp.QuoteMeta("FROM hot.enriched_history_operations")).
		WithArgs(operationID, int64(123)).
		WillReturnRows(sqlmock.NewRows(horizonOperationColumns).AddRow(
			"txhash", operationID, int64(123), "2026-07-10T12:00:00Z", "GA", int32(1),
			"GB", "USD", "GISSUER", "1.0000000", true, int64(100), true, false,
			nil, nil, nil,
		))

	op, err := reader.GetOperationByID(context.Background(), operationID)
	if err != nil {
		t.Fatalf("GetOperationByID: %v", err)
	}
	if op.OperationID != operationID || op.TransactionHash != "txhash" || op.LedgerSequence != 123 {
		t.Fatalf("operation = %+v", op)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestHorizonOperationReaderGetOperationByIDFallsBackToCold(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	operationID := toid.New(3177525, 1, 1).ToInt64()
	reader := &HorizonOperationReader{
		db:         db,
		hotSchema:  "hot",
		coldSchema: "cold",
		schemaCaps: map[string]bool{"hot": true, "cold": true},
	}

	mock.ExpectQuery(regexp.QuoteMeta("FROM hot.enriched_history_operations")).
		WithArgs(operationID, int64(3177525)).
		WillReturnRows(sqlmock.NewRows(horizonOperationColumns))
	mock.ExpectQuery(regexp.QuoteMeta("FROM cold.enriched_history_operations")).
		WithArgs(operationID, int64(3177525)).
		WillReturnRows(sqlmock.NewRows(horizonOperationColumns).AddRow(
			"coldtx", operationID, int64(3177525), "2026-06-19T20:29:57Z", "GA", int32(24),
			nil, nil, nil, nil, true, int64(100), false, true,
			"CCONTRACT", "transfer", "[]",
		))

	op, err := reader.GetOperationByID(context.Background(), operationID)
	if err != nil {
		t.Fatalf("GetOperationByID: %v", err)
	}
	if op.OperationID != operationID || op.TransactionHash != "coldtx" || !op.IsSorobanOp {
		t.Fatalf("operation = %+v", op)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestHorizonOperationReaderComputesOperationIDFromTransactionID(t *testing.T) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	schema := `(
		transaction_hash VARCHAR,
		transaction_id BIGINT,
		operation_id BIGINT,
		operation_index INTEGER,
		ledger_sequence BIGINT,
		ledger_closed_at TIMESTAMP,
		source_account VARCHAR,
		type INTEGER,
		destination VARCHAR,
		asset_code VARCHAR,
		asset_issuer VARCHAR,
		amount VARCHAR,
		tx_successful BOOLEAN,
		tx_fee_charged BIGINT,
		is_payment_op BOOLEAN,
		is_soroban_op BOOLEAN,
		contract_id VARCHAR,
		function_name VARCHAR,
		parameters VARCHAR
	)`
	for _, stmt := range []string{
		`CREATE SCHEMA hot`,
		`CREATE SCHEMA cold`,
		`CREATE TABLE hot.enriched_history_operations ` + schema,
		`CREATE TABLE cold.enriched_history_operations ` + schema,
	} {
		if _, err := db.Exec(stmt); err != nil {
			t.Fatal(err)
		}
	}

	transactionID := toid.New(123, 2, 0).ToInt64()
	operationID := toid.New(123, 2, 1).ToInt64()
	if _, err := db.Exec(`
		INSERT INTO cold.enriched_history_operations VALUES
		('txhash', ?, NULL, 0, 123, TIMESTAMP '2026-07-10 12:00:00',
		 'GA', 1, 'GB', 'USD', 'GISSUER', '1.0000000', true, 100,
		 true, false, NULL, NULL, NULL)
	`, transactionID); err != nil {
		t.Fatal(err)
	}

	reader := &HorizonOperationReader{
		db:         db,
		hotSchema:  "hot",
		coldSchema: "cold",
		schemaCaps: make(map[string]bool),
	}
	ops, _, _, err := reader.GetEnrichedOperationsWithCursor(context.Background(), OperationFilters{
		TxHash: "txhash",
		Limit:  10,
		Order:  "asc",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(ops) != 1 || ops[0].OperationID != operationID {
		t.Fatalf("ops = %+v, want computed operation id %d", ops, operationID)
	}
}

func TestHorizonOperationReaderUsesServingAccountOperationsWhenCovered(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	operationID := toid.New(20, 1, 1).ToInt64()
	nextOperationID := toid.New(19, 2, 1).ToInt64()
	closedAt := time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC)

	mock.ExpectQuery("FROM serving.sv_watermarks").
		WillReturnRows(sqlmock.NewRows([]string{"status", "complete_from", "complete_thru"}).
			AddRow("complete", int64(1), int64(20)))
	mock.ExpectQuery("SELECT COALESCE\\(MAX\\(ledger_sequence\\), 0\\) FROM serving.sv_ledger_stats_recent").
		WillReturnRows(sqlmock.NewRows([]string{"latest"}).AddRow(int64(20)))
	mock.ExpectQuery("SELECT operation_toid").
		WithArgs("GA", int64(1), int64(20), 4).
		WillReturnRows(sqlmock.NewRows([]string{"operation_toid"}).
			AddRow(operationID).
			AddRow(nextOperationID))
	mock.ExpectQuery("SELECT DISTINCT e.operation_toid").
		WithArgs("GA", int64(1), int64(20), 4).
		WillReturnRows(sqlmock.NewRows([]string{"operation_toid"}))
	mock.ExpectQuery("WHERE operation_toid = ANY").
		WithArgs(sqlmock.AnyArg(), "GA", 2).
		WillReturnRows(sqlmock.NewRows([]string{
			"operation_toid", "tx_hash", "ledger_sequence", "closed_at", "source_account",
			"type_code", "type_name", "destination_account", "asset_key", "amount_stroops",
			"successful", "is_payment_op", "is_soroban_op", "contract_id", "function_name",
		}).AddRow(operationID, "txhash", int64(20), closedAt, "GA", int64(1), "payment", "GB", "USD:GISSUER", int64(25000000), true, true, false, nil, nil).
			AddRow(nextOperationID, "nexttx", int64(19), closedAt, "GA", int64(1), "payment", "GC", "native:XLM", int64(10000000), true, true, false, nil, nil))

	reader := NewHorizonOperationReader(
		&UnifiedDuckDBReader{db: db, hotSchema: "hot", coldSchema: "cold"},
		&SilverHotReader{db: db, network: "testnet"},
	)
	ops, next, hasMore, err := reader.GetEnrichedOperationsWithCursor(context.Background(), OperationFilters{
		AccountID:    "GA",
		PaymentsOnly: true,
		Limit:        1,
		Order:        "desc",
	})
	if err != nil {
		t.Fatalf("GetEnrichedOperationsWithCursor: %v", err)
	}
	if !hasMore || next == "" {
		t.Fatalf("pagination = hasMore %v next %q", hasMore, next)
	}
	if len(ops) != 1 {
		t.Fatalf("ops len = %d", len(ops))
	}
	op := ops[0]
	if op.OperationID != operationID || op.TransactionHash != "txhash" || !op.IsPaymentOp {
		t.Fatalf("op = %+v", op)
	}
	if op.AssetCode == nil || *op.AssetCode != "USD" || op.AssetIssuer == nil || *op.AssetIssuer != "GISSUER" {
		t.Fatalf("asset = code %v issuer %v", op.AssetCode, op.AssetIssuer)
	}
	if op.Amount == nil || *op.Amount != "2.5000000" {
		t.Fatalf("amount = %v", op.Amount)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestHorizonOperationReaderUsesSACEffectsForServingAccountPayments(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	operationID := toid.New(25, 1, 1).ToInt64()
	closedAt := time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC)

	mock.ExpectQuery("FROM serving.sv_watermarks").
		WillReturnRows(sqlmock.NewRows([]string{"status", "complete_from", "complete_thru"}).
			AddRow("complete", int64(1), int64(25)))
	mock.ExpectQuery("SELECT COALESCE\\(MAX\\(ledger_sequence\\), 0\\) FROM serving.sv_ledger_stats_recent").
		WillReturnRows(sqlmock.NewRows([]string{"latest"}).AddRow(int64(25)))
	mock.ExpectQuery("SELECT operation_toid").
		WithArgs("GA", int64(1), int64(25), 4).
		WillReturnRows(sqlmock.NewRows([]string{"operation_toid"}))
	mock.ExpectQuery("SELECT DISTINCT e.operation_toid").
		WithArgs("GA", int64(1), int64(25), 4).
		WillReturnRows(sqlmock.NewRows([]string{"operation_toid"}).AddRow(operationID))
	mock.ExpectQuery("WHERE operation_toid = ANY").
		WithArgs(sqlmock.AnyArg(), "GA", 2).
		WillReturnRows(sqlmock.NewRows([]string{
			"operation_toid", "tx_hash", "ledger_sequence", "closed_at", "source_account",
			"type_code", "type_name", "destination_account", "asset_key", "amount_stroops",
			"successful", "is_payment_op", "is_soroban_op", "contract_id", "function_name",
		}).AddRow(operationID, "txhash", int64(25), closedAt, "GSOURCE", int64(24), "invoke_host_function", nil, "USD:GISSUER", int64(25000000), true, false, true, "CCONTRACT", "transfer"))

	reader := NewHorizonOperationReader(
		&UnifiedDuckDBReader{db: db, hotSchema: "hot", coldSchema: "cold"},
		&SilverHotReader{db: db, network: "testnet"},
	)
	ops, _, hasMore, err := reader.GetEnrichedOperationsWithCursor(context.Background(), OperationFilters{
		AccountID:    "GA",
		PaymentsOnly: true,
		Limit:        1,
		Order:        "desc",
	})
	if err != nil {
		t.Fatalf("GetEnrichedOperationsWithCursor: %v", err)
	}
	if hasMore {
		t.Fatalf("hasMore = true, want false")
	}
	if len(ops) != 1 || ops[0].OperationID != operationID || ops[0].Type != 24 {
		t.Fatalf("ops = %+v", ops)
	}
	if ops[0].IsPaymentOp {
		t.Fatalf("IsPaymentOp = true, want false to prove SAC effect predicate supplied the payment match")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestHorizonOperationReaderUsesSACEffectsForServingGlobalPayments(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	operationID := toid.New(30, 1, 1).ToInt64()
	closedAt := time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC)

	mock.ExpectQuery("FROM serving.sv_watermarks").
		WillReturnRows(sqlmock.NewRows([]string{"status", "complete_thru"}).
			AddRow("complete", int64(30)))
	mock.ExpectQuery("SELECT COALESCE\\(MAX\\(ledger_sequence\\), 0\\) FROM serving.sv_ledger_stats_recent").
		WillReturnRows(sqlmock.NewRows([]string{"latest"}).AddRow(int64(30)))
	mock.ExpectQuery("(?s)FROM serving\\.sv_operations_by_account o.*serving\\.sv_effects_by_account").
		WithArgs(2).
		WillReturnRows(sqlmock.NewRows([]string{
			"operation_toid", "tx_hash", "ledger_sequence", "closed_at", "source_account",
			"type_code", "type_name", "destination_account", "asset_key", "amount_stroops",
			"successful", "is_payment_op", "is_soroban_op", "contract_id", "function_name",
		}).AddRow(operationID, "txhash", int64(30), closedAt, "GSOURCE", int64(24), "invoke_host_function", nil, "USD:GISSUER", int64(25000000), true, false, true, "CCONTRACT", "transfer"))

	reader := NewHorizonOperationReader(
		&UnifiedDuckDBReader{db: db, hotSchema: "hot", coldSchema: "cold"},
		&SilverHotReader{db: db, network: "testnet"},
	)
	ops, _, hasMore, err := reader.GetEnrichedOperationsWithCursor(context.Background(), OperationFilters{
		PaymentsOnly: true,
		Limit:        1,
		Order:        "desc",
	})
	if err != nil {
		t.Fatalf("GetEnrichedOperationsWithCursor: %v", err)
	}
	if hasMore {
		t.Fatalf("hasMore = true, want false")
	}
	if len(ops) != 1 || ops[0].OperationID != operationID || ops[0].Type != 24 {
		t.Fatalf("ops = %+v", ops)
	}
	if ops[0].IsPaymentOp {
		t.Fatalf("IsPaymentOp = true, want false to prove SAC effect predicate supplied the payment match")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestHorizonOperationReaderUsesServingGlobalOperationsWhenCovered(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	operationID := toid.New(30, 2, 1).ToInt64()
	nextOperationID := toid.New(29, 1, 1).ToInt64()
	closedAt := time.Date(2026, 7, 10, 12, 5, 0, 0, time.UTC)

	mock.ExpectQuery("FROM serving.sv_watermarks").
		WillReturnRows(sqlmock.NewRows([]string{"status", "complete_thru"}).
			AddRow("complete", int64(30)))
	mock.ExpectQuery("SELECT COALESCE\\(MAX\\(ledger_sequence\\), 0\\) FROM serving.sv_ledger_stats_recent").
		WillReturnRows(sqlmock.NewRows([]string{"latest"}).AddRow(int64(30)))
	mock.ExpectQuery("FROM serving.sv_operations_by_account").
		WithArgs(2).
		WillReturnRows(sqlmock.NewRows([]string{
			"operation_toid", "tx_hash", "ledger_sequence", "closed_at", "source_account",
			"type_code", "type_name", "destination_account", "asset_key", "amount_stroops",
			"successful", "is_payment_op", "is_soroban_op", "contract_id", "function_name",
		}).AddRow(operationID, "txhash", int64(30), closedAt, "GA", int64(24), "invoke_host_function", nil, nil, nil, true, false, true, "CCONTRACT", "invoke").
			AddRow(nextOperationID, "nexttx", int64(29), closedAt, "GB", int64(1), "payment", "GC", "native:XLM", int64(10000000), true, true, false, nil, nil))

	reader := NewHorizonOperationReader(
		&UnifiedDuckDBReader{db: db, hotSchema: "hot", coldSchema: "cold"},
		&SilverHotReader{db: db, network: "testnet"},
	)
	ops, next, hasMore, err := reader.GetEnrichedOperationsWithCursor(context.Background(), OperationFilters{
		Limit: 1,
		Order: "desc",
	})
	if err != nil {
		t.Fatalf("GetEnrichedOperationsWithCursor: %v", err)
	}
	if !hasMore || next == "" {
		t.Fatalf("pagination = hasMore %v next %q", hasMore, next)
	}
	if len(ops) != 1 {
		t.Fatalf("ops len = %d", len(ops))
	}
	if ops[0].OperationID != operationID || ops[0].TransactionHash != "txhash" || !ops[0].IsSorobanOp {
		t.Fatalf("op = %+v", ops[0])
	}
	if ops[0].SorobanContractID == nil || *ops[0].SorobanContractID != "CCONTRACT" {
		t.Fatalf("contract id = %v", ops[0].SorobanContractID)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}
