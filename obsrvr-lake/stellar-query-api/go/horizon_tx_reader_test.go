package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"regexp"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
)

var horizonTxColumns = []string{
	"ledger_sequence",
	"transaction_hash",
	"transaction_id",
	"source_account",
	"source_account_muxed",
	"account_sequence",
	"fee_charged",
	"max_fee",
	"successful",
	"operation_count",
	"memo_type",
	"memo",
	"created_at",
	"tx_envelope",
	"tx_result",
	"tx_meta",
	"tx_fee_meta",
	"tx_signers",
	"fee_account_muxed",
	"inner_transaction_hash",
}

func TestHorizonTransactionReaderBuildsProtocolTransaction(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	closedAt := time.Date(2026, 7, 9, 15, 4, 0, 0, time.UTC)
	mock.ExpectQuery(regexp.QuoteMeta(horizonHotTransactionQuery)).
		WithArgs("txhash").
		WillReturnRows(sqlmock.NewRows(horizonTxColumns).AddRow(
			int64(123),
			"txhash",
			int64(528280977408),
			"GACCOUNT",
			nil,
			int64(99),
			int64(100),
			int64(1000),
			true,
			int64(2),
			"text_base64",
			"hello",
			closedAt,
			"AAAA-envelope",
			"AAAA-result",
			"AAAA-meta",
			"AAAA-fee-meta",
			`["sig1","sig2"]`,
			nil,
			nil,
		))

	reader := &HorizonTransactionReader{hot: db}
	got, err := reader.GetTransactionByHash(context.Background(), "txhash")
	if err != nil {
		t.Fatalf("GetTransactionByHash: %v", err)
	}

	if got.ID != "txhash" || got.Hash != "txhash" || got.PT != "528280977408" {
		t.Fatalf("transaction identity = id:%q hash:%q pt:%q", got.ID, got.Hash, got.PT)
	}
	if got.Ledger != 123 || got.Account != "GACCOUNT" || got.AccountSequence != 99 {
		t.Fatalf("transaction account/ledger fields = %+v", got)
	}
	if got.EnvelopeXdr != "AAAA-envelope" || got.ResultXdr != "AAAA-result" || got.FeeMetaXdr != "AAAA-fee-meta" {
		t.Fatalf("xdr fields = envelope:%q result:%q fee:%q", got.EnvelopeXdr, got.ResultXdr, got.FeeMetaXdr)
	}
	if len(got.Signatures) != 2 || got.Signatures[0] != "sig1" || got.Signatures[1] != "sig2" {
		t.Fatalf("signatures = %#v", got.Signatures)
	}
	if got.MemoType != "text" || got.Memo != "hello" {
		t.Fatalf("memo = type:%q value:%q", got.MemoType, got.Memo)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestHorizonTransactionReaderRejectsMissingXDR(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	mock.ExpectQuery(regexp.QuoteMeta(horizonHotTransactionQuery)).
		WithArgs("txhash").
		WillReturnRows(sqlmock.NewRows(horizonTxColumns).AddRow(
			int64(123),
			"txhash",
			int64(528280977408),
			"GACCOUNT",
			nil,
			int64(99),
			int64(100),
			int64(1000),
			true,
			int64(2),
			"none",
			nil,
			time.Date(2026, 7, 9, 15, 4, 0, 0, time.UTC),
			"",
			"",
			"AAAA-meta",
			"",
			"[]",
			nil,
			nil,
		))

	reader := &HorizonTransactionReader{hot: db}
	_, err = reader.GetTransactionByHash(context.Background(), "txhash")
	if !errors.Is(err, errHorizonTransactionXDRUnavailable) {
		t.Fatalf("error = %v, want errHorizonTransactionXDRUnavailable", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestHorizonTransactionReaderUsesIndexLedgerHintForColdLookup(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	coldTable := "bronze.transactions_row_v2"
	query := fmt.Sprintf(horizonColdTransactionQueryWithLedger, coldTable)
	closedAt := time.Date(2026, 7, 9, 15, 4, 0, 0, time.UTC)
	mock.ExpectQuery(regexp.QuoteMeta(query)).
		WithArgs(int64(456), "txhash").
		WillReturnRows(sqlmock.NewRows(horizonTxColumns).AddRow(
			int64(456),
			"txhash",
			int64(1958505086976),
			"GACCOUNT",
			nil,
			int64(99),
			int64(100),
			int64(1000),
			true,
			int64(2),
			"none",
			nil,
			closedAt,
			"AAAA-envelope",
			"AAAA-result",
			"AAAA-meta",
			"AAAA-fee-meta",
			`["sig1"]`,
			nil,
			nil,
		))

	index := &fakeTransactionLocationLookup{loc: &TxLocation{LedgerSequence: 456}}
	reader := &HorizonTransactionReader{cold: db, coldTable: coldTable, index: index}
	got, err := reader.GetTransactionByHash(context.Background(), "txhash")
	if err != nil {
		t.Fatalf("GetTransactionByHash: %v", err)
	}

	if got.Ledger != 456 || got.Hash != "txhash" {
		t.Fatalf("transaction = ledger:%d hash:%q", got.Ledger, got.Hash)
	}
	if index.calls != 1 || index.hash != "txhash" {
		t.Fatalf("index lookup calls = %d hash = %q", index.calls, index.hash)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestHorizonTransactionReaderUsesProvidedLedgerHintForColdLookup(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	coldTable := "bronze.transactions_row_v2"
	query := fmt.Sprintf(horizonColdTransactionQueryWithLedger, coldTable)
	closedAt := time.Date(2026, 7, 9, 15, 4, 0, 0, time.UTC)
	mock.ExpectQuery(regexp.QuoteMeta(query)).
		WithArgs(int64(789), "txhash").
		WillReturnRows(sqlmock.NewRows(horizonTxColumns).AddRow(
			int64(789),
			"txhash",
			int64(3388729196544),
			"GACCOUNT",
			nil,
			int64(99),
			int64(100),
			int64(1000),
			true,
			int64(2),
			"none",
			nil,
			closedAt,
			"AAAA-envelope",
			"AAAA-result",
			"AAAA-meta",
			"AAAA-fee-meta",
			`["sig1"]`,
			nil,
			nil,
		))

	index := &fakeTransactionLocationLookup{err: errors.New("index should not be used")}
	reader := &HorizonTransactionReader{cold: db, coldTable: coldTable, index: index}
	got, err := reader.GetTransactionByHashAtLedger(context.Background(), "txhash", 789)
	if err != nil {
		t.Fatalf("GetTransactionByHashAtLedger: %v", err)
	}

	if got.Ledger != 789 || got.Hash != "txhash" {
		t.Fatalf("transaction = ledger:%d hash:%q", got.Ledger, got.Hash)
	}
	if index.calls != 0 {
		t.Fatalf("index lookup calls = %d, want 0", index.calls)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestHorizonTransactionReaderUsesProvidedLedgerHintForHotLookup(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	closedAt := time.Date(2026, 7, 9, 15, 4, 0, 0, time.UTC)
	mock.ExpectQuery(regexp.QuoteMeta(horizonHotTransactionQueryWithLedger)).
		WithArgs(int64(789), "txhash").
		WillReturnRows(sqlmock.NewRows(horizonTxColumns).AddRow(
			int64(789),
			"txhash",
			int64(3388729196544),
			"GACCOUNT",
			nil,
			int64(99),
			int64(100),
			int64(1000),
			true,
			int64(2),
			"none",
			nil,
			closedAt,
			"AAAA-envelope",
			"AAAA-result",
			"AAAA-meta",
			"AAAA-fee-meta",
			`["sig1"]`,
			nil,
			nil,
		))

	reader := &HorizonTransactionReader{hot: db}
	got, err := reader.GetTransactionByHashAtLedger(context.Background(), "txhash", 789)
	if err != nil {
		t.Fatalf("GetTransactionByHashAtLedger: %v", err)
	}

	if got.Ledger != 789 || got.Hash != "txhash" {
		t.Fatalf("transaction = ledger:%d hash:%q", got.Ledger, got.Hash)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestHorizonTransactionReaderUsesTransactionIDAndLedgerForHotLookup(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	closedAt := time.Date(2026, 7, 9, 15, 4, 0, 0, time.UTC)
	mock.ExpectQuery(regexp.QuoteMeta(horizonHotTransactionIDQueryWithLedger)).
		WithArgs(int64(789), int64(3388729196544)).
		WillReturnRows(sqlmock.NewRows(horizonTxColumns).AddRow(
			int64(789),
			"txhash",
			int64(3388729196544),
			"GACCOUNT",
			nil,
			int64(99),
			int64(100),
			int64(1000),
			true,
			int64(2),
			"none",
			nil,
			closedAt,
			"AAAA-envelope",
			"AAAA-result",
			"AAAA-meta",
			"AAAA-fee-meta",
			`["sig1"]`,
			nil,
			nil,
		))

	reader := &HorizonTransactionReader{hot: db}
	got, err := reader.GetTransactionByIDAtLedger(context.Background(), 3388729196544, 789)
	if err != nil {
		t.Fatalf("GetTransactionByIDAtLedger: %v", err)
	}

	if got.PT != "3388729196544" || got.Hash != "txhash" {
		t.Fatalf("transaction = pt:%q hash:%q", got.PT, got.Hash)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestHorizonTransactionReaderUsesServingTransactionIDLookupFirst(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	closedAt := time.Date(2026, 7, 9, 15, 4, 0, 0, time.UTC)
	mock.ExpectQuery(regexp.QuoteMeta(horizonServingTransactionIDQueryWithLedger)).
		WithArgs(int64(789), int64(3388729196544)).
		WillReturnRows(sqlmock.NewRows(horizonTxColumns).AddRow(
			int64(789),
			"txhash",
			int64(3388729196544),
			"GACCOUNT",
			nil,
			int64(99),
			int64(100),
			int64(1000),
			true,
			int64(2),
			"none",
			nil,
			closedAt,
			"AAAA-envelope",
			"AAAA-result",
			"AAAA-meta",
			"AAAA-fee-meta",
			`["sig1"]`,
			nil,
			nil,
		))

	reader := &HorizonTransactionReader{serving: db}
	if !reader.Available() {
		t.Fatalf("serving-only reader should be available")
	}
	got, err := reader.GetTransactionByIDAtLedger(context.Background(), 3388729196544, 789)
	if err != nil {
		t.Fatalf("GetTransactionByIDAtLedger: %v", err)
	}

	if got.PT != "3388729196544" || got.Hash != "txhash" || got.EnvelopeXdr != "AAAA-envelope" {
		t.Fatalf("transaction = pt:%q hash:%q envelope:%q", got.PT, got.Hash, got.EnvelopeXdr)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestHorizonTransactionReaderReturnsServingXDRUnavailableWithoutFallback(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	mock.ExpectQuery(regexp.QuoteMeta(horizonServingTransactionIDQueryWithLedger)).
		WithArgs(int64(789), int64(3388729196544)).
		WillReturnRows(sqlmock.NewRows(horizonTxColumns).AddRow(
			int64(789),
			"txhash",
			int64(3388729196544),
			"GACCOUNT",
			nil,
			int64(99),
			int64(100),
			int64(1000),
			true,
			int64(2),
			"none",
			nil,
			time.Date(2026, 7, 9, 15, 4, 0, 0, time.UTC),
			"",
			"",
			"AAAA-meta",
			"",
			"[]",
			nil,
			nil,
		))

	reader := &HorizonTransactionReader{serving: db}
	_, err = reader.GetTransactionByIDAtLedger(context.Background(), 3388729196544, 789)
	if !errors.Is(err, errHorizonTransactionXDRUnavailable) {
		t.Fatalf("error = %v, want errHorizonTransactionXDRUnavailable", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestHorizonTransactionReaderFallsBackWhenServingResourceSchemaMissing(t *testing.T) {
	servingDB, servingMock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New serving: %v", err)
	}
	defer servingDB.Close()
	hotDB, hotMock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New hot: %v", err)
	}
	defer hotDB.Close()

	closedAt := time.Date(2026, 7, 9, 15, 4, 0, 0, time.UTC)
	servingMock.ExpectQuery(regexp.QuoteMeta(horizonServingTransactionIDQueryWithLedger)).
		WithArgs(int64(789), int64(3388729196544)).
		WillReturnError(fmt.Errorf(`ERROR: column "tx_envelope" does not exist`))
	hotMock.ExpectQuery(regexp.QuoteMeta(horizonHotTransactionIDQueryWithLedger)).
		WithArgs(int64(789), int64(3388729196544)).
		WillReturnRows(sqlmock.NewRows(horizonTxColumns).AddRow(
			int64(789),
			"txhash",
			int64(3388729196544),
			"GACCOUNT",
			nil,
			int64(99),
			int64(100),
			int64(1000),
			true,
			int64(2),
			"none",
			nil,
			closedAt,
			"AAAA-envelope",
			"AAAA-result",
			"AAAA-meta",
			"AAAA-fee-meta",
			`["sig1"]`,
			nil,
			nil,
		))

	reader := &HorizonTransactionReader{serving: servingDB, hot: hotDB}
	got, err := reader.GetTransactionByIDAtLedger(context.Background(), 3388729196544, 789)
	if err != nil {
		t.Fatalf("GetTransactionByIDAtLedger: %v", err)
	}
	if got.Hash != "txhash" {
		t.Fatalf("hash = %q", got.Hash)
	}
	if err := servingMock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet serving expectations: %v", err)
	}
	if err := hotMock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet hot expectations: %v", err)
	}
}

func TestHorizonTransactionReaderFallsBackWhenIndexLookupFails(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	coldTable := "bronze.transactions_row_v2"
	query := fmt.Sprintf(horizonColdTransactionQuery, coldTable)
	closedAt := time.Date(2026, 7, 9, 15, 4, 0, 0, time.UTC)
	mock.ExpectQuery(regexp.QuoteMeta(query)).
		WithArgs("txhash").
		WillReturnRows(sqlmock.NewRows(horizonTxColumns).AddRow(
			int64(456),
			"txhash",
			int64(1958505086976),
			"GACCOUNT",
			nil,
			int64(99),
			int64(100),
			int64(1000),
			true,
			int64(2),
			"none",
			nil,
			closedAt,
			"AAAA-envelope",
			"AAAA-result",
			"AAAA-meta",
			"AAAA-fee-meta",
			`["sig1"]`,
			nil,
			nil,
		))

	index := &fakeTransactionLocationLookup{err: errors.New("index unavailable")}
	reader := &HorizonTransactionReader{cold: db, coldTable: coldTable, index: index}
	got, err := reader.GetTransactionByHash(context.Background(), "txhash")
	if err != nil {
		t.Fatalf("GetTransactionByHash: %v", err)
	}

	if got.Ledger != 456 || got.Hash != "txhash" {
		t.Fatalf("transaction = ledger:%d hash:%q", got.Ledger, got.Hash)
	}
	if index.calls != 1 || index.hash != "txhash" {
		t.Fatalf("index lookup calls = %d hash = %q", index.calls, index.hash)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestParseHorizonSignatures(t *testing.T) {
	tests := []struct {
		name string
		raw  sql.NullString
		want []string
	}{
		{name: "json", raw: sql.NullString{String: `["sig1","sig2"]`, Valid: true}, want: []string{"sig1", "sig2"}},
		{name: "postgres array", raw: sql.NullString{String: `{sig1,sig2}`, Valid: true}, want: []string{"sig1", "sig2"}},
		{name: "empty", raw: sql.NullString{String: `[]`, Valid: true}, want: nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := parseHorizonSignatures(tt.raw)
			if len(got) != len(tt.want) {
				t.Fatalf("len = %d, want %d (%#v)", len(got), len(tt.want), got)
			}
			for i := range got {
				if got[i] != tt.want[i] {
					t.Fatalf("got[%d] = %q, want %q", i, got[i], tt.want[i])
				}
			}
		})
	}
}

type fakeTransactionLocationLookup struct {
	loc   *TxLocation
	err   error
	calls int
	hash  string
}

func (f *fakeTransactionLocationLookup) LookupTransactionHash(ctx context.Context, txHash string) (*TxLocation, error) {
	f.calls++
	f.hash = txHash
	return f.loc, f.err
}
