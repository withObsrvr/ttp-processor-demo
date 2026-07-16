package main

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"regexp"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/stellar/go-stellar-sdk/network"
	"github.com/stellar/go-stellar-sdk/xdr"
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

	// Realistic history-loader-backfilled row: text memos are stored
	// base64-encoded with memo_type "text_base64".
	textEnvelopeXDR := horizonTestEnvelopeXDR(xdr.Memo{Type: xdr.MemoTypeMemoText, Text: strPtr("hello")})
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
			"aGVsbG8=",
			closedAt,
			textEnvelopeXDR,
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
	if got.EnvelopeXdr != textEnvelopeXDR || got.ResultXdr != "AAAA-result" || got.FeeMetaXdr != "AAAA-fee-meta" {
		t.Fatalf("xdr fields = envelope:%q result:%q fee:%q", got.EnvelopeXdr, got.ResultXdr, got.FeeMetaXdr)
	}
	if len(got.Signatures) != 2 || got.Signatures[0] != "sig1" || got.Signatures[1] != "sig2" {
		t.Fatalf("signatures = %#v", got.Signatures)
	}
	if got.MemoType != "text" || got.Memo != "hello" || got.MemoBytes != "aGVsbG8=" {
		t.Fatalf("memo = type:%q value:%q bytes:%q", got.MemoType, got.Memo, got.MemoBytes)
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

func TestHorizonTransactionPreconditionsFromEnvelopeXDR(t *testing.T) {
	minSeq := xdr.SequenceNumber(98)
	extraSigner := xdr.MustSigner("GAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAWHF")
	envelope := xdr.TransactionEnvelope{
		Type: xdr.EnvelopeTypeEnvelopeTypeTx,
		V1: &xdr.TransactionV1Envelope{
			Tx: xdr.Transaction{
				SourceAccount: xdr.MustMuxedAddress("GAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAWHF"),
				Fee:           100,
				SeqNum:        99,
				Cond: xdr.Preconditions{
					Type: xdr.PreconditionTypePrecondV2,
					V2: &xdr.PreconditionsV2{
						TimeBounds:      &xdr.TimeBounds{MinTime: 1, MaxTime: 2},
						LedgerBounds:    &xdr.LedgerBounds{MinLedger: 3, MaxLedger: 4},
						MinSeqNum:       &minSeq,
						MinSeqAge:       5,
						MinSeqLedgerGap: 6,
						ExtraSigners:    []xdr.SignerKey{extraSigner},
					},
				},
				Memo:       xdr.Memo{Type: xdr.MemoTypeMemoNone},
				Operations: []xdr.Operation{},
			},
		},
	}
	got := horizonTransactionPreconditions(envelope)
	if got == nil {
		t.Fatal("preconditions = nil")
	}
	if got.TimeBounds == nil || got.TimeBounds.MinTime != "1" || got.TimeBounds.MaxTime != "2" {
		t.Fatalf("timebounds = %#v", got.TimeBounds)
	}
	if got.LedgerBounds == nil || got.LedgerBounds.MinLedger != 3 || got.LedgerBounds.MaxLedger != 4 {
		t.Fatalf("ledgerbounds = %#v", got.LedgerBounds)
	}
	if got.MinAccountSequence != "98" || got.MinAccountSequenceAge != "5" || got.MinAccountSequenceLedgerGap != 6 {
		t.Fatalf("min sequence fields = %#v", got)
	}
	if len(got.ExtraSigners) != 1 || got.ExtraSigners[0] != extraSigner.Address() {
		t.Fatalf("extra signers = %#v", got.ExtraSigners)
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
			horizonTestEnvelopeNoMemoXDR,
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
			horizonTestEnvelopeNoMemoXDR,
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
			horizonTestEnvelopeNoMemoXDR,
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
			horizonTestEnvelopeNoMemoXDR,
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
			horizonTestEnvelopeNoMemoXDR,
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

	if got.PT != "3388729196544" || got.Hash != "txhash" || got.EnvelopeXdr != horizonTestEnvelopeNoMemoXDR {
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
			horizonTestEnvelopeNoMemoXDR,
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
			horizonTestEnvelopeNoMemoXDR,
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
		{name: "malformed json refused", raw: sql.NullString{String: `["sig1",`, Valid: true}, want: nil},
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

func TestHorizonTransactionRowMemoVariants(t *testing.T) {
	memoHash := xdr.Hash{0x01, 0x02, 0x03}
	tests := []struct {
		name          string
		memo          xdr.Memo
		wantType      string
		wantMemo      string
		wantMemoBytes string
	}{
		{
			name:     "none",
			memo:     xdr.Memo{Type: xdr.MemoTypeMemoNone},
			wantType: "none",
		},
		{
			name:          "text served decoded with memo bytes",
			memo:          xdr.Memo{Type: xdr.MemoTypeMemoText, Text: strPtr("hello")},
			wantType:      "text",
			wantMemo:      "hello",
			wantMemoBytes: "aGVsbG8=",
		},
		{
			name:     "id",
			memo:     xdr.Memo{Type: xdr.MemoTypeMemoId, Id: uint64Ptr(42)},
			wantType: "id",
			wantMemo: "42",
		},
		{
			name:     "hash served base64 not hex",
			memo:     xdr.Memo{Type: xdr.MemoTypeMemoHash, Hash: &memoHash},
			wantType: "hash",
			wantMemo: base64.StdEncoding.EncodeToString(memoHash[:]),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			row := horizonTestRow(horizonTestEnvelopeXDR(tt.memo))
			got, err := row.toProtocol()
			if err != nil {
				t.Fatalf("toProtocol: %v", err)
			}
			if got.MemoType != tt.wantType || got.Memo != tt.wantMemo || got.MemoBytes != tt.wantMemoBytes {
				t.Fatalf("memo = type:%q value:%q bytes:%q, want type:%q value:%q bytes:%q",
					got.MemoType, got.Memo, got.MemoBytes, tt.wantType, tt.wantMemo, tt.wantMemoBytes)
			}
		})
	}
}

func TestHorizonTransactionRowRejectsUndecodableEnvelope(t *testing.T) {
	row := horizonTestRow("AAAA-not-an-envelope")
	_, err := row.toProtocol()
	if !errors.Is(err, errHorizonTransactionXDRUnavailable) {
		t.Fatalf("error = %v, want errHorizonTransactionXDRUnavailable", err)
	}
}

func TestHorizonTransactionRowFeeBump(t *testing.T) {
	const passphrase = "Test SDF Network ; September 2015"
	feeSource := "GBTHMMFWTAPFAHRGS33LKETZYJKBTNEENRN47EDZMZPT2BNCJO47GVQG"
	innerTx := xdr.Transaction{
		SourceAccount: xdr.MustMuxedAddress("GAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAWHF"),
		Fee:           100,
		SeqNum:        99,
		Cond:          xdr.Preconditions{Type: xdr.PreconditionTypePrecondNone},
		Memo:          xdr.Memo{Type: xdr.MemoTypeMemoNone},
		Operations:    []xdr.Operation{},
	}
	envelope := xdr.TransactionEnvelope{
		Type: xdr.EnvelopeTypeEnvelopeTypeTxFeeBump,
		FeeBump: &xdr.FeeBumpTransactionEnvelope{
			Tx: xdr.FeeBumpTransaction{
				FeeSource: xdr.MustMuxedAddress(feeSource),
				Fee:       400,
				InnerTx: xdr.FeeBumpTransactionInnerTx{
					Type: xdr.EnvelopeTypeEnvelopeTypeTx,
					V1: &xdr.TransactionV1Envelope{
						Tx:         innerTx,
						Signatures: []xdr.DecoratedSignature{{Signature: []byte("inner-sig")}},
					},
				},
			},
			Signatures: []xdr.DecoratedSignature{{Signature: []byte("outer-sig")}},
		},
	}
	envelopeXDR, err := xdr.MarshalBase64(envelope)
	if err != nil {
		t.Fatalf("MarshalBase64: %v", err)
	}
	outerSig := base64.StdEncoding.EncodeToString([]byte("outer-sig"))
	innerSig := base64.StdEncoding.EncodeToString([]byte("inner-sig"))
	innerHashBytes, err := network.HashTransaction(innerTx, passphrase)
	if err != nil {
		t.Fatalf("HashTransaction: %v", err)
	}
	wantInnerHash := hex.EncodeToString(innerHashBytes[:])

	t.Run("with passphrase", func(t *testing.T) {
		t.Setenv("NETWORK_PASSPHRASE", passphrase)
		got, err := horizonTestRow(envelopeXDR).toProtocol()
		if err != nil {
			t.Fatalf("toProtocol: %v", err)
		}
		if got.FeeAccount != feeSource || got.Account != "GAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAWHF" {
			t.Fatalf("fee/source accounts = fee:%q source:%q", got.FeeAccount, got.Account)
		}
		if got.MaxFee != 400 {
			t.Fatalf("max_fee = %d, want outer fee 400", got.MaxFee)
		}
		if len(got.Signatures) != 1 || got.Signatures[0] != outerSig {
			t.Fatalf("top-level signatures = %#v, want outer", got.Signatures)
		}
		if got.FeeBumpTransaction == nil || got.FeeBumpTransaction.Hash != "txhash" {
			t.Fatalf("fee_bump_transaction = %#v", got.FeeBumpTransaction)
		}
		if got.InnerTransaction == nil || got.InnerTransaction.Hash != wantInnerHash ||
			got.InnerTransaction.MaxFee != 100 ||
			len(got.InnerTransaction.Signatures) != 1 || got.InnerTransaction.Signatures[0] != innerSig {
			t.Fatalf("inner_transaction = %#v", got.InnerTransaction)
		}
	})

	t.Run("without passphrase still fixes fee account", func(t *testing.T) {
		t.Setenv("NETWORK_PASSPHRASE", "")
		got, err := horizonTestRow(envelopeXDR).toProtocol()
		if err != nil {
			t.Fatalf("toProtocol: %v", err)
		}
		if got.FeeAccount != feeSource || got.MaxFee != 400 {
			t.Fatalf("fee account/max fee = %q/%d", got.FeeAccount, got.MaxFee)
		}
		if got.FeeBumpTransaction == nil || got.InnerTransaction != nil {
			t.Fatalf("fee_bump=%#v inner=%#v, want fee_bump set and inner omitted", got.FeeBumpTransaction, got.InnerTransaction)
		}
	})
}

func horizonTestRow(envelopeXDR string) *horizonTransactionRow {
	return &horizonTransactionRow{
		LedgerSequence:  123,
		TransactionHash: "txhash",
		TransactionID:   sql.NullInt64{Int64: 528280977408, Valid: true},
		SourceAccount:   sql.NullString{String: "GAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAWHF", Valid: true},
		AccountSequence: sql.NullInt64{Int64: 99, Valid: true},
		FeeCharged:      sql.NullInt64{Int64: 100, Valid: true},
		MaxFee:          sql.NullInt64{Int64: 1000, Valid: true},
		Successful:      sql.NullBool{Bool: true, Valid: true},
		OperationCount:  sql.NullInt64{Int64: 2, Valid: true},
		CreatedAt:       sql.NullTime{Time: time.Date(2026, 7, 9, 15, 4, 0, 0, time.UTC), Valid: true},
		EnvelopeXDR:     sql.NullString{String: envelopeXDR, Valid: true},
		ResultXDR:       sql.NullString{String: "AAAA-result", Valid: true},
		ResultMetaXDR:   sql.NullString{String: "AAAA-meta", Valid: true},
		FeeMetaXDR:      sql.NullString{String: "AAAA-fee-meta", Valid: true},
		RawSignatures:   sql.NullString{String: `["sig1"]`, Valid: true},
	}
}

func horizonTestEnvelopeXDR(memo xdr.Memo) string {
	envelope := xdr.TransactionEnvelope{
		Type: xdr.EnvelopeTypeEnvelopeTypeTx,
		V1: &xdr.TransactionV1Envelope{
			Tx: xdr.Transaction{
				SourceAccount: xdr.MustMuxedAddress("GAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAWHF"),
				Fee:           100,
				SeqNum:        99,
				Cond:          xdr.Preconditions{Type: xdr.PreconditionTypePrecondNone},
				Memo:          memo,
				Operations:    []xdr.Operation{},
			},
			Signatures: []xdr.DecoratedSignature{},
		},
	}
	out, err := xdr.MarshalBase64(envelope)
	if err != nil {
		panic(err)
	}
	return out
}

var horizonTestEnvelopeNoMemoXDR = horizonTestEnvelopeXDR(xdr.Memo{Type: xdr.MemoTypeMemoNone})

func strPtr(s string) *string { return &s }

func uint64Ptr(v uint64) *xdr.Uint64 {
	out := xdr.Uint64(v)
	return &out
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
