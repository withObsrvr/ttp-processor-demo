package main

import (
	"context"
	"database/sql"
	"errors"
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
