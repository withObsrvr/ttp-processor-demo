package main

import (
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
)

func TestScanContractEventsAcceptsStringEventType(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	closedAt := time.Date(2026, 7, 10, 2, 33, 21, 0, time.UTC)
	rows := sqlmock.NewRows([]string{
		"event_id",
		"contract_id",
		"ledger_sequence",
		"transaction_hash",
		"operation_index",
		"event_index",
		"event_type",
		"topics_json",
		"data_decoded",
		"closed_at",
		"ledger_range",
		"era_id",
		"version_label",
	}).AddRow(
		"evt-1",
		nil,
		int64(833243),
		"tx-hash",
		int64(1),
		int64(0),
		"contract",
		`["transfer"]`,
		`{"amount":"1"}`,
		closedAt,
		int64(833000),
		"horizon-xdr-20260709",
		"contract-events-utf8",
	)

	mock.ExpectQuery("SELECT").WillReturnRows(rows)
	sqlRows, err := db.Query("SELECT")
	if err != nil {
		t.Fatalf("query rows: %v", err)
	}
	defer sqlRows.Close()

	got, err := scanContractEvents(sqlRows)
	if err != nil {
		t.Fatalf("scanContractEvents: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("got %d events, want 1", len(got))
	}
	if got[0]["event_type"] != "contract" {
		t.Fatalf("event_type = %#v, want contract", got[0]["event_type"])
	}
	if got[0]["contract_id"] != nil {
		t.Fatalf("contract_id = %#v, want nil", got[0]["contract_id"])
	}
}
