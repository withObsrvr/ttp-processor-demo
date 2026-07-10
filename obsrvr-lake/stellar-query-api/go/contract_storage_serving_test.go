package main

import (
	"context"
	"database/sql"
	"regexp"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
)

func TestGetServingContractStorage(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	reader := &SilverHotReader{db: db, network: "testnet"}
	closedAt := time.Date(2026, 7, 6, 12, 0, 0, 0, time.UTC)
	mock.ExpectQuery(regexp.QuoteMeta(`FROM serving.sv_contract_storage_current s`)).
		WithArgs("CCONTRACT", "persistent", 5, 0).
		WillReturnRows(sqlmock.NewRows([]string{
			"contract_id", "key", "key_hash", "type", "durability", "size_bytes", "data_value",
			"last_modified_ledger", "closed_at", "live_until_ledger_seq", "ttl_remaining", "expired",
		}).AddRow("CCONTRACT", "kh1", "kh1", "persistent", "persistent", int32(12), sql.NullString{String: "AAAA", Valid: true}, int64(123), closedAt, int64(456), int32(333), false))

	got, err := reader.GetServingContractStorage(context.Background(), "CCONTRACT", "persistent", true, 5, 0)
	if err != nil {
		t.Fatalf("GetServingContractStorage: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("len(got) = %d, want 1", len(got))
	}
	if got[0].ContractID != "CCONTRACT" || got[0].KeyHash != "kh1" || got[0].LastModifiedLedger != 123 {
		t.Fatalf("unexpected entry: %+v", got[0])
	}
	if got[0].TTLRemaining == nil || *got[0].TTLRemaining != 333 {
		t.Fatalf("ttl remaining = %v, want 333", got[0].TTLRemaining)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestGetServingContractStorageSummaryIncludesCoverage(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	reader := &SilverHotReader{db: db, network: "testnet"}
	updatedAt := time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC)
	latestClosedAt := time.Date(2026, 7, 10, 11, 59, 0, 0, time.UTC)
	mock.ExpectQuery(regexp.QuoteMeta("FROM serving.sv_contract_storage_summary")).
		WithArgs("CCONTRACT").
		WillReturnRows(sqlmock.NewRows([]string{
			"contract_id", "total_entries", "live_entries", "expired_entries", "deleted_entries",
			"persistent_entries", "temporary_entries", "instance_entries", "total_size_bytes",
			"latest_ledger", "latest_closed_at", "updated_at",
		}).AddRow("CCONTRACT", int64(43), int64(40), int64(3), int64(0), int64(35), int64(7), int64(1), int64(2048), int64(12345), latestClosedAt, updatedAt))
	mock.ExpectQuery(regexp.QuoteMeta("FROM serving.sv_watermarks")).
		WithArgs("serving.sv_contract_storage_summary").
		WillReturnRows(sqlmock.NewRows([]string{"table_name", "status", "complete_from", "complete_thru", "updated_at"}).
			AddRow("serving.sv_contract_storage_summary", "complete", int64(3), int64(12345), updatedAt))

	got, err := reader.GetServingContractStorageSummary(context.Background(), "CCONTRACT")
	if err != nil {
		t.Fatalf("GetServingContractStorageSummary: %v", err)
	}
	if got == nil || got.TotalEntries != 43 || got.LiveEntries != 40 {
		t.Fatalf("summary = %#v", got)
	}
	if got.Coverage == nil || got.Coverage.CompleteThru != 12345 {
		t.Fatalf("coverage = %#v, want complete thru 12345", got.Coverage)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}
