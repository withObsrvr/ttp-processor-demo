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
	mock.ExpectQuery(regexp.QuoteMeta(`
		SELECT contract_id, key, key_hash, type, durability, size_bytes, data_value,
		       last_modified_ledger, closed_at, live_until_ledger_seq, ttl_remaining, expired
		FROM serving.sv_contract_storage_current
		WHERE contract_id = $1 AND durability = $2 AND expired = false
		ORDER BY key_hash
		LIMIT $3 OFFSET $4
	`)).
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
