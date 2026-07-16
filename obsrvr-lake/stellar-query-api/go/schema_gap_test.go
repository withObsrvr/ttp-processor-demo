package main

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/lib/pq"
)

func TestIsSchemaGapError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{name: "nil", err: nil, want: false},
		{name: "pq undefined column", err: &pq.Error{Code: "42703"}, want: true},
		{name: "pq undefined table", err: &pq.Error{Code: "42P01"}, want: true},
		{name: "pq permission denied is not a gap", err: &pq.Error{Code: "42501", Message: `permission denied for table sv_transactions_recent`}, want: false},
		{name: "wrapped pq error", err: fmt.Errorf("query: %w", &pq.Error{Code: "42703"}), want: true},
		{name: "duckdb binder missing column", err: errors.New(`Binder Error: Referenced column "sequence_ledger" not found in FROM clause!`), want: true},
		{name: "duckdb catalog missing table", err: errors.New(`Catalog Error: Table with name accounts_current does not exist!`), want: true},
		{name: "text column does not exist", err: errors.New(`ERROR: column "tx_envelope" does not exist (SQLSTATE 42703)`), want: true},
		{name: "constraint violation mentioning column is not a gap", err: errors.New(`ERROR: null value in column "transaction_id" violates not-null constraint`), want: false},
		{name: "timeout mentioning table is not a gap", err: errors.New(`canceling statement due to statement timeout while scanning sv_transactions_recent`), want: false},
		{name: "network error is not a gap", err: errors.New("connection refused"), want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isSchemaGapError(tt.err); got != tt.want {
				t.Fatalf("isSchemaGapError(%v) = %v, want %v", tt.err, got, tt.want)
			}
		})
	}
}

func TestUnifiedGetAccountCurrentFallsBackOnColdSchemaGap(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	updatedAt := time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC).Format(time.RFC3339)
	cols := []string{
		"account_id", "balance", "sequence_number", "num_subentries",
		"num_sponsoring", "num_sponsored", "last_modified_ledger",
		"sequence_ledger", "sequence_time", "updated_at", "home_domain",
		"sponsor_account", "auth_required", "auth_revocable", "auth_immutable", "auth_clawback_enabled",
	}
	mock.ExpectQuery("FROM hot_db.silver.accounts_current").
		WithArgs("GA").
		WillReturnError(errors.New(`Binder Error: Referenced column "sequence_ledger" not found in FROM clause!`))
	mock.ExpectQuery(`NULL::BIGINT AS sequence_ledger`).
		WithArgs("GA").
		WillReturnRows(sqlmock.NewRows(cols).AddRow(
			"GA", "1000000000", "123", int64(2), int64(0), int64(0), int64(50),
			nil, nil, updatedAt, nil, nil, nil, nil, nil, nil,
		))

	reader := &UnifiedDuckDBReader{db: db, hotSchema: "hot_db.silver", coldSchema: "cold_db.silver"}
	acc, err := reader.GetAccountCurrent(context.Background(), "GA")
	if err != nil {
		t.Fatalf("GetAccountCurrent: %v", err)
	}
	if acc == nil || acc.AccountID != "GA" || acc.SequenceNumber != "123" {
		t.Fatalf("account = %#v", acc)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestUnifiedGetAccountCurrentDoesNotFallBackOnTransientError(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	mock.ExpectQuery("FROM hot_db.silver.accounts_current").
		WithArgs("GA").
		WillReturnError(errors.New("connection refused"))

	reader := &UnifiedDuckDBReader{db: db, hotSchema: "hot_db.silver", coldSchema: "cold_db.silver"}
	_, err = reader.GetAccountCurrent(context.Background(), "GA")
	if err == nil {
		t.Fatal("transient error must propagate, not trigger schema-gap fallback")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}
