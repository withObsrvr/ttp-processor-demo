package main

import (
	"context"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestLedgerRangeBounds(t *testing.T) {
	for _, tt := range []struct {
		name      string
		start     int64
		end       int64
		wantStart int64
		wantEnd   int64
	}{
		{name: "genesis", start: 3, end: 3, wantStart: 0, wantEnd: 0},
		{name: "exact historical ledger", start: 3177525, end: 3177525, wantStart: 3170000, wantEnd: 3170000},
		{name: "crosses range boundary", start: 9999, end: 10000, wantStart: 0, wantEnd: 10000},
		{name: "reversed", start: 20000, end: 10000, wantStart: 20000, wantEnd: 20000},
	} {
		t.Run(tt.name, func(t *testing.T) {
			gotStart, gotEnd := ledgerRangeBounds(tt.start, tt.end)
			if gotStart != tt.wantStart || gotEnd != tt.wantEnd {
				t.Fatalf("ledgerRangeBounds(%d, %d) = (%d, %d), want (%d, %d)", tt.start, tt.end, gotStart, gotEnd, tt.wantStart, tt.wantEnd)
			}
		})
	}
}

func TestColdReaderQueryLedgersUsesLedgerRangePredicate(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	reader := &ColdReader{
		db: db,
		config: DuckLakeConfig{
			CatalogName: "cat",
			SchemaName:  "bronze",
		},
	}
	mock.ExpectQuery(`(?s)FROM cat\.bronze\.ledgers_row_v2.*ledger_range >= \? AND ledger_range <= \?.*LIMIT \?`).
		WithArgs(int64(3177525), int64(3177525), int64(3170000), int64(3170000), 1).
		WillReturnRows(sqlmock.NewRows([]string{"sequence"}))

	rows, err := reader.QueryLedgers(context.Background(), 3177525, 3177525, 1, "sequence_asc")
	if err != nil {
		t.Fatalf("QueryLedgers: %v", err)
	}
	rows.Close()
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestColdReaderQueryLedgerUsesExactLedgerRangePredicate(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	reader := &ColdReader{
		db: db,
		config: DuckLakeConfig{
			CatalogName: "cat",
			SchemaName:  "bronze",
		},
	}
	mock.ExpectQuery(`(?s)FROM cat\.bronze\.ledgers_row_v2.*WHERE sequence = \?\s+AND ledger_range = \?.*LIMIT 1`).
		WithArgs(int64(3177525), int64(3170000)).
		WillReturnRows(sqlmock.NewRows([]string{"sequence"}))

	rows, err := reader.QueryLedger(context.Background(), 3177525)
	if err != nil {
		t.Fatalf("QueryLedger: %v", err)
	}
	rows.Close()
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestHotReaderQueryLedgersUsesLedgerRangePredicate(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	reader := &HotReader{db: db}
	mock.ExpectQuery(`(?s)FROM ledgers_row_v2.*ledger_range >= \$3 AND ledger_range <= \$4.*LIMIT \$5`).
		WithArgs(int64(3538756), int64(3538756), int64(3530000), int64(3530000), 1).
		WillReturnRows(sqlmock.NewRows([]string{"sequence"}))

	rows, err := reader.QueryLedgers(context.Background(), 3538756, 3538756, 1, "sequence_desc")
	if err != nil {
		t.Fatalf("QueryLedgers: %v", err)
	}
	rows.Close()
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestHotReaderQueryLedgerUsesExactLedgerRangePredicate(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	reader := &HotReader{db: db}
	mock.ExpectQuery(`(?s)FROM ledgers_row_v2.*WHERE sequence = \$1\s+AND ledger_range = \$2.*LIMIT 1`).
		WithArgs(int64(3538756), int64(3530000)).
		WillReturnRows(sqlmock.NewRows([]string{"sequence"}))

	rows, err := reader.QueryLedger(context.Background(), 3538756)
	if err != nil {
		t.Fatalf("QueryLedger: %v", err)
	}
	rows.Close()
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}
