package main

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"

	_ "github.com/duckdb/duckdb-go/v2"
)

// newCoverageTestLoader builds a Loader backed by an in-memory DuckDB with the
// minimal Bronze tables verifyBronzeCoverage reads. Real DuckDB + real SQL, no
// mocks.
func newCoverageTestLoader(t *testing.T) *Loader {
	t.Helper()
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	t.Cleanup(func() { db.Close() })

	stmts := []string{
		"CREATE SCHEMA IF NOT EXISTS bronze",
		"CREATE TABLE memory.bronze.ledgers_row_v2 (sequence BIGINT, tx_set_operation_count INTEGER, transaction_count INTEGER)",
		"CREATE TABLE memory.bronze.operations_row_v2 (ledger_sequence BIGINT)",
		"CREATE TABLE memory.bronze.transactions_row_v2 (ledger_sequence BIGINT)",
	}
	for _, s := range stmts {
		if _, err := db.Exec(s); err != nil {
			t.Fatalf("setup %q: %v", s, err)
		}
	}
	return &Loader{db: db, cfg: Config{Network: "testnet", BronzeAlias: "memory", BronzeSchema: "bronze"}}
}

// insertLedger writes a ledger header; pass txSetOps < 0 to store NULL.
func insertLedger(t *testing.T, l *Loader, seq int64, txSetOps, txCount int) {
	t.Helper()
	opVal := "NULL"
	if txSetOps >= 0 {
		opVal = fmt.Sprintf("%d", txSetOps)
	}
	q := fmt.Sprintf("INSERT INTO memory.bronze.ledgers_row_v2 VALUES (%d, %s, %d)", seq, opVal, txCount)
	if _, err := l.db.Exec(q); err != nil {
		t.Fatalf("insert ledger %d: %v", seq, err)
	}
}

func insertFactRows(t *testing.T, l *Loader, table string, ledgerSeq int64, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		if _, err := l.db.Exec("INSERT INTO memory.bronze."+table+" VALUES (?)", ledgerSeq); err != nil {
			t.Fatalf("insert %s: %v", table, err)
		}
	}
}

func TestVerifyBronzeCoverage(t *testing.T) {
	ctx := context.Background()

	t.Run("matching counts pass", func(t *testing.T) {
		l := newCoverageTestLoader(t)
		for seq := int64(1); seq <= 3; seq++ {
			insertLedger(t, l, seq, 2, 1) // 2 ops, 1 tx per ledger
			insertFactRows(t, l, "operations_row_v2", seq, 2)
			insertFactRows(t, l, "transactions_row_v2", seq, 1)
		}
		if err := l.verifyBronzeCoverage(ctx, 1, 3); err != nil {
			t.Fatalf("expected pass, got %v", err)
		}
	})

	t.Run("truncated operations fail", func(t *testing.T) {
		l := newCoverageTestLoader(t)
		for seq := int64(1); seq <= 3; seq++ {
			insertLedger(t, l, seq, 2, 1)
			insertFactRows(t, l, "transactions_row_v2", seq, 1)
		}
		// Only 5 op rows present where SUM(tx_set_operation_count)=6.
		insertFactRows(t, l, "operations_row_v2", 1, 2)
		insertFactRows(t, l, "operations_row_v2", 2, 2)
		insertFactRows(t, l, "operations_row_v2", 3, 1)
		err := l.verifyBronzeCoverage(ctx, 1, 3)
		if err == nil || !strings.Contains(err.Error(), "operations_row_v2 incomplete") {
			t.Fatalf("expected operations incompleteness error, got %v", err)
		}
	})

	t.Run("missing ledger in spine fails", func(t *testing.T) {
		l := newCoverageTestLoader(t)
		insertLedger(t, l, 1, 0, 0)
		insertLedger(t, l, 2, 0, 0)
		err := l.verifyBronzeCoverage(ctx, 1, 3)
		if err == nil || !strings.Contains(err.Error(), "bronze coverage incomplete") {
			t.Fatalf("expected spine coverage error, got %v", err)
		}
	})

	t.Run("null tx_set_operation_count skips op check instead of false-failing", func(t *testing.T) {
		l := newCoverageTestLoader(t)
		insertLedger(t, l, 1, 2, 1)
		insertLedger(t, l, 2, -1, 1) // NULL tx_set_operation_count
		insertLedger(t, l, 3, 2, 1)
		// Deliberately omit op rows; must NOT fail because a header is NULL.
		for seq := int64(1); seq <= 3; seq++ {
			insertFactRows(t, l, "transactions_row_v2", seq, 1)
		}
		if err := l.verifyBronzeCoverage(ctx, 1, 3); err != nil {
			t.Fatalf("expected NULL header to skip op check, got %v", err)
		}
	})
}
