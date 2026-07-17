package main

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"
)

func TestBuildTempTransactionInsertUsesBoundedParameters(t *testing.T) {
	closedAt := time.Date(2026, time.July, 16, 12, 0, 0, 0, time.FixedZone("EDT", -4*60*60))
	createdAt := closedAt.Add(time.Minute)
	transactions := []TransactionIndex{
		{
			TxHash:         "abc",
			LedgerSequence: 100,
			OperationCount: 2,
			Successful:     true,
			ClosedAt:       closedAt,
			LedgerRange:    1,
		},
		{
			TxHash:         "def",
			LedgerSequence: 101,
			OperationCount: 1,
			Successful:     false,
			ClosedAt:       closedAt.Add(time.Second),
			LedgerRange:    1,
		},
	}

	query, args := buildTempTransactionInsert(transactions, createdAt)
	if got := strings.Count(query, "(?, ?, ?, ?, ?, ?, ?)"); got != len(transactions) {
		t.Fatalf("placeholder rows = %d, want %d: %s", got, len(transactions), query)
	}
	if len(args) != len(transactions)*7 {
		t.Fatalf("args = %d, want %d", len(args), len(transactions)*7)
	}
	if args[0] != "abc" || args[7] != "def" {
		t.Fatalf("unexpected transaction hashes in args: %#v", args)
	}
	if got := args[4].(time.Time); !got.Equal(closedAt.UTC()) || got.Location() != time.UTC {
		t.Fatalf("closed_at = %v, want UTC %v", got, closedAt.UTC())
	}
	if got := args[6].(time.Time); !got.Equal(createdAt.UTC()) || got.Location() != time.UTC {
		t.Fatalf("created_at = %v, want UTC %v", got, createdAt.UTC())
	}
}

func TestBuildTempTransactionInsertExecutesInDuckDB(t *testing.T) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	defer db.Close()

	if _, err := db.Exec(`
		CREATE TEMP TABLE temp_tx_index (
			tx_hash VARCHAR,
			ledger_sequence BIGINT,
			operation_count INTEGER,
			successful BOOLEAN,
			closed_at TIMESTAMP,
			ledger_range BIGINT,
			created_at TIMESTAMP
		)
	`); err != nil {
		t.Fatalf("create temp table: %v", err)
	}

	now := time.Now().UTC().Truncate(time.Microsecond)
	transactions := []TransactionIndex{
		{TxHash: "abc", LedgerSequence: 100, OperationCount: 2, Successful: true, ClosedAt: now, LedgerRange: 1},
		{TxHash: "def", LedgerSequence: 101, OperationCount: 1, Successful: false, ClosedAt: now.Add(time.Second), LedgerRange: 1},
	}
	query, args := buildTempTransactionInsert(transactions, now)
	if _, err := db.ExecContext(context.Background(), query, args...); err != nil {
		t.Fatalf("execute parameterized insert: %v", err)
	}

	var count int
	if err := db.QueryRow("SELECT COUNT(*) FROM temp_tx_index").Scan(&count); err != nil {
		t.Fatalf("count rows: %v", err)
	}
	if count != len(transactions) {
		t.Fatalf("rows = %d, want %d", count, len(transactions))
	}
}

func TestCheckpointAtHotBoundary(t *testing.T) {
	tests := []struct {
		name        string
		lastLedger  int64
		minLedger   int64
		wantLedger  int64
		wantAdvance bool
	}{
		{
			name:        "stale checkpoint advances before oldest hot ledger",
			lastLedger:  63_504_729,
			minLedger:   63_505_348,
			wantLedger:  63_505_347,
			wantAdvance: true,
		},
		{
			name:        "current checkpoint is unchanged",
			lastLedger:  63_505_347,
			minLedger:   63_505_348,
			wantLedger:  63_505_347,
			wantAdvance: false,
		},
		{
			name:        "genesis boundary is unchanged",
			lastLedger:  0,
			minLedger:   1,
			wantLedger:  0,
			wantAdvance: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotLedger, gotAdvance := checkpointAtHotBoundary(tt.lastLedger, tt.minLedger)
			if gotLedger != tt.wantLedger || gotAdvance != tt.wantAdvance {
				t.Fatalf("checkpointAtHotBoundary(%d, %d) = (%d, %t), want (%d, %t)",
					tt.lastLedger, tt.minLedger, gotLedger, gotAdvance, tt.wantLedger, tt.wantAdvance)
			}
		})
	}
}
