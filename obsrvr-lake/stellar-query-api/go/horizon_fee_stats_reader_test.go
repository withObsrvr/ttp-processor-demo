package main

import (
	"context"
	"database/sql"
	"testing"

	_ "github.com/duckdb/duckdb-go/v2"
)

func TestHorizonFeeStatsUsesPerOperationFeesAndRoundedCapacity(t *testing.T) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	for _, stmt := range []string{
		`CREATE TABLE ledgers_row_v2 (sequence BIGINT, base_fee BIGINT, operation_count BIGINT, max_tx_set_size BIGINT)`,
		`CREATE TABLE transactions_row_v2 (ledger_sequence BIGINT, fee_charged BIGINT, max_fee BIGINT, operation_count BIGINT)`,
		`INSERT INTO ledgers_row_v2 VALUES
			(1,100,1,50),
			(2,100,1,50),
			(3,100,2,50),
			(4,100,16,50),
			(5,100,50,50),
			(6,100,3,50)`,
		`INSERT INTO transactions_row_v2 VALUES
			(2,100,200,1),
			(3,100,400,2),
			(4,99,256000,16),
			(6,300,600,3)`,
	} {
		if _, err := db.Exec(stmt); err != nil {
			t.Fatal(err)
		}
	}

	reader := &HorizonFeeStatsReader{hot: &HotReader{db: db}}
	stats, err := reader.GetFeeStats(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if stats.LastLedger != 6 || stats.LastLedgerBaseFee != 100 {
		t.Fatalf("ledger metadata = %#v", stats)
	}
	if stats.LedgerCapacityUsage != 0.09 {
		t.Fatalf("capacity usage = %v, want 0.09", stats.LedgerCapacityUsage)
	}
	if stats.FeeCharged.Min != 6 || stats.FeeCharged.P50 != 50 || stats.FeeCharged.Mode != 100 || stats.FeeCharged.Max != 100 {
		t.Fatalf("fee charged distribution = %#v", stats.FeeCharged)
	}
	if stats.MaxFee.Min != 200 || stats.MaxFee.P90 != 16000 || stats.MaxFee.Max != 16000 {
		t.Fatalf("max fee distribution = %#v", stats.MaxFee)
	}
}

func TestHorizonFeeStatsFallsBackToBaseFeeWhenWindowHasNoTransactions(t *testing.T) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	for _, stmt := range []string{
		`CREATE TABLE ledgers_row_v2 (sequence BIGINT, base_fee BIGINT, operation_count BIGINT, max_tx_set_size BIGINT)`,
		`CREATE TABLE transactions_row_v2 (ledger_sequence BIGINT, fee_charged BIGINT, max_fee BIGINT, operation_count BIGINT)`,
		`INSERT INTO ledgers_row_v2 VALUES (10,123,0,50)`,
	} {
		if _, err := db.Exec(stmt); err != nil {
			t.Fatal(err)
		}
	}

	reader := &HorizonFeeStatsReader{hot: &HotReader{db: db}}
	stats, err := reader.GetFeeStats(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if stats.LastLedger != 10 || stats.LastLedgerBaseFee != 123 {
		t.Fatalf("ledger metadata = %#v", stats)
	}
	if stats.FeeCharged.Min != 123 || stats.FeeCharged.P99 != 123 || stats.MaxFee.Mode != 123 {
		t.Fatalf("base-fee fallback not applied: fee=%#v max=%#v", stats.FeeCharged, stats.MaxFee)
	}
}
