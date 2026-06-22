package main

import (
	"context"
	"database/sql"
	"testing"

	_ "github.com/duckdb/duckdb-go/v2"
)

func TestGetAccountTransactionsHotColdDedupAndPagination(t *testing.T) {
	db := newAccountHistoryDuckDB(t)
	defer db.Close()
	reader := &UnifiedDuckDBReader{db: db, hotSchema: "memory.hot", coldSchema: "memory.cold"}
	ctx := context.Background()

	insertAccountTxFixtures(t, db)

	got, cursor, hasMore, err := reader.GetAccountTransactions(ctx, AccountTransactionsFilters{AccountID: "GA", Limit: 2, Order: "desc"})
	if err != nil {
		t.Fatalf("GetAccountTransactions: %v", err)
	}
	if !hasMore || cursor == "" {
		t.Fatalf("expected pagination cursor, hasMore=%v cursor=%q", hasMore, cursor)
	}
	if len(got) != 2 {
		t.Fatalf("len=%d want 2", len(got))
	}
	if got[0].TransactionHash != "tx3" || got[1].TransactionHash != "tx2" {
		t.Fatalf("unexpected tx order: %#v", got)
	}
	if got[1].Summary == "" || len(got[1].ActivityTypes) == 0 || len(got[1].SourceTables) == 0 {
		t.Fatalf("missing semantic summary fields: %#v", got[1])
	}

	decoded, err := DecodeHistoryCursor(cursor)
	if err != nil {
		t.Fatalf("decode cursor: %v", err)
	}
	next, _, _, err := reader.GetAccountTransactions(ctx, AccountTransactionsFilters{AccountID: "GA", Limit: 2, Order: "desc", Cursor: decoded})
	if err != nil {
		t.Fatalf("GetAccountTransactions page 2: %v", err)
	}
	if len(next) != 1 || next[0].TransactionHash != "tx1" {
		t.Fatalf("unexpected page 2: %#v", next)
	}
}

func TestGetAddressBalanceHistoryClassicAndContract(t *testing.T) {
	db := newAccountHistoryDuckDB(t)
	defer db.Close()
	reader := &UnifiedDuckDBReader{db: db, hotSchema: "memory.hot", coldSchema: "memory.cold"}
	ctx := context.Background()
	insertBalanceHistoryFixtures(t, db)

	classic, _, _, err := reader.GetAddressBalanceHistory(ctx, BalanceHistoryFilters{Address: "GA", Asset: "XLM", Limit: 10, Order: "asc"})
	if err != nil {
		t.Fatalf("classic balance history: %v", err)
	}
	if len(classic) != 2 || classic[0].Balance != "100" || classic[1].Balance != "150" {
		t.Fatalf("unexpected classic history: %#v", classic)
	}

	contract, _, _, err := reader.GetAddressBalanceHistory(ctx, BalanceHistoryFilters{Address: "GA", ContractID: "CC", Limit: 10, Order: "asc"})
	if err != nil {
		t.Fatalf("contract balance history: %v", err)
	}
	if len(contract) != 3 {
		t.Fatalf("contract len=%d want 3: %#v", len(contract), contract)
	}
	wantBalances := []string{"10.0000000", "7.0000000", "12.0000000"}
	for i := range wantBalances {
		if contract[i].Balance != wantBalances[i] {
			t.Fatalf("contract[%d].Balance=%q want %q; all=%#v", i, contract[i].Balance, wantBalances[i], contract)
		}
	}
}

func newAccountHistoryDuckDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	for _, schema := range []string{"hot", "cold"} {
		if _, err := db.Exec(`CREATE SCHEMA ` + schema); err != nil {
			t.Fatalf("create schema %s: %v", schema, err)
		}
		stmts := []string{
			`CREATE TABLE memory.` + schema + `.enriched_history_operations (ledger_sequence BIGINT, ledger_closed_at TIMESTAMP, transaction_hash VARCHAR, tx_successful BOOLEAN, source_account VARCHAR, destination VARCHAR, from_account VARCHAR, to_address VARCHAR, address VARCHAR, tx_fee_charged VARCHAR, tx_memo_type VARCHAR, tx_memo VARCHAR, is_payment_op BOOLEAN, is_soroban_op BOOLEAN, type_string VARCHAR)`,
			`CREATE TABLE memory.` + schema + `.token_transfers_raw (ledger_sequence BIGINT, "timestamp" TIMESTAMP, transaction_hash VARCHAR, transaction_successful BOOLEAN, from_account VARCHAR, to_account VARCHAR, token_contract_id VARCHAR, amount VARCHAR, event_index INTEGER)`,
			`CREATE TABLE memory.` + schema + `.contract_invocations_raw (ledger_sequence BIGINT, closed_at TIMESTAMP, transaction_hash VARCHAR, successful BOOLEAN, source_account VARCHAR, contract_id VARCHAR)`,
		}
		if schema == "cold" {
			stmts = append(stmts, `CREATE TABLE memory.`+schema+`.balance_changes (ledger_sequence BIGINT, ledger_closed_at TIMESTAMP, address VARCHAR, asset_type VARCHAR, asset_code VARCHAR, asset_issuer VARCHAR, balance VARCHAR, deleted BOOLEAN)`)
		}
		for _, stmt := range stmts {
			if _, err := db.Exec(stmt); err != nil {
				t.Fatalf("create table: %v\n%s", err, stmt)
			}
		}
	}
	return db
}

func insertAccountTxFixtures(t *testing.T, db *sql.DB) {
	t.Helper()
	stmts := []string{
		`INSERT INTO cold.enriched_history_operations VALUES (1, TIMESTAMP '2026-01-01 00:00:01', 'tx1', true, 'GA', 'GB', NULL, NULL, NULL, '100', 'none', NULL, true, false, 'payment')`,
		`INSERT INTO cold.token_transfers_raw VALUES (2, TIMESTAMP '2026-01-01 00:00:02', 'tx2', true, 'GB', 'GA', 'CC', '5', 0)`,
		`INSERT INTO hot.token_transfers_raw VALUES (2, TIMESTAMP '2026-01-01 00:00:02', 'tx2', true, 'GB', 'GA', 'CC', '5', 0)`,
		`INSERT INTO hot.contract_invocations_raw VALUES (3, TIMESTAMP '2026-01-01 00:00:03', 'tx3', true, 'GA', 'CC')`,
	}
	for _, stmt := range stmts {
		if _, err := db.Exec(stmt); err != nil {
			t.Fatalf("fixture insert: %v\n%s", err, stmt)
		}
	}
}

func insertBalanceHistoryFixtures(t *testing.T, db *sql.DB) {
	t.Helper()
	stmts := []string{
		`INSERT INTO cold.balance_changes VALUES (1, TIMESTAMP '2026-01-01 00:00:01', 'GA', 'native', 'XLM', NULL, '100', false)`,
		`INSERT INTO cold.balance_changes VALUES (3, TIMESTAMP '2026-01-01 00:00:03', 'GA', 'native', 'XLM', NULL, '150', false)`,
		`INSERT INTO cold.token_transfers_raw VALUES (1, TIMESTAMP '2026-01-01 00:00:01', 'tx1', true, NULL, 'GA', 'CC', '10', 0)`,
		`INSERT INTO cold.token_transfers_raw VALUES (2, TIMESTAMP '2026-01-01 00:00:02', 'tx2', true, 'GA', 'GB', 'CC', '3', 0)`,
		`INSERT INTO hot.token_transfers_raw VALUES (4, TIMESTAMP '2026-01-01 00:00:04', 'tx4', true, 'GB', 'GA', 'CC', '5', 0)`,
	}
	for _, stmt := range stmts {
		if _, err := db.Exec(stmt); err != nil {
			t.Fatalf("fixture insert: %v\n%s", err, stmt)
		}
	}
}
