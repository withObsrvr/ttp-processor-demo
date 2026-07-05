package main

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
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

// TestGetAccountTransactionsPaginationNoDropsAcrossPages walks the full paginated history of an
// account whose transactions span both the cold and hot arms, with a page size smaller than the
// total. It guards the per-arm bounding/overscan change: every transaction must come back exactly
// once, in descending ledger order, across page boundaries (no drops, no duplicates).
func TestGetAccountTransactionsPaginationNoDropsAcrossPages(t *testing.T) {
	db := newAccountHistoryDuckDB(t)
	defer db.Close()
	reader := &UnifiedDuckDBReader{db: db, hotSchema: "memory.hot", coldSchema: "memory.cold"}
	ctx := context.Background()

	// One transaction per ledger; cold holds the older ledgers (10-13), hot the newer (14-16).
	// Ledgers 11/13/15 carry a second op (GA as destination) to exercise op->transaction collapse.
	insert := func(schema string, ledger int, tx string, secondOp bool) {
		stmts := []string{fmt.Sprintf(`INSERT INTO %s.enriched_history_operations VALUES (%d, TIMESTAMP '2026-01-01 00:00:%02d', '%s', true, 'GA', 'GB', NULL, NULL, NULL, '100', 'none', NULL, true, false, 'payment')`, schema, ledger, ledger, tx)}
		if secondOp {
			stmts = append(stmts, fmt.Sprintf(`INSERT INTO %s.enriched_history_operations VALUES (%d, TIMESTAMP '2026-01-01 00:00:%02d', '%s', true, 'GC', 'GA', NULL, NULL, NULL, '100', 'none', NULL, true, false, 'payment')`, schema, ledger, ledger, tx))
		}
		for _, s := range stmts {
			if _, err := db.Exec(s); err != nil {
				t.Fatalf("insert: %v\n%s", err, s)
			}
		}
	}
	insert("cold", 10, "tx10", false)
	insert("cold", 11, "tx11", true)
	insert("cold", 12, "tx12", false)
	insert("cold", 13, "tx13", true)
	insert("hot", 14, "tx14", false)
	insert("hot", 15, "tx15", true)
	insert("hot", 16, "tx16", false)

	var seen []string
	var cursor *HistoryCursor
	for page := 0; page < 20; page++ {
		txs, cur, hasMore, err := reader.GetAccountTransactions(ctx, AccountTransactionsFilters{AccountID: "GA", Limit: 2, Order: "desc", Cursor: cursor})
		if err != nil {
			t.Fatalf("page %d: %v", page, err)
		}
		if len(txs) > 2 {
			t.Fatalf("page %d returned %d txns, want <= page limit 2", page, len(txs))
		}
		for _, tx := range txs {
			seen = append(seen, tx.TransactionHash)
		}
		if !hasMore {
			break
		}
		decoded, err := DecodeHistoryCursor(cur)
		if err != nil {
			t.Fatalf("decode cursor: %v", err)
		}
		cursor = decoded
	}

	want := []string{"tx16", "tx15", "tx14", "tx13", "tx12", "tx11", "tx10"}
	if len(seen) != len(want) {
		t.Fatalf("walked %d txns %v, want %d %v", len(seen), seen, len(want), want)
	}
	uniq := map[string]bool{}
	for i := range want {
		if seen[i] != want[i] {
			t.Fatalf("page-walk order mismatch at %d: got %v want %v", i, seen, want)
		}
		if uniq[seen[i]] {
			t.Fatalf("duplicate transaction %s across pages: %v", seen[i], seen)
		}
		uniq[seen[i]] = true
	}
}

// TestGetAccountTransactionsHighFanoutDoesNotHideOlder reproduces the bug where a single recent
// transaction with many op/event rows (here: a token-transfer batch emitting 20 rows for GA in one
// tx) could consume a per-row arm bound and make older transactions unreachable. The arm must bound
// by DISTINCT transactions, so the fat transaction collapses to one and older history still pages.
func TestGetAccountTransactionsHighFanoutDoesNotHideOlder(t *testing.T) {
	db := newAccountHistoryDuckDB(t)
	defer db.Close()
	reader := &UnifiedDuckDBReader{db: db, hotSchema: "memory.hot", coldSchema: "memory.cold"}
	ctx := context.Background()

	// Newest ledger 100 = one transaction with 20 token-transfer rows for GA. Older ledgers 99/98
	// are ordinary single-op transactions. With a per-row bound and limit=1 the 20 fanout rows would
	// fill the arm and hide tx99/tx98 entirely.
	for i := 0; i < 20; i++ {
		stmt := fmt.Sprintf(`INSERT INTO hot.token_transfers_raw VALUES (100, TIMESTAMP '2026-01-01 00:01:40', 'tx100', true, 'GA', 'GZ', 'CC', '1', %d)`, i)
		if _, err := db.Exec(stmt); err != nil {
			t.Fatalf("fanout insert: %v", err)
		}
	}
	for _, s := range []string{
		`INSERT INTO hot.enriched_history_operations VALUES (99, TIMESTAMP '2026-01-01 00:01:39', 'tx99', true, 'GA', 'GB', NULL, NULL, NULL, '100', 'none', NULL, true, false, 'payment')`,
		`INSERT INTO hot.enriched_history_operations VALUES (98, TIMESTAMP '2026-01-01 00:01:38', 'tx98', true, 'GA', 'GB', NULL, NULL, NULL, '100', 'none', NULL, true, false, 'payment')`,
	} {
		if _, err := db.Exec(s); err != nil {
			t.Fatalf("older insert: %v", err)
		}
	}

	var seen []string
	var cursor *HistoryCursor
	for page := 0; page < 10; page++ {
		txs, cur, hasMore, err := reader.GetAccountTransactions(ctx, AccountTransactionsFilters{AccountID: "GA", Limit: 1, Order: "desc", Cursor: cursor})
		if err != nil {
			t.Fatalf("page %d: %v", page, err)
		}
		for _, tx := range txs {
			seen = append(seen, tx.TransactionHash)
		}
		if !hasMore {
			break
		}
		cursor, err = DecodeHistoryCursor(cur)
		if err != nil {
			t.Fatalf("decode cursor: %v", err)
		}
	}

	want := []string{"tx100", "tx99", "tx98"}
	if len(seen) != len(want) {
		t.Fatalf("walked %v, want %v (older transactions hidden by the high-fanout tx?)", seen, want)
	}
	for i := range want {
		if seen[i] != want[i] {
			t.Fatalf("order mismatch at %d: got %v want %v", i, seen, want)
		}
	}
}

func TestAccountLedgerBucketIsStable(t *testing.T) {
	const account = "GCFOH4PUYAXJH75SLBXT7NZWOT2JWXGCBI6YF3RXV6LXUHJCCKA4HH4I"
	got := AccountLedgerBucket(account, 256)
	if got != 203 {
		t.Fatalf("AccountLedgerBucket(%q, 256)=%d want 203", account, got)
	}
	if got < 0 || got >= 256 {
		t.Fatalf("bucket %d outside configured range", got)
	}
}

func TestBuildAccountTransactionsQueryAddsColdLedgerRangePruning(t *testing.T) {
	query := buildAccountTransactionsQuery("memory.hot", "memory.cold", "1=1", " AND ledger_range IN ($2, $3)", "DESC", 4)

	if got := strings.Count(query, "ledger_range IN ($2, $3)"); got != 3 {
		t.Fatalf("ledger range pruning count=%d want 3\n%s", got, query)
	}
	if strings.Contains(query, "memory.hot.enriched_history_operations\n\t\tWHERE (source_account = $1 OR destination = $1 OR from_account = $1 OR to_address = $1 OR address = $1) AND ledger_range") {
		t.Fatalf("hot branch should not receive cold ledger_range pruning\n%s", query)
	}
	for _, want := range []string{
		"FROM memory.cold.enriched_history_operations\n\t\tWHERE (source_account = $1 OR destination = $1) AND ledger_range IN ($2, $3)",
		"FROM memory.cold.token_transfers_raw\n\t\tWHERE (from_account = $1 OR to_account = $1) AND ledger_range IN ($2, $3)",
		"FROM memory.cold.contract_invocations_raw\n\t\tWHERE (source_account = $1 OR contract_id = $1) AND ledger_range IN ($2, $3)",
	} {
		if !strings.Contains(query, want) {
			t.Fatalf("missing cold pruning fragment %q\n%s", want, query)
		}
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
