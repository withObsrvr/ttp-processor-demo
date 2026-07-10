package main

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	_ "github.com/duckdb/duckdb-go/v2"
)

func TestGetAccountTransactionsHotColdDedupAndPagination(t *testing.T) {
	db := newAccountHistoryDuckDB(t)
	defer db.Close()
	reader := &UnifiedDuckDBReader{db: db, hotSchema: "memory.hot", coldSchema: "memory.cold"}
	ctx := context.Background()

	insertAccountTxFixtures(t, db)

	got, cursor, hasMore, _, err := reader.GetAccountTransactions(ctx, AccountTransactionsFilters{AccountID: "GA", Limit: 2, Order: "desc"})
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
	next, _, _, _, err := reader.GetAccountTransactions(ctx, AccountTransactionsFilters{AccountID: "GA", Limit: 2, Order: "desc", Cursor: decoded})
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
		txs, cur, hasMore, _, err := reader.GetAccountTransactions(ctx, AccountTransactionsFilters{AccountID: "GA", Limit: 2, Order: "desc", Cursor: cursor})
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
		txs, cur, hasMore, _, err := reader.GetAccountTransactions(ctx, AccountTransactionsFilters{AccountID: "GA", Limit: 1, Order: "desc", Cursor: cursor})
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

func TestAccountLedgerRangeForLedgerUsesConfiguredPartitionSize(t *testing.T) {
	if got := AccountLedgerRangeForLedger(249999, 50000); got != 4 {
		t.Fatalf("AccountLedgerRangeForLedger(249999, 50000)=%d want 4", got)
	}
	if got := AccountLedgerRangeForLedger(100000, 0); got != 1 {
		t.Fatalf("AccountLedgerRangeForLedger default=%d want 1", got)
	}
}

func TestAccountLedgerIndexSourceEnv(t *testing.T) {
	t.Setenv("ACCOUNT_LEDGER_INDEX_SOURCE", "")
	if got := accountLedgerIndexSource(); got != "ducklake" {
		t.Fatalf("default source=%q want ducklake", got)
	}
	t.Setenv("ACCOUNT_LEDGER_INDEX_SOURCE", "postgres")
	if got := accountLedgerIndexSource(); got != "postgres" {
		t.Fatalf("postgres source=%q want postgres", got)
	}
	t.Setenv("ACCOUNT_LEDGER_INDEX_SOURCE", "pg")
	if got := accountLedgerIndexSource(); got != "postgres" {
		t.Fatalf("pg source=%q want postgres", got)
	}
	t.Setenv("ACCOUNT_LEDGER_INDEX_SOURCE", "bogus")
	if got := accountLedgerIndexSource(); got != "ducklake" {
		t.Fatalf("bogus source=%q want ducklake", got)
	}
}

func TestPostgresAccountLedgerIndexLookupUsesCatalogDB(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	reader := &AccountLedgerIndexReader{
		catalogDB:     db,
		source:        "postgres",
		schemaName:    "index",
		tableName:     "account_ledger_index",
		bucketCount:   defaultAccountLedgerIndexBuckets,
		partitionSize: 100000,
	}
	mock.ExpectQuery(`SELECT DISTINCT ledger_range\s+FROM index\.account_ledger_index\s+WHERE account_bucket = \$1\s+AND account_id = \$2\s+AND ledger_range >= \$3\s+AND ledger_range <= \$4\s+ORDER BY ledger_range`).
		WithArgs(AccountLedgerBucket("GA", defaultAccountLedgerIndexBuckets), "GA", int64(1), int64(3)).
		WillReturnRows(sqlmock.NewRows([]string{"ledger_range"}).AddRow(int64(1)).AddRow(int64(3)))

	got, err := reader.LookupLedgerRanges(context.Background(), "GA", 100000, 300000)
	if err != nil {
		t.Fatalf("LookupLedgerRanges: %v", err)
	}
	if len(got) != 2 || got[0] != 1 || got[1] != 3 {
		t.Fatalf("ranges=%v want [1 3]", got)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestAccountLedgerIndexCoverageStates(t *testing.T) {
	tests := []struct {
		name              string
		backfillLedger    int64
		backfillStatus    string
		incrementalLedger int64
		wantStatus        string
		wantComplete      bool
		wantCoveredEnd    int64
		wantCoversFull    bool
		wantCoversPrefix  bool
	}{
		{
			name:              "complete backfill covers through incremental checkpoint",
			backfillLedger:    3449201,
			backfillStatus:    "complete",
			incrementalLedger: 3449500,
			wantStatus:        "complete",
			wantComplete:      true,
			wantCoveredEnd:    3449500,
			wantCoversFull:    true,
			wantCoversPrefix:  true,
		},
		{
			name:             "running backfill only covers completed prefix",
			backfillLedger:   200000,
			backfillStatus:   "running",
			wantStatus:       "backfill_running",
			wantCoveredEnd:   200000,
			wantCoversFull:   false,
			wantCoversPrefix: true,
		},
		{
			name:             "pending backfill has no coverage",
			backfillStatus:   "pending",
			wantStatus:       "uncovered",
			wantCoversFull:   false,
			wantCoversPrefix: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatalf("sqlmock.New: %v", err)
			}
			defer db.Close()
			mock.ExpectQuery("SELECT last_ledger_sequence, status FROM index.account_ledger_backfill_checkpoint WHERE id = 1").
				WillReturnRows(sqlmock.NewRows([]string{"last_ledger_sequence", "status"}).AddRow(tt.backfillLedger, tt.backfillStatus))
			mock.ExpectQuery("SELECT last_ledger_sequence FROM index.account_ledger_transformer_checkpoint WHERE id = 1").
				WillReturnRows(sqlmock.NewRows([]string{"last_ledger_sequence"}).AddRow(tt.incrementalLedger))

			reader := &AccountLedgerIndexReader{
				catalogDB:             db,
				transformerCheckpoint: "index.account_ledger_transformer_checkpoint",
				backfillCheckpoint:    "index.account_ledger_backfill_checkpoint",
				canPrune:              true,
			}
			got := reader.LoadCoverage(context.Background())
			if got.Status != tt.wantStatus || got.Complete != tt.wantComplete || got.CoveredEnd != tt.wantCoveredEnd {
				t.Fatalf("coverage=%+v want status=%s complete=%v covered_end=%d", got, tt.wantStatus, tt.wantComplete, tt.wantCoveredEnd)
			}
			if got.Covers(0, 0) != tt.wantCoversFull {
				t.Fatalf("Covers full=%v want %v for %+v", got.Covers(0, 0), tt.wantCoversFull, got)
			}
			if got.Covers(1, 100000) != tt.wantCoversPrefix {
				t.Fatalf("Covers prefix=%v want %v for %+v", got.Covers(1, 100000), tt.wantCoversPrefix, got)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatalf("unmet expectations: %v", err)
			}
		})
	}
}

func TestPostgresAccountLedgerIndexCoverageUsesMirrorRows(t *testing.T) {
	tests := []struct {
		name           string
		checkpoint     int64
		minFrom        int64
		maxTo          int64
		wantStatus     string
		wantComplete   bool
		wantCoversFull bool
	}{
		{
			name:           "partial mirror does not claim full history",
			checkpoint:     3466943,
			minFrom:        3400000,
			maxTo:          3499999,
			wantStatus:     "partial",
			wantComplete:   false,
			wantCoversFull: false,
		},
		{
			name:           "mirror from ledger one through checkpoint is complete",
			checkpoint:     3466943,
			minFrom:        1,
			maxTo:          3499999,
			wantStatus:     "complete",
			wantComplete:   true,
			wantCoversFull: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatalf("sqlmock.New: %v", err)
			}
			defer db.Close()

			mock.ExpectQuery("SELECT checkpoint FROM index.account_ledger_postgres_checkpoint WHERE sink = \\$1").
				WithArgs("index.account_ledger_index").
				WillReturnRows(sqlmock.NewRows([]string{"checkpoint"}).AddRow(tt.checkpoint))
			mock.ExpectQuery("SELECT MIN\\(ledger_from\\), MAX\\(ledger_to\\) FROM index.account_ledger_index").
				WillReturnRows(sqlmock.NewRows([]string{"min", "max"}).AddRow(tt.minFrom, tt.maxTo))

			reader := &AccountLedgerIndexReader{
				catalogDB:          db,
				source:             "postgres",
				schemaName:         "index",
				tableName:          "account_ledger_index",
				postgresCheckpoint: "index.account_ledger_postgres_checkpoint",
				canPrune:           true,
			}
			got := reader.LoadCoverage(context.Background())
			if got.Status != tt.wantStatus || got.Complete != tt.wantComplete {
				t.Fatalf("coverage=%+v want status=%s complete=%v", got, tt.wantStatus, tt.wantComplete)
			}
			if got.Covers(0, 0) != tt.wantCoversFull {
				t.Fatalf("Covers full=%v want %v for %+v", got.Covers(0, 0), tt.wantCoversFull, got)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatalf("unmet expectations: %v", err)
			}
		})
	}
}

func TestGetServingAccountTransactionsUsesWatermarkAndTOIDCursor(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	hot := &SilverHotReader{db: db, network: "testnet"}
	closed := time.Date(2026, 1, 1, 0, 0, 4, 0, time.UTC)

	mock.ExpectQuery("SELECT status, complete_from, complete_thru").
		WillReturnRows(sqlmock.NewRows([]string{"status", "complete_from", "complete_thru"}).AddRow("complete", int64(1), int64(10)))
	mock.ExpectQuery("FROM serving.sv_transactions_by_account").
		WithArgs("GA", int64(1), int64(10), 2).
		WillReturnRows(sqlmock.NewRows([]string{
			"ledger_sequence", "closed_at", "tx_hash", "toid", "successful", "source_account",
			"fee_charged", "memo_type", "memo_value", "activity_type", "source_table", "summary",
		}).AddRow(int64(4), closed, "tx4", int64(17179873280), true, "GA", "101", "text", "memo", "payment", "sv_transactions_by_account", "Observed account transaction from serving feed").
			AddRow(int64(3), closed.Add(-time.Second), "tx3", int64(12884905984), true, "GA", "100", "none", nil, "payment", "sv_transactions_by_account", "Observed account transaction from serving feed"))

	got, cursor, hasMore, covered, err := hot.GetServingAccountTransactions(context.Background(), AccountTransactionsFilters{AccountID: "GA", StartLedger: 1, EndLedger: 10, Limit: 1, Order: "desc"})
	if err != nil {
		t.Fatalf("GetServingAccountTransactions: %v", err)
	}
	if !covered || !hasMore || cursor == "" {
		t.Fatalf("covered=%v hasMore=%v cursor=%q", covered, hasMore, cursor)
	}
	if len(got) != 1 || got[0].TransactionHash != "tx4" || got[0].ActivityTypes[0] != "payment" || got[0].TransactionID != 17179873280 {
		t.Fatalf("unexpected feed rows: %#v", got)
	}
	decoded, err := DecodeHistoryCursor(cursor)
	if err != nil {
		t.Fatalf("DecodeHistoryCursor: %v", err)
	}
	if decoded.TieBreaker != "17179873280" {
		t.Fatalf("cursor tie breaker = %q, want TOID", decoded.TieBreaker)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestGetServingAccountTransactionsFallsBackForNonTOIDCursor(t *testing.T) {
	for name, tieBreaker := range map[string]string{
		"federated cursor without tie-breaker": "",
		"non-numeric tie-breaker":              "42|token_transfers_raw|deadbeef|0",
	} {
		t.Run(name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatalf("sqlmock.New: %v", err)
			}
			defer db.Close()

			hot := &SilverHotReader{db: db, network: "testnet"}
			mock.ExpectQuery("SELECT status, complete_from, complete_thru").
				WillReturnRows(sqlmock.NewRows([]string{"status", "complete_from", "complete_thru"}).AddRow("complete", int64(1), int64(10)))

			cursor := &HistoryCursor{LedgerSequence: 5, TransactionHash: "tx5", TieBreaker: tieBreaker}
			got, next, hasMore, covered, err := hot.GetServingAccountTransactions(context.Background(), AccountTransactionsFilters{
				AccountID: "GA", StartLedger: 1, EndLedger: 10, Limit: 5, Order: "desc", Cursor: cursor,
			})
			if err != nil {
				t.Fatalf("GetServingAccountTransactions: %v", err)
			}
			if covered || hasMore || next != "" || len(got) != 0 {
				t.Fatalf("expected fallback to federated path, got covered=%v hasMore=%v next=%q rows=%d", covered, hasMore, next, len(got))
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatalf("unmet expectations: %v", err)
			}
		})
	}
}

func TestGetAccountTransactionsAccountIndexSkipsColdWhenCoveredAndNoRanges(t *testing.T) {
	db := newAccountHistoryDuckDB(t)
	defer db.Close()
	addColdLedgerRangeColumns(t, db)
	setupAccountLedgerIndexTable(t, db)
	reader := newAccountIndexPruningReader(db, AccountLedgerIndexCoverage{Enabled: true, PruningEnabled: true, Status: "complete", Complete: true, CoveredStart: 1, CoveredEnd: 200000})
	if _, err := db.Exec(`INSERT INTO cold.enriched_history_operations VALUES (1, TIMESTAMP '2026-01-01 00:00:01', 'tx1', true, 'GA', 'GB', NULL, NULL, NULL, '100', 'none', NULL, true, false, 'payment', 0)`); err != nil {
		t.Fatalf("fixture insert: %v", err)
	}

	got, _, _, coverage, err := reader.GetAccountTransactions(context.Background(), AccountTransactionsFilters{AccountID: "GA", Limit: 10, Order: "desc"})
	if err != nil {
		t.Fatalf("GetAccountTransactions: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("covered empty lookup should skip cold arm, got %#v", got)
	}
	if !coverage.Used || !coverage.SkippedCold || coverage.Uncovered {
		t.Fatalf("unexpected coverage: %+v", coverage)
	}
}

func TestGetAccountTransactionsAccountIndexPrunesCoveredRanges(t *testing.T) {
	db := newAccountHistoryDuckDB(t)
	defer db.Close()
	addColdLedgerRangeColumns(t, db)
	setupAccountLedgerIndexTable(t, db)
	reader := newAccountIndexPruningReader(db, AccountLedgerIndexCoverage{Enabled: true, PruningEnabled: true, Status: "complete", Complete: true, CoveredStart: 1, CoveredEnd: 200000})
	for _, stmt := range []string{
		`INSERT INTO cold.enriched_history_operations VALUES (1, TIMESTAMP '2026-01-01 00:00:01', 'tx_range0', true, 'GA', 'GB', NULL, NULL, NULL, '100', 'none', NULL, true, false, 'payment', 0)`,
		`INSERT INTO cold.enriched_history_operations VALUES (150000, TIMESTAMP '2026-01-01 00:00:02', 'tx_range1', true, 'GA', 'GB', NULL, NULL, NULL, '100', 'none', NULL, true, false, 'payment', 1)`,
		fmt.Sprintf(`INSERT INTO memory.index.account_ledger_index VALUES ('GA', %d, 1)`, AccountLedgerBucket("GA", defaultAccountLedgerIndexBuckets)),
	} {
		if _, err := db.Exec(stmt); err != nil {
			t.Fatalf("fixture insert: %v\n%s", err, stmt)
		}
	}

	got, _, _, coverage, err := reader.GetAccountTransactions(context.Background(), AccountTransactionsFilters{AccountID: "GA", Limit: 10, Order: "desc"})
	if err != nil {
		t.Fatalf("GetAccountTransactions: %v", err)
	}
	if len(got) != 1 || got[0].TransactionHash != "tx_range1" {
		t.Fatalf("unexpected pruned transactions: %#v", got)
	}
	if !coverage.Used || coverage.SkippedCold || coverage.Uncovered || len(coverage.PrunedRanges) != 1 || coverage.PrunedRanges[0] != 1 {
		t.Fatalf("unexpected coverage: %+v", coverage)
	}
}

func TestGetAccountTransactionsAccountIndexFallsBackWhenUncovered(t *testing.T) {
	db := newAccountHistoryDuckDB(t)
	defer db.Close()
	addColdLedgerRangeColumns(t, db)
	setupAccountLedgerIndexTable(t, db)
	reader := newAccountIndexPruningReader(db, AccountLedgerIndexCoverage{Enabled: true, PruningEnabled: true, Status: "backfill_running", CoveredStart: 1, CoveredEnd: 50})
	if _, err := db.Exec(`INSERT INTO cold.enriched_history_operations VALUES (1000, TIMESTAMP '2026-01-01 00:00:01', 'tx_uncovered', true, 'GA', 'GB', NULL, NULL, NULL, '100', 'none', NULL, true, false, 'payment', 0)`); err != nil {
		t.Fatalf("fixture insert: %v", err)
	}

	got, _, _, coverage, err := reader.GetAccountTransactions(context.Background(), AccountTransactionsFilters{AccountID: "GA", Limit: 10, Order: "desc", EndLedger: 1000})
	if err != nil {
		t.Fatalf("GetAccountTransactions: %v", err)
	}
	if len(got) != 1 || got[0].TransactionHash != "tx_uncovered" {
		t.Fatalf("uncovered span should fall back to unpruned cold query, got %#v", got)
	}
	if coverage.Used || !coverage.Uncovered {
		t.Fatalf("unexpected coverage: %+v", coverage)
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

func addColdLedgerRangeColumns(t *testing.T, db *sql.DB) {
	t.Helper()
	for _, table := range []string{"enriched_history_operations", "token_transfers_raw", "contract_invocations_raw"} {
		if _, err := db.Exec(`ALTER TABLE memory.cold.` + table + ` ADD COLUMN ledger_range BIGINT DEFAULT 0`); err != nil {
			t.Fatalf("add ledger_range to %s: %v", table, err)
		}
	}
}

func setupAccountLedgerIndexTable(t *testing.T, db *sql.DB) {
	t.Helper()
	for _, stmt := range []string{
		`CREATE SCHEMA index`,
		`CREATE TABLE memory.index.account_ledger_index (account_id VARCHAR, account_bucket BIGINT, ledger_range BIGINT)`,
	} {
		if _, err := db.Exec(stmt); err != nil {
			t.Fatalf("setup account index: %v\n%s", err, stmt)
		}
	}
}

func newAccountIndexPruningReader(db *sql.DB, coverage AccountLedgerIndexCoverage) *UnifiedDuckDBReader {
	return &UnifiedDuckDBReader{
		db:         db,
		hotSchema:  "memory.hot",
		coldSchema: "memory.cold",
		accountIndex: &AccountLedgerIndexReader{
			db:                       db,
			catalogName:              "memory",
			schemaName:               "index",
			tableName:                "account_ledger_index",
			bucketCount:              defaultAccountLedgerIndexBuckets,
			partitionSize:            defaultAccountLedgerIndexPartitionSize,
			canPrune:                 true,
			coverageCache:            coverage,
			coverageCacheRefreshedAt: time.Now(),
		},
	}
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
