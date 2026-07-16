package main

import (
	"context"
	"strconv"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/stellar/go-stellar-sdk/toid"
)

var scanLedgerColumns = []string{
	"sequence", "ledger_hash", "previous_ledger_hash", "transaction_count",
	"operation_count", "successful_tx_count", "failed_tx_count",
	"tx_set_operation_count", "closed_at", "total_coins", "fee_pool",
	"base_fee", "base_reserve", "max_tx_set_size", "protocol_version",
	"ledger_header", "soroban_fee_write_1kb", "node_id", "signature",
	"ledger_range", "era_id", "version_label", "soroban_op_count",
	"total_fee_charged", "contract_events_count", "created_at",
}

func scanLedgerRow(rows *sqlmock.Rows, sequence int64, closedAt time.Time) *sqlmock.Rows {
	seq := strconv.FormatInt(sequence, 10)
	return rows.AddRow(
		sequence, "hash-"+seq, "prev-"+seq, int64(10),
		int64(25), int64(9), int64(1),
		int64(25), closedAt, int64(1054439020873472922), int64(3145593103143089),
		int64(100), int64(5000000), int64(200), int64(23),
		"AAAA-header", nil, nil, nil,
		(sequence/10000)*10000, nil, nil, nil,
		nil, nil, closedAt,
	)
}

func TestHorizonLedgerPagingToken(t *testing.T) {
	if got, want := horizonLedgerPagingToken(456), toid.New(456, 0, 0).String(); got != want {
		t.Fatalf("paging token = %q, want %q", got, want)
	}
	// Beyond int32 range there is no valid TOID; the raw sequence is the fallback.
	if got := horizonLedgerPagingToken(int64(1) << 33); got != "8589934592" {
		t.Fatalf("out-of-range paging token = %q", got)
	}
}

func TestHorizonLedgerSequenceFromCursor(t *testing.T) {
	for _, empty := range []string{"", "now"} {
		seq, has, err := horizonLedgerSequenceFromCursor(empty)
		if err != nil || has || seq != 0 {
			t.Fatalf("cursor(%q) = (%d,%v,%v), want no cursor", empty, seq, has, err)
		}
	}

	// A raw ledger sequence (below 2^32) passes through.
	seq, has, err := horizonLedgerSequenceFromCursor("456")
	if err != nil || !has || seq != 456 {
		t.Fatalf("raw cursor = (%d,%v,%v)", seq, has, err)
	}

	// A TOID paging token (at or above 2^32) resolves to its ledger part.
	token := toid.New(456, 0, 0).String()
	seq, has, err = horizonLedgerSequenceFromCursor(token)
	if err != nil || !has || seq != 456 {
		t.Fatalf("toid cursor = (%d,%v,%v)", seq, has, err)
	}

	if _, _, err := horizonLedgerSequenceFromCursor("garbage"); err == nil {
		t.Fatal("garbage cursor should error")
	}
	if _, _, err := horizonLedgerSequenceFromCursor("-5"); err == nil {
		t.Fatal("negative cursor should error")
	}
}

func TestInt64FromMapHandlesDecimalStrings(t *testing.T) {
	row := map[string]interface{}{
		"int":         int64(42),
		"decimal_str": "1054439020873472922.0000000",
		"plain_str":   "123",
		"garbage":     "not-a-number",
		"nil":         nil,
	}
	if got := int64FromMap(row, "int"); got != 42 {
		t.Fatalf("int = %d", got)
	}
	// Stroop totals exceed float64's exact-integer range; the decimal part must
	// be truncated losslessly, not through a float round-trip or a silent 0.
	if got := int64FromMap(row, "decimal_str"); got != 1054439020873472922 {
		t.Fatalf("decimal_str = %d, want 1054439020873472922", got)
	}
	if got := int64FromMap(row, "plain_str"); got != 123 {
		t.Fatalf("plain_str = %d", got)
	}
	if got := int64FromMap(row, "garbage"); got != 0 {
		t.Fatalf("garbage = %d", got)
	}
	if got := int64FromMap(row, "nil"); got != 0 {
		t.Fatalf("nil = %d", got)
	}
}

func TestHorizonLedgerFromMap(t *testing.T) {
	closedAt := time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC)
	ledger := horizonLedgerFromMap(map[string]interface{}{
		"sequence":               int64(456),
		"ledger_hash":            "abc",
		"previous_ledger_hash":   "def",
		"successful_tx_count":    int64(9),
		"failed_tx_count":        int64(1),
		"operation_count":        int64(25),
		"tx_set_operation_count": int64(26),
		"closed_at":              closedAt,
		"total_coins":            int64(1054439020873472922),
		"fee_pool":               int64(3145593103143089),
		"base_fee":               int64(100),
		"base_reserve":           int64(5000000),
		"max_tx_set_size":        int64(200),
		"protocol_version":       int64(23),
		"ledger_header":          "AAAA-header",
	})
	if ledger.Sequence != 456 || ledger.Hash != "abc" || ledger.PrevHash != "def" {
		t.Fatalf("identity fields = %+v", ledger)
	}
	if ledger.PT != toid.New(456, 0, 0).String() {
		t.Fatalf("paging token = %q", ledger.PT)
	}
	if ledger.SuccessfulTransactionCount != 9 || ledger.FailedTransactionCount == nil || *ledger.FailedTransactionCount != 1 {
		t.Fatalf("tx counts = %+v", ledger)
	}
	if ledger.TotalCoins != "105443902087.3472922" || ledger.FeePool != "314559310.3143089" {
		t.Fatalf("amounts = coins:%q pool:%q", ledger.TotalCoins, ledger.FeePool)
	}
	if ledger.ProtocolVersion != 23 || ledger.HeaderXDR != "AAAA-header" || !ledger.ClosedAt.Equal(closedAt) {
		t.Fatalf("remaining fields = %+v", ledger)
	}
}

func TestHorizonLedgerReaderGetLedgersDescWindowsCursor(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	closedAt := time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC)
	// ledgerBounds via hot watermarks.
	mock.ExpectQuery(`SELECT MIN\(sequence\) FROM ledgers_row_v2`).
		WillReturnRows(sqlmock.NewRows([]string{"min"}).AddRow(int64(100)))
	mock.ExpectQuery(`SELECT MAX\(sequence\) FROM ledgers_row_v2`).
		WillReturnRows(sqlmock.NewRows([]string{"max"}).AddRow(int64(500)))
	// Cursor at ledger 400 with order=desc must scan 100..399: the cursor
	// ledger is excluded, off-by-one here repeats or skips it on every page.
	rangeStart, rangeEnd := ledgerRangeBounds(100, 399)
	rows := sqlmock.NewRows(scanLedgerColumns)
	rows = scanLedgerRow(rows, 399, closedAt)
	rows = scanLedgerRow(rows, 398, closedAt)
	mock.ExpectQuery("ORDER BY sequence DESC").
		WithArgs(int64(100), int64(399), rangeStart, rangeEnd, 2).
		WillReturnRows(rows)

	reader := &HorizonLedgerReader{queryService: &QueryService{hot: &HotReader{db: db}}}
	cursor := toid.New(400, 0, 0).String()
	ledgers, err := reader.GetLedgers(context.Background(), horizonPageQuery{Cursor: cursor, Order: "desc", Limit: 2})
	if err != nil {
		t.Fatalf("GetLedgers: %v", err)
	}
	if len(ledgers) != 2 || ledgers[0].Sequence != 399 || ledgers[1].Sequence != 398 {
		t.Fatalf("ledgers = %+v", ledgers)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestHorizonLedgerReaderGetLedgersAscStartsAfterCursor(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	closedAt := time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC)
	mock.ExpectQuery(`SELECT MIN\(sequence\) FROM ledgers_row_v2`).
		WillReturnRows(sqlmock.NewRows([]string{"min"}).AddRow(int64(100)))
	mock.ExpectQuery(`SELECT MAX\(sequence\) FROM ledgers_row_v2`).
		WillReturnRows(sqlmock.NewRows([]string{"max"}).AddRow(int64(500)))
	rangeStart, rangeEnd := ledgerRangeBounds(401, 500)
	rows := sqlmock.NewRows(scanLedgerColumns)
	rows = scanLedgerRow(rows, 401, closedAt)
	mock.ExpectQuery("ORDER BY sequence ASC").
		WithArgs(int64(401), int64(500), rangeStart, rangeEnd, 1).
		WillReturnRows(rows)

	reader := &HorizonLedgerReader{queryService: &QueryService{hot: &HotReader{db: db}}}
	cursor := toid.New(400, 0, 0).String()
	ledgers, err := reader.GetLedgers(context.Background(), horizonPageQuery{Cursor: cursor, Order: "asc", Limit: 1})
	if err != nil {
		t.Fatalf("GetLedgers: %v", err)
	}
	if len(ledgers) != 1 || ledgers[0].Sequence != 401 {
		t.Fatalf("ledgers = %+v", ledgers)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}
