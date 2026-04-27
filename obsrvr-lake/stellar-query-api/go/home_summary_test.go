package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"regexp"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
)

func TestHandleExplorerSummaryUnavailable(t *testing.T) {
	h := NewExplorerHomeSummaryHandler(nil, nil, "testnet")
	req := httptest.NewRequest(http.MethodGet, "/api/v1/explorer/summary", nil)
	w := httptest.NewRecorder()

	h.HandleExplorerSummary(w, req)

	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected status %d, got %d", http.StatusServiceUnavailable, w.Code)
	}
}

func TestHandleExplorerSummaryHotOnlySuccess(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	hot := &SilverHotReader{db: db, network: "testnet"}
	h := NewExplorerHomeSummaryHandler(hot, nil, "testnet")

	now := time.Date(2026, 4, 21, 12, 0, 0, 0, time.UTC)
	closedAt := now.Add(-10 * time.Second)

	mock.ExpectQuery(regexp.QuoteMeta(`
		SELECT generated_at, latest_ledger, latest_ledger_closed_at, avg_close_time_seconds,
		       protocol_version, tx_24h_total, tx_24h_failed, active_accounts_24h, created_accounts_24h
		FROM serving.sv_network_stats_current
		WHERE network = $1
		LIMIT 1
	`)).WithArgs("testnet").WillReturnRows(sqlmock.NewRows([]string{
		"generated_at", "latest_ledger", "latest_ledger_closed_at", "avg_close_time_seconds",
		"protocol_version", "tx_24h_total", "tx_24h_failed", "active_accounts_24h", "created_accounts_24h",
	}).AddRow(now, int64(2144030), closedAt, 5.0, int64(23), int64(1200), int64(12), int64(345), int64(9)))

	mock.ExpectQuery(regexp.QuoteMeta(`SELECT COUNT(*) FROM serving.sv_accounts_current`)).WillReturnRows(
		sqlmock.NewRows([]string{"count"}).AddRow(int64(9999)),
	)

	mock.ExpectQuery(regexp.QuoteMeta(`SELECT COALESCE(MAX(ledger_sequence), 0) FROM serving.sv_ledger_stats_recent`)).WillReturnRows(
		sqlmock.NewRows([]string{"max"}).AddRow(int64(2144030)),
	)

	mock.ExpectQuery(regexp.QuoteMeta(`
		SELECT ledger_sequence, closed_at, COALESCE(ledger_hash, ''), COALESCE(prev_hash, ''),
		       COALESCE(protocol_version, 0), COALESCE(base_fee_stroops, 0),
		       COALESCE(successful_tx_count, 0), COALESCE(failed_tx_count, 0), COALESCE(operation_count, 0)
		FROM serving.sv_ledger_stats_recent
		ORDER BY ledger_sequence DESC
		LIMIT $1
	`)).WithArgs(20).WillReturnRows(sqlmock.NewRows([]string{
		"ledger_sequence", "closed_at", "ledger_hash", "prev_hash", "protocol_version", "base_fee_stroops", "successful_tx_count", "failed_tx_count", "operation_count",
	}).AddRow(int64(2144030), closedAt, "abc", "def", 23, int64(100), 11, 1, 44).
		AddRow(int64(2144029), closedAt.Add(-5*time.Second), "abc2", "def2", 23, int64(100), 10, 0, 30))

	mock.ExpectQuery(`(?s)SELECT COUNT\(\*\)\s+FROM serving\.sv_contract_stats_current\s+WHERE COALESCE\(total_calls_24h, 0\) > 0`).WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(int64(1)))

	req := httptest.NewRequest(http.MethodGet, "/api/v1/home/summary", nil)
	w := httptest.NewRecorder()

	h.HandleExplorerSummary(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, w.Code, w.Body.String())
	}

	var resp ExplorerHomeSummaryResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("json.Unmarshal: %v\nbody=%s", err, w.Body.String())
	}

	if resp.Network != "testnet" {
		t.Fatalf("expected network testnet, got %q", resp.Network)
	}
	if resp.Provenance.Route != "/api/v1/home/summary" {
		t.Fatalf("expected route provenance to match request path, got %q", resp.Provenance.Route)
	}
	if resp.Provenance.DataSource != "silver_hot" {
		t.Fatalf("expected silver_hot datasource, got %q", resp.Provenance.DataSource)
	}
	if !resp.Provenance.Partial {
		t.Fatalf("expected partial=true for hot-only summary")
	}
	if resp.Header.LatestLedgerSequence != 2144030 {
		t.Fatalf("expected latest ledger 2144030, got %d", resp.Header.LatestLedgerSequence)
	}
	if resp.Hero.Cadence.TxPerLedgerRecentAvg != 11 {
		t.Fatalf("expected tx_per_ledger_recent_avg 11, got %d", resp.Hero.Cadence.TxPerLedgerRecentAvg)
	}
	if resp.Hero.Contracts.Active24h != 1 {
		t.Fatalf("expected active_24h 1, got %d", resp.Hero.Contracts.Active24h)
	}
	// TTL-backed sections are best-effort in the hot-only test harness.
	if len(resp.Leaders) != 0 {
		t.Fatalf("expected 0 leaders in hot-only warning case, got %d", len(resp.Leaders))
	}
	if resp.Hero.Health.Status == "" {
		t.Fatalf("expected non-empty health status")
	}
	if len(resp.Provenance.Warnings) == 0 {
		t.Fatalf("expected warnings for missing unified reader")
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet SQL expectations: %v", err)
	}
}

func TestSplitProtocolAndContract(t *testing.T) {
	protocol, contract := splitProtocolAndContract("Blend · Lending Pool", "C123")
	if protocol != "Blend" || contract != "Lending Pool" {
		t.Fatalf("unexpected split: %q / %q", protocol, contract)
	}
}
