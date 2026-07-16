package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
)

func TestHandleAccountOverviewUsesCoveredServingFeeds(t *testing.T) {
	t.Setenv("ACCOUNT_TX_FEED_ENABLED", "true")

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	accountID := "GAACCOUNT"
	closedAt := time.Date(2026, 7, 15, 17, 0, 0, 0, time.UTC)
	createdAt := "2026-07-01T12:00:00Z"

	mock.ExpectQuery("FROM accounts_current").
		WithArgs(accountID).
		WillReturnRows(sqlmock.NewRows([]string{
			"account_id", "balance", "sequence_number", "num_subentries", "num_sponsoring",
			"num_sponsored", "last_modified_ledger", "sequence_ledger", "sequence_time",
			"updated_at", "home_domain", "sponsor_account", "auth_required", "auth_revocable",
			"auth_immutable", "auth_clawback_enabled",
		}).AddRow(accountID, "12.5000000", "123", int64(1), int64(0), int64(0), int64(100), int64(100), int64(1), closedAt.Format(time.RFC3339), nil, nil, nil, nil, nil, nil))

	mock.ExpectQuery("WHERE table_name = 'serving.sv_transactions_by_account'").
		WillReturnRows(sqlmock.NewRows([]string{"status", "complete_from", "complete_thru"}).AddRow("complete", int64(3), int64(100)))
	mock.ExpectQuery("SELECT COALESCE\\(MAX\\(ledger_sequence\\), 0\\)").
		WillReturnRows(sqlmock.NewRows([]string{"latest"}).AddRow(int64(100)))
	mock.ExpectQuery("FROM serving.sv_transactions_by_account").
		WithArgs(accountID, int64(3), int64(100), 11).
		WillReturnRows(sqlmock.NewRows([]string{
			"ledger_sequence", "closed_at", "tx_hash", "toid", "successful", "source_account",
			"fee_charged", "memo_type", "memo_value", "activity_type", "source_table", "summary",
		}).AddRow(int64(100), closedAt, "txhash", int64(429496733696), true, accountID, "100", "none", nil, "payment", "sv_transactions_by_account", "Observed account transaction from serving feed"))

	mock.ExpectQuery("WHERE table_name = 'serving.sv_operations_by_account'").
		WillReturnRows(sqlmock.NewRows([]string{"status", "complete_from", "complete_thru"}).AddRow("complete", int64(3), int64(100)))
	mock.ExpectQuery("SELECT COALESCE\\(MAX\\(ledger_sequence\\), 0\\)").
		WillReturnRows(sqlmock.NewRows([]string{"latest"}).AddRow(int64(100)))
	mock.ExpectQuery("FROM serving.sv_operations_by_account").
		WithArgs(accountID, int64(3), int64(100), sqlmock.AnyArg(), sqlmock.AnyArg(), 20).
		WillReturnRows(sqlmock.NewRows([]string{
			"closed_at", "tx_hash", "ledger_sequence", "source_account", "destination_account",
			"asset_key", "amount_stroops", "contract_id", "is_soroban_op", "successful",
		}).AddRow(closedAt, "txhash", int64(100), accountID, "GBDESTINATION", "native:XLM", int64(25000000), nil, false, true))

	mock.ExpectQuery("FROM serving.sv_accounts_current").
		WithArgs(accountID).
		WillReturnRows(sqlmock.NewRows([]string{
			"account_id", "balance_stroops", "sequence_number", "num_subentries", "num_sponsoring",
			"num_sponsored", "last_modified_ledger", "sequence_ledger", "sequence_time", "updated_at",
			"home_domain", "created_at", "sponsor", "auth_required", "auth_revocable", "auth_immutable",
			"auth_clawback_enabled",
		}).AddRow(accountID, int64(125000000), int64(123), int64(1), int64(0), int64(0), int64(100), int64(100), int64(1), closedAt, nil, createdAt, nil, nil, nil, nil, nil))

	hot := &SilverHotReader{db: db, network: "testnet"}
	handlers := NewSilverHandlers(NewUnifiedSilverReader(hot, nil), nil, ReaderModeUnified)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/silver/explorer/account?account_id="+accountID, nil)
	rec := httptest.NewRecorder()

	handlers.HandleAccountOverview(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	var got struct {
		Account          *AccountCurrent     `json:"account"`
		RecentOperations []EnrichedOperation `json:"recent_operations"`
		RecentTransfers  []TokenTransfer     `json:"recent_transfers"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if got.Account == nil || got.Account.Balance != "12.5000000" || got.Account.CreatedAt == nil || *got.Account.CreatedAt != createdAt {
		t.Fatalf("account = %#v", got.Account)
	}
	if len(got.RecentOperations) != 1 || got.RecentOperations[0].TransactionHash != "txhash" {
		t.Fatalf("recent operations = %#v", got.RecentOperations)
	}
	if len(got.RecentTransfers) != 1 || got.RecentTransfers[0].Amount == nil || *got.RecentTransfers[0].Amount != "2.5000000" {
		t.Fatalf("recent transfers = %#v", got.RecentTransfers)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}
