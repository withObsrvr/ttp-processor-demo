package main

import (
	"context"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestHorizonAccountReaderPreservesServingTrustlineBalancesWhenSignersExist(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	updatedAt := time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC)
	mock.ExpectQuery("FROM serving.sv_accounts_current").
		WithArgs("GA").
		WillReturnRows(sqlmock.NewRows([]string{
			"account_id", "balance_stroops", "sequence_number", "num_subentries",
			"num_sponsoring", "num_sponsored", "last_modified_ledger", "sequence_ledger",
			"sequence_time", "updated_at", "home_domain", "created_at", "sponsor",
			"auth_required", "auth_revocable", "auth_immutable", "auth_clawback_enabled",
		}).AddRow("GA", int64(1000000000), int64(123), int64(2), int64(1), int64(3), int64(50), int64(49), int64(1783699200), updatedAt, nil, nil, nil, false, false, false, false))
	mock.ExpectQuery("FROM serving.sv_account_balances_current").
		WithArgs("GA").
		WillReturnRows(sqlmock.NewRows([]string{
			"asset_type", "asset_code", "asset_issuer", "balance_stroops", "limit_stroops",
			"is_authorized", "buying_liabilities_stroops", "selling_liabilities_stroops",
			"is_authorized_to_maintain_liabilities", "is_clawback_enabled", "sponsor", "last_modified_ledger",
		}).AddRow("native", "XLM", nil, int64(1000000000), nil, nil, nil, nil, nil, nil, nil, int64(50)).
			AddRow("credit_alphanum4", "USDC", "GISSUER", int64(25000000), int64(1000000000), true, int64(100), int64(200), true, false, nil, int64(50)))
	mock.ExpectQuery("FROM accounts_current").
		WithArgs("GA").
		WillReturnRows(sqlmock.NewRows([]string{
			"account_id", "signers", "master_weight", "low_threshold", "med_threshold", "high_threshold",
		}).AddRow("GA", "[]", 1, 0, 0, 0))

	reader := &HorizonAccountReader{hot: &SilverHotReader{db: db}}
	account, err := reader.GetHorizonAccount(context.Background(), "GA")
	if err != nil {
		t.Fatalf("GetHorizonAccount: %v", err)
	}
	if len(account.Balances) != 2 {
		t.Fatalf("balances = %#v, want native plus trustline", account.Balances)
	}
	foundUSDC := false
	for _, balance := range account.Balances {
		if balance.Asset.Code == "USDC" && balance.Asset.Issuer == "GISSUER" {
			foundUSDC = true
			if balance.Balance != "2.5000000" || balance.Limit != "100.0000000" || balance.BuyingLiabilities != "0.0000100" || balance.SellingLiabilities != "0.0000200" || balance.IsAuthorized == nil || !*balance.IsAuthorized {
				t.Fatalf("USDC balance = %#v", balance)
			}
		}
	}
	if !foundUSDC {
		t.Fatalf("missing USDC trustline balance: %#v", account.Balances)
	}
	if len(account.Signers) != 1 || account.Signers[0].Key != "GA" {
		t.Fatalf("signers = %#v", account.Signers)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}
