package main

import (
	"context"
	"database/sql"
	"errors"
	"strconv"
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

func TestHorizonAccountReaderInfersMissingSequenceMetadata(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	updatedAt := time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC)
	sequenceClosedAt := updatedAt.Add(-time.Second)
	mock.ExpectQuery("FROM serving.sv_accounts_current").
		WithArgs("GA").
		WillReturnRows(sqlmock.NewRows([]string{
			"account_id", "balance_stroops", "sequence_number", "num_subentries",
			"num_sponsoring", "num_sponsored", "last_modified_ledger", "sequence_ledger",
			"sequence_time", "updated_at", "home_domain", "created_at", "sponsor",
			"auth_required", "auth_revocable", "auth_immutable", "auth_clawback_enabled",
		}).AddRow("GA", int64(1000000000), int64(123), int64(0), int64(0), int64(0), int64(50), nil, nil, updatedAt, nil, nil, nil, false, false, false, false))
	mock.ExpectQuery("FROM accounts_current").
		WithArgs("GA").
		WillReturnError(sql.ErrNoRows)
	mock.ExpectQuery("FROM serving.sv_ledger_stats_recent").
		WithArgs(int64(50)).
		WillReturnRows(sqlmock.NewRows([]string{"closed_at"}).AddRow(sequenceClosedAt))
	mock.ExpectQuery("FROM serving.sv_account_balances_current").
		WithArgs("GA").
		WillReturnRows(sqlmock.NewRows([]string{
			"asset_type", "asset_code", "asset_issuer", "balance_stroops", "limit_stroops",
			"is_authorized", "buying_liabilities_stroops", "selling_liabilities_stroops",
			"is_authorized_to_maintain_liabilities", "is_clawback_enabled", "sponsor", "last_modified_ledger",
		}).AddRow("native", "XLM", nil, int64(1000000000), nil, nil, nil, nil, nil, nil, nil, int64(50)))
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
	if account.SequenceLedger != 50 || account.SequenceTime != strconv.FormatInt(sequenceClosedAt.Unix(), 10) {
		t.Fatalf("sequence metadata = (%d,%q), want inferred values", account.SequenceLedger, account.SequenceTime)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestHorizonAccountCurrentUsesHotFallbackAfterUnifiedSequenceSchemaGap(t *testing.T) {
	servingDB, servingMock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New serving: %v", err)
	}
	defer servingDB.Close()
	unifiedDB, unifiedMock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New unified: %v", err)
	}
	defer unifiedDB.Close()

	updatedAt := time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC)
	servingMock.ExpectQuery("FROM serving.sv_accounts_current").
		WithArgs("GA").
		WillReturnRows(sqlmock.NewRows([]string{
			"account_id", "balance_stroops", "sequence_number", "num_subentries",
			"num_sponsoring", "num_sponsored", "last_modified_ledger", "sequence_ledger",
			"sequence_time", "updated_at", "home_domain", "created_at", "sponsor",
			"auth_required", "auth_revocable", "auth_immutable", "auth_clawback_enabled",
		}).AddRow("GA", int64(1000000000), int64(123), int64(0), int64(0), int64(0), int64(50), nil, nil, updatedAt, nil, nil, nil, false, false, false, false))
	unifiedMock.ExpectQuery("SELECT account_id, balance, sequence_number, num_subentries").
		WithArgs("GA").
		WillReturnError(errors.New(`Binder Error: Referenced column "sequence_ledger" not found in FROM clause`))
	// The unified reader retries with cold columns null-cast; here the hot arm is
	// missing the columns too, so the retry fails as well and the account reader
	// falls back to hot silver.
	unifiedMock.ExpectQuery(`NULL::BIGINT AS sequence_ledger`).
		WithArgs("GA").
		WillReturnError(errors.New(`Binder Error: Referenced column "sequence_ledger" not found in FROM clause`))
	servingMock.ExpectQuery("FROM accounts_current").
		WithArgs("GA").
		WillReturnRows(sqlmock.NewRows([]string{
			"account_id", "balance", "sequence_number", "num_subentries",
			"num_sponsoring", "num_sponsored", "last_modified_ledger", "sequence_ledger",
			"sequence_time", "updated_at", "home_domain", "sponsor_account",
			"auth_required", "auth_revocable", "auth_immutable", "auth_clawback_enabled",
		}).AddRow("GA", "1000000000", "123", int64(0), int64(0), int64(0), int64(50), int64(50), int64(1783699200), "2026-07-10T12:00:00Z", nil, nil, false, false, false, false))

	reader := &HorizonAccountReader{
		hot:     &SilverHotReader{db: servingDB},
		unified: &UnifiedDuckDBReader{db: unifiedDB, hotSchema: "hot", coldSchema: "cold"},
	}
	account, err := reader.currentAccount(context.Background(), "GA")
	if err != nil {
		t.Fatalf("currentAccount: %v", err)
	}
	if account == nil || account.AccountID != "GA" || account.SequenceNumber != "123" || account.SequenceLedger != 50 || account.SequenceTime != 1783699200 {
		t.Fatalf("account = %#v, want serving account with hot sequence metadata", account)
	}
	if err := servingMock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet serving expectations: %v", err)
	}
	if err := unifiedMock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet unified expectations: %v", err)
	}
}

func TestMergeAccountCurrentDoesNotFillSequenceMetadataForDifferentSequence(t *testing.T) {
	primary := &AccountCurrent{
		AccountID:           "GA",
		SequenceNumber:      "200",
		LastModifiedLedger:  20,
		SequenceLedger:      0,
		SequenceTime:        0,
		AuthRequired:        testBoolPtr(false),
		AuthRevocable:       testBoolPtr(false),
		AuthImmutable:       testBoolPtr(false),
		AuthClawbackEnabled: testBoolPtr(false),
	}
	fallback := &AccountCurrent{
		AccountID:           "GA",
		SequenceNumber:      "199",
		LastModifiedLedger:  10,
		SequenceLedger:      9,
		SequenceTime:        1000,
		AuthRequired:        testBoolPtr(false),
		AuthRevocable:       testBoolPtr(false),
		AuthImmutable:       testBoolPtr(false),
		AuthClawbackEnabled: testBoolPtr(false),
	}

	merged := mergeAccountCurrent(primary, fallback)
	if merged.SequenceLedger != 0 || merged.SequenceTime != 0 {
		t.Fatalf("merged sequence metadata = (%d,%d), want zero values for mismatched sequence", merged.SequenceLedger, merged.SequenceTime)
	}
}

func testBoolPtr(v bool) *bool {
	return &v
}

func horizonAccountReaderTestAccountRow(mock sqlmock.Sqlmock, signersJSON string, masterWeight int) {
	updatedAt := time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC)
	mock.ExpectQuery("FROM serving.sv_accounts_current").
		WithArgs("GA").
		WillReturnRows(sqlmock.NewRows([]string{
			"account_id", "balance_stroops", "sequence_number", "num_subentries",
			"num_sponsoring", "num_sponsored", "last_modified_ledger", "sequence_ledger",
			"sequence_time", "updated_at", "home_domain", "created_at", "sponsor",
			"auth_required", "auth_revocable", "auth_immutable", "auth_clawback_enabled",
		}).AddRow("GA", int64(1000000000), int64(123), int64(0), int64(0), int64(0), int64(50), int64(49), int64(1783699200), updatedAt, nil, nil, nil, false, false, false, false))
	mock.ExpectQuery("FROM serving.sv_ledger_stats_recent").
		WithArgs(int64(49)).
		WillReturnRows(sqlmock.NewRows([]string{"closed_at"}).AddRow(updatedAt))
	mock.ExpectQuery("FROM serving.sv_account_balances_current").
		WithArgs("GA").
		WillReturnRows(sqlmock.NewRows([]string{
			"asset_type", "asset_code", "asset_issuer", "balance_stroops", "limit_stroops",
			"is_authorized", "buying_liabilities_stroops", "selling_liabilities_stroops",
			"is_authorized_to_maintain_liabilities", "is_clawback_enabled", "sponsor", "last_modified_ledger",
		}).AddRow("native", "XLM", nil, int64(1000000000), nil, nil, nil, nil, nil, nil, nil, int64(50)))
	mock.ExpectQuery("FROM accounts_current").
		WithArgs("GA").
		WillReturnRows(sqlmock.NewRows([]string{
			"account_id", "signers", "master_weight", "low_threshold", "med_threshold", "high_threshold",
		}).AddRow("GA", signersJSON, masterWeight, 1, 2, 3))
}

func TestHorizonAccountReaderServesLockedAccountWithoutFabricatedSigner(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	// Master weight 0 and no extra signers: a locked account. The reader must
	// serve the empty signer set rather than fabricating a weight-1 master key.
	horizonAccountReaderTestAccountRow(mock, "[]", 0)

	reader := &HorizonAccountReader{hot: &SilverHotReader{db: db}}
	account, err := reader.GetHorizonAccount(context.Background(), "GA")
	if err != nil {
		t.Fatalf("GetHorizonAccount: %v", err)
	}
	if len(account.Signers) != 0 {
		t.Fatalf("signers = %#v, want empty for locked account", account.Signers)
	}
	if account.Thresholds.LowThreshold != 1 || account.Thresholds.MedThreshold != 2 || account.Thresholds.HighThreshold != 3 {
		t.Fatalf("thresholds = %#v", account.Thresholds)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestHorizonAccountReaderRefusesCorruptSignersJSON(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	horizonAccountReaderTestAccountRow(mock, `[{"key": truncated`, 1)

	reader := &HorizonAccountReader{hot: &SilverHotReader{db: db}}
	_, err = reader.GetHorizonAccount(context.Background(), "GA")
	if err == nil {
		t.Fatal("corrupt signers JSON must not serve fabricated signer data")
	}
}

func TestBuildHorizonAccountShellRefusesUnparseableSequence(t *testing.T) {
	// The legacy/unified paths scan sequence_number as text; a garbled value must
	// not be served as sequence 0.
	_, err := buildHorizonAccountShell(&AccountCurrent{AccountID: "GA", SequenceNumber: "not-a-number"}, "GA")
	if !errors.Is(err, errHorizonAccountDataUnavailable) {
		t.Fatalf("error = %v, want errHorizonAccountDataUnavailable", err)
	}
}

func TestHorizonAccountCurrentFallsBackToHotSilverOnUnifiedTimeout(t *testing.T) {
	servingDB, servingMock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New serving: %v", err)
	}
	defer servingDB.Close()
	unifiedDB, unifiedMock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New unified: %v", err)
	}
	defer unifiedDB.Close()

	updatedAt := time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC)
	servingMock.ExpectQuery("FROM serving.sv_accounts_current").
		WithArgs("GA").
		WillReturnRows(sqlmock.NewRows([]string{
			"account_id", "balance_stroops", "sequence_number", "num_subentries",
			"num_sponsoring", "num_sponsored", "last_modified_ledger", "sequence_ledger",
			"sequence_time", "updated_at", "home_domain", "created_at", "sponsor",
			"auth_required", "auth_revocable", "auth_immutable", "auth_clawback_enabled",
		}).AddRow("GA", int64(1000000000), int64(123), int64(0), int64(0), int64(0), int64(50), nil, nil, updatedAt, nil, nil, nil, false, false, false, false))
	// A cold scan that cannot answer within the interactive deadline must not
	// 504 the whole account lookup when hot silver can still answer.
	unifiedMock.ExpectQuery("SELECT account_id, balance, sequence_number, num_subentries").
		WithArgs("GA").
		WillReturnError(errors.New("unified GetAccountCurrent: context deadline exceeded"))
	servingMock.ExpectQuery("FROM accounts_current").
		WithArgs("GA").
		WillReturnRows(sqlmock.NewRows([]string{
			"account_id", "balance", "sequence_number", "num_subentries",
			"num_sponsoring", "num_sponsored", "last_modified_ledger", "sequence_ledger",
			"sequence_time", "updated_at", "home_domain", "sponsor_account",
			"auth_required", "auth_revocable", "auth_immutable", "auth_clawback_enabled",
		}).AddRow("GA", "1000000000", "123", int64(0), int64(0), int64(0), int64(50), int64(50), int64(1783699200), "2026-07-10T12:00:00Z", nil, nil, false, false, false, false))

	reader := &HorizonAccountReader{
		hot:     &SilverHotReader{db: servingDB},
		unified: &UnifiedDuckDBReader{db: unifiedDB, hotSchema: "hot", coldSchema: "cold"},
	}
	account, err := reader.currentAccount(context.Background(), "GA")
	if err != nil {
		t.Fatalf("currentAccount: %v", err)
	}
	if account == nil || account.AccountID != "GA" || account.SequenceLedger != 50 {
		t.Fatalf("account = %#v, want hot-silver fallback result", account)
	}
}

func TestGetServingAccountCurrentToleratesNullCounters(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	// Backfilled serving rows carry NULL num_subentries/num_sponsoring/
	// num_sponsored; a dormant account's root lookup must not fail on them.
	updatedAt := time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC)
	mock.ExpectQuery("FROM serving.sv_accounts_current").
		WithArgs("GDORMANT").
		WillReturnRows(sqlmock.NewRows([]string{
			"account_id", "balance_stroops", "sequence_number", "num_subentries",
			"num_sponsoring", "num_sponsored", "last_modified_ledger", "sequence_ledger",
			"sequence_time", "updated_at", "home_domain", "created_at", "sponsor",
			"auth_required", "auth_revocable", "auth_immutable", "auth_clawback_enabled",
		}).AddRow("GDORMANT", int64(100000000000), int64(13743955476742144), nil, nil, nil, int64(3200014), nil, nil, updatedAt, nil, nil, nil, nil, nil, nil, nil))

	reader := &SilverHotReader{db: db}
	acc, err := reader.GetServingAccountCurrent(context.Background(), "GDORMANT")
	if err != nil {
		t.Fatalf("GetServingAccountCurrent: %v", err)
	}
	if acc == nil || acc.AccountID != "GDORMANT" || acc.NumSubentries != 0 || acc.NumSponsoring != 0 {
		t.Fatalf("account = %#v", acc)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}
