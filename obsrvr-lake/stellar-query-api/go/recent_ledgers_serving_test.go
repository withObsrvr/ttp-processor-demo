package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/stellar/go-stellar-sdk/strkey"
)

func TestGetServingRecentLedgersReturnsExplicitOperationSemanticsAndValidator(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	rawValidator := bytes.Repeat([]byte{0x2a}, 32)
	nodeID := base64.StdEncoding.EncodeToString(rawValidator)
	validatorAddress, err := strkey.Encode(strkey.VersionByteAccountID, rawValidator)
	if err != nil {
		t.Fatalf("encode validator fixture: %v", err)
	}
	closedAt := time.Date(2026, 7, 20, 12, 0, 0, 0, time.UTC)

	mock.ExpectQuery(`SELECT COALESCE\(MAX\(ledger_sequence\), 0\) FROM serving\.sv_ledger_stats_recent`).
		WillReturnRows(sqlmock.NewRows([]string{"max"}).AddRow(int64(3707457)))
	mock.ExpectQuery(`(?s)SELECT ledger_sequence, closed_at.*tx_set_operation_count.*validator_node_id.*ledger_close_signature.*FROM serving\.sv_ledger_stats_recent`).
		WithArgs(6).
		WillReturnRows(sqlmock.NewRows([]string{
			"ledger_sequence", "closed_at", "ledger_hash", "prev_hash", "protocol_version", "base_fee_stroops",
			"successful_tx_count", "failed_tx_count", "operation_count", "tx_set_operation_count",
			"validator_node_id", "ledger_close_signature",
			"op_category_account_creation", "op_category_payments", "op_category_offers_and_amms", "op_category_trustlines",
			"op_category_claimable_balances", "op_category_sponsorship", "op_category_soroban", "op_category_other",
			"successful_op_category_account_creation", "successful_op_category_payments", "successful_op_category_offers_and_amms", "successful_op_category_trustlines",
			"successful_op_category_claimable_balances", "successful_op_category_sponsorship", "successful_op_category_soroban", "successful_op_category_other",
			"operation_categories_complete",
		}).AddRow(int64(3707457), closedAt, "hash", "prev", 23, int64(100), 13, 1, 15, 19, nodeID, "signature",
			1, 2, 3, 1, 1, 1, 8, 2,
			1, 2, 2, 1, 1, 1, 5, 2, true))
	mock.ExpectQuery(`(?s)SELECT public_key.*FROM serving\.sv_validator_identity_current.*WHERE network = \$1 AND public_key = ANY\(\$2\)`).
		WithArgs("testnet", sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{
			"public_key", "name", "display_name", "alias", "home_domain", "organization_id", "source", "source_updated_at", "observed_at",
		}).AddRow(validatorAddress, "SDF Testnet 3", "SDF Testnet 3", "sdf_testnet_3", "", "", "radar", closedAt.Add(-time.Hour), closedAt))

	reader := &SilverHotReader{db: db, network: "testnet"}
	latest, ledgers, err := reader.GetServingRecentLedgers(context.Background(), 6)
	if err != nil {
		t.Fatalf("GetServingRecentLedgers: %v", err)
	}
	if latest != 3707457 || len(ledgers) != 1 {
		t.Fatalf("unexpected result latest=%d ledgers=%d", latest, len(ledgers))
	}

	got := ledgers[0]
	if got.TransactionCount != 14 || got.Transactions.Total != 14 || got.Transactions.Successful != 13 || got.Transactions.Failed != 1 {
		t.Fatalf("unexpected transaction counts: %+v", got.Transactions)
	}
	if got.OperationCount != 15 || got.TransactionSetOperationCount != 19 || got.SuccessfulOperationCount != 15 || got.FailedOperationCount != 4 {
		t.Fatalf("unexpected operation compatibility fields: %+v", got)
	}
	if got.Operations.Included != 19 || got.Operations.Successful != 15 || got.Operations.Failed != 4 {
		t.Fatalf("unexpected operation counts: %+v", got.Operations)
	}
	if categoryTotal(got.Operations.Categories) != 19 || categoryTotal(got.Operations.SuccessfulCategories) != 15 {
		t.Fatalf("operation category totals do not match counts: %+v", got.Operations)
	}
	if got.Operations.ClassificationStatus != "materialized" {
		t.Fatalf("unexpected classification status: %+v", got.Operations)
	}
	if got.Validator.PublicKey != validatorAddress || !got.Validator.AttributionAvailable {
		t.Fatalf("unexpected validator: %+v", got.Validator)
	}
	if got.Validator.Status != "resolved" || got.Validator.Name != "SDF Testnet 3" || got.Validator.Source != "radar" {
		t.Fatalf("unexpected validator identity: %+v", got.Validator)
	}
	if got.LedgerCloseSignature != "signature" {
		t.Fatalf("unexpected ledger close signature %q", got.LedgerCloseSignature)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet SQL expectations: %v", err)
	}
}

func TestServingRecentLedgerIdentityIsSoftDependency(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	mock.ExpectQuery(`(?s)FROM serving\.sv_validator_identity_current`).
		WithArgs("testnet", sqlmock.AnyArg()).
		WillReturnError(errors.New("identity projection unavailable"))

	reader := &SilverHotReader{db: db, network: "testnet"}
	ledgers := []ServingRecentLedger{{Validator: ServingRecentLedgerValidator{
		PublicKey:            "GVALIDATOR",
		AttributionAvailable: true,
		Status:               "not_found",
		Source:               "radar",
	}}}
	reader.enrichServingLedgerValidatorIdentities(context.Background(), ledgers)

	if ledgers[0].Validator.Status != "unavailable" || ledgers[0].Validator.PublicKey != "GVALIDATOR" {
		t.Fatalf("base validator attribution was not preserved: %+v", ledgers[0].Validator)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet SQL expectations: %v", err)
	}
}

func categoryTotal(counts ServingLedgerOperationCategoryCounts) int {
	return counts.AccountCreation + counts.Payments + counts.OffersAndAMMs + counts.Trustlines +
		counts.ClaimableBalances + counts.Sponsorship + counts.Soroban + counts.Other
}
