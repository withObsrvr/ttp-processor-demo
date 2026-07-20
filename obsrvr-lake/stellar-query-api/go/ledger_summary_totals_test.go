package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/stellar/go-stellar-sdk/strkey"
)

func TestLedgerSummaryTotalsExposeIncludedAndSuccessfulOperations(t *testing.T) {
	totals := ledgerSummaryTotalsFromRow(map[string]interface{}{
		"transaction_count":      int64(14),
		"successful_tx_count":    int64(13),
		"failed_tx_count":        int64(1),
		"operation_count":        int64(15),
		"tx_set_operation_count": int64(19),
	})

	if totals.OperationCount != 15 || totals.SuccessfulOperationCount != 15 {
		t.Fatalf("successful operation compatibility fields changed: %+v", totals)
	}
	if totals.TransactionSetOperationCount != 19 || totals.FailedOperationCount != 4 {
		t.Fatalf("explicit operation semantics are incorrect: %+v", totals)
	}
}

func TestLedgerSummaryTotalsFallsBackForPreMigrationRows(t *testing.T) {
	totals := ledgerSummaryTotalsFromRow(map[string]interface{}{
		"operation_count": int64(15),
	})

	if totals.TransactionSetOperationCount != 15 || totals.SuccessfulOperationCount != 15 || totals.FailedOperationCount != 0 {
		t.Fatalf("pre-migration fallback broke operation invariants: %+v", totals)
	}
}

func TestLedgerSummaryIncludesResolvedValidatorIdentity(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	rawValidator := bytes.Repeat([]byte{0x2a}, 32)
	nodeID := base64.StdEncoding.EncodeToString(rawValidator)
	publicKey, err := strkey.Encode(strkey.VersionByteAccountID, rawValidator)
	if err != nil {
		t.Fatalf("encode validator fixture: %v", err)
	}
	observedAt := time.Date(2026, 7, 20, 12, 0, 0, 0, time.UTC)
	mock.ExpectQuery(`(?s)SELECT public_key.*FROM serving\.sv_validator_identity_current.*WHERE network = \$1 AND public_key = ANY\(\$2\)`).
		WithArgs("testnet", sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{
			"public_key", "name", "display_name", "alias", "home_domain", "organization_id", "source", "source_updated_at", "observed_at",
		}).AddRow(publicKey, "SDF Testnet 3", "SDF Testnet 3", "sdf_testnet_3", "", "", "radar", observedAt.Add(-time.Hour), observedAt))

	handler := &LedgerSummaryHandler{hot: &SilverHotReader{db: db, network: "testnet"}}
	resp := LedgerSummaryResponse{}
	handler.enrichLedgerSummaryValidator(context.Background(), &resp, nodeID)

	if resp.Ledger.ClosedByValidator != publicKey || resp.Ledger.Validator == nil {
		t.Fatalf("validator attribution missing: %+v", resp.Ledger)
	}
	if resp.Ledger.Validator.Status != "resolved" || resp.Ledger.Validator.Name != "SDF Testnet 3" {
		t.Fatalf("validator identity not resolved: %+v", resp.Ledger.Validator)
	}
	if resp.Provenance.Partial {
		t.Fatal("resolved validator identity must not mark the summary partial")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet SQL expectations: %v", err)
	}
}
