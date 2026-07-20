package main

import "testing"

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
