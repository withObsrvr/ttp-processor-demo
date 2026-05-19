package main

import "testing"

func boolPtr(v bool) *bool { return &v }

func TestExplorerEventSuccessFieldsUsesTransactionSuccess(t *testing.T) {
	txSuccessful, compatSuccessful := explorerEventSuccessFields(boolPtr(true), boolPtr(false))
	if txSuccessful == nil || !*txSuccessful {
		t.Fatalf("transaction_successful = %v, want true", txSuccessful)
	}
	if compatSuccessful == nil || !*compatSuccessful {
		t.Fatalf("successful compatibility alias = %v, want true", compatSuccessful)
	}
}

func TestExplorerEventSuccessFieldsFailedTransaction(t *testing.T) {
	txSuccessful, compatSuccessful := explorerEventSuccessFields(boolPtr(false), boolPtr(true))
	if txSuccessful == nil || *txSuccessful {
		t.Fatalf("transaction_successful = %v, want false", txSuccessful)
	}
	if compatSuccessful == nil || *compatSuccessful {
		t.Fatalf("successful compatibility alias = %v, want false", compatSuccessful)
	}
}

func TestExplorerEventSuccessFieldsPreservesUnknown(t *testing.T) {
	txSuccessful, compatSuccessful := explorerEventSuccessFields(nil, nil)
	if txSuccessful != nil {
		t.Fatalf("transaction_successful = %v, want nil", txSuccessful)
	}
	if compatSuccessful != nil {
		t.Fatalf("successful compatibility alias = %v, want nil", compatSuccessful)
	}
}
