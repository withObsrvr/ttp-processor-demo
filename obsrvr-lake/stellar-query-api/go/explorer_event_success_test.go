package main

import (
	"database/sql"
	"testing"
)

func TestApplyExplorerEventSuccessDoesNotUseSubCallFlagForPublicStatus(t *testing.T) {
	e := ExplorerEvent{}
	applyExplorerEventSuccess(&e, sql.NullBool{Bool: true, Valid: true}, sql.NullBool{Bool: false, Valid: true})

	if !e.Successful {
		t.Fatal("successful compatibility alias = false, want true from transaction_successful")
	}
	if e.TransactionSuccessful == nil || !*e.TransactionSuccessful {
		t.Fatalf("transaction_successful = %v, want true", e.TransactionSuccessful)
	}
	if e.InSuccessfulContractCall == nil || *e.InSuccessfulContractCall {
		t.Fatalf("in_successful_contract_call = %v, want false", e.InSuccessfulContractCall)
	}
}

func TestApplyExplorerEventSuccessFailedTransaction(t *testing.T) {
	e := ExplorerEvent{}
	applyExplorerEventSuccess(&e, sql.NullBool{Bool: false, Valid: true}, sql.NullBool{Bool: true, Valid: true})

	if e.Successful {
		t.Fatal("successful compatibility alias = true, want false from transaction_successful")
	}
	if e.TransactionSuccessful == nil || *e.TransactionSuccessful {
		t.Fatalf("transaction_successful = %v, want false", e.TransactionSuccessful)
	}
	if e.InSuccessfulContractCall == nil || !*e.InSuccessfulContractCall {
		t.Fatalf("in_successful_contract_call = %v, want true", e.InSuccessfulContractCall)
	}
}

func TestApplyExplorerEventSuccessPreservesUnknownTransactionSuccess(t *testing.T) {
	e := ExplorerEvent{}
	applyExplorerEventSuccess(&e, sql.NullBool{}, sql.NullBool{})

	if e.Successful {
		t.Fatal("successful compatibility alias = true, want false when transaction success is unknown")
	}
	if e.TransactionSuccessful != nil {
		t.Fatalf("transaction_successful = %v, want nil", e.TransactionSuccessful)
	}
	if e.InSuccessfulContractCall != nil {
		t.Fatalf("in_successful_contract_call = %v, want nil", e.InSuccessfulContractCall)
	}
}
