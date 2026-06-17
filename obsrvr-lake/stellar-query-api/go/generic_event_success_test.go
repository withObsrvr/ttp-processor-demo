package main

import (
	"database/sql"
	"testing"
)

func TestApplyGenericEventSuccessExposesTransactionSuccessful(t *testing.T) {
	e := GenericEvent{Successful: false}
	applyGenericEventSuccess(&e, sql.NullBool{Bool: true, Valid: true})

	if e.TransactionSuccessful == nil || !*e.TransactionSuccessful {
		t.Fatalf("transaction_successful = %v, want true", e.TransactionSuccessful)
	}
	if e.Successful {
		t.Fatal("in_successful_contract_call changed; want raw false preserved")
	}
}

func TestApplyGenericEventSuccessPreservesUnknown(t *testing.T) {
	e := GenericEvent{}
	applyGenericEventSuccess(&e, sql.NullBool{})
	if e.TransactionSuccessful != nil {
		t.Fatalf("transaction_successful = %v, want nil", e.TransactionSuccessful)
	}
}
