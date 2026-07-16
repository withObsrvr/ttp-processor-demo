package main

import (
	"strings"
	"testing"
)

func TestContractStorageStartLedgerUsesAuthoritativeWatermark(t *testing.T) {
	if got := contractStorageStartLedger(150, true, 200); got != 200 {
		t.Fatalf("start ledger = %d, want watermark 200", got)
	}
	if got := contractStorageStartLedger(150, false, 0); got != 150 {
		t.Fatalf("start ledger = %d, want checkpoint 150", got)
	}
}

func TestContractStorageMutationsAreVersionGuarded(t *testing.T) {
	if !strings.Contains(contractStorageUpsertSQL, "last_modified_ledger <= EXCLUDED.last_modified_ledger") {
		t.Fatal("contract storage upsert must not overwrite a newer restored value")
	}
	if !strings.Contains(contractStorageDeleteSQL, "last_modified_ledger <= $3") {
		t.Fatal("contract storage deletion must not remove a value restored after the tombstone")
	}
}

func TestContractStorageTTLUpdateTypesArithmeticParameters(t *testing.T) {
	if !strings.Contains(contractStorageTTLUpdateSQL, "$2::bigint - $3::bigint") {
		t.Fatal("TTL update must type both arithmetic parameters for PostgreSQL")
	}
	if !strings.Contains(contractStorageTTLUpdateSQL, "$2::bigint < $3::bigint") {
		t.Fatal("TTL update must type both comparison parameters for PostgreSQL")
	}
}
