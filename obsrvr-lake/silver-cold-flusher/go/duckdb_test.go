package main

import (
	"reflect"
	"testing"
)

func TestBuildFlushColumns(t *testing.T) {
	// cold is wider than hot (era_id/version_label/ledger_range are cold-only) and hot has an
	// extra column (amount) cold doesn't. ledger_range has a computed override.
	cold := []string{"id", "balance", "ledger_sequence", "era_id", "version_label", "ledger_range"}
	hot := []string{"id", "balance", "ledger_sequence", "amount"}
	overrides := map[string]string{"ledger_range": "FLOOR(ledger_sequence / 100000)"}

	ins, sel := buildFlushColumns(cold, hot, overrides)

	wantIns := []string{"id", "balance", "ledger_sequence", "ledger_range"}
	wantSel := []string{"id", "balance", "ledger_sequence", "FLOOR(ledger_sequence / 100000) AS ledger_range"}

	if !reflect.DeepEqual(ins, wantIns) {
		t.Fatalf("insert cols = %v, want %v", ins, wantIns)
	}
	if !reflect.DeepEqual(sel, wantSel) {
		t.Fatalf("select exprs = %v, want %v", sel, wantSel)
	}
	// era_id/version_label are cold-only with no override -> dropped (take their column default).
	// amount is hot-only -> ignored.
}

func TestBuildFlushColumnsNilOverrides(t *testing.T) {
	cold := []string{"a", "b", "extra_cold"}
	hot := []string{"a", "b", "extra_hot"}
	ins, sel := buildFlushColumns(cold, hot, nil)
	want := []string{"a", "b"}
	if !reflect.DeepEqual(ins, want) || !reflect.DeepEqual(sel, want) {
		t.Fatalf("ins=%v sel=%v, want %v (only shared columns, no panic on nil overrides)", ins, sel, want)
	}
}
