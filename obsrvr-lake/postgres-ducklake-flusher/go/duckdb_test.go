package main

import (
	"strings"
	"testing"
)

func TestSequenceColumnForTable(t *testing.T) {
	tests := map[string]string{
		"ledgers_row_v2":            "sequence",
		"contract_creations_v1":     "created_ledger",
		"transactions_row_v2":       "ledger_sequence",
		"contract_events_stream_v1": "ledger_sequence",
	}

	for table, want := range tests {
		if got := sequenceColumnForTable(table); got != want {
			t.Fatalf("sequenceColumnForTable(%q) = %q, want %q", table, got, want)
		}
	}
}

func TestBuildDeleteRangeSQLDeletesOnlyRetryRange(t *testing.T) {
	client := &DuckDBClient{config: &DuckLakeConfig{
		CatalogName: "testnet_catalog",
		SchemaName:  "bronze",
	}}

	got := compactSQL(client.buildDeleteRangeSQL("transactions_row_v2", "ledger_sequence", 100, 200))
	want := "DELETE FROM testnet_catalog.bronze.transactions_row_v2 WHERE ledger_sequence > 100 AND ledger_sequence <= 200;"
	if got != want {
		t.Fatalf("delete SQL:\n got: %s\nwant: %s", got, want)
	}
}

func TestBuildFlushSQLUsesLedgerRangePredicate(t *testing.T) {
	client := &DuckDBClient{config: &DuckLakeConfig{
		CatalogName: "testnet_catalog",
		SchemaName:  "bronze",
	}}

	got := compactSQL(client.buildFlushSQL("transactions_row_v2", "postgres://example", "ledger_sequence", 100, 200))
	if !strings.Contains(got, "WHERE ledger_sequence > 100 AND ledger_sequence <= 200;") {
		t.Fatalf("flush SQL missing bounded range predicate: %s", got)
	}
	if !strings.Contains(got, "FROM postgres_scan('postgres://example', 'public', 'transactions_row_v2')") {
		t.Fatalf("flush SQL missing postgres_scan source: %s", got)
	}
}

func TestBuildFlushSQLQuotesReservedColumnsForTokenTransfers(t *testing.T) {
	client := &DuckDBClient{config: &DuckLakeConfig{
		CatalogName: "testnet_catalog",
		SchemaName:  "bronze",
	}}

	got := compactSQL(client.buildFlushSQL("token_transfers_stream_v1", "postgres://example", "ledger_sequence", 100, 200))
	if !strings.Contains(got, `"from", "to"`) {
		t.Fatalf("token transfer SQL should quote reserved columns: %s", got)
	}
}

func compactSQL(s string) string {
	return strings.Join(strings.Fields(s), " ")
}
