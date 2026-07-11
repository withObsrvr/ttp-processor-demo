package main

import (
	"os"
	"path/filepath"
	"regexp"
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

// TestExplicitColumnListsMatchDuckLakeSchema pins every explicit flush column
// list against the DuckLake schema file: a column named here that the schema
// lacks would fail every flush, and (more dangerously) a schema column added
// mid-table without an explicit list would silently shift positional values.
func TestExplicitColumnListsMatchDuckLakeSchema(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "v3_bronze_schema.sql"))
	if err != nil {
		t.Fatalf("read v3_bronze_schema.sql: %v", err)
	}
	schema := string(raw)

	for table, cols := range explicitColumnTables {
		pattern := regexp.MustCompile(`(?s)CREATE TABLE IF NOT EXISTS bronze\.` + regexp.QuoteMeta(table) + `\s*\((.*?)\n\);`)
		match := pattern.FindStringSubmatch(schema)
		if match == nil {
			t.Errorf("table %s has an explicit column list but no schema definition", table)
			continue
		}
		schemaColumns := map[string]bool{}
		for _, line := range strings.Split(match[1], "\n") {
			fields := strings.Fields(strings.TrimSpace(line))
			if len(fields) >= 2 {
				schemaColumns[strings.Trim(fields[0], `"`)] = true
			}
		}
		for _, col := range strings.Split(cols, ",") {
			col = strings.Trim(strings.TrimSpace(col), `"`)
			if col == "" {
				continue
			}
			if !schemaColumns[col] {
				t.Errorf("table %s: explicit column %q not present in v3_bronze_schema.sql", table, col)
			}
		}
	}
}

func TestAccountsSnapshotUsesExplicitColumns(t *testing.T) {
	client := &DuckDBClient{config: &DuckLakeConfig{
		CatalogName: "testnet_catalog",
		SchemaName:  "bronze",
	}}

	got := compactSQL(client.buildFlushSQL("accounts_snapshot_v1", "postgres://example", "ledger_sequence", 100, 200))
	if strings.Contains(got, "SELECT * FROM postgres_scan") {
		t.Fatalf("accounts_snapshot_v1 must not flush positionally: PG appends sequence_ledger/sequence_time at the end while the DuckLake schema defines them mid-table: %s", got)
	}
	if !strings.Contains(got, "sequence_ledger, sequence_time") {
		t.Fatalf("accounts_snapshot_v1 flush SQL missing sequence metadata columns: %s", got)
	}
}
