package main

import (
	"database/sql"
	"reflect"
	"strings"
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

func TestBuildDeleteRangeSQL(t *testing.T) {
	client := &DuckDBClient{config: &DuckLakeConfig{
		CatalogName: "testnet_catalog",
		SchemaName:  "silver",
	}}

	got := compactSQL(client.buildDeleteRangeSQL("token_transfers_raw", "ledger_sequence", 200, 100))
	want := "DELETE FROM testnet_catalog.silver.token_transfers_raw WHERE ledger_sequence > 100 AND ledger_sequence <= 200"
	if got != want {
		t.Fatalf("delete SQL:\n got: %s\nwant: %s", got, want)
	}
}

func TestTableFlushOverridesAddsNetworkAndContractLedgerRange(t *testing.T) {
	client := &DuckDBClient{network: "testnet"}
	overrides := client.tableFlushOverrides("contract_balance_changes")
	if overrides["network"] != "'testnet'" {
		t.Fatalf("network override = %q", overrides["network"])
	}
	if overrides["ledger_range"] != "FLOOR(ledger_sequence / 100000)" {
		t.Fatalf("ledger_range override = %q", overrides["ledger_range"])
	}
}

func TestPostgresExactBalanceSourceCastsBeforeDuckDB(t *testing.T) {
	got := postgresExactBalanceSource("silver_hot_exact", "contract_balance_changes", "ledger_sequence", 200, 100,
		[]string{"owner_address", "balance_raw", "ledger_sequence"})
	for _, want := range []string{
		`"balance_raw"::text AS "balance_raw"`,
		`FROM public."contract_balance_changes"`,
		`"ledger_sequence" > 100 AND "ledger_sequence" <= 200`,
		"postgres_query('silver_hot_exact'",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("exact balance source missing %q:\n%s", want, got)
		}
	}
}

func TestContractBalanceReconciliationSQLIsNetworkAndTombstoneAware(t *testing.T) {
	client := &DuckDBClient{network: "testnet", config: &DuckLakeConfig{
		CatalogName: "testnet_catalog",
		SchemaName:  "silver",
	}}
	stageSQL, deleteSQL, insertSQL := client.contractBalanceReconciliationSQL(200, 100)
	stageSQL = compactSQL(stageSQL)
	deleteSQL = compactSQL(deleteSQL)
	insertSQL = compactSQL(insertSQL)
	for _, want := range []string{
		"history.network = 'testnet'",
		"history.ledger_sequence <= 200",
		"ledger_sequence > 100 AND ledger_sequence <= 200",
		"newer.last_updated_ledger > 200",
	} {
		if !strings.Contains(stageSQL, want) {
			t.Fatalf("stage SQL missing %q:\n%s", want, stageSQL)
		}
	}
	for _, want := range []string{
		"FROM temp_contract_balance_reconcile",
		"COALESCE(deleted, false) = false",
		"regexp_matches(CAST(balance_raw AS VARCHAR)",
		"NOT has_newer_current",
	} {
		if !strings.Contains(insertSQL, want) {
			t.Fatalf("insert SQL missing %q:\n%s", want, insertSQL)
		}
	}
	if !strings.Contains(deleteSQL, "current.network = 'testnet' OR current.network IS NULL") {
		t.Fatalf("delete SQL is not network-aware:\n%s", deleteSQL)
	}
	if !strings.Contains(deleteSQL, "FROM temp_contract_balance_reconcile affected") {
		t.Fatalf("delete SQL does not use materialized affected keys:\n%s", deleteSQL)
	}
}

func TestReconcileContractBalancesCurrentAppliesLatestChange(t *testing.T) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, statement := range []string{
		`CREATE SCHEMA silver`,
		`CREATE TABLE silver.address_balances_current (
			network VARCHAR, owner_address VARCHAR, asset_key VARCHAR, asset_type VARCHAR,
			token_contract_id VARCHAR, asset_code VARCHAR, asset_issuer VARCHAR, symbol VARCHAR,
			decimals INTEGER, balance_raw VARCHAR, balance_display VARCHAR, balance_source VARCHAR,
			last_updated_ledger BIGINT, last_updated_at TIMESTAMP, updated_at TIMESTAMP
		)`,
		`CREATE TABLE silver.contract_balance_changes (
			network VARCHAR, owner_address VARCHAR, owner_type VARCHAR, asset_key VARCHAR,
			asset_type VARCHAR, token_contract_id VARCHAR, asset_code VARCHAR, asset_issuer VARCHAR,
			symbol VARCHAR, decimals INTEGER, balance_raw VARCHAR, balance_source VARCHAR,
			key_hash VARCHAR, ledger_sequence BIGINT, ledger_closed_at TIMESTAMP,
			deleted BOOLEAN, ledger_range BIGINT
		)`,
		`INSERT INTO silver.address_balances_current VALUES
			('testnet','CDELETE','CXLM','native','CXLM','XLM',NULL,'XLM',7,'10000000','1.0','contract_storage_state',100,TIMESTAMP '2026-01-01',current_timestamp),
			('testnet','CDELETE','CXLM','native','CXLM','XLM',NULL,'XLM',7,'9000000','0.9','contract_storage_state',90,TIMESTAMP '2025-12-31',current_timestamp),
			('mainnet','CDELETE','CXLM','native','CXLM','XLM',NULL,'XLM',7,'20000000','2.0','contract_storage_state',100,TIMESTAMP '2026-01-01',current_timestamp),
			('testnet','CLARGE','CTWO','soroban_token','CTWO',NULL,NULL,'TWO',2,'1','0.01','contract_storage_state',120,TIMESTAMP '2026-01-01',current_timestamp),
			('testnet','CFUTURE','CTWO','soroban_token','CTWO',NULL,NULL,'TWO',2,'999','9.99','contract_storage_state',250,TIMESTAMP '2026-01-05',current_timestamp)`,
		`INSERT INTO silver.contract_balance_changes VALUES
			('testnet','CDELETE','contract','CXLM','native','CXLM','XLM',NULL,'XLM',7,'10000000','contract_storage_state','K1',100,TIMESTAMP '2026-01-01',false,0),
			('testnet','CDELETE','contract','CXLM','native','CXLM','XLM',NULL,'XLM',7,'0','contract_storage_state','K1',150,TIMESTAMP '2026-01-02',true,0),
			('testnet','CLARGE','contract','CTWO','soroban_token','CTWO',NULL,NULL,'TWO',2,'999999999999999999999999999999999999999','contract_storage_state','K2',160,TIMESTAMP '2026-01-03',false,0),
			('testnet','CZERO','contract','CTWO','soroban_token','CTWO',NULL,NULL,'TWO',2,'0','contract_storage_state','K3',170,TIMESTAMP '2026-01-04',false,0),
			('testnet','CFUTURE','contract','CTWO','soroban_token','CTWO',NULL,NULL,'TWO',2,'100','contract_storage_state','K4',160,TIMESTAMP '2026-01-03',false,0)`,
	} {
		if _, err := db.Exec(statement); err != nil {
			t.Fatalf("setup: %v\n%s", err, statement)
		}
	}

	client := &DuckDBClient{db: db, network: "testnet", config: &DuckLakeConfig{CatalogName: "memory", SchemaName: "silver"}}
	if _, err := client.ReconcileContractBalancesCurrent(200, 100); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	var testnetDeleted, mainnetPreserved, zeroRows int
	if err := db.QueryRow(`SELECT count(*) FROM silver.address_balances_current WHERE network='testnet' AND owner_address='CDELETE'`).Scan(&testnetDeleted); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT count(*) FROM silver.address_balances_current WHERE network='mainnet' AND owner_address='CDELETE'`).Scan(&mainnetPreserved); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT count(*) FROM silver.address_balances_current WHERE owner_address='CZERO'`).Scan(&zeroRows); err != nil {
		t.Fatal(err)
	}
	if testnetDeleted != 0 || mainnetPreserved != 1 || zeroRows != 0 {
		t.Fatalf("deleted=%d mainnet=%d zero=%d", testnetDeleted, mainnetPreserved, zeroRows)
	}
	var futureRows int
	var futureLedger int64
	if err := db.QueryRow(`SELECT count(*), max(last_updated_ledger) FROM silver.address_balances_current WHERE network='testnet' AND owner_address='CFUTURE'`).Scan(&futureRows, &futureLedger); err != nil {
		t.Fatal(err)
	}
	if futureRows != 1 || futureLedger != 250 {
		t.Fatalf("newer current state was not preserved: rows=%d ledger=%d", futureRows, futureLedger)
	}
	var raw, display string
	if err := db.QueryRow(`SELECT balance_raw, balance_display FROM silver.address_balances_current WHERE network='testnet' AND owner_address='CLARGE'`).Scan(&raw, &display); err != nil {
		t.Fatal(err)
	}
	if raw != "999999999999999999999999999999999999999" || display != "9999999999999999999999999999999999999.99" {
		t.Fatalf("large balance raw/display = %q/%q", raw, display)
	}
	var largeRows int
	if err := db.QueryRow(`SELECT count(*) FROM silver.address_balances_current WHERE network='testnet' AND owner_address='CLARGE'`).Scan(&largeRows); err != nil {
		t.Fatal(err)
	}
	if largeRows != 1 {
		t.Fatalf("CLARGE rows = %d, want exactly one reconciled current row", largeRows)
	}
}

func compactSQL(s string) string {
	return strings.Join(strings.Fields(s), " ")
}

func TestAddressBalancesCurrentIsRetainedInHotServing(t *testing.T) {
	if shouldDeleteFlushedTable("address_balances_current") {
		t.Fatal("address_balances_current is bounded latest-per-key serving state and must not be deleted after cold archival")
	}
	for _, tableName := range []string{
		"smart_account_context_rules",
		"smart_account_signers",
		"smart_account_policies",
	} {
		if shouldDeleteFlushedTable(tableName) {
			t.Fatalf("%s is bounded smart-account current state and must not be deleted after cold archival", tableName)
		}
	}
	if !shouldDeleteFlushedTable("contract_balance_changes") {
		t.Fatal("append-only contract_balance_changes should still be deleted after cold archival")
	}
}
