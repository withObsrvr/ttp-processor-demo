package main

import (
	"context"
	"database/sql"
	"strings"
	"testing"
)

func testLoader() *Loader {
	return &Loader{cfg: Config{Network: "mainnet", BronzeAlias: "bronze_catalog", BronzeSchema: "bronze", SilverAlias: "silver_catalog", SilverSchema: "silver"}}
}

func TestSelectContractMetadataUsesContractCreations(t *testing.T) {
	sql := selectContractMetadata(testLoader(), 100, 200)
	if !strings.Contains(sql, "contract_creations_v1") {
		t.Fatalf("expected contract_metadata to read contract_creations_v1, got SQL: %s", sql)
	}
	if strings.Contains(sql, "operations_row_v2") || strings.Contains(sql, "GROUP BY") {
		t.Fatalf("contract_metadata should not synthesize metadata from operations: %s", sql)
	}
	if !strings.Contains(sql, "created_ledger BETWEEN 100 AND 200") {
		t.Fatalf("expected created_ledger range filter, got SQL: %s", sql)
	}
}

func TestSelectTradesNormalizesAmountsAndFractionalPrice(t *testing.T) {
	sql := selectTrades(testLoader(), 100, 200)
	for _, want := range []string{
		"TRY_CAST(selling_amount AS BIGINT)",
		"TRY_CAST(buying_amount AS BIGINT)",
		"contains(CAST(price AS VARCHAR), '/')",
		"DECIMAL(20, 7)",
	} {
		if !strings.Contains(sql, want) {
			t.Fatalf("expected %q in trades SQL: %s", want, sql)
		}
	}
}

func TestSelectContractBalanceChangesPreservesRemovalSignals(t *testing.T) {
	sql := selectContractBalanceChanges(testLoader(), 100, 200)
	for _, want := range []string{
		"balance_holder AS owner_address",
		"contract_id AS asset_key",
		"contract_id AS token_contract_id",
		"ledger_sequence",
		"deleted",
		"balance_holder IS NOT NULL",
		"balance IS NOT NULL",
	} {
		if !strings.Contains(sql, want) {
			t.Fatalf("expected %q in contract balance SQL: %s", want, sql)
		}
	}
	if strings.Contains(sql, "b.deleted = false") {
		t.Fatalf("contract balance changes must retain deleted rows: %s", sql)
	}
}

func TestSelectContractBalanceChangesEnrichesFromContractMetadata(t *testing.T) {
	sql := selectContractBalanceChanges(testLoader(), 100, 200)
	for _, want := range []string{
		"token_symbol",
		"token_decimals",
		"metadata",
		"COALESCE(NULLIF(b.asset_code, ''), m.asset_code)",
		"COALESCE(m.token_decimals, 7)",
	} {
		if !strings.Contains(sql, want) {
			t.Fatalf("expected %q in metadata-aware contract balance SQL: %s", want, sql)
		}
	}
}

func TestSelectContractBalanceChangesExecutesWithMetadataAndTombstones(t *testing.T) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx := context.Background()
	for _, stmt := range []string{
		`CREATE SCHEMA bronze`,
		`CREATE TABLE bronze.contract_data_snapshot_v1 (
			contract_id VARCHAR, ledger_sequence BIGINT, ledger_key_hash VARCHAR,
			contract_key_type VARCHAR, contract_durability VARCHAR,
			asset_code VARCHAR, asset_issuer VARCHAR, asset_type VARCHAR,
			balance_holder VARCHAR, balance VARCHAR, last_modified_ledger BIGINT,
			ledger_entry_change INTEGER, deleted BOOLEAN, closed_at TIMESTAMP,
			contract_data_xdr VARCHAR, created_at TIMESTAMP, ledger_range BIGINT,
			token_name VARCHAR, token_symbol VARCHAR, token_decimals INTEGER,
			era_id VARCHAR, version_label VARCHAR
		)`,
		`INSERT INTO bronze.contract_data_snapshot_v1 VALUES
			('CXLM',90,'MX','ScValTypeScvLedgerKeyContractInstance','persistent','',NULL,'AssetTypeAssetTypeNative',NULL,NULL,90,0,false,TIMESTAMP '2026-01-01 00:00:09',NULL,NULL,1,'Stellar Lumens','XLM',7,NULL,NULL),
			('CXLM',100,'BX','balance','persistent',NULL,NULL,NULL,'CWALLET','1230000000',100,0,false,TIMESTAMP '2026-01-01 00:00:10',NULL,NULL,1,NULL,NULL,NULL,NULL,NULL),
			('CTWO',80,'MT','ScValTypeScvLedgerKeyContractInstance','persistent',NULL,NULL,NULL,NULL,NULL,80,0,false,TIMESTAMP '2026-01-01 00:00:08',NULL,NULL,1,'Two Decimal Token','TWO',2,NULL,NULL),
			('CTWO',100,'BT','balance','persistent',NULL,NULL,NULL,'CWALLET','12345',100,0,false,TIMESTAMP '2026-01-01 00:00:10',NULL,NULL,1,NULL,NULL,NULL,NULL,NULL),
			('CTWO',110,'BT','balance','persistent',NULL,NULL,NULL,'CWALLET','12345',110,0,true,TIMESTAMP '2026-01-01 00:00:11',NULL,NULL,1,NULL,NULL,NULL,NULL,NULL)`,
	} {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			t.Fatalf("setup failed: %v\n%s", err, stmt)
		}
	}

	loader := &Loader{db: db, cfg: Config{Network: "mainnet", BronzeAlias: "memory", BronzeSchema: "bronze"}}
	query := `CREATE TABLE result AS ` + selectContractBalanceChanges(loader, 100, 200)
	if _, err := db.ExecContext(ctx, query); err != nil {
		t.Fatalf("execute contract balance transform: %v\n%s", err, query)
	}

	var assetType, assetCode, symbol string
	var decimals int
	if err := db.QueryRow(`SELECT asset_type, asset_code, symbol, decimals FROM result WHERE asset_key='CXLM'`).
		Scan(&assetType, &assetCode, &symbol, &decimals); err != nil {
		t.Fatal(err)
	}
	if assetType != "native" || assetCode != "XLM" || symbol != "XLM" || decimals != 7 {
		t.Fatalf("XLM metadata = %s/%s/%s/%d", assetType, assetCode, symbol, decimals)
	}
	if err := db.QueryRow(`SELECT symbol, decimals FROM result WHERE asset_key='CTWO' LIMIT 1`).Scan(&symbol, &decimals); err != nil {
		t.Fatal(err)
	}
	if symbol != "TWO" || decimals != 2 {
		t.Fatalf("custom token metadata = %s/%d", symbol, decimals)
	}
	var tombstones int
	if err := db.QueryRow(`SELECT COUNT(*) FROM result WHERE asset_key='CTWO' AND deleted`).Scan(&tombstones); err != nil {
		t.Fatal(err)
	}
	if tombstones != 1 {
		t.Fatalf("tombstones = %d, want 1", tombstones)
	}
}

func TestTransformsIncludeContractBalanceChanges(t *testing.T) {
	for _, transform := range transforms() {
		if transform.Table == "contract_balance_changes" {
			return
		}
	}
	t.Fatal("contract_balance_changes transform is not registered")
}

func TestSelectTransformsFiltersAndDeduplicates(t *testing.T) {
	selected, err := selectTransforms(" contract_balance_changes, effects,contract_balance_changes ")
	if err != nil {
		t.Fatal(err)
	}
	if len(selected) != 2 || selected[0].Table != "contract_balance_changes" || selected[1].Table != "effects" {
		t.Fatalf("selected transforms = %#v", selected)
	}
}

func TestSelectTransformsRejectsUnknownTable(t *testing.T) {
	if _, err := selectTransforms("contract_balance_changes,not_a_table"); err == nil {
		t.Fatal("expected unknown table error")
	}
}

func TestContractBalanceOnlyDoesNotRequireClassicFactCoverage(t *testing.T) {
	selected, err := selectTransforms("contract_balance_changes")
	if err != nil {
		t.Fatal(err)
	}
	loader := &Loader{transforms: selected}
	if loader.needsBronzeOperations() || loader.needsBronzeTransactions() {
		t.Fatal("contract-balance-only load must not depend on classic operation/transaction fact tables")
	}
}

func TestChunkCompleteCountsOnlySelectedTables(t *testing.T) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx := context.Background()
	if _, err := db.Exec(`CREATE SCHEMA silver`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`CREATE TABLE silver.silver_load_manifest (
		network VARCHAR, start_ledger BIGINT, end_ledger BIGINT,
		table_name VARCHAR, status VARCHAR
	)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO silver.silver_load_manifest VALUES
		('testnet', 100, 199, 'contract_balance_changes', 'completed'),
		('testnet', 100, 199, 'effects', 'completed')`); err != nil {
		t.Fatal(err)
	}
	selected, err := selectTransforms("contract_balance_changes")
	if err != nil {
		t.Fatal(err)
	}
	loader := &Loader{db: db, cfg: Config{Network: "testnet", SilverAlias: "memory", SilverSchema: "silver"}, transforms: selected}
	complete, err := loader.chunkComplete(ctx, 100, 199)
	if err != nil {
		t.Fatal(err)
	}
	if !complete {
		t.Fatal("selected contract-balance manifest should be complete even with unrelated entries")
	}

	selected, err = selectTransforms("contract_balance_changes,balance_changes")
	if err != nil {
		t.Fatal(err)
	}
	loader.transforms = selected
	complete, err = loader.chunkComplete(ctx, 100, 199)
	if err != nil {
		t.Fatal(err)
	}
	if complete {
		t.Fatal("manifest should be incomplete while a selected table is missing")
	}
}
