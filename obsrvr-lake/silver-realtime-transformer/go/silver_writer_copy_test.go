package main

import (
	"context"
	"database/sql"
	"strings"
	"testing"

	_ "github.com/duckdb/duckdb-go/v2"
)

func TestPrepareAddressBalanceStateStageRowRemovesMetadataNULs(t *testing.T) {
	row, err := prepareAddressBalanceStateStageRow(AddressBalanceStateStageRow{
		OwnerAddress:  "COWNER",
		AssetKey:      "CASSET",
		AssetTypeHint: "credit_alphanum4\x00",
		AssetCode:     "US\x00D\x00",
		AssetIssuer:   "GISSUER\x00",
		Symbol:        "USDC\x00",
		BalanceRaw:    "123",
		KeyHash:       "HASH",
	})
	if err != nil {
		t.Fatal(err)
	}
	if row.AssetTypeHint != "credit_alphanum4" || row.AssetCode != "USD" ||
		row.AssetIssuer != "GISSUER" || row.Symbol != "USDC" {
		t.Fatalf("sanitized metadata = %q/%q/%q/%q", row.AssetTypeHint, row.AssetCode, row.AssetIssuer, row.Symbol)
	}
}

func TestPrepareAddressBalanceStateStageRowRejectsIdentityNULs(t *testing.T) {
	base := AddressBalanceStateStageRow{
		OwnerAddress: "COWNER",
		AssetKey:     "CASSET",
		BalanceRaw:   "123",
		KeyHash:      "HASH",
	}
	for _, tc := range []struct {
		name  string
		field string
		set   func(*AddressBalanceStateStageRow)
	}{
		{name: "owner", field: "owner_address", set: func(row *AddressBalanceStateStageRow) { row.OwnerAddress += "\x00" }},
		{name: "asset", field: "asset_key", set: func(row *AddressBalanceStateStageRow) { row.AssetKey += "\x00" }},
		{name: "balance", field: "balance_raw", set: func(row *AddressBalanceStateStageRow) { row.BalanceRaw += "\x00" }},
		{name: "hash", field: "key_hash", set: func(row *AddressBalanceStateStageRow) { row.KeyHash += "\x00" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			row := base
			tc.set(&row)
			_, err := prepareAddressBalanceStateStageRow(row)
			if err == nil || !strings.Contains(err.Error(), tc.field) {
				t.Fatalf("error = %v, want field %s", err, tc.field)
			}
		})
	}
}

func TestMergeAddressBalanceStateStageEnrichesMetadataAndPreservesRemovals(t *testing.T) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	for _, stmt := range []string{
		`CREATE TEMP TABLE temp_address_balances_state_stage (
			owner_address VARCHAR, asset_key VARCHAR, asset_type_hint VARCHAR,
			asset_code VARCHAR, asset_issuer VARCHAR, symbol VARCHAR, decimals INTEGER,
			balance_raw DECIMAL(38,0),
			key_hash VARCHAR, deleted BOOLEAN, last_updated_ledger BIGINT,
			last_updated_at TIMESTAMP
		)`,
		`CREATE TABLE token_registry (
			contract_id VARCHAR PRIMARY KEY, token_name VARCHAR, token_symbol VARCHAR,
			token_decimals INTEGER, asset_code VARCHAR, asset_issuer VARCHAR,
			token_type VARCHAR, first_seen_ledger BIGINT, last_updated_ledger BIGINT
		)`,
		`CREATE TABLE contract_balance_changes (
			owner_address VARCHAR, owner_type VARCHAR, asset_key VARCHAR, asset_type VARCHAR,
			token_contract_id VARCHAR, asset_code VARCHAR, asset_issuer VARCHAR, symbol VARCHAR,
			decimals INTEGER, balance_raw DECIMAL(38,0), balance_source VARCHAR,
			key_hash VARCHAR, ledger_sequence BIGINT, ledger_closed_at TIMESTAMP,
			deleted BOOLEAN, inserted_at TIMESTAMP,
			PRIMARY KEY (owner_address, asset_key, ledger_sequence)
		)`,
		`CREATE TABLE address_balances_current (
			owner_address VARCHAR, asset_key VARCHAR, asset_type VARCHAR,
			token_contract_id VARCHAR, asset_code VARCHAR, asset_issuer VARCHAR,
			symbol VARCHAR, decimals INTEGER, balance_raw DECIMAL(38,0),
			balance_display VARCHAR, balance_source VARCHAR, last_updated_ledger BIGINT,
			last_updated_at TIMESTAMP, updated_at TIMESTAMP,
			PRIMARY KEY (owner_address, asset_key)
		)`,
		`INSERT INTO token_registry VALUES
			('CXLM', 'Stellar Lumens', 'XLM', 7, 'XLM', NULL, 'sac', 1, 1),
			('CTWO', 'Two Decimal Token', 'TWO', 2, NULL, NULL, 'custom_soroban', 1, 1)`,
		`INSERT INTO address_balances_current VALUES
			('CWALLET', 'CDELETED', 'soroban_token', 'CDELETED', NULL, NULL, 'OLD', 7,
			 900, '0.00009', 'contract_storage_state', 9, TIMESTAMP '2026-01-01 00:00:09', NOW()),
			('CWALLET', 'CTWO', 'unknown', 'CTWO', NULL, NULL, 'OLD', 7,
			 100, '0.00001', 'contract_storage_state', 9, TIMESTAMP '2026-01-01 00:00:09', NOW())`,
		`INSERT INTO temp_address_balances_state_stage VALUES
			('CWALLET', 'CXLM', '', '', '', 'XLM', 7, 1230000000, 'KXLM', false, 10, TIMESTAMP '2026-01-01 00:00:10'),
			('CWALLET', 'CTWO', '', '', '', 'TWO', 2, 12345, 'KTWO', false, 10, TIMESTAMP '2026-01-01 00:00:10'),
			('CWALLET', 'CDELETED', '', '', '', NULL, NULL, 900, 'KDEL', true, 10, TIMESTAMP '2026-01-01 00:00:10')`,
	} {
		if _, err := db.Exec(stmt); err != nil {
			t.Fatalf("setup failed: %v\n%s", err, stmt)
		}
	}

	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	writer := &SilverWriter{}
	if err := writer.mergeAddressBalanceStateStage(context.Background(), tx); err != nil {
		_ = tx.Rollback()
		t.Fatalf("mergeAddressBalanceStateStage: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	var assetType, assetCode, symbol, display string
	var decimals int
	if err := db.QueryRow(`SELECT asset_type, asset_code, symbol, decimals, balance_display
		FROM address_balances_current WHERE owner_address='CWALLET' AND asset_key='CXLM'`).
		Scan(&assetType, &assetCode, &symbol, &decimals, &display); err != nil {
		t.Fatal(err)
	}
	if assetType != "native" || assetCode != "XLM" || symbol != "XLM" || decimals != 7 || display != "123.0" {
		t.Fatalf("XLM metadata/display = %s/%s/%s/%d/%s", assetType, assetCode, symbol, decimals, display)
	}

	var nullableCode sql.NullString
	if err := db.QueryRow(`SELECT asset_type, asset_code, symbol, decimals, balance_display
		FROM address_balances_current WHERE owner_address='CWALLET' AND asset_key='CTWO'`).
		Scan(&assetType, &nullableCode, &symbol, &decimals, &display); err != nil {
		t.Fatal(err)
	}
	if assetType != "soroban_token" || nullableCode.Valid || symbol != "TWO" || decimals != 2 || display != "123.45" {
		t.Fatalf("custom metadata/display = %s/%v/%s/%d/%s", assetType, nullableCode, symbol, decimals, display)
	}

	var currentDeleted, changeDeleted int
	if err := db.QueryRow(`SELECT COUNT(*) FROM address_balances_current WHERE asset_key='CDELETED'`).Scan(&currentDeleted); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM contract_balance_changes WHERE asset_key='CDELETED' AND deleted`).Scan(&changeDeleted); err != nil {
		t.Fatal(err)
	}
	if currentDeleted != 0 || changeDeleted != 1 {
		t.Fatalf("deleted state/current change = %d/%d, want 0/1", currentDeleted, changeDeleted)
	}
}
