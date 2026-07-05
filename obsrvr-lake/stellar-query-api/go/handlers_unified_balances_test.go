package main

import (
	"context"
	"database/sql"
	"testing"

	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/stellar/go-stellar-sdk/strkey"
)

func TestGetUnifiedAddressBalancesComposesClassicAndSoroban(t *testing.T) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	addr := testAccountAddress(t)
	contractID := testContractAddress(t)
	issuer := testIssuerAddress(t)

	setup := []string{
		`CREATE TABLE accounts_current (account_id VARCHAR, balance BIGINT, last_modified_ledger BIGINT)`,
		`CREATE TABLE trustlines_current (account_id VARCHAR, asset_type VARCHAR, asset_code VARCHAR, asset_issuer VARCHAR, balance BIGINT, trust_line_limit BIGINT, flags BIGINT, last_modified_ledger BIGINT)`,
		`CREATE TABLE token_transfers_raw (from_account VARCHAR, to_account VARCHAR, amount BIGINT, token_contract_id VARCHAR, asset_code VARCHAR, asset_issuer VARCHAR, source_type VARCHAR, timestamp TIMESTAMP, transaction_hash VARCHAR, ledger_sequence BIGINT, event_index BIGINT, transaction_successful BOOLEAN)`,
		`CREATE TABLE token_registry (contract_id VARCHAR, token_name VARCHAR, token_symbol VARCHAR, token_decimals INTEGER, token_type VARCHAR)`,
	}
	for _, stmt := range setup {
		if _, err := db.Exec(stmt); err != nil {
			t.Fatalf("setup failed: %v", err)
		}
	}
	if _, err := db.Exec(`INSERT INTO accounts_current VALUES (?, 125000000, 10)`, addr); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO trustlines_current VALUES (?, 'credit_alphanum4', 'USDC', ?, 42000000, 1000000000, 1, 11)`, addr, issuer); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO token_transfers_raw VALUES (NULL, ?, 5000, ?, NULL, NULL, 'soroban_contract', TIMESTAMP '2026-06-21 12:00:00', 'tx1', 12, 0, true)`, addr, contractID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO token_registry VALUES (?, 'Demo Token', 'DEMO', 2, 'custom_soroban')`, contractID); err != nil {
		t.Fatal(err)
	}

	reader := &UnifiedDuckDBReader{db: db, hotSchema: "main", coldSchema: "main"}
	h := &SilverHandlers{unifiedReader: reader}

	resp, err := h.GetUnifiedAddressBalances(context.Background(), addr, false)
	if err != nil {
		t.Fatal(err)
	}
	if resp.Address != addr || resp.TotalBalances != 2 {
		t.Fatalf("unexpected response: addr=%s total=%d", resp.Address, resp.TotalBalances)
	}
	if !resp.Partial || len(resp.Warnings) == 0 {
		t.Fatalf("default response should mark omitted token holdings as partial: %+v", resp)
	}

	byType := map[string]UnifiedAddressBalance{}
	for _, b := range resp.Balances {
		if b.ContractID != nil {
			byType["contract"] = b
		} else {
			byType[b.AssetType+":"+b.AssetCode] = b
		}
	}
	if got := byType["native:XLM"].BalanceRaw; got != "125000000" {
		t.Fatalf("native balance raw = %q", got)
	}
	if got := byType["credit_alphanum4:USDC"].BalanceDisplay; got != "4.2000000" {
		t.Fatalf("trustline display = %q", got)
	}

	resp, err = h.GetUnifiedAddressBalances(context.Background(), addr, true)
	if err != nil {
		t.Fatal(err)
	}
	if resp.Address != addr || resp.TotalBalances != 3 {
		t.Fatalf("unexpected opt-in response: addr=%s total=%d", resp.Address, resp.TotalBalances)
	}
	byType = map[string]UnifiedAddressBalance{}
	for _, b := range resp.Balances {
		if b.ContractID != nil {
			byType["contract"] = b
		} else {
			byType[b.AssetType+":"+b.AssetCode] = b
		}
	}
	if byType["contract"].ContractID == nil || *byType["contract"].ContractID != contractID {
		t.Fatalf("missing soroban contract balance: %+v", byType["contract"])
	}
	if got := byType["contract"].Decimals; got == nil || *got != 2 {
		t.Fatalf("soroban decimals = %v", got)
	}
}

func TestIsValidStellarAddress(t *testing.T) {
	if !isValidStellarAddress(testAccountAddress(t)) {
		t.Fatal("account address should be valid")
	}
	if !isValidStellarAddress(testContractAddress(t)) {
		t.Fatal("contract address should be valid")
	}
	if isValidStellarAddress("not-an-address") {
		t.Fatal("invalid address accepted")
	}
}

func testAccountAddress(t *testing.T) string {
	t.Helper()
	payload := make([]byte, 32)
	payload[31] = 1
	addr, err := strkey.Encode(strkey.VersionByteAccountID, payload)
	if err != nil {
		t.Fatal(err)
	}
	return addr
}

func testIssuerAddress(t *testing.T) string {
	t.Helper()
	payload := make([]byte, 32)
	payload[31] = 2
	addr, err := strkey.Encode(strkey.VersionByteAccountID, payload)
	if err != nil {
		t.Fatal(err)
	}
	return addr
}

func testContractAddress(t *testing.T) string {
	t.Helper()
	payload := make([]byte, 32)
	payload[31] = 3
	addr, err := strkey.Encode(strkey.VersionByteContract, payload)
	if err != nil {
		t.Fatal(err)
	}
	return addr
}
