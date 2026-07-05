package main

import (
	"os"
	"strings"
	"testing"
)

func TestAccountBucketDeterministic(t *testing.T) {
	account := "GCFOH4PUYAXJH75SLBXT7NZWOT2JWXGCBI6YF3RXV6LXUHJCCKA4HH4I"
	got := AccountBucket(account, 256)
	if got != AccountBucket(account, 256) {
		t.Fatalf("bucket is not deterministic")
	}
	if got < 0 || got >= 256 {
		t.Fatalf("bucket %d outside expected range", got)
	}
	if AccountBucket(account, 0) != got {
		t.Fatalf("zero bucket count should use default")
	}
}

func TestConfigDefaultsToAccountLedgerIndex(t *testing.T) {
	path := writeTempConfig(t, `
service:
  name: account-index-transformer
silver_hot:
  host: localhost
catalog:
  host: localhost
index_cold:
  catalog_name: testnet_catalog
  schema_name: index
`)

	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if cfg.IndexCold.TableName != "account_ledger_index" {
		t.Fatalf("unexpected table: %s", cfg.IndexCold.TableName)
	}
	if cfg.IndexCold.TableKind != "account_ledger" {
		t.Fatalf("unexpected table kind: %s", cfg.IndexCold.TableKind)
	}
	if cfg.Checkpoint.Table != "index.account_ledger_transformer_checkpoint" {
		t.Fatalf("unexpected checkpoint table: %s", cfg.Checkpoint.Table)
	}
	if cfg.Backfill.Table != "index.account_ledger_backfill_checkpoint" {
		t.Fatalf("unexpected backfill checkpoint table: %s", cfg.Backfill.Table)
	}
	if cfg.AccountBucketCount() != 256 {
		t.Fatalf("unexpected bucket count: %d", cfg.AccountBucketCount())
	}
}

func TestConfigCanOverrideAccountIndexSettings(t *testing.T) {
	path := writeTempConfig(t, `
service:
  name: account-index-transformer
silver_hot:
  host: localhost
catalog:
  host: localhost
index_cold:
  catalog_name: testnet_catalog
  schema_name: index
  table_name: account_ledger_index_v2
  duckdb_path: /tmp/duckdb/index_writer.db
checkpoint:
  table: index.custom_account_checkpoint
account_index:
  account_bucket_count: 512
`)

	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if cfg.Checkpoint.Table != "index.custom_account_checkpoint" {
		t.Fatalf("unexpected checkpoint: %s", cfg.Checkpoint.Table)
	}
	if cfg.AccountBucketCount() != 512 {
		t.Fatalf("unexpected bucket count: %d", cfg.AccountBucketCount())
	}
	accountIndex := cfg.IndexConfig()
	if accountIndex.TableName != "account_ledger_index_v2" {
		t.Fatalf("unexpected account table: %s", accountIndex.TableName)
	}
	if accountIndex.TableKind != "account_ledger" {
		t.Fatalf("unexpected account table kind: %s", accountIndex.TableKind)
	}
}

func TestAccountLedgerDDLIsRangeGranularAndBucketPartitioned(t *testing.T) {
	writer := &IndexWriter{config: &IndexColdConfig{TableKind: "account_ledger"}}
	ddl, partition := writer.createTableDDL("testnet_catalog.index.account_ledger_index")
	if partition != "account_bucket" {
		t.Fatalf("partition = %s, want account_bucket", partition)
	}
	for _, want := range []string{"account_id VARCHAR", "account_bucket BIGINT", "ledger_range BIGINT"} {
		if !strings.Contains(ddl, want) {
			t.Fatalf("DDL missing %q:\n%s", want, ddl)
		}
	}
	for _, notWant := range []string{"ledger_sequence", "created_at", "first_seen_at"} {
		if strings.Contains(ddl, notWant) {
			t.Fatalf("DDL should not contain %q:\n%s", notWant, ddl)
		}
	}
}

func TestAccountLedgerHotParticipantQueryShape(t *testing.T) {
	query := buildAccountLedgerRangesQuery(100000)
	for _, want := range []string{
		"CAST(ledger_sequence / 100000 AS BIGINT) AS ledger_range",
		"FROM enriched_history_operations",
		"SELECT source_account AS account_id",
		"SELECT destination AS account_id",
		"SELECT from_account AS account_id",
		"SELECT to_address AS account_id",
		"SELECT address AS account_id",
		"FROM token_transfers_raw",
		"SELECT to_account AS account_id",
		"FROM contract_invocations_raw",
	} {
		if !strings.Contains(query, want) {
			t.Fatalf("query missing %q:\n%s", want, query)
		}
	}
}

func TestAccountLedgerColdParticipantQueryShapeAvoidsUnsafeEHOColumns(t *testing.T) {
	query := buildColdAccountLedgerRangesQuery("testnet_catalog", "silver", 100000)
	for _, want := range []string{
		"FROM testnet_catalog.silver.enriched_history_operations",
		"SELECT source_account AS account_id",
		"SELECT destination AS account_id",
		"FROM testnet_catalog.silver.token_transfers_raw",
		"SELECT from_account AS account_id",
		"SELECT to_account AS account_id",
		"FROM testnet_catalog.silver.contract_invocations_raw",
		"CAST(ledger_sequence / 100000 AS BIGINT) AS ledger_range",
	} {
		if !strings.Contains(query, want) {
			t.Fatalf("cold query missing %q:\n%s", want, query)
		}
	}
	for _, notWant := range []string{
		"to_address AS account_id",
		"address AS account_id",
	} {
		if strings.Contains(query, notWant) {
			t.Fatalf("cold query should not include unsafe EHO column %q:\n%s", notWant, query)
		}
	}
}

func writeTempConfig(t *testing.T, content string) string {
	t.Helper()
	f, err := os.CreateTemp(t.TempDir(), "config-*.yaml")
	if err != nil {
		t.Fatalf("CreateTemp: %v", err)
	}
	if _, err := f.WriteString(content); err != nil {
		t.Fatalf("WriteString: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	return f.Name()
}
