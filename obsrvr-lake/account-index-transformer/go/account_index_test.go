package main

import (
	"context"
	"os"
	"strings"
	"testing"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
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
	if cfg.ServingFeed.Enabled {
		t.Fatalf("serving feed should default disabled")
	}
	if cfg.ServingFeed.TransactionsTable != "sv_transactions_by_account" {
		t.Fatalf("unexpected serving feed tx table: %s", cfg.ServingFeed.TransactionsTable)
	}
	if cfg.ServingFeed.ConsumerTable != "ops.consumers" {
		t.Fatalf("unexpected serving feed consumer table: %s", cfg.ServingFeed.ConsumerTable)
	}
	if cfg.IndexPostgres.Enabled {
		t.Fatalf("Postgres index sink should default disabled")
	}
	if cfg.IndexPostgres.Mode != "mirror" {
		t.Fatalf("unexpected Postgres index mode: %s", cfg.IndexPostgres.Mode)
	}
	if cfg.IndexPostgres.Table != "account_ledger_index" {
		t.Fatalf("unexpected Postgres index table: %s", cfg.IndexPostgres.Table)
	}
	if cfg.IndexPostgres.CheckpointTable != "index.account_ledger_postgres_checkpoint" {
		t.Fatalf("unexpected Postgres index checkpoint: %s", cfg.IndexPostgres.CheckpointTable)
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
index_postgres:
  enabled: true
  mode: primary
  schema: index_v2
  table: account_ledger_index_pg
  checkpoint_table: index_v2.account_checkpoint
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
	if !cfg.IndexPostgres.Enabled {
		t.Fatalf("Postgres index sink should be enabled")
	}
	if cfg.IndexPostgres.Mode != "primary" {
		t.Fatalf("unexpected Postgres index mode: %s", cfg.IndexPostgres.Mode)
	}
	if cfg.IndexPostgres.Schema != "index_v2" {
		t.Fatalf("unexpected Postgres index schema: %s", cfg.IndexPostgres.Schema)
	}
	if cfg.IndexPostgres.Table != "account_ledger_index_pg" {
		t.Fatalf("unexpected Postgres index table: %s", cfg.IndexPostgres.Table)
	}
	if cfg.IndexPostgres.CheckpointTable != "index_v2.account_checkpoint" {
		t.Fatalf("unexpected Postgres index checkpoint: %s", cfg.IndexPostgres.CheckpointTable)
	}
}

func TestAccountLedgerRangeBounds(t *testing.T) {
	from, to := AccountLedgerRangeBounds(34, 100000)
	if from != 3400000 || to != 3499999 {
		t.Fatalf("range 34 bounds = %d-%d, want 3400000-3499999", from, to)
	}
	from, to = AccountLedgerRangeBounds(3, 50000)
	if from != 150000 || to != 199999 {
		t.Fatalf("range 3 bounds = %d-%d, want 150000-199999", from, to)
	}
}

func TestResolveBackfillStart(t *testing.T) {
	tests := []struct {
		name       string
		opts       BackfillOptions
		checkpoint int64
		want       int64
	}{
		{
			name:       "default resumes after checkpoint",
			opts:       BackfillOptions{},
			checkpoint: 100,
			want:       101,
		},
		{
			name:       "explicit older start resumes unless forced",
			opts:       BackfillOptions{StartLedger: 3},
			checkpoint: 100,
			want:       101,
		},
		{
			name:       "force restart honors explicit start",
			opts:       BackfillOptions{StartLedger: 3, ForceRestart: true},
			checkpoint: 100,
			want:       3,
		},
		{
			name:       "explicit newer start is honored",
			opts:       BackfillOptions{StartLedger: 200},
			checkpoint: 100,
			want:       200,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := resolveBackfillStart(tt.opts, tt.checkpoint); got != tt.want {
				t.Fatalf("resolveBackfillStart=%d want %d", got, tt.want)
			}
		})
	}
}

func TestPostgresIndexWriterLoadCheckpoint(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	writer := NewPostgresIndexWriter(db, IndexPostgresConfig{
		Schema:          "index",
		Table:           "account_ledger_index",
		CheckpointTable: "index.account_ledger_postgres_checkpoint",
	}, 100000)
	mock.ExpectExec("CREATE SCHEMA IF NOT EXISTS").
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(`CREATE TABLE IF NOT EXISTS "index"\."account_ledger_index"`).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(`CREATE INDEX IF NOT EXISTS "account_ledger_index_bucket_account_range_idx"`).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(`CREATE INDEX IF NOT EXISTS "account_ledger_index_account_ledger_to_idx"`).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec("CREATE TABLE IF NOT EXISTS index.account_ledger_postgres_checkpoint").
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery("SELECT checkpoint").
		WithArgs("index.account_ledger_index").
		WillReturnRows(sqlmock.NewRows([]string{"checkpoint"}).AddRow(int64(3466943)))

	got, err := writer.LoadCheckpoint(context.Background())
	if err != nil {
		t.Fatalf("LoadCheckpoint: %v", err)
	}
	if got != 3466943 {
		t.Fatalf("checkpoint=%d want 3466943", got)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
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

func TestAccountFeedHotQueryShape(t *testing.T) {
	query := buildAccountFeedRowsQuery()
	for _, want := range []string{
		"e.transaction_id IS NOT NULL",
		"e.operation_id IS NOT NULL",
		"transaction_id AS toid",
		"operation_id AS operation_toid",
		"SELECT source_account AS account_id",
		"SELECT destination AS account_id",
		"SELECT from_account AS account_id",
		"SELECT to_address AS account_id",
		"SELECT address AS account_id",
		"SELECT into_account AS account_id",
		"COALESCE(tx_successful, transaction_successful, false) AS successful",
		// Roles must be OR-merged, not deduplicated: an account that is both
		// source and destination of the same operation keeps both mask bits.
		"BIT_OR(source_mask)::SMALLINT AS source_mask",
		"GROUP BY 1, 3",
	} {
		if !strings.Contains(query, want) {
			t.Fatalf("feed query missing %q:\n%s", want, query)
		}
	}
	if strings.Contains(query, "DISTINCT ON") {
		t.Fatalf("feed query must not dedupe participant roles with DISTINCT ON:\n%s", query)
	}
}

// servingSchemaColumnDefs parses the canonical serving schema (owned by
// serving-projection-processor) and returns column -> definition for a table.
func servingSchemaColumnDefs(t *testing.T, table string) map[string]string {
	t.Helper()
	raw, err := os.ReadFile("../../serving-projection-processor/go/schema/serving_schema.sql")
	if err != nil {
		t.Fatalf("read serving schema: %v", err)
	}
	marker := "create table if not exists " + table + " ("
	idx := strings.Index(string(raw), marker)
	if idx < 0 {
		t.Fatalf("table %s not found in serving schema", table)
	}
	body := string(raw)[idx+len(marker):]
	end := strings.Index(body, ");")
	if end < 0 {
		t.Fatalf("unterminated definition for %s", table)
	}
	cols := map[string]string{}
	for _, line := range strings.Split(body[:end], "\n") {
		line = strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(line), ","))
		if line == "" || strings.HasPrefix(line, "primary key") {
			continue
		}
		cols[strings.Fields(line)[0]] = strings.ToLower(line)
	}
	return cols
}

func TestServingFeedWriterColumnsMatchServingSchema(t *testing.T) {
	for table, insertCols := range map[string][]string{
		"serving.sv_transactions_by_account": servingFeedTxInsertColumns,
		"serving.sv_operations_by_account":   servingFeedOpInsertColumns,
	} {
		schemaCols := servingSchemaColumnDefs(t, table)
		inserted := map[string]bool{}
		for _, col := range insertCols {
			inserted[col] = true
			if _, ok := schemaCols[col]; !ok {
				t.Errorf("%s: insert column %q does not exist in serving_schema.sql", table, col)
			}
		}
		for col, def := range schemaCols {
			if strings.Contains(def, "not null") && !strings.Contains(def, "default") && !inserted[col] {
				t.Errorf("%s: NOT NULL column %q without default is not supplied by the feed insert", table, col)
			}
		}
	}
}

func TestServingFeedWriterSaveCheckpointCoversBothTables(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	writer := NewServingFeedWriter(db, ServingFeedConfig{
		Schema:            "serving",
		TransactionsTable: "sv_transactions_by_account",
		OperationsTable:   "sv_operations_by_account",
		WatermarkTable:    "serving.sv_watermarks",
		ConsumerTable:     "ops.consumers",
		Pipeline:          "account-feed",
	})

	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO ops.consumers").
		WithArgs("account-feed", "serving.sv_transactions_by_account", int64(77)).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("INSERT INTO ops.consumers").
		WithArgs("account-feed", "serving.sv_operations_by_account", int64(77)).
		WillReturnResult(sqlmock.NewResult(0, 1))

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if err := writer.saveCheckpoint(context.Background(), tx, 77); err != nil {
		t.Fatalf("saveCheckpoint: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
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
