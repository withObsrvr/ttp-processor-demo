package main

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"

	_ "github.com/duckdb/duckdb-go/v2"
)

func TestPlanChunksDeterministic(t *testing.T) {
	got := PlanChunks(3, 13, 5)
	want := []Chunk{{Start: 3, End: 7, Index: 0}, {Start: 8, End: 12, Index: 1}, {Start: 13, End: 13, Index: 2}}
	if len(got) != len(want) {
		t.Fatalf("len = %d, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("chunk %d = %+v, want %+v", i, got[i], want[i])
		}
	}
}

func TestRequiredProjectionContractIncludesCheckpointTargets(t *testing.T) {
	got := requiredProjections("serving")
	if len(got) != 20 {
		t.Fatalf("required projection count = %d, want 20", len(got))
	}
	checkpointed := 0
	for _, p := range got {
		if !p.Required {
			t.Fatalf("%s required=%v, want true", p.Name, p.Required)
		}
		if p.Checkpoint {
			checkpointed++
		}
		if (p.Name == "sv_events_recent" || p.Name == "sv_explorer_events_recent") && (p.Checkpoint || p.InitialClass != "blocked_source_mapping") {
			t.Fatalf("%s checkpoint=%v class=%s, want false/blocked_source_mapping", p.Name, p.Checkpoint, p.InitialClass)
		}
	}
	if checkpointed != 18 {
		t.Fatalf("checkpointed projection count = %d, want 18", checkpointed)
	}
}

func TestTOIDHelpersMatchSEP35Packing(t *testing.T) {
	if got, want := transactionTOID(3, 1), int64(12884905984); got != want {
		t.Fatalf("transactionTOID(3,1) = %d, want %d", got, want)
	}
	if got, want := operationTOID(3, 1, 1), int64(12884905985); got != want {
		t.Fatalf("operationTOID(3,1,1) = %d, want %d", got, want)
	}
	if got := operationTOID(3, 1, 1) & toidOperationMask; got != 1 {
		t.Fatalf("operation bits = %d, want 1", got)
	}
}

func TestAssignedChunkOverridesChunkPlan(t *testing.T) {
	cfg := Config{Start: 3, End: 100, Chunk: 10, ChunkStart: 23, ChunkEnd: 32}
	got := cfg.PlannedChunks()
	if len(got) != 1 || got[0].Start != 23 || got[0].End != 32 {
		t.Fatalf("PlannedChunks() = %+v, want assigned chunk 23..32", got)
	}
}

func TestClassifiedOptionalTablesAreDocumented(t *testing.T) {
	got := classifiedOptionalTables("serving")
	if len(got) < 17 {
		t.Fatalf("classified optional table count = %d, want at least 17", len(got))
	}
	for _, p := range got {
		if p.InitialClass == "" || p.Rationale == "" {
			t.Fatalf("%s missing class/rationale", p.Name)
		}
	}
}

func TestClassifyFailurePrecedence(t *testing.T) {
	if got := classifyFailure(errors.New("postgres schema mismatch")); got != FailureNonRetryableSchema {
		t.Fatalf("classifyFailure schema = %s", got)
	}
	if got := classifyFailure(errors.New("target postgres connection timed out")); got != FailureRetryableInfrastructure {
		t.Fatalf("classifyFailure timeout = %s", got)
	}
}

func TestFeedBackfillRerunResumeNoDuplicates(t *testing.T) {
	ctx := context.Background()
	db := openBackfillFixtureDB(t)
	defer db.Close()
	loadBackfillFixture(t, ctx, db)

	cfg := Config{
		Network:       "mainnet",
		Start:         3,
		End:           6,
		Chunk:         2,
		RetentionDays: 30,
		BronzeSchema:  "bronze",
		SilverSchema:  "silver",
		ServingSchema: "serving",
	}
	backfiller := NewBackfillerWithDB(db, cfg)
	if err := backfiller.ensureServingSchema(ctx); err != nil {
		t.Fatal(err)
	}
	if err := backfiller.ensureManifest(ctx); err != nil {
		t.Fatal(err)
	}

	var out bytes.Buffer
	if err := backfiller.Run(ctx, &out); err != nil {
		t.Fatalf("first run: %v\n%s", err, out.String())
	}
	if err := backfiller.Run(ctx, &out); err != nil {
		t.Fatalf("rerun: %v\n%s", err, out.String())
	}

	assertBackfillCount(t, db, `SELECT COUNT(*) FROM serving.sv_ledger_stats_recent`, 4)
	assertBackfillCount(t, db, `SELECT COUNT(*) FROM serving.sv_transactions_recent`, 3)
	assertBackfillCount(t, db, `SELECT transaction_id FROM serving.sv_transactions_recent WHERE tx_hash='tx3' AND tx_envelope='env3' AND tx_result='res3' AND tx_meta='meta3' AND tx_fee_meta='fee3' AND tx_signers='["sig3"]'`, transactionTOID(3, 1))
	assertBackfillCount(t, db, `SELECT COUNT(*) FROM serving.sv_operations_recent`, 4)
	assertBackfillCount(t, db, `SELECT COUNT(*) FROM serving.sv_contract_calls_recent`, 2)
	assertBackfillCount(t, db, `SELECT COUNT(*) FROM serving.sv_tx_receipts`, 3)
	assertBackfillCount(t, db, `SELECT COUNT(*) FROM serving.sv_transactions_by_account`, 6)
	assertBackfillCount(t, db, `SELECT COUNT(*) FROM serving.sv_operations_by_account`, 7)
	assertBackfillCount(t, db, `SELECT COUNT(*) FROM serving.sv_accounts_current`, 2)
	assertBackfillCount(t, db, `SELECT COUNT(*) FROM serving.sv_account_balances_current`, 3)
	assertBackfillCount(t, db, `SELECT COUNT(*) FROM serving.sv_network_stats_current`, 1)
	assertBackfillCount(t, db, `SELECT COUNT(*) FROM serving.sv_assets_current`, 2)
	assertBackfillCount(t, db, `SELECT COUNT(*) FROM serving.sv_asset_stats_current`, 2)
	assertBackfillCount(t, db, `SELECT COUNT(*) FROM serving.sv_contracts_current`, 1)
	assertBackfillCount(t, db, `SELECT COUNT(*) FROM serving.sv_contract_storage_current`, 2)
	assertBackfillString(t, db, `SELECT type FROM serving.sv_contract_storage_current WHERE key_hash='K2'`, "instance")
	assertBackfillCount(t, db, `SELECT COUNT(*) FROM serving.sv_contract_storage_current WHERE key_hash='K1' AND ttl_remaining=4 AND expired=false`, 1)
	assertBackfillCount(t, db, `SELECT COUNT(*) FROM serving.sv_contract_storage_current WHERE key_hash='K2' AND ttl_remaining=-1 AND expired=true`, 1)
	assertBackfillCount(t, db, `SELECT COUNT(*) FROM serving.sv_contract_storage_summary`, 1)
	assertBackfillCount(t, db, `SELECT live_entries FROM serving.sv_contract_storage_summary WHERE contract_id='CC1'`, 1)
	assertBackfillCount(t, db, `SELECT expired_entries FROM serving.sv_contract_storage_summary WHERE contract_id='CC1'`, 1)
	assertBackfillCount(t, db, `SELECT COUNT(*) FROM serving.sv_contract_stats_current`, 1)
	assertBackfillCount(t, db, `SELECT COUNT(*) FROM serving.sv_contract_function_stats_current`, 1)
	assertBackfillCount(t, db, `SELECT COUNT(*) FROM serving.sv_contract_activity_summary`, 1)
	assertBackfillString(t, db, `SELECT activity_classification FROM serving.sv_contract_activity_summary WHERE contract_id='CC1'`, "invoked_contract")
	assertBackfillCount(t, db, `SELECT invocation_count_7d FROM serving.sv_contract_activity_summary WHERE contract_id='CC1'`, 2)
	assertBackfillCount(t, db, `SELECT COUNT(*) FROM serving.sv_backfill_manifest WHERE status='completed'`, 25)
	assertBackfillCount(t, db, `SELECT COUNT(*) FROM serving.sv_projection_checkpoints WHERE last_ledger_sequence=6`, 18)
	assertBackfillCount(t, db, `SELECT COUNT(*) FROM serving.sv_watermarks WHERE status='complete' AND complete_thru=6`, 18)
	assertBackfillCount(t, db, `SELECT COUNT(*) FROM ops.consumers WHERE pipeline='serving-cold-backfill' AND checkpoint=6`, 18)
	assertBackfillCount(t, db, `SELECT COUNT(*) FROM (SELECT tx_hash, COUNT(*) n FROM serving.sv_transactions_recent GROUP BY tx_hash HAVING COUNT(*) > 1)`, 0)
	assertBackfillCount(t, db, `SELECT COUNT(*) FROM (SELECT operation_id, COUNT(*) n FROM serving.sv_operations_recent GROUP BY operation_id HAVING COUNT(*) > 1)`, 0)
	assertBackfillCount(t, db, `SELECT COUNT(*) FROM (SELECT account_id, toid, COUNT(*) n FROM serving.sv_transactions_by_account GROUP BY account_id, toid HAVING COUNT(*) > 1)`, 0)
	assertBackfillCount(t, db, `SELECT COUNT(*) FROM (SELECT account_id, operation_toid, COUNT(*) n FROM serving.sv_operations_by_account GROUP BY account_id, operation_toid HAVING COUNT(*) > 1)`, 0)
	assertBackfillCount(t, db, `SELECT COUNT(*) FROM duckdb_indexes() WHERE index_name IN ('sv_transactions_by_account_page_idx', 'sv_operations_by_account_page_idx')`, 2)
	assertBackfillCount(t, db, `SELECT toid FROM serving.sv_transactions_by_account WHERE account_id='GA1' AND tx_hash='tx3'`, transactionTOID(3, 1))
	assertBackfillCount(t, db, `SELECT operation_toid FROM serving.sv_operations_by_account WHERE account_id='GA1' AND tx_hash='tx3'`, operationTOID(3, 1, 1))
	assertBackfillString(t, db, `SELECT typeof(involved_accounts) FROM serving.sv_tx_receipts LIMIT 1`, "VARCHAR[]")
	assertBackfillCount(t, db, `SELECT total_calls_24h FROM serving.sv_contract_stats_current WHERE contract_id='CC1'`, 1)
	assertBackfillCount(t, db, `SELECT total_calls_7d FROM serving.sv_contract_stats_current WHERE contract_id='CC1'`, 2)
	assertBackfillCount(t, db, `SELECT calls_24h FROM serving.sv_contract_function_stats_current WHERE contract_id='CC1' AND function_name='transfer'`, 1)
	assertBackfillCount(t, db, `SELECT calls_7d FROM serving.sv_contract_function_stats_current WHERE contract_id='CC1' AND function_name='transfer'`, 2)

	resumeCfg := cfg
	resumeCfg.Resume = true
	resumeBackfiller := NewBackfillerWithDB(db, resumeCfg)
	out.Reset()
	if err := resumeBackfiller.Run(ctx, &out); err != nil {
		t.Fatalf("resume run: %v\n%s", err, out.String())
	}
	if got := strings.Count(out.String(), "component.chunk_skipped"); got != 2 {
		t.Fatalf("resume skipped chunks = %d, want 2\n%s", got, out.String())
	}
	assertBackfillCount(t, db, `SELECT COUNT(*) FROM serving.sv_transactions_recent`, 3)
	assertBackfillCount(t, db, `SELECT COUNT(*) FROM serving.sv_backfill_manifest WHERE status='completed'`, 25)
	assertBackfillCount(t, db, `SELECT COUNT(*) FROM serving.sv_projection_checkpoints WHERE last_ledger_sequence=6`, 18)
}

func TestByAccountOnlyProjectionSelection(t *testing.T) {
	ctx := context.Background()
	db := openBackfillFixtureDB(t)
	defer db.Close()
	loadBackfillFixture(t, ctx, db)

	cfg := Config{
		Network:         "mainnet",
		Start:           3,
		End:             6,
		Chunk:           4,
		RetentionDays:   30,
		BronzeSchema:    "bronze",
		SilverSchema:    "silver",
		ServingSchema:   "serving",
		FeedProjections: "sv_transactions_by_account,sv_operations_by_account",
		SkipCurrent:     true,
	}
	backfiller := NewBackfillerWithDB(db, cfg)
	if err := backfiller.ensureServingSchema(ctx); err != nil {
		t.Fatal(err)
	}
	if err := backfiller.ensureManifest(ctx); err != nil {
		t.Fatal(err)
	}

	var out bytes.Buffer
	if err := backfiller.Run(ctx, &out); err != nil {
		t.Fatalf("by-account run: %v\n%s", err, out.String())
	}

	assertBackfillCount(t, db, `SELECT COUNT(*) FROM serving.sv_transactions_by_account`, 6)
	assertBackfillCount(t, db, `SELECT COUNT(*) FROM serving.sv_operations_by_account`, 7)
	assertBackfillCount(t, db, `SELECT COUNT(*) FROM serving.sv_backfill_manifest WHERE status='completed'`, 2)
	assertBackfillCount(t, db, `SELECT COUNT(*) FROM serving.sv_projection_checkpoints WHERE last_ledger_sequence=6`, 2)
	assertBackfillCount(t, db, `SELECT COUNT(*) FROM serving.sv_watermarks WHERE status='complete' AND complete_thru=6`, 2)
	assertBackfillCount(t, db, `SELECT COUNT(*) FROM ops.consumers WHERE pipeline='serving-cold-backfill' AND checkpoint=6`, 2)
	assertBackfillCount(t, db, `SELECT COUNT(*) FROM duckdb_tables() WHERE schema_name='serving' AND table_name='sv_transactions_recent'`, 0)
}

func TestUnknownProjectionSelectionFails(t *testing.T) {
	if _, err := selectFeedProjections(feedProjections(), "sv_transactions_by_account,missing_projection"); err == nil {
		t.Fatal("expected unknown feed projection error")
	}
	if _, err := selectCurrentProjections(currentProjections(), "missing_projection"); err == nil {
		t.Fatal("expected unknown current projection error")
	}
}

func TestCheckpointHandoffRequiresCurrentProjectionSuccess(t *testing.T) {
	ctx := context.Background()
	db := openBackfillFixtureDB(t)
	defer db.Close()
	loadBackfillFixture(t, ctx, db)
	if _, err := db.ExecContext(ctx, `DROP TABLE silver.accounts_current`); err != nil {
		t.Fatal(err)
	}
	cfg := Config{
		Network:       "mainnet",
		Start:         3,
		End:           6,
		Chunk:         2,
		RetentionDays: 30,
		BronzeSchema:  "bronze",
		SilverSchema:  "silver",
		ServingSchema: "serving",
	}
	backfiller := NewBackfillerWithDB(db, cfg)
	if err := backfiller.ensureServingSchema(ctx); err != nil {
		t.Fatal(err)
	}
	if err := backfiller.ensureManifest(ctx); err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	err := backfiller.Run(ctx, &out)
	if err == nil {
		t.Fatal("expected current projection failure")
	}
	assertBackfillCount(t, db, `SELECT COUNT(*) FROM serving.sv_projection_checkpoints`, 0)
}

func openBackfillFixtureDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	return db
}

func loadBackfillFixture(t *testing.T, ctx context.Context, db *sql.DB) {
	t.Helper()
	stmts := []string{
		`CREATE SCHEMA bronze`,
		`CREATE SCHEMA silver`,
		`CREATE SCHEMA serving`,
		`CREATE TABLE silver.enriched_ledgers (
			network VARCHAR, ledger_sequence BIGINT, ledger_closed_at TIMESTAMP, ledger_hash VARCHAR,
			previous_ledger_hash VARCHAR, ledger_version INTEGER, base_fee BIGINT,
			successful_tx_count INTEGER, failed_tx_count INTEGER, operation_count INTEGER
		)`,
		`CREATE TABLE bronze.transactions_row_v2 (
			transaction_hash VARCHAR, ledger_sequence BIGINT, created_at TIMESTAMP, source_account VARCHAR,
			fee_charged BIGINT, max_fee BIGINT, successful BOOLEAN, operation_count INTEGER,
			soroban_contract_id VARCHAR, memo_type VARCHAR, memo VARCHAR, account_sequence BIGINT,
			soroban_resources_instructions BIGINT, soroban_resources_read_bytes BIGINT, soroban_resources_write_bytes BIGINT,
			transaction_id BIGINT, tx_envelope VARCHAR, tx_result VARCHAR, tx_meta VARCHAR, tx_fee_meta VARCHAR, tx_signers VARCHAR
		)`,
		`CREATE TABLE bronze.operations_row_v2 (
			transaction_hash VARCHAR, ledger_sequence BIGINT, operation_index INTEGER,
			transaction_index INTEGER, transaction_id BIGINT, operation_id BIGINT
		)`,
		`CREATE TABLE silver.enriched_history_operations (
			network VARCHAR, transaction_hash VARCHAR, operation_index INTEGER, ledger_sequence BIGINT,
			created_at TIMESTAMP, type INTEGER, type_string VARCHAR, source_account VARCHAR,
			destination VARCHAR, asset VARCHAR, amount VARCHAR, contract_id VARCHAR, function_name VARCHAR,
			tx_successful BOOLEAN, is_payment_op BOOLEAN, is_soroban_op BOOLEAN
		)`,
		`CREATE TABLE silver.contract_invocations_raw (
			network VARCHAR, transaction_hash VARCHAR, operation_index INTEGER, ledger_sequence BIGINT,
			closed_at TIMESTAMP, contract_id VARCHAR, source_account VARCHAR, function_name VARCHAR, successful BOOLEAN
		)`,
		`CREATE TABLE silver.accounts_current (
			network VARCHAR, account_id VARCHAR, balance VARCHAR, sequence_number BIGINT,
			num_subentries INTEGER, created_at TIMESTAMP, last_modified_ledger BIGINT,
			updated_at TIMESTAMP, home_domain VARCHAR, master_weight INTEGER,
			low_threshold INTEGER, med_threshold INTEGER, high_threshold INTEGER,
			signers VARCHAR, ledger_range BIGINT
		)`,
		`CREATE TABLE silver.address_balances_current (
			network VARCHAR, owner_address VARCHAR, asset_key VARCHAR, asset_code VARCHAR,
			asset_issuer VARCHAR, asset_type VARCHAR, balance_raw VARCHAR,
			balance_display VARCHAR, last_updated_ledger BIGINT, updated_at TIMESTAMP
		)`,
		`CREATE TABLE silver.contract_metadata (
			network VARCHAR, contract_id VARCHAR, creator_address VARCHAR, wasm_hash VARCHAR,
			created_ledger BIGINT, created_at TIMESTAMP
		)`,
		`CREATE TABLE silver.contract_data_current (
			network VARCHAR, contract_id VARCHAR, key_hash VARCHAR, durability VARCHAR,
			asset_type VARCHAR, asset_code VARCHAR, asset_issuer VARCHAR, data_value VARCHAR,
			last_modified_ledger BIGINT, ledger_sequence BIGINT, closed_at TIMESTAMP,
			created_at TIMESTAMP, ledger_range BIGINT, updated_at TIMESTAMP
		)`,
		`CREATE TABLE silver.ttl_current (
			network VARCHAR, key_hash VARCHAR, live_until_ledger_seq BIGINT,
			ttl_remaining BIGINT, expired BOOLEAN, last_modified_ledger BIGINT,
			ledger_sequence BIGINT, closed_at TIMESTAMP, created_at TIMESTAMP,
			ledger_range BIGINT, updated_at TIMESTAMP
		)`,
		`INSERT INTO silver.enriched_ledgers VALUES
			('mainnet',3,'2026-01-01 00:00:03','h3','h2',23,100,1,0,1),
			('mainnet',4,'2026-01-01 00:00:04','h4','h3',23,100,1,0,2),
			('mainnet',5,'2026-01-01 00:00:05','h5','h4',23,100,0,1,1),
			('mainnet',6,'2026-01-01 00:00:06','h6','h5',23,100,1,0,0)`,
		`INSERT INTO bronze.transactions_row_v2 VALUES
			('tx3',3,'2026-01-01 00:00:03','GA1',100,200,true,1,NULL,'none',NULL,10,NULL,NULL,NULL,12884905984,'env3','res3','meta3','fee3','["sig3"]'),
			('tx4',4,'2026-01-01 00:00:04','GA2',101,201,true,2,'CC1','text','memo',11,1000,10,20,17179873280,'env4','res4','meta4','fee4','["sig4"]'),
			('tx5',5,'2026-01-01 00:00:05','GA3',102,202,false,1,NULL,'none',NULL,12,NULL,NULL,NULL,21474840576,'env5','res5','meta5','fee5','["sig5"]')`,
		`INSERT INTO bronze.operations_row_v2 VALUES
			('tx3',3,0,1,12884905984,12884905985),
			('tx4',4,0,1,17179873280,17179873281),
			('tx4',4,1,1,17179873280,17179873282),
			('tx5',5,0,1,21474840576,21474840577)`,
		`INSERT INTO silver.enriched_history_operations VALUES
			('mainnet','tx3',0,3,'2026-01-01 00:00:03',1,'payment','GA1','GB1','native','100',NULL,NULL,true,true,false),
			('mainnet','tx4',0,4,'2026-01-01 00:00:04',24,'invoke_host_function','GA2',NULL,NULL,NULL,'CC1','transfer',true,false,true),
			('mainnet','tx4',1,4,'2026-01-01 00:00:04',1,'payment','GA2','GB2','USD:ISSUER','50',NULL,NULL,true,true,false),
			('mainnet','tx5',0,5,'2026-01-01 00:00:05',1,'payment','GA3','GB3','native','75',NULL,NULL,false,true,false)`,
		`INSERT INTO silver.contract_invocations_raw VALUES
			('mainnet','tx3',0,3,'2025-12-30 00:00:03','CC1','GA9','transfer',true),
			('mainnet','tx4',0,4,'2026-01-01 00:00:04','CC1','GA2','transfer',true)`,
		`INSERT INTO silver.accounts_current VALUES
			('mainnet','GA1','1000',10,1,'2026-01-01 00:00:03',4,'2026-01-01 00:00:04',NULL,1,1,1,1,'[]',3),
			('mainnet','GA2','2000',11,1,'2026-01-01 00:00:04',5,'2026-01-01 00:00:05','example.com',1,1,1,1,'[]',4),
			('mainnet','GA3','3000',12,1,'2026-01-01 00:00:08',8,'2026-01-01 00:00:08',NULL,1,1,1,1,'[]',8)`,
		`INSERT INTO silver.address_balances_current VALUES
			('mainnet','GA1','native:XLM','XLM',NULL,'native','1000','0.0001000',4,'2026-01-01 00:00:04'),
			('mainnet','GA2','native:XLM','XLM',NULL,'native','2000','0.0002000',5,'2026-01-01 00:00:05'),
			('mainnet','GA2','credit_alphanum4:USD:ISSUER','USD','ISSUER','credit_alphanum4','50000000','5.0000000',5,'2026-01-01 00:00:05'),
			('mainnet','GA3','native:XLM','XLM',NULL,'native','3000','0.0003000',8,'2026-01-01 00:00:08')`,
		`INSERT INTO silver.contract_metadata VALUES
			('mainnet','CC1','GA2','wasm1',4,'2026-01-01 00:00:04'),
			('mainnet','CC2','GA3','wasm2',8,'2026-01-01 00:00:08')`,
		`INSERT INTO silver.contract_data_current VALUES
			('mainnet','CC1','K1','persistent',NULL,NULL,NULL,'AAAA',4,4,'2026-01-01 00:00:04','2026-01-01 00:00:04',4,'2026-01-01 00:00:04'),
			('mainnet','CC1','K2','instance',NULL,NULL,NULL,'BBBBBB',5,5,'2026-01-01 00:00:05','2026-01-01 00:00:05',5,'2026-01-01 00:00:05'),
			('mainnet','CC2','K3','persistent',NULL,NULL,NULL,'CCCC',8,8,'2026-01-01 00:00:08','2026-01-01 00:00:08',8,'2026-01-01 00:00:08')`,
		`INSERT INTO silver.ttl_current VALUES
			('mainnet','K1',10,4,false,4,4,'2026-01-01 00:00:04','2026-01-01 00:00:04',4,'2026-01-01 00:00:04'),
			('mainnet','K2',5,-1,true,5,5,'2026-01-01 00:00:05','2026-01-01 00:00:05',5,'2026-01-01 00:00:05'),
			('mainnet','K3',20,14,false,8,8,'2026-01-01 00:00:08','2026-01-01 00:00:08',8,'2026-01-01 00:00:08')`,
	}
	for _, stmt := range stmts {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			t.Fatalf("fixture failed: %v\n%s", err, stmt)
		}
	}
}

func assertBackfillCount(t *testing.T, db *sql.DB, query string, want int64) {
	t.Helper()
	var got int64
	if err := db.QueryRow(query).Scan(&got); err != nil {
		t.Fatalf("%s: %v", query, err)
	}
	if got != want {
		t.Fatalf("%s = %d, want %d", query, got, want)
	}
}

func assertBackfillString(t *testing.T, db *sql.DB, query, want string) {
	t.Helper()
	var got string
	if err := db.QueryRow(query).Scan(&got); err != nil {
		t.Fatalf("%s: %v", query, err)
	}
	if got != want {
		t.Fatalf("%s = %q, want %q", query, got, want)
	}
}
