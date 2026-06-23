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
	got := PlanChunks(3, 12, 5)
	want := []Chunk{{Start: 3, End: 7, Index: 0}, {Start: 8, End: 12, Index: 1}}
	if len(got) != len(want) {
		t.Fatalf("len = %d, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("chunk %d = %+v, want %+v", i, got[i], want[i])
		}
	}
}

func TestConfigRequiresBoundedRangeAndSilverInputs(t *testing.T) {
	_, err := parseConfig([]string{"--network", "mainnet", "--start-ledger", "3", "--end-ledger", "10"})
	if err == nil {
		t.Fatal("expected missing silver config error")
	}
}

func TestAssignedChunkOverridesChunkPlan(t *testing.T) {
	cfg := Config{Start: 3, End: 100, Chunk: 10, ChunkStart: 23, ChunkEnd: 32}
	got := cfg.PlannedChunks()
	if len(got) != 1 || got[0].Start != 23 || got[0].End != 32 {
		t.Fatalf("PlannedChunks() = %+v, want assigned chunk 23..32", got)
	}
}

func TestCurrentProjectorPlansSingleAsOfWorkUnit(t *testing.T) {
	cfg := Config{Start: 3, End: 100, Chunk: 10}
	got := cfg.PlannedChunks()
	if len(got) != 1 || got[0].Start != 3 || got[0].End != 100 {
		t.Fatalf("PlannedChunks() = %+v, want single as-of work unit 3..100", got)
	}
}

func TestClassifyFailurePrecedence(t *testing.T) {
	if got := classifyFailure(errors.New("catalog column schema mismatch")); got != FailureNonRetryableSchema {
		t.Fatalf("classifyFailure schema = %s", got)
	}
	if got := classifyFailure(errors.New("catalog postgres connection timed out")); got != FailureRetryableInfrastructure {
		t.Fatalf("classifyFailure timeout = %s", got)
	}
}

func TestProjectorDerivesCurrentTablesFromSilverFixture(t *testing.T) {
	ctx := context.Background()
	db := openFixtureDB(t)
	defer db.Close()
	loadCurrentProjectorFixture(t, ctx, db)

	cfg := Config{
		Network:      "mainnet",
		Start:        3,
		End:          5,
		Chunk:        100,
		Partitions:   3,
		SilverSchema: "silver",
	}
	projector := NewProjectorWithDB(db, cfg)
	if err := projector.ensureSchema(ctx); err != nil {
		t.Fatal(err)
	}
	if err := projector.ensureManifest(ctx); err != nil {
		t.Fatal(err)
	}

	var out bytes.Buffer
	if err := projector.Run(ctx, &out); err != nil {
		t.Fatalf("first run: %v\n%s", err, out.String())
	}
	if err := projector.Run(ctx, &out); err != nil {
		t.Fatalf("second idempotent run: %v\n%s", err, out.String())
	}

	assertCount(t, db, `SELECT COUNT(*) FROM silver.accounts_current WHERE network='mainnet'`, 2)
	assertCount(t, db, `SELECT COUNT(*) FROM silver.trustlines_current WHERE network='mainnet'`, 2)
	assertCount(t, db, `SELECT COUNT(*) FROM silver.offers_current WHERE network='mainnet'`, 1)
	assertCount(t, db, `SELECT COUNT(*) FROM silver.contract_data_current WHERE network='mainnet'`, 1)
	assertCount(t, db, `SELECT COUNT(*) FROM silver.native_balances_current WHERE network='mainnet'`, 2)
	assertCount(t, db, `SELECT COUNT(*) FROM silver.address_balances_current WHERE network='mainnet'`, 4)
	assertCount(t, db, `SELECT COUNT(*) FROM silver.silver_current_projector_manifest WHERE status='completed'`, 6)

	var accountBalance string
	if err := db.QueryRow(`SELECT balance FROM silver.accounts_current WHERE account_id='GA1'`).Scan(&accountBalance); err != nil {
		t.Fatal(err)
	}
	if accountBalance != "200" {
		t.Fatalf("GA1 balance = %s, want latest <= end ledger balance 200", accountBalance)
	}
	var trustlineBalance int64
	if err := db.QueryRow(`SELECT balance FROM silver.trustlines_current WHERE account_id='GA1' AND asset_code='USD'`).Scan(&trustlineBalance); err != nil {
		t.Fatal(err)
	}
	if trustlineBalance != 25000000 {
		t.Fatalf("trustline stroops = %d, want 25000000", trustlineBalance)
	}
	var balanceRaw, balanceDisplay string
	if err := db.QueryRow(`SELECT balance_raw, balance_display FROM silver.address_balances_current WHERE owner_address='GA1' AND asset_code='USD'`).Scan(&balanceRaw, &balanceDisplay); err != nil {
		t.Fatal(err)
	}
	if balanceRaw != "25000000" || balanceDisplay != "2.5" {
		t.Fatalf("address USD balance raw/display = %s/%s, want 25000000/2.5", balanceRaw, balanceDisplay)
	}
	var contractCount int64
	if err := db.QueryRow(`SELECT COUNT(*) FROM silver.contract_data_current WHERE contract_id='CC1' AND key_hash='K2'`).Scan(&contractCount); err != nil {
		t.Fatal(err)
	}
	if contractCount != 0 {
		t.Fatalf("deleted latest contract key survived projection")
	}
	var futureRows int64
	if err := db.QueryRow(`SELECT COUNT(*) FROM silver.accounts_current WHERE last_modified_ledger > 5`).Scan(&futureRows); err != nil {
		t.Fatal(err)
	}
	if futureRows != 0 {
		t.Fatalf("projected %d rows beyond end ledger", futureRows)
	}
}

func TestProjectorVerificationFailures(t *testing.T) {
	ctx := context.Background()
	db := openFixtureDB(t)
	defer db.Close()
	loadCurrentProjectorFixture(t, ctx, db)
	cfg := Config{Network: "mainnet", Start: 3, End: 5, Chunk: 100, Partitions: 2, SilverSchema: "silver"}
	projector := NewProjectorWithDB(db, cfg)
	if err := projector.ensureSchema(ctx); err != nil {
		t.Fatal(err)
	}
	if err := projector.ensureManifest(ctx); err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	if _, err := projector.replaceProjection(ctx, &out, Chunk{Start: 3, End: 5}, executableCurrentProjections()[0]); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO silver.accounts_current SELECT * FROM silver.accounts_current WHERE account_id='GA1'`); err != nil {
		t.Fatal(err)
	}
	_, err := projector.verifyProjection(ctx, executableCurrentProjections()[0])
	if err == nil || !strings.Contains(err.Error(), "duplicate-key verification failed") {
		t.Fatalf("duplicate verification err = %v", err)
	}

	if _, err := db.Exec(`DELETE FROM silver.accounts_current WHERE account_id='GA1'`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`UPDATE silver.accounts_current SET last_modified_ledger=99 WHERE account_id='GA2'`); err != nil {
		t.Fatal(err)
	}
	_, err = projector.verifyProjection(ctx, executableCurrentProjections()[0])
	if err == nil || !strings.Contains(err.Error(), "max-ledger verification failed") {
		t.Fatalf("max ledger verification err = %v", err)
	}
}

func TestPublishBucketsRecordedAndResumable(t *testing.T) {
	ctx := context.Background()
	db := openFixtureDB(t)
	defer db.Close()
	loadCurrentProjectorFixture(t, ctx, db)
	cfg := Config{Network: "mainnet", Start: 3, End: 5, Chunk: 100, SilverSchema: "silver", PublishBuckets: 8}
	projector := NewProjectorWithDB(db, cfg)
	if err := projector.ensureSchema(ctx); err != nil {
		t.Fatal(err)
	}
	if err := projector.ensureManifest(ctx); err != nil {
		t.Fatal(err)
	}

	var out bytes.Buffer
	proj := executableCurrentProjections()[0] // accounts_current
	if _, err := projector.replaceProjection(ctx, &out, Chunk{Start: 3, End: 5}, proj); err != nil {
		t.Fatalf("first publish: %v\n%s", err, out.String())
	}
	assertCount(t, db, `SELECT COUNT(*) FROM silver.accounts_current WHERE network='mainnet'`, 2)
	assertCount(t, db, `SELECT COUNT(*) FROM silver.silver_current_projector_publish_manifest WHERE projection_name='accounts_current' AND status='completed'`, 8)

	// Resume run: every bucket is already complete, so publish skips them and the
	// target stays correct (idempotent) without re-issuing DuckLake writes.
	projector.cfg.Resume = true
	out.Reset()
	if _, err := projector.replaceProjection(ctx, &out, Chunk{Start: 3, End: 5}, proj); err != nil {
		t.Fatalf("resume publish: %v\n%s", err, out.String())
	}
	assertCount(t, db, `SELECT COUNT(*) FROM silver.accounts_current WHERE network='mainnet'`, 2)
	if !strings.Contains(out.String(), "publish_bucket_skipped") {
		t.Fatalf("expected publish_bucket_skipped events on resume run, got:\n%s", out.String())
	}
}

func TestSinglePassIsChunkIndependentAndLatestPerKey(t *testing.T) {
	ctx := context.Background()
	db := openFixtureDB(t)
	defer db.Close()
	loadCurrentProjectorFixture(t, ctx, db)
	// Chunk=1 would have produced many windows under the old per-window fold; single-pass
	// must still yield exactly latest-row-per-key as of the end ledger, independent of chunk.
	cfg := Config{Network: "mainnet", Start: 3, End: 5, Chunk: 1, SilverSchema: "silver", PublishBuckets: 4}
	projector := NewProjectorWithDB(db, cfg)
	if err := projector.ensureSchema(ctx); err != nil {
		t.Fatal(err)
	}
	if err := projector.ensureManifest(ctx); err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	if err := projector.Run(ctx, &out); err != nil {
		t.Fatalf("run: %v\n%s", err, out.String())
	}

	// Same expected current state the windowed version produced (latest row <= end ledger 5).
	assertCount(t, db, `SELECT COUNT(*) FROM silver.accounts_current WHERE network='mainnet'`, 2)
	var bal string
	if err := db.QueryRow(`SELECT balance FROM silver.accounts_current WHERE account_id='GA1'`).Scan(&bal); err != nil {
		t.Fatal(err)
	}
	if bal != "200" {
		t.Fatalf("GA1 balance = %s, want 200 (ledger 4 is latest <= end 5; ledger 8 excluded)", bal)
	}
	var future int64
	if err := db.QueryRow(`SELECT COUNT(*) FROM silver.accounts_current WHERE last_modified_ledger > 5`).Scan(&future); err != nil {
		t.Fatal(err)
	}
	if future != 0 {
		t.Fatalf("projected %d rows beyond end ledger", future)
	}
	// Confirm it took the single-pass path (no per-window fold) and emitted the new phase.
	if strings.Contains(out.String(), "ledger_window_started") {
		t.Fatalf("expected single-pass staging, but found ledger_window events")
	}
	if !strings.Contains(out.String(), "component.staging_completed") {
		t.Fatalf("expected component.staging_completed event from single-pass path")
	}
}

func TestLedgerWindowMergeAcrossWindowsLatestWins(t *testing.T) {
	ctx := context.Background()
	db := openFixtureDB(t)
	defer db.Close()
	loadCurrentProjectorFixture(t, ctx, db)
	// Chunk=1 forces a separate ledger window per ledger, so the merge must combine windows:
	// later windows overwrite earlier keys, deletes in a later window remove a key seen earlier.
	cfg := Config{Network: "mainnet", Start: 3, End: 5, Chunk: 1, SilverSchema: "silver", PublishBuckets: 4}
	projector := NewProjectorWithDB(db, cfg)
	if err := projector.ensureSchema(ctx); err != nil {
		t.Fatal(err)
	}
	if err := projector.ensureManifest(ctx); err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	if err := projector.Run(ctx, &out); err != nil {
		t.Fatalf("run: %v\n%s", err, out.String())
	}

	// Identical result regardless of window count (latest <= end ledger 5, no dup keys).
	assertCount(t, db, `SELECT COUNT(*) FROM silver.accounts_current WHERE network='mainnet'`, 2)
	assertCount(t, db, `SELECT COUNT(*) FROM silver.trustlines_current WHERE network='mainnet'`, 2)
	assertCount(t, db, `SELECT COUNT(*) FROM silver.address_balances_current WHERE network='mainnet'`, 4)
	var bal string
	if err := db.QueryRow(`SELECT balance FROM silver.accounts_current WHERE account_id='GA1'`).Scan(&bal); err != nil {
		t.Fatal(err)
	}
	if bal != "200" {
		t.Fatalf("GA1 balance = %s, want 200 (ledger 4 wins across windows; ledger 8 excluded)", bal)
	}
	// Deleted-in-a-later-window key must be removed from staging by the merge.
	var k2 int64
	if err := db.QueryRow(`SELECT COUNT(*) FROM silver.contract_data_current WHERE contract_id='CC1' AND key_hash='K2'`).Scan(&k2); err != nil {
		t.Fatal(err)
	}
	if k2 != 0 {
		t.Fatalf("CC1/K2 deleted in a later window but survived the merge")
	}
	if !strings.Contains(out.String(), "ledger_window_completed") {
		t.Fatalf("expected ledger_window_completed events from the windowed merge")
	}
}

func TestResumeSkipsCompletedProjectionsAcrossRunIDs(t *testing.T) {
	ctx := context.Background()
	db := openFixtureDB(t)
	defer db.Close()
	loadCurrentProjectorFixture(t, ctx, db)
	cfg := Config{Network: "mainnet", Start: 3, End: 5, Chunk: 100, SilverSchema: "silver", PublishBuckets: 2}
	projector := NewProjectorWithDB(db, cfg)
	if err := projector.ensureSchema(ctx); err != nil {
		t.Fatal(err)
	}
	if err := projector.ensureManifest(ctx); err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	if err := projector.Run(ctx, &out); err != nil {
		t.Fatalf("first run: %v\n%s", err, out.String())
	}

	// Simulate a partial state: one projection not yet completed (so the chunk isn't skipped
	// wholesale). Drop its manifest rows; the other five remain completed.
	if _, err := db.Exec(`DELETE FROM silver.silver_current_projector_manifest WHERE projection_name='trustlines_current'`); err != nil {
		t.Fatal(err)
	}

	// Re-dispatch with a NEW run id (the wrapper stamps a fresh timestamp each time) + resume.
	// The five completed projections must be skipped despite the differing run id; only the
	// incomplete one re-runs.
	projector.cfg.Resume = true
	projector.cfg.FlowctlRunID = "redispatch-with-a-different-run-id"
	out.Reset()
	if err := projector.Run(ctx, &out); err != nil {
		t.Fatalf("resume run: %v\n%s", err, out.String())
	}
	if got := strings.Count(out.String(), "component.projection_skipped"); got != 5 {
		t.Fatalf("expected 5 projection_skipped events on resume, got %d:\n%s", got, out.String())
	}
	if got := strings.Count(out.String(), `"event_type":"component.staging_started"`); got != 1 {
		t.Fatalf("expected exactly 1 projection (trustlines) to re-run, got %d staging_started", got)
	}
	if !strings.Contains(out.String(), `"event_type":"component.staging_started","component_id":"silver-current-state-projector","run_id":"redispatch-with-a-different-run-id","network":"mainnet","chunk_start":3,"chunk_end":5,"projection_name":"trustlines_current"`) {
		t.Fatalf("expected the re-run to be trustlines_current:\n%s", out.String())
	}
	// Data intact and unchanged.
	assertCount(t, db, `SELECT COUNT(*) FROM silver.accounts_current WHERE network='mainnet'`, 2)
	assertCount(t, db, `SELECT COUNT(*) FROM silver.trustlines_current WHERE network='mainnet'`, 2)
	assertCount(t, db, `SELECT COUNT(*) FROM silver.address_balances_current WHERE network='mainnet'`, 4)
}

func TestIsRetryableIOAndExecWindow(t *testing.T) {
	// Transient cold-storage read blips are retryable; logic/schema errors are not.
	if !isRetryableIO(errors.New("IO Error: Transferred a partial file error for HTTP GET to '...parquet'")) {
		t.Fatal("partial-file B2 read should be classified retryable")
	}
	if !isRetryableIO(errors.New("Connection reset by peer")) {
		t.Fatal("connection reset should be retryable")
	}
	if isRetryableIO(errors.New("Binder Error: column nope does not exist")) {
		t.Fatal("binder/schema error must NOT be retryable")
	}

	ctx := context.Background()
	db := openFixtureDB(t)
	defer db.Close()
	if _, err := db.Exec(`CREATE SCHEMA s`); err != nil {
		t.Fatal(err)
	}
	p := NewProjectorWithDB(db, Config{Network: "mainnet", SilverSchema: "s"})
	var out bytes.Buffer
	// happy path: succeeds first try
	if err := p.execWindow(ctx, &out, `CREATE OR REPLACE TABLE s.t AS SELECT 1 AS a`, "test", 0); err != nil {
		t.Fatalf("execWindow happy path: %v", err)
	}
	// idempotent on repeat (CREATE OR REPLACE)
	if err := p.execWindow(ctx, &out, `CREATE OR REPLACE TABLE s.t AS SELECT 2 AS a`, "test", 1); err != nil {
		t.Fatalf("execWindow repeat: %v", err)
	}
	// non-retryable error returns promptly (does not loop)
	if err := p.execWindow(ctx, &out, `CREATE OR REPLACE TABLE s.t AS SELECT nonexistent_col`, "test", 2); err == nil {
		t.Fatal("expected an error for a bad column reference")
	}
}

func openFixtureDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	return db
}

func loadCurrentProjectorFixture(t *testing.T, ctx context.Context, db *sql.DB) {
	t.Helper()
	stmts := []string{
		`CREATE SCHEMA silver`,
		`CREATE TABLE silver.accounts_snapshot (
			network VARCHAR, account_id VARCHAR, ledger_sequence BIGINT, closed_at TIMESTAMP,
			balance VARCHAR, sequence_number BIGINT, num_subentries INTEGER, num_sponsoring INTEGER,
			num_sponsored INTEGER, home_domain VARCHAR, master_weight INTEGER, low_threshold INTEGER,
			med_threshold INTEGER, high_threshold INTEGER, flags INTEGER, auth_required BOOLEAN,
			auth_revocable BOOLEAN, auth_immutable BOOLEAN, auth_clawback_enabled BOOLEAN, signers VARCHAR,
			sponsor_account VARCHAR, created_at TIMESTAMP, updated_at TIMESTAMP, ledger_range BIGINT,
			era_id VARCHAR, version_label VARCHAR
		)`,
		`CREATE TABLE silver.trustlines_snapshot (
			network VARCHAR, account_id VARCHAR, asset_code VARCHAR, asset_issuer VARCHAR, asset_type VARCHAR,
			balance VARCHAR, trust_limit VARCHAR, buying_liabilities VARCHAR, selling_liabilities VARCHAR,
			authorized BOOLEAN, authorized_to_maintain_liabilities BOOLEAN, clawback_enabled BOOLEAN,
			ledger_sequence BIGINT, created_at TIMESTAMP, ledger_range BIGINT, era_id VARCHAR, version_label VARCHAR
		)`,
		`CREATE TABLE silver.offers_snapshot (
			network VARCHAR, offer_id BIGINT, seller_account VARCHAR, ledger_sequence BIGINT, closed_at TIMESTAMP,
			selling_asset_type VARCHAR, selling_asset_code VARCHAR, selling_asset_issuer VARCHAR,
			buying_asset_type VARCHAR, buying_asset_code VARCHAR, buying_asset_issuer VARCHAR,
			amount VARCHAR, price VARCHAR, flags INTEGER, created_at TIMESTAMP, ledger_range BIGINT,
			era_id VARCHAR, version_label VARCHAR
		)`,
		`CREATE TABLE silver.contract_data_changes (
			network VARCHAR, contract_id VARCHAR, key_hash VARCHAR, contract_key_type VARCHAR,
			contract_durability VARCHAR, asset_type VARCHAR, asset_code VARCHAR, asset_issuer VARCHAR,
			balance_holder VARCHAR, balance VARCHAR, data_value VARCHAR, last_modified_ledger BIGINT,
			ledger_sequence BIGINT, closed_at TIMESTAMP, deleted BOOLEAN, ledger_range BIGINT
		)`,
		`CREATE TABLE silver.balance_changes (
			network VARCHAR, address VARCHAR, asset_type VARCHAR, asset_code VARCHAR, asset_issuer VARCHAR,
			balance VARCHAR, ledger_sequence BIGINT, ledger_closed_at TIMESTAMP, deleted BOOLEAN, ledger_range BIGINT
		)`,
		`INSERT INTO silver.accounts_snapshot VALUES
			('mainnet','GA1',3,'2026-01-01 00:00:03','100',10,1,0,0,NULL,1,1,1,1,0,false,false,false,false,NULL,NULL,'2026-01-01 00:00:03','2026-01-01 00:00:03',3,'era','v1'),
			('mainnet','GA1',4,'2026-01-01 00:00:04','200',11,1,0,0,NULL,1,1,1,1,0,false,false,false,false,NULL,NULL,'2026-01-01 00:00:03','2026-01-01 00:00:04',4,'era','v1'),
			('mainnet','GA1',8,'2026-01-01 00:00:08','999',12,1,0,0,NULL,1,1,1,1,0,false,false,false,false,NULL,NULL,'2026-01-01 00:00:03','2026-01-01 00:00:08',8,'era','v1'),
			('mainnet','GA2',3,'2026-01-01 00:00:03','50',20,1,0,0,NULL,1,1,1,1,0,false,false,false,false,NULL,NULL,'2026-01-01 00:00:03','2026-01-01 00:00:03',3,'era','v1')`,
		`INSERT INTO silver.trustlines_snapshot VALUES
			('mainnet','GA1','USD','ISSUER','credit_alphanum4','1.5','100','0','0',true,false,false,3,'2026-01-01 00:00:03',3,'era','v1'),
			('mainnet','GA1','USD','ISSUER','credit_alphanum4','2.5','100','0','0',true,true,false,5,'2026-01-01 00:00:05',5,'era','v1'),
			('mainnet','GA2','EUR','ISSUER2','credit_alphanum4','3','100','0','0',true,false,true,4,'2026-01-01 00:00:04',4,'era','v1')`,
		`INSERT INTO silver.offers_snapshot VALUES
			('mainnet',1,'GA1',3,'2026-01-01 00:00:03','credit_alphanum4','USD','ISSUER','native',NULL,NULL,'100','1/2',0,'2026-01-01 00:00:03',3,'era','v1'),
			('mainnet',1,'GA1',4,'2026-01-01 00:00:04','credit_alphanum4','USD','ISSUER','native',NULL,NULL,'200','1/2',0,'2026-01-01 00:00:04',4,'era','v1'),
			('mainnet',2,'GA2',8,'2026-01-01 00:00:08','credit_alphanum4','EUR','ISSUER2','native',NULL,NULL,'300','1/3',0,'2026-01-01 00:00:08',8,'era','v1')`,
		`INSERT INTO silver.contract_data_changes VALUES
			('mainnet','CC1','K1','instance','persistent','credit_alphanum4','USD','ISSUER','GA1','10','value1',3,3,'2026-01-01 00:00:03',false,3),
			('mainnet','CC1','K1','instance','persistent','credit_alphanum4','USD','ISSUER','GA1','20','value2',4,4,'2026-01-01 00:00:04',false,4),
			('mainnet','CC1','K2','instance','persistent','credit_alphanum4','USD','ISSUER','GA1','20','old',4,4,'2026-01-01 00:00:04',false,4),
			('mainnet','CC1','K2','instance','persistent','credit_alphanum4','USD','ISSER','GA1','20','deleted',5,5,'2026-01-01 00:00:05',true,5),
			('mainnet','CC2','K1','instance','persistent','credit_alphanum4','EUR','ISSUER2','GA2','20','future',8,8,'2026-01-01 00:00:08',false,8)`,
		`INSERT INTO silver.balance_changes VALUES
			('mainnet','GA1','native','XLM',NULL,'100',3,'2026-01-01 00:00:03',false,3),
			('mainnet','GA1','native','XLM',NULL,'999',4,'2026-01-01 00:00:04',false,4),
			('mainnet','GA2','native','XLM',NULL,'50',3,'2026-01-01 00:00:03',false,3),
			('mainnet','GA1','credit_alphanum4','USD','ISSUER','2.5',5,'2026-01-01 00:00:05',false,5),
			('mainnet','GA2','credit_alphanum4','EUR','ISSUER2','3',4,'2026-01-01 00:00:04',false,4),
			('mainnet','GA3','native','XLM',NULL,'1',8,'2026-01-01 00:00:08',false,8)`,
	}
	for _, stmt := range stmts {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			t.Fatalf("fixture stmt failed: %v\n%s", err, stmt)
		}
	}
}

func assertCount(t *testing.T, db *sql.DB, query string, want int64) {
	t.Helper()
	var got int64
	if err := db.QueryRow(query).Scan(&got); err != nil {
		t.Fatalf("%s: %v", query, err)
	}
	if got != want {
		t.Fatalf("%s = %d, want %d", query, got, want)
	}
}
