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

func TestContractBalanceProjectionPreservesArbitraryPrecision(t *testing.T) {
	sql := selectAddressBalancesCurrent(&Projector{cfg: Config{Network: "testnet", SilverSchema: "silver"}}, 100, 200)
	for _, want := range []string{
		"repeat('0'",
		"regexp_matches(CAST(balance_raw AS VARCHAR), '^[0-9]*[1-9][0-9]*$')",
	} {
		if !strings.Contains(sql, want) {
			t.Fatalf("contract balance projection SQL missing %q:\n%s", want, sql)
		}
	}
	db := openFixtureDB(t)
	defer db.Close()
	displayExpr := contractBalanceDisplayExpr("balance_raw", "decimals")
	if strings.Contains(displayExpr, "DECIMAL(38,7)") || strings.Contains(displayExpr, "POWER(10") {
		t.Fatalf("contract balance display still narrows raw balances through numeric arithmetic:\n%s", displayExpr)
	}

	tests := []struct {
		name     string
		raw      string
		decimals int
		want     string
	}{
		{name: "zero decimals", raw: "123", decimals: 0, want: "123"},
		{name: "two decimals", raw: "12345", decimals: 2, want: "123.45"},
		{name: "native seven decimals", raw: "99950000000", decimals: 7, want: "9995.0000000"},
		{name: "fraction smaller than one", raw: "1", decimals: 7, want: "0.0000001"},
		{name: "nine decimals", raw: "123456789", decimals: 9, want: "0.123456789"},
		{name: "twelve decimals preserves trailing digits", raw: "12345678901234567890", decimals: 12, want: "12345678.901234567890"},
		{name: "thirty one raw digits", raw: "1234567890123456789012345678901", decimals: 7, want: "123456789012345678901234.5678901"},
		{name: "thirty two raw digits", raw: "12345678901234567890123456789012", decimals: 7, want: "1234567890123456789012345.6789012"},
		{name: "thirty nine raw digits", raw: "999999999999999999999999999999999999999", decimals: 2, want: "9999999999999999999999999999999999999.99"},
	}
	query := `SELECT ` + displayExpr + `
		FROM (SELECT ?::VARCHAR AS balance_raw, ?::INTEGER AS decimals)`
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var display string
			if err := db.QueryRow(query, tc.raw, tc.decimals).Scan(&display); err != nil {
				t.Fatalf("execute arbitrary-precision display expression: %v\n%s", err, query)
			}
			if display != tc.want {
				t.Fatalf("display = %q, want %q", display, tc.want)
			}
		})
	}
}

func TestProjectionResumeIdentityIncludesDefinitionVersion(t *testing.T) {
	ctx := context.Background()
	db := openFixtureDB(t)
	defer db.Close()
	p := NewProjectorWithDB(db, Config{Network: "testnet", Start: 3, End: 100002, SilverSchema: "silver"})
	if err := p.ensureSchema(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`
		CREATE TABLE silver.silver_current_projector_manifest (
			run_id VARCHAR,
			component_id VARCHAR,
			projection_name VARCHAR,
			network VARCHAR,
			start_ledger BIGINT,
			end_ledger BIGINT,
			status VARCHAR,
			row_count BIGINT,
			error_message VARCHAR,
			started_at TIMESTAMP,
			completed_at TIMESTAMP
		);
		INSERT INTO silver.silver_current_projector_manifest (
			run_id, component_id, projection_name, network,
			start_ledger, end_ledger, status, row_count,
			error_message, started_at, completed_at
		) VALUES (
			'legacy-run', 'silver-current-state-projector', 'address_balances_current', 'testnet',
			3, 100002, 'completed', 1, '', current_timestamp, current_timestamp
		)
	`); err != nil {
		t.Fatalf("create legacy manifest fixture: %v", err)
	}
	if err := p.ensureManifest(ctx); err != nil {
		t.Fatal(err)
	}

	var versionColumns int64
	if err := db.QueryRow(`
		SELECT count(*) FROM information_schema.columns
		WHERE table_schema = 'silver'
		  AND table_name = 'silver_current_projector_manifest'
		  AND column_name = 'projection_version'
	`).Scan(&versionColumns); err != nil {
		t.Fatal(err)
	}
	if versionColumns != 1 {
		t.Fatalf("projection manifest version columns = %d, want 1", versionColumns)
	}

	projection := currentProjectionByName(t, "address_balances_current")
	done, err := p.projectionComplete(ctx, 3, 100002, projection)
	if err != nil {
		t.Fatalf("check legacy projection completion: %v", err)
	}
	if done {
		t.Fatal("legacy completion row incorrectly matched the current address-balance projection definition")
	}

	if err := p.markManifest(ctx, 3, 100002, projection, "completed", "", 1); err != nil {
		t.Fatalf("mark current projection definition complete: %v", err)
	}
	p.cfg.FlowctlRunID = "redispatch-with-new-run-id"
	done, err = p.projectionComplete(ctx, 3, 100002, projection)
	if err != nil {
		t.Fatalf("check current projection completion: %v", err)
	}
	if !done {
		t.Fatal("same projection definition did not resume across run IDs")
	}
}

func currentProjectionByName(t *testing.T, name string) CurrentProjection {
	t.Helper()
	for _, projection := range executableCurrentProjections() {
		if projection.Name == name {
			return projection
		}
	}
	t.Fatalf("projection %q not found", name)
	return CurrentProjection{}
}

func TestAddressBalanceProjectionWorksWithoutLegacyBalanceChanges(t *testing.T) {
	db := openFixtureDB(t)
	defer db.Close()
	for _, stmt := range []string{
		`CREATE SCHEMA silver`,
		`CREATE TABLE silver.contract_balance_changes (
			network VARCHAR, owner_address VARCHAR, owner_type VARCHAR, asset_key VARCHAR,
			asset_type VARCHAR, token_contract_id VARCHAR, asset_code VARCHAR, asset_issuer VARCHAR,
			symbol VARCHAR, decimals INTEGER, balance_raw VARCHAR, balance_source VARCHAR,
			key_hash VARCHAR, ledger_sequence BIGINT, ledger_closed_at TIMESTAMP, deleted BOOLEAN
		)`,
		`INSERT INTO silver.contract_balance_changes VALUES
			('testnet','CC1','contract','CXLM','native','CXLM','XLM',NULL,'XLM',7,'10000000','contract_storage_state','K1',10,'2026-01-01 00:00:10',false),
			('testnet','CC2','contract','CXLM','native','CXLM','XLM',NULL,'XLM',7,'0','contract_storage_state','K2',10,'2026-01-01 00:00:10',false)`,
	} {
		if _, err := db.Exec(stmt); err != nil {
			t.Fatalf("setup: %v\n%s", err, stmt)
		}
	}

	p := NewProjectorWithDB(db, Config{Network: "testnet", SilverSchema: "silver"})
	available, err := p.sourceTableExists(context.Background(), "balance_changes")
	if err != nil {
		t.Fatal(err)
	}
	p.sourceAvailabilityKnown = true
	p.balanceChangesAvailable = available
	if available {
		t.Fatal("legacy balance_changes unexpectedly exists")
	}

	projectionSQL := selectAddressBalancesCurrent(p, 3, 20)
	if strings.Contains(projectionSQL, `."balance_changes"`) {
		t.Fatalf("projection unexpectedly requires legacy balance_changes:\n%s", projectionSQL)
	}
	var rows int64
	if err := db.QueryRow(`SELECT COUNT(*) FROM (` + projectionSQL + `)`).Scan(&rows); err != nil {
		t.Fatalf("execute contract-only projection: %v\n%s", err, projectionSQL)
	}
	if rows != 1 {
		t.Fatalf("contract-only projection rows = %d, want 1 positive balance", rows)
	}

	keysSQL := selectAddressBalancesCurrentKeys(p, 3, 20)
	if err := db.QueryRow(`SELECT COUNT(*) FROM (` + keysSQL + `)`).Scan(&rows); err != nil {
		t.Fatalf("execute contract-only keys: %v\n%s", err, keysSQL)
	}
	if rows != 2 {
		t.Fatalf("contract-only key rows = %d, want 2", rows)
	}
}

func TestEnsureAddressBalancesCurrentAddsNetworkToLegacyTable(t *testing.T) {
	db := openFixtureDB(t)
	defer db.Close()
	if _, err := db.Exec(`CREATE SCHEMA silver; CREATE TABLE silver.address_balances_current (
		owner_address VARCHAR, asset_key VARCHAR, balance_raw VARCHAR, last_updated_ledger BIGINT
	)`); err != nil {
		t.Fatal(err)
	}
	p := NewProjectorWithDB(db, Config{SilverSchema: "silver"})
	projection := CurrentProjection{Name: "address_balances_current", TargetTable: "address_balances_current"}
	if err := p.ensureTargetColumns(context.Background(), projection); err != nil {
		t.Fatal(err)
	}
	var columns int64
	if err := db.QueryRow(`SELECT COUNT(*) FROM information_schema.columns
		WHERE table_schema='silver' AND table_name='address_balances_current' AND column_name='network'`).Scan(&columns); err != nil {
		t.Fatal(err)
	}
	if columns != 1 {
		t.Fatalf("network column count = %d, want 1", columns)
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
	assertCount(t, db, `SELECT COUNT(*) FROM silver.ttl_current WHERE network='mainnet'`, 2)
	assertCount(t, db, `SELECT COUNT(*) FROM silver.native_balances_current WHERE network='mainnet'`, 2)
	assertCount(t, db, `SELECT COUNT(*) FROM silver.address_balances_current WHERE network='mainnet'`, 6)
	assertCount(t, db, `SELECT COUNT(*) FROM silver.silver_current_projector_manifest WHERE status='completed'`, 7)

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
	if err := db.QueryRow(`SELECT balance_raw, balance_display FROM silver.address_balances_current WHERE owner_address='CC1' AND token_contract_id='CXLM'`).Scan(&balanceRaw, &balanceDisplay); err != nil {
		t.Fatal(err)
	}
	if balanceRaw != "99950000000" || balanceDisplay != "9995.0000000" {
		t.Fatalf("contract XLM balance raw/display = %s/%s, want 99950000000/9995.0000000", balanceRaw, balanceDisplay)
	}
	if err := db.QueryRow(`SELECT balance_raw, balance_display FROM silver.address_balances_current WHERE owner_address='CC5' AND token_contract_id='CTWO'`).Scan(&balanceRaw, &balanceDisplay); err != nil {
		t.Fatal(err)
	}
	if balanceRaw != "12345" || balanceDisplay != "123.45" {
		t.Fatalf("two-decimal contract balance raw/display = %s/%s, want 12345/123.45", balanceRaw, balanceDisplay)
	}
	assertCount(t, db, `SELECT COUNT(*) FROM silver.address_balances_current WHERE owner_address IN ('CC2','CC3')`, 0)
	var contractCount int64
	if err := db.QueryRow(`SELECT COUNT(*) FROM silver.contract_data_current WHERE contract_id='CC1' AND key_hash='K2'`).Scan(&contractCount); err != nil {
		t.Fatal(err)
	}
	if contractCount != 0 {
		t.Fatalf("deleted latest contract key survived projection")
	}
	var ttlRemaining int32
	var ttlExpired bool
	if err := db.QueryRow(`SELECT ttl_remaining, expired FROM silver.ttl_current WHERE key_hash='TK1'`).Scan(&ttlRemaining, &ttlExpired); err != nil {
		t.Fatal(err)
	}
	if ttlRemaining != 5 || ttlExpired {
		t.Fatalf("TK1 ttl remaining/expired = %d/%v, want 5/false", ttlRemaining, ttlExpired)
	}
	if err := db.QueryRow(`SELECT ttl_remaining, expired FROM silver.ttl_current WHERE key_hash='TK2'`).Scan(&ttlRemaining, &ttlExpired); err != nil {
		t.Fatal(err)
	}
	if ttlRemaining != -1 || !ttlExpired {
		t.Fatalf("TK2 ttl remaining/expired = %d/%v, want -1/true", ttlRemaining, ttlExpired)
	}
	assertCount(t, db, `SELECT COUNT(*) FROM silver.ttl_current WHERE key_hash='TK3'`, 0)
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

func TestContractDataCurrentMigratesLegacyTargetNetwork(t *testing.T) {
	ctx := context.Background()
	db := openFixtureDB(t)
	defer db.Close()
	if _, err := db.Exec(`CREATE SCHEMA silver`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`CREATE TABLE silver.contract_data_snapshot_v1 (
		contract_id VARCHAR, ledger_key_hash VARCHAR, contract_durability VARCHAR,
		asset_type VARCHAR, asset_code VARCHAR, asset_issuer VARCHAR, contract_data_xdr VARCHAR,
		last_modified_ledger BIGINT, ledger_sequence BIGINT, closed_at TIMESTAMP,
		deleted BOOLEAN, ledger_range BIGINT
	)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`CREATE TABLE silver.contract_data_current (
		contract_id VARCHAR, key_hash VARCHAR, durability VARCHAR,
		asset_type VARCHAR, asset_code VARCHAR, asset_issuer VARCHAR, data_value VARCHAR,
		last_modified_ledger BIGINT, ledger_sequence BIGINT, closed_at TIMESTAMP,
		created_at TIMESTAMP, ledger_range BIGINT, updated_at TIMESTAMP
	)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO silver.contract_data_current (contract_id, key_hash) VALUES ('CC1', 'K1')`); err != nil {
		t.Fatal(err)
	}

	projector := NewProjectorWithDB(db, Config{Network: "testnet", Start: 3, End: 5, SilverSchema: "silver"})
	projection := executableCurrentProjections()[3]
	if err := projector.ensureTargetTable(ctx, projection); err != nil {
		t.Fatalf("ensureTargetTable: %v", err)
	}

	var network sql.NullString
	if err := db.QueryRow(`SELECT network FROM silver.contract_data_current WHERE contract_id='CC1'`).Scan(&network); err != nil {
		t.Fatal(err)
	}
	if network.Valid {
		t.Fatalf("legacy row network = %q, want NULL until bounded publication replaces it", network.String)
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
	assertCount(t, db, `SELECT COUNT(*) FROM silver.address_balances_current WHERE network='mainnet'`, 6)
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
	// wholesale). Drop its manifest rows; the other projections remain completed.
	if _, err := db.Exec(`DELETE FROM silver.silver_current_projector_manifest WHERE projection_name='trustlines_current'`); err != nil {
		t.Fatal(err)
	}

	// Re-dispatch with a NEW run id (the wrapper stamps a fresh timestamp each time) + resume.
	// The completed projections must be skipped despite the differing run id; only the
	// incomplete one re-runs.
	projector.cfg.Resume = true
	projector.cfg.FlowctlRunID = "redispatch-with-a-different-run-id"
	out.Reset()
	if err := projector.Run(ctx, &out); err != nil {
		t.Fatalf("resume run: %v\n%s", err, out.String())
	}
	if got := strings.Count(out.String(), "component.projection_skipped"); got != 6 {
		t.Fatalf("expected 6 projection_skipped events on resume, got %d:\n%s", got, out.String())
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
	assertCount(t, db, `SELECT COUNT(*) FROM silver.address_balances_current WHERE network='mainnet'`, 6)
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

func TestCurrentStateScansFromGenesisRegardlessOfStart(t *testing.T) {
	// PR #84 (Codex P1): a mid-history --start must NOT truncate current state. GA2 was last
	// modified at ledger 3; running with Start=4 must still include it — current state is always
	// computed from genesis, else publishProjection (replace-all-buckets) would delete GA2 from
	// the target. (This is the bug that corrupted accounts_current when run with --start mid-history.)
	ctx := context.Background()
	db := openFixtureDB(t)
	defer db.Close()
	loadCurrentProjectorFixture(t, ctx, db)
	cfg := Config{Network: "mainnet", Start: 4, End: 5, Chunk: 1, SilverSchema: "silver", PublishBuckets: 4}
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
	// Both accounts present even though GA2's last change (ledger 3) predates Start=4.
	assertCount(t, db, `SELECT COUNT(*) FROM silver.accounts_current WHERE network='mainnet'`, 2)
	var bal string
	if err := db.QueryRow(`SELECT balance FROM silver.accounts_current WHERE account_id='GA2'`).Scan(&bal); err != nil {
		t.Fatalf("GA2 dropped by non-genesis start (regression of PR #84 fix): %v", err)
	}
	if bal != "50" {
		t.Fatalf("GA2 balance = %s, want 50 (ledger 3, before start=4)", bal)
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
			balance VARCHAR, sequence_number BIGINT, sequence_ledger BIGINT, sequence_time BIGINT,
			num_subentries INTEGER, num_sponsoring INTEGER,
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
		`CREATE TABLE silver.contract_data_snapshot_v1 (
			contract_id VARCHAR, ledger_key_hash VARCHAR,
			contract_durability VARCHAR, asset_type VARCHAR, asset_code VARCHAR, asset_issuer VARCHAR,
			contract_data_xdr VARCHAR, last_modified_ledger BIGINT,
			ledger_sequence BIGINT, closed_at TIMESTAMP, deleted BOOLEAN, ledger_range BIGINT
		)`,
		`CREATE TABLE silver.ttl_snapshot_v1 (
			key_hash VARCHAR, live_until_ledger_seq BIGINT, last_modified_ledger BIGINT,
			ledger_entry_change INTEGER, deleted BOOLEAN, closed_at TIMESTAMP,
			ledger_sequence BIGINT, created_at TIMESTAMP, ledger_range BIGINT,
			era_id VARCHAR, version_label VARCHAR
		)`,
		`CREATE TABLE silver.balance_changes (
			network VARCHAR, address VARCHAR, asset_type VARCHAR, asset_code VARCHAR, asset_issuer VARCHAR,
			balance VARCHAR, ledger_sequence BIGINT, ledger_closed_at TIMESTAMP, deleted BOOLEAN, ledger_range BIGINT
		)`,
		`CREATE TABLE silver.contract_balance_changes (
			network VARCHAR, owner_address VARCHAR, owner_type VARCHAR, asset_key VARCHAR, asset_type VARCHAR,
			token_contract_id VARCHAR, asset_code VARCHAR, asset_issuer VARCHAR, symbol VARCHAR, decimals INTEGER,
			balance_raw VARCHAR, balance_source VARCHAR, key_hash VARCHAR, ledger_sequence BIGINT,
			ledger_closed_at TIMESTAMP, deleted BOOLEAN, ledger_range BIGINT
		)`,
		`INSERT INTO silver.accounts_snapshot VALUES
			('mainnet','GA1',3,'2026-01-01 00:00:03','100',10,3,3,1,0,0,NULL,1,1,1,1,0,false,false,false,false,NULL,NULL,'2026-01-01 00:00:03','2026-01-01 00:00:03',3,'era','v1'),
			('mainnet','GA1',4,'2026-01-01 00:00:04','200',11,4,4,1,0,0,NULL,1,1,1,1,0,false,false,false,false,NULL,NULL,'2026-01-01 00:00:03','2026-01-01 00:00:04',4,'era','v1'),
			('mainnet','GA1',8,'2026-01-01 00:00:08','999',12,8,8,1,0,0,NULL,1,1,1,1,0,false,false,false,false,NULL,NULL,'2026-01-01 00:00:03','2026-01-01 00:00:08',8,'era','v1'),
			('mainnet','GA2',3,'2026-01-01 00:00:03','50',20,3,3,1,0,0,NULL,1,1,1,1,0,false,false,false,false,NULL,NULL,'2026-01-01 00:00:03','2026-01-01 00:00:03',3,'era','v1')`,
		`INSERT INTO silver.trustlines_snapshot VALUES
			('mainnet','GA1','USD','ISSUER','credit_alphanum4','1.5','100','0','0',true,false,false,3,'2026-01-01 00:00:03',3,'era','v1'),
			('mainnet','GA1','USD','ISSUER','credit_alphanum4','2.5','100','0','0',true,true,false,5,'2026-01-01 00:00:05',5,'era','v1'),
			('mainnet','GA2','EUR','ISSUER2','credit_alphanum4','3','100','0','0',true,false,true,4,'2026-01-01 00:00:04',4,'era','v1')`,
		`INSERT INTO silver.offers_snapshot VALUES
			('mainnet',1,'GA1',3,'2026-01-01 00:00:03','credit_alphanum4','USD','ISSUER','native',NULL,NULL,'100','1/2',0,'2026-01-01 00:00:03',3,'era','v1'),
			('mainnet',1,'GA1',4,'2026-01-01 00:00:04','credit_alphanum4','USD','ISSUER','native',NULL,NULL,'200','1/2',0,'2026-01-01 00:00:04',4,'era','v1'),
			('mainnet',2,'GA2',8,'2026-01-01 00:00:08','credit_alphanum4','EUR','ISSUER2','native',NULL,NULL,'300','1/3',0,'2026-01-01 00:00:08',8,'era','v1')`,
		`INSERT INTO silver.contract_data_snapshot_v1 VALUES
			('CC1','K1','persistent','credit_alphanum4','USD','ISSUER','value1',3,3,'2026-01-01 00:00:03',false,3),
			('CC1','K1','persistent','credit_alphanum4','USD','ISSUER','value2',4,4,'2026-01-01 00:00:04',false,4),
			('CC1','K2','persistent','credit_alphanum4','USD','ISSUER','old',4,4,'2026-01-01 00:00:04',false,4),
			('CC1','K2','persistent','credit_alphanum4','USD','ISSER','deleted',5,5,'2026-01-01 00:00:05',true,5),
			('CC2','K1','persistent','credit_alphanum4','EUR','ISSUER2','future',8,8,'2026-01-01 00:00:08',false,8)`,
		`INSERT INTO silver.ttl_snapshot_v1 VALUES
			('TK1',8,3,0,false,'2026-01-01 00:00:03',3,'2026-01-01 00:00:03',3,'era','v1'),
			('TK1',10,4,0,false,'2026-01-01 00:00:04',4,'2026-01-01 00:00:04',4,'era','v1'),
			('TK2',4,4,0,false,'2026-01-01 00:00:04',4,'2026-01-01 00:00:04',4,'era','v1'),
			('TK3',9,4,0,false,'2026-01-01 00:00:04',4,'2026-01-01 00:00:04',4,'era','v1'),
			('TK3',11,5,0,true,'2026-01-01 00:00:05',5,'2026-01-01 00:00:05',5,'era','v1'),
			('TK4',20,8,0,false,'2026-01-01 00:00:08',8,'2026-01-01 00:00:08',8,'era','v1')`,
		`INSERT INTO silver.balance_changes VALUES
			('mainnet','GA1','native','XLM',NULL,'100',3,'2026-01-01 00:00:03',false,3),
			('mainnet','GA1','native','XLM',NULL,'999',4,'2026-01-01 00:00:04',false,4),
			('mainnet','GA2','native','XLM',NULL,'50',3,'2026-01-01 00:00:03',false,3),
			('mainnet','GA1','credit_alphanum4','USD','ISSUER','2.5',5,'2026-01-01 00:00:05',false,5),
			('mainnet','GA2','credit_alphanum4','EUR','ISSUER2','3',4,'2026-01-01 00:00:04',false,4),
			('mainnet','GA3','native','XLM',NULL,'1',8,'2026-01-01 00:00:08',false,8)`,
		`INSERT INTO silver.contract_balance_changes VALUES
			('mainnet','CC1','contract','CXLM','native','CXLM','XLM',NULL,'XLM',7,'10000000000','contract_storage_state','BK1',3,'2026-01-01 00:00:03',false,3),
			('mainnet','CC1','contract','CXLM','native','CXLM','XLM',NULL,'XLM',7,'99950000000','contract_storage_state','BK1',5,'2026-01-01 00:00:05',false,5),
			('mainnet','CC2','contract','CXLM','native','CXLM','XLM',NULL,'XLM',7,'50000000','contract_storage_state','BK2',3,'2026-01-01 00:00:03',false,3),
			('mainnet','CC2','contract','CXLM','native','CXLM','XLM',NULL,'XLM',7,'50000000','contract_storage_state','BK2',4,'2026-01-01 00:00:04',true,4),
			('mainnet','CC3','contract','CXLM','native','CXLM','XLM',NULL,'XLM',7,'0','contract_storage_state','BK3',4,'2026-01-01 00:00:04',false,4),
			('mainnet','CC4','contract','CXLM','native','CXLM','XLM',NULL,'XLM',7,'70000000','contract_storage_state','BK4',8,'2026-01-01 00:00:08',false,8),
			('mainnet','CC5','contract','CTWO','soroban_token','CTWO',NULL,NULL,'TWO',2,'12345','contract_storage_state','BK5',5,'2026-01-01 00:00:05',false,5)`,
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
