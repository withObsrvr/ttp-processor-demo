package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	_ "github.com/duckdb/duckdb-go/v2"
)

var Version = "dev"

const componentID = "silver-current-state-projector"

type FailureClass string

const (
	FailureUnknown                 FailureClass = "unknown"
	FailureRetryableInfrastructure FailureClass = "retryable_infrastructure"
	FailureVerification            FailureClass = "verification"
	FailureNonRetryableData        FailureClass = "non_retryable_data"
	FailureNonRetryableSchema      FailureClass = "non_retryable_schema"
	FailureConfig                  FailureClass = "config"
)

type Config struct {
	Network    string
	Start      int64
	End        int64
	Chunk      int64
	ChunkStart int64
	ChunkEnd   int64
	Resume     bool
	Status     bool

	SilverCatalog string
	SilverData    string
	SilverAlias   string
	SilverSchema  string
	SilverMeta    string
	ManifestPath  string
	SummaryPath   string

	FlowctlEnabled   bool
	FlowctlEndpoint  string
	FlowctlRunID     string
	FlowctlAttempt   string
	FlowctlComponent string
}

type Chunk struct {
	Start int64 `json:"start_ledger"`
	End   int64 `json:"end_ledger"`
	Index int   `json:"index"`
}

type Projection struct {
	Name        string `json:"name"`
	TargetTable string `json:"target_table"`
	Source      string `json:"source"`
	Mode        string `json:"mode"`
	Required    bool   `json:"required"`
	Status      string `json:"status"`
	Gap         string `json:"gap,omitempty"`
}

type Event struct {
	Timestamp       string                 `json:"timestamp"`
	EventType       string                 `json:"event_type"`
	ComponentID     string                 `json:"component_id"`
	RunID           string                 `json:"run_id,omitempty"`
	Network         string                 `json:"network,omitempty"`
	ChunkStart      int64                  `json:"chunk_start,omitempty"`
	ChunkEnd        int64                  `json:"chunk_end,omitempty"`
	ProjectionName  string                 `json:"projection_name,omitempty"`
	TargetTable     string                 `json:"target_table,omitempty"`
	Phase           string                 `json:"phase,omitempty"`
	Status          string                 `json:"status,omitempty"`
	FailureClass    FailureClass           `json:"failure_class,omitempty"`
	Error           string                 `json:"error,omitempty"`
	Recommended     string                 `json:"recommended_action,omitempty"`
	FlowctlEndpoint string                 `json:"flowctl_endpoint,omitempty"`
	Metadata        map[string]interface{} `json:"metadata,omitempty"`
}

type ManifestRecord struct {
	RunID          string `json:"run_id"`
	ComponentID    string `json:"component_id"`
	ProjectionName string `json:"projection_name"`
	Network        string `json:"network"`
	StartLedger    int64  `json:"start_ledger"`
	EndLedger      int64  `json:"end_ledger"`
	Status         string `json:"status"`
	FailureClass   string `json:"failure_class,omitempty"`
	ErrorMessage   string `json:"error_message,omitempty"`
	RowCount       int64  `json:"row_count"`
	UpdatedAt      string `json:"updated_at"`
}

type ManifestStore interface {
	Mark(context.Context, ManifestRecord) error
	Completed(context.Context, string, string, int64, int64) (bool, error)
}

type JSONLManifest struct {
	path string
}

type Projector struct {
	db    *sql.DB
	cfg   Config
	jsonl ManifestStore
}

type CurrentProjection struct {
	Name         string
	TargetTable  string
	Source       string
	MaxLedgerCol string
	KeyExprs     []string
	SelectSQL    func(*Projector) string
}

func main() {
	ctx := context.Background()
	cfg, err := parseConfig(os.Args[1:])
	if err != nil {
		emit(os.Stdout, Event{EventType: "component.failed", ComponentID: componentID, FailureClass: FailureConfig, Error: err.Error(), Recommended: "fix_config"})
		os.Exit(2)
	}

	if cfg.Status {
		if err := writeStatus(os.Stdout, cfg); err != nil {
			os.Exit(1)
		}
		return
	}

	if err := run(ctx, cfg, os.Stdout); err != nil {
		emit(os.Stdout, Event{EventType: "component.failed", ComponentID: cfg.Component(), RunID: cfg.RunID(), Network: cfg.Network, FailureClass: classifyFailure(err), Error: err.Error(), Recommended: recommendedAction(classifyFailure(err))})
		os.Exit(exitCode(classifyFailure(err)))
	}
}

func parseConfig(args []string) (Config, error) {
	var cfg Config
	fs := flag.NewFlagSet(componentID, flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	fs.StringVar(&cfg.Network, "network", getenv("NETWORK", "mainnet"), "network label")
	fs.Int64Var(&cfg.Start, "start-ledger", envInt64("START_LEDGER", 0), "inclusive start ledger")
	fs.Int64Var(&cfg.End, "end-ledger", envInt64("END_LEDGER", 0), "inclusive end/as-of ledger")
	fs.Int64Var(&cfg.Chunk, "chunk-size", envInt64("CHUNK_SIZE", 100000), "deterministic planning chunk size")
	fs.Int64Var(&cfg.ChunkStart, "chunk-start", envInt64("CHUNK_START", 0), "optional assigned inclusive chunk start")
	fs.Int64Var(&cfg.ChunkEnd, "chunk-end", envInt64("CHUNK_END", 0), "optional assigned inclusive chunk end")
	fs.BoolVar(&cfg.Resume, "resume", getenv("RESUME", "") == "true", "skip completed manifest records")
	fs.BoolVar(&cfg.Status, "status", false, "emit machine-readable component status and exit")
	fs.StringVar(&cfg.SilverCatalog, "silver-ducklake-catalog", getenv("SILVER_DUCKLAKE_CATALOG", ""), "Silver DuckLake catalog DSN/path")
	fs.StringVar(&cfg.SilverData, "silver-data-path", getenv("SILVER_DATA_PATH", ""), "Silver DuckLake data path")
	fs.StringVar(&cfg.SilverAlias, "silver-catalog-name", getenv("SILVER_CATALOG_NAME", "silver_catalog"), "DuckDB alias for Silver catalog")
	fs.StringVar(&cfg.SilverSchema, "silver-schema", getenv("SILVER_SCHEMA", "silver"), "Silver schema name")
	fs.StringVar(&cfg.SilverMeta, "silver-metadata-schema", getenv("SILVER_DUCKLAKE_METADATA_SCHEMA", "silver_meta"), "Silver DuckLake metadata schema")
	fs.StringVar(&cfg.ManifestPath, "manifest-path", getenv("MANIFEST_PATH", ""), "JSONL manifest path for batch-local durable status")
	fs.StringVar(&cfg.SummaryPath, "summary-path", getenv("SUMMARY_PATH", ""), "optional JSON summary output path")
	if err := fs.Parse(args); err != nil {
		return Config{}, err
	}
	cfg.FlowctlEnabled = getenv("ENABLE_FLOWCTL", "") == "true"
	cfg.FlowctlEndpoint = getenv("FLOWCTL_ENDPOINT", "")
	cfg.FlowctlRunID = getenv("FLOWCTL_RUN_ID", "")
	cfg.FlowctlAttempt = getenv("FLOWCTL_ATTEMPT", "")
	cfg.FlowctlComponent = getenv("FLOWCTL_COMPONENT_ID", componentID)
	return cfg, cfg.Validate()
}

func (c Config) Validate() error {
	if c.Status {
		return nil
	}
	if c.Network == "" {
		return errors.New("--network is required")
	}
	if c.Start <= 0 || c.End <= 0 || c.End < c.Start {
		return errors.New("--start-ledger and --end-ledger are required and end must be >= start")
	}
	if c.Chunk <= 0 {
		return errors.New("--chunk-size must be > 0")
	}
	if (c.ChunkStart == 0) != (c.ChunkEnd == 0) {
		return errors.New("--chunk-start and --chunk-end must be provided together")
	}
	if c.ChunkStart != 0 && (c.ChunkEnd < c.ChunkStart || c.ChunkStart < c.Start || c.ChunkEnd > c.End) {
		return errors.New("--chunk-start/--chunk-end must form a valid subrange within --start-ledger/--end-ledger")
	}
	if c.SilverCatalog == "" || c.SilverData == "" {
		return errors.New("--silver-ducklake-catalog and --silver-data-path are required")
	}
	return nil
}

func (c Config) RunID() string {
	if c.FlowctlRunID != "" {
		return c.FlowctlRunID
	}
	return fmt.Sprintf("%s-%s-%d-%d", componentID, c.Network, c.Start, c.End)
}

func (c Config) Component() string {
	if c.FlowctlComponent != "" {
		return c.FlowctlComponent
	}
	return componentID
}

func run(ctx context.Context, cfg Config, out io.Writer) error {
	projector, err := NewProjector(ctx, cfg)
	if err != nil {
		return err
	}
	defer projector.Close()
	return projector.Run(ctx, out)
}

func NewProjector(ctx context.Context, cfg Config) (*Projector, error) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		return nil, err
	}
	p := &Projector{db: db, cfg: cfg, jsonl: JSONLManifest{path: cfg.ManifestPath}}
	for _, stmt := range []string{"INSTALL ducklake FROM core_nightly", "LOAD ducklake", "INSTALL httpfs", "LOAD httpfs"} {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			db.Close()
			return nil, fmt.Errorf("%s: %w", stmt, err)
		}
	}
	if err := p.attachSilver(ctx); err != nil {
		db.Close()
		return nil, err
	}
	if err := p.ensureSchema(ctx); err != nil {
		db.Close()
		return nil, err
	}
	if err := p.ensureManifest(ctx); err != nil {
		db.Close()
		return nil, err
	}
	return p, nil
}

func NewProjectorWithDB(db *sql.DB, cfg Config) *Projector {
	return &Projector{db: db, cfg: cfg, jsonl: JSONLManifest{path: cfg.ManifestPath}}
}

func (p *Projector) Close() error {
	return p.db.Close()
}

func (p *Projector) attachSilver(ctx context.Context) error {
	stmt := fmt.Sprintf("ATTACH %s AS %s (DATA_PATH %s, METADATA_SCHEMA %s, AUTOMATIC_MIGRATION TRUE, OVERRIDE_DATA_PATH TRUE)", q(p.cfg.SilverCatalog), ident(p.cfg.SilverAlias), q(p.cfg.SilverData), q(p.cfg.SilverMeta))
	if _, err := p.db.ExecContext(ctx, stmt); err != nil {
		fallback := fmt.Sprintf("ATTACH %s AS %s (TYPE ducklake, DATA_PATH %s, METADATA_SCHEMA %s, AUTOMATIC_MIGRATION TRUE, OVERRIDE_DATA_PATH TRUE)", q(p.cfg.SilverCatalog), ident(p.cfg.SilverAlias), q(p.cfg.SilverData), q(p.cfg.SilverMeta))
		if _, fallbackErr := p.db.ExecContext(ctx, fallback); fallbackErr != nil {
			return fmt.Errorf("attach silver ducklake: %w (fallback with TYPE ducklake also failed: %v)", err, fallbackErr)
		}
	}
	return nil
}

func (p *Projector) Run(ctx context.Context, out io.Writer) error {
	cfg := p.cfg
	chunks := cfg.PlannedChunks()
	emit(out, Event{EventType: "component.run_started", ComponentID: cfg.Component(), RunID: cfg.RunID(), Network: cfg.Network, Status: "running", FlowctlEndpoint: safeEndpoint(cfg.FlowctlEndpoint), Metadata: map[string]interface{}{"version": Version, "range_start": cfg.Start, "range_end": cfg.End, "chunk_count": len(chunks), "capabilities": []string{"current-state-as-of", "deterministic-chunk-inputs", "idempotent-replace-contract", "resume-manifest", "typed-failures", "duplicate-key-verify", "max-ledger-verify"}}})

	for _, chunk := range chunks {
		if cfg.Resume {
			done, err := p.chunkComplete(ctx, chunk.Start, chunk.End)
			if err != nil {
				return err
			}
			if done {
				emit(out, Event{EventType: "component.chunk_skipped", ComponentID: cfg.Component(), RunID: cfg.RunID(), Network: cfg.Network, ChunkStart: chunk.Start, ChunkEnd: chunk.End, Status: "completed"})
				continue
			}
		}
		emit(out, Event{EventType: "component.chunk_started", ComponentID: cfg.Component(), RunID: cfg.RunID(), Network: cfg.Network, ChunkStart: chunk.Start, ChunkEnd: chunk.End, Status: "running"})
		for _, projection := range executableCurrentProjections() {
			emit(out, Event{EventType: "component.projection_started", ComponentID: cfg.Component(), RunID: cfg.RunID(), Network: cfg.Network, ChunkStart: chunk.Start, ChunkEnd: chunk.End, ProjectionName: projection.Name, TargetTable: projection.TargetTable, Phase: "replace_current", Status: "running"})
			rows, err := p.replaceProjection(ctx, chunk, projection)
			if err != nil {
				_ = p.markManifest(ctx, chunk.Start, chunk.End, projection.Name, "failed", err.Error(), 0)
				emit(out, Event{EventType: "component.failed", ComponentID: cfg.Component(), RunID: cfg.RunID(), Network: cfg.Network, ChunkStart: chunk.Start, ChunkEnd: chunk.End, ProjectionName: projection.Name, TargetTable: projection.TargetTable, Phase: "replace_current", Status: "failed", FailureClass: classifyFailure(err), Error: err.Error(), Recommended: recommendedAction(classifyFailure(err))})
				return err
			}
			emit(out, Event{EventType: "component.projection_completed", ComponentID: cfg.Component(), RunID: cfg.RunID(), Network: cfg.Network, ChunkStart: chunk.Start, ChunkEnd: chunk.End, ProjectionName: projection.Name, TargetTable: projection.TargetTable, Phase: "replace_current", Status: "completed", Metadata: map[string]interface{}{"source": projection.Source, "mode": "replace_as_of", "row_count": rows, "as_of_ledger": cfg.End}})
		}
		emit(out, Event{EventType: "component.chunk_completed", ComponentID: cfg.Component(), RunID: cfg.RunID(), Network: cfg.Network, ChunkStart: chunk.Start, ChunkEnd: chunk.End, Status: "completed"})
	}
	emit(out, Event{EventType: "component.run_completed", ComponentID: cfg.Component(), RunID: cfg.RunID(), Network: cfg.Network, Status: "completed", Metadata: map[string]interface{}{"projection_count": len(executableCurrentProjections()), "checkpoint_contract": "current-state tables are replace-as-of end-ledger; no serving handoff checkpoint is advanced by this component"}})
	return writeSummary(cfg, chunks, silverCurrentProjections())
}

func silverCurrentProjections() []Projection {
	return []Projection{
		{Name: "accounts_current", TargetTable: "silver.accounts_current", Source: "silver.accounts_snapshot", Mode: "replace_as_of", Required: true, Status: "implemented"},
		{Name: "trustlines_current", TargetTable: "silver.trustlines_current", Source: "silver.trustlines_snapshot", Mode: "replace_as_of", Required: true, Status: "implemented"},
		{Name: "offers_current", TargetTable: "silver.offers_current", Source: "silver.offers_snapshot", Mode: "replace_as_of", Required: true, Status: "implemented"},
		{Name: "contract_data_current", TargetTable: "silver.contract_data_current", Source: "silver.contract_data_changes", Mode: "replace_as_of", Required: true, Status: "implemented"},
		{Name: "native_balances_current", TargetTable: "silver.native_balances_current", Source: "silver.balance_changes", Mode: "replace_as_of", Required: true, Status: "implemented"},
		{Name: "address_balances_current", TargetTable: "silver.address_balances_current", Source: "silver.balance_changes", Mode: "replace_as_of", Required: true, Status: "implemented"},
		{Name: "claimable_balances_current", TargetTable: "silver.claimable_balances_current", Source: "source mapping open", Mode: "replace_as_of", Required: true, Status: "blocked_source_mapping", Gap: "design asks for explicit implementation/source mapping before production completion"},
		{Name: "ttl_current", TargetTable: "silver.ttl_current", Source: "source mapping open", Mode: "replace_as_of", Required: true, Status: "blocked_source_mapping", Gap: "TTL authoritative source not yet identified in cold Silver tables"},
		{Name: "token_registry", TargetTable: "silver.token_registry", Source: "source mapping open", Mode: "replace_as_of", Required: true, Status: "blocked_source_mapping", Gap: "registry bootstrap/source of truth not yet specified"},
	}
}

func executableCurrentProjections() []CurrentProjection {
	return []CurrentProjection{
		{Name: "accounts_current", TargetTable: "accounts_current", Source: "accounts_snapshot", MaxLedgerCol: "last_modified_ledger", KeyExprs: []string{"account_id"}, SelectSQL: selectAccountsCurrent},
		{Name: "trustlines_current", TargetTable: "trustlines_current", Source: "trustlines_snapshot", MaxLedgerCol: "last_modified_ledger", KeyExprs: []string{"account_id", "asset_type", "asset_code", "asset_issuer", "liquidity_pool_id"}, SelectSQL: selectTrustlinesCurrent},
		{Name: "offers_current", TargetTable: "offers_current", Source: "offers_snapshot", MaxLedgerCol: "last_modified_ledger", KeyExprs: []string{"offer_id"}, SelectSQL: selectOffersCurrent},
		{Name: "contract_data_current", TargetTable: "contract_data_current", Source: "contract_data_changes", MaxLedgerCol: "last_modified_ledger", KeyExprs: []string{"contract_id", "key_hash"}, SelectSQL: selectContractDataCurrent},
		{Name: "native_balances_current", TargetTable: "native_balances_current", Source: "balance_changes", MaxLedgerCol: "last_modified_ledger", KeyExprs: []string{"account_id"}, SelectSQL: selectNativeBalancesCurrent},
		{Name: "address_balances_current", TargetTable: "address_balances_current", Source: "balance_changes", MaxLedgerCol: "last_updated_ledger", KeyExprs: []string{"owner_address", "asset_key"}, SelectSQL: selectAddressBalancesCurrent},
	}
}

func (p *Projector) replaceProjection(ctx context.Context, chunk Chunk, projection CurrentProjection) (int64, error) {
	if err := p.markManifest(ctx, chunk.Start, chunk.End, projection.Name, "running", "", 0); err != nil {
		return 0, err
	}
	if err := p.ensureTargetTable(ctx, projection); err != nil {
		return 0, err
	}
	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("begin %s: %w", projection.Name, err)
	}
	if _, err := tx.ExecContext(ctx, fmt.Sprintf("DELETE FROM %s WHERE network = %s", p.table(projection.TargetTable), q(p.cfg.Network))); err != nil {
		_ = tx.Rollback()
		return 0, fmt.Errorf("delete %s: %w", projection.Name, err)
	}
	if _, err := tx.ExecContext(ctx, fmt.Sprintf("INSERT INTO %s %s", p.table(projection.TargetTable), projection.SelectSQL(p))); err != nil {
		_ = tx.Rollback()
		return 0, fmt.Errorf("insert %s: %w", projection.Name, err)
	}
	if err := tx.Commit(); err != nil {
		_ = tx.Rollback()
		return 0, fmt.Errorf("commit %s: %w", projection.Name, err)
	}
	rowCount, err := p.verifyProjection(ctx, projection)
	if err != nil {
		return 0, err
	}
	if err := p.markManifest(ctx, chunk.Start, chunk.End, projection.Name, "completed", "", rowCount); err != nil {
		return 0, err
	}
	return rowCount, nil
}

func (p *Projector) ensureTargetTable(ctx context.Context, projection CurrentProjection) error {
	stmt := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s AS SELECT * FROM (%s) seed WHERE false", p.table(projection.TargetTable), projection.SelectSQL(p))
	if _, err := p.db.ExecContext(ctx, stmt); err != nil {
		return fmt.Errorf("create %s: %w", projection.Name, err)
	}
	return nil
}

func (p *Projector) verifyProjection(ctx context.Context, projection CurrentProjection) (int64, error) {
	var rowCount int64
	if err := p.db.QueryRowContext(ctx, fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE network = %s", p.table(projection.TargetTable), q(p.cfg.Network))).Scan(&rowCount); err != nil {
		return 0, fmt.Errorf("count %s: %w", projection.Name, err)
	}
	parts := make([]string, 0, len(projection.KeyExprs))
	for _, key := range projection.KeyExprs {
		parts = append(parts, fmt.Sprintf("COALESCE(CAST(%s AS VARCHAR), '')", ident(key)))
	}
	var duplicates int64
	dupSQL := fmt.Sprintf(`WITH keyed AS (
		SELECT %s, COUNT(*) AS n
		FROM %s
		WHERE network = %s
		GROUP BY %s
		HAVING COUNT(*) > 1
	) SELECT COUNT(*) FROM keyed`, strings.Join(parts, ", "), p.table(projection.TargetTable), q(p.cfg.Network), strings.Join(parts, ", "))
	if err := p.db.QueryRowContext(ctx, dupSQL).Scan(&duplicates); err != nil {
		return 0, fmt.Errorf("duplicate-key verification %s: %w", projection.Name, err)
	}
	if duplicates != 0 {
		return 0, fmt.Errorf("duplicate-key verification failed for %s: %d duplicate key groups", projection.Name, duplicates)
	}
	var maxLedger sql.NullInt64
	maxSQL := fmt.Sprintf("SELECT MAX(%s) FROM %s WHERE network = %s", ident(projection.MaxLedgerCol), p.table(projection.TargetTable), q(p.cfg.Network))
	if err := p.db.QueryRowContext(ctx, maxSQL).Scan(&maxLedger); err != nil {
		return 0, fmt.Errorf("max-ledger verification %s: %w", projection.Name, err)
	}
	if maxLedger.Valid && maxLedger.Int64 > p.cfg.End {
		return 0, fmt.Errorf("max-ledger verification failed for %s: max %d > end ledger %d", projection.Name, maxLedger.Int64, p.cfg.End)
	}
	return rowCount, nil
}

func (p *Projector) ensureSchema(ctx context.Context) error {
	if _, err := p.db.ExecContext(ctx, "CREATE SCHEMA IF NOT EXISTS "+p.schema()); err != nil {
		return fmt.Errorf("create silver schema: %w", err)
	}
	return nil
}

func (p *Projector) ensureManifest(ctx context.Context) error {
	stmt := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
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
	)`, p.table("silver_current_projector_manifest"))
	if _, err := p.db.ExecContext(ctx, stmt); err != nil {
		return fmt.Errorf("create manifest: %w", err)
	}
	return nil
}

func (p *Projector) markManifest(ctx context.Context, start, end int64, projection, status, message string, rows int64) error {
	if _, err := p.db.ExecContext(ctx, fmt.Sprintf("DELETE FROM %s WHERE run_id=%s AND network=%s AND start_ledger=%d AND end_ledger=%d AND projection_name=%s", p.table("silver_current_projector_manifest"), q(p.cfg.RunID()), q(p.cfg.Network), start, end, q(projection))); err != nil {
		return err
	}
	completed := "NULL"
	if status == "completed" || status == "failed" {
		completed = "current_timestamp"
	}
	stmt := fmt.Sprintf(`INSERT INTO %s VALUES (%s, %s, %s, %s, %d, %d, %s, %d, %s, current_timestamp, %s)`,
		p.table("silver_current_projector_manifest"),
		q(p.cfg.RunID()), q(p.cfg.Component()), q(projection), q(p.cfg.Network), start, end, q(status), rows, q(message), completed)
	if _, err := p.db.ExecContext(ctx, stmt); err != nil {
		return err
	}
	return p.jsonl.Mark(ctx, ManifestRecord{RunID: p.cfg.RunID(), ComponentID: p.cfg.Component(), ProjectionName: projection, Network: p.cfg.Network, StartLedger: start, EndLedger: end, Status: status, ErrorMessage: message, RowCount: rows, UpdatedAt: time.Now().UTC().Format(time.RFC3339Nano)})
}

func (p *Projector) chunkComplete(ctx context.Context, start, end int64) (bool, error) {
	var count int64
	query := fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE run_id=%s AND network=%s AND start_ledger=%d AND end_ledger=%d AND status='completed'", p.table("silver_current_projector_manifest"), q(p.cfg.RunID()), q(p.cfg.Network), start, end)
	if err := p.db.QueryRowContext(ctx, query).Scan(&count); err != nil {
		return false, err
	}
	return count == int64(len(executableCurrentProjections())), nil
}

func selectAccountsCurrent(p *Projector) string {
	return fmt.Sprintf(`SELECT network, account_id, balance, sequence_number, num_subentries,
		num_sponsoring, num_sponsored, home_domain, master_weight, low_threshold, med_threshold,
		high_threshold, flags, auth_required, auth_revocable, auth_immutable, auth_clawback_enabled,
		signers, sponsor_account, created_at, COALESCE(updated_at, closed_at, current_timestamp) AS updated_at,
		ledger_sequence AS last_modified_ledger, ledger_range, era_id, version_label
		FROM (
			SELECT *, row_number() OVER (PARTITION BY account_id ORDER BY ledger_sequence DESC, COALESCE(updated_at, closed_at) DESC NULLS LAST) AS rn
			FROM %s
			WHERE network = %s AND ledger_sequence <= %d
		) WHERE rn = 1`, p.table("accounts_snapshot"), q(p.cfg.Network), p.cfg.End)
}

func selectTrustlinesCurrent(p *Projector) string {
	return fmt.Sprintf(`SELECT network, account_id, asset_type, asset_issuer, asset_code,
		NULL::VARCHAR AS liquidity_pool_id,
		TRY_CAST(ROUND(TRY_CAST(balance AS DOUBLE) * 10000000) AS BIGINT) AS balance,
		TRY_CAST(ROUND(TRY_CAST(trust_limit AS DOUBLE) * 10000000) AS BIGINT) AS trust_line_limit,
		TRY_CAST(ROUND(TRY_CAST(buying_liabilities AS DOUBLE) * 10000000) AS BIGINT) AS buying_liabilities,
		TRY_CAST(ROUND(TRY_CAST(selling_liabilities AS DOUBLE) * 10000000) AS BIGINT) AS selling_liabilities,
		(CASE WHEN authorized THEN 1 ELSE 0 END)
			+ (CASE WHEN authorized_to_maintain_liabilities THEN 2 ELSE 0 END)
			+ (CASE WHEN clawback_enabled THEN 4 ELSE 0 END) AS flags,
		ledger_sequence AS last_modified_ledger, ledger_sequence, created_at, NULL::VARCHAR AS sponsor,
		ledger_range, era_id, version_label, current_timestamp AS inserted_at, current_timestamp AS updated_at
		FROM (
			SELECT *, row_number() OVER (
				PARTITION BY account_id, asset_type, COALESCE(asset_code, ''), COALESCE(asset_issuer, '')
				ORDER BY ledger_sequence DESC, created_at DESC NULLS LAST
			) AS rn
			FROM %s
			WHERE network = %s AND ledger_sequence <= %d
		) WHERE rn = 1`, p.table("trustlines_snapshot"), q(p.cfg.Network), p.cfg.End)
}

func selectOffersCurrent(p *Projector) string {
	return fmt.Sprintf(`SELECT network, offer_id, seller_account AS seller_id,
		selling_asset_type, selling_asset_code, selling_asset_issuer,
		buying_asset_type, buying_asset_code, buying_asset_issuer,
		TRY_CAST(amount AS BIGINT) AS amount,
		CASE WHEN price IS NULL THEN NULL WHEN contains(CAST(price AS VARCHAR), '/') THEN TRY_CAST(split_part(CAST(price AS VARCHAR), '/', 1) AS INTEGER) ELSE NULL END AS price_n,
		CASE WHEN price IS NULL THEN NULL WHEN contains(CAST(price AS VARCHAR), '/') THEN TRY_CAST(split_part(CAST(price AS VARCHAR), '/', 2) AS INTEGER) ELSE NULL END AS price_d,
		TRY_CAST(CASE
			WHEN price IS NULL THEN NULL
			WHEN contains(CAST(price AS VARCHAR), '/') THEN TRY_CAST(split_part(CAST(price AS VARCHAR), '/', 1) AS DOUBLE) / NULLIF(TRY_CAST(split_part(CAST(price AS VARCHAR), '/', 2) AS DOUBLE), 0)
			ELSE TRY_CAST(price AS DOUBLE)
		END AS DECIMAL(20, 7)) AS price,
		flags, ledger_sequence AS last_modified_ledger, ledger_sequence, created_at, NULL::VARCHAR AS sponsor,
		ledger_range, era_id, version_label, current_timestamp AS inserted_at, current_timestamp AS updated_at
		FROM (
			SELECT *, row_number() OVER (PARTITION BY offer_id ORDER BY ledger_sequence DESC, created_at DESC NULLS LAST) AS rn
			FROM %s
			WHERE network = %s AND ledger_sequence <= %d
		) WHERE rn = 1`, p.table("offers_snapshot"), q(p.cfg.Network), p.cfg.End)
}

func selectContractDataCurrent(p *Projector) string {
	return fmt.Sprintf(`SELECT network, contract_id, key_hash, contract_durability AS durability,
		asset_type, asset_code, asset_issuer, data_value,
		last_modified_ledger, ledger_sequence, closed_at, closed_at AS created_at, ledger_range,
		current_timestamp AS updated_at
		FROM (
			SELECT *, row_number() OVER (
				PARTITION BY contract_id, key_hash
				ORDER BY ledger_sequence DESC, last_modified_ledger DESC NULLS LAST, closed_at DESC NULLS LAST
			) AS rn
			FROM %s
			WHERE network = %s AND ledger_sequence <= %d
		) WHERE rn = 1 AND COALESCE(deleted, false) = false`, p.table("contract_data_changes"), q(p.cfg.Network), p.cfg.End)
}

func selectNativeBalancesCurrent(p *Projector) string {
	return fmt.Sprintf(`SELECT network, address AS account_id, TRY_CAST(balance AS BIGINT) AS balance,
		0::BIGINT AS buying_liabilities, 0::BIGINT AS selling_liabilities,
		NULL::INTEGER AS num_subentries, NULL::INTEGER AS num_sponsoring, NULL::INTEGER AS num_sponsored,
		NULL::BIGINT AS sequence_number, ledger_sequence AS last_modified_ledger, ledger_sequence,
		ledger_range, current_timestamp AS inserted_at, current_timestamp AS updated_at
		FROM (
			SELECT *, row_number() OVER (
				PARTITION BY address
				ORDER BY ledger_sequence DESC, ledger_closed_at DESC NULLS LAST
			) AS rn
			FROM %s
			WHERE network = %s AND ledger_sequence <= %d AND (asset_type = 'native' OR asset_code = 'XLM')
		) WHERE rn = 1 AND COALESCE(deleted, false) = false`, p.table("balance_changes"), q(p.cfg.Network), p.cfg.End)
}

func selectAddressBalancesCurrent(p *Projector) string {
	assetKey := `CASE
		WHEN asset_type = 'native' OR asset_code = 'XLM' THEN 'native:XLM'
		WHEN asset_issuer IS NULL OR asset_issuer = '' THEN concat(COALESCE(asset_type, 'asset'), ':', COALESCE(asset_code, ''))
		ELSE concat(COALESCE(asset_type, 'asset'), ':', COALESCE(asset_code, ''), ':', asset_issuer)
	END`
	return fmt.Sprintf(`SELECT network, address AS owner_address, asset_key, asset_type,
		NULL::VARCHAR AS token_contract_id, asset_code, asset_issuer, asset_code AS symbol, 7::INTEGER AS decimals,
		CAST(balance AS VARCHAR) AS balance_raw, CAST(balance AS VARCHAR) AS balance_display,
		'silver.balance_changes' AS balance_source, ledger_sequence AS last_updated_ledger,
		ledger_closed_at AS last_updated_at, current_timestamp AS updated_at
		FROM (
			SELECT *, %s AS asset_key, row_number() OVER (
				PARTITION BY address, %s
				ORDER BY ledger_sequence DESC, ledger_closed_at DESC NULLS LAST
			) AS rn
			FROM %s
			WHERE network = %s AND ledger_sequence <= %d
		) WHERE rn = 1 AND COALESCE(deleted, false) = false`, assetKey, assetKey, p.table("balance_changes"), q(p.cfg.Network), p.cfg.End)
}

func PlanChunks(start, end, size int64) []Chunk {
	if start <= 0 || end < start || size <= 0 {
		return nil
	}
	var chunks []Chunk
	for s, i := start, 0; s <= end; s, i = s+size, i+1 {
		e := s + size - 1
		if e > end {
			e = end
		}
		chunks = append(chunks, Chunk{Start: s, End: e, Index: i})
	}
	return chunks
}

func (c Config) PlannedChunks() []Chunk {
	if c.ChunkStart != 0 {
		return []Chunk{{Start: c.ChunkStart, End: c.ChunkEnd, Index: 0}}
	}
	return PlanChunks(c.Start, c.End, c.Chunk)
}

func (p *Projector) table(table string) string {
	return fmt.Sprintf("%s.%s", p.schema(), ident(table))
}

func (p *Projector) schema() string {
	if p.cfg.SilverAlias == "" {
		return ident(p.cfg.SilverSchema)
	}
	return fmt.Sprintf("%s.%s", ident(p.cfg.SilverAlias), ident(p.cfg.SilverSchema))
}

func (m JSONLManifest) Mark(_ context.Context, rec ManifestRecord) error {
	if m.path == "" {
		return nil
	}
	f, err := os.OpenFile(m.path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return fmt.Errorf("open manifest: %w", err)
	}
	defer f.Close()
	return json.NewEncoder(f).Encode(rec)
}

func (m JSONLManifest) Completed(_ context.Context, runID, network string, start, end int64) (bool, error) {
	if m.path == "" {
		return false, nil
	}
	f, err := os.Open(m.path)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("read manifest: %w", err)
	}
	defer f.Close()
	dec := json.NewDecoder(f)
	completed := map[string]bool{}
	for {
		var rec ManifestRecord
		if err := dec.Decode(&rec); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return false, fmt.Errorf("decode manifest: %w", err)
		}
		if rec.RunID == runID && rec.Network == network && rec.StartLedger == start && rec.EndLedger == end && rec.Status == "completed" {
			completed[rec.ProjectionName] = true
		}
	}
	for _, p := range silverCurrentProjections() {
		if !completed[p.Name] {
			return false, nil
		}
	}
	return true, nil
}

func emit(out io.Writer, ev Event) {
	if ev.Timestamp == "" {
		ev.Timestamp = time.Now().UTC().Format(time.RFC3339Nano)
	}
	_ = json.NewEncoder(out).Encode(ev)
}

func writeStatus(out io.Writer, cfg Config) error {
	return json.NewEncoder(out).Encode(map[string]interface{}{
		"component_id": cfg.Component(),
		"version":      Version,
		"status":       "ready",
		"capabilities": []string{"current-state-as-of", "deterministic-chunk-inputs", "resume-manifest", "typed-failures", "json-lifecycle-events"},
		"projections":  silverCurrentProjections(),
	})
}

func writeSummary(cfg Config, chunks []Chunk, projections []Projection) error {
	if cfg.SummaryPath == "" {
		return nil
	}
	f, err := os.OpenFile(cfg.SummaryPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return fmt.Errorf("write summary: %w", err)
	}
	defer f.Close()
	return json.NewEncoder(f).Encode(map[string]interface{}{"component_id": cfg.Component(), "run_id": cfg.RunID(), "network": cfg.Network, "chunks": chunks, "projections": projections, "status": "completed"})
}

func classifyFailure(err error) FailureClass {
	if err == nil {
		return FailureUnknown
	}
	msg := strings.ToLower(err.Error())
	switch {
	case strings.Contains(msg, "schema") || strings.Contains(msg, "column"):
		return FailureNonRetryableSchema
	case strings.Contains(msg, "duplicate") || strings.Contains(msg, "validation") || strings.Contains(msg, "decode"):
		return FailureNonRetryableData
	case strings.Contains(msg, "verify") || strings.Contains(msg, "checkpoint"):
		return FailureVerification
	case strings.Contains(msg, "timeout") || strings.Contains(msg, "connection") || strings.Contains(msg, "s3") || strings.Contains(msg, "catalog"):
		return FailureRetryableInfrastructure
	default:
		return FailureUnknown
	}
}

func recommendedAction(class FailureClass) string {
	switch class {
	case FailureRetryableInfrastructure:
		return "retry_same_chunk"
	case FailureVerification:
		return "retry_verification_after_inspection"
	case FailureNonRetryableData, FailureNonRetryableSchema:
		return "fix_component_or_source_then_rerun"
	case FailureConfig:
		return "fix_config"
	default:
		return "inspect"
	}
}

func exitCode(class FailureClass) int {
	switch class {
	case FailureConfig:
		return 2
	case FailureRetryableInfrastructure:
		return 75
	default:
		return 1
	}
}

func getenv(k, fallback string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return fallback
}

func envInt64(k string, fallback int64) int64 {
	v := os.Getenv(k)
	if v == "" {
		return fallback
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		return fallback
	}
	return n
}

func safeEndpoint(endpoint string) string {
	if endpoint == "" {
		return ""
	}
	return strings.Split(endpoint, "@")[len(strings.Split(endpoint, "@"))-1]
}

func ident(s string) string { return `"` + strings.ReplaceAll(s, `"`, `""`) + `"` }
func q(s string) string     { return `'` + strings.ReplaceAll(s, `'`, `''`) + `'` }
