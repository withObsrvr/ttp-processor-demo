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
	"path/filepath"
	"strconv"
	"strings"
	"time"

	_ "github.com/duckdb/duckdb-go/v2"
)

var Version = "dev"

const componentID = "silver-current-state-projector"
const progressInterval = 15 * time.Second

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
	Partitions int
	Resume     bool
	Status     bool

	SilverCatalog string
	SilverData    string
	SilverAlias   string
	SilverSchema  string
	SilverMeta    string
	BronzeCatalog string
	BronzeData    string
	BronzeAlias   string
	BronzeSchema  string
	BronzeMeta    string
	ManifestPath  string
	SummaryPath   string
	Projections   string
	S3KeyID       string
	S3Secret      string
	S3Region      string
	S3Endpoint    string

	FlowctlEnabled   bool
	FlowctlEndpoint  string
	FlowctlRunID     string
	FlowctlAttempt   string
	FlowctlComponent string

	// LocalScratchPath, when set, points staging/keys/delta tables at a local
	// on-disk DuckDB attached at this path (NVMe recommended) instead of the
	// DuckLake catalog, and sets temp_directory for spill. Empty = in-process
	// memory catalog. MemoryLimit sets DuckDB memory_limit. PublishBuckets is
	// the number of hash buckets used for the resumable, bounded final publish.
	LocalScratchPath string
	MemoryLimit      string
	PublishBuckets   int
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
	RunID             string `json:"run_id"`
	ComponentID       string `json:"component_id"`
	ProjectionName    string `json:"projection_name"`
	ProjectionVersion string `json:"projection_version,omitempty"`
	Network           string `json:"network"`
	StartLedger       int64  `json:"start_ledger"`
	EndLedger         int64  `json:"end_ledger"`
	Status            string `json:"status"`
	FailureClass      string `json:"failure_class,omitempty"`
	ErrorMessage      string `json:"error_message,omitempty"`
	RowCount          int64  `json:"row_count"`
	UpdatedAt         string `json:"updated_at"`
}

type ManifestStore interface {
	Mark(context.Context, ManifestRecord) error
	Completed(context.Context, string, string, int64, int64) (bool, error)
}

type JSONLManifest struct {
	path string
}

type Projector struct {
	db                      *sql.DB
	cfg                     Config
	jsonl                   ManifestStore
	localAlias              string // attached catalog for local staging; "" => in-process memory catalog
	scratchFile             string // on-disk scratch DB path to remove on Close, if any
	sourceAvailabilityKnown bool
	balanceChangesAvailable bool
}

type CurrentProjection struct {
	Name            string
	ManifestVersion string
	TargetTable     string
	Source          string
	SourceLedgerCol string
	SourceFilter    string
	PartitionExpr   string
	MaxLedgerCol    string
	KeyExprs        []string
	SelectSQL       func(*Projector, int64, int64) string
	KeySQL          func(*Projector, int64, int64) string
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
	fs.StringVar(&cfg.BronzeCatalog, "bronze-ducklake-catalog", getenv("BRONZE_DUCKLAKE_CATALOG", ""), "Bronze DuckLake catalog DSN/path; defaults to silver catalog when empty")
	fs.StringVar(&cfg.BronzeData, "bronze-data-path", getenv("BRONZE_DATA_PATH", ""), "Bronze DuckLake data path; defaults to silver data path when empty")
	fs.StringVar(&cfg.BronzeAlias, "bronze-catalog-name", getenv("BRONZE_CATALOG_NAME", "bronze_catalog"), "DuckDB alias for Bronze catalog")
	fs.StringVar(&cfg.BronzeSchema, "bronze-schema", getenv("BRONZE_SCHEMA", "bronze"), "Bronze schema name")
	fs.StringVar(&cfg.BronzeMeta, "bronze-metadata-schema", getenv("BRONZE_DUCKLAKE_METADATA_SCHEMA", "bronze_meta"), "Bronze DuckLake metadata schema")
	fs.StringVar(&cfg.ManifestPath, "manifest-path", getenv("MANIFEST_PATH", ""), "JSONL manifest path for batch-local durable status")
	fs.StringVar(&cfg.SummaryPath, "summary-path", getenv("SUMMARY_PATH", ""), "optional JSON summary output path")
	fs.StringVar(&cfg.Projections, "projections", getenv("PROJECTIONS", ""), "optional comma-separated current projection names to run")
	fs.StringVar(&cfg.S3KeyID, "s3-key-id", getenv("S3_KEY_ID", getenv("B2_KEY_ID", "")), "S3/B2 access key ID")
	fs.StringVar(&cfg.S3Secret, "s3-secret", getenv("S3_SECRET", getenv("B2_KEY_SECRET", "")), "S3/B2 secret access key")
	fs.StringVar(&cfg.S3Region, "s3-region", getenv("S3_REGION", getenv("B2_REGION", "us-west-004")), "S3/B2 region")
	fs.StringVar(&cfg.S3Endpoint, "s3-endpoint", getenv("S3_ENDPOINT", getenv("B2_ENDPOINT", "")), "S3/B2 endpoint")
	fs.StringVar(&cfg.LocalScratchPath, "local-scratch-path", getenv("LOCAL_SCRATCH_PATH", ""), "local on-disk dir for staging + spill (NVMe); empty uses the in-memory catalog")
	fs.StringVar(&cfg.MemoryLimit, "memory-limit", getenv("MEMORY_LIMIT", ""), "DuckDB memory_limit (e.g. 80GB); empty leaves the DuckDB default")
	fs.IntVar(&cfg.PublishBuckets, "publish-buckets", envInt("PUBLISH_BUCKETS", 32), "hash buckets for the resumable bounded publish")
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

func (c Config) effectiveBronzeCatalog() string {
	if c.BronzeCatalog != "" {
		return c.BronzeCatalog
	}
	return c.SilverCatalog
}

func (c Config) effectiveBronzeData() string {
	if c.BronzeData != "" {
		return c.BronzeData
	}
	return c.SilverData
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
	// Pin to a single connection so SET memory_limit / temp_directory / the S3 secret / the
	// DuckLake ATTACH all apply to the one connection that runs every query. With a pool, those
	// session settings could land on a connection the heavy query never uses (a prime suspect for
	// memory_limit not constraining the run).
	db.SetMaxOpenConns(1)
	p := &Projector{db: db, cfg: cfg, jsonl: JSONLManifest{path: cfg.ManifestPath}}
	for _, stmt := range []string{"INSTALL ducklake", "LOAD ducklake", "INSTALL httpfs", "LOAD httpfs"} {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			db.Close()
			return nil, fmt.Errorf("%s: %w", stmt, err)
		}
	}
	if err := p.configureS3(ctx); err != nil {
		db.Close()
		return nil, err
	}
	if err := p.attachSilver(ctx); err != nil {
		db.Close()
		return nil, err
	}
	p.balanceChangesAvailable, err = p.sourceTableExists(ctx, "balance_changes")
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("inspect optional balance_changes source: %w", err)
	}
	p.sourceAvailabilityKnown = true
	if err := p.attachBronze(ctx); err != nil {
		db.Close()
		return nil, err
	}
	if err := p.configureLocal(ctx); err != nil {
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

func (p *Projector) sourceTableExists(ctx context.Context, table string) (bool, error) {
	var count int64
	query := `SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = ? AND table_name = ?`
	args := []any{p.cfg.SilverSchema, table}
	if p.cfg.SilverAlias != "" {
		query += ` AND table_catalog = ?`
		args = append(args, p.cfg.SilverAlias)
	}
	if err := p.db.QueryRowContext(ctx, query, args...).Scan(&count); err != nil {
		return false, err
	}
	return count > 0, nil
}

func (p *Projector) includeBalanceChanges() bool {
	return !p.sourceAvailabilityKnown || p.balanceChangesAvailable
}

func (p *Projector) configureS3(ctx context.Context) error {
	if p.cfg.S3KeyID == "" && p.cfg.S3Secret == "" {
		return nil
	}
	if p.cfg.S3KeyID == "" || p.cfg.S3Secret == "" {
		return errors.New("--s3-key-id and --s3-secret must be provided together")
	}
	endpoint := strings.TrimPrefix(strings.TrimPrefix(p.cfg.S3Endpoint, "https://"), "http://")
	stmt := fmt.Sprintf("CREATE OR REPLACE SECRET (TYPE S3, KEY_ID %s, SECRET %s, REGION %s, ENDPOINT %s, URL_STYLE 'path')", q(p.cfg.S3KeyID), q(p.cfg.S3Secret), q(p.cfg.S3Region), q(endpoint))
	if _, err := p.db.ExecContext(ctx, stmt); err != nil {
		return fmt.Errorf("create s3 secret: %w", err)
	}
	return nil
}

func (p *Projector) Close() error {
	if p.localAlias != "" {
		_, _ = p.db.ExecContext(context.Background(), "DETACH "+ident(p.localAlias))
	}
	err := p.db.Close()
	if p.scratchFile != "" {
		_ = os.Remove(p.scratchFile)
	}
	return err
}

// configureLocal sets spill/memory pragmas and, when LocalScratchPath is set,
// attaches an on-disk DuckDB as the "scratch" catalog so staging/keys/delta live
// on local NVMe instead of the DuckLake (object-storage) catalog. With no path,
// local tables fall back to the in-process memory catalog (fine for tests/small
// runs). Either way, no intermediate writes hit DuckLake — only the final publish.
func (p *Projector) configureLocal(ctx context.Context) error {
	if _, err := p.db.ExecContext(ctx, "SET preserve_insertion_order=false"); err != nil {
		return fmt.Errorf("set preserve_insertion_order: %w", err)
	}
	if p.cfg.MemoryLimit != "" {
		if _, err := p.db.ExecContext(ctx, fmt.Sprintf("SET memory_limit=%s", q(p.cfg.MemoryLimit))); err != nil {
			return fmt.Errorf("set memory_limit: %w", err)
		}
	}
	if p.cfg.LocalScratchPath == "" {
		return nil
	}
	tmp := filepath.Join(p.cfg.LocalScratchPath, "duckdb_tmp")
	if err := os.MkdirAll(tmp, 0o755); err != nil {
		return fmt.Errorf("create temp dir: %w", err)
	}
	if _, err := p.db.ExecContext(ctx, fmt.Sprintf("SET temp_directory=%s", q(tmp))); err != nil {
		return fmt.Errorf("set temp_directory: %w", err)
	}
	p.scratchFile = filepath.Join(p.cfg.LocalScratchPath, "silver_current_projector_scratch.duckdb")
	_ = os.Remove(p.scratchFile) // a stale scratch from a killed run is never reused
	if _, err := p.db.ExecContext(ctx, fmt.Sprintf("ATTACH %s AS %s", q(p.scratchFile), ident("scratch"))); err != nil {
		return fmt.Errorf("attach local scratch: %w", err)
	}
	p.localAlias = "scratch"
	return nil
}

// localTable resolves a staging/keys/delta table name to the local scratch
// catalog (never DuckLake). With no attached scratch it uses the in-process
// memory catalog (default catalog/schema), which is still local, not DuckLake.
func (p *Projector) localTable(name string) string {
	if p.localAlias == "" {
		return ident(name)
	}
	return fmt.Sprintf("%s.main.%s", ident(p.localAlias), ident(name))
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

func (p *Projector) attachBronze(ctx context.Context) error {
	catalog := p.cfg.effectiveBronzeCatalog()
	if catalog == "" {
		return nil
	}
	dataPath := p.cfg.effectiveBronzeData()
	stmt := fmt.Sprintf("ATTACH %s AS %s (DATA_PATH %s, METADATA_SCHEMA %s, AUTOMATIC_MIGRATION TRUE, OVERRIDE_DATA_PATH TRUE)", q(catalog), ident(p.cfg.BronzeAlias), q(dataPath), q(p.cfg.BronzeMeta))
	if _, err := p.db.ExecContext(ctx, stmt); err != nil {
		fallback := fmt.Sprintf("ATTACH %s AS %s (TYPE ducklake, DATA_PATH %s, METADATA_SCHEMA %s, AUTOMATIC_MIGRATION TRUE, OVERRIDE_DATA_PATH TRUE)", q(catalog), ident(p.cfg.BronzeAlias), q(dataPath), q(p.cfg.BronzeMeta))
		if _, fallbackErr := p.db.ExecContext(ctx, fallback); fallbackErr != nil {
			return fmt.Errorf("attach bronze ducklake: %w (fallback with TYPE ducklake also failed: %v)", err, fallbackErr)
		}
	}
	return nil
}

func (p *Projector) Run(ctx context.Context, out io.Writer) error {
	cfg := p.cfg
	chunks := cfg.PlannedChunks()
	projections, err := selectCurrentProjections(executableCurrentProjections(), cfg.Projections)
	if err != nil {
		return err
	}
	ledgerWindowStart, ledgerWindowEnd := cfg.Start, cfg.End
	if len(chunks) > 0 {
		ledgerWindowStart, ledgerWindowEnd = chunks[0].Start, chunks[0].End
	}
	ledgerWindows := PlanChunks(ledgerWindowStart, ledgerWindowEnd, cfg.Chunk)
	// Surface the DuckDB settings actually in effect so the log proves whether memory_limit /
	// temp_directory were applied (the recurring kernel-OOM-with-no-DuckDB-error suggested they
	// might not be). Best-effort — never blocks the run.
	var effMemLimit, effTempDir string
	_ = p.db.QueryRowContext(ctx, "SELECT current_setting('memory_limit')").Scan(&effMemLimit)
	_ = p.db.QueryRowContext(ctx, "SELECT current_setting('temp_directory')").Scan(&effTempDir)
	emit(out, Event{EventType: "component.run_started", ComponentID: cfg.Component(), RunID: cfg.RunID(), Network: cfg.Network, Status: "running", FlowctlEndpoint: safeEndpoint(cfg.FlowctlEndpoint), Metadata: map[string]interface{}{"version": Version, "range_start": ledgerWindowStart, "range_end": ledgerWindowEnd, "chunk_count": len(chunks), "ledger_window_count": len(ledgerWindows), "ledger_window_size": cfg.Chunk, "effective_memory_limit": effMemLimit, "effective_temp_directory": effTempDir, "capabilities": []string{"current-state-as-of", "deterministic-chunk-inputs", "idempotent-replace-contract", "ledger-window-staging-merge", "resume-manifest", "typed-failures", "duplicate-key-verify", "max-ledger-verify"}}})

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
		for _, projection := range projections {
			if cfg.Resume {
				done, err := p.projectionComplete(ctx, chunk.Start, chunk.End, projection)
				if err != nil {
					return err
				}
				if done {
					emit(out, Event{EventType: "component.projection_skipped", ComponentID: cfg.Component(), RunID: cfg.RunID(), Network: cfg.Network, ChunkStart: chunk.Start, ChunkEnd: chunk.End, ProjectionName: projection.Name, TargetTable: projection.TargetTable, Phase: "replace_current", Status: "completed", Metadata: map[string]interface{}{"reason": "already completed in manifest (resume)"}})
					continue
				}
			}
			emit(out, Event{EventType: "component.projection_started", ComponentID: cfg.Component(), RunID: cfg.RunID(), Network: cfg.Network, ChunkStart: chunk.Start, ChunkEnd: chunk.End, ProjectionName: projection.Name, TargetTable: projection.TargetTable, Phase: "replace_current", Status: "running"})
			stopProgress := startProgressHeartbeat(out, Event{ComponentID: cfg.Component(), RunID: cfg.RunID(), Network: cfg.Network, ChunkStart: chunk.Start, ChunkEnd: chunk.End, ProjectionName: projection.Name, TargetTable: projection.TargetTable, Phase: "replace_current", Status: "running"}, map[string]interface{}{"source": projection.Source, "mode": "replace_as_of", "as_of_ledger": chunk.End})
			rows, err := p.replaceProjection(ctx, out, chunk, projection)
			stopProgress()
			if err != nil {
				_ = p.markManifest(ctx, chunk.Start, chunk.End, projection, "failed", err.Error(), 0)
				emit(out, Event{EventType: "component.failed", ComponentID: cfg.Component(), RunID: cfg.RunID(), Network: cfg.Network, ChunkStart: chunk.Start, ChunkEnd: chunk.End, ProjectionName: projection.Name, TargetTable: projection.TargetTable, Phase: "replace_current", Status: "failed", FailureClass: classifyFailure(err), Error: err.Error(), Recommended: recommendedAction(classifyFailure(err))})
				return err
			}
			emit(out, Event{EventType: "component.projection_completed", ComponentID: cfg.Component(), RunID: cfg.RunID(), Network: cfg.Network, ChunkStart: chunk.Start, ChunkEnd: chunk.End, ProjectionName: projection.Name, TargetTable: projection.TargetTable, Phase: "replace_current", Status: "completed", Metadata: map[string]interface{}{"source": projection.Source, "mode": "replace_as_of", "row_count": rows, "as_of_ledger": chunk.End}})
		}
		emit(out, Event{EventType: "component.chunk_completed", ComponentID: cfg.Component(), RunID: cfg.RunID(), Network: cfg.Network, ChunkStart: chunk.Start, ChunkEnd: chunk.End, Status: "completed"})
	}
	emit(out, Event{EventType: "component.run_completed", ComponentID: cfg.Component(), RunID: cfg.RunID(), Network: cfg.Network, Status: "completed", Metadata: map[string]interface{}{"projection_count": len(projections), "checkpoint_contract": "current-state tables are replace-as-of end-ledger; no serving handoff checkpoint is advanced by this component"}})
	return writeSummary(cfg, chunks, silverCurrentProjections())
}

func silverCurrentProjections() []Projection {
	return []Projection{
		{Name: "accounts_current", TargetTable: "silver.accounts_current", Source: "silver.accounts_snapshot", Mode: "replace_as_of", Required: true, Status: "implemented"},
		{Name: "trustlines_current", TargetTable: "silver.trustlines_current", Source: "silver.trustlines_snapshot", Mode: "replace_as_of", Required: true, Status: "implemented"},
		{Name: "offers_current", TargetTable: "silver.offers_current", Source: "silver.offers_snapshot", Mode: "replace_as_of", Required: true, Status: "implemented"},
		{Name: "contract_data_current", TargetTable: "silver.contract_data_current", Source: "bronze.contract_data_snapshot_v1", Mode: "replace_as_of", Required: true, Status: "implemented"},
		{Name: "native_balances_current", TargetTable: "silver.native_balances_current", Source: "silver.balance_changes", Mode: "replace_as_of", Required: true, Status: "implemented"},
		{Name: "address_balances_current", TargetTable: "silver.address_balances_current", Source: "silver.balance_changes + silver.contract_balance_changes", Mode: "replace_as_of", Required: true, Status: "implemented"},
		{Name: "ttl_current", TargetTable: "silver.ttl_current", Source: "bronze.ttl_snapshot_v1", Mode: "replace_as_of", Required: true, Status: "implemented"},
		{Name: "claimable_balances_current", TargetTable: "silver.claimable_balances_current", Source: "source mapping open", Mode: "replace_as_of", Required: true, Status: "blocked_source_mapping", Gap: "design asks for explicit implementation/source mapping before production completion"},
		{Name: "token_registry", TargetTable: "silver.token_registry", Source: "source mapping open", Mode: "replace_as_of", Required: true, Status: "blocked_source_mapping", Gap: "registry bootstrap/source of truth not yet specified"},
	}
}

func executableCurrentProjections() []CurrentProjection {
	assetKey := addressBalanceAssetKeyExpr()
	return []CurrentProjection{
		{Name: "accounts_current", TargetTable: "accounts_current", Source: "accounts_snapshot", SourceLedgerCol: "ledger_sequence", PartitionExpr: "account_id", MaxLedgerCol: "last_modified_ledger", KeyExprs: []string{"account_id"}, SelectSQL: selectAccountsCurrent, KeySQL: selectAccountsCurrentKeys},
		{Name: "trustlines_current", TargetTable: "trustlines_current", Source: "trustlines_snapshot", SourceLedgerCol: "ledger_sequence", PartitionExpr: "concat(account_id, ':', COALESCE(asset_type, ''), ':', COALESCE(asset_code, ''), ':', COALESCE(asset_issuer, ''))", MaxLedgerCol: "last_modified_ledger", KeyExprs: []string{"account_id", "asset_type", "asset_code", "asset_issuer", "liquidity_pool_id"}, SelectSQL: selectTrustlinesCurrent, KeySQL: selectTrustlinesCurrentKeys},
		{Name: "offers_current", TargetTable: "offers_current", Source: "offers_snapshot", SourceLedgerCol: "ledger_sequence", PartitionExpr: "offer_id", MaxLedgerCol: "last_modified_ledger", KeyExprs: []string{"offer_id"}, SelectSQL: selectOffersCurrent, KeySQL: selectOffersCurrentKeys},
		{Name: "contract_data_current", TargetTable: "contract_data_current", Source: "contract_data_snapshot_v1", SourceLedgerCol: "ledger_sequence", PartitionExpr: "concat(contract_id, ':', ledger_key_hash)", MaxLedgerCol: "last_modified_ledger", KeyExprs: []string{"contract_id", "key_hash"}, SelectSQL: selectContractDataCurrent, KeySQL: selectContractDataCurrentKeys},
		{Name: "ttl_current", TargetTable: "ttl_current", Source: "ttl_snapshot_v1", SourceLedgerCol: "ledger_sequence", PartitionExpr: "key_hash", MaxLedgerCol: "last_modified_ledger", KeyExprs: []string{"key_hash"}, SelectSQL: selectTTLCurrent, KeySQL: selectTTLCurrentKeys},
		{Name: "native_balances_current", TargetTable: "native_balances_current", Source: "balance_changes", SourceLedgerCol: "ledger_sequence", SourceFilter: "(asset_type = 'native' OR asset_code = 'XLM')", PartitionExpr: "address", MaxLedgerCol: "last_modified_ledger", KeyExprs: []string{"account_id"}, SelectSQL: selectNativeBalancesCurrent, KeySQL: selectNativeBalancesCurrentKeys},
		{Name: "address_balances_current", ManifestVersion: "contract-balance-changes-v1", TargetTable: "address_balances_current", Source: "balance_changes + contract_balance_changes", SourceLedgerCol: "ledger_sequence", PartitionExpr: "concat(address, ':', " + assetKey + ")", MaxLedgerCol: "last_updated_ledger", KeyExprs: []string{"owner_address", "asset_key"}, SelectSQL: selectAddressBalancesCurrent, KeySQL: selectAddressBalancesCurrentKeys},
	}
}

func selectCurrentProjections(all []CurrentProjection, csv string) ([]CurrentProjection, error) {
	if strings.TrimSpace(csv) == "" {
		return all, nil
	}
	wanted := projectionSet(csv)
	selected := make([]CurrentProjection, 0, len(wanted))
	for _, p := range all {
		if wanted[p.Name] {
			selected = append(selected, p)
			delete(wanted, p.Name)
		}
	}
	if len(wanted) > 0 {
		return nil, fmt.Errorf("unknown projection(s): %s", strings.Join(sortedKeys(wanted), ","))
	}
	return selected, nil
}

func projectionSet(csv string) map[string]bool {
	out := map[string]bool{}
	for _, raw := range strings.Split(csv, ",") {
		name := strings.TrimSpace(raw)
		if name != "" {
			out[name] = true
		}
	}
	return out
}

func sortedKeys(m map[string]bool) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	for i := 1; i < len(keys); i++ {
		for j := i; j > 0 && keys[j] < keys[j-1]; j-- {
			keys[j], keys[j-1] = keys[j-1], keys[j]
		}
	}
	return keys
}

func (p *Projector) replaceProjection(ctx context.Context, out io.Writer, chunk Chunk, projection CurrentProjection) (int64, error) {
	if err := p.markManifest(ctx, chunk.Start, chunk.End, projection, "running", "", 0); err != nil {
		return 0, err
	}
	if err := p.ensureTargetTable(ctx, projection); err != nil {
		return 0, err
	}

	staging := p.localTable(projection.TargetTable + "__staging")
	if _, err := p.db.ExecContext(ctx, "DROP TABLE IF EXISTS "+staging); err != nil {
		return 0, fmt.Errorf("drop staging %s: %w", projection.Name, err)
	}
	// Ledger-windowed merge. Each window reads only rows in [w.Start, w.End] — a pushdown-able
	// ledger range, so DuckLake/Parquet reads just that slice (bounded memory + I/O), and each
	// source row is read once across all windows (O(N)). This is the original fold's structure;
	// the bug it fixes is the cumulative `ledger_sequence <= end` filter (every window re-scanned
	// the whole history -> O(N^2) AND a full-table read that buffered to OOM). KeySQL yields every
	// key touched in the window (incl. deletes) for the DELETE; SelectSQL yields the non-deleted
	// latest rows for the INSERT. Ascending windows => later windows overwrite earlier keys, so
	// staging converges to the global latest-per-key. Working set is one window, regardless of
	// whether memory_limit is honored.
	if _, err := p.db.ExecContext(ctx, fmt.Sprintf("CREATE TABLE %s AS SELECT * FROM (%s) seed WHERE false", staging, projection.SelectSQL(p, chunk.Start, chunk.Start-1))); err != nil {
		return 0, fmt.Errorf("create staging %s: %w", projection.Name, err)
	}
	// Current-state must reflect EVERY key as-of chunk.End, so the windowed merge always scans
	// from genesis (ledger 1, the first Stellar ledger) regardless of chunk.Start. A non-genesis
	// start would drop keys last-modified before it, and publishProjection (which replaces ALL
	// target buckets) would then delete them — producing truncated current tables (PR #84 review;
	// this is the bug that corrupted accounts_current/trustlines_current when run with --start
	// mid-history). The per-window BETWEEN filter still bounds memory; leading empty windows are
	// pushdown-skipped. (PlanChunks requires start >= 1.)
	const genesisLedger = 1
	windows := PlanChunks(genesisLedger, chunk.End, p.cfg.Chunk)
	emit(out, Event{EventType: "component.staging_started", ComponentID: p.cfg.Component(), RunID: p.cfg.RunID(), Network: p.cfg.Network, ChunkStart: chunk.Start, ChunkEnd: chunk.End, ProjectionName: projection.Name, TargetTable: projection.TargetTable, Phase: "staging_merge", Status: "running", Metadata: map[string]interface{}{"mode": "ledger_window_merge", "window_count": len(windows), "window_start": genesisLedger, "requested_start": chunk.Start, "as_of_ledger": chunk.End, "source": projection.Source}})
	keys := p.localTable(projection.TargetTable + "__keys")
	delta := p.localTable(projection.TargetTable + "__delta")
	for _, w := range windows {
		// keys/delta read source Parquet from cold storage (B2); a single transient read blip
		// ("partial file"/HTTP) over a multi-hour projection otherwise kills the whole run.
		// Retry those reads — CREATE OR REPLACE keeps each attempt idempotent.
		if err := p.execWindow(ctx, out, fmt.Sprintf("CREATE OR REPLACE TABLE %s AS %s", keys, projection.KeySQL(p, w.Start, w.End)), projection.Name, w.Index); err != nil {
			return 0, fmt.Errorf("compute keys %s window %d: %w", projection.Name, w.Index, err)
		}
		if err := p.execWindow(ctx, out, fmt.Sprintf("CREATE OR REPLACE TABLE %s AS %s", delta, projection.SelectSQL(p, w.Start, w.End)), projection.Name, w.Index); err != nil {
			return 0, fmt.Errorf("compute delta %s window %d: %w", projection.Name, w.Index, err)
		}
		if _, err := p.db.ExecContext(ctx, p.deleteSeenKeysSQL(staging, keys, projection)); err != nil {
			return 0, fmt.Errorf("merge delete %s window %d: %w", projection.Name, w.Index, err)
		}
		if _, err := p.db.ExecContext(ctx, fmt.Sprintf("INSERT INTO %s SELECT * FROM %s", staging, delta)); err != nil {
			return 0, fmt.Errorf("merge insert %s window %d: %w", projection.Name, w.Index, err)
		}
		if w.Index%50 == 0 || w.Index == len(windows)-1 {
			emit(out, Event{EventType: "component.ledger_window_completed", ComponentID: p.cfg.Component(), RunID: p.cfg.RunID(), Network: p.cfg.Network, ChunkStart: w.Start, ChunkEnd: w.End, ProjectionName: projection.Name, TargetTable: projection.TargetTable, Phase: "staging_merge", Status: "running", Metadata: map[string]interface{}{"window_index": w.Index, "window_count": len(windows)}})
		}
	}
	_, _ = p.db.ExecContext(ctx, "DROP TABLE IF EXISTS "+keys)
	_, _ = p.db.ExecContext(ctx, "DROP TABLE IF EXISTS "+delta)
	emit(out, Event{EventType: "component.staging_completed", ComponentID: p.cfg.Component(), RunID: p.cfg.RunID(), Network: p.cfg.Network, ChunkStart: chunk.Start, ChunkEnd: chunk.End, ProjectionName: projection.Name, TargetTable: projection.TargetTable, Phase: "staging_merge", Status: "completed", Metadata: map[string]interface{}{"mode": "ledger_window_merge", "window_count": len(windows), "as_of_ledger": chunk.End}})
	if _, err := p.verifyProjectionTable(ctx, projection, staging); err != nil {
		return 0, err
	}
	if err := p.publishProjection(ctx, out, chunk, projection, staging); err != nil {
		return 0, err
	}
	rowCount, err := p.verifyProjection(ctx, projection)
	if err != nil {
		return 0, err
	}
	if err := p.markManifest(ctx, chunk.Start, chunk.End, projection, "completed", "", rowCount); err != nil {
		return 0, err
	}
	_, _ = p.db.ExecContext(ctx, "DROP TABLE IF EXISTS "+staging)
	return rowCount, nil
}

func (p *Projector) ensureTargetTable(ctx context.Context, projection CurrentProjection) error {
	stmt := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s AS SELECT * FROM (%s) seed WHERE false", p.table(projection.TargetTable), projection.SelectSQL(p, p.cfg.Start, p.cfg.Start-1))
	if _, err := p.db.ExecContext(ctx, stmt); err != nil {
		return fmt.Errorf("create %s: %w", projection.Name, err)
	}
	if err := p.ensureTargetColumns(ctx, projection); err != nil {
		return err
	}
	return nil
}

func (p *Projector) ensureTargetColumns(ctx context.Context, projection CurrentProjection) error {
	target := p.table(projection.TargetTable)
	if projection.Name == "address_balances_current" {
		columns := []string{
			"network VARCHAR",
			"owner_address VARCHAR",
			"asset_key VARCHAR",
			"asset_type VARCHAR",
			"token_contract_id VARCHAR",
			"asset_code VARCHAR",
			"asset_issuer VARCHAR",
			"symbol VARCHAR",
			"decimals INTEGER",
			"balance_raw VARCHAR",
			"balance_display VARCHAR",
			"balance_source VARCHAR",
			"last_updated_ledger BIGINT",
			"last_updated_at TIMESTAMP",
			"updated_at TIMESTAMP",
		}
		for _, column := range columns {
			if _, err := p.db.ExecContext(ctx, fmt.Sprintf("ALTER TABLE %s ADD COLUMN IF NOT EXISTS %s", target, column)); err != nil {
				return fmt.Errorf("migrate %s add column %s: %w", projection.Name, column, err)
			}
		}
		return nil
	}
	if projection.Name == "contract_data_current" {
		if _, err := p.db.ExecContext(ctx, fmt.Sprintf("ALTER TABLE %s ADD COLUMN IF NOT EXISTS network VARCHAR", target)); err != nil {
			return fmt.Errorf("migrate %s add network: %w", projection.Name, err)
		}
		return nil
	}
	if projection.Name != "ttl_current" {
		return nil
	}
	columns := []string{
		"network VARCHAR",
		"key_hash VARCHAR",
		"live_until_ledger_seq BIGINT",
		"ttl_remaining INTEGER",
		"expired BOOLEAN",
		"last_modified_ledger BIGINT",
		"ledger_sequence BIGINT",
		"closed_at TIMESTAMP",
		"created_at TIMESTAMP",
		"ledger_range BIGINT",
		"updated_at TIMESTAMP",
	}
	for _, column := range columns {
		if _, err := p.db.ExecContext(ctx, fmt.Sprintf("ALTER TABLE %s ADD COLUMN IF NOT EXISTS %s", target, column)); err != nil {
			return fmt.Errorf("migrate %s add column %s: %w", projection.Name, column, err)
		}
	}
	return nil
}

func (p *Projector) verifyProjection(ctx context.Context, projection CurrentProjection) (int64, error) {
	return p.verifyProjectionTable(ctx, projection, p.table(projection.TargetTable))
}

func (p *Projector) verifyProjectionTable(ctx context.Context, projection CurrentProjection, table string) (int64, error) {
	var rowCount int64
	if err := p.db.QueryRowContext(ctx, fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE network = %s", table, q(p.cfg.Network))).Scan(&rowCount); err != nil {
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
	) SELECT COUNT(*) FROM keyed`, strings.Join(parts, ", "), table, q(p.cfg.Network), strings.Join(parts, ", "))
	if err := p.db.QueryRowContext(ctx, dupSQL).Scan(&duplicates); err != nil {
		return 0, fmt.Errorf("duplicate-key verification %s: %w", projection.Name, err)
	}
	if duplicates != 0 {
		return 0, fmt.Errorf("duplicate-key verification failed for %s: %d duplicate key groups", projection.Name, duplicates)
	}
	var maxLedger sql.NullInt64
	maxSQL := fmt.Sprintf("SELECT MAX(%s) FROM %s WHERE network = %s", ident(projection.MaxLedgerCol), table, q(p.cfg.Network))
	if err := p.db.QueryRowContext(ctx, maxSQL).Scan(&maxLedger); err != nil {
		return 0, fmt.Errorf("max-ledger verification %s: %w", projection.Name, err)
	}
	if maxLedger.Valid && maxLedger.Int64 > p.cfg.End {
		return 0, fmt.Errorf("max-ledger verification failed for %s: max %d > end ledger %d", projection.Name, maxLedger.Int64, p.cfg.End)
	}
	return rowCount, nil
}

func (p *Projector) sourceFilter(projection CurrentProjection, start int64, end int64) string {
	// Per-window ledger RANGE (not cumulative <= end). The range is pushdown-able to
	// DuckLake/Parquet, so each window reads only its slice — bounded memory + I/O, and each
	// source row is read once across all windows (O(N), not the old O(N^2) cumulative rescan).
	parts := []string{
		fmt.Sprintf("network = %s", q(p.cfg.Network)),
		fmt.Sprintf("%s >= %d", ident(projection.SourceLedgerCol), start),
		fmt.Sprintf("%s <= %d", ident(projection.SourceLedgerCol), end),
	}
	if projection.SourceFilter != "" {
		parts = append(parts, fmt.Sprintf("(%s)", projection.SourceFilter))
	}
	return strings.Join(parts, " AND ")
}

func (p *Projector) sourceLedgerFilter(projection CurrentProjection, start int64, end int64) string {
	parts := []string{
		fmt.Sprintf("%s >= %d", ident(projection.SourceLedgerCol), start),
		fmt.Sprintf("%s <= %d", ident(projection.SourceLedgerCol), end),
	}
	if projection.SourceFilter != "" {
		parts = append(parts, fmt.Sprintf("(%s)", projection.SourceFilter))
	}
	return strings.Join(parts, " AND ")
}

func (p *Projector) deleteSeenKeysSQL(staging, keys string, projection CurrentProjection) string {
	parts := make([]string, 0, len(projection.KeyExprs))
	for _, key := range projection.KeyExprs {
		col := ident(key)
		parts = append(parts, fmt.Sprintf("COALESCE(CAST(s.%s AS VARCHAR), '') = COALESCE(CAST(k.%s AS VARCHAR), '')", col, col))
	}
	return fmt.Sprintf("DELETE FROM %s AS s WHERE EXISTS (SELECT 1 FROM %s AS k WHERE %s)", staging, keys, strings.Join(parts, " AND "))
}

// publishProjection writes the locally-computed staging table into the DuckLake
// target in N hash-bucket transactions instead of one giant DELETE+INSERT. Each
// bucket is idempotent (DELETE then INSERT for that bucket) and checkpointed, so
// an external kill costs one bucket of work rather than the whole projection.
func (p *Projector) publishProjection(ctx context.Context, out io.Writer, chunk Chunk, projection CurrentProjection, staging string) error {
	n := p.cfg.PublishBuckets
	if n < 1 {
		n = 1
	}
	target := p.table(projection.TargetTable)
	bexpr := bucketExpr(projection, n)
	for i := 0; i < n; i++ {
		if p.cfg.Resume {
			done, err := p.bucketComplete(ctx, chunk, projection.Name, i, n)
			if err != nil {
				return err
			}
			if done {
				emit(out, Event{EventType: "component.publish_bucket_skipped", ComponentID: p.cfg.Component(), RunID: p.cfg.RunID(), Network: p.cfg.Network, ChunkStart: chunk.Start, ChunkEnd: chunk.End, ProjectionName: projection.Name, TargetTable: projection.TargetTable, Phase: "publish", Status: "completed", Metadata: map[string]interface{}{"bucket": i, "bucket_count": n}})
				continue
			}
		}
		tx, err := p.db.BeginTx(ctx, nil)
		if err != nil {
			return fmt.Errorf("begin publish %s bucket %d: %w", projection.Name, i, err)
		}
		if _, err := tx.ExecContext(ctx, fmt.Sprintf("DELETE FROM %s WHERE (network = %s OR network IS NULL) AND (%s) = %d", target, q(p.cfg.Network), bexpr, i)); err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("publish delete %s bucket %d: %w", projection.Name, i, err)
		}
		if _, err := tx.ExecContext(ctx, fmt.Sprintf("INSERT INTO %s BY NAME SELECT * FROM %s WHERE (%s) = %d", target, staging, bexpr, i)); err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("publish insert %s bucket %d: %w", projection.Name, i, err)
		}
		if err := tx.Commit(); err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("publish commit %s bucket %d: %w", projection.Name, i, err)
		}
		if err := p.markBucketComplete(ctx, chunk, projection.Name, i); err != nil {
			return err
		}
		emit(out, Event{EventType: "component.publish_bucket_completed", ComponentID: p.cfg.Component(), RunID: p.cfg.RunID(), Network: p.cfg.Network, ChunkStart: chunk.Start, ChunkEnd: chunk.End, ProjectionName: projection.Name, TargetTable: projection.TargetTable, Phase: "publish", Status: "completed", Metadata: map[string]interface{}{"bucket": i, "bucket_count": n}})
	}
	return nil
}

// bucketExpr is a deterministic hash of the projection's key columns into [0, n).
// It is evaluated identically on the target (for DELETE) and staging (for INSERT)
// so the two sides of a bucket's publish always refer to the same logical rows.
func bucketExpr(projection CurrentProjection, n int) string {
	parts := make([]string, 0, len(projection.KeyExprs))
	for _, key := range projection.KeyExprs {
		parts = append(parts, fmt.Sprintf("COALESCE(CAST(%s AS VARCHAR), '')", ident(key)))
	}
	return fmt.Sprintf("CAST(hash(concat_ws('|', %s)) %% %d AS INTEGER)", strings.Join(parts, ", "), n)
}

func (p *Projector) bucketComplete(ctx context.Context, chunk Chunk, projection string, bucket int, bucketCount int) (bool, error) {
	var count int64
	query := fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE run_id=%s AND component_id=%s AND network=%s AND start_ledger=%d AND end_ledger=%d AND projection_name=%s AND bucket=%d AND bucket_count=%d AND status='completed'",
		p.table("silver_current_projector_publish_manifest"), q(p.cfg.RunID()), q(p.cfg.Component()), q(p.cfg.Network), chunk.Start, chunk.End, q(projection), bucket, bucketCount)
	if err := p.db.QueryRowContext(ctx, query).Scan(&count); err != nil {
		return false, err
	}
	return count > 0, nil
}

func (p *Projector) markBucketComplete(ctx context.Context, chunk Chunk, projection string, bucket int) error {
	n := p.cfg.PublishBuckets
	if n < 1 {
		n = 1
	}
	table := p.table("silver_current_projector_publish_manifest")
	if _, err := p.db.ExecContext(ctx, fmt.Sprintf("DELETE FROM %s WHERE run_id=%s AND component_id=%s AND network=%s AND start_ledger=%d AND end_ledger=%d AND projection_name=%s AND bucket=%d AND bucket_count=%d",
		table, q(p.cfg.RunID()), q(p.cfg.Component()), q(p.cfg.Network), chunk.Start, chunk.End, q(projection), bucket, n)); err != nil {
		return err
	}
	_, err := p.db.ExecContext(ctx, fmt.Sprintf(`INSERT INTO %s (
		run_id,
		component_id,
		projection_name,
		network,
		start_ledger,
		end_ledger,
		bucket,
		bucket_count,
		status,
		updated_at
	) VALUES (%s, %s, %s, %s, %d, %d, %d, %d, 'completed', current_timestamp)`,
		table, q(p.cfg.RunID()), q(p.cfg.Component()), q(projection), q(p.cfg.Network), chunk.Start, chunk.End, bucket, n))
	return err
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
		projection_version VARCHAR,
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
	if _, err := p.db.ExecContext(ctx, fmt.Sprintf("ALTER TABLE %s ADD COLUMN IF NOT EXISTS projection_version VARCHAR", p.table("silver_current_projector_manifest"))); err != nil {
		return fmt.Errorf("migrate manifest projection_version: %w", err)
	}
	pubStmt := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
		run_id VARCHAR,
		component_id VARCHAR,
		projection_name VARCHAR,
		network VARCHAR,
		start_ledger BIGINT,
		end_ledger BIGINT,
		bucket INTEGER,
		bucket_count INTEGER,
		status VARCHAR,
		updated_at TIMESTAMP
	)`, p.table("silver_current_projector_publish_manifest"))
	if _, err := p.db.ExecContext(ctx, pubStmt); err != nil {
		return fmt.Errorf("create publish manifest: %w", err)
	}
	if _, err := p.db.ExecContext(ctx, fmt.Sprintf("ALTER TABLE %s ADD COLUMN IF NOT EXISTS bucket_count INTEGER", p.table("silver_current_projector_publish_manifest"))); err != nil {
		return fmt.Errorf("migrate publish manifest bucket_count: %w", err)
	}
	return nil
}

func (p *Projector) markManifest(ctx context.Context, start, end int64, projection CurrentProjection, status, message string, rows int64) error {
	if _, err := p.db.ExecContext(ctx, fmt.Sprintf("DELETE FROM %s WHERE run_id=%s AND component_id=%s AND network=%s AND start_ledger=%d AND end_ledger=%d AND projection_name=%s AND COALESCE(projection_version, '')=%s", p.table("silver_current_projector_manifest"), q(p.cfg.RunID()), q(p.cfg.Component()), q(p.cfg.Network), start, end, q(projection.Name), q(projection.ManifestVersion))); err != nil {
		return err
	}
	completed := "NULL"
	if status == "completed" || status == "failed" {
		completed = "current_timestamp"
	}
	stmt := fmt.Sprintf(`INSERT INTO %s (
		run_id, component_id, projection_name, projection_version, network,
		start_ledger, end_ledger, status, row_count, error_message, started_at, completed_at
	) VALUES (%s, %s, %s, %s, %s, %d, %d, %s, %d, %s, current_timestamp, %s)`,
		p.table("silver_current_projector_manifest"),
		q(p.cfg.RunID()), q(p.cfg.Component()), q(projection.Name), q(projection.ManifestVersion), q(p.cfg.Network), start, end, q(status), rows, q(message), completed)
	if _, err := p.db.ExecContext(ctx, stmt); err != nil {
		return err
	}
	return p.jsonl.Mark(ctx, ManifestRecord{RunID: p.cfg.RunID(), ComponentID: p.cfg.Component(), ProjectionName: projection.Name, ProjectionVersion: projection.ManifestVersion, Network: p.cfg.Network, StartLedger: start, EndLedger: end, Status: status, ErrorMessage: message, RowCount: rows, UpdatedAt: time.Now().UTC().Format(time.RFC3339Nano)})
}

func (p *Projector) chunkComplete(ctx context.Context, start, end int64) (bool, error) {
	for _, projection := range executableCurrentProjections() {
		done, err := p.projectionComplete(ctx, start, end, projection)
		if err != nil {
			return false, err
		}
		if !done {
			return false, nil
		}
	}
	return true, nil
}

// projectionComplete reports whether this projection is already published for the same
// (component, network, ledger range). It intentionally does NOT filter on run_id: each
// dispatch gets a fresh run_id (the wrapper stamps a timestamp), so resuming across
// re-dispatches — e.g. to bump --chunk-size without redoing finished projections — must
// match on the durable identity of the work, not the run. ProjectionVersion invalidates
// completed records when a projection's source semantics change. Unversioned projections
// retain compatibility with their legacy manifest rows.
func (p *Projector) projectionComplete(ctx context.Context, start, end int64, projection CurrentProjection) (bool, error) {
	var count int64
	query := fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE component_id=%s AND network=%s AND start_ledger=%d AND end_ledger=%d AND projection_name=%s AND COALESCE(projection_version, '')=%s AND status='completed'",
		p.table("silver_current_projector_manifest"), q(p.cfg.Component()), q(p.cfg.Network), start, end, q(projection.Name), q(projection.ManifestVersion))
	if err := p.db.QueryRowContext(ctx, query).Scan(&count); err != nil {
		return false, err
	}
	return count > 0, nil
}

func selectAccountsCurrent(p *Projector, start, end int64) string {
	return fmt.Sprintf(`SELECT network, account_id, balance, sequence_number, num_subentries,
		num_sponsoring, num_sponsored, home_domain, master_weight, low_threshold, med_threshold,
		high_threshold, flags, auth_required, auth_revocable, auth_immutable, auth_clawback_enabled,
		signers, sponsor_account, created_at, COALESCE(updated_at, closed_at, current_timestamp) AS updated_at,
		ledger_sequence AS last_modified_ledger, sequence_ledger, sequence_time, ledger_range, era_id, version_label
		FROM (
			SELECT r.* FROM (
				SELECT arg_max(s, ROW(s.ledger_sequence, COALESCE(s.updated_at, s.closed_at))) AS r
				FROM %s s WHERE %s GROUP BY account_id
			)
		)`, p.table("accounts_snapshot"), p.sourceFilter(CurrentProjection{SourceLedgerCol: "ledger_sequence"}, start, end))
}

func selectAccountsCurrentKeys(p *Projector, start, end int64) string {
	return fmt.Sprintf(`SELECT account_id FROM (
			SELECT account_id, row_number() OVER (PARTITION BY account_id ORDER BY ledger_sequence DESC, COALESCE(updated_at, closed_at) DESC NULLS LAST) AS rn
			FROM %s
			WHERE %s
		) WHERE rn = 1`, p.table("accounts_snapshot"), p.sourceFilter(CurrentProjection{SourceLedgerCol: "ledger_sequence"}, start, end))
}

func selectTrustlinesCurrent(p *Projector, start, end int64) string {
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
			SELECT r.* FROM (
				SELECT arg_max(s, ROW(s.ledger_sequence, s.created_at)) AS r
				FROM %s s WHERE %s
				GROUP BY account_id, asset_type, COALESCE(asset_code, ''), COALESCE(asset_issuer, '')
			)
		)`, p.table("trustlines_snapshot"), p.sourceFilter(CurrentProjection{SourceLedgerCol: "ledger_sequence"}, start, end))
}

func selectTrustlinesCurrentKeys(p *Projector, start, end int64) string {
	return fmt.Sprintf(`SELECT account_id, asset_type, asset_code, asset_issuer, NULL::VARCHAR AS liquidity_pool_id FROM (
			SELECT account_id, asset_type, asset_code, asset_issuer, row_number() OVER (
				PARTITION BY account_id, asset_type, COALESCE(asset_code, ''), COALESCE(asset_issuer, '')
				ORDER BY ledger_sequence DESC, created_at DESC NULLS LAST
			) AS rn
			FROM %s
			WHERE %s
		) WHERE rn = 1`, p.table("trustlines_snapshot"), p.sourceFilter(CurrentProjection{SourceLedgerCol: "ledger_sequence"}, start, end))
}

func selectOffersCurrent(p *Projector, start, end int64) string {
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
			SELECT r.* FROM (
				SELECT arg_max(s, ROW(s.ledger_sequence, s.created_at)) AS r
				FROM %s s WHERE %s GROUP BY offer_id
			)
		)`, p.table("offers_snapshot"), p.sourceFilter(CurrentProjection{SourceLedgerCol: "ledger_sequence"}, start, end))
}

func selectOffersCurrentKeys(p *Projector, start, end int64) string {
	return fmt.Sprintf(`SELECT offer_id FROM (
			SELECT offer_id, row_number() OVER (PARTITION BY offer_id ORDER BY ledger_sequence DESC, created_at DESC NULLS LAST) AS rn
			FROM %s
			WHERE %s
		) WHERE rn = 1`, p.table("offers_snapshot"), p.sourceFilter(CurrentProjection{SourceLedgerCol: "ledger_sequence"}, start, end))
}

func selectContractDataCurrent(p *Projector, start, end int64) string {
	return fmt.Sprintf(`SELECT %s AS network, contract_id, ledger_key_hash AS key_hash, contract_durability AS durability,
		asset_type, asset_code, asset_issuer, contract_data_xdr AS data_value,
		last_modified_ledger, ledger_sequence, closed_at, closed_at AS created_at, ledger_range,
		current_timestamp AS updated_at
		FROM (
			SELECT r.* FROM (
				SELECT arg_max(s, ROW(s.ledger_sequence, s.last_modified_ledger, s.closed_at)) AS r
				FROM %s s WHERE %s GROUP BY contract_id, ledger_key_hash
			)
		) WHERE COALESCE(deleted, false) = false`, q(p.cfg.Network), p.bronzeTable("contract_data_snapshot_v1"), p.sourceLedgerFilter(CurrentProjection{SourceLedgerCol: "ledger_sequence"}, start, end))
}

func selectContractDataCurrentKeys(p *Projector, start, end int64) string {
	return fmt.Sprintf(`SELECT contract_id, ledger_key_hash AS key_hash FROM (
			SELECT contract_id, ledger_key_hash, row_number() OVER (
				PARTITION BY contract_id, ledger_key_hash
				ORDER BY ledger_sequence DESC, last_modified_ledger DESC NULLS LAST, closed_at DESC NULLS LAST
			) AS rn
			FROM %s
			WHERE %s
		) WHERE rn = 1`, p.bronzeTable("contract_data_snapshot_v1"), p.sourceLedgerFilter(CurrentProjection{SourceLedgerCol: "ledger_sequence"}, start, end))
}

func selectTTLCurrent(p *Projector, start, end int64) string {
	return fmt.Sprintf(`SELECT %s AS network, key_hash, live_until_ledger_seq,
		TRY_CAST(live_until_ledger_seq - %d AS INTEGER) AS ttl_remaining,
		(live_until_ledger_seq < %d) AS expired,
		last_modified_ledger, ledger_sequence, closed_at, created_at, ledger_range,
		current_timestamp AS updated_at
		FROM (
			SELECT r.* FROM (
				SELECT arg_max(s, ROW(s.ledger_sequence, s.last_modified_ledger, s.closed_at)) AS r
				FROM %s s WHERE %s GROUP BY key_hash
			)
		) WHERE COALESCE(deleted, false) = false`, q(p.cfg.Network), end, end, p.bronzeTable("ttl_snapshot_v1"), p.sourceLedgerFilter(CurrentProjection{SourceLedgerCol: "ledger_sequence"}, start, end))
}

func selectTTLCurrentKeys(p *Projector, start, end int64) string {
	return fmt.Sprintf(`SELECT key_hash FROM (
			SELECT key_hash, row_number() OVER (
				PARTITION BY key_hash
				ORDER BY ledger_sequence DESC, last_modified_ledger DESC NULLS LAST, closed_at DESC NULLS LAST
			) AS rn
			FROM %s
			WHERE %s
		) WHERE rn = 1`, p.bronzeTable("ttl_snapshot_v1"), p.sourceLedgerFilter(CurrentProjection{SourceLedgerCol: "ledger_sequence"}, start, end))
}

func selectNativeBalancesCurrent(p *Projector, start, end int64) string {
	return fmt.Sprintf(`SELECT network, address AS account_id, TRY_CAST(balance AS BIGINT) AS balance,
		0::BIGINT AS buying_liabilities, 0::BIGINT AS selling_liabilities,
		NULL::INTEGER AS num_subentries, NULL::INTEGER AS num_sponsoring, NULL::INTEGER AS num_sponsored,
		NULL::BIGINT AS sequence_number, ledger_sequence AS last_modified_ledger, ledger_sequence,
		ledger_range, current_timestamp AS inserted_at, current_timestamp AS updated_at
		FROM (
			SELECT r.* FROM (
				SELECT arg_max(s, ROW(s.ledger_sequence, s.ledger_closed_at)) AS r
				FROM %s s WHERE %s GROUP BY address
			)
		) WHERE COALESCE(deleted, false) = false`, p.table("balance_changes"), p.sourceFilter(CurrentProjection{SourceLedgerCol: "ledger_sequence", SourceFilter: "(asset_type = 'native' OR asset_code = 'XLM')"}, start, end))
}

func selectNativeBalancesCurrentKeys(p *Projector, start, end int64) string {
	return fmt.Sprintf(`SELECT address AS account_id FROM (
			SELECT address, row_number() OVER (
				PARTITION BY address
				ORDER BY ledger_sequence DESC, ledger_closed_at DESC NULLS LAST
			) AS rn
			FROM %s
			WHERE %s
		) WHERE rn = 1`, p.table("balance_changes"), p.sourceFilter(CurrentProjection{SourceLedgerCol: "ledger_sequence", SourceFilter: "(asset_type = 'native' OR asset_code = 'XLM')"}, start, end))
}

func addressBalanceAssetKeyExpr() string {
	return `CASE
		WHEN asset_type = 'native' OR asset_code = 'XLM' THEN 'native:XLM'
		WHEN asset_issuer IS NULL OR asset_issuer = '' THEN concat(COALESCE(asset_type, 'asset'), ':', COALESCE(asset_code, ''))
		ELSE concat(COALESCE(asset_type, 'asset'), ':', COALESCE(asset_code, ''), ':', asset_issuer)
	END`
}

func contractBalanceDisplayExpr(balanceRaw, decimals string) string {
	decimals = fmt.Sprintf("COALESCE(%s, 7)", decimals)
	raw := fmt.Sprintf("CAST(%s AS VARCHAR)", balanceRaw)
	return fmt.Sprintf(`CASE
		WHEN %[2]s <= 0 THEN %[1]s
		WHEN length(%[1]s) <= %[2]s THEN '0.' || repeat('0', %[2]s - length(%[1]s)) || %[1]s
		ELSE left(%[1]s, length(%[1]s) - %[2]s) || '.' || right(%[1]s, %[2]s)
	END`, raw, decimals)
}

func contractBalancePositiveExpr(balanceRaw string) string {
	return fmt.Sprintf("regexp_matches(CAST(%s AS VARCHAR), '^[0-9]*[1-9][0-9]*$')", balanceRaw)
}

func selectAddressBalancesCurrent(p *Projector, start, end int64) string {
	assetKey := addressBalanceAssetKeyExpr()
	balanceRaw := `CASE
		WHEN asset_type = 'native' OR asset_code = 'XLM' THEN TRY_CAST(balance AS BIGINT)
		ELSE TRY_CAST(ROUND(TRY_CAST(balance AS DOUBLE) * 10000000) AS BIGINT)
	END`
	balanceDisplay := `CASE
		WHEN asset_type = 'native' OR asset_code = 'XLM' THEN TRY_CAST(balance AS DECIMAL(38,7)) / 10000000
		ELSE TRY_CAST(balance AS DECIMAL(38,7))
	END`
	contractBalanceDisplay := contractBalanceDisplayExpr("balance_raw", "decimals")
	contractBalancePositive := contractBalancePositiveExpr("balance_raw")
	changeSources := []string{fmt.Sprintf(`SELECT network, owner_address, owner_type, asset_key, asset_type, token_contract_id,
			asset_code, asset_issuer, symbol, decimals, balance_raw,
			%s AS balance_display,
			balance_source, ledger_sequence, ledger_closed_at, deleted
		FROM %s WHERE %s`, contractBalanceDisplay,
		p.table("contract_balance_changes"), p.sourceFilter(CurrentProjection{SourceLedgerCol: "ledger_sequence"}, start, end))}
	if p.includeBalanceChanges() {
		legacySource := fmt.Sprintf(`SELECT network, address AS owner_address, 'account' AS owner_type, %s AS asset_key, asset_type,
			NULL::VARCHAR AS token_contract_id, asset_code, asset_issuer, asset_code AS symbol, 7::INTEGER AS decimals,
			CAST(%s AS VARCHAR) AS balance_raw, CAST(%s AS VARCHAR) AS balance_display,
			'silver.balance_changes' AS balance_source, ledger_sequence, ledger_closed_at, deleted
		FROM %s WHERE %s`, assetKey, balanceRaw, balanceDisplay,
			p.table("balance_changes"), p.sourceFilter(CurrentProjection{SourceLedgerCol: "ledger_sequence"}, start, end))
		changeSources = append([]string{legacySource}, changeSources...)
	}
	return fmt.Sprintf(`WITH changes AS (
			%s
		), ranked AS (
			SELECT *, row_number() OVER (
				PARTITION BY owner_address, asset_key
				ORDER BY ledger_sequence DESC, ledger_closed_at DESC NULLS LAST
			) AS rn
			FROM changes
		)
		SELECT network, owner_address, asset_key, asset_type, token_contract_id,
			asset_code, asset_issuer, symbol, decimals, balance_raw, balance_display,
			balance_source, ledger_sequence AS last_updated_ledger,
			ledger_closed_at AS last_updated_at, current_timestamp AS updated_at
		FROM ranked
		WHERE rn = 1
		  AND COALESCE(deleted, false) = false
		  AND (owner_type <> 'contract' OR %s)`,
		strings.Join(changeSources, "\nUNION ALL\n"), contractBalancePositive)
}

func selectAddressBalancesCurrentKeys(p *Projector, start, end int64) string {
	assetKey := addressBalanceAssetKeyExpr()
	keySources := []string{fmt.Sprintf(`SELECT owner_address, asset_key, ledger_sequence, ledger_closed_at
		FROM %s WHERE %s`, p.table("contract_balance_changes"), p.sourceFilter(CurrentProjection{SourceLedgerCol: "ledger_sequence"}, start, end))}
	if p.includeBalanceChanges() {
		legacySource := fmt.Sprintf(`SELECT address AS owner_address, %s AS asset_key, ledger_sequence, ledger_closed_at
		FROM %s WHERE %s`, assetKey,
			p.table("balance_changes"), p.sourceFilter(CurrentProjection{SourceLedgerCol: "ledger_sequence"}, start, end))
		keySources = append([]string{legacySource}, keySources...)
	}
	return fmt.Sprintf(`WITH changed_keys AS (
			%s
		)
		SELECT owner_address, asset_key FROM (
			SELECT owner_address, asset_key, row_number() OVER (
				PARTITION BY owner_address, asset_key
				ORDER BY ledger_sequence DESC, ledger_closed_at DESC NULLS LAST
			) AS rn
			FROM changed_keys
		) WHERE rn = 1`, strings.Join(keySources, "\nUNION ALL\n"))
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
	return []Chunk{{Start: c.Start, End: c.End, Index: 0}}
}

func (p *Projector) table(table string) string {
	return fmt.Sprintf("%s.%s", p.schema(), ident(table))
}

func (p *Projector) bronzeTable(table string) string {
	if p.cfg.effectiveBronzeCatalog() == "" {
		return p.table(table)
	}
	return fmt.Sprintf("%s.%s.%s", ident(p.cfg.BronzeAlias), ident(p.cfg.BronzeSchema), ident(table))
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
		if rec.RunID == runID && rec.ComponentID == componentID && rec.Network == network && rec.StartLedger == start && rec.EndLedger == end && rec.Status == "completed" {
			completed[rec.ProjectionName] = true
		}
	}
	for _, p := range executableCurrentProjections() {
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

func startProgressHeartbeat(out io.Writer, base Event, metadata map[string]interface{}) func() {
	started := time.Now()
	done := make(chan struct{})
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		ticker := time.NewTicker(progressInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				progressMetadata := map[string]interface{}{}
				for k, v := range metadata {
					progressMetadata[k] = v
				}
				progressMetadata["elapsed_seconds"] = int64(time.Since(started).Seconds())
				progressMetadata["heartbeat_interval_seconds"] = int64(progressInterval.Seconds())
				ev := base
				ev.EventType = "component.projection_progress"
				ev.Metadata = progressMetadata
				emit(out, ev)
			case <-done:
				return
			}
		}
	}()
	return func() {
		close(done)
		<-stopped
	}
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

// execWindow runs a per-window statement, retrying transient cold-storage IO errors with
// backoff. The window keys/delta CREATEs read source Parquet from B2 and occasionally hit a
// "Transferred a partial file"/HTTP blip; without retry, one blip over a multi-hour projection
// fails the whole run (exit 75). Statements passed here are idempotent (CREATE OR REPLACE).
func (p *Projector) execWindow(ctx context.Context, out io.Writer, sql, projection string, windowIndex int) error {
	const maxAttempts = 6
	var err error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		if _, err = p.db.ExecContext(ctx, sql); err == nil {
			return nil
		}
		if attempt == maxAttempts || !isRetryableIO(err) {
			return err
		}
		emit(out, Event{EventType: "component.retry", ComponentID: p.cfg.Component(), RunID: p.cfg.RunID(), Network: p.cfg.Network, ProjectionName: projection, Phase: "staging_merge", Status: "running", FailureClass: FailureRetryableInfrastructure, Error: err.Error(), Metadata: map[string]interface{}{"window_index": windowIndex, "attempt": attempt, "max_attempts": maxAttempts}})
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Duration(attempt*3) * time.Second): // 3,6,9,12,15s
		}
	}
	return err
}

func isRetryableIO(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	for _, s := range []string{"io error", "transferred a partial file", "http", "connection", "timeout", "reset by peer", "temporarily", "503", "500", "throttl"} {
		if strings.Contains(msg, s) {
			return true
		}
	}
	return false
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

func envInt(k string, fallback int) int {
	v := os.Getenv(k)
	if v == "" {
		return fallback
	}
	n, err := strconv.Atoi(v)
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
