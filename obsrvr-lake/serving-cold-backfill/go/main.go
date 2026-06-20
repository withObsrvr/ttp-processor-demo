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

const componentID = "serving-cold-backfill"

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
	Network       string
	Start         int64
	End           int64
	Chunk         int64
	ChunkStart    int64
	ChunkEnd      int64
	RetentionDays int
	Resume        bool
	Status        bool

	BronzeCatalog  string
	BronzeData     string
	BronzeAlias    string
	BronzeSchema   string
	BronzeMeta     string
	SilverCatalog  string
	SilverData     string
	SilverAlias    string
	SilverSchema   string
	SilverMeta     string
	TargetPostgres string
	ServingSchema  string
	ManifestPath   string
	SummaryPath    string

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
	Name         string `json:"name"`
	TargetTable  string `json:"target_table"`
	Source       string `json:"source"`
	Mode         string `json:"mode"`
	Checkpoint   bool   `json:"checkpoint"`
	Required     bool   `json:"required"`
	InitialClass string `json:"initial_class"`
	Rationale    string `json:"rationale,omitempty"`
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

type Checkpoint struct {
	ProjectionName     string `json:"projection_name"`
	Network            string `json:"network"`
	LastLedgerSequence int64  `json:"last_ledger_sequence"`
	Status             string `json:"status"`
}

type ManifestStore interface {
	Mark(context.Context, ManifestRecord) error
	Completed(context.Context, string, string, int64, int64) (bool, error)
}

type JSONLManifest struct {
	path string
}

type Backfiller struct {
	db    *sql.DB
	cfg   Config
	jsonl ManifestStore
}

type FeedProjection struct {
	Name        string
	TargetTable string
	RangeCol    string
	KeyExprs    []string
	SelectSQL   func(*Backfiller, Chunk) string
}

type CurrentProjection struct {
	Name         string
	TargetTable  string
	MaxLedgerCol string
	KeyExprs     []string
	SelectSQL    func(*Backfiller) string
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
		class := classifyFailure(err)
		emit(os.Stdout, Event{EventType: "component.failed", ComponentID: cfg.Component(), RunID: cfg.RunID(), Network: cfg.Network, FailureClass: class, Error: err.Error(), Recommended: recommendedAction(class)})
		os.Exit(exitCode(class))
	}
}

func parseConfig(args []string) (Config, error) {
	var cfg Config
	fs := flag.NewFlagSet(componentID, flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	fs.StringVar(&cfg.Network, "network", getenv("NETWORK", "mainnet"), "network label")
	fs.Int64Var(&cfg.Start, "start-ledger", envInt64("START_LEDGER", 0), "inclusive start ledger")
	fs.Int64Var(&cfg.End, "end-ledger", envInt64("END_LEDGER", 0), "inclusive end ledger")
	fs.Int64Var(&cfg.Chunk, "chunk-size", envInt64("CHUNK_SIZE", 100000), "ledgers per chunk")
	fs.Int64Var(&cfg.ChunkStart, "chunk-start", envInt64("CHUNK_START", 0), "optional assigned inclusive chunk start")
	fs.Int64Var(&cfg.ChunkEnd, "chunk-end", envInt64("CHUNK_END", 0), "optional assigned inclusive chunk end")
	fs.IntVar(&cfg.RetentionDays, "retention-days", envInt("RETENTION_DAYS", 30), "recent serving retention window")
	fs.BoolVar(&cfg.Resume, "resume", getenv("RESUME", "") == "true", "skip completed manifest records")
	fs.BoolVar(&cfg.Status, "status", false, "emit machine-readable component status and exit")
	fs.StringVar(&cfg.BronzeCatalog, "bronze-ducklake-catalog", getenv("BRONZE_DUCKLAKE_CATALOG", ""), "Bronze DuckLake catalog DSN/path")
	fs.StringVar(&cfg.BronzeData, "bronze-data-path", getenv("BRONZE_DATA_PATH", ""), "Bronze DuckLake data path")
	fs.StringVar(&cfg.BronzeAlias, "bronze-catalog-name", getenv("BRONZE_CATALOG_NAME", "bronze_catalog"), "DuckDB alias for Bronze catalog")
	fs.StringVar(&cfg.BronzeSchema, "bronze-schema", getenv("BRONZE_SCHEMA", "bronze"), "Bronze schema name")
	fs.StringVar(&cfg.BronzeMeta, "bronze-metadata-schema", getenv("BRONZE_DUCKLAKE_METADATA_SCHEMA", "bronze_meta"), "Bronze DuckLake metadata schema")
	fs.StringVar(&cfg.SilverCatalog, "silver-ducklake-catalog", getenv("SILVER_DUCKLAKE_CATALOG", ""), "Silver DuckLake catalog DSN/path")
	fs.StringVar(&cfg.SilverData, "silver-data-path", getenv("SILVER_DATA_PATH", ""), "Silver DuckLake data path")
	fs.StringVar(&cfg.SilverAlias, "silver-catalog-name", getenv("SILVER_CATALOG_NAME", "silver_catalog"), "DuckDB alias for Silver catalog")
	fs.StringVar(&cfg.SilverSchema, "silver-schema", getenv("SILVER_SCHEMA", "silver"), "Silver schema name")
	fs.StringVar(&cfg.SilverMeta, "silver-metadata-schema", getenv("SILVER_DUCKLAKE_METADATA_SCHEMA", "silver_meta"), "Silver DuckLake metadata schema")
	fs.StringVar(&cfg.TargetPostgres, "target-postgres", getenv("TARGET_POSTGRES", ""), "target serving PostgreSQL DSN")
	fs.StringVar(&cfg.ServingSchema, "serving-schema", getenv("SERVING_SCHEMA", "serving"), "target serving schema")
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
	if c.RetentionDays <= 0 {
		return errors.New("--retention-days must be > 0")
	}
	if c.BronzeCatalog == "" || c.BronzeData == "" || c.SilverCatalog == "" || c.SilverData == "" || c.TargetPostgres == "" {
		return errors.New("--bronze-ducklake-catalog, --bronze-data-path, --silver-ducklake-catalog, --silver-data-path, and --target-postgres are required")
	}
	if c.ServingSchema == "" {
		return errors.New("--serving-schema is required")
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
	backfiller, err := NewBackfiller(ctx, cfg)
	if err != nil {
		return err
	}
	defer backfiller.Close()
	return backfiller.Run(ctx, out)
}

func NewBackfiller(ctx context.Context, cfg Config) (*Backfiller, error) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		return nil, err
	}
	b := &Backfiller{db: db, cfg: cfg, jsonl: JSONLManifest{path: cfg.ManifestPath}}
	for _, stmt := range []string{"INSTALL ducklake FROM core_nightly", "LOAD ducklake", "INSTALL httpfs", "LOAD httpfs"} {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			db.Close()
			return nil, fmt.Errorf("%s: %w", stmt, err)
		}
	}
	if err := b.attach(ctx, cfg.BronzeAlias, cfg.BronzeCatalog, cfg.BronzeData, cfg.BronzeMeta); err != nil {
		db.Close()
		return nil, err
	}
	if err := b.attach(ctx, cfg.SilverAlias, cfg.SilverCatalog, cfg.SilverData, cfg.SilverMeta); err != nil {
		db.Close()
		return nil, err
	}
	if err := b.ensureServingSchema(ctx); err != nil {
		db.Close()
		return nil, err
	}
	if err := b.ensureManifest(ctx); err != nil {
		db.Close()
		return nil, err
	}
	return b, nil
}

func NewBackfillerWithDB(db *sql.DB, cfg Config) *Backfiller {
	return &Backfiller{db: db, cfg: cfg, jsonl: JSONLManifest{path: cfg.ManifestPath}}
}

func (b *Backfiller) Close() error {
	return b.db.Close()
}

func (b *Backfiller) attach(ctx context.Context, alias, catalog, data, meta string) error {
	stmt := fmt.Sprintf("ATTACH %s AS %s (DATA_PATH %s, METADATA_SCHEMA %s, AUTOMATIC_MIGRATION TRUE, OVERRIDE_DATA_PATH TRUE)", q(catalog), ident(alias), q(data), q(meta))
	if _, err := b.db.ExecContext(ctx, stmt); err != nil {
		fallback := fmt.Sprintf("ATTACH %s AS %s (TYPE ducklake, DATA_PATH %s, METADATA_SCHEMA %s, AUTOMATIC_MIGRATION TRUE, OVERRIDE_DATA_PATH TRUE)", q(catalog), ident(alias), q(data), q(meta))
		if _, fallbackErr := b.db.ExecContext(ctx, fallback); fallbackErr != nil {
			return fmt.Errorf("attach %s: %w (fallback with TYPE ducklake also failed: %v)", alias, err, fallbackErr)
		}
	}
	return nil
}

func (b *Backfiller) Run(ctx context.Context, out io.Writer) error {
	cfg := b.cfg
	chunks := cfg.PlannedChunks()
	feed := feedProjections()
	current := currentProjections()
	emit(out, Event{EventType: "component.run_started", ComponentID: cfg.Component(), RunID: cfg.RunID(), Network: cfg.Network, Status: "running", FlowctlEndpoint: safeEndpoint(cfg.FlowctlEndpoint), Metadata: map[string]interface{}{"version": Version, "range_start": cfg.Start, "range_end": cfg.End, "chunk_count": len(chunks), "retention_days": cfg.RetentionDays, "capabilities": []string{"feed-cold-backfill", "current-state-backfill", "deterministic-chunk-inputs", "chunk-delete-insert", "current-replace", "resume-manifest", "typed-failures", "checkpoint-handoff"}}})
	for _, chunk := range chunks {
		if cfg.Resume {
			done, err := b.chunkComplete(ctx, chunk)
			if err != nil {
				return err
			}
			if done {
				emit(out, Event{EventType: "component.chunk_skipped", ComponentID: cfg.Component(), RunID: cfg.RunID(), Network: cfg.Network, ChunkStart: chunk.Start, ChunkEnd: chunk.End, Status: "completed"})
				continue
			}
		}
		emit(out, Event{EventType: "component.chunk_started", ComponentID: cfg.Component(), RunID: cfg.RunID(), Network: cfg.Network, ChunkStart: chunk.Start, ChunkEnd: chunk.End, Status: "running"})
		for _, p := range feed {
			emit(out, Event{EventType: "component.projection_started", ComponentID: cfg.Component(), RunID: cfg.RunID(), Network: cfg.Network, ChunkStart: chunk.Start, ChunkEnd: chunk.End, ProjectionName: p.Name, TargetTable: b.servingTable(p.TargetTable), Phase: "chunk_replace", Status: "running"})
			rows, err := b.replaceFeedProjection(ctx, chunk, p)
			if err != nil {
				_ = b.markManifest(ctx, chunk, p.Name, "failed", 0, err.Error())
				emit(out, Event{EventType: "component.failed", ComponentID: cfg.Component(), RunID: cfg.RunID(), Network: cfg.Network, ChunkStart: chunk.Start, ChunkEnd: chunk.End, ProjectionName: p.Name, TargetTable: b.servingTable(p.TargetTable), Phase: "chunk_replace", FailureClass: classifyFailure(err), Error: err.Error(), Recommended: recommendedAction(classifyFailure(err))})
				return err
			}
			emit(out, Event{EventType: "component.projection_completed", ComponentID: cfg.Component(), RunID: cfg.RunID(), Network: cfg.Network, ChunkStart: chunk.Start, ChunkEnd: chunk.End, ProjectionName: p.Name, TargetTable: b.servingTable(p.TargetTable), Phase: "chunk_replace", Status: "completed", Metadata: map[string]interface{}{"row_count": rows, "mode": "chunk_delete_insert"}})
		}
		emit(out, Event{EventType: "component.chunk_completed", ComponentID: cfg.Component(), RunID: cfg.RunID(), Network: cfg.Network, ChunkStart: chunk.Start, ChunkEnd: chunk.End, Status: "completed"})
	}
	currentChunk := Chunk{Start: cfg.Start, End: cfg.End}
	for _, p := range current {
		emit(out, Event{EventType: "component.projection_started", ComponentID: cfg.Component(), RunID: cfg.RunID(), Network: cfg.Network, ChunkStart: currentChunk.Start, ChunkEnd: currentChunk.End, ProjectionName: p.Name, TargetTable: b.servingTable(p.TargetTable), Phase: "current_replace", Status: "running"})
		rows, err := b.replaceCurrentProjection(ctx, currentChunk, p)
		if err != nil {
			_ = b.markManifest(ctx, currentChunk, p.Name, "failed", 0, err.Error())
			emit(out, Event{EventType: "component.failed", ComponentID: cfg.Component(), RunID: cfg.RunID(), Network: cfg.Network, ChunkStart: currentChunk.Start, ChunkEnd: currentChunk.End, ProjectionName: p.Name, TargetTable: b.servingTable(p.TargetTable), Phase: "current_replace", FailureClass: classifyFailure(err), Error: err.Error(), Recommended: recommendedAction(classifyFailure(err))})
			return err
		}
		emit(out, Event{EventType: "component.projection_completed", ComponentID: cfg.Component(), RunID: cfg.RunID(), Network: cfg.Network, ChunkStart: currentChunk.Start, ChunkEnd: currentChunk.End, ProjectionName: p.Name, TargetTable: b.servingTable(p.TargetTable), Phase: "current_replace", Status: "completed", Metadata: map[string]interface{}{"row_count": rows, "mode": "current_replace"}})
	}
	if err := b.verifyEnabledProjections(ctx, chunks, feed, current); err != nil {
		emit(out, Event{EventType: "component.failed", ComponentID: cfg.Component(), RunID: cfg.RunID(), Network: cfg.Network, Phase: "verification", FailureClass: FailureVerification, Error: err.Error(), Recommended: recommendedAction(FailureVerification)})
		return err
	}
	if err := b.writeProjectionCheckpoints(ctx, append(feedProjectionNames(feed), currentProjectionNames(current)...)); err != nil {
		emit(out, Event{EventType: "component.failed", ComponentID: cfg.Component(), RunID: cfg.RunID(), Network: cfg.Network, Phase: "checkpoint_handoff", FailureClass: classifyFailure(err), Error: err.Error(), Recommended: recommendedAction(classifyFailure(err))})
		return err
	}
	checkpoints := checkpointPlan(cfg, requiredProjections(cfg.ServingSchema))
	emit(out, Event{EventType: "component.run_completed", ComponentID: cfg.Component(), RunID: cfg.RunID(), Network: cfg.Network, Status: "completed", Metadata: map[string]interface{}{"projection_count": len(feed) + len(current), "checkpoint_count": len(feed) + len(current), "checkpoint_contract": "enabled projections verified before sv_projection_checkpoints handoff"}})
	return writeSummary(cfg, chunks, requiredProjections(cfg.ServingSchema), checkpoints)
}

func requiredProjections(schema string) []Projection {
	table := func(name string) string { return schema + "." + name }
	return []Projection{
		{Name: "sv_ledger_stats_recent", TargetTable: table("sv_ledger_stats_recent"), Source: "silver.enriched_ledgers", Mode: "recent_range_replace", Checkpoint: true, Required: true, InitialClass: "backfilled_now"},
		{Name: "sv_transactions_recent", TargetTable: table("sv_transactions_recent"), Source: "bronze.transactions_row_v2 + silver enriched data", Mode: "recent_range_replace", Checkpoint: true, Required: true, InitialClass: "backfilled_now"},
		{Name: "sv_operations_recent", TargetTable: table("sv_operations_recent"), Source: "silver.enriched_history_operations", Mode: "recent_range_replace", Checkpoint: true, Required: true, InitialClass: "backfilled_now"},
		{Name: "sv_events_recent", TargetTable: table("sv_events_recent"), Source: "bronze.contract_events_stream_v1 / silver effects", Mode: "recent_range_replace", Checkpoint: false, Required: true, InitialClass: "blocked_source_mapping", Rationale: "required by the broader serving contract, but this binary does not enable the projection yet"},
		{Name: "sv_explorer_events_recent", TargetTable: table("sv_explorer_events_recent"), Source: "bronze.contract_events_stream_v1 + classifier rules", Mode: "recent_range_replace", Checkpoint: false, Required: true, InitialClass: "blocked_source_mapping", Rationale: "required by the broader serving contract, but classifier/source mapping is not enabled yet"},
		{Name: "sv_contract_calls_recent", TargetTable: table("sv_contract_calls_recent"), Source: "silver.contract_invocations_raw", Mode: "recent_range_replace", Checkpoint: true, Required: true, InitialClass: "backfilled_now"},
		{Name: "sv_tx_receipts", TargetTable: table("sv_tx_receipts"), Source: "bronze transactions/operations/effects + silver enriched rows", Mode: "recent_range_replace", Checkpoint: true, Required: true, InitialClass: "backfilled_now"},
		{Name: "sv_accounts_current", TargetTable: table("sv_accounts_current"), Source: "silver.accounts_current", Mode: "current_replace", Checkpoint: true, Required: true, InitialClass: "backfilled_now"},
		{Name: "sv_account_balances_current", TargetTable: table("sv_account_balances_current"), Source: "silver.address_balances_current / silver.native_balances_current", Mode: "current_replace", Checkpoint: true, Required: true, InitialClass: "backfilled_now"},
		{Name: "sv_network_stats_current", TargetTable: table("sv_network_stats_current"), Source: "silver.enriched_ledgers + aggregate counts", Mode: "current_replace", Checkpoint: true, Required: true, InitialClass: "backfilled_now"},
		{Name: "sv_assets_current", TargetTable: table("sv_assets_current"), Source: "sv_account_balances_current aggregates", Mode: "current_replace", Checkpoint: true, Required: true, InitialClass: "backfilled_now"},
		{Name: "sv_asset_stats_current", TargetTable: table("sv_asset_stats_current"), Source: "sv_account_balances_current aggregates", Mode: "current_replace", Checkpoint: true, Required: true, InitialClass: "backfilled_now"},
		{Name: "sv_contracts_current", TargetTable: table("sv_contracts_current"), Source: "silver.contract_metadata + silver.contract_data_current", Mode: "current_replace", Checkpoint: true, Required: true, InitialClass: "backfilled_now"},
		{Name: "sv_contract_stats_current", TargetTable: table("sv_contract_stats_current"), Source: "silver.contract_invocations_raw / contract events", Mode: "current_replace", Checkpoint: true, Required: true, InitialClass: "backfilled_now"},
		{Name: "sv_contract_function_stats_current", TargetTable: table("sv_contract_function_stats_current"), Source: "silver.contract_invocations_raw grouped by contract/function", Mode: "current_replace", Checkpoint: true, Required: true, InitialClass: "backfilled_now"},
	}
}

func classifiedOptionalTables(schema string) []Projection {
	table := func(name string) string { return schema + "." + name }
	return []Projection{
		{Name: "sv_offers_current", TargetTable: table("sv_offers_current"), InitialClass: "out_of_scope", Rationale: "not listed in required first serving backfill release"},
		{Name: "sv_liquidity_pools_current", TargetTable: table("sv_liquidity_pools_current"), InitialClass: "out_of_scope", Rationale: "source mapping and serving contract need separate design"},
		{Name: "sv_trades_recent", TargetTable: table("sv_trades_recent"), InitialClass: "out_of_scope", Rationale: "not listed in required first serving backfill release"},
		{Name: "sv_asset_holders_top", TargetTable: table("sv_asset_holders_top"), InitialClass: "out_of_scope", Rationale: "derived ranking table; not part of first required set"},
		{Name: "sv_search_entities", TargetTable: table("sv_search_entities"), InitialClass: "seeded_or_registry_managed", Rationale: "search index population requires registry/bootstrap policy"},
		{Name: "sv_asset_metadata", TargetTable: table("sv_asset_metadata"), InitialClass: "seeded_or_registry_managed", Rationale: "metadata source of truth is external/registry-managed"},
		{Name: "sv_contract_labels", TargetTable: table("sv_contract_labels"), InitialClass: "seeded_or_registry_managed", Rationale: "labels are registry-managed"},
		{Name: "sv_defi_protocols", TargetTable: table("sv_defi_protocols"), InitialClass: "seeded_or_registry_managed", Rationale: "protocol registry table"},
		{Name: "sv_defi_protocol_contracts", TargetTable: table("sv_defi_protocol_contracts"), InitialClass: "seeded_or_registry_managed", Rationale: "protocol registry table"},
		{Name: "sv_defi_markets_current", TargetTable: table("sv_defi_markets_current"), InitialClass: "out_of_scope", Rationale: "DeFi projections require domain-specific backfill design"},
		{Name: "sv_defi_positions_current", TargetTable: table("sv_defi_positions_current"), InitialClass: "out_of_scope", Rationale: "DeFi projections require domain-specific backfill design"},
		{Name: "sv_defi_position_components_current", TargetTable: table("sv_defi_position_components_current"), InitialClass: "out_of_scope", Rationale: "DeFi projections require domain-specific backfill design"},
		{Name: "sv_defi_user_totals_current", TargetTable: table("sv_defi_user_totals_current"), InitialClass: "out_of_scope", Rationale: "DeFi projections require domain-specific backfill design"},
		{Name: "sv_defi_user_totals_history", TargetTable: table("sv_defi_user_totals_history"), InitialClass: "out_of_scope", Rationale: "DeFi projections require domain-specific backfill design"},
		{Name: "sv_defi_position_history", TargetTable: table("sv_defi_position_history"), InitialClass: "out_of_scope", Rationale: "DeFi projections require domain-specific backfill design"},
		{Name: "sv_defi_prices_current", TargetTable: table("sv_defi_prices_current"), InitialClass: "out_of_scope", Rationale: "pricing source and cadence are not part of cold serving handoff"},
		{Name: "sv_defi_protocol_status", TargetTable: table("sv_defi_protocol_status"), InitialClass: "seeded_or_registry_managed", Rationale: "registry/status table"},
	}
}

func checkpointPlan(cfg Config, projections []Projection) []Checkpoint {
	var checkpoints []Checkpoint
	for _, p := range projections {
		if p.Checkpoint {
			checkpoints = append(checkpoints, Checkpoint{ProjectionName: p.Name, Network: cfg.Network, LastLedgerSequence: cfg.End, Status: "planned_after_all_verify"})
		}
	}
	return checkpoints
}

func feedProjections() []FeedProjection {
	return []FeedProjection{
		{Name: "sv_ledger_stats_recent", TargetTable: "sv_ledger_stats_recent", RangeCol: "ledger_sequence", KeyExprs: []string{"ledger_sequence"}, SelectSQL: selectLedgerStatsRecent},
		{Name: "sv_transactions_recent", TargetTable: "sv_transactions_recent", RangeCol: "ledger_sequence", KeyExprs: []string{"tx_hash"}, SelectSQL: selectTransactionsRecent},
		{Name: "sv_operations_recent", TargetTable: "sv_operations_recent", RangeCol: "ledger_sequence", KeyExprs: []string{"operation_id"}, SelectSQL: selectOperationsRecent},
		{Name: "sv_contract_calls_recent", TargetTable: "sv_contract_calls_recent", RangeCol: "ledger_sequence", KeyExprs: []string{"call_id"}, SelectSQL: selectContractCallsRecent},
		{Name: "sv_tx_receipts", TargetTable: "sv_tx_receipts", RangeCol: "ledger_sequence", KeyExprs: []string{"tx_hash"}, SelectSQL: selectTxReceipts},
	}
}

func currentProjections() []CurrentProjection {
	return []CurrentProjection{
		{Name: "sv_accounts_current", TargetTable: "sv_accounts_current", MaxLedgerCol: "last_modified_ledger", KeyExprs: []string{"account_id"}, SelectSQL: selectAccountsCurrent},
		{Name: "sv_account_balances_current", TargetTable: "sv_account_balances_current", MaxLedgerCol: "last_modified_ledger", KeyExprs: []string{"account_id", "asset_key"}, SelectSQL: selectAccountBalancesCurrent},
		{Name: "sv_network_stats_current", TargetTable: "sv_network_stats_current", MaxLedgerCol: "latest_ledger", KeyExprs: []string{"network"}, SelectSQL: selectNetworkStatsCurrent},
		{Name: "sv_assets_current", TargetTable: "sv_assets_current", KeyExprs: []string{"asset_key"}, SelectSQL: selectAssetsCurrent},
		{Name: "sv_asset_stats_current", TargetTable: "sv_asset_stats_current", KeyExprs: []string{"asset_key"}, SelectSQL: selectAssetStatsCurrent},
		{Name: "sv_contracts_current", TargetTable: "sv_contracts_current", MaxLedgerCol: "deploy_ledger", KeyExprs: []string{"contract_id"}, SelectSQL: selectContractsCurrent},
		{Name: "sv_contract_stats_current", TargetTable: "sv_contract_stats_current", KeyExprs: []string{"contract_id"}, SelectSQL: selectContractStatsCurrent},
		{Name: "sv_contract_function_stats_current", TargetTable: "sv_contract_function_stats_current", KeyExprs: []string{"contract_id", "function_name"}, SelectSQL: selectContractFunctionStatsCurrent},
	}
}

func (b *Backfiller) replaceFeedProjection(ctx context.Context, chunk Chunk, projection FeedProjection) (int64, error) {
	if err := b.markManifest(ctx, chunk, projection.Name, "running", 0, ""); err != nil {
		return 0, err
	}
	if err := b.ensureTargetTable(ctx, projection, chunk); err != nil {
		return 0, err
	}
	tx, err := b.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("begin %s: %w", projection.Name, err)
	}
	deleteSQL := fmt.Sprintf("DELETE FROM %s WHERE %s BETWEEN %d AND %d", b.servingTable(projection.TargetTable), ident(projection.RangeCol), chunk.Start, chunk.End)
	if _, err := tx.ExecContext(ctx, deleteSQL); err != nil {
		_ = tx.Rollback()
		return 0, fmt.Errorf("delete %s %d..%d: %w", projection.Name, chunk.Start, chunk.End, err)
	}
	insertSQL := fmt.Sprintf("INSERT INTO %s %s", b.servingTable(projection.TargetTable), projection.SelectSQL(b, chunk))
	if _, err := tx.ExecContext(ctx, insertSQL); err != nil {
		_ = tx.Rollback()
		return 0, fmt.Errorf("insert %s %d..%d: %w", projection.Name, chunk.Start, chunk.End, err)
	}
	if err := tx.Commit(); err != nil {
		_ = tx.Rollback()
		return 0, fmt.Errorf("commit %s %d..%d: %w", projection.Name, chunk.Start, chunk.End, err)
	}
	rows, err := b.countProjectionRows(ctx, projection, chunk)
	if err != nil {
		return 0, err
	}
	if err := b.markManifest(ctx, chunk, projection.Name, "completed", rows, ""); err != nil {
		return 0, err
	}
	return rows, nil
}

func (b *Backfiller) replaceCurrentProjection(ctx context.Context, chunk Chunk, projection CurrentProjection) (int64, error) {
	if err := b.markManifest(ctx, chunk, projection.Name, "running", 0, ""); err != nil {
		return 0, err
	}
	if err := b.ensureCurrentTargetTable(ctx, projection); err != nil {
		return 0, err
	}
	tx, err := b.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("begin %s: %w", projection.Name, err)
	}
	if _, err := tx.ExecContext(ctx, "DELETE FROM "+b.servingTable(projection.TargetTable)); err != nil {
		_ = tx.Rollback()
		return 0, fmt.Errorf("delete %s: %w", projection.Name, err)
	}
	if _, err := tx.ExecContext(ctx, fmt.Sprintf("INSERT INTO %s %s", b.servingTable(projection.TargetTable), projection.SelectSQL(b))); err != nil {
		_ = tx.Rollback()
		return 0, fmt.Errorf("insert %s: %w", projection.Name, err)
	}
	if err := tx.Commit(); err != nil {
		_ = tx.Rollback()
		return 0, fmt.Errorf("commit %s: %w", projection.Name, err)
	}
	rows, err := b.countCurrentRows(ctx, projection)
	if err != nil {
		return 0, err
	}
	if err := b.markManifest(ctx, chunk, projection.Name, "completed", rows, ""); err != nil {
		return 0, err
	}
	return rows, nil
}

func (b *Backfiller) ensureTargetTable(ctx context.Context, projection FeedProjection, chunk Chunk) error {
	stmt := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s AS SELECT * FROM (%s) seed WHERE false", b.servingTable(projection.TargetTable), projection.SelectSQL(b, chunk))
	if _, err := b.db.ExecContext(ctx, stmt); err != nil {
		return fmt.Errorf("create %s: %w", projection.Name, err)
	}
	return nil
}

func (b *Backfiller) ensureCurrentTargetTable(ctx context.Context, projection CurrentProjection) error {
	stmt := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s AS SELECT * FROM (%s) seed WHERE false", b.servingTable(projection.TargetTable), projection.SelectSQL(b))
	if _, err := b.db.ExecContext(ctx, stmt); err != nil {
		return fmt.Errorf("create %s: %w", projection.Name, err)
	}
	return nil
}

func (b *Backfiller) countProjectionRows(ctx context.Context, projection FeedProjection, chunk Chunk) (int64, error) {
	var rows int64
	query := fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE %s BETWEEN %d AND %d", b.servingTable(projection.TargetTable), ident(projection.RangeCol), chunk.Start, chunk.End)
	if err := b.db.QueryRowContext(ctx, query).Scan(&rows); err != nil {
		return 0, fmt.Errorf("count %s: %w", projection.Name, err)
	}
	return rows, nil
}

func (b *Backfiller) countCurrentRows(ctx context.Context, projection CurrentProjection) (int64, error) {
	var rows int64
	if err := b.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+b.servingTable(projection.TargetTable)).Scan(&rows); err != nil {
		return 0, fmt.Errorf("count %s: %w", projection.Name, err)
	}
	return rows, nil
}

func (b *Backfiller) ensureServingSchema(ctx context.Context) error {
	if _, err := b.db.ExecContext(ctx, "CREATE SCHEMA IF NOT EXISTS "+ident(b.cfg.ServingSchema)); err != nil {
		return fmt.Errorf("create serving schema: %w", err)
	}
	return nil
}

func (b *Backfiller) ensureManifest(ctx context.Context) error {
	stmt := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
		run_id VARCHAR,
		projection_name VARCHAR,
		network VARCHAR,
		start_ledger BIGINT,
		end_ledger BIGINT,
		status VARCHAR,
		row_count BIGINT,
		error_message VARCHAR,
		started_at TIMESTAMP,
		completed_at TIMESTAMP
	)`, b.servingTable("sv_backfill_manifest"))
	if _, err := b.db.ExecContext(ctx, stmt); err != nil {
		return fmt.Errorf("create sv_backfill_manifest: %w", err)
	}
	checkpoints := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
		projection_name VARCHAR,
		network VARCHAR,
		last_ledger_sequence BIGINT,
		last_closed_at TIMESTAMP,
		updated_at TIMESTAMP
	)`, b.servingTable("sv_projection_checkpoints"))
	if _, err := b.db.ExecContext(ctx, checkpoints); err != nil {
		return fmt.Errorf("create sv_projection_checkpoints: %w", err)
	}
	return nil
}

func (b *Backfiller) markManifest(ctx context.Context, chunk Chunk, projection, status string, rows int64, message string) error {
	if _, err := b.db.ExecContext(ctx, fmt.Sprintf("DELETE FROM %s WHERE run_id=%s AND projection_name=%s AND network=%s AND start_ledger=%d AND end_ledger=%d", b.servingTable("sv_backfill_manifest"), q(b.cfg.RunID()), q(projection), q(b.cfg.Network), chunk.Start, chunk.End)); err != nil {
		return err
	}
	completed := "NULL"
	if status == "completed" || status == "failed" {
		completed = "current_timestamp"
	}
	stmt := fmt.Sprintf("INSERT INTO %s VALUES (%s, %s, %s, %d, %d, %s, %d, %s, current_timestamp, %s)",
		b.servingTable("sv_backfill_manifest"), q(b.cfg.RunID()), q(projection), q(b.cfg.Network), chunk.Start, chunk.End, q(status), rows, q(message), completed)
	if _, err := b.db.ExecContext(ctx, stmt); err != nil {
		return err
	}
	return b.jsonl.Mark(ctx, ManifestRecord{RunID: b.cfg.RunID(), ComponentID: b.cfg.Component(), ProjectionName: projection, Network: b.cfg.Network, StartLedger: chunk.Start, EndLedger: chunk.End, Status: status, ErrorMessage: message, RowCount: rows, UpdatedAt: time.Now().UTC().Format(time.RFC3339Nano)})
}

func (b *Backfiller) chunkComplete(ctx context.Context, chunk Chunk) (bool, error) {
	var rows int64
	query := fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE run_id=%s AND network=%s AND start_ledger=%d AND end_ledger=%d AND status='completed'", b.servingTable("sv_backfill_manifest"), q(b.cfg.RunID()), q(b.cfg.Network), chunk.Start, chunk.End)
	if err := b.db.QueryRowContext(ctx, query).Scan(&rows); err != nil {
		return false, err
	}
	return rows == int64(len(feedProjections())), nil
}

func (b *Backfiller) verifyEnabledProjections(ctx context.Context, chunks []Chunk, feed []FeedProjection, current []CurrentProjection) error {
	for _, projection := range feed {
		if err := b.verifyNoDuplicates(ctx, projection.Name, projection.TargetTable, projection.KeyExprs); err != nil {
			return err
		}
		for _, chunk := range chunks {
			var manifestRows int64
			query := fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE run_id=%s AND projection_name=%s AND network=%s AND start_ledger=%d AND end_ledger=%d AND status='completed'", b.servingTable("sv_backfill_manifest"), q(b.cfg.RunID()), q(projection.Name), q(b.cfg.Network), chunk.Start, chunk.End)
			if err := b.db.QueryRowContext(ctx, query).Scan(&manifestRows); err != nil {
				return fmt.Errorf("verify manifest %s %d..%d: %w", projection.Name, chunk.Start, chunk.End, err)
			}
			if manifestRows != 1 {
				return fmt.Errorf("verify manifest failed for %s %d..%d: got %d completed rows", projection.Name, chunk.Start, chunk.End, manifestRows)
			}
		}
	}
	currentChunk := Chunk{Start: b.cfg.Start, End: b.cfg.End}
	for _, projection := range current {
		if err := b.verifyNoDuplicates(ctx, projection.Name, projection.TargetTable, projection.KeyExprs); err != nil {
			return err
		}
		if projection.MaxLedgerCol != "" {
			if err := b.verifyMaxLedger(ctx, projection.Name, projection.TargetTable, projection.MaxLedgerCol); err != nil {
				return err
			}
		}
		var manifestRows int64
		query := fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE run_id=%s AND projection_name=%s AND network=%s AND start_ledger=%d AND end_ledger=%d AND status='completed'", b.servingTable("sv_backfill_manifest"), q(b.cfg.RunID()), q(projection.Name), q(b.cfg.Network), currentChunk.Start, currentChunk.End)
		if err := b.db.QueryRowContext(ctx, query).Scan(&manifestRows); err != nil {
			return fmt.Errorf("verify manifest %s: %w", projection.Name, err)
		}
		if manifestRows != 1 {
			return fmt.Errorf("verify manifest failed for %s: got %d completed rows", projection.Name, manifestRows)
		}
	}
	return nil
}

func (b *Backfiller) verifyNoDuplicates(ctx context.Context, projectionName, table string, keyExprs []string) error {
	if len(keyExprs) == 0 {
		return nil
	}
	parts := make([]string, 0, len(keyExprs))
	for _, key := range keyExprs {
		parts = append(parts, fmt.Sprintf("COALESCE(CAST(%s AS VARCHAR), '')", ident(key)))
	}
	var duplicates int64
	query := fmt.Sprintf(`WITH keyed AS (
		SELECT %s, COUNT(*) AS n
		FROM %s
		GROUP BY %s
		HAVING COUNT(*) > 1
	) SELECT COUNT(*) FROM keyed`, strings.Join(parts, ", "), b.servingTable(table), strings.Join(parts, ", "))
	if err := b.db.QueryRowContext(ctx, query).Scan(&duplicates); err != nil {
		return fmt.Errorf("duplicate verification %s: %w", projectionName, err)
	}
	if duplicates != 0 {
		return fmt.Errorf("duplicate verification failed for %s: %d duplicate key groups", projectionName, duplicates)
	}
	return nil
}

func (b *Backfiller) verifyMaxLedger(ctx context.Context, projectionName, table, maxLedgerCol string) error {
	var maxLedger sql.NullInt64
	query := fmt.Sprintf("SELECT MAX(%s) FROM %s", ident(maxLedgerCol), b.servingTable(table))
	if err := b.db.QueryRowContext(ctx, query).Scan(&maxLedger); err != nil {
		return fmt.Errorf("max-ledger verification %s: %w", projectionName, err)
	}
	if maxLedger.Valid && maxLedger.Int64 > b.cfg.End {
		return fmt.Errorf("max-ledger verification failed for %s: max %d > end ledger %d", projectionName, maxLedger.Int64, b.cfg.End)
	}
	return nil
}

func (b *Backfiller) writeProjectionCheckpoints(ctx context.Context, projectionNames []string) error {
	var endClosedAt sql.NullTime
	_ = b.db.QueryRowContext(ctx, fmt.Sprintf("SELECT MAX(closed_at) FROM %s WHERE ledger_sequence = %d", b.servingTable("sv_ledger_stats_recent"), b.cfg.End)).Scan(&endClosedAt)
	tx, err := b.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin checkpoint handoff: %w", err)
	}
	for _, name := range projectionNames {
		if _, err := tx.ExecContext(ctx, fmt.Sprintf("DELETE FROM %s WHERE projection_name=%s AND network=%s", b.servingTable("sv_projection_checkpoints"), q(name), q(b.cfg.Network))); err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("delete checkpoint %s: %w", name, err)
		}
		closedAt := "NULL"
		if endClosedAt.Valid {
			closedAt = q(endClosedAt.Time.Format("2006-01-02 15:04:05"))
		}
		stmt := fmt.Sprintf("INSERT INTO %s VALUES (%s, %s, %d, %s, current_timestamp)", b.servingTable("sv_projection_checkpoints"), q(name), q(b.cfg.Network), b.cfg.End, closedAt)
		if _, err := tx.ExecContext(ctx, stmt); err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("insert checkpoint %s: %w", name, err)
		}
	}
	if err := tx.Commit(); err != nil {
		_ = tx.Rollback()
		return fmt.Errorf("commit checkpoint handoff: %w", err)
	}
	return nil
}

func feedProjectionNames(feed []FeedProjection) []string {
	names := make([]string, 0, len(feed))
	for _, projection := range feed {
		names = append(names, projection.Name)
	}
	return names
}

func currentProjectionNames(current []CurrentProjection) []string {
	names := make([]string, 0, len(current))
	for _, projection := range current {
		names = append(names, projection.Name)
	}
	return names
}

func (b *Backfiller) retentionPredicate(timeExpr string) string {
	return fmt.Sprintf("%s >= COALESCE((SELECT MAX(ledger_closed_at) - INTERVAL %d DAY FROM %s WHERE network=%s AND ledger_sequence <= %d), %s)",
		timeExpr, b.cfg.RetentionDays, b.silverTable("enriched_ledgers"), q(b.cfg.Network), b.cfg.End, timeExpr)
}

func selectLedgerStatsRecent(b *Backfiller, chunk Chunk) string {
	return fmt.Sprintf(`SELECT ledger_sequence, ledger_closed_at AS closed_at, ledger_hash,
		previous_ledger_hash AS prev_hash, ledger_version AS protocol_version,
		base_fee AS base_fee_stroops, NULL::INTEGER AS max_tx_set_size,
		successful_tx_count, failed_tx_count, operation_count,
		NULL::INTEGER AS soroban_op_count, NULL::BIGINT AS events_emitted,
		NULL::BIGINT AS total_fee_charged_stroops, NULL::BIGINT AS total_cpu_insns,
		NULL::BIGINT AS total_read_bytes, NULL::BIGINT AS total_write_bytes,
		NULL::BIGINT AS total_rent_stroops, NULL::DOUBLE AS close_time_seconds
		FROM %s
		WHERE network=%s AND ledger_sequence BETWEEN %d AND %d AND %s`,
		b.silverTable("enriched_ledgers"), q(b.cfg.Network), chunk.Start, chunk.End, b.retentionPredicate("ledger_closed_at"))
}

func selectTransactionsRecent(b *Backfiller, chunk Chunk) string {
	return fmt.Sprintf(`SELECT transaction_hash AS tx_hash, ledger_sequence, created_at,
		source_account, fee_charged AS fee_charged_stroops, max_fee AS max_fee_stroops,
		successful, operation_count, CASE WHEN soroban_contract_id IS NOT NULL THEN 'contract_call' ELSE 'transaction' END AS tx_type,
		concat('Transaction ', transaction_hash) AS summary_text,
		'{}' AS summary_json, soroban_contract_id AS primary_contract_id,
		NULL::VARCHAR AS primary_asset_key, NULL::BIGINT AS primary_amount_stroops,
		memo_type, memo AS memo_value, account_sequence,
		(soroban_contract_id IS NOT NULL) AS is_soroban,
		soroban_resources_instructions AS cpu_insns, NULL::BIGINT AS mem_bytes,
		soroban_resources_read_bytes AS read_bytes, soroban_resources_write_bytes AS write_bytes,
		current_timestamp AS ingested_at
		FROM %s
		WHERE ledger_sequence BETWEEN %d AND %d AND %s`,
		b.bronzeTable("transactions_row_v2"), chunk.Start, chunk.End, b.retentionPredicate("created_at"))
}

func selectOperationsRecent(b *Backfiller, chunk Chunk) string {
	return fmt.Sprintf(`SELECT concat(transaction_hash, ':', operation_index::VARCHAR) AS operation_id,
		transaction_hash AS tx_hash, ledger_sequence, created_at, operation_index AS op_index,
		type AS type_code, type_string AS type_name, source_account, destination AS destination_account,
		CASE WHEN asset IS NULL OR asset='native' THEN 'native:XLM' ELSE asset END AS asset_key,
		TRY_CAST(amount AS BIGINT) AS amount_stroops, contract_id, function_name,
		tx_successful AS successful, is_payment_op, is_soroban_op, type_string AS summary_text
		FROM %s
		WHERE network=%s AND ledger_sequence BETWEEN %d AND %d AND %s`,
		b.silverTable("enriched_history_operations"), q(b.cfg.Network), chunk.Start, chunk.End, b.retentionPredicate("created_at"))
}

func selectContractCallsRecent(b *Backfiller, chunk Chunk) string {
	return fmt.Sprintf(`SELECT concat(transaction_hash, ':', operation_index::VARCHAR) AS call_id,
		transaction_hash AS tx_hash, ledger_sequence, closed_at AS created_at,
		contract_id, source_account AS caller_account, function_name, successful,
		NULL::BIGINT AS cpu_insns, NULL::BIGINT AS mem_bytes,
		NULL::BIGINT AS read_bytes, NULL::BIGINT AS write_bytes,
		concat('Call ', COALESCE(function_name, 'unknown')) AS summary_text
		FROM %s
		WHERE network=%s AND ledger_sequence BETWEEN %d AND %d AND %s`,
		b.silverTable("contract_invocations_raw"), q(b.cfg.Network), chunk.Start, chunk.End, b.retentionPredicate("closed_at"))
}

func selectTxReceipts(b *Backfiller, chunk Chunk) string {
	return fmt.Sprintf(`WITH ops AS (
			SELECT transaction_hash, COUNT(*) AS op_count,
				list(DISTINCT source_account) FILTER (WHERE source_account IS NOT NULL AND source_account <> '') AS accounts,
				list(DISTINCT contract_id) FILTER (WHERE contract_id IS NOT NULL AND contract_id <> '') AS contracts
			FROM %s
			WHERE network=%s AND ledger_sequence BETWEEN %d AND %d AND %s
			GROUP BY transaction_hash
		)
		SELECT t.transaction_hash AS tx_hash, t.ledger_sequence, t.created_at, t.source_account,
			t.successful, COALESCE(o.op_count, t.operation_count) AS operation_count,
			'{}' AS full_json, '{}' AS semantic_json, '[]' AS effects_json,
			'[]' AS diffs_json, '[]' AS events_json,
			COALESCE(o.contracts, []::VARCHAR[]) AS involved_contracts,
			COALESCE(o.accounts, []::VARCHAR[]) AS involved_accounts,
			t.soroban_contract_id AS primary_contract_id,
			CASE WHEN t.soroban_contract_id IS NOT NULL THEN 'contract_call' ELSE 'transaction' END AS tx_type,
			current_timestamp AS materialized_at, 'v1' AS source_version
		FROM %s t
		LEFT JOIN ops o ON o.transaction_hash = t.transaction_hash
		WHERE t.ledger_sequence BETWEEN %d AND %d AND %s`,
		b.silverTable("enriched_history_operations"), q(b.cfg.Network), chunk.Start, chunk.End, b.retentionPredicate("created_at"), b.bronzeTable("transactions_row_v2"), chunk.Start, chunk.End, b.retentionPredicate("t.created_at"))
}

func selectAccountsCurrent(b *Backfiller) string {
	return fmt.Sprintf(`SELECT account_id, TRY_CAST(balance AS BIGINT) AS balance_stroops,
		sequence_number, num_subentries, created_at, last_modified_ledger, updated_at,
		home_domain, master_weight, low_threshold, med_threshold, high_threshold,
		COALESCE(signers, '[]') AS signers_json, false AS is_smart_account,
		NULL::VARCHAR AS smart_account_type, ledger_range AS first_seen_ledger
		FROM %s WHERE network=%s AND last_modified_ledger <= %d`,
		b.silverTable("accounts_current"), q(b.cfg.Network), b.cfg.End)
}

func selectAccountBalancesCurrent(b *Backfiller) string {
	return fmt.Sprintf(`SELECT owner_address AS account_id, asset_key,
		COALESCE(asset_code, CASE WHEN asset_type='native' THEN 'XLM' ELSE asset_key END) AS asset_code,
		asset_issuer, asset_type, TRY_CAST(balance_raw AS BIGINT) AS balance_stroops,
		TRY_CAST(balance_display AS DECIMAL(38,7)) AS balance_display,
		NULL::BIGINT AS limit_stroops, NULL::BOOLEAN AS is_authorized,
		last_updated_ledger AS last_modified_ledger, updated_at
		FROM %s WHERE network=%s AND last_updated_ledger <= %d`,
		b.silverTable("address_balances_current"), q(b.cfg.Network), b.cfg.End)
}

func selectNetworkStatsCurrent(b *Backfiller) string {
	return fmt.Sprintf(`WITH latest AS (
			SELECT ledger_sequence, closed_at, protocol_version
			FROM %s ORDER BY ledger_sequence DESC LIMIT 1
		), tx AS (
			SELECT COUNT(*) AS total, COUNT(*) FILTER (WHERE NOT successful) AS failed,
				COUNT(DISTINCT source_account) AS active_accounts
			FROM %s, latest
			WHERE created_at >= latest.closed_at - INTERVAL 1 DAY
		), ops AS (
			SELECT COUNT(*) AS total,
				COUNT(*) FILTER (WHERE is_soroban_op) AS contract_ops,
				COUNT(*) FILTER (WHERE is_payment_op) AS payment_ops
			FROM %s, latest
			WHERE created_at >= latest.closed_at - INTERVAL 1 DAY
		), contracts AS (
			SELECT COUNT(DISTINCT contract_id) AS active_contracts
			FROM %s, latest
			WHERE created_at >= latest.closed_at - INTERVAL 1 DAY
		)
		SELECT %s AS network, COALESCE((SELECT closed_at FROM latest), current_timestamp) AS generated_at,
			COALESCE((SELECT ledger_sequence FROM latest), %d) AS latest_ledger,
			(SELECT closed_at FROM latest) AS latest_ledger_closed_at,
			NULL::DOUBLE AS avg_close_time_seconds,
			(SELECT protocol_version FROM latest) AS protocol_version,
			(SELECT total FROM tx) AS tx_24h_total,
			(SELECT failed FROM tx) AS tx_24h_failed,
			(SELECT total FROM ops) AS ops_24h_total,
			(SELECT contract_ops FROM ops) AS ops_24h_contract_invoke,
			(SELECT payment_ops FROM ops) AS ops_24h_payments,
			NULL::BIGINT AS ops_prev_24h_total,
			NULL::BIGINT AS ops_prev_24h_contract_invoke,
			NULL::BIGINT AS fee_median_stroops,
			NULL::BIGINT AS fee_p99_stroops,
			NULL::BIGINT AS fee_daily_total_stroops,
			false AS surge_active,
			(SELECT active_accounts FROM tx) AS active_accounts_24h,
			NULL::BIGINT AS created_accounts_24h,
			(SELECT active_contracts FROM contracts) AS active_contracts_24h,
			NULL::BIGINT AS avg_cpu_insns_24h,
			NULL::BIGINT AS rent_burned_24h_stroops,
			NULL::INTEGER AS validator_count`,
		b.servingTable("sv_ledger_stats_recent"), b.servingTable("sv_transactions_recent"), b.servingTable("sv_operations_recent"), b.servingTable("sv_contract_calls_recent"), q(b.cfg.Network), b.cfg.End)
}

func selectAssetsCurrent(b *Backfiller) string {
	return fmt.Sprintf(`SELECT asset_key, MAX(asset_code) AS asset_code, MAX(asset_issuer) AS asset_issuer,
		MAX(asset_type) AS asset_type, bool_or(asset_type='native') AS is_native,
		false AS is_sep41, NULL::VARCHAR AS contract_id, NULL::VARCHAR AS name,
		MAX(asset_code) AS symbol, 7::INTEGER AS decimals, NULL::VARCHAR AS icon_url,
		false AS verified, NULL::VARCHAR AS issuer_domain,
		COUNT(DISTINCT account_id) AS holder_count,
		COUNT(*) AS trustline_count,
		SUM(balance_display) AS circulating_supply,
		current_timestamp AS updated_at
		FROM %s GROUP BY asset_key`,
		b.servingTable("sv_account_balances_current"))
}

func selectAssetStatsCurrent(b *Backfiller) string {
	return fmt.Sprintf(`SELECT asset_key, COUNT(DISTINCT account_id) AS holder_count,
		COUNT(*) AS trustline_count, SUM(balance_display) AS circulating_supply,
		NULL::DECIMAL(38,7) AS volume_24h, NULL::BIGINT AS transfers_24h,
		COUNT(DISTINCT account_id) AS unique_accounts_24h,
		NULL::DOUBLE AS top10_concentration, current_timestamp AS updated_at
		FROM %s GROUP BY asset_key`,
		b.servingTable("sv_account_balances_current"))
}

func selectContractsCurrent(b *Backfiller) string {
	return fmt.Sprintf(`WITH storage AS (
			SELECT contract_id,
				COUNT(*) FILTER (WHERE durability='persistent') AS persistent_entries,
				COUNT(*) FILTER (WHERE durability='temporary') AS temporary_entries,
				COUNT(*) FILTER (WHERE durability='instance') AS instance_entries,
				MAX(updated_at) AS last_seen_at
			FROM %s WHERE network=%s GROUP BY contract_id
		)
		SELECT m.contract_id, NULL::VARCHAR AS name, NULL::VARCHAR AS symbol,
			'soroban' AS contract_type, NULL::VARCHAR AS wallet_type,
			m.creator_address AS creator_account, m.created_ledger AS deploy_ledger,
			m.created_at AS deploy_timestamp, m.wasm_hash, NULL::BIGINT AS wasm_size_bytes,
			COALESCE(s.persistent_entries, 0)::INTEGER AS persistent_entries,
			COALESCE(s.temporary_entries, 0)::INTEGER AS temporary_entries,
			COALESCE(s.instance_entries, 0)::INTEGER AS instance_entries,
			NULL::BIGINT AS total_state_size_bytes,
			NULL::BIGINT AS estimated_monthly_rent_stroops,
			'[]' AS exported_functions_json,
			m.created_at AS first_seen_at,
			COALESCE(s.last_seen_at, m.created_at) AS last_seen_at,
			current_timestamp AS updated_at
		FROM %s m LEFT JOIN storage s ON s.contract_id=m.contract_id
		WHERE m.network=%s AND m.created_ledger <= %d`,
		b.silverTable("contract_data_current"), q(b.cfg.Network), b.silverTable("contract_metadata"), q(b.cfg.Network), b.cfg.End)
}

func selectContractStatsCurrent(b *Backfiller) string {
	return fmt.Sprintf(`SELECT contract_id,
		COUNT(*) AS total_calls_24h, COUNT(*) AS total_calls_7d, COUNT(*) AS total_calls_30d,
		COUNT(DISTINCT caller_account) AS unique_callers_24h,
		COUNT(DISTINCT caller_account) AS unique_callers_7d,
		COUNT(*) FILTER (WHERE successful) AS success_count_24h,
		COUNT(*) FILTER (WHERE NOT successful) AS failure_count_24h,
		CASE WHEN COUNT(*) = 0 THEN NULL ELSE COUNT(*) FILTER (WHERE successful)::DOUBLE / COUNT(*)::DOUBLE END AS success_rate_24h,
		MAX(function_name) AS top_function,
		MAX(created_at) AS last_activity_at,
		AVG(cpu_insns)::BIGINT AS avg_cpu_insns_24h,
		MIN(created_at) AS first_seen_at,
		current_timestamp AS updated_at
		FROM %s GROUP BY contract_id`,
		b.servingTable("sv_contract_calls_recent"))
}

func selectContractFunctionStatsCurrent(b *Backfiller) string {
	return fmt.Sprintf(`SELECT contract_id, COALESCE(function_name, '') AS function_name,
		COUNT(*) AS calls_24h, COUNT(*) AS calls_7d, COUNT(*) AS calls_30d,
		COUNT(*) FILTER (WHERE successful) AS success_count_24h,
		COUNT(*) FILTER (WHERE NOT successful) AS failure_count_24h,
		AVG(cpu_insns)::BIGINT AS avg_cpu_insns_24h,
		MAX(created_at) AS last_called_at,
		current_timestamp AS updated_at
		FROM %s GROUP BY contract_id, COALESCE(function_name, '')`,
		b.servingTable("sv_contract_calls_recent"))
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

func (b *Backfiller) servingTable(table string) string {
	return fmt.Sprintf("%s.%s", ident(b.cfg.ServingSchema), ident(table))
}

func (b *Backfiller) bronzeTable(table string) string {
	if b.cfg.BronzeAlias == "" {
		return fmt.Sprintf("%s.%s", ident(b.cfg.BronzeSchema), ident(table))
	}
	return fmt.Sprintf("%s.%s.%s", ident(b.cfg.BronzeAlias), ident(b.cfg.BronzeSchema), ident(table))
}

func (b *Backfiller) silverTable(table string) string {
	if b.cfg.SilverAlias == "" {
		return fmt.Sprintf("%s.%s", ident(b.cfg.SilverSchema), ident(table))
	}
	return fmt.Sprintf("%s.%s.%s", ident(b.cfg.SilverAlias), ident(b.cfg.SilverSchema), ident(table))
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
	for _, name := range append(feedProjectionNames(feedProjections()), currentProjectionNames(currentProjections())...) {
		if !completed[name] {
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
		"component_id":     cfg.Component(),
		"version":          Version,
		"status":           "ready",
		"capabilities":     []string{"serving-cold-backfill-contract", "deterministic-chunk-inputs", "resume-manifest", "typed-failures", "checkpoint-handoff-contract", "json-lifecycle-events"},
		"required":         requiredProjections(cfg.ServingSchema),
		"classified_extra": classifiedOptionalTables(cfg.ServingSchema),
	})
}

func writeSummary(cfg Config, chunks []Chunk, projections []Projection, checkpoints []Checkpoint) error {
	if cfg.SummaryPath == "" {
		return nil
	}
	f, err := os.OpenFile(cfg.SummaryPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return fmt.Errorf("write summary: %w", err)
	}
	defer f.Close()
	return json.NewEncoder(f).Encode(map[string]interface{}{"component_id": cfg.Component(), "run_id": cfg.RunID(), "network": cfg.Network, "chunks": chunks, "required_projections": projections, "classified_extra": classifiedOptionalTables(cfg.ServingSchema), "checkpoint_plan": checkpoints, "status": "completed"})
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
	case strings.Contains(msg, "timeout") || strings.Contains(msg, "connection") || strings.Contains(msg, "s3") || strings.Contains(msg, "catalog") || strings.Contains(msg, "postgres"):
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
	parts := strings.Split(endpoint, "@")
	return parts[len(parts)-1]
}

func ident(s string) string { return `"` + strings.ReplaceAll(s, `"`, `""`) + `"` }
func q(s string) string     { return `'` + strings.ReplaceAll(s, `'`, `''`) + `'` }
