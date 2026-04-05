package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"strings"

	_ "github.com/duckdb/duckdb-go/v2"
	pb "github.com/withObsrvr/obsrvr-lake-unified-processor/gen/bronze_ledger_service"
)

type DuckLakeWriter struct {
	db  *sql.DB
	cfg *Config
}

func NewDuckLakeWriter(cfg *Config) (*DuckLakeWriter, error) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		return nil, fmt.Errorf("open duckdb: %w", err)
	}
	db.SetMaxOpenConns(1)

	w := &DuckLakeWriter{db: db, cfg: cfg}

	if err := w.initialize(); err != nil {
		db.Close()
		return nil, err
	}

	return w, nil
}

func (w *DuckLakeWriter) initialize() error {
	ctx := context.Background()

	// Install and load extensions
	for _, ext := range []string{"ducklake", "httpfs"} {
		if _, err := w.db.ExecContext(ctx, fmt.Sprintf("INSTALL %s FROM core_nightly", ext)); err != nil {
			if _, err2 := w.db.ExecContext(ctx, fmt.Sprintf("INSTALL %s", ext)); err2 != nil {
				return fmt.Errorf("install %s: %w", ext, err)
			}
		}
		if _, err := w.db.ExecContext(ctx, fmt.Sprintf("LOAD %s", ext)); err != nil {
			return fmt.Errorf("load %s: %w", ext, err)
		}
	}
	log.Println("Extensions loaded: ducklake, httpfs")

	// Configure S3
	s3SQL := fmt.Sprintf(`
		CREATE SECRET s3_secret (
			TYPE S3,
			KEY_ID '%s',
			SECRET '%s',
			REGION '%s',
			ENDPOINT '%s',
			URL_COMPATIBILITY_MODE true
		)`, w.cfg.S3.KeyID, w.cfg.S3.KeySecret, w.cfg.S3.Region, w.cfg.S3.Endpoint)
	if _, err := w.db.ExecContext(ctx, s3SQL); err != nil {
		return fmt.Errorf("configure S3: %w", err)
	}

	// Set inlining threshold
	if w.cfg.DuckLake.InliningLimit > 0 {
		w.db.ExecContext(ctx, fmt.Sprintf(
			"SET ducklake_default_data_inlining_row_limit = %d", w.cfg.DuckLake.InliningLimit))
	}

	// Attach DuckLake catalog
	metaSchema := w.cfg.DuckLake.MetadataSchema
	if metaSchema == "" {
		metaSchema = "lake_meta"
	}
	attachSQL := fmt.Sprintf(
		"ATTACH '%s' AS %s (DATA_PATH '%s', METADATA_SCHEMA '%s', AUTOMATIC_MIGRATION TRUE, OVERRIDE_DATA_PATH TRUE)",
		w.cfg.DuckLake.CatalogPath, w.cfg.DuckLake.CatalogName, w.cfg.DuckLake.DataPath, metaSchema)
	if _, err := w.db.ExecContext(ctx, attachSQL); err != nil {
		return fmt.Errorf("attach catalog: %w", err)
	}
	log.Printf("DuckLake catalog '%s' attached", w.cfg.DuckLake.CatalogName)

	// Create silver schema (bronze schema already exists from unified processor)
	silverSchema := w.cfg.DuckLake.SilverSchema
	createSQL := fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s.%s", w.cfg.DuckLake.CatalogName, silverSchema)
	if _, err := w.db.ExecContext(ctx, createSQL); err != nil {
		return fmt.Errorf("create schema %s: %w", silverSchema, err)
	}
	log.Printf("Schema created: %s", silverSchema)

	// Create silver tables
	if err := w.createSilverTables(ctx); err != nil {
		return fmt.Errorf("create silver tables: %w", err)
	}

	log.Println("Silver DuckLake tables ready")
	return nil
}

// ProcessBronzeToSilver transforms bronze data and writes silver tables in a single transaction.
func (w *DuckLakeWriter) ProcessBronzeToSilver(ctx context.Context, data *pb.BronzeLedgerData) error {
	tx, err := w.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	cat := w.cfg.DuckLake.CatalogName
	bronze := w.cfg.DuckLake.BronzeSchema
	silver := w.cfg.DuckLake.SilverSchema
	seq := int64(data.LedgerSequence)

	// Phase A: Bronze -> Silver transforms
	if err := w.transformEnrichedOperations(ctx, tx, cat, bronze, silver, data); err != nil {
		return fmt.Errorf("enriched operations: %w", err)
	}

	if err := w.transformTokenTransfers(ctx, tx, cat, bronze, silver, data); err != nil {
		return fmt.Errorf("token transfers: %w", err)
	}

	if err := w.transformContractInvocations(ctx, tx, cat, silver, data); err != nil {
		return fmt.Errorf("contract invocations: %w", err)
	}

	if err := w.transformTrades(ctx, tx, cat, silver, data); err != nil {
		return fmt.Errorf("trades: %w", err)
	}

	if err := w.transformTokenRegistry(ctx, tx, cat, silver, data); err != nil {
		return fmt.Errorf("token registry: %w", err)
	}

	if err := w.transformContractMetadata(ctx, tx, cat, silver, data); err != nil {
		return fmt.Errorf("contract metadata: %w", err)
	}

	if err := w.transformCurrentState(ctx, tx, cat, silver, data); err != nil {
		return fmt.Errorf("current state: %w", err)
	}

	if err := w.transformEffects(ctx, tx, cat, silver, data); err != nil {
		return fmt.Errorf("effects: %w", err)
	}

	if err := w.transformEvictedKeys(ctx, tx, cat, silver, data); err != nil {
		return fmt.Errorf("evicted keys: %w", err)
	}

	if err := w.transformRestoredKeys(ctx, tx, cat, silver, data); err != nil {
		return fmt.Errorf("restored keys: %w", err)
	}

	if err := w.transformSnapshots(ctx, tx, cat, silver, data); err != nil {
		return fmt.Errorf("snapshots: %w", err)
	}

	// Phase B: Semantic transforms (silver -> silver)
	if err := w.transformSemanticActivities(ctx, tx, cat, silver, seq); err != nil {
		return fmt.Errorf("semantic activities: %w", err)
	}

	if err := w.transformSemanticFlows(ctx, tx, cat, silver, seq); err != nil {
		return fmt.Errorf("semantic flows: %w", err)
	}

	if err := w.transformSemanticEntitiesContracts(ctx, tx, cat, silver, seq); err != nil {
		return fmt.Errorf("semantic entities contracts: %w", err)
	}

	if err := w.transformSemanticContractFunctions(ctx, tx, cat, silver, seq); err != nil {
		return fmt.Errorf("semantic contract functions: %w", err)
	}

	if err := w.transformSemanticAssetStats(ctx, tx, cat, silver, seq); err != nil {
		return fmt.Errorf("semantic asset stats: %w", err)
	}

	if err := w.transformSemanticDexPairs(ctx, tx, cat, silver, seq); err != nil {
		return fmt.Errorf("semantic dex pairs: %w", err)
	}

	if err := w.transformSemanticAccountSummary(ctx, tx, cat, silver, seq); err != nil {
		return fmt.Errorf("semantic account summary: %w", err)
	}

	// Save checkpoint
	if err := w.saveCheckpoint(ctx, tx, cat, seq); err != nil {
		return fmt.Errorf("save checkpoint: %w", err)
	}

	return tx.Commit()
}

func (w *DuckLakeWriter) FlushInlinedData(ctx context.Context) error {
	_, err := w.db.ExecContext(ctx, fmt.Sprintf(
		"CALL ducklake_flush_inlined_data('%s')", w.cfg.DuckLake.CatalogName))
	return err
}

func (w *DuckLakeWriter) LoadCheckpoint(ctx context.Context) (int64, error) {
	cat := w.cfg.DuckLake.CatalogName
	silver := w.cfg.DuckLake.SilverSchema
	query := fmt.Sprintf("SELECT MAX(ledger_sequence) FROM %s.%s.enriched_history_operations", cat, silver)
	var maxSeq sql.NullInt64
	if err := w.db.QueryRowContext(ctx, query).Scan(&maxSeq); err != nil {
		return 0, nil // Table may not exist yet
	}
	if maxSeq.Valid {
		return maxSeq.Int64, nil
	}
	return 0, nil
}

func (w *DuckLakeWriter) saveCheckpoint(ctx context.Context, tx *sql.Tx, cat string, ledgerSeq int64) error {
	// Checkpoint is implicit -- MAX(ledger_sequence) in silver.enriched_history_operations
	return nil
}

func (w *DuckLakeWriter) Close() error {
	if w.db != nil {
		return w.db.Close()
	}
	return nil
}

func (w *DuckLakeWriter) createSilverTables(ctx context.Context) error {
	cat := w.cfg.DuckLake.CatalogName
	schema := w.cfg.DuckLake.SilverSchema
	ddl := silverSchemaSQL(cat, schema)
	return w.execSchemaStatements(ctx, ddl)
}

func (w *DuckLakeWriter) execSchemaStatements(ctx context.Context, schema string) error {
	for _, stmt := range strings.Split(schema, ";") {
		stmt = strings.TrimSpace(stmt)
		if stmt == "" || strings.HasPrefix(stmt, "--") {
			continue
		}
		if _, err := w.db.ExecContext(ctx, stmt+";"); err != nil {
			log.Printf("Schema warning: %v (stmt: %.80s...)", err, stmt)
		}
	}
	return nil
}
