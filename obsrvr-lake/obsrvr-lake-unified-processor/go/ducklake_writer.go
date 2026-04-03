package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"strings"

	_ "github.com/duckdb/duckdb-go/v2"
)

// DuckLakeWriter manages all writes to DuckLake (bronze, silver, index)
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
			// Try without core_nightly fallback
			if _, err2 := w.db.ExecContext(ctx, fmt.Sprintf("INSTALL %s", ext)); err2 != nil {
				return fmt.Errorf("install %s: %w", ext, err)
			}
		}
		if _, err := w.db.ExecContext(ctx, fmt.Sprintf("LOAD %s", ext)); err != nil {
			return fmt.Errorf("load %s: %w", ext, err)
		}
	}
	log.Println("Extensions loaded: ducklake, httpfs")

	// Configure S3 credentials for B2
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
	log.Println("S3 credentials configured")

	// Set inlining threshold
	if w.cfg.DuckLake.InliningLimit > 0 {
		if _, err := w.db.ExecContext(ctx, fmt.Sprintf(
			"SET ducklake_default_data_inlining_row_limit = %d", w.cfg.DuckLake.InliningLimit)); err != nil {
			log.Printf("Warning: could not set inlining limit: %v (may not be supported in this version)", err)
		}
	}

	// Attach DuckLake catalog with explicit metadata schema
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

	// Create schemas within the DuckLake catalog
	schemas := []string{w.cfg.DuckLake.BronzeSchema, w.cfg.DuckLake.SilverSchema, w.cfg.DuckLake.IndexSchema}
	// Deduplicate in case schemas are the same
	seen := map[string]bool{}
	for _, schema := range schemas {
		if seen[schema] {
			continue
		}
		seen[schema] = true
		createSQL := fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s.%s", w.cfg.DuckLake.CatalogName, schema)
		if _, err := w.db.ExecContext(ctx, createSQL); err != nil {
			return fmt.Errorf("create schema %s: %w", schema, err)
		}
	}
	log.Printf("Schemas created: %v", schemas)

	// Create tables
	if err := w.createBronzeTables(ctx); err != nil {
		return fmt.Errorf("create bronze tables: %w", err)
	}
	if err := w.createSilverTables(ctx); err != nil {
		return fmt.Errorf("create silver tables: %w", err)
	}
	if err := w.createIndexTables(ctx); err != nil {
		return fmt.Errorf("create index tables: %w", err)
	}

	log.Println("All DuckLake tables ready")
	return nil
}

// ProcessLedgerData writes bronze data and transforms to silver within a single transaction
func (w *DuckLakeWriter) ProcessLedgerData(ctx context.Context, data *BronzeData, ledgerSeq uint32) error {
	tx, err := w.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	cat := w.cfg.DuckLake.CatalogName
	bronze := w.cfg.DuckLake.BronzeSchema

	// Step 1: Write bronze tables
	if err := w.writeBronzeLedger(ctx, tx, cat, bronze, data); err != nil {
		return fmt.Errorf("write bronze: %w", err)
	}

	// Step 2: Transform bronze → silver (INSERT-SELECT within same tx)
	silver := w.cfg.DuckLake.SilverSchema
	seq := int64(ledgerSeq)
	if err := w.transformSilver(ctx, tx, cat, bronze, silver, seq, seq); err != nil {
		return fmt.Errorf("transform silver: %w", err)
	}

	// Step 2b: Materialize current-state tables
	if err := transformCurrentState(ctx, tx, cat, bronze, silver, seq, seq); err != nil {
		return fmt.Errorf("transform current state: %w", err)
	}

	// Step 3: Materialize semantic layer
	if err := w.transformSemantic(ctx, tx, cat, silver, seq, seq); err != nil {
		return fmt.Errorf("transform semantic: %w", err)
	}

	// Step 4: Build indexes
	idx := w.cfg.DuckLake.IndexSchema
	if err := w.buildIndexes(ctx, tx, cat, bronze, idx, seq, seq); err != nil {
		return fmt.Errorf("build indexes: %w", err)
	}

	// Step 5: Update checkpoint
	if err := w.saveCheckpoint(ctx, tx, cat, seq); err != nil {
		return fmt.Errorf("save checkpoint: %w", err)
	}

	return tx.Commit()
}

// FlushInlinedData consolidates inlined rows to Parquet files on B2
func (w *DuckLakeWriter) FlushInlinedData(ctx context.Context) error {
	_, err := w.db.ExecContext(ctx, fmt.Sprintf(
		"CALL ducklake_flush_inlined_data('%s')", w.cfg.DuckLake.CatalogName))
	return err
}

// LoadCheckpoint reads the last processed ledger
func (w *DuckLakeWriter) LoadCheckpoint(ctx context.Context) (int64, error) {
	cat := w.cfg.DuckLake.CatalogName
	bronze := w.cfg.DuckLake.BronzeSchema
	query := fmt.Sprintf("SELECT MAX(sequence) FROM %s.%s.ledgers_row_v2", cat, bronze)
	var maxSeq sql.NullInt64
	if err := w.db.QueryRowContext(ctx, query).Scan(&maxSeq); err != nil {
		return 0, err
	}
	if maxSeq.Valid {
		return maxSeq.Int64, nil
	}
	return 0, nil
}

func (w *DuckLakeWriter) saveCheckpoint(ctx context.Context, tx *sql.Tx, cat string, ledgerSeq int64) error {
	// Checkpoint is implicit — the max ledger in bronze.ledgers_row_v2 IS the checkpoint
	// No separate checkpoint table needed
	return nil
}

func (w *DuckLakeWriter) Close() error {
	if w.db != nil {
		return w.db.Close()
	}
	return nil
}

// execSchemaStatements runs a multi-statement schema string
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
