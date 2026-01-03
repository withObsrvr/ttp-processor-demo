package main

import (
	"database/sql"
	"fmt"
	"log"
	"strings"
	"time"

	_ "github.com/lib/pq"
)

// Flusher manages the high-watermark flush process
type Flusher struct {
	pgDB       *sql.DB
	duckDB     *DuckDBClient
	config     *Config
	flushCount int64
	totalRows  int64
}

// NewFlusher creates a new flusher instance
func NewFlusher(config *Config) (*Flusher, error) {
	// Connect to PostgreSQL silver_hot
	log.Printf("🔗 Connecting to PostgreSQL (silver_hot)...")
	pgDB, err := sql.Open("postgres", config.Postgres.ConnectionString())
	if err != nil {
		return nil, fmt.Errorf("failed to connect to PostgreSQL: %w", err)
	}

	if err := pgDB.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping PostgreSQL: %w", err)
	}
	log.Println("✅ Connected to PostgreSQL")

	// Connect to DuckDB
	duckDB, err := NewDuckDBClient(&config.DuckLake)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to DuckDB: %w", err)
	}

	return &Flusher{
		pgDB:   pgDB,
		duckDB: duckDB,
		config: config,
	}, nil
}

// ExecuteFlush performs a single flush cycle
func (f *Flusher) ExecuteFlush() error {
	startTime := time.Now()
	log.Printf("🚀 Starting flush cycle #%d", f.flushCount+1)

	// Step 1: MARK - Get watermark from transformer checkpoint
	watermark, err := f.getWatermark()
	if err != nil {
		return fmt.Errorf("failed to get watermark: %w", err)
	}

	if watermark == 0 {
		log.Println("⚠️  No data to flush (watermark = 0)")
		return nil
	}

	log.Printf("📍 Watermark: ledger sequence = %d", watermark)

	// Step 2: FLUSH - Flush all tables to DuckLake
	rowsFlushed, err := f.flushAllTables(watermark)
	if err != nil {
		return fmt.Errorf("flush failed: %w", err)
	}

	log.Printf("✅ Flushed %d rows to DuckLake", rowsFlushed)

	// Step 3: DELETE - Remove flushed data from PostgreSQL
	rowsDeleted, err := f.deleteFlushedData(watermark)
	if err != nil {
		return fmt.Errorf("failed to delete flushed data: %w", err)
	}

	log.Printf("🗑️  Deleted %d rows from PostgreSQL", rowsDeleted)

	// Step 4: VACUUM (every Nth flush)
	f.flushCount++
	if f.config.Vacuum.Enabled && f.flushCount%int64(f.config.Vacuum.EveryNFlushes) == 0 {
		log.Println("🧹 Running VACUUM ANALYZE...")
		if err := f.vacuumAllTables(); err != nil {
			log.Printf("⚠️  VACUUM failed (non-fatal): %v", err)
		} else {
			log.Println("✅ VACUUM completed")
		}
	}

	f.totalRows += rowsFlushed

	duration := time.Since(startTime)
	log.Printf("✅ Flush cycle #%d completed in %v (watermark=%d, flushed=%d, deleted=%d, total=%d)",
		f.flushCount, duration, watermark, rowsFlushed, rowsDeleted, f.totalRows)

	return nil
}

// getWatermark retrieves the current watermark (last ledger from transformer checkpoint)
func (f *Flusher) getWatermark() (int64, error) {
	var watermark int64
	query := "SELECT COALESCE(MAX(last_ledger_sequence), 0) FROM realtime_transformer_checkpoint"
	err := f.pgDB.QueryRow(query).Scan(&watermark)
	if err != nil {
		return 0, fmt.Errorf("failed to query watermark: %w", err)
	}
	return watermark, nil
}

// flushAllTables flushes all silver tables to DuckLake
func (f *Flusher) flushAllTables(watermark int64) (int64, error) {
	tables := GetTablesToFlush()
	totalRows := int64(0)

	pgConnStr := f.config.Postgres.ConnectionString()

	// Snapshot tables (use ledger_sequence)
	snapshotTables := map[string]bool{
		"accounts_snapshot":             true,
		"trustlines_snapshot":           true,
		"offers_snapshot":               true,
		"account_signers_snapshot":      true,
		"claimable_balances_snapshot":   true,
	}

	// Enriched/event tables (use ledger_sequence)
	eventTables := map[string]bool{
		"enriched_history_operations":         true,
		"enriched_history_operations_soroban": true,
		"token_transfers_raw":                 true,
		"soroban_history_operations":          true,
	}

	for _, tableName := range tables {
		log.Printf("   Flushing %s...", tableName)

		var rowsFlushed int64
		var err error

		if snapshotTables[tableName] || eventTables[tableName] {
			// Use ledger_sequence for snapshot and event tables
			rowsFlushed, err = f.duckDB.FlushSnapshotTable(tableName, watermark, pgConnStr)
		} else {
			// Use last_modified_ledger for current state tables
			rowsFlushed, err = f.duckDB.FlushTable(tableName, watermark, pgConnStr)
		}

		if err != nil {
			log.Printf("⚠️  Failed to flush %s: %v", tableName, err)
			continue // Continue with next table
		}

		log.Printf("   ✓ Flushed %d rows from %s", rowsFlushed, tableName)
		totalRows += rowsFlushed
	}

	return totalRows, nil
}

// deleteFlushedData removes flushed data from PostgreSQL
func (f *Flusher) deleteFlushedData(watermark int64) (int64, error) {
	tables := GetTablesToFlush()
	totalDeleted := int64(0)

	// Snapshot tables (use ledger_sequence)
	snapshotTables := map[string]bool{
		"accounts_snapshot":             true,
		"trustlines_snapshot":           true,
		"offers_snapshot":               true,
		"account_signers_snapshot":      true,
		"claimable_balances_snapshot":   true,
	}

	// Enriched/event tables (use ledger_sequence)
	eventTables := map[string]bool{
		"enriched_history_operations":         true,
		"enriched_history_operations_soroban": true,
		"token_transfers_raw":                 true,
		"soroban_history_operations":          true,
	}

	for _, tableName := range tables {
		var query string
		if snapshotTables[tableName] || eventTables[tableName] {
			query = fmt.Sprintf("DELETE FROM %s WHERE ledger_sequence <= $1", tableName)
		} else {
			query = fmt.Sprintf("DELETE FROM %s WHERE last_modified_ledger <= $1", tableName)
		}

		result, err := f.pgDB.Exec(query, watermark)
		if err != nil {
			log.Printf("⚠️  Failed to delete from %s: %v", tableName, err)
			continue
		}

		rowsDeleted, _ := result.RowsAffected()
		totalDeleted += rowsDeleted
	}

	return totalDeleted, nil
}

// vacuumAllTables runs VACUUM ANALYZE on all tables
func (f *Flusher) vacuumAllTables() error {
	tables := GetTablesToFlush()

	for _, tableName := range tables {
		query := fmt.Sprintf("VACUUM ANALYZE %s", tableName)
		if _, err := f.pgDB.Exec(query); err != nil {
			// VACUUM errors are non-fatal, log and continue
			log.Printf("   ⚠️  VACUUM failed for %s: %v", tableName, err)
		}
	}

	return nil
}

// GetStats returns flush statistics
func (f *Flusher) GetStats() FlushStats {
	return FlushStats{
		FlushCount: f.flushCount,
		TotalRows:  f.totalRows,
	}
}

// Close closes all connections
func (f *Flusher) Close() error {
	var errs []string

	if err := f.pgDB.Close(); err != nil {
		errs = append(errs, fmt.Sprintf("PostgreSQL: %v", err))
	}

	if err := f.duckDB.Close(); err != nil {
		errs = append(errs, fmt.Sprintf("DuckDB: %v", err))
	}

	if len(errs) > 0 {
		return fmt.Errorf("errors closing connections: %s", strings.Join(errs, ", "))
	}

	return nil
}

// FlushStats holds flush statistics
type FlushStats struct {
	FlushCount int64
	TotalRows  int64
}
