package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	_ "github.com/lib/pq"
)

// Flusher manages the high-watermark flush process
type Flusher struct {
	pgDB       *sql.DB
	duckDB     *DuckDBClient
	config     *Config
	mu         sync.RWMutex // Protects against concurrent maintenance operations
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

	// Create checkpoint table for tracking last-flushed watermark
	_, err = pgDB.Exec(`CREATE TABLE IF NOT EXISTS cold_flusher_checkpoint (
		id INTEGER PRIMARY KEY DEFAULT 1 CHECK (id = 1),
		last_flushed_watermark BIGINT NOT NULL DEFAULT 0,
		updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
	)`)
	if err != nil {
		pgDB.Close()
		duckDB.Close()
		return nil, fmt.Errorf("failed to create checkpoint table: %w", err)
	}
	_, err = pgDB.Exec(`INSERT INTO cold_flusher_checkpoint (id, last_flushed_watermark)
		VALUES (1, 0) ON CONFLICT (id) DO NOTHING`)
	if err != nil {
		pgDB.Close()
		duckDB.Close()
		return nil, fmt.Errorf("failed to initialize checkpoint row: %w", err)
	}

	flusher := &Flusher{
		pgDB:   pgDB,
		duckDB: duckDB,
		config: config,
	}

	// Set flusher reference for mutex coordination
	duckDB.SetFlusher(flusher)

	return flusher, nil
}

// getLastFlushedWatermark retrieves the last successfully flushed watermark
func (f *Flusher) getLastFlushedWatermark() (int64, error) {
	var wm int64
	err := f.pgDB.QueryRow("SELECT last_flushed_watermark FROM cold_flusher_checkpoint WHERE id = 1").Scan(&wm)
	return wm, err
}

// updateLastFlushedWatermark persists the watermark after a successful flush
func (f *Flusher) updateLastFlushedWatermark(watermark int64) error {
	_, err := f.pgDB.Exec(
		"UPDATE cold_flusher_checkpoint SET last_flushed_watermark = $1, updated_at = NOW() WHERE id = 1",
		watermark)
	return err
}

// ExecuteFlush performs a single flush cycle
func (f *Flusher) ExecuteFlush() error {
	// Acquire read lock - allows concurrent flushes but blocks maintenance
	f.mu.RLock()
	defer f.mu.RUnlock()

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

	// Step 1b: Get last flushed watermark for incremental flush
	lastFlushed, err := f.getLastFlushedWatermark()
	if err != nil {
		return fmt.Errorf("failed to get last flushed watermark: %w", err)
	}
	if lastFlushed >= watermark {
		log.Printf("Nothing new to flush (lastFlushed=%d >= watermark=%d)", lastFlushed, watermark)
		return nil
	}
	log.Printf("📍 Incremental flush: ledgers %d..%d", lastFlushed+1, watermark)

	// Step 2: FLUSH - Flush all tables to DuckLake
	rowsFlushed, successfullyFlushedTables, err := f.flushAllTables(watermark, lastFlushed)
	if err != nil {
		return fmt.Errorf("flush failed: %w", err)
	}

	log.Printf("✅ Flushed %d rows to DuckLake", rowsFlushed)

	// Step 2b: Update checkpoint after successful flush
	if err := f.updateLastFlushedWatermark(watermark); err != nil {
		return fmt.Errorf("failed to update flush checkpoint: %w", err)
	}

	// Step 3: DELETE - Remove flushed data from PostgreSQL (ONLY for successfully flushed tables)
	var rowsDeleted int64
	if len(successfullyFlushedTables) > 0 {
		rowsDeleted, err = f.deleteFlushedData(watermark, successfullyFlushedTables)
		if err != nil {
			return fmt.Errorf("failed to delete flushed data: %w", err)
		}
	} else {
		log.Println("⚠️  No tables flushed successfully, skipping deletion")
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

	// Step 4b: DuckLake maintenance (every Nth flush)
	if f.config.Maintenance.Enabled && f.flushCount%int64(f.config.Maintenance.EveryNFlushes) == 0 {
		log.Printf("🔧 Running DuckLake maintenance (flush #%d)...", f.flushCount)
		ctx := context.Background()
		if err := f.duckDB.RunCheckpoint(ctx, f.config.Maintenance.MaxCompactedFiles); err != nil {
			log.Printf("⚠️  DuckLake maintenance failed (non-fatal): %v", err)
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
func (f *Flusher) flushAllTables(watermark, lastFlushed int64) (int64, []string, error) {
	tables := GetTablesToFlush()
	totalRows := int64(0)
	successfullyFlushed := make([]string, 0, len(tables))

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
		"contract_invocations_raw":            true,
		"semantic_activities":                 true,
		"semantic_flows_value":                true,
	}

	// Tables with non-standard watermark column
	customWatermarkCol := map[string]string{
		"contract_metadata": "created_ledger",
		"token_registry":    "last_updated_ledger",
	}

	for _, tableName := range tables {
		log.Printf("   Flushing %s...", tableName)

		var rowsFlushed int64
		var err error

		if col, ok := customWatermarkCol[tableName]; ok {
			// Use custom watermark column
			rowsFlushed, err = f.duckDB.FlushTableWithColumn(tableName, watermark, pgConnStr, col, lastFlushed)
		} else if snapshotTables[tableName] || eventTables[tableName] {
			// Use ledger_sequence for snapshot and event tables
			rowsFlushed, err = f.duckDB.FlushSnapshotTable(tableName, watermark, pgConnStr, lastFlushed)
		} else {
			// Use last_modified_ledger for current state tables
			rowsFlushed, err = f.duckDB.FlushTable(tableName, watermark, pgConnStr, lastFlushed)
		}

		if err != nil {
			log.Printf("⚠️  Failed to flush %s: %v", tableName, err)
			continue // Continue with next table
		}

		log.Printf("   ✓ Flushed %d rows from %s", rowsFlushed, tableName)
		totalRows += rowsFlushed
		successfullyFlushed = append(successfullyFlushed, tableName)
	}

	return totalRows, successfullyFlushed, nil
}

// deleteFlushedData removes flushed data from PostgreSQL — ONLY for successfully flushed tables
func (f *Flusher) deleteFlushedData(watermark int64, tables []string) (int64, error) {
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
		"contract_invocations_raw":            true,
		"semantic_activities":                 true,
		"semantic_flows_value":                true,
	}

	// Tables with non-standard watermark column
	customWatermarkCol := map[string]string{
		"contract_metadata": "created_ledger",
		"token_registry":    "last_updated_ledger",
	}

	for _, tableName := range tables {
		var query string
		if col, ok := customWatermarkCol[tableName]; ok {
			query = fmt.Sprintf("DELETE FROM %s WHERE %s <= $1", tableName, col)
		} else if snapshotTables[tableName] || eventTables[tableName] {
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

// GetDuckDB returns the DuckDB client for maintenance operations
func (f *Flusher) GetDuckDB() *DuckDBClient {
	return f.duckDB
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
