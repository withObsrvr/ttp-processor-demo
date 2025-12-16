package main

import (
	"context"
	"fmt"
	"log"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Flusher manages the high-watermark flush process
type Flusher struct {
	pgPool      *pgxpool.Pool
	duckdb      *DuckDBClient
	config      *Config
	flushCount  atomic.Int64
	lastFlush   atomic.Int64 // Unix timestamp
	lastWater   atomic.Int64 // Last watermark flushed
	totalFlushed atomic.Int64 // Total rows flushed across all flushes
}

// FlushMetrics contains metrics from a flush operation
type FlushMetrics struct {
	Watermark    int64
	RowsFlushed  int64
	RowsDeleted  int64
	Duration     time.Duration
	TablesSuccess int
	TablesFailed  int
	VacuumRun    bool
}

// NewFlusher creates a new Flusher instance
func NewFlusher(config *Config) (*Flusher, error) {
	// Create PostgreSQL connection pool
	pgConfig, err := pgxpool.ParseConfig(config.Postgres.GetPostgresDSN())
	if err != nil {
		return nil, fmt.Errorf("failed to parse PostgreSQL DSN: %w", err)
	}

	pgPool, err := pgxpool.NewWithConfig(context.Background(), pgConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create PostgreSQL connection pool: %w", err)
	}

	// Test PostgreSQL connection
	if err := pgPool.Ping(context.Background()); err != nil {
		pgPool.Close()
		return nil, fmt.Errorf("failed to ping PostgreSQL: %w", err)
	}

	log.Printf("Connected to PostgreSQL: %s:%d/%s", config.Postgres.Host, config.Postgres.Port, config.Postgres.Database)

	// Create DuckDB client
	duckdb, err := NewDuckDBClient(&config.DuckLake)
	if err != nil {
		pgPool.Close()
		return nil, fmt.Errorf("failed to create DuckDB client: %w", err)
	}

	return &Flusher{
		pgPool: pgPool,
		duckdb: duckdb,
		config: config,
	}, nil
}

// GetHighWatermark retrieves the maximum ledger sequence from PostgreSQL
func (f *Flusher) GetHighWatermark(ctx context.Context) (int64, error) {
	var watermark int64
	err := f.pgPool.QueryRow(ctx, "SELECT COALESCE(MAX(sequence), 0) FROM ledgers_row_v2;").Scan(&watermark)
	if err != nil {
		return 0, fmt.Errorf("failed to get high watermark: %w", err)
	}
	return watermark, nil
}

// Flush performs the complete flush operation
func (f *Flusher) Flush(ctx context.Context) (*FlushMetrics, error) {
	startTime := time.Now()
	metrics := &FlushMetrics{}

	// 1. Get high watermark
	watermark, err := f.GetHighWatermark(ctx)
	if err != nil {
		return nil, err
	}

	if watermark == 0 {
		log.Println("No data to flush (watermark=0)")
		return metrics, nil
	}

	metrics.Watermark = watermark
	log.Printf("Starting flush with watermark=%d", watermark)

	// 2. Flush all tables to DuckLake
	tables := GetTablesToFlush()
	postgresDSN := f.config.Postgres.GetPostgresDSN()

	var totalRowsFlushed int64
	for _, tableName := range tables {
		rowsFlushed, err := f.duckdb.FlushTableFromPostgres(ctx, postgresDSN, tableName, watermark)
		if err != nil {
			log.Printf("Warning: Failed to flush table %s: %v", tableName, err)
			metrics.TablesFailed++
			continue
		}

		totalRowsFlushed += rowsFlushed
		metrics.TablesSuccess++
	}

	metrics.RowsFlushed = totalRowsFlushed
	log.Printf("Flushed %d rows from %d tables to DuckLake", totalRowsFlushed, metrics.TablesSuccess)

	// 3. Delete flushed data from PostgreSQL
	if totalRowsFlushed > 0 {
		rowsDeleted, err := f.deleteFromPostgres(ctx, watermark)
		if err != nil {
			return nil, fmt.Errorf("failed to delete from PostgreSQL: %w", err)
		}
		metrics.RowsDeleted = rowsDeleted
		log.Printf("Deleted %d rows from PostgreSQL", rowsDeleted)
	}

	// 4. Increment flush count
	flushCount := f.flushCount.Add(1)

	// 5. Optional: VACUUM (every Nth flush)
	if f.config.Vacuum.Enabled && flushCount%int64(f.config.Vacuum.EveryNFlushes) == 0 {
		log.Printf("Running VACUUM ANALYZE (flush #%d)...", flushCount)
		if err := f.vacuum(ctx); err != nil {
			log.Printf("Warning: VACUUM failed: %v", err)
		} else {
			metrics.VacuumRun = true
			log.Println("VACUUM ANALYZE completed")
		}
	}

	// 6. Update metrics
	metrics.Duration = time.Since(startTime)
	f.lastFlush.Store(time.Now().Unix())
	f.lastWater.Store(watermark)
	f.totalFlushed.Add(totalRowsFlushed)

	log.Printf("Flush completed in %v (watermark=%d, flushed=%d, deleted=%d, success=%d, failed=%d)",
		metrics.Duration, metrics.Watermark, metrics.RowsFlushed, metrics.RowsDeleted,
		metrics.TablesSuccess, metrics.TablesFailed)

	return metrics, nil
}

// deleteFromPostgres deletes flushed data from all PostgreSQL tables
func (f *Flusher) deleteFromPostgres(ctx context.Context, watermark int64) (int64, error) {
	tables := GetTablesToFlush()
	var totalRowsDeleted int64

	for _, tableName := range tables {
		// Determine the sequence column name based on table type
		sequenceColumn := "ledger_sequence"
		if tableName == "ledgers_row_v2" {
			sequenceColumn = "sequence"
		}

		deleteSQL := fmt.Sprintf("DELETE FROM %s WHERE %s <= $1;", tableName, sequenceColumn)

		result, err := f.pgPool.Exec(ctx, deleteSQL, watermark)
		if err != nil {
			return 0, fmt.Errorf("failed to delete from %s: %w", tableName, err)
		}

		rowsDeleted := result.RowsAffected()
		totalRowsDeleted += rowsDeleted

		if rowsDeleted > 0 {
			log.Printf("Deleted %d rows from %s", rowsDeleted, tableName)
		}
	}

	return totalRowsDeleted, nil
}

// vacuum runs VACUUM ANALYZE on all tables
func (f *Flusher) vacuum(ctx context.Context) error {
	tables := GetTablesToFlush()

	for _, tableName := range tables {
		vacuumSQL := fmt.Sprintf("VACUUM ANALYZE %s;", tableName)
		if _, err := f.pgPool.Exec(ctx, vacuumSQL); err != nil {
			return fmt.Errorf("failed to vacuum %s: %w", tableName, err)
		}
	}

	return nil
}

// GetFlushCount returns the total number of flushes performed
func (f *Flusher) GetFlushCount() int64 {
	return f.flushCount.Load()
}

// GetLastFlushTime returns the Unix timestamp of the last flush
func (f *Flusher) GetLastFlushTime() int64 {
	return f.lastFlush.Load()
}

// GetLastWatermark returns the watermark of the last flush
func (f *Flusher) GetLastWatermark() int64 {
	return f.lastWater.Load()
}

// GetTotalFlushed returns the total rows flushed across all flushes
func (f *Flusher) GetTotalFlushed() int64 {
	return f.totalFlushed.Load()
}

// Close closes all connections
func (f *Flusher) Close() {
	if f.pgPool != nil {
		f.pgPool.Close()
	}
	if f.duckdb != nil {
		f.duckdb.Close()
	}
}
