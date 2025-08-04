package main

import (
	"database/sql"
	"embed"
	"fmt"
	"strings"

	"time"

	"github.com/rs/zerolog/log"
)

//go:embed schema.sql
var schemaFS embed.FS

// initializeSchema ensures the database schema is properly set up
func (c *PostgreSQLConsumer) initializeSchema() error {
	log.Info().Msg("Initializing database schema")
	
	// Read schema file
	schemaContent, err := schemaFS.ReadFile("schema.sql")
	if err != nil {
		return fmt.Errorf("failed to read schema file: %w", err)
	}
	
	// Split into individual statements (simple split by semicolon)
	statements := splitSQLStatements(string(schemaContent))
	
	// Execute each statement
	for i, stmt := range statements {
		stmt = strings.TrimSpace(stmt)
		if stmt == "" {
			continue
		}
		
		if _, err := c.db.Exec(stmt); err != nil {
			// Log but don't fail on certain errors (like "already exists")
			if isIgnorableError(err) {
				log.Debug().
					Int("statement", i).
					Err(err).
					Msg("Ignoring expected error")
			} else {
				return fmt.Errorf("failed to execute statement %d: %w", i, err)
			}
		}
	}
	
	// Verify critical tables exist
	if err := c.verifySchema(); err != nil {
		return fmt.Errorf("schema verification failed: %w", err)
	}
	
	log.Info().Msg("Database schema initialized successfully")
	return nil
}

// splitSQLStatements splits SQL content into individual statements
func splitSQLStatements(sql string) []string {
	// This is a simple implementation that splits on semicolons
	// In production, you might want a more sophisticated parser
	var statements []string
	var current strings.Builder
	inString := false
	escape := false
	
	for _, ch := range sql {
		current.WriteRune(ch)
		
		if escape {
			escape = false
			continue
		}
		
		switch ch {
		case '\\':
			escape = true
		case '\'':
			inString = !inString
		case ';':
			if !inString {
				statements = append(statements, current.String())
				current.Reset()
			}
		}
	}
	
	// Add any remaining content
	if current.Len() > 0 {
		statements = append(statements, current.String())
	}
	
	return statements
}

// isIgnorableError checks if an error can be safely ignored
func isIgnorableError(err error) bool {
	errStr := err.Error()
	ignorablePatterns := []string{
		"already exists",
		"duplicate key",
		"unique constraint",
	}
	
	for _, pattern := range ignorablePatterns {
		if strings.Contains(strings.ToLower(errStr), pattern) {
			return true
		}
	}
	
	return false
}

// verifySchema verifies that critical tables exist
func (c *PostgreSQLConsumer) verifySchema() error {
	tables := []string{
		"contract_data.entries",
		"contract_data.ingestion_progress",
	}
	
	for _, table := range tables {
		var exists bool
		parts := strings.Split(table, ".")
		schema, tableName := parts[0], parts[1]
		
		err := c.db.QueryRow(`
			SELECT EXISTS (
				SELECT 1 
				FROM information_schema.tables 
				WHERE table_schema = $1 
				AND table_name = $2
			)`, schema, tableName).Scan(&exists)
		
		if err != nil {
			return fmt.Errorf("failed to check table %s: %w", table, err)
		}
		
		if !exists {
			return fmt.Errorf("required table %s does not exist", table)
		}
	}
	
	return nil
}

// refreshMaterializedViews refreshes all materialized views
func (c *PostgreSQLConsumer) refreshMaterializedViews() error {
	_, err := c.db.Exec("SELECT contract_data.refresh_current_states()")
	if err != nil {
		return fmt.Errorf("failed to refresh materialized views: %w", err)
	}
	return nil
}

// getIngestionStats returns current ingestion statistics
func (c *PostgreSQLConsumer) getIngestionStats() (*IngestionStats, error) {
	stats := &IngestionStats{}
	
	// Get total entries
	err := c.db.QueryRow(`
		SELECT 
			COUNT(*),
			MIN(ledger_sequence),
			MAX(ledger_sequence),
			MAX(ingested_at)
		FROM contract_data.entries
	`).Scan(&stats.TotalEntries, &stats.MinLedger, &stats.MaxLedger, &stats.LastIngested)
	
	if err != nil {
		return nil, fmt.Errorf("failed to get entry stats: %w", err)
	}
	
	// Get unique contracts
	err = c.db.QueryRow(`
		SELECT COUNT(DISTINCT contract_id)
		FROM contract_data.entries
	`).Scan(&stats.UniqueContracts)
	
	if err != nil {
		return nil, fmt.Errorf("failed to get contract count: %w", err)
	}
	
	// Get batch stats
	err = c.db.QueryRow(`
		SELECT 
			COUNT(*),
			SUM(entries_processed)
		FROM contract_data.ingestion_progress
		WHERE status = 'completed'
	`).Scan(&stats.CompletedBatches, &stats.ProcessedEntries)
	
	if err != nil {
		return nil, fmt.Errorf("failed to get batch stats: %w", err)
	}
	
	return stats, nil
}

// IngestionStats contains database ingestion statistics
type IngestionStats struct {
	TotalEntries     int64
	UniqueContracts  int64
	MinLedger        *uint32
	MaxLedger        *uint32
	LastIngested     *time.Time
	CompletedBatches int64
	ProcessedEntries int64
}