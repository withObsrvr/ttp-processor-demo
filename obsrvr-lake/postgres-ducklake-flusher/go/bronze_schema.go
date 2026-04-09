package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
)

// createBronzeTables ensures the Bronze schema and tables exist, matching
// v3_bronze_schema.sql. This function is NON-DESTRUCTIVE: if the schema and
// all expected tables already exist, it does nothing. It will only create
// tables that are missing.
//
// Why non-destructive: previously this function did `DROP SCHEMA … CASCADE`
// followed by `CREATE TABLE IF NOT EXISTS`. In a regular database that would
// be fine, but DuckLake treats DROP SCHEMA as "set end_snapshot on all
// tables in the schema", which orphans every existing data file from the
// query API's perspective (the API filters by `end_snapshot IS NULL`). On
// every flusher restart we were silently rotating to a new generation of
// table_ids and losing visibility of all historical Parquet files. To add
// new columns going forward, write an ALTER TABLE migration in
// applyBronzeMigrations rather than re-introducing the DROP.
func (c *DuckDBClient) createBronzeTables(ctx context.Context) error {
	log.Println("Ensuring Bronze tables exist (non-destructive)...")

	// Create the schema if it doesn't already exist. CREATE SCHEMA IF NOT
	// EXISTS is a no-op when the schema is present, so existing tables and
	// their data files are untouched.
	createSchemaSQL := fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s.%s", c.config.CatalogName, c.config.SchemaName)
	if _, err := c.db.ExecContext(ctx, createSchemaSQL); err != nil {
		return fmt.Errorf("failed to ensure schema exists: %w", err)
	}

	// Read the v3_bronze_schema.sql file
	// Try current directory first (for Docker), then parent (for local dev)
	schemaPath := "v3_bronze_schema.sql"
	content, err := os.ReadFile(schemaPath)
	if err != nil {
		// Try parent directory for local development
		schemaPath = filepath.Join("..", "v3_bronze_schema.sql")
		content, err = os.ReadFile(schemaPath)
		if err != nil {
			return fmt.Errorf("failed to read v3_bronze_schema.sql: %w", err)
		}
	}

	schemaSQL := string(content)

	// Replace bronze. with catalog.schema. prefix
	schemaSQL = strings.ReplaceAll(schemaSQL, "bronze.", fmt.Sprintf("%s.%s.", c.config.CatalogName, c.config.SchemaName))

	// Split into individual CREATE TABLE statements. The schema file uses
	// CREATE TABLE IF NOT EXISTS, so re-running these against an existing
	// schema is a no-op for tables that already exist.
	statements := splitSQLStatements(schemaSQL)

	tableCount := 0
	for _, stmt := range statements {
		stmt = strings.TrimSpace(stmt)
		if stmt == "" || strings.HasPrefix(stmt, "--") {
			continue
		}

		if strings.Contains(stmt, "CREATE TABLE") {
			if _, err := c.db.ExecContext(ctx, stmt); err != nil {
				return fmt.Errorf("failed to create table: %w\nSQL: %s", err, stmt[:min(len(stmt), 200)])
			}
			tableCount++
		}
	}

	log.Printf("Bronze schema ensured (%d CREATE TABLE statements processed)", tableCount)

	// Add partitioning to all tables. ALTER TABLE … SET PARTITIONED BY is
	// also idempotent — re-applying the same partition spec is a no-op.
	if err := c.partitionBronzeTables(ctx); err != nil {
		return fmt.Errorf("failed to partition Bronze tables: %w", err)
	}

	return nil
}

// partitionBronzeTables adds DuckLake partitioning to all Bronze tables by ledger_range
// This organizes Parquet files into partition folders for efficient queries
func (c *DuckDBClient) partitionBronzeTables(ctx context.Context) error {
	log.Println("Adding DuckLake partitioning to Bronze tables...")

	successCount := 0
	for _, table := range BronzeTables {
		fullTableName := fmt.Sprintf("%s.%s.%s", c.config.CatalogName, c.config.SchemaName, table)
		partitionSQL := fmt.Sprintf(`ALTER TABLE %s SET PARTITIONED BY (ledger_range)`, fullTableName)

		if _, err := c.db.ExecContext(ctx, partitionSQL); err != nil {
			log.Printf("⚠️  Failed to partition %s: %v", table, err)
			// Don't fail - some tables might not exist or not have ledger_range
			continue
		}

		log.Printf("✅ Partitioned: %s by ledger_range", table)
		successCount++
	}

	if successCount == 0 {
		return fmt.Errorf("failed to partition all %d Bronze tables", len(BronzeTables))
	}

	if successCount < len(BronzeTables) {
		log.Printf("⚠️  Partitioned %d/%d Bronze tables successfully (some failures)", successCount, len(BronzeTables))
	} else {
		log.Printf("✅ Partitioned %d/%d Bronze tables successfully", successCount, len(BronzeTables))
	}

	return nil
}

// splitSQLStatements splits SQL content into individual statements
func splitSQLStatements(sql string) []string {
	// Split by semicolon, accounting for comments
	var statements []string
	var currentStmt strings.Builder

	lines := strings.Split(sql, "\n")
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)

		// Skip comment lines
		if strings.HasPrefix(trimmed, "--") {
			if currentStmt.Len() > 0 {
				currentStmt.WriteString("\n")
			}
			continue
		}

		currentStmt.WriteString(line)
		currentStmt.WriteString("\n")

		// Check if line ends with semicolon (end of statement)
		if strings.HasSuffix(trimmed, ");") {
			statements = append(statements, currentStmt.String())
			currentStmt.Reset()
		}
	}

	// Add any remaining content
	if currentStmt.Len() > 0 {
		statements = append(statements, currentStmt.String())
	}

	return statements
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
