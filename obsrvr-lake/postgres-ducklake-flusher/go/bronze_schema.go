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

// applyBronzeMigrations runs idempotent ALTER-style adjustments to the
// DuckLake bronze schema for columns added AFTER initial catalog creation.
//
// Why this exists: v3_bronze_schema.sql is applied via CREATE TABLE IF NOT
// EXISTS, so once a table is created in the catalog its column set is fixed
// from that file's perspective. New columns must therefore be applied with
// ALTER TABLE against the live catalog.
//
// Each migration here MUST be idempotent — this runs on every flusher start.
// Use "ADD COLUMN IF NOT EXISTS" and guard DDL with existence checks where
// IF NOT EXISTS isn't supported by the DuckLake version.
func (c *DuckDBClient) applyBronzeMigrations(ctx context.Context) error {
	type migration struct {
		name string
		sql  string
	}
	qualified := fmt.Sprintf("%s.%s", c.config.CatalogName, c.config.SchemaName)

	// Ordered list — append only; never reorder, never remove.
	//
	// WARNING: columns here MUST land in the same tail order as their
	// PostgreSQL counterparts (ALTER TABLE ADD COLUMN appends at the end in
	// both systems). The flusher flushes operations_row_v2 via SELECT * from
	// postgres_scan, so column positions must align.
	migrations := []migration{
		{
			name: "operations_row_v2.soroban_auth_credentials_types",
			sql: fmt.Sprintf(
				`ALTER TABLE %s.operations_row_v2 ADD COLUMN IF NOT EXISTS soroban_auth_credentials_types VARCHAR`,
				qualified,
			),
		},
		{
			name: "operations_row_v2.soroban_auth_addresses",
			sql: fmt.Sprintf(
				`ALTER TABLE %s.operations_row_v2 ADD COLUMN IF NOT EXISTS soroban_auth_addresses VARCHAR`,
				qualified,
			),
		},
	}

	for _, m := range migrations {
		if _, err := c.db.ExecContext(ctx, m.sql); err != nil {
			// DuckLake may report "already exists" even with IF NOT EXISTS on
			// older extension builds; log-and-continue so a single bad
			// migration doesn't block the flusher from starting.
			errStr := err.Error()
			if strings.Contains(errStr, "already exists") ||
				strings.Contains(errStr, "Duplicate column") {
				log.Printf("Migration %q: column already present, skipping", m.name)
				continue
			}
			return fmt.Errorf("migration %q failed: %w", m.name, err)
		}
		log.Printf("Migration %q applied", m.name)
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
