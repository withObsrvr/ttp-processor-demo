package main

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
)

// createSilverTables creates all 17 Silver tables from silver_schema.sql
func (c *DuckDBClient) createSilverTables() error {
	log.Println("Creating Silver tables from silver_schema.sql...")

	// Drop existing Silver schema and recreate it to ensure clean state
	dropSchemaSQL := fmt.Sprintf("DROP SCHEMA IF EXISTS %s.%s CASCADE", c.config.CatalogName, c.config.SchemaName)
	if _, err := c.db.Exec(dropSchemaSQL); err != nil {
		log.Printf("Warning: Failed to drop existing schema: %v", err)
	}

	createSchemaSQL := fmt.Sprintf("CREATE SCHEMA %s.%s", c.config.CatalogName, c.config.SchemaName)
	if _, err := c.db.Exec(createSchemaSQL); err != nil {
		return fmt.Errorf("failed to recreate schema: %w", err)
	}

	// Read the silver_schema.sql file
	// Try multiple paths: current directory (Docker), schema/ subdirectory, parent
	var content []byte
	var err error

	schemaPaths := []string{
		"silver_schema.sql",
		"schema/silver_schema.sql",
		filepath.Join("..", "schema", "silver_schema.sql"),
	}

	for _, schemaPath := range schemaPaths {
		content, err = os.ReadFile(schemaPath)
		if err == nil {
			log.Printf("Loaded schema from: %s", schemaPath)
			break
		}
	}

	if err != nil {
		return fmt.Errorf("failed to read silver_schema.sql from any location: %w", err)
	}

	schemaSQL := string(content)

	// Replace hardcoded testnet_catalog.silver. with actual catalog and schema names
	schemaSQL = strings.ReplaceAll(schemaSQL, "testnet_catalog.silver.", fmt.Sprintf("%s.%s.", c.config.CatalogName, c.config.SchemaName))

	// Split into individual statements
	statements := splitSQLStatements(schemaSQL)

	tableCount := 0
	for _, stmt := range statements {
		stmt = strings.TrimSpace(stmt)
		if stmt == "" || strings.HasPrefix(stmt, "--") {
			continue
		}

		// Execute CREATE TABLE and COMMENT ON statements
		if strings.Contains(stmt, "CREATE TABLE") || strings.Contains(stmt, "COMMENT ON TABLE") {
			if _, err := c.db.Exec(stmt); err != nil {
				return fmt.Errorf("failed to execute statement: %w\nSQL: %s", err, stmt[:min(len(stmt), 200)])
			}
			if strings.Contains(stmt, "CREATE TABLE") {
				tableCount++
			}
		}
	}

	log.Printf("✅ Created %d Silver tables successfully", tableCount)

	// Add partitioning to all tables
	if err := c.partitionSilverTables(); err != nil {
		return fmt.Errorf("failed to partition Silver tables: %w", err)
	}

	return nil
}

// partitionSilverTables adds DuckLake partitioning to all Silver tables by ledger_range
// This organizes Parquet files into partition folders for efficient queries
func (c *DuckDBClient) partitionSilverTables() error {
	log.Println("Adding DuckLake partitioning to Silver tables...")

	// Tables with ledger_range column
	tablesWithLedgerRange := []string{
		// Snapshot tables (SCD Type 2)
		"accounts_snapshot",
		"trustlines_snapshot",
		"offers_snapshot",
		"account_signers_snapshot",

		// Current state tables
		"accounts_current",
		"trustlines_current",
		"offers_current",
		"account_signers_current",
		"claimable_balances_current",
		"contract_data_current",

		// Enriched operations tables
		"enriched_history_operations",
		"enriched_history_operations_soroban",
		"token_transfers_raw",
	}

	successCount := 0
	for _, table := range tablesWithLedgerRange {
		fullTableName := fmt.Sprintf("%s.%s.%s", c.config.CatalogName, c.config.SchemaName, table)
		partitionSQL := fmt.Sprintf(`ALTER TABLE %s SET PARTITIONED BY (ledger_range)`, fullTableName)

		if _, err := c.db.Exec(partitionSQL); err != nil {
			log.Printf("⚠️  Failed to partition %s: %v", table, err)
			// Don't fail - some tables might not exist or not have ledger_range
			continue
		}

		log.Printf("✅ Partitioned: %s by ledger_range", table)
		successCount++
	}

	log.Printf("✅ Partitioned %d/%d Silver tables successfully", successCount, len(tablesWithLedgerRange))
	return nil
}

// splitSQLStatements splits SQL content into individual statements
func splitSQLStatements(sql string) []string {
	var statements []string
	var currentStmt strings.Builder

	lines := strings.Split(sql, "\n")
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)

		// Skip comment-only lines
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
