package main

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
)

// createSilverTables creates all Silver tables from silver_schema.sql if they
// don't already exist. This is intentionally NON-destructive: it never drops
// the schema or its tables, so cold storage data is preserved across restarts.
//
// All CREATE TABLE statements in silver_schema.sql use CREATE TABLE IF NOT
// EXISTS, and CREATE SCHEMA IF NOT EXISTS ensures the schema is created only
// once. Running this multiple times is a no-op after the first successful run.
func (c *DuckDBClient) createSilverTables() error {
	log.Println("Ensuring Silver tables exist (non-destructive)...")

	createSchemaSQL := fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s.%s", c.config.CatalogName, c.config.SchemaName)
	if _, err := c.db.Exec(createSchemaSQL); err != nil {
		return fmt.Errorf("failed to create schema: %w", err)
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

	// Apply schema-drift migrations (idempotent ALTERs for columns added after
	// initial CREATE TABLE — CREATE TABLE IF NOT EXISTS can't update columns
	// on an existing table).
	if err := c.applySilverMigrations(); err != nil {
		return fmt.Errorf("failed to apply Silver migrations: %w", err)
	}

	return nil
}

// applySilverMigrations runs idempotent ALTER-style adjustments to the DuckLake
// silver schema for columns added after initial table creation.
//
// Each migration MUST be idempotent — this runs on every flusher start.
// Use "ADD COLUMN IF NOT EXISTS" throughout.
func (c *DuckDBClient) applySilverMigrations() error {
	migrations := []struct {
		name string
		sql  string
	}{
		{
			name: "effects.inserted_at",
			sql: fmt.Sprintf(
				`ALTER TABLE %s.%s.effects ADD COLUMN IF NOT EXISTS inserted_at TIMESTAMP`,
				c.config.CatalogName, c.config.SchemaName,
			),
		},
		{
			// Soroban i128/u128 token balances can exceed DECIMAL(38, 0)
			// max — e.g. 99999999999999997748809823456034029568 fails to
			// cast during flush. Convert to VARCHAR so the column holds
			// arbitrary-precision values losslessly.
			name: "address_balances_current.balance_raw -> VARCHAR",
			sql: fmt.Sprintf(
				`ALTER TABLE %s.%s.address_balances_current ALTER COLUMN balance_raw SET DATA TYPE VARCHAR`,
				c.config.CatalogName, c.config.SchemaName,
			),
		},
		{
			name: "contract_invocations_raw.era_id",
			sql: fmt.Sprintf(
				`ALTER TABLE %s.%s.contract_invocations_raw ADD COLUMN IF NOT EXISTS era_id VARCHAR`,
				c.config.CatalogName, c.config.SchemaName,
			),
		},
		{
			name: "contract_invocations_raw.version_label",
			sql: fmt.Sprintf(
				`ALTER TABLE %s.%s.contract_invocations_raw ADD COLUMN IF NOT EXISTS version_label VARCHAR`,
				c.config.CatalogName, c.config.SchemaName,
			),
		},
		{
			name: "contract_metadata.era_id",
			sql: fmt.Sprintf(
				`ALTER TABLE %s.%s.contract_metadata ADD COLUMN IF NOT EXISTS era_id VARCHAR`,
				c.config.CatalogName, c.config.SchemaName,
			),
		},
		{
			name: "contract_metadata.version_label",
			sql: fmt.Sprintf(
				`ALTER TABLE %s.%s.contract_metadata ADD COLUMN IF NOT EXISTS version_label VARCHAR`,
				c.config.CatalogName, c.config.SchemaName,
			),
		},
		{
			name: "trustlines_current.era_id",
			sql: fmt.Sprintf(
				`ALTER TABLE %s.%s.trustlines_current ADD COLUMN IF NOT EXISTS era_id VARCHAR`,
				c.config.CatalogName, c.config.SchemaName,
			),
		},
		{
			name: "trustlines_current.version_label",
			sql: fmt.Sprintf(
				`ALTER TABLE %s.%s.trustlines_current ADD COLUMN IF NOT EXISTS version_label VARCHAR`,
				c.config.CatalogName, c.config.SchemaName,
			),
		},
		{
			name: "offers_current.era_id",
			sql: fmt.Sprintf(
				`ALTER TABLE %s.%s.offers_current ADD COLUMN IF NOT EXISTS era_id VARCHAR`,
				c.config.CatalogName, c.config.SchemaName,
			),
		},
		{
			name: "offers_current.version_label",
			sql: fmt.Sprintf(
				`ALTER TABLE %s.%s.offers_current ADD COLUMN IF NOT EXISTS version_label VARCHAR`,
				c.config.CatalogName, c.config.SchemaName,
			),
		},
	}

	for _, m := range migrations {
		if _, err := c.db.Exec(m.sql); err != nil {
			log.Printf("   ⚠️  Migration %q failed (non-fatal): %v", m.name, err)
			continue
		}
		log.Printf("   Migration %q applied", m.name)
	}
	return nil
}

// tablesWithLedgerRange lists tables that have a ledger_range column and can be partitioned.
// Tables without ledger_range (token_registry, contract_metadata, semantic_* tables)
// must NOT be partitioned — attempting it triggers a DuckDB FATAL error that invalidates
// the entire connection.
var tablesWithLedgerRange = map[string]bool{
	"accounts_snapshot":                   true,
	"accounts_current":                    true,
	"trustlines_snapshot":                 true,
	"trustlines_current":                  true,
	"offers_snapshot":                     true,
	"offers_current":                      true,
	"account_signers_snapshot":            true,
	"claimable_balances_current":          true,
	"claimable_balances_snapshot":         true,
	"contract_data_current":               true,
	"enriched_history_operations":         true,
	"enriched_history_operations_soroban": true,
	"token_transfers_raw":                 true,
	"contract_invocations_raw":            true,
	"effects":                             true,
	"evicted_keys":                        true,
	"trades":                              true,
	"restored_keys":                       true,
	"native_balances_current":             true,
	"ttl_current":                         true,
	// address_balances_current has no ledger_range column — deliberately omitted
}

// partitionSilverTables adds DuckLake partitioning to Silver tables that have ledger_range
// This organizes Parquet files into partition folders for efficient queries
func (c *DuckDBClient) partitionSilverTables() error {
	log.Println("Adding DuckLake partitioning to Silver tables...")

	successCount := 0
	skippedCount := 0
	for _, table := range SilverTables {
		if !tablesWithLedgerRange[table] {
			log.Printf("   Skipping partition for %s (no ledger_range column)", table)
			skippedCount++
			continue
		}

		fullTableName := fmt.Sprintf("%s.%s.%s", c.config.CatalogName, c.config.SchemaName, table)
		partitionSQL := fmt.Sprintf(`ALTER TABLE %s SET PARTITIONED BY (ledger_range)`, fullTableName)

		if _, err := c.db.Exec(partitionSQL); err != nil {
			log.Printf("   Failed to partition %s: %v", table, err)
			continue
		}

		log.Printf("   Partitioned: %s by ledger_range", table)
		successCount++
	}

	partitionable := len(SilverTables) - skippedCount
	if partitionable > 0 && successCount == 0 {
		return fmt.Errorf("failed to partition all %d partitionable Silver tables", partitionable)
	}
	log.Printf("   Partitioned %d/%d tables (%d skipped, no ledger_range)", successCount, partitionable, skippedCount)

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
