package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
)

// createBronzeTables creates all 19 Bronze tables from v3_bronze_schema.sql
func (c *DuckDBClient) createBronzeTables(ctx context.Context) error {
	log.Println("Creating Bronze tables from v3_bronze_schema.sql...")

	// Drop existing Bronze schema and recreate it to ensure clean state
	dropSchemaSQL := fmt.Sprintf("DROP SCHEMA IF EXISTS %s.%s CASCADE", c.config.CatalogName, c.config.SchemaName)
	if _, err := c.db.ExecContext(ctx, dropSchemaSQL); err != nil {
		log.Printf("Warning: Failed to drop existing schema: %v", err)
	}

	createSchemaSQL := fmt.Sprintf("CREATE SCHEMA %s.%s", c.config.CatalogName, c.config.SchemaName)
	if _, err := c.db.ExecContext(ctx, createSchemaSQL); err != nil {
		return fmt.Errorf("failed to recreate schema: %w", err)
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

	// Split into individual CREATE TABLE statements
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

	log.Printf("Created %d Bronze tables successfully", tableCount)
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
