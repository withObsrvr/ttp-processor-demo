package main

import (
	"database/sql"
	_ "embed"
	"fmt"
	"log"
	"strings"
)

//go:embed schema/init_silver_hot_complete.sql
var silverHotSchema string

// EnsureSilverHotSchema runs the embedded DDL against silver_hot.
// Statements are executed individually so that index failures on existing tables
// (e.g. column mismatches from schema drift) don't block table creation.
func EnsureSilverHotSchema(db *sql.DB) error {
	log.Println("🔧 Ensuring silver_hot schema is up to date...")

	statements := splitSQL(silverHotSchema)

	var created, skipped, failed int
	for _, stmt := range statements {
		stmt = strings.TrimSpace(stmt)
		if stmt == "" {
			continue
		}

		_, err := db.Exec(stmt)
		if err != nil {
			// Log but don't fail on index/grant errors — the table itself is what matters
			upper := strings.ToUpper(stmt)
			if strings.HasPrefix(upper, "CREATE TABLE") {
				return fmt.Errorf("failed to create table: %w\nStatement: %.200s", err, stmt)
			}
			log.Printf("  ⚠️  Skipped (non-fatal): %v", err)
			failed++
		} else {
			if strings.HasPrefix(strings.ToUpper(stmt), "CREATE TABLE") {
				created++
			} else {
				skipped++ // indexes, grants, inserts — counted as "other"
			}
		}
	}

	log.Printf("✅ Silver hot schema verified (%d table stmts ok, %d other stmts ok, %d skipped)", created, skipped, failed)
	return nil
}

// splitSQL splits a SQL file into individual statements on semicolons,
// respecting dollar-quoted strings ($$...$$) used in DO blocks.
func splitSQL(sql string) []string {
	var statements []string
	var current strings.Builder
	inDollarQuote := false

	for i := 0; i < len(sql); i++ {
		if sql[i] == '$' && i+1 < len(sql) && sql[i+1] == '$' {
			inDollarQuote = !inDollarQuote
			current.WriteByte(sql[i])
			current.WriteByte(sql[i+1])
			i++
			continue
		}

		if sql[i] == ';' && !inDollarQuote {
			s := strings.TrimSpace(current.String())
			if s != "" {
				statements = append(statements, s)
			}
			current.Reset()
			continue
		}

		current.WriteByte(sql[i])
	}

	// Handle trailing statement without semicolon
	if s := strings.TrimSpace(current.String()); s != "" {
		statements = append(statements, s)
	}

	return statements
}
