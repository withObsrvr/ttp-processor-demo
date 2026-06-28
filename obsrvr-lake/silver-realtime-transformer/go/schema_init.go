package main

import (
	"database/sql"
	_ "embed"
	"fmt"
	"log"
	"strings"
	"time"
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

	log.Printf("✅ Silver hot schema verified (%d table stmts ok, %d other stmts ok, %d non-fatal errors)", created, skipped, failed)
	return nil
}

// participantIndex is a partial index on a participant column of enriched_history_operations that
// the hot+cold account-history federation needs (WHERE source_account/destination/from_account/
// to_address/address = $1). Without them a receive-heavy account seq-scans the ~175M-row table.
// They are built CONCURRENTLY in the background — NOT inline in the startup schema — because a plain
// CREATE INDEX takes a write-blocking lock and would freeze the realtime transformer/ingesters while
// it scans the whole table. See docs/enriched-operations-participant-indexes-shaped-fix.md.
type participantIndex struct {
	name string
	col  string
}

// EnsureSilverHotIndexes idempotently builds the enriched_history_operations participant indexes
// CONCURRENTLY. Best-effort: every failure is logged and skipped — a missing index only makes account
// history slow, it must never crash the transformer or block ingestion. CONCURRENTLY builds on a large
// table can take minutes, so this MUST be run in the background, never on the startup critical path.
func EnsureSilverHotIndexes(db *sql.DB) {
	want := []participantIndex{
		{"idx_enriched_ops_destination", "destination"},
		{"idx_enriched_ops_from_account", "from_account"},
		{"idx_enriched_ops_to_address", "to_address"},
		{"idx_enriched_ops_address", "address"},
	}
	for _, x := range want {
		// A CONCURRENTLY build interrupted by a restart leaves an INVALID index that
		// CREATE ... IF NOT EXISTS would then skip forever — drop it first.
		var invalid bool
		if err := db.QueryRow(
			`SELECT EXISTS (SELECT 1 FROM pg_class c JOIN pg_index i ON i.indexrelid = c.oid
			   WHERE c.relname = $1 AND NOT i.indisvalid)`, x.name,
		).Scan(&invalid); err == nil && invalid {
			log.Printf("silver-index: dropping invalid leftover %s", x.name)
			if _, err := db.Exec(fmt.Sprintf("DROP INDEX CONCURRENTLY IF EXISTS %s", x.name)); err != nil {
				log.Printf("silver-index: drop invalid %s failed (non-fatal): %v", x.name, err)
			}
		}

		start := time.Now()
		// Partial (NOT NULL): these columns are null for most op types, so the index stays small, and
		// `col = $1` implies `col IS NOT NULL` so the planner still uses it.
		stmt := fmt.Sprintf(
			"CREATE INDEX CONCURRENTLY IF NOT EXISTS %s ON enriched_history_operations(%s) WHERE %s IS NOT NULL",
			x.name, x.col, x.col)
		if _, err := db.Exec(stmt); err != nil {
			log.Printf("silver-index: ensure %s failed (non-fatal — account history stays slow until present): %v", x.name, err)
			continue
		}
		log.Printf("silver-index: %s ready in %s", x.name, time.Since(start).Round(time.Second))
	}
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
