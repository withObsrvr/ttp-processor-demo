package main

import (
	"errors"
	"log"
	"strings"

	"github.com/lib/pq"
)

// isSchemaGapError reports whether err is a genuine missing-column/missing-table
// error — PostgreSQL SQLSTATE 42703 (undefined_column) / 42P01 (undefined_table),
// or a DuckDB binder/catalog resolution failure. Fallback paths must key on this
// rather than substring-matching column names: permission errors, constraint
// violations, and timeouts all mention column and table names too, and treating
// those as "schema not migrated yet" silently reroutes traffic to slower or
// staler tiers.
func isSchemaGapError(err error) bool {
	if err == nil {
		return false
	}
	var pqErr *pq.Error
	if errors.As(err, &pqErr) {
		return pqErr.Code == "42703" || pqErr.Code == "42P01"
	}
	msg := strings.ToLower(err.Error())
	// DuckDB (and PG errors flattened to text by wrapping) expose only the
	// message. DuckDB reports unresolved columns/tables as Binder/Catalog errors.
	if strings.Contains(msg, "binder error") || strings.Contains(msg, "catalog error") {
		return strings.Contains(msg, "does not exist") ||
			strings.Contains(msg, "not found") ||
			strings.Contains(msg, "does not have a column")
	}
	if strings.Contains(msg, "sqlstate 42703") || strings.Contains(msg, "sqlstate 42p01") {
		return true
	}
	return (strings.Contains(msg, "column") && strings.Contains(msg, "does not exist")) ||
		(strings.Contains(msg, "relation") && strings.Contains(msg, "does not exist"))
}

// logTierFallback records that a request abandoned one storage tier for another.
// Every fallback must leave a trace: a systematically failing tier (revoked
// grants, missed migration, wedged attach) is otherwise invisible except as
// elevated latency or quietly degraded responses.
func logTierFallback(component, from, to string, err error) {
	log.Printf("%s tier_fallback from=%s to=%s err=%v", component, from, to, err)
}
