package main

import (
	"os"
	"regexp"
	"strings"
	"testing"
)

// These tests pin projector INSERT column lists against the canonical serving
// schema so a drifted column breaks the build instead of silently breaking the
// serving read path in production (the query-api unit tests mock these tables
// and cannot catch the drift).

func servingSchemaTableColumns(t *testing.T, table string) (map[string]bool, map[string]bool) {
	t.Helper()
	raw, err := os.ReadFile("schema/serving_schema.sql")
	if err != nil {
		t.Fatalf("read serving schema: %v", err)
	}
	pattern := regexp.MustCompile(`(?s)create table if not exists ` + regexp.QuoteMeta(table) + `\s*\((.*?)\n\);`)
	match := pattern.FindSubmatch(raw)
	if match == nil {
		t.Fatalf("table %s not found in serving_schema.sql", table)
	}
	columns := map[string]bool{}
	requiredNoDefault := map[string]bool{}
	for _, line := range strings.Split(string(match[1]), "\n") {
		line = strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(line), ","))
		if line == "" || strings.HasPrefix(line, "primary key") || strings.HasPrefix(line, "constraint") || strings.HasPrefix(line, "unique") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		name := fields[0]
		columns[name] = true
		lower := strings.ToLower(line)
		if strings.Contains(lower, "not null") && !strings.Contains(lower, "default") {
			requiredNoDefault[name] = true
		}
	}
	return columns, requiredNoDefault
}

func upsertInsertColumns(t *testing.T, upsertSQL, table string) []string {
	t.Helper()
	pattern := regexp.MustCompile(`(?s)INSERT INTO ` + regexp.QuoteMeta(table) + `\s*\((.*?)\)\s*VALUES`)
	match := pattern.FindStringSubmatch(upsertSQL)
	if match == nil {
		t.Fatalf("could not extract insert column list for %s", table)
	}
	var out []string
	for _, col := range strings.Split(match[1], ",") {
		col = strings.TrimSpace(col)
		if col != "" {
			out = append(out, col)
		}
	}
	return out
}

func assertUpsertMatchesSchema(t *testing.T, upsertSQL, table string) {
	t.Helper()
	schemaColumns, required := servingSchemaTableColumns(t, table)
	inserted := upsertInsertColumns(t, upsertSQL, table)
	insertedSet := map[string]bool{}
	for _, col := range inserted {
		if !schemaColumns[col] {
			t.Errorf("insert column %q does not exist in %s schema", col, table)
		}
		insertedSet[col] = true
	}
	for col := range required {
		if !insertedSet[col] {
			t.Errorf("schema column %q is NOT NULL without default but missing from the insert list", col)
		}
	}
}

func TestEffectsByAccountUpsertColumnsMatchServingSchema(t *testing.T) {
	assertUpsertMatchesSchema(t, effectsByAccountUpsertSQL, "serving.sv_effects_by_account")
}

func TestTransactionsRecentUpsertColumnsMatchServingSchema(t *testing.T) {
	assertUpsertMatchesSchema(t, transactionsRecentUpsertSQL, "serving.sv_transactions_recent")
}

func TestAccountBalancesIncludedInServingUniqueIndexSelfHeal(t *testing.T) {
	raw, err := os.ReadFile("schema/serving_schema.sql")
	if err != nil {
		t.Fatalf("read serving schema: %v", err)
	}
	expected := "('serving.sv_account_balances_current','sv_account_balances_current_uq',    'account_id, asset_key')"
	if !strings.Contains(string(raw), expected) {
		t.Fatalf("account balance ON CONFLICT key is missing from serving unique-index self-heal")
	}
}
