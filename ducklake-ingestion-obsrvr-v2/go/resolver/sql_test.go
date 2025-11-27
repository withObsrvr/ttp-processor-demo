// Package resolver provides Bronze layer data resolution and routing.
// Cycle 14: SQL Generation Tests for Incremental Versioning
package resolver

import (
	"strings"
	"testing"
	"time"
)

// TestGenerateSQL_WithVersionFilter verifies version-aware WHERE clauses.
func TestGenerateSQL_WithVersionFilter(t *testing.T) {
	r := &Resolver{
		catalogName: "obsrvr_lake_catalog_dev_4",
	}

	result := &ResolvedDataset{
		Dataset:      "core.ledgers_row_v2",
		Network:      "testnet",
		EraID:        "p23_plus",
		VersionLabel: "v1",
		Manifest: &ReadManifest{
			LedgerStart: 40000,
			LedgerEnd:   45000,
		},
	}

	options := SQLOptions{
		IncludeVersionFilter: true,
		OrderBy:              []string{"ledger_sequence"},
	}

	sql, err := r.GenerateSQL(result, options)
	if err != nil {
		t.Fatalf("GenerateSQL failed: %v", err)
	}

	// Verify critical components for incremental versioning
	checks := []struct {
		name  string
		check string
	}{
		{"has era filter", "era_id = 'p23_plus'"},
		{"has version filter", "version_label = 'v1'"},
		{"has ledger range start", "ledger_sequence >= 40000"},
		{"has ledger range end", "ledger_sequence <= 45000"},
		{"has order by", "ORDER BY ledger_sequence"},
		{"has correct table", "testnet.ledgers_row_v2"},
	}

	for _, tc := range checks {
		if !strings.Contains(sql, tc.check) {
			t.Errorf("%s: expected SQL to contain %q\nGot:\n%s", tc.name, tc.check, sql)
		}
	}

	t.Logf("Generated SQL:\n%s", sql)
}

// TestGenerateSQL_WithoutVersionFilter verifies backward compatibility.
func TestGenerateSQL_WithoutVersionFilter(t *testing.T) {
	r := &Resolver{
		catalogName: "obsrvr_lake_catalog_dev_4",
	}

	result := &ResolvedDataset{
		Dataset:      "core.ledgers_row_v2",
		Network:      "testnet",
		EraID:        "p23_plus",
		VersionLabel: "v1",
		Manifest: &ReadManifest{
			LedgerStart: 40000,
			LedgerEnd:   45000,
		},
	}

	options := SQLOptions{
		IncludeVersionFilter: false, // Explicitly disable version filter
	}

	sql, err := r.GenerateSQL(result, options)
	if err != nil {
		t.Fatalf("GenerateSQL failed: %v", err)
	}

	// Should NOT have version filters
	if strings.Contains(sql, "era_id") {
		t.Errorf("SQL should not contain era_id filter when IncludeVersionFilter=false\nGot:\n%s", sql)
	}
	if strings.Contains(sql, "version_label") {
		t.Errorf("SQL should not contain version_label filter when IncludeVersionFilter=false\nGot:\n%s", sql)
	}

	t.Logf("Generated SQL (no version filter):\n%s", sql)
}

// TestGenerateSQLSimple verifies convenience method.
func TestGenerateSQLSimple(t *testing.T) {
	r := &Resolver{
		catalogName: "obsrvr_lake_catalog_dev_4",
	}

	result := &ResolvedDataset{
		Dataset:      "core.transactions_row_v2",
		Network:      "mainnet",
		EraID:        "p23_plus",
		VersionLabel: "v2",
		Manifest: &ReadManifest{
			LedgerStart: 50000000,
			LedgerEnd:   51000000,
		},
	}

	sql, err := r.GenerateSQLSimple(result)
	if err != nil {
		t.Fatalf("GenerateSQLSimple failed: %v", err)
	}

	// Simple method should always include version filters
	if !strings.Contains(sql, "era_id = 'p23_plus'") {
		t.Errorf("GenerateSQLSimple should include era_id filter\nGot:\n%s", sql)
	}
	if !strings.Contains(sql, "version_label = 'v2'") {
		t.Errorf("GenerateSQLSimple should include version_label filter\nGot:\n%s", sql)
	}

	t.Logf("Generated SQL (simple):\n%s", sql)
}

// TestGenerateSQL_CustomColumns verifies column selection.
func TestGenerateSQL_CustomColumns(t *testing.T) {
	r := &Resolver{
		catalogName: "obsrvr_lake_catalog_dev_4",
	}

	result := &ResolvedDataset{
		Dataset:      "core.ledgers_row_v2",
		Network:      "testnet",
		EraID:        "p23_plus",
		VersionLabel: "v1",
		Manifest: &ReadManifest{
			LedgerStart: 100,
			LedgerEnd:   200,
		},
	}

	options := SQLOptions{
		Columns:              []string{"ledger_sequence", "closed_at", "tx_count"},
		IncludeVersionFilter: true,
		OrderBy:              []string{"ledger_sequence DESC"},
		Limit:                100,
	}

	sql, err := r.GenerateSQL(result, options)
	if err != nil {
		t.Fatalf("GenerateSQL failed: %v", err)
	}

	// Verify custom options
	if !strings.Contains(sql, "SELECT ledger_sequence, closed_at, tx_count") {
		t.Errorf("Expected custom column selection\nGot:\n%s", sql)
	}
	if !strings.Contains(sql, "ORDER BY ledger_sequence DESC") {
		t.Errorf("Expected DESC ordering\nGot:\n%s", sql)
	}
	if !strings.Contains(sql, "LIMIT 100") {
		t.Errorf("Expected LIMIT clause\nGot:\n%s", sql)
	}

	t.Logf("Generated SQL (custom):\n%s", sql)
}

// TestGenerateSQLForManifest verifies manifest-specific generation.
func TestGenerateSQLForManifest(t *testing.T) {
	r := &Resolver{
		catalogName: "obsrvr_lake_catalog_dev_4",
	}

	manifest := &ReadManifest{
		Dataset:     "core.ledgers_row_v2",
		EraID:       "p23_plus",
		LedgerStart: 40000,
		LedgerEnd:   45000,
		SnapshotID:  12345,
		GeneratedAt: time.Now(),
	}

	sql, err := r.GenerateSQLForManifest(manifest)
	if err != nil {
		t.Fatalf("GenerateSQLForManifest failed: %v", err)
	}

	// Should have ledger range from manifest
	if !strings.Contains(sql, "ledger_sequence >= 40000") {
		t.Errorf("Expected manifest ledger range\nGot:\n%s", sql)
	}
	if !strings.Contains(sql, "ledger_sequence <= 45000") {
		t.Errorf("Expected manifest ledger range\nGot:\n%s", sql)
	}

	t.Logf("Generated SQL (from manifest):\n%s", sql)
}

// TestGenerateSQLWithVersionOverlay verifies "latest version wins" query logic.
func TestGenerateSQLWithVersionOverlay(t *testing.T) {
	r := &Resolver{
		catalogName: "obsrvr_lake_catalog_dev_4",
	}

	result := &ResolvedDataset{
		Dataset:      "core.ledgers_row_v2",
		Network:      "testnet",
		EraID:        "p23_plus",
		VersionLabel: "v1",
		Manifest: &ReadManifest{
			LedgerStart: 40000,
			LedgerEnd:   45000,
		},
	}

	options := SQLOptions{
		UseVersionOverlay: true,
		OrderBy:           []string{"ledger_sequence"},
	}

	sql, err := r.GenerateSQLWithVersionOverlay(result, options)
	if err != nil {
		t.Fatalf("GenerateSQLWithVersionOverlay failed: %v", err)
	}

	// Verify critical components for version overlay
	checks := []struct {
		name  string
		check string
	}{
		{"has window function", "ROW_NUMBER() OVER"},
		{"partitions by ledger", "PARTITION BY ledger_sequence"},
		{"orders by era and version", "ORDER BY era_id DESC, version_label DESC"},
		{"filters to rank 1", "WHERE version_rank = 1"},
		{"has ledger range", "ledger_sequence >= 40000"},
	}

	for _, tc := range checks {
		if !strings.Contains(sql, tc.check) {
			t.Errorf("%s: expected SQL to contain %q\nGot:\n%s", tc.name, tc.check, sql)
		}
	}

	t.Logf("Generated SQL (version overlay):\n%s", sql)
}

// TestGenerateSQLWithVersionOverlay_CompositeKey verifies composite partition keys.
func TestGenerateSQLWithVersionOverlay_CompositeKey(t *testing.T) {
	r := &Resolver{
		catalogName: "obsrvr_lake_catalog_dev_4",
	}

	result := &ResolvedDataset{
		Dataset:      "core.accounts_snapshot_v1",
		Network:      "testnet",
		EraID:        "p23_plus",
		VersionLabel: "v1",
		Manifest: &ReadManifest{
			LedgerStart: 40000,
			LedgerEnd:   45000,
		},
	}

	options := SQLOptions{
		UseVersionOverlay: true,
		PartitionKey:      []string{"account_id", "ledger_sequence"},
		OrderBy:           []string{"account_id", "ledger_sequence"},
	}

	sql, err := r.GenerateSQLWithVersionOverlay(result, options)
	if err != nil {
		t.Fatalf("GenerateSQLWithVersionOverlay failed: %v", err)
	}

	// Verify composite partition key
	if !strings.Contains(sql, "PARTITION BY account_id, ledger_sequence") {
		t.Errorf("Expected composite partition key\nGot:\n%s", sql)
	}

	t.Logf("Generated SQL (composite key):\n%s", sql)
}
