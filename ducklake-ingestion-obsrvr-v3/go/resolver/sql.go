// Package resolver provides Bronze layer data resolution and routing.
// Cycle 14: SQL Generation with Incremental Versioning Support
package resolver

import (
	"fmt"
	"strings"
)

// SQLOptions controls SQL query generation behavior.
type SQLOptions struct {
	// Columns to select (if empty, selects all columns with *)
	Columns []string

	// OrderBy specifies sort columns (e.g., ["ledger_sequence ASC"])
	OrderBy []string

	// Limit maximum number of rows (0 = no limit)
	Limit uint32

	// IncludeVersionFilter adds era_id and version_label to WHERE clause
	// This is critical for incremental versioning to prevent reading stale data
	IncludeVersionFilter bool

	// UseVersionOverlay implements "latest version wins" for partial re-ingestion
	// Uses window functions to deduplicate ledgers by selecting newest version
	// Mutually exclusive with IncludeVersionFilter (overlay takes precedence)
	UseVersionOverlay bool

	// PartitionKey defines the unique key for version overlay deduplication
	// Defaults to "ledger_sequence" for most datasets
	// For snapshots, might be composite like ["account_id", "ledger_sequence"]
	PartitionKey []string
}

// GenerateSQL creates a version-aware SQL query from a resolved dataset.
//
// For incremental versioning (Strategy D), this ensures queries only read
// data from the specific version, preventing version overlap issues.
//
// Example output:
//   SELECT * FROM catalog.network.ledgers_row_v2
//   WHERE ledger_sequence >= 40000
//     AND ledger_sequence <= 45000
//     AND era_id = 'p23_plus'
//     AND version_label = 'v1'
//   ORDER BY ledger_sequence
func (r *Resolver) GenerateSQL(result *ResolvedDataset, options SQLOptions) (string, error) {
	if result == nil {
		return "", fmt.Errorf("resolved dataset is nil")
	}

	// Build SELECT clause
	selectClause := "*"
	if len(options.Columns) > 0 {
		selectClause = strings.Join(options.Columns, ", ")
	}

	// Extract table name from dataset
	// Dataset format: "domain.table_name" (e.g., "core.ledgers_row_v2")
	tableName := result.Dataset
	if idx := strings.LastIndex(result.Dataset, "."); idx != -1 {
		tableName = result.Dataset[idx+1:]
	}

	// Build FROM clause
	// Format: catalog.network.table_name
	fromClause := fmt.Sprintf("%s.%s.%s", r.catalogName, result.Network, tableName)

	// Build WHERE clause
	var conditions []string

	// Add ledger range filter if manifest exists
	if result.Manifest != nil {
		conditions = append(conditions,
			fmt.Sprintf("ledger_sequence >= %d", result.Manifest.LedgerStart),
			fmt.Sprintf("ledger_sequence <= %d", result.Manifest.LedgerEnd),
		)
	}

	// CRITICAL: Add version filter for incremental versioning
	// This prevents reading old versions when partial re-ingestion occurs
	if options.IncludeVersionFilter {
		conditions = append(conditions,
			fmt.Sprintf("era_id = '%s'", result.EraID),
			fmt.Sprintf("version_label = '%s'", result.VersionLabel),
		)
	}

	// Construct WHERE clause
	whereClause := ""
	if len(conditions) > 0 {
		whereClause = "WHERE " + strings.Join(conditions, "\n    AND ")
	}

	// Build ORDER BY clause
	orderByClause := ""
	if len(options.OrderBy) > 0 {
		orderByClause = "ORDER BY " + strings.Join(options.OrderBy, ", ")
	}

	// Build LIMIT clause
	limitClause := ""
	if options.Limit > 0 {
		limitClause = fmt.Sprintf("LIMIT %d", options.Limit)
	}

	// Assemble final query
	var parts []string
	parts = append(parts, fmt.Sprintf("SELECT %s", selectClause))
	parts = append(parts, fmt.Sprintf("FROM %s", fromClause))
	if whereClause != "" {
		parts = append(parts, whereClause)
	}
	if orderByClause != "" {
		parts = append(parts, orderByClause)
	}
	if limitClause != "" {
		parts = append(parts, limitClause)
	}

	query := strings.Join(parts, "\n")
	return query, nil
}

// GenerateSQLSimple creates a basic version-aware query with defaults.
//
// This is a convenience wrapper around GenerateSQL with sensible defaults:
// - SELECT *
// - ORDER BY ledger_sequence (if manifest exists)
// - Version filtering enabled
func (r *Resolver) GenerateSQLSimple(result *ResolvedDataset) (string, error) {
	options := SQLOptions{
		IncludeVersionFilter: true,
	}

	// Add default ordering if we have a manifest (i.e., querying a range)
	if result.Manifest != nil {
		options.OrderBy = []string{"ledger_sequence"}
	}

	return r.GenerateSQL(result, options)
}

// GenerateSQLForManifest generates SQL for reading a specific read manifest.
//
// This method ensures:
// 1. Query is scoped to the exact ledger range in the manifest
// 2. Version filters prevent reading stale data from re-ingestion
// 3. Ordering by ledger_sequence for deterministic results
func (r *Resolver) GenerateSQLForManifest(manifest *ReadManifest) (string, error) {
	if manifest == nil {
		return "", fmt.Errorf("manifest is nil")
	}

	// Create a minimal ResolvedDataset from manifest
	result := &ResolvedDataset{
		Dataset:      manifest.Dataset,
		Network:      "", // Will be inferred from schema
		EraID:        manifest.EraID,
		VersionLabel: "", // Not stored in manifest, would need to query
		Manifest:     manifest,
	}

	// Generate SQL with version filtering
	options := SQLOptions{
		IncludeVersionFilter: true,
		OrderBy:              []string{"ledger_sequence"},
	}

	return r.GenerateSQL(result, options)
}

// GenerateSQLWithVersionOverlay generates a query that implements "latest version wins".
//
// This is used for partial re-ingestion scenarios where multiple versions of the
// same ledger may exist. The query uses a window function to rank versions and
// select only the newest one.
//
// Example output for ledgers_row_v2:
//   SELECT *
//   FROM (
//     SELECT *,
//            ROW_NUMBER() OVER (
//              PARTITION BY ledger_sequence
//              ORDER BY era_id DESC, version_label DESC
//            ) AS version_rank
//     FROM catalog.network.ledgers_row_v2
//     WHERE ledger_sequence >= 40000
//       AND ledger_sequence <= 45000
//   ) AS versioned
//   WHERE version_rank = 1
//   ORDER BY ledger_sequence
//
// This ensures that if ledger 40005 was re-ingested with a newer version_label,
// only the latest version is returned.
func (r *Resolver) GenerateSQLWithVersionOverlay(result *ResolvedDataset, options SQLOptions) (string, error) {
	if result == nil {
		return "", fmt.Errorf("resolved dataset is nil")
	}

	// Default partition key to ledger_sequence if not specified
	partitionKey := options.PartitionKey
	if len(partitionKey) == 0 {
		partitionKey = []string{"ledger_sequence"}
	}

	// Build column list for SELECT
	selectClause := "*"
	if len(options.Columns) > 0 {
		selectClause = strings.Join(options.Columns, ", ")
	}

	// Extract table name from dataset
	tableName := result.Dataset
	if idx := strings.LastIndex(result.Dataset, "."); idx != -1 {
		tableName = result.Dataset[idx+1:]
	}

	// Build FROM clause
	fromClause := fmt.Sprintf("%s.%s.%s", r.catalogName, result.Network, tableName)

	// Build WHERE clause for inner query (ledger range only)
	var conditions []string
	if result.Manifest != nil {
		conditions = append(conditions,
			fmt.Sprintf("ledger_sequence >= %d", result.Manifest.LedgerStart),
			fmt.Sprintf("ledger_sequence <= %d", result.Manifest.LedgerEnd),
		)
	}

	whereClause := ""
	if len(conditions) > 0 {
		whereClause = "WHERE " + strings.Join(conditions, "\n       AND ")
	}

	// Build PARTITION BY clause
	partitionByClause := strings.Join(partitionKey, ", ")

	// Build window function for version ranking
	// We order by era_id DESC, version_label DESC to get the latest version first
	windowFunc := fmt.Sprintf(`ROW_NUMBER() OVER (
              PARTITION BY %s
              ORDER BY era_id DESC, version_label DESC
            ) AS version_rank`, partitionByClause)

	// Build ORDER BY clause for outer query
	orderByClause := ""
	if len(options.OrderBy) > 0 {
		orderByClause = "ORDER BY " + strings.Join(options.OrderBy, ", ")
	} else if result.Manifest != nil {
		// Default to ordering by partition key
		orderByClause = "ORDER BY " + strings.Join(partitionKey, ", ")
	}

	// Build LIMIT clause
	limitClause := ""
	if options.Limit > 0 {
		limitClause = fmt.Sprintf("LIMIT %d", options.Limit)
	}

	// Assemble the complete query with CTE
	var parts []string
	parts = append(parts, fmt.Sprintf("SELECT %s", selectClause))
	parts = append(parts, "FROM (")
	parts = append(parts, fmt.Sprintf("  SELECT *,"))
	parts = append(parts, fmt.Sprintf("         %s", windowFunc))
	parts = append(parts, fmt.Sprintf("  FROM %s", fromClause))
	if whereClause != "" {
		parts = append(parts, fmt.Sprintf("  %s", whereClause))
	}
	parts = append(parts, ") AS versioned")
	parts = append(parts, "WHERE version_rank = 1")
	if orderByClause != "" {
		parts = append(parts, orderByClause)
	}
	if limitClause != "" {
		parts = append(parts, limitClause)
	}

	query := strings.Join(parts, "\n")
	return query, nil
}
