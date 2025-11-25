// Package resolver provides Bronze layer data resolution and routing.
// Cycle 5: Bronze Resolver Library
package resolver

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

// Resolver routes dataset queries to the correct era and provides metadata.
type Resolver struct {
	db           *sql.DB
	catalogName  string
	schemaName   string
	cache        *cache
	cacheEnabled bool
}

// Config holds configuration for the resolver.
type Config struct {
	// DB is the DuckDB connection with DuckLake catalog attached
	DB *sql.DB

	// CatalogName is the DuckLake catalog name
	CatalogName string

	// SchemaName is the metadata schema name (e.g., "testnet", "mainnet")
	SchemaName string

	// CacheEnabled enables in-memory caching of era maps and coverage
	CacheEnabled bool

	// CacheTTL is how long to cache era maps (default: 5 minutes)
	CacheTTL time.Duration
}

// New creates a new Resolver instance.
func New(cfg Config) (*Resolver, error) {
	if cfg.DB == nil {
		return nil, fmt.Errorf("db connection is required")
	}
	if cfg.CatalogName == "" {
		return nil, fmt.Errorf("catalog_name is required")
	}
	if cfg.SchemaName == "" {
		return nil, fmt.Errorf("schema_name is required")
	}

	// Default cache TTL
	if cfg.CacheTTL == 0 {
		cfg.CacheTTL = 5 * time.Minute
	}

	r := &Resolver{
		db:           cfg.DB,
		catalogName:  cfg.CatalogName,
		schemaName:   cfg.SchemaName,
		cacheEnabled: cfg.CacheEnabled,
	}

	if cfg.CacheEnabled {
		r.cache = newCache(cfg.CacheTTL)
	}

	return r, nil
}

// ResolveDataset resolves a dataset query based on the intent.
// This is the main entry point for consumers.
func (r *Resolver) ResolveDataset(ctx context.Context, dataset string, intent Intent) (*ResolvedDataset, error) {
	// Set default network if not specified
	if intent.Network == "" {
		intent.Network = r.schemaName
	}

	// 1. Pick the correct era
	era, err := r.pickEra(ctx, intent)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve era: %w", err)
	}

	// 2. Get coverage for the dataset/era
	coverage, err := r.GetCoverage(ctx, dataset, era.EraID, era.VersionLabel)
	if err != nil {
		return nil, fmt.Errorf("failed to get coverage: %w", err)
	}

	// 3. Get dataset metadata
	ds, err := r.getDataset(ctx, dataset)
	if err != nil {
		return nil, fmt.Errorf("failed to get dataset metadata: %w", err)
	}

	resolved := &ResolvedDataset{
		Dataset:      dataset,
		Network:      intent.Network,
		EraID:        era.EraID,
		VersionLabel: era.VersionLabel,
		Coverage:     *coverage,
		PASVerified:  coverage.LastVerified == coverage.TailLedger,
	}

	// Add dataset metadata if available
	if ds.SchemaHash != nil {
		resolved.SchemaHash = *ds.SchemaHash
	}
	if ds.Compatibility != nil {
		resolved.Compatibility = *ds.Compatibility
	}

	// 4. If range was specified, generate read manifest
	if intent.Range != nil {
		manifest, err := r.GetReadManifest(ctx, dataset, era.EraID, era.VersionLabel, *intent.Range)
		if err != nil {
			return nil, fmt.Errorf("failed to generate manifest: %w", err)
		}
		resolved.Manifest = manifest

		// Check PAS verification for the requested range
		if intent.StrictPAS && !r.isRangePASVerified(manifest, coverage) {
			return nil, fmt.Errorf("range %d-%d is not fully PAS verified", intent.Range.Start, intent.Range.End)
		}
	}

	return resolved, nil
}

// GetEraMap returns all eras for a network with their boundaries.
func (r *Resolver) GetEraMap(ctx context.Context, network string) ([]Era, error) {
	// Check cache first
	if r.cacheEnabled {
		if eras, ok := r.cache.getEraMap(network); ok {
			return eras, nil
		}
	}

	eras, err := r.listEras(ctx, network)
	if err != nil {
		return nil, err
	}

	// Cache the result
	if r.cacheEnabled {
		r.cache.setEraMap(network, eras)
	}

	return eras, nil
}

// ListDatasets returns all available datasets, optionally filtered by era.
func (r *Resolver) ListDatasets(ctx context.Context, network string, eraID *string) ([]Dataset, error) {
	query := fmt.Sprintf(`
		SELECT dataset, tier, domain, major_version, current_minor_version,
		       owner, purpose, grain, era_id, version_label, schema_hash,
		       compatibility, created_at, updated_at
		FROM %s.%s._meta_datasets
		WHERE 1=1
	`, r.catalogName, r.schemaName)

	args := []interface{}{}

	// Filter by era if specified
	if eraID != nil {
		query += " AND era_id = ?"
		args = append(args, *eraID)
	}

	query += " ORDER BY dataset"

	rows, err := r.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to list datasets: %w", err)
	}
	defer rows.Close()

	var datasets []Dataset
	for rows.Next() {
		var ds Dataset
		var eraID, versionLabel, schemaHash, compatibility sql.NullString
		var owner, purpose sql.NullString

		err := rows.Scan(
			&ds.Name, &ds.Tier, &ds.Domain, &ds.MajorVersion, &ds.MinorVersion,
			&owner, &purpose, &ds.Grain, &eraID, &versionLabel, &schemaHash,
			&compatibility, &ds.CreatedAt, &ds.UpdatedAt,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan dataset: %w", err)
		}

		if owner.Valid {
			ds.Owner = owner.String
		}
		if purpose.Valid {
			ds.Purpose = purpose.String
		}
		if eraID.Valid {
			ds.EraID = &eraID.String
		}
		if versionLabel.Valid {
			ds.VersionLabel = &versionLabel.String
		}
		if schemaHash.Valid {
			ds.SchemaHash = &schemaHash.String
		}
		if compatibility.Valid {
			ds.Compatibility = &compatibility.String
		}

		datasets = append(datasets, ds)
	}

	return datasets, nil
}

// isRangePASVerified checks if the entire range in the manifest is PAS verified.
func (r *Resolver) isRangePASVerified(manifest *ReadManifest, coverage *Coverage) bool {
	return manifest.LedgerEnd <= coverage.LastVerified
}

// getDataset retrieves dataset metadata.
func (r *Resolver) getDataset(ctx context.Context, datasetName string) (*Dataset, error) {
	query := fmt.Sprintf(`
		SELECT dataset, tier, domain, major_version, current_minor_version,
		       owner, purpose, grain, era_id, version_label, schema_hash,
		       compatibility, created_at, updated_at
		FROM %s.%s._meta_datasets
		WHERE dataset = ?
		LIMIT 1
	`, r.catalogName, r.schemaName)

	var ds Dataset
	var eraID, versionLabel, schemaHash, compatibility sql.NullString
	var owner, purpose sql.NullString

	err := r.db.QueryRowContext(ctx, query, datasetName).Scan(
		&ds.Name, &ds.Tier, &ds.Domain, &ds.MajorVersion, &ds.MinorVersion,
		&owner, &purpose, &ds.Grain, &eraID, &versionLabel, &schemaHash,
		&compatibility, &ds.CreatedAt, &ds.UpdatedAt,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to get dataset %s: %w", datasetName, err)
	}

	if owner.Valid {
		ds.Owner = owner.String
	}
	if purpose.Valid {
		ds.Purpose = purpose.String
	}
	if eraID.Valid {
		ds.EraID = &eraID.String
	}
	if versionLabel.Valid {
		ds.VersionLabel = &versionLabel.String
	}
	if schemaHash.Valid {
		ds.SchemaHash = &schemaHash.String
	}
	if compatibility.Valid {
		ds.Compatibility = &compatibility.String
	}

	return &ds, nil
}
