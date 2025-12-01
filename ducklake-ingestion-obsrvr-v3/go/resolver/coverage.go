// Package resolver provides Bronze layer data resolution and routing.
// Cycle 5: Bronze Resolver Library - Coverage Queries
package resolver

import (
	"context"
	"fmt"
)

// GetCoverage returns coverage information for a dataset/era combination.
func (r *Resolver) GetCoverage(ctx context.Context, dataset, eraID, versionLabel string) (*Coverage, error) {
	// Check cache first
	cacheKey := fmt.Sprintf("%s:%s:%s", dataset, eraID, versionLabel)
	if r.cacheEnabled {
		if coverage, ok := r.cache.getCoverage(cacheKey); ok {
			return coverage, nil
		}
	}

	// Query lineage for all partitions
	query := fmt.Sprintf(`
		SELECT source_ledger_start, source_ledger_end, row_count, pas_verified
		FROM %s.%s._meta_lineage
		WHERE dataset = ?
		  AND era_id = ?
		  AND version_label = ?
		ORDER BY source_ledger_start
	`, r.catalogName, r.schemaName)

	rows, err := r.db.QueryContext(ctx, query, dataset, eraID, versionLabel)
	if err != nil {
		return nil, fmt.Errorf("failed to query coverage: %w", err)
	}
	defer rows.Close()

	// Build committed ranges and detect gaps
	var ranges []LedgerRange
	var gaps []LedgerRange
	var lastEnd uint32
	var totalRows int64
	var lastVerified uint32

	for rows.Next() {
		var start, end uint32
		var rowCount int64
		var pasVerified bool

		if err := rows.Scan(&start, &end, &rowCount, &pasVerified); err != nil {
			return nil, fmt.Errorf("failed to scan coverage row: %w", err)
		}

		// Detect gap (if there's a missing range between partitions)
		if lastEnd > 0 && start > lastEnd+1 {
			gaps = append(gaps, LedgerRange{Start: lastEnd + 1, End: start - 1})
		}

		ranges = append(ranges, LedgerRange{Start: start, End: end})
		totalRows += rowCount
		lastEnd = end

		if pasVerified && end > lastVerified {
			lastVerified = end
		}
	}

	coverage := &Coverage{
		CommittedRanges: ranges,
		TailLedger:      lastEnd,
		Gaps:            gaps,
		LastVerified:    lastVerified,
		TotalRows:       totalRows,
	}

	// Cache the result
	if r.cacheEnabled {
		r.cache.setCoverage(cacheKey, coverage)
	}

	return coverage, nil
}

// GetCoverageForRange returns coverage information for a specific ledger range.
// This is useful for checking if a requested range is fully covered.
func (r *Resolver) GetCoverageForRange(ctx context.Context, dataset, eraID, versionLabel string, ledgerRange LedgerRange) (*Coverage, error) {
	// Query lineage for partitions that overlap with the requested range
	query := fmt.Sprintf(`
		SELECT source_ledger_start, source_ledger_end, row_count, pas_verified
		FROM %s.%s._meta_lineage
		WHERE dataset = ?
		  AND era_id = ?
		  AND version_label = ?
		  AND source_ledger_start <= ?
		  AND source_ledger_end >= ?
		ORDER BY source_ledger_start
	`, r.catalogName, r.schemaName)

	rows, err := r.db.QueryContext(ctx, query, dataset, eraID, versionLabel, ledgerRange.End, ledgerRange.Start)
	if err != nil {
		return nil, fmt.Errorf("failed to query range coverage: %w", err)
	}
	defer rows.Close()

	var ranges []LedgerRange
	var gaps []LedgerRange
	var totalRows int64
	var lastVerified uint32
	var lastEnd uint32

	for rows.Next() {
		var start, end uint32
		var rowCount int64
		var pasVerified bool

		if err := rows.Scan(&start, &end, &rowCount, &pasVerified); err != nil {
			return nil, fmt.Errorf("failed to scan range coverage row: %w", err)
		}

		// Clamp to requested range
		if start < ledgerRange.Start {
			start = ledgerRange.Start
		}
		if end > ledgerRange.End {
			end = ledgerRange.End
		}

		// Detect gap within the requested range
		if lastEnd > 0 && start > lastEnd+1 {
			gapStart := lastEnd + 1
			gapEnd := start - 1
			// Only report gap if it's within the requested range
			if gapStart >= ledgerRange.Start && gapEnd <= ledgerRange.End {
				gaps = append(gaps, LedgerRange{Start: gapStart, End: gapEnd})
			}
		}

		ranges = append(ranges, LedgerRange{Start: start, End: end})
		totalRows += rowCount
		lastEnd = end

		if pasVerified && end > lastVerified {
			lastVerified = end
		}
	}

	// Check if there's a gap at the beginning of the range
	if len(ranges) > 0 && ranges[0].Start > ledgerRange.Start {
		gaps = append([]LedgerRange{{Start: ledgerRange.Start, End: ranges[0].Start - 1}}, gaps...)
	}

	// Check if there's a gap at the end of the range
	if len(ranges) > 0 && lastEnd < ledgerRange.End {
		gaps = append(gaps, LedgerRange{Start: lastEnd + 1, End: ledgerRange.End})
	}

	// If no ranges found, the entire requested range is a gap
	if len(ranges) == 0 {
		gaps = []LedgerRange{ledgerRange}
	}

	return &Coverage{
		CommittedRanges: ranges,
		TailLedger:      lastEnd,
		Gaps:            gaps,
		LastVerified:    lastVerified,
		TotalRows:       totalRows,
	}, nil
}

// HasGaps returns true if there are any gaps in coverage for a dataset/era.
func (r *Resolver) HasGaps(ctx context.Context, dataset, eraID, versionLabel string) (bool, error) {
	coverage, err := r.GetCoverage(ctx, dataset, eraID, versionLabel)
	if err != nil {
		return false, err
	}
	return len(coverage.Gaps) > 0, nil
}

// IsContinuous checks if a dataset/era has continuous coverage (no gaps).
func (r *Resolver) IsContinuous(ctx context.Context, dataset, eraID, versionLabel string) (bool, error) {
	hasGaps, err := r.HasGaps(ctx, dataset, eraID, versionLabel)
	if err != nil {
		return false, err
	}
	return !hasGaps, nil
}
