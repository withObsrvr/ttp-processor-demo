// Package resolver provides Bronze layer data resolution and routing.
// Cycle 5: Bronze Resolver Library - Manifest Generation
package resolver

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"
	"time"
)

// GetReadManifest generates a deterministic read manifest for a dataset/era/range.
func (r *Resolver) GetReadManifest(ctx context.Context, dataset, eraID, versionLabel string, ledgerRange LedgerRange) (*ReadManifest, error) {
	// Query lineage for partitions that overlap with the requested range
	query := fmt.Sprintf(`
		SELECT id, source_ledger_start, source_ledger_end, row_count, partition, checksum
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
		return nil, fmt.Errorf("failed to query manifest partitions: %w", err)
	}
	defer rows.Close()

	var files []ManifestFile
	var totalRows int64
	var maxID int64

	for rows.Next() {
		var id int64
		var start, end uint32
		var rowCount int64
		var partition, checksum string

		if err := rows.Scan(&id, &start, &end, &rowCount, &partition, &checksum); err != nil {
			return nil, fmt.Errorf("failed to scan manifest row: %w", err)
		}

		if id > maxID {
			maxID = id
		}

		// Calculate file path based on partition
		// This is a simplified version - real implementation would query DuckLake for actual file paths
		filePath := r.constructFilePath(dataset, eraID, partition, start, end)

		files = append(files, ManifestFile{
			Path:     filePath,
			Checksum: checksum,
			RowCount: rowCount,
			Bytes:    0, // Not tracked in lineage, would need filesystem query
		})

		totalRows += rowCount
	}

	// Sort files by path for determinism
	sort.Slice(files, func(i, j int) bool {
		return files[i].Path < files[j].Path
	})

	// Generate manifest checksum
	manifestChecksum := r.computeManifestChecksum(dataset, eraID, ledgerRange, files)

	manifest := &ReadManifest{
		Dataset:     dataset,
		EraID:       eraID,
		SnapshotID:  maxID,
		LedgerStart: ledgerRange.Start,
		LedgerEnd:   ledgerRange.End,
		Files:       files,
		TotalRows:   totalRows,
		GeneratedAt: time.Now(),
		Checksum:    manifestChecksum,
	}

	return manifest, nil
}

// constructFilePath builds a file path for a partition.
// This is a simplified version - real implementation would query DuckLake metadata.
func (r *Resolver) constructFilePath(dataset, eraID, partition string, start, end uint32) string {
	// Extract table name from dataset (e.g., "core.ledgers_row_v2" -> "ledgers_row_v2")
	tableName := dataset
	if idx := strings.LastIndex(dataset, "."); idx != -1 {
		tableName = dataset[idx+1:]
	}

	// Construct path based on Obsrvr's file naming convention
	// Example: {data_path}/{era_id}/{table_name}/{partition}.parquet
	if partition != "" {
		return fmt.Sprintf("%s/%s/%s/%s.parquet", eraID, tableName, partition, partition)
	}

	// Fallback: ledger_range as partition
	return fmt.Sprintf("%s/%s/ledgers_%d_%d.parquet", eraID, tableName, start, end)
}

// computeManifestChecksum generates a deterministic checksum for a manifest.
func (r *Resolver) computeManifestChecksum(dataset, eraID string, ledgerRange LedgerRange, files []ManifestFile) string {
	h := sha256.New()

	// Include manifest identity
	fmt.Fprintf(h, "dataset:%s\n", dataset)
	fmt.Fprintf(h, "era:%s\n", eraID)
	fmt.Fprintf(h, "range:%d-%d\n", ledgerRange.Start, ledgerRange.End)

	// Include all file paths and checksums (deterministically ordered)
	for _, f := range files {
		fmt.Fprintf(h, "file:%s:%s\n", f.Path, f.Checksum)
	}

	sum := h.Sum(nil)
	return hex.EncodeToString(sum[:16]) // First 16 bytes = 32 hex chars
}

// ValidateManifest verifies that a manifest's checksum matches its contents.
func (r *Resolver) ValidateManifest(manifest *ReadManifest) bool {
	computed := r.computeManifestChecksum(
		manifest.Dataset,
		manifest.EraID,
		LedgerRange{Start: manifest.LedgerStart, End: manifest.LedgerEnd},
		manifest.Files,
	)
	return computed == manifest.Checksum
}

// GetPartitionCount returns the number of partitions for a dataset/era/range.
func (r *Resolver) GetPartitionCount(ctx context.Context, dataset, eraID, versionLabel string, ledgerRange LedgerRange) (int, error) {
	query := fmt.Sprintf(`
		SELECT COUNT(*)
		FROM %s.%s._meta_lineage
		WHERE dataset = ?
		  AND era_id = ?
		  AND version_label = ?
		  AND source_ledger_start <= ?
		  AND source_ledger_end >= ?
	`, r.catalogName, r.schemaName)

	var count int
	err := r.db.QueryRowContext(ctx, query, dataset, eraID, versionLabel, ledgerRange.End, ledgerRange.Start).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("failed to get partition count: %w", err)
	}

	return count, nil
}
