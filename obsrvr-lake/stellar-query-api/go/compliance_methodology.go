package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// ============================================
// METHODOLOGY DOCUMENT GENERATION
// ============================================

// MethodologyDoc represents the auto-generated methodology documentation
type MethodologyDoc struct {
	ArchiveID          string
	AssetCode          string
	AssetIssuer        string
	AssetType          string
	PeriodStart        string
	PeriodEnd          string
	GeneratedAt        string
	MethodologyVersion string
	Artifacts          []ArtifactInfo
}

// ArtifactInfo describes an artifact for methodology docs
type ArtifactInfo struct {
	Name     string
	Type     string
	Checksum string
	RowCount int
}

// GenerateMethodologyDocument creates the methodology.md content
func GenerateMethodologyDocument(doc MethodologyDoc) string {
	var sb strings.Builder

	sb.WriteString("# Compliance Archive Methodology\n\n")

	// Archive Information
	sb.WriteString("## Archive Information\n\n")
	sb.WriteString(fmt.Sprintf("- **Archive ID:** %s\n", doc.ArchiveID))
	if doc.AssetIssuer != "" {
		sb.WriteString(fmt.Sprintf("- **Asset:** %s:%s\n", doc.AssetCode, doc.AssetIssuer))
	} else {
		sb.WriteString(fmt.Sprintf("- **Asset:** %s (native)\n", doc.AssetCode))
	}
	sb.WriteString(fmt.Sprintf("- **Period:** %s to %s\n", doc.PeriodStart, doc.PeriodEnd))
	sb.WriteString(fmt.Sprintf("- **Generated:** %s\n", doc.GeneratedAt))
	sb.WriteString(fmt.Sprintf("- **Methodology Version:** %s\n\n", doc.MethodologyVersion))

	// Data Sources
	sb.WriteString("## Data Sources\n\n")
	sb.WriteString("### Bronze Layer (Raw Evidence)\n\n")
	sb.WriteString("- `ledgers_row_v2` - Raw ledger headers\n")
	sb.WriteString("- `transactions_row_v2` - Raw transaction envelopes\n")
	sb.WriteString("- `operations_row_v2` - Raw operation data\n")
	sb.WriteString("- `effects_row_v1` - Raw effect data\n\n")

	sb.WriteString("### Silver Layer (Canonical State)\n\n")
	sb.WriteString("- `enriched_history_operations` - Normalized operation history\n")
	sb.WriteString("- `trustlines_snapshot` - SCD Type 2 balance history\n")
	sb.WriteString("- `accounts_snapshot` - SCD Type 2 account state\n\n")

	// Derivation Methods
	sb.WriteString("## Derivation Methods\n\n")

	sb.WriteString("### Transaction Lineage\n\n")
	sb.WriteString("Transactions are extracted from `enriched_history_operations` filtered by:\n\n")
	sb.WriteString(fmt.Sprintf("- `asset_code` = '%s'\n", doc.AssetCode))
	if doc.AssetIssuer != "" {
		sb.WriteString(fmt.Sprintf("- `asset_issuer` = '%s'\n", doc.AssetIssuer))
	}
	sb.WriteString("- `is_payment_op` = true (includes payment, path_payment_*, claimable_balance, clawback)\n\n")

	sb.WriteString("### Balance Snapshots\n\n")
	sb.WriteString("Balances are derived from `trustlines_snapshot` using SCD Type 2 logic:\n\n")
	sb.WriteString("- Records where `created_at <= snapshot_timestamp`\n")
	sb.WriteString("- AND (`valid_to IS NULL` OR `valid_to > snapshot_timestamp`)\n\n")
	sb.WriteString("This ensures we capture the exact balance that was valid at the snapshot time.\n\n")

	sb.WriteString("### Supply Timeline\n\n")
	sb.WriteString("Daily supply is aggregated from trustline balances:\n\n")
	sb.WriteString("- `total_supply` = SUM of all positive trustline balances\n")
	sb.WriteString("- `circulating_supply` = total_supply - issuer_balance\n")
	sb.WriteString("- `holder_count` = COUNT of accounts with balance > 0\n\n")

	// Reproducibility Guarantee
	sb.WriteString("## Reproducibility Guarantee\n\n")
	sb.WriteString("Given the same:\n\n")
	sb.WriteString("- Asset (code + issuer)\n")
	sb.WriteString("- Time range\n")
	sb.WriteString("- Methodology version\n\n")
	sb.WriteString("This archive will produce **identical results** when regenerated.\n\n")

	// Checksums
	sb.WriteString("## Checksums\n\n")
	sb.WriteString("All artifacts include SHA-256 checksums computed over the canonical JSON representation.\n")
	sb.WriteString("Checksums are deterministic - the same data always produces the same checksum.\n\n")

	if len(doc.Artifacts) > 0 {
		sb.WriteString("### Artifact Checksums\n\n")
		sb.WriteString("| Artifact | Type | Rows | Checksum |\n")
		sb.WriteString("|----------|------|------|----------|\n")
		for _, a := range doc.Artifacts {
			rowStr := "-"
			if a.RowCount > 0 {
				rowStr = fmt.Sprintf("%d", a.RowCount)
			}
			sb.WriteString(fmt.Sprintf("| %s | %s | %s | `%s` |\n", a.Name, a.Type, rowStr, a.Checksum))
		}
		sb.WriteString("\n")
	}

	// Schema Definitions
	sb.WriteString("## Schema Definitions\n\n")

	sb.WriteString("### transactions.json / transactions.parquet\n\n")
	sb.WriteString("| Column | Type | Description |\n")
	sb.WriteString("|--------|------|-------------|\n")
	sb.WriteString("| ledger_sequence | INT64 | Ledger number |\n")
	sb.WriteString("| closed_at | TIMESTAMP | Ledger close time |\n")
	sb.WriteString("| transaction_hash | STRING | Transaction hash |\n")
	sb.WriteString("| operation_index | INT32 | Operation index within tx |\n")
	sb.WriteString("| operation_type | STRING | Operation type |\n")
	sb.WriteString("| from_account | STRING | Source of funds |\n")
	sb.WriteString("| to_account | STRING | Destination of funds |\n")
	sb.WriteString("| amount | STRING | Amount (7 decimal precision) |\n")
	sb.WriteString("| successful | BOOL | Transaction success status |\n\n")

	sb.WriteString("### balances_*.json / balances_*.parquet\n\n")
	sb.WriteString("| Column | Type | Description |\n")
	sb.WriteString("|--------|------|-------------|\n")
	sb.WriteString("| account_id | STRING | Stellar account ID |\n")
	sb.WriteString("| balance | STRING | Balance (7 decimal precision) |\n")
	sb.WriteString("| percent_of_supply | STRING | % of total supply |\n\n")

	sb.WriteString("### supply_timeline.json / supply_timeline.parquet\n\n")
	sb.WriteString("| Column | Type | Description |\n")
	sb.WriteString("|--------|------|-------------|\n")
	sb.WriteString("| timestamp | TIMESTAMP | Snapshot time |\n")
	sb.WriteString("| ledger_sequence | INT64 | Ledger number |\n")
	sb.WriteString("| total_supply | STRING | Total issued |\n")
	sb.WriteString("| circulating_supply | STRING | Excluding issuer |\n")
	sb.WriteString("| issuer_balance | STRING | Issuer's balance |\n")
	sb.WriteString("| holder_count | INT32 | Number of holders |\n")
	sb.WriteString("| supply_change | STRING | Change from previous period |\n")
	sb.WriteString("| supply_change_percent | STRING | Percent change |\n\n")

	// Contact
	sb.WriteString("## Support\n\n")
	sb.WriteString("For questions about this archive or methodology, contact support@withobsrvr.com\n")

	return sb.String()
}

// ============================================
// MANIFEST GENERATION
// ============================================

// ArchiveManifest represents the manifest.json structure
type ArchiveManifest struct {
	ArchiveID          string                    `json:"archive_id"`
	Version            string                    `json:"manifest_version"`
	Asset              AssetInfo                 `json:"asset"`
	Period             PeriodInfo                `json:"period"`
	MethodologyVersion string                    `json:"methodology_version"`
	GeneratedAt        string                    `json:"generated_at"`
	GeneratedBy        string                    `json:"generated_by"`
	Artifacts          []ManifestArtifact        `json:"artifacts"`
	Checksums          map[string]string         `json:"checksums"`
	TotalRows          int                       `json:"total_rows"`
	ReproducibilityKey string                    `json:"reproducibility_key"`
}

// ManifestArtifact represents an artifact entry in the manifest
type ManifestArtifact struct {
	Name        string `json:"name"`
	Type        string `json:"type"`
	Format      string `json:"format"`
	RowCount    int    `json:"row_count,omitempty"`
	SnapshotAt  string `json:"snapshot_at,omitempty"`
	Checksum    string `json:"checksum"`
	GeneratedAt string `json:"generated_at"`
}

// GenerateManifest creates the manifest.json for an archive
func GenerateManifest(job *ArchiveJob) (*ArchiveManifest, error) {
	checksums := make(map[string]string)
	totalRows := 0
	artifacts := make([]ManifestArtifact, 0, len(job.Artifacts))

	for _, a := range job.Artifacts {
		checksums[a.Type] = a.Checksum
		totalRows += a.RowCount

		format := "json"
		if strings.HasSuffix(a.Name, ".parquet") {
			format = "parquet"
		} else if strings.HasSuffix(a.Name, ".csv") {
			format = "csv"
		}

		artifacts = append(artifacts, ManifestArtifact{
			Name:        a.Name,
			Type:        a.Type,
			Format:      format,
			RowCount:    a.RowCount,
			SnapshotAt:  a.SnapshotAt,
			Checksum:    a.Checksum,
			GeneratedAt: a.GeneratedAt,
		})
	}

	// Generate reproducibility key (hash of asset + period + methodology)
	issuer := ""
	if job.Asset.Issuer != nil {
		issuer = *job.Asset.Issuer
	}
	reproKey := generateReproducibilityKey(job.Asset.Code, issuer, job.Period.Start, job.Period.End, job.Request.Include)

	manifest := &ArchiveManifest{
		ArchiveID:          job.ID,
		Version:            "1.0",
		Asset:              job.Asset,
		Period:             job.Period,
		MethodologyVersion: MethodologyArchiveV1,
		GeneratedAt:        job.CompletedAt.Format(time.RFC3339),
		GeneratedBy:        "obsrvr-lake-api",
		Artifacts:          artifacts,
		Checksums:          checksums,
		TotalRows:          totalRows,
		ReproducibilityKey: reproKey,
	}

	return manifest, nil
}

// generateReproducibilityKey creates a deterministic key for archive reproducibility
func generateReproducibilityKey(assetCode, assetIssuer, periodStart, periodEnd string, include []string) string {
	// Sort include for determinism
	sortedInclude := make([]string, len(include))
	copy(sortedInclude, include)
	// Simple sort
	for i := 0; i < len(sortedInclude)-1; i++ {
		for j := i + 1; j < len(sortedInclude); j++ {
			if sortedInclude[i] > sortedInclude[j] {
				sortedInclude[i], sortedInclude[j] = sortedInclude[j], sortedInclude[i]
			}
		}
	}

	key := fmt.Sprintf("%s|%s|%s|%s|%s|%s",
		assetCode,
		assetIssuer,
		periodStart,
		periodEnd,
		strings.Join(sortedInclude, ","),
		MethodologyArchiveV1,
	)

	hash := sha256.Sum256([]byte(key))
	return "repro:" + hex.EncodeToString(hash[:16]) // Use first 16 bytes for shorter key
}

// GenerateManifestJSON creates the JSON bytes for manifest
func GenerateManifestJSON(manifest *ArchiveManifest) ([]byte, error) {
	return json.MarshalIndent(manifest, "", "  ")
}
