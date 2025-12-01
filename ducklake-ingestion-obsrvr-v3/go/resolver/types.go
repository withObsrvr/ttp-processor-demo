// Package resolver provides Bronze layer data resolution and routing.
// Cycle 5: Bronze Resolver Library
package resolver

import "time"

// IntentMode specifies how to route a dataset query to an era.
type IntentMode string

const (
	// IntentLatest routes to the active era (highest tail)
	IntentLatest IntentMode = "latest"

	// IntentAsOfLedger routes to the era covering a specific ledger
	IntentAsOfLedger IntentMode = "as_of_ledger"

	// IntentAsOfProtocol routes to the era covering a protocol version
	IntentAsOfProtocol IntentMode = "as_of_protocol"

	// IntentExplicit routes to a specific era by ID
	IntentExplicit IntentMode = "explicit"
)

// Intent describes what dataset the caller wants and how to route it.
type Intent struct {
	// Mode specifies the routing strategy
	Mode IntentMode

	// Network is the Stellar network (mainnet, testnet)
	Network string

	// Ledger is the target ledger number (for IntentAsOfLedger)
	Ledger *uint32

	// Protocol is the target protocol version (for IntentAsOfProtocol)
	Protocol *int

	// EraID is the explicit era identifier (for IntentExplicit)
	EraID string

	// VersionLabel is the explicit era version (for IntentExplicit)
	VersionLabel string

	// Range optionally limits the query to a ledger range
	Range *LedgerRange

	// StrictPAS requires PAS verification for all partitions
	StrictPAS bool
}

// LedgerRange represents an inclusive range of ledgers.
type LedgerRange struct {
	Start uint32
	End   uint32
}

// ResolvedDataset represents the result of resolving a dataset intent.
type ResolvedDataset struct {
	// Dataset name (e.g., "core.ledgers_row_v2")
	Dataset string

	// Network that was resolved
	Network string

	// EraID that was selected
	EraID string

	// VersionLabel of the selected era
	VersionLabel string

	// SchemaHash for compatibility checking
	SchemaHash string

	// Compatibility flag (strict, adapter_required, breaking)
	Compatibility string

	// Coverage information for the resolved era
	Coverage Coverage

	// Manifest is the optional read manifest (if range was specified)
	Manifest *ReadManifest

	// PASVerified indicates if all data in this resolution has PAS verification
	PASVerified bool
}

// Coverage describes what ledger ranges are available for a dataset/era.
type Coverage struct {
	// CommittedRanges are the ledger ranges that have been flushed
	CommittedRanges []LedgerRange

	// TailLedger is the highest ledger committed
	TailLedger uint32

	// Gaps are missing ranges between committed data
	Gaps []LedgerRange

	// LastVerified is the highest ledger with PAS verification
	LastVerified uint32

	// TotalRows across all committed ranges
	TotalRows int64
}

// ReadManifest describes a deterministic snapshot of dataset files.
type ReadManifest struct {
	// Dataset name
	Dataset string

	// EraID that this manifest covers
	EraID string

	// SnapshotID is a monotonic identifier (e.g., max lineage ID)
	SnapshotID int64

	// LedgerStart of the range
	LedgerStart uint32

	// LedgerEnd of the range
	LedgerEnd uint32

	// Files to read for this manifest
	Files []ManifestFile

	// TotalRows across all files
	TotalRows int64

	// GeneratedAt timestamp
	GeneratedAt time.Time

	// Checksum of the manifest for verification
	Checksum string
}

// ManifestFile represents a single data file in a read manifest.
type ManifestFile struct {
	// Path to the file (relative or absolute)
	Path string

	// Checksum of the file contents
	Checksum string

	// RowCount in this file
	RowCount int64

	// Bytes (file size)
	Bytes int64
}

// Era represents an era record from _meta_eras.
type Era struct {
	EraID        string
	Network      string
	VersionLabel string
	LedgerStart  uint32
	LedgerEnd    *uint32 // NULL if active
	ProtocolMin  *int
	ProtocolMax  *int
	Status       string
	SchemaEpoch  *string
	PASChainHead *string
	CreatedAt    time.Time
	FrozenAt     *time.Time
}

// Dataset represents a dataset record from _meta_datasets.
type Dataset struct {
	Name          string
	Tier          string
	Domain        string
	MajorVersion  int
	MinorVersion  int
	Owner         string
	Purpose       string
	Grain         string
	EraID         *string
	VersionLabel  *string
	SchemaHash    *string
	Compatibility *string
	CreatedAt     time.Time
	UpdatedAt     time.Time
}
