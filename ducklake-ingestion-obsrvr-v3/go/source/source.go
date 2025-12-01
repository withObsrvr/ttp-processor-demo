// Package source provides ledger data sources for the Bronze Copier.
// It abstracts the underlying data source (Galexie archives, gRPC, etc.)
// behind a common interface.
package source

import (
	"context"
	"fmt"

	"github.com/stellar/go/xdr"
)

// LedgerResult represents a ledger fetch result with optional error.
type LedgerResult struct {
	Ledger *xdr.LedgerCloseMeta
	Err    error
}

// LedgerSource is the interface for fetching ledger data from various sources.
// Implementations include DatastoreSource (Galexie archives) and GRPCSource.
type LedgerSource interface {
	// GetLedger fetches a single ledger by sequence number.
	GetLedger(ctx context.Context, seq uint32) (*xdr.LedgerCloseMeta, error)

	// GetLedgerRange returns a channel that streams ledgers in the given range.
	// The channel is closed when the range is exhausted or an error occurs.
	// For unbounded ranges (end=0), the channel remains open until context cancellation.
	GetLedgerRange(ctx context.Context, start, end uint32) (<-chan LedgerResult, error)

	// PrepareRange prepares the source for reading the given range.
	// For buffered backends, this triggers prefetching.
	PrepareRange(ctx context.Context, start, end uint32) error

	// Close releases resources associated with the source.
	Close() error
}

// SourceConfig holds configuration for creating a LedgerSource.
type SourceConfig struct {
	// Mode selects the source type: "datastore" or "grpc"
	Mode string `yaml:"mode"`

	// Datastore configuration (for Galexie archive access)
	Datastore DatastoreConfig `yaml:"datastore"`

	// GRPC configuration (legacy mode)
	GRPC GRPCConfig `yaml:"grpc"`

	// Common configuration
	NetworkPassphrase string `yaml:"network_passphrase"`
	StartLedger       uint32 `yaml:"start_ledger"`
	EndLedger         uint32 `yaml:"end_ledger"` // 0 = tail mode (unbounded)
}

// DatastoreConfig holds configuration for the Datastore source.
type DatastoreConfig struct {
	// Type is the storage backend: "GCS" or "S3"
	Type string `yaml:"type"`

	// Path is the bucket path (e.g., "bucket-name/landing/ledgers/pubnet")
	Path string `yaml:"path"`

	// Schema configuration
	LedgersPerFile    uint32 `yaml:"ledgers_per_file"`    // Default: 1 (Galexie format)
	FilesPerPartition uint32 `yaml:"files_per_partition"` // Default: 64000 (Galexie format)

	// BufferedStorageBackend configuration
	BufferSize  uint32 `yaml:"buffer_size"`   // Default: 100
	NumWorkers  uint32 `yaml:"num_workers"`   // Default: 5
	RetryLimit  uint32 `yaml:"retry_limit"`   // Default: 3
	RetryWaitMs int64  `yaml:"retry_wait_ms"` // Default: 1000 (1 second)

	// S3-specific configuration
	S3Region   string `yaml:"s3_region"`
	S3Endpoint string `yaml:"s3_endpoint"` // For S3-compatible storage (B2, MinIO, R2)
}

// GRPCConfig holds configuration for the gRPC source.
type GRPCConfig struct {
	// Endpoint is the gRPC server address (e.g., "localhost:50052")
	Endpoint string `yaml:"endpoint"`

	// MaxMessageSize is the maximum gRPC message size in bytes (default: 100MB)
	MaxMessageSize int `yaml:"max_message_size"`
}

// NewLedgerSource creates a LedgerSource based on the configuration.
func NewLedgerSource(cfg SourceConfig) (LedgerSource, error) {
	switch cfg.Mode {
	case "datastore":
		return NewDatastoreSource(cfg.Datastore, cfg.NetworkPassphrase)
	case "grpc":
		return NewGRPCSource(cfg.GRPC, cfg.NetworkPassphrase)
	default:
		return nil, fmt.Errorf("unknown source mode: %s (supported: datastore, grpc)", cfg.Mode)
	}
}

// Validate validates the source configuration.
func (c *SourceConfig) Validate() error {
	if c.Mode == "" {
		return fmt.Errorf("source.mode is required")
	}

	if c.NetworkPassphrase == "" {
		return fmt.Errorf("source.network_passphrase is required")
	}

	switch c.Mode {
	case "datastore":
		if err := c.Datastore.Validate(); err != nil {
			return fmt.Errorf("source.datastore: %w", err)
		}
	case "grpc":
		if err := c.GRPC.Validate(); err != nil {
			return fmt.Errorf("source.grpc: %w", err)
		}
	default:
		return fmt.Errorf("unknown source.mode: %s", c.Mode)
	}

	return nil
}

// Validate validates the datastore configuration.
func (c *DatastoreConfig) Validate() error {
	if c.Type == "" {
		return fmt.Errorf("type is required (GCS or S3)")
	}
	if c.Type != "GCS" && c.Type != "S3" {
		return fmt.Errorf("type must be GCS or S3, got: %s", c.Type)
	}
	if c.Path == "" {
		return fmt.Errorf("path is required")
	}
	return nil
}

// Validate validates the gRPC configuration.
func (c *GRPCConfig) Validate() error {
	if c.Endpoint == "" {
		return fmt.Errorf("endpoint is required")
	}
	return nil
}

// ApplyDefaults applies default values to the datastore configuration.
func (c *DatastoreConfig) ApplyDefaults() {
	if c.LedgersPerFile == 0 {
		c.LedgersPerFile = 1 // Galexie format
	}
	if c.FilesPerPartition == 0 {
		c.FilesPerPartition = 64000 // Galexie format
	}
	if c.BufferSize == 0 {
		c.BufferSize = 100
	}
	if c.NumWorkers == 0 {
		c.NumWorkers = 5
	}
	if c.RetryLimit == 0 {
		c.RetryLimit = 3
	}
	if c.RetryWaitMs == 0 {
		c.RetryWaitMs = 1000
	}
}

// ApplyDefaults applies default values to the gRPC configuration.
func (c *GRPCConfig) ApplyDefaults() {
	if c.MaxMessageSize == 0 {
		c.MaxMessageSize = 100 * 1024 * 1024 // 100MB
	}
}
