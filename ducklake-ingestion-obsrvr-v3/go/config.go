package main

import (
	"fmt"
	"os"

	"github.com/withObsrvr/ttp-processor-demo/ducklake-ingestion-obsrvr-v3/go/checkpoint"
	"github.com/withObsrvr/ttp-processor-demo/ducklake-ingestion-obsrvr-v3/go/era"
	"github.com/withObsrvr/ttp-processor-demo/ducklake-ingestion-obsrvr-v3/go/manifest"
	"github.com/withObsrvr/ttp-processor-demo/ducklake-ingestion-obsrvr-v3/go/metrics"
	"github.com/withObsrvr/ttp-processor-demo/ducklake-ingestion-obsrvr-v3/go/pas"
	"github.com/withObsrvr/ttp-processor-demo/ducklake-ingestion-obsrvr-v3/go/pipeline"
	"github.com/withObsrvr/ttp-processor-demo/ducklake-ingestion-obsrvr-v3/go/source"
	"gopkg.in/yaml.v3"
)

// AppConfig represents the full application configuration.
// This extends the original Config with new source options while maintaining
// backward compatibility with existing config files.
type AppConfig struct {
	Service struct {
		Name        string `yaml:"name"`
		Environment string `yaml:"environment"`
	} `yaml:"service"`

	// Source configuration - supports both legacy and new formats
	Source SourceConfigCompat `yaml:"source"`

	DuckLake struct {
		CatalogPath           string `yaml:"catalog_path"`
		DataPath              string `yaml:"data_path"`
		MetadataSchema        string `yaml:"metadata_schema"`
		CatalogName           string `yaml:"catalog_name"`
		SchemaName            string `yaml:"schema_name"`
		TableName             string `yaml:"table_name"`
		PartitionSize         int    `yaml:"partition_size"` // Ledgers per partition (default: 64000)
		AWSAccessKeyID        string `yaml:"aws_access_key_id"`
		AWSSecretAccessKey    string `yaml:"aws_secret_access_key"`
		AWSRegion             string `yaml:"aws_region"`
		AWSEndpoint           string `yaml:"aws_endpoint"`
		EnablePublicArchive   bool   `yaml:"enable_public_archive"`

		// Data inlining (DuckDB catalog only)
		// Buffers rows in catalog before writing to Parquet (prevents tiny files)
		DataInliningRowLimit  int    `yaml:"data_inlining_row_limit"`  // Rows to buffer before Parquet flush (default: 0 = disabled, recommended: 10000 for live mode)

		// Backfill mode settings (bulk ingestion)
		BatchSize             int    `yaml:"batch_size"`              // Ledgers per batch (default: 100)
		CommitIntervalSeconds int    `yaml:"commit_interval_seconds"` // Seconds between commits (default: 30)

		// Live mode settings (low-latency continuous ingestion)
		LiveBatchSize             int `yaml:"live_batch_size"`              // Ledgers per batch in live mode (default: 1)
		LiveCommitIntervalSeconds int `yaml:"live_commit_interval_seconds"` // Seconds between commits in live mode (default: 5)

		UseUpsert             bool   `yaml:"use_upsert"`
		CreateIndexes         bool   `yaml:"create_indexes"`
		NumWorkers            int    `yaml:"num_workers"`
	} `yaml:"ducklake"`

	Logging struct {
		Level  string `yaml:"level"`
		Format string `yaml:"format"`
	} `yaml:"logging"`

	// Cycle 2: Audit & Lineage configuration
	Checkpoint checkpoint.Config `yaml:"checkpoint"`
	Manifest   manifest.Config   `yaml:"manifest"`
	PAS        pas.Config        `yaml:"pas"`

	// Cycle 3: Parallel Pipeline & Era Isolation
	Era      era.Config              `yaml:"era"`
	Pipeline pipeline.PipelineConfig `yaml:"pipeline"`
	Metrics  metrics.Config          `yaml:"metrics"`
}

// SourceConfigCompat provides backward compatibility with existing config files.
// It supports both the legacy format (endpoint directly under source) and the
// new format (mode + datastore/grpc sections).
type SourceConfigCompat struct {
	// New format fields
	Mode      string                 `yaml:"mode"`      // "datastore" or "grpc"
	Datastore source.DatastoreConfig `yaml:"datastore"` // Galexie archive config
	GRPC      source.GRPCConfig      `yaml:"grpc"`      // gRPC config

	// Legacy format fields (for backward compatibility)
	Endpoint string `yaml:"endpoint"` // Legacy: gRPC endpoint

	// Common fields (used in both formats)
	NetworkPassphrase string `yaml:"network_passphrase"`
	StartLedger       uint32 `yaml:"start_ledger"`
	EndLedger         uint32 `yaml:"end_ledger"` // 0 = continuous (live mode)
	LiveMode          bool   `yaml:"live_mode"`  // Enable live ingestion after catching up
	PollInterval      int    `yaml:"poll_interval_seconds"` // Seconds between polls in live mode (default: 5)
}

// ToSourceConfig converts the backward-compatible config to the new SourceConfig format.
func (c *SourceConfigCompat) ToSourceConfig() source.SourceConfig {
	cfg := source.SourceConfig{
		Mode:              c.Mode,
		Datastore:         c.Datastore,
		GRPC:              c.GRPC,
		NetworkPassphrase: c.NetworkPassphrase,
		StartLedger:       c.StartLedger,
		EndLedger:         c.EndLedger,
	}

	// Handle legacy format: if Mode is empty but Endpoint is set, use grpc mode
	if cfg.Mode == "" && c.Endpoint != "" {
		cfg.Mode = "grpc"
		cfg.GRPC.Endpoint = c.Endpoint
	}

	// Apply defaults
	cfg.Datastore.ApplyDefaults()
	cfg.GRPC.ApplyDefaults()

	return cfg
}

// LoadAppConfig loads the application configuration from a YAML file.
func LoadAppConfig(path string) (*AppConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	var config AppConfig
	if err := yaml.Unmarshal(data, &config); err != nil {
		return nil, fmt.Errorf("failed to parse config file: %w", err)
	}

	// Apply defaults
	if config.DuckLake.BatchSize == 0 {
		config.DuckLake.BatchSize = 200
	}
	if config.DuckLake.CommitIntervalSeconds == 0 {
		config.DuckLake.CommitIntervalSeconds = 30
	}
	if config.DuckLake.LiveBatchSize == 0 {
		config.DuckLake.LiveBatchSize = 1 // Default: 1 ledger at a time in live mode
	}
	if config.DuckLake.LiveCommitIntervalSeconds == 0 {
		config.DuckLake.LiveCommitIntervalSeconds = 5 // Default: 5 seconds
	}
	if config.DuckLake.NumWorkers == 0 {
		config.DuckLake.NumWorkers = 1
	}

	// Cycle 3: Apply defaults for era, pipeline, and metrics
	config.Era.ApplyDefaults()
	config.Pipeline.ApplyDefaults()
	config.Metrics.ApplyDefaults()

	return &config, nil
}

// Validate validates the application configuration.
func (c *AppConfig) Validate() error {
	srcCfg := c.Source.ToSourceConfig()
	if err := srcCfg.Validate(); err != nil {
		return fmt.Errorf("source config: %w", err)
	}

	if c.DuckLake.CatalogPath == "" {
		return fmt.Errorf("ducklake.catalog_path is required")
	}
	if c.DuckLake.DataPath == "" {
		return fmt.Errorf("ducklake.data_path is required")
	}
	if c.DuckLake.CatalogName == "" {
		return fmt.Errorf("ducklake.catalog_name is required")
	}
	if c.DuckLake.SchemaName == "" {
		return fmt.Errorf("ducklake.schema_name is required")
	}

	return nil
}

// IsDatastoreMode returns true if the source is configured for direct Galexie access.
func (c *AppConfig) IsDatastoreMode() bool {
	srcCfg := c.Source.ToSourceConfig()
	return srcCfg.Mode == "datastore"
}

// IsGRPCMode returns true if the source is configured for gRPC access.
func (c *AppConfig) IsGRPCMode() bool {
	srcCfg := c.Source.ToSourceConfig()
	return srcCfg.Mode == "grpc"
}
