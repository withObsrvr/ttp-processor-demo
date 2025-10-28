package config

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

// QuickstartConfig represents the complete quickstart.yaml structure
type QuickstartConfig struct {
	APIVersion string   `yaml:"apiVersion"`
	Kind       string   `yaml:"kind"`
	Metadata   Metadata `yaml:"metadata"`
	Spec       Spec     `yaml:"spec"`
}

// Metadata represents the metadata section
type Metadata struct {
	Name        string `yaml:"name"`
	Description string `yaml:"description"`
}

// Spec represents the spec section
type Spec struct {
	Source        Source        `yaml:"source"`
	Filters       Filters       `yaml:"filters"`
	Processor     Processor     `yaml:"processor"`
	Consumer      Consumer      `yaml:"consumer"`
	Observability Observability `yaml:"observability"`
}

// Source represents the data source configuration
type Source struct {
	Type    string         `yaml:"type"`    // Component type (e.g., "stellar-live-source-datalake")
	Network string         `yaml:"network"`
	Storage StorageConfig  `yaml:"storage"`
	Ledgers LedgersConfig  `yaml:"ledgers"`
}

// StorageConfig represents GCS storage configuration
type StorageConfig struct {
	Type   string `yaml:"type"`
	Bucket string `yaml:"bucket"`
	Path   string `yaml:"path"`
}

// LedgersConfig represents ledger range configuration
type LedgersConfig struct {
	Start uint32 `yaml:"start"`
	End   uint32 `yaml:"end"` // 0 = continuous
}

// Filters represents asset filtering configuration
type Filters struct {
	Assets        []AssetFilter `yaml:"assets"`
	AssetCodeOnly []string      `yaml:"assetCodeOnly"`
	IndexAll      bool          `yaml:"indexAll"`
}

// AssetFilter represents a specific asset filter
type AssetFilter struct {
	Code        string `yaml:"code"`
	Issuer      string `yaml:"issuer"`
	Description string `yaml:"description"`
}

// Processor represents processor configuration
type Processor struct {
	Type       string `yaml:"type"` // Component type (e.g., "account-balance", "payment", "operation")
	BatchSize  int    `yaml:"batchSize"`
	GRPCPort   int    `yaml:"grpcPort"`
	HealthPort int    `yaml:"healthPort"`
}

// Consumer represents consumer configuration
type Consumer struct {
	Type       string         `yaml:"type"` // Component type (e.g., "duckdb", "postgresql", "clickhouse")
	Database   DatabaseConfig `yaml:"database"`
	BatchSize  int            `yaml:"batchSize"`
	HealthPort int            `yaml:"healthPort"`
}

// DatabaseConfig represents DuckDB configuration
type DatabaseConfig struct {
	Path  string      `yaml:"path"`
	Table TableConfig `yaml:"table"`
}

// TableConfig represents table configuration
type TableConfig struct {
	Name string `yaml:"name"`
}

// Observability represents observability configuration
type Observability struct {
	HealthChecks HealthChecksConfig `yaml:"healthChecks"`
}

// HealthChecksConfig represents health check configuration
type HealthChecksConfig struct {
	Enabled  bool   `yaml:"enabled"`
	Interval string `yaml:"interval"`
}

// LoadConfig loads and parses the quickstart YAML config file
func LoadConfig(path string) (*QuickstartConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file %s: %w", path, err)
	}

	var config QuickstartConfig
	if err := yaml.Unmarshal(data, &config); err != nil {
		return nil, fmt.Errorf("failed to parse config file %s: %w", path, err)
	}

	// Apply defaults
	config.ApplyDefaults()

	// Validate the configuration
	if err := config.Validate(); err != nil {
		return nil, fmt.Errorf("invalid configuration: %w", err)
	}

	return &config, nil
}

// ApplyDefaults fills in default values for optional fields
func (c *QuickstartConfig) ApplyDefaults() {
	// Component type defaults
	if c.Spec.Source.Type == "" {
		c.Spec.Source.Type = "stellar-live-source-datalake"
	}
	if c.Spec.Processor.Type == "" {
		c.Spec.Processor.Type = "account-balance"
	}
	if c.Spec.Consumer.Type == "" {
		c.Spec.Consumer.Type = "duckdb"
	}

	// Network defaults
	if c.Spec.Source.Network == "" {
		c.Spec.Source.Network = "testnet"
	}

	// Auto-configure storage based on network if not explicitly set
	if c.Spec.Source.Storage.Type == "" {
		c.Spec.Source.Storage.Type = "gcs"
	}
	if c.Spec.Source.Storage.Bucket == "" {
		if c.Spec.Source.Network == "mainnet" {
			c.Spec.Source.Storage.Bucket = "obsrvr-stellar-ledger-data-mainnet-data"
			c.Spec.Source.Storage.Path = "landing/ledgers/mainnet"
		} else {
			c.Spec.Source.Storage.Bucket = "obsrvr-stellar-ledger-data-testnet-data"
			c.Spec.Source.Storage.Path = "landing/ledgers/testnet"
		}
	}

	// Ledger defaults
	if c.Spec.Source.Ledgers.End == 0 {
		// 0 means continuous streaming (no default change needed)
	}

	// Processor defaults
	if c.Spec.Processor.BatchSize == 0 {
		c.Spec.Processor.BatchSize = 5000
	}
	if c.Spec.Processor.GRPCPort == 0 {
		c.Spec.Processor.GRPCPort = 50054
	}
	if c.Spec.Processor.HealthPort == 0 {
		c.Spec.Processor.HealthPort = 8089
	}

	// Consumer defaults
	if c.Spec.Consumer.BatchSize == 0 {
		c.Spec.Consumer.BatchSize = 5000
	}
	if c.Spec.Consumer.HealthPort == 0 {
		c.Spec.Consumer.HealthPort = 8090
	}
	if c.Spec.Consumer.Database.Table.Name == "" {
		c.Spec.Consumer.Database.Table.Name = "account_balances"
	}

	// Observability defaults
	if c.Spec.Observability.HealthChecks.Interval == "" {
		c.Spec.Observability.HealthChecks.Interval = "30s"
	}
}

// Validate checks if the configuration is valid
func (c *QuickstartConfig) Validate() error {
	// API version check
	if c.APIVersion != "flowctl.io/v1alpha1" {
		return fmt.Errorf("unsupported apiVersion: %s (expected flowctl.io/v1alpha1)", c.APIVersion)
	}

	// Kind check
	if c.Kind != "QuickstartPipeline" {
		return fmt.Errorf("unsupported kind: %s (expected QuickstartPipeline)", c.Kind)
	}

	// Metadata validation
	if c.Metadata.Name == "" {
		return fmt.Errorf("metadata.name is required")
	}

	// Component type validation
	validSourceTypes := []string{"stellar-live-source-datalake"}
	if !contains(validSourceTypes, c.Spec.Source.Type) {
		return fmt.Errorf(
			"spec.source.type must be one of %v, got: %s\n\n"+
				"Available source types:\n"+
				"  - stellar-live-source-datalake: Stream ledgers from GCS\n"+
				"\nNote: More source types coming in future releases",
			validSourceTypes, c.Spec.Source.Type,
		)
	}

	validProcessorTypes := []string{"account-balance"}
	if !contains(validProcessorTypes, c.Spec.Processor.Type) {
		return fmt.Errorf(
			"spec.processor.type must be one of %v, got: %s\n\n"+
				"Available processor types:\n"+
				"  - account-balance: Extract account balance changes from ledgers\n"+
				"\nNote: More processor types (payment, operation, etc.) coming in future releases",
			validProcessorTypes, c.Spec.Processor.Type,
		)
	}

	validConsumerTypes := []string{"duckdb"}
	if !contains(validConsumerTypes, c.Spec.Consumer.Type) {
		return fmt.Errorf(
			"spec.consumer.type must be one of %v, got: %s\n\n"+
				"Available consumer types:\n"+
				"  - duckdb: Embedded DuckDB database\n"+
				"\nNote: More consumer types (postgresql, clickhouse, parquet) coming in future releases",
			validConsumerTypes, c.Spec.Consumer.Type,
		)
	}

	// Network validation
	if c.Spec.Source.Network != "testnet" && c.Spec.Source.Network != "mainnet" {
		return fmt.Errorf("spec.source.network must be 'testnet' or 'mainnet', got: %s", c.Spec.Source.Network)
	}

	// Ledger range validation
	if c.Spec.Source.Ledgers.Start == 0 {
		return fmt.Errorf("spec.source.ledgers.start is required and must be > 0")
	}
	if c.Spec.Source.Ledgers.End > 0 && c.Spec.Source.Ledgers.End < c.Spec.Source.Ledgers.Start {
		return fmt.Errorf(
			"spec.source.ledgers.end (%d) must be >= start (%d) or 0 for continuous streaming",
			c.Spec.Source.Ledgers.End,
			c.Spec.Source.Ledgers.Start,
		)
	}

	// Filter validation (mutually exclusive)
	filterCount := 0
	if len(c.Spec.Filters.Assets) > 0 {
		filterCount++
	}
	if len(c.Spec.Filters.AssetCodeOnly) > 0 {
		filterCount++
	}
	if c.Spec.Filters.IndexAll {
		filterCount++
	}

	if filterCount > 1 {
		return fmt.Errorf(
			"spec.filters: only one of 'assets', 'assetCodeOnly', or 'indexAll' can be set\n\n" +
				"Choose one filtering option:\n" +
				"  - Use 'assets' for specific assets with issuers\n" +
				"  - Use 'assetCodeOnly' for asset codes (any issuer)\n" +
				"  - Use 'indexAll: true' for all assets",
		)
	}

	// If assets are specified, validate them
	if len(c.Spec.Filters.Assets) > 0 {
		for i, asset := range c.Spec.Filters.Assets {
			if asset.Code == "" {
				return fmt.Errorf("spec.filters.assets[%d].code is required", i)
			}
			if asset.Issuer == "" {
				return fmt.Errorf("spec.filters.assets[%d].issuer is required", i)
			}
			// Basic issuer format validation (should start with G)
			if len(asset.Issuer) != 56 || asset.Issuer[0] != 'G' {
				return fmt.Errorf(
					"spec.filters.assets[%d].issuer appears invalid: %s\n"+
						"Expected Stellar address starting with 'G' (56 characters)",
					i, asset.Issuer,
				)
			}
		}
	}

	// Consumer database path validation
	if c.Spec.Consumer.Database.Path == "" {
		return fmt.Errorf("spec.consumer.database.path is required")
	}

	// Batch size validation
	if c.Spec.Processor.BatchSize < 1 || c.Spec.Processor.BatchSize > 100000 {
		return fmt.Errorf("spec.processor.batchSize must be between 1 and 100000, got: %d", c.Spec.Processor.BatchSize)
	}
	if c.Spec.Consumer.BatchSize < 1 || c.Spec.Consumer.BatchSize > 100000 {
		return fmt.Errorf("spec.consumer.batchSize must be between 1 and 100000, got: %d", c.Spec.Consumer.BatchSize)
	}

	// Port validation
	if c.Spec.Processor.GRPCPort < 1024 || c.Spec.Processor.GRPCPort > 65535 {
		return fmt.Errorf("spec.processor.grpcPort must be between 1024 and 65535, got: %d", c.Spec.Processor.GRPCPort)
	}
	if c.Spec.Processor.HealthPort < 1024 || c.Spec.Processor.HealthPort > 65535 {
		return fmt.Errorf("spec.processor.healthPort must be between 1024 and 65535, got: %d", c.Spec.Processor.HealthPort)
	}
	if c.Spec.Consumer.HealthPort < 1024 || c.Spec.Consumer.HealthPort > 65535 {
		return fmt.Errorf("spec.consumer.healthPort must be between 1024 and 65535, got: %d", c.Spec.Consumer.HealthPort)
	}

	return nil
}

// GetNetworkPassphrase returns the Stellar network passphrase based on the network
func (c *QuickstartConfig) GetNetworkPassphrase() string {
	if c.Spec.Source.Network == "mainnet" {
		return "Public Global Stellar Network ; September 2015"
	}
	return "Test SDF Network ; September 2015"
}

// HasAssetFilter returns true if any asset filtering is configured
func (c *QuickstartConfig) HasAssetFilter() bool {
	return len(c.Spec.Filters.Assets) > 0 || len(c.Spec.Filters.AssetCodeOnly) > 0
}

// ShouldFilterAsset returns true if this asset should be filtered out
// Returns false if the asset should be included
func (c *QuickstartConfig) ShouldFilterAsset(assetCode, assetIssuer string) bool {
	// If indexAll is true, include everything
	if c.Spec.Filters.IndexAll {
		return false
	}

	// If no filters are configured, include everything
	if !c.HasAssetFilter() {
		return false
	}

	// Check asset code only filters
	if len(c.Spec.Filters.AssetCodeOnly) > 0 {
		for _, code := range c.Spec.Filters.AssetCodeOnly {
			if code == assetCode {
				return false // Include this asset
			}
		}
		return true // Not in the list, filter it out
	}

	// Check specific asset filters
	if len(c.Spec.Filters.Assets) > 0 {
		for _, asset := range c.Spec.Filters.Assets {
			if asset.Code == assetCode && asset.Issuer == assetIssuer {
				return false // Include this asset
			}
		}
		return true // Not in the list, filter it out
	}

	// Default: include everything
	return false
}

// contains checks if a string slice contains a specific string
func contains(slice []string, item string) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}
