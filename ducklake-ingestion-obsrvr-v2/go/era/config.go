// Package era provides era and version configuration for Bronze Copier V2.
// Eras represent processing epochs (e.g., pre-Protocol 23 vs post-Protocol 23)
// and allow for isolated catalog naming and storage paths.
package era

import (
	"fmt"
	"strings"
)

// Config holds era/version configuration.
type Config struct {
	// EraID identifies the processing era (e.g., "pre_p23", "p23_plus")
	EraID string `yaml:"era_id"`

	// VersionLabel is the version within an era (e.g., "v1", "v2")
	VersionLabel string `yaml:"version_label"`

	// Network is the Stellar network (e.g., "mainnet", "testnet")
	Network string `yaml:"network"`
}

// ApplyDefaults sets default values for era config.
func (c *Config) ApplyDefaults() {
	if c.EraID == "" {
		c.EraID = "p23_plus" // Default to Protocol 23+ era
	}
	if c.VersionLabel == "" {
		c.VersionLabel = "v1"
	}
	if c.Network == "" {
		c.Network = "testnet"
	}
}

// Validate checks if the era config is valid.
func (c *Config) Validate() error {
	if c.EraID == "" {
		return fmt.Errorf("era_id is required")
	}
	if c.VersionLabel == "" {
		return fmt.Errorf("version_label is required")
	}
	if c.Network == "" {
		return fmt.Errorf("network is required")
	}

	// Validate era_id format (alphanumeric + underscore)
	for _, r := range c.EraID {
		if !((r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '_') {
			return fmt.Errorf("era_id must be lowercase alphanumeric with underscores")
		}
	}

	// Validate version_label format
	if !strings.HasPrefix(c.VersionLabel, "v") {
		return fmt.Errorf("version_label must start with 'v' (e.g., 'v1', 'v2')")
	}

	return nil
}

// CatalogName generates the DuckLake catalog name based on era config.
// Format: {network}_bronze_{era}_{version}
// Example: mainnet_bronze_p23_plus_v1
func (c *Config) CatalogName() string {
	return fmt.Sprintf("%s_bronze_%s_%s", c.Network, c.EraID, c.VersionLabel)
}

// SchemaName returns the schema name (same as network for simplicity).
func (c *Config) SchemaName() string {
	return c.Network
}

// StoragePath generates a storage path segment for era isolation.
// Example: bronze/mainnet/p23_plus/v1
func (c *Config) StoragePath() string {
	return fmt.Sprintf("bronze/%s/%s/%s", c.Network, c.EraID, c.VersionLabel)
}

// ProducerID generates a unique producer identifier for PAS events.
func (c *Config) ProducerID(instanceID string) string {
	return fmt.Sprintf("bronze-copier-%s-%s-%s", c.Network, c.EraID, instanceID)
}

// String returns a human-readable representation.
func (c *Config) String() string {
	return fmt.Sprintf("%s/%s/%s", c.Network, c.EraID, c.VersionLabel)
}

// Known era identifiers.
const (
	// EraPreP23 represents data before Protocol 23 (no Soroban)
	EraPreP23 = "pre_p23"

	// EraP23Plus represents data from Protocol 23 onwards (Soroban enabled)
	EraP23Plus = "p23_plus"
)

// NewFromNetworkPassphrase creates an EraConfig from a network passphrase.
func NewFromNetworkPassphrase(passphrase string) *Config {
	cfg := &Config{
		EraID:        EraP23Plus,
		VersionLabel: "v1",
	}

	if passphrase == "Public Global Stellar Network ; September 2015" {
		cfg.Network = "mainnet"
	} else if passphrase == "Test SDF Network ; September 2015" {
		cfg.Network = "testnet"
	} else {
		cfg.Network = "unknown"
	}

	return cfg
}

// IsCompatible checks if two era configs are compatible for merging data.
func (c *Config) IsCompatible(other *Config) bool {
	return c.Network == other.Network && c.EraID == other.EraID
}
