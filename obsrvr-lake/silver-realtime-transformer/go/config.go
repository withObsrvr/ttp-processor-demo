package main

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

// Config represents the service configuration
type Config struct {
	Service          ServiceConfig          `yaml:"service"`
	BronzeSource     BronzeSourceConfig     `yaml:"bronze_source"` // gRPC source (replaces polling)
	BronzeHot        DatabaseConfig         `yaml:"bronze_hot"`
	BronzeCold       *DuckLakeConfig        `yaml:"bronze_cold,omitempty"`
	S3               *S3Config              `yaml:"s3,omitempty"`
	SilverHot        DatabaseConfig         `yaml:"silver_hot"`
	Checkpoint       CheckpointConfig       `yaml:"checkpoint"`
	Performance      PerformanceConfig      `yaml:"performance"`
	GapDetection     GapDetectionConfig     `yaml:"gap_detection"`
	Fallback         FallbackConfig         `yaml:"fallback"`
	SorobanMigration SorobanMigrationConfig `yaml:"soroban_migration"`
}

// BronzeSourceConfig configures the gRPC connection to the bronze ingester's SourceService.
// When mode is "grpc", the transformer receives push notifications instead of polling.
type BronzeSourceConfig struct {
	Mode     string `yaml:"mode"`     // "poll" (default) or "grpc"
	Endpoint string `yaml:"endpoint"` // gRPC endpoint, e.g. "stellar-postgres-ingester:50054"
}

// DuckLakeConfig holds DuckLake (cold storage) connection settings
type DuckLakeConfig struct {
	CatalogPath    string `yaml:"catalog_path"`
	DataPath       string `yaml:"data_path"`
	CatalogName    string `yaml:"catalog_name"`
	SchemaName     string `yaml:"schema_name"`
	MetadataSchema string `yaml:"metadata_schema"`
}

// S3Config holds S3/Backblaze B2 credentials for cold storage access
type S3Config struct {
	KeyID    string `yaml:"key_id"`
	Secret   string `yaml:"secret"`
	Region   string `yaml:"region"`
	Endpoint string `yaml:"endpoint"`
}

// FallbackConfig holds settings for cold storage fallback behavior
type FallbackConfig struct {
	Enabled           bool `yaml:"enabled"`
	BackfillBatchSize int  `yaml:"backfill_batch_size"`
}

// ServiceConfig holds service-level settings
type ServiceConfig struct {
	Name                string `yaml:"name"`
	HealthPort          string `yaml:"health_port"`
	GRPCPort            int    `yaml:"grpc_port"`
	PollIntervalSeconds int    `yaml:"poll_interval_seconds"`
}

// SorobanMigrationConfig controls one-time startup repair behavior.
// The address balance rebuilds can be extremely expensive on large datasets,
// so they are disabled by default and should be run explicitly as a separate
// backfill/maintenance operation.
type SorobanMigrationConfig struct {
	RebuildAddressBalancesOnStartup bool `yaml:"rebuild_address_balances_on_startup"`
	RebuildStateBalancesOnStartup   bool `yaml:"rebuild_state_balances_on_startup"`
}

// DatabaseConfig holds database connection settings
type DatabaseConfig struct {
	Host                   string `yaml:"host"`
	Port                   int    `yaml:"port"`
	Database               string `yaml:"database"`
	User                   string `yaml:"user"`
	Password               string `yaml:"password"`
	SSLMode                string `yaml:"sslmode"`
	MaxOpenConns           int    `yaml:"max_open_conns"`
	MaxIdleConns           int    `yaml:"max_idle_conns"`
	ConnMaxLifetimeSeconds int    `yaml:"conn_max_lifetime_seconds"`
	ConnMaxIdleTimeSeconds int    `yaml:"conn_max_idle_time_seconds"`
}

// CheckpointConfig holds checkpoint tracking settings
type CheckpointConfig struct {
	Table string `yaml:"table"`
}

// PerformanceConfig holds performance tuning settings
type PerformanceConfig struct {
	BatchSize        int `yaml:"batch_size"`
	MaxWorkers       int `yaml:"max_workers"` // Deprecated: used as fallback for read/write worker limits
	MaxBronzeReaders int `yaml:"max_bronze_readers"`
	MaxSilverWriters int `yaml:"max_silver_writers"`
	InsertBatchSize  int `yaml:"insert_batch_size"` // Max rows per multi-row INSERT (default: 500)
}

// GapDetectionConfig holds gap detection and recovery settings
type GapDetectionConfig struct {
	// MaxEmptyPolls is the number of consecutive empty polls before checking for gaps
	MaxEmptyPolls int `yaml:"max_empty_polls"`
	// AutoSkip enables automatic checkpoint advancement when a gap is detected
	AutoSkip bool `yaml:"auto_skip"`
	// BackfillMaxEmptyPolls is the number of consecutive empty polls in backfill mode
	// before skipping to hot storage. When cold storage also has a gap, the transformer
	// will advance the checkpoint to hot MIN after this many empty polls.
	// Default: MaxEmptyPolls * 3 (or 30 if MaxEmptyPolls is 0)
	BackfillMaxEmptyPolls int `yaml:"backfill_max_empty_polls"`
}

// LoadConfig loads configuration from a YAML file
func LoadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	var config Config
	if err := yaml.Unmarshal(data, &config); err != nil {
		return nil, fmt.Errorf("failed to parse config: %w", err)
	}

	return &config, nil
}

// Validate validates the configuration
func (c *Config) Validate() error {
	if c.Service.PollIntervalSeconds < 1 {
		return fmt.Errorf("poll_interval_seconds must be at least 1")
	}

	if c.Performance.BatchSize < 1 {
		return fmt.Errorf("batch_size must be at least 1")
	}

	if c.Performance.MaxWorkers < 0 {
		return fmt.Errorf("max_workers must be >= 0")
	}

	if c.Performance.MaxBronzeReaders < 0 {
		return fmt.Errorf("max_bronze_readers must be >= 0")
	}

	if c.Performance.MaxSilverWriters < 0 {
		return fmt.Errorf("max_silver_writers must be >= 0")
	}

	if c.MaxBronzeReaders() < 1 {
		return fmt.Errorf("effective max bronze readers must be at least 1")
	}

	if c.MaxSilverWriters() < 1 {
		return fmt.Errorf("effective max silver writers must be at least 1")
	}

	// Validate bronze source config
	switch c.BronzeSource.Mode {
	case "", "poll":
		// poll is default, no extra config needed
	case "grpc":
		if c.BronzeSource.Endpoint == "" {
			return fmt.Errorf("bronze_source.endpoint is required when mode is \"grpc\"")
		}
	default:
		return fmt.Errorf("bronze_source.mode must be \"poll\" or \"grpc\", got %q", c.BronzeSource.Mode)
	}

	// Validate fallback config if enabled
	if c.Fallback.Enabled {
		if c.BronzeCold == nil {
			return fmt.Errorf("bronze_cold config required when fallback is enabled")
		}
		if c.S3 == nil {
			return fmt.Errorf("s3 config required when fallback is enabled")
		}
		if c.BronzeCold.CatalogPath == "" {
			return fmt.Errorf("bronze_cold.catalog_path required when fallback is enabled")
		}
		if c.BronzeCold.DataPath == "" {
			return fmt.Errorf("bronze_cold.data_path required when fallback is enabled")
		}
	}

	return nil
}

// GetBackfillBatchSize returns the batch size for backfill operations
// Falls back to performance.batch_size if not explicitly set
func (c *Config) GetBackfillBatchSize() int {
	if c.Fallback.BackfillBatchSize > 0 {
		return c.Fallback.BackfillBatchSize
	}
	return c.Performance.BatchSize
}

// PollInterval returns the poll interval as a Duration
func (c *Config) PollInterval() time.Duration {
	return time.Duration(c.Service.PollIntervalSeconds) * time.Second
}

func (c *Config) MaxBronzeReaders() int {
	if c.Performance.MaxBronzeReaders > 0 {
		return c.Performance.MaxBronzeReaders
	}
	if c.Performance.MaxWorkers > 0 {
		return c.Performance.MaxWorkers
	}
	return 4
}

func (c *Config) MaxSilverWriters() int {
	if c.Performance.MaxSilverWriters > 0 {
		return c.Performance.MaxSilverWriters
	}
	if c.Performance.MaxWorkers > 0 {
		return c.Performance.MaxWorkers
	}
	return 4
}

func (d *DatabaseConfig) MaxIdleConnsOrDefault() int {
	if d.MaxIdleConns > 0 {
		return d.MaxIdleConns
	}
	if d.MaxOpenConns > 0 {
		return d.MaxOpenConns
	}
	return 4
}

func (d *DatabaseConfig) ConnMaxLifetime() time.Duration {
	if d.ConnMaxLifetimeSeconds <= 0 {
		return 0
	}
	return time.Duration(d.ConnMaxLifetimeSeconds) * time.Second
}

func (d *DatabaseConfig) ConnMaxIdleTime() time.Duration {
	if d.ConnMaxIdleTimeSeconds <= 0 {
		return 0
	}
	return time.Duration(d.ConnMaxIdleTimeSeconds) * time.Second
}

// ConnectionString builds a PostgreSQL connection string
func (d *DatabaseConfig) ConnectionString() string {
	return fmt.Sprintf(
		"host=%s port=%d dbname=%s user=%s password=%s sslmode=%s",
		d.Host, d.Port, d.Database, d.User, d.Password, d.SSLMode,
	)
}
