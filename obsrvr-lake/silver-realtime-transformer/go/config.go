package main

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

// Config represents the service configuration
type Config struct {
	Service      ServiceConfig      `yaml:"service"`
	BronzeHot    DatabaseConfig     `yaml:"bronze_hot"`
	BronzeCold   *DuckLakeConfig    `yaml:"bronze_cold,omitempty"`
	S3           *S3Config          `yaml:"s3,omitempty"`
	SilverHot    DatabaseConfig     `yaml:"silver_hot"`
	Checkpoint   CheckpointConfig   `yaml:"checkpoint"`
	Performance  PerformanceConfig  `yaml:"performance"`
	GapDetection GapDetectionConfig `yaml:"gap_detection"`
	Fallback     FallbackConfig     `yaml:"fallback"`
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
	Name               string `yaml:"name"`
	HealthPort         string `yaml:"health_port"`
	PollIntervalSeconds int   `yaml:"poll_interval_seconds"`
}

// DatabaseConfig holds database connection settings
type DatabaseConfig struct {
	Host     string `yaml:"host"`
	Port     int    `yaml:"port"`
	Database string `yaml:"database"`
	User     string `yaml:"user"`
	Password string `yaml:"password"`
	SSLMode  string `yaml:"sslmode"`
}

// CheckpointConfig holds checkpoint tracking settings
type CheckpointConfig struct {
	Table string `yaml:"table"`
}

// PerformanceConfig holds performance tuning settings
type PerformanceConfig struct {
	BatchSize  int `yaml:"batch_size"`
	MaxWorkers int `yaml:"max_workers"`
}

// GapDetectionConfig holds gap detection and recovery settings
type GapDetectionConfig struct {
	// MaxEmptyPolls is the number of consecutive empty polls before checking for gaps
	MaxEmptyPolls int `yaml:"max_empty_polls"`
	// AutoSkip enables automatic checkpoint advancement when a gap is detected
	AutoSkip bool `yaml:"auto_skip"`
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

	if c.Performance.MaxWorkers < 1 {
		return fmt.Errorf("max_workers must be at least 1")
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

// ConnectionString builds a PostgreSQL connection string
func (d *DatabaseConfig) ConnectionString() string {
	return fmt.Sprintf(
		"host=%s port=%d dbname=%s user=%s password=%s sslmode=%s",
		d.Host, d.Port, d.Database, d.User, d.Password, d.SSLMode,
	)
}
