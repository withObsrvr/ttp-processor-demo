package main

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

// Config holds all configuration for the Bronze → Silver transformer
type Config struct {
	Service   ServiceConfig   `yaml:"service"`
	DuckLake  DuckLakeConfig  `yaml:"ducklake"`
	Checkpoint CheckpointConfig `yaml:"checkpoint"`
}

// ServiceConfig contains service-level settings
type ServiceConfig struct {
	Name                     string `yaml:"name"`
	HealthPort               string `yaml:"health_port"`
	TransformIntervalMinutes int    `yaml:"transform_interval_minutes"`
}

// DuckLakeConfig contains DuckLake catalog and schema settings
type DuckLakeConfig struct {
	// PostgreSQL catalog connection string
	// Format: "ducklake:postgres:postgresql://user:pass@host:port/db?sslmode=require"
	CatalogPath    string `yaml:"catalog_path"`
	CatalogName    string `yaml:"catalog_name"`
	MetadataSchema string `yaml:"metadata_schema"`

	// Bronze schema (source)
	BronzeSchema   string `yaml:"bronze_schema"`
	BronzeDataPath string `yaml:"bronze_data_path"`

	// Silver schema (destination)
	SilverSchema   string `yaml:"silver_schema"`
	SilverDataPath string `yaml:"silver_data_path"`

	// S3/B2 credentials
	AWSAccessKeyID     string `yaml:"aws_access_key_id"`
	AWSSecretAccessKey string `yaml:"aws_secret_access_key"`
	AWSRegion          string `yaml:"aws_region"`
	AWSEndpoint        string `yaml:"aws_endpoint"`
}

// CheckpointConfig contains checkpoint settings
type CheckpointConfig struct {
	Table  string `yaml:"table"`  // Table name to store checkpoint
	Schema string `yaml:"schema"` // Schema to store checkpoint table
}

// LoadConfig loads configuration from YAML file
func LoadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	var config Config
	if err := yaml.Unmarshal(data, &config); err != nil {
		return nil, fmt.Errorf("failed to parse config: %w", err)
	}

	// Set defaults
	if config.Service.HealthPort == "" {
		config.Service.HealthPort = "8093"
	}
	if config.Service.TransformIntervalMinutes == 0 {
		config.Service.TransformIntervalMinutes = 15
	}
	if config.DuckLake.CatalogName == "" {
		config.DuckLake.CatalogName = "testnet_catalog"
	}
	if config.DuckLake.MetadataSchema == "" {
		config.DuckLake.MetadataSchema = "ducklake_meta"
	}
	if config.DuckLake.BronzeSchema == "" {
		config.DuckLake.BronzeSchema = "bronze"
	}
	if config.DuckLake.SilverSchema == "" {
		config.DuckLake.SilverSchema = "silver"
	}
	if config.Checkpoint.Table == "" {
		config.Checkpoint.Table = "silver_transformer_checkpoint"
	}
	if config.Checkpoint.Schema == "" {
		config.Checkpoint.Schema = "bronze"
	}

	return &config, nil
}

// Validate checks if the configuration is valid
func (c *Config) Validate() error {
	if c.DuckLake.CatalogPath == "" {
		return fmt.Errorf("ducklake.catalog_path is required")
	}
	if c.DuckLake.BronzeDataPath == "" {
		return fmt.Errorf("ducklake.bronze_data_path is required")
	}
	if c.DuckLake.SilverDataPath == "" {
		return fmt.Errorf("ducklake.silver_data_path is required")
	}
	if c.DuckLake.AWSAccessKeyID == "" {
		return fmt.Errorf("ducklake.aws_access_key_id is required")
	}
	if c.DuckLake.AWSSecretAccessKey == "" {
		return fmt.Errorf("ducklake.aws_secret_access_key is required")
	}
	if c.Service.TransformIntervalMinutes < 5 {
		return fmt.Errorf("service.transform_interval_minutes must be at least 5")
	}
	return nil
}

// TransformInterval returns the transformation interval as a Duration
func (c *Config) TransformInterval() time.Duration {
	return time.Duration(c.Service.TransformIntervalMinutes) * time.Minute
}
