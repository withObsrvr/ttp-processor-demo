package main

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

// Config represents the transformer configuration
type Config struct {
	Service    ServiceConfig    `yaml:"service"`
	BronzeHot  DatabaseConfig   `yaml:"bronze_hot"`
	Catalog    DatabaseConfig   `yaml:"catalog"`
	S3         S3Config         `yaml:"s3"`
	Indexing   IndexingConfig   `yaml:"indexing"`
	Health     HealthConfig     `yaml:"health"`
}

// ServiceConfig contains service-level configuration
type ServiceConfig struct {
	Name    string `yaml:"name"`
	Version string `yaml:"version"`
}

// DatabaseConfig holds PostgreSQL connection settings
type DatabaseConfig struct {
	Host     string `yaml:"host"`
	Port     int    `yaml:"port"`
	Database string `yaml:"database"`
	User     string `yaml:"user"`
	Password string `yaml:"password"`
	SSLMode  string `yaml:"sslmode"`
}

// S3Config contains S3/B2 storage configuration
type S3Config struct {
	AccessKeyID     string `yaml:"access_key_id"`
	SecretAccessKey string `yaml:"secret_access_key"`
	Endpoint        string `yaml:"endpoint"`
	Region          string `yaml:"region"`
	Bucket          string `yaml:"bucket"`
}

// IndexingConfig contains indexing behavior configuration
type IndexingConfig struct {
	PollInterval   string `yaml:"poll_interval"`    // e.g., "30s"
	BatchSize      int64  `yaml:"batch_size"`       // Max ledgers per batch
	CheckpointFile string `yaml:"checkpoint_file"`  // Path to checkpoint.json
}

// HealthConfig contains health endpoint configuration
type HealthConfig struct {
	Port int `yaml:"port"`
}

// LoadConfig loads configuration from a YAML file
func LoadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	// Expand environment variables
	expanded := os.ExpandEnv(string(data))

	var config Config
	if err := yaml.Unmarshal([]byte(expanded), &config); err != nil {
		return nil, fmt.Errorf("failed to parse config: %w", err)
	}

	// Set defaults
	if config.Service.Name == "" {
		config.Service.Name = "contract-event-index-transformer"
	}
	if config.Service.Version == "" {
		config.Service.Version = "1.0.0"
	}
	if config.BronzeHot.SSLMode == "" {
		config.BronzeHot.SSLMode = "require"
	}
	if config.Catalog.SSLMode == "" {
		config.Catalog.SSLMode = "require"
	}
	if config.Indexing.PollInterval == "" {
		config.Indexing.PollInterval = "30s"
	}
	if config.Indexing.BatchSize == 0 {
		config.Indexing.BatchSize = 1000
	}
	if config.Indexing.CheckpointFile == "" {
		config.Indexing.CheckpointFile = "checkpoint.json"
	}
	if config.Health.Port == 0 {
		config.Health.Port = 8096
	}

	return &config, nil
}

// GetPollInterval returns the poll interval as a duration
func (c *Config) GetPollInterval() (time.Duration, error) {
	return time.ParseDuration(c.Indexing.PollInterval)
}

// ConnectionString builds a PostgreSQL connection string
func (d *DatabaseConfig) ConnectionString() string {
	return fmt.Sprintf(
		"host=%s port=%d dbname=%s user=%s password=%s sslmode=%s",
		d.Host, d.Port, d.Database, d.User, d.Password, d.SSLMode,
	)
}

// CatalogPath builds the DuckLake catalog connection string
func (c *Config) CatalogPath() string {
	return fmt.Sprintf(
		"ducklake:postgres:postgresql://%s:%s@%s:%d/%s?sslmode=%s",
		c.Catalog.User,
		c.Catalog.Password,
		c.Catalog.Host,
		c.Catalog.Port,
		c.Catalog.Database,
		c.Catalog.SSLMode,
	)
}

// DataPath builds the S3 data path
func (c *Config) DataPath() string {
	return fmt.Sprintf("s3://%s/", c.S3.Bucket)
}
