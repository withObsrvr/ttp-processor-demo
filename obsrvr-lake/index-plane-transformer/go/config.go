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
	IndexCold  IndexColdConfig  `yaml:"index_cold"`
	Checkpoint CheckpointConfig `yaml:"checkpoint"`
	Health     HealthConfig     `yaml:"health"`
}

// ServiceConfig contains service-level configuration
type ServiceConfig struct {
	Name                string `yaml:"name"`
	Version             string `yaml:"version"`
	PollIntervalSeconds int    `yaml:"poll_interval_seconds"`
	BatchSize           int64  `yaml:"batch_size"`
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

// IndexColdConfig contains Index Plane DuckLake target configuration
type IndexColdConfig struct {
	CatalogName       string `yaml:"catalog_name"`
	SchemaName        string `yaml:"schema_name"`
	TableName         string `yaml:"table_name"`
	DuckDBPath        string `yaml:"duckdb_path"`
	CatalogHost       string `yaml:"catalog_host"`
	CatalogPort       int    `yaml:"catalog_port"`
	CatalogDatabase   string `yaml:"catalog_database"`
	CatalogUser       string `yaml:"catalog_user"`
	CatalogPassword   string `yaml:"catalog_password"`
	S3Endpoint        string `yaml:"s3_endpoint"`
	S3Region          string `yaml:"s3_region"`
	S3AccessKeyID     string `yaml:"s3_access_key_id"`
	S3SecretAccessKey string `yaml:"s3_secret_access_key"`
	S3Bucket          string `yaml:"s3_bucket"`
	PartitionSize     int64  `yaml:"partition_size"`
}

// CheckpointConfig contains PostgreSQL checkpoint configuration
type CheckpointConfig struct {
	Table string `yaml:"table"`
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
	if config.Service.PollIntervalSeconds == 0 {
		config.Service.PollIntervalSeconds = 30
	}
	if config.Service.BatchSize == 0 {
		config.Service.BatchSize = 10000
	}
	if config.BronzeHot.SSLMode == "" {
		config.BronzeHot.SSLMode = "require"
	}
	if config.IndexCold.PartitionSize == 0 {
		config.IndexCold.PartitionSize = 100000
	}
	if config.Checkpoint.Table == "" {
		config.Checkpoint.Table = "index.transformer_checkpoint"
	}
	if config.Health.Port == 0 {
		config.Health.Port = 8096
	}

	return &config, nil
}

// GetPollInterval returns the poll interval as a duration
func (c *Config) GetPollInterval() time.Duration {
	return time.Duration(c.Service.PollIntervalSeconds) * time.Second
}

// ConnectionString builds a PostgreSQL connection string for BronzeHot
func (d *DatabaseConfig) ConnectionString() string {
	return fmt.Sprintf(
		"host=%s port=%d dbname=%s user=%s password=%s sslmode=%s",
		d.Host, d.Port, d.Database, d.User, d.Password, d.SSLMode,
	)
}
