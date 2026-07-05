package main

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

// Config represents the transformer configuration
type Config struct {
	Service      ServiceConfig      `yaml:"service"`
	BronzeSource BronzeSourceConfig `yaml:"bronze_source"`
	SilverHot    DatabaseConfig     `yaml:"silver_hot"`
	SilverCold   SilverColdConfig   `yaml:"silver_cold"`
	Catalog      DatabaseConfig     `yaml:"catalog"`
	IndexCold    IndexColdConfig    `yaml:"index_cold"`
	Account      AccountIndexConfig `yaml:"account_index"`
	Checkpoint   CheckpointConfig   `yaml:"checkpoint"`
	Backfill     CheckpointConfig   `yaml:"backfill_checkpoint"`
	Maintenance  MaintenanceConfig  `yaml:"maintenance"`
	Health       HealthConfig       `yaml:"health"`
}

// BronzeSourceConfig configures the gRPC connection to the bronze ingester's SourceService.
type BronzeSourceConfig struct {
	Mode     string `yaml:"mode"`     // "poll" (default) or "grpc"
	Endpoint string `yaml:"endpoint"` // gRPC endpoint, e.g. "localhost:50054"
}

// ServiceConfig contains service-level configuration
type ServiceConfig struct {
	Name                   string `yaml:"name"`
	Version                string `yaml:"version"`
	PollIntervalSeconds    int    `yaml:"poll_interval_seconds"`
	BatchSize              int64  `yaml:"batch_size"`
	AllowRetentionGapStart bool   `yaml:"allow_retention_gap_start"`
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
	CatalogSSLMode    string `yaml:"catalog_sslmode"`
	S3Endpoint        string `yaml:"s3_endpoint"`
	S3Region          string `yaml:"s3_region"`
	S3AccessKeyID     string `yaml:"s3_access_key_id"`
	S3SecretAccessKey string `yaml:"s3_secret_access_key"`
	S3Bucket          string `yaml:"s3_bucket"`
	PartitionSize     int64  `yaml:"partition_size"`
	TableKind         string `yaml:"table_kind"`
}

type SilverColdConfig struct {
	CatalogName       string `yaml:"catalog_name"`
	SchemaName        string `yaml:"schema_name"`
	MetadataSchema    string `yaml:"metadata_schema"`
	DuckDBPath        string `yaml:"duckdb_path"`
	CatalogHost       string `yaml:"catalog_host"`
	CatalogPort       int    `yaml:"catalog_port"`
	CatalogDatabase   string `yaml:"catalog_database"`
	CatalogUser       string `yaml:"catalog_user"`
	CatalogPassword   string `yaml:"catalog_password"`
	CatalogSSLMode    string `yaml:"catalog_sslmode"`
	S3Endpoint        string `yaml:"s3_endpoint"`
	S3Region          string `yaml:"s3_region"`
	S3AccessKeyID     string `yaml:"s3_access_key_id"`
	S3SecretAccessKey string `yaml:"s3_secret_access_key"`
	S3Bucket          string `yaml:"s3_bucket"`
	DataPath          string `yaml:"data_path"`
}

type AccountIndexConfig struct {
	AccountBucketCount int `yaml:"account_bucket_count"`
}

// CheckpointConfig contains PostgreSQL checkpoint configuration
type CheckpointConfig struct {
	Table string `yaml:"table"`
}

// MaintenanceConfig contains DuckLake maintenance settings
type MaintenanceConfig struct {
	Enabled           bool `yaml:"enabled"`
	EveryNWrites      int  `yaml:"every_n_writes"`
	MaxCompactedFiles int  `yaml:"max_compacted_files"`
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
	if config.SilverHot.SSLMode == "" {
		config.SilverHot.SSLMode = "require"
	}
	if config.IndexCold.CatalogSSLMode == "" {
		config.IndexCold.CatalogSSLMode = "require"
	}
	if config.Catalog.SSLMode == "" {
		config.Catalog.SSLMode = config.IndexCold.CatalogSSLMode
		if config.Catalog.SSLMode == "" {
			config.Catalog.SSLMode = "require"
		}
	}
	if config.Catalog.Host == "" {
		config.Catalog = config.DefaultCatalogDatabase()
	}
	if config.IndexCold.PartitionSize == 0 {
		config.IndexCold.PartitionSize = 100000
	}
	if config.Checkpoint.Table == "" {
		config.Checkpoint.Table = "index.account_ledger_transformer_checkpoint"
	}
	if config.Backfill.Table == "" {
		config.Backfill.Table = "index.account_ledger_backfill_checkpoint"
	}
	if config.IndexCold.TableName == "" {
		config.IndexCold.TableName = "account_ledger_index"
	}
	config.IndexCold.TableKind = "account_ledger"
	config.applySilverColdDefaults()
	if config.Account.AccountBucketCount == 0 {
		config.Account.AccountBucketCount = defaultAccountIndexBuckets
	}
	if config.Maintenance.EveryNWrites == 0 {
		config.Maintenance.EveryNWrites = 100
	}
	if config.Maintenance.MaxCompactedFiles == 0 {
		config.Maintenance.MaxCompactedFiles = 200
	}
	if config.Health.Port == 0 {
		config.Health.Port = 8096
	}

	// Validate bronze source config
	if config.BronzeSource.Mode == "grpc" && config.BronzeSource.Endpoint == "" {
		return nil, fmt.Errorf("bronze_source.endpoint is required when mode is \"grpc\"")
	}
	if config.Catalog.Host == "" {
		return nil, fmt.Errorf("catalog.host is required or index_cold catalog connection fields must be set")
	}

	return &config, nil
}

func (c *Config) applySilverColdDefaults() {
	if c.SilverCold.CatalogName == "" {
		c.SilverCold.CatalogName = c.IndexCold.CatalogName
	}
	if c.SilverCold.SchemaName == "" {
		c.SilverCold.SchemaName = "silver"
	}
	if c.SilverCold.MetadataSchema == "" {
		c.SilverCold.MetadataSchema = "silver_meta"
	}
	if c.SilverCold.DuckDBPath == "" {
		c.SilverCold.DuckDBPath = "/tmp/duckdb/account_index_silver_reader.db"
	}
	if c.SilverCold.CatalogHost == "" {
		c.SilverCold.CatalogHost = c.IndexCold.CatalogHost
		c.SilverCold.CatalogPort = c.IndexCold.CatalogPort
		c.SilverCold.CatalogDatabase = c.IndexCold.CatalogDatabase
		c.SilverCold.CatalogUser = c.IndexCold.CatalogUser
		c.SilverCold.CatalogPassword = c.IndexCold.CatalogPassword
		c.SilverCold.CatalogSSLMode = c.IndexCold.CatalogSSLMode
	}
	if c.SilverCold.CatalogSSLMode == "" {
		c.SilverCold.CatalogSSLMode = "require"
	}
	if c.SilverCold.S3Endpoint == "" {
		c.SilverCold.S3Endpoint = c.IndexCold.S3Endpoint
	}
	if c.SilverCold.S3Region == "" {
		c.SilverCold.S3Region = c.IndexCold.S3Region
	}
	if c.SilverCold.S3AccessKeyID == "" {
		c.SilverCold.S3AccessKeyID = c.IndexCold.S3AccessKeyID
	}
	if c.SilverCold.S3SecretAccessKey == "" {
		c.SilverCold.S3SecretAccessKey = c.IndexCold.S3SecretAccessKey
	}
	if c.SilverCold.S3Bucket == "" {
		c.SilverCold.S3Bucket = c.IndexCold.S3Bucket
	}
	if c.SilverCold.DataPath == "" && c.SilverCold.S3Bucket != "" {
		c.SilverCold.DataPath = fmt.Sprintf("s3://%s/", c.SilverCold.S3Bucket)
	}
}

func (c *Config) AccountBucketCount() int {
	if c.Account.AccountBucketCount == 0 {
		return defaultAccountIndexBuckets
	}
	return c.Account.AccountBucketCount
}

func (c *Config) IndexConfig() IndexColdConfig {
	cfg := c.IndexCold
	cfg.TableKind = "account_ledger"
	return cfg
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

func (c *Config) DefaultCatalogDatabase() DatabaseConfig {
	if c.IndexCold.CatalogHost != "" {
		return DatabaseConfig{
			Host:     c.IndexCold.CatalogHost,
			Port:     c.IndexCold.CatalogPort,
			Database: c.IndexCold.CatalogDatabase,
			User:     c.IndexCold.CatalogUser,
			Password: c.IndexCold.CatalogPassword,
			SSLMode:  c.IndexCold.CatalogSSLMode,
		}
	}
	return DatabaseConfig{}
}
