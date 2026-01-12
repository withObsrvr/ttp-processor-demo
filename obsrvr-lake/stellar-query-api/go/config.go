package main

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Service        ServiceConfig          `yaml:"service"`
	Postgres       PostgresConfig         `yaml:"postgres"`
	PostgresSilver *PostgresConfig        `yaml:"postgres_silver,omitempty"`
	DuckLake       DuckLakeConfig         `yaml:"ducklake"`
	DuckLakeSilver *DuckLakeConfig        `yaml:"ducklake_silver,omitempty"`
	Index          *IndexConfig           `yaml:"index,omitempty"`
	ContractIndex  *ContractIndexConfig   `yaml:"contract_index,omitempty"`
	Query          QueryConfig            `yaml:"query"`
	Unified        *UnifiedReaderConfig   `yaml:"unified,omitempty"` // Config for DuckDB ATTACH unified reader
}

type ServiceConfig struct {
	Name                string `yaml:"name"`
	Port                int    `yaml:"port"`
	ReadTimeoutSeconds  int    `yaml:"read_timeout_seconds"`
	WriteTimeoutSeconds int    `yaml:"write_timeout_seconds"`
}

type PostgresConfig struct {
	Host           string `yaml:"host"`
	Port           int    `yaml:"port"`
	Database       string `yaml:"database"`
	User           string `yaml:"user"`
	Password       string `yaml:"password"`
	SSLMode        string `yaml:"sslmode"`
	MaxConnections int    `yaml:"max_connections"`
}

type DuckLakeConfig struct {
	CatalogPath         string `yaml:"catalog_path"`
	DataPath            string `yaml:"data_path"`
	CatalogName         string `yaml:"catalog_name"`
	SchemaName          string `yaml:"schema_name"`
	MetadataSchema      string `yaml:"metadata_schema"`
	AWSAccessKeyID      string `yaml:"aws_access_key_id"`
	AWSSecretAccessKey  string `yaml:"aws_secret_access_key"`
	AWSRegion           string `yaml:"aws_region"`
	AWSEndpoint         string `yaml:"aws_endpoint"`
}

type QueryConfig struct {
	DefaultLimit    int        `yaml:"default_limit"`
	MaxLimit        int        `yaml:"max_limit"`
	CacheTTLSeconds int        `yaml:"cache_ttl_seconds"`
	ReaderMode      ReaderMode `yaml:"reader_mode"` // legacy, unified, or hybrid
}

// ReaderMode determines which reader implementation to use for queries
type ReaderMode string

const (
	// ReaderModeLegacy uses the current Go-layer merge (UnifiedSilverReader)
	ReaderModeLegacy ReaderMode = "legacy"
	// ReaderModeUnified uses the new DuckDB ATTACH model (UnifiedDuckDBReader)
	ReaderModeUnified ReaderMode = "unified"
	// ReaderModeHybrid runs both and compares results for validation
	ReaderModeHybrid ReaderMode = "hybrid"
)

// UnifiedReaderConfig contains configuration for the UnifiedDuckDBReader
// which ATTACHes both PostgreSQL (hot) and DuckLake (cold) to a single DuckDB instance
type UnifiedReaderConfig struct {
	Postgres UnifiedPostgresConfig `yaml:"postgres"`
	S3       UnifiedS3Config       `yaml:"s3"`
	DuckLake UnifiedDuckLakeConfig `yaml:"ducklake"`
}

// UnifiedPostgresConfig contains PostgreSQL connection details for hot data
type UnifiedPostgresConfig struct {
	Host     string `yaml:"host"`
	Port     int    `yaml:"port"`
	Database string `yaml:"database"`
	User     string `yaml:"user"`
	Password string `yaml:"password"`
	SSLMode  string `yaml:"sslmode"`
	Schema   string `yaml:"schema"` // Optional, defaults to public
}

// UnifiedS3Config contains S3 credentials for DuckLake cold storage
type UnifiedS3Config struct {
	KeyID    string `yaml:"key_id"`
	Secret   string `yaml:"secret"`
	Region   string `yaml:"region"`
	Endpoint string `yaml:"endpoint"`
}

// UnifiedDuckLakeConfig contains DuckLake catalog configuration
type UnifiedDuckLakeConfig struct {
	CatalogPath    string `yaml:"catalog_path"`
	DataPath       string `yaml:"data_path"`
	SchemaName     string `yaml:"schema_name"`
	MetadataSchema string `yaml:"metadata_schema"`
}

type IndexConfig struct {
	Enabled            bool   `yaml:"enabled"`
	CatalogHost        string `yaml:"catalog_host"`
	CatalogPort        int    `yaml:"catalog_port"`
	CatalogDatabase    string `yaml:"catalog_database"`
	CatalogUser        string `yaml:"catalog_user"`
	CatalogPassword    string `yaml:"catalog_password"`
	DuckLakePath       string `yaml:"ducklake_path"`
	S3Endpoint         string `yaml:"s3_endpoint"`
	S3Region           string `yaml:"s3_region"`
	S3Bucket           string `yaml:"s3_bucket"`
	S3AccessKeyID      string `yaml:"s3_access_key_id"`
	S3SecretAccessKey  string `yaml:"s3_secret_access_key"`
}

type ContractIndexConfig struct {
	Enabled            bool   `yaml:"enabled"`
	CatalogHost        string `yaml:"catalog_host"`
	CatalogPort        int    `yaml:"catalog_port"`
	CatalogDatabase    string `yaml:"catalog_database"`
	CatalogUser        string `yaml:"catalog_user"`
	CatalogPassword    string `yaml:"catalog_password"`
	S3Endpoint         string `yaml:"s3_endpoint"`
	S3Region           string `yaml:"s3_region"`
	S3Bucket           string `yaml:"s3_bucket"`
	S3AccessKeyID      string `yaml:"s3_access_key_id"`
	S3SecretAccessKey  string `yaml:"s3_secret_access_key"`
}

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

func (c *PostgresConfig) DSN() string {
	return fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
		c.Host, c.Port, c.User, c.Password, c.Database, c.SSLMode)
}
