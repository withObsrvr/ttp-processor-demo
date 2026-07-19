package main

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Service           ServiceConfig           `yaml:"service"`
	Postgres          PostgresConfig          `yaml:"postgres"`
	PostgresSilver    *PostgresConfig         `yaml:"postgres_silver,omitempty"`
	DuckLake          DuckLakeConfig          `yaml:"ducklake"`
	DuckLakeSilver    *DuckLakeConfig         `yaml:"ducklake_silver,omitempty"`
	Index             *IndexConfig            `yaml:"index,omitempty"`
	ContractIndex     *ContractIndexConfig    `yaml:"contract_index,omitempty"`
	Query             QueryConfig             `yaml:"query"`
	Unified           *UnifiedReaderConfig    `yaml:"unified,omitempty"` // Config for DuckDB ATTACH unified reader
	RPCFallback       *RPCFallbackConfig      `yaml:"rpc_fallback,omitempty"`
	ContractArtifacts *ContractArtifactConfig `yaml:"contract_artifacts,omitempty"`
}

type ServiceConfig struct {
	Name                string `yaml:"name"`
	Network             string `yaml:"network"`
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
	CatalogPath        string `yaml:"catalog_path"`
	DataPath           string `yaml:"data_path"`
	CatalogName        string `yaml:"catalog_name"`
	SchemaName         string `yaml:"schema_name"`
	MetadataSchema     string `yaml:"metadata_schema"`
	AWSAccessKeyID     string `yaml:"aws_access_key_id"`
	AWSSecretAccessKey string `yaml:"aws_secret_access_key"`
	AWSRegion          string `yaml:"aws_region"`
	AWSEndpoint        string `yaml:"aws_endpoint"`
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
	Postgres       UnifiedPostgresConfig  `yaml:"postgres"`
	BronzePostgres *UnifiedPostgresConfig `yaml:"bronze_postgres,omitempty"`
	BronzeDuckLake *UnifiedDuckLakeConfig `yaml:"bronze_ducklake,omitempty"`
	S3             UnifiedS3Config        `yaml:"s3"`
	DuckLake       UnifiedDuckLakeConfig  `yaml:"ducklake"`
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
	Enabled           bool   `yaml:"enabled"`
	CatalogHost       string `yaml:"catalog_host"`
	CatalogPort       int    `yaml:"catalog_port"`
	CatalogDatabase   string `yaml:"catalog_database"`
	CatalogUser       string `yaml:"catalog_user"`
	CatalogPassword   string `yaml:"catalog_password"`
	DuckLakePath      string `yaml:"ducklake_path"`
	S3Endpoint        string `yaml:"s3_endpoint"`
	S3Region          string `yaml:"s3_region"`
	S3Bucket          string `yaml:"s3_bucket"`
	S3AccessKeyID     string `yaml:"s3_access_key_id"`
	S3SecretAccessKey string `yaml:"s3_secret_access_key"`
}

type ContractIndexConfig struct {
	Enabled           bool   `yaml:"enabled"`
	CatalogHost       string `yaml:"catalog_host"`
	CatalogPort       int    `yaml:"catalog_port"`
	CatalogDatabase   string `yaml:"catalog_database"`
	CatalogUser       string `yaml:"catalog_user"`
	CatalogPassword   string `yaml:"catalog_password"`
	S3Endpoint        string `yaml:"s3_endpoint"`
	S3Region          string `yaml:"s3_region"`
	S3Bucket          string `yaml:"s3_bucket"`
	S3AccessKeyID     string `yaml:"s3_access_key_id"`
	S3SecretAccessKey string `yaml:"s3_secret_access_key"`
}

type RPCFallbackConfig struct {
	Enabled        bool   `yaml:"enabled"`
	URL            string `yaml:"url"`
	AuthHeader     string `yaml:"auth_header"`
	TimeoutSeconds int    `yaml:"timeout_seconds"`
}

// ContractArtifactConfig controls content-addressed persistence for immutable
// contract WASM and its decoded interface. The contract-to-hash mapping is
// always resolved from current ledger state, so cached code remains upgrade-safe.
type ContractArtifactConfig struct {
	CacheDirectory string `yaml:"cache_directory"`
	MaxWASMBytes   int64  `yaml:"max_wasm_bytes"`
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
	if err := applyRuntimeEnvOverrides(&config); err != nil {
		return nil, err
	}
	if config.Service.Network == "" {
		config.Service.Network = "testnet"
	}
	switch config.Service.Network {
	case "mainnet", "testnet":
		// valid
	default:
		return nil, fmt.Errorf("invalid service.network %q: must be one of %q or %q", config.Service.Network, "mainnet", "testnet")
	}

	return &config, nil
}

func applyRuntimeEnvOverrides(config *Config) error {
	if config == nil {
		return errors.New("config is nil")
	}
	if url := strings.TrimSpace(os.Getenv("RPC_FALLBACK_URL")); url != "" {
		if config.RPCFallback == nil {
			config.RPCFallback = &RPCFallbackConfig{}
		}
		config.RPCFallback.Enabled = true
		config.RPCFallback.URL = url
		config.RPCFallback.AuthHeader = strings.TrimSpace(os.Getenv("RPC_FALLBACK_AUTH_HEADER"))
		if rawTimeout := strings.TrimSpace(os.Getenv("RPC_FALLBACK_TIMEOUT")); rawTimeout != "" {
			timeout, err := strconv.Atoi(rawTimeout)
			if err != nil || timeout <= 0 {
				return fmt.Errorf("invalid RPC_FALLBACK_TIMEOUT %q", rawTimeout)
			}
			config.RPCFallback.TimeoutSeconds = timeout
		}
	}
	if directory := strings.TrimSpace(os.Getenv("CONTRACT_ARTIFACT_CACHE_DIR")); directory != "" {
		if config.ContractArtifacts == nil {
			config.ContractArtifacts = &ContractArtifactConfig{}
		}
		config.ContractArtifacts.CacheDirectory = directory
	}
	if rawLimit := strings.TrimSpace(os.Getenv("CONTRACT_ARTIFACT_MAX_WASM_BYTES")); rawLimit != "" {
		limit, err := strconv.ParseInt(rawLimit, 10, 64)
		if err != nil || limit <= 0 {
			return fmt.Errorf("invalid CONTRACT_ARTIFACT_MAX_WASM_BYTES %q", rawLimit)
		}
		if config.ContractArtifacts == nil {
			config.ContractArtifacts = &ContractArtifactConfig{}
		}
		config.ContractArtifacts.MaxWASMBytes = limit
	}
	return nil
}

func (c *PostgresConfig) DSN() string {
	return fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
		c.Host, c.Port, c.User, c.Password, c.Database, c.SSLMode)
}
