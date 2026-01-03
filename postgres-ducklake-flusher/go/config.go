package main

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

// Config represents the full configuration for the flusher service
type Config struct {
	Service  ServiceConfig  `yaml:"service"`
	Postgres PostgresConfig `yaml:"postgres"`
	DuckLake DuckLakeConfig `yaml:"ducklake"`
	Vacuum   VacuumConfig   `yaml:"vacuum"`
}

// ServiceConfig contains service-level settings
type ServiceConfig struct {
	Name                 string `yaml:"name"`
	HealthPort           int    `yaml:"health_port"`
	FlushIntervalMinutes int    `yaml:"flush_interval_minutes"`
}

// PostgresConfig contains PostgreSQL connection settings
type PostgresConfig struct {
	Host     string `yaml:"host"`
	Port     int    `yaml:"port"`
	Database string `yaml:"database"`
	User     string `yaml:"user"`
	Password string `yaml:"password"`
	SSLMode  string `yaml:"sslmode"`
}

// DuckLakeConfig contains DuckLake catalog and storage settings
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

// VacuumConfig contains VACUUM settings
type VacuumConfig struct {
	Enabled       bool `yaml:"enabled"`
	EveryNFlushes int  `yaml:"every_n_flushes"`
}

// LoadConfig loads configuration from a YAML file
func LoadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	var config Config
	if err := yaml.Unmarshal(data, &config); err != nil {
		return nil, fmt.Errorf("failed to parse config file: %w", err)
	}

	// Set defaults
	if config.Service.HealthPort == 0 {
		config.Service.HealthPort = 8089
	}
	if config.Service.FlushIntervalMinutes == 0 {
		config.Service.FlushIntervalMinutes = 10
	}
	if config.Postgres.SSLMode == "" {
		config.Postgres.SSLMode = "disable"
	}
	if config.Vacuum.EveryNFlushes == 0 {
		config.Vacuum.EveryNFlushes = 10
	}

	return &config, nil
}

// GetPostgresDSN returns the PostgreSQL connection string
func (c *PostgresConfig) GetPostgresDSN() string {
	return fmt.Sprintf("host=%s port=%d dbname=%s user=%s password=%s sslmode=%s",
		c.Host, c.Port, c.Database, c.User, c.Password, c.SSLMode)
}

// GetDuckDBConnString returns the DuckDB catalog connection string
func (c *DuckLakeConfig) GetDuckDBConnString() string {
	return fmt.Sprintf("ducklake:postgres:%s", c.CatalogPath)
}

// Tables returns the list of all 19 tables to flush
func GetTablesToFlush() []string {
	return []string{
		// Core tables
		"ledgers_row_v2",
		"transactions_row_v2",
		"operations_row_v2",
		"effects_row_v1",
		"trades_row_v1",

		// Account tables
		"accounts_snapshot_v1",
		"trustlines_snapshot_v1",
		"account_signers_snapshot_v1",
		"native_balances_snapshot_v1",

		// Market tables
		"offers_snapshot_v1",
		"liquidity_pools_snapshot_v1",
		"claimable_balances_snapshot_v1",

		// Soroban tables
		"contract_events_stream_v1",
		"contract_data_snapshot_v1",
		"contract_code_snapshot_v1",
		"restored_keys_state_v1",

		// State tables
		"ttl_snapshot_v1",
		"evicted_keys_state_v1",
		"config_settings_snapshot_v1",
	}
}
