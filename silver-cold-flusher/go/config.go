package main

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

// Config holds all configuration for the silver cold flusher
type Config struct {
	Service  ServiceConfig  `yaml:"service"`
	Postgres PostgresConfig `yaml:"postgres"`
	DuckLake DuckLakeConfig `yaml:"ducklake"`
	Vacuum   VacuumConfig   `yaml:"vacuum"`
}

// ServiceConfig holds service-level configuration
type ServiceConfig struct {
	Name              string `yaml:"name"`
	HealthPort        string `yaml:"health_port"`
	FlushIntervalHours int    `yaml:"flush_interval_hours"`
}

// PostgresConfig holds PostgreSQL connection configuration
type PostgresConfig struct {
	Host     string `yaml:"host"`
	Port     int    `yaml:"port"`
	Database string `yaml:"database"`
	User     string `yaml:"user"`
	Password string `yaml:"password"`
	SSLMode  string `yaml:"sslmode"`
}

// DuckLakeConfig holds DuckLake configuration
type DuckLakeConfig struct {
	CatalogPath    string `yaml:"catalog_path"`
	DataPath       string `yaml:"data_path"`
	CatalogName    string `yaml:"catalog_name"`
	SchemaName     string `yaml:"schema_name"`
	MetadataSchema string `yaml:"metadata_schema"`
}

// VacuumConfig holds vacuum configuration
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
		return nil, fmt.Errorf("failed to parse config YAML: %w", err)
	}

	// Set defaults
	if config.Service.HealthPort == "" {
		config.Service.HealthPort = "8095"
	}
	if config.Service.FlushIntervalHours == 0 {
		config.Service.FlushIntervalHours = 3
	}
	if config.Vacuum.EveryNFlushes == 0 {
		config.Vacuum.EveryNFlushes = 10
	}

	// Validate
	if config.Service.FlushIntervalHours < 1 || config.Service.FlushIntervalHours > 24 {
		return nil, fmt.Errorf("flush_interval_hours must be between 1 and 24, got %d", config.Service.FlushIntervalHours)
	}

	return &config, nil
}

// PostgresConnectionString returns the PostgreSQL connection string
func (c *PostgresConfig) ConnectionString() string {
	return fmt.Sprintf("host=%s port=%d dbname=%s user=%s password=%s sslmode=%s",
		c.Host, c.Port, c.Database, c.User, c.Password, c.SSLMode)
}

// FlushInterval returns the flush interval as a time.Duration
func (c *ServiceConfig) FlushInterval() time.Duration {
	return time.Duration(c.FlushIntervalHours) * time.Hour
}

// GetTablesToFlush returns the list of all silver tables to flush
func GetTablesToFlush() []string {
	return []string{
		// Current state tables (9)
		"accounts_current",
		"account_signers_current",
		"trustlines_current",
		"offers_current",
		"claimable_balances_current",
		"liquidity_pools_current",
		"contract_data_current",
		"contract_code_current",
		"config_settings_current",
		"ttl_current",

		// Snapshot tables (4)
		"accounts_snapshot",
		"trustlines_snapshot",
		"offers_snapshot",
		"account_signers_snapshot",

		// Enriched/event tables (2)
		"enriched_history_operations",
		"token_transfers_raw",
		// "soroban_history_operations", // TODO: Enable when table exists in PostgreSQL
	}
}
