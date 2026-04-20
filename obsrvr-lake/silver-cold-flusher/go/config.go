package main

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

// Config holds all configuration for the silver cold flusher
type Config struct {
	Service     ServiceConfig     `yaml:"service"`
	Postgres    PostgresConfig    `yaml:"postgres"`
	DuckLake    DuckLakeConfig    `yaml:"ducklake"`
	Vacuum      VacuumConfig      `yaml:"vacuum"`
	Maintenance MaintenanceConfig `yaml:"maintenance"`
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

// MaintenanceConfig contains DuckLake maintenance settings
type MaintenanceConfig struct {
	Enabled           bool `yaml:"enabled"`
	EveryNFlushes     int  `yaml:"every_n_flushes"`
	MaxCompactedFiles int  `yaml:"max_compacted_files"`
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
	if config.Maintenance.EveryNFlushes == 0 {
		config.Maintenance.EveryNFlushes = 6
	}
	if config.Maintenance.MaxCompactedFiles == 0 {
		config.Maintenance.MaxCompactedFiles = 200
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
		// Current state tables (4)
		"accounts_current",
		"trustlines_current",
		"offers_current",
		"claimable_balances_current",
		"contract_data_current",
		"token_registry",

		// Snapshot tables (5)
		"accounts_snapshot",
		"trustlines_snapshot",
		"offers_snapshot",
		"account_signers_snapshot",
		// "claimable_balances_snapshot", // Source table does not exist in silver_hot

		// Enriched/event tables (3)
		"enriched_history_operations",
		"enriched_history_operations_soroban",
		"token_transfers_raw",

		// Contract tracking tables
		"contract_invocations_raw",
		"contract_metadata",

		// Semantic layer tables (event/append tables only; entities_contracts is UPSERT-only, not flushed)
		"semantic_activities",
		"semantic_flows_value",

		// High-volume append-only historical tables — MUST be retained in this
		// list or hot PG will grow unbounded. Previous to April 2026 these
		// were missing and `silver_hot.effects` hit 10 GB + 16M rows on
		// testnet despite the rest of silver being pruned to a ~50-min window.
		"effects",
		"evicted_keys",

		// Append-only DEX/ledger events (added April 2026 — same leak class
		// as effects/evicted_keys, just quieter on testnet so it went unnoticed
		// longer).
		"trades",
		"restored_keys",

		// Additional current-state tables (were only in hot PG; now flushed
		// to cold like accounts_current / trustlines_current / etc. so cold
		// queries can reconstruct historical state).
		"native_balances_current",
		"ttl_current",
		"address_balances_current",
	}
}
