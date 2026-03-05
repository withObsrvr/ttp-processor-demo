package main

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

// Config represents the processor configuration
type Config struct {
	Service     ServiceConfig    `yaml:"service"`
	SourceDB    DatabaseConfig   `yaml:"source_db"`
	DestDB      DatabaseConfig   `yaml:"destination_db"`
	Discovery   DiscoveryConfig  `yaml:"discovery"`
	Checkpoint  CheckpointConfig `yaml:"checkpoint"`
	Health      HealthConfig     `yaml:"health"`
}

// ServiceConfig contains service-level configuration
type ServiceConfig struct {
	Name                string `yaml:"name"`
	Version             string `yaml:"version"`
	PollIntervalSeconds int    `yaml:"poll_interval_seconds"`
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

// DiscoveryConfig contains token discovery settings
type DiscoveryConfig struct {
	BatchSize                  int `yaml:"batch_size"`
	SEP41MinFunctions          int `yaml:"sep41_min_functions"`
	UpdateStatsIntervalSeconds int `yaml:"update_stats_interval_seconds"`
	MaxLedgersPerCycle         int `yaml:"max_ledgers_per_cycle"`
}

// CheckpointConfig contains checkpoint settings
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
	if config.Service.Name == "" {
		config.Service.Name = "token-discovery-processor"
	}
	if config.Service.Version == "" {
		config.Service.Version = "1.0.0"
	}
	if config.Service.PollIntervalSeconds == 0 {
		config.Service.PollIntervalSeconds = 30
	}
	if config.SourceDB.SSLMode == "" {
		config.SourceDB.SSLMode = "disable"
	}
	if config.DestDB.SSLMode == "" {
		config.DestDB.SSLMode = "disable"
	}
	if config.Discovery.BatchSize == 0 {
		config.Discovery.BatchSize = 1000
	}
	if config.Discovery.SEP41MinFunctions == 0 {
		config.Discovery.SEP41MinFunctions = 3
	}
	if config.Discovery.UpdateStatsIntervalSeconds == 0 {
		config.Discovery.UpdateStatsIntervalSeconds = 300
	}
	if config.Discovery.MaxLedgersPerCycle == 0 {
		config.Discovery.MaxLedgersPerCycle = 10000
	}
	if config.Checkpoint.Table == "" {
		config.Checkpoint.Table = "token_discovery_checkpoint"
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

// ConnectionString builds a PostgreSQL connection string
func (d *DatabaseConfig) ConnectionString() string {
	return fmt.Sprintf(
		"host=%s port=%d dbname=%s user=%s password=%s sslmode=%s",
		d.Host, d.Port, d.Database, d.User, d.Password, d.SSLMode,
	)
}
