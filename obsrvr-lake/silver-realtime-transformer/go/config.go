package main

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

// Config represents the service configuration
type Config struct {
	Service     ServiceConfig     `yaml:"service"`
	BronzeHot   DatabaseConfig    `yaml:"bronze_hot"`
	SilverHot   DatabaseConfig    `yaml:"silver_hot"`
	Checkpoint  CheckpointConfig  `yaml:"checkpoint"`
	Performance PerformanceConfig `yaml:"performance"`
}

// ServiceConfig holds service-level settings
type ServiceConfig struct {
	Name               string `yaml:"name"`
	HealthPort         string `yaml:"health_port"`
	PollIntervalSeconds int   `yaml:"poll_interval_seconds"`
}

// DatabaseConfig holds database connection settings
type DatabaseConfig struct {
	Host     string `yaml:"host"`
	Port     int    `yaml:"port"`
	Database string `yaml:"database"`
	User     string `yaml:"user"`
	Password string `yaml:"password"`
	SSLMode  string `yaml:"sslmode"`
}

// CheckpointConfig holds checkpoint tracking settings
type CheckpointConfig struct {
	Table string `yaml:"table"`
}

// PerformanceConfig holds performance tuning settings
type PerformanceConfig struct {
	BatchSize  int `yaml:"batch_size"`
	MaxWorkers int `yaml:"max_workers"`
}

// LoadConfig loads configuration from a YAML file
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

// Validate validates the configuration
func (c *Config) Validate() error {
	if c.Service.PollIntervalSeconds < 1 {
		return fmt.Errorf("poll_interval_seconds must be at least 1")
	}

	if c.Performance.BatchSize < 1 {
		return fmt.Errorf("batch_size must be at least 1")
	}

	if c.Performance.MaxWorkers < 1 {
		return fmt.Errorf("max_workers must be at least 1")
	}

	return nil
}

// PollInterval returns the poll interval as a Duration
func (c *Config) PollInterval() time.Duration {
	return time.Duration(c.Service.PollIntervalSeconds) * time.Second
}

// ConnectionString builds a PostgreSQL connection string
func (d *DatabaseConfig) ConnectionString() string {
	return fmt.Sprintf(
		"host=%s port=%d dbname=%s user=%s password=%s sslmode=%s",
		d.Host, d.Port, d.Database, d.User, d.Password, d.SSLMode,
	)
}
