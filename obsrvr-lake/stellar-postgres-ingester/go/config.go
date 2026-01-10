package main

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

// Config represents the application configuration
type Config struct {
	Service struct {
		Name       string `yaml:"name"`
		HealthPort int    `yaml:"health_port"`
	} `yaml:"service"`

	Source struct {
		Endpoint          string `yaml:"endpoint"`
		NetworkPassphrase string `yaml:"network_passphrase"`
		StartLedger       uint32 `yaml:"start_ledger"`
		EndLedger         uint32 `yaml:"end_ledger"` // 0 = continuous (live mode)
	} `yaml:"source"`

	Postgres struct {
		Host                  string `yaml:"host"`
		Port                  int    `yaml:"port"`
		Database              string `yaml:"database"`
		User                  string `yaml:"user"`
		Password              string `yaml:"password"`
		SSLMode               string `yaml:"sslmode"`
		BatchSize             int    `yaml:"batch_size"`              // Ledgers per batch
		CommitIntervalSeconds int    `yaml:"commit_interval_seconds"` // Auto-commit interval
		MaxRetries            int    `yaml:"max_retries"`             // Retry attempts on errors
	} `yaml:"postgres"`

	Checkpoint struct {
		FilePath string `yaml:"file_path"`
	} `yaml:"checkpoint"`

	Logging struct {
		Level  string `yaml:"level"`
		Format string `yaml:"format"`
	} `yaml:"logging"`
}

// LoadConfig loads configuration from a YAML file
func LoadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("failed to parse config: %w", err)
	}

	// Set defaults
	if cfg.Service.HealthPort == 0 {
		cfg.Service.HealthPort = 8088
	}
	if cfg.Postgres.BatchSize == 0 {
		cfg.Postgres.BatchSize = 50
	}
	if cfg.Postgres.CommitIntervalSeconds == 0 {
		cfg.Postgres.CommitIntervalSeconds = 5
	}
	if cfg.Postgres.MaxRetries == 0 {
		cfg.Postgres.MaxRetries = 3
	}
	if cfg.Postgres.SSLMode == "" {
		cfg.Postgres.SSLMode = "disable"
	}

	return &cfg, nil
}

// GetPostgresConnectionString returns a connection string for PostgreSQL
func (c *Config) GetPostgresConnectionString() string {
	return fmt.Sprintf(
		"host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
		c.Postgres.Host,
		c.Postgres.Port,
		c.Postgres.User,
		c.Postgres.Password,
		c.Postgres.Database,
		c.Postgres.SSLMode,
	)
}
