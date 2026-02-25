package main

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Service struct {
		Name       string `yaml:"name"`
		HealthPort int    `yaml:"health_port"`
	} `yaml:"service"`

	Source struct {
		Endpoint   string `yaml:"endpoint"`
		StartScanID uint32 `yaml:"start_scan_id"` // 0 = from latest
	} `yaml:"source"`

	Postgres struct {
		Host       string `yaml:"host"`
		Port       int    `yaml:"port"`
		Database   string `yaml:"database"`
		User       string `yaml:"user"`
		Password   string `yaml:"password"`
		SSLMode    string `yaml:"sslmode"`
		MaxRetries int    `yaml:"max_retries"`
	} `yaml:"postgres"`

	Checkpoint struct {
		FilePath string `yaml:"file_path"`
	} `yaml:"checkpoint"`
}

func LoadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("failed to parse config: %w", err)
	}

	if cfg.Service.HealthPort == 0 {
		cfg.Service.HealthPort = 8096
	}
	if cfg.Postgres.MaxRetries == 0 {
		cfg.Postgres.MaxRetries = 3
	}
	if cfg.Postgres.SSLMode == "" {
		cfg.Postgres.SSLMode = "disable"
	}

	return &cfg, nil
}

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
