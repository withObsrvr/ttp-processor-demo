package main

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

// Config holds all configuration for the unified processor
type Config struct {
	Service  ServiceConfig  `yaml:"service"`
	Source   SourceConfig   `yaml:"source"`
	DuckLake DuckLakeConfig `yaml:"ducklake"`
	S3       S3Config       `yaml:"s3"`
}

type ServiceConfig struct {
	Name       string `yaml:"name"`
	HealthPort int    `yaml:"health_port"`
}

type SourceConfig struct {
	Endpoint          string `yaml:"endpoint"`
	StartLedger       uint32 `yaml:"start_ledger"`
	NetworkPassphrase string `yaml:"network_passphrase"`
}

type DuckLakeConfig struct {
	CatalogPath    string `yaml:"catalog_path"`    // ducklake:postgres:postgresql://...
	DataPath       string `yaml:"data_path"`       // s3://bucket/
	CatalogName    string `yaml:"catalog_name"`    // testnet_catalog
	BronzeSchema   string `yaml:"bronze_schema"`   // bronze
	SilverSchema   string `yaml:"silver_schema"`   // silver
	IndexSchema    string `yaml:"index_schema"`    // index
	MetadataSchema string `yaml:"metadata_schema"` // bronze_meta (for bronze), etc.
	FlushInterval  int    `yaml:"flush_interval_seconds"` // seconds between ducklake_flush_inlined_data calls
	InliningLimit  int    `yaml:"inlining_row_limit"`     // default 10
}

type S3Config struct {
	KeyID     string `yaml:"key_id"`
	KeySecret string `yaml:"key_secret"`
	Region    string `yaml:"region"`
	Endpoint  string `yaml:"endpoint"`
}

func LoadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}

	// Defaults
	if cfg.Service.Name == "" {
		cfg.Service.Name = "obsrvr-lake-unified-processor"
	}
	if cfg.Service.HealthPort == 0 {
		cfg.Service.HealthPort = 8098
	}
	if cfg.DuckLake.CatalogName == "" {
		cfg.DuckLake.CatalogName = "lake"
	}
	if cfg.DuckLake.BronzeSchema == "" {
		cfg.DuckLake.BronzeSchema = "bronze"
	}
	if cfg.DuckLake.SilverSchema == "" {
		cfg.DuckLake.SilverSchema = "silver"
	}
	if cfg.DuckLake.IndexSchema == "" {
		cfg.DuckLake.IndexSchema = "index"
	}
	if cfg.DuckLake.FlushInterval == 0 {
		cfg.DuckLake.FlushInterval = 300 // 5 minutes
	}
	if cfg.DuckLake.InliningLimit == 0 {
		cfg.DuckLake.InliningLimit = 10
	}

	return &cfg, nil
}
