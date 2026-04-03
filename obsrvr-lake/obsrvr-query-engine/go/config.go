package main

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Service  ServiceConfig  `yaml:"service"`
	DuckLake DuckLakeConfig `yaml:"ducklake"`
	S3       S3Config       `yaml:"s3"`
}

type ServiceConfig struct {
	Name string `yaml:"name"`
	Port int    `yaml:"port"`
}

type DuckLakeConfig struct {
	CatalogPath    string `yaml:"catalog_path"`
	DataPath       string `yaml:"data_path"`
	CatalogName    string `yaml:"catalog_name"`
	BronzeSchema   string `yaml:"bronze_schema"`
	SilverSchema   string `yaml:"silver_schema"`
	MetadataSchema string `yaml:"metadata_schema"`
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
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	// Expand environment variables in the YAML content
	expanded := os.ExpandEnv(string(data))

	var cfg Config
	if err := yaml.Unmarshal([]byte(expanded), &cfg); err != nil {
		return nil, fmt.Errorf("failed to parse config file: %w", err)
	}

	// Defaults
	if cfg.Service.Port == 0 {
		cfg.Service.Port = 8092
	}
	if cfg.Service.Name == "" {
		cfg.Service.Name = "obsrvr-query-engine"
	}
	if cfg.DuckLake.CatalogName == "" {
		cfg.DuckLake.CatalogName = "testnet_catalog"
	}
	if cfg.DuckLake.BronzeSchema == "" {
		cfg.DuckLake.BronzeSchema = "bronze"
	}
	if cfg.DuckLake.SilverSchema == "" {
		cfg.DuckLake.SilverSchema = "silver"
	}
	if cfg.DuckLake.MetadataSchema == "" {
		cfg.DuckLake.MetadataSchema = "bronze_meta"
	}

	return &cfg, nil
}
