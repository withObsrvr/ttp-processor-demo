package main

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Service      ServiceConfig  `yaml:"service"`
	Postgres     PostgresConfig `yaml:"postgres"`
	DuckLake     DuckLakeConfig `yaml:"ducklake"`
	DuckLakeSilver *DuckLakeConfig `yaml:"ducklake_silver,omitempty"`
	Query        QueryConfig    `yaml:"query"`
}

type ServiceConfig struct {
	Name                string `yaml:"name"`
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
	CatalogPath         string `yaml:"catalog_path"`
	DataPath            string `yaml:"data_path"`
	CatalogName         string `yaml:"catalog_name"`
	SchemaName          string `yaml:"schema_name"`
	MetadataSchema      string `yaml:"metadata_schema"`
	AWSAccessKeyID      string `yaml:"aws_access_key_id"`
	AWSSecretAccessKey  string `yaml:"aws_secret_access_key"`
	AWSRegion           string `yaml:"aws_region"`
	AWSEndpoint         string `yaml:"aws_endpoint"`
}

type QueryConfig struct {
	DefaultLimit     int `yaml:"default_limit"`
	MaxLimit         int `yaml:"max_limit"`
	CacheTTLSeconds  int `yaml:"cache_ttl_seconds"`
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

	return &config, nil
}

func (c *PostgresConfig) DSN() string {
	return fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
		c.Host, c.Port, c.User, c.Password, c.Database, c.SSLMode)
}
