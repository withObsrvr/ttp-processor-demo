package main

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Service    ServiceConfig    `yaml:"service"`
	Source     SourceConfig     `yaml:"source"`
	Target     TargetConfig     `yaml:"target"`
	Projectors ProjectorsConfig `yaml:"projectors"`
	Schema     SchemaConfig     `yaml:"schema"`
	Health     HealthConfig     `yaml:"health"`
	Trigger    TriggerConfig    `yaml:"trigger"`
}

type ServiceConfig struct {
	Name                string `yaml:"name"`
	TickIntervalSeconds int    `yaml:"tick_interval_seconds"`
	LogLevel            string `yaml:"log_level"`
}

type HealthConfig struct {
	Port int `yaml:"port"`
}

type TriggerConfig struct {
	Mode                string `yaml:"mode"`
	Endpoint            string `yaml:"endpoint"`
	FallbackPollSeconds int    `yaml:"fallback_poll_seconds"`
}

type SourceConfig struct {
	BronzeHot DatabaseConfig `yaml:"bronze_hot"`
	SilverHot DatabaseConfig `yaml:"silver_hot"`
}

type TargetConfig struct {
	ServingPostgres DatabaseConfig `yaml:"serving_postgres"`
}

type DatabaseConfig struct {
	Host     string `yaml:"host"`
	Port     int    `yaml:"port"`
	Database string `yaml:"database"`
	User     string `yaml:"user"`
	Password string `yaml:"password"`
	SSLMode  string `yaml:"sslmode"`
}

type ProjectorsConfig struct {
	LedgersRecent        ProjectorConfig `yaml:"ledgers_recent"`
	TransactionsRecent   ProjectorConfig `yaml:"transactions_recent"`
	AccountsCurrent      ProjectorConfig `yaml:"accounts_current"`
	AccountBalances      ProjectorConfig `yaml:"account_balances"`
	NetworkStats         ProjectorConfig `yaml:"network_stats"`
	AssetStats           ProjectorConfig `yaml:"asset_stats"`
	ContractsCurrent     ProjectorConfig `yaml:"contracts_current"`
	ContractStats        ProjectorConfig `yaml:"contract_stats"`
	OperationsRecent     ProjectorConfig `yaml:"operations_recent"`
	EventsRecent         ProjectorConfig `yaml:"events_recent"`
	ExplorerEventsRecent ProjectorConfig `yaml:"explorer_events_recent"`
	ContractCallsRecent  ProjectorConfig `yaml:"contract_calls_recent"`
	TxReceipts           ProjectorConfig `yaml:"tx_receipts"`
}

type ProjectorConfig struct {
	Enabled   bool `yaml:"enabled"`
	BatchSize int  `yaml:"batch_size"`
}

type SchemaConfig struct {
	AutoApply bool `yaml:"auto_apply"`
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

	if cfg.Service.Name == "" {
		cfg.Service.Name = "serving-projection-processor"
	}
	if cfg.Service.TickIntervalSeconds <= 0 {
		cfg.Service.TickIntervalSeconds = 10
	}
	if cfg.Projectors.LedgersRecent.BatchSize <= 0 {
		cfg.Projectors.LedgersRecent.BatchSize = 256
	}
	if cfg.Projectors.TransactionsRecent.BatchSize <= 0 {
		cfg.Projectors.TransactionsRecent.BatchSize = 512
	}
	if cfg.Projectors.AccountsCurrent.BatchSize <= 0 {
		cfg.Projectors.AccountsCurrent.BatchSize = 512
	}
	if cfg.Projectors.AccountBalances.BatchSize <= 0 {
		cfg.Projectors.AccountBalances.BatchSize = 256
	}
	if cfg.Projectors.NetworkStats.BatchSize <= 0 {
		cfg.Projectors.NetworkStats.BatchSize = 1
	}
	if cfg.Projectors.AssetStats.BatchSize <= 0 {
		cfg.Projectors.AssetStats.BatchSize = 1
	}
	if cfg.Projectors.ContractsCurrent.BatchSize <= 0 {
		cfg.Projectors.ContractsCurrent.BatchSize = 1
	}
	if cfg.Projectors.ContractStats.BatchSize <= 0 {
		cfg.Projectors.ContractStats.BatchSize = 1
	}
	if cfg.Projectors.OperationsRecent.BatchSize <= 0 {
		cfg.Projectors.OperationsRecent.BatchSize = 1
	}
	if cfg.Projectors.EventsRecent.BatchSize <= 0 {
		cfg.Projectors.EventsRecent.BatchSize = 1
	}
	if cfg.Projectors.ExplorerEventsRecent.BatchSize <= 0 {
		cfg.Projectors.ExplorerEventsRecent.BatchSize = 1
	}
	if cfg.Projectors.ContractCallsRecent.BatchSize <= 0 {
		cfg.Projectors.ContractCallsRecent.BatchSize = 1
	}
	if cfg.Projectors.TxReceipts.BatchSize <= 0 {
		// tx_receipts does 4 sub-queries per batch and builds JSON blobs in
		// memory; 256 is a safe starting point. Raise if throughput is fine
		// and memory headroom allows.
		cfg.Projectors.TxReceipts.BatchSize = 256
	}
	if cfg.Health.Port == 0 {
		cfg.Health.Port = 8097
	}
	if cfg.Trigger.Mode == "" {
		cfg.Trigger.Mode = "poll"
	}
	if cfg.Trigger.FallbackPollSeconds <= 0 {
		cfg.Trigger.FallbackPollSeconds = cfg.Service.TickIntervalSeconds
	}

	return &cfg, cfg.Validate()
}

func (c *Config) Validate() error {
	if c.Source.BronzeHot.Host == "" {
		return fmt.Errorf("source.bronze_hot.host is required")
	}
	if c.Source.SilverHot.Host == "" {
		return fmt.Errorf("source.silver_hot.host is required")
	}
	if c.Target.ServingPostgres.Host == "" {
		return fmt.Errorf("target.serving_postgres.host is required")
	}
	if c.Trigger.Mode != "poll" && c.Trigger.Mode != "grpc" {
		return fmt.Errorf("trigger.mode must be \"poll\" or \"grpc\", got %q", c.Trigger.Mode)
	}
	if c.Trigger.Mode == "grpc" && c.Trigger.Endpoint == "" {
		return fmt.Errorf("trigger.endpoint is required when trigger.mode is \"grpc\"")
	}
	return nil
}

func (d DatabaseConfig) ConnectionString() string {
	return fmt.Sprintf(
		"host=%s port=%d dbname=%s user=%s password=%s sslmode=%s",
		d.Host, d.Port, d.Database, d.User, d.Password, d.SSLMode,
	)
}

func (c *Config) TickInterval() time.Duration {
	return time.Duration(c.Service.TickIntervalSeconds) * time.Second
}

func (c *Config) FallbackPollInterval() time.Duration {
	return time.Duration(c.Trigger.FallbackPollSeconds) * time.Second
}
