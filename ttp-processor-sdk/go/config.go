package main

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/withObsrvr/flowctl-sdk/pkg/processor"
	"go.uber.org/zap"
)

// TTPConfig holds all configuration for the TTP processor
type TTPConfig struct {
	// Embedded flowctl processor config
	ProcessorConfig *processor.Config

	// Stellar network configuration
	NetworkPassphrase string
	NetworkName       string

	// Domain-specific configurations
	Filter *FilterConfig
	Batch  *BatchConfig
}

// LoadTTPConfig loads configuration from environment variables
func LoadTTPConfig(logger *zap.Logger) (*TTPConfig, error) {
	// Start with flowctl processor defaults
	processorConfig := processor.DefaultConfig()

	// Override with environment variables
	processorConfig.ID = getEnv("COMPONENT_ID", "ttp-processor")
	processorConfig.Name = "Token Transfer Processor"
	processorConfig.Description = "Extracts SEP-41 token transfer events from Stellar ledgers"
	processorConfig.Version = "2.0.0-sdk"
	processorConfig.Endpoint = getEnv("PORT", ":50051")
	processorConfig.HealthPort = parseInt(getEnv("HEALTH_PORT", "8088"))

	// Event types
	processorConfig.InputEventTypes = []string{"stellar.ledger.v1"}
	processorConfig.OutputEventTypes = []string{"stellar.token.transfer.v1"}

	// Configure flowctl integration
	if strings.ToLower(getEnv("ENABLE_FLOWCTL", "false")) == "true" {
		processorConfig.FlowctlConfig.Enabled = true
		processorConfig.FlowctlConfig.Endpoint = getEnv("FLOWCTL_ENDPOINT", "localhost:8080")
		processorConfig.FlowctlConfig.HeartbeatInterval = parseDuration(getEnv("FLOWCTL_HEARTBEAT_INTERVAL", "10s"))
	}

	// Get network passphrase (required)
	networkPassphrase := os.Getenv("NETWORK_PASSPHRASE")
	if networkPassphrase == "" {
		return nil, fmt.Errorf("NETWORK_PASSPHRASE environment variable not set")
	}

	// Determine network name for metrics
	networkName := determineNetworkName(networkPassphrase)

	// Load filter configuration
	filterConfig := LoadFilterConfig(logger)

	// Load batch configuration
	batchConfig := LoadBatchConfig(logger)

	return &TTPConfig{
		ProcessorConfig:   processorConfig,
		NetworkPassphrase: networkPassphrase,
		NetworkName:       networkName,
		Filter:            filterConfig,
		Batch:             batchConfig,
	}, nil
}

// Validate validates the configuration
func (c *TTPConfig) Validate(logger *zap.Logger) error {
	// Validate processor config
	if err := c.ProcessorConfig.Validate(); err != nil {
		return fmt.Errorf("invalid processor config: %w", err)
	}

	// Validate using config validator
	validator := NewConfigValidator(logger)
	if !validator.ValidateConfiguration(c.NetworkPassphrase, c.Filter, c.Batch) {
		return fmt.Errorf("configuration validation failed with fatal errors")
	}

	return nil
}

// determineNetworkName determines the network name from passphrase
func determineNetworkName(passphrase string) string {
	if strings.Contains(passphrase, "Public Global") {
		return "mainnet"
	} else if strings.Contains(passphrase, "Test SDF") {
		return "testnet"
	} else if strings.Contains(passphrase, "Future") {
		return "futurenet"
	}
	return "unknown"
}

// Helper functions for environment variable parsing

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func parseInt(s string) int {
	v, _ := strconv.Atoi(s)
	return v
}

func parseDuration(s string) time.Duration {
	d, _ := time.ParseDuration(s)
	return d
}

func parseBool(s string) bool {
	return s == "true" || s == "1" || s == "yes"
}
