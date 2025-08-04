package config

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

// Config holds all configuration for the contract data processor
type Config struct {
	// Service identification
	ServiceName    string
	ServiceVersion string

	// Network configuration
	NetworkPassphrase string
	
	// Server addresses
	GRPCAddress   string // Control plane gRPC address
	FlightAddress string // Data plane Arrow Flight address
	HealthPort    int    // HTTP health check port
	
	// Upstream source
	SourceEndpoint string // stellar-live-source-datalake endpoint
	
	// Processing configuration
	BatchSize      int           // Number of records per Arrow batch
	MaxMemoryMB    int           // Maximum memory usage in MB
	FlushInterval  time.Duration // How often to flush batches
	
	// Flowctl integration
	FlowctlEnabled           bool
	FlowctlEndpoint          string
	FlowctlHeartbeatInterval time.Duration
	
	// Filtering configuration
	FilterContractIDs   []string // Specific contract IDs to track
	FilterAssetCodes    []string // Asset codes to filter
	FilterAssetIssuers  []string // Asset issuers to filter
	IncludeDeleted      bool     // Include deleted contract data
	
	// Performance tuning
	WorkerCount        int // Number of parallel workers
	ChannelBufferSize  int // Size of internal channels
	
	// Logging
	LogLevel string
	Debug    bool
}

// LoadFromEnv loads configuration from environment variables
func LoadFromEnv() (*Config, error) {
	cfg := &Config{
		// Defaults
		ServiceName:              "contract-data-processor",
		ServiceVersion:           "v1.0.0",
		NetworkPassphrase:        "Test SDF Network ; September 2015",
		GRPCAddress:              ":50054",
		FlightAddress:            ":8816",
		HealthPort:               8089,
		SourceEndpoint:           "localhost:50053",
		BatchSize:                1000,
		MaxMemoryMB:              1024,
		FlushInterval:            5 * time.Second,
		FlowctlEnabled:           false,
		FlowctlEndpoint:          "localhost:8080",
		FlowctlHeartbeatInterval: 10 * time.Second,
		WorkerCount:              4,
		ChannelBufferSize:        100,
		LogLevel:                 "info",
		Debug:                    false,
		IncludeDeleted:           false,
	}

	// Override with environment variables
	if v := os.Getenv("SERVICE_NAME"); v != "" {
		cfg.ServiceName = v
	}
	if v := os.Getenv("SERVICE_VERSION"); v != "" {
		cfg.ServiceVersion = v
	}
	if v := os.Getenv("NETWORK_PASSPHRASE"); v != "" {
		cfg.NetworkPassphrase = v
	}
	if v := os.Getenv("GRPC_ADDRESS"); v != "" {
		cfg.GRPCAddress = v
	}
	if v := os.Getenv("FLIGHT_ADDRESS"); v != "" {
		cfg.FlightAddress = v
	}
	if v := os.Getenv("HEALTH_PORT"); v != "" {
		port, err := strconv.Atoi(v)
		if err != nil {
			return nil, fmt.Errorf("invalid HEALTH_PORT: %w", err)
		}
		cfg.HealthPort = port
	}
	if v := os.Getenv("SOURCE_ENDPOINT"); v != "" {
		cfg.SourceEndpoint = v
	}
	if v := os.Getenv("BATCH_SIZE"); v != "" {
		size, err := strconv.Atoi(v)
		if err != nil {
			return nil, fmt.Errorf("invalid BATCH_SIZE: %w", err)
		}
		cfg.BatchSize = size
	}
	if v := os.Getenv("MAX_MEMORY_MB"); v != "" {
		mem, err := strconv.Atoi(v)
		if err != nil {
			return nil, fmt.Errorf("invalid MAX_MEMORY_MB: %w", err)
		}
		cfg.MaxMemoryMB = mem
	}
	if v := os.Getenv("FLUSH_INTERVAL"); v != "" {
		interval, err := time.ParseDuration(v)
		if err != nil {
			return nil, fmt.Errorf("invalid FLUSH_INTERVAL: %w", err)
		}
		cfg.FlushInterval = interval
	}
	if v := os.Getenv("FLOWCTL_ENABLED"); v != "" {
		cfg.FlowctlEnabled = v == "true" || v == "1"
	}
	if v := os.Getenv("FLOWCTL_ENDPOINT"); v != "" {
		cfg.FlowctlEndpoint = v
	}
	if v := os.Getenv("FLOWCTL_HEARTBEAT_INTERVAL"); v != "" {
		interval, err := time.ParseDuration(v)
		if err != nil {
			return nil, fmt.Errorf("invalid FLOWCTL_HEARTBEAT_INTERVAL: %w", err)
		}
		cfg.FlowctlHeartbeatInterval = interval
	}
	if v := os.Getenv("WORKER_COUNT"); v != "" {
		count, err := strconv.Atoi(v)
		if err != nil {
			return nil, fmt.Errorf("invalid WORKER_COUNT: %w", err)
		}
		cfg.WorkerCount = count
	}
	if v := os.Getenv("CHANNEL_BUFFER_SIZE"); v != "" {
		size, err := strconv.Atoi(v)
		if err != nil {
			return nil, fmt.Errorf("invalid CHANNEL_BUFFER_SIZE: %w", err)
		}
		cfg.ChannelBufferSize = size
	}
	if v := os.Getenv("LOG_LEVEL"); v != "" {
		cfg.LogLevel = v
	}
	if v := os.Getenv("DEBUG"); v != "" {
		cfg.Debug = v == "true" || v == "1"
	}
	if v := os.Getenv("INCLUDE_DELETED"); v != "" {
		cfg.IncludeDeleted = v == "true" || v == "1"
	}

	// Parse filter lists (comma-separated)
	if v := os.Getenv("FILTER_CONTRACT_IDS"); v != "" {
		cfg.FilterContractIDs = parseCSV(v)
	}
	if v := os.Getenv("FILTER_ASSET_CODES"); v != "" {
		cfg.FilterAssetCodes = parseCSV(v)
	}
	if v := os.Getenv("FILTER_ASSET_ISSUERS"); v != "" {
		cfg.FilterAssetIssuers = parseCSV(v)
	}

	return cfg, nil
}

// parseCSV parses a comma-separated string into a slice
func parseCSV(s string) []string {
	if s == "" {
		return nil
	}
	
	var result []string
	for _, v := range splitCSV(s) {
		if v != "" {
			result = append(result, v)
		}
	}
	return result
}

// splitCSV splits a comma-separated string
func splitCSV(s string) []string {
	var result []string
	var current string
	
	for _, c := range s {
		if c == ',' {
			result = append(result, current)
			current = ""
		} else {
			current += string(c)
		}
	}
	
	if current != "" {
		result = append(result, current)
	}
	
	return result
}

// Validate ensures the configuration is valid
func (c *Config) Validate() error {
	if c.NetworkPassphrase == "" {
		return fmt.Errorf("network passphrase is required")
	}
	if c.SourceEndpoint == "" {
		return fmt.Errorf("source endpoint is required")
	}
	if c.BatchSize <= 0 {
		return fmt.Errorf("batch size must be positive")
	}
	if c.MaxMemoryMB <= 0 {
		return fmt.Errorf("max memory must be positive")
	}
	if c.WorkerCount <= 0 {
		return fmt.Errorf("worker count must be positive")
	}
	if c.GRPCAddress == "" {
		return fmt.Errorf("gRPC address is required")
	}
	if c.FlightAddress == "" {
		return fmt.Errorf("Arrow Flight address is required")
	}
	if c.HealthPort <= 0 || c.HealthPort > 65535 {
		return fmt.Errorf("invalid health port: %d", c.HealthPort)
	}
	
	return nil
}

// String returns a string representation of the config
func (c *Config) String() string {
	return fmt.Sprintf(
		"Config{Service: %s/%s, Network: %s, Addresses: [gRPC:%s, Arrow:%s, Health:%d], "+
		"Source: %s, BatchSize: %d, Workers: %d, Flowctl: %v}",
		c.ServiceName, c.ServiceVersion, c.NetworkPassphrase,
		c.GRPCAddress, c.FlightAddress, c.HealthPort,
		c.SourceEndpoint, c.BatchSize, c.WorkerCount, c.FlowctlEnabled,
	)
}