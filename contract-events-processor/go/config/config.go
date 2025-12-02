package config

import (
	"fmt"
	"os"
	"strings"
)

// Config holds the server configuration
type Config struct {
	Port              string
	HealthPort        string
	LiveSourceAddress string
	NetworkPassphrase string
}

// LoadConfig loads configuration from environment variables
func LoadConfig() (*Config, error) {
	cfg := &Config{
		Port:              getEnvOrDefault("PORT", ":50053"),
		HealthPort:        getEnvOrDefault("HEALTH_PORT", "8089"),
		LiveSourceAddress: getEnvOrDefault("LIVE_SOURCE_ADDRESS", "localhost:50052"),
		NetworkPassphrase: os.Getenv("NETWORK_PASSPHRASE"),
	}

	// Ensure port starts with ":"
	if !strings.HasPrefix(cfg.Port, ":") {
		cfg.Port = ":" + cfg.Port
	}

	// Validate required fields
	if cfg.NetworkPassphrase == "" {
		return nil, fmt.Errorf("NETWORK_PASSPHRASE environment variable is required")
	}

	return cfg, nil
}

func getEnvOrDefault(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}
