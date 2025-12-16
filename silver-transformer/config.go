package main

import (
	"fmt"
	"os"
	"time"
)

// Config holds all configuration for the silver transformer service
type Config struct {
	// Bronze Query API URL (HTTP endpoint for querying bronze catalog)
	BronzeQueryURL string

	// Silver catalog path (read-write)
	SilverPath string

	// Bronze schema name (e.g., "testnet", "mainnet")
	BronzeSchema string

	// Polling interval for checking new data
	PollInterval time.Duration

	// Batch size for processing ledgers
	BatchSize int

	// Health check endpoint port
	HealthPort string

	// Metrics endpoint port
	MetricsPort string
}

// LoadConfig loads configuration from environment variables with defaults
func LoadConfig() Config {
	return Config{
		BronzeQueryURL: getEnv("BRONZE_QUERY_URL", "http://localhost:8081"),
		SilverPath:     getEnv("SILVER_PATH", "catalogs/silver.duckdb"),
		BronzeSchema:   getEnv("BRONZE_SCHEMA", "testnet"),
		PollInterval:   getDurationEnv("POLL_INTERVAL", 1*time.Second),
		BatchSize:      getIntEnv("BATCH_SIZE", 100),
		HealthPort:     getEnv("HEALTH_PORT", "8082"),
		MetricsPort:    getEnv("METRICS_PORT", "9092"),
	}
}

func (c Config) Validate() error {
	if c.BronzeQueryURL == "" {
		return fmt.Errorf("BRONZE_QUERY_URL is required")
	}
	if c.SilverPath == "" {
		return fmt.Errorf("SILVER_PATH is required")
	}
	if c.BronzeSchema == "" {
		return fmt.Errorf("BRONZE_SCHEMA is required")
	}
	if c.PollInterval <= 0 {
		return fmt.Errorf("POLL_INTERVAL must be positive")
	}
	if c.BatchSize <= 0 {
		return fmt.Errorf("BATCH_SIZE must be positive")
	}
	return nil
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func getIntEnv(key string, defaultValue int) int {
	if value := os.Getenv(key); value != "" {
		var intVal int
		if _, err := fmt.Sscanf(value, "%d", &intVal); err == nil {
			return intVal
		}
	}
	return defaultValue
}

func getDurationEnv(key string, defaultValue time.Duration) time.Duration {
	if value := os.Getenv(key); value != "" {
		if duration, err := time.ParseDuration(value); err == nil {
			return duration
		}
	}
	return defaultValue
}
