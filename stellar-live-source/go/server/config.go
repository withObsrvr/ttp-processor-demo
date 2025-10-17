package server

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config holds the configuration for the RawLedgerServer
type Config struct {
	// Core configuration
	RPCEndpoint       string
	NetworkPassphrase string

	// Protocol 23: External datastore configuration
	ServeFromDatastore bool   // Maps to SERVE_LEDGERS_FROM_DATASTORE in RPC
	DatastoreType      string // "GCS" or "S3"
	BucketPath         string // Destination bucket path
	Region             string // AWS region (S3 only)
	
	// Protocol 23: Datastore schema configuration
	LedgersPerFile    uint32 // Default: 1
	FilesPerPartition uint32 // Default: 64000
	
	// Protocol 23: Buffered storage backend configuration
	BufferSize  int           // Default: 100
	NumWorkers  int           // Default: 10
	RetryLimit  int           // Default: 3
	RetryWait   time.Duration // Default: 5s

	// Cache configuration (our enhancement)
	CacheHistoricalResponses  bool
	HistoricalCacheDuration   time.Duration
	EnablePredictivePrefetch  bool
	PrefetchConcurrency       int

	// Performance tuning
	BatchSize                 int
	MaxRetries                int
	InitialBackoff            time.Duration
	MaxBackoff                time.Duration
	CircuitBreakerThreshold   int
	CircuitBreakerTimeout     time.Duration
}

// LoadConfig loads configuration from environment variables
func LoadConfig() (*Config, error) {
	config := &Config{
		// Core configuration
		RPCEndpoint:       getEnvOrDefault("RPC_ENDPOINT", ""),
		NetworkPassphrase: getEnvOrDefault("NETWORK_PASSPHRASE", "Test SDF Network ; September 2015"),

		// Protocol 23: External datastore configuration
		ServeFromDatastore: getBoolEnv("SERVE_LEDGERS_FROM_DATASTORE", true), // Default enabled
		DatastoreType:      getEnvOrDefault("DATASTORE_TYPE", "GCS"),
		BucketPath:         getEnvOrDefault("DATASTORE_BUCKET_PATH", ""),
		Region:             getEnvOrDefault("DATASTORE_REGION", "us-east-1"),
		
		// Protocol 23: Datastore schema configuration
		LedgersPerFile:    getUint32Env("LEDGERS_PER_FILE", 1),
		FilesPerPartition: getUint32Env("FILES_PER_PARTITION", 64000),
		
		// Protocol 23: Buffered storage backend configuration
		BufferSize:  getIntEnv("BUFFER_SIZE", 100),
		NumWorkers:  getIntEnv("NUM_WORKERS", 10),
		RetryLimit:  getIntEnv("RETRY_LIMIT", 3),
		RetryWait:   getDurationEnv("RETRY_WAIT", 5*time.Second),

		// Cache configuration (our enhancement)
		CacheHistoricalResponses:  getBoolEnv("CACHE_HISTORICAL_RESPONSES", true),
		HistoricalCacheDuration:   getDurationEnv("HISTORICAL_CACHE_DURATION", 1*time.Hour),
		EnablePredictivePrefetch:  getBoolEnv("ENABLE_PREDICTIVE_PREFETCH", true),
		PrefetchConcurrency:       getIntEnv("PREFETCH_CONCURRENCY", 10),

		// Performance tuning
		BatchSize:               getIntEnv("BATCH_SIZE", 100),
		MaxRetries:              getIntEnv("MAX_RETRIES", 5),
		InitialBackoff:          getDurationEnv("INITIAL_BACKOFF", 1*time.Second),
		MaxBackoff:              getDurationEnv("MAX_BACKOFF", 30*time.Second),
		CircuitBreakerThreshold: getIntEnv("CIRCUIT_BREAKER_THRESHOLD", 5),
		CircuitBreakerTimeout:   getDurationEnv("CIRCUIT_BREAKER_TIMEOUT", 30*time.Second),
	}

	// Validate required configuration
	if config.RPCEndpoint == "" {
		return nil, fmt.Errorf("RPC_ENDPOINT is required")
	}

	// If using external datastore, validate bucket path
	if config.ServeFromDatastore && config.BucketPath == "" {
		// Try common environment variables
		config.BucketPath = getEnvOrDefault("BUCKET_PATH", getEnvOrDefault("GCS_BUCKET_PATH", getEnvOrDefault("S3_BUCKET_PATH", "")))
		if config.BucketPath == "" {
			return nil, fmt.Errorf("DATASTORE_BUCKET_PATH is required when SERVE_LEDGERS_FROM_DATASTORE is true")
		}
	}

	return config, nil
}

// Helper functions for environment variable parsing
func getEnvOrDefault(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func getBoolEnv(key string, defaultValue bool) bool {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}
	result, err := strconv.ParseBool(value)
	if err != nil {
		return defaultValue
	}
	return result
}

func getIntEnv(key string, defaultValue int) int {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}
	result, err := strconv.Atoi(value)
	if err != nil {
		return defaultValue
	}
	return result
}

func getUint32Env(key string, defaultValue uint32) uint32 {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}
	result, err := strconv.ParseUint(value, 10, 32)
	if err != nil {
		return defaultValue
	}
	return uint32(result)
}

func getDurationEnv(key string, defaultValue time.Duration) time.Duration {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}
	result, err := time.ParseDuration(value)
	if err != nil {
		return defaultValue
	}
	return result
}

func getStringSliceEnv(key, separator string) []string {
	value := os.Getenv(key)
	if value == "" {
		return []string{}
	}
	return strings.Split(value, separator)
}