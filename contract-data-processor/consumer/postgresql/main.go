package main

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/apache/arrow/go/v17/arrow/flight"
	"github.com/google/uuid"
	_ "github.com/lib/pq"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

type Config struct {
	// Database configuration
	DBHost     string
	DBPort     int
	DBName     string
	DBUser     string
	DBPassword string
	DBSSLMode  string

	// Arrow Flight source
	FlightEndpoint string
	
	// Processing configuration
	BatchSize      int
	CommitInterval time.Duration
	MaxRetries     int
	
	// Monitoring
	MetricsPort int
	
	// Flowctl integration
	FlowctlEnabled           bool
	FlowctlEndpoint          string
	FlowctlHeartbeatInterval time.Duration
}

func main() {
	// Initialize logger
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})
	
	// Load configuration
	cfg := loadConfig()
	
	// Create context for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	
	// Set up signal handling
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	
	// Create consumer
	consumer, err := NewPostgreSQLConsumer(cfg)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to create consumer")
	}
	defer consumer.Close()
	
	// Start consuming
	go func() {
		if err := consumer.Start(ctx); err != nil {
			log.Error().Err(err).Msg("Consumer error")
			cancel()
		}
	}()
	
	// Wait for shutdown signal
	select {
	case sig := <-sigChan:
		log.Info().Str("signal", sig.String()).Msg("Received shutdown signal")
	case <-ctx.Done():
		log.Info().Msg("Context cancelled")
	}
	
	// Graceful shutdown
	log.Info().Msg("Shutting down gracefully...")
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer shutdownCancel()
	
	if err := consumer.Shutdown(shutdownCtx); err != nil {
		log.Error().Err(err).Msg("Error during shutdown")
	}
	
	log.Info().Msg("PostgreSQL consumer stopped")
}

func loadConfig() *Config {
	cfg := &Config{
		// Database defaults
		DBHost:     getEnvOrDefault("DB_HOST", "localhost"),
		DBPort:     getEnvAsIntOrDefault("DB_PORT", 5432),
		DBName:     getEnvOrDefault("DB_NAME", "stellar_contracts"),
		DBUser:     getEnvOrDefault("DB_USER", "postgres"),
		DBPassword: os.Getenv("DB_PASSWORD"),
		DBSSLMode:  getEnvOrDefault("DB_SSLMODE", "disable"),
		
		// Arrow Flight defaults
		FlightEndpoint: getEnvOrDefault("FLIGHT_ENDPOINT", "localhost:8816"),
		
		// Processing defaults
		BatchSize:      getEnvAsIntOrDefault("BATCH_SIZE", 1000),
		CommitInterval: getEnvAsDurationOrDefault("COMMIT_INTERVAL", 5*time.Second),
		MaxRetries:     getEnvAsIntOrDefault("MAX_RETRIES", 3),
		
		// Monitoring
		MetricsPort: getEnvAsIntOrDefault("METRICS_PORT", 9090),
		
		// Flowctl
		FlowctlEnabled:           getEnvOrDefault("FLOWCTL_ENABLED", "false") == "true",
		FlowctlEndpoint:          getEnvOrDefault("FLOWCTL_ENDPOINT", "localhost:8080"),
		FlowctlHeartbeatInterval: getEnvAsDurationOrDefault("FLOWCTL_HEARTBEAT_INTERVAL", 10*time.Second),
	}
	
	// Validate required fields
	if cfg.DBPassword == "" {
		log.Fatal().Msg("DB_PASSWORD is required")
	}
	
	return cfg
}

func getEnvOrDefault(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func getEnvAsIntOrDefault(key string, defaultValue int) int {
	if value := os.Getenv(key); value != "" {
		var intValue int
		if _, err := fmt.Sscanf(value, "%d", &intValue); err == nil {
			return intValue
		}
	}
	return defaultValue
}

func getEnvAsDurationOrDefault(key string, defaultValue time.Duration) time.Duration {
	if value := os.Getenv(key); value != "" {
		if duration, err := time.ParseDuration(value); err == nil {
			return duration
		}
	}
	return defaultValue
}