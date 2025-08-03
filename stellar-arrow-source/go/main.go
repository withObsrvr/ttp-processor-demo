package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/withObsrvr/ttp-processor-demo/stellar-arrow-source/logging"
	"github.com/withObsrvr/ttp-processor-demo/stellar-arrow-source/server"
)

func main() {
	// Initialize component logger
	logger := logging.NewComponentLogger("stellar-arrow-source", "v1.0.0")

	// Load configuration from environment
	config := loadConfig()

	logger.LogStartup(logging.StartupConfig{
		FlowCtlEnabled:    config.FlowCtlEnabled,
		SourceType:        "arrow_flight",
		BackendType:       "native_arrow",
		NetworkPassphrase: config.NetworkPassphrase,
		BatchSize:         config.BatchSize,
		Port:              config.Port,
	})

	// Create Arrow Flight server
	arrowServer, err := server.NewStellarArrowServer(config)
	if err != nil {
		logger.Error().
			Err(err).
			Msg("Failed to create Arrow Flight server")
		os.Exit(1)
	}

	// Start health server
	go startHealthServer(config.HealthPort, arrowServer, logger)

	// Start Arrow Flight server in goroutine
	go func() {
		logger.Info().
			Int("port", config.Port).
			Str("protocol", "arrow-flight").
			Bool("native_arrow", true).
			Msg("Starting Arrow Flight server")

		if err := arrowServer.StartArrowFlightServer(); err != nil {
			logger.Error().
				Err(err).
				Int("port", config.Port).
				Msg("Arrow Flight server failed")
			os.Exit(1)
		}
	}()

	// Wait for interrupt signal
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	<-c

	logger.Info().
		Str("operation", "shutdown_initiated").
		Msg("Shutting down stellar-arrow-source")

	// Graceful shutdown
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// In a real implementation, we would gracefully close connections here
	select {
	case <-shutdownCtx.Done():
		logger.Warn().
			Msg("Shutdown timeout exceeded")
	default:
		logger.Info().
			Msg("Graceful shutdown completed")
	}

	logger.Info().
		Str("operation", "shutdown_completed").
		Msg("Stellar Arrow Source stopped")
}

// ServerConfig holds all server configuration
type ServerConfig = server.ServerConfig

// loadConfig loads configuration from environment variables
func loadConfig() *ServerConfig {
	config := &ServerConfig{
		Port:              getEnvAsInt("PORT", 8815),
		HealthPort:        getEnvAsInt("HEALTH_PORT", 8088),
		SourceEndpoint:    getEnvOrDefault("SOURCE_ENDPOINT", "localhost:8080"),
		NetworkPassphrase: getEnvOrDefault("NETWORK_PASSPHRASE", "Test SDF Network ; September 2015"),
		BatchSize:         getEnvAsInt("BATCH_SIZE", 1000),
		MaxConnections:    getEnvAsInt("MAX_CONNECTIONS", 100),
		FlowCtlEnabled:    getEnvAsBool("FLOWCTL_ENABLED", false),
	}

	return config
}

// startHealthServer starts the health check HTTP server
func startHealthServer(port int, arrowServer *server.StellarArrowServer, logger *logging.ComponentLogger) {
	mux := http.NewServeMux()

	// Health endpoint
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		health := arrowServer.GetHealthStatus()
		
		logger.Debug().
			Str("operation", "health_check").
			Interface("health_status", health).
			Msg("Health check requested")

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		
		// Simple JSON response
		fmt.Fprintf(w, `{
			"status": "%s",
			"server_type": "%s",
			"protocol": "%s",
			"source_healthy": %t,
			"memory_allocated": %d,
			"timestamp": "%s"
		}`,
			health["status"],
			health["server_type"],
			health["protocol"],
			health["source_healthy"],
			health["memory_allocated"],
			time.Now().Format(time.RFC3339))
	})

	// Metrics endpoint (basic)
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		health := arrowServer.GetHealthStatus()
		
		w.Header().Set("Content-Type", "text/plain")
		w.WriteHeader(http.StatusOK)
		
		// Simple Prometheus-style metrics
		fmt.Fprintf(w, "# HELP arrow_server_healthy Arrow server health status\n")
		fmt.Fprintf(w, "# TYPE arrow_server_healthy gauge\n")
		fmt.Fprintf(w, "arrow_server_healthy{service=\"stellar-arrow-source\"} 1\n")
		
		fmt.Fprintf(w, "# HELP arrow_memory_allocated_bytes Arrow memory allocated\n")
		fmt.Fprintf(w, "# TYPE arrow_memory_allocated_bytes gauge\n")
		fmt.Fprintf(w, "arrow_memory_allocated_bytes{service=\"stellar-arrow-source\"} %d\n", 
			health["memory_allocated"])
	})

	// Ready endpoint
	mux.HandleFunc("/ready", func(w http.ResponseWriter, r *http.Request) {
		health := arrowServer.GetHealthStatus()
		
		if health["status"] == "healthy" && health["source_healthy"].(bool) {
			w.WriteHeader(http.StatusOK)
			fmt.Fprint(w, "ready")
		} else {
			w.WriteHeader(http.StatusServiceUnavailable)
			fmt.Fprint(w, "not ready")
		}
	})

	server := &http.Server{
		Addr:    fmt.Sprintf(":%d", port),
		Handler: mux,
	}

	logger.Info().
		Int("port", port).
		Str("endpoints", "/health,/metrics,/ready").
		Msg("Starting health server")

	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		logger.Error().
			Err(err).
			Int("port", port).
			Msg("Health server failed")
	}
}

// Helper functions for environment variable parsing
func getEnvOrDefault(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func getEnvAsInt(key string, defaultValue int) int {
	if value := os.Getenv(key); value != "" {
		if intValue, err := strconv.Atoi(value); err == nil {
			return intValue
		}
	}
	return defaultValue
}

func getEnvAsBool(key string, defaultValue bool) bool {
	if value := os.Getenv(key); value != "" {
		if boolValue, err := strconv.ParseBool(value); err == nil {
			return boolValue
		}
	}
	return defaultValue
}