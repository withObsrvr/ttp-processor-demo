package main

import (
	"os"
	"os/signal"
	"syscall"

	"github.com/withObsrvr/ttp-processor-demo/contract-data-processor/config"
	"github.com/withObsrvr/ttp-processor-demo/contract-data-processor/logging"
	"github.com/withObsrvr/ttp-processor-demo/contract-data-processor/server"
)

func main() {
	// Load configuration
	cfg, err := config.LoadFromEnv()
	if err != nil {
		panic(err)
	}

	// Validate configuration
	if err := cfg.Validate(); err != nil {
		panic(err)
	}

	// Initialize logger
	logger := logging.NewComponentLogger(cfg.ServiceName, cfg.ServiceVersion)
	logger.Info().
		Str("service", cfg.ServiceName).
		Str("version", cfg.ServiceVersion).
		Msg("Starting contract data processor")

	// Create hybrid server
	hybridServer, err := server.NewHybridServer(cfg, logger)
	if err != nil {
		logger.Fatal().Err(err).Msg("Failed to create hybrid server")
	}

	// Start server
	if err := hybridServer.Start(); err != nil {
		logger.Fatal().Err(err).Msg("Failed to start hybrid server")
	}

	// Set up signal handling
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Wait for shutdown signal
	<-sigChan
	logger.Info().Msg("Shutdown signal received")

	// Graceful shutdown
	if err := hybridServer.Stop(); err != nil {
		logger.Error().Err(err).Msg("Error during shutdown")
	}

	logger.Info().Msg("Contract data processor stopped")
}