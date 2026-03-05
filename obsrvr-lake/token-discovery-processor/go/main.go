package main

import (
	"database/sql"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	_ "github.com/lib/pq"
)

func main() {
	// Parse command-line flags
	configPath := flag.String("config", "/local/config.yaml", "Path to configuration file")
	flag.Parse()

	// Load configuration
	log.Printf("🔧 Loading configuration from %s", *configPath)
	config, err := LoadConfig(*configPath)
	if err != nil {
		log.Fatalf("Failed to load configuration: %v", err)
	}

	log.Printf("📋 Service: %s v%s", config.Service.Name, config.Service.Version)
	log.Printf("📋 Poll interval: %v", config.GetPollInterval())
	log.Printf("📋 Batch size: %d contracts", config.Discovery.BatchSize)
	log.Printf("📋 SEP-41 min functions: %d", config.Discovery.SEP41MinFunctions)

	// Connect to Source DB (Silver layer PostgreSQL)
	log.Println("🔗 Connecting to Source DB (Silver layer)...")
	sourceDB, err := sql.Open("postgres", config.SourceDB.ConnectionString())
	if err != nil {
		log.Fatalf("Failed to open Source DB connection: %v", err)
	}
	defer sourceDB.Close()

	if err := sourceDB.Ping(); err != nil {
		log.Fatalf("Failed to ping Source DB: %v", err)
	}
	log.Println("✅ Connected to Source DB (Silver layer)")

	// Connect to Destination DB (Token Registry)
	log.Println("🔗 Connecting to Destination DB (Token Registry)...")
	destDB, err := sql.Open("postgres", config.DestDB.ConnectionString())
	if err != nil {
		log.Fatalf("Failed to open Destination DB connection: %v", err)
	}
	defer destDB.Close()

	if err := destDB.Ping(); err != nil {
		log.Fatalf("Failed to ping Destination DB: %v", err)
	}
	log.Println("✅ Connected to Destination DB (Token Registry)")

	// Create components
	reader := NewSilverReader(sourceDB)
	writer := NewTokenWriter(destDB)
	checkpoint, err := NewCheckpointManager(destDB, &config.Checkpoint)
	if err != nil {
		log.Fatalf("Failed to create checkpoint manager: %v", err)
	}
	health := NewHealthServer(config.Health.Port)

	// Create processor
	log.Println("🔧 Creating Token Discovery Processor...")
	processor := NewProcessor(config, reader, writer, checkpoint, health)

	// Setup signal handling for graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Start processor in background
	errChan := make(chan error, 1)
	go func() {
		if err := processor.Start(); err != nil {
			errChan <- err
		}
	}()

	// Wait for shutdown signal or error
	select {
	case sig := <-sigChan:
		log.Printf("Received signal: %v", sig)
		if err := processor.Stop(); err != nil {
			log.Printf("Error during shutdown: %v", err)
			os.Exit(1)
		}
	case err := <-errChan:
		log.Printf("Processor error: %v", err)
		if stopErr := processor.Stop(); stopErr != nil {
			log.Printf("Error during shutdown: %v", stopErr)
		}
		os.Exit(1)
	}

	log.Println("👋 Goodbye!")
}
