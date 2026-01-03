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
	log.Printf("📋 Batch size: %d ledgers", config.Service.BatchSize)

	// Connect to Bronze Hot (PostgreSQL stellar_hot)
	log.Println("🔗 Connecting to Bronze Hot (PostgreSQL stellar_hot)...")
	bronzeDB, err := sql.Open("postgres", config.BronzeHot.ConnectionString())
	if err != nil {
		log.Fatalf("Failed to open Bronze Hot connection: %v", err)
	}
	defer bronzeDB.Close()

	if err := bronzeDB.Ping(); err != nil {
		log.Fatalf("Failed to ping Bronze Hot: %v", err)
	}
	log.Println("✅ Connected to Bronze Hot")

	// Connect to Catalog DB (for checkpoints - uses BronzeHot config)
	log.Println("🔗 Connecting to Catalog DB...")
	catalogDB, err := sql.Open("postgres", config.BronzeHot.ConnectionString())
	if err != nil {
		log.Fatalf("Failed to open Catalog connection: %v", err)
	}
	defer catalogDB.Close()

	if err := catalogDB.Ping(); err != nil {
		log.Fatalf("Failed to ping Catalog DB: %v", err)
	}
	log.Println("✅ Connected to Catalog DB")

	// Create transformer
	log.Println("🔗 Creating Index Plane Transformer...")
	transformer, err := NewTransformer(config, bronzeDB, catalogDB)
	if err != nil {
		log.Fatalf("Failed to create transformer: %v", err)
	}

	// Setup signal handling for graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Start transformer in background
	errChan := make(chan error, 1)
	go func() {
		if err := transformer.Start(); err != nil {
			errChan <- err
		}
	}()

	// Wait for shutdown signal or error
	select {
	case sig := <-sigChan:
		log.Printf("Received signal: %v", sig)
		if err := transformer.Stop(); err != nil {
			log.Printf("Error during shutdown: %v", err)
			os.Exit(1)
		}
	case err := <-errChan:
		log.Printf("Transformer error: %v", err)
		if stopErr := transformer.Stop(); stopErr != nil {
			log.Printf("Error during shutdown: %v", stopErr)
		}
		os.Exit(1)
	}

	log.Println("👋 Goodbye!")
}
