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
	// Parse command line flags
	configPath := flag.String("config", "config.yaml", "Path to config file")
	flag.Parse()

	log.Println("🔧 Loading configuration from", *configPath)

	// Load configuration
	config, err := LoadConfig(*configPath)
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	if err := config.Validate(); err != nil {
		log.Fatalf("Invalid config: %v", err)
	}

	log.Printf("📋 Service: %s", config.Service.Name)
	log.Printf("📋 Poll interval: %v", config.PollInterval())
	log.Printf("📋 Batch size: %d ledgers", config.Performance.BatchSize)

	// Connect to Bronze Hot (stellar_hot PostgreSQL)
	log.Println("🔗 Connecting to Bronze Hot (stellar_hot)...")
	bronzeDB, err := sql.Open("postgres", config.BronzeHot.ConnectionString())
	if err != nil {
		log.Fatalf("Failed to connect to bronze hot: %v", err)
	}
	defer bronzeDB.Close()

	if err := bronzeDB.Ping(); err != nil {
		log.Fatalf("Failed to ping bronze hot: %v", err)
	}
	log.Println("✅ Connected to Bronze Hot")

	// Connect to Silver Hot (silver_hot PostgreSQL)
	log.Println("🔗 Connecting to Silver Hot (silver_hot)...")
	silverDB, err := sql.Open("postgres", config.SilverHot.ConnectionString())
	if err != nil {
		log.Fatalf("Failed to connect to silver hot: %v", err)
	}
	defer silverDB.Close()

	if err := silverDB.Ping(); err != nil {
		log.Fatalf("Failed to ping silver hot: %v", err)
	}
	log.Println("✅ Connected to Silver Hot")

	// Initialize components
	bronzeHotReader := NewBronzeReader(bronzeDB)
	silverWriter := NewSilverWriter(silverDB)
	checkpoint := NewCheckpointManager(silverDB, config.Checkpoint.Table)

	// Initialize Bronze Cold reader if configured
	var bronzeColdReader *BronzeColdReader
	if config.Fallback.Enabled && config.BronzeCold != nil && config.S3 != nil {
		log.Println("🔗 Connecting to Bronze Cold (DuckLake)...")
		var err error
		bronzeColdReader, err = NewBronzeColdReader(config.BronzeCold, config.S3)
		if err != nil {
			log.Printf("⚠️  Failed to initialize Bronze Cold reader: %v", err)
			log.Println("   Fallback to cold storage will be disabled")
		} else {
			log.Println("✅ Connected to Bronze Cold (DuckLake)")
		}
	} else if config.Fallback.Enabled {
		log.Println("⚠️  Fallback enabled but bronze_cold or s3 config missing - fallback disabled")
	}

	// Create source manager with hot/cold readers
	sourceManager := NewSourceManager(bronzeHotReader, bronzeColdReader, config.Fallback.Enabled)
	if bronzeColdReader != nil {
		defer sourceManager.Close()
	}

	// Create transformer
	transformer := NewRealtimeTransformer(config, sourceManager, silverWriter, checkpoint, silverDB)

	// Start health server in goroutine
	healthServer := NewHealthServer(transformer)
	go func() {
		if err := healthServer.Start(config.Service.HealthPort); err != nil {
			log.Fatalf("Health server failed: %v", err)
		}
	}()

	// Setup graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	go func() {
		<-sigChan
		log.Println("🛑 Shutdown signal received...")
		transformer.Stop()
	}()

	// Start transformer (blocks until stopped)
	if err := transformer.Start(); err != nil {
		log.Fatalf("Transformer failed: %v", err)
	}

	log.Println("👋 Shutdown complete")
}
