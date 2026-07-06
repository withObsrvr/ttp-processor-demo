package main

import (
	"context"
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
	backfill := flag.Bool("backfill", false, "Run one-shot cold Silver account index backfill and exit")
	backfillStart := flag.Int64("start-ledger", 0, "Backfill start ledger, inclusive (0 = resume from checkpoint)")
	backfillEnd := flag.Int64("end-ledger", 0, "Backfill end ledger, inclusive (0 = cold Silver max ledger)")
	backfillBatch := flag.Int64("batch-ledgers", 0, "Backfill batch size in ledgers (0 = index partition size)")
	backfillForceRestart := flag.Bool("force-restart", false, "Backfill from --start-ledger even when checkpoint is already ahead")
	backfillPostgresOnly := flag.Bool("postgres-only", false, "Backfill only the Postgres account index mirror without touching the DuckLake index/checkpoint")
	flag.Parse()

	// Load configuration
	log.Printf("🔧 Loading configuration from %s", *configPath)
	config, err := LoadConfig(*configPath)
	if err != nil {
		log.Fatalf("Failed to load configuration: %v", err)
	}

	log.Printf("📋 Service: %s %s", config.Service.Name, config.Service.Version)
	log.Printf("📋 Poll interval: %v", config.GetPollInterval())
	log.Printf("📋 Batch size: %d ledgers", config.Service.BatchSize)
	log.Printf("📋 account buckets: %d", config.AccountBucketCount())

	log.Println("🔗 Connecting to Silver Hot (PostgreSQL silver_hot)...")
	silverDB, err := sql.Open("postgres", config.SilverHot.ConnectionString())
	if err != nil {
		log.Fatalf("Failed to open Silver Hot connection: %v", err)
	}
	defer silverDB.Close()

	if err := silverDB.Ping(); err != nil {
		log.Fatalf("Failed to ping Silver Hot: %v", err)
	}
	log.Println("✅ Connected to Silver Hot")

	// Connect to Catalog DB (for checkpoints)
	log.Printf("🔗 Connecting to Catalog DB (%s:%d/%s)...", config.Catalog.Host, config.Catalog.Port, config.Catalog.Database)
	catalogDB, err := sql.Open("postgres", config.Catalog.ConnectionString())
	if err != nil {
		log.Fatalf("Failed to open Catalog connection: %v", err)
	}
	defer catalogDB.Close()

	if err := catalogDB.Ping(); err != nil {
		log.Fatalf("Failed to ping Catalog DB: %v", err)
	}
	log.Println("✅ Connected to Catalog DB")

	if *backfill {
		opts := BackfillOptions{
			StartLedger:  *backfillStart,
			EndLedger:    *backfillEnd,
			BatchLedgers: *backfillBatch,
			ForceRestart: *backfillForceRestart,
			PostgresOnly: *backfillPostgresOnly,
		}
		if err := validateBackfillOptions(opts); err != nil {
			log.Fatalf("Invalid backfill options: %v", err)
		}
		if err := RunBackfill(context.Background(), config, catalogDB, opts); err != nil {
			log.Fatalf("Backfill failed: %v", err)
		}
		return
	}

	// Create transformer
	log.Println("🔗 Creating Index Plane Transformer...")
	transformer, err := NewTransformer(config, silverDB, catalogDB)
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
