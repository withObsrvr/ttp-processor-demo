package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	_ "github.com/lib/pq"
)

func main() {
	// Parse command-line flags
	configPath := flag.String("config", "config.yaml", "Path to configuration file")
	flag.Parse()

	// Load configuration
	log.Printf("📖 Loading configuration from: %s", *configPath)
	config, err := LoadConfig(*configPath)
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	log.Printf("🔧 Service: %s v%s", config.Service.Name, config.Service.Version)

	// Create transformer
	transformer, err := NewTransformer(config)
	if err != nil {
		log.Fatalf("Failed to create transformer: %v", err)
	}

	// Set up signal handling for graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	// Start transformer in goroutine
	errChan := make(chan error, 1)
	go func() {
		if err := transformer.Start(); err != nil {
			errChan <- err
		}
	}()

	// Wait for shutdown signal or error
	select {
	case <-sigChan:
		log.Println("📥 Shutdown signal received")
		if err := transformer.Stop(); err != nil {
			log.Printf("Error during shutdown: %v", err)
			os.Exit(1)
		}
	case err := <-errChan:
		log.Printf("❌ Transformer error: %v", err)
		transformer.Stop()
		os.Exit(1)
	}

	log.Println("👋 Goodbye!")
}

// connectPostgres creates a PostgreSQL database connection
func connectPostgres(config *DatabaseConfig) (*sql.DB, error) {
	connStr := config.ConnectionString()

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	// Test connection
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	log.Printf("✅ Connected to PostgreSQL: %s:%d/%s", config.Host, config.Port, config.Database)
	return db, nil
}
