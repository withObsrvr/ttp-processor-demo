package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	// Parse command-line flags
	configPath := flag.String("config", "config.yaml", "Path to configuration file")
	flag.Parse()

	log.Println("Starting postgres-ducklake-flusher")

	// Load configuration
	config, err := LoadConfig(*configPath)
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	log.Printf("Service: %s", config.Service.Name)
	log.Printf("Flush interval: %d minutes", config.Service.FlushIntervalMinutes)
	log.Printf("PostgreSQL: %s:%d/%s", config.Postgres.Host, config.Postgres.Port, config.Postgres.Database)
	log.Printf("DuckLake catalog: %s", config.DuckLake.CatalogName)

	// Create flusher
	flusher, err := NewFlusher(config)
	if err != nil {
		log.Fatalf("Failed to create flusher: %v", err)
	}
	defer flusher.Close()

	// Start health server in background with maintenance endpoints
	healthServer := NewHealthServer(flusher, flusher.GetDuckDB(), config.Service.HealthPort)
	go func() {
		if err := healthServer.Start(); err != nil {
			log.Fatalf("Health server failed: %v", err)
		}
	}()

	// Create ticker for flush interval
	flushInterval := time.Duration(config.Service.FlushIntervalMinutes) * time.Minute
	ticker := time.NewTicker(flushInterval)
	defer ticker.Stop()

	// Set up signal handling for graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	log.Printf("Flusher started. Will flush every %v", flushInterval)
	log.Println("Press Ctrl+C to stop")

	// Main loop
	for {
		select {
		case <-ticker.C:
			// Perform flush operation
			ctx := context.Background()
			metrics, err := flusher.Flush(ctx)
			if err != nil {
				log.Printf("ERROR: Flush failed: %v", err)
				// Continue running despite errors
				continue
			}

			// Log summary
			if metrics.Watermark > 0 {
				log.Printf("Flush #%d: watermark=%d, flushed=%d rows in %v (success=%d, failed=%d)",
					flusher.GetFlushCount(), metrics.Watermark, metrics.RowsFlushed,
					metrics.Duration, metrics.TablesSuccess, metrics.TablesFailed)
			}

		case sig := <-sigChan:
			log.Printf("Received signal %v, shutting down gracefully...", sig)

			// Perform one final flush before shutting down
			log.Println("Performing final flush...")
			ctx := context.Background()
			if _, err := flusher.Flush(ctx); err != nil {
				log.Printf("Warning: Final flush failed: %v", err)
			}

			log.Println("Shutdown complete")
			return
		}
	}
}
