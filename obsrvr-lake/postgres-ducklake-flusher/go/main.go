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
	flushOnce := flag.Bool("flush-once", false, "Run one flush cycle and exit")
	finalFlushOnShutdown := flag.Bool("final-flush-on-shutdown", false, "Run one final flush after SIGINT/SIGTERM before exiting")
	flushTimeout := flag.Duration("flush-timeout", 0, "Optional timeout for each flush cycle, for example 30m; 0 means no timeout")
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
	if config.Downstream.IsConfigured() {
		log.Printf("Downstream checkpoint: %s:%d/%s (table=%s, column=%s)",
			config.Downstream.Host, config.Downstream.Port, config.Downstream.Database,
			config.Downstream.Table, config.Downstream.Column)
	} else {
		log.Println("WARNING: No downstream checkpoint configured — deletion will not be checkpoint-aware")
	}

	// Create flusher
	flusher, err := NewFlusher(config)
	if err != nil {
		log.Fatalf("Failed to create flusher: %v", err)
	}
	defer flusher.Close()

	runFlush := func(reason string) (*FlushMetrics, error) {
		ctx := context.Background()
		cancel := func() {}
		if *flushTimeout > 0 {
			ctx, cancel = context.WithTimeout(ctx, *flushTimeout)
		}
		defer cancel()

		log.Printf("Starting %s flush", reason)
		metrics, err := flusher.Flush(ctx)
		if err != nil {
			return metrics, err
		}
		if metrics.Watermark > 0 {
			log.Printf("%s flush complete: count=%d, watermark=%d, flushed=%d rows in %v (success=%d, failed=%d)",
				reason, flusher.GetFlushCount(), metrics.Watermark, metrics.RowsFlushed,
				metrics.Duration, metrics.TablesSuccess, metrics.TablesFailed)
		}
		return metrics, nil
	}

	if *flushOnce {
		if _, err := runFlush("one-shot"); err != nil {
			log.Fatalf("One-shot flush failed: %v", err)
		}
		log.Println("One-shot flush complete; exiting")
		return
	}

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
			if _, err := runFlush("scheduled"); err != nil {
				log.Printf("ERROR: Flush failed: %v", err)
				continue
			}

		case sig := <-sigChan:
			log.Printf("Received signal %v, shutting down gracefully...", sig)

			if *finalFlushOnShutdown {
				if _, err := runFlush("shutdown"); err != nil {
					log.Printf("Warning: Final flush failed: %v", err)
				}
			} else {
				log.Println("Skipping final flush on shutdown; use -flush-once for controlled catch-up flushes")
			}

			log.Println("Shutdown complete")
			return
		}
	}
}
