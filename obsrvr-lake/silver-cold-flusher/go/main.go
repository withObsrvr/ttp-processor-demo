package main

import (
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
	flag.Parse()

	// Load configuration
	log.Printf("🔧 Loading configuration from %s", *configPath)
	config, err := LoadConfig(*configPath)
	if err != nil {
		log.Fatalf("Failed to load configuration: %v", err)
	}

	log.Printf("📋 Service: %s", config.Service.Name)
	log.Printf("📋 Flush interval: %v", config.Service.FlushInterval())
	log.Printf("📋 Vacuum: enabled=%v, every=%d flushes", config.Vacuum.Enabled, config.Vacuum.EveryNFlushes)

	// Create flusher
	flusher, err := NewFlusher(config)
	if err != nil {
		log.Fatalf("Failed to create flusher: %v", err)
	}
	defer flusher.Close()

	runFlush := func(reason string) error {
		log.Printf("🔄 Starting %s flush...", reason)
		return flusher.ExecuteFlush()
	}

	if *flushOnce {
		if err := runFlush("one-shot"); err != nil {
			log.Fatalf("One-shot flush failed: %v", err)
		}
		log.Println("One-shot flush complete; exiting")
		return
	}

	// Start health server
	healthServer := NewHealthServer(config.Service.HealthPort, config.Service.FlushInterval(), flusher.GetDuckDB())
	go func() {
		if err := healthServer.Start(); err != nil {
			log.Printf("Health server error: %v", err)
		}
	}()

	// Setup shutdown handling
	shutdownChan := make(chan os.Signal, 1)
	signal.Notify(shutdownChan, os.Interrupt, syscall.SIGTERM)

	// Create flush ticker
	ticker := time.NewTicker(config.Service.FlushInterval())
	defer ticker.Stop()

	log.Printf("🚀 Silver Cold Flusher started")
	log.Printf("⏱️  Next flush in %v", config.Service.FlushInterval())

	// Run initial flush immediately
	if err := runFlush("initial"); err != nil {
		log.Printf("⚠️  Initial flush error: %v", err)
	}

	// Update health server
	watermark, _ := flusher.getWatermark()
	healthServer.UpdateStats(watermark, flusher.GetStats(), time.Now().Add(config.Service.FlushInterval()))

	// Main loop
	for {
		select {
		case <-ticker.C:
			if err := runFlush("scheduled"); err != nil {
				log.Printf("❌ Flush error: %v", err)
			}

			// Update health server
			watermark, _ := flusher.getWatermark()
			healthServer.UpdateStats(watermark, flusher.GetStats(), time.Now().Add(config.Service.FlushInterval()))

		case <-shutdownChan:
			log.Println("🛑 Shutdown signal received")

			if *finalFlushOnShutdown {
				if err := runFlush("shutdown"); err != nil {
					log.Printf("⚠️  Final flush error: %v", err)
				}
			} else {
				log.Println("Skipping final flush on shutdown; use -flush-once for controlled catch-up flushes")
			}

			log.Println("👋 Shutting down gracefully")
			healthServer.Stop()
			return
		}
	}
}
