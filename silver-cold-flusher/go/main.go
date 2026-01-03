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

	// Start health server
	healthServer := NewHealthServer(config.Service.HealthPort, config.Service.FlushInterval())
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
	log.Println("🔍 Running initial flush...")
	if err := flusher.ExecuteFlush(); err != nil {
		log.Printf("⚠️  Initial flush error: %v", err)
	}

	// Update health server
	watermark, _ := flusher.getWatermark()
	healthServer.UpdateStats(watermark, flusher.GetStats(), time.Now().Add(config.Service.FlushInterval()))

	// Main loop
	for {
		select {
		case <-ticker.C:
			log.Println("⏰ Flush interval elapsed, starting flush...")
			if err := flusher.ExecuteFlush(); err != nil {
				log.Printf("❌ Flush error: %v", err)
			}

			// Update health server
			watermark, _ := flusher.getWatermark()
			healthServer.UpdateStats(watermark, flusher.GetStats(), time.Now().Add(config.Service.FlushInterval()))

		case <-shutdownChan:
			log.Println("🛑 Shutdown signal received")
			log.Println("🔄 Performing final flush...")

			if err := flusher.ExecuteFlush(); err != nil {
				log.Printf("⚠️  Final flush error: %v", err)
			}

			log.Println("👋 Shutting down gracefully")
			healthServer.Stop()
			return
		}
	}
}
