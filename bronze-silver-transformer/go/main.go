package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	// Parse command-line flags
	configPath := flag.String("config", "config.yaml", "Path to configuration file")
	flag.Parse()

	// Load configuration
	config, err := LoadConfig(*configPath)
	if err != nil {
		log.Fatalf("❌ Failed to load config: %v", err)
	}

	log.Printf("🔧 Loaded configuration from %s", *configPath)

	// Create transformer
	transformer, err := NewTransformer(config)
	if err != nil {
		log.Fatalf("❌ Failed to create transformer: %v", err)
	}

	// Start health server in background
	healthServer := NewHealthServer(transformer, config.Service.HealthPort)
	go func() {
		if err := healthServer.Start(); err != nil {
			log.Printf("❌ Health server error: %v", err)
		}
	}()

	// Setup graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Start transformer in goroutine
	errChan := make(chan error, 1)
	go func() {
		errChan <- transformer.Start()
	}()

	// Wait for shutdown signal or error
	select {
	case <-sigChan:
		log.Println("\n📨 Received shutdown signal")
		transformer.Stop()
		log.Println("✅ Graceful shutdown complete")
	case err := <-errChan:
		if err != nil {
			log.Fatalf("❌ Transformer error: %v", err)
		}
	}
}
