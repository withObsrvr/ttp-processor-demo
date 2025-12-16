package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	configPath := flag.String("config", "config.yaml", "Path to config file")
	flag.Parse()

	// Load configuration
	config, err := LoadConfig(*configPath)
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	log.Printf("Starting %s", config.Service.Name)
	log.Printf("API server will listen on port %d", config.Service.Port)

	// Create hot reader (PostgreSQL)
	hotReader, err := NewHotReader(config.Postgres)
	if err != nil {
		log.Fatalf("Failed to create hot reader: %v", err)
	}
	defer hotReader.Close()
	log.Println("Connected to PostgreSQL hot buffer")

	// Create cold reader (DuckLake)
	coldReader, err := NewColdReader(config.DuckLake)
	if err != nil {
		log.Fatalf("Failed to create cold reader: %v", err)
	}
	defer coldReader.Close()
	log.Println("Connected to DuckLake cold storage")

	// Create query service
	queryService := NewQueryService(hotReader, coldReader, config.Query)
	log.Println("Query service initialized")

	// Create HTTP server
	mux := http.NewServeMux()

	// Register API endpoints
	mux.HandleFunc("/health", handleHealth)
	mux.HandleFunc("/ledgers", queryService.HandleLedgers)
	mux.HandleFunc("/transactions", queryService.HandleTransactions)
	mux.HandleFunc("/operations", queryService.HandleOperations)
	mux.HandleFunc("/effects", queryService.HandleEffects)
	mux.HandleFunc("/trades", queryService.HandleTrades)
	mux.HandleFunc("/accounts", queryService.HandleAccounts)
	mux.HandleFunc("/trustlines", queryService.HandleTrustlines)
	mux.HandleFunc("/offers", queryService.HandleOffers)
	mux.HandleFunc("/contract_events", queryService.HandleContractEvents)

	server := &http.Server{
		Addr:         fmt.Sprintf(":%d", config.Service.Port),
		Handler:      mux,
		ReadTimeout:  time.Duration(config.Service.ReadTimeoutSeconds) * time.Second,
		WriteTimeout: time.Duration(config.Service.WriteTimeoutSeconds) * time.Second,
	}

	// Start server in goroutine
	go func() {
		log.Printf("API server listening on :%d", config.Service.Port)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Server error: %v", err)
		}
	}()

	// Wait for interrupt signal
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("Shutting down server...")

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := server.Shutdown(ctx); err != nil {
		log.Printf("Server forced to shutdown: %v", err)
	}

	log.Println("Server exited")
}

func handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(`{"status":"healthy"}`))
}
