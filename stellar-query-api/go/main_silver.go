package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

func mainWithSilver() {
	configPath := flag.String("config", "config.yaml", "Path to config file")
	flag.Parse()

	// Load configuration
	config, err := LoadConfig(*configPath)
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	log.Printf("Starting %s with Silver layer support", config.Service.Name)
	log.Printf("API server will listen on port %d", config.Service.Port)

	// Create hot reader (PostgreSQL)
	hotReader, err := NewHotReader(config.Postgres)
	if err != nil {
		log.Fatalf("Failed to create hot reader: %v", err)
	}
	defer hotReader.Close()
	log.Println("✅ Connected to PostgreSQL hot buffer")

	// Create cold reader (DuckLake Bronze)
	coldReader, err := NewColdReader(config.DuckLake)
	if err != nil {
		log.Fatalf("Failed to create cold reader: %v", err)
	}
	defer coldReader.Close()
	log.Println("✅ Connected to DuckLake Bronze (cold storage)")

	// Create query service for Bronze layer
	queryService := NewQueryService(hotReader, coldReader, config.Query)
	log.Println("✅ Query service initialized (Hot + Bronze)")

	// Create Silver reader if configured (hot + cold)
	var silverHandlers *SilverHandlers
	if config.DuckLakeSilver != nil && config.PostgresSilver != nil {
		// Create hot reader (PostgreSQL silver_hot)
		silverHotReader, err := NewSilverHotReader(*config.PostgresSilver)
		if err != nil {
			log.Fatalf("Failed to create Silver hot reader: %v", err)
		}
		defer silverHotReader.Close()
		log.Println("✅ Connected to PostgreSQL silver_hot (hot buffer)")

		// Create cold reader (DuckLake Silver)
		silverColdReader, err := NewSilverColdReader(*config.DuckLakeSilver)
		if err != nil {
			log.Fatalf("Failed to create Silver cold reader: %v", err)
		}
		defer silverColdReader.Close()
		log.Println("✅ Connected to DuckLake Silver (cold storage)")

		// Create unified reader
		unifiedSilverReader := NewUnifiedSilverReader(silverHotReader, silverColdReader)
		silverHandlers = NewSilverHandlers(unifiedSilverReader)
		log.Println("✅ Silver API handlers initialized (hot + cold)")
	} else {
		log.Println("⚠️  Silver layer not fully configured - Silver endpoints disabled")
		log.Println("     Requires both postgres_silver and ducklake_silver in config")
	}

	// Create Index Plane reader if configured
	var indexHandlers *IndexHandlers
	if config.Index != nil && config.Index.Enabled {
		indexReader, err := NewIndexReader(*config.Index)
		if err != nil {
			log.Printf("⚠️  Failed to create Index Plane reader: %v", err)
			log.Println("     Index Plane endpoints will be disabled")
		} else {
			defer indexReader.Close()
			log.Println("✅ Connected to Index Plane (fast transaction lookups)")
			indexHandlers = NewIndexHandlers(indexReader)
			log.Println("✅ Index Plane API handlers initialized")
		}
	} else {
		log.Println("ℹ️  Index Plane not configured - fast hash lookups disabled")
	}

	// Create Contract Event Index reader if configured
	var contractIndexHandlers *ContractIndexHandlers
	if config.ContractIndex != nil && config.ContractIndex.Enabled {
		contractIndexReader, err := NewContractIndexReader(*config.ContractIndex)
		if err != nil {
			log.Printf("⚠️  Failed to create Contract Event Index reader: %v", err)
			log.Println("     Contract Event Index endpoints will be disabled")
		} else {
			defer contractIndexReader.Close()
			log.Println("✅ Connected to Contract Event Index (fast contract event lookups)")
			contractIndexHandlers = NewContractIndexHandlers(contractIndexReader)
			log.Println("✅ Contract Event Index API handlers initialized")
		}
	} else {
		log.Println("ℹ️  Contract Event Index not configured - contract event lookups disabled")
	}

	// Create HTTP server
	mux := http.NewServeMux()

	// Health endpoint
	mux.HandleFunc("/health", handleHealthWithSilverAndIndexAndContractIndex(
		config.DuckLakeSilver != nil,
		config.Index != nil && config.Index.Enabled && indexHandlers != nil,
		config.ContractIndex != nil && config.ContractIndex.Enabled && contractIndexHandlers != nil,
	))

	// Bronze layer endpoints (existing)
	mux.HandleFunc("/ledgers", queryService.HandleLedgers)
	mux.HandleFunc("/transactions", queryService.HandleTransactions)
	mux.HandleFunc("/operations", queryService.HandleOperations)
	mux.HandleFunc("/effects", queryService.HandleEffects)
	mux.HandleFunc("/trades", queryService.HandleTrades)
	mux.HandleFunc("/accounts", queryService.HandleAccounts)
	mux.HandleFunc("/trustlines", queryService.HandleTrustlines)
	mux.HandleFunc("/offers", queryService.HandleOffers)
	mux.HandleFunc("/contract_events", queryService.HandleContractEvents)

	// Silver layer endpoints (if enabled)
	if silverHandlers != nil {
		log.Println("Registering Silver API endpoints:")

		// Account endpoints
		mux.HandleFunc("/api/v1/silver/accounts/current", silverHandlers.HandleAccountCurrent)
		mux.HandleFunc("/api/v1/silver/accounts/history", silverHandlers.HandleAccountHistory)
		mux.HandleFunc("/api/v1/silver/accounts/top", silverHandlers.HandleTopAccounts)
		log.Println("  ✓ /api/v1/silver/accounts/*")

		// Operations endpoints
		mux.HandleFunc("/api/v1/silver/operations/enriched", silverHandlers.HandleEnrichedOperations)
		mux.HandleFunc("/api/v1/silver/operations/soroban", silverHandlers.HandleSorobanOperations)
		mux.HandleFunc("/api/v1/silver/payments", silverHandlers.HandlePayments)
		log.Println("  ✓ /api/v1/silver/operations/*")
		log.Println("  ✓ /api/v1/silver/payments")

		// Transfer endpoints
		mux.HandleFunc("/api/v1/silver/transfers", silverHandlers.HandleTokenTransfers)
		mux.HandleFunc("/api/v1/silver/transfers/stats", silverHandlers.HandleTokenTransferStats)
		log.Println("  ✓ /api/v1/silver/transfers/*")

		// Block explorer specific endpoints
		mux.HandleFunc("/api/v1/silver/explorer/account", silverHandlers.HandleAccountOverview)
		mux.HandleFunc("/api/v1/silver/explorer/transaction", silverHandlers.HandleTransactionDetails)
		mux.HandleFunc("/api/v1/silver/explorer/asset", silverHandlers.HandleAssetOverview)
		log.Println("  ✓ /api/v1/silver/explorer/*")
	}

	// Index Plane endpoints (if enabled)
	if indexHandlers != nil {
		log.Println("Registering Index Plane API endpoints:")

		// Transaction hash lookup endpoints
		mux.HandleFunc("/transactions/", indexHandlers.HandleTransactionLookup)
		mux.HandleFunc("/api/v1/index/transactions/", indexHandlers.HandleTransactionLookup)
		mux.HandleFunc("/api/v1/index/transactions/lookup", indexHandlers.HandleBatchTransactionLookup)
		mux.HandleFunc("/api/v1/index/health", indexHandlers.HandleIndexHealth)
		log.Println("  ✓ /transactions/{hash} - Fast transaction hash lookup")
		log.Println("  ✓ /api/v1/index/transactions/{hash} - Fast transaction hash lookup")
		log.Println("  ✓ /api/v1/index/transactions/lookup - Batch hash lookup (POST)")
		log.Println("  ✓ /api/v1/index/health - Index coverage statistics")
	}

	// Contract Event Index endpoints (if enabled)
	if contractIndexHandlers != nil {
		log.Println("Registering Contract Event Index API endpoints:")

		// Contract event lookup endpoints
		mux.HandleFunc("/api/v1/index/contracts/health", contractIndexHandlers.HandleContractIndexHealth)
		mux.HandleFunc("/api/v1/index/contracts/lookup", contractIndexHandlers.HandleBatchContractLookup)

		// These need to be registered with a prefix handler pattern
		mux.HandleFunc("/api/v1/index/contracts/", func(w http.ResponseWriter, r *http.Request) {
			// Route to appropriate handler based on suffix
			if r.URL.Path == "/api/v1/index/contracts/lookup" {
				contractIndexHandlers.HandleBatchContractLookup(w, r)
			} else if r.URL.Path == "/api/v1/index/contracts/health" {
				contractIndexHandlers.HandleContractIndexHealth(w, r)
			} else if strings.HasSuffix(r.URL.Path, "/ledgers") {
				contractIndexHandlers.HandleContractLedgers(w, r)
			} else if strings.HasSuffix(r.URL.Path, "/summary") {
				contractIndexHandlers.HandleContractEventSummary(w, r)
			} else {
				respondError(w, "invalid endpoint", http.StatusNotFound)
			}
		})

		log.Println("  ✓ /api/v1/index/contracts/{contract_id}/ledgers - Get ledgers for contract")
		log.Println("  ✓ /api/v1/index/contracts/{contract_id}/summary - Get event summary")
		log.Println("  ✓ /api/v1/index/contracts/lookup - Batch contract lookup (POST)")
		log.Println("  ✓ /api/v1/index/contracts/health - Contract index statistics")
	}

	server := &http.Server{
		Addr:         fmt.Sprintf(":%d", config.Service.Port),
		Handler:      corsMiddleware(mux),
		ReadTimeout:  time.Duration(config.Service.ReadTimeoutSeconds) * time.Second,
		WriteTimeout: time.Duration(config.Service.WriteTimeoutSeconds) * time.Second,
	}

	// Start server in goroutine
	go func() {
		log.Printf("🚀 API server listening on :%d", config.Service.Port)
		if silverHandlers != nil {
			log.Printf("📊 Silver layer endpoints available at /api/v1/silver/*")
		}
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Server error: %v", err)
		}
	}()

	// Wait for interrupt signal
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("🛑 Shutting down server...")

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := server.Shutdown(ctx); err != nil {
		log.Printf("Server forced to shutdown: %v", err)
	}

	log.Println("✅ Server exited gracefully")
}

func handleHealthWithSilverAndIndexAndContractIndex(silverEnabled, indexEnabled, contractIndexEnabled bool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)

		response := map[string]interface{}{
			"status": "healthy",
			"layers": map[string]bool{
				"hot":            true,
				"bronze":         true,
				"silver":         silverEnabled,
				"index":          indexEnabled,
				"contract_index": contractIndexEnabled,
			},
		}

		json.NewEncoder(w).Encode(response)
	}
}

// corsMiddleware adds CORS headers for block explorer frontend
func corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Allow requests from block explorer frontend
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")

		// Handle preflight
		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusOK)
			return
		}

		next.ServeHTTP(w, r)
	})
}

func main() {
	mainWithSilver()
}
