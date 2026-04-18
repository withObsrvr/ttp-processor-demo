package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gorilla/mux"
	httpSwagger "github.com/swaggo/http-swagger"
)

type application struct {
	config                *Config
	queryService          *QueryService
	silverHandlers        *SilverHandlers
	unifiedSilverReader   *UnifiedSilverReader
	unifiedDuckDBReader   *UnifiedDuckDBReader
	silverHotReader       *SilverHotReader
	hotReader             *HotReader
	coldReader            *ColdReader
	indexHandlers         *IndexHandlers
	indexReader           *IndexReader
	contractIndexHandlers *ContractIndexHandlers
	readerMode            ReaderMode
}

func (app *application) routes() http.Handler {
	router := mux.NewRouter()

	router.HandleFunc("/health", handleHealthWithSilverAndIndexAndContractIndex(
		app.config.DuckLakeSilver != nil,
		app.config.Index != nil && app.config.Index.Enabled && app.indexHandlers != nil,
		app.config.ContractIndex != nil && app.config.ContractIndex.Enabled && app.contractIndexHandlers != nil,
		app.readerMode,
		app.unifiedDuckDBReader,
	))

	router.PathPrefix("/swagger/").Handler(httpSwagger.Handler(
		httpSwagger.URL("/swagger/doc.json"),
		httpSwagger.DeepLinking(true),
		httpSwagger.DocExpansion("list"),
		httpSwagger.DomID("swagger-ui"),
	))
	log.Println("📖 Swagger UI available at /swagger/index.html")

	log.Println("Registering Bronze API endpoints:")
	router.HandleFunc("/api/v1/bronze/ledgers", app.queryService.HandleLedgers)
	router.HandleFunc("/api/v1/bronze/transactions", app.queryService.HandleTransactions)
	router.HandleFunc("/api/v1/bronze/operations", app.queryService.HandleOperations)
	router.HandleFunc("/api/v1/bronze/effects", app.queryService.HandleEffects)
	router.HandleFunc("/api/v1/bronze/trades", app.queryService.HandleTrades)
	router.HandleFunc("/api/v1/bronze/accounts", app.queryService.HandleAccounts)
	router.HandleFunc("/api/v1/bronze/trustlines", app.queryService.HandleTrustlines)
	router.HandleFunc("/api/v1/bronze/offers", app.queryService.HandleOffers)
	router.HandleFunc("/api/v1/bronze/contract_events", app.queryService.HandleContractEvents)
	log.Println("  ✓ /api/v1/bronze/ledgers")
	log.Println("  ✓ /api/v1/bronze/transactions")
	log.Println("  ✓ /api/v1/bronze/operations")
	log.Println("  ✓ /api/v1/bronze/effects")
	log.Println("  ✓ /api/v1/bronze/trades")
	log.Println("  ✓ /api/v1/bronze/accounts")
	log.Println("  ✓ /api/v1/bronze/trustlines")
	log.Println("  ✓ /api/v1/bronze/offers")
	log.Println("  ✓ /api/v1/bronze/contract_events")

	if app.silverHandlers != nil {
		app.registerSilverRoutes(router)
	}

	if app.indexHandlers != nil {
		log.Println("Registering Index Plane API endpoints:")
		router.HandleFunc("/transactions/{hash}", app.indexHandlers.HandleTransactionLookup).Methods("GET")
		router.HandleFunc("/api/v1/index/transactions/{hash}", app.indexHandlers.HandleTransactionLookup).Methods("GET")
		router.HandleFunc("/api/v1/index/transactions/lookup", app.indexHandlers.HandleBatchTransactionLookup).Methods("POST")
		router.HandleFunc("/api/v1/index/health", app.indexHandlers.HandleIndexHealth).Methods("GET")
		log.Println("  ✓ /transactions/{hash} - Fast transaction hash lookup")
		log.Println("  ✓ /api/v1/index/transactions/{hash} - Fast transaction hash lookup")
		log.Println("  ✓ /api/v1/index/transactions/lookup - Batch hash lookup (POST)")
		log.Println("  ✓ /api/v1/index/health - Index coverage statistics")
	}

	if app.contractIndexHandlers != nil {
		log.Println("Registering Contract Event Index API endpoints:")
		router.HandleFunc("/api/v1/index/contracts/health", app.contractIndexHandlers.HandleContractIndexHealth).Methods("GET")
		router.HandleFunc("/api/v1/index/contracts/lookup", app.contractIndexHandlers.HandleBatchContractLookup).Methods("POST")
		router.HandleFunc("/api/v1/index/contracts/{contract_id}/ledgers", app.contractIndexHandlers.HandleContractLedgers).Methods("GET")
		router.HandleFunc("/api/v1/index/contracts/{contract_id}/summary", app.contractIndexHandlers.HandleContractEventSummary).Methods("GET")
		log.Println("  ✓ /api/v1/index/contracts/{contract_id}/ledgers - Get ledgers for contract")
		log.Println("  ✓ /api/v1/index/contracts/{contract_id}/summary - Get event summary")
		log.Println("  ✓ /api/v1/index/contracts/lookup - Batch contract lookup (POST)")
		log.Println("  ✓ /api/v1/index/contracts/health - Contract index statistics")
	}

	return chainMiddleware(
		router,
		recoverPanicMiddleware,
		requestIDMiddleware,
		requestLoggingMiddleware,
		corsMiddleware,
	)
}

func (app *application) serve() error {
	server := &http.Server{
		Addr:         fmt.Sprintf(":%d", app.config.Service.Port),
		Handler:      app.routes(),
		ReadTimeout:  time.Duration(app.config.Service.ReadTimeoutSeconds) * time.Second,
		WriteTimeout: time.Duration(app.config.Service.WriteTimeoutSeconds) * time.Second,
	}

	go func() {
		log.Printf("🚀 API server listening on :%d", app.config.Service.Port)
		if app.silverHandlers != nil {
			log.Printf("📊 Silver layer endpoints available at /api/v1/silver/*")
			log.Printf("🥇 Gold layer endpoints available at /api/v1/gold/*")
			log.Printf("📋 Compliance endpoints available at /api/v1/gold/compliance/*")
		}
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Server error: %v", err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("🛑 Shutting down server...")

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := server.Shutdown(ctx); err != nil {
		log.Printf("Server forced to shutdown: %v", err)
		return err
	}

	log.Println("✅ Server exited gracefully")
	return nil
}

func handleHealthWithSilverAndIndexAndContractIndex(silverEnabled, indexEnabled, contractIndexEnabled bool, readerMode ReaderMode, unifiedReader *UnifiedDuckDBReader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		response := map[string]interface{}{
			"status": "healthy",
			"layers": map[string]bool{
				"hot":            true,
				"bronze":         true,
				"silver":         silverEnabled,
				"gold":           silverEnabled,
				"compliance":     silverEnabled,
				"index":          indexEnabled,
				"contract_index": contractIndexEnabled,
			},
			"reader_mode": string(readerMode),
		}

		if unifiedReader != nil {
			ctx := r.Context()
			if healthStatus, err := unifiedReader.HealthCheck(ctx); err == nil {
				response["unified_reader"] = healthStatus
			} else {
				response["unified_reader"] = map[string]string{"error": err.Error()}
			}
		}

		if err := writeJSON(w, http.StatusOK, response, nil); err != nil {
			writeError(w, http.StatusInternalServerError, "failed to encode health response")
		}
	}
}
