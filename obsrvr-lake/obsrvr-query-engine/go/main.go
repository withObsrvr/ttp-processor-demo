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
)

// corsMiddleware adds CORS headers to all responses.
func corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")

		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}

		next.ServeHTTP(w, r)
	})
}

func main() {
	// Determine config path
	configPath := "config.yaml"
	if len(os.Args) > 1 {
		configPath = os.Args[1]
	}

	cfg, err := LoadConfig(configPath)
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	log.Printf("Starting %s on port %d", cfg.Service.Name, cfg.Service.Port)

	// Initialize DuckLake reader
	reader, err := NewDuckLakeReader(cfg)
	if err != nil {
		log.Fatalf("Failed to initialize DuckLake reader: %v", err)
	}
	defer reader.Close()

	// Set up routes
	r := mux.NewRouter()
	r.Use(corsMiddleware)

	// ── Health ──────────────────────────────────────────────
	r.HandleFunc("/health", HealthHandler).Methods("GET", "OPTIONS")

	// ── Bronze Endpoints ──────────────────────────────────────
	r.HandleFunc("/api/v1/bronze/ledgers", LedgersHandler(reader)).Methods("GET", "OPTIONS")
	r.HandleFunc("/api/v1/bronze/transactions", TransactionsHandler(reader)).Methods("GET", "OPTIONS")
	r.HandleFunc("/api/v1/bronze/operations", OperationsHandler(reader)).Methods("GET", "OPTIONS")
	r.HandleFunc("/api/v1/bronze/effects", EffectsHandler(reader)).Methods("GET", "OPTIONS")
	r.HandleFunc("/api/v1/bronze/trades", BronzeTradesHandler(reader)).Methods("GET", "OPTIONS")
	r.HandleFunc("/api/v1/bronze/accounts", BronzeAccountsHandler(reader)).Methods("GET", "OPTIONS")
	r.HandleFunc("/api/v1/bronze/trustlines", BronzeTrustlinesHandler(reader)).Methods("GET", "OPTIONS")
	r.HandleFunc("/api/v1/bronze/offers", BronzeOffersHandler(reader)).Methods("GET", "OPTIONS")
	r.HandleFunc("/api/v1/bronze/contract_events", ContractEventsHandler(reader)).Methods("GET", "OPTIONS")
	r.HandleFunc("/api/v1/bronze/stats/network", BronzeNetworkStatsHandler(reader)).Methods("GET", "OPTIONS")

	// ── Silver - Accounts ──────────────────────────────────────
	r.HandleFunc("/api/v1/silver/accounts", ListAccountsHandler(reader)).Methods("GET", "OPTIONS")
	r.HandleFunc("/api/v1/silver/accounts/current", AccountCurrentHandler(reader)).Methods("GET", "OPTIONS")
	r.HandleFunc("/api/v1/silver/accounts/history", AccountHistoryHandler(reader)).Methods("GET", "OPTIONS")
	r.HandleFunc("/api/v1/silver/accounts/top", TopAccountsHandler(reader)).Methods("GET", "OPTIONS")
	r.HandleFunc("/api/v1/silver/accounts/signers", AccountSignersHandler(reader)).Methods("GET", "OPTIONS")
	r.HandleFunc("/api/v1/silver/accounts/{id}/balances", AccountBalancesHandler(reader)).Methods("GET", "OPTIONS")
	r.HandleFunc("/api/v1/silver/accounts/{id}/offers", AccountOffersHandler(reader)).Methods("GET", "OPTIONS")
	r.HandleFunc("/api/v1/silver/accounts/{id}/contracts", AccountContractsHandler(reader)).Methods("GET", "OPTIONS")
	r.HandleFunc("/api/v1/silver/accounts/{id}/activity", AccountActivityHandler(reader)).Methods("GET", "OPTIONS")

	// ── Silver - Assets ──────────────────────────────────────
	r.HandleFunc("/api/v1/silver/assets", AssetListHandler(reader)).Methods("GET", "OPTIONS")
	r.HandleFunc("/api/v1/silver/assets/{asset}/holders", TokenHoldersHandler(reader)).Methods("GET", "OPTIONS")
	r.HandleFunc("/api/v1/silver/assets/{asset}/stats", TokenStatsHandler(reader)).Methods("GET", "OPTIONS")

	// ── Silver - Operations ──────────────────────────────────
	r.HandleFunc("/api/v1/silver/operations/enriched", EnrichedOperationsHandler(reader)).Methods("GET", "OPTIONS")
	r.HandleFunc("/api/v1/silver/operations/soroban/by-function", SorobanOpsByFunctionHandler(reader)).Methods("GET", "OPTIONS")
	r.HandleFunc("/api/v1/silver/calls", SorobanOpsByFunctionHandler(reader)).Methods("GET", "OPTIONS")
	r.HandleFunc("/api/v1/silver/operations/soroban", SorobanOperationsHandler(reader)).Methods("GET", "OPTIONS")
	r.HandleFunc("/api/v1/silver/payments", PaymentsHandler(reader)).Methods("GET", "OPTIONS")

	// ── Silver - Transfers ──────────────────────────────────
	r.HandleFunc("/api/v1/silver/transfers", TokenTransfersHandler(reader)).Methods("GET", "OPTIONS")
	r.HandleFunc("/api/v1/silver/transfers/stats", TokenTransferStatsHandler(reader)).Methods("GET", "OPTIONS")

	// ── Silver - Explorer ──────────────────────────────────
	r.HandleFunc("/api/v1/silver/explorer/account", ExplorerAccountHandler(reader)).Methods("GET", "OPTIONS")
	r.HandleFunc("/api/v1/silver/explorer/transaction", ExplorerTransactionHandler(reader)).Methods("GET", "OPTIONS")
	r.HandleFunc("/api/v1/silver/explorer/asset", ExplorerAssetHandler(reader)).Methods("GET", "OPTIONS")

	// ── Silver - Offers ──────────────────────────────────────
	r.HandleFunc("/api/v1/silver/offers", SilverOffersHandler(reader)).Methods("GET", "OPTIONS")
	r.HandleFunc("/api/v1/silver/offers/pair", SilverOffersByPairHandler(reader)).Methods("GET", "OPTIONS")
	r.HandleFunc("/api/v1/silver/offers/{id}", SilverOfferByIDHandler(reader)).Methods("GET", "OPTIONS")

	// ── Silver - Liquidity Pools (from bronze snapshots) ──
	r.HandleFunc("/api/v1/silver/liquidity-pools", LiquidityPoolsHandler(reader)).Methods("GET", "OPTIONS")
	r.HandleFunc("/api/v1/silver/liquidity-pools/asset", LiquidityPoolsByAssetHandler(reader)).Methods("GET", "OPTIONS")
	r.HandleFunc("/api/v1/silver/liquidity-pools/{id}", LiquidityPoolByIDHandler(reader)).Methods("GET", "OPTIONS")

	// ── Silver - Claimable Balances ──────────────────────────
	r.HandleFunc("/api/v1/silver/claimable-balances", ClaimableBalancesHandler(reader)).Methods("GET", "OPTIONS")
	r.HandleFunc("/api/v1/silver/claimable-balances/asset", ClaimableBalancesByAssetHandler(reader)).Methods("GET", "OPTIONS")
	r.HandleFunc("/api/v1/silver/claimable-balances/{id}", ClaimableBalanceByIDHandler(reader)).Methods("GET", "OPTIONS")

	// ── Silver - Trades ──────────────────────────────────────
	r.HandleFunc("/api/v1/silver/trades", SilverTradesHandler(reader)).Methods("GET", "OPTIONS")
	r.HandleFunc("/api/v1/silver/trades/by-pair", SilverTradesByPairHandler(reader)).Methods("GET", "OPTIONS")
	r.HandleFunc("/api/v1/silver/trades/stats", SilverTradeStatsHandler(reader)).Methods("GET", "OPTIONS")

	// ── Silver - Effects ──────────────────────────────────────
	r.HandleFunc("/api/v1/silver/effects", SilverEffectsHandler(reader)).Methods("GET", "OPTIONS")
	r.HandleFunc("/api/v1/silver/effects/types", EffectTypesHandler(reader)).Methods("GET", "OPTIONS")
	r.HandleFunc("/api/v1/silver/effects/transaction/{tx_hash}", EffectsByTransactionHandler(reader)).Methods("GET", "OPTIONS")

	// ── Silver - Soroban (from bronze snapshot tables) ──────────
	r.HandleFunc("/api/v1/silver/soroban/contract-code", ContractCodeHandler(reader)).Methods("GET", "OPTIONS")
	r.HandleFunc("/api/v1/silver/soroban/ttl", TTLHandler(reader)).Methods("GET", "OPTIONS")
	r.HandleFunc("/api/v1/silver/soroban/ttl/expiring", TTLExpiringHandler(reader)).Methods("GET", "OPTIONS")
	r.HandleFunc("/api/v1/silver/soroban/ttl/expired", TTLExpiredHandler(reader)).Methods("GET", "OPTIONS")
	r.HandleFunc("/api/v1/silver/soroban/evicted-keys", EvictedKeysHandler(reader)).Methods("GET", "OPTIONS")
	r.HandleFunc("/api/v1/silver/soroban/restored-keys", RestoredKeysHandler(reader)).Methods("GET", "OPTIONS")
	r.HandleFunc("/api/v1/silver/soroban/config", SorobanConfigHandler(reader)).Methods("GET", "OPTIONS")
	r.HandleFunc("/api/v1/silver/soroban/config/limits", SorobanConfigHandler(reader)).Methods("GET", "OPTIONS")
	r.HandleFunc("/api/v1/silver/soroban/contract-data", ContractDataHandler(reader)).Methods("GET", "OPTIONS")
	r.HandleFunc("/api/v1/silver/soroban/contract-data/entry", ContractDataEntryHandler(reader)).Methods("GET", "OPTIONS")

	// ── Silver - Stats ──────────────────────────────────────
	r.HandleFunc("/api/v1/silver/stats/network", SilverNetworkStatsHandler(reader)).Methods("GET", "OPTIONS")
	r.HandleFunc("/api/v1/silver/stats/fees", FeeStatsHandler(reader)).Methods("GET", "OPTIONS")
	r.HandleFunc("/api/v1/silver/stats/soroban", SorobanStatsHandler(reader)).Methods("GET", "OPTIONS")
	r.HandleFunc("/api/v1/silver/data-boundaries", DataBoundariesHandler(reader)).Methods("GET", "OPTIONS")

	// ── Silver - Transaction Analysis ──────────────────────────
	// NOTE: batch/decoded must be before {hash} routes to avoid matching "batch" as a hash
	r.HandleFunc("/api/v1/silver/tx/batch/decoded", BatchDecodedTransactionsHandler(reader)).Methods("GET", "POST", "OPTIONS")
	r.HandleFunc("/api/v1/silver/tx/{hash}/contracts-involved", ContractsInvolvedHandler(reader)).Methods("GET", "OPTIONS")
	r.HandleFunc("/api/v1/silver/tx/{hash}/call-graph", CallGraphHandler(reader)).Methods("GET", "OPTIONS")
	r.HandleFunc("/api/v1/silver/tx/{hash}/hierarchy", TransactionHierarchyHandler(reader)).Methods("GET", "OPTIONS")
	r.HandleFunc("/api/v1/silver/tx/{hash}/contracts-summary", ContractsSummaryHandler(reader)).Methods("GET", "OPTIONS")
	r.HandleFunc("/api/v1/silver/tx/{hash}/diffs", TransactionDiffsHandler(reader)).Methods("GET", "OPTIONS")
	r.HandleFunc("/api/v1/silver/tx/{hash}/events", TransactionEventsHandler(reader)).Methods("GET", "OPTIONS")
	r.HandleFunc("/api/v1/silver/tx/{hash}/decoded", DecodedTransactionHandler(reader)).Methods("GET", "OPTIONS")
	r.HandleFunc("/api/v1/silver/tx/{hash}/full", FullTransactionHandler(reader)).Methods("GET", "OPTIONS")

	// ── Silver - Contracts ──────────────────────────────────
	r.HandleFunc("/api/v1/silver/contracts/top", TopContractsHandler(reader)).Methods("GET", "OPTIONS")
	r.HandleFunc("/api/v1/silver/contracts/{id}/recent-calls", ContractRecentCallsHandler(reader)).Methods("GET", "OPTIONS")
	r.HandleFunc("/api/v1/silver/contracts/{id}/callers", ContractCallersHandler(reader)).Methods("GET", "OPTIONS")
	r.HandleFunc("/api/v1/silver/contracts/{id}/callees", ContractCalleesHandler(reader)).Methods("GET", "OPTIONS")
	r.HandleFunc("/api/v1/silver/contracts/{id}/call-summary", ContractCallSummaryHandler(reader)).Methods("GET", "OPTIONS")
	r.HandleFunc("/api/v1/silver/contracts/{id}/analytics", ContractAnalyticsHandler(reader)).Methods("GET", "OPTIONS")
	r.HandleFunc("/api/v1/silver/contracts/{id}/metadata", ContractMetadataHandler(reader)).Methods("GET", "OPTIONS")
	r.HandleFunc("/api/v1/silver/contracts/{id}/storage", ContractStorageHandler(reader)).Methods("GET", "OPTIONS")
	r.HandleFunc("/api/v1/silver/contracts/{id}/interface", ContractInterfaceHandler(reader)).Methods("GET", "OPTIONS")

	// ── Silver - Advanced Stats (from bronze.transactions_row_v2) ──
	r.HandleFunc("/api/v1/silver/ledgers/{seq}/fees", LedgerFeesHandler(reader)).Methods("GET", "OPTIONS")
	r.HandleFunc("/api/v1/silver/ledgers/{seq}/soroban", LedgerSorobanHandler(reader)).Methods("GET", "OPTIONS")

	// ── Silver - Transaction Summaries ──────────────────────
	r.HandleFunc("/api/v1/silver/transactions/summaries", TransactionSummariesHandler(reader)).Methods("GET", "OPTIONS")

	// ── Silver - Generic Events (from bronze contract_events) ──
	r.HandleFunc("/api/v1/silver/events/generic", GenericEventsHandler(reader)).Methods("GET", "OPTIONS")
	r.HandleFunc("/api/v1/silver/events/contract/{contract_id}", ContractGenericEventsHandler(reader)).Methods("GET", "OPTIONS")

	// ── Silver - Search ──────────────────────────────────────
	r.HandleFunc("/api/v1/silver/search", SearchHandler(reader)).Methods("GET", "OPTIONS")

	// ── Silver - Prices (from bronze.trades_row_v1) ──────────
	r.HandleFunc("/api/v1/silver/prices/pairs", TradePairsHandler(reader)).Methods("GET", "OPTIONS")
	r.HandleFunc("/api/v1/silver/prices/{base}/{counter}/ohlc", OHLCCandlesHandler(reader)).Methods("GET", "OPTIONS")
	r.HandleFunc("/api/v1/silver/prices/{base}/{counter}/latest", LatestPriceHandler(reader)).Methods("GET", "OPTIONS")

	// ── Silver - Smart Wallet (SEP-50 detection) ──────────
	r.HandleFunc("/api/v1/silver/smart-wallet/{contract_id}", SmartWalletHandler(reader)).Methods("GET", "OPTIONS")

	// ── Silver - Unified Events (transfer/mint/burn from token_transfers_raw) ──
	r.HandleFunc("/api/v1/silver/events", UnifiedEventsHandler(reader)).Methods("GET", "OPTIONS")
	r.HandleFunc("/api/v1/silver/events/by-contract", UnifiedEventsByContractHandler(reader)).Methods("GET", "OPTIONS")
	r.HandleFunc("/api/v1/silver/address/{addr}/events", AddressEventsHandler(reader)).Methods("GET", "OPTIONS")

	// ── Silver - SEP-41 Tokens ──────────────────────────────
	r.HandleFunc("/api/v1/silver/tokens/{contract_id}/balances", SEP41TokenBalancesHandler(reader)).Methods("GET", "OPTIONS")
	r.HandleFunc("/api/v1/silver/tokens/{contract_id}/balance/{address}", SEP41SingleBalanceHandler(reader)).Methods("GET", "OPTIONS")
	r.HandleFunc("/api/v1/silver/tokens/{contract_id}/transfers", SEP41TokenTransfersHandler(reader)).Methods("GET", "OPTIONS")
	r.HandleFunc("/api/v1/silver/tokens/{contract_id}/stats", SEP41TokenStatsHandler(reader)).Methods("GET", "OPTIONS")
	r.HandleFunc("/api/v1/silver/tokens/{contract_id}", SEP41TokenMetadataHandler(reader)).Methods("GET", "OPTIONS")
	r.HandleFunc("/api/v1/silver/address/{addr}/token-balances", AddressTokenPortfolioHandler(reader)).Methods("GET", "OPTIONS")

	// ── Silver - Decode ──────────────────────────────────────
	r.HandleFunc("/api/v1/silver/decode/scval", DecodeScValHandler()).Methods("POST", "OPTIONS")

	// ── Gold - Snapshots (point-in-time from bronze snapshots) ──
	r.HandleFunc("/api/v1/gold/snapshots/account", GoldAccountSnapshotHandler(reader)).Methods("GET", "OPTIONS")
	r.HandleFunc("/api/v1/gold/snapshots/balance", GoldAssetHoldersHandler(reader)).Methods("GET", "OPTIONS")
	r.HandleFunc("/api/v1/gold/snapshots/portfolio", GoldPortfolioHandler(reader)).Methods("GET", "OPTIONS")
	r.HandleFunc("/api/v1/gold/snapshots/accounts/batch", GoldBatchAccountsHandler(reader)).Methods("GET", "POST", "OPTIONS")

	// ── Gold - Compliance ──────────────────────────────────────
	r.HandleFunc("/api/v1/gold/compliance/transactions", ComplianceTransactionsHandler(reader)).Methods("GET", "OPTIONS")
	r.HandleFunc("/api/v1/gold/compliance/balances", ComplianceBalancesHandler(reader)).Methods("GET", "OPTIONS")
	r.HandleFunc("/api/v1/gold/compliance/supply", ComplianceSupplyHandler(reader)).Methods("GET", "OPTIONS")
	r.HandleFunc("/api/v1/gold/compliance/archive", notImplementedHandler("gold/compliance/archive")).Methods("POST", "OPTIONS")
	r.HandleFunc("/api/v1/gold/compliance/archive/{id}", notImplementedHandler("gold/compliance/archive/status")).Methods("GET", "OPTIONS")
	r.HandleFunc("/api/v1/gold/compliance/archive/{id}/download/{artifact}", notImplementedHandler("gold/compliance/archive/download")).Methods("GET", "OPTIONS")
	r.HandleFunc("/api/v1/gold/compliance/lineage", notImplementedHandler("gold/compliance/lineage")).Methods("GET", "OPTIONS")

	// ── Semantic Layer ──────────────────────────────────────
	r.HandleFunc("/api/v1/semantic/activities", SemanticActivitiesHandler(reader)).Methods("GET", "OPTIONS")
	r.HandleFunc("/api/v1/semantic/contracts", SemanticContractsHandler(reader)).Methods("GET", "OPTIONS")
	r.HandleFunc("/api/v1/semantic/flows", SemanticFlowsHandler(reader)).Methods("GET", "OPTIONS")
	r.HandleFunc("/api/v1/semantic/contracts/functions", SemanticContractFunctionsHandler(reader)).Methods("GET", "OPTIONS")
	r.HandleFunc("/api/v1/semantic/assets", SemanticAssetsHandler(reader)).Methods("GET", "OPTIONS")
	r.HandleFunc("/api/v1/semantic/dex/pairs", SemanticDexPairsHandler(reader)).Methods("GET", "OPTIONS")
	r.HandleFunc("/api/v1/semantic/accounts/summary", SemanticAccountSummaryHandler(reader)).Methods("GET", "OPTIONS")
	r.HandleFunc("/api/v1/semantic/tokens/{contract_id}", SemanticTokenSummaryHandler(reader)).Methods("GET", "OPTIONS")

	// ── Index Plane (queries bronze directly) ──────────────
	r.HandleFunc("/transactions/{hash}", IndexTransactionLookupHandler(reader)).Methods("GET", "OPTIONS")
	r.HandleFunc("/api/v1/index/transactions/{hash}", IndexTransactionLookupHandler(reader)).Methods("GET", "OPTIONS")
	r.HandleFunc("/api/v1/index/transactions/lookup", IndexBatchTransactionLookupHandler(reader)).Methods("POST", "OPTIONS")
	r.HandleFunc("/api/v1/index/health", IndexHealthHandler(reader)).Methods("GET", "OPTIONS")

	// ── Contract Index (queries bronze contract_events directly) ──
	r.HandleFunc("/api/v1/index/contracts/health", ContractIndexHealthHandler(reader)).Methods("GET", "OPTIONS")
	r.HandleFunc("/api/v1/index/contracts/lookup", ContractIndexLookupHandler(reader)).Methods("POST", "OPTIONS")
	r.HandleFunc("/api/v1/index/contracts/{contract_id}/ledgers", ContractIndexLedgersHandler(reader)).Methods("GET", "OPTIONS")
	r.HandleFunc("/api/v1/index/contracts/{contract_id}/summary", ContractIndexSummaryHandler(reader)).Methods("GET", "OPTIONS")

	// HTTP server
	srv := &http.Server{
		Addr:         fmt.Sprintf(":%d", cfg.Service.Port),
		Handler:      r,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 60 * time.Second,
		IdleTimeout:  120 * time.Second,
	}

	// Graceful shutdown
	go func() {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
		<-sigCh

		log.Println("Shutting down...")
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		if err := srv.Shutdown(ctx); err != nil {
			log.Printf("HTTP server shutdown error: %v", err)
		}
	}()

	log.Printf("Listening on %s with %d registered routes", srv.Addr, countRoutes(r))
	if err := srv.ListenAndServe(); err != http.ErrServerClosed {
		log.Fatalf("HTTP server error: %v", err)
	}

	log.Println("Server stopped")
}

// notImplementedHandler returns a handler that returns a 501 status for endpoints
// that need data sources not yet available in DuckLake cold storage.
func notImplementedHandler(name string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		respondJSON(w, http.StatusNotImplemented, map[string]interface{}{
			"error":    "not yet implemented in query engine",
			"endpoint": name,
			"message":  "This endpoint requires data sources not yet available in DuckLake cold storage. Use stellar-query-api for this endpoint.",
		})
	}
}

// countRoutes counts the number of registered routes.
func countRoutes(r *mux.Router) int {
	count := 0
	r.Walk(func(route *mux.Route, router *mux.Router, ancestors []*mux.Route) error {
		count++
		return nil
	})
	return count
}
