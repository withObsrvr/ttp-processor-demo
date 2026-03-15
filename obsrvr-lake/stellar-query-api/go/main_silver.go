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
	"syscall"
	"time"

	"github.com/gorilla/mux"
	httpSwagger "github.com/swaggo/http-swagger"

	_ "github.com/withobsrvr/stellar-query-api/docs" // swagger docs
)

// @title Stellar Query API
// @version 1.0
// @description API for querying Stellar blockchain data across Bronze, Silver, and Gold data layers.
// @description
// @description ## Data Layers
// @description - **Bronze**: Raw blockchain data (ledgers, transactions, operations, effects)
// @description - **Silver**: Enriched and processed data (accounts, assets, transfers, trades)
// @description - **Gold**: Analytical views (snapshots, compliance reports)
// @description - **Index Plane**: Fast lookup services (transaction hash, contract events)
// @description
// @description ## Amount Formatting
// @description All amounts are returned in decimal format with 7 decimal places (Stellar stroops conversion).
// @description Example: Raw stroops `10000000` is formatted as `"1.0000000"`

// @contact.name OBSRVR Team
// @contact.url https://obsrvr.com
// @contact.email support@obsrvr.com

// @license.name Apache 2.0
// @license.url https://www.apache.org/licenses/LICENSE-2.0.html

// @host gateway.withobsrvr.com
// @BasePath /
// @schemes https

// @securityDefinitions.apikey ApiKeyAuth
// @in header
// @name Authorization
// @description API Key authentication. Format: "Api-Key YOUR_API_KEY"

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
	var unifiedSilverReader *UnifiedSilverReader
	var unifiedDuckDBReader *UnifiedDuckDBReader
	readerMode := config.Query.ReaderMode
	if readerMode == "" {
		readerMode = ReaderModeLegacy // Default to legacy for backward compatibility
	}

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

		// Create legacy unified reader (always needed for legacy/hybrid modes)
		unifiedSilverReader = NewUnifiedSilverReader(silverHotReader, silverColdReader)
		log.Println("✅ Legacy UnifiedSilverReader initialized (Go-layer merge)")

		// Create new unified DuckDB reader if configured and mode requires it
		if (readerMode == ReaderModeUnified || readerMode == ReaderModeHybrid) && config.Unified != nil {
			var err error
			unifiedDuckDBReader, err = NewUnifiedDuckDBReader(*config.Unified)
			if err != nil {
				if readerMode == ReaderModeUnified {
					log.Fatalf("Failed to create UnifiedDuckDBReader (required for unified mode): %v", err)
				}
				log.Printf("⚠️  Failed to create UnifiedDuckDBReader, falling back to legacy: %v", err)
				readerMode = ReaderModeLegacy
			} else {
				defer unifiedDuckDBReader.Close()
				log.Println("✅ UnifiedDuckDBReader initialized (DuckDB ATTACH mode)")
			}
		}

		// Create handlers based on reader mode
		silverHandlers = NewSilverHandlers(unifiedSilverReader, unifiedDuckDBReader, readerMode)
		log.Printf("✅ Silver API handlers initialized (reader_mode: %s)", readerMode)
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

	// Create HTTP server with gorilla/mux for path parameter support
	router := mux.NewRouter()

	// Health endpoint
	router.HandleFunc("/health", handleHealthWithSilverAndIndexAndContractIndex(
		config.DuckLakeSilver != nil,
		config.Index != nil && config.Index.Enabled && indexHandlers != nil,
		config.ContractIndex != nil && config.ContractIndex.Enabled && contractIndexHandlers != nil,
		readerMode,
		unifiedDuckDBReader,
	))

	// Swagger UI endpoint
	router.PathPrefix("/swagger/").Handler(httpSwagger.Handler(
		httpSwagger.URL("/swagger/doc.json"),
		httpSwagger.DeepLinking(true),
		httpSwagger.DocExpansion("list"),
		httpSwagger.DomID("swagger-ui"),
	))
	log.Println("📖 Swagger UI available at /swagger/index.html")

	// Bronze layer endpoints - /api/v1/bronze/*
	log.Println("Registering Bronze API endpoints:")
	router.HandleFunc("/api/v1/bronze/ledgers", queryService.HandleLedgers)
	router.HandleFunc("/api/v1/bronze/transactions", queryService.HandleTransactions)
	router.HandleFunc("/api/v1/bronze/operations", queryService.HandleOperations)
	router.HandleFunc("/api/v1/bronze/effects", queryService.HandleEffects)
	router.HandleFunc("/api/v1/bronze/trades", queryService.HandleTrades)
	router.HandleFunc("/api/v1/bronze/accounts", queryService.HandleAccounts)
	router.HandleFunc("/api/v1/bronze/trustlines", queryService.HandleTrustlines)
	router.HandleFunc("/api/v1/bronze/offers", queryService.HandleOffers)
	router.HandleFunc("/api/v1/bronze/contract_events", queryService.HandleContractEvents)
	log.Println("  ✓ /api/v1/bronze/ledgers")
	log.Println("  ✓ /api/v1/bronze/transactions")
	log.Println("  ✓ /api/v1/bronze/operations")
	log.Println("  ✓ /api/v1/bronze/effects")
	log.Println("  ✓ /api/v1/bronze/trades")
	log.Println("  ✓ /api/v1/bronze/accounts")
	log.Println("  ✓ /api/v1/bronze/trustlines")
	log.Println("  ✓ /api/v1/bronze/offers")
	log.Println("  ✓ /api/v1/bronze/contract_events")

	// Silver layer endpoints (if enabled)
	if silverHandlers != nil {
		log.Println("Registering Silver API endpoints:")

		// Account endpoints
		router.HandleFunc("/api/v1/silver/accounts", silverHandlers.HandleListAccounts)
		router.HandleFunc("/api/v1/silver/accounts/current", silverHandlers.HandleAccountCurrent)
		router.HandleFunc("/api/v1/silver/accounts/history", silverHandlers.HandleAccountHistory)
		router.HandleFunc("/api/v1/silver/accounts/top", silverHandlers.HandleTopAccounts)
		router.HandleFunc("/api/v1/silver/accounts/signers", silverHandlers.HandleAccountSigners)
		router.HandleFunc("/api/v1/silver/accounts/{id}/balances", silverHandlers.HandleAccountBalances).Methods("GET")
		router.HandleFunc("/api/v1/silver/accounts/{id}/offers", silverHandlers.HandleAccountOffers).Methods("GET")
		log.Println("  ✓ /api/v1/silver/accounts (list all)")
		log.Println("  ✓ /api/v1/silver/accounts/*")
		log.Println("  ✓ /api/v1/silver/accounts/signers")
		log.Println("  ✓ /api/v1/silver/accounts/{id}/balances")
		log.Println("  ✓ /api/v1/silver/accounts/{id}/offers")

		// Token/Asset endpoints
		// IMPORTANT: /assets must be registered BEFORE /assets/{asset}/* to avoid path matching issues
		router.HandleFunc("/api/v1/silver/assets", silverHandlers.HandleAssetList).Methods("GET")
		router.HandleFunc("/api/v1/silver/assets/{asset}/holders", silverHandlers.HandleTokenHolders).Methods("GET")
		router.HandleFunc("/api/v1/silver/assets/{asset}/stats", silverHandlers.HandleTokenStats).Methods("GET")
		log.Println("  ✓ /api/v1/silver/assets (list all assets)")
		log.Println("  ✓ /api/v1/silver/assets/{asset}/holders")
		log.Println("  ✓ /api/v1/silver/assets/{asset}/stats")

		// Operations endpoints
		router.HandleFunc("/api/v1/silver/operations/enriched", silverHandlers.HandleEnrichedOperations)
		router.HandleFunc("/api/v1/silver/operations/soroban/by-function", silverHandlers.HandleSorobanOpsByFunction).Methods("GET")
		router.HandleFunc("/api/v1/silver/calls", silverHandlers.HandleSorobanOpsByFunction).Methods("GET")
		router.HandleFunc("/api/v1/silver/operations/soroban", silverHandlers.HandleSorobanOperations)
		router.HandleFunc("/api/v1/silver/payments", silverHandlers.HandlePayments)
		log.Println("  ✓ /api/v1/silver/operations/*")
		log.Println("  ✓ /api/v1/silver/operations/soroban/by-function (filter by contract/function)")
		log.Println("  ✓ /api/v1/silver/calls (alias for by-function)")
		log.Println("  ✓ /api/v1/silver/payments")

		// Transfer endpoints
		router.HandleFunc("/api/v1/silver/transfers", silverHandlers.HandleTokenTransfers)
		router.HandleFunc("/api/v1/silver/transfers/stats", silverHandlers.HandleTokenTransferStats)
		log.Println("  ✓ /api/v1/silver/transfers/*")

		// Block explorer specific endpoints
		router.HandleFunc("/api/v1/silver/explorer/account", silverHandlers.HandleAccountOverview)
		router.HandleFunc("/api/v1/silver/explorer/transaction", silverHandlers.HandleTransactionDetails)
		router.HandleFunc("/api/v1/silver/explorer/asset", silverHandlers.HandleAssetOverview)
		log.Println("  ✓ /api/v1/silver/explorer/*")

		// Phase 6: State Table Endpoints - Offers
		router.HandleFunc("/api/v1/silver/offers", silverHandlers.HandleOffers).Methods("GET")
		router.HandleFunc("/api/v1/silver/offers/pair", silverHandlers.HandleOffersByPair).Methods("GET")
		router.HandleFunc("/api/v1/silver/offers/{id}", silverHandlers.HandleOfferByID).Methods("GET")
		log.Println("  ✓ /api/v1/silver/offers (list, filter by seller)")
		log.Println("  ✓ /api/v1/silver/offers/pair (filter by trading pair)")
		log.Println("  ✓ /api/v1/silver/offers/{id} (single lookup)")

		// Phase 6: State Table Endpoints - Liquidity Pools
		router.HandleFunc("/api/v1/silver/liquidity-pools", silverHandlers.HandleLiquidityPools).Methods("GET")
		router.HandleFunc("/api/v1/silver/liquidity-pools/asset", silverHandlers.HandleLiquidityPoolsByAsset).Methods("GET")
		router.HandleFunc("/api/v1/silver/liquidity-pools/{id}", silverHandlers.HandleLiquidityPoolByID).Methods("GET")
		log.Println("  ✓ /api/v1/silver/liquidity-pools (list)")
		log.Println("  ✓ /api/v1/silver/liquidity-pools/asset (filter by asset)")
		log.Println("  ✓ /api/v1/silver/liquidity-pools/{id} (single lookup)")

		// Phase 6: State Table Endpoints - Claimable Balances
		router.HandleFunc("/api/v1/silver/claimable-balances", silverHandlers.HandleClaimableBalances).Methods("GET")
		router.HandleFunc("/api/v1/silver/claimable-balances/asset", silverHandlers.HandleClaimableBalancesByAsset).Methods("GET")
		router.HandleFunc("/api/v1/silver/claimable-balances/{id}", silverHandlers.HandleClaimableBalanceByID).Methods("GET")
		log.Println("  ✓ /api/v1/silver/claimable-balances (list, filter by sponsor)")
		log.Println("  ✓ /api/v1/silver/claimable-balances/asset (filter by asset)")
		log.Println("  ✓ /api/v1/silver/claimable-balances/{id} (single lookup)")

		// Phase 7: Event Table Endpoints - Trades
		router.HandleFunc("/api/v1/silver/trades", silverHandlers.HandleTrades).Methods("GET")
		router.HandleFunc("/api/v1/silver/trades/by-pair", silverHandlers.HandleTradesByPair).Methods("GET")
		router.HandleFunc("/api/v1/silver/trades/stats", silverHandlers.HandleTradeStats).Methods("GET")
		log.Println("  ✓ /api/v1/silver/trades (list, filter by account/time)")
		log.Println("  ✓ /api/v1/silver/trades/by-pair (filter by asset pair)")
		log.Println("  ✓ /api/v1/silver/trades/stats (aggregated statistics)")

		// Phase 7: Event Table Endpoints - Effects
		router.HandleFunc("/api/v1/silver/effects", silverHandlers.HandleEffects).Methods("GET")
		router.HandleFunc("/api/v1/silver/effects/types", silverHandlers.HandleEffectTypes).Methods("GET")
		router.HandleFunc("/api/v1/silver/effects/transaction/{tx_hash}", silverHandlers.HandleEffectsByTransaction).Methods("GET")
		log.Println("  ✓ /api/v1/silver/effects (list, filter by account/type/time)")
		log.Println("  ✓ /api/v1/silver/effects/types (effect type counts)")
		log.Println("  ✓ /api/v1/silver/effects/transaction/{tx_hash} (effects by transaction)")

		// Phase 8: Soroban Table Endpoints - Contract Code
		router.HandleFunc("/api/v1/silver/soroban/contract-code", silverHandlers.HandleContractCode).Methods("GET")
		log.Println("  ✓ /api/v1/silver/soroban/contract-code (WASM metadata lookup)")

		// Phase 8: Soroban Table Endpoints - TTL
		router.HandleFunc("/api/v1/silver/soroban/ttl", silverHandlers.HandleTTL).Methods("GET")
		router.HandleFunc("/api/v1/silver/soroban/ttl/expiring", silverHandlers.HandleTTLExpiring).Methods("GET")
		router.HandleFunc("/api/v1/silver/soroban/ttl/expired", silverHandlers.HandleTTLExpired).Methods("GET")
		log.Println("  ✓ /api/v1/silver/soroban/ttl (single TTL lookup)")
		log.Println("  ✓ /api/v1/silver/soroban/ttl/expiring (entries expiring within N ledgers)")
		log.Println("  ✓ /api/v1/silver/soroban/ttl/expired (already expired entries)")

		// Phase 8: Soroban Table Endpoints - Evictions/Restorations
		router.HandleFunc("/api/v1/silver/soroban/evicted-keys", silverHandlers.HandleEvictedKeys).Methods("GET")
		router.HandleFunc("/api/v1/silver/soroban/restored-keys", silverHandlers.HandleRestoredKeys).Methods("GET")
		log.Println("  ✓ /api/v1/silver/soroban/evicted-keys (eviction events)")
		log.Println("  ✓ /api/v1/silver/soroban/restored-keys (restoration events)")

		// Phase 8: Soroban Table Endpoints - Config
		router.HandleFunc("/api/v1/silver/soroban/config", silverHandlers.HandleSorobanConfig).Methods("GET")
		router.HandleFunc("/api/v1/silver/soroban/config/limits", silverHandlers.HandleSorobanConfigLimits).Methods("GET")
		log.Println("  ✓ /api/v1/silver/soroban/config (network configuration)")
		log.Println("  ✓ /api/v1/silver/soroban/config/limits (simplified limits view)")

		// Phase 8: Soroban Table Endpoints - Contract Data
		router.HandleFunc("/api/v1/silver/soroban/contract-data", silverHandlers.HandleContractData).Methods("GET")
		router.HandleFunc("/api/v1/silver/soroban/contract-data/entry", silverHandlers.HandleContractDataEntry).Methods("GET")
		log.Println("  ✓ /api/v1/silver/soroban/contract-data (contract storage)")
		log.Println("  ✓ /api/v1/silver/soroban/contract-data/entry (single entry lookup)")

		// Network statistics endpoint (Phase 2)
		// Pass Bronze coldReader for accurate total account count
		networkStatsHandler := NewNetworkStatsHandler(unifiedSilverReader, coldReader)
		if unifiedDuckDBReader != nil {
			networkStatsHandler.SetUnifiedReader(unifiedDuckDBReader)
			router.HandleFunc("/api/v1/bronze/stats/network", networkStatsHandler.HandleBronzeNetworkStats).Methods("GET")
			log.Println("  ✓ /api/v1/bronze/stats/network (bronze headline statistics)")
		}
		router.HandleFunc("/api/v1/silver/stats/network", networkStatsHandler.HandleNetworkStats).Methods("GET")
		log.Println("  ✓ /api/v1/silver/stats/network (headline statistics)")

		// Data boundaries endpoint (RPC v2 compatibility)
		router.HandleFunc("/api/v1/silver/data-boundaries", silverHandlers.HandleDataBoundaries).Methods("GET")
		log.Println("  ✓ /api/v1/silver/data-boundaries (available ledger range)")

		// Account activity endpoint (Phase 3)
		accountActivityHandler := NewAccountActivityHandler(unifiedSilverReader)
		router.HandleFunc("/api/v1/silver/accounts/{id}/activity", accountActivityHandler.HandleAccountActivity).Methods("GET")
		log.Println("  ✓ /api/v1/silver/accounts/{id}/activity (unified timeline)")

		// Contract call endpoints (Freighter "Contracts Involved" feature)
		var contractCallHandlers *ContractCallHandlers
		if unifiedDuckDBReader != nil {
			contractCallHandlers = NewContractCallHandlersWithUnified(unifiedSilverReader, unifiedDuckDBReader)
		} else {
			contractCallHandlers = NewContractCallHandlers(unifiedSilverReader)
		}

		// Transaction-centric endpoints with path parameters
		router.HandleFunc("/api/v1/silver/tx/{hash}/contracts-involved", contractCallHandlers.HandleContractsInvolved).Methods("GET")
		router.HandleFunc("/api/v1/silver/tx/{hash}/call-graph", contractCallHandlers.HandleCallGraph).Methods("GET")
		router.HandleFunc("/api/v1/silver/tx/{hash}/hierarchy", contractCallHandlers.HandleTransactionHierarchy).Methods("GET")
		router.HandleFunc("/api/v1/silver/tx/{hash}/contracts-summary", contractCallHandlers.HandleContractsSummary).Methods("GET")
		log.Println("  ✓ /api/v1/silver/tx/{hash}/contracts-involved")
		log.Println("  ✓ /api/v1/silver/tx/{hash}/call-graph")
		log.Println("  ✓ /api/v1/silver/tx/{hash}/hierarchy")
		log.Println("  ✓ /api/v1/silver/tx/{hash}/contracts-summary (wallet-friendly)")

		// Contract analytics endpoints (Phase 1)
		router.HandleFunc("/api/v1/silver/contracts/top", contractCallHandlers.HandleTopContracts).Methods("GET")
		log.Println("  ✓ /api/v1/silver/contracts/top (top active contracts)")

		// Contract-centric endpoints with path parameters
		router.HandleFunc("/api/v1/silver/contracts/{id}/recent-calls", contractCallHandlers.HandleRecentCalls).Methods("GET")
		router.HandleFunc("/api/v1/silver/contracts/{id}/callers", contractCallHandlers.HandleContractCallers).Methods("GET")
		router.HandleFunc("/api/v1/silver/contracts/{id}/callees", contractCallHandlers.HandleContractCallees).Methods("GET")
		router.HandleFunc("/api/v1/silver/contracts/{id}/call-summary", contractCallHandlers.HandleContractCallSummary).Methods("GET")
		router.HandleFunc("/api/v1/silver/contracts/{id}/analytics", contractCallHandlers.HandleContractAnalyticsSummary).Methods("GET")
		log.Println("  ✓ /api/v1/silver/contracts/{id}/recent-calls")
		log.Println("  ✓ /api/v1/silver/contracts/{id}/callers")
		log.Println("  ✓ /api/v1/silver/contracts/{id}/callees")
		log.Println("  ✓ /api/v1/silver/contracts/{id}/call-summary")
		log.Println("  ✓ /api/v1/silver/contracts/{id}/analytics (comprehensive analytics)")

		// Contract metadata endpoint (uses silver hot reader)
		router.HandleFunc("/api/v1/silver/contracts/{id}/metadata", contractCallHandlers.HandleContractMetadata).Methods("GET")
		log.Println("  ✓ /api/v1/silver/contracts/{id}/metadata (contract creator, WASM, storage)")

		// Phase B: New endpoints (require unified reader for contract storage, tx summaries, fees)
		if unifiedDuckDBReader != nil {
			// Fee statistics endpoints
			feeStatsHandler := NewFeeStatsHandler(unifiedDuckDBReader)
			router.HandleFunc("/api/v1/silver/stats/fees", feeStatsHandler.HandleFeeStats).Methods("GET")
			router.HandleFunc("/api/v1/silver/ledgers/{seq}/fees", feeStatsHandler.HandleLedgerFees).Methods("GET")
			log.Println("  ✓ /api/v1/silver/stats/fees (fee percentiles)")
			log.Println("  ✓ /api/v1/silver/ledgers/{seq}/fees (per-ledger fee histogram)")

			// Contract storage endpoint
			router.HandleFunc("/api/v1/silver/contracts/{id}/storage", silverHandlers.HandleContractStorage).Methods("GET")
			log.Println("  ✓ /api/v1/silver/contracts/{id}/storage (contract data entries with TTL)")

			// Soroban stats endpoint
			sorobanStatsHandler := NewSorobanStatsHandler(unifiedDuckDBReader, unifiedSilverReader.hot)
			router.HandleFunc("/api/v1/silver/stats/soroban", sorobanStatsHandler.HandleSorobanStats).Methods("GET")
			log.Println("  ✓ /api/v1/silver/stats/soroban (Soroban network statistics)")

			// Transaction summaries endpoint
			router.HandleFunc("/api/v1/silver/transactions/summaries", silverHandlers.HandleTransactionSummaries).Methods("GET")
			log.Println("  ✓ /api/v1/silver/transactions/summaries (batch tx summaries)")
		}

		// Prism Block Explorer endpoints (require unified reader)
		if unifiedDuckDBReader != nil {
			// Feature 1: Generic CAP-67 Events (from bronze)
			genericEventHandlers := NewGenericEventHandlers(unifiedDuckDBReader)
			router.HandleFunc("/api/v1/silver/events/generic", genericEventHandlers.HandleGenericEvents).Methods("GET")
			router.HandleFunc("/api/v1/silver/events/contract/{contract_id}", genericEventHandlers.HandleContractGenericEvents).Methods("GET")
			log.Println("  ✓ /api/v1/silver/events/generic (all contract events from bronze)")
			log.Println("  ✓ /api/v1/silver/events/contract/{contract_id} (contract events)")

			// Feature 2: Unified Search
			searchHandlers := NewSearchHandlers(unifiedDuckDBReader)
			router.HandleFunc("/api/v1/silver/search", searchHandlers.HandleSearch).Methods("GET")
			log.Println("  ✓ /api/v1/silver/search (unified search)")

			// Feature 4: Transaction Diffs (balance/state changes)
			txDiffHandlers := NewTxDiffHandlers(unifiedDuckDBReader)
			router.HandleFunc("/api/v1/silver/tx/{hash}/diffs", txDiffHandlers.HandleTransactionDiffs).Methods("GET")
			log.Println("  ✓ /api/v1/silver/tx/{hash}/diffs (balance/state changes)")

			// Feature 3: Price Data (OHLC & Latest)
			priceHandlers := NewPriceHandlers(unifiedDuckDBReader)
			router.HandleFunc("/api/v1/silver/prices/pairs", priceHandlers.HandleTradePairs).Methods("GET")
			router.HandleFunc("/api/v1/silver/prices/{base}/{counter}/ohlc", priceHandlers.HandleOHLCCandles).Methods("GET")
			router.HandleFunc("/api/v1/silver/prices/{base}/{counter}/latest", priceHandlers.HandleLatestPrice).Methods("GET")
			log.Println("  ✓ /api/v1/silver/prices/pairs (available trading pairs)")
			log.Println("  ✓ /api/v1/silver/prices/{base}/{counter}/ohlc (OHLC candles)")
			log.Println("  ✓ /api/v1/silver/prices/{base}/{counter}/latest (latest price)")

			// Feature 5: Smart Wallet Detection (SEP-50)
			smartWalletHandlers := NewSmartWalletHandlers(unifiedDuckDBReader)
			router.HandleFunc("/api/v1/silver/smart-wallet/{contract_id}", smartWalletHandlers.HandleSmartWalletInfo).Methods("GET")
			log.Println("  ✓ /api/v1/silver/smart-wallet/{contract_id} (SEP-50 detection)")
		}

		// CAP-67 Unified Event Stream + SEP-41 Token endpoints
		if unifiedDuckDBReader != nil {
			eventHandlers := NewEventHandlers(unifiedDuckDBReader)
			log.Println("Registering CAP-67 Event Stream endpoints:")

			router.HandleFunc("/api/v1/silver/events", eventHandlers.HandleUnifiedEvents).Methods("GET")
			router.HandleFunc("/api/v1/silver/events/by-contract", eventHandlers.HandleContractEvents).Methods("GET")
			router.HandleFunc("/api/v1/silver/address/{addr}/events", eventHandlers.HandleAddressEvents).Methods("GET")
			router.HandleFunc("/api/v1/silver/tx/{hash}/events", eventHandlers.HandleTransactionEvents).Methods("GET")
			log.Println("  ✓ /api/v1/silver/events (unified CAP-67 event stream)")
			log.Println("  ✓ /api/v1/silver/events/by-contract (filter by contract)")
			log.Println("  ✓ /api/v1/silver/address/{addr}/events (address events)")
			log.Println("  ✓ /api/v1/silver/tx/{hash}/events (transaction events)")

			// SEP-41 Token API endpoints
			sep41Handlers := NewSEP41Handlers(unifiedDuckDBReader)
			log.Println("Registering SEP-41 Token API endpoints:")

			router.HandleFunc("/api/v1/silver/tokens/{contract_id}/balances", sep41Handlers.HandleTokenBalances).Methods("GET")
			router.HandleFunc("/api/v1/silver/tokens/{contract_id}/balance/{address}", sep41Handlers.HandleSingleBalance).Methods("GET")
			router.HandleFunc("/api/v1/silver/tokens/{contract_id}/transfers", sep41Handlers.HandleTokenTransfers).Methods("GET")
			router.HandleFunc("/api/v1/silver/tokens/{contract_id}/stats", sep41Handlers.HandleTokenStats).Methods("GET")
			router.HandleFunc("/api/v1/silver/tokens/{contract_id}", sep41Handlers.HandleTokenMetadata).Methods("GET")
			router.HandleFunc("/api/v1/silver/address/{addr}/token-balances", sep41Handlers.HandleAddressTokenPortfolio).Methods("GET")
			log.Println("  ✓ /api/v1/silver/tokens/{contract_id} (SEP-41 metadata)")
			log.Println("  ✓ /api/v1/silver/tokens/{contract_id}/balances (token holders)")
			log.Println("  ✓ /api/v1/silver/tokens/{contract_id}/balance/{address} (single balance)")
			log.Println("  ✓ /api/v1/silver/tokens/{contract_id}/transfers (token transfers)")
			log.Println("  ✓ /api/v1/silver/tokens/{contract_id}/stats (token statistics)")
			log.Println("  ✓ /api/v1/silver/address/{addr}/token-balances (address portfolio)")

			// Transaction Decode + Human-Readable Summary endpoints
			decodeHandlers := NewDecodeHandlers(unifiedDuckDBReader, unifiedSilverReader)
			log.Println("Registering Transaction Decode endpoints:")

			router.HandleFunc("/api/v1/silver/tx/{hash}/decoded", decodeHandlers.HandleDecodedTransaction).Methods("GET")
			router.HandleFunc("/api/v1/silver/tx/{hash}/full", decodeHandlers.HandleFullTransaction).Methods("GET")
			router.HandleFunc("/api/v1/silver/contracts/{id}/interface", decodeHandlers.HandleContractInterface).Methods("GET")
			router.HandleFunc("/api/v1/silver/decode/scval", decodeHandlers.HandleDecodeScVal).Methods("POST")
			log.Println("  ✓ /api/v1/silver/tx/{hash}/decoded (human-readable transaction)")
			log.Println("  ✓ /api/v1/silver/tx/{hash}/full (composite transaction analysis)")
			log.Println("  ✓ /api/v1/silver/contracts/{id}/interface (contract ABI)")
			log.Println("  ✓ /api/v1/silver/decode/scval (ScVal decoder)")
		}

		// Gold layer endpoints (Snapshot API) - uses Silver data
		goldHandlers := NewGoldHandlers(unifiedSilverReader)
		log.Println("Registering Gold API endpoints:")

		router.HandleFunc("/api/v1/gold/snapshots/account", goldHandlers.HandleAccountSnapshot).Methods("GET")
		router.HandleFunc("/api/v1/gold/snapshots/balance", goldHandlers.HandleAssetHolders).Methods("GET")
		router.HandleFunc("/api/v1/gold/snapshots/portfolio", goldHandlers.HandlePortfolioSnapshot).Methods("GET")
		router.HandleFunc("/api/v1/gold/snapshots/accounts/batch", goldHandlers.HandleBatchAccounts).Methods("POST")

		log.Println("  ✓ /api/v1/gold/snapshots/account (account state at timestamp)")
		log.Println("  ✓ /api/v1/gold/snapshots/balance (asset holders at timestamp)")
		log.Println("  ✓ /api/v1/gold/snapshots/portfolio (all balances at timestamp)")
		log.Println("  ✓ /api/v1/gold/snapshots/accounts/batch (batch account lookup)")

		// Gold Compliance Archive API endpoints
		complianceHandlers := NewComplianceHandlers(unifiedSilverReader)
		log.Println("Registering Gold Compliance API endpoints:")

		router.HandleFunc("/api/v1/gold/compliance/transactions", complianceHandlers.HandleTransactionArchive).Methods("GET")
		router.HandleFunc("/api/v1/gold/compliance/balances", complianceHandlers.HandleBalanceArchive).Methods("GET")
		router.HandleFunc("/api/v1/gold/compliance/supply", complianceHandlers.HandleSupplyTimeline).Methods("GET")

		// Week 2 - Full archive and lineage endpoints
		router.HandleFunc("/api/v1/gold/compliance/archive", complianceHandlers.HandleFullArchive).Methods("POST")
		router.HandleFunc("/api/v1/gold/compliance/archive/{id}", complianceHandlers.HandleArchiveStatus).Methods("GET")
		router.HandleFunc("/api/v1/gold/compliance/archive/{id}/download/{artifact}", complianceHandlers.HandleArchiveDownload).Methods("GET")
		router.HandleFunc("/api/v1/gold/compliance/lineage", complianceHandlers.HandleLineage).Methods("GET")

		log.Println("  ✓ /api/v1/gold/compliance/transactions (asset transaction archive)")
		log.Println("  ✓ /api/v1/gold/compliance/balances (point-in-time holder snapshot)")
		log.Println("  ✓ /api/v1/gold/compliance/supply (supply timeline)")
		log.Println("  ✓ /api/v1/gold/compliance/archive (POST: async full package)")
		log.Println("  ✓ /api/v1/gold/compliance/archive/{id} (GET: archive status)")
		log.Println("  ✓ /api/v1/gold/compliance/archive/{id}/download/{artifact} (GET: download artifact)")
		log.Println("  ✓ /api/v1/gold/compliance/lineage (GET: archive audit trail)")

		// Semantic layer endpoints
		semanticHandlers := NewSemanticHandlers(unifiedSilverReader)
		if unifiedDuckDBReader != nil {
			semanticHandlers.SetDuckDBReader(unifiedDuckDBReader)
		}
		log.Println("Registering Semantic Layer API endpoints:")

		router.HandleFunc("/api/v1/semantic/activities", semanticHandlers.HandleSemanticActivities).Methods("GET")
		router.HandleFunc("/api/v1/semantic/contracts", semanticHandlers.HandleSemanticContracts).Methods("GET")
		router.HandleFunc("/api/v1/semantic/flows", semanticHandlers.HandleSemanticFlows).Methods("GET")
		log.Println("  ✓ /api/v1/semantic/activities (unified activity feed)")
		log.Println("  ✓ /api/v1/semantic/contracts (contract registry)")
		log.Println("  ✓ /api/v1/semantic/flows (value transfer flows)")

		// Semantic Layer Phase 2 endpoints
		router.HandleFunc("/api/v1/semantic/contracts/functions", semanticHandlers.HandleSemanticContractFunctions).Methods("GET")
		router.HandleFunc("/api/v1/semantic/assets", semanticHandlers.HandleSemanticAssets).Methods("GET")
		router.HandleFunc("/api/v1/semantic/dex/pairs", semanticHandlers.HandleSemanticDexPairs).Methods("GET")
		router.HandleFunc("/api/v1/semantic/accounts/summary", semanticHandlers.HandleSemanticAccountSummary).Methods("GET")
		router.HandleFunc("/api/v1/semantic/tokens/{contract_id}", semanticHandlers.HandleSemanticTokenSummary).Methods("GET")
		log.Println("  ✓ /api/v1/semantic/contracts/functions (per-function call stats)")
		log.Println("  ✓ /api/v1/semantic/assets (asset directory with stats)")
		log.Println("  ✓ /api/v1/semantic/dex/pairs (DEX trading pairs)")
		log.Println("  ✓ /api/v1/semantic/accounts/summary (account activity summary)")
		log.Println("  ✓ /api/v1/semantic/tokens/{contract_id} (token summary, ?address= for balance)")

	}

	// Index Plane endpoints (if enabled)
	if indexHandlers != nil {
		log.Println("Registering Index Plane API endpoints:")

		// Transaction hash lookup endpoints
		router.HandleFunc("/transactions/{hash}", indexHandlers.HandleTransactionLookup).Methods("GET")
		router.HandleFunc("/api/v1/index/transactions/{hash}", indexHandlers.HandleTransactionLookup).Methods("GET")
		router.HandleFunc("/api/v1/index/transactions/lookup", indexHandlers.HandleBatchTransactionLookup).Methods("POST")
		router.HandleFunc("/api/v1/index/health", indexHandlers.HandleIndexHealth).Methods("GET")
		log.Println("  ✓ /transactions/{hash} - Fast transaction hash lookup")
		log.Println("  ✓ /api/v1/index/transactions/{hash} - Fast transaction hash lookup")
		log.Println("  ✓ /api/v1/index/transactions/lookup - Batch hash lookup (POST)")
		log.Println("  ✓ /api/v1/index/health - Index coverage statistics")
	}

	// Contract Event Index endpoints (if enabled)
	if contractIndexHandlers != nil {
		log.Println("Registering Contract Event Index API endpoints:")

		// Contract event lookup endpoints with path parameters
		router.HandleFunc("/api/v1/index/contracts/health", contractIndexHandlers.HandleContractIndexHealth).Methods("GET")
		router.HandleFunc("/api/v1/index/contracts/lookup", contractIndexHandlers.HandleBatchContractLookup).Methods("POST")
		router.HandleFunc("/api/v1/index/contracts/{contract_id}/ledgers", contractIndexHandlers.HandleContractLedgers).Methods("GET")
		router.HandleFunc("/api/v1/index/contracts/{contract_id}/summary", contractIndexHandlers.HandleContractEventSummary).Methods("GET")

		log.Println("  ✓ /api/v1/index/contracts/{contract_id}/ledgers - Get ledgers for contract")
		log.Println("  ✓ /api/v1/index/contracts/{contract_id}/summary - Get event summary")
		log.Println("  ✓ /api/v1/index/contracts/lookup - Batch contract lookup (POST)")
		log.Println("  ✓ /api/v1/index/contracts/health - Contract index statistics")
	}

	server := &http.Server{
		Addr:         fmt.Sprintf(":%d", config.Service.Port),
		Handler:      corsMiddleware(router),
		ReadTimeout:  time.Duration(config.Service.ReadTimeoutSeconds) * time.Second,
		WriteTimeout: time.Duration(config.Service.WriteTimeoutSeconds) * time.Second,
	}

	// Start server in goroutine
	go func() {
		log.Printf("🚀 API server listening on :%d", config.Service.Port)
		if silverHandlers != nil {
			log.Printf("📊 Silver layer endpoints available at /api/v1/silver/*")
			log.Printf("🥇 Gold layer endpoints available at /api/v1/gold/*")
			log.Printf("📋 Compliance endpoints available at /api/v1/gold/compliance/*")
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

func handleHealthWithSilverAndIndexAndContractIndex(silverEnabled, indexEnabled, contractIndexEnabled bool, readerMode ReaderMode, unifiedReader *UnifiedDuckDBReader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)

		response := map[string]interface{}{
			"status": "healthy",
			"layers": map[string]bool{
				"hot":            true,
				"bronze":         true,
				"silver":         silverEnabled,
				"gold":           silverEnabled, // Gold requires Silver
				"compliance":     silverEnabled, // Compliance requires Silver
				"index":          indexEnabled,
				"contract_index": contractIndexEnabled,
			},
			"reader_mode": string(readerMode),
		}

		// Add unified reader health if available
		if unifiedReader != nil {
			ctx := r.Context()
			if healthStatus, err := unifiedReader.HealthCheck(ctx); err == nil {
				response["unified_reader"] = healthStatus
			} else {
				response["unified_reader"] = map[string]string{
					"error": err.Error(),
				}
			}
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
