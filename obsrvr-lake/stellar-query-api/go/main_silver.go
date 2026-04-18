package main

import (
	"flag"
	"log"

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

	if config.RPCFallback != nil && config.RPCFallback.Enabled && config.RPCFallback.URL != "" {
		walletRPCFallback = NewWalletRPCFallback(*config.RPCFallback)
		log.Printf("✅ Smart-wallet RPC fallback enabled: %s", config.RPCFallback.URL)
	}

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
	var silverHotReader *SilverHotReader
	readerMode := config.Query.ReaderMode
	if readerMode == "" {
		readerMode = ReaderModeLegacy // Default to legacy for backward compatibility
	}

	if config.DuckLakeSilver != nil && config.PostgresSilver != nil {
		// Create hot reader (PostgreSQL silver_hot)
		var err error
		silverHotReader, err = NewSilverHotReader(*config.PostgresSilver)
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
		if hotReader != nil {
			silverHotReader.SetBronzeHotPG(hotReader.DB())
		}

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
	var indexReader *IndexReader
	if config.Index != nil && config.Index.Enabled {
		var err error
		indexReader, err = NewIndexReader(*config.Index)
		if err != nil {
			log.Printf("⚠️  Failed to create Index Plane reader: %v", err)
			log.Println("     Index Plane endpoints will be disabled")
			indexReader = nil
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

	app := &application{
		config:                config,
		queryService:          queryService,
		silverHandlers:        silverHandlers,
		unifiedSilverReader:   unifiedSilverReader,
		unifiedDuckDBReader:   unifiedDuckDBReader,
		silverHotReader:       silverHotReader,
		hotReader:             hotReader,
		coldReader:            coldReader,
		indexHandlers:         indexHandlers,
		indexReader:           indexReader,
		contractIndexHandlers: contractIndexHandlers,
		readerMode:            readerMode,
	}

	if err := app.serve(); err != nil {
		log.Fatalf("server stopped with error: %v", err)
	}
}

func main() {
	mainWithSilver()
}
