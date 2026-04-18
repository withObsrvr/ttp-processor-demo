package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	_ "github.com/jackc/pgx/v5/stdlib"
	_ "github.com/lib/pq"
	"google.golang.org/grpc"
)

func configureSQLDBPool(db *sql.DB, cfg DatabaseConfig) {
	if cfg.MaxOpenConns > 0 {
		db.SetMaxOpenConns(cfg.MaxOpenConns)
	}
	db.SetMaxIdleConns(cfg.MaxIdleConnsOrDefault())

	if lifetime := cfg.ConnMaxLifetime(); lifetime > 0 {
		db.SetConnMaxLifetime(lifetime)
	}
	if idle := cfg.ConnMaxIdleTime(); idle > 0 {
		db.SetConnMaxIdleTime(idle)
	}
}

func main() {
	// Parse command line flags
	configPath := flag.String("config", "config.yaml", "Path to config file")
	coldReplay := flag.Bool("cold-replay", false, "Run one-shot cold replay: read from bronze cold DuckLake, write to silver hot")
	replayStart := flag.Int64("replay-start", 0, "Start ledger for cold replay (0 = auto-detect from checkpoint)")
	replayEnd := flag.Int64("replay-end", 0, "End ledger for cold replay (0 = auto-detect from bronze cold MAX)")
	replayBatchSize := flag.Int64("replay-batch-size", 500, "Ledgers per batch during cold replay")
	flag.Parse()

	log.Println("🔧 Loading configuration from", *configPath)

	// Load configuration
	config, err := LoadConfig(*configPath)
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	if err := config.Validate(); err != nil {
		log.Fatalf("Invalid config: %v", err)
	}

	log.Printf("📋 Service: %s", config.Service.Name)
	log.Printf("📋 Poll interval: %v", config.PollInterval())
	log.Printf("📋 Batch size: %d ledgers", config.Performance.BatchSize)
	log.Printf("📋 Bronze readers: %d", config.MaxBronzeReaders())
	log.Printf("📋 Silver writers: %d", config.MaxSilverWriters())

	// Connect to Bronze Hot (stellar_hot PostgreSQL)
	log.Println("🔗 Connecting to Bronze Hot (stellar_hot)...")
	bronzeDB, err := sql.Open("pgx", config.BronzeHot.ConnectionString())
	if err != nil {
		log.Fatalf("Failed to connect to bronze hot: %v", err)
	}
	defer bronzeDB.Close()

	configureSQLDBPool(bronzeDB, config.BronzeHot)

	if err := bronzeDB.Ping(); err != nil {
		log.Fatalf("Failed to ping bronze hot: %v", err)
	}
	log.Println("✅ Connected to Bronze Hot")

	// Connect to Silver Hot (silver_hot PostgreSQL)
	log.Println("🔗 Connecting to Silver Hot (silver_hot) with pgx stdlib...")
	silverDB, err := sql.Open("pgx", config.SilverHot.ConnectionString())
	if err != nil {
		log.Fatalf("Failed to connect to silver hot: %v", err)
	}
	defer silverDB.Close()

	configureSQLDBPool(silverDB, config.SilverHot)

	if err := silverDB.Ping(); err != nil {
		log.Fatalf("Failed to ping silver hot: %v", err)
	}
	log.Println("✅ Connected to Silver Hot")

	// Ensure silver_hot schema is up to date (CREATE IF NOT EXISTS)
	if err := EnsureSilverHotSchema(silverDB); err != nil {
		log.Fatalf("Failed to ensure silver_hot schema: %v", err)
	}

	// Initialize components
	bronzeHotReader := NewBronzeReader(bronzeDB)
	silverWriter := NewSilverWriter(silverDB)
	checkpoint := NewCheckpointManager(silverDB, config.Checkpoint.Table)

	// Initialize Bronze Cold reader if configured
	var bronzeColdReader *BronzeColdReader
	if config.Fallback.Enabled && config.BronzeCold != nil && config.S3 != nil {
		log.Println("🔗 Connecting to Bronze Cold (DuckLake)...")
		var err error
		bronzeColdReader, err = NewBronzeColdReader(config.BronzeCold, config.S3)
		if err != nil {
			log.Printf("⚠️  Failed to initialize Bronze Cold reader: %v", err)
			log.Println("   Fallback to cold storage will be disabled")
		} else {
			log.Println("✅ Connected to Bronze Cold (DuckLake)")
		}
	} else if config.Fallback.Enabled {
		log.Println("⚠️  Fallback enabled but bronze_cold or s3 config missing - fallback disabled")
	}

	// Create source manager with hot/cold readers
	sourceManager := NewSourceManager(bronzeHotReader, bronzeColdReader, config.Fallback.Enabled)
	if bronzeColdReader != nil {
		defer sourceManager.Close()
	}

	// Create transformer
	transformer := NewRealtimeTransformer(config, sourceManager, silverWriter, checkpoint, silverDB)

	// Start downstream flowctl SourceService gRPC server for serving subscribers (optional)
	var silverSource *SilverSourceServer
	if config.Service.GRPCPort > 0 {
		silverSource = NewSilverSourceServer(checkpoint)
		transformer.SetSourceServer(silverSource)

		grpcServer := grpc.NewServer()
		silverSource.Register(grpcServer)
		lis, err := net.Listen("tcp", fmt.Sprintf(":%d", config.Service.GRPCPort))
		if err != nil {
			log.Fatalf("Failed to listen on downstream gRPC port %d: %v", config.Service.GRPCPort, err)
		}
		go func() {
			log.Printf("flowctl SourceService gRPC server listening on :%d", config.Service.GRPCPort)
			if err := grpcServer.Serve(lis); err != nil {
				log.Fatalf("downstream gRPC server failed: %v", err)
			}
		}()
		defer grpcServer.GracefulStop()
	}

	// Cold replay mode: one-shot batch processing from bronze cold → silver hot
	if *coldReplay {
		if bronzeColdReader == nil {
			log.Fatalf("❌ Cold replay requires bronze_cold and s3 configuration. Enable fallback and configure bronze_cold + s3 in config.")
		}

		// Determine start ledger
		start := *replayStart
		if start == 0 {
			// Auto-detect from existing checkpoint
			existing, err := checkpoint.Load()
			if err != nil {
				log.Fatalf("Failed to load checkpoint: %v", err)
			}
			if existing > 0 {
				start = existing + 1
				log.Printf("📍 Resuming cold replay from checkpoint: ledger %d", start)
			} else {
				// Detect from bronze cold MIN
				ctx := context.Background()
				coldMin, err := bronzeColdReader.GetMinLedgerSequence(ctx)
				if err != nil {
					log.Fatalf("Failed to get bronze cold min ledger: %v", err)
				}
				start = coldMin
				log.Printf("📍 Starting cold replay from bronze cold MIN: ledger %d", start)
			}
		}

		// Determine end ledger
		end := *replayEnd
		if end == 0 {
			ctx := context.Background()
			coldMax, err := bronzeColdReader.GetMaxLedgerSequence(ctx)
			if err != nil {
				log.Fatalf("Failed to get bronze cold max ledger: %v", err)
			}
			end = coldMax
			log.Printf("📍 Cold replay end: bronze cold MAX ledger %d", end)
		}

		log.Printf("🔄 COLD REPLAY MODE: ledgers %d → %d, batch size %d", start, end, *replayBatchSize)

		ctx, cancel := context.WithCancel(context.Background())
		sigChan := make(chan os.Signal, 1)
		signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
		go func() {
			<-sigChan
			log.Println("🛑 Shutdown signal received, stopping replay...")
			cancel()
		}()

		if err := transformer.RunColdReplay(ctx, start, end, *replayBatchSize); err != nil {
			log.Fatalf("Cold replay failed: %v", err)
		}

		log.Println("👋 Cold replay complete")
		return
	}

	// Normal mode: start health server and polling loop
	healthServer := NewHealthServer(transformer)
	go func() {
		if err := healthServer.Start(config.Service.HealthPort); err != nil {
			log.Fatalf("Health server failed: %v", err)
		}
	}()

	// Setup graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	go func() {
		<-sigChan
		log.Println("🛑 Shutdown signal received...")
		transformer.Stop()
	}()

	// Start transformer (blocks until stopped)
	if err := transformer.Start(); err != nil {
		log.Fatalf("Transformer failed: %v", err)
	}

	log.Println("👋 Shutdown complete")
}
