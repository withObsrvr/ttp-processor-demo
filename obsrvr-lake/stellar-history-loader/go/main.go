package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"runtime"
	"time"
)

// Version is set at build time via -ldflags "-X main.Version=..."
var Version = "dev"

func main() {
	start := flag.Uint("start", 0, "Start ledger sequence (required)")
	end := flag.Uint("end", 0, "End ledger sequence (required)")
	workers := flag.Int("workers", runtime.NumCPU(), "Number of parallel workers")
	output := flag.String("output", "", "Output directory for Parquet files (required)")
	batchSize := flag.Int("batch-size", 1000, "Ledgers per extraction batch")
	networkPassphrase := flag.String("network-passphrase", "Test SDF Network ; September 2015", "Stellar network passphrase")
	storageType := flag.String("storage-type", "FS", "Backend storage type: GCS, S3, FS")
	bucket := flag.String("bucket", "", "Storage bucket/path for ledger data (required)")
	ledgersPerFile := flag.Uint("ledgers-per-file", 1, "Ledgers per archive file (GCS/S3)")
	filesPerPartition := flag.Uint("files-per-partition", 64000, "Files per archive partition (GCS/S3)")
	runSilver := flag.Bool("silver", false, "Run silver transforms after bronze extraction")
	runValidate := flag.Bool("validate", false, "Run quality validation checks after extraction")
	runDuckLake := flag.Bool("ducklake", false, "Push bronze Parquet to DuckLake (B2 + catalog)")
	dlCatalog := flag.String("ducklake-catalog", "", "PostgreSQL catalog DSN for DuckLake")
	dlDataPath := flag.String("ducklake-data-path", "", "S3/B2 bucket path (e.g., s3://obsrvr-lake-testnet/)")
	dlMetaSchema := flag.String("ducklake-metadata-schema", "bronze_meta", "DuckLake metadata schema")
	dlSchemaSQL := flag.String("ducklake-schema-sql", "", "Path to v3_bronze_schema.sql (optional)")
	b2KeyID := flag.String("b2-key-id", "", "B2/S3 access key ID")
	b2KeySecret := flag.String("b2-key-secret", "", "B2/S3 secret access key")
	b2Endpoint := flag.String("b2-endpoint", "s3.us-west-004.backblazeb2.com", "B2/S3 endpoint")
	b2Region := flag.String("b2-region", "us-west-004", "B2/S3 region")
	pgHost := flag.String("pg-host", "", "PostgreSQL host for hot buffer (enables hot buffer loading)")
	pgPort := flag.Int("pg-port", 25060, "PostgreSQL port")
	pgDB := flag.String("pg-database", "stellar_hot", "PostgreSQL database")
	pgUser := flag.String("pg-user", "", "PostgreSQL user")
	pgPassword := flag.String("pg-password", "", "PostgreSQL password")
	pgSSL := flag.String("pg-sslmode", "require", "PostgreSQL SSL mode")
	tailLedgers := flag.Uint("tail-ledgers", 100000, "Number of recent ledgers to load into hot buffer")

	flag.Parse()

	// Validate required flags
	if *start == 0 {
		log.Fatal("--start is required and must be > 0")
	}
	if *end == 0 {
		log.Fatal("--end is required and must be > 0")
	}
	if *output == "" {
		log.Fatal("--output is required")
	}
	if *bucket == "" && !*runDuckLake {
		log.Fatal("--bucket is required (unless using --ducklake with existing output)")
	}

	startLedger := uint32(*start)
	endLedger := uint32(*end)

	if endLedger <= startLedger {
		log.Fatalf("--end (%d) must be greater than --start (%d)", endLedger, startLedger)
	}

	if *workers < 1 {
		log.Fatal("--workers must be >= 1")
	}

	if *batchSize < 1 {
		log.Fatal("--batch-size must be >= 1")
	}

	switch *storageType {
	case "GCS", "S3", "FS", "XDR", "RPC":
		// valid
	default:
		log.Fatalf("--storage-type must be one of: GCS, S3, FS, XDR, RPC (got %q)", *storageType)
	}

	// Ensure output directory exists
	if err := os.MkdirAll(*output, 0o755); err != nil {
		log.Fatalf("Failed to create output directory %q: %v", *output, err)
	}

	totalLedgers := endLedger - startLedger + 1

	fmt.Println("=== Stellar History Loader ===")
	fmt.Printf("Ledger range:      %d - %d (%d ledgers)\n", startLedger, endLedger, totalLedgers)
	fmt.Printf("Workers:           %d\n", *workers)
	fmt.Printf("Batch size:        %d\n", *batchSize)
	fmt.Printf("Output directory:  %s\n", *output)
	fmt.Printf("Storage type:      %s\n", *storageType)
	fmt.Printf("Bucket:            %s\n", *bucket)
	fmt.Printf("Network:           %s\n", *networkPassphrase)
	fmt.Println()

	// Skip extraction if only doing DuckLake push on existing output
	if *bucket != "" {
		config := OrchestratorConfig{
			StartLedger:       startLedger,
			EndLedger:         endLedger,
			NumWorkers:        *workers,
			OutputDir:         *output,
			BatchSize:         *batchSize,
			NetworkPassphrase: *networkPassphrase,
			StorageType:       *storageType,
			Bucket:            *bucket,
			LedgersPerFile:    uint32(*ledgersPerFile),
			FilesPerPartition: uint32(*filesPerPartition),
		}

		orchestrator := NewOrchestrator(config)

		startTime := time.Now()

		if err := orchestrator.Run(); err != nil {
			log.Fatalf("Orchestrator failed: %v", err)
		}

		elapsed := time.Since(startTime)
		throughput := float64(totalLedgers) / elapsed.Seconds()

		fmt.Println()
		fmt.Println("=== Summary ===")
		fmt.Printf("Total ledgers processed: %d\n", totalLedgers)
		fmt.Printf("Elapsed time:           %s\n", elapsed.Round(time.Millisecond))
		fmt.Printf("Throughput:             %.1f ledgers/sec\n", throughput)
	} else {
		fmt.Println("Skipping extraction (no --bucket specified, using existing output)")
	}

	// Run silver transforms if requested
	if *runSilver {
		fmt.Println()
		fmt.Println("=== Silver Transforms ===")
		silverStart := time.Now()

		st, err := NewSilverTransformer(*output)
		if err != nil {
			log.Fatalf("Failed to create silver transformer: %v", err)
		}
		defer st.Close()

		if err := st.RunAll(context.Background()); err != nil {
			log.Printf("Silver transforms had errors: %v", err)
		}

		fmt.Printf("Silver transforms completed in %s\n", time.Since(silverStart).Round(time.Millisecond))
	}

	// Run quality validation if requested
	if *runValidate {
		fmt.Println()
		validator, err := NewValidator(*output)
		if err != nil {
			log.Printf("Validation setup failed: %v", err)
		} else {
			defer validator.Close()
			if err := validator.RunAll(context.Background()); err != nil {
				log.Printf("Validation: %v", err)
			}
		}
	}

	// Push to DuckLake if configured
	if *runDuckLake {
		if *dlCatalog == "" || *dlDataPath == "" || *b2KeyID == "" || *b2KeySecret == "" {
			log.Fatal("--ducklake requires --ducklake-catalog, --ducklake-data-path, --b2-key-id, --b2-key-secret")
		}

		fmt.Println()
		fmt.Println("=== DuckLake Push ===")
		dlStart := time.Now()

		pusher, err := NewDuckLakePusher(DuckLakeConfig{
			CatalogDSN:      *dlCatalog,
			DataPath:        *dlDataPath,
			MetadataSchema:  *dlMetaSchema,
			S3KeyID:         *b2KeyID,
			S3KeySecret:     *b2KeySecret,
			S3Endpoint:      *b2Endpoint,
			S3Region:        *b2Region,
			BronzeSchemaSQL: *dlSchemaSQL,
		})
		if err != nil {
			log.Fatalf("DuckLake setup failed: %v", err)
		}
		defer pusher.Close()

		if err := pusher.Push(context.Background(), *output); err != nil {
			log.Fatalf("DuckLake push failed: %v", err)
		}

		fmt.Printf("DuckLake push completed in %s\n", time.Since(dlStart).Round(time.Millisecond))
	}

	// Load hot buffer if PostgreSQL is configured
	if *pgHost != "" {
		fmt.Println()
		fmt.Println("=== Hot Buffer Population ===")
		hotStart := time.Now()

		loader, err := NewHotBufferLoader(*output, HotBufferConfig{
			Host:        *pgHost,
			Port:        *pgPort,
			Database:    *pgDB,
			User:        *pgUser,
			Password:    *pgPassword,
			SSLMode:     *pgSSL,
			TailLedgers: uint32(*tailLedgers),
		})
		if err != nil {
			log.Printf("Hot buffer connection failed: %v", err)
		} else {
			defer loader.Close()
			if err := loader.LoadTail(context.Background(), uint32(*tailLedgers), endLedger); err != nil {
				log.Printf("Hot buffer load had errors: %v", err)
			}
			fmt.Printf("Hot buffer populated in %s\n", time.Since(hotStart).Round(time.Millisecond))
		}
	}

	fmt.Println()
	if err := GenerateCatalogReport(*output); err != nil {
		log.Printf("Warning: catalog report failed: %v", err)
	}
}
