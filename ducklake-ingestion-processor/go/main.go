package main

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	_ "github.com/marcboeker/go-duckdb/v2"
	"github.com/stellar/go-stellar-sdk/ingest"
	"github.com/stellar/go-stellar-sdk/strkey"
	"github.com/stellar/go-stellar-sdk/xdr"
	pb "github.com/withObsrvr/ttp-processor-demo/stellar-live-source-datalake/go/gen/raw_ledger_service"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"gopkg.in/yaml.v3"
)

const (
	// Column counts for multi-table ingestion
	ledgerColumnCount      = 24
	transactionColumnCount = 13
	operationColumnCount   = 13  // 11 base fields + 2 payment fields (PaymentTo, PaymentAmount)
	balanceColumnCount     = 11  // 11 fields: account_id, balance, buying/selling liabilities, subentries, sponsoring/sponsored, sequence, last_modified_ledger, ledger_sequence, ledger_range
)

// Config represents the application configuration
type Config struct {
	Service struct {
		Name        string `yaml:"name"`
		Environment string `yaml:"environment"`
	} `yaml:"service"`

	Source struct {
		Endpoint         string `yaml:"endpoint"`
		NetworkPassphrase string `yaml:"network_passphrase"`
		StartLedger       uint32 `yaml:"start_ledger"`
		EndLedger         uint32 `yaml:"end_ledger"`
	} `yaml:"source"`

	DuckLake struct {
		CatalogPath           string `yaml:"catalog_path"`
		DataPath              string `yaml:"data_path"`
		CatalogName           string `yaml:"catalog_name"`
		SchemaName            string `yaml:"schema_name"`
		TableName             string `yaml:"table_name"`
		AWSAccessKeyID        string `yaml:"aws_access_key_id"`
		AWSSecretAccessKey    string `yaml:"aws_secret_access_key"`
		AWSRegion             string `yaml:"aws_region"`
		AWSEndpoint           string `yaml:"aws_endpoint"`
		EnablePublicArchive   bool   `yaml:"enable_public_archive"`
		BatchSize             int    `yaml:"batch_size"`
		CommitIntervalSeconds int    `yaml:"commit_interval_seconds"`
		UseUpsert             bool   `yaml:"use_upsert"`
		CreateIndexes         bool   `yaml:"create_indexes"`
		NumWorkers            int    `yaml:"num_workers"` // 1 = single-threaded (default), 2-8 = parallel
	} `yaml:"ducklake"`

	Logging struct {
		Level  string `yaml:"level"`
		Format string `yaml:"format"`
	} `yaml:"logging"`
}

// LedgerData represents the ledger data we extract
type LedgerData struct {
	// Core metadata (existing 13 fields)
	Sequence            uint32
	LedgerHash          string
	PreviousLedgerHash  string
	ClosedAt            time.Time
	ProtocolVersion     uint32
	TotalCoins          int64
	FeePool             int64
	BaseFee             uint32
	BaseReserve         uint32
	MaxTxSetSize        uint32
	SuccessfulTxCount   uint32
	FailedTxCount       uint32
	LedgerRange         uint32 // Partition key: (sequence / 10000) * 10000

	// NEW: Operation counts (stellar-etl alignment)
	TransactionCount    uint32 // successful_tx_count + failed_tx_count
	OperationCount      uint32 // Operations in successful transactions only
	TxSetOperationCount uint32 // All operations (including failed txs)

	// NEW: Soroban (Protocol 20+)
	SorobanFeeWrite1KB int64 // Cost per 1KB write

	// NEW: Consensus metadata
	NodeID       string // SCP validator node
	Signature    string // SCP signature (base64)
	LedgerHeader string // Full header XDR (base64)

	// NEW: State tracking
	BucketListSize       uint64 // Total bucket list bytes
	LiveSorobanStateSize uint64 // Live Soroban state bytes

	// NEW: Protocol 23 (CAP-62) - Hot Archive
	EvictedKeysCount uint32 // Number of evicted keys
}

// TransactionData represents transaction data for multi-table ingestion
type TransactionData struct {
	LedgerSequence        uint32
	TransactionHash       string
	SourceAccount         string
	FeeCharged            int64
	MaxFee                int64
	Successful            bool
	TransactionResultCode string
	OperationCount        int32
	CreatedAt             time.Time
	AccountSequence       int64
	MemoType              string
	Memo                  string
	LedgerRange           uint32
}

// OperationData represents operation data for multi-table ingestion (MVP: base fields only)
type OperationData struct {
	TransactionHash       string
	OperationIndex        int32
	LedgerSequence        uint32
	SourceAccount         string
	Type                  int32
	TypeString            string
	CreatedAt             time.Time
	TransactionSuccessful bool
	OperationResultCode   string
	OperationTraceCode    string
	LedgerRange           uint32

	// Operation-specific fields (simplified for MVP)
	PaymentTo     string
	PaymentAmount int64
}

// NativeBalanceData represents native XLM balance changes
type BalanceData struct {
	AccountID            string
	Balance              int64
	BuyingLiabilities    int64
	SellingLiabilities   int64
	NumSubentries        int32
	NumSponsoring        int32
	NumSponsored         int32
	SequenceNumber       int64
	LastModifiedLedger   uint32
	LedgerSequence       uint32
	LedgerRange          uint32
}

// WorkerBuffers holds buffers for all tables for a single worker
type WorkerBuffers struct {
	ledgers       []LedgerData
	transactions  []TransactionData
	operations    []OperationData
	balances      []BalanceData
	lastCommit    time.Time
}

// Ingester handles the ledger ingestion pipeline
type Ingester struct {
	config      *Config
	grpcConn    *grpc.ClientConn
	grpcClient  pb.RawLedgerServiceClient
	db          *sql.DB
	buffers     WorkerBuffers
	lastCommit  time.Time
}

func main() {
	configPath := flag.String("config", "config/testnet.yaml", "Path to config file")
	startLedger := flag.Uint("start-ledger", 0, "Override start ledger")
	flag.Parse()

	log.Println("DuckLake Ingestion Processor - Cycle 1")

	// Load config
	config, err := loadConfig(*configPath)
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	// Override start ledger if provided
	if *startLedger > 0 {
		config.Source.StartLedger = uint32(*startLedger)
	}

	// Setup graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Choose execution path based on num_workers
	errChan := make(chan error, 1)
	if config.DuckLake.NumWorkers <= 1 {
		// Single-threaded path (existing behavior)
		log.Printf("Starting single-threaded ingestion from ledger %d", config.Source.StartLedger)
		go func() {
			ingester, err := NewIngester(config)
			if err != nil {
				errChan <- fmt.Errorf("failed to create ingester: %w", err)
				return
			}
			defer ingester.Close()
			errChan <- ingester.Start(ctx)
		}()
	} else {
		// Parallel path
		log.Printf("Starting parallel ingestion with %d workers from ledger %d to %d",
			config.DuckLake.NumWorkers, config.Source.StartLedger, config.Source.EndLedger)
		go func() {
			errChan <- runParallelWorkers(ctx, config)
		}()
	}

	// Wait for shutdown signal or error
	select {
	case sig := <-sigChan:
		log.Printf("Received signal %v, shutting down gracefully...", sig)
		cancel()
		// Wait a bit for graceful shutdown
		time.Sleep(2 * time.Second)
	case err := <-errChan:
		if err != nil {
			log.Printf("Ingestion error: %v", err)
			os.Exit(1)
		}
	}

	log.Println("Shutdown complete")
}

// loadConfig loads configuration from YAML file
func loadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read config: %w", err)
	}

	// Expand environment variables
	expanded := os.ExpandEnv(string(data))

	var config Config
	if err := yaml.Unmarshal([]byte(expanded), &config); err != nil {
		return nil, fmt.Errorf("failed to parse config: %w", err)
	}

	// Apply defaults
	if config.DuckLake.NumWorkers == 0 {
		config.DuckLake.NumWorkers = 1
	}

	return &config, nil
}

// runParallelWorkers orchestrates multiple workers processing different ledger ranges
func runParallelWorkers(ctx context.Context, config *Config) error {
	numWorkers := config.DuckLake.NumWorkers

	// Validate ledger range before calculating total (prevent uint32 underflow)
	if config.Source.EndLedger <= config.Source.StartLedger {
		return fmt.Errorf("end_ledger (%d) must be greater than start_ledger (%d)",
			config.Source.EndLedger, config.Source.StartLedger)
	}

	totalLedgers := config.Source.EndLedger - config.Source.StartLedger
	chunkSize := totalLedgers / uint32(numWorkers)

	if chunkSize == 0 {
		return fmt.Errorf("not enough ledgers to split among %d workers (total: %d)", numWorkers, totalLedgers)
	}

	log.Printf("Splitting %d ledgers into %d chunks of ~%d ledgers each",
		totalLedgers, numWorkers, chunkSize)

	var wg sync.WaitGroup
	errChan := make(chan error, numWorkers)

	// Create cancellable context for workers
	workerCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Spawn workers
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)

		workerStart := config.Source.StartLedger + (uint32(i) * chunkSize)
		workerEnd := workerStart + chunkSize
		// NOTE: Load imbalance - last worker gets remainder when totalLedgers % numWorkers != 0
		// This can result in the last worker processing up to (numWorkers-1) additional ledgers
		if i == numWorkers-1 {
			workerEnd = config.Source.EndLedger // Last worker gets remainder
		}

		go func(workerID int, start, end uint32) {
			defer wg.Done()
			log.Printf("[Worker %d] Starting: ledgers %d-%d (%d ledgers)",
				workerID, start, end, end-start)

			if err := runWorker(workerCtx, workerID, start, end, config); err != nil {
				errChan <- fmt.Errorf("worker %d failed: %w", workerID, err)
				cancel() // Cancel all workers on first error
			} else {
				log.Printf("[Worker %d] Completed successfully", workerID)
			}
		}(i, workerStart, workerEnd)
	}

	// Wait for all workers
	wg.Wait()
	close(errChan)

	// Return first error (if any)
	if err, ok := <-errChan; ok {
		return err
	}

	log.Println("All workers completed successfully")
	return nil
}

// runWorker processes a ledger range for one worker
func runWorker(ctx context.Context, workerID int, startLedger, endLedger uint32, config *Config) error {
	// Create worker-specific config
	workerConfig := *config
	workerConfig.Source.StartLedger = startLedger
	workerConfig.Source.EndLedger = endLedger

	// Create ingester for this worker
	ingester, err := NewIngester(&workerConfig)
	if err != nil {
		return fmt.Errorf("failed to create ingester: %w", err)
	}
	defer ingester.Close()

	// Start processing with per-worker progress tracking
	startTime := time.Now()
	processed := 0

	// Start streaming ledgers
	stream, err := ingester.grpcClient.StreamRawLedgers(ctx, &pb.StreamLedgersRequest{
		StartLedger: startLedger,
	})
	if err != nil {
		return fmt.Errorf("failed to start stream: %w", err)
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// Receive ledger
		rawLedger, err := stream.Recv()
		if err == io.EOF {
			log.Printf("[Worker %d] Stream ended", workerID)
			return ingester.flush(ctx)
		}
		if err != nil {
			return fmt.Errorf("stream error: %w", err)
		}

		// Check if we've reached the end ledger (use > to include endLedger in processing)
		if rawLedger.Sequence > endLedger {
			log.Printf("[Worker %d] Reached end ledger %d, stopping ingestion", workerID, endLedger)
			return ingester.flush(ctx)
		}

		// Process ledger
		if err := ingester.processLedger(ctx, rawLedger); err != nil {
			log.Printf("[Worker %d] Error processing ledger %d: %v", workerID, rawLedger.Sequence, err)
			return err
		}

		processed++

		// Periodic logging with worker ID
		if processed%100 == 0 {
			elapsed := time.Since(startTime)
			rate := float64(processed) / elapsed.Seconds()
			log.Printf("[Worker %d] Processed %d ledgers (%.2f ledgers/sec)", workerID, processed, rate)
		}
	}
}

// NewIngester creates a new ingester
func NewIngester(config *Config) (*Ingester, error) {
	ing := &Ingester{
		config:     config,
		buffers:    WorkerBuffers{},
		lastCommit: time.Now(),
	}

	// Connect to stellar-live-source-datalake via gRPC
	conn, err := grpc.Dial(
		config.Source.Endpoint,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(100*1024*1024)),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to source: %w", err)
	}
	ing.grpcConn = conn
	ing.grpcClient = pb.NewRawLedgerServiceClient(conn)

	// Initialize DuckLake connection
	if err := ing.initializeDuckLake(); err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to initialize DuckLake: %w", err)
	}

	log.Println("Ingester initialized successfully")
	return ing, nil
}

// initializeDuckLake sets up DuckDB with DuckLake
func (ing *Ingester) initializeDuckLake() error {
	// Open DuckDB connection
	db, err := sql.Open("duckdb", "")
	if err != nil {
		return fmt.Errorf("failed to open DuckDB: %w", err)
	}
	ing.db = db

	// Install and load extensions
	extensions := []string{
		"INSTALL ducklake",
		"INSTALL httpfs",
		"LOAD ducklake",
		"LOAD httpfs",
	}

	for _, ext := range extensions {
		if _, err := ing.db.Exec(ext); err != nil {
			log.Printf("Extension setup: %s (error: %v)", ext, err)
		}
	}

	// Configure S3/B2 credentials (if using remote storage)
	if ing.config.DuckLake.AWSAccessKeyID != "" {
		ing.configureS3()
	}

	// Attach DuckLake catalog (matching cdp-pipeline-workflow pattern)
	// Try WITHOUT TYPE ducklake first (for existing catalogs)
	var attachSQL string
	if ing.config.DuckLake.DataPath != "" {
		// Remote DuckLake with DATA_PATH
		attachSQL = fmt.Sprintf(
			"ATTACH '%s' AS %s (DATA_PATH '%s')",
			ing.config.DuckLake.CatalogPath,
			ing.config.DuckLake.CatalogName,
			ing.config.DuckLake.DataPath,
		)
	} else {
		// Local DuckDB file
		attachSQL = fmt.Sprintf(
			"ATTACH '%s' AS %s",
			ing.config.DuckLake.CatalogPath,
			ing.config.DuckLake.CatalogName,
		)
	}

	if _, err := ing.db.Exec(attachSQL); err != nil {
		errStr := err.Error()
		// Check if error is due to database not existing (first-time initialization)
		if strings.Contains(errStr, "database does not exist") || strings.Contains(errStr, "Cannot open database") {
			log.Printf("DuckLake catalog does not exist, creating new catalog...")

			// Try to create the catalog by using TYPE ducklake (allows creation)
			var createAttachSQL string
			if ing.config.DuckLake.DataPath != "" {
				createAttachSQL = fmt.Sprintf(
					"ATTACH '%s' AS %s (TYPE ducklake, DATA_PATH '%s')",
					ing.config.DuckLake.CatalogPath,
					ing.config.DuckLake.CatalogName,
					ing.config.DuckLake.DataPath,
				)
			} else {
				createAttachSQL = fmt.Sprintf(
					"ATTACH '%s' AS %s",
					ing.config.DuckLake.CatalogPath,
					ing.config.DuckLake.CatalogName,
				)
			}

			if _, err := ing.db.Exec(createAttachSQL); err != nil {
				return fmt.Errorf("failed to create and attach DuckLake catalog: %w", err)
			}
			log.Println("Created and attached new DuckLake catalog")
		} else {
			return fmt.Errorf("failed to attach DuckLake catalog: %w", err)
		}
	} else {
		log.Printf("Attached existing DuckLake catalog: %s", ing.config.DuckLake.CatalogName)
	}

	// Re-apply S3 configuration after catalog attachment
	// DuckLake may reset some S3 settings during ATTACH
	if ing.config.DuckLake.AWSAccessKeyID != "" {
		log.Println("Re-applying S3 configuration after catalog attachment")
		ing.configureS3()
	}

	// Create schema if it doesn't exist
	createSchemaSQL := fmt.Sprintf(
		"CREATE SCHEMA IF NOT EXISTS %s.%s",
		ing.config.DuckLake.CatalogName,
		ing.config.DuckLake.SchemaName,
	)
	if _, err := ing.db.Exec(createSchemaSQL); err != nil {
		return fmt.Errorf("failed to create schema: %w", err)
	}

	log.Printf("Schema ready: %s.%s", ing.config.DuckLake.CatalogName, ing.config.DuckLake.SchemaName)

	// Use the schema
	useSQL := fmt.Sprintf("USE %s.%s", ing.config.DuckLake.CatalogName, ing.config.DuckLake.SchemaName)
	if _, err := ing.db.Exec(useSQL); err != nil {
		return fmt.Errorf("failed to use schema: %w", err)
	}

	// Create table (idempotent - only creates if doesn't exist)
	// NOTE: We do NOT drop the table to avoid orphaning Parquet files in B2
	// DuckLake manages file lifecycle - dropping the table removes catalog metadata
	// but leaves files in storage, causing duplicates on next run
	if err := ing.createTable(); err != nil {
		return fmt.Errorf("failed to create ledgers table: %w", err)
	}

	// Create transactions table (Cycle 3)
	if err := ing.createTransactionsTable(); err != nil {
		return fmt.Errorf("failed to create transactions table: %w", err)
	}

	// Create operations table (Cycle 3)
	if err := ing.createOperationsTable(); err != nil {
		return fmt.Errorf("failed to create operations table: %w", err)
	}

	// Create native_balances table (Cycle 3)
	if err := ing.createNativeBalancesTable(); err != nil {
		return fmt.Errorf("failed to create native_balances table: %w", err)
	}

	// Note: We build INSERT SQL dynamically in flush() for multi-row optimization
	return nil
}

// configureS3 configures S3/B2 credentials
func (ing *Ingester) configureS3() {
	cfg := ing.config.DuckLake

	urlStyle := "path"
	endpoint := cfg.AWSEndpoint
	region := cfg.AWSRegion

	// Remove https:// prefix
	endpoint = strings.TrimPrefix(endpoint, "https://")
	endpoint = strings.TrimPrefix(endpoint, "http://")

	// Create unnamed S3 secret
	ing.db.Exec("DROP SECRET IF EXISTS __default_s3")

	createSecretSQL := fmt.Sprintf(`
		CREATE SECRET (
			TYPE S3,
			KEY_ID '%s',
			SECRET '%s',
			REGION '%s',
			ENDPOINT '%s',
			URL_STYLE '%s'
		)
	`, cfg.AWSAccessKeyID, cfg.AWSSecretAccessKey, region, endpoint, urlStyle)

	log.Printf("Creating S3 secret with: ENDPOINT='%s', REGION='%s', URL_STYLE='%s'", endpoint, region, urlStyle)

	if _, err := ing.db.Exec(createSecretSQL); err != nil {
		log.Printf("Failed to create S3 secret: %v", err)
	} else {
		log.Println("S3 credentials configured successfully")

		// Verify secret was created
		rows, err := ing.db.Query("SELECT name, type FROM duckdb_secrets()")
		if err == nil {
			defer rows.Close()
			log.Println("Registered secrets:")
			for rows.Next() {
				var name, secretType string
				rows.Scan(&name, &secretType)
				log.Printf("  - %s (type: %s)", name, secretType)
			}
		}
	}
}

// createTable creates the ledgers table with partition column
func (ing *Ingester) createTable() error {
	// Create table with ledger_range column for partitioning
	// DuckLake will organize files based on this column's values
	// We'll compute: ledger_range = (sequence / 10000) * 10000
	// This creates logical partitions: 0, 10000, 20000, 160000, etc.
	createSQL := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s.%s.%s (
			sequence BIGINT NOT NULL,
			ledger_hash VARCHAR NOT NULL,
			previous_ledger_hash VARCHAR NOT NULL,
			closed_at TIMESTAMP NOT NULL,
			protocol_version INT NOT NULL,
			total_coins BIGINT NOT NULL,
			fee_pool BIGINT NOT NULL,
			base_fee INT NOT NULL,
			base_reserve INT NOT NULL,
			max_tx_set_size INT NOT NULL,
			successful_tx_count INT NOT NULL,
			failed_tx_count INT NOT NULL,
			ingestion_timestamp TIMESTAMP,
			ledger_range BIGINT,

			-- NEW: Operation counts (stellar-etl alignment)
			transaction_count INT,
			operation_count INT,
			tx_set_operation_count INT,

			-- NEW: Soroban (Protocol 20+)
			soroban_fee_write1kb BIGINT,

			-- NEW: Consensus metadata
			node_id VARCHAR,
			signature VARCHAR,
			ledger_header TEXT,

			-- NEW: State tracking
			bucket_list_size BIGINT,
			live_soroban_state_size BIGINT,

			-- NEW: Protocol 23 (CAP-62) - Hot Archive
			evicted_keys_count INT
		)`,
		ing.config.DuckLake.CatalogName,
		ing.config.DuckLake.SchemaName,
		ing.config.DuckLake.TableName,
	)

	if _, err := ing.db.Exec(createSQL); err != nil {
		return fmt.Errorf("failed to create table: %w", err)
	}

	log.Printf("Table ready: %s.%s.%s",
		ing.config.DuckLake.CatalogName,
		ing.config.DuckLake.SchemaName,
		ing.config.DuckLake.TableName)

	return nil
}

// createTransactionsTable creates the transactions table
func (ing *Ingester) createTransactionsTable() error {
	createSQL := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s.%s.transactions (
			ledger_sequence BIGINT NOT NULL,
			transaction_hash VARCHAR NOT NULL,
			source_account VARCHAR NOT NULL,
			fee_charged BIGINT NOT NULL,
			max_fee BIGINT NOT NULL,
			successful BOOLEAN NOT NULL,
			transaction_result_code VARCHAR NOT NULL,
			operation_count INT NOT NULL,
			memo_type VARCHAR,
			memo VARCHAR,
			created_at TIMESTAMP NOT NULL,
			account_sequence BIGINT,
			ledger_range BIGINT
		)`,
		ing.config.DuckLake.CatalogName,
		ing.config.DuckLake.SchemaName,
	)

	if _, err := ing.db.Exec(createSQL); err != nil {
		return fmt.Errorf("failed to create transactions table: %w", err)
	}

	log.Printf("Table ready: %s.%s.transactions",
		ing.config.DuckLake.CatalogName,
		ing.config.DuckLake.SchemaName)

	return nil
}

// createOperationsTable creates the operations table (MVP with base fields only)
func (ing *Ingester) createOperationsTable() error {
	createSQL := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s.%s.operations (
			transaction_hash VARCHAR NOT NULL,
			operation_index INT NOT NULL,
			ledger_sequence BIGINT NOT NULL,
			source_account VARCHAR NOT NULL,
			type INT NOT NULL,
			type_string VARCHAR NOT NULL,
			created_at TIMESTAMP NOT NULL,
			transaction_successful BOOLEAN NOT NULL,
			operation_result_code VARCHAR,
			operation_trace_code VARCHAR,
			ledger_range BIGINT,
			payment_to VARCHAR,
			payment_amount BIGINT
		)`,
		ing.config.DuckLake.CatalogName,
		ing.config.DuckLake.SchemaName,
	)

	if _, err := ing.db.Exec(createSQL); err != nil {
		return fmt.Errorf("failed to create operations table: %w", err)
	}

	log.Printf("Table ready: %s.%s.operations",
		ing.config.DuckLake.CatalogName,
		ing.config.DuckLake.SchemaName)

	return nil
}

// createNativeBalancesTable creates the native_balances table
func (ing *Ingester) createNativeBalancesTable() error {
	createSQL := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s.%s.native_balances (
			account_id VARCHAR NOT NULL,
			balance BIGINT NOT NULL,
			buying_liabilities BIGINT NOT NULL,
			selling_liabilities BIGINT NOT NULL,
			num_subentries INT NOT NULL,
			num_sponsoring INT NOT NULL,
			num_sponsored INT NOT NULL,
			sequence_number BIGINT,
			last_modified_ledger BIGINT NOT NULL,
			ledger_sequence BIGINT NOT NULL,
			ledger_range BIGINT
		)`,
		ing.config.DuckLake.CatalogName,
		ing.config.DuckLake.SchemaName,
	)

	if _, err := ing.db.Exec(createSQL); err != nil {
		return fmt.Errorf("failed to create native_balances table: %w", err)
	}

	log.Printf("Table ready: %s.%s.native_balances",
		ing.config.DuckLake.CatalogName,
		ing.config.DuckLake.SchemaName)

	return nil
}

// Start begins the ingestion process
func (ing *Ingester) Start(ctx context.Context) error {
	// Start streaming ledgers
	stream, err := ing.grpcClient.StreamRawLedgers(ctx, &pb.StreamLedgersRequest{
		StartLedger: ing.config.Source.StartLedger,
	})
	if err != nil {
		return fmt.Errorf("failed to start stream: %w", err)
	}

	log.Printf("Streaming ledgers from %d", ing.config.Source.StartLedger)

	processed := 0
	startTime := time.Now()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// Receive ledger
		rawLedger, err := stream.Recv()
		if err == io.EOF {
			log.Println("Stream ended")
			return ing.flush(ctx)
		}
		if err != nil {
			return fmt.Errorf("stream error: %w", err)
		}

		// Check if we've reached the end ledger
		if ing.config.Source.EndLedger > 0 && rawLedger.Sequence >= uint32(ing.config.Source.EndLedger) {
			log.Printf("Reached end ledger %d, stopping ingestion", ing.config.Source.EndLedger)
			return ing.flush(ctx)
		}

		// Process ledger
		if err := ing.processLedger(ctx, rawLedger); err != nil {
			log.Printf("Error processing ledger %d: %v", rawLedger.Sequence, err)
			return err
		}

		processed++

		// Periodic logging
		if processed%100 == 0 {
			elapsed := time.Since(startTime)
			rate := float64(processed) / elapsed.Seconds()
			log.Printf("Processed %d ledgers (%.2f ledgers/sec)", processed, rate)
		}
	}
}

// processLedger processes a single ledger
func (ing *Ingester) processLedger(ctx context.Context, rawLedger *pb.RawLedger) error {
	// Unmarshal XDR
	var lcm xdr.LedgerCloseMeta
	if err := lcm.UnmarshalBinary(rawLedger.LedgerCloseMetaXdr); err != nil {
		return fmt.Errorf("failed to unmarshal XDR: %w", err)
	}

	// Extract ledger data
	ledgerData := ing.extractLedgerData(&lcm)

	// Get closed_at timestamp for child tables
	var closedAt time.Time
	switch lcm.V {
	case 0:
		closedAt = time.Unix(int64(lcm.MustV0().LedgerHeader.Header.ScpValue.CloseTime), 0)
	case 1:
		closedAt = time.Unix(int64(lcm.MustV1().LedgerHeader.Header.ScpValue.CloseTime), 0)
	case 2:
		closedAt = time.Unix(int64(lcm.MustV2().LedgerHeader.Header.ScpValue.CloseTime), 0)
	}

	// Extract data for all tables
	transactions := ing.extractTransactions(&lcm, closedAt)
	operations := ing.extractOperations(&lcm, closedAt)
	balances := ing.extractBalances(&lcm)

	// Add to buffers
	ing.buffers.ledgers = append(ing.buffers.ledgers, ledgerData)
	ing.buffers.transactions = append(ing.buffers.transactions, transactions...)
	ing.buffers.operations = append(ing.buffers.operations, operations...)
	ing.buffers.balances = append(ing.buffers.balances, balances...)

	// Check if we should flush (based on ledger count or time)
	shouldFlush := len(ing.buffers.ledgers) >= ing.config.DuckLake.BatchSize ||
		time.Since(ing.lastCommit) > time.Duration(ing.config.DuckLake.CommitIntervalSeconds)*time.Second

	if shouldFlush {
		return ing.flush(ctx)
	}

	return nil
}

// extractLedgerData extracts data from LedgerCloseMeta
func (ing *Ingester) extractLedgerData(lcm *xdr.LedgerCloseMeta) LedgerData {
	var data LedgerData

	// Get ledger header based on version
	var header xdr.LedgerHeaderHistoryEntry
	switch lcm.V {
	case 0:
		header = lcm.MustV0().LedgerHeader
	case 1:
		header = lcm.MustV1().LedgerHeader
	case 2:
		header = lcm.MustV2().LedgerHeader
	default:
		log.Printf("Unknown LedgerCloseMeta version: %d", lcm.V)
		return data
	}

	// Extract core fields
	data.Sequence = uint32(header.Header.LedgerSeq)
	data.LedgerHash = hex.EncodeToString(header.Hash[:])
	data.PreviousLedgerHash = hex.EncodeToString(header.Header.PreviousLedgerHash[:])
	data.ClosedAt = time.Unix(int64(header.Header.ScpValue.CloseTime), 0)
	data.ProtocolVersion = uint32(header.Header.LedgerVersion)
	data.TotalCoins = int64(header.Header.TotalCoins)
	data.FeePool = int64(header.Header.FeePool)
	data.BaseFee = uint32(header.Header.BaseFee)
	data.BaseReserve = uint32(header.Header.BaseReserve)
	data.MaxTxSetSize = uint32(header.Header.MaxTxSetSize)

	// Count transactions and operations
	// NOTE: For V1/V2, we count operations from successful transaction results
	// This approach works when TxSet phases are complex (GeneralizedTransactionSet)
	var txCount uint32
	var failedCount uint32
	var operationCount uint32
	var txSetOperationCount uint32

	switch lcm.V {
	case 0:
		v0 := lcm.MustV0()
		txCount = uint32(len(v0.TxSet.Txs))
		// V0 doesn't have tx processing results, count all ops from envelopes
		for _, tx := range v0.TxSet.Txs {
			opCount := uint32(len(tx.Operations()))
			txSetOperationCount += opCount
			operationCount += opCount // V0 doesn't track failures, count all
		}
	case 1:
		v1 := lcm.MustV1()
		txCount = uint32(len(v1.TxProcessing))

		// Count operations from transaction results
		// For each successful transaction, count the operation results
		for _, txApply := range v1.TxProcessing {
			// Try to get operation results (available for both successful and failed txs)
			if opResults, ok := txApply.Result.Result.OperationResults(); ok {
				opCount := uint32(len(opResults))
				txSetOperationCount += opCount

				// Check if transaction was successful using Successful() method
				if txApply.Result.Result.Successful() {
					operationCount += opCount
				} else {
					failedCount++
				}
			} else {
				// No operation results - likely failed before any ops executed
				failedCount++
			}
		}
	case 2:
		v2 := lcm.MustV2()
		txCount = uint32(len(v2.TxProcessing))

		// Count operations from transaction results
		for _, txApply := range v2.TxProcessing {
			// Try to get operation results (available for both successful and failed txs)
			if opResults, ok := txApply.Result.Result.OperationResults(); ok {
				opCount := uint32(len(opResults))
				txSetOperationCount += opCount

				// Check if transaction was successful using Successful() method
				if txApply.Result.Result.Successful() {
					operationCount += opCount
				} else {
					failedCount++
				}
			} else {
				// No operation results - likely failed before any ops executed
				failedCount++
			}
		}
	}

	data.TransactionCount = txCount
	data.SuccessfulTxCount = txCount - failedCount
	data.FailedTxCount = failedCount
	data.OperationCount = operationCount
	data.TxSetOperationCount = txSetOperationCount

	// Extract Soroban fields (Protocol 20+)
	// Soroban fee is available in both V1 and V2 via Ext.V1
	if lcmV1, ok := lcm.GetV1(); ok {
		if extV1, ok := lcmV1.Ext.GetV1(); ok {
			data.SorobanFeeWrite1KB = int64(extV1.SorobanFeeWrite1Kb)
		}
	} else if lcmV2, ok := lcm.GetV2(); ok {
		if extV1, ok := lcmV2.Ext.GetV1(); ok {
			data.SorobanFeeWrite1KB = int64(extV1.SorobanFeeWrite1Kb)
		}
	}

	// Extract consensus metadata (node_id and signature from SCP value)
	if lcValueSig, ok := header.Header.ScpValue.Ext.GetLcValueSignature(); ok {
		// Node ID - convert from AccountID to string address
		nodeIDBytes := lcValueSig.NodeId.Ed25519
		if nodeIDStr, err := strkey.Encode(strkey.VersionByteAccountID, nodeIDBytes[:]); err == nil {
			data.NodeID = nodeIDStr
		}
		// Signature - base64 encode the signature bytes
		data.Signature = base64.StdEncoding.EncodeToString(lcValueSig.Signature[:])
	}

	// Full ledger header as XDR (base64 encoded)
	ledgerHeaderBytes, err := header.Header.MarshalBinary()
	if err == nil {
		data.LedgerHeader = base64.StdEncoding.EncodeToString(ledgerHeaderBytes)
	}

	// Extract state tracking (Protocol 20+)
	// NOTE: Both fields use TotalByteSizeOfLiveSorobanState (matches stellar-etl behavior)
	// stellar-etl assigns the same value to both bucket_list_size and live_soroban_state_size
	if lcmV1, ok := lcm.GetV1(); ok {
		sorobanStateSize := uint64(lcmV1.TotalByteSizeOfLiveSorobanState)
		data.BucketListSize = sorobanStateSize
		data.LiveSorobanStateSize = sorobanStateSize
	} else if lcmV2, ok := lcm.GetV2(); ok {
		sorobanStateSize := uint64(lcmV2.TotalByteSizeOfLiveSorobanState)
		data.BucketListSize = sorobanStateSize
		data.LiveSorobanStateSize = sorobanStateSize
	}

	// Extract Protocol 23 (CAP-62) eviction count
	// Count of evicted ledger keys (renamed from EvictedTemporaryLedgerKeys to EvictedKeys)
	if lcmV1, ok := lcm.GetV1(); ok {
		data.EvictedKeysCount = uint32(len(lcmV1.EvictedKeys))
	} else if lcmV2, ok := lcm.GetV2(); ok {
		data.EvictedKeysCount = uint32(len(lcmV2.EvictedKeys))
	}

	// Compute partition key (10K ledger ranges)
	data.LedgerRange = (data.Sequence / 10000) * 10000

	return data
}

// extractTransactions extracts transaction data from LedgerCloseMeta
func (ing *Ingester) extractTransactions(lcm *xdr.LedgerCloseMeta, closedAt time.Time) []TransactionData {
	var transactions []TransactionData

	// Use ingest package to read transactions
	reader, err := ingest.NewLedgerTransactionReaderFromLedgerCloseMeta(ing.config.Source.NetworkPassphrase, *lcm)
	if err != nil {
		log.Printf("Failed to create transaction reader: %v", err)
		return transactions
	}
	defer reader.Close()

	// Get ledger sequence for FK
	var ledgerSeq uint32
	switch lcm.V {
	case 0:
		ledgerSeq = uint32(lcm.MustV0().LedgerHeader.Header.LedgerSeq)
	case 1:
		ledgerSeq = uint32(lcm.MustV1().LedgerHeader.Header.LedgerSeq)
	case 2:
		ledgerSeq = uint32(lcm.MustV2().LedgerHeader.Header.LedgerSeq)
	}

	// Read all transactions
	for {
		tx, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Printf("Error reading transaction: %v", err)
			continue
		}

		// Extract transaction data
		txData := TransactionData{
			LedgerSequence:        ledgerSeq,
			TransactionHash:       hex.EncodeToString(tx.Result.TransactionHash[:]),
			SourceAccount:         tx.Envelope.SourceAccount().ToAccountId().Address(),
			FeeCharged:            int64(tx.Result.Result.FeeCharged),
			MaxFee:                int64(tx.Envelope.Fee()),
			Successful:            tx.Result.Successful(),
			TransactionResultCode: tx.Result.Result.Result.Code.String(),
			OperationCount:        int32(len(tx.Envelope.Operations())),
			CreatedAt:             closedAt,
			AccountSequence:       int64(tx.Envelope.SeqNum()),
			LedgerRange:           (ledgerSeq / 10000) * 10000,
		}

		// Extract memo
		memo := tx.Envelope.Memo()
		switch memo.Type {
		case xdr.MemoTypeMemoNone:
			txData.MemoType = "none"
		case xdr.MemoTypeMemoText:
			txData.MemoType = "text"
			if text, ok := memo.GetText(); ok {
				txData.Memo = text
			}
		case xdr.MemoTypeMemoId:
			txData.MemoType = "id"
			if id, ok := memo.GetId(); ok {
				txData.Memo = fmt.Sprintf("%d", id)
			}
		case xdr.MemoTypeMemoHash:
			txData.MemoType = "hash"
			if hash, ok := memo.GetHash(); ok {
				txData.Memo = hex.EncodeToString(hash[:])
			}
		case xdr.MemoTypeMemoReturn:
			txData.MemoType = "return"
			if ret, ok := memo.GetRetHash(); ok {
				txData.Memo = hex.EncodeToString(ret[:])
			}
		}

		transactions = append(transactions, txData)
	}

	return transactions
}

// extractOperations extracts operation data from LedgerCloseMeta
// MVP version: extracts base fields + simple payment data
func (ing *Ingester) extractOperations(lcm *xdr.LedgerCloseMeta, closedAt time.Time) []OperationData {
	var operations []OperationData

	// Use ingest package to read transactions
	reader, err := ingest.NewLedgerTransactionReaderFromLedgerCloseMeta(ing.config.Source.NetworkPassphrase, *lcm)
	if err != nil {
		log.Printf("Failed to create transaction reader for operations: %v", err)
		return operations
	}
	defer reader.Close()

	// Get ledger sequence for FK
	var ledgerSeq uint32
	switch lcm.V {
	case 0:
		ledgerSeq = uint32(lcm.MustV0().LedgerHeader.Header.LedgerSeq)
	case 1:
		ledgerSeq = uint32(lcm.MustV1().LedgerHeader.Header.LedgerSeq)
	case 2:
		ledgerSeq = uint32(lcm.MustV2().LedgerHeader.Header.LedgerSeq)
	}

	// Read all transactions and their operations
	for {
		tx, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Printf("Error reading transaction for operations: %v", err)
			continue
		}

		txHash := hex.EncodeToString(tx.Result.TransactionHash[:])
		txSuccessful := tx.Result.Successful()

		// Extract each operation
		for i, op := range tx.Envelope.Operations() {
			// Get operation source account (use transaction's source if not specified)
			var opSourceAccount string
			if op.SourceAccount != nil {
				opSourceAccount = op.SourceAccount.ToAccountId().Address()
			} else {
				opSourceAccount = tx.Envelope.SourceAccount().ToAccountId().Address()
			}

			opData := OperationData{
				TransactionHash:       txHash,
				OperationIndex:        int32(i),
				LedgerSequence:        ledgerSeq,
				SourceAccount:         opSourceAccount,
				Type:                  int32(op.Body.Type),
				TypeString:            op.Body.Type.String(),
				CreatedAt:             closedAt,
				TransactionSuccessful: txSuccessful,
				LedgerRange:           (ledgerSeq / 10000) * 10000,
			}

			// Get operation result code if available
			// TODO(Cycle 4): Differentiate OperationResultCode from OperationTraceCode
			// Currently both fields hold the same value (opResults[i].Code.String()).
			// For proper stellar-etl alignment:
			// - OperationResultCode should be the category (e.g., "op_inner", "op_bad_auth")
			// - OperationTraceCode should be the specific result (e.g., "PAYMENT_SUCCESS", "PAYMENT_UNDERFUNDED")
			// Requires additional XDR parsing to extract both levels from OperationResult.
			if opResults, ok := tx.Result.Result.OperationResults(); ok && i < len(opResults) {
				opData.OperationResultCode = opResults[i].Code.String()
				// Trace code is the specific operation result (e.g., PAYMENT_SUCCESS)
				opData.OperationTraceCode = opResults[i].Code.String()
			}

			// MVP: Extract simple payment data (Payment, PathPaymentStrictSend, PathPaymentStrictReceive)
			switch op.Body.Type {
			case xdr.OperationTypePayment:
				if payment, ok := op.Body.GetPaymentOp(); ok {
					opData.PaymentTo = payment.Destination.ToAccountId().Address()
					opData.PaymentAmount = int64(payment.Amount)
				}
			case xdr.OperationTypePathPaymentStrictSend, xdr.OperationTypePathPaymentStrictReceive:
				// For MVP, we'll extract destination and amount similar to payment
				// Full implementation would extract path, source/dest assets, etc.
				// This is a simplified version for MVP
			}

			operations = append(operations, opData)
		}
	}

	return operations
}

// extractBalances extracts native XLM balance data from LedgerCloseMeta
func (ing *Ingester) extractBalances(lcm *xdr.LedgerCloseMeta) []BalanceData {
	var balances []BalanceData

	// Get ledger sequence for FK
	var ledgerSeq uint32
	switch lcm.V {
	case 0:
		ledgerSeq = uint32(lcm.MustV0().LedgerHeader.Header.LedgerSeq)
	case 1:
		ledgerSeq = uint32(lcm.MustV1().LedgerHeader.Header.LedgerSeq)
	case 2:
		ledgerSeq = uint32(lcm.MustV2().LedgerHeader.Header.LedgerSeq)
	}

	// Use ingest package to read ledger changes
	reader, err := ingest.NewLedgerChangeReaderFromLedgerCloseMeta(ing.config.Source.NetworkPassphrase, *lcm)
	if err != nil {
		log.Printf("Failed to create change reader: %v", err)
		return balances
	}
	defer reader.Close()

	// Track unique accounts (to avoid duplicates within a ledger)
	accountMap := make(map[string]*BalanceData)

	// Read all changes
	for {
		change, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Printf("Error reading change: %v", err)
			continue
		}

		// We only care about AccountEntry changes (native balances)
		if change.Type != xdr.LedgerEntryTypeAccount {
			continue
		}

		// Get the account entry after the change (post state)
		var accountEntry *xdr.AccountEntry
		if change.Post != nil {
			if ae, ok := change.Post.Data.GetAccount(); ok {
				accountEntry = &ae
			}
		}

		if accountEntry == nil {
			continue
		}

		// Extract account ID
		accountID := accountEntry.AccountId.Address()

		// Create or update balance data
		balanceData := BalanceData{
			AccountID:          accountID,
			Balance:            int64(accountEntry.Balance),
			BuyingLiabilities:  0,  // Default
			SellingLiabilities: 0,  // Default
			NumSubentries:      int32(accountEntry.NumSubEntries),
			NumSponsoring:      0,  // Default
			NumSponsored:       0,  // Default
			SequenceNumber:     int64(accountEntry.SeqNum),
			LastModifiedLedger: ledgerSeq,
			LedgerSequence:     ledgerSeq,
			LedgerRange:        (ledgerSeq / 10000) * 10000,
		}

		// Extract liabilities (Protocol 10+) and sponsorship counts (Protocol 14+)
		if ext, ok := accountEntry.Ext.GetV1(); ok {
			balanceData.BuyingLiabilities = int64(ext.Liabilities.Buying)
			balanceData.SellingLiabilities = int64(ext.Liabilities.Selling)

			// Extract sponsorship counts if available (Protocol 14+)
			if ext2, ok := ext.Ext.GetV2(); ok {
				balanceData.NumSponsoring = int32(ext2.NumSponsoring)
				balanceData.NumSponsored = int32(ext2.NumSponsored)
			}
		}

		// Store in map (overwrites if account appears multiple times)
		accountMap[accountID] = &balanceData
	}

	// Convert map to slice
	for _, balance := range accountMap {
		balances = append(balances, *balance)
	}

	return balances
}

// flush writes buffered data to DuckLake using multi-row INSERT for all 4 tables
func (ing *Ingester) flush(ctx context.Context) error {
	if len(ing.buffers.ledgers) == 0 {
		return nil
	}

	numLedgers := len(ing.buffers.ledgers)
	numTransactions := len(ing.buffers.transactions)
	numOperations := len(ing.buffers.operations)
	numBalances := len(ing.buffers.balances)

	log.Printf("[FLUSH] Starting multi-table flush (separate transactions): %d ledgers, %d transactions, %d operations, %d balances",
		numLedgers, numTransactions, numOperations, numBalances)
	flushStart := time.Now()

	// NOTE: Using separate transactions per table instead of atomic multi-table transaction
	// This avoids DuckLake catalog synchronization hang when writing multiple tables atomically
	// Trade-off: Loses atomicity, but enables multi-table ingestion to complete

	// ========================================
	// 1. INSERT INTO ledgers
	// ========================================
	if numLedgers > 0 {
		log.Printf("[FLUSH] Preparing INSERT for %d ledgers...", numLedgers)
		prepStart := time.Now()

		insertSQL := fmt.Sprintf(`
			INSERT INTO %s.%s.%s (
				sequence, ledger_hash, previous_ledger_hash, closed_at,
				protocol_version, total_coins, fee_pool, base_fee, base_reserve,
				max_tx_set_size, successful_tx_count, failed_tx_count,
				ingestion_timestamp, ledger_range,
				transaction_count, operation_count, tx_set_operation_count,
				soroban_fee_write1kb, node_id, signature, ledger_header,
				bucket_list_size, live_soroban_state_size, evicted_keys_count
			) VALUES `,
			ing.config.DuckLake.CatalogName,
			ing.config.DuckLake.SchemaName,
			ing.config.DuckLake.TableName,
		)

		valuePlaceholders := make([]string, numLedgers)
		args := make([]interface{}, 0, numLedgers*ledgerColumnCount)

		for i, ledger := range ing.buffers.ledgers {
			placeholders := make([]string, ledgerColumnCount)
			for j := range placeholders {
				placeholders[j] = "?"
			}
			valuePlaceholders[i] = "(" + strings.Join(placeholders, ",") + ")"

			args = append(args,
				ledger.Sequence,
				ledger.LedgerHash,
				ledger.PreviousLedgerHash,
				ledger.ClosedAt,
				ledger.ProtocolVersion,
				ledger.TotalCoins,
				ledger.FeePool,
				ledger.BaseFee,
				ledger.BaseReserve,
				ledger.MaxTxSetSize,
				ledger.SuccessfulTxCount,
				ledger.FailedTxCount,
				time.Now(),
				ledger.LedgerRange,
				ledger.TransactionCount,
				ledger.OperationCount,
				ledger.TxSetOperationCount,
				ledger.SorobanFeeWrite1KB,
				ledger.NodeID,
				ledger.Signature,
				ledger.LedgerHeader,
				ledger.BucketListSize,
				ledger.LiveSorobanStateSize,
				ledger.EvictedKeysCount,
			)
		}

		insertSQL += strings.Join(valuePlaceholders, ",")
		log.Printf("[FLUSH] Prepared ledgers INSERT in %v, executing...", time.Since(prepStart))

		execStart := time.Now()
		execCtx, execCancel := context.WithTimeout(ctx, 180*time.Second)
		defer execCancel()

		if _, err := ing.db.ExecContext(execCtx, insertSQL, args...); err != nil {
			if execCtx.Err() == context.DeadlineExceeded {
				return fmt.Errorf("TIMEOUT inserting %d ledgers after %v: %w", numLedgers, time.Since(execStart), err)
			}
			return fmt.Errorf("failed to insert %d ledgers after %v: %w", numLedgers, time.Since(execStart), err)
		}
		log.Printf("[FLUSH] ✓ Inserted %d ledgers in %v", numLedgers, time.Since(execStart))
	}

	// ========================================
	// 2. INSERT INTO transactions
	// ========================================
	if numTransactions > 0 {
		log.Printf("[FLUSH] Preparing INSERT for %d transactions...", numTransactions)
		prepStart := time.Now()

		insertSQL := fmt.Sprintf(`
			INSERT INTO %s.%s.transactions (
				ledger_sequence, transaction_hash, source_account,
				fee_charged, max_fee, successful, transaction_result_code,
				operation_count, memo_type, memo, created_at,
				account_sequence, ledger_range
			) VALUES `,
			ing.config.DuckLake.CatalogName,
			ing.config.DuckLake.SchemaName,
		)

		valuePlaceholders := make([]string, numTransactions)
		args := make([]interface{}, 0, numTransactions*transactionColumnCount)

		for i, tx := range ing.buffers.transactions {
			placeholders := make([]string, transactionColumnCount)
			for j := range placeholders {
				placeholders[j] = "?"
			}
			valuePlaceholders[i] = "(" + strings.Join(placeholders, ",") + ")"

			args = append(args,
				tx.LedgerSequence,
				tx.TransactionHash,
				tx.SourceAccount,
				tx.FeeCharged,
				tx.MaxFee,
				tx.Successful,
				tx.TransactionResultCode,
				tx.OperationCount,
				tx.MemoType,
				tx.Memo,
				tx.CreatedAt,
				tx.AccountSequence,
				tx.LedgerRange,
			)
		}

		insertSQL += strings.Join(valuePlaceholders, ",")
		log.Printf("[FLUSH] Prepared transactions INSERT in %v, executing...", time.Since(prepStart))

		execStart := time.Now()
		execCtx, execCancel := context.WithTimeout(ctx, 180*time.Second)
		defer execCancel()

		if _, err := ing.db.ExecContext(execCtx, insertSQL, args...); err != nil {
			if execCtx.Err() == context.DeadlineExceeded {
				return fmt.Errorf("TIMEOUT inserting %d transactions after %v: %w", numTransactions, time.Since(execStart), err)
			}
			return fmt.Errorf("failed to insert %d transactions after %v: %w", numTransactions, time.Since(execStart), err)
		}
		log.Printf("[FLUSH] ✓ Inserted %d transactions in %v", numTransactions, time.Since(execStart))
	}

	// ========================================
	// 3. INSERT INTO operations (CHUNKED for large batches)
	// ========================================
	if numOperations > 0 {
		log.Printf("[FLUSH] Preparing INSERT for %d operations...", numOperations)
		chunkStart := time.Now()

		// Split into chunks of 2000 to avoid large INSERT hangs
		const chunkSize = 2000
		totalInserted := 0

		for chunkOffset := 0; chunkOffset < numOperations; chunkOffset += chunkSize {
			chunkEnd := chunkOffset + chunkSize
			if chunkEnd > numOperations {
				chunkEnd = numOperations
			}
			chunkOps := ing.buffers.operations[chunkOffset:chunkEnd]
			chunkLen := len(chunkOps)

			log.Printf("[FLUSH] Preparing operations chunk %d-%d (%d rows)...", chunkOffset, chunkEnd-1, chunkLen)
			prepStart := time.Now()

			insertSQL := fmt.Sprintf(`
				INSERT INTO %s.%s.operations (
					transaction_hash, operation_index, ledger_sequence,
					source_account, type, type_string, created_at,
					transaction_successful, operation_result_code,
					operation_trace_code, ledger_range,
					payment_to, payment_amount
				) VALUES `,
				ing.config.DuckLake.CatalogName,
				ing.config.DuckLake.SchemaName,
			)

			valuePlaceholders := make([]string, chunkLen)
			args := make([]interface{}, 0, chunkLen*operationColumnCount)

			for i, op := range chunkOps {
				placeholders := make([]string, operationColumnCount)
				for j := range placeholders {
					placeholders[j] = "?"
				}
				valuePlaceholders[i] = "(" + strings.Join(placeholders, ",") + ")"

				args = append(args,
					op.TransactionHash,
					op.OperationIndex,
					op.LedgerSequence,
					op.SourceAccount,
					op.Type,
					op.TypeString,
					op.CreatedAt,
					op.TransactionSuccessful,
					op.OperationResultCode,
					op.OperationTraceCode,
					op.LedgerRange,
					op.PaymentTo,
					op.PaymentAmount,
				)
			}

			insertSQL += strings.Join(valuePlaceholders, ",")
			log.Printf("[FLUSH] Prepared operations chunk in %v, executing...", time.Since(prepStart))

			execStart := time.Now()
			execCtx, execCancel := context.WithTimeout(ctx, 180*time.Second)
			defer execCancel()

			if _, err := ing.db.ExecContext(execCtx, insertSQL, args...); err != nil {
				if execCtx.Err() == context.DeadlineExceeded {
					return fmt.Errorf("TIMEOUT inserting operations chunk %d-%d after %v: %w", chunkOffset, chunkEnd-1, time.Since(execStart), err)
				}
				return fmt.Errorf("failed to insert operations chunk %d-%d after %v: %w", chunkOffset, chunkEnd-1, time.Since(execStart), err)
			}
			totalInserted += chunkLen
			log.Printf("[FLUSH] ✓ Inserted operations chunk %d-%d (%d rows) in %v", chunkOffset, chunkEnd-1, chunkLen, time.Since(execStart))
		}

		log.Printf("[FLUSH] ✅ Inserted %d total operations in %v", totalInserted, time.Since(chunkStart))
	}

	// ========================================
	// 4. INSERT INTO native_balances (CHUNKED for large batches)
	// ========================================
	if numBalances > 0 {
		log.Printf("[FLUSH] Preparing INSERT for %d native_balances...", numBalances)
		chunkStart := time.Now()

		// Split into chunks of 2000 to avoid large INSERT hangs
		const chunkSize = 2000
		totalInserted := 0

		for chunkOffset := 0; chunkOffset < numBalances; chunkOffset += chunkSize {
			chunkEnd := chunkOffset + chunkSize
			if chunkEnd > numBalances {
				chunkEnd = numBalances
			}
			chunkBals := ing.buffers.balances[chunkOffset:chunkEnd]
			chunkLen := len(chunkBals)

			log.Printf("[FLUSH] Preparing native_balances chunk %d-%d (%d rows)...", chunkOffset, chunkEnd-1, chunkLen)
			prepStart := time.Now()

			insertSQL := fmt.Sprintf(`
				INSERT INTO %s.%s.native_balances (
					account_id, balance, buying_liabilities, selling_liabilities,
					num_subentries, num_sponsoring, num_sponsored, sequence_number,
					last_modified_ledger, ledger_sequence, ledger_range
				) VALUES `,
				ing.config.DuckLake.CatalogName,
				ing.config.DuckLake.SchemaName,
			)

			valuePlaceholders := make([]string, chunkLen)
			args := make([]interface{}, 0, chunkLen*balanceColumnCount)

			for i, bal := range chunkBals {
				placeholders := make([]string, balanceColumnCount)
				for j := range placeholders {
					placeholders[j] = "?"
				}
				valuePlaceholders[i] = "(" + strings.Join(placeholders, ",") + ")"

				args = append(args,
					bal.AccountID,
					bal.Balance,
					bal.BuyingLiabilities,
					bal.SellingLiabilities,
					bal.NumSubentries,
					bal.NumSponsoring,
					bal.NumSponsored,
					bal.SequenceNumber,
					bal.LastModifiedLedger,
					bal.LedgerSequence,
					bal.LedgerRange,
				)
			}

			insertSQL += strings.Join(valuePlaceholders, ",")
			log.Printf("[FLUSH] Prepared native_balances chunk in %v, executing...", time.Since(prepStart))

			execStart := time.Now()
			execCtx, execCancel := context.WithTimeout(ctx, 180*time.Second)
			defer execCancel()

			if _, err := ing.db.ExecContext(execCtx, insertSQL, args...); err != nil {
				if execCtx.Err() == context.DeadlineExceeded {
					return fmt.Errorf("TIMEOUT inserting native_balances chunk %d-%d after %v: %w", chunkOffset, chunkEnd-1, time.Since(execStart), err)
				}
				return fmt.Errorf("failed to insert native_balances chunk %d-%d after %v: %w", chunkOffset, chunkEnd-1, time.Since(execStart), err)
			}
			totalInserted += chunkLen
			log.Printf("[FLUSH] ✓ Inserted native_balances chunk %d-%d (%d rows) in %v", chunkOffset, chunkEnd-1, chunkLen, time.Since(execStart))
		}

		log.Printf("[FLUSH] ✅ Inserted %d total native_balances in %v", totalInserted, time.Since(chunkStart))
	}

	log.Printf("[FLUSH] ✅ COMPLETE: Flushed %d ledgers, %d transactions, %d operations, %d balances in %v total",
		numLedgers, numTransactions, numOperations, numBalances, time.Since(flushStart))

	// Clear all buffers
	ing.buffers.ledgers = ing.buffers.ledgers[:0]
	ing.buffers.transactions = ing.buffers.transactions[:0]
	ing.buffers.operations = ing.buffers.operations[:0]
	ing.buffers.balances = ing.buffers.balances[:0]
	ing.lastCommit = time.Now()

	return nil
}

// Close cleans up resources
func (ing *Ingester) Close() error {
	log.Println("Closing ingester...")

	// Flush remaining data
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := ing.flush(ctx); err != nil {
		log.Printf("Error flushing on close: %v", err)
	}

	// Close resources
	if ing.db != nil {
		ing.db.Close()
	}
	if ing.grpcConn != nil {
		ing.grpcConn.Close()
	}

	log.Println("Ingester closed")
	return nil
}
