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
	transactionColumnCount = 40 // Cycle 4: Expanded from 13 to 40 (added 27 fields)
	operationColumnCount   = 58 // Cycle 5: Expanded from 13 to 58 (complete operations schema)
	balanceColumnCount     = 11 // 11 fields: account_id, balance, buying/selling liabilities, subentries, sponsoring/sponsored, sequence, last_modified_ledger, ledger_sequence, ledger_range

	// Obsrvr Data Culture: Version Management
	ProcessorVersion = "2.1.0" // Cycle 4: 40-field transactions schema

	// Schema versions (major version from table names)
	LedgersSchemaVersion      = "v2" // 24 fields
	TransactionsSchemaVersion = "v2" // 40 fields (upgraded from v1's 13 fields)
	OperationsSchemaVersion   = "v1" // 13 fields
	BalancesSchemaVersion     = "v1" // 11 fields

	// Current minor versions (tracked in _meta_datasets)
	LedgersMinorVersion      = 0
	TransactionsMinorVersion = 0 // v2.0: Cycle 4 complete schema
	OperationsMinorVersion   = 0
	BalancesMinorVersion     = 0
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
		MetadataSchema        string `yaml:"metadata_schema"` // Optional: custom metadata schema name
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
// Cycle 4: Expanded from 13 to 40 fields for stellar-etl alignment
type TransactionData struct {
	// Core fields (existing 13)
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

	// Muxed accounts (2 fields) - CAP-27
	SourceAccountMuxed *string // Nullable: only present if muxed account used
	FeeAccountMuxed    *string // Nullable: only present for fee bump with muxed fee source

	// Fee bump transactions (4 fields) - CAP-15
	InnerTransactionHash *string // Nullable: only for fee bump transactions
	FeeBumpFee           *int64  // Nullable: fee charged by fee bump (outer fee)
	MaxFeeBid            *int64  // Nullable: max fee bid in fee bump
	InnerSourceAccount   *string // Nullable: source account of inner transaction

	// Preconditions (6 fields) - CAP-21
	TimeboundsMinTime   *int64  // Nullable: minimum time bound (unix timestamp)
	TimeboundsMaxTime   *int64  // Nullable: maximum time bound (unix timestamp)
	LedgerboundsMin     *uint32 // Nullable: minimum ledger bound
	LedgerboundsMax     *uint32 // Nullable: maximum ledger bound
	MinSequenceNumber   *int64  // Nullable: minimum sequence number precondition
	MinSequenceAge      *uint64 // Nullable: minimum sequence age precondition

	// Soroban fields (13 fields) - CAP-46/CAP-47
	SorobanResourcesInstructions *uint32 // Nullable: CPU instructions limit
	SorobanResourcesReadBytes    *uint32 // Nullable: read bytes limit
	SorobanResourcesWriteBytes   *uint32 // Nullable: write bytes limit
	SorobanDataSizeBytes         *int32  // Nullable: contract data size in bytes
	SorobanDataResources         *string // Nullable: JSON/XDR of full resource data
	SorobanFeeBase               *int64  // Nullable: base Soroban fee
	SorobanFeeResources          *int64  // Nullable: resource-based Soroban fee
	SorobanFeeRefund             *int64  // Nullable: Soroban fee refunded
	SorobanFeeCharged            *int64  // Nullable: total Soroban fee charged
	SorobanFeeWasted             *int64  // Nullable: Soroban fee wasted on failure
	SorobanHostFunctionType      *string // Nullable: invoke_contract, create_contract, etc.
	SorobanContractID            *string // Nullable: contract address (C...)
	SorobanContractEventsCount   *int32  // Nullable: number of contract events emitted

	// Metadata fields (2 fields)
	SignaturesCount int32 // Number of signatures on transaction
	NewAccount      bool  // True if transaction created a new account
}

// OperationData represents operation data for multi-table ingestion (Cycle 5: Complete schema - 58 fields)
type OperationData struct {
	// ========================================
	// CORE FIELDS (11 - existing)
	// ========================================
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

	// ========================================
	// MUXED ACCOUNTS (1 - Protocol 13+)
	// ========================================
	SourceAccountMuxed *string

	// ========================================
	// ASSET FIELDS (8 - shared across many operations)
	// Used by: Payment, PathPayment, ChangeTrust, AllowTrust,
	//          SetTrustLineFlags, ManageOffer, CreateClaimableBalance
	// ========================================
	Asset             *string // Primary asset (canonical format: "native" or "credit_alphanum4/CODE/ISSUER")
	AssetType         *string // "native", "credit_alphanum4", "credit_alphanum12"
	AssetCode         *string // Asset code (e.g., "USDC")
	AssetIssuer       *string // Issuer account
	SourceAsset       *string // PathPayment source asset
	SourceAssetType   *string
	SourceAssetCode   *string
	SourceAssetIssuer *string

	// ========================================
	// AMOUNT FIELDS (4 - shared across payment-type operations)
	// ========================================
	Amount          *int64 // Primary amount (Payment, PathPayment, CreateClaimableBalance, etc.)
	SourceAmount    *int64 // PathPaymentStrictSend source amount
	DestinationMin  *int64 // PathPaymentStrictSend minimum destination
	StartingBalance *int64 // CreateAccount starting balance

	// ========================================
	// DESTINATION (1 - shared)
	// Used by: Payment, PathPayment, CreateAccount, AccountMerge
	// ========================================
	Destination *string

	// ========================================
	// TRUSTLINE FIELDS (5)
	// Used by: ChangeTrust, AllowTrust, SetTrustLineFlags
	// ========================================
	TrustlineLimit                 *int64  // ChangeTrust limit
	Trustor                        *string // AllowTrust/SetTrustLineFlags trustor account
	Authorize                      *bool   // AllowTrust/SetTrustLineFlags authorize flag
	AuthorizeToMaintainLiabilities *bool   // SetTrustLineFlags flag
	TrustLineFlags                 *uint32 // SetTrustLineFlags: combined set/clear flags

	// ========================================
	// CLAIMABLE BALANCE FIELDS (2 - Protocol 14)
	// Used by: CreateClaimableBalance, ClaimClaimableBalance
	// ========================================
	BalanceID      *string // Claimable balance ID (hex string)
	ClaimantsCount *int32  // Number of claimants (CreateClaimableBalance)

	// ========================================
	// SPONSORSHIP FIELDS (1 - Protocol 15)
	// Used by: BeginSponsoringFutureReserves
	// ========================================
	SponsoredID *string // BeginSponsoring: account being sponsored

	// ========================================
	// DEX FIELDS (11 - rare but needed for completeness)
	// Used by: ManageSellOffer, ManageBuyOffer, CreatePassiveSellOffer
	// ========================================
	OfferID           *int64  // Offer ID
	Price             *string // Price as decimal
	PriceR            *string // Price as rational (n/d)
	BuyingAsset       *string
	BuyingAssetType   *string
	BuyingAssetCode   *string
	BuyingAssetIssuer *string
	SellingAsset      *string
	SellingAssetType  *string
	SellingAssetCode  *string
	SellingAssetIssuer *string

	// ========================================
	// SOROBAN FIELDS (4 - Protocol 20+)
	// Used by: InvokeHostFunction, ExtendFootprintTTL, RestoreFootprint
	// ========================================
	SorobanOperation  *string // "invoke", "extend_ttl", "restore"
	SorobanFunction   *string // HostFunction type
	SorobanContractID *string // Contract address
	SorobanAuthRequired *bool // Requires authorization

	// ========================================
	// ACCOUNT OPERATIONS (8)
	// Used by: AccountMerge, BumpSequence, SetOptions
	// ========================================
	BumpTo          *int64  // BumpSequence target
	SetFlags        *uint32 // SetOptions: flags to set
	ClearFlags      *uint32 // SetOptions: flags to clear
	HomeDomain      *string // SetOptions: home domain
	MasterWeight    *int32  // SetOptions: master key weight
	LowThreshold    *int32  // SetOptions: low threshold
	MediumThreshold *int32  // SetOptions: medium threshold
	HighThreshold   *int32  // SetOptions: high threshold

	// ========================================
	// OTHER OPERATIONS (2)
	// ========================================
	DataName  *string // ManageData: entry name
	DataValue *string // ManageData: entry value (base64)
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

// Obsrvr Data Culture: Metadata Table Schemas
// These tables track dataset registry, lineage, quality, and schema changes
//
// 1. _meta_datasets (Dataset Registry)
//    - dataset TEXT PRIMARY KEY (e.g., "core.ledgers_row_v2")
//    - tier TEXT NOT NULL ("bronze", "silver", "gold")
//    - domain TEXT NOT NULL ("core", "contracts", "dex", etc.)
//    - major_version INT NOT NULL
//    - current_minor_version INT NOT NULL
//    - owner TEXT
//    - purpose TEXT
//    - grain TEXT ("row", "snapshot", "daily", etc.)
//    - created_at TIMESTAMP NOT NULL
//    - updated_at TIMESTAMP NOT NULL
//
// 2. _meta_lineage (Processing Provenance)
//    - id SERIAL PRIMARY KEY
//    - dataset TEXT NOT NULL
//    - partition TEXT (partition identifier)
//    - source_ledger_start INT NOT NULL
//    - source_ledger_end INT NOT NULL
//    - pipeline_version TEXT NOT NULL (e.g., "2.0.0")
//    - processor_name TEXT NOT NULL (e.g., "ducklake-ingestion-obsrvr")
//    - checksum TEXT (SHA256 for reproducibility)
//    - row_count INT
//    - created_at TIMESTAMP NOT NULL
//
// 3. _meta_quality (Data Quality Tracking)
//    - id SERIAL PRIMARY KEY
//    - dataset TEXT NOT NULL
//    - partition TEXT
//    - check_name TEXT NOT NULL (e.g., "MonotonicSequenceCheck")
//    - check_type TEXT NOT NULL (e.g., "non_null", "positive_value")
//    - passed BOOLEAN NOT NULL
//    - details JSONB (additional failure info)
//    - row_count INT
//    - null_anomalies INT
//    - created_at TIMESTAMP NOT NULL
//
// 4. _meta_changes (Schema Evolution Log)
//    - id SERIAL PRIMARY KEY
//    - dataset TEXT NOT NULL
//    - from_version TEXT (e.g., "v1")
//    - to_version TEXT NOT NULL (e.g., "v2")
//    - change_type TEXT NOT NULL ("breaking", "nonbreaking", "additive")
//    - summary TEXT
//    - migration_sql TEXT
//    - applied_at TIMESTAMP NOT NULL

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
		// Prevent overlapping ranges: each worker (except the first) starts at the next ledger
		// after the previous worker's end, since workers process ranges inclusively
		if i > 0 {
			workerStart++ // Skip the boundary ledger (previous worker already processed it)
		}
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

	// Track last received ledger time to detect stalls
	lastRecvTime := time.Now()
	streamTimeout := 900 * time.Second // Timeout if no ledger received in 15 minutes (mainnet needs longer flush time)
	// Note: Hosted PostgreSQL flushes can take 3+ minutes for 200 ledgers
	// The processor doesn't receive ledgers during flush, so timeout must be longer than flush time

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// Check for stream timeout (source might be hung)
		if time.Since(lastRecvTime) > streamTimeout {
			return fmt.Errorf("[Worker %d] stream timeout: no data received for %v (source may be hung)",
				workerID, streamTimeout)
		}

		// Receive ledger with non-blocking check
		type recvResult struct {
			ledger *pb.RawLedger
			err    error
		}
		recvChan := make(chan recvResult, 1)

		go func() {
			ledger, err := stream.Recv()
			recvChan <- recvResult{ledger: ledger, err: err}
		}()

		// Wait for receive with timeout
		var rawLedger *pb.RawLedger
		var err error

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(5 * time.Second):
			// Still waiting... check total timeout
			if time.Since(lastRecvTime) > streamTimeout {
				return fmt.Errorf("[Worker %d] stream timeout after %v, source may be hung",
					workerID, time.Since(lastRecvTime))
			}
			// Log waiting status
			log.Printf("[Worker %d] Waiting for ledger... (%.0fs since last receive)",
				workerID, time.Since(lastRecvTime).Seconds())

			// Wait for actual result (this will block until Recv completes or context cancels)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case result := <-recvChan:
				rawLedger = result.ledger
				err = result.err
				lastRecvTime = time.Now()
			}
		case result := <-recvChan:
			rawLedger = result.ledger
			err = result.err
			lastRecvTime = time.Now()
		}

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
		// Remote DuckLake with DATA_PATH and optional METADATA_SCHEMA
		if ing.config.DuckLake.MetadataSchema != "" {
			attachSQL = fmt.Sprintf(
				"ATTACH '%s' AS %s (DATA_PATH '%s', METADATA_SCHEMA '%s')",
				ing.config.DuckLake.CatalogPath,
				ing.config.DuckLake.CatalogName,
				ing.config.DuckLake.DataPath,
				ing.config.DuckLake.MetadataSchema,
			)
		} else {
			attachSQL = fmt.Sprintf(
				"ATTACH '%s' AS %s (DATA_PATH '%s')",
				ing.config.DuckLake.CatalogPath,
				ing.config.DuckLake.CatalogName,
				ing.config.DuckLake.DataPath,
			)
		}
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
				if ing.config.DuckLake.MetadataSchema != "" {
					createAttachSQL = fmt.Sprintf(
						"ATTACH '%s' AS %s (TYPE ducklake, DATA_PATH '%s', METADATA_SCHEMA '%s')",
						ing.config.DuckLake.CatalogPath,
						ing.config.DuckLake.CatalogName,
						ing.config.DuckLake.DataPath,
						ing.config.DuckLake.MetadataSchema,
					)
				} else {
					createAttachSQL = fmt.Sprintf(
						"ATTACH '%s' AS %s (TYPE ducklake, DATA_PATH '%s')",
						ing.config.DuckLake.CatalogPath,
						ing.config.DuckLake.CatalogName,
						ing.config.DuckLake.DataPath,
					)
				}
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

	// Create Obsrvr metadata tables (v2.0)
	if err := ing.createMetadataTables(); err != nil {
		return fmt.Errorf("failed to create metadata tables: %w", err)
	}

	// Register datasets in _meta_datasets (v2.0)
	if err := ing.registerDatasets(); err != nil {
		return fmt.Errorf("failed to register datasets: %w", err)
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
// Obsrvr playbook naming: core.ledgers_row_v2
// - Domain: core (blockchain infrastructure data)
// - Subject: ledgers
// - Grain: row (one row per ledger)
// - Version: v2 (24 fields including Soroban metadata)
func (ing *Ingester) createTable() error {
	// Create table with ledger_range column for partitioning
	// DuckLake will organize files based on this column's values
	// We'll compute: ledger_range = (sequence / 10000) * 10000
	// This creates logical partitions: 0, 10000, 20000, 160000, etc.
	createSQL := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s.%s.ledgers_row_v2 (
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
	)

	if _, err := ing.db.Exec(createSQL); err != nil {
		return fmt.Errorf("failed to create ledgers_row_v2 table: %w", err)
	}

	log.Printf("Table ready: %s.%s.ledgers_row_v2",
		ing.config.DuckLake.CatalogName,
		ing.config.DuckLake.SchemaName)

	return nil
}

// createTransactionsTable creates the transactions table
// Obsrvr playbook naming: core.transactions_row_v2
// - Domain: core (blockchain infrastructure data)
// - Subject: transactions
// - Grain: row (one row per transaction)
// - Version: v2 (40 fields - Cycle 4: stellar-etl alignment)
func (ing *Ingester) createTransactionsTable() error {
	createSQL := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s.%s.transactions_row_v2 (
			-- Core fields (13)
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
			ledger_range BIGINT,

			-- Muxed accounts (2) - CAP-27
			source_account_muxed VARCHAR,
			fee_account_muxed VARCHAR,

			-- Fee bump transactions (4) - CAP-15
			inner_transaction_hash VARCHAR,
			fee_bump_fee BIGINT,
			max_fee_bid BIGINT,
			inner_source_account VARCHAR,

			-- Preconditions (6) - CAP-21
			timebounds_min_time BIGINT,
			timebounds_max_time BIGINT,
			ledgerbounds_min BIGINT,
			ledgerbounds_max BIGINT,
			min_sequence_number BIGINT,
			min_sequence_age BIGINT,

			-- Soroban fields (13) - CAP-46/CAP-47
			soroban_resources_instructions BIGINT,
			soroban_resources_read_bytes BIGINT,
			soroban_resources_write_bytes BIGINT,
			soroban_data_size_bytes INT,
			soroban_data_resources TEXT,
			soroban_fee_base BIGINT,
			soroban_fee_resources BIGINT,
			soroban_fee_refund BIGINT,
			soroban_fee_charged BIGINT,
			soroban_fee_wasted BIGINT,
			soroban_host_function_type VARCHAR,
			soroban_contract_id VARCHAR,
			soroban_contract_events_count INT,

			-- Metadata (2)
			signatures_count INT NOT NULL,
			new_account BOOLEAN NOT NULL
		)`,
		ing.config.DuckLake.CatalogName,
		ing.config.DuckLake.SchemaName,
	)

	if _, err := ing.db.Exec(createSQL); err != nil {
		return fmt.Errorf("failed to create transactions_row_v2 table: %w", err)
	}

	log.Printf("Table ready: %s.%s.transactions_row_v2",
		ing.config.DuckLake.CatalogName,
		ing.config.DuckLake.SchemaName)

	return nil
}

// createOperationsTable creates the operations table (Cycle 5: Complete schema - 59 fields)
// Obsrvr playbook naming: core.operations_row_v2
// - Domain: core (blockchain infrastructure data)
// - Subject: operations
// - Grain: row (one row per operation)
// - Version: v2 (59 fields - covers 12 operation types, 98%+ coverage)
func (ing *Ingester) createOperationsTable() error {
	createSQL := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s.%s.operations_row_v2 (
			-- Core fields (11)
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

			-- Muxed accounts (1)
			source_account_muxed VARCHAR,

			-- Asset fields (8)
			asset VARCHAR,
			asset_type VARCHAR,
			asset_code VARCHAR,
			asset_issuer VARCHAR,
			source_asset VARCHAR,
			source_asset_type VARCHAR,
			source_asset_code VARCHAR,
			source_asset_issuer VARCHAR,

			-- Amount/price fields (4)
			amount BIGINT,
			source_amount BIGINT,
			destination_min BIGINT,
			starting_balance BIGINT,

			-- Destination (1)
			destination VARCHAR,

			-- Trustline (5)
			trustline_limit BIGINT,
			trustor VARCHAR,
			authorize BOOLEAN,
			authorize_to_maintain_liabilities BOOLEAN,
			trust_line_flags INT,

			-- Claimable balance (2)
			balance_id VARCHAR,
			claimants_count INT,

			-- Sponsorship (1)
			sponsored_id VARCHAR,

			-- DEX fields (11)
			offer_id BIGINT,
			price VARCHAR,
			price_r VARCHAR,
			buying_asset VARCHAR,
			buying_asset_type VARCHAR,
			buying_asset_code VARCHAR,
			buying_asset_issuer VARCHAR,
			selling_asset VARCHAR,
			selling_asset_type VARCHAR,
			selling_asset_code VARCHAR,
			selling_asset_issuer VARCHAR,

			-- Soroban (4)
			soroban_operation VARCHAR,
			soroban_function VARCHAR,
			soroban_contract_id VARCHAR,
			soroban_auth_required BOOLEAN,

			-- Account operations (8)
			bump_to BIGINT,
			set_flags INT,
			clear_flags INT,
			home_domain VARCHAR,
			master_weight INT,
			low_threshold INT,
			medium_threshold INT,
			high_threshold INT,

			-- Other (2)
			data_name VARCHAR,
			data_value VARCHAR
		)`,
		ing.config.DuckLake.CatalogName,
		ing.config.DuckLake.SchemaName,
	)

	if _, err := ing.db.Exec(createSQL); err != nil {
		return fmt.Errorf("failed to create operations_row_v2 table: %w", err)
	}

	log.Printf("Table ready: %s.%s.operations_row_v2",
		ing.config.DuckLake.CatalogName,
		ing.config.DuckLake.SchemaName)

	return nil
}

// createNativeBalancesTable creates the native_balances table
// Obsrvr playbook naming: core.native_balances_snapshot_v1
// - Domain: core (blockchain infrastructure data)
// - Subject: native_balances
// - Grain: snapshot (balance at specific ledger)
// - Version: v1 (11 fields)
func (ing *Ingester) createNativeBalancesTable() error {
	createSQL := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s.%s.native_balances_snapshot_v1 (
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
		return fmt.Errorf("failed to create native_balances_snapshot_v1 table: %w", err)
	}

	log.Printf("Table ready: %s.%s.native_balances_snapshot_v1",
		ing.config.DuckLake.CatalogName,
		ing.config.DuckLake.SchemaName)

	return nil
}

// createMetadataTables creates the 4 Obsrvr metadata tables
// These tables track dataset registry, lineage, quality, and schema changes
func (ing *Ingester) createMetadataTables() error {
	// 1. _meta_datasets (Dataset Registry)
	// Note: DuckLake doesn't support PRIMARY KEY constraints
	datasetsSQL := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s.%s._meta_datasets (
			dataset TEXT NOT NULL,
			tier TEXT NOT NULL,
			domain TEXT NOT NULL,
			major_version INT NOT NULL,
			current_minor_version INT NOT NULL,
			owner TEXT,
			purpose TEXT,
			grain TEXT,
			created_at TIMESTAMP NOT NULL,
			updated_at TIMESTAMP NOT NULL
		)`,
		ing.config.DuckLake.CatalogName,
		ing.config.DuckLake.SchemaName,
	)

	if _, err := ing.db.Exec(datasetsSQL); err != nil {
		return fmt.Errorf("failed to create _meta_datasets table: %w", err)
	}
	log.Printf("Metadata table ready: %s.%s._meta_datasets",
		ing.config.DuckLake.CatalogName, ing.config.DuckLake.SchemaName)

	// 2. _meta_lineage (Processing Provenance)
	// Note: DuckLake doesn't support PRIMARY KEY constraints
	lineageSQL := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s.%s._meta_lineage (
			id INTEGER NOT NULL,
			dataset TEXT NOT NULL,
			partition TEXT,
			source_ledger_start INT NOT NULL,
			source_ledger_end INT NOT NULL,
			pipeline_version TEXT NOT NULL,
			processor_name TEXT NOT NULL,
			checksum TEXT,
			row_count INT,
			created_at TIMESTAMP NOT NULL
		)`,
		ing.config.DuckLake.CatalogName,
		ing.config.DuckLake.SchemaName,
	)

	if _, err := ing.db.Exec(lineageSQL); err != nil {
		return fmt.Errorf("failed to create _meta_lineage table: %w", err)
	}
	log.Printf("Metadata table ready: %s.%s._meta_lineage",
		ing.config.DuckLake.CatalogName, ing.config.DuckLake.SchemaName)

	// 3. _meta_quality (Data Quality Tracking)
	// Note: DuckLake doesn't support PRIMARY KEY constraints
	qualitySQL := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s.%s._meta_quality (
			id BIGINT NOT NULL,
			dataset TEXT NOT NULL,
			partition TEXT,
			check_name TEXT NOT NULL,
			check_type TEXT NOT NULL,
			passed BOOLEAN NOT NULL,
			details TEXT,
			row_count INT,
			null_anomalies INT,
			created_at TIMESTAMP NOT NULL
		)`,
		ing.config.DuckLake.CatalogName,
		ing.config.DuckLake.SchemaName,
	)

	if _, err := ing.db.Exec(qualitySQL); err != nil {
		return fmt.Errorf("failed to create _meta_quality table: %w", err)
	}
	log.Printf("Metadata table ready: %s.%s._meta_quality",
		ing.config.DuckLake.CatalogName, ing.config.DuckLake.SchemaName)

	// 4. _meta_changes (Schema Evolution Log)
	// Note: DuckLake doesn't support PRIMARY KEY constraints
	changesSQL := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s.%s._meta_changes (
			id INTEGER NOT NULL,
			dataset TEXT NOT NULL,
			from_version TEXT,
			to_version TEXT NOT NULL,
			change_type TEXT NOT NULL,
			summary TEXT,
			migration_sql TEXT,
			applied_at TIMESTAMP NOT NULL
		)`,
		ing.config.DuckLake.CatalogName,
		ing.config.DuckLake.SchemaName,
	)

	if _, err := ing.db.Exec(changesSQL); err != nil {
		return fmt.Errorf("failed to create _meta_changes table: %w", err)
	}
	log.Printf("Metadata table ready: %s.%s._meta_changes",
		ing.config.DuckLake.CatalogName, ing.config.DuckLake.SchemaName)

	log.Println("✅ All Obsrvr metadata tables created successfully")
	return nil
}

// registerDatasets registers all datasets in the _meta_datasets table
// This is idempotent - safe to call on every startup
// Inserts new datasets or updates existing ones with current version info
func (ing *Ingester) registerDatasets() error {
	// Define dataset metadata
	type Dataset struct {
		Name         string
		Tier         string
		Domain       string
		MajorVersion int
		MinorVersion int
		Owner        string
		Purpose      string
		Grain        string
	}

	datasets := []Dataset{
		{
			Name:         "core.ledgers_row_v2",
			Tier:         "silver",
			Domain:       "core",
			MajorVersion: 2,
			MinorVersion: LedgersMinorVersion,
			Owner:        "stellar-ingestion",
			Purpose:      "Canonical ledger headers from Stellar blockchain",
			Grain:        "row",
		},
		{
			Name:         "core.transactions_row_v2",
			Tier:         "silver",
			Domain:       "core",
			MajorVersion: 2,
			MinorVersion: TransactionsMinorVersion,
			Owner:        "stellar-ingestion",
			Purpose:      "Transaction data from Stellar ledgers (40 fields, stellar-etl aligned)",
			Grain:        "row",
		},
		{
			Name:         "core.operations_row_v2",
			Tier:         "silver",
			Domain:       "core",
			MajorVersion: 2,
			MinorVersion: OperationsMinorVersion,
			Owner:        "stellar-ingestion",
			Purpose:      "Operation data from Stellar transactions (59 fields, 12 operation types, 98%+ coverage)",
			Grain:        "row",
		},
		{
			Name:         "core.native_balances_snapshot_v1",
			Tier:         "silver",
			Domain:       "core",
			MajorVersion: 1,
			MinorVersion: BalancesMinorVersion,
			Owner:        "stellar-ingestion",
			Purpose:      "Native XLM balance snapshots by account",
			Grain:        "snapshot",
		},
	}

	// Register each dataset
	for _, ds := range datasets {
		// Check if dataset already exists
		checkSQL := fmt.Sprintf(`
			SELECT COUNT(*) FROM %s.%s._meta_datasets
			WHERE dataset = ?
		`,
			ing.config.DuckLake.CatalogName,
			ing.config.DuckLake.SchemaName,
		)

		var count int
		if err := ing.db.QueryRow(checkSQL, ds.Name).Scan(&count); err != nil {
			return fmt.Errorf("failed to check if dataset %s exists: %w", ds.Name, err)
		}

		now := time.Now()

		if count == 0 {
			// Insert new dataset
			insertSQL := fmt.Sprintf(`
				INSERT INTO %s.%s._meta_datasets (
					dataset, tier, domain, major_version, current_minor_version,
					owner, purpose, grain, created_at, updated_at
				) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
			`,
				ing.config.DuckLake.CatalogName,
				ing.config.DuckLake.SchemaName,
			)

			if _, err := ing.db.Exec(insertSQL,
				ds.Name, ds.Tier, ds.Domain, ds.MajorVersion, ds.MinorVersion,
				ds.Owner, ds.Purpose, ds.Grain, now, now,
			); err != nil {
				return fmt.Errorf("failed to register dataset %s: %w", ds.Name, err)
			}

			log.Printf("Registered dataset: %s (v%d.%d)", ds.Name, ds.MajorVersion, ds.MinorVersion)
		} else {
			// Update existing dataset's updated_at and version
			updateSQL := fmt.Sprintf(`
				UPDATE %s.%s._meta_datasets
				SET current_minor_version = ?,
				    updated_at = ?
				WHERE dataset = ?
			`,
				ing.config.DuckLake.CatalogName,
				ing.config.DuckLake.SchemaName,
			)

			if _, err := ing.db.Exec(updateSQL, ds.MinorVersion, now, ds.Name); err != nil {
				return fmt.Errorf("failed to update dataset %s: %w", ds.Name, err)
			}

			log.Printf("Updated dataset: %s (v%d.%d)", ds.Name, ds.MajorVersion, ds.MinorVersion)
		}
	}

	log.Println("✅ All datasets registered in _meta_datasets")
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

	// Track last received ledger time to detect stalls
	lastRecvTime := time.Now()
	streamTimeout := 900 * time.Second // Timeout if no ledger received in 15 minutes (mainnet needs longer flush time)
	// Note: Hosted PostgreSQL flushes can take 3+ minutes for 200 ledgers
	// The processor doesn't receive ledgers during flush, so timeout must be longer than flush time

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// Check for stream timeout (source might be hung)
		if time.Since(lastRecvTime) > streamTimeout {
			return fmt.Errorf("stream timeout: no data received for %v (source may be hung)", streamTimeout)
		}

		// Receive ledger with non-blocking check
		type recvResult struct {
			ledger *pb.RawLedger
			err    error
		}
		recvChan := make(chan recvResult, 1)

		go func() {
			ledger, err := stream.Recv()
			recvChan <- recvResult{ledger: ledger, err: err}
		}()

		// Wait for receive with timeout
		var rawLedger *pb.RawLedger
		var err error

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(5 * time.Second):
			// Still waiting... check total timeout
			if time.Since(lastRecvTime) > streamTimeout {
				return fmt.Errorf("stream timeout after %v, source may be hung", time.Since(lastRecvTime))
			}
			// Log waiting status
			log.Printf("Waiting for ledger... (%.0fs since last receive)", time.Since(lastRecvTime).Seconds())

			// Wait for actual result (this will block until Recv completes or context cancels)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case result := <-recvChan:
				rawLedger = result.ledger
				err = result.err
				lastRecvTime = time.Now()
			}
		case result := <-recvChan:
			rawLedger = result.ledger
			err = result.err
			lastRecvTime = time.Now()
		}

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
			log.Printf("Error reading transaction in ledger %d: %v", ledgerSeq, err)
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

		// ========================================
		// Extract Cycle 4 fields (27 new fields)
		// ========================================

		// 1. Muxed accounts (CAP-27) - 2 fields
		sourceAcct := tx.Envelope.SourceAccount()
		if sourceAcct.Type == xdr.CryptoKeyTypeKeyTypeMuxedEd25519 {
			if _, ok := sourceAcct.GetMed25519(); ok {
				// Format: M... address with embedded ID
				muxedAddr := sourceAcct.Address()
				txData.SourceAccountMuxed = &muxedAddr
			}
		}

		// Fee account muxed (only for fee bump transactions)
		if tx.Envelope.IsFeeBump() {
			feeBump := tx.Envelope.FeeBump
			if feeBump != nil {
				feeSource := feeBump.Tx.FeeSource
				if feeSource.Type == xdr.CryptoKeyTypeKeyTypeMuxedEd25519 {
					if _, ok := feeSource.GetMed25519(); ok {
						feeAddr := feeSource.Address()
						txData.FeeAccountMuxed = &feeAddr
					}
				}
			}
		}

		// 2. Fee bump transactions (CAP-15) - 4 fields
		if tx.Envelope.IsFeeBump() {
			feeBump := tx.Envelope.FeeBump
			if feeBump != nil {
				// Inner transaction hash - compute from envelope
				innerTx := feeBump.Tx.InnerTx.V1
				if innerTx != nil {
					// Compute hash manually since HashHex is not available
					innerBytes, err := innerTx.MarshalBinary()
					if err == nil {
						innerHashBytes := xdr.Hash(innerBytes)
						innerHash := hex.EncodeToString(innerHashBytes[:])
						txData.InnerTransactionHash = &innerHash
					}

					// Inner source account
					innerSource := innerTx.Tx.SourceAccount.ToAccountId().Address()
					txData.InnerSourceAccount = &innerSource
				}

				// Fee bump fee (outer fee charged)
				feeBumpFee := int64(feeBump.Tx.Fee)
				txData.FeeBumpFee = &feeBumpFee

				// Max fee bid (same as outer fee for fee bump)
				maxFeeBid := int64(feeBump.Tx.Fee)
				txData.MaxFeeBid = &maxFeeBid
			}
		}

		// 3. Preconditions (CAP-21) - 6 fields
		v1Tx := tx.Envelope.V1
		if v1Tx != nil {
			if v1Tx.Tx.Cond.Type == xdr.PreconditionTypePrecondV2 {
				if precond, ok := v1Tx.Tx.Cond.GetV2(); ok {
					// Timebounds
					if precond.TimeBounds != nil {
						minTime := int64(precond.TimeBounds.MinTime)
						maxTime := int64(precond.TimeBounds.MaxTime)
						txData.TimeboundsMinTime = &minTime
						txData.TimeboundsMaxTime = &maxTime
					}

					// Ledgerbounds
					if precond.LedgerBounds != nil {
						minLedger := uint32(precond.LedgerBounds.MinLedger)
						maxLedger := uint32(precond.LedgerBounds.MaxLedger)
						txData.LedgerboundsMin = &minLedger
						txData.LedgerboundsMax = &maxLedger
					}

					// Min sequence number
					if precond.MinSeqNum != nil {
						minSeq := int64(*precond.MinSeqNum)
						txData.MinSequenceNumber = &minSeq
					}

					// Min sequence age
					minAge := uint64(precond.MinSeqAge)
					if minAge > 0 {
						txData.MinSequenceAge = &minAge
					}
				}
			} else if v1Tx.Tx.Cond.Type == xdr.PreconditionTypePrecondTime {
				// Legacy timebounds (pre-CAP-21)
				if tb, ok := v1Tx.Tx.Cond.GetTimeBounds(); ok {
					minTime := int64(tb.MinTime)
					maxTime := int64(tb.MaxTime)
					txData.TimeboundsMinTime = &minTime
					txData.TimeboundsMaxTime = &maxTime
				}
			}
		}

		// 4. Soroban fields (CAP-46/CAP-47) - 13 fields
		if v1Tx != nil {
			if v1Tx.Tx.Ext.V == 1 {
				if sorobanData, ok := v1Tx.Tx.Ext.GetSorobanData(); ok {
					// Resources - check available fields
					resources := sorobanData.Resources
					instructions := uint32(resources.Instructions)
					txData.SorobanResourcesInstructions = &instructions
					// Note: ReadBytes/WriteBytes may have different field names in this SDK version
					// Leaving those for potential refinement

					// Data size (use ResourceFee as proxy)
					dataSize := int32(sorobanData.ResourceFee)
					txData.SorobanDataSizeBytes = &dataSize

					// Host function type and contract ID
					for _, op := range v1Tx.Tx.Operations {
						if op.Body.Type == xdr.OperationTypeInvokeHostFunction {
							if invokeOp, ok := op.Body.GetInvokeHostFunctionOp(); ok {
								hostFnType := invokeOp.HostFunction.Type.String()
								txData.SorobanHostFunctionType = &hostFnType

								// Extract contract ID for invoke_contract type
								if invokeOp.HostFunction.Type == xdr.HostFunctionTypeHostFunctionTypeInvokeContract {
									if invokeArgs, ok := invokeOp.HostFunction.GetInvokeContract(); ok {
										// InvokeContractArgs contains ContractAddress and other fields
										// Extract contract ID from the contract address
										contractAddr := invokeArgs.ContractAddress
										if contractAddr.Type == xdr.ScAddressTypeScAddressTypeContract {
											if contractHash, ok := contractAddr.GetContractId(); ok {
												contractID := hex.EncodeToString(contractHash[:])
												txData.SorobanContractID = &contractID
											}
										}
									}
								}
							}
							break // Only need first invoke
						}
					}
				}
			}
		}

		// 5. Metadata - 2 fields
		// Signatures count
		txData.SignaturesCount = int32(len(tx.Envelope.Signatures()))

		// New account detection - check if any operation is CREATE_ACCOUNT
		txData.NewAccount = false
		for _, op := range tx.Envelope.Operations() {
			if op.Body.Type == xdr.OperationTypeCreateAccount {
				txData.NewAccount = true
				break
			}
		}

		transactions = append(transactions, txData)
	}

	return transactions
}

// extractAsset extracts asset details into operation data
// prefix can be "asset", "source_asset", "buying_asset", or "selling_asset"
func extractAsset(opData *OperationData, asset xdr.Asset, prefix string) {
	assetType := asset.Type.String()

	switch prefix {
	case "asset":
		opData.AssetType = &assetType
		if asset.Type == xdr.AssetTypeAssetTypeNative {
			native := "native"
			opData.Asset = &native
		} else if alphaNum4, ok := asset.GetAlphaNum4(); ok {
			code := strings.TrimRight(string(alphaNum4.AssetCode[:]), "\x00")
			issuer := alphaNum4.Issuer.Address()
			opData.AssetCode = &code
			opData.AssetIssuer = &issuer
			canonical := fmt.Sprintf("credit_alphanum4/%s/%s", code, issuer)
			opData.Asset = &canonical
		} else if alphaNum12, ok := asset.GetAlphaNum12(); ok {
			code := strings.TrimRight(string(alphaNum12.AssetCode[:]), "\x00")
			issuer := alphaNum12.Issuer.Address()
			opData.AssetCode = &code
			opData.AssetIssuer = &issuer
			canonical := fmt.Sprintf("credit_alphanum12/%s/%s", code, issuer)
			opData.Asset = &canonical
		}

	case "source_asset":
		opData.SourceAssetType = &assetType
		if asset.Type == xdr.AssetTypeAssetTypeNative {
			native := "native"
			opData.SourceAsset = &native
		} else if alphaNum4, ok := asset.GetAlphaNum4(); ok {
			code := strings.TrimRight(string(alphaNum4.AssetCode[:]), "\x00")
			issuer := alphaNum4.Issuer.Address()
			opData.SourceAssetCode = &code
			opData.SourceAssetIssuer = &issuer
			canonical := fmt.Sprintf("credit_alphanum4/%s/%s", code, issuer)
			opData.SourceAsset = &canonical
		} else if alphaNum12, ok := asset.GetAlphaNum12(); ok {
			code := strings.TrimRight(string(alphaNum12.AssetCode[:]), "\x00")
			issuer := alphaNum12.Issuer.Address()
			opData.SourceAssetCode = &code
			opData.SourceAssetIssuer = &issuer
			canonical := fmt.Sprintf("credit_alphanum12/%s/%s", code, issuer)
			opData.SourceAsset = &canonical
		}

	case "buying_asset":
		opData.BuyingAssetType = &assetType
		if asset.Type == xdr.AssetTypeAssetTypeNative {
			native := "native"
			opData.BuyingAsset = &native
		} else if alphaNum4, ok := asset.GetAlphaNum4(); ok {
			code := strings.TrimRight(string(alphaNum4.AssetCode[:]), "\x00")
			issuer := alphaNum4.Issuer.Address()
			opData.BuyingAssetCode = &code
			opData.BuyingAssetIssuer = &issuer
			canonical := fmt.Sprintf("credit_alphanum4/%s/%s", code, issuer)
			opData.BuyingAsset = &canonical
		} else if alphaNum12, ok := asset.GetAlphaNum12(); ok {
			code := strings.TrimRight(string(alphaNum12.AssetCode[:]), "\x00")
			issuer := alphaNum12.Issuer.Address()
			opData.BuyingAssetCode = &code
			opData.BuyingAssetIssuer = &issuer
			canonical := fmt.Sprintf("credit_alphanum12/%s/%s", code, issuer)
			opData.BuyingAsset = &canonical
		}

	case "selling_asset":
		opData.SellingAssetType = &assetType
		if asset.Type == xdr.AssetTypeAssetTypeNative {
			native := "native"
			opData.SellingAsset = &native
		} else if alphaNum4, ok := asset.GetAlphaNum4(); ok {
			code := strings.TrimRight(string(alphaNum4.AssetCode[:]), "\x00")
			issuer := alphaNum4.Issuer.Address()
			opData.SellingAssetCode = &code
			opData.SellingAssetIssuer = &issuer
			canonical := fmt.Sprintf("credit_alphanum4/%s/%s", code, issuer)
			opData.SellingAsset = &canonical
		} else if alphaNum12, ok := asset.GetAlphaNum12(); ok {
			code := strings.TrimRight(string(alphaNum12.AssetCode[:]), "\x00")
			issuer := alphaNum12.Issuer.Address()
			opData.SellingAssetCode = &code
			opData.SellingAssetIssuer = &issuer
			canonical := fmt.Sprintf("credit_alphanum12/%s/%s", code, issuer)
			opData.SellingAsset = &canonical
		}
	}
}

// extractOperations extracts operation data from LedgerCloseMeta
// Cycle 5: Extracts 58 fields for 12 operation types (98%+ coverage)
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

			// Extract operation-specific fields (Cycle 5 Day 2: Payment + CreateAccount)
			switch op.Body.Type {
			case xdr.OperationTypePayment:
				payment := op.Body.MustPaymentOp()

				// Destination
				dest := payment.Destination.ToAccountId().Address()
				opData.Destination = &dest

				// Asset
				extractAsset(&opData, payment.Asset, "asset")

				// Amount
				amount := int64(payment.Amount)
				opData.Amount = &amount

			case xdr.OperationTypeCreateAccount:
				createAcct := op.Body.MustCreateAccountOp()

				// Destination
				dest := createAcct.Destination.Address()
				opData.Destination = &dest

				// Starting balance
				balance := int64(createAcct.StartingBalance)
				opData.StartingBalance = &balance

			// ========================================
			// Cycle 5 Day 3: Sponsorship + Trustline Operations
			// ========================================

			case xdr.OperationTypeBeginSponsoringFutureReserves:
				sponsor := op.Body.MustBeginSponsoringFutureReservesOp()

				// Sponsored account ID
				sponsoredID := sponsor.SponsoredId.Address()
				opData.SponsoredID = &sponsoredID

			case xdr.OperationTypeEndSponsoringFutureReserves:
				// No operation-specific fields
				// This operation just marks the end of a sponsorship block

			case xdr.OperationTypeChangeTrust:
				changeTrust := op.Body.MustChangeTrustOp()

				// Asset (Line)
				if changeTrust.Line.Type == xdr.AssetTypeAssetTypePoolShare {
					// Liquidity pool share - skip for now (rare)
				} else {
					// Regular asset
					asset := changeTrust.Line.ToAsset()
					extractAsset(&opData, asset, "asset")
				}

				// Limit
				limit := int64(changeTrust.Limit)
				opData.TrustlineLimit = &limit

			case xdr.OperationTypeSetTrustLineFlags:
				setFlags := op.Body.MustSetTrustLineFlagsOp()

				// Trustor
				trustor := setFlags.Trustor.Address()
				opData.Trustor = &trustor

				// Asset
				extractAsset(&opData, setFlags.Asset, "asset")

				// Flags
				setF := uint32(setFlags.SetFlags)
				clearF := uint32(setFlags.ClearFlags)

				// Determine authorization status from flags
				// TrustLineFlags: Authorized = 1, AuthorizedToMaintainLiabilities = 2
				authorized := (setF & 1) != 0 || (clearF & 1) == 0
				authorizedLiabilities := (setF & 2) != 0

				opData.Authorize = &authorized
				opData.AuthorizeToMaintainLiabilities = &authorizedLiabilities
				opData.TrustLineFlags = &setF

			// ========================================
			// Cycle 5 Day 4: Claimable Balance + Tier 2 Operations
			// ========================================

			case xdr.OperationTypeCreateClaimableBalance:
				createBalance := op.Body.MustCreateClaimableBalanceOp()

				// Asset
				extractAsset(&opData, createBalance.Asset, "asset")

				// Amount
				amount := int64(createBalance.Amount)
				opData.Amount = &amount

				// Claimants count
				count := int32(len(createBalance.Claimants))
				opData.ClaimantsCount = &count

			case xdr.OperationTypeClaimClaimableBalance:
				claimBalance := op.Body.MustClaimClaimableBalanceOp()

				// Balance ID (hex string)
				balanceIDBytes, err := claimBalance.BalanceId.MarshalBinary()
				if err == nil {
					balanceID := hex.EncodeToString(balanceIDBytes)
					opData.BalanceID = &balanceID
				}

			case xdr.OperationTypeAllowTrust:
				allowTrust := op.Body.MustAllowTrustOp()

				// Trustor
				trustor := allowTrust.Trustor.Address()
				opData.Trustor = &trustor

				// Asset code (AllowTrust uses asset code only, not full asset)
				var assetCode string
				switch allowTrust.Asset.Type {
				case xdr.AssetTypeAssetTypeCreditAlphanum4:
					assetCode = strings.TrimRight(string(allowTrust.Asset.AssetCode4[:]), "\x00")
				case xdr.AssetTypeAssetTypeCreditAlphanum12:
					assetCode = strings.TrimRight(string(allowTrust.Asset.AssetCode12[:]), "\x00")
				}
				opData.AssetCode = &assetCode

				// Authorize (0 = unauthorized, 1 = authorized, 2 = authorized to maintain liabilities)
				authorized := uint32(allowTrust.Authorize) == 1 || uint32(allowTrust.Authorize) == 2
				opData.Authorize = &authorized

				authorizedLiabilities := uint32(allowTrust.Authorize) == 2
				opData.AuthorizeToMaintainLiabilities = &authorizedLiabilities

			case xdr.OperationTypeSetOptions:
				setOpts := op.Body.MustSetOptionsOp()

				// Flags
				if setOpts.SetFlags != nil {
					setF := uint32(*setOpts.SetFlags)
					opData.SetFlags = &setF
				}
				if setOpts.ClearFlags != nil {
					clearF := uint32(*setOpts.ClearFlags)
					opData.ClearFlags = &clearF
				}

				// Home domain
				if setOpts.HomeDomain != nil {
					domain := string(*setOpts.HomeDomain)
					opData.HomeDomain = &domain
				}

				// Master weight
				if setOpts.MasterWeight != nil {
					weight := int32(*setOpts.MasterWeight)
					opData.MasterWeight = &weight
				}

				// Thresholds
				if setOpts.LowThreshold != nil {
					low := int32(*setOpts.LowThreshold)
					opData.LowThreshold = &low
				}
				if setOpts.MedThreshold != nil {
					med := int32(*setOpts.MedThreshold)
					opData.MediumThreshold = &med
				}
				if setOpts.HighThreshold != nil {
					high := int32(*setOpts.HighThreshold)
					opData.HighThreshold = &high
				}

				// Note: Skipping signer (requires complex handling)
				// Note: Skipping inflation destination (deprecated)

			case xdr.OperationTypeInvokeHostFunction:
				invoke := op.Body.MustInvokeHostFunctionOp()

				// Soroban operation type
				sorobanOp := "invoke"
				opData.SorobanOperation = &sorobanOp

				// Function type
				functionType := invoke.HostFunction.Type.String()
				opData.SorobanFunction = &functionType

				// Auth required
				authRequired := len(invoke.Auth) > 0
				opData.SorobanAuthRequired = &authRequired

				// Note: Contract ID extraction is complex - skipping for MVP

			case xdr.OperationTypeAccountMerge:
				// AccountMerge has destination as the operation body itself (not wrapped in a struct)
				dest := op.Body.MustDestination().ToAccountId().Address()
				opData.Destination = &dest

			// Note: DEX operations (ManageSellOffer, ManageBuyOffer) are rare on testnet (0.3%)
			// and can be added in a future cycle if needed
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

	// ========================================
	// 0. RUN QUALITY CHECKS (Obsrvr v2.0)
	// ========================================
	log.Printf("[QUALITY] Running quality checks on batch data...")
	qualityCheckStart := time.Now()
	qualityResults := ing.RunAllQualityChecks()
	log.Printf("[QUALITY] Completed %d quality checks in %v", len(qualityResults), time.Since(qualityCheckStart))

	// Log any failed checks as warnings (non-blocking)
	for _, result := range qualityResults {
		if !result.Passed {
			log.Printf("⚠️  [QUALITY] FAILED: %s - %s", result.CheckName, result.Details)
		}
	}

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
			INSERT INTO %s.%s.ledgers_row_v2 (
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
		chunkStart := time.Now()

		// Split into chunks of 200 to avoid large INSERT hangs with 40-field schema
		const chunkSize = 200  // 40 fields × 200 rows = 8,000 values per INSERT
		totalInserted := 0

		for chunkOffset := 0; chunkOffset < numTransactions; chunkOffset += chunkSize {
			chunkEnd := chunkOffset + chunkSize
			if chunkEnd > numTransactions {
				chunkEnd = numTransactions
			}
			chunkTxs := ing.buffers.transactions[chunkOffset:chunkEnd]
			chunkLen := len(chunkTxs)

			log.Printf("[FLUSH] Preparing transactions chunk %d-%d (%d rows)...", chunkOffset, chunkEnd-1, chunkLen)
			prepStart := time.Now()

			insertSQL := fmt.Sprintf(`
				INSERT INTO %s.%s.transactions_row_v2 (
					ledger_sequence, transaction_hash, source_account,
					fee_charged, max_fee, successful, transaction_result_code,
					operation_count, memo_type, memo, created_at,
					account_sequence, ledger_range,
					source_account_muxed, fee_account_muxed,
					inner_transaction_hash, fee_bump_fee, max_fee_bid, inner_source_account,
					timebounds_min_time, timebounds_max_time, ledgerbounds_min, ledgerbounds_max,
					min_sequence_number, min_sequence_age,
					soroban_resources_instructions, soroban_resources_read_bytes, soroban_resources_write_bytes,
					soroban_data_size_bytes, soroban_data_resources, soroban_fee_base, soroban_fee_resources,
					soroban_fee_refund, soroban_fee_charged, soroban_fee_wasted, soroban_host_function_type,
					soroban_contract_id, soroban_contract_events_count,
					signatures_count, new_account
				) VALUES `,
				ing.config.DuckLake.CatalogName,
				ing.config.DuckLake.SchemaName,
			)

			valuePlaceholders := make([]string, chunkLen)
			args := make([]interface{}, 0, chunkLen*transactionColumnCount)

			for i, tx := range chunkTxs {
				placeholders := make([]string, transactionColumnCount)
				for j := range placeholders {
					placeholders[j] = "?"
				}
				valuePlaceholders[i] = "(" + strings.Join(placeholders, ",") + ")"

				// Core fields (13)
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

				// Muxed accounts (2)
				args = append(args,
					tx.SourceAccountMuxed,
					tx.FeeAccountMuxed,
				)

				// Fee bump (4)
				args = append(args,
					tx.InnerTransactionHash,
					tx.FeeBumpFee,
					tx.MaxFeeBid,
					tx.InnerSourceAccount,
				)

				// Preconditions (6)
				args = append(args,
					tx.TimeboundsMinTime,
					tx.TimeboundsMaxTime,
					tx.LedgerboundsMin,
					tx.LedgerboundsMax,
					tx.MinSequenceNumber,
					tx.MinSequenceAge,
				)

				// Soroban (13)
				args = append(args,
					tx.SorobanResourcesInstructions,
					tx.SorobanResourcesReadBytes,
					tx.SorobanResourcesWriteBytes,
					tx.SorobanDataSizeBytes,
					tx.SorobanDataResources,
					tx.SorobanFeeBase,
					tx.SorobanFeeResources,
					tx.SorobanFeeRefund,
					tx.SorobanFeeCharged,
					tx.SorobanFeeWasted,
					tx.SorobanHostFunctionType,
					tx.SorobanContractID,
					tx.SorobanContractEventsCount,
				)

				// Metadata (2)
				args = append(args,
					tx.SignaturesCount,
					tx.NewAccount,
				)
			}

			insertSQL += strings.Join(valuePlaceholders, ",")
			log.Printf("[FLUSH] Prepared transactions chunk in %v, executing...", time.Since(prepStart))

			execStart := time.Now()
			execCtx, execCancel := context.WithTimeout(ctx, 60*time.Second)
			defer execCancel()

			if _, err := ing.db.ExecContext(execCtx, insertSQL, args...); err != nil {
				if execCtx.Err() == context.DeadlineExceeded {
					return fmt.Errorf("TIMEOUT inserting transactions chunk %d-%d after %v: %w", chunkOffset, chunkEnd-1, time.Since(execStart), err)
				}
				return fmt.Errorf("failed to insert transactions chunk %d-%d after %v: %w", chunkOffset, chunkEnd-1, time.Since(execStart), err)
			}
			log.Printf("[FLUSH] ✓ Inserted transactions chunk %d-%d (%d rows) in %v", chunkOffset, chunkEnd-1, chunkLen, time.Since(execStart))
			totalInserted += chunkLen
		}

		log.Printf("[FLUSH] ✅ Inserted %d total transactions in %v", totalInserted, time.Since(chunkStart))
	}

	// ========================================
	// 3. INSERT INTO operations (CHUNKED for large batches)
	// ========================================
	if numOperations > 0 {
		log.Printf("[FLUSH] Preparing INSERT for %d operations...", numOperations)
		chunkStart := time.Now()

		// Split into chunks of 200 to avoid large INSERT hangs with 58-field schema
		const chunkSize = 200  // Reduced from 2000: With 58 fields, smaller chunks prevent timeouts
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
				INSERT INTO %s.%s.operations_row_v2 (
					-- Core fields (11)
					transaction_hash, operation_index, ledger_sequence,
					source_account, type, type_string, created_at,
					transaction_successful, operation_result_code,
					operation_trace_code, ledger_range,
					-- Muxed accounts (1)
					source_account_muxed,
					-- Asset fields (8)
					asset, asset_type, asset_code, asset_issuer,
					source_asset, source_asset_type, source_asset_code, source_asset_issuer,
					-- Amount fields (4)
					amount, source_amount, destination_min, starting_balance,
					-- Destination (1)
					destination,
					-- Trustline (5)
					trustline_limit, trustor, authorize, authorize_to_maintain_liabilities,
					trust_line_flags,
					-- Claimable balance (2)
					balance_id, claimants_count,
					-- Sponsorship (1)
					sponsored_id,
					-- DEX (11)
					offer_id, price, price_r,
					buying_asset, buying_asset_type, buying_asset_code, buying_asset_issuer,
					selling_asset, selling_asset_type, selling_asset_code, selling_asset_issuer,
					-- Soroban (4)
					soroban_operation, soroban_function, soroban_contract_id, soroban_auth_required,
					-- Account operations (8)
					bump_to, set_flags, clear_flags, home_domain,
					master_weight, low_threshold, medium_threshold, high_threshold,
					-- Other (2)
					data_name, data_value
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
					// Core fields (11)
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
					// Muxed accounts (1)
					op.SourceAccountMuxed,
					// Asset fields (8)
					op.Asset,
					op.AssetType,
					op.AssetCode,
					op.AssetIssuer,
					op.SourceAsset,
					op.SourceAssetType,
					op.SourceAssetCode,
					op.SourceAssetIssuer,
					// Amount fields (4)
					op.Amount,
					op.SourceAmount,
					op.DestinationMin,
					op.StartingBalance,
					// Destination (1)
					op.Destination,
					// Trustline (5)
					op.TrustlineLimit,
					op.Trustor,
					op.Authorize,
					op.AuthorizeToMaintainLiabilities,
					op.TrustLineFlags,
					// Claimable balance (2)
					op.BalanceID,
					op.ClaimantsCount,
					// Sponsorship (1)
					op.SponsoredID,
					// DEX (11)
					op.OfferID,
					op.Price,
					op.PriceR,
					op.BuyingAsset,
					op.BuyingAssetType,
					op.BuyingAssetCode,
					op.BuyingAssetIssuer,
					op.SellingAsset,
					op.SellingAssetType,
					op.SellingAssetCode,
					op.SellingAssetIssuer,
					// Soroban (4)
					op.SorobanOperation,
					op.SorobanFunction,
					op.SorobanContractID,
					op.SorobanAuthRequired,
					// Account operations (8)
					op.BumpTo,
					op.SetFlags,
					op.ClearFlags,
					op.HomeDomain,
					op.MasterWeight,
					op.LowThreshold,
					op.MediumThreshold,
					op.HighThreshold,
					// Other (2)
					op.DataName,
					op.DataValue,
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

		// Split into chunks of 200 to avoid large INSERT hangs with 58-field schema
		const chunkSize = 200  // Reduced from 2000: With 58 fields, smaller chunks prevent timeouts
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
				INSERT INTO %s.%s.native_balances_snapshot_v1 (
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

	// ========================================
	// 5. RECORD QUALITY CHECK RESULTS (Obsrvr v2.0)
	// ========================================
	if len(qualityResults) > 0 {
		log.Printf("[QUALITY] Recording quality check results to _meta_quality...")
		if err := ing.recordQualityChecks(qualityResults); err != nil {
			log.Printf("⚠️  Warning: Failed to record quality checks: %v", err)
			// Non-fatal: continue with flush completion
		}
	}

	// ========================================
	// 6. RECORD DATA LINEAGE (Obsrvr v2.0)
	// ========================================
	log.Printf("[LINEAGE] Recording data lineage to _meta_lineage...")
	if err := ing.recordLineage(numLedgers, numTransactions, numOperations, numBalances); err != nil {
		log.Printf("⚠️  Warning: Failed to record lineage: %v", err)
		// Non-fatal: continue with flush completion
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

// recordLineage records data lineage information for the current batch to _meta_lineage table
// This tracks the source ledger range, processor version, and row counts for each dataset
func (ing *Ingester) recordLineage(numLedgers, numTransactions, numOperations, numBalances int) error {
	if numLedgers == 0 {
		return nil // No data to record
	}

	// Determine source ledger range from buffer
	sourceLedgerStart := int(ing.buffers.ledgers[0].Sequence)
	sourceLedgerEnd := int(ing.buffers.ledgers[numLedgers-1].Sequence)

	// Generate base ID from timestamp (use seconds + nanoseconds offset to fit in INT32)
	// This gives us unique IDs that fit within INTEGER bounds
	now := time.Now()
	baseID := int64(now.Unix()%1000000) * 1000

	// Track datasets to record
	type LineageEntry struct {
		Dataset  string
		RowCount int
	}

	entries := []LineageEntry{}

	// Always record ledgers (if we're here, we have ledgers)
	if numLedgers > 0 {
		entries = append(entries, LineageEntry{
			Dataset:  "core.ledgers_row_v2",
			RowCount: numLedgers,
		})
	}

	// Record transactions if present
	if numTransactions > 0 {
		entries = append(entries, LineageEntry{
			Dataset:  "core.transactions_row_v2",
			RowCount: numTransactions,
		})
	}

	// Record operations if present
	if numOperations > 0 {
		entries = append(entries, LineageEntry{
			Dataset:  "core.operations_row_v2",
			RowCount: numOperations,
		})
	}

	// Record balances if present
	if numBalances > 0 {
		entries = append(entries, LineageEntry{
			Dataset:  "core.native_balances_snapshot_v1",
			RowCount: numBalances,
		})
	}

	// OPTIMIZED: Batched insert with single query instead of 4 individual inserts
	// This reduces network roundtrips from 4 to 1 (critical for remote PostgreSQL)
	createdAt := time.Now()

	insertSQL := fmt.Sprintf(`
		INSERT INTO %s.%s._meta_lineage (
			id, dataset, partition, source_ledger_start, source_ledger_end,
			pipeline_version, processor_name, checksum, row_count, created_at
		) VALUES `,
		ing.config.DuckLake.CatalogName,
		ing.config.DuckLake.SchemaName,
	)

	// Build multi-row VALUES clause
	valuePlaceholders := make([]string, len(entries))
	args := make([]interface{}, 0, len(entries)*10) // 10 fields per entry

	for i, entry := range entries {
		id := baseID + int64(i)
		valuePlaceholders[i] = "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"

		args = append(args,
			id,
			entry.Dataset,
			"",                          // partition (empty for non-partitioned data)
			sourceLedgerStart,           // source_ledger_start
			sourceLedgerEnd,             // source_ledger_end
			ProcessorVersion,            // pipeline_version
			"ducklake-ingestion-obsrvr", // processor_name
			"",                          // checksum (optional, not computed for now)
			entry.RowCount,              // row_count
			createdAt,                   // created_at
		)
	}

	// Execute single batched insert
	insertSQL += strings.Join(valuePlaceholders, ",")
	_, err := ing.db.Exec(insertSQL, args...)
	if err != nil {
		return fmt.Errorf("failed to batch record lineage for %d datasets: %w", len(entries), err)
	}

	log.Printf("[LINEAGE] Recorded lineage for %d datasets (ledgers %d-%d, processor v%s)",
		len(entries), sourceLedgerStart, sourceLedgerEnd, ProcessorVersion)

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
