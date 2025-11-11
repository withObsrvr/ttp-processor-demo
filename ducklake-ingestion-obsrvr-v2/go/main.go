package main

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
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

	duckdb "github.com/marcboeker/go-duckdb/v2"
	_ "github.com/marcboeker/go-duckdb/v2"
	"github.com/stellar/go/ingest"
	"github.com/stellar/go/strkey"
	"github.com/stellar/go/xdr"
	pb "github.com/withObsrvr/ttp-processor-demo/stellar-live-source-datalake/go/gen/raw_ledger_service"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"gopkg.in/yaml.v3"
)

const (
	// Column counts for multi-table ingestion
	ledgerColumnCount      = 24
	transactionColumnCount = 46 // Cycle 6: Expanded from 40 to 46 (added 4 XDR + 2 signer fields)
	operationColumnCount   = 58 // Cycle 5: Expanded from 13 to 58 (complete operations schema)
	balanceColumnCount     = 11 // 11 fields: account_id, balance, buying/selling liabilities, subentries, sponsoring/sponsored, sequence, last_modified_ledger, ledger_sequence, ledger_range
	effectColumnCount      = 25 // Cycle 8: Effects table (state changes from operations)
	tradeColumnCount       = 17 // Cycle 8: Trades table (DEX trade executions)

	// Obsrvr Data Culture: Version Management
	ProcessorVersion = "2.3.0" // Cycle 8: Effects & Trades tables

	// Schema versions (major version from table names)
	LedgersSchemaVersion      = "v2" // 24 fields
	TransactionsSchemaVersion = "v2" // 40 fields (upgraded from v1's 13 fields)
	OperationsSchemaVersion   = "v1" // 13 fields
	BalancesSchemaVersion     = "v1" // 11 fields
	EffectsSchemaVersion      = "v1" // Cycle 8: 25 fields
	TradesSchemaVersion       = "v1" // Cycle 8: 17 fields

	// Current minor versions (tracked in _meta_datasets)
	LedgersMinorVersion      = 0
	TransactionsMinorVersion = 0 // v2.0: Cycle 4 complete schema
	OperationsMinorVersion   = 0
	BalancesMinorVersion     = 0
	EffectsMinorVersion      = 0 // Cycle 8: v1.0
	TradesMinorVersion       = 0 // Cycle 8: v1.0
	AccountsMinorVersion     = 0 // Cycle 9: v1.0
	TrustlinesMinorVersion   = 0 // Cycle 9: v1.0
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

	// Cycle 6: XDR fields (4 fields) - Complete transaction reconstruction
	TxEnvelope *string // Nullable: Full transaction envelope (base64 XDR)
	TxResult   *string // Nullable: Transaction result (base64 XDR)
	TxMeta     *string // Nullable: Transaction metadata (base64 XDR)
	TxFeeMeta  *string // Nullable: Fee metadata (base64 XDR)

	// Cycle 6: Signer fields (2 fields) - Multi-sig analysis
	TxSigners    *string // Nullable: JSON array of all signer public keys
	ExtraSigners *string // Nullable: JSON array of extra signers (not source account)
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

// EffectData represents state changes from operations (Cycle 8)
// Extracted from transaction meta - the "what actually happened" layer
type EffectData struct {
	// Identity (4 fields)
	LedgerSequence  uint32
	TransactionHash string
	OperationIndex  int32
	EffectIndex     int32 // Order within operation (0-based)

	// Effect type (2 fields)
	EffectType       int32  // Numeric type code (0-33)
	EffectTypeString string // Human-readable (account_credited, account_debited, etc.)

	// Account affected (1 field)
	AccountID *string // Nullable: which account changed (not all effects have accounts)

	// Amount changes (4 fields) - Nullable
	Amount      *string // Amount changed (decimal string)
	AssetCode   *string // Asset code (XLM, USDC, etc.) - NULL for native
	AssetIssuer *string // Asset issuer - NULL for native
	AssetType   *string // "native", "credit_alphanum4", "credit_alphanum12"

	// Trustline effects (3 fields) - Nullable
	TrustlineLimit *string // Trustline limit (decimal string)
	AuthorizeFlag  *bool   // Authorized/deauthorized
	ClawbackFlag   *bool   // Clawback enabled/disabled

	// Signer effects (2 fields) - Nullable
	SignerAccount *string // Signer added/removed
	SignerWeight  *int32  // Signer weight

	// Offer effects (3 fields) - Nullable
	OfferID       *int64  // Offer created/removed/updated
	SellerAccount *string // Offer seller

	// Metadata (3 fields)
	CreatedAt   time.Time // When ingested
	LedgerRange uint32    // Data partition key
}

// TradeData represents DEX trade executions (Cycle 8)
// Extracted from trade effects in transaction meta
type TradeData struct {
	// Identity (4 fields)
	LedgerSequence  uint32
	TransactionHash string
	OperationIndex  int32
	TradeIndex      int32 // Order within operation (0-based)

	// Trade details (2 fields)
	TradeType      string    // "orderbook", "liquidity_pool"
	TradeTimestamp time.Time // ledger closed_at

	// Seller side (4 fields)
	SellerAccount      string  // Seller account ID
	SellingAssetCode   *string // Asset sold - NULL for native
	SellingAssetIssuer *string // Issuer of asset sold - NULL for native
	SellingAmount      string  // Amount sold (decimal string)

	// Buyer side (4 fields)
	BuyerAccount      string  // Buyer account ID (or liquidity pool)
	BuyingAssetCode   *string // Asset bought - NULL for native
	BuyingAssetIssuer *string // Issuer of asset bought - NULL for native
	BuyingAmount      string  // Amount bought (decimal string)

	// Price (1 field)
	Price string // selling_amount / buying_amount (decimal string)

	// Metadata (2 fields)
	CreatedAt   time.Time // When ingested
	LedgerRange uint32    // Data partition key
}

// AccountData represents a single account snapshot (Cycle 9)
// Obsrvr playbook: core.accounts_snapshot_v1
type AccountData struct {
	// Identity (3 fields)
	AccountID       string    // Account public key
	LedgerSequence  uint32    // Ledger when snapshot taken
	ClosedAt        time.Time // Ledger close time

	// Balance (1 field)
	Balance string // Native XLM balance in stroops

	// Account Settings (5 fields)
	SequenceNumber uint64  // Account sequence number
	NumSubentries  uint32  // Count of trustlines + offers + signers + data
	NumSponsoring  uint32  // Number of entries this account sponsors
	NumSponsored   uint32  // Number of entries sponsored for this account
	HomeDomain     *string // Account home domain (nullable)

	// Thresholds (4 fields)
	MasterWeight  uint32 // Master key weight
	LowThreshold  uint32 // Low threshold
	MedThreshold  uint32 // Medium threshold
	HighThreshold uint32 // High threshold

	// Flags (5 fields)
	Flags              uint32 // Raw flags bitmask
	AuthRequired       bool   // Authorization required flag
	AuthRevocable      bool   // Authorization revocable flag
	AuthImmutable      bool   // Authorization immutable flag
	AuthClawbackEnabled bool  // Clawback enabled flag

	// Signers (1 field)
	Signers *string // JSON array of signers: [{"key": "...", "weight": N}]

	// Sponsorship (1 field)
	SponsorAccount *string // Account that sponsors this account (nullable)

	// Metadata (3 fields)
	CreatedAt   time.Time // When ingested
	UpdatedAt   time.Time // Last updated
	LedgerRange uint32    // Data partition key (ledger / 10000 * 10000)
}

// TrustlineData represents a single trustline snapshot (Cycle 9)
// Obsrvr playbook: core.trustlines_snapshot_v1
type TrustlineData struct {
	// Identity (4 fields)
	AccountID   string // Account that trusts the asset
	AssetCode   string // Asset code (e.g., "USDC")
	AssetIssuer string // Asset issuer account
	AssetType   string // Asset type (credit_alphanum4, credit_alphanum12)

	// Trust & Balance (4 fields)
	Balance            string // Current asset balance
	TrustLimit         string // Maximum trusted amount
	BuyingLiabilities  string // Liabilities for buying
	SellingLiabilities string // Liabilities for selling

	// Authorization (3 fields)
	Authorized                        bool // Trustline is authorized
	AuthorizedToMaintainLiabilities   bool // Can maintain liabilities
	ClawbackEnabled                   bool // Clawback is enabled

	// Metadata (3 fields)
	LedgerSequence uint32    // Ledger when snapshot taken
	CreatedAt      time.Time // When ingested
	LedgerRange    uint32    // Data partition key
}

// WorkerBuffers holds buffers for all tables for a single worker
type WorkerBuffers struct {
	ledgers       []LedgerData
	transactions  []TransactionData
	operations    []OperationData
	balances      []BalanceData
	effects       []EffectData    // Cycle 8: Effects table
	trades        []TradeData     // Cycle 8: Trades table
	accounts      []AccountData   // Cycle 9: Accounts snapshot
	trustlines    []TrustlineData // Cycle 9: Trustlines snapshot
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

	// V2: Dual connection model - sql.DB for DDL/queries, native conn for Appender API
	connector   *duckdb.Connector  // Shared connector for both connections
	db          *sql.DB              // Keep for DDL, queries, and metadata operations
	conn        *duckdb.Conn         // Native connection for Appender API

	// V2: Appenders for high-performance inserts
	ledgerAppender      *duckdb.Appender
	transactionAppender *duckdb.Appender
	operationAppender   *duckdb.Appender
	balanceAppender     *duckdb.Appender
	effectAppender      *duckdb.Appender  // Cycle 8: Effects appender
	tradeAppender       *duckdb.Appender  // Cycle 8: Trades appender
	accountAppender     *duckdb.Appender  // Cycle 9: Accounts appender
	trustlineAppender   *duckdb.Appender  // Cycle 9: Trustlines appender

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
	// V2: Create shared connector for both sql.DB and native connection
	connector, err := duckdb.NewConnector("", nil)
	if err != nil {
		return fmt.Errorf("failed to create DuckDB connector: %w", err)
	}
	ing.connector = connector

	// Create sql.DB from shared connector
	ing.db = sql.OpenDB(connector)

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

	// Create effects table (Cycle 8)
	if err := ing.createEffectsTable(); err != nil {
		return fmt.Errorf("failed to create effects table: %w", err)
	}

	// Create trades table (Cycle 8)
	if err := ing.createTradesTable(); err != nil {
		return fmt.Errorf("failed to create trades table: %w", err)
	}

	// Create accounts table (Cycle 9)
	if err := ing.createAccountsTable(); err != nil {
		return fmt.Errorf("failed to create accounts table: %w", err)
	}

	// Create trustlines table (Cycle 9)
	if err := ing.createTrustlinesTable(); err != nil {
		return fmt.Errorf("failed to create trustlines table: %w", err)
	}

	// Create Obsrvr metadata tables (v2.0)
	if err := ing.createMetadataTables(); err != nil {
		return fmt.Errorf("failed to create metadata tables: %w", err)
	}

	// Register datasets in _meta_datasets (v2.0)
	if err := ing.registerDatasets(); err != nil {
		return fmt.Errorf("failed to register datasets: %w", err)
	}

	// V2: Get native connection from shared connector for Appender API
	conn, err := ing.connector.Connect(context.Background())
	if err != nil {
		return fmt.Errorf("failed to get native connection: %w", err)
	}
	duckConn, ok := conn.(*duckdb.Conn)
	if !ok {
		return fmt.Errorf("failed to cast to *duckdb.Conn")
	}
	ing.conn = duckConn

	// V2: CRITICAL - Execute USE statement on native connection
	// Appender requires current catalog/schema to be set via USE statement
	// Then pass empty string as schema parameter to Appender
	_, err = ing.conn.ExecContext(
		context.Background(),
		useSQL, // Reuse the same USE statement from above
		nil,
	)
	if err != nil {
		return fmt.Errorf("failed to set schema on native connection: %w", err)
	}

	// V2: Initialize Appenders for high-performance inserts
	// Use empty string for schema since we set it with USE statement
	// IMPORTANT: Use actual table names with version suffixes
	ing.ledgerAppender, err = duckdb.NewAppenderFromConn(ing.conn, "", "ledgers_row_v2")
	if err != nil {
		return fmt.Errorf("failed to create ledger appender: %w", err)
	}

	ing.transactionAppender, err = duckdb.NewAppenderFromConn(ing.conn, "", "transactions_row_v2")
	if err != nil {
		return fmt.Errorf("failed to create transaction appender: %w", err)
	}

	ing.operationAppender, err = duckdb.NewAppenderFromConn(ing.conn, "", "operations_row_v2")
	if err != nil {
		return fmt.Errorf("failed to create operation appender: %w", err)
	}

	ing.balanceAppender, err = duckdb.NewAppenderFromConn(ing.conn, "", "native_balances_snapshot_v1")
	if err != nil {
		return fmt.Errorf("failed to create balance appender: %w", err)
	}

	// Cycle 8: Effects appender
	ing.effectAppender, err = duckdb.NewAppenderFromConn(ing.conn, "", "effects_row_v1")
	if err != nil {
		return fmt.Errorf("failed to create effect appender: %w", err)
	}

	// Cycle 8: Trades appender
	ing.tradeAppender, err = duckdb.NewAppenderFromConn(ing.conn, "", "trades_row_v1")
	if err != nil {
		return fmt.Errorf("failed to create trade appender: %w", err)
	}

	// Cycle 9: Accounts appender
	ing.accountAppender, err = duckdb.NewAppenderFromConn(ing.conn, "", "accounts_snapshot_v1")
	if err != nil {
		return fmt.Errorf("failed to create account appender: %w", err)
	}

	// Cycle 9: Trustlines appender
	ing.trustlineAppender, err = duckdb.NewAppenderFromConn(ing.conn, "", "trustlines_snapshot_v1")
	if err != nil {
		return fmt.Errorf("failed to create trustline appender: %w", err)
	}

	log.Println("V2: Appenders initialized for all 8 tables (ledgers_row_v2, transactions_row_v2, operations_row_v2, native_balances_snapshot_v1, effects_row_v1, trades_row_v1, accounts_snapshot_v1, trustlines_snapshot_v1)")

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
		{
			Name:         "core.effects_row_v1",
			Tier:         "silver",
			Domain:       "core",
			MajorVersion: 1,
			MinorVersion: EffectsMinorVersion,
			Owner:        "stellar-ingestion",
			Purpose:      "State change effects from operations (15 effect types, supports V0-V4 meta)",
			Grain:        "row",
		},
		{
			Name:         "core.trades_row_v1",
			Tier:         "silver",
			Domain:       "core",
			MajorVersion: 1,
			MinorVersion: TradesMinorVersion,
			Owner:        "stellar-ingestion",
			Purpose:      "DEX trade executions from orderbook (5 operation types, supports V0-V4 meta)",
			Grain:        "row",
		},
		{
			Name:         "core.accounts_snapshot_v1",
			Tier:         "silver",
			Domain:       "core",
			MajorVersion: 1,
			MinorVersion: AccountsMinorVersion,
			Owner:        "stellar-ingestion",
			Purpose:      "Complete account state snapshots including flags, thresholds, signers, and sponsorship",
			Grain:        "snapshot",
		},
		{
			Name:         "core.trustlines_snapshot_v1",
			Tier:         "silver",
			Domain:       "core",
			MajorVersion: 1,
			MinorVersion: TrustlinesMinorVersion,
			Owner:        "stellar-ingestion",
			Purpose:      "Non-native asset holdings with trust limits, liabilities, and authorization flags",
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
	effects := ing.extractEffectsForLedger(&lcm, closedAt)   // Cycle 8
	trades := ing.extractTradesForLedger(&lcm, closedAt)     // Cycle 8
	accounts := ing.extractAccounts(&lcm)                     // Cycle 9
	trustlines := ing.extractTrustlines(&lcm)                 // Cycle 9

	// Add to buffers
	ing.buffers.ledgers = append(ing.buffers.ledgers, ledgerData)
	ing.buffers.transactions = append(ing.buffers.transactions, transactions...)
	ing.buffers.operations = append(ing.buffers.operations, operations...)
	ing.buffers.balances = append(ing.buffers.balances, balances...)
	ing.buffers.effects = append(ing.buffers.effects, effects...)       // Cycle 8
	ing.buffers.trades = append(ing.buffers.trades, trades...)           // Cycle 8
	ing.buffers.accounts = append(ing.buffers.accounts, accounts...)     // Cycle 9
	ing.buffers.trustlines = append(ing.buffers.trustlines, trustlines...) // Cycle 9

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

		// ========================================
		// Cycle 6: XDR Fields (4 fields)
		// ========================================

		// 1. Transaction Envelope XDR
		if envelopeBytes, err := tx.Envelope.MarshalBinary(); err == nil {
			envelopeB64 := base64.StdEncoding.EncodeToString(envelopeBytes)
			txData.TxEnvelope = &envelopeB64
		}

		// 2. Transaction Result XDR
		if resultBytes, err := tx.Result.MarshalBinary(); err == nil {
			resultB64 := base64.StdEncoding.EncodeToString(resultBytes)
			txData.TxResult = &resultB64
		}

		// 3. Transaction Meta XDR (all versions: V1, V2, V3)
		if metaBytes, err := tx.UnsafeMeta.MarshalBinary(); err == nil {
			metaB64 := base64.StdEncoding.EncodeToString(metaBytes)
			txData.TxMeta = &metaB64
		}

		// 4. Fee Meta XDR (fee changes from meta)
		// Fee changes are in the soroban meta or operations meta
		// For simplicity, we'll skip this for now and set to NULL
		// TODO: Extract fee changes properly from meta
		// txData.TxFeeMeta remains nil

		// ========================================
		// Cycle 6: Signer Fields (2 fields)
		// ========================================

		// Extract all signer public keys
		signatures := tx.Envelope.Signatures()
		if len(signatures) > 0 {
			var signers []string

			for _, sig := range signatures {
				// Get the signer key from the signature
				// Note: We can't directly get the public key from DecoratedSignature
				// We'll need to extract it from the hint or use a different approach
				// For now, store signature hints as hex
				hint := hex.EncodeToString(sig.Hint[:])
				signers = append(signers, hint)
			}

			// Convert to JSON arrays
			if signersJSON, err := json.Marshal(signers); err == nil {
				signersStr := string(signersJSON)
				txData.TxSigners = &signersStr
			}

			// Extra signers (beyond source account) - for now, all non-source signatures
			// This is a simplification; proper implementation would track pre-auth and hash signers
			if len(signers) > 1 {
				if extraSignersJSON, err := json.Marshal(signers[1:]); err == nil {
					extraStr := string(extraSignersJSON)
					txData.ExtraSigners = &extraStr
				}
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

			// ========================================
			// Cycle 7: DEX Operations (ManageOffer, PathPayment, etc.)
			// ========================================

			case xdr.OperationTypeManageSellOffer:
				manageSell := op.Body.MustManageSellOfferOp()

				// Selling asset
				extractAsset(&opData, manageSell.Selling, "selling_asset")

				// Buying asset
				extractAsset(&opData, manageSell.Buying, "buying_asset")

				// Amount to sell
				amount := int64(manageSell.Amount)
				opData.Amount = &amount

				// Price (as string, e.g., "1.5")
				priceFloat := float64(manageSell.Price.N) / float64(manageSell.Price.D)
				priceStr := fmt.Sprintf("%.7f", priceFloat)
				opData.Price = &priceStr

				// Price ratio (N:D)
				priceRatio := fmt.Sprintf("%d:%d", manageSell.Price.N, manageSell.Price.D)
				opData.PriceR = &priceRatio

				// Offer ID (0 = create new, >0 = update existing)
				offerID := int64(manageSell.OfferId)
				opData.OfferID = &offerID

			case xdr.OperationTypeManageBuyOffer:
				manageBuy := op.Body.MustManageBuyOfferOp()

				// Selling asset
				extractAsset(&opData, manageBuy.Selling, "selling_asset")

				// Buying asset
				extractAsset(&opData, manageBuy.Buying, "buying_asset")

				// Amount to buy
				amount := int64(manageBuy.BuyAmount)
				opData.Amount = &amount

				// Price (as string, e.g., "1.5")
				priceFloat := float64(manageBuy.Price.N) / float64(manageBuy.Price.D)
				priceStr := fmt.Sprintf("%.7f", priceFloat)
				opData.Price = &priceStr

				// Price ratio (N:D)
				priceRatio := fmt.Sprintf("%d:%d", manageBuy.Price.N, manageBuy.Price.D)
				opData.PriceR = &priceRatio

				// Offer ID (0 = create new, >0 = update existing)
				offerID := int64(manageBuy.OfferId)
				opData.OfferID = &offerID

			case xdr.OperationTypeCreatePassiveSellOffer:
				passiveSell := op.Body.MustCreatePassiveSellOfferOp()

				// Selling asset
				extractAsset(&opData, passiveSell.Selling, "selling_asset")

				// Buying asset
				extractAsset(&opData, passiveSell.Buying, "buying_asset")

				// Amount to sell
				amount := int64(passiveSell.Amount)
				opData.Amount = &amount

				// Price (as string, e.g., "1.5")
				priceFloat := float64(passiveSell.Price.N) / float64(passiveSell.Price.D)
				priceStr := fmt.Sprintf("%.7f", priceFloat)
				opData.Price = &priceStr

				// Price ratio (N:D)
				priceRatio := fmt.Sprintf("%d:%d", passiveSell.Price.N, passiveSell.Price.D)
				opData.PriceR = &priceRatio

			case xdr.OperationTypePathPaymentStrictReceive:
				pathPayment := op.Body.MustPathPaymentStrictReceiveOp()

				// Sending asset
				extractAsset(&opData, pathPayment.SendAsset, "source_asset")

				// Destination
				dest := pathPayment.Destination.ToAccountId().Address()
				opData.Destination = &dest

				// Destination asset
				extractAsset(&opData, pathPayment.DestAsset, "asset")

				// Destination amount (what receiver gets)
				destAmount := int64(pathPayment.DestAmount)
				opData.Amount = &destAmount

				// Send max (maximum willing to send)
				sendMax := int64(pathPayment.SendMax)
				opData.SourceAmount = &sendMax

				// Note: Path is complex (array of assets) - skipping for now

			case xdr.OperationTypePathPaymentStrictSend:
				pathPayment := op.Body.MustPathPaymentStrictSendOp()

				// Sending asset
				extractAsset(&opData, pathPayment.SendAsset, "source_asset")

				// Send amount (what sender pays)
				sendAmount := int64(pathPayment.SendAmount)
				opData.SourceAmount = &sendAmount

				// Destination
				dest := pathPayment.Destination.ToAccountId().Address()
				opData.Destination = &dest

				// Destination asset
				extractAsset(&opData, pathPayment.DestAsset, "asset")

				// Destination min (minimum receiver gets)
				destMin := int64(pathPayment.DestMin)
				opData.DestinationMin = &destMin

				// Note: Path is complex (array of assets) - skipping for now

			case xdr.OperationTypeBumpSequence:
				bumpSeq := op.Body.MustBumpSequenceOp()

				// Bump to sequence number
				bumpTo := int64(bumpSeq.BumpTo)
				opData.BumpTo = &bumpTo

			case xdr.OperationTypeManageData:
				manageData := op.Body.MustManageDataOp()

				// Data name
				dataName := string(manageData.DataName)
				opData.DataName = &dataName

				// Data value (nil = delete, non-nil = set)
				if manageData.DataValue != nil {
					dataValue := base64.StdEncoding.EncodeToString(*manageData.DataValue)
					opData.DataValue = &dataValue
				}

			case xdr.OperationTypeAccountMerge:
				// AccountMerge has destination as the operation body itself (not wrapped in a struct)
				dest := op.Body.MustDestination().ToAccountId().Address()
				opData.Destination = &dest
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

// extractEffectsForLedger extracts effects from all transactions in a ledger (Cycle 8)
func (ing *Ingester) extractEffectsForLedger(lcm *xdr.LedgerCloseMeta, closedAt time.Time) []EffectData {
	var allEffects []EffectData

	// Use ingest package to read transactions
	reader, err := ingest.NewLedgerTransactionReaderFromLedgerCloseMeta(ing.config.Source.NetworkPassphrase, *lcm)
	if err != nil {
		log.Printf("Failed to create transaction reader for effects: %v", err)
		return allEffects
	}
	defer reader.Close()

	// Get ledger sequence
	var ledgerSeq uint32
	switch lcm.V {
	case 0:
		ledgerSeq = uint32(lcm.MustV0().LedgerHeader.Header.LedgerSeq)
	case 1:
		ledgerSeq = uint32(lcm.MustV1().LedgerHeader.Header.LedgerSeq)
	case 2:
		ledgerSeq = uint32(lcm.MustV2().LedgerHeader.Header.LedgerSeq)
	}

	// Read all transactions and extract effects
	for {
		tx, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Printf("Error reading transaction for effects: %v", err)
			continue
		}

		txHash := hex.EncodeToString(tx.Result.TransactionHash[:])

		// Extract effects for this transaction
		effects, err := extractEffects(&tx, ledgerSeq, closedAt, txHash)
		if err != nil {
			log.Printf("Failed to extract effects for tx %s: %v", txHash, err)
			continue
		}

		allEffects = append(allEffects, effects...)
	}

	return allEffects
}

// extractTradesForLedger extracts trades from all transactions in a ledger (Cycle 8)
func (ing *Ingester) extractTradesForLedger(lcm *xdr.LedgerCloseMeta, closedAt time.Time) []TradeData {
	var allTrades []TradeData

	// Use ingest package to read transactions
	reader, err := ingest.NewLedgerTransactionReaderFromLedgerCloseMeta(ing.config.Source.NetworkPassphrase, *lcm)
	if err != nil {
		log.Printf("Failed to create transaction reader for trades: %v", err)
		return allTrades
	}
	defer reader.Close()

	// Get ledger sequence
	var ledgerSeq uint32
	switch lcm.V {
	case 0:
		ledgerSeq = uint32(lcm.MustV0().LedgerHeader.Header.LedgerSeq)
	case 1:
		ledgerSeq = uint32(lcm.MustV1().LedgerHeader.Header.LedgerSeq)
	case 2:
		ledgerSeq = uint32(lcm.MustV2().LedgerHeader.Header.LedgerSeq)
	}

	// Read all transactions and extract trades
	for {
		tx, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Printf("Error reading transaction for trades: %v", err)
			continue
		}

		txHash := hex.EncodeToString(tx.Result.TransactionHash[:])

		// Extract trades for this transaction
		trades, err := extractTrades(&tx, ledgerSeq, closedAt, txHash)
		if err != nil {
			log.Printf("Failed to extract trades for tx %s: %v", txHash, err)
			continue
		}

		allTrades = append(allTrades, trades...)
	}

	return allTrades
}

// V2: Helper functions to safely handle nullable pointer fields for Appender API
// DuckDB Appender expects either the value or nil, not a pointer to the value

func ptrToInterface[T any](ptr *T) interface{} {
	if ptr == nil {
		return nil
	}
	return *ptr
}

// flush writes buffered data to DuckLake using Appender API for high-performance inserts (V2)
func (ing *Ingester) flush(ctx context.Context) error {
	if len(ing.buffers.ledgers) == 0 {
		return nil
	}

	numLedgers := len(ing.buffers.ledgers)
	numTransactions := len(ing.buffers.transactions)
	numOperations := len(ing.buffers.operations)
	numBalances := len(ing.buffers.balances)
	numEffects := len(ing.buffers.effects)       // Cycle 8
	numTrades := len(ing.buffers.trades)         // Cycle 8
	numAccounts := len(ing.buffers.accounts)     // Cycle 9
	numTrustlines := len(ing.buffers.trustlines) // Cycle 9

	log.Printf("[FLUSH] Starting multi-table flush (separate transactions): %d ledgers, %d transactions, %d operations, %d balances, %d effects, %d trades, %d accounts, %d trustlines",
		numLedgers, numTransactions, numOperations, numBalances, numEffects, numTrades, numAccounts, numTrustlines)
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
	// 1. INSERT INTO ledgers (V2: Using Appender API)
	// ========================================
	if numLedgers > 0 {
		log.Printf("[FLUSH] V2: Appending %d ledgers via Appender API...", numLedgers)
		appendStart := time.Now()

		for _, ledger := range ing.buffers.ledgers {
			err := ing.ledgerAppender.AppendRow(
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
				time.Now(), // ingestion_timestamp
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
			if err != nil {
				return fmt.Errorf("failed to append ledger %d: %w", ledger.Sequence, err)
			}
		}

		// Flush appender to commit rows to DuckLake
		flushStart := time.Now()
		if err := ing.ledgerAppender.Flush(); err != nil {
			return fmt.Errorf("failed to flush ledger appender: %w", err)
		}
		log.Printf("[FLUSH] ✓ V2: Appended and flushed %d ledgers in %v (append: %v, flush: %v)",
			numLedgers, time.Since(appendStart), flushStart.Sub(appendStart), time.Since(flushStart))
	}

	// ========================================
	// 2. INSERT INTO transactions (V2: Using Appender API)
	// ========================================
	if numTransactions > 0 {
		log.Printf("[FLUSH] V2: Appending %d transactions via Appender API...", numTransactions)
		appendStart := time.Now()

		for _, tx := range ing.buffers.transactions {
			err := ing.transactionAppender.AppendRow(
				// Core fields (13) - no pointers
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
				// Muxed accounts (2) - nullable
				ptrToInterface(tx.SourceAccountMuxed),
				ptrToInterface(tx.FeeAccountMuxed),
				// Fee bump (4) - nullable
				ptrToInterface(tx.InnerTransactionHash),
				ptrToInterface(tx.FeeBumpFee),
				ptrToInterface(tx.MaxFeeBid),
				ptrToInterface(tx.InnerSourceAccount),
				// Preconditions (6) - nullable
				ptrToInterface(tx.TimeboundsMinTime),
				ptrToInterface(tx.TimeboundsMaxTime),
				ptrToInterface(tx.LedgerboundsMin),
				ptrToInterface(tx.LedgerboundsMax),
				ptrToInterface(tx.MinSequenceNumber),
				ptrToInterface(tx.MinSequenceAge),
				// Soroban (13) - nullable
				ptrToInterface(tx.SorobanResourcesInstructions),
				ptrToInterface(tx.SorobanResourcesReadBytes),
				ptrToInterface(tx.SorobanResourcesWriteBytes),
				ptrToInterface(tx.SorobanDataSizeBytes),
				ptrToInterface(tx.SorobanDataResources),
				ptrToInterface(tx.SorobanFeeBase),
				ptrToInterface(tx.SorobanFeeResources),
				ptrToInterface(tx.SorobanFeeRefund),
				ptrToInterface(tx.SorobanFeeCharged),
				ptrToInterface(tx.SorobanFeeWasted),
				ptrToInterface(tx.SorobanHostFunctionType),
				ptrToInterface(tx.SorobanContractID),
				ptrToInterface(tx.SorobanContractEventsCount),
				// Metadata (2) - no pointers
				tx.SignaturesCount,
				tx.NewAccount,
				// Cycle 6: XDR fields (4) - nullable
				ptrToInterface(tx.TxEnvelope),
				ptrToInterface(tx.TxResult),
				ptrToInterface(tx.TxMeta),
				ptrToInterface(tx.TxFeeMeta),
				// Cycle 6: Signer fields (2) - nullable
				ptrToInterface(tx.TxSigners),
				ptrToInterface(tx.ExtraSigners),
			)
			if err != nil {
				return fmt.Errorf("failed to append transaction %s: %w", tx.TransactionHash, err)
			}
		}

		// Flush appender to commit rows to DuckLake
		flushStart := time.Now()
		if err := ing.transactionAppender.Flush(); err != nil {
			return fmt.Errorf("failed to flush transaction appender: %w", err)
		}
		log.Printf("[FLUSH] ✓ V2: Appended and flushed %d transactions in %v (append: %v, flush: %v)",
			numTransactions, time.Since(appendStart), flushStart.Sub(appendStart), time.Since(flushStart))
	}

	// ========================================
	// 3. INSERT INTO operations (V2: Using Appender API)
	// ========================================
	if numOperations > 0 {
		log.Printf("[FLUSH] V2: Appending %d operations via Appender API...", numOperations)
		appendStart := time.Now()

		for _, op := range ing.buffers.operations {
			err := ing.operationAppender.AppendRow(
				// Core fields (11) - no pointers
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
				// Muxed accounts (1) - nullable
				ptrToInterface(op.SourceAccountMuxed),
				// Asset fields (8) - nullable
				ptrToInterface(op.Asset),
				ptrToInterface(op.AssetType),
				ptrToInterface(op.AssetCode),
				ptrToInterface(op.AssetIssuer),
				ptrToInterface(op.SourceAsset),
				ptrToInterface(op.SourceAssetType),
				ptrToInterface(op.SourceAssetCode),
				ptrToInterface(op.SourceAssetIssuer),
				// Amount fields (4) - nullable
				ptrToInterface(op.Amount),
				ptrToInterface(op.SourceAmount),
				ptrToInterface(op.DestinationMin),
				ptrToInterface(op.StartingBalance),
				// Destination (1) - nullable
				ptrToInterface(op.Destination),
				// Trustline (5) - nullable
				ptrToInterface(op.TrustlineLimit),
				ptrToInterface(op.Trustor),
				ptrToInterface(op.Authorize),
				ptrToInterface(op.AuthorizeToMaintainLiabilities),
				ptrToInterface(op.TrustLineFlags),
				// Claimable balance (2) - nullable
				ptrToInterface(op.BalanceID),
				ptrToInterface(op.ClaimantsCount),
				// Sponsorship (1) - nullable
				ptrToInterface(op.SponsoredID),
				// DEX (11) - nullable
				ptrToInterface(op.OfferID),
				ptrToInterface(op.Price),
				ptrToInterface(op.PriceR),
				ptrToInterface(op.BuyingAsset),
				ptrToInterface(op.BuyingAssetType),
				ptrToInterface(op.BuyingAssetCode),
				ptrToInterface(op.BuyingAssetIssuer),
				ptrToInterface(op.SellingAsset),
				ptrToInterface(op.SellingAssetType),
				ptrToInterface(op.SellingAssetCode),
				ptrToInterface(op.SellingAssetIssuer),
				// Soroban (4) - nullable
				ptrToInterface(op.SorobanOperation),
				ptrToInterface(op.SorobanFunction),
				ptrToInterface(op.SorobanContractID),
				ptrToInterface(op.SorobanAuthRequired),
				// Account operations (8) - nullable
				ptrToInterface(op.BumpTo),
				ptrToInterface(op.SetFlags),
				ptrToInterface(op.ClearFlags),
				ptrToInterface(op.HomeDomain),
				ptrToInterface(op.MasterWeight),
				ptrToInterface(op.LowThreshold),
				ptrToInterface(op.MediumThreshold),
				ptrToInterface(op.HighThreshold),
				// Other (2) - nullable
				ptrToInterface(op.DataName),
				ptrToInterface(op.DataValue),
			)
			if err != nil {
				return fmt.Errorf("failed to append operation %d: %w", op.OperationIndex, err)
			}
		}

		// Flush appender to commit rows to DuckLake
		flushStart := time.Now()
		if err := ing.operationAppender.Flush(); err != nil {
			return fmt.Errorf("failed to flush operation appender: %w", err)
		}
		log.Printf("[FLUSH] ✓ V2: Appended and flushed %d operations in %v (append: %v, flush: %v)",
			numOperations, time.Since(appendStart), flushStart.Sub(appendStart), time.Since(flushStart))
	}

	// ========================================
	// 4. INSERT INTO native_balances (V2: Using Appender API)
	// ========================================
	if numBalances > 0 {
		log.Printf("[FLUSH] V2: Appending %d native_balances via Appender API...", numBalances)
		appendStart := time.Now()

		for _, bal := range ing.buffers.balances {
			err := ing.balanceAppender.AppendRow(
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
			if err != nil {
				return fmt.Errorf("failed to append balance for account %s: %w", bal.AccountID, err)
			}
		}

		// Flush appender to commit rows to DuckLake
		flushStart := time.Now()
		if err := ing.balanceAppender.Flush(); err != nil {
			return fmt.Errorf("failed to flush balance appender: %w", err)
		}
		log.Printf("[FLUSH] ✓ V2: Appended and flushed %d native_balances in %v (append: %v, flush: %v)",
			numBalances, time.Since(appendStart), flushStart.Sub(appendStart), time.Since(flushStart))
	}

	// ========================================
	// 5. INSERT INTO effects (V2: Cycle 8)
	// ========================================
	if numEffects > 0 {
		log.Printf("[FLUSH] V2: Appending %d effects via Appender API...", numEffects)
		appendStart := time.Now()

		for _, effect := range ing.buffers.effects {
			err := ing.effectAppender.AppendRow(
				// Identity (4 fields)
				effect.LedgerSequence,
				effect.TransactionHash,
				effect.OperationIndex,
				effect.EffectIndex,
				// Effect type (2 fields)
				effect.EffectType,
				effect.EffectTypeString,
				// Account (1 field)
				ptrToInterface(effect.AccountID),
				// Amount changes (4 fields)
				ptrToInterface(effect.Amount),
				ptrToInterface(effect.AssetCode),
				ptrToInterface(effect.AssetIssuer),
				ptrToInterface(effect.AssetType),
				// Trustline (3 fields)
				ptrToInterface(effect.TrustlineLimit),
				ptrToInterface(effect.AuthorizeFlag),
				ptrToInterface(effect.ClawbackFlag),
				// Signer (2 fields)
				ptrToInterface(effect.SignerAccount),
				ptrToInterface(effect.SignerWeight),
				// Offer (3 fields)
				ptrToInterface(effect.OfferID),
				ptrToInterface(effect.SellerAccount),
				// Metadata (2 fields)
				effect.CreatedAt,
				effect.LedgerRange,
			)
			if err != nil {
				return fmt.Errorf("failed to append effect %d for operation %d: %w", effect.EffectIndex, effect.OperationIndex, err)
			}
		}

		// Flush appender to commit rows to DuckLake
		flushStart := time.Now()
		if err := ing.effectAppender.Flush(); err != nil {
			return fmt.Errorf("failed to flush effect appender: %w", err)
		}
		log.Printf("[FLUSH] ✓ V2: Appended and flushed %d effects in %v (append: %v, flush: %v)",
			numEffects, time.Since(appendStart), flushStart.Sub(appendStart), time.Since(flushStart))
	}

	// ========================================
	// 6. INSERT INTO trades (V2: Cycle 8)
	// ========================================
	if numTrades > 0 {
		log.Printf("[FLUSH] V2: Appending %d trades via Appender API...", numTrades)
		appendStart := time.Now()

		for _, trade := range ing.buffers.trades {
			err := ing.tradeAppender.AppendRow(
				// Identity (4 fields)
				trade.LedgerSequence,
				trade.TransactionHash,
				trade.OperationIndex,
				trade.TradeIndex,
				// Trade details (2 fields)
				trade.TradeType,
				trade.TradeTimestamp,
				// Seller side (4 fields)
				trade.SellerAccount,
				ptrToInterface(trade.SellingAssetCode),
				ptrToInterface(trade.SellingAssetIssuer),
				trade.SellingAmount,
				// Buyer side (4 fields)
				trade.BuyerAccount,
				ptrToInterface(trade.BuyingAssetCode),
				ptrToInterface(trade.BuyingAssetIssuer),
				trade.BuyingAmount,
				// Price (1 field)
				trade.Price,
				// Metadata (2 fields)
				trade.CreatedAt,
				trade.LedgerRange,
			)
			if err != nil {
				return fmt.Errorf("failed to append trade %d for operation %d: %w", trade.TradeIndex, trade.OperationIndex, err)
			}
		}

		// Flush appender to commit rows to DuckLake
		flushStart := time.Now()
		if err := ing.tradeAppender.Flush(); err != nil {
			return fmt.Errorf("failed to flush trade appender: %w", err)
		}
		log.Printf("[FLUSH] ✓ V2: Appended and flushed %d trades in %v (append: %v, flush: %v)",
			numTrades, time.Since(appendStart), flushStart.Sub(appendStart), time.Since(flushStart))
	}

	// ========================================
	// 7. INSERT INTO accounts_snapshot_v1 (V2: Cycle 9)
	// ========================================
	if numAccounts > 0 {
		log.Printf("[FLUSH] V2: Appending %d accounts via Appender API...", numAccounts)
		appendStart := time.Now()

		for _, account := range ing.buffers.accounts {
			err := ing.accountAppender.AppendRow(
				// Identity (3 fields)
				account.AccountID,
				account.LedgerSequence,
				account.ClosedAt,
				// Balance (1 field)
				account.Balance,
				// Account Settings (5 fields)
				account.SequenceNumber,
				account.NumSubentries,
				account.NumSponsoring,
				account.NumSponsored,
				ptrToInterface(account.HomeDomain),
				// Thresholds (4 fields)
				account.MasterWeight,
				account.LowThreshold,
				account.MedThreshold,
				account.HighThreshold,
				// Flags (5 fields)
				account.Flags,
				account.AuthRequired,
				account.AuthRevocable,
				account.AuthImmutable,
				account.AuthClawbackEnabled,
				// Signers (1 field)
				ptrToInterface(account.Signers),
				// Sponsorship (1 field)
				ptrToInterface(account.SponsorAccount),
				// Metadata (3 fields)
				account.CreatedAt,
				account.UpdatedAt,
				account.LedgerRange,
			)
			if err != nil {
				return fmt.Errorf("failed to append account %s at ledger %d: %w", account.AccountID, account.LedgerSequence, err)
			}
		}

		// Flush appender to commit rows to DuckLake
		flushStart := time.Now()
		if err := ing.accountAppender.Flush(); err != nil {
			return fmt.Errorf("failed to flush account appender: %w", err)
		}
		log.Printf("[FLUSH] ✓ V2: Appended and flushed %d accounts in %v (append: %v, flush: %v)",
			numAccounts, time.Since(appendStart), flushStart.Sub(appendStart), time.Since(flushStart))
	}

	// ========================================
	// 8. INSERT INTO trustlines_snapshot_v1 (V2: Cycle 9)
	// ========================================
	if numTrustlines > 0 {
		log.Printf("[FLUSH] V2: Appending %d trustlines via Appender API...", numTrustlines)
		appendStart := time.Now()

		for _, trustline := range ing.buffers.trustlines {
			err := ing.trustlineAppender.AppendRow(
				// Identity (4 fields)
				trustline.AccountID,
				trustline.AssetCode,
				trustline.AssetIssuer,
				trustline.AssetType,
				// Trust & Balance (4 fields)
				trustline.Balance,
				trustline.TrustLimit,
				trustline.BuyingLiabilities,
				trustline.SellingLiabilities,
				// Authorization (3 fields)
				trustline.Authorized,
				trustline.AuthorizedToMaintainLiabilities,
				trustline.ClawbackEnabled,
				// Metadata (3 fields)
				trustline.LedgerSequence,
				trustline.CreatedAt,
				trustline.LedgerRange,
			)
			if err != nil {
				return fmt.Errorf("failed to append trustline %s:%s for account %s: %w", trustline.AssetCode, trustline.AssetIssuer, trustline.AccountID, err)
			}
		}

		// Flush appender to commit rows to DuckLake
		flushStart := time.Now()
		if err := ing.trustlineAppender.Flush(); err != nil {
			return fmt.Errorf("failed to flush trustline appender: %w", err)
		}
		log.Printf("[FLUSH] ✓ V2: Appended and flushed %d trustlines in %v (append: %v, flush: %v)",
			numTrustlines, time.Since(appendStart), flushStart.Sub(appendStart), time.Since(flushStart))
	}

	// ========================================
	// 9. RECORD QUALITY CHECK RESULTS (Obsrvr v2.0)
	// ========================================
	if len(qualityResults) > 0 {
		log.Printf("[QUALITY] Recording quality check results to _meta_quality...")
		if err := ing.recordQualityChecks(qualityResults); err != nil {
			log.Printf("⚠️  Warning: Failed to record quality checks: %v", err)
			// Non-fatal: continue with flush completion
		}
	}

	// ========================================
	// 10. RECORD DATA LINEAGE (Obsrvr v2.0)
	// ========================================
	log.Printf("[LINEAGE] Recording data lineage to _meta_lineage...")
	if err := ing.recordLineage(numLedgers, numTransactions, numOperations, numBalances, numEffects, numTrades, numAccounts, numTrustlines); err != nil {
		log.Printf("⚠️  Warning: Failed to record lineage: %v", err)
		// Non-fatal: continue with flush completion
	}

	log.Printf("[FLUSH] ✅ COMPLETE: Flushed %d ledgers, %d transactions, %d operations, %d balances, %d effects, %d trades, %d accounts, %d trustlines in %v total",
		numLedgers, numTransactions, numOperations, numBalances, numEffects, numTrades, numAccounts, numTrustlines, time.Since(flushStart))

	// Clear all buffers
	ing.buffers.ledgers = ing.buffers.ledgers[:0]
	ing.buffers.transactions = ing.buffers.transactions[:0]
	ing.buffers.operations = ing.buffers.operations[:0]
	ing.buffers.balances = ing.buffers.balances[:0]
	ing.buffers.effects = ing.buffers.effects[:0]       // Cycle 8
	ing.buffers.trades = ing.buffers.trades[:0]         // Cycle 8
	ing.buffers.accounts = ing.buffers.accounts[:0]     // Cycle 9
	ing.buffers.trustlines = ing.buffers.trustlines[:0] // Cycle 9
	ing.lastCommit = time.Now()

	return nil
}

// recordLineage records data lineage information for the current batch to _meta_lineage table
// This tracks the source ledger range, processor version, and row counts for each dataset
func (ing *Ingester) recordLineage(numLedgers, numTransactions, numOperations, numBalances, numEffects, numTrades, numAccounts, numTrustlines int) error {
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

	// Record effects if present (Cycle 8)
	if numEffects > 0 {
		entries = append(entries, LineageEntry{
			Dataset:  "core.effects_row_v1",
			RowCount: numEffects,
		})
	}

	// Record trades if present (Cycle 8)
	if numTrades > 0 {
		entries = append(entries, LineageEntry{
			Dataset:  "core.trades_row_v1",
			RowCount: numTrades,
		})
	}

	// Record accounts if present (Cycle 9)
	if numAccounts > 0 {
		entries = append(entries, LineageEntry{
			Dataset:  "core.accounts_snapshot_v1",
			RowCount: numAccounts,
		})
	}

	// Record trustlines if present (Cycle 9)
	if numTrustlines > 0 {
		entries = append(entries, LineageEntry{
			Dataset:  "core.trustlines_snapshot_v1",
			RowCount: numTrustlines,
		})
	}

	// OPTIMIZED: Batched insert with single query instead of 8 individual inserts
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

	// Close appenders
	if ing.ledgerAppender != nil {
		ing.ledgerAppender.Close()
	}
	if ing.transactionAppender != nil {
		ing.transactionAppender.Close()
	}
	if ing.operationAppender != nil {
		ing.operationAppender.Close()
	}
	if ing.balanceAppender != nil {
		ing.balanceAppender.Close()
	}
	if ing.effectAppender != nil {
		ing.effectAppender.Close()
	}
	if ing.tradeAppender != nil {
		ing.tradeAppender.Close()
	}
	if ing.accountAppender != nil {
		ing.accountAppender.Close()
	}
	if ing.trustlineAppender != nil {
		ing.trustlineAppender.Close()
	}

	// Close native connection
	if ing.conn != nil {
		ing.conn.Close()
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
