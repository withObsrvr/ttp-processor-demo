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
	"strconv"
	"strings"
	"syscall"
	"time"

	_ "github.com/marcboeker/go-duckdb/v2"
	"github.com/stellar/go/ingest"
	"github.com/stellar/go/strkey"
	"github.com/stellar/go/xdr"
	pb "github.com/withObsrvr/ttp-processor-demo/stellar-live-source-datalake/go/gen/raw_ledger_service"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"gopkg.in/yaml.v3"
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
		BatchSizes            struct {
			Ledgers         int `yaml:"ledgers"`
			Transactions    int `yaml:"transactions"`
			Operations      int `yaml:"operations"`
			NativeBalances  int `yaml:"native_balances"`
			AccountBalances int `yaml:"account_balances"`
		} `yaml:"batch_sizes"`
		CommitIntervalSeconds int  `yaml:"commit_interval_seconds"`
		UseUpsert             bool `yaml:"use_upsert"`
		CreateIndexes         bool `yaml:"create_indexes"`
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

// TransactionData represents a single transaction (Cycle 2)
type TransactionData struct {
	LedgerSequence        uint32
	Hash                  string
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

// AccountBalanceData represents account balance changes (Cycle 2)
type AccountBalanceData struct {
	AccountID          string
	AssetCode          string
	AssetIssuer        string
	Balance            int64
	TrustLineLimit     int64
	BuyingLiabilities  int64
	SellingLiabilities int64
	Flags              uint32
	LastModifiedLedger uint32
	LedgerSequence     uint32
	LedgerRange        uint32
}

// OperationData represents a single operation (Cycle 3)
// Schema-complete with all 103 fields for all 24 operation types
// Most fields will be NULL for any given operation
type OperationData struct {
	// Common fields (12 fields - all operations)
	TransactionHash          string
	OperationIndex           int32
	LedgerSequence           uint32
	SourceAccount            string
	SourceAccountMuxed       string
	Type                     int32
	TypeString               string
	CreatedAt                time.Time
	TransactionSuccessful    bool
	OperationResultCode      string
	OperationTraceCode       string
	LedgerRange              uint32

	// Payment operations (14 fields)
	PaymentAssetType    string
	PaymentAssetCode    string
	PaymentAssetIssuer  string
	PaymentFrom         string
	PaymentFromMuxed    string
	PaymentTo           string
	PaymentToMuxed      string
	PaymentAmount       int64
	SourceAssetType     string
	SourceAssetCode     string
	SourceAssetIssuer   string
	SourceAmount        int64
	SourceMax           int64
	DestinationMin      int64
	Path                string // JSON array

	// DEX operations (12 fields)
	OfferID             int64
	SellingAssetType    string
	SellingAssetCode    string
	SellingAssetIssuer  string
	BuyingAssetType     string
	BuyingAssetCode     string
	BuyingAssetIssuer   string
	OfferAmount         int64
	OfferPriceN         int32
	OfferPriceD         int32
	OfferPrice          string
	Passive             bool

	// Account operations (7 fields)
	StartingBalance       int64
	Funder                string
	FunderMuxed           string
	Account               string
	AccountMuxed          string
	MergeDestination      string
	MergeDestinationMuxed string

	// Trust operations (14 fields)
	TrustAssetType                    string
	TrustAssetCode                    string
	TrustAssetIssuer                  string
	Trustor                           string
	TrustorMuxed                      string
	Trustee                           string
	TrusteeMuxed                      string
	TrustLimit                        int64
	Authorize                         bool
	AuthorizeToMaintainLiabilities    bool
	SetFlags                          int32
	SetFlagsS                         string
	ClearFlags                        int32
	ClearFlagsS                       string

	// Claimable balance operations (8 fields)
	BalanceID      string
	CBAssetType    string
	CBAssetCode    string
	CBAssetIssuer  string
	CBAmount       int64
	Claimant       string
	ClaimantMuxed  string
	Claimants      string // JSON array

	// Soroban operations (11 fields)
	Function              string
	FunctionName          string
	Parameters            string // JSON array
	ContractID            string
	ContractCodeHash      string
	ContractCode          string
	AssetBalanceChanges   string // JSON array
	ExtendTo              int32
	SorobanResources      string // JSON
	SorobanData           string // JSON
	SorobanAuth           string // JSON

	// SetOptions (9 fields)
	HomeDomain       string
	InflationDest    string
	MasterKeyWeight  int32
	LowThreshold     int32
	MedThreshold     int32
	HighThreshold    int32
	SignerKey        string
	SignerWeight     int32
	SignerType       string

	// Data operations (2 fields)
	DataName  string
	DataValue string

	// Sequence operations (1 field)
	BumpTo int64

	// Sponsorship operations (5 fields)
	SponsoredID              string
	BeginSponsor             string
	BeginSponsorMuxed        string
	RevokeSponsorshipType    string
	RevokeSponsorshipData    string // JSON

	// Liquidity pool operations (13 fields)
	LiquidityPoolID     string
	ReservesMax         string // JSON array
	MinPrice            string
	MinPriceN           int32
	MinPriceD           int32
	MaxPrice            string
	MaxPriceN           int32
	MaxPriceD           int32
	ReservesDeposited   string // JSON array
	SharesReceived      int64
	ReservesMin         string // JSON array
	Shares              int64
	ReservesReceived    string // JSON array
}

// NativeBalanceData represents XLM balance changes (Cycle 3)
type NativeBalanceData struct {
	AccountID          string
	Balance            int64
	BuyingLiabilities  int64
	SellingLiabilities int64
	NumSubentries      int32
	NumSponsoring      int32
	NumSponsored       int32
	SequenceNumber     int64
	LastModifiedLedger uint32
	LedgerSequence     uint32
	LedgerRange        uint32
}

// Ingester handles the ledger ingestion pipeline
type Ingester struct {
	config             *Config
	grpcConn           *grpc.ClientConn
	grpcClient         pb.RawLedgerServiceClient
	db                 *sql.DB
	insertStmt         *sql.Stmt
	buffer             []LedgerData
	lastCommit         time.Time
	networkPassphrase  string
	// Cycle 2: Transaction and balance tracking
	txInsertStmt       *sql.Stmt
	balanceInsertStmt  *sql.Stmt
	txBuffer           []TransactionData
	balanceBuffer      []AccountBalanceData
	// Cycle 3: Operations and native balances
	operationInsertStmt      *sql.Stmt
	nativeBalanceInsertStmt  *sql.Stmt
	operationBuffer          []OperationData
	nativeBalanceBuffer      []NativeBalanceData
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

	log.Printf("Starting ingestion from ledger %d", config.Source.StartLedger)

	// Create ingester
	ingester, err := NewIngester(config)
	if err != nil {
		log.Fatalf("Failed to create ingester: %v", err)
	}
	defer ingester.Close()

	// Setup graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Start ingestion in goroutine
	errChan := make(chan error, 1)
	go func() {
		errChan <- ingester.Start(ctx)
	}()

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

	return &config, nil
}

// NewIngester creates a new ingester
func NewIngester(config *Config) (*Ingester, error) {
	ing := &Ingester{
		config:            config,
		buffer:            make([]LedgerData, 0, config.DuckLake.BatchSizes.Ledgers),
		lastCommit:        time.Now(),
		networkPassphrase: config.Source.NetworkPassphrase,
		// Cycle 2: Initialize transaction and balance buffers with independent batch sizes
		txBuffer:          make([]TransactionData, 0, config.DuckLake.BatchSizes.Transactions),
		balanceBuffer:     make([]AccountBalanceData, 0, config.DuckLake.BatchSizes.AccountBalances),
		// Cycle 3: Initialize operation and native balance buffers with independent batch sizes
		operationBuffer:      make([]OperationData, 0, config.DuckLake.BatchSizes.Operations),
		nativeBalanceBuffer:  make([]NativeBalanceData, 0, config.DuckLake.BatchSizes.NativeBalances),
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

	// Create schema if not exists
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
		return fmt.Errorf("failed to create table: %w", err)
	}

	// Prepare INSERT statement
	insertSQL := fmt.Sprintf(`
		INSERT INTO %s.%s.%s
		(sequence, ledger_hash, previous_ledger_hash, closed_at, protocol_version,
		 total_coins, fee_pool, base_fee, base_reserve, max_tx_set_size,
		 successful_tx_count, failed_tx_count, ingestion_timestamp, ledger_range,
		 transaction_count, operation_count, tx_set_operation_count,
		 soroban_fee_write1kb, node_id, signature, ledger_header,
		 bucket_list_size, live_soroban_state_size, evicted_keys_count)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		ing.config.DuckLake.CatalogName,
		ing.config.DuckLake.SchemaName,
		ing.config.DuckLake.TableName,
	)

	stmt, err := ing.db.Prepare(insertSQL)
	if err != nil {
		return fmt.Errorf("failed to prepare INSERT: %w", err)
	}
	ing.insertStmt = stmt

	// Cycle 2: Prepare INSERT statement for transactions
	txInsertSQL := fmt.Sprintf(`
		INSERT INTO %s.%s.transactions
		(ledger_sequence, transaction_hash, source_account, fee_charged, max_fee,
		 successful, transaction_result_code, operation_count, created_at, account_sequence,
		 memo_type, memo, ledger_range)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		ing.config.DuckLake.CatalogName,
		ing.config.DuckLake.SchemaName,
	)

	txStmt, err := ing.db.Prepare(txInsertSQL)
	if err != nil {
		return fmt.Errorf("failed to prepare transaction INSERT: %w", err)
	}
	ing.txInsertStmt = txStmt

	// Cycle 2: Prepare INSERT statement for account_balances
	balanceInsertSQL := fmt.Sprintf(`
		INSERT INTO %s.%s.account_balances
		(account_id, asset_code, asset_issuer, balance, trust_line_limit,
		 buying_liabilities, selling_liabilities, flags, last_modified_ledger,
		 ledger_sequence, ledger_range)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		ing.config.DuckLake.CatalogName,
		ing.config.DuckLake.SchemaName,
	)

	balanceStmt, err := ing.db.Prepare(balanceInsertSQL)
	if err != nil {
		return fmt.Errorf("failed to prepare balance INSERT: %w", err)
	}
	ing.balanceInsertStmt = balanceStmt

	// Cycle 3: Prepare INSERT statement for operations
	operationInsertSQL := fmt.Sprintf(`
		INSERT INTO %s.%s.operations (
			transaction_hash, operation_index, ledger_sequence, source_account, source_account_muxed,
			type, type_string, created_at, transaction_successful, operation_result_code, operation_trace_code, ledger_range,
			payment_asset_type, payment_asset_code, payment_asset_issuer, payment_from, payment_from_muxed,
			payment_to, payment_to_muxed, payment_amount, source_asset_type, source_asset_code, source_asset_issuer,
			source_amount, source_max, destination_min, path,
			offer_id, selling_asset_type, selling_asset_code, selling_asset_issuer,
			buying_asset_type, buying_asset_code, buying_asset_issuer, offer_amount,
			offer_price_n, offer_price_d, offer_price, passive,
			starting_balance, funder, funder_muxed, account, account_muxed, merge_destination, merge_destination_muxed,
			trust_asset_type, trust_asset_code, trust_asset_issuer, liquidity_pool_id,
			trustor, trustor_muxed, trustee, trustee_muxed, trust_limit, authorize, authorize_to_maintain_liabilities,
			set_flags, set_flags_s, clear_flags, clear_flags_s,
			balance_id, cb_asset_type, cb_asset_code, cb_asset_issuer, cb_amount, claimant, claimant_muxed, claimants,
			function, function_name, parameters, contract_id, contract_code_hash, contract_code,
			asset_balance_changes, extend_to, soroban_resources, soroban_data, soroban_auth,
			home_domain, inflation_dest, master_key_weight, low_threshold, med_threshold, high_threshold,
			signer_key, signer_weight, signer_type,
			data_name, data_value,
			bump_to,
			sponsored_id, begin_sponsor, begin_sponsor_muxed, revoke_sponsorship_type, revoke_sponsorship_data,
			reserves_max, min_price, min_price_n, min_price_d, max_price, max_price_n, max_price_d,
			reserves_deposited, shares_received, reserves_min, shares, reserves_received
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		ing.config.DuckLake.CatalogName,
		ing.config.DuckLake.SchemaName,
	)

	operationStmt, err := ing.db.Prepare(operationInsertSQL)
	if err != nil {
		return fmt.Errorf("failed to prepare operation INSERT: %w", err)
	}
	ing.operationInsertStmt = operationStmt

	// Cycle 3: Prepare INSERT statement for native_balances
	nativeBalanceInsertSQL := fmt.Sprintf(`
		INSERT INTO %s.%s.native_balances
		(account_id, balance, buying_liabilities, selling_liabilities, num_subentries,
		 num_sponsoring, num_sponsored, sequence_number, last_modified_ledger,
		 ledger_sequence, ledger_range)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		ing.config.DuckLake.CatalogName,
		ing.config.DuckLake.SchemaName,
	)

	nativeBalanceStmt, err := ing.db.Prepare(nativeBalanceInsertSQL)
	if err != nil {
		return fmt.Errorf("failed to prepare native balance INSERT: %w", err)
	}
	ing.nativeBalanceInsertStmt = nativeBalanceStmt

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

// createTable creates the ledgers, transactions, and account_balances tables
func (ing *Ingester) createTable() error {
	// Create ledgers table with ledger_range column for partitioning
	// DuckLake will organize files based on this column's values
	// We'll compute: ledger_range = (sequence / 10000) * 10000
	// This creates logical partitions: 0, 10000, 20000, 160000, etc.
	createLedgersSQL := fmt.Sprintf(`
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

	if _, err := ing.db.Exec(createLedgersSQL); err != nil {
		return fmt.Errorf("failed to create ledgers table: %w", err)
	}

	log.Printf("Table ready: %s.%s.%s",
		ing.config.DuckLake.CatalogName,
		ing.config.DuckLake.SchemaName,
		ing.config.DuckLake.TableName)

	// Cycle 2: Create transactions table
	createTransactionsSQL := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s.%s.transactions (
			ledger_sequence BIGINT NOT NULL,
			transaction_hash VARCHAR NOT NULL,
			source_account VARCHAR NOT NULL,
			fee_charged BIGINT NOT NULL,
			max_fee BIGINT NOT NULL,
			successful BOOLEAN NOT NULL,
			transaction_result_code VARCHAR,
			operation_count INT NOT NULL,
			created_at TIMESTAMP NOT NULL,
			account_sequence BIGINT NOT NULL,
			memo_type VARCHAR,
			memo TEXT,
			ledger_range BIGINT
		)`,
		ing.config.DuckLake.CatalogName,
		ing.config.DuckLake.SchemaName,
	)

	if _, err := ing.db.Exec(createTransactionsSQL); err != nil {
		return fmt.Errorf("failed to create transactions table: %w", err)
	}

	log.Printf("Table ready: %s.%s.transactions",
		ing.config.DuckLake.CatalogName,
		ing.config.DuckLake.SchemaName)

	// Cycle 2: Create account_balances table
	createBalancesSQL := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s.%s.account_balances (
			account_id VARCHAR NOT NULL,
			asset_code VARCHAR NOT NULL,
			asset_issuer VARCHAR NOT NULL,
			balance BIGINT NOT NULL,
			trust_line_limit BIGINT NOT NULL,
			buying_liabilities BIGINT NOT NULL,
			selling_liabilities BIGINT NOT NULL,
			flags INT NOT NULL,
			last_modified_ledger BIGINT NOT NULL,
			ledger_sequence BIGINT NOT NULL,
			ledger_range BIGINT
		)`,
		ing.config.DuckLake.CatalogName,
		ing.config.DuckLake.SchemaName,
	)

	if _, err := ing.db.Exec(createBalancesSQL); err != nil {
		return fmt.Errorf("failed to create account_balances table: %w", err)
	}

	log.Printf("Table ready: %s.%s.account_balances",
		ing.config.DuckLake.CatalogName,
		ing.config.DuckLake.SchemaName)

	// Cycle 3: Create operations table
	createOperationsSQL := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s.%s.operations (
			-- Common fields (12)
			transaction_hash VARCHAR NOT NULL,
			operation_index INT NOT NULL,
			ledger_sequence BIGINT NOT NULL,
			source_account VARCHAR NOT NULL,
			source_account_muxed VARCHAR,
			type INT NOT NULL,
			type_string VARCHAR NOT NULL,
			created_at TIMESTAMP NOT NULL,
			transaction_successful BOOLEAN NOT NULL,
			operation_result_code VARCHAR,
			operation_trace_code VARCHAR,
			ledger_range BIGINT,

			-- Payment operations (14 fields)
			payment_asset_type VARCHAR,
			payment_asset_code VARCHAR,
			payment_asset_issuer VARCHAR,
			payment_from VARCHAR,
			payment_from_muxed VARCHAR,
			payment_to VARCHAR,
			payment_to_muxed VARCHAR,
			payment_amount BIGINT,
			source_asset_type VARCHAR,
			source_asset_code VARCHAR,
			source_asset_issuer VARCHAR,
			source_amount BIGINT,
			source_max BIGINT,
			destination_min BIGINT,
			path TEXT,

			-- DEX operations (12 fields)
			offer_id BIGINT,
			selling_asset_type VARCHAR,
			selling_asset_code VARCHAR,
			selling_asset_issuer VARCHAR,
			buying_asset_type VARCHAR,
			buying_asset_code VARCHAR,
			buying_asset_issuer VARCHAR,
			offer_amount BIGINT,
			offer_price_n INT,
			offer_price_d INT,
			offer_price VARCHAR,
			passive BOOLEAN,

			-- Account operations (7 fields)
			starting_balance BIGINT,
			funder VARCHAR,
			funder_muxed VARCHAR,
			account VARCHAR,
			account_muxed VARCHAR,
			merge_destination VARCHAR,
			merge_destination_muxed VARCHAR,

			-- Trust operations (14 fields)
			trust_asset_type VARCHAR,
			trust_asset_code VARCHAR,
			trust_asset_issuer VARCHAR,
			liquidity_pool_id VARCHAR,
			trustor VARCHAR,
			trustor_muxed VARCHAR,
			trustee VARCHAR,
			trustee_muxed VARCHAR,
			trust_limit BIGINT,
			authorize BOOLEAN,
			authorize_to_maintain_liabilities BOOLEAN,
			set_flags INT,
			set_flags_s TEXT,
			clear_flags INT,
			clear_flags_s TEXT,

			-- Claimable Balance operations (8 fields)
			balance_id VARCHAR,
			cb_asset_type VARCHAR,
			cb_asset_code VARCHAR,
			cb_asset_issuer VARCHAR,
			cb_amount BIGINT,
			claimant VARCHAR,
			claimant_muxed VARCHAR,
			claimants TEXT,

			-- Soroban operations (11 fields)
			function VARCHAR,
			function_name VARCHAR,
			parameters TEXT,
			contract_id VARCHAR,
			contract_code_hash VARCHAR,
			contract_code VARCHAR,
			asset_balance_changes TEXT,
			extend_to INT,
			soroban_resources TEXT,
			soroban_data TEXT,
			soroban_auth TEXT,

			-- SetOptions (9 fields)
			home_domain VARCHAR,
			inflation_dest VARCHAR,
			master_key_weight INT,
			low_threshold INT,
			med_threshold INT,
			high_threshold INT,
			signer_key VARCHAR,
			signer_weight INT,
			signer_type VARCHAR,

			-- Data operations (2 fields)
			data_name VARCHAR,
			data_value TEXT,

			-- Sequence operations (1 field)
			bump_to BIGINT,

			-- Sponsorship operations (5 fields)
			sponsored_id VARCHAR,
			begin_sponsor VARCHAR,
			begin_sponsor_muxed VARCHAR,
			revoke_sponsorship_type VARCHAR,
			revoke_sponsorship_data TEXT,

			-- Liquidity Pool operations (13 fields)
			reserves_max TEXT,
			min_price VARCHAR,
			min_price_n INT,
			min_price_d INT,
			max_price VARCHAR,
			max_price_n INT,
			max_price_d INT,
			reserves_deposited TEXT,
			shares_received BIGINT,
			reserves_min TEXT,
			shares BIGINT,
			reserves_received TEXT
		)`,
		ing.config.DuckLake.CatalogName,
		ing.config.DuckLake.SchemaName,
	)

	if _, err := ing.db.Exec(createOperationsSQL); err != nil {
		return fmt.Errorf("failed to create operations table: %w", err)
	}

	log.Printf("Table ready: %s.%s.operations",
		ing.config.DuckLake.CatalogName,
		ing.config.DuckLake.SchemaName)

	// Cycle 3: Create native_balances table
	createNativeBalancesSQL := fmt.Sprintf(`
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

	if _, err := ing.db.Exec(createNativeBalancesSQL); err != nil {
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

	// Create a ticker to check for timeout-based flushes every 5 seconds
	flushTicker := time.NewTicker(5 * time.Second)
	defer flushTicker.Stop()
	log.Println("Created flush ticker (fires every 5 seconds)")

	// Channel to receive ledgers from stream.Recv() (non-blocking)
	ledgerChan := make(chan *pb.RawLedger)
	errChan := make(chan error)

	// Start goroutine to receive ledgers
	go func() {
		log.Println("Stream receiver goroutine started")
		for {
			rawLedger, err := stream.Recv()
			if err != nil {
				log.Printf("Stream receiver got error: %v", err)
				errChan <- err
				return
			}
			ledgerChan <- rawLedger
		}
	}()

	log.Println("Entering select loop")
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case <-flushTicker.C:
			// Periodic check for both timeout-based and threshold-based flushes
			elapsed := time.Since(ing.lastCommit)
			threshold := time.Duration(ing.config.DuckLake.CommitIntervalSeconds) * time.Second

			// Check per-table thresholds
			flushLedgers := len(ing.buffer) >= ing.config.DuckLake.BatchSizes.Ledgers
			flushTx := len(ing.txBuffer) >= ing.config.DuckLake.BatchSizes.Transactions
			flushOps := len(ing.operationBuffer) >= ing.config.DuckLake.BatchSizes.Operations
			flushNativeBalances := len(ing.nativeBalanceBuffer) >= ing.config.DuckLake.BatchSizes.NativeBalances
			flushAccountBalances := len(ing.balanceBuffer) >= ing.config.DuckLake.BatchSizes.AccountBalances
			timeoutFlush := elapsed > threshold

			anyThresholdMet := flushLedgers || flushTx || flushOps || flushNativeBalances || flushAccountBalances

			log.Printf("[FLUSH TICKER] Elapsed: %.1fs, Threshold: %.1fs, Buffers: L:%d/%d T:%d/%d AB:%d/%d O:%d/%d NB:%d/%d",
				elapsed.Seconds(), threshold.Seconds(),
				len(ing.buffer), ing.config.DuckLake.BatchSizes.Ledgers,
				len(ing.txBuffer), ing.config.DuckLake.BatchSizes.Transactions,
				len(ing.balanceBuffer), ing.config.DuckLake.BatchSizes.AccountBalances,
				len(ing.operationBuffer), ing.config.DuckLake.BatchSizes.Operations,
				len(ing.nativeBalanceBuffer), ing.config.DuckLake.BatchSizes.NativeBalances)

			if timeoutFlush || anyThresholdMet {
				if timeoutFlush {
					log.Println("Commit interval timeout - flushing all buffered data")
				}
				if anyThresholdMet {
					log.Printf("Threshold met - flushing (L:%v T:%v AB:%v O:%v NB:%v)",
						flushLedgers, flushTx, flushAccountBalances, flushOps, flushNativeBalances)
				}
				if err := ing.flush(ctx, timeoutFlush); err != nil {
					return fmt.Errorf("flush error: %w", err)
				}
			}

		case err := <-errChan:
			if err == io.EOF {
				log.Println("Stream ended")
				return ing.flush(ctx, true) // Force flush all tables at end of stream
			}
			return fmt.Errorf("stream error: %w", err)

		case rawLedger := <-ledgerChan:
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
	ledgerSeq := ledgerData.Sequence

	// Add ledger to buffer
	ing.buffer = append(ing.buffer, ledgerData)

	// Cycle 2: Extract transactions
	transactions, err := ing.extractTransactions(&lcm, ledgerSeq)
	if err != nil {
		return fmt.Errorf("failed to extract transactions: %w", err)
	}
	ing.txBuffer = append(ing.txBuffer, transactions...)

	// Cycle 2: Extract balance changes
	balances, err := ing.extractBalances(&lcm, ledgerSeq)
	if err != nil {
		return fmt.Errorf("failed to extract balances: %w", err)
	}
	ing.balanceBuffer = append(ing.balanceBuffer, balances...)

	// Cycle 3: Extract operations
	operations, err := ing.extractOperations(&lcm, ledgerSeq)
	if err != nil {
		return fmt.Errorf("failed to extract operations: %w", err)
	}
	ing.operationBuffer = append(ing.operationBuffer, operations...)

	// Cycle 3: Extract native balances
	nativeBalances, err := ing.extractNativeBalances(&lcm, ledgerSeq)
	if err != nil {
		return fmt.Errorf("failed to extract native balances: %w", err)
	}
	ing.nativeBalanceBuffer = append(ing.nativeBalanceBuffer, nativeBalances...)

	// Check per-table thresholds for selective flushing
	// Each table flushes independently when its buffer reaches threshold
	// This minimizes B2/S3 uploads by avoiding unnecessary flushes of empty tables
	timeoutFlush := time.Since(ing.lastCommit) > time.Duration(ing.config.DuckLake.CommitIntervalSeconds)*time.Second

	// Track which tables need flushing
	flushLedgers := len(ing.buffer) >= ing.config.DuckLake.BatchSizes.Ledgers
	flushTx := len(ing.txBuffer) >= ing.config.DuckLake.BatchSizes.Transactions
	flushOps := len(ing.operationBuffer) >= ing.config.DuckLake.BatchSizes.Operations
	flushNativeBalances := len(ing.nativeBalanceBuffer) >= ing.config.DuckLake.BatchSizes.NativeBalances
	flushAccountBalances := len(ing.balanceBuffer) >= ing.config.DuckLake.BatchSizes.AccountBalances

	// Flush if any table reaches threshold OR timeout occurs (flush all tables)
	if flushLedgers || flushTx || flushOps || flushNativeBalances || flushAccountBalances || timeoutFlush {
		return ing.flush(ctx, timeoutFlush)
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

// flush writes buffered data to DuckLake
// forceAll=true flushes all tables regardless of buffer size (used for timeout-based flushes)
// forceAll=false only flushes tables that have data (selective flushing for per-table batch thresholds)
func (ing *Ingester) flush(ctx context.Context, forceAll bool) error {
	// Skip if no data in any buffer
	if len(ing.buffer) == 0 && len(ing.txBuffer) == 0 && len(ing.balanceBuffer) == 0 &&
		len(ing.operationBuffer) == 0 && len(ing.nativeBalanceBuffer) == 0 {
		return nil
	}

	// Create a separate context for the transaction with its own timeout
	// This prevents parent context cancellation from invalidating the transaction mid-flight
	// Increased to 900s (15 minutes) to accommodate DuckLake/S3 write latency for very large batches
	// batch_size=500 with high transaction activity can exceed 300s (observed 302s at ledger 502)
	txCtx, cancel := context.WithTimeout(context.Background(), 900*time.Second)
	defer cancel()

	// Begin transaction with independent context
	tx, err := ing.db.BeginTx(txCtx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	// Prepare statement in transaction
	stmt := tx.Stmt(ing.insertStmt)

	// Insert all buffered records
	ingestionTime := time.Now()
	for _, ledger := range ing.buffer {
		_, err := stmt.Exec(
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
			ingestionTime,
			ledger.LedgerRange,
			// NEW: Operation counts
			ledger.TransactionCount,
			ledger.OperationCount,
			ledger.TxSetOperationCount,
			// NEW: Soroban
			ledger.SorobanFeeWrite1KB,
			// NEW: Consensus
			ledger.NodeID,
			ledger.Signature,
			ledger.LedgerHeader,
			// NEW: State tracking
			ledger.BucketListSize,
			ledger.LiveSorobanStateSize,
			// NEW: Protocol 23
			ledger.EvictedKeysCount,
		)
		if err != nil {
			return fmt.Errorf("failed to insert ledger %d: %w", ledger.Sequence, err)
		}
	}

	// Cycle 2: Insert transactions
	txStmt := tx.Stmt(ing.txInsertStmt)
	for _, txData := range ing.txBuffer {
		_, err := txStmt.Exec(
			txData.LedgerSequence,
			txData.Hash,
			txData.SourceAccount,
			txData.FeeCharged,
			txData.MaxFee,
			txData.Successful,
			txData.TransactionResultCode,
			txData.OperationCount,
			txData.CreatedAt,
			txData.AccountSequence,
			txData.MemoType,
			txData.Memo,
			txData.LedgerRange,
		)
		if err != nil {
			return fmt.Errorf("failed to insert transaction %s: %w", txData.Hash, err)
		}
	}

	// Cycle 2: Insert balance changes
	balanceStmt := tx.Stmt(ing.balanceInsertStmt)
	for _, balance := range ing.balanceBuffer {
		_, err := balanceStmt.Exec(
			balance.AccountID,
			balance.AssetCode,
			balance.AssetIssuer,
			balance.Balance,
			balance.TrustLineLimit,
			balance.BuyingLiabilities,
			balance.SellingLiabilities,
			balance.Flags,
			balance.LastModifiedLedger,
			balance.LedgerSequence,
			balance.LedgerRange,
		)
		if err != nil {
			return fmt.Errorf("failed to insert balance for %s: %w", balance.AccountID, err)
		}
	}

	// Cycle 3: Insert operations
	operationStmt := tx.Stmt(ing.operationInsertStmt)
	for _, op := range ing.operationBuffer {
		_, err := operationStmt.Exec(
			// Common fields (12)
			op.TransactionHash, op.OperationIndex, op.LedgerSequence,
			op.SourceAccount, op.SourceAccountMuxed, op.Type, op.TypeString,
			op.CreatedAt, op.TransactionSuccessful, op.OperationResultCode,
			op.OperationTraceCode, op.LedgerRange,
			// Payment operations (14)
			op.PaymentAssetType, op.PaymentAssetCode, op.PaymentAssetIssuer,
			op.PaymentFrom, op.PaymentFromMuxed, op.PaymentTo, op.PaymentToMuxed,
			op.PaymentAmount, op.SourceAssetType, op.SourceAssetCode,
			op.SourceAssetIssuer, op.SourceAmount, op.SourceMax, op.DestinationMin, op.Path,
			// DEX operations (12)
			op.OfferID, op.SellingAssetType, op.SellingAssetCode, op.SellingAssetIssuer,
			op.BuyingAssetType, op.BuyingAssetCode, op.BuyingAssetIssuer,
			op.OfferAmount, op.OfferPriceN, op.OfferPriceD, op.OfferPrice, op.Passive,
			// Account operations (7)
			op.StartingBalance, op.Funder, op.FunderMuxed, op.Account, op.AccountMuxed,
			op.MergeDestination, op.MergeDestinationMuxed,
			// Trust operations (14)
			op.TrustAssetType, op.TrustAssetCode, op.TrustAssetIssuer, op.LiquidityPoolID,
			op.Trustor, op.TrustorMuxed, op.Trustee, op.TrusteeMuxed,
			op.TrustLimit, op.Authorize, op.AuthorizeToMaintainLiabilities,
			op.SetFlags, op.SetFlagsS, op.ClearFlags, op.ClearFlagsS,
			// Claimable Balance operations (8)
			op.BalanceID, op.CBAssetType, op.CBAssetCode, op.CBAssetIssuer,
			op.CBAmount, op.Claimant, op.ClaimantMuxed, op.Claimants,
			// Soroban operations (11)
			op.Function, op.FunctionName, op.Parameters, op.ContractID,
			op.ContractCodeHash, op.ContractCode, op.AssetBalanceChanges,
			op.ExtendTo, op.SorobanResources, op.SorobanData, op.SorobanAuth,
			// SetOptions (9)
			op.HomeDomain, op.InflationDest, op.MasterKeyWeight,
			op.LowThreshold, op.MedThreshold, op.HighThreshold,
			op.SignerKey, op.SignerWeight, op.SignerType,
			// Data operations (2)
			op.DataName, op.DataValue,
			// Sequence operations (1)
			op.BumpTo,
			// Sponsorship operations (5)
			op.SponsoredID, op.BeginSponsor, op.BeginSponsorMuxed,
			op.RevokeSponsorshipType, op.RevokeSponsorshipData,
			// Liquidity Pool operations (13)
			op.ReservesMax, op.MinPrice, op.MinPriceN, op.MinPriceD,
			op.MaxPrice, op.MaxPriceN, op.MaxPriceD,
			op.ReservesDeposited, op.SharesReceived, op.ReservesMin,
			op.Shares, op.ReservesReceived,
		)
		if err != nil {
			return fmt.Errorf("failed to insert operation %s:%d: %w", op.TransactionHash, op.OperationIndex, err)
		}
	}

	// Cycle 3: Insert native balances
	nativeBalanceStmt := tx.Stmt(ing.nativeBalanceInsertStmt)
	for _, balance := range ing.nativeBalanceBuffer {
		_, err := nativeBalanceStmt.Exec(
			balance.AccountID,
			balance.Balance,
			balance.BuyingLiabilities,
			balance.SellingLiabilities,
			balance.NumSubentries,
			balance.NumSponsoring,
			balance.NumSponsored,
			balance.SequenceNumber,
			balance.LastModifiedLedger,
			balance.LedgerSequence,
			balance.LedgerRange,
		)
		if err != nil {
			return fmt.Errorf("failed to insert native balance for %s: %w", balance.AccountID, err)
		}
	}

	// Commit transaction
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit: %w", err)
	}

	// Build descriptive flush message
	flushType := "selective"
	if forceAll {
		flushType = "timeout"
	}
	log.Printf("Flushed [%s] %d ledgers, %d transactions, %d account_balances, %d operations, %d native_balances to DuckLake",
		flushType, len(ing.buffer), len(ing.txBuffer), len(ing.balanceBuffer), len(ing.operationBuffer), len(ing.nativeBalanceBuffer))

	// Clear buffers
	ing.buffer = ing.buffer[:0]
	ing.txBuffer = ing.txBuffer[:0]
	ing.balanceBuffer = ing.balanceBuffer[:0]
	ing.operationBuffer = ing.operationBuffer[:0]
	ing.nativeBalanceBuffer = ing.nativeBalanceBuffer[:0]
	ing.lastCommit = time.Now()

	return nil
}

// Close cleans up resources
func (ing *Ingester) Close() error {
	log.Println("Closing ingester...")

	// Try to flush remaining data, but don't fail if it errors during shutdown
	// The main processLedgers loop should have already flushed most data
	totalBuffered := len(ing.buffer) + len(ing.txBuffer) + len(ing.balanceBuffer) +
		len(ing.operationBuffer) + len(ing.nativeBalanceBuffer)
	if totalBuffered > 0 {
		log.Printf("Attempting final flush of buffered data (L:%d T:%d AB:%d O:%d NB:%d)...",
			len(ing.buffer), len(ing.txBuffer), len(ing.balanceBuffer),
			len(ing.operationBuffer), len(ing.nativeBalanceBuffer))
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		if err := ing.flush(ctx, true); err != nil {
			log.Printf("Warning: Final flush failed (this is expected during shutdown): %v", err)
			// Don't return the error - we still want to close resources cleanly
		}
	}

	// Close resources
	if ing.insertStmt != nil {
		ing.insertStmt.Close()
	}
	if ing.txInsertStmt != nil {
		ing.txInsertStmt.Close()
	}
	if ing.balanceInsertStmt != nil {
		ing.balanceInsertStmt.Close()
	}
	if ing.operationInsertStmt != nil {
		ing.operationInsertStmt.Close()
	}
	if ing.nativeBalanceInsertStmt != nil {
		ing.nativeBalanceInsertStmt.Close()
	}
	if ing.db != nil {
		ing.db.Close()
	}
	if ing.grpcConn != nil {
		ing.grpcConn.Close()
	}

	log.Println("Ingester closed")
	return nil
}

// Cycle 2: extractTransactions extracts transaction data from LedgerCloseMeta
func (ing *Ingester) extractTransactions(lcm *xdr.LedgerCloseMeta, ledgerSeq uint32) ([]TransactionData, error) {
	txReader, err := ingest.NewLedgerTransactionReaderFromLedgerCloseMeta(ing.networkPassphrase, *lcm)
	if err != nil {
		return nil, fmt.Errorf("failed to create transaction reader: %w", err)
	}
	defer txReader.Close()

	header := lcm.LedgerHeaderHistoryEntry()
	closedAt := time.Unix(int64(header.Header.ScpValue.CloseTime), 0)
	ledgerRange := (ledgerSeq / 10000) * 10000

	var transactions []TransactionData

	for {
		tx, err := txReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("failed to read transaction: %w", err)
		}

		txData := TransactionData{
			LedgerSequence:        ledgerSeq,
			Hash:                  tx.Result.TransactionHash.HexString(),
			SourceAccount:         tx.Envelope.SourceAccount().ToAccountId().Address(),
			FeeCharged:            int64(tx.Result.Result.FeeCharged),
			MaxFee:                int64(tx.Envelope.Fee()),
			Successful:            tx.Result.Result.Successful(),
			TransactionResultCode: tx.Result.Result.Result.Code.String(),
			OperationCount:        int32(len(tx.Envelope.Operations())),
			CreatedAt:             closedAt,
			AccountSequence:       int64(tx.Envelope.SeqNum()),
			MemoType:              tx.Envelope.Memo().Type.String(),
			Memo:                  extractMemoValue(tx.Envelope.Memo()),
			LedgerRange:           ledgerRange,
		}

		transactions = append(transactions, txData)
	}

	return transactions, nil
}

// extractMemoValue extracts the memo value based on memo type
func extractMemoValue(memo xdr.Memo) string {
	switch memo.Type {
	case xdr.MemoTypeMemoText:
		if memo.Text != nil {
			return string(*memo.Text)
		}
		return ""
	case xdr.MemoTypeMemoId:
		if memo.Id != nil {
			return strconv.FormatUint(uint64(*memo.Id), 10)
		}
		return ""
	case xdr.MemoTypeMemoHash:
		if memo.Hash != nil {
			hash := *memo.Hash
			return base64.StdEncoding.EncodeToString(hash[:])
		}
		return ""
	case xdr.MemoTypeMemoReturn:
		if memo.RetHash != nil {
			ret := *memo.RetHash
			return base64.StdEncoding.EncodeToString(ret[:])
		}
		return ""
	default:
		return ""
	}
}

// Cycle 2: extractBalances extracts account balance changes from LedgerCloseMeta
func (ing *Ingester) extractBalances(lcm *xdr.LedgerCloseMeta, ledgerSeq uint32) ([]AccountBalanceData, error) {
	txReader, err := ingest.NewLedgerTransactionReaderFromLedgerCloseMeta(ing.networkPassphrase, *lcm)
	if err != nil {
		return nil, fmt.Errorf("failed to create transaction reader: %w", err)
	}
	defer txReader.Close()

	ledgerRange := (ledgerSeq / 10000) * 10000
	var balances []AccountBalanceData

	for {
		tx, err := txReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("failed to read transaction: %w", err)
		}

		changes, err := tx.GetChanges()
		if err != nil {
			continue // Skip if we can't get changes
		}

		for _, change := range changes {
			if change.Type != xdr.LedgerEntryTypeTrustline {
				continue // Only process trustline changes for Cycle 2
			}

			// Extract trustline data from the change
			// Only process Post state (current/new state after change)
			// Skip deletions (Post == nil) for Cycle 2 - deletions can be tracked in future cycles
			if change.Post == nil {
				continue // Skip deletions
			}

			trustline := change.Post.Data.TrustLine
			lastModifiedLedger := uint32(change.Post.LastModifiedLedgerSeq)

			// Extract asset information
			var assetCode, assetIssuer string
			switch trustline.Asset.Type {
			case xdr.AssetTypeAssetTypeCreditAlphanum4:
				if trustline.Asset.AlphaNum4 != nil {
					assetCode = strings.TrimRight(string(trustline.Asset.AlphaNum4.AssetCode[:]), "\x00")
					assetIssuer = trustline.Asset.AlphaNum4.Issuer.Address()
				}
			case xdr.AssetTypeAssetTypeCreditAlphanum12:
				if trustline.Asset.AlphaNum12 != nil {
					assetCode = strings.TrimRight(string(trustline.Asset.AlphaNum12.AssetCode[:]), "\x00")
					assetIssuer = trustline.Asset.AlphaNum12.Issuer.Address()
				}
			case xdr.AssetTypeAssetTypePoolShare:
				// Skip liquidity pool shares for Cycle 2
				continue
			default:
				continue
			}

			balance := AccountBalanceData{
				AccountID:          trustline.AccountId.Address(),
				AssetCode:          assetCode,
				AssetIssuer:        assetIssuer,
				Balance:            int64(trustline.Balance),
				TrustLineLimit:     int64(trustline.Limit),
				BuyingLiabilities:  int64(trustline.Liabilities().Buying),
				SellingLiabilities: int64(trustline.Liabilities().Selling),
				Flags:              uint32(trustline.Flags),
				LastModifiedLedger: lastModifiedLedger,
				LedgerSequence:     ledgerSeq,
				LedgerRange:        ledgerRange,
			}

			balances = append(balances, balance)
		}
	}

	return balances, nil
}

// Cycle 3: extractOperations extracts operations from transactions
func (ing *Ingester) extractOperations(lcm *xdr.LedgerCloseMeta, ledgerSeq uint32) ([]OperationData, error) {
	txReader, err := ingest.NewLedgerTransactionReaderFromLedgerCloseMeta(ing.networkPassphrase, *lcm)
	if err != nil {
		return nil, fmt.Errorf("failed to create transaction reader: %w", err)
	}
	defer txReader.Close()

	header := lcm.LedgerHeaderHistoryEntry()
	closedAt := time.Unix(int64(header.Header.ScpValue.CloseTime), 0)
	ledgerRange := (ledgerSeq / 10000) * 10000

	var operations []OperationData

	for {
		tx, err := txReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("failed to read transaction: %w", err)
		}

		txHash := tx.Result.TransactionHash.HexString()
		txSuccessful := tx.Result.Result.Successful()
		txOps := tx.Envelope.Operations()

		// Get operation results (returns results slice and success bool)
		opResults, _ := tx.Result.Result.OperationResults()

		for opIndex, op := range txOps {
			// Common fields for all operations
			opData := OperationData{
				TransactionHash:       txHash,
				OperationIndex:        int32(opIndex),
				LedgerSequence:        ledgerSeq,
				SourceAccount:         getOperationSourceAccount(op, tx.Envelope),
				Type:                  int32(op.Body.Type),
				TypeString:            op.Body.Type.String(),
				CreatedAt:             closedAt,
				TransactionSuccessful: txSuccessful,
				LedgerRange:           ledgerRange,
			}

			// Extract muxed account if present
			if op.SourceAccount != nil {
				opData.SourceAccountMuxed = getMuxedAccount(*op.SourceAccount)
			}

			// Extract operation result code
			if opIndex < len(opResults) {
				opData.OperationResultCode = opResults[opIndex].Code.String()
				// TODO: Extract trace code from specific result types in future cycles
			}

			// Extract type-specific fields based on operation type
			switch op.Body.Type {
			case xdr.OperationTypePayment:
				ing.extractPaymentFields(&opData, op.Body.PaymentOp)
			case xdr.OperationTypePathPaymentStrictReceive:
				ing.extractPathPaymentStrictReceiveFields(&opData, op.Body.PathPaymentStrictReceiveOp)
			case xdr.OperationTypePathPaymentStrictSend:
				ing.extractPathPaymentStrictSendFields(&opData, op.Body.PathPaymentStrictSendOp)
			case xdr.OperationTypeCreateAccount:
				ing.extractCreateAccountFields(&opData, op.Body.CreateAccountOp)
			case xdr.OperationTypeAccountMerge:
				ing.extractAccountMergeFields(&opData, op.Body.Destination)
			case xdr.OperationTypeManageBuyOffer:
				ing.extractManageBuyOfferFields(&opData, op.Body.ManageBuyOfferOp)
			case xdr.OperationTypeManageSellOffer:
				ing.extractManageSellOfferFields(&opData, op.Body.ManageSellOfferOp)
			case xdr.OperationTypeChangeTrust:
				ing.extractChangeTrustFields(&opData, op.Body.ChangeTrustOp)
			case xdr.OperationTypeAllowTrust:
				ing.extractAllowTrustFields(&opData, op.Body.AllowTrustOp)
			// TODO: Add other operation types in future cycles
			default:
				// For unimplemented operations, common fields are already populated
			}

			operations = append(operations, opData)
		}
	}

	return operations, nil
}

// Helper: getOperationSourceAccount returns the source account for an operation
// Uses operation source if present, otherwise falls back to transaction source
func getOperationSourceAccount(op xdr.Operation, envelope xdr.TransactionEnvelope) string {
	if op.SourceAccount != nil {
		return op.SourceAccount.ToAccountId().Address()
	}
	return envelope.SourceAccount().ToAccountId().Address()
}

// Helper: getMuxedAccount returns muxed account address if present
func getMuxedAccount(muxed xdr.MuxedAccount) string {
	if muxed.Type == xdr.CryptoKeyTypeKeyTypeMuxedEd25519 {
		if muxed.Med25519 != nil {
			// Return muxed address in M... format
			// For now, return the underlying account ID
			// TODO: Format as M... address in future
			return muxed.ToAccountId().Address()
		}
	}
	return muxed.ToAccountId().Address()
}

// Helper: assetToStrings converts xdr.Asset to type, code, issuer strings
func assetToStrings(asset xdr.Asset) (assetType, assetCode, assetIssuer string) {
	switch asset.Type {
	case xdr.AssetTypeAssetTypeNative:
		return "native", "", ""
	case xdr.AssetTypeAssetTypeCreditAlphanum4:
		if asset.AlphaNum4 != nil {
			return "credit_alphanum4",
				strings.TrimRight(string(asset.AlphaNum4.AssetCode[:]), "\x00"),
				asset.AlphaNum4.Issuer.Address()
		}
	case xdr.AssetTypeAssetTypeCreditAlphanum12:
		if asset.AlphaNum12 != nil {
			return "credit_alphanum12",
				strings.TrimRight(string(asset.AlphaNum12.AssetCode[:]), "\x00"),
				asset.AlphaNum12.Issuer.Address()
		}
	case xdr.AssetTypeAssetTypePoolShare:
		// Pool shares - extract liquidity pool ID in future cycles
		return "liquidity_pool_shares", "", ""
	}
	return "", "", ""
}

// Payment operation extraction
func (ing *Ingester) extractPaymentFields(opData *OperationData, payment *xdr.PaymentOp) {
	if payment == nil {
		return
	}

	// Asset being sent
	assetType, assetCode, assetIssuer := assetToStrings(payment.Asset)
	opData.PaymentAssetType = assetType
	opData.PaymentAssetCode = assetCode
	opData.PaymentAssetIssuer = assetIssuer

	// Sender (operation source account - already in SourceAccount)
	opData.PaymentFrom = opData.SourceAccount
	if opData.SourceAccountMuxed != "" {
		opData.PaymentFromMuxed = opData.SourceAccountMuxed
	}

	// Recipient
	opData.PaymentTo = payment.Destination.ToAccountId().Address()
	opData.PaymentToMuxed = getMuxedAccount(payment.Destination)

	// Amount in stroops
	opData.PaymentAmount = int64(payment.Amount)
}

// PathPaymentStrictReceive operation extraction
func (ing *Ingester) extractPathPaymentStrictReceiveFields(opData *OperationData, pathPayment *xdr.PathPaymentStrictReceiveOp) {
	if pathPayment == nil {
		return
	}

	// Source asset (what sender sends)
	sourceType, sourceCode, sourceIssuer := assetToStrings(pathPayment.SendAsset)
	opData.SourceAssetType = sourceType
	opData.SourceAssetCode = sourceCode
	opData.SourceAssetIssuer = sourceIssuer
	opData.SourceMax = int64(pathPayment.SendMax)

	// Destination asset (what recipient receives)
	destType, destCode, destIssuer := assetToStrings(pathPayment.DestAsset)
	opData.PaymentAssetType = destType
	opData.PaymentAssetCode = destCode
	opData.PaymentAssetIssuer = destIssuer
	opData.DestinationMin = int64(pathPayment.DestAmount)

	// Parties
	opData.PaymentFrom = opData.SourceAccount
	if opData.SourceAccountMuxed != "" {
		opData.PaymentFromMuxed = opData.SourceAccountMuxed
	}
	opData.PaymentTo = pathPayment.Destination.ToAccountId().Address()
	opData.PaymentToMuxed = getMuxedAccount(pathPayment.Destination)

	// Path (intermediate assets) - encode as JSON
	if len(pathPayment.Path) > 0 {
		pathAssets := make([]map[string]string, len(pathPayment.Path))
		for i, asset := range pathPayment.Path {
			assetType, assetCode, assetIssuer := assetToStrings(asset)
			pathAssets[i] = map[string]string{
				"asset_type":   assetType,
				"asset_code":   assetCode,
				"asset_issuer": assetIssuer,
			}
		}
		if pathJSON, err := json.Marshal(pathAssets); err == nil {
			opData.Path = string(pathJSON)
		}
	}
}

// PathPaymentStrictSend operation extraction
func (ing *Ingester) extractPathPaymentStrictSendFields(opData *OperationData, pathPayment *xdr.PathPaymentStrictSendOp) {
	if pathPayment == nil {
		return
	}

	// Source asset (what sender sends)
	sourceType, sourceCode, sourceIssuer := assetToStrings(pathPayment.SendAsset)
	opData.SourceAssetType = sourceType
	opData.SourceAssetCode = sourceCode
	opData.SourceAssetIssuer = sourceIssuer
	opData.SourceAmount = int64(pathPayment.SendAmount)

	// Destination asset (what recipient receives)
	destType, destCode, destIssuer := assetToStrings(pathPayment.DestAsset)
	opData.PaymentAssetType = destType
	opData.PaymentAssetCode = destCode
	opData.PaymentAssetIssuer = destIssuer
	opData.DestinationMin = int64(pathPayment.DestMin)

	// Parties
	opData.PaymentFrom = opData.SourceAccount
	if opData.SourceAccountMuxed != "" {
		opData.PaymentFromMuxed = opData.SourceAccountMuxed
	}
	opData.PaymentTo = pathPayment.Destination.ToAccountId().Address()
	opData.PaymentToMuxed = getMuxedAccount(pathPayment.Destination)

	// Path (intermediate assets) - encode as JSON
	if len(pathPayment.Path) > 0 {
		pathAssets := make([]map[string]string, len(pathPayment.Path))
		for i, asset := range pathPayment.Path {
			assetType, assetCode, assetIssuer := assetToStrings(asset)
			pathAssets[i] = map[string]string{
				"asset_type":   assetType,
				"asset_code":   assetCode,
				"asset_issuer": assetIssuer,
			}
		}
		if pathJSON, err := json.Marshal(pathAssets); err == nil {
			opData.Path = string(pathJSON)
		}
	}
}

// CreateAccount operation extraction
func (ing *Ingester) extractCreateAccountFields(opData *OperationData, createAccount *xdr.CreateAccountOp) {
	if createAccount == nil {
		return
	}

	opData.Funder = opData.SourceAccount
	if opData.SourceAccountMuxed != "" {
		opData.FunderMuxed = opData.SourceAccountMuxed
	}
	opData.Account = createAccount.Destination.Address()
	opData.StartingBalance = int64(createAccount.StartingBalance)
}

// AccountMerge operation extraction
func (ing *Ingester) extractAccountMergeFields(opData *OperationData, destination *xdr.MuxedAccount) {
	if destination == nil {
		return
	}

	opData.Account = opData.SourceAccount
	if opData.SourceAccountMuxed != "" {
		opData.AccountMuxed = opData.SourceAccountMuxed
	}
	opData.MergeDestination = destination.ToAccountId().Address()
	opData.MergeDestinationMuxed = getMuxedAccount(*destination)
}

// ManageBuyOffer operation extraction
func (ing *Ingester) extractManageBuyOfferFields(opData *OperationData, offer *xdr.ManageBuyOfferOp) {
	if offer == nil {
		return
	}

	// Selling asset
	sellingType, sellingCode, sellingIssuer := assetToStrings(offer.Selling)
	opData.SellingAssetType = sellingType
	opData.SellingAssetCode = sellingCode
	opData.SellingAssetIssuer = sellingIssuer

	// Buying asset
	buyingType, buyingCode, buyingIssuer := assetToStrings(offer.Buying)
	opData.BuyingAssetType = buyingType
	opData.BuyingAssetCode = buyingCode
	opData.BuyingAssetIssuer = buyingIssuer

	// Offer details
	opData.OfferID = int64(offer.OfferId)
	opData.OfferAmount = int64(offer.BuyAmount)
	opData.OfferPriceN = int32(offer.Price.N)
	opData.OfferPriceD = int32(offer.Price.D)
	if offer.Price.D != 0 {
		price := float64(offer.Price.N) / float64(offer.Price.D)
		opData.OfferPrice = fmt.Sprintf("%.7f", price)
	}
	opData.Passive = false
}

// ManageSellOffer operation extraction
func (ing *Ingester) extractManageSellOfferFields(opData *OperationData, offer *xdr.ManageSellOfferOp) {
	if offer == nil {
		return
	}

	// Selling asset
	sellingType, sellingCode, sellingIssuer := assetToStrings(offer.Selling)
	opData.SellingAssetType = sellingType
	opData.SellingAssetCode = sellingCode
	opData.SellingAssetIssuer = sellingIssuer

	// Buying asset
	buyingType, buyingCode, buyingIssuer := assetToStrings(offer.Buying)
	opData.BuyingAssetType = buyingType
	opData.BuyingAssetCode = buyingCode
	opData.BuyingAssetIssuer = buyingIssuer

	// Offer details
	opData.OfferID = int64(offer.OfferId)
	opData.OfferAmount = int64(offer.Amount)
	opData.OfferPriceN = int32(offer.Price.N)
	opData.OfferPriceD = int32(offer.Price.D)
	if offer.Price.D != 0 {
		price := float64(offer.Price.N) / float64(offer.Price.D)
		opData.OfferPrice = fmt.Sprintf("%.7f", price)
	}
	opData.Passive = false
}

// ChangeTrust operation extraction
func (ing *Ingester) extractChangeTrustFields(opData *OperationData, changeTrust *xdr.ChangeTrustOp) {
	if changeTrust == nil {
		return
	}

	// Handle both asset types and liquidity pool shares
	switch changeTrust.Line.Type {
	case xdr.AssetTypeAssetTypeCreditAlphanum4, xdr.AssetTypeAssetTypeCreditAlphanum12:
		assetType, assetCode, assetIssuer := assetToStrings(changeTrust.Line.ToAsset())
		opData.TrustAssetType = assetType
		opData.TrustAssetCode = assetCode
		opData.TrustAssetIssuer = assetIssuer
	case xdr.AssetTypeAssetTypePoolShare:
		opData.TrustAssetType = "liquidity_pool_shares"
		// TODO: Extract liquidity pool ID in future cycles
		// Requires accessing LiquidityPoolParameters from ChangeTrustAsset
	}

	opData.Trustor = opData.SourceAccount
	if opData.SourceAccountMuxed != "" {
		opData.TrustorMuxed = opData.SourceAccountMuxed
	}
	opData.TrustLimit = int64(changeTrust.Limit)
}

// AllowTrust operation extraction
func (ing *Ingester) extractAllowTrustFields(opData *OperationData, allowTrust *xdr.AllowTrustOp) {
	if allowTrust == nil {
		return
	}

	opData.Trustee = opData.SourceAccount
	if opData.SourceAccountMuxed != "" {
		opData.TrusteeMuxed = opData.SourceAccountMuxed
	}
	opData.Trustor = allowTrust.Trustor.Address()

	// Extract asset code
	switch allowTrust.Asset.Type {
	case xdr.AssetTypeAssetTypeCreditAlphanum4:
		opData.TrustAssetCode = strings.TrimRight(string(allowTrust.Asset.AssetCode4[:]), "\x00")
	case xdr.AssetTypeAssetTypeCreditAlphanum12:
		opData.TrustAssetCode = strings.TrimRight(string(allowTrust.Asset.AssetCode12[:]), "\x00")
	}
	opData.TrustAssetType = "credit_alphanum4" // Default, can be either
	opData.TrustAssetIssuer = opData.Trustee

	// Authorization flags
	opData.Authorize = (allowTrust.Authorize == xdr.Uint32(xdr.TrustLineFlagsAuthorizedFlag))
	opData.AuthorizeToMaintainLiabilities = (allowTrust.Authorize == xdr.Uint32(xdr.TrustLineFlagsAuthorizedToMaintainLiabilitiesFlag))
}

// Cycle 3: extractNativeBalances extracts native XLM balance changes from LedgerCloseMeta
func (ing *Ingester) extractNativeBalances(lcm *xdr.LedgerCloseMeta, ledgerSeq uint32) ([]NativeBalanceData, error) {
	txReader, err := ingest.NewLedgerTransactionReaderFromLedgerCloseMeta(ing.networkPassphrase, *lcm)
	if err != nil {
		return nil, fmt.Errorf("failed to create transaction reader: %w", err)
	}
	defer txReader.Close()

	ledgerRange := (ledgerSeq / 10000) * 10000
	var balances []NativeBalanceData

	for {
		tx, err := txReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("failed to read transaction: %w", err)
		}

		changes, err := tx.GetChanges()
		if err != nil {
			continue // Skip if we can't get changes
		}

		for _, change := range changes {
			if change.Type != xdr.LedgerEntryTypeAccount {
				continue // Only process account changes
			}

			// Only process Post state (current/new state after change)
			// Skip deletions (Post == nil) and Pre-only states
			if change.Post == nil {
				continue
			}

			account := change.Post.Data.Account
			lastModifiedLedger := uint32(change.Post.LastModifiedLedgerSeq)

			balance := NativeBalanceData{
				AccountID:          account.AccountId.Address(),
				Balance:            int64(account.Balance),
				BuyingLiabilities:  int64(account.Liabilities().Buying),
				SellingLiabilities: int64(account.Liabilities().Selling),
				NumSubentries:      int32(account.NumSubEntries),
				LastModifiedLedger: lastModifiedLedger,
				LedgerSequence:     ledgerSeq,
				LedgerRange:        ledgerRange,
			}

			// Extract sponsorship fields (Protocol 14+)
			// TODO: Find correct field names in AccountEntryExtensionV1/V2 for sponsorship counts
			// For now, these will default to 0
			// if account.Ext.V1 != nil {
			// 	balance.NumSponsoring = int32(account.Ext.V1.???)
			// 	balance.NumSponsored = int32(account.Ext.V1.???)
			// }

			// Extract sequence number
			balance.SequenceNumber = int64(account.SeqNum)

			balances = append(balances, balance)
		}
	}

	return balances, nil
}
