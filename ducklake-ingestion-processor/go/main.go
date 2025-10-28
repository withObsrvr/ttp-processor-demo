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
		BatchSize             int    `yaml:"batch_size"`
		CommitIntervalSeconds int    `yaml:"commit_interval_seconds"`
		UseUpsert             bool   `yaml:"use_upsert"`
		CreateIndexes         bool   `yaml:"create_indexes"`
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
		buffer:            make([]LedgerData, 0, config.DuckLake.BatchSize),
		lastCommit:        time.Now(),
		networkPassphrase: config.Source.NetworkPassphrase,
		// Cycle 2: Initialize transaction and balance buffers
		txBuffer:          make([]TransactionData, 0, config.DuckLake.BatchSize*10), // Estimate ~10 tx per ledger
		balanceBuffer:     make([]AccountBalanceData, 0, config.DuckLake.BatchSize*20), // Estimate ~20 balance changes per ledger
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

	// Check if we should flush
	shouldFlush := len(ing.buffer) >= ing.config.DuckLake.BatchSize ||
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

// flush writes buffered data to DuckLake
func (ing *Ingester) flush(ctx context.Context) error {
	if len(ing.buffer) == 0 {
		return nil
	}

	// Begin transaction
	tx, err := ing.db.BeginTx(ctx, nil)
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

	// Commit transaction
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit: %w", err)
	}

	log.Printf("Flushed %d ledgers, %d transactions, %d balances to DuckLake",
		len(ing.buffer), len(ing.txBuffer), len(ing.balanceBuffer))

	// Clear buffers
	ing.buffer = ing.buffer[:0]
	ing.txBuffer = ing.txBuffer[:0]
	ing.balanceBuffer = ing.balanceBuffer[:0]
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
	if ing.insertStmt != nil {
		ing.insertStmt.Close()
	}
	if ing.txInsertStmt != nil {
		ing.txInsertStmt.Close()
	}
	if ing.balanceInsertStmt != nil {
		ing.balanceInsertStmt.Close()
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
