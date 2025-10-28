package main

import (
	"context"
	"database/sql"
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	_ "github.com/marcboeker/go-duckdb/v2"
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
}

// Ingester handles the ledger ingestion pipeline
type Ingester struct {
	config      *Config
	grpcConn    *grpc.ClientConn
	grpcClient  pb.RawLedgerServiceClient
	db          *sql.DB
	insertStmt  *sql.Stmt
	buffer      []LedgerData
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
		config:     config,
		buffer:     make([]LedgerData, 0, config.DuckLake.BatchSize),
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
		 successful_tx_count, failed_tx_count, ingestion_timestamp, ledger_range)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		ing.config.DuckLake.CatalogName,
		ing.config.DuckLake.SchemaName,
		ing.config.DuckLake.TableName,
	)

	stmt, err := ing.db.Prepare(insertSQL)
	if err != nil {
		return fmt.Errorf("failed to prepare INSERT: %w", err)
	}
	ing.insertStmt = stmt

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
			ledger_range BIGINT
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

	// Add to buffer
	ing.buffer = append(ing.buffer, ledgerData)

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

	// Extract fields
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

	// Count transactions
	var txCount uint32
	var failedCount uint32

	switch lcm.V {
	case 0:
		txCount = uint32(len(lcm.MustV0().TxSet.Txs))
	case 1:
		v1 := lcm.MustV1()
		txCount = uint32(len(v1.TxProcessing))
		for _, tx := range v1.TxProcessing {
			if tx.Result.Result.Result.Code.String() != "txSUCCESS" {
				failedCount++
			}
		}
	case 2:
		v2 := lcm.MustV2()
		txCount = uint32(len(v2.TxProcessing))
		for _, tx := range v2.TxProcessing {
			if tx.Result.Result.Result.Code.String() != "txSUCCESS" {
				failedCount++
			}
		}
	}

	data.SuccessfulTxCount = txCount - failedCount
	data.FailedTxCount = failedCount

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
		)
		if err != nil {
			return fmt.Errorf("failed to insert ledger %d: %w", ledger.Sequence, err)
		}
	}

	// Commit transaction
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit: %w", err)
	}

	log.Printf("Flushed %d ledgers to DuckLake", len(ing.buffer))

	// Clear buffer
	ing.buffer = ing.buffer[:0]
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
	if ing.db != nil {
		ing.db.Close()
	}
	if ing.grpcConn != nil {
		ing.grpcConn.Close()
	}

	log.Println("Ingester closed")
	return nil
}
