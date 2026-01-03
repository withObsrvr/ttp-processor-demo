package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"sync"
	"time"

	duckdb "github.com/duckdb/duckdb-go/v2"
)

// SourceRunner manages a single network source and its ingestion pipeline
// It wraps a full Ingester instance configured for a specific network
type SourceRunner struct {
	name     string
	ingester *Ingester // Full ingester with writeQueue integration
}

// MultiSourceOrchestrator coordinates multiple network sources
type MultiSourceOrchestrator struct {
	appConfig *AppConfig
	sources   []*SourceRunner

	// Shared write queue and worker (prevents EDEADLK)
	writeQueue  *WriteQueue
	queueWriter *QueueWriter

	// Shared DuckDB connection (used by QueueWriter)
	connector *duckdb.Connector
	db        *sql.DB
	conn      *duckdb.Conn

	// HTTP Query API server
	queryServer *QueryServer
	queryPort   string

	// Lifecycle management
	wg         sync.WaitGroup
	ctx        context.Context
	cancelFunc context.CancelFunc
}

// NewMultiSourceOrchestrator creates a new multi-source orchestrator
func NewMultiSourceOrchestrator(appConfig *AppConfig) (*MultiSourceOrchestrator, error) {
	ctx, cancel := context.WithCancel(context.Background())

	o := &MultiSourceOrchestrator{
		appConfig:  appConfig,
		ctx:        ctx,
		cancelFunc: cancel,
	}

	// Initialize shared DuckDB connection
	if err := o.initDuckDB(); err != nil {
		cancel()
		return nil, fmt.Errorf("failed to initialize DuckDB: %w", err)
	}

	// Create QueueWriter with shared connector and catalog name
	catalogName := appConfig.DuckLake.CatalogName
	queueWriter, err := NewQueueWriter(o.connector, o.db, catalogName, &appConfig.Era)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to create queue writer: %w", err)
	}
	o.queueWriter = queueWriter

	// Create WriteQueue (serializes all network writes)
	queueSize := 100 // Buffered queue for batches from multiple networks
	timeoutSec := 30 // 30 second timeout for queue submission
	logDepth := true // Log queue depth for monitoring
	logIntervalSec := 10 // Log every 10 seconds
	o.writeQueue = NewWriteQueue(queueSize, timeoutSec, logDepth, logIntervalSec)

	// Create source runners for each configured source
	sources := appConfig.GetSources()
	log.Printf("Multi-network mode: %d sources configured", len(sources))

	for i, srcCfg := range sources {
		// Derive network name from network passphrase (critical for multi-network isolation!)
		networkName := getNetworkName(srcCfg.NetworkPassphrase)
		if networkName == "" || networkName == "unknown" {
			networkName = fmt.Sprintf("network_%d", i)
		}

		// Use network name as schema (per-network isolation)
		schema := networkName

		runner, err := o.createSourceRunner(i, networkName, schema, srcCfg)
		if err != nil {
			cancel()
			return nil, fmt.Errorf("failed to create source runner %d (%s): %w", i, networkName, err)
		}

		o.sources = append(o.sources, runner)
	}

	log.Printf("✅ Multi-source orchestrator initialized (%d networks, shared write queue)", len(sources))
	return o, nil
}

// SetQueryPort configures the HTTP Query API server
func (o *MultiSourceOrchestrator) SetQueryPort(port string) {
	if port == "" {
		log.Printf("Query API disabled (no port specified)")
		return
	}

	o.queryPort = port
	o.queryServer = NewQueryServer(o.db, port)
	log.Printf("Query API configured on port %s", port)
}

// initDuckDB initializes the shared DuckDB connection
func (o *MultiSourceOrchestrator) initDuckDB() error {
	catalogPath := o.appConfig.DuckLake.CatalogPath
	catalogName := o.appConfig.DuckLake.CatalogName

	log.Printf("Initializing DuckDB catalog '%s' at: %s", catalogName, catalogPath)

	// Open the catalog file directly as the main database
	// This allows appenders to use schema.table format without catalog prefix
	connector, err := duckdb.NewConnector(catalogPath, nil)
	if err != nil {
		return fmt.Errorf("failed to create DuckDB connector: %w", err)
	}
	o.connector = connector

	// Create sql.DB for DDL/queries
	o.db = sql.OpenDB(connector)

	// Configure connection pooling for concurrent queries
	o.db.SetMaxOpenConns(20) // Allow up to 20 concurrent connections
	o.db.SetMaxIdleConns(5)  // Keep 5 idle connections ready
	log.Printf("Configured connection pool: max=20, idle=5")

	// Create native connection for Appender API
	conn, err := connector.Connect(context.Background())
	if err != nil {
		return fmt.Errorf("failed to create DuckDB connection: %w", err)
	}
	o.conn = conn.(*duckdb.Conn)

	// Configure WAL for concurrent query support
	// DuckDB v1.3+ uses WAL by default, allowing unlimited concurrent readers while writes happen
	log.Printf("Configuring WAL for concurrent queries...")

	// Set WAL autocheckpoint threshold (when WAL reaches this size, checkpoint automatically)
	// Default is 16MB, we'll use 1GB to reduce checkpoint frequency and improve write throughput
	if _, err := o.db.Exec("PRAGMA wal_autocheckpoint = '1GB'"); err != nil {
		return fmt.Errorf("failed to set WAL autocheckpoint threshold: %w", err)
	}

	log.Printf("✅ WAL configured (concurrent queries supported, autocheckpoint: 1GB)")

	// Install DuckDB extensions
	extensions := []string{
		"INSTALL ducklake",
		"INSTALL httpfs",
		"LOAD ducklake",
		"LOAD httpfs",
	}
	for _, ext := range extensions {
		if _, err := o.db.Exec(ext); err != nil {
			log.Printf("Extension setup: %s (error: %v)", ext, err)
		}
	}

	log.Printf("✅ Opened catalog as main database: %s", catalogPath)

	// Initialize catalog, schemas, and tables for all networks
	log.Printf("Creating schemas and tables for all networks...")

	sources := o.appConfig.GetSources()
	for i, srcCfg := range sources {
		// Derive network name from network passphrase
		networkName := getNetworkName(srcCfg.NetworkPassphrase)
		if networkName == "" || networkName == "unknown" {
			networkName = fmt.Sprintf("network_%d", i)
		}

		// Use network name as schema (testnet, mainnet, etc.)
		schema := networkName

		log.Printf("  Creating schema and tables for: %s", schema)

		// Create schema (no catalog prefix needed since catalog is main DB)
		createSchemaSQL := fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s", schema)
		if _, err := o.db.Exec(createSchemaSQL); err != nil {
			return fmt.Errorf("failed to create schema %s: %w", schema, err)
		}

		// Create all tables for this schema
		if err := o.createTablesForSchema(schema); err != nil {
			return fmt.Errorf("failed to create tables for schema %s: %w", schema, err)
		}
	}

	log.Printf("✅ DuckDB catalog initialized successfully")

	return nil
}

// createTablesForSchema creates all required tables for a given schema in the shared DB
func (o *MultiSourceOrchestrator) createTablesForSchema(schema string) error {
	// All table creation SQL statements (CREATE TABLE IF NOT EXISTS)
	// Copied from tables.go with catalog prefix removed (schema.table format only)
	// No catalog prefix needed since catalog is the main database
	tables := []string{
		// Ledgers table
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s.ledgers_row_v2 (
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
			transaction_count INT,
			operation_count INT,
			tx_set_operation_count INT,
			soroban_fee_write1kb BIGINT,
			node_id VARCHAR,
			signature VARCHAR,
			ledger_header TEXT,
			bucket_list_size BIGINT,
			live_soroban_state_size BIGINT,
			evicted_keys_count INT,
			era_id VARCHAR,
			version_label VARCHAR
		)`, schema),

		// Transactions table
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s.transactions_row_v2 (
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
			source_account_muxed VARCHAR,
			fee_account_muxed VARCHAR,
			inner_transaction_hash VARCHAR,
			fee_bump_fee BIGINT,
			max_fee_bid BIGINT,
			inner_source_account VARCHAR,
			timebounds_min_time BIGINT,
			timebounds_max_time BIGINT,
			ledgerbounds_min BIGINT,
			ledgerbounds_max BIGINT,
			min_sequence_number BIGINT,
			min_sequence_age BIGINT,
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
			signatures_count INT NOT NULL,
			new_account BOOLEAN NOT NULL,
			tx_envelope TEXT,
			tx_result TEXT,
			tx_meta TEXT,
			tx_fee_meta TEXT,
			tx_signers TEXT,
			extra_signers TEXT,
			era_id VARCHAR,
			version_label VARCHAR
		)`, schema),

		// Operations table
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s.operations_row_v2 (
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
			source_account_muxed VARCHAR,
			asset VARCHAR,
			asset_type VARCHAR,
			asset_code VARCHAR,
			asset_issuer VARCHAR,
			source_asset VARCHAR,
			source_asset_type VARCHAR,
			source_asset_code VARCHAR,
			source_asset_issuer VARCHAR,
			amount BIGINT,
			source_amount BIGINT,
			destination_min BIGINT,
			starting_balance BIGINT,
			destination VARCHAR,
			trustline_limit BIGINT,
			trustor VARCHAR,
			authorize BOOLEAN,
			authorize_to_maintain_liabilities BOOLEAN,
			trust_line_flags INT,
			balance_id VARCHAR,
			claimants_count INT,
			sponsored_id VARCHAR,
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
			soroban_operation VARCHAR,
			soroban_function VARCHAR,
			soroban_contract_id VARCHAR,
			soroban_auth_required BOOLEAN,
			bump_to BIGINT,
			set_flags INT,
			clear_flags INT,
			home_domain VARCHAR,
			master_weight INT,
			low_threshold INT,
			medium_threshold INT,
			high_threshold INT,
			data_name VARCHAR,
			data_value VARCHAR,
			era_id VARCHAR,
			version_label VARCHAR
		)`, schema),

		// Native Balances table
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s.native_balances_snapshot_v1 (
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
			ledger_range BIGINT,
			era_id VARCHAR,
			version_label VARCHAR
		)`, schema),

		// Effects table
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s.effects_row_v1 (
			ledger_sequence BIGINT NOT NULL,
			transaction_hash VARCHAR NOT NULL,
			operation_index INT NOT NULL,
			effect_index INT NOT NULL,
			effect_type INT NOT NULL,
			effect_type_string VARCHAR NOT NULL,
			account_id VARCHAR,
			amount VARCHAR,
			asset_code VARCHAR,
			asset_issuer VARCHAR,
			asset_type VARCHAR,
			trustline_limit VARCHAR,
			authorize_flag BOOLEAN,
			clawback_flag BOOLEAN,
			signer_account VARCHAR,
			signer_weight INT,
			offer_id BIGINT,
			seller_account VARCHAR,
			created_at TIMESTAMP NOT NULL,
			ledger_range BIGINT,
			era_id VARCHAR,
			version_label VARCHAR
		)`, schema),

		// Trades table
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s.trades_row_v1 (
			ledger_sequence BIGINT NOT NULL,
			transaction_hash VARCHAR NOT NULL,
			operation_index INT NOT NULL,
			trade_index INT NOT NULL,
			trade_type VARCHAR NOT NULL,
			trade_timestamp TIMESTAMP NOT NULL,
			seller_account VARCHAR NOT NULL,
			selling_asset_code VARCHAR,
			selling_asset_issuer VARCHAR,
			selling_amount VARCHAR NOT NULL,
			buyer_account VARCHAR NOT NULL,
			buying_asset_code VARCHAR,
			buying_asset_issuer VARCHAR,
			buying_amount VARCHAR NOT NULL,
			price VARCHAR NOT NULL,
			created_at TIMESTAMP NOT NULL,
			ledger_range BIGINT,
			era_id VARCHAR,
			version_label VARCHAR
		)`, schema),

		// Accounts table
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s.accounts_snapshot_v1 (
			account_id VARCHAR NOT NULL,
			ledger_sequence BIGINT NOT NULL,
			closed_at TIMESTAMP NOT NULL,
			balance VARCHAR NOT NULL,
			sequence_number BIGINT NOT NULL,
			num_subentries INT NOT NULL,
			num_sponsoring INT NOT NULL,
			num_sponsored INT NOT NULL,
			home_domain VARCHAR,
			master_weight INT NOT NULL,
			low_threshold INT NOT NULL,
			med_threshold INT NOT NULL,
			high_threshold INT NOT NULL,
			flags INT NOT NULL,
			auth_required BOOLEAN NOT NULL,
			auth_revocable BOOLEAN NOT NULL,
			auth_immutable BOOLEAN NOT NULL,
			auth_clawback_enabled BOOLEAN NOT NULL,
			signers VARCHAR,
			sponsor_account VARCHAR,
			created_at TIMESTAMP NOT NULL,
			updated_at TIMESTAMP NOT NULL,
			ledger_range BIGINT NOT NULL,
			era_id VARCHAR,
			version_label VARCHAR
		)`, schema),

		// Trustlines table
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s.trustlines_snapshot_v1 (
			account_id VARCHAR NOT NULL,
			asset_code VARCHAR NOT NULL,
			asset_issuer VARCHAR NOT NULL,
			asset_type VARCHAR NOT NULL,
			balance VARCHAR NOT NULL,
			trust_limit VARCHAR NOT NULL,
			buying_liabilities VARCHAR NOT NULL,
			selling_liabilities VARCHAR NOT NULL,
			authorized BOOLEAN NOT NULL,
			authorized_to_maintain_liabilities BOOLEAN NOT NULL,
			clawback_enabled BOOLEAN NOT NULL,
			ledger_sequence BIGINT NOT NULL,
			created_at TIMESTAMP NOT NULL,
			ledger_range BIGINT NOT NULL,
			era_id VARCHAR,
			version_label VARCHAR
		)`, schema),

		// Offers table
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s.offers_snapshot_v1 (
			offer_id BIGINT NOT NULL,
			seller_account VARCHAR NOT NULL,
			ledger_sequence BIGINT NOT NULL,
			closed_at TIMESTAMP NOT NULL,
			selling_asset_type VARCHAR NOT NULL,
			selling_asset_code VARCHAR,
			selling_asset_issuer VARCHAR,
			buying_asset_type VARCHAR NOT NULL,
			buying_asset_code VARCHAR,
			buying_asset_issuer VARCHAR,
			amount VARCHAR NOT NULL,
			price VARCHAR NOT NULL,
			flags INT NOT NULL,
			created_at TIMESTAMP NOT NULL,
			ledger_range BIGINT NOT NULL,
			era_id VARCHAR,
			version_label VARCHAR
		)`, schema),

		// Claimable Balances table
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s.claimable_balances_snapshot_v1 (
			balance_id VARCHAR NOT NULL,
			sponsor VARCHAR NOT NULL,
			ledger_sequence BIGINT NOT NULL,
			closed_at TIMESTAMP NOT NULL,
			asset_type VARCHAR NOT NULL,
			asset_code VARCHAR,
			asset_issuer VARCHAR,
			amount BIGINT NOT NULL,
			claimants_count INT NOT NULL,
			flags INT NOT NULL,
			created_at TIMESTAMP NOT NULL,
			ledger_range BIGINT NOT NULL,
			era_id VARCHAR,
			version_label VARCHAR
		)`, schema),

		// Liquidity Pools table
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s.liquidity_pools_snapshot_v1 (
			liquidity_pool_id VARCHAR NOT NULL,
			ledger_sequence BIGINT NOT NULL,
			closed_at TIMESTAMP NOT NULL,
			pool_type VARCHAR NOT NULL,
			fee INT NOT NULL,
			trustline_count INT NOT NULL,
			total_pool_shares BIGINT NOT NULL,
			asset_a_type VARCHAR NOT NULL,
			asset_a_code VARCHAR,
			asset_a_issuer VARCHAR,
			asset_a_amount BIGINT NOT NULL,
			asset_b_type VARCHAR NOT NULL,
			asset_b_code VARCHAR,
			asset_b_issuer VARCHAR,
			asset_b_amount BIGINT NOT NULL,
			created_at TIMESTAMP NOT NULL,
			ledger_range BIGINT NOT NULL,
			era_id VARCHAR,
			version_label VARCHAR
		)`, schema),

		// Contract Events table
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s.contract_events_stream_v1 (
			event_id VARCHAR NOT NULL,
			contract_id VARCHAR,
			ledger_sequence BIGINT NOT NULL,
			transaction_hash VARCHAR NOT NULL,
			closed_at TIMESTAMP NOT NULL,
			event_type VARCHAR NOT NULL,
			in_successful_contract_call BOOLEAN NOT NULL,
			topics_json VARCHAR NOT NULL,
			topics_decoded VARCHAR NOT NULL,
			data_xdr VARCHAR NOT NULL,
			data_decoded VARCHAR NOT NULL,
			topic_count INT NOT NULL,
			operation_index INT NOT NULL,
			event_index INT NOT NULL,
			created_at TIMESTAMP NOT NULL,
			ledger_range BIGINT NOT NULL,
			era_id VARCHAR,
			version_label VARCHAR
		)`, schema),

		// Contract Data table
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s.contract_data_snapshot_v1 (
			contract_id VARCHAR NOT NULL,
			ledger_sequence BIGINT NOT NULL,
			ledger_key_hash VARCHAR NOT NULL,
			contract_key_type VARCHAR NOT NULL,
			contract_durability VARCHAR NOT NULL,
			asset_code VARCHAR,
			asset_issuer VARCHAR,
			asset_type VARCHAR,
			balance_holder VARCHAR,
			balance VARCHAR,
			last_modified_ledger INT NOT NULL,
			ledger_entry_change INT NOT NULL,
			deleted BOOLEAN NOT NULL,
			closed_at TIMESTAMP NOT NULL,
			contract_data_xdr TEXT NOT NULL,
			created_at TIMESTAMP NOT NULL,
			ledger_range BIGINT NOT NULL,
			era_id VARCHAR,
			version_label VARCHAR
		)`, schema),

		// Contract Code table
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s.contract_code_snapshot_v1 (
			contract_code_hash VARCHAR NOT NULL,
			ledger_key_hash VARCHAR NOT NULL,
			contract_code_ext_v INT NOT NULL,
			last_modified_ledger INT NOT NULL,
			ledger_entry_change INT NOT NULL,
			deleted BOOLEAN NOT NULL,
			closed_at TIMESTAMP NOT NULL,
			ledger_sequence BIGINT NOT NULL,
			n_instructions BIGINT,
			n_functions BIGINT,
			n_globals BIGINT,
			n_table_entries BIGINT,
			n_types BIGINT,
			n_data_segments BIGINT,
			n_elem_segments BIGINT,
			n_imports BIGINT,
			n_exports BIGINT,
			n_data_segment_bytes BIGINT,
			created_at TIMESTAMP NOT NULL,
			ledger_range BIGINT NOT NULL,
			era_id VARCHAR,
			version_label VARCHAR
		)`, schema),

		// Config Settings table
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s.config_settings_snapshot_v1 (
			config_setting_id INT NOT NULL,
			ledger_sequence BIGINT NOT NULL,
			last_modified_ledger INT NOT NULL,
			deleted BOOLEAN NOT NULL,
			closed_at TIMESTAMP NOT NULL,
			ledger_max_instructions BIGINT,
			tx_max_instructions BIGINT,
			fee_rate_per_instructions_increment BIGINT,
			tx_memory_limit UINTEGER,
			ledger_max_read_ledger_entries UINTEGER,
			ledger_max_read_bytes UINTEGER,
			ledger_max_write_ledger_entries UINTEGER,
			ledger_max_write_bytes UINTEGER,
			tx_max_read_ledger_entries UINTEGER,
			tx_max_read_bytes UINTEGER,
			tx_max_write_ledger_entries UINTEGER,
			tx_max_write_bytes UINTEGER,
			contract_max_size_bytes UINTEGER,
			config_setting_xdr TEXT NOT NULL,
			created_at TIMESTAMP NOT NULL,
			ledger_range BIGINT NOT NULL,
			era_id VARCHAR,
			version_label VARCHAR
		)`, schema),

		// TTL table
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s.ttl_snapshot_v1 (
			key_hash VARCHAR NOT NULL,
			ledger_sequence BIGINT NOT NULL,
			live_until_ledger_seq BIGINT NOT NULL,
			ttl_remaining BIGINT NOT NULL,
			expired BOOLEAN NOT NULL,
			last_modified_ledger INT NOT NULL,
			deleted BOOLEAN NOT NULL,
			closed_at TIMESTAMP NOT NULL,
			created_at TIMESTAMP NOT NULL,
			ledger_range BIGINT NOT NULL,
			era_id VARCHAR,
			version_label VARCHAR
		)`, schema),

		// Evicted Keys table
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s.evicted_keys_state_v1 (
			key_hash VARCHAR NOT NULL,
			ledger_sequence BIGINT NOT NULL,
			contract_id VARCHAR NOT NULL,
			key_type VARCHAR NOT NULL,
			durability VARCHAR NOT NULL,
			closed_at TIMESTAMP NOT NULL,
			ledger_range BIGINT NOT NULL,
			created_at TIMESTAMP NOT NULL,
			era_id VARCHAR,
			version_label VARCHAR
		)`, schema),

		// Restored Keys table
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s.restored_keys_state_v1 (
			key_hash VARCHAR NOT NULL,
			ledger_sequence BIGINT NOT NULL,
			contract_id VARCHAR NOT NULL,
			key_type VARCHAR NOT NULL,
			durability VARCHAR NOT NULL,
			restored_from_ledger BIGINT NOT NULL,
			closed_at TIMESTAMP NOT NULL,
			ledger_range BIGINT NOT NULL,
			created_at TIMESTAMP NOT NULL,
			era_id VARCHAR,
			version_label VARCHAR
		)`, schema),

		// Account Signers table
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s.account_signers_snapshot_v1 (
			account_id VARCHAR NOT NULL,
			signer VARCHAR NOT NULL,
			ledger_sequence BIGINT NOT NULL,
			weight INTEGER NOT NULL,
			sponsor VARCHAR,
			deleted BOOLEAN NOT NULL,
			closed_at TIMESTAMP NOT NULL,
			ledger_range BIGINT NOT NULL,
			created_at TIMESTAMP NOT NULL,
			era_id VARCHAR,
			version_label VARCHAR
		)`, schema),
	}

	// Execute each CREATE TABLE statement
	for _, sql := range tables {
		if _, err := o.db.Exec(sql); err != nil {
			return fmt.Errorf("failed to create table: %w", err)
		}
	}

	log.Printf("  ✅ Created all 19 tables for schema: %s", schema)
	return nil
}

// createSourceRunner creates a source runner for a specific network
func (o *MultiSourceOrchestrator) createSourceRunner(index int, networkName, schema string, srcCfg SourceConfigCompat) (*SourceRunner, error) {
	log.Printf("[%s] Creating source runner (index: %d, schema: %s)", networkName, index, schema)

	// Create per-network AppConfig (clone from base appConfig)
	netConfig := &AppConfig{
		Service:  o.appConfig.Service,
		Source:   srcCfg, // Use network-specific source config
		DuckLake: o.appConfig.DuckLake,
		Logging:  o.appConfig.Logging,
		Era:      o.appConfig.Era,
		Pipeline: o.appConfig.Pipeline,
		Metrics:  o.appConfig.Metrics,
	}

	// Override era network name
	netConfig.Era.Network = networkName

	// Override schema name for multi-network isolation
	netConfig.DuckLake.SchemaName = schema

	// Override checkpoint config for per-network checkpoint files
	if o.appConfig.Checkpoint.Enabled {
		checkpointCfg := o.appConfig.Checkpoint
		checkpointCfg.Filename = fmt.Sprintf("checkpoint-%s.json", networkName)
		netConfig.Checkpoint = checkpointCfg
	} else {
		netConfig.Checkpoint = o.appConfig.Checkpoint
	}

	// Set manifest and PAS configs
	netConfig.Manifest = o.appConfig.Manifest
	netConfig.PAS = o.appConfig.PAS

	// Create Ingester from per-network config
	log.Printf("[%s] Creating Ingester instance...", networkName)
	ingester, err := NewIngesterFromAppConfig(netConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create ingester: %w", err)
	}

	// Configure Ingester for multi-network mode
	ingester.writeQueue = o.writeQueue
	ingester.networkName = networkName
	ingester.schema = schema

	log.Printf("[%s] ✅ Ingester configured with write queue (schema: %s)", networkName, schema)

	runner := &SourceRunner{
		name:     networkName,
		ingester: ingester,
	}

	return runner, nil
}

// Start starts all source runners and the write worker
func (o *MultiSourceOrchestrator) Start() error {
	log.Printf("Starting multi-source orchestrator with %d sources", len(o.sources))

	// Start write queue worker (consumes batches from all networks and serializes DuckDB writes)
	log.Printf("[WRITE-QUEUE] Starting queue worker...")
	o.writeQueue.Start(o.ctx, o.queueWriter)

	// Start HTTP Query API server (if configured)
	if o.queryServer != nil {
		o.wg.Add(1)
		go func() {
			defer o.wg.Done()
			log.Printf("[QUERY-API] Starting HTTP server on %s", o.queryPort)
			if err := o.queryServer.Start(); err != nil {
				log.Printf("[QUERY-API] ❌ Server stopped with error: %v", err)
			} else {
				log.Printf("[QUERY-API] ✅ Server stopped gracefully")
			}
		}()
	}

	// Start each source runner (Ingester) in its own goroutine
	for _, runner := range o.sources {
		o.wg.Add(1)
		go o.runSource(runner)
	}

	log.Printf("✅ Multi-source orchestrator started (%d networks running)", len(o.sources))

	return nil
}

// runSource runs a single Ingester in its own goroutine
func (o *MultiSourceOrchestrator) runSource(runner *SourceRunner) {
	defer o.wg.Done()

	log.Printf("[%s] Starting Ingester...", runner.name)

	// Run the Ingester (it will use the shared write queue)
	if err := runner.ingester.Start(o.ctx); err != nil {
		log.Printf("[%s] ❌ Ingester stopped with error: %v", runner.name, err)
	} else {
		log.Printf("[%s] ✅ Ingester stopped gracefully", runner.name)
	}
}

// Stop gracefully shuts down the orchestrator
func (o *MultiSourceOrchestrator) Stop() error {
	log.Printf("Stopping multi-source orchestrator...")

	// Stop HTTP Query API server (if running)
	if o.queryServer != nil {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		log.Printf("[QUERY-API] Stopping HTTP server...")
		if err := o.queryServer.Stop(shutdownCtx); err != nil {
			log.Printf("[QUERY-API] ⚠️ Server shutdown error: %v", err)
		}
	}

	// Cancel context to signal all Ingesters to stop
	o.cancelFunc()

	// Wait for all Ingester goroutines to finish
	log.Printf("Waiting for all Ingesters to stop...")
	o.wg.Wait()

	// Stop write queue (drains remaining batches)
	log.Printf("Stopping write queue...")
	o.writeQueue.Stop()

	// Close QueueWriter (closes all appenders)
	log.Printf("Closing queue writer...")
	if o.queueWriter != nil {
		o.queueWriter.Close()
	}

	// Close each Ingester
	for _, runner := range o.sources {
		log.Printf("[%s] Closing Ingester...", runner.name)
		if runner.ingester != nil {
			runner.ingester.Close()
		}
	}

	// Close shared DuckDB connections
	if o.conn != nil {
		o.conn.Close()
	}
	if o.db != nil {
		o.db.Close()
	}

	log.Printf("✅ Multi-source orchestrator stopped")

	return nil
}
