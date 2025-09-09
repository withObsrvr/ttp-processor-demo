package main

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"time"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/flight"
	"github.com/google/uuid"
	"github.com/lib/pq"
	"github.com/rs/zerolog/log"
)

// PostgreSQLConsumer consumes contract data from Arrow Flight and stores in PostgreSQL
type PostgreSQLConsumer struct {
	cfg    *Config
	db     *sql.DB
	client flight.Client
	
	// Batch processing
	batchBuffer []ContractDataEntry
	batchMutex  sync.Mutex
	batchID     uuid.UUID
	
	// Metrics
	metrics     ConsumerMetrics
	metricsMu   sync.RWMutex
	
	// Lifecycle
	wg         sync.WaitGroup
	cancelFunc context.CancelFunc
}

// ContractDataEntry represents a single contract data entry
type ContractDataEntry struct {
	ContractID                string
	LedgerSequence            uint32
	ContractKeyType           string
	ContractDurability        string
	AssetCode                 *string
	AssetIssuer               *string
	AssetType                 *string
	BalanceHolder             *string
	Balance                   *int64
	LastModifiedLedger        uint32
	Deleted                   bool
	ClosedAt                  time.Time
	LedgerKeyHash             *string
	KeyXDR                    *string
	ValXDR                    *string
	ContractInstanceType      *string
	ContractInstanceWASMHash  *string
	ExpirationLedgerSeq       *uint32
}

// ConsumerMetrics tracks consumer performance
type ConsumerMetrics struct {
	RecordsReceived   uint64
	RecordsInserted   uint64
	BatchesProcessed  uint64
	ErrorCount        uint64
	LastProcessedTime time.Time
	StartTime         time.Time
}

// NewPostgreSQLConsumer creates a new PostgreSQL consumer
func NewPostgreSQLConsumer(cfg *Config) (*PostgreSQLConsumer, error) {
	// Connect to database
	connStr := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
		cfg.DBHost, cfg.DBPort, cfg.DBUser, cfg.DBPassword, cfg.DBName, cfg.DBSSLMode)
	
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}
	
	// Test connection
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}
	
	// Configure connection pool
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(5 * time.Minute)
	
	// Connect to Arrow Flight server
	client, err := flight.NewClientWithMiddleware(cfg.FlightEndpoint, nil, nil)
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to create flight client: %w", err)
	}
	
	consumer := &PostgreSQLConsumer{
		cfg:         cfg,
		db:          db,
		client:      client,
		batchBuffer: make([]ContractDataEntry, 0, cfg.BatchSize),
		metrics: ConsumerMetrics{
			StartTime: time.Now(),
		},
	}
	
	// Initialize database schema
	if err := consumer.initializeSchema(); err != nil {
		consumer.Close()
		return nil, fmt.Errorf("failed to initialize schema: %w", err)
	}
	
	return consumer, nil
}

// Start begins consuming data from Arrow Flight
func (c *PostgreSQLConsumer) Start(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	c.cancelFunc = cancel
	
	log.Info().
		Str("flight_endpoint", c.cfg.FlightEndpoint).
		Int("batch_size", c.cfg.BatchSize).
		Msg("Starting PostgreSQL consumer")
	
	// Start metrics server
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		c.runMetricsServer(ctx)
	}()
	
	// Start batch commit timer
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		c.runBatchCommitter(ctx)
	}()
	
	// Main consumption loop
	return c.consumeStream(ctx)
}

// consumeStream consumes data from Arrow Flight stream
func (c *PostgreSQLConsumer) consumeStream(ctx context.Context) error {
	// Get flight info
	descriptor := &flight.FlightDescriptor{
		Type: flight.FlightDescriptor_PATH,
		Path: []string{"contract", "data"},
	}
	
	info, err := c.client.GetFlightInfo(ctx, descriptor)
	if err != nil {
		return fmt.Errorf("failed to get flight info: %w", err)
	}
	
	log.Info().
		Int("endpoints", len(info.Endpoint)).
		Msg("Retrieved flight info")
	
	// Stream from first endpoint
	if len(info.Endpoint) == 0 {
		return fmt.Errorf("no endpoints available")
	}
	
	stream, err := c.client.DoGet(ctx, info.Endpoint[0].Ticket)
	if err != nil {
		return fmt.Errorf("failed to start stream: %w", err)
	}
	defer stream.CloseSend()
	
	// Read records
	for stream.Next() {
		record := stream.Record()
		if err := c.processRecord(record); err != nil {
			log.Error().Err(err).Msg("Failed to process record")
			c.incrementErrorCount()
		}
		record.Release()
	}
	
	if err := stream.Err(); err != nil {
		return fmt.Errorf("stream error: %w", err)
	}
	
	// Flush any remaining entries
	if err := c.flushBatch(ctx); err != nil {
		log.Error().Err(err).Msg("Failed to flush final batch")
	}
	
	return nil
}

// processRecord processes a single Arrow record
func (c *PostgreSQLConsumer) processRecord(record arrow.Record) error {
	numRows := record.NumRows()
	
	// Extract columns
	contractIDCol := record.Column(0).(*array.String)
	contractKeyTypeCol := record.Column(1).(*array.String)
	contractDurabilityCol := record.Column(2).(*array.String)
	assetCodeCol := record.Column(3).(*array.String)
	assetIssuerCol := record.Column(4).(*array.String)
	assetTypeCol := record.Column(5).(*array.String)
	balanceHolderCol := record.Column(6).(*array.String)
	balanceCol := record.Column(7).(*array.Int64)
	lastModifiedLedgerCol := record.Column(8).(*array.Uint32)
	deletedCol := record.Column(9).(*array.Boolean)
	ledgerSequenceCol := record.Column(10).(*array.Uint32)
	closedAtCol := record.Column(11).(*array.Timestamp)
	ledgerKeyHashCol := record.Column(12).(*array.String)
	keyXDRCol := record.Column(13).(*array.String)
	valXDRCol := record.Column(14).(*array.String)
	contractInstanceTypeCol := record.Column(15).(*array.String)
	contractInstanceWASMHashCol := record.Column(16).(*array.String)
	expirationLedgerSeqCol := record.Column(17).(*array.Uint32)
	
	// Process each row
	for i := 0; i < int(numRows); i++ {
		entry := ContractDataEntry{
			ContractID:         contractIDCol.Value(i),
			LedgerSequence:     ledgerSequenceCol.Value(i),
			ContractKeyType:    contractKeyTypeCol.Value(i),
			ContractDurability: contractDurabilityCol.Value(i),
			LastModifiedLedger: lastModifiedLedgerCol.Value(i),
			Deleted:            deletedCol.Value(i),
			ClosedAt:           closedAtCol.Value(i).ToTime(arrow.Microsecond),
		}
		
		// Handle nullable fields
		if !assetCodeCol.IsNull(i) {
			v := assetCodeCol.Value(i)
			entry.AssetCode = &v
		}
		if !assetIssuerCol.IsNull(i) {
			v := assetIssuerCol.Value(i)
			entry.AssetIssuer = &v
		}
		if !assetTypeCol.IsNull(i) {
			v := assetTypeCol.Value(i)
			entry.AssetType = &v
		}
		if !balanceHolderCol.IsNull(i) {
			v := balanceHolderCol.Value(i)
			entry.BalanceHolder = &v
		}
		if !balanceCol.IsNull(i) {
			v := balanceCol.Value(i)
			entry.Balance = &v
		}
		if !ledgerKeyHashCol.IsNull(i) {
			v := ledgerKeyHashCol.Value(i)
			entry.LedgerKeyHash = &v
		}
		if !keyXDRCol.IsNull(i) {
			v := keyXDRCol.Value(i)
			entry.KeyXDR = &v
		}
		if !valXDRCol.IsNull(i) {
			v := valXDRCol.Value(i)
			entry.ValXDR = &v
		}
		if !contractInstanceTypeCol.IsNull(i) {
			v := contractInstanceTypeCol.Value(i)
			entry.ContractInstanceType = &v
		}
		if !contractInstanceWASMHashCol.IsNull(i) {
			v := contractInstanceWASMHashCol.Value(i)
			entry.ContractInstanceWASMHash = &v
		}
		if !expirationLedgerSeqCol.IsNull(i) {
			v := expirationLedgerSeqCol.Value(i)
			entry.ExpirationLedgerSeq = &v
		}
		
		// Add to batch
		c.addToBatch(entry)
	}
	
	c.updateMetrics(uint64(numRows), 0)
	return nil
}

// addToBatch adds an entry to the current batch
func (c *PostgreSQLConsumer) addToBatch(entry ContractDataEntry) {
	c.batchMutex.Lock()
	defer c.batchMutex.Unlock()
	
	c.batchBuffer = append(c.batchBuffer, entry)
	
	// Check if batch is full
	if len(c.batchBuffer) >= c.cfg.BatchSize {
		// Create new batch for processing
		batch := make([]ContractDataEntry, len(c.batchBuffer))
		copy(batch, c.batchBuffer)
		c.batchBuffer = c.batchBuffer[:0]
		
		// Process batch asynchronously
		go func() {
			if err := c.insertBatch(context.Background(), batch); err != nil {
				log.Error().Err(err).Msg("Failed to insert batch")
				c.incrementErrorCount()
			}
		}()
	}
}

// flushBatch flushes any remaining entries in the batch
func (c *PostgreSQLConsumer) flushBatch(ctx context.Context) error {
	c.batchMutex.Lock()
	defer c.batchMutex.Unlock()
	
	if len(c.batchBuffer) == 0 {
		return nil
	}
	
	batch := make([]ContractDataEntry, len(c.batchBuffer))
	copy(batch, c.batchBuffer)
	c.batchBuffer = c.batchBuffer[:0]
	
	return c.insertBatch(ctx, batch)
}

// insertBatch inserts a batch of entries into PostgreSQL
func (c *PostgreSQLConsumer) insertBatch(ctx context.Context, batch []ContractDataEntry) error {
	if len(batch) == 0 {
		return nil
	}
	
	batchID := uuid.New()
	startTime := time.Now()
	
	// Start transaction
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()
	
	// Use COPY for bulk insert
	stmt, err := tx.Prepare(pq.CopyIn("contract_data.entries",
		"contract_id", "ledger_sequence", "contract_key_type", "contract_durability",
		"asset_code", "asset_issuer", "asset_type", "balance_holder", "balance",
		"last_modified_ledger", "deleted", "closed_at", "ledger_key_hash",
		"key_xdr", "val_xdr", "contract_instance_type", "contract_instance_wasm_hash",
		"expiration_ledger_seq", "batch_id"))
	if err != nil {
		return fmt.Errorf("failed to prepare copy statement: %w", err)
	}
	defer stmt.Close()
	
	// Insert all entries
	for _, entry := range batch {
		_, err = stmt.Exec(
			entry.ContractID, entry.LedgerSequence, entry.ContractKeyType, entry.ContractDurability,
			entry.AssetCode, entry.AssetIssuer, entry.AssetType, entry.BalanceHolder, entry.Balance,
			entry.LastModifiedLedger, entry.Deleted, entry.ClosedAt, entry.LedgerKeyHash,
			entry.KeyXDR, entry.ValXDR, entry.ContractInstanceType, entry.ContractInstanceWASMHash,
			entry.ExpirationLedgerSeq, batchID,
		)
		if err != nil {
			return fmt.Errorf("failed to add entry to copy: %w", err)
		}
	}
	
	// Execute the copy
	if _, err = stmt.Exec(); err != nil {
		return fmt.Errorf("failed to execute copy: %w", err)
	}
	
	// Record ingestion progress
	if err := c.recordIngestionProgress(tx, batchID, batch, startTime); err != nil {
		return fmt.Errorf("failed to record progress: %w", err)
	}
	
	// Commit transaction
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}
	
	c.updateMetrics(0, uint64(len(batch)))
	
	log.Info().
		Int("entries", len(batch)).
		Str("batch_id", batchID.String()).
		Dur("duration", time.Since(startTime)).
		Msg("Inserted batch")
	
	return nil
}

// recordIngestionProgress records batch processing progress
func (c *PostgreSQLConsumer) recordIngestionProgress(tx *sql.Tx, batchID uuid.UUID, batch []ContractDataEntry, startTime time.Time) error {
	// Find ledger range
	var startLedger, endLedger uint32 = ^uint32(0), 0
	for _, entry := range batch {
		if entry.LedgerSequence < startLedger {
			startLedger = entry.LedgerSequence
		}
		if entry.LedgerSequence > endLedger {
			endLedger = entry.LedgerSequence
		}
	}
	
	_, err := tx.Exec(`
		INSERT INTO contract_data.ingestion_progress 
		(batch_id, start_ledger, end_ledger, entries_processed, started_at, completed_at, status)
		VALUES ($1, $2, $3, $4, $5, $6, 'completed')`,
		batchID, startLedger, endLedger, len(batch), startTime, time.Now())
	
	return err
}

// runBatchCommitter periodically commits incomplete batches
func (c *PostgreSQLConsumer) runBatchCommitter(ctx context.Context) {
	ticker := time.NewTicker(c.cfg.CommitInterval)
	defer ticker.Stop()
	
	for {
		select {
		case <-ticker.C:
			if err := c.flushBatch(ctx); err != nil {
				log.Error().Err(err).Msg("Failed to flush batch on timer")
			}
		case <-ctx.Done():
			return
		}
	}
}

// Close closes the consumer
func (c *PostgreSQLConsumer) Close() error {
	if c.cancelFunc != nil {
		c.cancelFunc()
	}
	
	c.wg.Wait()
	
	if c.client != nil {
		c.client.Close()
	}
	
	if c.db != nil {
		return c.db.Close()
	}
	
	return nil
}

// Shutdown performs graceful shutdown
func (c *PostgreSQLConsumer) Shutdown(ctx context.Context) error {
	log.Info().Msg("Starting graceful shutdown")
	
	// Cancel context to stop new work
	if c.cancelFunc != nil {
		c.cancelFunc()
	}
	
	// Flush any remaining batches
	if err := c.flushBatch(ctx); err != nil {
		log.Error().Err(err).Msg("Error flushing final batch")
	}
	
	// Wait for workers with timeout
	done := make(chan struct{})
	go func() {
		c.wg.Wait()
		close(done)
	}()
	
	select {
	case <-done:
		log.Info().Msg("All workers stopped")
	case <-ctx.Done():
		log.Warn().Msg("Shutdown timeout exceeded")
	}
	
	// Log final metrics
	metrics := c.GetMetrics()
	log.Info().
		Uint64("records_received", metrics.RecordsReceived).
		Uint64("records_inserted", metrics.RecordsInserted).
		Uint64("batches_processed", metrics.BatchesProcessed).
		Uint64("errors", metrics.ErrorCount).
		Msg("Final consumer metrics")
	
	return nil
}

// GetMetrics returns current metrics
func (c *PostgreSQLConsumer) GetMetrics() ConsumerMetrics {
	c.metricsMu.RLock()
	defer c.metricsMu.RUnlock()
	return c.metrics
}

// updateMetrics updates consumer metrics
func (c *PostgreSQLConsumer) updateMetrics(received, inserted uint64) {
	c.metricsMu.Lock()
	defer c.metricsMu.Unlock()
	
	c.metrics.RecordsReceived += received
	c.metrics.RecordsInserted += inserted
	if inserted > 0 {
		c.metrics.BatchesProcessed++
	}
	c.metrics.LastProcessedTime = time.Now()
}

// incrementErrorCount increments the error counter
func (c *PostgreSQLConsumer) incrementErrorCount() {
	c.metricsMu.Lock()
	defer c.metricsMu.Unlock()
	c.metrics.ErrorCount++
}