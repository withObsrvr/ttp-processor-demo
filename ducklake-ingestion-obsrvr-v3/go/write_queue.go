package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"sync"
	"time"

	duckdb "github.com/duckdb/duckdb-go/v2"
	"github.com/withObsrvr/ttp-processor-demo/ducklake-ingestion-obsrvr-v3/go/era"
)

// WriteBatch represents a batch of data from one network ready to be written to DuckDB
type WriteBatch struct {
	// Network metadata
	NetworkName string // testnet, mainnet, futurenet
	Schema      string // testnet., mainnet., futurenet. (for schema isolation)

	// Buffered data from WorkerBuffers
	Ledgers           []LedgerData
	Transactions      []TransactionData
	Operations        []OperationData
	Balances          []BalanceData
	Effects           []EffectData
	Trades            []TradeData
	Accounts          []AccountData
	Trustlines        []TrustlineData
	Offers            []OfferData
	ClaimableBalances []ClaimableBalanceData
	LiquidityPools    []LiquidityPoolData
	ContractEvents    []ContractEventData
	ContractData      []ContractDataData
	ContractCode      []ContractCodeData
	ConfigSettings    []ConfigSettingData
	TTL               []TTLData
	EvictedKeys       []EvictedKeyData
	RestoredKeys      []RestoredKeyData
	AccountSigners    []AccountSignerData

	// Quality check results (pre-computed before submission)
	QualityResults []QualityCheckResult

	// Batch metadata
	BatchStartLedger uint32
	BatchEndLedger   uint32
	SubmittedAt      time.Time

	// Result channel for async feedback
	ResultChan chan error
}

// NewWriteBatchFromBuffers converts WorkerBuffers to WriteBatch for queue submission
// This is called by Ingester.flush() to prepare data for the shared write queue
func NewWriteBatchFromBuffers(
	buffers *WorkerBuffers,
	networkName string,
	schema string,
	batchStartLedger uint32,
	batchEndLedger uint32,
	qualityResults []QualityCheckResult,
) *WriteBatch {
	return &WriteBatch{
		// Network metadata
		NetworkName: networkName,
		Schema:      schema,

		// Copy all data slices from WorkerBuffers
		Ledgers:           buffers.ledgers,
		Transactions:      buffers.transactions,
		Operations:        buffers.operations,
		Balances:          buffers.balances,
		Effects:           buffers.effects,
		Trades:            buffers.trades,
		Accounts:          buffers.accounts,
		Trustlines:        buffers.trustlines,
		Offers:            buffers.offers,
		ClaimableBalances: buffers.claimableBalances,
		LiquidityPools:    buffers.liquidityPools,
		ContractEvents:    buffers.contractEvents,
		ContractData:      buffers.contractData,
		ContractCode:      buffers.contractCode,
		ConfigSettings:    buffers.configSettings,
		TTL:               buffers.ttl,
		EvictedKeys:       buffers.evictedKeys,
		RestoredKeys:      buffers.restoredKeys,
		AccountSigners:    buffers.accountSigners,

		// Quality results (pre-computed)
		QualityResults: qualityResults,

		// Batch metadata
		BatchStartLedger: batchStartLedger,
		BatchEndLedger:   batchEndLedger,
		SubmittedAt:      time.Time{}, // Will be set by WriteQueue.Submit()

		// Create result channel for async feedback
		ResultChan: make(chan error, 1),
	}
}

// WriteQueue manages serialized writes to DuckDB from multiple network pipelines
type WriteQueue struct {
	queue   chan *WriteBatch
	done    chan struct{}
	wg      sync.WaitGroup
	metrics *WriteQueueMetrics

	// Configuration
	queueSize       int
	timeoutSeconds  int
	logQueueDepth   bool
	logIntervalSecs int
}

// WriteQueueMetrics tracks queue performance
type WriteQueueMetrics struct {
	mu sync.Mutex

	TotalBatchesSubmitted   int64
	TotalBatchesWritten     int64
	TotalBatchesFailed      int64
	TotalLedgersWritten     int64
	TotalTransactionsWritten int64
	CurrentQueueDepth       int

	// Per-network metrics
	NetworkMetrics map[string]*NetworkWriteMetrics
}

// NetworkWriteMetrics tracks per-network write performance
type NetworkWriteMetrics struct {
	BatchesWritten     int64
	LedgersWritten     int64
	TransactionsWritten int64
	LastWriteTime      time.Time
	TotalWriteTime     time.Duration
}

// NewWriteQueue creates a new write queue for serializing DuckDB writes
func NewWriteQueue(queueSize, timeoutSeconds int, logQueueDepth bool, logIntervalSecs int) *WriteQueue {
	return &WriteQueue{
		queue:           make(chan *WriteBatch, queueSize),
		done:            make(chan struct{}),
		queueSize:       queueSize,
		timeoutSeconds:  timeoutSeconds,
		logQueueDepth:   logQueueDepth,
		logIntervalSecs: logIntervalSecs,
		metrics: &WriteQueueMetrics{
			NetworkMetrics: make(map[string]*NetworkWriteMetrics),
		},
	}
}

// Submit submits a batch to the write queue (non-blocking with timeout)
func (wq *WriteQueue) Submit(batch *WriteBatch) error {
	wq.metrics.mu.Lock()
	wq.metrics.TotalBatchesSubmitted++
	wq.metrics.CurrentQueueDepth = len(wq.queue)
	wq.metrics.mu.Unlock()

	batch.SubmittedAt = time.Now()

	// Try to submit with timeout
	timeout := time.Duration(wq.timeoutSeconds) * time.Second
	select {
	case wq.queue <- batch:
		log.Printf("[%s] Batch submitted to write queue (ledgers %d-%d, queue depth: %d/%d)",
			batch.NetworkName, batch.BatchStartLedger, batch.BatchEndLedger,
			len(wq.queue), wq.queueSize)
		return nil
	case <-time.After(timeout):
		return fmt.Errorf("[%s] write queue submission timeout after %v (queue full)", batch.NetworkName, timeout)
	}
}

// Start starts the write queue worker goroutine
// The worker drains the queue and writes batches to DuckDB serially
func (wq *WriteQueue) Start(ctx context.Context, writer *QueueWriter) {
	wq.wg.Add(1)
	go func() {
		defer wq.wg.Done()
		wq.writerLoop(ctx, writer)
	}()

	// Start queue depth monitoring if enabled
	if wq.logQueueDepth && wq.logIntervalSecs > 0 {
		wq.wg.Add(1)
		go func() {
			defer wq.wg.Done()
			wq.monitorLoop(ctx)
		}()
	}
}

// writerLoop is the main queue draining loop
func (wq *WriteQueue) writerLoop(ctx context.Context, writer *QueueWriter) {
	log.Printf("[WRITE-QUEUE] Writer loop started")

	for {
		select {
		case <-ctx.Done():
			log.Printf("[WRITE-QUEUE] Context cancelled, draining remaining batches...")
			wq.drainQueue(writer)
			log.Printf("[WRITE-QUEUE] Writer loop stopped")
			return

		case <-wq.done:
			log.Printf("[WRITE-QUEUE] Shutdown signal received, draining remaining batches...")
			wq.drainQueue(writer)
			log.Printf("[WRITE-QUEUE] Writer loop stopped")
			return

		case batch := <-wq.queue:
			wq.writeBatch(batch, writer)
		}
	}
}

// drainQueue drains remaining batches on shutdown
func (wq *WriteQueue) drainQueue(writer *QueueWriter) {
	for {
		select {
		case batch := <-wq.queue:
			log.Printf("[WRITE-QUEUE] Draining batch from %s", batch.NetworkName)
			wq.writeBatch(batch, writer)
		default:
			log.Printf("[WRITE-QUEUE] Queue drained")
			return
		}
	}
}

// writeBatch writes a single batch to DuckDB
func (wq *WriteQueue) writeBatch(batch *WriteBatch, writer *QueueWriter) {
	start := time.Now()
	queueWaitTime := start.Sub(batch.SubmittedAt)

	log.Printf("[WRITE-QUEUE] [%s] Writing batch (ledgers %d-%d, waited %v in queue)",
		batch.NetworkName, batch.BatchStartLedger, batch.BatchEndLedger, queueWaitTime)

	// Write batch using QueueWriter
	err := writer.WriteBatch(batch)

	// Update metrics
	wq.metrics.mu.Lock()
	if err != nil {
		wq.metrics.TotalBatchesFailed++
	} else {
		wq.metrics.TotalBatchesWritten++
		wq.metrics.TotalLedgersWritten += int64(len(batch.Ledgers))
		wq.metrics.TotalTransactionsWritten += int64(len(batch.Transactions))

		// Update per-network metrics
		netMetrics, ok := wq.metrics.NetworkMetrics[batch.NetworkName]
		if !ok {
			netMetrics = &NetworkWriteMetrics{}
			wq.metrics.NetworkMetrics[batch.NetworkName] = netMetrics
		}
		netMetrics.BatchesWritten++
		netMetrics.LedgersWritten += int64(len(batch.Ledgers))
		netMetrics.TransactionsWritten += int64(len(batch.Transactions))
		netMetrics.LastWriteTime = time.Now()
		netMetrics.TotalWriteTime += time.Since(start)
	}
	wq.metrics.CurrentQueueDepth = len(wq.queue)
	wq.metrics.mu.Unlock()

	// Send result to batch submitter (if result channel exists)
	if batch.ResultChan != nil {
		select {
		case batch.ResultChan <- err:
		default:
			// Don't block if nobody is listening
		}
	}

	if err != nil {
		log.Printf("[WRITE-QUEUE] [%s] ❌ Batch write failed (took %v): %v",
			batch.NetworkName, time.Since(start), err)
	} else {
		log.Printf("[WRITE-QUEUE] [%s] ✅ Batch written successfully (took %v, %d ledgers, %d transactions)",
			batch.NetworkName, time.Since(start), len(batch.Ledgers), len(batch.Transactions))
	}
}

// monitorLoop logs queue depth at regular intervals
func (wq *WriteQueue) monitorLoop(ctx context.Context) {
	ticker := time.NewTicker(time.Duration(wq.logIntervalSecs) * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-wq.done:
			return
		case <-ticker.C:
			wq.metrics.mu.Lock()
			depth := len(wq.queue)
			utilization := float64(depth) / float64(wq.queueSize) * 100
			submitted := wq.metrics.TotalBatchesSubmitted
			written := wq.metrics.TotalBatchesWritten
			failed := wq.metrics.TotalBatchesFailed
			wq.metrics.mu.Unlock()

			log.Printf("[WRITE-QUEUE] Queue depth: %d/%d (%.1f%%), submitted: %d, written: %d, failed: %d",
				depth, wq.queueSize, utilization, submitted, written, failed)

			// Log per-network stats
			wq.metrics.mu.Lock()
			for network, netMetrics := range wq.metrics.NetworkMetrics {
				if netMetrics.BatchesWritten > 0 {
					avgWriteTime := netMetrics.TotalWriteTime / time.Duration(netMetrics.BatchesWritten)
					log.Printf("[WRITE-QUEUE] [%s] batches: %d, ledgers: %d, txs: %d, avg write time: %v",
						network, netMetrics.BatchesWritten, netMetrics.LedgersWritten,
						netMetrics.TransactionsWritten, avgWriteTime)
				}
			}
			wq.metrics.mu.Unlock()
		}
	}
}

// Stop stops the write queue gracefully
func (wq *WriteQueue) Stop() {
	log.Printf("[WRITE-QUEUE] Stopping...")
	close(wq.done)
	wq.wg.Wait()
	log.Printf("[WRITE-QUEUE] Stopped")
}

// GetMetrics returns current queue metrics (thread-safe)
func (wq *WriteQueue) GetMetrics() WriteQueueMetrics {
	wq.metrics.mu.Lock()
	defer wq.metrics.mu.Unlock()

	// Deep copy to avoid race conditions
	metrics := WriteQueueMetrics{
		TotalBatchesSubmitted:    wq.metrics.TotalBatchesSubmitted,
		TotalBatchesWritten:      wq.metrics.TotalBatchesWritten,
		TotalBatchesFailed:       wq.metrics.TotalBatchesFailed,
		TotalLedgersWritten:      wq.metrics.TotalLedgersWritten,
		TotalTransactionsWritten: wq.metrics.TotalTransactionsWritten,
		CurrentQueueDepth:        len(wq.queue),
		NetworkMetrics:           make(map[string]*NetworkWriteMetrics),
	}

	for network, netMetrics := range wq.metrics.NetworkMetrics {
		metrics.NetworkMetrics[network] = &NetworkWriteMetrics{
			BatchesWritten:      netMetrics.BatchesWritten,
			LedgersWritten:      netMetrics.LedgersWritten,
			TransactionsWritten: netMetrics.TransactionsWritten,
			LastWriteTime:       netMetrics.LastWriteTime,
			TotalWriteTime:      netMetrics.TotalWriteTime,
		}
	}

	return metrics
}

// QueueWriter handles the actual DuckDB write operations for batches
// This is separate from WriteQueue to allow easier testing and isolation
type QueueWriter struct {
	// DuckDB connections and appenders
	connector   *duckdb.Connector
	conn        *duckdb.Conn
	db          *sql.DB     // Used for CHECKPOINT and other SQL commands
	catalogName string      // DuckDB catalog name for multi-network mode

	// Per-schema appenders (map of schema name -> appenders)
	schemaAppenders map[string]*NetworkAppenders
	mu              sync.Mutex // Protects schemaAppenders map

	// Era config (shared across all networks)
	eraConfig *era.Config
}

// NetworkAppenders holds all DuckDB appenders for a network
// Each network gets its own set of appenders with schema prefix
type NetworkAppenders struct {
	ledgerAppender           *duckdb.Appender
	transactionAppender      *duckdb.Appender
	operationAppender        *duckdb.Appender
	balanceAppender          *duckdb.Appender
	effectAppender           *duckdb.Appender
	tradeAppender            *duckdb.Appender
	accountAppender          *duckdb.Appender
	trustlineAppender        *duckdb.Appender
	offerAppender            *duckdb.Appender
	claimableBalanceAppender *duckdb.Appender
	liquidityPoolAppender    *duckdb.Appender
	contractEventAppender    *duckdb.Appender
	contractDataAppender     *duckdb.Appender
	contractCodeAppender     *duckdb.Appender
	configSettingsAppender   *duckdb.Appender
	ttlAppender              *duckdb.Appender
	evictedKeysAppender      *duckdb.Appender
	restoredKeysAppender     *duckdb.Appender
	accountSignersAppender   *duckdb.Appender
}

// NewQueueWriter creates a QueueWriter with shared DuckDB connection
func NewQueueWriter(connector *duckdb.Connector, db *sql.DB, catalogName string, eraConfig *era.Config) (*QueueWriter, error) {
	// Get native connection for appenders
	conn, err := connector.Connect(context.Background())
	if err != nil {
		return nil, fmt.Errorf("failed to get native connection: %w", err)
	}
	duckConn, ok := conn.(*duckdb.Conn)
	if !ok {
		return nil, fmt.Errorf("failed to cast to *duckdb.Conn")
	}

	// Note: Catalog is opened directly as main database (not attached)
	// Appenders will use schema.table format (e.g., testnet.ledgers_row_v2)
	// No need to execute USE catalog_name since the catalog is the main DB
	log.Printf("[QUEUE-WRITER] Initialized with catalog: %s (opened as main database)", catalogName)

	// Note: Appenders will be created per-network as batches arrive
	// This allows schema isolation (testnet., mainnet., etc.)

	return &QueueWriter{
		connector:       connector,
		conn:            duckConn,
		db:              db,
		catalogName:     catalogName,
		eraConfig:       eraConfig,
		schemaAppenders: make(map[string]*NetworkAppenders),
	}, nil
}

// WriteBatch writes a batch to DuckDB using the appropriate schema
func (qw *QueueWriter) WriteBatch(batch *WriteBatch) error {
	log.Printf("[QUEUE-WRITER] Writing batch from %s (ledgers %d-%d)",
		batch.NetworkName, batch.BatchStartLedger, batch.BatchEndLedger)

	// Get or create appenders for this network's schema
	appenders, err := qw.getOrCreateAppenders(batch.Schema)
	if err != nil {
		return fmt.Errorf("failed to get appenders for schema %s: %w", batch.Schema, err)
	}

	// Write ledgers
	if len(batch.Ledgers) > 0 {
		for _, ledger := range batch.Ledgers {
			err := appenders.ledgerAppender.AppendRow(
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
				qw.eraConfig.EraID,
				qw.eraConfig.VersionLabel,
			)
			if err != nil {
				return fmt.Errorf("failed to append ledger %d: %w", ledger.Sequence, err)
			}
		}
		if err := appenders.ledgerAppender.Flush(); err != nil {
			return fmt.Errorf("failed to flush ledger appender: %w", err)
		}
		log.Printf("[QUEUE-WRITER] [%s] ✓ Wrote %d ledgers", batch.NetworkName, len(batch.Ledgers))
	}

	// Write transactions
	if len(batch.Transactions) > 0 {
		for _, tx := range batch.Transactions {
			err := appenders.transactionAppender.AppendRow(
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
				ptrToInterface(tx.SourceAccountMuxed),
				ptrToInterface(tx.FeeAccountMuxed),
				ptrToInterface(tx.InnerTransactionHash),
				ptrToInterface(tx.FeeBumpFee),
				ptrToInterface(tx.MaxFeeBid),
				ptrToInterface(tx.InnerSourceAccount),
				ptrToInterface(tx.TimeboundsMinTime),
				ptrToInterface(tx.TimeboundsMaxTime),
				ptrToInterface(tx.LedgerboundsMin),
				ptrToInterface(tx.LedgerboundsMax),
				ptrToInterface(tx.MinSequenceNumber),
				ptrToInterface(tx.MinSequenceAge),
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
				tx.SignaturesCount,
				tx.NewAccount,
				ptrToInterface(tx.TxEnvelope),
				ptrToInterface(tx.TxResult),
				ptrToInterface(tx.TxMeta),
				ptrToInterface(tx.TxFeeMeta),
				ptrToInterface(tx.TxSigners),
				ptrToInterface(tx.ExtraSigners),
				qw.eraConfig.EraID,
				qw.eraConfig.VersionLabel,
			)
			if err != nil {
				return fmt.Errorf("failed to append transaction %s: %w", tx.TransactionHash, err)
			}
		}
		if err := appenders.transactionAppender.Flush(); err != nil {
			return fmt.Errorf("failed to flush transaction appender: %w", err)
		}
		log.Printf("[QUEUE-WRITER] [%s] ✓ Wrote %d transactions", batch.NetworkName, len(batch.Transactions))
	}

	// Write operations
	if len(batch.Operations) > 0 {
		for _, op := range batch.Operations {
			err := appenders.operationAppender.AppendRow(
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
				ptrToInterface(op.SourceAccountMuxed),
				// Asset fields (8)
				ptrToInterface(op.Asset),
				ptrToInterface(op.AssetType),
				ptrToInterface(op.AssetCode),
				ptrToInterface(op.AssetIssuer),
				ptrToInterface(op.SourceAsset),
				ptrToInterface(op.SourceAssetType),
				ptrToInterface(op.SourceAssetCode),
				ptrToInterface(op.SourceAssetIssuer),
				// Amount fields (4)
				ptrToInterface(op.Amount),
				ptrToInterface(op.SourceAmount),
				ptrToInterface(op.DestinationMin),
				ptrToInterface(op.StartingBalance),
				// Destination (1)
				ptrToInterface(op.Destination),
				// Trustline (5)
				ptrToInterface(op.TrustlineLimit),
				ptrToInterface(op.Trustor),
				ptrToInterface(op.Authorize),
				ptrToInterface(op.AuthorizeToMaintainLiabilities),
				ptrToInterface(op.TrustLineFlags),
				// Claimable balance (2)
				ptrToInterface(op.BalanceID),
				ptrToInterface(op.ClaimantsCount),
				// Sponsorship (1)
				ptrToInterface(op.SponsoredID),
				// DEX (11)
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
				// Soroban (4)
				ptrToInterface(op.SorobanOperation),
				ptrToInterface(op.SorobanFunction),
				ptrToInterface(op.SorobanContractID),
				ptrToInterface(op.SorobanAuthRequired),
				// Account operations (8)
				ptrToInterface(op.BumpTo),
				ptrToInterface(op.SetFlags),
				ptrToInterface(op.ClearFlags),
				ptrToInterface(op.HomeDomain),
				ptrToInterface(op.MasterWeight),
				ptrToInterface(op.LowThreshold),
				ptrToInterface(op.MediumThreshold),
				ptrToInterface(op.HighThreshold),
				// Other (2)
				ptrToInterface(op.DataName),
				ptrToInterface(op.DataValue),
				qw.eraConfig.EraID,
				qw.eraConfig.VersionLabel,
			)
			if err != nil {
				return fmt.Errorf("failed to append operation: %w", err)
			}
		}
		if err := appenders.operationAppender.Flush(); err != nil {
			return fmt.Errorf("failed to flush operation appender: %w", err)
		}
		log.Printf("[QUEUE-WRITER] [%s] ✓ Wrote %d operations", batch.NetworkName, len(batch.Operations))
	}

	// Write accounts
	if len(batch.Accounts) > 0 {
		for _, account := range batch.Accounts {
			err := appenders.accountAppender.AppendRow(
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
				qw.eraConfig.EraID,
				qw.eraConfig.VersionLabel,
			)
			if err != nil {
				return fmt.Errorf("failed to append account %s: %w", account.AccountID, err)
			}
		}
		if err := appenders.accountAppender.Flush(); err != nil {
			return fmt.Errorf("failed to flush account appender: %w", err)
		}
		log.Printf("[QUEUE-WRITER] [%s] ✓ Wrote %d accounts", batch.NetworkName, len(batch.Accounts))
	}

	// Write trustlines
	if len(batch.Trustlines) > 0 {
		for _, tl := range batch.Trustlines {
			err := appenders.trustlineAppender.AppendRow(
				// Identity (4 fields)
				tl.AccountID,
				tl.AssetCode,
				tl.AssetIssuer,
				tl.AssetType,
				// Trust & Balance (4 fields)
				tl.Balance,
				tl.TrustLimit,
				tl.BuyingLiabilities,
				tl.SellingLiabilities,
				// Authorization (3 fields)
				tl.Authorized,
				tl.AuthorizedToMaintainLiabilities,
				tl.ClawbackEnabled,
				// Metadata (5 fields)
				tl.LedgerSequence,
				tl.CreatedAt,
				tl.LedgerRange,
				qw.eraConfig.EraID,
				qw.eraConfig.VersionLabel,
			)
			if err != nil {
				return fmt.Errorf("failed to append trustline: %w", err)
			}
		}
		if err := appenders.trustlineAppender.Flush(); err != nil {
			return fmt.Errorf("failed to flush trustline appender: %w", err)
		}
		log.Printf("[QUEUE-WRITER] [%s] ✓ Wrote %d trustlines", batch.NetworkName, len(batch.Trustlines))
	}

	// Write offers
	if len(batch.Offers) > 0 {
		for _, offer := range batch.Offers {
			err := appenders.offerAppender.AppendRow(
				// Identity (4 fields)
				offer.OfferID,
				offer.SellerAccount,
				offer.LedgerSequence,
				offer.ClosedAt,
				// Selling Asset (3 fields)
				offer.SellingAssetType,
				ptrToInterface(offer.SellingAssetCode),
				ptrToInterface(offer.SellingAssetIssuer),
				// Buying Asset (3 fields)
				offer.BuyingAssetType,
				ptrToInterface(offer.BuyingAssetCode),
				ptrToInterface(offer.BuyingAssetIssuer),
				// Offer Details (2 fields)
				offer.Amount,
				offer.Price,
				// Flags (1 field)
				offer.Flags,
				// Metadata (2 fields)
				offer.CreatedAt,
				offer.LedgerRange,
				qw.eraConfig.EraID,
				qw.eraConfig.VersionLabel,
			)
			if err != nil {
				return fmt.Errorf("failed to append offer %d: %w", offer.OfferID, err)
			}
		}
		if err := appenders.offerAppender.Flush(); err != nil {
			return fmt.Errorf("failed to flush offer appender: %w", err)
		}
		log.Printf("[QUEUE-WRITER] [%s] ✓ Wrote %d offers", batch.NetworkName, len(batch.Offers))
	}

	// Write claimable balances
	if len(batch.ClaimableBalances) > 0 {
		for _, cb := range batch.ClaimableBalances {
			err := appenders.claimableBalanceAppender.AppendRow(
				// Identity (4 fields)
				cb.BalanceID,
				cb.Sponsor,
				cb.LedgerSequence,
				cb.ClosedAt,
				// Asset & Amount (4 fields)
				cb.AssetType,
				ptrToInterface(cb.AssetCode),
				ptrToInterface(cb.AssetIssuer),
				cb.Amount,
				// Claimants (1 field)
				cb.ClaimantsCount,
				// Flags (1 field)
				cb.Flags,
				// Metadata (4 fields)
				cb.CreatedAt,
				cb.LedgerRange,
				qw.eraConfig.EraID,
				qw.eraConfig.VersionLabel,
			)
			if err != nil {
				return fmt.Errorf("failed to append claimable balance %s: %w", cb.BalanceID, err)
			}
		}
		if err := appenders.claimableBalanceAppender.Flush(); err != nil {
			return fmt.Errorf("failed to flush claimable balance appender: %w", err)
		}
		log.Printf("[QUEUE-WRITER] [%s] ✓ Wrote %d claimable balances", batch.NetworkName, len(batch.ClaimableBalances))
	}

	// Write liquidity pools
	if len(batch.LiquidityPools) > 0 {
		for _, pool := range batch.LiquidityPools {
			err := appenders.liquidityPoolAppender.AppendRow(
				// Identity (3 fields)
				pool.LiquidityPoolID,
				pool.LedgerSequence,
				pool.ClosedAt,
				// Pool Type (1 field)
				pool.PoolType,
				// Fee (1 field)
				pool.Fee,
				// Pool Shares (2 fields)
				pool.TrustlineCount,
				pool.TotalPoolShares,
				// Asset A (4 fields)
				pool.AssetAType,
				ptrToInterface(pool.AssetACode),
				ptrToInterface(pool.AssetAIssuer),
				pool.AssetAAmount,
				// Asset B (4 fields)
				pool.AssetBType,
				ptrToInterface(pool.AssetBCode),
				ptrToInterface(pool.AssetBIssuer),
				pool.AssetBAmount,
				// Metadata (2 fields)
				pool.CreatedAt,
				pool.LedgerRange,
				qw.eraConfig.EraID,
				qw.eraConfig.VersionLabel,
			)
			if err != nil {
				return fmt.Errorf("failed to append liquidity pool %s: %w", pool.LiquidityPoolID, err)
			}
		}
		if err := appenders.liquidityPoolAppender.Flush(); err != nil {
			return fmt.Errorf("failed to flush liquidity pool appender: %w", err)
		}
		log.Printf("[QUEUE-WRITER] [%s] ✓ Wrote %d liquidity pools", batch.NetworkName, len(batch.LiquidityPools))
	}

	// Write contract data
	if len(batch.ContractData) > 0 {
		for _, cd := range batch.ContractData {
			err := appenders.contractDataAppender.AppendRow(
				// Identity (3 fields)
				cd.ContractId,
				cd.LedgerSequence,
				cd.LedgerKeyHash,
				// Contract metadata (2 fields)
				cd.ContractKeyType,
				cd.ContractDurability,
				// Asset information (3 fields, nullable)
				ptrToInterface(cd.AssetCode),
				ptrToInterface(cd.AssetIssuer),
				ptrToInterface(cd.AssetType),
				// Balance information (2 fields, nullable)
				ptrToInterface(cd.BalanceHolder),
				ptrToInterface(cd.Balance),
				// Ledger metadata (4 fields)
				cd.LastModifiedLedger,
				cd.LedgerEntryChange,
				cd.Deleted,
				cd.ClosedAt,
				// XDR data (1 field)
				cd.ContractDataXDR,
				// Metadata (4 fields)
				cd.CreatedAt,
				cd.LedgerRange,
				qw.eraConfig.EraID,
				qw.eraConfig.VersionLabel,
			)
			if err != nil {
				return fmt.Errorf("failed to append contract data: %w", err)
			}
		}
		if err := appenders.contractDataAppender.Flush(); err != nil {
			return fmt.Errorf("failed to flush contract data appender: %w", err)
		}
		log.Printf("[QUEUE-WRITER] [%s] ✓ Wrote %d contract data entries", batch.NetworkName, len(batch.ContractData))
	}

	// Write contract code
	if len(batch.ContractCode) > 0 {
		for _, cc := range batch.ContractCode {
			err := appenders.contractCodeAppender.AppendRow(
				// Identity (2 fields)
				cc.ContractCodeHash,
				cc.LedgerKeyHash,
				// Extension (1 field)
				cc.ContractCodeExtV,
				// Ledger metadata (4 fields)
				cc.LastModifiedLedger,
				cc.LedgerEntryChange,
				cc.Deleted,
				cc.ClosedAt,
				// Ledger tracking (1 field)
				cc.LedgerSequence,
				// WASM metadata (10 fields, nullable)
				ptrToInterface(cc.NInstructions),
				ptrToInterface(cc.NFunctions),
				ptrToInterface(cc.NGlobals),
				ptrToInterface(cc.NTableEntries),
				ptrToInterface(cc.NTypes),
				ptrToInterface(cc.NDataSegments),
				ptrToInterface(cc.NElemSegments),
				ptrToInterface(cc.NImports),
				ptrToInterface(cc.NExports),
				ptrToInterface(cc.NDataSegmentBytes),
				// Metadata (4 fields)
				cc.CreatedAt,
				cc.LedgerRange,
				qw.eraConfig.EraID,
				qw.eraConfig.VersionLabel,
			)
			if err != nil {
				return fmt.Errorf("failed to append contract code: %w", err)
			}
		}
		if err := appenders.contractCodeAppender.Flush(); err != nil {
			return fmt.Errorf("failed to flush contract code appender: %w", err)
		}
		log.Printf("[QUEUE-WRITER] [%s] ✓ Wrote %d contract code entries", batch.NetworkName, len(batch.ContractCode))
	}

	// Write config settings
	if len(batch.ConfigSettings) > 0 {
		for _, cs := range batch.ConfigSettings {
			err := appenders.configSettingsAppender.AppendRow(
				// Identity (2 fields)
				cs.ConfigSettingID,
				cs.LedgerSequence,
				// Ledger metadata (3 fields)
				cs.LastModifiedLedger,
				cs.Deleted,
				cs.ClosedAt,
				// Soroban compute settings (4 fields, nullable)
				ptrToInterface(cs.LedgerMaxInstructions),
				ptrToInterface(cs.TxMaxInstructions),
				ptrToInterface(cs.FeeRatePerInstructionsIncrement),
				ptrToInterface(cs.TxMemoryLimit),
				// Metadata (4 fields)
				cs.CreatedAt,
				cs.LedgerRange,
				qw.eraConfig.EraID,
				qw.eraConfig.VersionLabel,
			)
			if err != nil {
				return fmt.Errorf("failed to append config setting: %w", err)
			}
		}
		if err := appenders.configSettingsAppender.Flush(); err != nil {
			return fmt.Errorf("failed to flush config settings appender: %w", err)
		}
		log.Printf("[QUEUE-WRITER] [%s] ✓ Wrote %d config settings", batch.NetworkName, len(batch.ConfigSettings))
	}

	// Write TTL entries
	if len(batch.TTL) > 0 {
		for _, ttl := range batch.TTL {
			err := appenders.ttlAppender.AppendRow(
				// Identity (2 fields)
				ttl.KeyHash,
				ttl.LedgerSequence,
				// TTL tracking (3 fields)
				ttl.LiveUntilLedgerSeq,
				ttl.TTLRemaining,
				ttl.Expired,
				// Ledger metadata (3 fields)
				ttl.LastModifiedLedger,
				ttl.Deleted,
				ttl.ClosedAt,
				// Metadata (4 fields)
				ttl.CreatedAt,
				ttl.LedgerRange,
				qw.eraConfig.EraID,
				qw.eraConfig.VersionLabel,
			)
			if err != nil {
				return fmt.Errorf("failed to append TTL entry: %w", err)
			}
		}
		if err := appenders.ttlAppender.Flush(); err != nil {
			return fmt.Errorf("failed to flush TTL appender: %w", err)
		}
		log.Printf("[QUEUE-WRITER] [%s] ✓ Wrote %d TTL entries", batch.NetworkName, len(batch.TTL))
	}

	// Write account signers
	if len(batch.AccountSigners) > 0 {
		for _, signer := range batch.AccountSigners {
			err := appenders.accountSignersAppender.AppendRow(
				// Identity (3 fields)
				signer.AccountID,
				signer.Signer,
				signer.LedgerSequence,
				// Signer details (2 fields)
				signer.Weight,
				signer.Sponsor,
				// Status (1 field)
				signer.Deleted,
				// Metadata (5 fields)
				signer.ClosedAt,
				signer.LedgerRange,
				signer.CreatedAt,
				qw.eraConfig.EraID,
				qw.eraConfig.VersionLabel,
			)
			if err != nil {
				return fmt.Errorf("failed to append account signer: %w", err)
			}
		}
		if err := appenders.accountSignersAppender.Flush(); err != nil {
			return fmt.Errorf("failed to flush account signers appender: %w", err)
		}
		log.Printf("[QUEUE-WRITER] [%s] ✓ Wrote %d account signers", batch.NetworkName, len(batch.AccountSigners))
	}

	// Checkpoint the WAL to merge changes into main database file
	// This keeps the WAL file small and ensures data is persisted
	if _, err := qw.db.Exec("CHECKPOINT"); err != nil {
		// Log error but don't fail the batch - data is already written to WAL
		log.Printf("[QUEUE-WRITER] [%s] ⚠️  CHECKPOINT warning (non-fatal): %v", batch.NetworkName, err)
	}

	log.Printf("[QUEUE-WRITER] [%s] ✅ Batch write complete (checkpointed)", batch.NetworkName)
	return nil
}

// getOrCreateAppenders gets or creates appenders for the given schema
func (qw *QueueWriter) getOrCreateAppenders(schema string) (*NetworkAppenders, error) {
	qw.mu.Lock()
	defer qw.mu.Unlock()

	// Return existing appenders if already created
	if appenders, ok := qw.schemaAppenders[schema]; ok {
		return appenders, nil
	}

	// Create new appenders for this schema
	log.Printf("[QUEUE-WRITER] Creating appenders for schema: %s (catalog context: %s)", schema, qw.catalogName)

	appenders := &NetworkAppenders{}
	var err error

	// Create ledger appender
	// Note: Using just schema name since USE catalog_name was executed in NewQueueWriter
	appenders.ledgerAppender, err = duckdb.NewAppenderFromConn(qw.conn, schema, "ledgers_row_v2")
	if err != nil {
		return nil, fmt.Errorf("failed to create ledger appender: %w", err)
	}

	// Create transaction appender
	appenders.transactionAppender, err = duckdb.NewAppenderFromConn(qw.conn, schema, "transactions_row_v2")
	if err != nil {
		return nil, fmt.Errorf("failed to create transaction appender: %w", err)
	}

	// Create operation appender
	appenders.operationAppender, err = duckdb.NewAppenderFromConn(qw.conn, schema, "operations_row_v2")
	if err != nil {
		return nil, fmt.Errorf("failed to create operation appender: %w", err)
	}

	// Create balance appender
	appenders.balanceAppender, err = duckdb.NewAppenderFromConn(qw.conn, schema, "native_balances_snapshot_v1")
	if err != nil {
		return nil, fmt.Errorf("failed to create balance appender: %w", err)
	}

	// Create effect appender
	appenders.effectAppender, err = duckdb.NewAppenderFromConn(qw.conn, schema, "effects_row_v1")
	if err != nil {
		return nil, fmt.Errorf("failed to create effect appender: %w", err)
	}

	// Create trade appender
	appenders.tradeAppender, err = duckdb.NewAppenderFromConn(qw.conn, schema, "trades_row_v1")
	if err != nil {
		return nil, fmt.Errorf("failed to create trade appender: %w", err)
	}

	// Create account appender
	appenders.accountAppender, err = duckdb.NewAppenderFromConn(qw.conn, schema, "accounts_snapshot_v1")
	if err != nil {
		return nil, fmt.Errorf("failed to create account appender: %w", err)
	}

	// Create trustline appender
	appenders.trustlineAppender, err = duckdb.NewAppenderFromConn(qw.conn, schema, "trustlines_snapshot_v1")
	if err != nil {
		return nil, fmt.Errorf("failed to create trustline appender: %w", err)
	}

	// Create offer appender
	appenders.offerAppender, err = duckdb.NewAppenderFromConn(qw.conn, schema, "offers_snapshot_v1")
	if err != nil {
		return nil, fmt.Errorf("failed to create offer appender: %w", err)
	}

	// Create claimable balance appender
	appenders.claimableBalanceAppender, err = duckdb.NewAppenderFromConn(qw.conn, schema, "claimable_balances_snapshot_v1")
	if err != nil {
		return nil, fmt.Errorf("failed to create claimable balance appender: %w", err)
	}

	// Create liquidity pool appender
	appenders.liquidityPoolAppender, err = duckdb.NewAppenderFromConn(qw.conn, schema, "liquidity_pools_snapshot_v1")
	if err != nil {
		return nil, fmt.Errorf("failed to create liquidity pool appender: %w", err)
	}

	// Create contract event appender
	appenders.contractEventAppender, err = duckdb.NewAppenderFromConn(qw.conn, schema, "contract_events_stream_v1")
	if err != nil {
		return nil, fmt.Errorf("failed to create contract event appender: %w", err)
	}

	// Create contract data appender
	appenders.contractDataAppender, err = duckdb.NewAppenderFromConn(qw.conn, schema, "contract_data_snapshot_v1")
	if err != nil {
		return nil, fmt.Errorf("failed to create contract data appender: %w", err)
	}

	// Create contract code appender
	appenders.contractCodeAppender, err = duckdb.NewAppenderFromConn(qw.conn, schema, "contract_code_snapshot_v1")
	if err != nil {
		return nil, fmt.Errorf("failed to create contract code appender: %w", err)
	}

	// Create config settings appender
	appenders.configSettingsAppender, err = duckdb.NewAppenderFromConn(qw.conn, schema, "config_settings_snapshot_v1")
	if err != nil {
		return nil, fmt.Errorf("failed to create config settings appender: %w", err)
	}

	// Create TTL appender
	appenders.ttlAppender, err = duckdb.NewAppenderFromConn(qw.conn, schema, "ttl_snapshot_v1")
	if err != nil {
		return nil, fmt.Errorf("failed to create TTL appender: %w", err)
	}

	// Create evicted keys appender
	appenders.evictedKeysAppender, err = duckdb.NewAppenderFromConn(qw.conn, schema, "evicted_keys_state_v1")
	if err != nil {
		return nil, fmt.Errorf("failed to create evicted keys appender: %w", err)
	}

	// Create restored keys appender
	appenders.restoredKeysAppender, err = duckdb.NewAppenderFromConn(qw.conn, schema, "restored_keys_state_v1")
	if err != nil {
		return nil, fmt.Errorf("failed to create restored keys appender: %w", err)
	}

	// Create account signers appender
	appenders.accountSignersAppender, err = duckdb.NewAppenderFromConn(qw.conn, schema, "account_signers_snapshot_v1")
	if err != nil {
		return nil, fmt.Errorf("failed to create account signers appender: %w", err)
	}

	// Store appenders in map
	qw.schemaAppenders[schema] = appenders
	log.Printf("[QUEUE-WRITER] ✓ Created all appenders for schema: %s", schema)

	return appenders, nil
}

// Close closes all appenders and connections
func (qw *QueueWriter) Close() error {
	qw.mu.Lock()
	defer qw.mu.Unlock()

	// Close all appenders for all schemas
	for schema, appenders := range qw.schemaAppenders {
		log.Printf("[QUEUE-WRITER] Closing appenders for schema: %s", schema)

		// Close each appender (Note: duckdb.Appender has Close() method)
		if appenders.ledgerAppender != nil {
			appenders.ledgerAppender.Close()
		}
		if appenders.transactionAppender != nil {
			appenders.transactionAppender.Close()
		}
		if appenders.operationAppender != nil {
			appenders.operationAppender.Close()
		}
		if appenders.balanceAppender != nil {
			appenders.balanceAppender.Close()
		}
		if appenders.effectAppender != nil {
			appenders.effectAppender.Close()
		}
		if appenders.tradeAppender != nil {
			appenders.tradeAppender.Close()
		}
		if appenders.accountAppender != nil {
			appenders.accountAppender.Close()
		}
		if appenders.trustlineAppender != nil {
			appenders.trustlineAppender.Close()
		}
		if appenders.offerAppender != nil {
			appenders.offerAppender.Close()
		}
		if appenders.claimableBalanceAppender != nil {
			appenders.claimableBalanceAppender.Close()
		}
		if appenders.liquidityPoolAppender != nil {
			appenders.liquidityPoolAppender.Close()
		}
		if appenders.contractEventAppender != nil {
			appenders.contractEventAppender.Close()
		}
		if appenders.contractDataAppender != nil {
			appenders.contractDataAppender.Close()
		}
		if appenders.contractCodeAppender != nil {
			appenders.contractCodeAppender.Close()
		}
		if appenders.configSettingsAppender != nil {
			appenders.configSettingsAppender.Close()
		}
		if appenders.ttlAppender != nil {
			appenders.ttlAppender.Close()
		}
		if appenders.evictedKeysAppender != nil {
			appenders.evictedKeysAppender.Close()
		}
		if appenders.restoredKeysAppender != nil {
			appenders.restoredKeysAppender.Close()
		}
		if appenders.accountSignersAppender != nil {
			appenders.accountSignersAppender.Close()
		}
	}

	// Note: duckdb.Conn doesn't have Close() in the public API
	// Connection cleanup is handled by the connector
	return nil
}
