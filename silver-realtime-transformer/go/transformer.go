package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/lib/pq"
)

// RealtimeTransformer handles real-time bronze → silver transformation
type RealtimeTransformer struct {
	config         *Config
	bronzeReader   *BronzeReader
	silverWriter   *SilverWriter
	checkpoint     *CheckpointManager
	silverDB       *sql.DB
	stopChan       chan struct{}

	// Stats
	mu                          sync.RWMutex
	transformationsTotal        int64
	transformationErrors        int64
	lastLedgerSequence          int64
	lastTransformationTime      time.Time
	lastTransformationDuration  time.Duration
	lastTransformationRowCount  int64
}

// NewRealtimeTransformer creates a new realtime transformer
func NewRealtimeTransformer(config *Config, bronzeReader *BronzeReader, silverWriter *SilverWriter, checkpoint *CheckpointManager, silverDB *sql.DB) *RealtimeTransformer {
	return &RealtimeTransformer{
		config:       config,
		bronzeReader: bronzeReader,
		silverWriter: silverWriter,
		checkpoint:   checkpoint,
		silverDB:     silverDB,
		stopChan:     make(chan struct{}),
	}
}

// Start begins the real-time transformation loop
func (rt *RealtimeTransformer) Start() error {
	log.Println("🚀 Starting Real-Time Silver Transformer")
	log.Printf("Poll Interval: %v", rt.config.PollInterval())
	log.Printf("Target Latency: < 10 seconds")

	// Load initial checkpoint
	lastLedger, err := rt.checkpoint.Load()
	if err != nil {
		return fmt.Errorf("failed to load checkpoint: %w", err)
	}

	rt.mu.Lock()
	rt.lastLedgerSequence = lastLedger
	rt.mu.Unlock()

	if lastLedger == 0 {
		log.Println("⚠️  No checkpoint found, starting from beginning")
	} else {
		log.Printf("📍 Resuming from ledger sequence: %d", lastLedger)
	}

	// Run immediate transformation check
	log.Println("🔍 Running initial transformation check...")
	if err := rt.runTransformationCycle(); err != nil {
		log.Printf("⚠️  Initial transformation error: %v", err)
		rt.incrementErrors()
	}

	// Start polling loop
	ticker := time.NewTicker(rt.config.PollInterval())
	defer ticker.Stop()

	log.Println("✅ Transformer ready - polling for new data...")

	for {
		select {
		case <-ticker.C:
			if err := rt.runTransformationCycle(); err != nil {
				log.Printf("❌ Transformation error: %v", err)
				rt.incrementErrors()
			}
		case <-rt.stopChan:
			log.Println("🛑 Stopping transformer...")
			return nil
		}
	}
}

// Stop gracefully stops the transformer
func (rt *RealtimeTransformer) Stop() {
	close(rt.stopChan)
}

// runTransformationCycle executes a single transformation cycle
func (rt *RealtimeTransformer) runTransformationCycle() error {
	startTime := time.Now()
	ctx := context.Background()

	// Get current checkpoint
	lastLedger := rt.getLastLedger()

	// Check for new data in bronze hot
	maxBronzeLedger, err := rt.bronzeReader.GetMaxLedgerSequence(ctx)
	if err != nil {
		return fmt.Errorf("failed to query max ledger: %w", err)
	}

	if maxBronzeLedger == 0 {
		// No data in bronze yet
		return nil
	}

	if maxBronzeLedger <= lastLedger {
		// No new data
		return nil
	}

	var startLedger int64
	if lastLedger == 0 {
		// First run - start from where bronze actually begins
		minBronzeLedger, err := rt.bronzeReader.GetMinLedgerSequence(ctx)
		if err != nil {
			return fmt.Errorf("failed to query min ledger: %w", err)
		}
		if minBronzeLedger == 0 {
			return nil // No data yet
		}
		startLedger = minBronzeLedger
		log.Printf("🆕 First run - starting from MIN ledger in bronze: %d", startLedger)
	} else {
		startLedger = lastLedger + 1
	}
	endLedger := maxBronzeLedger

	// Limit batch size
	if endLedger-startLedger+1 > int64(rt.config.Performance.BatchSize) {
		endLedger = startLedger + int64(rt.config.Performance.BatchSize) - 1
	}

	log.Printf("📊 New data available (ledgers %d to %d)", startLedger, endLedger)

	// Start transaction for atomic writes + checkpoint
	tx, err := rt.silverDB.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	totalRows := int64(0)

	// Transform enriched operations
	opsCount, err := rt.transformEnrichedOperations(ctx, tx, startLedger, endLedger)
	if err != nil {
		return fmt.Errorf("failed to transform enriched operations: %w", err)
	}
	totalRows += opsCount

	// Transform token transfers
	transfersCount, err := rt.transformTokenTransfers(ctx, tx, startLedger, endLedger)
	if err != nil {
		return fmt.Errorf("failed to transform token transfers: %w", err)
	}
	totalRows += transfersCount

	// Transform accounts current (UPSERT pattern - Cycle 2)
	accountsCount, err := rt.transformAccountsCurrent(ctx, tx, startLedger, endLedger)
	if err != nil {
		return fmt.Errorf("failed to transform accounts current: %w", err)
	}
	totalRows += accountsCount

	// Transform accounts snapshot (SCD Type 2 - Cycle 3)
	snapshotCount, err := rt.transformAccountsSnapshot(ctx, tx, startLedger, endLedger)
	if err != nil {
		return fmt.Errorf("failed to transform accounts snapshot: %w", err)
	}
	totalRows += snapshotCount

	// Transform trustlines snapshot (SCD Type 2 - Cycle 3)
	trustlinesCount, err := rt.transformTrustlinesSnapshot(ctx, tx, startLedger, endLedger)
	if err != nil {
		return fmt.Errorf("failed to transform trustlines snapshot: %w", err)
	}
	totalRows += trustlinesCount

	// Transform offers snapshot (SCD Type 2 - Cycle 3)
	offersCount, err := rt.transformOffersSnapshot(ctx, tx, startLedger, endLedger)
	if err != nil {
		return fmt.Errorf("failed to transform offers snapshot: %w", err)
	}
	totalRows += offersCount

	// Transform account signers snapshot (SCD Type 2 - Cycle 3)
	signersCount, err := rt.transformAccountSignersSnapshot(ctx, tx, startLedger, endLedger)
	if err != nil {
		return fmt.Errorf("failed to transform account signers snapshot: %w", err)
	}
	totalRows += signersCount

	// Transform contract invocations (Cycle 5 - Contract Invocations)
	invocationsCount, err := rt.transformContractInvocations(ctx, tx, startLedger, endLedger)
	if err != nil {
		return fmt.Errorf("failed to transform contract invocations: %w", err)
	}
	totalRows += invocationsCount

	// Transform contract calls (Cycle 6 - Cross-Contract Call Tracking for Freighter)
	callsCount, err := rt.transformContractCalls(ctx, tx, startLedger, endLedger)
	if err != nil {
		return fmt.Errorf("failed to transform contract calls: %w", err)
	}
	totalRows += callsCount

	// Update checkpoint
	if err := rt.checkpoint.SaveWithTx(tx, endLedger); err != nil {
		return fmt.Errorf("failed to save checkpoint: %w", err)
	}

	// Commit transaction
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	// Update stats
	duration := time.Since(startTime)
	rt.updateStats(endLedger, duration, totalRows)

	log.Printf("✅ Transformed %d rows in %v (ledgers %d-%d)", totalRows, duration, startLedger, endLedger)

	return nil
}

// transformEnrichedOperations transforms enriched operations for the ledger range
func (rt *RealtimeTransformer) transformEnrichedOperations(ctx context.Context, tx *sql.Tx, startLedger, endLedger int64) (int64, error) {
	rows, err := rt.bronzeReader.QueryEnrichedOperations(ctx, startLedger, endLedger)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	count := int64(0)

	for rows.Next() {
		row := &EnrichedOperationRow{}

		// Temporary variables for scanning NULL arrays
		var setFlagsS, clearFlagsS interface{}

		err := rows.Scan(
			&row.TransactionHash, &row.OperationIndex, &row.LedgerSequence, &row.SourceAccount,
			&row.Type, &row.TypeString, &row.CreatedAt, &row.TransactionSuccessful,
			&row.OperationResultCode, &row.OperationTraceCode, &row.LedgerRange,
			&row.SourceAccountMuxed, &row.Asset, &row.AssetType, &row.AssetCode, &row.AssetIssuer,
			&row.SourceAsset, &row.SourceAssetType, &row.SourceAssetCode, &row.SourceAssetIssuer,
			&row.Destination, &row.DestinationMuxed, &row.Amount, &row.SourceAmount,
			&row.FromAccount, &row.FromMuxed, &row.To, &row.ToMuxed,
			&row.LimitAmount, &row.OfferID,
			&row.SellingAsset, &row.SellingAssetType, &row.SellingAssetCode, &row.SellingAssetIssuer,
			&row.BuyingAsset, &row.BuyingAssetType, &row.BuyingAssetCode, &row.BuyingAssetIssuer,
			&row.PriceN, &row.PriceD, &row.Price,
			&row.StartingBalance, &row.HomeDomain, &row.InflationDest,
			&row.SetFlags, &setFlagsS, &row.ClearFlags, &clearFlagsS,
			&row.MasterKeyWeight, &row.LowThreshold, &row.MedThreshold, &row.HighThreshold,
			&row.SignerAccountID, &row.SignerKey, &row.SignerWeight,
			&row.DataName, &row.DataValue,
			&row.HostFunctionType, &row.Parameters, &row.Address, &row.ContractID, &row.FunctionName,
			&row.BalanceID, &row.Claimant, &row.ClaimantMuxed, &row.Predicate,
			&row.LiquidityPoolID, &row.ReserveAAsset, &row.ReserveAAmount,
			&row.ReserveBAsset, &row.ReserveBAmount, &row.Shares, &row.SharesReceived,
			&row.Into, &row.IntoMuxed,
			&row.Sponsor, &row.SponsoredID, &row.BeginSponsor,
			&row.TxSuccessful, &row.TxFeeCharged, &row.TxMaxFee, &row.TxOperationCount,
			&row.TxMemoType, &row.TxMemo,
			&row.LedgerClosedAt, &row.LedgerTotalCoins, &row.LedgerFeePool,
			&row.LedgerBaseFee, &row.LedgerBaseReserve,
			&row.LedgerTransactionCount, &row.LedgerOperationCount,
			&row.LedgerSuccessfulTxCount, &row.LedgerFailedTxCount,
			&row.IsPaymentOp, &row.IsSorobanOp,
		)

		// Store the NULL arrays
		row.SetFlagsS = setFlagsS
		row.ClearFlagsS = clearFlagsS

		if err != nil {
			return count, fmt.Errorf("failed to scan enriched operation row: %w", err)
		}

		if err := rt.silverWriter.WriteEnrichedOperation(ctx, tx, row); err != nil {
			return count, fmt.Errorf("failed to write enriched operation: %w", err)
		}

		count++
	}

	if err := rows.Err(); err != nil {
		return count, fmt.Errorf("error iterating enriched operations: %w", err)
	}

	return count, nil
}

// transformTokenTransfers transforms token transfers for the ledger range
func (rt *RealtimeTransformer) transformTokenTransfers(ctx context.Context, tx *sql.Tx, startLedger, endLedger int64) (int64, error) {
	rows, err := rt.bronzeReader.QueryTokenTransfers(ctx, startLedger, endLedger)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	count := int64(0)

	for rows.Next() {
		row := &TokenTransferRow{}

		err := rows.Scan(
			&row.Timestamp, &row.TransactionHash, &row.LedgerSequence, &row.SourceType,
			&row.FromAccount, &row.ToAccount, &row.AssetCode, &row.AssetIssuer, &row.Amount,
			&row.TokenContractID, &row.OperationType, &row.TransactionSuccessful,
		)

		if err != nil {
			return count, fmt.Errorf("failed to scan token transfer row: %w", err)
		}

		if err := rt.silverWriter.WriteTokenTransfer(ctx, tx, row); err != nil {
			return count, fmt.Errorf("failed to write token transfer: %w", err)
		}

		count++
	}

	if err := rows.Err(); err != nil {
		return count, fmt.Errorf("error iterating token transfers: %w", err)
	}

	return count, nil
}

// transformAccountsCurrent upserts accounts current state for the ledger range
func (rt *RealtimeTransformer) transformAccountsCurrent(ctx context.Context, tx *sql.Tx, startLedger, endLedger int64) (int64, error) {
	rows, err := rt.bronzeReader.QueryAccountsSnapshot(ctx, startLedger, endLedger)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	count := int64(0)

	for rows.Next() {
		row := &AccountCurrentRow{}

		err := rows.Scan(
			&row.AccountID, &row.Balance, &row.SequenceNumber, &row.NumSubentries,
			&row.NumSponsoring, &row.NumSponsored, &row.HomeDomain,
			&row.MasterWeight, &row.LowThreshold, &row.MedThreshold, &row.HighThreshold,
			&row.Flags, &row.AuthRequired, &row.AuthRevocable, &row.AuthImmutable, &row.AuthClawbackEnabled,
			&row.Signers, &row.SponsorAccount, &row.CreatedAt, &row.UpdatedAt,
			&row.LastModifiedLedger, &row.LedgerRange, &row.EraID, &row.VersionLabel,
		)

		if err != nil {
			return count, fmt.Errorf("failed to scan account row: %w", err)
		}

		if err := rt.silverWriter.WriteAccountCurrent(ctx, tx, row); err != nil {
			return count, fmt.Errorf("failed to write account current: %w", err)
		}

		count++
	}

	if err := rows.Err(); err != nil {
		return count, fmt.Errorf("error iterating accounts: %w", err)
	}

	return count, nil
}

// transformAccountsSnapshot appends account snapshot history (SCD Type 2 - Cycle 3)
func (rt *RealtimeTransformer) transformAccountsSnapshot(ctx context.Context, tx *sql.Tx, startLedger, endLedger int64) (int64, error) {
	// Step 1: INSERT all new snapshots with valid_to = NULL
	rows, err := rt.bronzeReader.QueryAccountsSnapshotAll(ctx, startLedger, endLedger)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	count := int64(0)

	for rows.Next() {
		row := &AccountSnapshotRow{}

		err := rows.Scan(
			&row.AccountID, &row.LedgerSequence, &row.ClosedAt, &row.Balance, &row.SequenceNumber,
			&row.NumSubentries, &row.NumSponsoring, &row.NumSponsored, &row.HomeDomain,
			&row.MasterWeight, &row.LowThreshold, &row.MedThreshold, &row.HighThreshold,
			&row.Flags, &row.AuthRequired, &row.AuthRevocable, &row.AuthImmutable, &row.AuthClawbackEnabled,
			&row.Signers, &row.SponsorAccount, &row.CreatedAt, &row.UpdatedAt,
			&row.LedgerRange, &row.EraID, &row.VersionLabel,
		)

		if err != nil {
			return count, fmt.Errorf("failed to scan account snapshot row: %w", err)
		}

		if err := rt.silverWriter.WriteAccountSnapshot(ctx, tx, row); err != nil {
			return count, fmt.Errorf("failed to write account snapshot: %w", err)
		}

		count++
	}

	if err := rows.Err(); err != nil {
		return count, fmt.Errorf("error iterating account snapshots: %w", err)
	}

	// Step 2: UPDATE valid_to for previous versions (incremental LEAD())
	if err := rt.silverWriter.UpdateAccountSnapshotValidTo(ctx, tx, startLedger, endLedger); err != nil {
		return count, fmt.Errorf("failed to update valid_to: %w", err)
	}

	return count, nil
}

// transformTrustlinesSnapshot appends trustline snapshot history (SCD Type 2 - Cycle 3)
func (rt *RealtimeTransformer) transformTrustlinesSnapshot(ctx context.Context, tx *sql.Tx, startLedger, endLedger int64) (int64, error) {
	rows, err := rt.bronzeReader.QueryTrustlinesSnapshotAll(ctx, startLedger, endLedger)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	count := int64(0)

	for rows.Next() {
		row := &TrustlineSnapshotRow{}

		err := rows.Scan(
			&row.AccountID, &row.AssetCode, &row.AssetIssuer, &row.AssetType,
			&row.Balance, &row.TrustLimit, &row.BuyingLiabilities, &row.SellingLiabilities,
			&row.Authorized, &row.AuthorizedToMaintainLiabilities, &row.ClawbackEnabled,
			&row.LedgerSequence, &row.ClosedAt, &row.CreatedAt,
			&row.LedgerRange, &row.EraID, &row.VersionLabel,
		)

		if err != nil {
			return count, fmt.Errorf("failed to scan trustline snapshot row: %w", err)
		}

		if err := rt.silverWriter.WriteTrustlineSnapshot(ctx, tx, row); err != nil {
			return count, fmt.Errorf("failed to write trustline snapshot: %w", err)
		}

		count++
	}

	if err := rows.Err(); err != nil {
		return count, fmt.Errorf("error iterating trustline snapshots: %w", err)
	}

	if err := rt.silverWriter.UpdateTrustlineSnapshotValidTo(ctx, tx, startLedger, endLedger); err != nil {
		return count, fmt.Errorf("failed to update trustline valid_to: %w", err)
	}

	return count, nil
}

// transformOffersSnapshot appends offer snapshot history (SCD Type 2 - Cycle 3)
func (rt *RealtimeTransformer) transformOffersSnapshot(ctx context.Context, tx *sql.Tx, startLedger, endLedger int64) (int64, error) {
	rows, err := rt.bronzeReader.QueryOffersSnapshotAll(ctx, startLedger, endLedger)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	count := int64(0)

	for rows.Next() {
		row := &OfferSnapshotRow{}

		err := rows.Scan(
			&row.OfferID, &row.SellerAccount, &row.LedgerSequence, &row.ClosedAt,
			&row.SellingAssetType, &row.SellingAssetCode, &row.SellingAssetIssuer,
			&row.BuyingAssetType, &row.BuyingAssetCode, &row.BuyingAssetIssuer,
			&row.Amount, &row.Price, &row.Flags, &row.CreatedAt,
			&row.LedgerRange, &row.EraID, &row.VersionLabel,
		)

		if err != nil {
			return count, fmt.Errorf("failed to scan offer snapshot row: %w", err)
		}

		if err := rt.silverWriter.WriteOfferSnapshot(ctx, tx, row); err != nil {
			return count, fmt.Errorf("failed to write offer snapshot: %w", err)
		}

		count++
	}

	if err := rows.Err(); err != nil {
		return count, fmt.Errorf("error iterating offer snapshots: %w", err)
	}

	if err := rt.silverWriter.UpdateOfferSnapshotValidTo(ctx, tx, startLedger, endLedger); err != nil {
		return count, fmt.Errorf("failed to update offer valid_to: %w", err)
	}

	return count, nil
}

// transformAccountSignersSnapshot appends account signer snapshot history (SCD Type 2 - Cycle 3)
func (rt *RealtimeTransformer) transformAccountSignersSnapshot(ctx context.Context, tx *sql.Tx, startLedger, endLedger int64) (int64, error) {
	rows, err := rt.bronzeReader.QueryAccountSignersSnapshotAll(ctx, startLedger, endLedger)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	count := int64(0)

	for rows.Next() {
		row := &AccountSignerSnapshotRow{}

		err := rows.Scan(
			&row.AccountID, &row.Signer, &row.LedgerSequence, &row.ClosedAt,
			&row.Weight, &row.Sponsor, &row.LedgerRange, &row.EraID, &row.VersionLabel,
		)

		if err != nil {
			return count, fmt.Errorf("failed to scan account signer snapshot row: %w", err)
		}

		if err := rt.silverWriter.WriteAccountSignerSnapshot(ctx, tx, row); err != nil {
			return count, fmt.Errorf("failed to write account signer snapshot: %w", err)
		}

		count++
	}

	if err := rows.Err(); err != nil {
		return count, fmt.Errorf("error iterating account signer snapshots: %w", err)
	}

	if err := rt.silverWriter.UpdateAccountSignerSnapshotValidTo(ctx, tx, startLedger, endLedger); err != nil {
		return count, fmt.Errorf("failed to update account signer valid_to: %w", err)
	}

	return count, nil
}

// GetStats returns current transformation statistics
func (rt *RealtimeTransformer) GetStats() TransformerStats {
	rt.mu.RLock()
	defer rt.mu.RUnlock()

	return TransformerStats{
		TransformationsTotal:       rt.transformationsTotal,
		TransformationErrors:       rt.transformationErrors,
		LastLedgerSequence:         rt.lastLedgerSequence,
		LastTransformationTime:     rt.lastTransformationTime,
		LastTransformationDuration: rt.lastTransformationDuration,
		LastTransformationRowCount: rt.lastTransformationRowCount,
	}
}

// TransformerStats holds transformation statistics
type TransformerStats struct {
	TransformationsTotal       int64
	TransformationErrors       int64
	LastLedgerSequence         int64
	LastTransformationTime     time.Time
	LastTransformationDuration time.Duration
	LastTransformationRowCount int64
}

// updateStats updates internal statistics
func (rt *RealtimeTransformer) updateStats(ledgerSeq int64, duration time.Duration, rowCount int64) {
	rt.mu.Lock()
	defer rt.mu.Unlock()

	rt.transformationsTotal++
	rt.lastLedgerSequence = ledgerSeq
	rt.lastTransformationTime = time.Now()
	rt.lastTransformationDuration = duration
	rt.lastTransformationRowCount = rowCount
}

// incrementErrors increments the error counter
func (rt *RealtimeTransformer) incrementErrors() {
	rt.mu.Lock()
	defer rt.mu.Unlock()

	rt.transformationErrors++
}

// getLastLedger returns the last processed ledger
func (rt *RealtimeTransformer) getLastLedger() int64 {
	rt.mu.RLock()
	defer rt.mu.RUnlock()

	return rt.lastLedgerSequence
}

// transformContractInvocations transforms contract invocations for the ledger range
func (rt *RealtimeTransformer) transformContractInvocations(ctx context.Context, tx *sql.Tx, startLedger, endLedger int64) (int64, error) {
	rows, err := rt.bronzeReader.QueryContractInvocations(ctx, startLedger, endLedger)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	count := int64(0)

	for rows.Next() {
		row := &ContractInvocationRow{}

		err := rows.Scan(
			&row.LedgerSequence,
			&row.TransactionIndex,
			&row.OperationIndex,
			&row.TransactionHash,
			&row.SourceAccount,
			&row.ContractID,
			&row.FunctionName,
			&row.ArgumentsJSON,
			&row.Successful,
			&row.ClosedAt,
			&row.LedgerRange,
		)

		if err != nil {
			return count, fmt.Errorf("failed to scan contract invocation row: %w", err)
		}

		if err := rt.silverWriter.WriteContractInvocation(ctx, tx, row); err != nil {
			return count, fmt.Errorf("failed to write contract invocation: %w", err)
		}

		count++
	}

	if err := rows.Err(); err != nil {
		return count, fmt.Errorf("error iterating contract invocations: %w", err)
	}

	return count, nil
}

// transformContractCalls transforms cross-contract call graphs for the ledger range
// Extracts call relationships from Bronze call graph data and writes to Silver tables
func (rt *RealtimeTransformer) transformContractCalls(ctx context.Context, tx *sql.Tx, startLedger, endLedger int64) (int64, error) {
	rows, err := rt.bronzeReader.QueryContractCallGraphs(ctx, startLedger, endLedger)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	count := int64(0)

	for rows.Next() {
		var (
			ledgerSequence   int64
			transactionIndex int
			operationIndex   int
			transactionHash  string
			sourceAccount    string
			contractID       sql.NullString
			functionName     sql.NullString
			argumentsJSON    sql.NullString
			contractCallsJSON sql.NullString
			contractsInvolved []string
			maxCallDepth     sql.NullInt32
			successful       bool
			closedAt         time.Time
			ledgerRange      int64
		)

		err := rows.Scan(
			&ledgerSequence,
			&transactionIndex,
			&operationIndex,
			&transactionHash,
			&sourceAccount,
			&contractID,
			&functionName,
			&argumentsJSON,
			&contractCallsJSON,
			pq.Array(&contractsInvolved),
			&maxCallDepth,
			&successful,
			&closedAt,
			&ledgerRange,
		)

		if err != nil {
			return count, fmt.Errorf("failed to scan contract call graph row: %w", err)
		}

		// Parse call graph JSON and write individual calls
		if contractCallsJSON.Valid && contractCallsJSON.String != "" {
			callRows, hierarchyRows, err := parseContractCallGraph(
				contractCallsJSON.String,
				transactionHash,
				ledgerSequence,
				transactionIndex,
				operationIndex,
				closedAt,
				ledgerRange,
				contractsInvolved,
			)
			if err != nil {
				log.Printf("Warning: Failed to parse call graph for tx %s: %v", transactionHash, err)
				continue
			}

			// Write call rows
			for _, callRow := range callRows {
				if err := rt.silverWriter.WriteContractCall(ctx, tx, callRow); err != nil {
					return count, fmt.Errorf("failed to write contract call: %w", err)
				}
				count++
			}

			// Write hierarchy rows
			for _, hierarchyRow := range hierarchyRows {
				if err := rt.silverWriter.WriteContractHierarchy(ctx, tx, hierarchyRow); err != nil {
					return count, fmt.Errorf("failed to write contract hierarchy: %w", err)
				}
			}
		} else if contractID.Valid && contractID.String != "" {
			// Single contract invocation with no cross-contract calls
			// Still create a root call row so it appears in call-related queries
			funcName := ""
			if functionName.Valid {
				funcName = functionName.String
			}

			rootCall := &ContractCallRow{
				LedgerSequence:   ledgerSequence,
				TransactionIndex: transactionIndex,
				OperationIndex:   operationIndex,
				TransactionHash:  transactionHash,
				FromContract:     sourceAccount, // External caller (the account)
				ToContract:       contractID.String,
				FunctionName:     funcName,
				CallDepth:        0,
				ExecutionOrder:   0,
				Successful:       successful,
				ClosedAt:         closedAt,
				LedgerRange:      ledgerRange,
			}

			if err := rt.silverWriter.WriteContractCall(ctx, tx, rootCall); err != nil {
				return count, fmt.Errorf("failed to write root contract call: %w", err)
			}
			count++

			// Also write a simple hierarchy entry for the root contract
			rootHierarchy := &ContractHierarchyRow{
				TransactionHash: transactionHash,
				RootContract:    contractID.String,
				ChildContract:   contractID.String, // Self-reference for root
				PathDepth:       0,
				FullPath:        []string{contractID.String},
				LedgerRange:     ledgerRange,
			}

			if err := rt.silverWriter.WriteContractHierarchy(ctx, tx, rootHierarchy); err != nil {
				return count, fmt.Errorf("failed to write root contract hierarchy: %w", err)
			}
		}
	}

	if err := rows.Err(); err != nil {
		return count, fmt.Errorf("error iterating contract call graphs: %w", err)
	}

	return count, nil
}

// parseContractCallGraph parses call graph JSON and creates ContractCallRow and ContractHierarchyRow
func parseContractCallGraph(
	callsJSON string,
	transactionHash string,
	ledgerSequence int64,
	transactionIndex int,
	operationIndex int,
	closedAt time.Time,
	ledgerRange int64,
	contractsInvolved []string,
) ([]*ContractCallRow, []*ContractHierarchyRow, error) {
	// Parse JSON array of calls
	var calls []struct {
		FromContract   string `json:"from_contract"`
		ToContract     string `json:"to_contract"`
		FunctionName   string `json:"function"`
		CallDepth      int    `json:"call_depth"`
		ExecutionOrder int    `json:"execution_order"`
		Successful     bool   `json:"successful"`
	}

	if err := json.Unmarshal([]byte(callsJSON), &calls); err != nil {
		return nil, nil, fmt.Errorf("failed to unmarshal call graph JSON: %w", err)
	}

	var callRows []*ContractCallRow
	var hierarchyRows []*ContractHierarchyRow

	// Create call rows
	for _, call := range calls {
		callRows = append(callRows, &ContractCallRow{
			LedgerSequence:   ledgerSequence,
			TransactionIndex: transactionIndex,
			OperationIndex:   operationIndex,
			TransactionHash:  transactionHash,
			FromContract:     call.FromContract,
			ToContract:       call.ToContract,
			FunctionName:     call.FunctionName,
			CallDepth:        call.CallDepth,
			ExecutionOrder:   call.ExecutionOrder,
			Successful:       call.Successful,
			ClosedAt:         closedAt,
			LedgerRange:      ledgerRange,
		})
	}

	// Create hierarchy rows (all unique contract pairs with their paths)
	// For each contract involved, create hierarchy entries showing relationships
	if len(contractsInvolved) > 1 {
		rootContract := contractsInvolved[0] // First contract is the root

		for i, childContract := range contractsInvolved[1:] {
			// Build path from root to this child
			path := contractsInvolved[:i+2] // Include all contracts up to this child

			hierarchyRows = append(hierarchyRows, &ContractHierarchyRow{
				TransactionHash: transactionHash,
				RootContract:    rootContract,
				ChildContract:   childContract,
				PathDepth:       i + 1,
				FullPath:        path,
				LedgerRange:     ledgerRange,
			})
		}
	}

	return callRows, hierarchyRows, nil
}
