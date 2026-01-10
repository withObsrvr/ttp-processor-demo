package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"strings"
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

	// Transform trustlines current (UPSERT pattern)
	trustlinesCurrentCount, err := rt.transformTrustlinesCurrent(ctx, tx, startLedger, endLedger)
	if err != nil {
		return fmt.Errorf("failed to transform trustlines current: %w", err)
	}
	totalRows += trustlinesCurrentCount

	// Transform offers current (UPSERT pattern)
	offersCurrentCount, err := rt.transformOffersCurrent(ctx, tx, startLedger, endLedger)
	if err != nil {
		return fmt.Errorf("failed to transform offers current: %w", err)
	}
	totalRows += offersCurrentCount

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

	// Transform liquidity pools current (Phase 1 - Core State Tables)
	liquidityPoolsCount, err := rt.transformLiquidityPoolsCurrent(ctx, tx, startLedger, endLedger)
	if err != nil {
		return fmt.Errorf("failed to transform liquidity pools current: %w", err)
	}
	totalRows += liquidityPoolsCount

	// Transform claimable balances current (Phase 1 - Core State Tables)
	claimableBalancesCount, err := rt.transformClaimableBalancesCurrent(ctx, tx, startLedger, endLedger)
	if err != nil {
		return fmt.Errorf("failed to transform claimable balances current: %w", err)
	}
	totalRows += claimableBalancesCount

	// Transform native balances current (Phase 1 - Core State Tables)
	nativeBalancesCount, err := rt.transformNativeBalancesCurrent(ctx, tx, startLedger, endLedger)
	if err != nil {
		return fmt.Errorf("failed to transform native balances current: %w", err)
	}
	totalRows += nativeBalancesCount

	// Transform trades (Phase 2 - Event Tables)
	tradesCount, err := rt.transformTrades(ctx, tx, startLedger, endLedger)
	if err != nil {
		return fmt.Errorf("failed to transform trades: %w", err)
	}
	totalRows += tradesCount

	// Transform effects (Phase 2 - Event Tables)
	effectsCount, err := rt.transformEffects(ctx, tx, startLedger, endLedger)
	if err != nil {
		return fmt.Errorf("failed to transform effects: %w", err)
	}
	totalRows += effectsCount

	// Transform contract data current (Phase 3 - Soroban Tables)
	contractDataCount, err := rt.transformContractDataCurrent(ctx, tx, startLedger, endLedger)
	if err != nil {
		return fmt.Errorf("failed to transform contract data current: %w", err)
	}
	totalRows += contractDataCount

	// Transform contract code current (Phase 3 - Soroban Tables)
	contractCodeCount, err := rt.transformContractCodeCurrent(ctx, tx, startLedger, endLedger)
	if err != nil {
		return fmt.Errorf("failed to transform contract code current: %w", err)
	}
	totalRows += contractCodeCount

	// Transform TTL current (Phase 3 - Soroban Tables)
	ttlCount, err := rt.transformTTLCurrent(ctx, tx, startLedger, endLedger)
	if err != nil {
		return fmt.Errorf("failed to transform TTL current: %w", err)
	}
	totalRows += ttlCount

	// Transform evicted keys (Phase 3 - Soroban Tables)
	evictedKeysCount, err := rt.transformEvictedKeys(ctx, tx, startLedger, endLedger)
	if err != nil {
		return fmt.Errorf("failed to transform evicted keys: %w", err)
	}
	totalRows += evictedKeysCount

	// Transform restored keys (Phase 3 - Soroban Tables)
	restoredKeysCount, err := rt.transformRestoredKeys(ctx, tx, startLedger, endLedger)
	if err != nil {
		return fmt.Errorf("failed to transform restored keys: %w", err)
	}
	totalRows += restoredKeysCount

	// Transform config settings current (Phase 4 - Config Settings)
	configSettingsCount, err := rt.transformConfigSettingsCurrent(ctx, tx, startLedger, endLedger)
	if err != nil {
		return fmt.Errorf("failed to transform config settings current: %w", err)
	}
	totalRows += configSettingsCount

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

// transformTrustlinesCurrent upserts trustlines current state for the ledger range
func (rt *RealtimeTransformer) transformTrustlinesCurrent(ctx context.Context, tx *sql.Tx, startLedger, endLedger int64) (int64, error) {
	rows, err := rt.bronzeReader.QueryTrustlinesSnapshot(ctx, startLedger, endLedger)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	count := int64(0)

	for rows.Next() {
		row := &TrustlineCurrentRow{}

		err := rows.Scan(
			&row.AccountID, &row.AssetType, &row.AssetIssuer, &row.AssetCode,
			&row.Balance, &row.TrustLineLimit, &row.BuyingLiabilities, &row.SellingLiabilities,
			&row.Authorized, &row.AuthorizedToMaintainLiabilities, &row.ClawbackEnabled,
			&row.LedgerSequence, &row.CreatedAt, &row.LedgerRange,
		)

		if err != nil {
			return count, fmt.Errorf("failed to scan trustline row: %w", err)
		}

		// Set last_modified_ledger to the ledger_sequence
		row.LastModifiedLedger = row.LedgerSequence

		// Compute flags from boolean fields (Stellar flag encoding)
		// 1 = authorized, 2 = authorized_to_maintain_liabilities, 4 = clawback_enabled
		row.Flags = 0
		if row.Authorized {
			row.Flags |= 1
		}
		if row.AuthorizedToMaintainLiabilities {
			row.Flags |= 2
		}
		if row.ClawbackEnabled {
			row.Flags |= 4
		}

		// LiquidityPoolID and Sponsor are NULL for classic trustlines from bronze
		// They remain as sql.NullString with Valid=false

		if err := rt.silverWriter.WriteTrustlineCurrent(ctx, tx, row); err != nil {
			return count, fmt.Errorf("failed to write trustline current: %w", err)
		}

		count++
	}

	if err := rows.Err(); err != nil {
		return count, fmt.Errorf("error iterating trustlines: %w", err)
	}

	return count, nil
}

// transformOffersCurrent upserts offers current state for the ledger range
func (rt *RealtimeTransformer) transformOffersCurrent(ctx context.Context, tx *sql.Tx, startLedger, endLedger int64) (int64, error) {
	rows, err := rt.bronzeReader.QueryOffersSnapshot(ctx, startLedger, endLedger)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	count := int64(0)

	for rows.Next() {
		row := &OfferCurrentRow{}

		err := rows.Scan(
			&row.OfferID, &row.SellerID, &row.SellingAssetType, &row.SellingAssetCode, &row.SellingAssetIssuer,
			&row.BuyingAssetType, &row.BuyingAssetCode, &row.BuyingAssetIssuer,
			&row.Amount, &row.Price, &row.Flags,
			&row.LedgerSequence, &row.CreatedAt, &row.LedgerRange,
		)

		if err != nil {
			return count, fmt.Errorf("failed to scan offer row: %w", err)
		}

		// Set last_modified_ledger to the ledger_sequence
		row.LastModifiedLedger = row.LedgerSequence

		// Parse price string to price_n/price_d and computed decimal
		// Bronze stores price as fractional string like "10/1" or "1/2"
		row.PriceN = 0
		row.PriceD = 1
		if parts := strings.Split(row.Price, "/"); len(parts) == 2 {
			if n, err := strconv.Atoi(parts[0]); err == nil {
				row.PriceN = n
			}
			if d, err := strconv.Atoi(parts[1]); err == nil && d > 0 {
				row.PriceD = d
			}
		}
		// Compute decimal price from fraction
		if row.PriceD > 0 {
			row.Price = fmt.Sprintf("%.7f", float64(row.PriceN)/float64(row.PriceD))
		} else {
			row.Price = "0"
		}

		// Sponsor is NULL from bronze
		// It remains as sql.NullString with Valid=false

		if err := rt.silverWriter.WriteOfferCurrent(ctx, tx, row); err != nil {
			return count, fmt.Errorf("failed to write offer current: %w", err)
		}

		count++
	}

	if err := rows.Err(); err != nil {
		return count, fmt.Errorf("error iterating offers: %w", err)
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

// transformLiquidityPoolsCurrent upserts liquidity pools current state for the ledger range
func (rt *RealtimeTransformer) transformLiquidityPoolsCurrent(ctx context.Context, tx *sql.Tx, startLedger, endLedger int64) (int64, error) {
	rows, err := rt.bronzeReader.QueryLiquidityPoolsSnapshot(ctx, startLedger, endLedger)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	count := int64(0)

	for rows.Next() {
		row := &LiquidityPoolCurrentRow{}

		err := rows.Scan(
			&row.LiquidityPoolID, &row.PoolType, &row.Fee, &row.TrustlineCount, &row.TotalPoolShares,
			&row.AssetAType, &row.AssetACode, &row.AssetAIssuer, &row.AssetAAmount,
			&row.AssetBType, &row.AssetBCode, &row.AssetBIssuer, &row.AssetBAmount,
			&row.LedgerSequence, &row.ClosedAt, &row.CreatedAt, &row.LedgerRange,
		)

		if err != nil {
			return count, fmt.Errorf("failed to scan liquidity pool row: %w", err)
		}

		// Set last_modified_ledger to the ledger_sequence
		row.LastModifiedLedger = row.LedgerSequence

		if err := rt.silverWriter.WriteLiquidityPoolCurrent(ctx, tx, row); err != nil {
			return count, fmt.Errorf("failed to write liquidity pool current: %w", err)
		}

		count++
	}

	if err := rows.Err(); err != nil {
		return count, fmt.Errorf("error iterating liquidity pools: %w", err)
	}

	return count, nil
}

// transformClaimableBalancesCurrent upserts claimable balances current state for the ledger range
func (rt *RealtimeTransformer) transformClaimableBalancesCurrent(ctx context.Context, tx *sql.Tx, startLedger, endLedger int64) (int64, error) {
	rows, err := rt.bronzeReader.QueryClaimableBalancesSnapshot(ctx, startLedger, endLedger)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	count := int64(0)

	for rows.Next() {
		row := &ClaimableBalanceCurrentRow{}

		err := rows.Scan(
			&row.BalanceID, &row.Sponsor, &row.AssetType, &row.AssetCode, &row.AssetIssuer,
			&row.Amount, &row.ClaimantsCount, &row.Flags,
			&row.LedgerSequence, &row.ClosedAt, &row.CreatedAt, &row.LedgerRange,
		)

		if err != nil {
			return count, fmt.Errorf("failed to scan claimable balance row: %w", err)
		}

		// Set last_modified_ledger to the ledger_sequence
		row.LastModifiedLedger = row.LedgerSequence

		if err := rt.silverWriter.WriteClaimableBalanceCurrent(ctx, tx, row); err != nil {
			return count, fmt.Errorf("failed to write claimable balance current: %w", err)
		}

		count++
	}

	if err := rows.Err(); err != nil {
		return count, fmt.Errorf("error iterating claimable balances: %w", err)
	}

	return count, nil
}

// transformNativeBalancesCurrent upserts native balances current state for the ledger range
func (rt *RealtimeTransformer) transformNativeBalancesCurrent(ctx context.Context, tx *sql.Tx, startLedger, endLedger int64) (int64, error) {
	rows, err := rt.bronzeReader.QueryNativeBalancesSnapshot(ctx, startLedger, endLedger)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	count := int64(0)

	for rows.Next() {
		row := &NativeBalanceCurrentRow{}

		err := rows.Scan(
			&row.AccountID, &row.Balance, &row.BuyingLiabilities, &row.SellingLiabilities,
			&row.NumSubentries, &row.NumSponsoring, &row.NumSponsored, &row.SequenceNumber,
			&row.LastModifiedLedger, &row.LedgerSequence, &row.LedgerRange,
		)

		if err != nil {
			return count, fmt.Errorf("failed to scan native balance row: %w", err)
		}

		if err := rt.silverWriter.WriteNativeBalanceCurrent(ctx, tx, row); err != nil {
			return count, fmt.Errorf("failed to write native balance current: %w", err)
		}

		count++
	}

	if err := rows.Err(); err != nil {
		return count, fmt.Errorf("error iterating native balances: %w", err)
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

// transformTrades inserts trade events for the ledger range (append-only event stream)
func (rt *RealtimeTransformer) transformTrades(ctx context.Context, tx *sql.Tx, startLedger, endLedger int64) (int64, error) {
	rows, err := rt.bronzeReader.QueryTrades(ctx, startLedger, endLedger)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	count := int64(0)

	for rows.Next() {
		row := &TradeRow{}

		err := rows.Scan(
			&row.LedgerSequence, &row.TransactionHash, &row.OperationIndex, &row.TradeIndex,
			&row.TradeType, &row.TradeTimestamp, &row.SellerAccount,
			&row.SellingAssetCode, &row.SellingAssetIssuer, &row.SellingAmount,
			&row.BuyerAccount, &row.BuyingAssetCode, &row.BuyingAssetIssuer, &row.BuyingAmount,
			&row.Price, &row.CreatedAt, &row.LedgerRange,
		)

		if err != nil {
			return count, fmt.Errorf("failed to scan trade row: %w", err)
		}

		// Parse price string (fractional format like "537735/2882858") to decimal
		if parts := strings.Split(row.Price, "/"); len(parts) == 2 {
			n, err1 := strconv.ParseFloat(parts[0], 64)
			d, err2 := strconv.ParseFloat(parts[1], 64)
			if err1 == nil && err2 == nil && d > 0 {
				row.Price = fmt.Sprintf("%.7f", n/d)
			} else {
				row.Price = "0"
			}
		}
		// If price is not a fraction, assume it's already a decimal and leave as-is

		if err := rt.silverWriter.WriteTrade(ctx, tx, row); err != nil {
			return count, fmt.Errorf("failed to write trade: %w", err)
		}

		count++
	}

	if err := rows.Err(); err != nil {
		return count, fmt.Errorf("error iterating trades: %w", err)
	}

	return count, nil
}

// transformEffects inserts effect events for the ledger range (append-only event stream)
func (rt *RealtimeTransformer) transformEffects(ctx context.Context, tx *sql.Tx, startLedger, endLedger int64) (int64, error) {
	rows, err := rt.bronzeReader.QueryEffects(ctx, startLedger, endLedger)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	count := int64(0)

	for rows.Next() {
		row := &EffectRow{}

		err := rows.Scan(
			&row.LedgerSequence, &row.TransactionHash, &row.OperationIndex, &row.EffectIndex,
			&row.EffectType, &row.EffectTypeString, &row.AccountID,
			&row.Amount, &row.AssetCode, &row.AssetIssuer, &row.AssetType,
			&row.TrustlineLimit, &row.AuthorizeFlag, &row.ClawbackFlag,
			&row.SignerAccount, &row.SignerWeight, &row.OfferID, &row.SellerAccount,
			&row.CreatedAt, &row.LedgerRange,
		)

		if err != nil {
			return count, fmt.Errorf("failed to scan effect row: %w", err)
		}

		if err := rt.silverWriter.WriteEffect(ctx, tx, row); err != nil {
			return count, fmt.Errorf("failed to write effect: %w", err)
		}

		count++
	}

	if err := rows.Err(); err != nil {
		return count, fmt.Errorf("error iterating effects: %w", err)
	}

	return count, nil
}

// =============================================================================
// Phase 3: Soroban Tables
// =============================================================================

// transformContractDataCurrent upserts contract data current state for the ledger range
func (rt *RealtimeTransformer) transformContractDataCurrent(ctx context.Context, tx *sql.Tx, startLedger, endLedger int64) (int64, error) {
	rows, err := rt.bronzeReader.QueryContractDataSnapshot(ctx, startLedger, endLedger)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	count := int64(0)

	for rows.Next() {
		row := &ContractDataCurrentRow{}

		err := rows.Scan(
			&row.ContractID, &row.KeyHash, &row.Durability,
			&row.AssetType, &row.AssetCode, &row.AssetIssuer,
			&row.DataValue, &row.LastModifiedLedger, &row.LedgerSequence,
			&row.ClosedAt, &row.CreatedAt, &row.LedgerRange,
		)

		if err != nil {
			return count, fmt.Errorf("failed to scan contract data row: %w", err)
		}

		if err := rt.silverWriter.WriteContractDataCurrent(ctx, tx, row); err != nil {
			return count, fmt.Errorf("failed to write contract data current: %w", err)
		}

		count++
	}

	if err := rows.Err(); err != nil {
		return count, fmt.Errorf("error iterating contract data: %w", err)
	}

	return count, nil
}

// transformContractCodeCurrent upserts contract code current state for the ledger range
func (rt *RealtimeTransformer) transformContractCodeCurrent(ctx context.Context, tx *sql.Tx, startLedger, endLedger int64) (int64, error) {
	rows, err := rt.bronzeReader.QueryContractCodeSnapshot(ctx, startLedger, endLedger)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	count := int64(0)

	for rows.Next() {
		row := &ContractCodeCurrentRow{}

		err := rows.Scan(
			&row.ContractCodeHash, &row.ContractCodeExtV,
			&row.NDataSegmentBytes, &row.NDataSegments, &row.NElemSegments, &row.NExports,
			&row.NFunctions, &row.NGlobals, &row.NImports, &row.NInstructions, &row.NTableEntries, &row.NTypes,
			&row.LastModifiedLedger, &row.LedgerSequence, &row.ClosedAt, &row.CreatedAt, &row.LedgerRange,
		)

		if err != nil {
			return count, fmt.Errorf("failed to scan contract code row: %w", err)
		}

		if err := rt.silverWriter.WriteContractCodeCurrent(ctx, tx, row); err != nil {
			return count, fmt.Errorf("failed to write contract code current: %w", err)
		}

		count++
	}

	if err := rows.Err(); err != nil {
		return count, fmt.Errorf("error iterating contract code: %w", err)
	}

	return count, nil
}

// transformTTLCurrent upserts TTL current state for the ledger range
func (rt *RealtimeTransformer) transformTTLCurrent(ctx context.Context, tx *sql.Tx, startLedger, endLedger int64) (int64, error) {
	rows, err := rt.bronzeReader.QueryTTLSnapshot(ctx, startLedger, endLedger)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	count := int64(0)

	for rows.Next() {
		row := &TTLCurrentRow{}

		err := rows.Scan(
			&row.KeyHash, &row.LiveUntilLedgerSeq, &row.TTLRemaining, &row.Expired,
			&row.LastModifiedLedger, &row.LedgerSequence, &row.ClosedAt, &row.CreatedAt, &row.LedgerRange,
		)

		if err != nil {
			return count, fmt.Errorf("failed to scan TTL row: %w", err)
		}

		if err := rt.silverWriter.WriteTTLCurrent(ctx, tx, row); err != nil {
			return count, fmt.Errorf("failed to write TTL current: %w", err)
		}

		count++
	}

	if err := rows.Err(); err != nil {
		return count, fmt.Errorf("error iterating TTL entries: %w", err)
	}

	return count, nil
}

// transformEvictedKeys inserts evicted key events for the ledger range (append-only event stream)
func (rt *RealtimeTransformer) transformEvictedKeys(ctx context.Context, tx *sql.Tx, startLedger, endLedger int64) (int64, error) {
	rows, err := rt.bronzeReader.QueryEvictedKeys(ctx, startLedger, endLedger)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	count := int64(0)

	for rows.Next() {
		row := &EvictedKeyRow{}

		err := rows.Scan(
			&row.ContractID, &row.KeyHash, &row.LedgerSequence,
			&row.ClosedAt, &row.CreatedAt, &row.LedgerRange,
		)

		if err != nil {
			return count, fmt.Errorf("failed to scan evicted key row: %w", err)
		}

		if err := rt.silverWriter.WriteEvictedKey(ctx, tx, row); err != nil {
			return count, fmt.Errorf("failed to write evicted key: %w", err)
		}

		count++
	}

	if err := rows.Err(); err != nil {
		return count, fmt.Errorf("error iterating evicted keys: %w", err)
	}

	return count, nil
}

// transformRestoredKeys inserts restored key events for the ledger range (append-only event stream)
func (rt *RealtimeTransformer) transformRestoredKeys(ctx context.Context, tx *sql.Tx, startLedger, endLedger int64) (int64, error) {
	rows, err := rt.bronzeReader.QueryRestoredKeys(ctx, startLedger, endLedger)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	count := int64(0)

	for rows.Next() {
		row := &RestoredKeyRow{}

		err := rows.Scan(
			&row.ContractID, &row.KeyHash, &row.LedgerSequence,
			&row.ClosedAt, &row.CreatedAt, &row.LedgerRange,
		)

		if err != nil {
			return count, fmt.Errorf("failed to scan restored key row: %w", err)
		}

		if err := rt.silverWriter.WriteRestoredKey(ctx, tx, row); err != nil {
			return count, fmt.Errorf("failed to write restored key: %w", err)
		}

		count++
	}

	if err := rows.Err(); err != nil {
		return count, fmt.Errorf("error iterating restored keys: %w", err)
	}

	return count, nil
}

// =============================================================================
// Phase 4: Config Settings
// =============================================================================

// transformConfigSettingsCurrent upserts config settings current state for the ledger range
func (rt *RealtimeTransformer) transformConfigSettingsCurrent(ctx context.Context, tx *sql.Tx, startLedger, endLedger int64) (int64, error) {
	rows, err := rt.bronzeReader.QueryConfigSettingsSnapshot(ctx, startLedger, endLedger)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	count := int64(0)

	for rows.Next() {
		row := &ConfigSettingsCurrentRow{}

		err := rows.Scan(
			&row.ConfigSettingID,
			&row.LedgerMaxInstructions, &row.TxMaxInstructions,
			&row.FeeRatePerInstructionsIncrement, &row.TxMemoryLimit,
			&row.LedgerMaxReadLedgerEntries, &row.LedgerMaxReadBytes,
			&row.LedgerMaxWriteLedgerEntries, &row.LedgerMaxWriteBytes,
			&row.TxMaxReadLedgerEntries, &row.TxMaxReadBytes,
			&row.TxMaxWriteLedgerEntries, &row.TxMaxWriteBytes,
			&row.ContractMaxSizeBytes, &row.ConfigSettingXDR,
			&row.LastModifiedLedger, &row.LedgerSequence, &row.ClosedAt, &row.CreatedAt, &row.LedgerRange,
		)

		if err != nil {
			return count, fmt.Errorf("failed to scan config settings row: %w", err)
		}

		if err := rt.silverWriter.WriteConfigSettingsCurrent(ctx, tx, row); err != nil {
			return count, fmt.Errorf("failed to write config settings current: %w", err)
		}

		count++
	}

	if err := rows.Err(); err != nil {
		return count, fmt.Errorf("error iterating config settings: %w", err)
	}

	return count, nil
}
