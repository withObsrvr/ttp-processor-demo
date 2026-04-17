package main

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stellar/go-stellar-sdk/xdr"
	extract "github.com/withObsrvr/stellar-extract"
	pb "github.com/withObsrvr/ttp-processor-demo/stellar-live-source-datalake/go/gen/raw_ledger_service"
)

// Writer handles writing ledger data to PostgreSQL
type Writer struct {
	db           *pgxpool.Pool
	config       *Config
	checkpoint   *Checkpoint
	healthServer *HealthServer
	broadcaster  *BronzeSourceServer
}

// SetBroadcaster sets the gRPC source server for broadcasting events after commit
func (w *Writer) SetBroadcaster(b *BronzeSourceServer) {
	w.broadcaster = b
}

// LedgerData represents extracted ledger information
type LedgerData struct {
	Sequence             uint32
	LedgerHash           string
	PreviousLedgerHash   string
	ClosedAt             time.Time
	ProtocolVersion      uint32
	TotalCoins           int64
	FeePool              int64
	BaseFee              uint32
	BaseReserve          uint32
	MaxTxSetSize         uint32
	TransactionCount     int
	OperationCount       int
	SuccessfulTxCount    int
	FailedTxCount        int
	TxSetOperationCount  int
	SorobanFeeWrite1KB   *int64
	NodeID               *string
	Signature            *string
	LedgerHeader         *string
	BucketListSize       *int64
	LiveSorobanStateSize *int64
	EvictedKeysCount     *int
	IngestionTimestamp   time.Time
	LedgerRange          uint32
	// Soroban aggregates per ledger
	SorobanOpCount      *int
	TotalFeeCharged     *int64
	ContractEventsCount *int
}

// NewWriter creates a new PostgreSQL writer
func NewWriter(db *pgxpool.Pool, config *Config, checkpoint *Checkpoint, healthServer *HealthServer) *Writer {
	return &Writer{
		db:           db,
		config:       config,
		checkpoint:   checkpoint,
		healthServer: healthServer,
	}
}

// bulkUpsertCopy performs a high-throughput upsert by COPYing rows into a
// per-call TEMP table shaped like target (LIKE ... INCLUDING DEFAULTS,
// ON COMMIT DROP) and then running a single INSERT … SELECT DISTINCT ON (…)
// FROM <tmp> ON CONFLICT … upsert from it.
//
// Compared with row-by-row tx.Exec this replaces N network round trips per
// batch (one per row) with exactly 3, and lets the target's indexes be
// maintained once in bulk rather than per row. The DISTINCT ON collapses
// any intra-batch duplicates on the conflict key, which the old per-row
// path handled implicitly via repeated ON CONFLICT DO UPDATEs.
//
// Parameters:
//   - target:       target table (e.g. "accounts_snapshot_v1")
//   - columns:      columns to COPY, in the same order as each row's values
//   - conflictCols: unique/PK columns used for both DISTINCT ON and ON CONFLICT
//   - updateCols:   columns set via EXCLUDED on conflict; empty ⇒ DO NOTHING
//   - rows:         [][]any, outer length = row count, inner matches `columns`
func (w *Writer) bulkUpsertCopy(
	ctx context.Context,
	tx pgx.Tx,
	target string,
	columns []string,
	conflictCols []string,
	updateCols []string,
	rows [][]interface{},
) error {
	if len(rows) == 0 {
		return nil
	}
	if len(conflictCols) == 0 {
		return fmt.Errorf("bulkUpsertCopy(%s): conflictCols required", target)
	}

	tmp := "_tmp_" + target

	// Per-call temp table shaped like the target. ON COMMIT DROP scopes the
	// lifetime to the enclosing transaction so repeated calls across batches
	// don't accumulate state. LIKE … INCLUDING DEFAULTS copies the column
	// definitions only — no indexes, constraints, or check expressions —
	// which keeps COPY O(rows) with no per-row index cost.
	_, err := tx.Exec(ctx, fmt.Sprintf(
		`CREATE TEMP TABLE %s (LIKE %s INCLUDING DEFAULTS) ON COMMIT DROP`,
		tmp, target,
	))
	if err != nil {
		return fmt.Errorf("create temp table %s: %w", tmp, err)
	}

	copied, err := tx.CopyFrom(ctx, pgx.Identifier{tmp}, columns, pgx.CopyFromRows(rows))
	if err != nil {
		return fmt.Errorf("COPY %d rows into %s: %w", len(rows), tmp, err)
	}
	if int(copied) != len(rows) {
		log.Printf("WARNING: COPY into %s expected %d rows, got %d", tmp, len(rows), copied)
	}

	colList := strings.Join(columns, ", ")
	conflictList := strings.Join(conflictCols, ", ")

	var onConflict string
	if len(updateCols) == 0 {
		onConflict = "DO NOTHING"
	} else {
		parts := make([]string, len(updateCols))
		for i, c := range updateCols {
			parts[i] = fmt.Sprintf("%s = EXCLUDED.%s", c, c)
		}
		onConflict = "DO UPDATE SET " + strings.Join(parts, ", ")
	}

	// DISTINCT ON (<conflictCols>) deduplicates within this batch. Without
	// it, Postgres raises "ON CONFLICT DO UPDATE command cannot affect row
	// a second time" when the same conflict key appears twice in the COPY —
	// which happens in practice for e.g. the same contract_data key modified
	// multiple times within one ledger range.
	_, err = tx.Exec(ctx, fmt.Sprintf(
		`INSERT INTO %s (%s)
		 SELECT DISTINCT ON (%s) %s FROM %s
		 ON CONFLICT (%s) %s`,
		target, colList, conflictList, colList, tmp, conflictList, onConflict,
	))
	if err != nil {
		return fmt.Errorf("upsert from %s into %s: %w", tmp, target, err)
	}
	return nil
}

// bulkInsertCopy COPYs rows directly into `target` with no conflict
// handling. Use this for append-only tables that carry no unique or
// exclusion constraints (e.g. token_transfers_stream_v1) — skipping the
// temp-table + INSERT SELECT round trip is the fastest path available.
func (w *Writer) bulkInsertCopy(
	ctx context.Context,
	tx pgx.Tx,
	target string,
	columns []string,
	rows [][]interface{},
) error {
	if len(rows) == 0 {
		return nil
	}
	copied, err := tx.CopyFrom(ctx, pgx.Identifier{target}, columns, pgx.CopyFromRows(rows))
	if err != nil {
		return fmt.Errorf("COPY %d rows into %s: %w", len(rows), target, err)
	}
	if int(copied) != len(rows) {
		log.Printf("WARNING: COPY into %s expected %d rows, got %d", target, len(rows), copied)
	}
	return nil
}

// batchExec sends N queries as a single pipelined pgx.Batch, replacing
// N synchronous tx.Exec round trips with one. Use this for low-row-count
// inserters where the TEMP-table overhead of bulkUpsertCopy isn't worth it
// (typically <100 rows/batch). The caller is responsible for baking the
// full INSERT … ON CONFLICT … SQL into `query`; each `rows[i]` is the
// parameter slice for one queued invocation.
func batchExec(ctx context.Context, tx pgx.Tx, query string, rows [][]interface{}) error {
	if len(rows) == 0 {
		return nil
	}
	b := &pgx.Batch{}
	for _, r := range rows {
		b.Queue(query, r...)
	}
	br := tx.SendBatch(ctx, b)
	defer br.Close()
	for i := range rows {
		if _, err := br.Exec(); err != nil {
			return fmt.Errorf("batch exec row %d: %w", i, err)
		}
	}
	return nil
}

// WriteBatch writes a batch of ledgers to PostgreSQL
func (w *Writer) WriteBatch(ctx context.Context, rawLedgers []*pb.RawLedger) error {
	if len(rawLedgers) == 0 {
		return nil
	}

	startTime := time.Now()
	log.Printf("Writing batch of %d ledgers (sequences %d-%d)",
		len(rawLedgers),
		rawLedgers[0].Sequence,
		rawLedgers[len(rawLedgers)-1].Sequence)

	// Begin transaction
	tx, err := w.db.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	// Process each ledger
	var totalTxCount, totalOpCount uint64
	allLedgers := make([]LedgerData, 0, len(rawLedgers))
	var allTransactions []TransactionData
	var allOperations []OperationData
	var allEffects []EffectData
	var allTrades []TradeData

	// Phase 1 accumulators (Day 1: accounts & offers)
	var allAccounts []AccountData
	var allOffers []OfferData

	// Phase 1 accumulators (Day 2-3: trustlines & account_signers)
	var allTrustlines []TrustlineData
	var allAccountSigners []AccountSignerData

	// Phase 2 accumulators (Day 4: claimable_balances & liquidity_pools)
	var allClaimableBalances []ClaimableBalanceData
	var allLiquidityPools []LiquidityPoolData

	// Phase 2 accumulators (Day 5: config_settings)
	var allConfigSettings []ConfigSettingData

	// Phase 2 accumulators (Day 6: ttl)
	var allTTL []TTLData

	// Phase 3 accumulators (Day 7: evicted_keys)
	var allEvictedKeys []EvictedKeyData

	// Phase 4 accumulators (Day 8: contract_events)
	var allContractEvents []ContractEventData

	// Phase 4 accumulators (Day 9: contract_data)
	var allContractData []ContractDataData

	// Phase 4 accumulators (Day 10: contract_code)
	var allContractCode []ContractCodeData

	// Phase 5 accumulators (Day 11: native_balances)
	var allNativeBalances []NativeBalanceData

	// Phase 5 accumulators (Day 11: restored_keys)
	var allRestoredKeys []RestoredKeyData

	// Token transfers (SDK-based unified extraction)
	var allTokenTransfers []TokenTransferData

	// Contract creation tracking (C11)
	var allContractCreations []ContractCreationData

	for _, rawLedger := range rawLedgers {
		// Create the library input ONCE per ledger (decodes XDR)
		input, err := extract.NewLedgerInputFromXDR(rawLedger.LedgerCloseMetaXdr, w.config.Source.NetworkPassphrase)
		if err != nil {
			return fmt.Errorf("failed to decode ledger %d: %w", rawLedger.Sequence, err)
		}

		// Extract ledger data (local method — uses pre-decoded LCM)
		ledgerData, err := w.extractLedgerDataFromLCM(input.LCM)
		if err != nil {
			return fmt.Errorf("failed to extract ledger %d: %w", rawLedger.Sequence, err)
		}

		// Extract transactions via library
		libTransactions, err := extract.ExtractTransactions(input)
		if err != nil {
			log.Printf("Warning: Failed to extract transactions for ledger %d: %v", rawLedger.Sequence, err)
		} else {
			for _, r := range libTransactions {
				allTransactions = append(allTransactions, convertTransaction(r))
			}
		}

		// Extract operations via library + soroban counting
		libOperations, err := extract.ExtractOperations(input)
		if err != nil {
			log.Printf("Warning: Failed to extract operations for ledger %d: %v", rawLedger.Sequence, err)
		} else {
			sorobanOps := 0
			convertedOps := make([]OperationData, 0, len(libOperations))
			for _, r := range libOperations {
				convertedOps = append(convertedOps, convertOperation(r))
				if r.LedgerSequence == ledgerData.Sequence {
					if r.OpType == 24 || r.OpType == 25 || r.OpType == 26 {
						sorobanOps++
					}
				}
			}
			// Enrich Soroban InvokeHostFunction ops with authorization-entry
			// credentials (type + authorizer address). The stellar-extract
			// library doesn't surface these, so we walk the LCM directly.
			if creds, cerr := buildAuthCredentialsMap(input); cerr != nil {
				log.Printf("Warning: Failed to build auth credentials map for ledger %d: %v", rawLedger.Sequence, cerr)
			} else {
				applyAuthCredentials(convertedOps, creds)
			}
			allOperations = append(allOperations, convertedOps...)
			ledgerData.SorobanOpCount = &sorobanOps
		}

		// Accumulate ledger (actual insert runs as a single bulk COPY after
		// the loop — see insertLedgers below). Must sit after the operations
		// extraction above so SorobanOpCount is populated before the copy.
		allLedgers = append(allLedgers, *ledgerData)

		// Extract effects via library
		libEffects, err := extract.ExtractEffects(input)
		if err != nil {
			log.Printf("Warning: Failed to extract effects for ledger %d: %v", rawLedger.Sequence, err)
		} else {
			for _, r := range libEffects {
				allEffects = append(allEffects, convertEffect(r))
			}
		}

		// Extract trades via library
		libTrades, err := extract.ExtractTrades(input)
		if err != nil {
			log.Printf("Warning: Failed to extract trades for ledger %d: %v", rawLedger.Sequence, err)
		} else {
			for _, r := range libTrades {
				allTrades = append(allTrades, convertTrade(r))
			}
		}

		// Extract accounts via library
		libAccounts, err := extract.ExtractAccounts(input)
		if err != nil {
			log.Printf("Warning: Failed to extract accounts for ledger %d: %v", rawLedger.Sequence, err)
		} else {
			for _, r := range libAccounts {
				allAccounts = append(allAccounts, convertAccount(r))
			}
		}

		// Extract offers via library
		libOffers, err := extract.ExtractOffers(input)
		if err != nil {
			log.Printf("Warning: Failed to extract offers for ledger %d: %v", rawLedger.Sequence, err)
		} else {
			for _, r := range libOffers {
				allOffers = append(allOffers, convertOffer(r))
			}
		}

		// Extract trustlines via library
		libTrustlines, err := extract.ExtractTrustlines(input)
		if err != nil {
			log.Printf("Warning: Failed to extract trustlines for ledger %d: %v", rawLedger.Sequence, err)
		} else {
			for _, r := range libTrustlines {
				allTrustlines = append(allTrustlines, convertTrustline(r))
			}
		}

		// Extract account signers via library
		libAccountSigners, err := extract.ExtractAccountSigners(input)
		if err != nil {
			log.Printf("Warning: Failed to extract account signers for ledger %d: %v", rawLedger.Sequence, err)
		} else {
			for _, r := range libAccountSigners {
				allAccountSigners = append(allAccountSigners, convertAccountSigner(r))
			}
		}

		// Extract claimable balances via library
		libClaimableBalances, err := extract.ExtractClaimableBalances(input)
		if err != nil {
			log.Printf("Warning: Failed to extract claimable balances for ledger %d: %v", rawLedger.Sequence, err)
		} else {
			for _, r := range libClaimableBalances {
				allClaimableBalances = append(allClaimableBalances, convertClaimableBalance(r))
			}
		}

		// Extract liquidity pools via library
		libLiquidityPools, err := extract.ExtractLiquidityPools(input)
		if err != nil {
			log.Printf("Warning: Failed to extract liquidity pools for ledger %d: %v", rawLedger.Sequence, err)
		} else {
			for _, r := range libLiquidityPools {
				allLiquidityPools = append(allLiquidityPools, convertLiquidityPool(r))
			}
		}

		// Extract config settings via library
		libConfigSettings, err := extract.ExtractConfigSettings(input)
		if err != nil {
			log.Printf("Warning: Failed to extract config settings for ledger %d: %v", rawLedger.Sequence, err)
		} else {
			for _, r := range libConfigSettings {
				allConfigSettings = append(allConfigSettings, convertConfigSetting(r))
			}
		}

		// Extract TTL via library
		libTTL, err := extract.ExtractTTL(input)
		if err != nil {
			log.Printf("Warning: Failed to extract TTL for ledger %d: %v", rawLedger.Sequence, err)
		} else {
			for _, r := range libTTL {
				allTTL = append(allTTL, convertTTL(r))
			}
		}

		// Extract evicted keys via library
		libEvictedKeys, err := extract.ExtractEvictedKeys(input)
		if err != nil {
			log.Printf("Warning: Failed to extract evicted keys for ledger %d: %v", rawLedger.Sequence, err)
		} else {
			for _, r := range libEvictedKeys {
				allEvictedKeys = append(allEvictedKeys, convertEvictedKey(r))
			}
		}

		// Extract contract events via library
		libContractEvents, err := extract.ExtractContractEvents(input)
		if err != nil {
			log.Printf("Warning: Failed to extract contract events for ledger %d: %v", rawLedger.Sequence, err)
		} else {
			for _, r := range libContractEvents {
				allContractEvents = append(allContractEvents, convertContractEvent(r))
			}
		}

		// Extract contract data via library
		libContractData, err := extract.ExtractContractData(input)
		if err != nil {
			log.Printf("Warning: Failed to extract contract data for ledger %d: %v", rawLedger.Sequence, err)
		} else {
			for _, r := range libContractData {
				allContractData = append(allContractData, convertContractData(r))
			}
		}

		// Extract contract code via library
		libContractCode, err := extract.ExtractContractCode(input)
		if err != nil {
			log.Printf("Warning: Failed to extract contract code for ledger %d: %v", rawLedger.Sequence, err)
		} else {
			for _, r := range libContractCode {
				allContractCode = append(allContractCode, convertContractCode(r))
			}
		}

		// Extract native balances via library
		libNativeBalances, err := extract.ExtractNativeBalances(input)
		if err != nil {
			log.Printf("Warning: Failed to extract native balances for ledger %d: %v", rawLedger.Sequence, err)
		} else {
			for _, r := range libNativeBalances {
				allNativeBalances = append(allNativeBalances, convertNativeBalance(r))
			}
		}

		// Extract restored keys via library
		libRestoredKeys, err := extract.ExtractRestoredKeys(input)
		if err != nil {
			log.Printf("Warning: Failed to extract restored keys for ledger %d: %v", rawLedger.Sequence, err)
		} else {
			for _, r := range libRestoredKeys {
				allRestoredKeys = append(allRestoredKeys, convertRestoredKey(r))
			}
		}

		// Extract contract creations via library
		libContractCreations, err := extract.ExtractContractCreations(input)
		if err != nil {
			log.Printf("Warning: Failed to extract contract creations for ledger %d: %v", rawLedger.Sequence, err)
		} else {
			for _, r := range libContractCreations {
				allContractCreations = append(allContractCreations, convertContractCreation(r))
			}
		}

		// Extract token transfers via library
		libTokenTransfers, err := extract.ExtractTokenTransfers(input)
		if err != nil {
			log.Printf("Warning: Failed to extract token transfers for ledger %d: %v", rawLedger.Sequence, err)
		} else {
			for _, r := range libTokenTransfers {
				allTokenTransfers = append(allTokenTransfers, convertTokenTransfer(r))
			}
		}

		// Update checkpoint
		if err := w.checkpoint.Update(
			ledgerData.Sequence,
			ledgerData.LedgerHash,
			ledgerData.LedgerRange,
			uint64(ledgerData.TransactionCount),
			uint64(ledgerData.OperationCount),
		); err != nil {
			return fmt.Errorf("failed to update checkpoint: %w", err)
		}

		totalTxCount += uint64(ledgerData.TransactionCount)
		totalOpCount += uint64(ledgerData.OperationCount)
	}

	// Insert all ledgers (one COPY for the whole batch)
	if len(allLedgers) > 0 {
		if err := w.insertLedgers(ctx, tx, allLedgers); err != nil {
			return fmt.Errorf("failed to insert ledgers: %w", err)
		}
		log.Printf("Inserted %d ledgers", len(allLedgers))
	}

	// Insert all transactions
	if len(allTransactions) > 0 {
		if err := w.insertTransactions(ctx, tx, allTransactions); err != nil {
			return fmt.Errorf("failed to insert transactions: %w", err)
		}
		log.Printf("Inserted %d transactions", len(allTransactions))
	}

	// Insert all operations
	if len(allOperations) > 0 {
		if err := w.insertOperations(ctx, tx, allOperations); err != nil {
			return fmt.Errorf("failed to insert operations: %w", err)
		}
		log.Printf("Inserted %d operations", len(allOperations))
	}

	// Insert all effects
	if len(allEffects) > 0 {
		if err := w.insertEffects(ctx, tx, allEffects); err != nil {
			return fmt.Errorf("failed to insert effects: %w", err)
		}
		log.Printf("Inserted %d effects", len(allEffects))
	}

	// Insert all trades
	if len(allTrades) > 0 {
		if err := w.insertTrades(ctx, tx, allTrades); err != nil {
			return fmt.Errorf("failed to insert trades: %w", err)
		}
		log.Printf("Inserted %d trades", len(allTrades))
	}

	// Insert all accounts (Phase 1 - Day 1)
	if len(allAccounts) > 0 {
		if err := w.insertAccounts(ctx, tx, allAccounts); err != nil {
			return fmt.Errorf("failed to insert accounts: %w", err)
		}
		log.Printf("Inserted %d accounts", len(allAccounts))
	}

	// Insert all offers (Phase 1 - Day 1)
	if len(allOffers) > 0 {
		if err := w.insertOffers(ctx, tx, allOffers); err != nil {
			return fmt.Errorf("failed to insert offers: %w", err)
		}
		log.Printf("Inserted %d offers", len(allOffers))
	}

	// Insert all trustlines (Phase 1 - Day 2)
	if len(allTrustlines) > 0 {
		if err := w.insertTrustlines(ctx, tx, allTrustlines); err != nil {
			return fmt.Errorf("failed to insert trustlines: %w", err)
		}
		log.Printf("Inserted %d trustlines", len(allTrustlines))
	}

	// Insert all account signers (Phase 1 - Day 3)
	if len(allAccountSigners) > 0 {
		if err := w.insertAccountSigners(ctx, tx, allAccountSigners); err != nil {
			return fmt.Errorf("failed to insert account signers: %w", err)
		}
		log.Printf("Inserted %d account signers", len(allAccountSigners))
	}

	// Insert all claimable balances (Phase 2 - Day 4)
	if len(allClaimableBalances) > 0 {
		if err := w.insertClaimableBalances(ctx, tx, allClaimableBalances); err != nil {
			return fmt.Errorf("failed to insert claimable balances: %w", err)
		}
		log.Printf("Inserted %d claimable balances", len(allClaimableBalances))
	}

	// Insert all liquidity pools (Phase 2 - Day 4)
	if len(allLiquidityPools) > 0 {
		if err := w.insertLiquidityPools(ctx, tx, allLiquidityPools); err != nil {
			return fmt.Errorf("failed to insert liquidity pools: %w", err)
		}
		log.Printf("Inserted %d liquidity pools", len(allLiquidityPools))
	}

	// Insert all config settings (Phase 2 - Day 5)
	if len(allConfigSettings) > 0 {
		if err := w.insertConfigSettings(ctx, tx, allConfigSettings); err != nil {
			return fmt.Errorf("failed to insert config settings: %w", err)
		}
		log.Printf("Inserted %d config settings", len(allConfigSettings))
	}

	// Insert all TTL (Phase 2 - Day 6)
	if len(allTTL) > 0 {
		if err := w.insertTTL(ctx, tx, allTTL); err != nil {
			return fmt.Errorf("failed to insert TTL: %w", err)
		}
		log.Printf("Inserted %d TTL entries", len(allTTL))
	}

	// Insert all evicted keys (Phase 3 - Day 7)
	if len(allEvictedKeys) > 0 {
		if err := w.insertEvictedKeys(ctx, tx, allEvictedKeys); err != nil {
			return fmt.Errorf("failed to insert evicted keys: %w", err)
		}
		log.Printf("Inserted %d evicted keys", len(allEvictedKeys))
	}

	// Insert all contract events (Phase 4 - Day 8)
	if len(allContractEvents) > 0 {
		if err := w.insertContractEvents(ctx, tx, allContractEvents); err != nil {
			return fmt.Errorf("failed to insert contract events: %w", err)
		}
		log.Printf("Inserted %d contract events", len(allContractEvents))
	}

	// Insert all contract data (Phase 4 - Day 9)
	if len(allContractData) > 0 {
		if err := w.insertContractData(ctx, tx, allContractData); err != nil {
			return fmt.Errorf("failed to insert contract data: %w", err)
		}
		log.Printf("Inserted %d contract data entries", len(allContractData))
	}

	// Insert all contract code (Phase 4 - Day 10)
	if len(allContractCode) > 0 {
		if err := w.insertContractCode(ctx, tx, allContractCode); err != nil {
			return fmt.Errorf("failed to insert contract code: %w", err)
		}
		log.Printf("Inserted %d contract code entries", len(allContractCode))
	}

	// Insert all native balances (Phase 5 - Day 11)
	if len(allNativeBalances) > 0 {
		if err := w.insertNativeBalances(ctx, tx, allNativeBalances); err != nil {
			return fmt.Errorf("failed to insert native balances: %w", err)
		}
		log.Printf("Inserted %d native balances", len(allNativeBalances))
	}

	// Insert all restored keys (Phase 5 - Day 11)
	if len(allRestoredKeys) > 0 {
		if err := w.insertRestoredKeys(ctx, tx, allRestoredKeys); err != nil {
			return fmt.Errorf("failed to insert restored keys: %w", err)
		}
		log.Printf("Inserted %d restored keys", len(allRestoredKeys))
	}

	// Insert all contract creations (C11)
	if len(allContractCreations) > 0 {
		if err := w.insertContractCreations(ctx, tx, allContractCreations); err != nil {
			return fmt.Errorf("failed to insert contract creations: %w", err)
		}
		log.Printf("Inserted %d contract creations", len(allContractCreations))
	}

	// Insert all token transfers
	if len(allTokenTransfers) > 0 {
		if err := w.insertTokenTransfers(ctx, tx, allTokenTransfers); err != nil {
			return fmt.Errorf("failed to insert token transfers: %w", err)
		}
		log.Printf("Inserted %d token transfers", len(allTokenTransfers))
	}

	// Commit transaction
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	// Save checkpoint
	if err := w.checkpoint.Save(); err != nil {
		log.Printf("Warning: Failed to save checkpoint: %v", err)
	}

	// Broadcast to gRPC subscribers after checkpoint is persisted
	if w.broadcaster != nil {
		w.broadcaster.Broadcast(BronzeBatchInfo{
			StartLedger: rawLedgers[0].Sequence,
			EndLedger:   rawLedgers[len(rawLedgers)-1].Sequence,
			ClosedAt:    time.Now(),
			TxCount:     totalTxCount,
			OpCount:     totalOpCount,
		})
	}

	// Update metrics
	w.healthServer.UpdateMetrics(uint64(len(rawLedgers)), totalTxCount, totalOpCount)

	log.Printf("Batch written successfully in %v (%.2f ledgers/sec)",
		time.Since(startTime),
		float64(len(rawLedgers))/time.Since(startTime).Seconds())

	return nil
}

// countContractEvents counts contract events from a TransactionMeta.
// Handles V3 (SorobanMeta.Events) and V4 (per-operation events from CAP-67).
func countContractEvents(meta *xdr.TransactionMeta) int {
	switch meta.V {
	case 3:
		v3 := meta.MustV3()
		if v3.SorobanMeta != nil {
			return len(v3.SorobanMeta.Events)
		}
	case 4:
		v4 := meta.MustV4()
		count := 0
		for _, op := range v4.Operations {
			count += len(op.Events)
		}
		return count
	}
	return 0
}

// extractLedgerData extracts ledger data from raw ledger protobuf
func (w *Writer) extractLedgerData(rawLedger *pb.RawLedger) (*LedgerData, error) {
	// Unmarshal XDR
	var lcm xdr.LedgerCloseMeta
	if err := lcm.UnmarshalBinary(rawLedger.LedgerCloseMetaXdr); err != nil {
		return nil, fmt.Errorf("failed to unmarshal XDR: %w", err)
	}

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
		return nil, fmt.Errorf("unknown LedgerCloseMeta version: %d", lcm.V)
	}

	// Extract core fields
	data := &LedgerData{
		Sequence:           uint32(header.Header.LedgerSeq),
		LedgerHash:         hex.EncodeToString(header.Hash[:]),
		PreviousLedgerHash: hex.EncodeToString(header.Header.PreviousLedgerHash[:]),
		ClosedAt:           time.Unix(int64(header.Header.ScpValue.CloseTime), 0).UTC(),
		ProtocolVersion:    uint32(header.Header.LedgerVersion),
		TotalCoins:         int64(header.Header.TotalCoins),
		FeePool:            int64(header.Header.FeePool),
		BaseFee:            uint32(header.Header.BaseFee),
		BaseReserve:        uint32(header.Header.BaseReserve),
		MaxTxSetSize:       uint32(header.Header.MaxTxSetSize),
		IngestionTimestamp: time.Now().UTC(),
	}

	// Calculate ledger_range (partition key)
	data.LedgerRange = (data.Sequence / 10000) * 10000

	// Count transactions and operations based on LedgerCloseMeta version
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
			operationCount += opCount
		}
	case 1:
		v1 := lcm.MustV1()
		txCount = uint32(len(v1.TxProcessing))

		// Count operations from transaction results
		for _, txApply := range v1.TxProcessing {
			if opResults, ok := txApply.Result.Result.OperationResults(); ok {
				opCount := uint32(len(opResults))
				txSetOperationCount += opCount

				if txApply.Result.Result.Successful() {
					operationCount += opCount
				} else {
					failedCount++
				}
			} else {
				failedCount++
			}
		}
	case 2:
		v2 := lcm.MustV2()
		txCount = uint32(len(v2.TxProcessing))

		// Count operations from transaction results
		for _, txApply := range v2.TxProcessing {
			if opResults, ok := txApply.Result.Result.OperationResults(); ok {
				opCount := uint32(len(opResults))
				txSetOperationCount += opCount

				if txApply.Result.Result.Successful() {
					operationCount += opCount
				} else {
					failedCount++
				}
			} else {
				failedCount++
			}
		}
	}

	data.TransactionCount = int(txCount)
	data.SuccessfulTxCount = int(txCount - failedCount)
	data.FailedTxCount = int(failedCount)
	data.OperationCount = int(operationCount)
	data.TxSetOperationCount = int(txSetOperationCount)

	// Compute per-ledger aggregates from TxProcessing
	var totalFeeCharged int64
	var contractEventsCount int

	switch lcm.V {
	case 1:
		for _, txApply := range lcm.MustV1().TxProcessing {
			totalFeeCharged += int64(txApply.Result.Result.FeeCharged)
			contractEventsCount += countContractEvents(&txApply.TxApplyProcessing)
		}
	case 2:
		for _, txApply := range lcm.MustV2().TxProcessing {
			totalFeeCharged += int64(txApply.Result.Result.FeeCharged)
			contractEventsCount += countContractEvents(&txApply.TxApplyProcessing)
		}
	}

	data.TotalFeeCharged = &totalFeeCharged
	data.ContractEventsCount = &contractEventsCount

	// Protocol 20+ Soroban fields
	if lcmV1, ok := lcm.GetV1(); ok {
		if extV1, ok := lcmV1.Ext.GetV1(); ok {
			feeWrite := int64(extV1.SorobanFeeWrite1Kb)
			data.SorobanFeeWrite1KB = &feeWrite
		}
	} else if lcmV2, ok := lcm.GetV2(); ok {
		if extV1, ok := lcmV2.Ext.GetV1(); ok {
			feeWrite := int64(extV1.SorobanFeeWrite1Kb)
			data.SorobanFeeWrite1KB = &feeWrite
		}
	}

	// Node ID and signature (from SCP value)
	if lcValueSig, ok := header.Header.ScpValue.Ext.GetLcValueSignature(); ok {
		nodeIDStr := base64.StdEncoding.EncodeToString(lcValueSig.NodeId.Ed25519[:])
		data.NodeID = &nodeIDStr

		sigStr := base64.StdEncoding.EncodeToString(lcValueSig.Signature[:])
		data.Signature = &sigStr
	}

	// Ledger header XDR (base64 encoded)
	headerXDR, err := header.Header.MarshalBinary()
	if err == nil {
		headerStr := base64.StdEncoding.EncodeToString(headerXDR)
		data.LedgerHeader = &headerStr
	}

	// Bucket list size and live Soroban state size (Protocol 20+)
	if lcmV1, ok := lcm.GetV1(); ok {
		sorobanStateSize := int64(lcmV1.TotalByteSizeOfLiveSorobanState)
		data.BucketListSize = &sorobanStateSize
		data.LiveSorobanStateSize = &sorobanStateSize
	} else if lcmV2, ok := lcm.GetV2(); ok {
		sorobanStateSize := int64(lcmV2.TotalByteSizeOfLiveSorobanState)
		data.BucketListSize = &sorobanStateSize
		data.LiveSorobanStateSize = &sorobanStateSize
	}

	// Protocol 23+ Hot Archive fields (evicted keys count)
	if lcmV1, ok := lcm.GetV1(); ok {
		evicted := int(len(lcmV1.EvictedKeys))
		data.EvictedKeysCount = &evicted
	} else if lcmV2, ok := lcm.GetV2(); ok {
		evicted := int(len(lcmV2.EvictedKeys))
		data.EvictedKeysCount = &evicted
	}

	return data, nil
}

// extractLedgerDataFromLCM extracts ledger data from an already-decoded LedgerCloseMeta.
// This avoids redundant XDR unmarshaling when the LCM is already available from the library input.
func (w *Writer) extractLedgerDataFromLCM(lcm xdr.LedgerCloseMeta) (*LedgerData, error) {
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
		return nil, fmt.Errorf("unknown LedgerCloseMeta version: %d", lcm.V)
	}

	// Extract core fields
	data := &LedgerData{
		Sequence:           uint32(header.Header.LedgerSeq),
		LedgerHash:         hex.EncodeToString(header.Hash[:]),
		PreviousLedgerHash: hex.EncodeToString(header.Header.PreviousLedgerHash[:]),
		ClosedAt:           time.Unix(int64(header.Header.ScpValue.CloseTime), 0).UTC(),
		ProtocolVersion:    uint32(header.Header.LedgerVersion),
		TotalCoins:         int64(header.Header.TotalCoins),
		FeePool:            int64(header.Header.FeePool),
		BaseFee:            uint32(header.Header.BaseFee),
		BaseReserve:        uint32(header.Header.BaseReserve),
		MaxTxSetSize:       uint32(header.Header.MaxTxSetSize),
		IngestionTimestamp: time.Now().UTC(),
	}

	// Calculate ledger_range (partition key)
	data.LedgerRange = (data.Sequence / 10000) * 10000

	// Count transactions and operations based on LedgerCloseMeta version
	var txCount uint32
	var failedCount uint32
	var operationCount uint32
	var txSetOperationCount uint32

	switch lcm.V {
	case 0:
		v0 := lcm.MustV0()
		txCount = uint32(len(v0.TxSet.Txs))
		for _, tx := range v0.TxSet.Txs {
			opCount := uint32(len(tx.Operations()))
			txSetOperationCount += opCount
			operationCount += opCount
		}
	case 1:
		v1 := lcm.MustV1()
		txCount = uint32(len(v1.TxProcessing))
		for _, txApply := range v1.TxProcessing {
			if opResults, ok := txApply.Result.Result.OperationResults(); ok {
				opCount := uint32(len(opResults))
				txSetOperationCount += opCount
				if txApply.Result.Result.Successful() {
					operationCount += opCount
				} else {
					failedCount++
				}
			} else {
				failedCount++
			}
		}
	case 2:
		v2 := lcm.MustV2()
		txCount = uint32(len(v2.TxProcessing))
		for _, txApply := range v2.TxProcessing {
			if opResults, ok := txApply.Result.Result.OperationResults(); ok {
				opCount := uint32(len(opResults))
				txSetOperationCount += opCount
				if txApply.Result.Result.Successful() {
					operationCount += opCount
				} else {
					failedCount++
				}
			} else {
				failedCount++
			}
		}
	}

	data.TransactionCount = int(txCount)
	data.SuccessfulTxCount = int(txCount - failedCount)
	data.FailedTxCount = int(failedCount)
	data.OperationCount = int(operationCount)
	data.TxSetOperationCount = int(txSetOperationCount)

	// Compute per-ledger aggregates from TxProcessing
	var totalFeeCharged int64
	var contractEventsCount int

	switch lcm.V {
	case 1:
		for _, txApply := range lcm.MustV1().TxProcessing {
			totalFeeCharged += int64(txApply.Result.Result.FeeCharged)
			contractEventsCount += countContractEvents(&txApply.TxApplyProcessing)
		}
	case 2:
		for _, txApply := range lcm.MustV2().TxProcessing {
			totalFeeCharged += int64(txApply.Result.Result.FeeCharged)
			contractEventsCount += countContractEvents(&txApply.TxApplyProcessing)
		}
	}

	data.TotalFeeCharged = &totalFeeCharged
	data.ContractEventsCount = &contractEventsCount

	// Protocol 20+ Soroban fields
	if lcmV1, ok := lcm.GetV1(); ok {
		if extV1, ok := lcmV1.Ext.GetV1(); ok {
			feeWrite := int64(extV1.SorobanFeeWrite1Kb)
			data.SorobanFeeWrite1KB = &feeWrite
		}
	} else if lcmV2, ok := lcm.GetV2(); ok {
		if extV1, ok := lcmV2.Ext.GetV1(); ok {
			feeWrite := int64(extV1.SorobanFeeWrite1Kb)
			data.SorobanFeeWrite1KB = &feeWrite
		}
	}

	// Node ID and signature (from SCP value)
	if lcValueSig, ok := header.Header.ScpValue.Ext.GetLcValueSignature(); ok {
		nodeIDStr := base64.StdEncoding.EncodeToString(lcValueSig.NodeId.Ed25519[:])
		data.NodeID = &nodeIDStr

		sigStr := base64.StdEncoding.EncodeToString(lcValueSig.Signature[:])
		data.Signature = &sigStr
	}

	// Ledger header XDR (base64 encoded)
	headerXDR, err := header.Header.MarshalBinary()
	if err == nil {
		headerStr := base64.StdEncoding.EncodeToString(headerXDR)
		data.LedgerHeader = &headerStr
	}

	// Bucket list size and live Soroban state size (Protocol 20+)
	if lcmV1, ok := lcm.GetV1(); ok {
		sorobanStateSize := int64(lcmV1.TotalByteSizeOfLiveSorobanState)
		data.BucketListSize = &sorobanStateSize
		data.LiveSorobanStateSize = &sorobanStateSize
	} else if lcmV2, ok := lcm.GetV2(); ok {
		sorobanStateSize := int64(lcmV2.TotalByteSizeOfLiveSorobanState)
		data.BucketListSize = &sorobanStateSize
		data.LiveSorobanStateSize = &sorobanStateSize
	}

	// Protocol 23+ Hot Archive fields (evicted keys count)
	if lcmV1, ok := lcm.GetV1(); ok {
		evicted := int(len(lcmV1.EvictedKeys))
		data.EvictedKeysCount = &evicted
	} else if lcmV2, ok := lcm.GetV2(); ok {
		evicted := int(len(lcmV2.EvictedKeys))
		data.EvictedKeysCount = &evicted
	}

	return data, nil
}

// ---------------------------------------------------------------------------
// Type conversion helpers: extract library types -> local ingester types
// The library types have additional fields (EraID, SourceAccountMuxed, etc.)
// that are not present in the local types. These helpers map the common fields.
// ---------------------------------------------------------------------------

func convertTransaction(r extract.TransactionData) TransactionData {
	return TransactionData{
		LedgerSequence:               r.LedgerSequence,
		TransactionHash:              r.TransactionHash,
		TransactionID:                r.TransactionID,
		SourceAccount:                r.SourceAccount,
		FeeCharged:                   r.FeeCharged,
		MaxFee:                       r.MaxFee,
		Successful:                   r.Successful,
		TransactionResultCode:        r.TransactionResultCode,
		OperationCount:               r.OperationCount,
		MemoType:                     r.MemoType,
		Memo:                         r.Memo,
		CreatedAt:                    r.CreatedAt,
		AccountSequence:              r.AccountSequence,
		LedgerRange:                  r.LedgerRange,
		SignaturesCount:              r.SignaturesCount,
		NewAccount:                   r.NewAccount,
		RentFeeCharged:               r.RentFeeCharged,
		SorobanResourcesInstructions: r.SorobanResourcesInstructions,
		SorobanResourcesReadBytes:    r.SorobanResourcesReadBytes,
		SorobanResourcesWriteBytes:   r.SorobanResourcesWriteBytes,
	}
}

func convertOperation(r extract.OperationData) OperationData {
	return OperationData{
		TransactionHash:       r.TransactionHash,
		TransactionID:         r.TransactionID,
		OperationID:           r.OperationID,
		TransactionIndex:      r.TransactionIndex,
		OperationIndex:        r.OperationIndex,
		LedgerSequence:        r.LedgerSequence,
		SourceAccount:         r.SourceAccount,
		OpType:                r.OpType,
		TypeString:            r.TypeString,
		CreatedAt:             r.CreatedAt,
		TransactionSuccessful: r.TransactionSuccessful,
		OperationResultCode:   r.OperationResultCode,
		LedgerRange:           r.LedgerRange,
		Amount:                r.Amount,
		Asset:                 r.Asset,
		Destination:           r.Destination,
		SorobanOperation:      r.SorobanOperation,
		SorobanContractID:     r.SorobanContractID,
		SorobanFunction:       r.SorobanFunction,
		SorobanArgumentsJSON:  r.SorobanArgumentsJSON,
		ContractCallsJSON:     r.ContractCallsJSON,
		ContractsInvolved:     r.ContractsInvolved,
		MaxCallDepth:          r.MaxCallDepth,
	}
}

func convertEffect(r extract.EffectData) EffectData {
	var opID int64
	if r.OperationID != nil {
		opID = *r.OperationID
	}
	return EffectData{
		LedgerSequence:   r.LedgerSequence,
		TransactionHash:  r.TransactionHash,
		OperationIndex:   r.OperationIndex,
		EffectIndex:      r.EffectIndex,
		OperationID:      opID,
		EffectType:       r.EffectType,
		EffectTypeString: r.EffectTypeString,
		AccountID:        r.AccountID,
		Amount:           r.Amount,
		AssetCode:        r.AssetCode,
		AssetIssuer:      r.AssetIssuer,
		AssetType:        r.AssetType,
		DetailsJSON:      r.DetailsJSON,
		TrustlineLimit:   r.TrustlineLimit,
		AuthorizeFlag:    r.AuthorizeFlag,
		ClawbackFlag:     r.ClawbackFlag,
		SignerAccount:    r.SignerAccount,
		SignerWeight:     r.SignerWeight,
		OfferID:          r.OfferID,
		SellerAccount:    r.SellerAccount,
		CreatedAt:        r.CreatedAt,
		LedgerRange:      r.LedgerRange,
	}
}

func convertTrade(r extract.TradeData) TradeData {
	return TradeData{
		LedgerSequence:     r.LedgerSequence,
		TransactionHash:    r.TransactionHash,
		OperationIndex:     r.OperationIndex,
		TradeIndex:         r.TradeIndex,
		TradeType:          r.TradeType,
		TradeTimestamp:     r.TradeTimestamp,
		SellerAccount:      r.SellerAccount,
		SellingAssetCode:   r.SellingAssetCode,
		SellingAssetIssuer: r.SellingAssetIssuer,
		SellingAmount:      r.SellingAmount,
		BuyerAccount:       r.BuyerAccount,
		BuyingAssetCode:    r.BuyingAssetCode,
		BuyingAssetIssuer:  r.BuyingAssetIssuer,
		BuyingAmount:       r.BuyingAmount,
		Price:              r.Price,
		CreatedAt:          r.CreatedAt,
		LedgerRange:        r.LedgerRange,
	}
}

func convertAccount(r extract.AccountData) AccountData {
	return AccountData{
		AccountID:           r.AccountID,
		LedgerSequence:      r.LedgerSequence,
		ClosedAt:            r.ClosedAt,
		Balance:             r.Balance,
		SequenceNumber:      r.SequenceNumber,
		NumSubentries:       r.NumSubentries,
		NumSponsoring:       r.NumSponsoring,
		NumSponsored:        r.NumSponsored,
		HomeDomain:          r.HomeDomain,
		MasterWeight:        r.MasterWeight,
		LowThreshold:        r.LowThreshold,
		MedThreshold:        r.MedThreshold,
		HighThreshold:       r.HighThreshold,
		Flags:               r.Flags,
		AuthRequired:        r.AuthRequired,
		AuthRevocable:       r.AuthRevocable,
		AuthImmutable:       r.AuthImmutable,
		AuthClawbackEnabled: r.AuthClawbackEnabled,
		Signers:             r.Signers,
		SponsorAccount:      r.SponsorAccount,
		CreatedAt:           r.CreatedAt,
		UpdatedAt:           r.UpdatedAt,
		LedgerRange:         r.LedgerRange,
	}
}

func convertOffer(r extract.OfferData) OfferData {
	return OfferData{
		OfferID:            r.OfferID,
		SellerAccount:      r.SellerAccount,
		LedgerSequence:     r.LedgerSequence,
		ClosedAt:           r.ClosedAt,
		SellingAssetType:   r.SellingAssetType,
		SellingAssetCode:   r.SellingAssetCode,
		SellingAssetIssuer: r.SellingAssetIssuer,
		BuyingAssetType:    r.BuyingAssetType,
		BuyingAssetCode:    r.BuyingAssetCode,
		BuyingAssetIssuer:  r.BuyingAssetIssuer,
		Amount:             r.Amount,
		Price:              r.Price,
		Flags:              r.Flags,
		CreatedAt:          r.CreatedAt,
		LedgerRange:        r.LedgerRange,
	}
}

func convertTrustline(r extract.TrustlineData) TrustlineData {
	return TrustlineData{
		AccountID:                       r.AccountID,
		AssetCode:                       r.AssetCode,
		AssetIssuer:                     r.AssetIssuer,
		AssetType:                       r.AssetType,
		Balance:                         r.Balance,
		TrustLimit:                      r.TrustLimit,
		BuyingLiabilities:               r.BuyingLiabilities,
		SellingLiabilities:              r.SellingLiabilities,
		Authorized:                      r.Authorized,
		AuthorizedToMaintainLiabilities: r.AuthorizedToMaintainLiabilities,
		ClawbackEnabled:                 r.ClawbackEnabled,
		LedgerSequence:                  r.LedgerSequence,
		CreatedAt:                       r.CreatedAt,
		LedgerRange:                     r.LedgerRange,
	}
}

func convertAccountSigner(r extract.AccountSignerData) AccountSignerData {
	return AccountSignerData{
		AccountID:      r.AccountID,
		Signer:         r.Signer,
		LedgerSequence: r.LedgerSequence,
		Weight:         r.Weight,
		Sponsor:        r.Sponsor,
		Deleted:        r.Deleted,
		ClosedAt:       r.ClosedAt,
		LedgerRange:    r.LedgerRange,
		CreatedAt:      r.CreatedAt,
	}
}

func convertClaimableBalance(r extract.ClaimableBalanceData) ClaimableBalanceData {
	return ClaimableBalanceData{
		BalanceID:      r.BalanceID,
		Sponsor:        r.Sponsor,
		LedgerSequence: r.LedgerSequence,
		ClosedAt:       r.ClosedAt,
		AssetType:      r.AssetType,
		AssetCode:      r.AssetCode,
		AssetIssuer:    r.AssetIssuer,
		Amount:         r.Amount,
		ClaimantsCount: r.ClaimantsCount,
		Flags:          r.Flags,
		CreatedAt:      r.CreatedAt,
		LedgerRange:    r.LedgerRange,
	}
}

func convertLiquidityPool(r extract.LiquidityPoolData) LiquidityPoolData {
	return LiquidityPoolData{
		LiquidityPoolID: r.LiquidityPoolID,
		LedgerSequence:  r.LedgerSequence,
		ClosedAt:        r.ClosedAt,
		PoolType:        r.PoolType,
		Fee:             r.Fee,
		TrustlineCount:  r.TrustlineCount,
		TotalPoolShares: r.TotalPoolShares,
		AssetAType:      r.AssetAType,
		AssetACode:      r.AssetACode,
		AssetAIssuer:    r.AssetAIssuer,
		AssetAAmount:    r.AssetAAmount,
		AssetBType:      r.AssetBType,
		AssetBCode:      r.AssetBCode,
		AssetBIssuer:    r.AssetBIssuer,
		AssetBAmount:    r.AssetBAmount,
		CreatedAt:       r.CreatedAt,
		LedgerRange:     r.LedgerRange,
	}
}

func convertConfigSetting(r extract.ConfigSettingData) ConfigSettingData {
	return ConfigSettingData{
		ConfigSettingID:                 r.ConfigSettingID,
		LedgerSequence:                  r.LedgerSequence,
		LastModifiedLedger:              r.LastModifiedLedger,
		Deleted:                         r.Deleted,
		ClosedAt:                        r.ClosedAt,
		LedgerMaxInstructions:           r.LedgerMaxInstructions,
		TxMaxInstructions:               r.TxMaxInstructions,
		FeeRatePerInstructionsIncrement: r.FeeRatePerInstructionsIncrement,
		TxMemoryLimit:                   r.TxMemoryLimit,
		LedgerMaxReadLedgerEntries:      r.LedgerMaxReadLedgerEntries,
		LedgerMaxReadBytes:              r.LedgerMaxReadBytes,
		LedgerMaxWriteLedgerEntries:     r.LedgerMaxWriteLedgerEntries,
		LedgerMaxWriteBytes:             r.LedgerMaxWriteBytes,
		TxMaxReadLedgerEntries:          r.TxMaxReadLedgerEntries,
		TxMaxReadBytes:                  r.TxMaxReadBytes,
		TxMaxWriteLedgerEntries:         r.TxMaxWriteLedgerEntries,
		TxMaxWriteBytes:                 r.TxMaxWriteBytes,
		ContractMaxSizeBytes:            r.ContractMaxSizeBytes,
		ConfigSettingXDR:                r.ConfigSettingXDR,
		CreatedAt:                       r.CreatedAt,
		LedgerRange:                     r.LedgerRange,
	}
}

func convertTTL(r extract.TTLData) TTLData {
	return TTLData{
		KeyHash:            r.KeyHash,
		LedgerSequence:     r.LedgerSequence,
		LiveUntilLedgerSeq: r.LiveUntilLedgerSeq,
		TTLRemaining:       r.TTLRemaining,
		Expired:            r.Expired,
		LastModifiedLedger: r.LastModifiedLedger,
		Deleted:            r.Deleted,
		ClosedAt:           r.ClosedAt,
		CreatedAt:          r.CreatedAt,
		LedgerRange:        r.LedgerRange,
	}
}

func convertEvictedKey(r extract.EvictedKeyData) EvictedKeyData {
	return EvictedKeyData{
		KeyHash:        r.KeyHash,
		LedgerSequence: r.LedgerSequence,
		ContractID:     r.ContractID,
		KeyType:        r.KeyType,
		Durability:     r.Durability,
		ClosedAt:       r.ClosedAt,
		LedgerRange:    r.LedgerRange,
		CreatedAt:      r.CreatedAt,
	}
}

func convertContractEvent(r extract.ContractEventData) ContractEventData {
	return ContractEventData{
		EventID:                  r.EventID,
		ContractID:               r.ContractID,
		LedgerSequence:           r.LedgerSequence,
		TransactionHash:          r.TransactionHash,
		ClosedAt:                 r.ClosedAt,
		EventType:                r.EventType,
		InSuccessfulContractCall: r.InSuccessfulContractCall,
		Successful:               r.Successful,
		ContractEventXDR:         r.ContractEventXDR,
		TopicsJSON:               r.TopicsJSON,
		TopicsDecoded:            r.TopicsDecoded,
		DataXDR:                  r.DataXDR,
		DataDecoded:              r.DataDecoded,
		TopicCount:               r.TopicCount,
		Topic0Decoded:            r.Topic0Decoded,
		Topic1Decoded:            r.Topic1Decoded,
		Topic2Decoded:            r.Topic2Decoded,
		Topic3Decoded:            r.Topic3Decoded,
		OperationIndex:           r.OperationIndex,
		EventIndex:               r.EventIndex,
		CreatedAt:                r.CreatedAt,
		LedgerRange:              r.LedgerRange,
	}
}

func convertContractData(r extract.ContractDataData) ContractDataData {
	return ContractDataData{
		ContractId:         r.ContractId,
		LedgerSequence:     r.LedgerSequence,
		LedgerKeyHash:      r.LedgerKeyHash,
		ContractKeyType:    r.ContractKeyType,
		ContractDurability: r.ContractDurability,
		AssetCode:          r.AssetCode,
		AssetIssuer:        r.AssetIssuer,
		AssetType:          r.AssetType,
		BalanceHolder:      r.BalanceHolder,
		Balance:            r.Balance,
		LastModifiedLedger: r.LastModifiedLedger,
		LedgerEntryChange:  r.LedgerEntryChange,
		Deleted:            r.Deleted,
		ClosedAt:           r.ClosedAt,
		ContractDataXDR:    r.ContractDataXDR,
		TokenName:          r.TokenName,
		TokenSymbol:        r.TokenSymbol,
		TokenDecimals:      r.TokenDecimals,
		CreatedAt:          r.CreatedAt,
		LedgerRange:        r.LedgerRange,
	}
}

func convertContractCode(r extract.ContractCodeData) ContractCodeData {
	return ContractCodeData{
		ContractCodeHash:   r.ContractCodeHash,
		LedgerKeyHash:      r.LedgerKeyHash,
		ContractCodeExtV:   r.ContractCodeExtV,
		LastModifiedLedger: r.LastModifiedLedger,
		LedgerEntryChange:  r.LedgerEntryChange,
		Deleted:            r.Deleted,
		ClosedAt:           r.ClosedAt,
		LedgerSequence:     r.LedgerSequence,
		NInstructions:      r.NInstructions,
		NFunctions:         r.NFunctions,
		NGlobals:           r.NGlobals,
		NTableEntries:      r.NTableEntries,
		NTypes:             r.NTypes,
		NDataSegments:      r.NDataSegments,
		NElemSegments:      r.NElemSegments,
		NImports:           r.NImports,
		NExports:           r.NExports,
		NDataSegmentBytes:  r.NDataSegmentBytes,
		CreatedAt:          r.CreatedAt,
		LedgerRange:        r.LedgerRange,
	}
}

func convertNativeBalance(r extract.NativeBalanceData) NativeBalanceData {
	return NativeBalanceData{
		AccountID:          r.AccountID,
		Balance:            r.Balance,
		BuyingLiabilities:  r.BuyingLiabilities,
		SellingLiabilities: r.SellingLiabilities,
		NumSubentries:      r.NumSubentries,
		NumSponsoring:      r.NumSponsoring,
		NumSponsored:       r.NumSponsored,
		SequenceNumber:     r.SequenceNumber,
		LastModifiedLedger: r.LastModifiedLedger,
		LedgerSequence:     r.LedgerSequence,
		LedgerRange:        r.LedgerRange,
	}
}

func convertRestoredKey(r extract.RestoredKeyData) RestoredKeyData {
	return RestoredKeyData{
		KeyHash:            r.KeyHash,
		LedgerSequence:     r.LedgerSequence,
		ContractID:         r.ContractID,
		KeyType:            r.KeyType,
		Durability:         r.Durability,
		RestoredFromLedger: r.RestoredFromLedger,
		ClosedAt:           r.ClosedAt,
		LedgerRange:        r.LedgerRange,
		CreatedAt:          r.CreatedAt,
	}
}

func convertContractCreation(r extract.ContractCreationData) ContractCreationData {
	return ContractCreationData{
		ContractID:     r.ContractID,
		CreatorAddress: r.CreatorAddress,
		WasmHash:       r.WasmHash,
		CreatedLedger:  r.CreatedLedger,
		CreatedAt:      r.CreatedAt,
		LedgerRange:    r.LedgerRange,
	}
}

func convertTokenTransfer(r extract.TokenTransferData) TokenTransferData {
	return TokenTransferData{
		LedgerSequence:  r.LedgerSequence,
		TransactionHash: r.TransactionHash,
		TransactionID:   r.TransactionID,
		OperationID:     r.OperationID,
		OperationIndex:  r.OperationIndex,
		EventType:       r.EventType,
		From:            r.From,
		To:              r.To,
		Asset:           r.Asset,
		AssetType:       r.AssetType,
		AssetCode:       r.AssetCode,
		AssetIssuer:     r.AssetIssuer,
		Amount:          r.Amount,
		AmountRaw:       r.AmountRaw,
		ContractID:      r.ContractID,
		ClosedAt:        r.ClosedAt,
		CreatedAt:       r.CreatedAt,
		LedgerRange:     r.LedgerRange,
	}
}

// insertLedgers bulk-upserts ledgers via COPY → temp → INSERT SELECT.
// Replaces the old per-ledger tx.Exec (previously invoked inside the
// per-ledger loop) with a single COPY for the whole batch.
func (w *Writer) insertLedgers(ctx context.Context, tx pgx.Tx, ledgers []LedgerData) error {
	columns := []string{
		"sequence", "ledger_hash", "previous_ledger_hash", "closed_at",
		"protocol_version", "total_coins", "fee_pool", "base_fee", "base_reserve",
		"max_tx_set_size", "successful_tx_count", "failed_tx_count",
		"ingestion_timestamp", "ledger_range",
		"transaction_count", "operation_count", "tx_set_operation_count",
		"soroban_fee_write1kb", "node_id", "signature", "ledger_header",
		"bucket_list_size", "live_soroban_state_size", "evicted_keys_count",
		"soroban_op_count", "total_fee_charged", "contract_events_count",
	}
	rows := make([][]interface{}, len(ledgers))
	for i := range ledgers {
		l := &ledgers[i]
		rows[i] = []interface{}{
			l.Sequence, l.LedgerHash, l.PreviousLedgerHash, l.ClosedAt,
			l.ProtocolVersion, l.TotalCoins, l.FeePool, l.BaseFee, l.BaseReserve,
			l.MaxTxSetSize, l.SuccessfulTxCount, l.FailedTxCount,
			l.IngestionTimestamp, l.LedgerRange,
			l.TransactionCount, l.OperationCount, l.TxSetOperationCount,
			l.SorobanFeeWrite1KB, l.NodeID, l.Signature, l.LedgerHeader,
			l.BucketListSize, l.LiveSorobanStateSize, l.EvictedKeysCount,
			l.SorobanOpCount, l.TotalFeeCharged, l.ContractEventsCount,
		}
	}
	return w.bulkUpsertCopy(ctx, tx, "ledgers_row_v2", columns,
		[]string{"sequence"},
		[]string{
			"ledger_hash", "closed_at",
			"transaction_count", "operation_count",
			"successful_tx_count", "failed_tx_count",
			"soroban_op_count", "total_fee_charged", "contract_events_count",
		},
		rows,
	)
}

// insertTransactions inserts transactions into PostgreSQL
func (w *Writer) insertTransactions(ctx context.Context, tx pgx.Tx, transactions []TransactionData) error {
	if len(transactions) == 0 {
		return nil
	}

	columns := []string{
		"ledger_sequence", "transaction_hash", "transaction_id", "source_account", "fee_charged",
		"max_fee", "successful", "transaction_result_code", "operation_count",
		"memo_type", "memo", "created_at", "account_sequence", "ledger_range",
		"signatures_count", "new_account", "rent_fee_charged",
		"soroban_resources_instructions", "soroban_resources_read_bytes",
		"soroban_resources_write_bytes",
	}

	rows := make([][]interface{}, len(transactions))
	for i := range transactions {
		txData := &transactions[i]
		// Stellar text memos are arbitrary bytes (up to 28) and can legally
		// contain NUL — PostgreSQL TEXT does not allow 0x00 so we strip.
		// MemoType and other free-form strings get the same treatment.
		txData.Memo = sanitizeStringPtr(txData.Memo)
		txData.MemoType = sanitizeStringPtr(txData.MemoType)
		rows[i] = []interface{}{
			txData.LedgerSequence,
			txData.TransactionHash,
			txData.TransactionID,
			txData.SourceAccount,
			txData.FeeCharged,
			txData.MaxFee,
			txData.Successful,
			txData.TransactionResultCode,
			txData.OperationCount,
			txData.MemoType,
			txData.Memo,
			txData.CreatedAt,
			txData.AccountSequence,
			txData.LedgerRange,
			txData.SignaturesCount,
			txData.NewAccount,
			txData.RentFeeCharged,
			txData.SorobanResourcesInstructions,
			txData.SorobanResourcesReadBytes,
			txData.SorobanResourcesWriteBytes,
		}
	}
	return w.bulkUpsertCopy(ctx, tx, "transactions_row_v2", columns,
		[]string{"ledger_sequence", "transaction_hash"},
		[]string{
			"transaction_id", "successful", "fee_charged", "rent_fee_charged",
			"soroban_resources_instructions",
			"soroban_resources_read_bytes",
			"soroban_resources_write_bytes",
		},
		rows,
	)
}

// insertOperations bulk-inserts operations using pgx Batch for efficiency.
func (w *Writer) insertOperations(ctx context.Context, tx pgx.Tx, operations []OperationData) error {
	if len(operations) == 0 {
		return nil
	}

	query := `
		INSERT INTO operations_row_v2 (
			transaction_hash, transaction_id, operation_id,
			transaction_index, operation_index, ledger_sequence, source_account,
			type, type_string, created_at, transaction_successful,
			operation_result_code, ledger_range, amount, asset, destination,
			soroban_operation, soroban_contract_id, soroban_function, soroban_arguments_json,
			contract_calls_json, contracts_involved, max_call_depth,
			soroban_auth_credentials_types, soroban_auth_addresses
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
			$11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23,
			$24, $25
		)
		ON CONFLICT (ledger_sequence, transaction_hash, operation_index) DO UPDATE SET
			transaction_id = EXCLUDED.transaction_id,
			operation_id = EXCLUDED.operation_id,
			transaction_successful = EXCLUDED.transaction_successful,
			transaction_index = EXCLUDED.transaction_index,
			soroban_operation = EXCLUDED.soroban_operation,
			soroban_contract_id = EXCLUDED.soroban_contract_id,
			soroban_function = EXCLUDED.soroban_function,
			soroban_arguments_json = EXCLUDED.soroban_arguments_json,
			contract_calls_json = EXCLUDED.contract_calls_json,
			contracts_involved = EXCLUDED.contracts_involved,
			max_call_depth = EXCLUDED.max_call_depth,
			soroban_auth_credentials_types = EXCLUDED.soroban_auth_credentials_types,
			soroban_auth_addresses = EXCLUDED.soroban_auth_addresses
	`

	batch := &pgx.Batch{}
	for i := range operations {
		opData := &operations[i]
		// Sanitize text fields
		opData.Asset = sanitizeStringPtr(opData.Asset)
		opData.Destination = sanitizeStringPtr(opData.Destination)
		opData.SorobanOperation = sanitizeStringPtr(opData.SorobanOperation)
		opData.SorobanContractID = sanitizeStringPtr(opData.SorobanContractID)
		opData.SorobanFunction = sanitizeStringPtr(opData.SorobanFunction)
		opData.SorobanArgumentsJSON = sanitizeStringPtr(opData.SorobanArgumentsJSON)
		opData.ContractCallsJSON = sanitizeStringPtr(opData.ContractCallsJSON)

		batch.Queue(query,
			opData.TransactionHash,
			opData.TransactionID,
			opData.OperationID,
			opData.TransactionIndex,
			opData.OperationIndex,
			opData.LedgerSequence,
			opData.SourceAccount,
			opData.OpType,
			opData.TypeString,
			opData.CreatedAt,
			opData.TransactionSuccessful,
			opData.OperationResultCode,
			opData.LedgerRange,
			opData.Amount,
			opData.Asset,
			opData.Destination,
			opData.SorobanOperation,
			opData.SorobanContractID,
			opData.SorobanFunction,
			opData.SorobanArgumentsJSON,
			opData.ContractCallsJSON,
			opData.ContractsInvolved,
			opData.MaxCallDepth,
			opData.SorobanAuthCredentialsTypes,
			opData.SorobanAuthAddresses,
		)
	}

	br := tx.SendBatch(ctx, batch)
	defer br.Close()
	for i := range operations {
		if _, err := br.Exec(); err != nil {
			return fmt.Errorf("failed to insert operation %s:%d: %w", operations[i].TransactionHash, operations[i].OperationIndex, err)
		}
	}

	return nil
}

// insertEffects inserts effects into PostgreSQL
func (w *Writer) insertEffects(ctx context.Context, tx pgx.Tx, effects []EffectData) error {
	if len(effects) == 0 {
		return nil
	}

	query := `
		INSERT INTO effects_row_v1 (
			ledger_sequence, transaction_hash, operation_index, effect_index,
			operation_id, effect_type, effect_type_string, account_id,
			amount, asset_code, asset_issuer, asset_type,
			details_json,
			trustline_limit, authorize_flag, clawback_flag,
			signer_account, signer_weight,
			offer_id, seller_account,
			created_at, ledger_range
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
			$11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22
		)
		ON CONFLICT (ledger_sequence, transaction_hash, operation_index, effect_index) DO NOTHING
	`

	batch := &pgx.Batch{}
	for i := range effects {
		e := &effects[i]
		// Sanitize text fields. DetailsJSON in particular is decoded
		// scval data and can carry raw bytes (including 0x00) from
		// contract storage; the rest are defensive cleanups for fields
		// that are typically constrained but typed as *string.
		e.DetailsJSON = sanitizeStringPtr(e.DetailsJSON)
		e.Amount = sanitizeStringPtr(e.Amount)
		e.AssetCode = sanitizeStringPtr(e.AssetCode)
		e.AssetIssuer = sanitizeStringPtr(e.AssetIssuer)
		e.AssetType = sanitizeStringPtr(e.AssetType)
		e.TrustlineLimit = sanitizeStringPtr(e.TrustlineLimit)
		e.SignerAccount = sanitizeStringPtr(e.SignerAccount)
		e.SellerAccount = sanitizeStringPtr(e.SellerAccount)
		batch.Queue(query,
			e.LedgerSequence,
			e.TransactionHash,
			e.OperationIndex,
			e.EffectIndex,
			e.OperationID,
			e.EffectType,
			e.EffectTypeString,
			e.AccountID,
			e.Amount,
			e.AssetCode,
			e.AssetIssuer,
			e.AssetType,
			e.DetailsJSON,
			e.TrustlineLimit,
			e.AuthorizeFlag,
			e.ClawbackFlag,
			e.SignerAccount,
			e.SignerWeight,
			e.OfferID,
			e.SellerAccount,
			e.CreatedAt,
			e.LedgerRange,
		)
	}

	br := tx.SendBatch(ctx, batch)
	defer br.Close()
	for i := range effects {
		if _, err := br.Exec(); err != nil {
			return fmt.Errorf("failed to insert effect %s:%d:%d: %w",
				effects[i].TransactionHash, effects[i].OperationIndex, effects[i].EffectIndex, err)
		}
	}

	return nil
}

// insertTrades inserts trades into PostgreSQL
func (w *Writer) insertTrades(ctx context.Context, tx pgx.Tx, trades []TradeData) error {
	if len(trades) == 0 {
		return nil
	}

	const query = `
		INSERT INTO trades_row_v1 (
			ledger_sequence, transaction_hash, operation_index, trade_index,
			trade_type, trade_timestamp,
			seller_account, selling_asset_code, selling_asset_issuer, selling_amount,
			buyer_account, buying_asset_code, buying_asset_issuer, buying_amount,
			price, created_at, ledger_range
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
			$11, $12, $13, $14, $15, $16, $17
		)
		ON CONFLICT (ledger_sequence, transaction_hash, operation_index, trade_index) DO NOTHING
	`

	rows := make([][]interface{}, len(trades))
	for i := range trades {
		t := &trades[i]
		rows[i] = []interface{}{
			t.LedgerSequence, t.TransactionHash, t.OperationIndex, t.TradeIndex,
			t.TradeType, t.TradeTimestamp,
			t.SellerAccount, t.SellingAssetCode, t.SellingAssetIssuer, t.SellingAmount,
			t.BuyerAccount, t.BuyingAssetCode, t.BuyingAssetIssuer, t.BuyingAmount,
			t.Price, t.CreatedAt, t.LedgerRange,
		}
	}
	return batchExec(ctx, tx, query, rows)
}

// insertAccounts inserts account snapshots into PostgreSQL
func (w *Writer) insertAccounts(ctx context.Context, tx pgx.Tx, accounts []AccountData) error {
	if len(accounts) == 0 {
		return nil
	}

	columns := []string{
		"account_id", "ledger_sequence", "closed_at", "balance",
		"sequence_number", "num_subentries", "num_sponsoring", "num_sponsored", "home_domain",
		"master_weight", "low_threshold", "med_threshold", "high_threshold",
		"flags", "auth_required", "auth_revocable", "auth_immutable", "auth_clawback_enabled",
		"signers", "sponsor_account",
		"created_at", "updated_at", "ledger_range",
	}
	rows := make([][]interface{}, len(accounts))
	for i := range accounts {
		acct := &accounts[i]
		// HomeDomain is user-controlled (up to 32 bytes, no protocol-level
		// validation). Signers is a JSON array marshalled from account
		// state. Both can carry NUL bytes that PG TEXT rejects.
		acct.HomeDomain = sanitizeStringPtr(acct.HomeDomain)
		acct.Signers = sanitizeStringPtr(acct.Signers)
		rows[i] = []interface{}{
			acct.AccountID,
			acct.LedgerSequence,
			acct.ClosedAt,
			acct.Balance,
			acct.SequenceNumber,
			acct.NumSubentries,
			acct.NumSponsoring,
			acct.NumSponsored,
			acct.HomeDomain,
			acct.MasterWeight,
			acct.LowThreshold,
			acct.MedThreshold,
			acct.HighThreshold,
			acct.Flags,
			acct.AuthRequired,
			acct.AuthRevocable,
			acct.AuthImmutable,
			acct.AuthClawbackEnabled,
			acct.Signers,
			acct.SponsorAccount,
			acct.CreatedAt,
			acct.UpdatedAt,
			acct.LedgerRange,
		}
	}
	return w.bulkUpsertCopy(ctx, tx, "accounts_snapshot_v1", columns,
		[]string{"account_id", "ledger_sequence"},
		[]string{"balance", "sequence_number", "num_subentries", "flags", "updated_at"},
		rows,
	)
}

// insertOffers inserts DEX offer snapshots into PostgreSQL
func (w *Writer) insertOffers(ctx context.Context, tx pgx.Tx, offers []OfferData) error {
	if len(offers) == 0 {
		return nil
	}

	const query = `
		INSERT INTO offers_snapshot_v1 (
			offer_id, seller_account, ledger_sequence, closed_at,
			selling_asset_type, selling_asset_code, selling_asset_issuer,
			buying_asset_type, buying_asset_code, buying_asset_issuer,
			amount, price, flags,
			created_at, ledger_range
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
			$11, $12, $13, $14, $15
		)
		ON CONFLICT (offer_id, ledger_sequence) DO UPDATE SET
			amount = EXCLUDED.amount,
			price = EXCLUDED.price,
			created_at = EXCLUDED.created_at
	`

	rows := make([][]interface{}, len(offers))
	for i := range offers {
		o := &offers[i]
		rows[i] = []interface{}{
			o.OfferID, o.SellerAccount, o.LedgerSequence, o.ClosedAt,
			o.SellingAssetType, o.SellingAssetCode, o.SellingAssetIssuer,
			o.BuyingAssetType, o.BuyingAssetCode, o.BuyingAssetIssuer,
			o.Amount, o.Price, o.Flags,
			o.CreatedAt, o.LedgerRange,
		}
	}
	return batchExec(ctx, tx, query, rows)
}

// insertTrustlines inserts trustline snapshots into PostgreSQL
func (w *Writer) insertTrustlines(ctx context.Context, tx pgx.Tx, trustlines []TrustlineData) error {
	if len(trustlines) == 0 {
		return nil
	}

	columns := []string{
		"account_id", "asset_code", "asset_issuer", "asset_type",
		"balance", "trust_limit", "buying_liabilities", "selling_liabilities",
		"authorized", "authorized_to_maintain_liabilities", "clawback_enabled",
		"ledger_sequence", "created_at", "ledger_range",
	}
	rows := make([][]interface{}, len(trustlines))
	for i := range trustlines {
		tl := &trustlines[i]
		rows[i] = []interface{}{
			tl.AccountID, tl.AssetCode, tl.AssetIssuer, tl.AssetType,
			tl.Balance, tl.TrustLimit, tl.BuyingLiabilities, tl.SellingLiabilities,
			tl.Authorized, tl.AuthorizedToMaintainLiabilities, tl.ClawbackEnabled,
			tl.LedgerSequence, tl.CreatedAt, tl.LedgerRange,
		}
	}
	return w.bulkUpsertCopy(ctx, tx, "trustlines_snapshot_v1", columns,
		[]string{"account_id", "asset_code", "asset_issuer", "asset_type", "ledger_sequence"},
		[]string{
			"balance", "trust_limit", "buying_liabilities", "selling_liabilities",
			"authorized", "authorized_to_maintain_liabilities", "clawback_enabled",
		},
		rows,
	)
}

// insertAccountSigners inserts account signer snapshots into PostgreSQL
func (w *Writer) insertAccountSigners(ctx context.Context, tx pgx.Tx, signers []AccountSignerData) error {
	if len(signers) == 0 {
		return nil
	}

	columns := []string{
		"account_id", "signer", "ledger_sequence", "weight", "sponsor",
		"deleted", "closed_at", "ledger_range", "created_at",
	}
	rows := make([][]interface{}, len(signers))
	for i := range signers {
		s := &signers[i]
		rows[i] = []interface{}{
			s.AccountID, s.Signer, s.LedgerSequence, s.Weight, nullString(s.Sponsor),
			s.Deleted, s.ClosedAt, s.LedgerRange, s.CreatedAt,
		}
	}
	return w.bulkUpsertCopy(ctx, tx, "account_signers_snapshot_v1", columns,
		[]string{"account_id", "signer", "ledger_sequence"},
		[]string{"weight", "sponsor", "deleted", "closed_at"},
		rows,
	)
}

// nullString returns nil if s is empty, otherwise returns s
func nullString(s string) interface{} {
	if s == "" {
		return nil
	}
	return s
}

// insertClaimableBalances inserts claimable balance snapshots into PostgreSQL
func (w *Writer) insertClaimableBalances(ctx context.Context, tx pgx.Tx, balances []ClaimableBalanceData) error {
	if len(balances) == 0 {
		return nil
	}

	const query = `
		INSERT INTO claimable_balances_snapshot_v1 (
			balance_id, sponsor, ledger_sequence, closed_at,
			asset_type, asset_code, asset_issuer, amount,
			claimants_count, flags, created_at, ledger_range
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12
		)
		ON CONFLICT (balance_id, ledger_sequence) DO UPDATE SET
			sponsor = EXCLUDED.sponsor,
			asset_type = EXCLUDED.asset_type,
			asset_code = EXCLUDED.asset_code,
			asset_issuer = EXCLUDED.asset_issuer,
			amount = EXCLUDED.amount,
			claimants_count = EXCLUDED.claimants_count,
			flags = EXCLUDED.flags
	`

	rows := make([][]interface{}, len(balances))
	for i := range balances {
		b := &balances[i]
		rows[i] = []interface{}{
			b.BalanceID, b.Sponsor, b.LedgerSequence, b.ClosedAt,
			b.AssetType, b.AssetCode, b.AssetIssuer, b.Amount,
			b.ClaimantsCount, b.Flags, b.CreatedAt, b.LedgerRange,
		}
	}
	return batchExec(ctx, tx, query, rows)
}

// insertLiquidityPools inserts liquidity pool snapshots into PostgreSQL
func (w *Writer) insertLiquidityPools(ctx context.Context, tx pgx.Tx, pools []LiquidityPoolData) error {
	if len(pools) == 0 {
		return nil
	}

	const query = `
		INSERT INTO liquidity_pools_snapshot_v1 (
			liquidity_pool_id, ledger_sequence, closed_at,
			pool_type, fee, trustline_count, total_pool_shares,
			asset_a_type, asset_a_code, asset_a_issuer, asset_a_amount,
			asset_b_type, asset_b_code, asset_b_issuer, asset_b_amount,
			created_at, ledger_range
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17
		)
		ON CONFLICT (liquidity_pool_id, ledger_sequence) DO UPDATE SET
			pool_type = EXCLUDED.pool_type,
			fee = EXCLUDED.fee,
			trustline_count = EXCLUDED.trustline_count,
			total_pool_shares = EXCLUDED.total_pool_shares,
			asset_a_amount = EXCLUDED.asset_a_amount,
			asset_b_amount = EXCLUDED.asset_b_amount
	`

	rows := make([][]interface{}, len(pools))
	for i := range pools {
		p := &pools[i]
		rows[i] = []interface{}{
			p.LiquidityPoolID, p.LedgerSequence, p.ClosedAt,
			p.PoolType, p.Fee, p.TrustlineCount, p.TotalPoolShares,
			p.AssetAType, p.AssetACode, p.AssetAIssuer, p.AssetAAmount,
			p.AssetBType, p.AssetBCode, p.AssetBIssuer, p.AssetBAmount,
			p.CreatedAt, p.LedgerRange,
		}
	}
	return batchExec(ctx, tx, query, rows)
}

// insertConfigSettings inserts config settings data into the database
func (w *Writer) insertConfigSettings(ctx context.Context, tx pgx.Tx, settings []ConfigSettingData) error {
	if len(settings) == 0 {
		return nil
	}

	const query = `
		INSERT INTO config_settings_snapshot_v1 (
			config_setting_id, ledger_sequence, last_modified_ledger, deleted, closed_at,
			ledger_max_instructions, tx_max_instructions, fee_rate_per_instructions_increment, tx_memory_limit,
			ledger_max_read_ledger_entries, ledger_max_read_bytes, ledger_max_write_ledger_entries, ledger_max_write_bytes,
			tx_max_read_ledger_entries, tx_max_read_bytes, tx_max_write_ledger_entries, tx_max_write_bytes,
			contract_max_size_bytes, config_setting_xdr, created_at, ledger_range
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21
		)
		ON CONFLICT (config_setting_id, ledger_sequence) DO UPDATE SET
			last_modified_ledger = EXCLUDED.last_modified_ledger,
			deleted = EXCLUDED.deleted,
			ledger_max_instructions = EXCLUDED.ledger_max_instructions,
			tx_max_instructions = EXCLUDED.tx_max_instructions,
			fee_rate_per_instructions_increment = EXCLUDED.fee_rate_per_instructions_increment,
			tx_memory_limit = EXCLUDED.tx_memory_limit,
			ledger_max_read_ledger_entries = EXCLUDED.ledger_max_read_ledger_entries,
			ledger_max_read_bytes = EXCLUDED.ledger_max_read_bytes,
			ledger_max_write_ledger_entries = EXCLUDED.ledger_max_write_ledger_entries,
			ledger_max_write_bytes = EXCLUDED.ledger_max_write_bytes,
			tx_max_read_ledger_entries = EXCLUDED.tx_max_read_ledger_entries,
			tx_max_read_bytes = EXCLUDED.tx_max_read_bytes,
			tx_max_write_ledger_entries = EXCLUDED.tx_max_write_ledger_entries,
			tx_max_write_bytes = EXCLUDED.tx_max_write_bytes,
			contract_max_size_bytes = EXCLUDED.contract_max_size_bytes,
			config_setting_xdr = EXCLUDED.config_setting_xdr
	`

	rows := make([][]interface{}, len(settings))
	for i := range settings {
		s := &settings[i]
		rows[i] = []interface{}{
			s.ConfigSettingID, s.LedgerSequence, s.LastModifiedLedger, s.Deleted, s.ClosedAt,
			s.LedgerMaxInstructions, s.TxMaxInstructions, s.FeeRatePerInstructionsIncrement, s.TxMemoryLimit,
			s.LedgerMaxReadLedgerEntries, s.LedgerMaxReadBytes, s.LedgerMaxWriteLedgerEntries, s.LedgerMaxWriteBytes,
			s.TxMaxReadLedgerEntries, s.TxMaxReadBytes, s.TxMaxWriteLedgerEntries, s.TxMaxWriteBytes,
			s.ContractMaxSizeBytes, s.ConfigSettingXDR, s.CreatedAt, s.LedgerRange,
		}
	}
	return batchExec(ctx, tx, query, rows)
}

// insertTTL inserts TTL data into the database
func (w *Writer) insertTTL(ctx context.Context, tx pgx.Tx, ttls []TTLData) error {
	if len(ttls) == 0 {
		return nil
	}

	columns := []string{
		"key_hash", "ledger_sequence", "live_until_ledger_seq", "ttl_remaining", "expired",
		"last_modified_ledger", "deleted", "closed_at", "created_at", "ledger_range",
	}
	rows := make([][]interface{}, len(ttls))
	for i := range ttls {
		ttl := &ttls[i]
		rows[i] = []interface{}{
			ttl.KeyHash, ttl.LedgerSequence, ttl.LiveUntilLedgerSeq, ttl.TTLRemaining, ttl.Expired,
			ttl.LastModifiedLedger, ttl.Deleted, ttl.ClosedAt, ttl.CreatedAt, ttl.LedgerRange,
		}
	}
	return w.bulkUpsertCopy(ctx, tx, "ttl_snapshot_v1", columns,
		[]string{"key_hash", "ledger_sequence"},
		[]string{"live_until_ledger_seq", "ttl_remaining", "expired", "last_modified_ledger", "deleted"},
		rows,
	)
}

// insertEvictedKeys inserts evicted keys data into the database
func (w *Writer) insertEvictedKeys(ctx context.Context, tx pgx.Tx, keys []EvictedKeyData) error {
	if len(keys) == 0 {
		return nil
	}

	columns := []string{
		"key_hash", "ledger_sequence", "contract_id", "key_type", "durability",
		"closed_at", "ledger_range", "created_at",
	}
	rows := make([][]interface{}, len(keys))
	for i := range keys {
		k := &keys[i]
		rows[i] = []interface{}{
			k.KeyHash, k.LedgerSequence, k.ContractID, k.KeyType, k.Durability,
			k.ClosedAt, k.LedgerRange, k.CreatedAt,
		}
	}
	return w.bulkUpsertCopy(ctx, tx, "evicted_keys_state_v1", columns,
		[]string{"key_hash", "ledger_sequence"},
		[]string{"contract_id", "key_type", "durability"},
		rows,
	)
}

// insertContractEvents bulk-inserts contract events using COPY + upsert.
// For large batches (18k+ events), this is orders of magnitude faster than row-by-row.
func (w *Writer) insertContractEvents(ctx context.Context, tx pgx.Tx, events []ContractEventData) error {
	if len(events) == 0 {
		return nil
	}

	// Sanitize all events first
	for i := range events {
		sanitizeEventStrings(&events[i])
	}

	// Create temp table for bulk load
	_, err := tx.Exec(ctx, `
		CREATE TEMP TABLE _tmp_events (LIKE contract_events_stream_v1 INCLUDING DEFAULTS) ON COMMIT DROP
	`)
	if err != nil {
		return fmt.Errorf("failed to create temp table: %w", err)
	}

	// COPY into temp table (no constraints — ultra fast)
	columns := []string{
		"event_id", "contract_id", "ledger_sequence", "transaction_hash", "closed_at",
		"event_type", "in_successful_contract_call", "successful", "contract_event_xdr",
		"topics_json", "topics_decoded", "data_xdr", "data_decoded", "topic_count",
		"topic0_decoded", "topic1_decoded", "topic2_decoded", "topic3_decoded",
		"operation_index", "event_index", "created_at", "ledger_range",
	}

	rows := make([][]interface{}, len(events))
	for i, event := range events {
		rows[i] = []interface{}{
			event.EventID,
			event.ContractID,
			event.LedgerSequence,
			event.TransactionHash,
			event.ClosedAt,
			event.EventType,
			event.InSuccessfulContractCall,
			event.Successful,
			event.ContractEventXDR,
			event.TopicsJSON,
			event.TopicsDecoded,
			event.DataXDR,
			event.DataDecoded,
			event.TopicCount,
			event.Topic0Decoded,
			event.Topic1Decoded,
			event.Topic2Decoded,
			event.Topic3Decoded,
			event.OperationIndex,
			event.EventIndex,
			event.CreatedAt,
			event.LedgerRange,
		}
	}

	copyCount, err := tx.CopyFrom(ctx, pgx.Identifier{"_tmp_events"}, columns,
		pgx.CopyFromRows(rows))
	if err != nil {
		return fmt.Errorf("failed to COPY %d contract events: %w", len(events), err)
	}
	if int(copyCount) != len(events) {
		log.Printf("WARNING: COPY expected %d rows but inserted %d", len(events), copyCount)
	}

	// Upsert from temp into target
	_, err = tx.Exec(ctx, `
		INSERT INTO contract_events_stream_v1 (
			event_id, contract_id, ledger_sequence, transaction_hash, closed_at,
			event_type, in_successful_contract_call, successful, contract_event_xdr,
			topics_json, topics_decoded, data_xdr, data_decoded, topic_count,
			topic0_decoded, topic1_decoded, topic2_decoded, topic3_decoded,
			operation_index, event_index, created_at, ledger_range
		)
		SELECT DISTINCT ON (ledger_sequence, transaction_hash, event_index)
			event_id, contract_id, ledger_sequence, transaction_hash, closed_at,
			event_type, in_successful_contract_call, successful, contract_event_xdr,
			topics_json, topics_decoded, data_xdr, data_decoded, topic_count,
			topic0_decoded, topic1_decoded, topic2_decoded, topic3_decoded,
			operation_index, event_index, created_at, ledger_range
		FROM _tmp_events
		ON CONFLICT (ledger_sequence, transaction_hash, event_index) DO UPDATE SET
			contract_id = EXCLUDED.contract_id,
			event_type = EXCLUDED.event_type,
			in_successful_contract_call = EXCLUDED.in_successful_contract_call,
			successful = EXCLUDED.successful,
			contract_event_xdr = EXCLUDED.contract_event_xdr,
			topics_json = EXCLUDED.topics_json,
			topics_decoded = EXCLUDED.topics_decoded,
			data_xdr = EXCLUDED.data_xdr,
			data_decoded = EXCLUDED.data_decoded,
			topic_count = EXCLUDED.topic_count,
			topic0_decoded = EXCLUDED.topic0_decoded,
			topic1_decoded = EXCLUDED.topic1_decoded,
			topic2_decoded = EXCLUDED.topic2_decoded,
			topic3_decoded = EXCLUDED.topic3_decoded
	`)
	if err != nil {
		return fmt.Errorf("failed to upsert contract events from temp: %w", err)
	}

	return nil
}

// insertContractData inserts contract data snapshots (Phase 4 - Day 9)
func (w *Writer) insertContractData(ctx context.Context, tx pgx.Tx, contractDataList []ContractDataData) error {
	if len(contractDataList) == 0 {
		return nil
	}

	columns := []string{
		"contract_id", "ledger_sequence", "ledger_key_hash",
		"contract_key_type", "contract_durability",
		"asset_code", "asset_issuer", "asset_type",
		"balance_holder", "balance",
		"last_modified_ledger", "ledger_entry_change", "deleted", "closed_at",
		"contract_data_xdr", "created_at", "ledger_range",
		"token_name", "token_symbol", "token_decimals",
	}
	rows := make([][]interface{}, len(contractDataList))
	for i := range contractDataList {
		data := &contractDataList[i]
		// TokenName/TokenSymbol come from SEP-41 instance METADATA — pure
		// developer input, can contain anything. BalanceHolder is a decoded
		// scval address. Sanitize all defensively.
		data.TokenName = sanitizeStringPtr(data.TokenName)
		data.TokenSymbol = sanitizeStringPtr(data.TokenSymbol)
		data.BalanceHolder = sanitizeStringPtr(data.BalanceHolder)
		data.Balance = sanitizeStringPtr(data.Balance)
		data.AssetCode = sanitizeStringPtr(data.AssetCode)
		data.AssetIssuer = sanitizeStringPtr(data.AssetIssuer)
		rows[i] = []interface{}{
			data.ContractId, data.LedgerSequence, data.LedgerKeyHash,
			data.ContractKeyType, data.ContractDurability,
			data.AssetCode, data.AssetIssuer, data.AssetType,
			data.BalanceHolder, data.Balance,
			data.LastModifiedLedger, data.LedgerEntryChange, data.Deleted, data.ClosedAt,
			data.ContractDataXDR, data.CreatedAt, data.LedgerRange,
			data.TokenName, data.TokenSymbol, data.TokenDecimals,
		}
	}
	return w.bulkUpsertCopy(ctx, tx, "contract_data_snapshot_v1", columns,
		[]string{"contract_id", "ledger_key_hash", "ledger_sequence"},
		[]string{
			"contract_key_type", "contract_durability",
			"asset_code", "asset_issuer", "asset_type",
			"balance_holder", "balance",
			"last_modified_ledger", "ledger_entry_change", "deleted",
			"contract_data_xdr",
			"token_name", "token_symbol", "token_decimals",
		},
		rows,
	)
}

// insertContractCode inserts contract code snapshots with WASM metrics (Phase 4 - Day 10)
func (w *Writer) insertContractCode(ctx context.Context, tx pgx.Tx, contractCodeList []ContractCodeData) error {
	if len(contractCodeList) == 0 {
		return nil
	}

	const query = `
		INSERT INTO contract_code_snapshot_v1 (
			contract_code_hash, ledger_key_hash, contract_code_ext_v,
			last_modified_ledger, ledger_entry_change, deleted, closed_at,
			ledger_sequence,
			n_instructions, n_functions, n_globals, n_table_entries, n_types,
			n_data_segments, n_elem_segments, n_imports, n_exports, n_data_segment_bytes,
			created_at, ledger_range
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20
		)
		ON CONFLICT (contract_code_hash, ledger_sequence) DO UPDATE SET
			ledger_key_hash = EXCLUDED.ledger_key_hash,
			contract_code_ext_v = EXCLUDED.contract_code_ext_v,
			last_modified_ledger = EXCLUDED.last_modified_ledger,
			ledger_entry_change = EXCLUDED.ledger_entry_change,
			deleted = EXCLUDED.deleted,
			n_instructions = EXCLUDED.n_instructions,
			n_functions = EXCLUDED.n_functions,
			n_globals = EXCLUDED.n_globals,
			n_table_entries = EXCLUDED.n_table_entries,
			n_types = EXCLUDED.n_types,
			n_data_segments = EXCLUDED.n_data_segments,
			n_elem_segments = EXCLUDED.n_elem_segments,
			n_imports = EXCLUDED.n_imports,
			n_exports = EXCLUDED.n_exports,
			n_data_segment_bytes = EXCLUDED.n_data_segment_bytes
	`

	rows := make([][]interface{}, len(contractCodeList))
	for i := range contractCodeList {
		c := &contractCodeList[i]
		rows[i] = []interface{}{
			c.ContractCodeHash, c.LedgerKeyHash, c.ContractCodeExtV,
			c.LastModifiedLedger, c.LedgerEntryChange, c.Deleted, c.ClosedAt,
			c.LedgerSequence,
			c.NInstructions, c.NFunctions, c.NGlobals, c.NTableEntries, c.NTypes,
			c.NDataSegments, c.NElemSegments, c.NImports, c.NExports, c.NDataSegmentBytes,
			c.CreatedAt, c.LedgerRange,
		}
	}
	return batchExec(ctx, tx, query, rows)
}

// insertNativeBalances inserts XLM-only balances (Phase 5 - Day 11)
func (w *Writer) insertNativeBalances(ctx context.Context, tx pgx.Tx, nativeBalancesList []NativeBalanceData) error {
	if len(nativeBalancesList) == 0 {
		return nil
	}

	columns := []string{
		"account_id", "balance", "buying_liabilities", "selling_liabilities",
		"num_subentries", "num_sponsoring", "num_sponsored", "sequence_number",
		"last_modified_ledger", "ledger_sequence", "ledger_range",
	}
	rows := make([][]interface{}, len(nativeBalancesList))
	for i := range nativeBalancesList {
		nb := &nativeBalancesList[i]
		rows[i] = []interface{}{
			nb.AccountID, nb.Balance, nb.BuyingLiabilities, nb.SellingLiabilities,
			nb.NumSubentries, nb.NumSponsoring, nb.NumSponsored, nb.SequenceNumber,
			nb.LastModifiedLedger, nb.LedgerSequence, nb.LedgerRange,
		}
	}
	return w.bulkUpsertCopy(ctx, tx, "native_balances_snapshot_v1", columns,
		[]string{"account_id", "ledger_sequence"},
		[]string{
			"balance", "buying_liabilities", "selling_liabilities",
			"num_subentries", "num_sponsoring", "num_sponsored",
			"sequence_number", "last_modified_ledger",
		},
		rows,
	)
}

// insertRestoredKeys inserts restored storage keys (Phase 5 - Day 11)
func (w *Writer) insertRestoredKeys(ctx context.Context, tx pgx.Tx, restoredKeysList []RestoredKeyData) error {
	if len(restoredKeysList) == 0 {
		return nil
	}

	const query = `
		INSERT INTO restored_keys_state_v1 (
			key_hash, ledger_sequence,
			contract_id, key_type, durability, restored_from_ledger,
			closed_at, ledger_range, created_at
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7, $8, $9
		)
		ON CONFLICT (key_hash, ledger_sequence) DO UPDATE SET
			contract_id = EXCLUDED.contract_id,
			key_type = EXCLUDED.key_type,
			durability = EXCLUDED.durability,
			restored_from_ledger = EXCLUDED.restored_from_ledger
	`

	rows := make([][]interface{}, len(restoredKeysList))
	for i := range restoredKeysList {
		rk := &restoredKeysList[i]
		rows[i] = []interface{}{
			rk.KeyHash, rk.LedgerSequence,
			rk.ContractID, rk.KeyType, rk.Durability, rk.RestoredFromLedger,
			rk.ClosedAt, rk.LedgerRange, rk.CreatedAt,
		}
	}
	return batchExec(ctx, tx, query, rows)
}

// insertContractCreations inserts contract creation records into PostgreSQL (C11)
func (w *Writer) insertContractCreations(ctx context.Context, tx pgx.Tx, creations []ContractCreationData) error {
	if len(creations) == 0 {
		return nil
	}

	const query = `
		INSERT INTO contract_creations_v1 (
			contract_id, creator_address, wasm_hash,
			created_ledger, created_at, ledger_range
		) VALUES ($1, $2, $3, $4, $5, $6)
		ON CONFLICT (contract_id) DO NOTHING
	`

	rows := make([][]interface{}, len(creations))
	for i := range creations {
		c := &creations[i]
		rows[i] = []interface{}{
			c.ContractID, c.CreatorAddress, c.WasmHash,
			c.CreatedLedger, c.CreatedAt, c.LedgerRange,
		}
	}
	return batchExec(ctx, tx, query, rows)
}

// sanitizeUTF8 ensures a string is valid UTF-8 for PostgreSQL.
// Strips null bytes (0x00), invalid UTF-8 sequences (e.g. 0x95 from Windows-1252),
// and JSON \u0000 escape sequences.
func sanitizeUTF8(s string) string {
	s = strings.ToValidUTF8(s, "")
	s = strings.ReplaceAll(s, "\x00", "")
	s = strings.ReplaceAll(s, "\\u0000", "")
	return s
}

func sanitizeStringPtr(s *string) *string {
	if s == nil {
		return nil
	}
	cleaned := sanitizeUTF8(*s)
	return &cleaned
}

// sanitizeEventStrings removes null bytes from all text fields of a contract event
func sanitizeEventStrings(e *ContractEventData) {
	e.TopicsJSON = sanitizeUTF8(e.TopicsJSON)
	e.TopicsDecoded = sanitizeUTF8(e.TopicsDecoded)
	e.DataXDR = sanitizeUTF8(e.DataXDR)
	e.DataDecoded = sanitizeUTF8(e.DataDecoded)
	e.Topic0Decoded = sanitizeStringPtr(e.Topic0Decoded)
	e.Topic1Decoded = sanitizeStringPtr(e.Topic1Decoded)
	e.Topic2Decoded = sanitizeStringPtr(e.Topic2Decoded)
	e.Topic3Decoded = sanitizeStringPtr(e.Topic3Decoded)
}

// insertTokenTransfers bulk-inserts token transfers using pgx Batch
func (w *Writer) insertTokenTransfers(ctx context.Context, tx pgx.Tx, transfers []TokenTransferData) error {
	if len(transfers) == 0 {
		return nil
	}

	// token_transfers_stream_v1 has no unique or exclusion constraint
	// (see migrations/004_add_toid_and_token_transfers.sql) — the original
	// ON CONFLICT DO NOTHING was a no-op arbiterless clause. We therefore
	// COPY directly into the target and skip the temp-table + INSERT SELECT
	// dance altogether.
	columns := []string{
		"ledger_sequence", "transaction_hash", "transaction_id", "operation_id",
		"operation_index", "event_type", "from", "to", "asset", "asset_type",
		"asset_code", "asset_issuer", "amount", "amount_raw", "contract_id",
		"closed_at", "created_at", "ledger_range",
	}
	rows := make([][]interface{}, len(transfers))
	for i := range transfers {
		t := &transfers[i]
		// AmountRaw and Asset come straight from SDK decoding — strip NULs /
		// invalid UTF-8. Others are defensive.
		t.EventType = sanitizeUTF8(t.EventType)
		t.Asset = sanitizeUTF8(t.Asset)
		t.AssetType = sanitizeUTF8(t.AssetType)
		t.AmountRaw = sanitizeUTF8(t.AmountRaw)
		t.From = sanitizeStringPtr(t.From)
		t.To = sanitizeStringPtr(t.To)
		t.AssetCode = sanitizeStringPtr(t.AssetCode)
		t.AssetIssuer = sanitizeStringPtr(t.AssetIssuer)
		rows[i] = []interface{}{
			t.LedgerSequence, t.TransactionHash, t.TransactionID, t.OperationID,
			t.OperationIndex, t.EventType, t.From, t.To, t.Asset, t.AssetType,
			t.AssetCode, t.AssetIssuer, t.Amount, t.AmountRaw, t.ContractID,
			t.ClosedAt, t.CreatedAt, t.LedgerRange,
		}
	}
	return w.bulkInsertCopy(ctx, tx, "token_transfers_stream_v1", columns, rows)
}
