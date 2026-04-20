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
)

// RealtimeTransformer handles real-time bronze → silver transformation
type RealtimeTransformer struct {
	config        *Config
	sourceManager *SourceManager
	silverWriter  *SilverWriter
	checkpoint    *CheckpointManager
	silverDB      *sql.DB
	sourceServer  *SilverSourceServer
	stopChan      chan struct{}

	// Stats
	mu                         sync.RWMutex
	transformationsTotal       int64
	transformationErrors       int64
	lastLedgerSequence         int64
	lastTransformationTime     time.Time
	lastTransformationDuration time.Duration
	lastTransformationRowCount int64

	// Gap detection
	consecutiveEmptyPolls int
}

// NewRealtimeTransformer creates a new realtime transformer
func NewRealtimeTransformer(config *Config, sourceManager *SourceManager, silverWriter *SilverWriter, checkpoint *CheckpointManager, silverDB *sql.DB) *RealtimeTransformer {
	return &RealtimeTransformer{
		config:        config,
		sourceManager: sourceManager,
		silverWriter:  silverWriter,
		checkpoint:    checkpoint,
		silverDB:      silverDB,
		stopChan:      make(chan struct{}),
	}
}

func (rt *RealtimeTransformer) SetSourceServer(sourceServer *SilverSourceServer) {
	rt.sourceServer = sourceServer
}

// Start begins the real-time transformation loop.
// It dispatches to either gRPC streaming or polling mode based on config.
func (rt *RealtimeTransformer) Start() error {
	log.Println("Starting Real-Time Silver Transformer")

	// Load initial checkpoint
	lastLedger, err := rt.checkpoint.Load()
	if err != nil {
		return fmt.Errorf("failed to load checkpoint: %w", err)
	}

	rt.mu.Lock()
	rt.lastLedgerSequence = lastLedger
	rt.mu.Unlock()

	if lastLedger == 0 {
		log.Println("No checkpoint found, starting from beginning")
	} else {
		log.Printf("Resuming from ledger sequence: %d", lastLedger)
	}

	// One-time migration: convert hex contract IDs to C-encoded strkey
	rt.migrateHexContractIDs()

	// One-time migration: parse SEP-41 Soroban events into token_transfers_raw
	rt.migrateSorobanTransfers()

	// Run immediate transformation check
	log.Println("Running initial transformation check...")
	if err := rt.runTransformationCycle(); err != nil {
		log.Printf("Initial transformation error: %v", err)
		rt.incrementErrors()
	}

	// Dispatch to the configured source mode
	if rt.config.BronzeSource.Mode == "grpc" {
		return rt.startGRPC()
	}
	return rt.startPolling()
}

// startPolling runs the original ticker-based polling loop
func (rt *RealtimeTransformer) startPolling() error {
	log.Printf("Transformer ready - polling for new data (interval: %v)...", rt.config.PollInterval())

	ticker := time.NewTicker(rt.config.PollInterval())
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if err := rt.runTransformationCycle(); err != nil {
				log.Printf("Transformation error: %v", err)
				rt.incrementErrors()
			}
		case <-rt.stopChan:
			log.Println("Stopping transformer...")
			return nil
		}
	}
}

// startGRPC connects to the bronze ingester's flowctl SourceService and triggers
// transformation cycles on each received ledger-committed event.
func (rt *RealtimeTransformer) startGRPC() error {
	endpoint := rt.config.BronzeSource.Endpoint
	log.Printf("Transformer ready - gRPC streaming from %s", endpoint)

	client, err := NewBronzeStreamClient(endpoint)
	if err != nil {
		return fmt.Errorf("failed to create bronze stream client: %w", err)
	}
	defer client.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Stop on shutdown signal
	go func() {
		<-rt.stopChan
		cancel()
	}()

	startLedger := rt.getLastLedger()
	eventCh := client.StreamLedgerEvents(ctx, startLedger)

	// Track whether a cycle is running to coalesce batches
	var pendingEnd int64
	var cycleRunning bool
	cycleDone := make(chan struct{}, 1)

	for {
		select {
		case event, ok := <-eventCh:
			if !ok {
				// Channel closed — stream ended
				log.Println("Stopping transformer (stream closed)...")
				return nil
			}

			endLedger := int64(event.EndLedger)

			if cycleRunning {
				// Coalesce: remember the latest end ledger for the next cycle
				if endLedger > pendingEnd {
					pendingEnd = endLedger
				}
				continue
			}

			// Run transformation cycle
			cycleRunning = true
			pendingEnd = 0
			go func() {
				if err := rt.runTransformationCycle(); err != nil {
					log.Printf("Transformation error: %v", err)
					rt.incrementErrors()
				}
				select {
				case cycleDone <- struct{}{}:
				case <-ctx.Done():
				}
			}()

		case <-cycleDone:
			cycleRunning = false
			// If events arrived while we were processing, run another cycle immediately
			if pendingEnd > 0 {
				pendingEnd = 0
				cycleRunning = true
				go func() {
					if err := rt.runTransformationCycle(); err != nil {
						log.Printf("Transformation error: %v", err)
						rt.incrementErrors()
					}
					select {
					case cycleDone <- struct{}{}:
					case <-ctx.Done():
					}
				}()
			}

		case <-ctx.Done():
			log.Println("Stopping transformer...")
			return nil
		}
	}
}

// Stop gracefully stops the transformer
func (rt *RealtimeTransformer) Stop() {
	close(rt.stopChan)
}

// RunColdReplay reads from bronze cold DuckLake and writes to silver hot PostgreSQL.
// This replays historical data through all 32 transform jobs without any code duplication.
// The existing runTransformationCycle() handles all transforms — we just force the
// SourceManager to read from cold instead of hot.
func (rt *RealtimeTransformer) RunColdReplay(ctx context.Context, startLedger, endLedger, batchSize int64) error {
	log.Printf("🔄 COLD REPLAY: Starting replay of ledgers %d → %d (batch size: %d)", startLedger, endLedger, batchSize)

	// Position checkpoint just before the start (both DB and in-memory)
	if err := rt.checkpoint.Reset(startLedger - 1); err != nil {
		return fmt.Errorf("failed to reset checkpoint for replay: %w", err)
	}
	rt.mu.Lock()
	rt.lastLedgerSequence = startLedger - 1
	rt.mu.Unlock()

	// Force SourceManager into backfill mode reading from cold
	rt.sourceManager.ForceBackfillMode(endLedger)

	// Override batch size for replay
	origBatchSize := rt.config.Performance.BatchSize
	rt.config.Performance.BatchSize = int(batchSize)
	defer func() { rt.config.Performance.BatchSize = origBatchSize }()

	totalStart := time.Now()
	var totalRows int64
	cycleCount := 0

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// Check current position
		currentLedger, err := rt.checkpoint.Load()
		if err != nil {
			return fmt.Errorf("failed to load checkpoint: %w", err)
		}

		if currentLedger >= endLedger {
			elapsed := time.Since(totalStart)
			log.Printf("✅ COLD REPLAY COMPLETE: %d ledgers processed, %d total rows, %s elapsed",
				endLedger-startLedger+1, totalRows, elapsed.Round(time.Second))
			return nil
		}

		// Run one transformation cycle (reuses all 32 transform jobs)
		cycleStart := time.Now()
		err = rt.runTransformationCycle()
		cycleDuration := time.Since(cycleStart)

		if err != nil {
			log.Printf("⚠️  Replay cycle error at ledger %d: %v", currentLedger, err)
			// Don't abort on individual cycle errors — log and continue
			continue
		}

		// Track progress
		newLedger, _ := rt.checkpoint.Load()
		cycleRows := rt.lastTransformationRowCount
		totalRows += cycleRows
		cycleCount++

		// Re-force backfill mode in case CheckAndSwitchMode tried to switch
		rt.sourceManager.ForceBackfillMode(endLedger)

		// Progress logging every 10 cycles
		if cycleCount%10 == 0 {
			elapsed := time.Since(totalStart)
			ledgersProcessed := newLedger - startLedger + 1
			ledgersRemaining := endLedger - newLedger
			ledgersPerSec := float64(ledgersProcessed) / elapsed.Seconds()
			var etaStr string
			if ledgersPerSec > 0 {
				etaSeconds := float64(ledgersRemaining) / ledgersPerSec
				etaStr = time.Duration(etaSeconds * float64(time.Second)).Round(time.Second).String()
			} else {
				etaStr = "unknown"
			}

			pct := float64(ledgersProcessed) / float64(endLedger-startLedger+1) * 100
			log.Printf("🔄 REPLAY PROGRESS: ledger %d / %d (%.1f%%) | %d rows | %.0f ledgers/sec | cycle %s | ETA %s",
				newLedger, endLedger, pct, totalRows, ledgersPerSec, cycleDuration.Round(time.Millisecond), etaStr)
		}
	}
}

// migrateHexContractIDs converts existing 64-char hex contract IDs to 56-char C-encoded strkey
// in token_transfers_raw, evicted_keys, and restored_keys tables. Runs once on startup.
func (rt *RealtimeTransformer) migrateHexContractIDs() {
	ctx := context.Background()

	tables := []struct {
		name   string
		column string
	}{
		{"token_transfers_raw", "token_contract_id"},
		{"evicted_keys", "contract_id"},
		{"restored_keys", "contract_id"},
	}

	for _, t := range tables {
		query := fmt.Sprintf(
			"SELECT DISTINCT %s FROM %s WHERE %s ~ '^[0-9a-fA-F]{64}$'",
			t.column, t.name, t.column,
		)
		rows, err := rt.silverDB.QueryContext(ctx, query)
		if err != nil {
			log.Printf("⚠️  Migration: failed to query %s for hex IDs: %v", t.name, err)
			continue
		}

		var converted int
		for rows.Next() {
			var hexID string
			if err := rows.Scan(&hexID); err != nil {
				continue
			}
			encoded, err := hexToStrKey(hexID)
			if err != nil {
				log.Printf("⚠️  Migration: failed to encode %s: %v", hexID, err)
				continue
			}
			updateQuery := fmt.Sprintf("UPDATE %s SET %s = $1 WHERE %s = $2", t.name, t.column, t.column)
			if _, err := rt.silverDB.ExecContext(ctx, updateQuery, encoded, hexID); err != nil {
				log.Printf("⚠️  Migration: failed to update %s in %s: %v", hexID, t.name, err)
				continue
			}
			converted++
		}
		rows.Close()

		if converted > 0 {
			log.Printf("✅ Migration: converted %d hex contract IDs to strkey in %s", converted, t.name)
		} else {
			log.Printf("ℹ️  Migration: no hex contract IDs found in %s", t.name)
		}
	}
}

// migrateSorobanTransfers performs a one-time migration to:
// 1. Change amount column from BIGINT to NUMERIC (i128 safety)
// 2. Add event_index column
// 3. Update unique index to include event_index
// 4. Backfill soroban rows with parsed SEP-41 event data from bronze
func (rt *RealtimeTransformer) migrateSorobanTransfers() {
	ctx := context.Background()

	// Bumping this constant forces the migration to rerun once on next startup.
	// History:
	//   v1: initial backfill with NULL-sentinel stale check
	//   v2: 2026-04-16 — drop INNER JOIN on classic bronze (transactions_row_v2,
	//       ledgers_row_v2) in Soroban branch of QueryTokenTransfers. Classic
	//       bronze retention is ~25 min vs ~41 days for contract_events, so the
	//       join silently dropped every Soroban transfer older than the classic
	//       window — leaving C... address balances empty.
	//   v3: 2026-04-16 — source backfill bounds from contract_events_stream_v1
	//       instead of ledgers_row_v2. v2 backfill was silently capped at the
	//       ~300-ledger classic retention tail because GetMin/MaxLedgerSequence
	//       read from ledgers_row_v2; v3 uses GetMin/MaxEventLedger.
	//   v4: 2026-04-16 — use e.successful instead of e.in_successful_contract_call
	//       as the transaction_successful column. The latter is sub-call scope
	//       and is ~99.7% false on transfer events in prod, which caused the
	//       balance aggregation filter (WHERE transaction_successful=true) to
	//       drop every backfilled transfer and leave C... balances empty.
	//   v5: 2026-04-16 — add Step 9 (state-based balance rebuild from
	//       contract_data_snapshot_v1.balance_holder). Required so historical
	//       SAC balances (including newly-decoded native XLM entries from the
	//       ingester fix) land in address_balances_current without waiting
	//       for new on-chain activity per wallet.
	//   v6: 2026-04-19 — stop treating null/null/null Soroban rows as a reason
	//       to rebuild all history on restart. Preserve unsupported token-like
	//       contract events in contract_events_unmatched and only write
	//       validated token semantics to token_transfers_raw.
	const currentMigrationVersion = 6

	// Ensure migration-tracking table exists.
	if _, err := rt.silverDB.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS silver_migrations (
			name TEXT PRIMARY KEY,
			version INTEGER NOT NULL,
			applied_at TIMESTAMP NOT NULL DEFAULT NOW()
		)
	`); err != nil {
		log.Printf("⚠️  Soroban migration: failed to ensure silver_migrations table: %v", err)
		return
	}

	// Check if migration is needed: event_index column must exist.
	var colExists bool
	err := rt.silverDB.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM information_schema.columns
			WHERE table_name = 'token_transfers_raw' AND column_name = 'event_index'
		)
	`).Scan(&colExists)
	if err != nil {
		log.Printf("⚠️  Soroban migration: failed to check column existence: %v", err)
		return
	}

	// Check recorded migration version.
	var appliedVersion int
	if err := rt.silverDB.QueryRowContext(ctx, `
		SELECT COALESCE(MAX(version), 0) FROM silver_migrations WHERE name = 'soroban_transfers'
	`).Scan(&appliedVersion); err != nil {
		log.Printf("⚠️  Soroban migration: failed to read migration version: %v", err)
		return
	}

	if colExists && appliedVersion >= currentMigrationVersion {
		var staleCount int64
		err := rt.silverDB.QueryRowContext(ctx, `
			SELECT COUNT(*) FROM token_transfers_raw
			WHERE source_type = 'soroban' AND from_account IS NULL AND to_account IS NULL AND amount IS NULL
		`).Scan(&staleCount)
		if err != nil {
			log.Printf("⚠️  Soroban migration: failed to check stale rows: %v", err)
			return
		}
		if staleCount > 0 {
			log.Printf("⚠️  Soroban migration: found %d legacy null Soroban rows, but will not trigger a full rebuild at v%d", staleCount, appliedVersion)
		}
		log.Printf("ℹ️  Soroban migration: already complete at v%d, skipping", appliedVersion)
		return
	} else if colExists && appliedVersion < currentMigrationVersion {
		log.Printf("🔄 Soroban migration: applied=v%d, current=v%d — re-running backfill with fixed query", appliedVersion, currentMigrationVersion)
	} else {
		log.Println("🔄 Soroban migration: starting schema changes...")
	}

	// Step 1: ALTER amount column to NUMERIC
	if _, err := rt.silverDB.ExecContext(ctx, `
		ALTER TABLE token_transfers_raw ALTER COLUMN amount TYPE NUMERIC USING amount::NUMERIC
	`); err != nil {
		log.Printf("⚠️  Soroban migration: failed to alter amount type (may already be NUMERIC): %v", err)
	}

	// Step 2: Add event_index column
	if _, err := rt.silverDB.ExecContext(ctx, `
		ALTER TABLE token_transfers_raw ADD COLUMN IF NOT EXISTS event_index INTEGER
	`); err != nil {
		log.Printf("⚠️  Soroban migration: failed to add event_index column: %v", err)
		return
	}

	// Step 3: Drop old unique index and create new one with event_index
	if _, err := rt.silverDB.ExecContext(ctx, `
		DROP INDEX IF EXISTS idx_token_transfers_unique
	`); err != nil {
		log.Printf("⚠️  Soroban migration: failed to drop old index: %v", err)
	}
	if _, err := rt.silverDB.ExecContext(ctx, `
		CREATE UNIQUE INDEX IF NOT EXISTS idx_token_transfers_unique
		ON token_transfers_raw(
			transaction_hash,
			ledger_sequence,
			source_type,
			COALESCE(from_account, ''),
			COALESCE(token_contract_id, ''),
			COALESCE(event_index, -1)
		)
	`); err != nil {
		log.Printf("⚠️  Soroban migration: failed to create new unique index: %v", err)
		return
	}

	// Step 4: Delete all soroban rows so they can be re-parsed with proper SEP-41 extraction
	result, err := rt.silverDB.ExecContext(ctx, `
		DELETE FROM token_transfers_raw WHERE source_type = 'soroban'
	`)
	if err != nil {
		log.Printf("⚠️  Soroban migration: failed to delete stale soroban rows: %v", err)
		return
	}
	deletedTransfers, _ := result.RowsAffected()

	// Step 5: Delete stale semantic flows for soroban tokens
	result, err = rt.silverDB.ExecContext(ctx, `
		DELETE FROM semantic_flows_value WHERE asset_type = 'soroban_token'
	`)
	if err != nil {
		log.Printf("⚠️  Soroban migration: failed to delete stale semantic flows: %v", err)
	}
	deletedFlows, _ := result.RowsAffected()

	// Step 5b: Delete preserved unmatched token-like events so the backfill can
	// repopulate them deterministically with the current parser rules.
	result, err = rt.silverDB.ExecContext(ctx, `
		DELETE FROM contract_events_unmatched
		WHERE event_name IN ('transfer', 'mint', 'burn', 'clawback')
	`)
	if err != nil {
		log.Printf("⚠️  Soroban migration: failed to delete unmatched token-like contract events: %v", err)
	}
	deletedUnmatched, _ := result.RowsAffected()

	log.Printf("🗑️  Soroban migration: deleted %d stale transfers, %d stale semantic flows, %d preserved unmatched token-like events", deletedTransfers, deletedFlows, deletedUnmatched)

	// Step 6: Backfill from bronze hot using JSON extraction
	// Get the current ledger range in bronze hot
	bronzeHotReader := rt.sourceManager.hotReader
	if bronzeHotReader == nil {
		log.Println("⚠️  Soroban migration: no bronze hot reader available, skipping backfill")
		return
	}

	// Source the backfill bounds from contract_events_stream_v1 (not ledgers_row_v2).
	// Classic bronze tables have ~25-min retention while events retain ~41 days;
	// using the classic bounds would silently limit backfill to a thin tail and
	// defeat the purpose of this migration.
	minLedger, err := bronzeHotReader.GetMinEventLedger(ctx)
	if err != nil || minLedger == 0 {
		log.Printf("⚠️  Soroban migration: failed to get min event ledger from bronze hot: %v", err)
		return
	}
	maxLedger, err := bronzeHotReader.GetMaxEventLedger(ctx)
	if err != nil || maxLedger == 0 {
		log.Printf("⚠️  Soroban migration: failed to get max event ledger from bronze hot: %v", err)
		return
	}

	log.Printf("🔄 Soroban migration: backfilling from bronze hot contract_events ledgers %d to %d", minLedger, maxLedger)

	// Direct INSERT-SELECT from bronze hot into silver hot
	backfillQuery := `
		INSERT INTO token_transfers_raw (
			timestamp, transaction_hash, ledger_sequence, source_type,
			from_account, to_account, asset_code, asset_issuer, amount,
			token_contract_id, operation_type, transaction_successful, event_index
		)
		SELECT
			l.closed_at AS timestamp,
			e.transaction_hash,
			e.ledger_sequence,
			'soroban' AS source_type,
			CASE
				WHEN replace(topics_decoded, '\u0000', '')::jsonb->>0 = 'transfer' THEN replace(topics_decoded, '\u0000', '')::jsonb->1->>'address'
				WHEN replace(topics_decoded, '\u0000', '')::jsonb->>0 = 'burn' THEN replace(topics_decoded, '\u0000', '')::jsonb->1->>'address'
				WHEN replace(topics_decoded, '\u0000', '')::jsonb->>0 = 'clawback' THEN replace(topics_decoded, '\u0000', '')::jsonb->1->>'address'
			END AS from_account,
			CASE
				WHEN replace(topics_decoded, '\u0000', '')::jsonb->>0 = 'transfer' THEN replace(topics_decoded, '\u0000', '')::jsonb->2->>'address'
				WHEN replace(topics_decoded, '\u0000', '')::jsonb->>0 = 'mint' AND jsonb_typeof(replace(topics_decoded, '\u0000', '')::jsonb->2) = 'object'
					THEN replace(topics_decoded, '\u0000', '')::jsonb->2->>'address'
				WHEN replace(topics_decoded, '\u0000', '')::jsonb->>0 = 'mint' AND jsonb_typeof(replace(topics_decoded, '\u0000', '')::jsonb->1) = 'object'
					AND (replace(topics_decoded, '\u0000', '')::jsonb->1->>'type') = 'account'
					THEN replace(topics_decoded, '\u0000', '')::jsonb->1->>'address'
			END AS to_account,
			NULL AS asset_code,
			NULL AS asset_issuer,
			COALESCE(
				replace(data_decoded, '\u0000', '')::jsonb->>'value',
				replace(data_decoded, '\u0000', '')::jsonb->'entries'->'amount'->>'value'
			)::NUMERIC AS amount,
			e.contract_id AS token_contract_id,
			24 AS operation_type,
			t.successful AS transaction_successful,
			e.event_index
		FROM dblink('bronze_hot',
			'SELECT transaction_hash, ledger_sequence, contract_id, topics_decoded, data_decoded, event_type, topic_count, event_index
			 FROM contract_events_stream_v1
			 WHERE ledger_sequence BETWEEN ' || $1 || ' AND ' || $2
		) AS e(transaction_hash TEXT, ledger_sequence BIGINT, contract_id TEXT, topics_decoded TEXT, data_decoded TEXT, event_type TEXT, topic_count INT, event_index INT)
		INNER JOIN dblink('bronze_hot',
			'SELECT transaction_hash, ledger_sequence, successful
			 FROM transactions_row_v2
			 WHERE ledger_sequence BETWEEN ' || $1 || ' AND ' || $2
		) AS t(transaction_hash TEXT, ledger_sequence BIGINT, successful BOOLEAN)
			ON e.transaction_hash = t.transaction_hash
			AND e.ledger_sequence = t.ledger_sequence
		INNER JOIN dblink('bronze_hot',
			'SELECT sequence, closed_at
			 FROM ledgers_row_v2
			 WHERE sequence BETWEEN ' || $1 || ' AND ' || $2
		) AS l(sequence BIGINT, closed_at TIMESTAMP)
			ON e.ledger_sequence = l.sequence
		WHERE e.event_type = 'contract'
		  AND e.topic_count >= 2
		  AND replace(topics_decoded, '\u0000', '')::jsonb->>0 IN ('transfer', 'mint', 'burn', 'clawback')
		ON CONFLICT DO NOTHING
	`

	// The backfill query uses dblink which requires the bronze_hot connection.
	// Since we have direct access to bronze hot DB, use a simpler cross-DB approach:
	// Query from bronze hot, insert into silver hot in batches.
	_ = backfillQuery // Not using dblink approach

	// Use direct query approach: read from bronze hot, write to silver hot
	const batchSize int64 = 5000
	var totalInserted int64
	var totalUnmatched int64

	for batchStart := minLedger; batchStart <= maxLedger; batchStart += batchSize {
		batchEnd := batchStart + batchSize - 1
		if batchEnd > maxLedger {
			batchEnd = maxLedger
		}

		rows, err := bronzeHotReader.QueryTokenTransfers(ctx, batchStart, batchEnd)
		if err != nil {
			log.Printf("⚠️  Soroban migration: failed to query bronze hot batch %d-%d: %v", batchStart, batchEnd, err)
			continue
		}

		silverTx, err := rt.silverDB.BeginTx(ctx, nil)
		if err != nil {
			rows.Close()
			log.Printf("⚠️  Soroban migration: failed to begin tx for batch %d-%d: %v", batchStart, batchEnd, err)
			continue
		}

		batchCount := int64(0)
		batchFailed := false
		for rows.Next() {
			row := &TokenTransferRow{}
			if err := rows.Scan(
				&row.Timestamp, &row.TransactionHash, &row.LedgerSequence, &row.SourceType,
				&row.FromAccount, &row.ToAccount, &row.AssetCode, &row.AssetIssuer, &row.Amount,
				&row.TokenContractID, &row.OperationType, &row.TransactionSuccessful,
				&row.EventIndex,
			); err != nil {
				log.Printf("⚠️  Soroban migration: scan error in batch %d-%d: %v", batchStart, batchEnd, err)
				batchFailed = true
				break
			}

			// Only backfill validated soroban semantic rows.
			if row.SourceType != "soroban" || !isValidTokenTransferSemantic(row) {
				continue
			}

			if err := rt.silverWriter.WriteTokenTransfer(ctx, silverTx, row); err != nil {
				log.Printf("⚠️  Soroban migration: write error in batch %d-%d: %v", batchStart, batchEnd, err)
				batchFailed = true
				break
			}
			batchCount++
		}
		rows.Close()

		if err := rows.Err(); err != nil {
			log.Printf("⚠️  Soroban migration: rows iteration error in batch %d-%d: %v", batchStart, batchEnd, err)
			batchFailed = true
		}

		if batchFailed {
			if rbErr := silverTx.Rollback(); rbErr != nil {
				log.Printf("⚠️  Soroban migration: rollback error for batch %d-%d: %v", batchStart, batchEnd, rbErr)
			}
			continue
		}

		if err := silverTx.Commit(); err != nil {
			log.Printf("⚠️  Soroban migration: commit error for batch %d-%d: %v", batchStart, batchEnd, err)
			continue
		}
		totalInserted += batchCount

		unmatchedRows, err := bronzeHotReader.QueryUnmatchedTokenLikeContractEvents(ctx, batchStart, batchEnd)
		if err != nil {
			log.Printf("⚠️  Soroban migration: failed to query unmatched token-like events for batch %d-%d: %v", batchStart, batchEnd, err)
			continue
		}

		unmatchedTx, err := rt.silverDB.BeginTx(ctx, nil)
		if err != nil {
			unmatchedRows.Close()
			log.Printf("⚠️  Soroban migration: failed to begin tx for unmatched batch %d-%d: %v", batchStart, batchEnd, err)
			continue
		}

		unmatchedCount := int64(0)
		unmatchedFailed := false
		for unmatchedRows.Next() {
			row := &UnmatchedContractEventRow{}
			if err := unmatchedRows.Scan(
				&row.Timestamp, &row.TransactionHash, &row.LedgerSequence, &row.ContractID,
				&row.EventIndex, &row.EventName, &row.TopicsDecoded, &row.DataDecoded,
				&row.Successful, &row.ParseReason,
			); err != nil {
				log.Printf("⚠️  Soroban migration: unmatched scan error in batch %d-%d: %v", batchStart, batchEnd, err)
				unmatchedFailed = true
				break
			}
			if err := rt.silverWriter.WriteUnmatchedContractEvent(ctx, unmatchedTx, row); err != nil {
				log.Printf("⚠️  Soroban migration: unmatched write error in batch %d-%d: %v", batchStart, batchEnd, err)
				unmatchedFailed = true
				break
			}
			unmatchedCount++
		}
		unmatchedRows.Close()
		if err := unmatchedRows.Err(); err != nil {
			log.Printf("⚠️  Soroban migration: unmatched rows iteration error in batch %d-%d: %v", batchStart, batchEnd, err)
			unmatchedFailed = true
		}
		if unmatchedFailed {
			if rbErr := unmatchedTx.Rollback(); rbErr != nil {
				log.Printf("⚠️  Soroban migration: unmatched rollback error for batch %d-%d: %v", batchStart, batchEnd, rbErr)
			}
			continue
		}
		if err := unmatchedTx.Commit(); err != nil {
			log.Printf("⚠️  Soroban migration: unmatched commit error for batch %d-%d: %v", batchStart, batchEnd, err)
			continue
		}
		totalUnmatched += unmatchedCount
	}

	log.Printf("✅ Soroban migration: backfilled %d SEP-41 token transfer events from bronze hot and preserved %d unmatched token-like contract events", totalInserted, totalUnmatched)

	// Step 7: Rebuild semantic_flows_value for the backfilled ledger range
	if totalInserted > 0 {
		log.Printf("🔄 Soroban migration: rebuilding semantic flows for ledgers %d to %d", minLedger, maxLedger)
		flowTx, err := rt.silverDB.BeginTx(ctx, nil)
		if err != nil {
			log.Printf("⚠️  Soroban migration: failed to begin tx for semantic flows rebuild: %v", err)
			return
		}
		flowCount, err := rt.transformSemanticFlows(ctx, flowTx, minLedger, maxLedger)
		if err != nil {
			log.Printf("⚠️  Soroban migration: failed to rebuild semantic flows: %v", err)
			flowTx.Rollback()
			return
		}
		if err := flowTx.Commit(); err != nil {
			log.Printf("⚠️  Soroban migration: failed to commit semantic flows rebuild: %v", err)
			return
		}
		log.Printf("✅ Soroban migration: rebuilt %d semantic flow rows", flowCount)

		// Step 8: Rebuild address_balances_current for the backfilled range.
		// This can be extremely expensive on large datasets, so by default we
		// skip it during startup migration and leave it for an explicit
		// maintenance/backfill workflow.
		if rt.config.SorobanMigration.RebuildAddressBalancesOnStartup {
			log.Printf("🔄 Soroban migration: rebuilding address balances for ledgers %d to %d", minLedger, maxLedger)
			balTx, err := rt.silverDB.BeginTx(ctx, nil)
			if err != nil {
				log.Printf("⚠️  Soroban migration: failed to begin tx for address balances rebuild: %v", err)
				return
			}
			balCount, err := rt.transformAddressBalancesCurrent(ctx, balTx, minLedger, maxLedger)
			if err != nil {
				log.Printf("⚠️  Soroban migration: failed to rebuild address balances: %v", err)
				balTx.Rollback()
				return
			}
			if err := balTx.Commit(); err != nil {
				log.Printf("⚠️  Soroban migration: failed to commit address balances rebuild: %v", err)
				return
			}
			log.Printf("✅ Soroban migration: rebuilt %d address balance rows", balCount)
		} else {
			log.Printf("⏭️  Soroban migration: skipping startup address balance rebuild for ledgers %d to %d (disabled by config)", minLedger, maxLedger)
		}

		// Step 9: Rebuild state-based address balances from
		// contract_data_snapshot_v1. This is also disabled by default on
		// startup to avoid long, opaque migration runtimes in production.
		if rt.config.SorobanMigration.RebuildStateBalancesOnStartup {
			log.Printf("🔄 Soroban migration: rebuilding state-based address balances for ledgers %d to %d", minLedger, maxLedger)
			stateTx, err := rt.silverDB.BeginTx(ctx, nil)
			if err != nil {
				log.Printf("⚠️  Soroban migration: failed to begin tx for state balances rebuild: %v", err)
				return
			}
			stateCount, err := rt.transformAddressBalancesFromContractState(ctx, stateTx, minLedger, maxLedger)
			if err != nil {
				log.Printf("⚠️  Soroban migration: failed to rebuild state balances: %v", err)
				stateTx.Rollback()
				return
			}
			if err := stateTx.Commit(); err != nil {
				log.Printf("⚠️  Soroban migration: failed to commit state balances rebuild: %v", err)
				return
			}
			log.Printf("✅ Soroban migration: upserted %d state-based balance rows", stateCount)
		} else {
			log.Printf("⏭️  Soroban migration: skipping startup state-based balance rebuild for ledgers %d to %d (disabled by config)", minLedger, maxLedger)
		}
	}

	// Record completion so we don't rerun on next startup unless the version bumps.
	if _, err := rt.silverDB.ExecContext(ctx, `
		INSERT INTO silver_migrations (name, version, applied_at)
		VALUES ('soroban_transfers', $1, NOW())
		ON CONFLICT (name) DO UPDATE SET
			version = EXCLUDED.version,
			applied_at = EXCLUDED.applied_at
	`, currentMigrationVersion); err != nil {
		log.Printf("⚠️  Soroban migration: failed to record migration version: %v", err)
	} else {
		log.Printf("📌 Soroban migration: recorded at v%d", currentMigrationVersion)
	}
}

// runTransformationCycle executes a single transformation cycle
func (rt *RealtimeTransformer) runTransformationCycle() error {
	startTime := time.Now()
	ctx := context.Background()

	// Get current checkpoint
	lastLedger := rt.getLastLedger()

	// Check for new data using the source manager (handles hot/cold switching)
	maxBronzeLedger, err := rt.sourceManager.GetMaxLedgerSequence(ctx)
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
		minBronzeLedger, err := rt.sourceManager.GetMinLedgerSequence(ctx)
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

	// Limit batch size (use backfill batch size if in backfill mode)
	batchSize := rt.config.Performance.BatchSize
	if rt.sourceManager.GetMode() == SourceModeBackfill {
		batchSize = rt.config.GetBackfillBatchSize()
	}
	if endLedger-startLedger+1 > int64(batchSize) {
		endLedger = startLedger + int64(batchSize) - 1
	}

	// Log with source mode indicator
	modeIndicator := "🔥" // Hot
	if rt.sourceManager.GetMode() == SourceModeBackfill {
		modeIndicator = "❄️" // Cold/Backfill
	}
	log.Printf("%s New data available (ledgers %d to %d) [mode: %s]", modeIndicator, startLedger, endLedger, rt.sourceManager.GetMode())

	// ==========================================================================
	// Phase A: Bronze → Silver row-level transforms (parallel with worker pool)
	// ==========================================================================
	// Each transform reads from bronze and writes to its own silver table(s).
	// We run them in parallel with bounded concurrency, each getting its own
	// transaction since they write to non-overlapping tables.

	type transformResult struct {
		name     string
		count    int64
		duration time.Duration
		err      error
	}

	type transformJob struct {
		name string
		fn   func(ctx context.Context, tx *sql.Tx, start, end int64) (int64, error)
	}

	bronzeTransforms := []transformJob{
		{"enriched_operations", rt.transformEnrichedOperations},
		{"token_transfers", rt.transformTokenTransfers},
		{"contract_events_unmatched", rt.transformUnmatchedTokenLikeContractEvents},
		{"accounts_current", rt.transformAccountsCurrent},
		{"trustlines_current", rt.transformTrustlinesCurrent},
		{"offers_current", rt.transformOffersCurrent},
		{"accounts_snapshot", rt.transformAccountsSnapshot},
		{"trustlines_snapshot", rt.transformTrustlinesSnapshot},
		{"offers_snapshot", rt.transformOffersSnapshot},
		{"account_signers_snapshot", rt.transformAccountSignersSnapshot},
		{"contract_invocations", rt.transformContractInvocations},
		{"contract_metadata", rt.transformContractMetadata},
		{"contract_calls", rt.transformContractCalls},
		{"liquidity_pools_current", rt.transformLiquidityPoolsCurrent},
		{"claimable_balances_current", rt.transformClaimableBalancesCurrent},
		{"native_balances_current", rt.transformNativeBalancesCurrent},
		{"trades", rt.transformTrades},
		{"effects", rt.transformEffects},
		{"contract_data_current", rt.transformContractDataCurrent},
		{"token_registry", rt.transformTokenRegistry},
		{"contract_code_current", rt.transformContractCodeCurrent},
		{"ttl_current", rt.transformTTLCurrent},
		{"evicted_keys", rt.transformEvictedKeys},
		{"restored_keys", rt.transformRestoredKeys},
		{"config_settings_current", rt.transformConfigSettingsCurrent},
	}

	maxWorkers := rt.config.MaxSilverWriters()

	results := make(chan transformResult, len(bronzeTransforms))
	semaphore := make(chan struct{}, maxWorkers)
	var wg sync.WaitGroup

	for _, job := range bronzeTransforms {
		wg.Add(1)
		go func(j transformJob) {
			defer wg.Done()
			semaphore <- struct{}{}        // acquire
			defer func() { <-semaphore }() // release

			jobStart := time.Now()
			tx, txErr := rt.silverDB.BeginTx(ctx, nil)
			if txErr != nil {
				results <- transformResult{name: j.name, count: 0, duration: time.Since(jobStart), err: fmt.Errorf("begin tx for %s: %w", j.name, txErr)}
				return
			}

			count, fnErr := j.fn(ctx, tx, startLedger, endLedger)
			if fnErr != nil {
				tx.Rollback()
				results <- transformResult{name: j.name, count: 0, duration: time.Since(jobStart), err: fmt.Errorf("transform %s: %w", j.name, fnErr)}
				return
			}

			if commitErr := tx.Commit(); commitErr != nil {
				results <- transformResult{name: j.name, count: 0, duration: time.Since(jobStart), err: fmt.Errorf("commit %s: %w", j.name, commitErr)}
				return
			}

			results <- transformResult{name: j.name, count: count, duration: time.Since(jobStart), err: nil}
		}(job)
	}

	// Close results channel when all goroutines complete
	go func() {
		wg.Wait()
		close(results)
	}()

	totalRows := int64(0)
	var firstErr error
	for r := range results {
		if r.err != nil {
			if firstErr == nil {
				firstErr = r.err
			}
			log.Printf("   ❌ %s failed after %v: %v", r.name, r.duration, r.err)
			continue
		}
		totalRows += r.count
		log.Printf("   ✅ %s: %d rows in %v", r.name, r.count, r.duration)
	}

	if firstErr != nil {
		return fmt.Errorf("phase A (bronze→silver) failed: %w", firstErr)
	}

	// ==========================================================================
	// Phase B: Silver → Silver semantic transforms + checkpoint
	// ==========================================================================
	// Semantic transforms read from silver tables committed in Phase A.
	// They run sequentially in one transaction along with the checkpoint update.

	semanticTx, err := rt.silverDB.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin semantic transaction: %w", err)
	}
	defer semanticTx.Rollback()

	semanticTransforms := []transformJob{
		{"semantic_activities", rt.transformSemanticActivities},
		{"semantic_entities", rt.transformSemanticEntities},
		{"semantic_flows", rt.transformSemanticFlows},
		{"address_balances_current", rt.transformAddressBalancesCurrent},
		// State-based balance source runs AFTER event-aggregated balances so
		// it can overwrite them via ON CONFLICT when state is authoritative
		// (SAC Balance entries, including native XLM).
		{"address_balances_state", rt.transformAddressBalancesFromContractState},
		{"semantic_contract_functions", rt.transformSemanticContractFunctions},
		{"semantic_asset_stats", rt.transformSemanticAssetStats},
		{"semantic_dex_pairs", rt.transformSemanticDexPairs},
		{"semantic_account_summary", rt.transformSemanticAccountSummary},
	}

	for _, j := range semanticTransforms {
		jobStart := time.Now()
		count, err := j.fn(ctx, semanticTx, startLedger, endLedger)
		if err != nil {
			return fmt.Errorf("failed to transform %s: %w", j.name, err)
		}
		totalRows += count
		log.Printf("   ✅ %s: %d rows in %v", j.name, count, time.Since(jobStart))
	}

	// Post-processing: wallet classification on newly upserted contracts
	if walletCount, err := rt.transformWalletClassification(ctx, semanticTx, startLedger, endLedger); err != nil {
		log.Printf("⚠️  Wallet classification failed (non-fatal): %v", err)
	} else if walletCount > 0 {
		log.Printf("   🔐 Classified %d smart wallets", walletCount)
	}

	// If no rows were produced across both phases, check whether the ledgers
	// actually exist in bronze. Early testnet ledgers (and genesis) have zero
	// transactions/operations, so all transform queries legitimately return 0 rows.
	// We must still advance the checkpoint past them to avoid looping forever.
	if totalRows == 0 {
		bronzeLedgerCount, countErr := rt.sourceManager.CountLedgersInRange(ctx, startLedger, endLedger)
		if countErr != nil {
			log.Printf("⚠️  Failed to count bronze ledgers: %v", countErr)
			bronzeLedgerCount = 0
		}

		if bronzeLedgerCount > 0 {
			// Ledgers exist but have no operations — advance checkpoint past them
			log.Printf("⏭️  Ledgers %d-%d exist but have no operations (%d ledgers) — advancing checkpoint",
				startLedger, endLedger, bronzeLedgerCount)

			if err := rt.checkpoint.SaveWithTx(semanticTx, endLedger); err != nil {
				return fmt.Errorf("failed to save checkpoint for empty ledgers: %w", err)
			}
			if err := semanticTx.Commit(); err != nil {
				return fmt.Errorf("failed to commit empty-ledger checkpoint: %w", err)
			}
			rt.mu.Lock()
			rt.lastLedgerSequence = endLedger
			rt.mu.Unlock()
			rt.consecutiveEmptyPolls = 0
			return nil
		}

		// Ledgers don't exist in bronze — source may be unavailable
		semanticTx.Rollback()
		rt.consecutiveEmptyPolls++
		log.Printf("⏸️  No data found for ledgers %d-%d in %v (source may be unavailable), not advancing checkpoint (empty polls: %d)",
			startLedger, endLedger, time.Since(startTime), rt.consecutiveEmptyPolls)

		// Check for gap if we've exceeded the threshold
		maxEmptyPolls := rt.config.GapDetection.MaxEmptyPolls
		if maxEmptyPolls == 0 {
			maxEmptyPolls = 10 // Default threshold
		}

		if rt.consecutiveEmptyPolls >= maxEmptyPolls {
			// Use SourceManager to check for gap and potentially switch to backfill mode
			switched, err := rt.sourceManager.CheckAndSwitchMode(ctx, lastLedger, false)
			if err != nil {
				log.Printf("⚠️  Failed to check/switch mode: %v", err)
				return nil
			}

			if switched {
				// Mode changed - reset empty poll counter
				rt.consecutiveEmptyPolls = 0
			} else if rt.sourceManager.GetMode() == SourceModeHot {
				// Still in hot mode but no data - might be a gap without cold fallback
				// Fall back to legacy auto-skip behavior if configured
				if rt.config.GapDetection.AutoSkip && !rt.config.Fallback.Enabled {
					// Refresh ledger ranges to get current hot min before skip decision
					if err := rt.sourceManager.RefreshLedgerRanges(ctx); err != nil {
						log.Printf("⚠️  Failed to refresh ledger ranges: %v", err)
					}
					hotMinLedger := rt.sourceManager.GetHotMinLedger()
					if hotMinLedger > startLedger {
						gapSize := hotMinLedger - startLedger
						newCheckpoint := hotMinLedger - 1
						log.Printf("🔄 AUTO-SKIP (no cold fallback): Advancing checkpoint from %d to %d (skipping %d ledgers)",
							lastLedger, newCheckpoint, gapSize)

						if err := rt.checkpoint.Save(newCheckpoint); err != nil {
							log.Printf("❌ Failed to update checkpoint: %v", err)
							return fmt.Errorf("failed to skip checkpoint: %w", err)
						}

						rt.mu.Lock()
						rt.lastLedgerSequence = newCheckpoint
						rt.mu.Unlock()
						rt.consecutiveEmptyPolls = 0
					}
				}
			} else if rt.sourceManager.GetMode() == SourceModeBackfill {
				// In backfill mode but cold storage also doesn't have the data.
				// After enough failed attempts, skip to hot MIN and resume realtime.
				backfillMaxPolls := rt.config.GapDetection.BackfillMaxEmptyPolls
				if backfillMaxPolls == 0 {
					// Default: 3x the normal threshold, minimum 30
					backfillMaxPolls = maxEmptyPolls * 3
					if backfillMaxPolls < 30 {
						backfillMaxPolls = 30
					}
				}

				if rt.consecutiveEmptyPolls >= backfillMaxPolls {
					// Refresh ledger ranges to get current hot min before skip decision
					if err := rt.sourceManager.RefreshLedgerRanges(ctx); err != nil {
						log.Printf("⚠️  Failed to refresh ledger ranges: %v", err)
					}
					hotMinLedger := rt.sourceManager.GetHotMinLedger()
					if hotMinLedger > startLedger {
						newCheckpoint := hotMinLedger - 1
						gapSize := newCheckpoint - lastLedger
						log.Printf("🔄 BACKFILL-SKIP: Cold storage also missing ledgers %d-%d. Advancing checkpoint from %d to %d (skipping %d ledgers)",
							startLedger, hotMinLedger-1, lastLedger, newCheckpoint, gapSize)

						if err := rt.checkpoint.Save(newCheckpoint); err != nil {
							log.Printf("❌ Failed to update checkpoint: %v", err)
							return fmt.Errorf("failed to skip checkpoint: %w", err)
						}

						rt.mu.Lock()
						rt.lastLedgerSequence = newCheckpoint
						rt.mu.Unlock()
						rt.consecutiveEmptyPolls = 0

						// Force back to hot mode since backfill can't help
						rt.sourceManager.ForceHotMode()
						log.Printf("✅ MODE SWITCH: BACKFILL → HOT (forced - cold storage gap)")
						log.Printf("   Resuming from Hot storage at ledger %d", newCheckpoint+1)
					}
				}
			}
		}
		return nil
	}

	// Reset empty poll counter on successful transformation
	rt.consecutiveEmptyPolls = 0

	// Update checkpoint in the semantic transaction
	if err := rt.checkpoint.SaveWithTx(semanticTx, endLedger); err != nil {
		return fmt.Errorf("failed to save checkpoint: %w", err)
	}

	// Commit semantic transaction + checkpoint
	if err := semanticTx.Commit(); err != nil {
		return fmt.Errorf("failed to commit semantic transaction: %w", err)
	}

	// Update stats
	duration := time.Since(startTime)
	rt.updateStats(endLedger, duration, totalRows)

	// Log with source mode indicator (reuse modeIndicator from earlier in function)
	if rt.sourceManager.GetMode() == SourceModeBackfill {
		// Show backfill progress
		target, progress := rt.sourceManager.GetBackfillProgress(endLedger)
		log.Printf("❄️ Transformed %d rows in %v (ledgers %d-%d) [backfill: %.1f%% to %d]",
			totalRows, duration, startLedger, endLedger, progress, target)
	} else {
		log.Printf("🔥 Transformed %d rows in %v (ledgers %d-%d)", totalRows, duration, startLedger, endLedger)
	}

	if rt.sourceServer != nil {
		rt.sourceServer.Broadcast(SilverBatchInfo{
			StartLedger: uint32(startLedger),
			EndLedger:   uint32(endLedger),
			ClosedAt:    time.Now().UTC(),
			RowCount:    uint64(totalRows),
		})
	}

	// Check if we should switch modes (e.g., backfill complete → switch to hot)
	if _, err := rt.sourceManager.CheckAndSwitchMode(ctx, endLedger, true); err != nil {
		log.Printf("⚠️  Failed to check mode after transformation: %v", err)
	}

	return nil
}

// transformEnrichedOperations transforms enriched operations for the ledger range
func (rt *RealtimeTransformer) transformEnrichedOperations(ctx context.Context, tx *sql.Tx, startLedger, endLedger int64) (int64, error) {
	rows, err := rt.sourceManager.QueryEnrichedOperations(ctx, startLedger, endLedger)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	batchSize := rt.insertBatchSize()
	batch := NewBatchInserter("enriched_history_operations", enrichedOpColumns, enrichedOpConflict, batchSize)
	sorobanBatch := NewBatchInserter("enriched_history_operations_soroban", enrichedOpColumns, enrichedOpSorobanConflict, batchSize)
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

		if err := batch.Add(row.Values()...); err != nil {
			return count, fmt.Errorf("failed to add row to %s batch: %w", batch.table, err)
		}
		if err := batch.FlushIfNeeded(ctx, tx); err != nil {
			return count, fmt.Errorf("failed to flush enriched operations batch: %w", err)
		}

		// Also batch to Soroban table if it's a Soroban operation
		if row.IsSorobanOp != nil && *row.IsSorobanOp {
			if err := sorobanBatch.Add(row.Values()...); err != nil {
				return count, fmt.Errorf("failed to add row to soroban batch: %w", err)
			}
			if err := sorobanBatch.FlushIfNeeded(ctx, tx); err != nil {
				return count, fmt.Errorf("failed to flush soroban operations batch: %w", err)
			}
		}

		count++
	}

	if err := rows.Err(); err != nil {
		return count, fmt.Errorf("error iterating enriched operations: %w", err)
	}

	// Flush remaining rows
	if err := batch.Flush(ctx, tx); err != nil {
		return count, fmt.Errorf("failed to flush enriched operations remainder: %w", err)
	}
	if err := sorobanBatch.Flush(ctx, tx); err != nil {
		return count, fmt.Errorf("failed to flush soroban operations remainder: %w", err)
	}

	return count, nil
}

func isValidTokenTransferSemantic(row *TokenTransferRow) bool {
	if row == nil {
		return false
	}
	if row.SourceType != "soroban" {
		return true
	}
	if !row.TokenContractID.Valid || row.TokenContractID.String == "" || !row.Amount.Valid || row.Amount.String == "" {
		return false
	}
	fromValid := row.FromAccount.Valid && row.FromAccount.String != ""
	toValid := row.ToAccount.Valid && row.ToAccount.String != ""

	// Supported Soroban semantic shapes:
	//   transfer: from + to + amount
	//   mint:     to + amount
	//   burn / clawback: from + amount
	return (fromValid && toValid) || (!fromValid && toValid) || (fromValid && !toValid)
}

// transformTokenTransfers transforms token transfers for the ledger range
func (rt *RealtimeTransformer) transformTokenTransfers(ctx context.Context, tx *sql.Tx, startLedger, endLedger int64) (int64, error) {
	rows, err := rt.sourceManager.QueryTokenTransfers(ctx, startLedger, endLedger)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	batch := NewBatchInserter("token_transfers_raw", tokenTransferColumns, tokenTransferConflict, rt.insertBatchSize())
	count := int64(0)

	for rows.Next() {
		row := &TokenTransferRow{}

		err := rows.Scan(
			&row.Timestamp, &row.TransactionHash, &row.LedgerSequence, &row.SourceType,
			&row.FromAccount, &row.ToAccount, &row.AssetCode, &row.AssetIssuer, &row.Amount,
			&row.TokenContractID, &row.OperationType, &row.TransactionSuccessful,
			&row.EventIndex,
		)

		if err != nil {
			return count, fmt.Errorf("failed to scan token transfer row: %w", err)
		}

		// Convert hex contract ID to C-encoded strkey (moved from WriteTokenTransfer)
		if row.TokenContractID.Valid && row.TokenContractID.String != "" {
			encoded, err := hexToStrKey(row.TokenContractID.String)
			if err != nil {
				return count, fmt.Errorf("failed to convert token contract ID to strkey: %w", err)
			}
			row.TokenContractID.String = encoded
		}

		if !isValidTokenTransferSemantic(row) {
			continue
		}

		if err := batch.Add(row.Values()...); err != nil {
			return count, fmt.Errorf("failed to add token transfer to batch: %w", err)
		}
		if err := batch.FlushIfNeeded(ctx, tx); err != nil {
			return count, fmt.Errorf("failed to flush token transfers batch: %w", err)
		}

		count++
	}

	if err := rows.Err(); err != nil {
		return count, fmt.Errorf("error iterating token transfers: %w", err)
	}

	if err := batch.Flush(ctx, tx); err != nil {
		return count, fmt.Errorf("failed to flush token transfers remainder: %w", err)
	}

	return count, nil
}

// transformUnmatchedTokenLikeContractEvents preserves token-like Soroban
// contract events that could not be safely normalized into token_transfers_raw.
func (rt *RealtimeTransformer) transformUnmatchedTokenLikeContractEvents(ctx context.Context, tx *sql.Tx, startLedger, endLedger int64) (int64, error) {
	rows, err := rt.sourceManager.QueryUnmatchedTokenLikeContractEvents(ctx, startLedger, endLedger)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	batch := NewBatchInserter("contract_events_unmatched", unmatchedContractEventColumns, unmatchedContractEventConflict, rt.insertBatchSize())
	count := int64(0)

	for rows.Next() {
		row := &UnmatchedContractEventRow{}
		if err := rows.Scan(
			&row.Timestamp, &row.TransactionHash, &row.LedgerSequence, &row.ContractID,
			&row.EventIndex, &row.EventName, &row.TopicsDecoded, &row.DataDecoded,
			&row.Successful, &row.ParseReason,
		); err != nil {
			return count, fmt.Errorf("failed to scan unmatched contract event row: %w", err)
		}

		if row.ContractID.Valid && row.ContractID.String != "" {
			encoded, err := hexToStrKey(row.ContractID.String)
			if err != nil {
				return count, fmt.Errorf("failed to convert unmatched contract ID to strkey: %w", err)
			}
			row.ContractID.String = encoded
		}

		if err := batch.Add(row.Values()...); err != nil {
			return count, fmt.Errorf("failed to add unmatched contract event to batch: %w", err)
		}
		if err := batch.FlushIfNeeded(ctx, tx); err != nil {
			return count, fmt.Errorf("failed to flush unmatched contract events batch: %w", err)
		}
		count++
	}

	if err := rows.Err(); err != nil {
		return count, fmt.Errorf("error iterating unmatched contract events: %w", err)
	}
	if err := batch.Flush(ctx, tx); err != nil {
		return count, fmt.Errorf("failed to flush unmatched contract events remainder: %w", err)
	}

	return count, nil
}

// transformAccountsCurrent upserts accounts current state for the ledger range
func (rt *RealtimeTransformer) transformAccountsCurrent(ctx context.Context, tx *sql.Tx, startLedger, endLedger int64) (int64, error) {
	rows, err := rt.sourceManager.QueryAccountsSnapshot(ctx, startLedger, endLedger)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	batch := NewBatchInserter("accounts_current", accountCurrentColumns, accountCurrentConflict, rt.insertBatchSize())
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

		if err := batch.Add(row.Values()...); err != nil {
			return count, fmt.Errorf("failed to add row to %s batch: %w", batch.table, err)
		}
		if err := batch.FlushIfNeeded(ctx, tx); err != nil {
			return count, fmt.Errorf("failed to flush accounts current batch: %w", err)
		}
		count++
	}

	if err := rows.Err(); err != nil {
		return count, fmt.Errorf("error iterating accounts: %w", err)
	}

	if err := batch.Flush(ctx, tx); err != nil {
		return count, fmt.Errorf("failed to flush accounts current remainder: %w", err)
	}

	return count, nil
}

// transformTrustlinesCurrent upserts trustlines current state for the ledger range
func (rt *RealtimeTransformer) transformTrustlinesCurrent(ctx context.Context, tx *sql.Tx, startLedger, endLedger int64) (int64, error) {
	rows, err := rt.sourceManager.QueryTrustlinesSnapshot(ctx, startLedger, endLedger)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	batch := NewBatchInserter("trustlines_current", trustlineCurrentColumns, trustlineCurrentConflict, rt.insertBatchSize())
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

		// TrustlineCurrentValues() converts balance/limit/liabilities to stroops in Go
		vals, err := row.TrustlineCurrentValues()
		if err != nil {
			return count, fmt.Errorf("failed to convert trustline values: %w", err)
		}
		if err := batch.Add(vals...); err != nil {
			return count, fmt.Errorf("failed to add trustline to batch: %w", err)
		}
		if err := batch.FlushIfNeeded(ctx, tx); err != nil {
			return count, fmt.Errorf("failed to flush trustlines current batch: %w", err)
		}
		count++
	}

	if err := rows.Err(); err != nil {
		return count, fmt.Errorf("error iterating trustlines: %w", err)
	}

	if err := batch.Flush(ctx, tx); err != nil {
		return count, fmt.Errorf("failed to flush trustlines current remainder: %w", err)
	}

	return count, nil
}

// transformOffersCurrent upserts offers current state for the ledger range
func (rt *RealtimeTransformer) transformOffersCurrent(ctx context.Context, tx *sql.Tx, startLedger, endLedger int64) (int64, error) {
	rows, err := rt.sourceManager.QueryOffersSnapshot(ctx, startLedger, endLedger)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	batch := NewBatchInserter("offers_current", offerCurrentColumns, offerCurrentConflict, rt.insertBatchSize())
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
		if row.PriceD > 0 {
			row.Price = fmt.Sprintf("%.7f", float64(row.PriceN)/float64(row.PriceD))
		} else {
			row.Price = "0"
		}

		// OfferCurrentValues() converts amount from string to int64
		vals, err := row.OfferCurrentValues()
		if err != nil {
			return count, fmt.Errorf("failed to convert offer values: %w", err)
		}
		if err := batch.Add(vals...); err != nil {
			return count, fmt.Errorf("failed to add offer to batch: %w", err)
		}
		if err := batch.FlushIfNeeded(ctx, tx); err != nil {
			return count, fmt.Errorf("failed to flush offers current batch: %w", err)
		}
		count++
	}

	if err := rows.Err(); err != nil {
		return count, fmt.Errorf("error iterating offers: %w", err)
	}

	if err := batch.Flush(ctx, tx); err != nil {
		return count, fmt.Errorf("failed to flush offers current remainder: %w", err)
	}

	return count, nil
}

// transformAccountsSnapshot appends account snapshot history (SCD Type 2 - Cycle 3)
func (rt *RealtimeTransformer) transformAccountsSnapshot(ctx context.Context, tx *sql.Tx, startLedger, endLedger int64) (int64, error) {
	rows, err := rt.sourceManager.QueryAccountsSnapshotAll(ctx, startLedger, endLedger)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	batch := NewBatchInserter("accounts_snapshot", accountSnapshotColumns, accountSnapshotConflict, rt.insertBatchSize())
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
		if err := batch.Add(row.Values()...); err != nil {
			return count, fmt.Errorf("failed to add row to %s batch: %w", batch.table, err)
		}
		if err := batch.FlushIfNeeded(ctx, tx); err != nil {
			return count, fmt.Errorf("failed to flush account snapshots batch: %w", err)
		}
		count++
	}
	if err := rows.Err(); err != nil {
		return count, fmt.Errorf("error iterating account snapshots: %w", err)
	}
	if err := batch.Flush(ctx, tx); err != nil {
		return count, fmt.Errorf("failed to flush account snapshots remainder: %w", err)
	}

	if err := rt.silverWriter.UpdateAccountSnapshotValidTo(ctx, tx, startLedger, endLedger); err != nil {
		return count, fmt.Errorf("failed to update valid_to: %w", err)
	}
	return count, nil
}

// transformTrustlinesSnapshot appends trustline snapshot history (SCD Type 2 - Cycle 3)
func (rt *RealtimeTransformer) transformTrustlinesSnapshot(ctx context.Context, tx *sql.Tx, startLedger, endLedger int64) (int64, error) {
	rows, err := rt.sourceManager.QueryTrustlinesSnapshotAll(ctx, startLedger, endLedger)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	batch := NewBatchInserter("trustlines_snapshot", trustlineSnapshotColumns, trustlineSnapshotConflict, rt.insertBatchSize())
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
		if err := batch.Add(row.Values()...); err != nil {
			return count, fmt.Errorf("failed to add row to %s batch: %w", batch.table, err)
		}
		if err := batch.FlushIfNeeded(ctx, tx); err != nil {
			return count, fmt.Errorf("failed to flush trustline snapshots batch: %w", err)
		}
		count++
	}
	if err := rows.Err(); err != nil {
		return count, fmt.Errorf("error iterating trustline snapshots: %w", err)
	}
	if err := batch.Flush(ctx, tx); err != nil {
		return count, fmt.Errorf("failed to flush trustline snapshots remainder: %w", err)
	}

	if err := rt.silverWriter.UpdateTrustlineSnapshotValidTo(ctx, tx, startLedger, endLedger); err != nil {
		return count, fmt.Errorf("failed to update trustline valid_to: %w", err)
	}
	return count, nil
}

// transformOffersSnapshot appends offer snapshot history (SCD Type 2 - Cycle 3)
func (rt *RealtimeTransformer) transformOffersSnapshot(ctx context.Context, tx *sql.Tx, startLedger, endLedger int64) (int64, error) {
	rows, err := rt.sourceManager.QueryOffersSnapshotAll(ctx, startLedger, endLedger)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	batch := NewBatchInserter("offers_snapshot", offerSnapshotColumns, offerSnapshotConflict, rt.insertBatchSize())
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
		if err := batch.Add(row.Values()...); err != nil {
			return count, fmt.Errorf("failed to add row to %s batch: %w", batch.table, err)
		}
		if err := batch.FlushIfNeeded(ctx, tx); err != nil {
			return count, fmt.Errorf("failed to flush offer snapshots batch: %w", err)
		}
		count++
	}
	if err := rows.Err(); err != nil {
		return count, fmt.Errorf("error iterating offer snapshots: %w", err)
	}
	if err := batch.Flush(ctx, tx); err != nil {
		return count, fmt.Errorf("failed to flush offer snapshots remainder: %w", err)
	}

	if err := rt.silverWriter.UpdateOfferSnapshotValidTo(ctx, tx, startLedger, endLedger); err != nil {
		return count, fmt.Errorf("failed to update offer valid_to: %w", err)
	}
	return count, nil
}

// transformAccountSignersSnapshot appends account signer snapshot history (SCD Type 2 - Cycle 3)
func (rt *RealtimeTransformer) transformAccountSignersSnapshot(ctx context.Context, tx *sql.Tx, startLedger, endLedger int64) (int64, error) {
	rows, err := rt.sourceManager.QueryAccountSignersSnapshotAll(ctx, startLedger, endLedger)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	batch := NewBatchInserter("account_signers_snapshot", accountSignerSnapshotColumns, accountSignerSnapshotConflict, rt.insertBatchSize())
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
		if err := batch.Add(row.Values()...); err != nil {
			return count, fmt.Errorf("failed to add row to %s batch: %w", batch.table, err)
		}
		if err := batch.FlushIfNeeded(ctx, tx); err != nil {
			return count, fmt.Errorf("failed to flush account signer snapshots batch: %w", err)
		}
		count++
	}
	if err := rows.Err(); err != nil {
		return count, fmt.Errorf("error iterating account signer snapshots: %w", err)
	}
	if err := batch.Flush(ctx, tx); err != nil {
		return count, fmt.Errorf("failed to flush account signer snapshots remainder: %w", err)
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

	stats := TransformerStats{
		TransformationsTotal:       rt.transformationsTotal,
		TransformationErrors:       rt.transformationErrors,
		LastLedgerSequence:         rt.lastLedgerSequence,
		LastTransformationTime:     rt.lastTransformationTime,
		LastTransformationDuration: rt.lastTransformationDuration,
		LastTransformationRowCount: rt.lastTransformationRowCount,
		SourceMode:                 string(rt.sourceManager.GetMode()),
		HotMinLedger:               rt.sourceManager.GetHotMinLedger(),
	}

	// Add backfill info if in backfill mode
	if rt.sourceManager.GetMode() == SourceModeBackfill {
		target, progress := rt.sourceManager.GetBackfillProgress(rt.lastLedgerSequence)
		stats.BackfillTarget = target
		stats.BackfillProgress = progress
	}

	return stats
}

// TransformerStats holds transformation statistics
type TransformerStats struct {
	TransformationsTotal       int64
	TransformationErrors       int64
	LastLedgerSequence         int64
	LastTransformationTime     time.Time
	LastTransformationDuration time.Duration
	LastTransformationRowCount int64
	SourceMode                 string
	HotMinLedger               int64
	BackfillTarget             int64
	BackfillProgress           float64
}

// GetSourceManager returns the source manager (for health endpoint access)
func (rt *RealtimeTransformer) GetSourceManager() *SourceManager {
	return rt.sourceManager
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

// insertBatchSize returns the configured insert batch size or the default.
func (rt *RealtimeTransformer) insertBatchSize() int {
	if rt.config.Performance.InsertBatchSize > 0 {
		return rt.config.Performance.InsertBatchSize
	}
	return defaultInsertBatchSize
}

// transformContractInvocations transforms contract invocations for the ledger range
func (rt *RealtimeTransformer) transformContractInvocations(ctx context.Context, tx *sql.Tx, startLedger, endLedger int64) (int64, error) {
	rows, err := rt.sourceManager.QueryContractInvocations(ctx, startLedger, endLedger)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	batch := NewBatchInserter("contract_invocations_raw", contractInvocationColumns, contractInvocationConflict, rt.insertBatchSize())
	count := int64(0)

	for rows.Next() {
		row := &ContractInvocationRow{}
		err := rows.Scan(
			&row.LedgerSequence, &row.TransactionIndex, &row.OperationIndex,
			&row.TransactionHash, &row.SourceAccount, &row.ContractID, &row.FunctionName,
			&row.ArgumentsJSON, &row.Successful, &row.ClosedAt, &row.LedgerRange,
		)
		if err != nil {
			return count, fmt.Errorf("failed to scan contract invocation row: %w", err)
		}
		if err := batch.Add(row.Values()...); err != nil {
			return count, fmt.Errorf("failed to add row to %s batch: %w", batch.table, err)
		}
		if err := batch.FlushIfNeeded(ctx, tx); err != nil {
			return count, fmt.Errorf("failed to flush contract invocations batch: %w", err)
		}
		count++
	}
	if err := rows.Err(); err != nil {
		return count, fmt.Errorf("error iterating contract invocations: %w", err)
	}
	if err := batch.Flush(ctx, tx); err != nil {
		return count, fmt.Errorf("failed to flush contract invocations remainder: %w", err)
	}
	return count, nil
}

// transformContractMetadata reads contract creation records from bronze and upserts into silver contract_metadata
func (rt *RealtimeTransformer) transformContractMetadata(ctx context.Context, tx *sql.Tx, startLedger, endLedger int64) (int64, error) {
	rows, err := rt.sourceManager.QueryContractCreations(ctx, startLedger, endLedger)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	batch := NewBatchInserter("contract_metadata", contractMetadataColumns, contractMetadataConflict, rt.insertBatchSize())
	count := int64(0)

	for rows.Next() {
		var (
			contractID     string
			creatorAddress string
			wasmHash       sql.NullString
			createdLedger  int64
			createdAt      time.Time
		)
		if err := rows.Scan(&contractID, &creatorAddress, &wasmHash, &createdLedger, &createdAt); err != nil {
			return count, fmt.Errorf("failed to scan contract creation row: %w", err)
		}

		var wasmHashPtr *string
		if wasmHash.Valid {
			wasmHashPtr = &wasmHash.String
		}

		if err := batch.Add(contractID, creatorAddress, wasmHashPtr, createdLedger, createdAt); err != nil {
			return count, fmt.Errorf("failed to add contract metadata to batch: %w", err)
		}
		if err := batch.FlushIfNeeded(ctx, tx); err != nil {
			return count, fmt.Errorf("failed to flush contract metadata batch: %w", err)
		}
		count++
	}
	if err := rows.Err(); err != nil {
		return count, fmt.Errorf("error iterating contract creations: %w", err)
	}
	if err := batch.Flush(ctx, tx); err != nil {
		return count, fmt.Errorf("failed to flush contract metadata remainder: %w", err)
	}
	return count, nil
}

// transformContractCalls transforms cross-contract call graphs for the ledger range
// Extracts call relationships from Bronze call graph data and writes to Silver tables
func (rt *RealtimeTransformer) transformContractCalls(ctx context.Context, tx *sql.Tx, startLedger, endLedger int64) (int64, error) {
	rows, err := rt.sourceManager.QueryContractCallGraphs(ctx, startLedger, endLedger)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	batchSize := rt.insertBatchSize()
	callBatch := NewBatchInserter("contract_invocation_calls", contractCallColumns, contractCallConflict, batchSize)
	hierarchyBatch := NewBatchInserter("contract_invocation_hierarchy", contractHierarchyColumns, contractHierarchyConflict, batchSize)
	count := int64(0)

	for rows.Next() {
		var (
			ledgerSequence       int64
			transactionIndex     int
			operationIndex       int
			transactionHash      string
			sourceAccount        string
			contractID           sql.NullString
			functionName         sql.NullString
			argumentsJSON        sql.NullString
			contractCallsJSON    sql.NullString
			contractsInvolvedRaw sql.NullString
			contractsInvolved    []string
			maxCallDepth         sql.NullInt32
			successful           bool
			closedAt             time.Time
			ledgerRange          int64
		)

		err := rows.Scan(
			&ledgerSequence, &transactionIndex, &operationIndex, &transactionHash,
			&sourceAccount, &contractID, &functionName, &argumentsJSON,
			&contractCallsJSON, &contractsInvolvedRaw, &maxCallDepth,
			&successful, &closedAt, &ledgerRange,
		)
		if err != nil {
			return count, fmt.Errorf("failed to scan contract call graph row: %w", err)
		}

		if contractsInvolvedRaw.Valid && contractsInvolvedRaw.String != "" {
			contractsInvolved = parseContractsInvolvedArray(contractsInvolvedRaw.String)
		}

		if contractCallsJSON.Valid && contractCallsJSON.String != "" {
			callRows, hierarchyRows, err := parseContractCallGraph(
				contractCallsJSON.String, transactionHash, ledgerSequence,
				transactionIndex, operationIndex, closedAt, ledgerRange, contractsInvolved,
			)
			if err != nil {
				log.Printf("Warning: Failed to parse call graph for tx %s: %v", transactionHash, err)
				continue
			}

			for _, callRow := range callRows {
				callBatch.Add(callRow.Values()...)
				if err := callBatch.FlushIfNeeded(ctx, tx); err != nil {
					return count, fmt.Errorf("failed to flush contract calls batch: %w", err)
				}
				count++
			}
			for _, hierarchyRow := range hierarchyRows {
				hierarchyBatch.Add(hierarchyRow.Values()...)
				if err := hierarchyBatch.FlushIfNeeded(ctx, tx); err != nil {
					return count, fmt.Errorf("failed to flush contract hierarchy batch: %w", err)
				}
			}
		} else if contractID.Valid && contractID.String != "" {
			funcName := ""
			if functionName.Valid {
				funcName = functionName.String
			}

			rootCall := &ContractCallRow{
				LedgerSequence: ledgerSequence, TransactionIndex: transactionIndex,
				OperationIndex: operationIndex, TransactionHash: transactionHash,
				FromContract: sourceAccount, ToContract: contractID.String,
				FunctionName: funcName, CallDepth: 0, ExecutionOrder: 0,
				Successful: successful, ClosedAt: closedAt, LedgerRange: ledgerRange,
			}
			callBatch.Add(rootCall.Values()...)
			if err := callBatch.FlushIfNeeded(ctx, tx); err != nil {
				return count, fmt.Errorf("failed to flush contract calls batch: %w", err)
			}
			count++

			rootHierarchy := &ContractHierarchyRow{
				TransactionHash: transactionHash, RootContract: contractID.String,
				ChildContract: contractID.String, PathDepth: 0,
				FullPath: []string{contractID.String}, LedgerRange: ledgerRange,
			}
			hierarchyBatch.Add(rootHierarchy.Values()...)
			if err := hierarchyBatch.FlushIfNeeded(ctx, tx); err != nil {
				return count, fmt.Errorf("failed to flush contract hierarchy batch: %w", err)
			}
		}
	}
	if err := rows.Err(); err != nil {
		return count, fmt.Errorf("error iterating contract call graphs: %w", err)
	}
	if err := callBatch.Flush(ctx, tx); err != nil {
		return count, fmt.Errorf("failed to flush contract calls remainder: %w", err)
	}
	if err := hierarchyBatch.Flush(ctx, tx); err != nil {
		return count, fmt.Errorf("failed to flush contract hierarchy remainder: %w", err)
	}
	return count, nil
}

// transformLiquidityPoolsCurrent upserts liquidity pools current state for the ledger range
func (rt *RealtimeTransformer) transformLiquidityPoolsCurrent(ctx context.Context, tx *sql.Tx, startLedger, endLedger int64) (int64, error) {
	rows, err := rt.sourceManager.QueryLiquidityPoolsSnapshot(ctx, startLedger, endLedger)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	batch := NewBatchInserter("liquidity_pools_current", liquidityPoolCurrentColumns, liquidityPoolCurrentConflict, rt.insertBatchSize())
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
		row.LastModifiedLedger = row.LedgerSequence
		if err := batch.Add(row.Values()...); err != nil {
			return count, fmt.Errorf("failed to add row to %s batch: %w", batch.table, err)
		}
		if err := batch.FlushIfNeeded(ctx, tx); err != nil {
			return count, fmt.Errorf("failed to flush liquidity pools batch: %w", err)
		}
		count++
	}
	if err := rows.Err(); err != nil {
		return count, fmt.Errorf("error iterating liquidity pools: %w", err)
	}
	if err := batch.Flush(ctx, tx); err != nil {
		return count, fmt.Errorf("failed to flush liquidity pools remainder: %w", err)
	}
	return count, nil
}

// transformClaimableBalancesCurrent upserts claimable balances current state for the ledger range
func (rt *RealtimeTransformer) transformClaimableBalancesCurrent(ctx context.Context, tx *sql.Tx, startLedger, endLedger int64) (int64, error) {
	rows, err := rt.sourceManager.QueryClaimableBalancesSnapshot(ctx, startLedger, endLedger)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	batch := NewBatchInserter("claimable_balances_current", claimableBalanceCurrentColumns, claimableBalanceCurrentConflict, rt.insertBatchSize())
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
		row.LastModifiedLedger = row.LedgerSequence
		if err := batch.Add(row.Values()...); err != nil {
			return count, fmt.Errorf("failed to add row to %s batch: %w", batch.table, err)
		}
		if err := batch.FlushIfNeeded(ctx, tx); err != nil {
			return count, fmt.Errorf("failed to flush claimable balances batch: %w", err)
		}
		count++
	}
	if err := rows.Err(); err != nil {
		return count, fmt.Errorf("error iterating claimable balances: %w", err)
	}
	if err := batch.Flush(ctx, tx); err != nil {
		return count, fmt.Errorf("failed to flush claimable balances remainder: %w", err)
	}
	return count, nil
}

// transformNativeBalancesCurrent upserts native balances current state for the ledger range
func (rt *RealtimeTransformer) transformNativeBalancesCurrent(ctx context.Context, tx *sql.Tx, startLedger, endLedger int64) (int64, error) {
	rows, err := rt.sourceManager.QueryNativeBalancesSnapshot(ctx, startLedger, endLedger)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	batch := NewBatchInserter("native_balances_current", nativeBalanceCurrentColumns, nativeBalanceCurrentConflict, rt.insertBatchSize())
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
		if err := batch.Add(row.Values()...); err != nil {
			return count, fmt.Errorf("failed to add row to %s batch: %w", batch.table, err)
		}
		if err := batch.FlushIfNeeded(ctx, tx); err != nil {
			return count, fmt.Errorf("failed to flush native balances batch: %w", err)
		}
		count++
	}
	if err := rows.Err(); err != nil {
		return count, fmt.Errorf("error iterating native balances: %w", err)
	}
	if err := batch.Flush(ctx, tx); err != nil {
		return count, fmt.Errorf("failed to flush native balances remainder: %w", err)
	}
	return count, nil
}

// parseContractsInvolvedArray parses the contracts_involved field from either PostgreSQL array format
// (e.g., "{CA...,CB...}") or plain text format (e.g., "CA...,CB..." or JSON array)
func parseContractsInvolvedArray(raw string) []string {
	if raw == "" {
		return nil
	}

	// Handle PostgreSQL array format: {val1,val2,val3}
	if strings.HasPrefix(raw, "{") && strings.HasSuffix(raw, "}") {
		inner := raw[1 : len(raw)-1]
		if inner == "" {
			return nil
		}
		return strings.Split(inner, ",")
	}

	// Handle JSON array format: ["val1","val2","val3"]
	if strings.HasPrefix(raw, "[") {
		var arr []string
		if err := json.Unmarshal([]byte(raw), &arr); err == nil {
			return arr
		}
	}

	// Handle comma-separated format: val1,val2,val3
	if strings.Contains(raw, ",") {
		return strings.Split(raw, ",")
	}

	// Single value
	return []string{raw}
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
	rows, err := rt.sourceManager.QueryTrades(ctx, startLedger, endLedger)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	batch := NewBatchInserter("trades", tradeColumns, tradeConflict, rt.insertBatchSize())
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

		vals, err := row.TradeValues()
		if err != nil {
			return count, fmt.Errorf("failed to convert trade values: %w", err)
		}
		if err := batch.Add(vals...); err != nil {
			return count, fmt.Errorf("failed to add trade to batch: %w", err)
		}
		if err := batch.FlushIfNeeded(ctx, tx); err != nil {
			return count, fmt.Errorf("failed to flush trades batch: %w", err)
		}

		count++
	}

	if err := rows.Err(); err != nil {
		return count, fmt.Errorf("error iterating trades: %w", err)
	}

	if err := batch.Flush(ctx, tx); err != nil {
		return count, fmt.Errorf("failed to flush remaining trades: %w", err)
	}

	return count, nil
}

// transformEffects inserts effect events for the ledger range (append-only event stream)
func (rt *RealtimeTransformer) transformEffects(ctx context.Context, tx *sql.Tx, startLedger, endLedger int64) (int64, error) {
	rows, err := rt.sourceManager.QueryEffects(ctx, startLedger, endLedger)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	batch := NewBatchInserter("effects", effectColumns, effectConflict, rt.insertBatchSize())
	count := int64(0)

	for rows.Next() {
		row := &EffectRow{}

		err := rows.Scan(
			&row.LedgerSequence, &row.TransactionHash, &row.OperationIndex, &row.EffectIndex,
			&row.OperationID, &row.EffectType, &row.EffectTypeString, &row.AccountID,
			&row.Amount, &row.AssetCode, &row.AssetIssuer, &row.AssetType,
			&row.DetailsJSON,
			&row.TrustlineLimit, &row.AuthorizeFlag, &row.ClawbackFlag,
			&row.SignerAccount, &row.SignerWeight, &row.OfferID, &row.SellerAccount,
			&row.CreatedAt, &row.LedgerRange,
		)

		if err != nil {
			return count, fmt.Errorf("failed to scan effect row: %w", err)
		}

		if err := batch.Add(row.Values()...); err != nil {
			return count, fmt.Errorf("failed to add row to %s batch: %w", batch.table, err)
		}
		if err := batch.FlushIfNeeded(ctx, tx); err != nil {
			return count, fmt.Errorf("failed to flush effects batch: %w", err)
		}

		count++
	}

	if err := rows.Err(); err != nil {
		return count, fmt.Errorf("error iterating effects: %w", err)
	}

	if err := batch.Flush(ctx, tx); err != nil {
		return count, fmt.Errorf("failed to flush remaining effects: %w", err)
	}

	return count, nil
}

// =============================================================================
// Phase 3: Soroban Tables
// =============================================================================

// transformContractDataCurrent upserts contract data current state for the ledger range
func (rt *RealtimeTransformer) transformContractDataCurrent(ctx context.Context, tx *sql.Tx, startLedger, endLedger int64) (int64, error) {
	rows, err := rt.sourceManager.QueryContractDataSnapshot(ctx, startLedger, endLedger)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	batch := NewBatchInserter("contract_data_current", contractDataCurrentColumns, contractDataCurrentConflict, rt.insertBatchSize())
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

		if err := batch.Add(row.Values()...); err != nil {
			return count, fmt.Errorf("failed to add row to %s batch: %w", batch.table, err)
		}
		if err := batch.FlushIfNeeded(ctx, tx); err != nil {
			return count, fmt.Errorf("failed to flush contract data batch: %w", err)
		}

		count++
	}

	if err := rows.Err(); err != nil {
		return count, fmt.Errorf("error iterating contract data: %w", err)
	}

	if err := batch.Flush(ctx, tx); err != nil {
		return count, fmt.Errorf("failed to flush remaining contract data: %w", err)
	}

	return count, nil
}

// transformContractCodeCurrent upserts contract code current state for the ledger range
func (rt *RealtimeTransformer) transformContractCodeCurrent(ctx context.Context, tx *sql.Tx, startLedger, endLedger int64) (int64, error) {
	rows, err := rt.sourceManager.QueryContractCodeSnapshot(ctx, startLedger, endLedger)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	batch := NewBatchInserter("contract_code_current", contractCodeCurrentColumns, contractCodeCurrentConflict, rt.insertBatchSize())
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

		if err := batch.Add(row.Values()...); err != nil {
			return count, fmt.Errorf("failed to add row to %s batch: %w", batch.table, err)
		}
		if err := batch.FlushIfNeeded(ctx, tx); err != nil {
			return count, fmt.Errorf("failed to flush contract code batch: %w", err)
		}

		count++
	}

	if err := rows.Err(); err != nil {
		return count, fmt.Errorf("error iterating contract code: %w", err)
	}

	if err := batch.Flush(ctx, tx); err != nil {
		return count, fmt.Errorf("failed to flush remaining contract code: %w", err)
	}

	return count, nil
}

// transformTTLCurrent upserts TTL current state for the ledger range
func (rt *RealtimeTransformer) transformTTLCurrent(ctx context.Context, tx *sql.Tx, startLedger, endLedger int64) (int64, error) {
	rows, err := rt.sourceManager.QueryTTLSnapshot(ctx, startLedger, endLedger)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	batch := NewBatchInserter("ttl_current", ttlCurrentColumns, ttlCurrentConflict, rt.insertBatchSize())
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

		if err := batch.Add(row.Values()...); err != nil {
			return count, fmt.Errorf("failed to add row to %s batch: %w", batch.table, err)
		}
		if err := batch.FlushIfNeeded(ctx, tx); err != nil {
			return count, fmt.Errorf("failed to flush TTL batch: %w", err)
		}

		count++
	}

	if err := rows.Err(); err != nil {
		return count, fmt.Errorf("error iterating TTL entries: %w", err)
	}

	if err := batch.Flush(ctx, tx); err != nil {
		return count, fmt.Errorf("failed to flush remaining TTL entries: %w", err)
	}

	return count, nil
}

// transformEvictedKeys inserts evicted key events for the ledger range (append-only event stream)
func (rt *RealtimeTransformer) transformEvictedKeys(ctx context.Context, tx *sql.Tx, startLedger, endLedger int64) (int64, error) {
	rows, err := rt.sourceManager.QueryEvictedKeys(ctx, startLedger, endLedger)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	batch := NewBatchInserter("evicted_keys", evictedKeyColumns, evictedKeyConflict, rt.insertBatchSize())
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

		// Convert hex contract ID to C-encoded strkey
		// Note: bronze data may have empty contract_id for some evicted keys
		if row.ContractID != "" {
			encoded, err := hexToStrKey(row.ContractID)
			if err != nil {
				log.Printf("warning: evicted key hexToStrKey(%q): %v, keeping raw value", row.ContractID, err)
			} else {
				row.ContractID = encoded
			}
		}

		if err := batch.Add(row.Values()...); err != nil {
			return count, fmt.Errorf("failed to add evicted key to batch: %w", err)
		}
		if err := batch.FlushIfNeeded(ctx, tx); err != nil {
			return count, fmt.Errorf("failed to flush evicted keys batch: %w", err)
		}

		count++
	}

	if err := rows.Err(); err != nil {
		return count, fmt.Errorf("error iterating evicted keys: %w", err)
	}

	if err := batch.Flush(ctx, tx); err != nil {
		return count, fmt.Errorf("failed to flush remaining evicted keys: %w", err)
	}

	return count, nil
}

// transformRestoredKeys inserts restored key events for the ledger range (append-only event stream)
func (rt *RealtimeTransformer) transformRestoredKeys(ctx context.Context, tx *sql.Tx, startLedger, endLedger int64) (int64, error) {
	rows, err := rt.sourceManager.QueryRestoredKeys(ctx, startLedger, endLedger)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	batch := NewBatchInserter("restored_keys", restoredKeyColumns, restoredKeyConflict, rt.insertBatchSize())
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

		// Convert hex contract ID to C-encoded strkey
		// Note: bronze data may have empty contract_id for some restored keys
		if row.ContractID != "" {
			encoded, err := hexToStrKey(row.ContractID)
			if err != nil {
				log.Printf("warning: restored key hexToStrKey(%q): %v, keeping raw value", row.ContractID, err)
			} else {
				row.ContractID = encoded
			}
		}

		if err := batch.Add(row.Values()...); err != nil {
			return count, fmt.Errorf("failed to add restored key to batch: %w", err)
		}
		if err := batch.FlushIfNeeded(ctx, tx); err != nil {
			return count, fmt.Errorf("failed to flush restored keys batch: %w", err)
		}

		count++
	}

	if err := rows.Err(); err != nil {
		return count, fmt.Errorf("error iterating restored keys: %w", err)
	}

	if err := batch.Flush(ctx, tx); err != nil {
		return count, fmt.Errorf("failed to flush remaining restored keys: %w", err)
	}

	return count, nil
}

// =============================================================================
// Phase 4: Config Settings
// =============================================================================

// transformConfigSettingsCurrent upserts config settings current state for the ledger range
func (rt *RealtimeTransformer) transformConfigSettingsCurrent(ctx context.Context, tx *sql.Tx, startLedger, endLedger int64) (int64, error) {
	rows, err := rt.sourceManager.QueryConfigSettingsSnapshot(ctx, startLedger, endLedger)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	batch := NewBatchInserter("config_settings_current", configSettingsCurrentColumns, configSettingsCurrentConflict, rt.insertBatchSize())
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

		if err := batch.Add(row.Values()...); err != nil {
			return count, fmt.Errorf("failed to add row to %s batch: %w", batch.table, err)
		}
		if err := batch.FlushIfNeeded(ctx, tx); err != nil {
			return count, fmt.Errorf("failed to flush config settings batch: %w", err)
		}

		count++
	}

	if err := rows.Err(); err != nil {
		return count, fmt.Errorf("error iterating config settings: %w", err)
	}

	if err := batch.Flush(ctx, tx); err != nil {
		return count, fmt.Errorf("failed to flush remaining config settings: %w", err)
	}

	return count, nil
}

// =============================================================================
// Token Registry
// =============================================================================

// transformTokenRegistry materializes token metadata into the token_registry table
func (rt *RealtimeTransformer) transformTokenRegistry(ctx context.Context, tx *sql.Tx, startLedger, endLedger int64) (int64, error) {
	rows, err := rt.sourceManager.QueryTokenMetadataEntries(ctx, startLedger, endLedger)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	batch := NewBatchInserter("token_registry", tokenRegistryColumns, tokenRegistryConflict, rt.insertBatchSize())
	count := int64(0)

	for rows.Next() {
		var contractID string
		var tokenName, tokenSymbol, assetCode, assetIssuer sql.NullString
		var tokenDecimals sql.NullInt32
		var ledgerSequence int64

		err := rows.Scan(
			&contractID, &tokenName, &tokenSymbol, &tokenDecimals,
			&assetCode, &assetIssuer, &ledgerSequence,
		)
		if err != nil {
			return count, fmt.Errorf("failed to scan token metadata row: %w", err)
		}

		// Determine token type
		tokenType := "custom_soroban"
		if assetCode.Valid && assetCode.String != "" {
			tokenType = "sac"
		}

		// Determine decimals (default 7)
		decimals := 7
		if tokenDecimals.Valid {
			decimals = int(tokenDecimals.Int32)
		}

		row := &TokenRegistryRow{
			ContractID:         contractID,
			TokenName:          tokenName,
			TokenSymbol:        tokenSymbol,
			TokenDecimals:      decimals,
			AssetCode:          assetCode,
			AssetIssuer:        assetIssuer,
			TokenType:          tokenType,
			FirstSeenLedger:    ledgerSequence,
			LastModifiedLedger: ledgerSequence,
		}

		if err := batch.Add(row.Values()...); err != nil {
			return count, fmt.Errorf("failed to add row to %s batch: %w", batch.table, err)
		}
		if err := batch.FlushIfNeeded(ctx, tx); err != nil {
			return count, fmt.Errorf("failed to flush token registry batch: %w", err)
		}

		count++
	}

	if err := rows.Err(); err != nil {
		return count, fmt.Errorf("error iterating token metadata: %w", err)
	}

	if err := batch.Flush(ctx, tx); err != nil {
		return count, fmt.Errorf("failed to flush remaining token registry: %w", err)
	}

	return count, nil
}

// ============================================================================
// SEMANTIC LAYER TRANSFORMS (Silver-to-Silver)
// ============================================================================

// transformSemanticActivities materializes the unified activity feed from Silver hot tables.
// Uses INSERT-from-SELECT against enriched_history_operations with CASE-based activity_type derivation.
func (rt *RealtimeTransformer) transformSemanticActivities(ctx context.Context, tx *sql.Tx, startLedger, endLedger int64) (int64, error) {
	query := `
		INSERT INTO semantic_activities (
			id, ledger_sequence, timestamp, activity_type, description,
			source_account, destination_account, contract_id,
			asset_code, asset_issuer, amount,
			is_soroban, soroban_function_name,
			transaction_hash, operation_index, successful, fee_charged
		)
		SELECT
			e.transaction_hash || ':' || e.operation_index,
			e.ledger_sequence,
			COALESCE(e.ledger_closed_at, e.created_at),
			CASE
				WHEN e.type = 0 THEN 'account_created'
				WHEN e.type = 1 THEN 'payment'
				WHEN e.type = 2 THEN 'path_payment'
				WHEN e.type = 3 THEN 'manage_offer'
				WHEN e.type = 4 THEN 'create_passive_offer'
				WHEN e.type = 5 THEN 'set_options'
				WHEN e.type = 6 THEN 'change_trust'
				WHEN e.type = 7 THEN 'allow_trust'
				WHEN e.type = 8 THEN 'account_merge'
				WHEN e.type = 9 THEN 'inflation'
				WHEN e.type = 10 THEN 'manage_data'
				WHEN e.type = 11 THEN 'bump_sequence'
				WHEN e.type = 12 THEN 'manage_offer'
				WHEN e.type = 13 THEN 'path_payment'
				WHEN e.type = 21 THEN 'set_trust_line_flags'
				WHEN e.type = 24 AND e.function_name IS NOT NULL THEN 'contract_call'
				WHEN e.type = 24 THEN 'invoke_host_function'
				ELSE 'other'
			END,
			CASE
				WHEN e.type = 0 THEN 'Created account ' || COALESCE(e.destination, '')
				WHEN e.type = 1 THEN 'Sent ' || COALESCE(CAST(e.amount AS TEXT), '?') || ' ' || COALESCE(e.asset_code, 'XLM') || ' to ' || COALESCE(e.destination, '')
				WHEN e.type IN (2, 13) THEN 'Path payment ' || COALESCE(CAST(e.amount AS TEXT), '?') || ' ' || COALESCE(e.asset_code, 'XLM')
				WHEN e.type = 6 THEN CASE
					WHEN COALESCE(e.asset_code, '') <> '' THEN 'Added trustline for ' || e.asset_code
					ELSE 'Updated trustline'
				END
				WHEN e.type = 7 THEN 'Updated trust authorization'
				WHEN e.type = 8 THEN 'Merged into ' || COALESCE(e.into_account, '')
				WHEN e.type = 21 THEN 'Updated trustline flags'
				WHEN e.type = 24 AND e.function_name IS NOT NULL THEN 'Called ' || e.function_name || ' on ' || COALESCE(e.contract_id, '')
				WHEN e.type = 24 THEN 'Invoked host function'
				ELSE NULL
			END,
			e.source_account,
			COALESCE(e.destination, e.to_address),
			e.contract_id,
			e.asset_code,
			e.asset_issuer,
			e.amount,
			COALESCE(e.is_soroban_op, false),
			e.function_name,
			e.transaction_hash,
			e.operation_index,
			COALESCE(e.transaction_successful, false),
			e.tx_fee_charged
		FROM enriched_history_operations e
		WHERE e.ledger_sequence BETWEEN $1 AND $2
		ON CONFLICT (id) DO NOTHING
	`

	result, err := tx.ExecContext(ctx, query, startLedger, endLedger)
	if err != nil {
		return 0, fmt.Errorf("failed to insert semantic activities: %w", err)
	}

	count, _ := result.RowsAffected()
	return count, nil
}

// transformSemanticEntities upserts the contract entity registry from Silver hot tables.
// Merges contract_invocations_raw (for activity stats) + token_registry (for token metadata)
// + contract_metadata (for deployment info).
func (rt *RealtimeTransformer) transformSemanticEntities(ctx context.Context, tx *sql.Tx, startLedger, endLedger int64) (int64, error) {
	query := `
		INSERT INTO semantic_entities_contracts (
			contract_id, contract_type,
			token_name, token_symbol, token_decimals,
			deployer_account, deployed_at, deployed_ledger,
			total_invocations, last_activity, unique_callers,
			observed_functions, updated_at
		)
		SELECT
			ci.contract_id,
			COALESCE(tr.token_type, 'unknown'),
			tr.token_name,
			tr.token_symbol,
			tr.token_decimals,
			cm.creator_address,
			cm.created_at,
			cm.created_ledger,
			COUNT(*),
			MAX(ci.closed_at),
			COUNT(DISTINCT ci.source_account),
			ARRAY_AGG(DISTINCT ci.function_name),
			NOW()
		FROM contract_invocations_raw ci
		LEFT JOIN token_registry tr ON tr.contract_id = ci.contract_id
		LEFT JOIN contract_metadata cm ON cm.contract_id = ci.contract_id
		WHERE ci.ledger_sequence BETWEEN $1 AND $2
		GROUP BY ci.contract_id, tr.token_type, tr.token_name, tr.token_symbol, tr.token_decimals,
			cm.creator_address, cm.created_at, cm.created_ledger
		ON CONFLICT (contract_id) DO UPDATE SET
			contract_type = CASE
				WHEN EXCLUDED.contract_type != 'unknown' THEN EXCLUDED.contract_type
				ELSE semantic_entities_contracts.contract_type
			END,
			token_name = COALESCE(EXCLUDED.token_name, semantic_entities_contracts.token_name),
			token_symbol = COALESCE(EXCLUDED.token_symbol, semantic_entities_contracts.token_symbol),
			token_decimals = COALESCE(EXCLUDED.token_decimals, semantic_entities_contracts.token_decimals),
			deployer_account = COALESCE(EXCLUDED.deployer_account, semantic_entities_contracts.deployer_account),
			deployed_at = COALESCE(EXCLUDED.deployed_at, semantic_entities_contracts.deployed_at),
			deployed_ledger = COALESCE(EXCLUDED.deployed_ledger, semantic_entities_contracts.deployed_ledger),
			total_invocations = semantic_entities_contracts.total_invocations + EXCLUDED.total_invocations,
			last_activity = GREATEST(semantic_entities_contracts.last_activity, EXCLUDED.last_activity),
			unique_callers = GREATEST(semantic_entities_contracts.unique_callers, EXCLUDED.unique_callers),
			observed_functions = (
				SELECT ARRAY(SELECT DISTINCT f FROM unnest(
					COALESCE(semantic_entities_contracts.observed_functions, ARRAY[]::text[]) || COALESCE(EXCLUDED.observed_functions, ARRAY[]::text[])
				) AS f WHERE f IS NOT NULL)
			),
			updated_at = NOW()
	`

	result, err := tx.ExecContext(ctx, query, startLedger, endLedger)
	if err != nil {
		return 0, fmt.Errorf("failed to upsert semantic entities: %w", err)
	}

	count, _ := result.RowsAffected()
	return count, nil
}

// transformWalletClassification runs the extensible wallet detector framework on
// contracts that were active in this ledger batch. Checks for __check_auth in
// observed_functions and classifies wallet type using the detector registry.
// This runs as a post-processing step after transformSemanticEntities.
func (rt *RealtimeTransformer) transformWalletClassification(ctx context.Context, tx *sql.Tx, startLedger, endLedger int64) (int64, error) {
	// Find contracts active in this batch that have wallet-like evidence but no
	// wallet_type yet. Besides __check_auth, allow known wallet implementation
	// hashes so hash-classified wallets are materialized even if auth events or
	// admin functions were never observed upstream.
	candidateQuery := fmt.Sprintf(`
		SELECT sec.contract_id, sec.observed_functions
		FROM semantic_entities_contracts sec
		JOIN contract_invocations_raw ci ON ci.contract_id = sec.contract_id
		LEFT JOIN contract_metadata cm ON cm.contract_id = sec.contract_id
		WHERE ci.ledger_sequence BETWEEN $1 AND $2
		  AND sec.wallet_type IS NULL
		  AND (
			sec.observed_functions && ARRAY['__check_auth']
			OR EXISTS (
				SELECT 1 FROM contract_invocations_raw ci2
				WHERE ci2.contract_id = sec.contract_id
				AND ci2.function_name = '__check_auth'
			)
			OR cm.wasm_hash IN (%s)
		  )
		GROUP BY sec.contract_id, sec.observed_functions
	`, transformerKnownWalletHashesSQLList())

	rows, err := tx.QueryContext(ctx, candidateQuery, startLedger, endLedger)
	if err != nil {
		return 0, fmt.Errorf("wallet candidate query: %w", err)
	}

	type candidate struct {
		contractID string
		functions  []string
	}
	var candidates []candidate
	for rows.Next() {
		var c candidate
		var funcs []byte // PostgreSQL text[] scans as []byte
		if err := rows.Scan(&c.contractID, &funcs); err != nil {
			continue
		}
		// Parse PostgreSQL array format {a,b,c}
		c.functions = parsePostgresArray(string(funcs))
		candidates = append(candidates, c)
	}
	rows.Close()

	if len(candidates) == 0 {
		return 0, nil
	}

	registry := newTransformerWalletRegistry()
	var classified int64

	for _, c := range candidates {
		// Assemble evidence from silver hot tables
		evidence := walletEvidenceFromSilver(ctx, tx, c.contractID, c.functions)

		result := registry.detect(evidence)
		if result == nil {
			continue
		}

		// Update semantic_entities_contracts with wallet classification
		var signersJSON []byte
		if len(result.signers) > 0 {
			signersJSON, _ = json.Marshal(result.signers)
		}

		updateQuery := `
			UPDATE semantic_entities_contracts
			SET contract_type = 'smart_wallet',
			    wallet_type = $2,
			    wallet_signers = $3::jsonb,
			    updated_at = NOW()
			WHERE contract_id = $1
		`
		var signersStr *string
		if signersJSON != nil {
			s := string(signersJSON)
			signersStr = &s
		}
		if _, err := tx.ExecContext(ctx, updateQuery, c.contractID, string(result.walletType), signersStr); err != nil {
			log.Printf("⚠️  Failed to classify wallet %s: %v", c.contractID, err)
			continue
		}
		classified++
	}

	return classified, nil
}

// Lightweight wallet detection types for the transformer (avoids importing query-api types)
type transformerWalletType string

const (
	twCrossmint    transformerWalletType = "crossmint"
	twOpenZeppelin transformerWalletType = "openzeppelin"
	twSEP50Generic transformerWalletType = "sep50_generic"
)

type transformerWalletResult struct {
	walletType transformerWalletType
	signers    []map[string]string
}

type transformerWalletEvidence struct {
	contractID        string
	wasmHash          string
	observedFunctions []string
	instanceStorage   []struct{ keyHash, dataValue string }
}

type transformerWalletRegistry struct{}

func newTransformerWalletRegistry() *transformerWalletRegistry {
	return &transformerWalletRegistry{}
}

func (r *transformerWalletRegistry) detect(evidence transformerWalletEvidence) *transformerWalletResult {
	if walletType, ok := transformerKnownWalletTypeByWasmHash(evidence.wasmHash); ok {
		return &transformerWalletResult{walletType: walletType}
	}

	// Crossmint: signer type tags in instance storage
	for _, entry := range evidence.instanceStorage {
		for _, tag := range []string{"Ed25519", "Secp256k1", "Secp256r1", "WebAuthn", "Passkey"} {
			if strings.Contains(entry.dataValue, tag) {
				return &transformerWalletResult{
					walletType: twCrossmint,
					signers:    extractCrossmintSigners(evidence.instanceStorage),
				}
			}
		}
	}

	// OpenZeppelin: signer management functions
	ozFuncs := map[string]bool{"add_signer": true, "remove_signer": true, "set_signer": true, "add_guardian": true, "recover": true}
	for _, fn := range evidence.observedFunctions {
		if ozFuncs[fn] {
			return &transformerWalletResult{walletType: twOpenZeppelin}
		}
	}
	// OZ: owner/guardian storage without Crossmint tags
	for _, entry := range evidence.instanceStorage {
		lower := strings.ToLower(entry.dataValue)
		if (strings.Contains(lower, "owner") || strings.Contains(lower, "guardian")) &&
			!strings.Contains(lower, "ed25519") && !strings.Contains(lower, "secp256") {
			return &transformerWalletResult{walletType: twOpenZeppelin}
		}
	}

	// SEP-50 fallback: any contract that made it here has __check_auth
	return &transformerWalletResult{walletType: twSEP50Generic}
}

func extractCrossmintSigners(storage []struct{ keyHash, dataValue string }) []map[string]string {
	var signers []map[string]string
	for _, entry := range storage {
		for _, tag := range []string{"Ed25519", "Secp256k1", "Secp256r1", "WebAuthn", "Passkey"} {
			if strings.Contains(entry.dataValue, tag) {
				keyType := strings.ToLower(tag)
				if keyType == "passkey" {
					keyType = "webauthn"
				}
				signers = append(signers, map[string]string{
					"id":       entry.keyHash,
					"key_type": keyType,
				})
				break
			}
		}
	}
	return signers
}

func walletEvidenceFromSilver(ctx context.Context, tx *sql.Tx, contractID string, functions []string) transformerWalletEvidence {
	evidence := transformerWalletEvidence{
		contractID:        contractID,
		observedFunctions: functions,
	}

	var wasmHash sql.NullString
	if err := tx.QueryRowContext(ctx, `
		SELECT wasm_hash
		FROM contract_metadata
		WHERE contract_id = $1
	`, contractID).Scan(&wasmHash); err == nil && wasmHash.Valid {
		evidence.wasmHash = wasmHash.String
	}

	// Get instance storage
	storageRows, err := tx.QueryContext(ctx,
		`SELECT key_hash, data_value FROM contract_data_current
		 WHERE contract_id = $1 AND durability = 'instance'`, contractID)
	if err == nil {
		for storageRows.Next() {
			var entry struct{ keyHash, dataValue string }
			var dv sql.NullString
			if storageRows.Scan(&entry.keyHash, &dv) == nil && dv.Valid {
				entry.dataValue = dv.String
				evidence.instanceStorage = append(evidence.instanceStorage, entry)
			}
		}
		storageRows.Close()
	}

	return evidence
}

func parsePostgresArray(s string) []string {
	s = strings.TrimPrefix(s, "{")
	s = strings.TrimSuffix(s, "}")
	if s == "" {
		return nil
	}
	return strings.Split(s, ",")
}

// transformSemanticFlows materializes normalized value transfers from Silver hot tables.
// Combines token_transfers_raw (classic + Soroban) into a unified flow format.
func (rt *RealtimeTransformer) transformSemanticFlows(ctx context.Context, tx *sql.Tx, startLedger, endLedger int64) (int64, error) {
	query := `
		INSERT INTO semantic_flows_value (
			id, ledger_sequence, timestamp, flow_type,
			from_account, to_account, contract_id,
			asset_code, asset_issuer, asset_type,
			amount, transaction_hash, operation_type, successful
		)
		SELECT
			t.transaction_hash || ':' || ROW_NUMBER() OVER (PARTITION BY t.transaction_hash ORDER BY t.from_account, t.to_account),
			t.ledger_sequence,
			t.timestamp,
			CASE
				WHEN t.from_account IS NULL OR t.from_account = '' THEN 'mint'
				WHEN t.to_account IS NULL OR t.to_account = '' THEN 'burn'
				ELSE 'transfer'
			END,
			t.from_account,
			t.to_account,
			t.token_contract_id,
			t.asset_code,
			t.asset_issuer,
			CASE
				WHEN t.source_type = 'soroban' THEN 'soroban_token'
				WHEN t.asset_code IS NULL OR t.asset_code = '' THEN 'native'
				WHEN LENGTH(t.asset_code) <= 4 THEN 'credit_alphanum4'
				ELSE 'credit_alphanum12'
			END,
			COALESCE(t.amount, 0),
			t.transaction_hash,
			t.operation_type,
			COALESCE(t.transaction_successful, false)
		FROM token_transfers_raw t
		WHERE t.ledger_sequence BETWEEN $1 AND $2
		  AND t.amount IS NOT NULL
		ON CONFLICT (id) DO NOTHING
	`

	result, err := tx.ExecContext(ctx, query, startLedger, endLedger)
	if err != nil {
		return 0, fmt.Errorf("failed to insert semantic flows: %w", err)
	}

	count, _ := result.RowsAffected()
	return count, nil
}

// ============================================================================
// SEMANTIC LAYER PHASE 2 TRANSFORMS
// ============================================================================

// transformSemanticContractFunctions upserts per-function call stats from contract_invocations_raw.
func (rt *RealtimeTransformer) transformAddressBalancesCurrent(ctx context.Context, tx *sql.Tx, startLedger, endLedger int64) (int64, error) {
	deleteQuery := `
		DELETE FROM address_balances_current
		WHERE owner_address IN (
			SELECT owner_address FROM (
				SELECT DISTINCT from_account AS owner_address
				FROM token_transfers_raw
				WHERE ledger_sequence BETWEEN $1 AND $2 AND from_account IS NOT NULL AND from_account != ''
				UNION
				SELECT DISTINCT to_account AS owner_address
				FROM token_transfers_raw
				WHERE ledger_sequence BETWEEN $1 AND $2 AND to_account IS NOT NULL AND to_account != ''
				UNION
				SELECT DISTINCT account_id AS owner_address
				FROM native_balances_current
				WHERE last_modified_ledger BETWEEN $1 AND $2
			) impacted
		)
	`
	if _, err := tx.ExecContext(ctx, deleteQuery, startLedger, endLedger); err != nil {
		return 0, fmt.Errorf("failed to delete impacted address balances: %w", err)
	}

	insertNative := `
		INSERT INTO address_balances_current (
			owner_address, asset_key, asset_type, token_contract_id,
			asset_code, asset_issuer, symbol, decimals,
			balance_raw, balance_display, balance_source,
			last_updated_ledger, last_updated_at, updated_at
		)
		SELECT
			nb.account_id,
			'native',
			'native',
			NULL,
			'XLM',
			NULL,
			'XLM',
			7,
			nb.balance,
			(n.bal/10000000.0)::text,
			'classic_account_state',
			nb.last_modified_ledger,
			NULL,
			NOW()
		FROM native_balances_current nb
		CROSS JOIN LATERAL (SELECT nb.balance::numeric AS bal) n
		WHERE nb.account_id IN (
			SELECT owner_address FROM (
				SELECT DISTINCT from_account AS owner_address FROM token_transfers_raw WHERE ledger_sequence BETWEEN $1 AND $2 AND from_account IS NOT NULL AND from_account != ''
				UNION
				SELECT DISTINCT to_account AS owner_address FROM token_transfers_raw WHERE ledger_sequence BETWEEN $1 AND $2 AND to_account IS NOT NULL AND to_account != ''
				UNION
				SELECT DISTINCT account_id AS owner_address FROM native_balances_current WHERE last_modified_ledger BETWEEN $1 AND $2
			) impacted
		)
		  AND COALESCE(nb.balance, 0) > 0
	`
	if _, err := tx.ExecContext(ctx, insertNative, startLedger, endLedger); err != nil {
		return 0, fmt.Errorf("failed to insert native address balances: %w", err)
	}

	insertTokens := `
		WITH impacted AS (
			SELECT DISTINCT from_account AS owner_address
			FROM token_transfers_raw
			WHERE ledger_sequence BETWEEN $1 AND $2 AND from_account IS NOT NULL AND from_account != ''
			UNION
			SELECT DISTINCT to_account AS owner_address
			FROM token_transfers_raw
			WHERE ledger_sequence BETWEEN $1 AND $2 AND to_account IS NOT NULL AND to_account != ''
		), dedup AS (
			SELECT DISTINCT ON (t.transaction_hash, t.ledger_sequence, COALESCE(t.from_account,''), COALESCE(t.to_account,''), COALESCE(t.token_contract_id,''), COALESCE(t.amount,0), COALESCE(t.event_index,-1))
				t.transaction_hash, t.ledger_sequence, t.timestamp, t.from_account, t.to_account,
				t.token_contract_id, t.asset_code, t.asset_issuer, t.amount, t.event_index
			FROM token_transfers_raw t
			WHERE t.transaction_successful = true
			  AND EXISTS (
				SELECT 1
				FROM impacted i
				WHERE i.owner_address = t.from_account OR i.owner_address = t.to_account
			  )
			ORDER BY t.transaction_hash, t.ledger_sequence, COALESCE(t.from_account,''), COALESCE(t.to_account,''), COALESCE(t.token_contract_id,''), COALESCE(t.amount,0), COALESCE(t.event_index,-1)
		), balances AS (
			SELECT
				addr.owner_address,
				COALESCE(d.token_contract_id, CASE WHEN COALESCE(d.asset_code,'') = '' THEN 'native' ELSE d.asset_code || ':' || COALESCE(d.asset_issuer,'') END) AS asset_key,
				MAX(d.token_contract_id) AS token_contract_id,
				MAX(d.asset_code) AS asset_code,
				MAX(d.asset_issuer) AS asset_issuer,
				SUM(CASE WHEN d.to_account = addr.owner_address THEN COALESCE(d.amount,0) ELSE 0 END) -
				SUM(CASE WHEN d.from_account = addr.owner_address THEN COALESCE(d.amount,0) ELSE 0 END) AS balance_raw,
				MAX(d.ledger_sequence) AS last_updated_ledger,
				MAX(d.timestamp) AS last_updated_at
			FROM impacted addr
			JOIN dedup d ON d.from_account = addr.owner_address OR d.to_account = addr.owner_address
			GROUP BY addr.owner_address,
				COALESCE(d.token_contract_id, CASE WHEN COALESCE(d.asset_code,'') = '' THEN 'native' ELSE d.asset_code || ':' || COALESCE(d.asset_issuer,'') END)
		)
		INSERT INTO address_balances_current (
			owner_address, asset_key, asset_type, token_contract_id,
			asset_code, asset_issuer, symbol, decimals,
			balance_raw, balance_display, balance_source,
			last_updated_ledger, last_updated_at, updated_at
		)
		SELECT
			b.owner_address,
			b.asset_key,
			CASE WHEN b.token_contract_id IS NOT NULL THEN COALESCE(tr.token_type, 'soroban_token')
			     WHEN COALESCE(b.asset_code,'') = '' THEN 'native'
			     WHEN LENGTH(b.asset_code) <= 4 THEN 'credit_alphanum4'
			     ELSE 'credit_alphanum12' END,
			b.token_contract_id,
			COALESCE(b.asset_code, CASE WHEN b.token_contract_id IS NULL THEN 'XLM' END),
			b.asset_issuer,
			COALESCE(tr.token_symbol, b.asset_code, CASE WHEN b.token_contract_id IS NULL AND COALESCE(b.asset_code,'')='' THEN 'XLM' END),
			COALESCE(tr.token_decimals, CASE WHEN b.token_contract_id IS NULL THEN 7 ELSE 7 END),
			b.balance_raw,
			CASE
				WHEN COALESCE(tr.token_decimals, 7) = 0 THEN b.balance_raw::text
				ELSE (b.balance_raw / POWER(10::numeric, COALESCE(tr.token_decimals, 7)))::text
			END,
			CASE WHEN b.token_contract_id IS NOT NULL THEN 'indexed_transfer_history' ELSE 'classic_transfer_history' END,
			b.last_updated_ledger,
			b.last_updated_at,
			NOW()
		FROM balances b
		LEFT JOIN token_registry tr ON tr.contract_id = b.token_contract_id
		WHERE b.balance_raw > 0
		  AND NOT (b.asset_key = 'native' AND EXISTS (SELECT 1 FROM native_balances_current nb WHERE nb.account_id = b.owner_address))
	`
	result, err := tx.ExecContext(ctx, insertTokens, startLedger, endLedger)
	if err != nil {
		return 0, fmt.Errorf("failed to insert token address balances: %w", err)
	}
	count, _ := result.RowsAffected()
	return count, nil
}

// transformAddressBalancesFromContractState populates address_balances_current
// from bronze contract_data_snapshot_v1 where the ingester decoded a
// Balance(Address) entry. This gives an authoritative state-based source for
// C... holdings (including native XLM SAC balances that the previous cycle's
// event-aggregation path couldn't fully capture).
//
// State rows win over event-aggregated rows ON CONFLICT when the state entry
// has a newer last_updated_ledger — the ledger-entry snapshot is the
// canonical on-chain truth.
func (rt *RealtimeTransformer) transformAddressBalancesFromContractState(ctx context.Context, tx *sql.Tx, startLedger, endLedger int64) (int64, error) {
	rows, err := rt.sourceManager.QueryBalanceHolderSnapshots(ctx, startLedger, endLedger)
	if err != nil {
		return 0, fmt.Errorf("query balance holder snapshots: %w", err)
	}
	defer rows.Close()

	batchSize := rt.insertBatchSize()
	pending := make([]AddressBalanceStateStageRow, 0, batchSize)
	flush := func() error {
		if len(pending) == 0 {
			return nil
		}
		if err := rt.silverWriter.BulkUpsertAddressBalanceState(ctx, tx, pending, batchSize); err != nil {
			return fmt.Errorf("bulk upsert contract_storage_state balances: %w", err)
		}
		pending = pending[:0]
		return nil
	}

	var count int64
	for rows.Next() {
		var (
			contractID         string
			balanceHolder      string
			balance            string
			assetType          sql.NullString
			assetCode          sql.NullString
			assetIssuer        sql.NullString
			lastModifiedLedger int64
			closedAt           time.Time
		)
		if err := rows.Scan(
			&contractID, &balanceHolder, &balance,
			&assetType, &assetCode, &assetIssuer,
			&lastModifiedLedger, &closedAt,
		); err != nil {
			return count, fmt.Errorf("scan balance holder row: %w", err)
		}

		pending = append(pending, AddressBalanceStateStageRow{
			OwnerAddress:      balanceHolder,
			AssetKey:          contractID,
			AssetTypeHint:     assetType.String,
			AssetCode:         assetCode.String,
			AssetIssuer:       assetIssuer.String,
			BalanceRaw:        balance,
			LastUpdatedLedger: lastModifiedLedger,
			LastUpdatedAt:     closedAt,
		})
		count++

		if len(pending) >= batchSize {
			if err := flush(); err != nil {
				return count, err
			}
		}
	}
	if err := rows.Err(); err != nil {
		return count, fmt.Errorf("iterate balance holder rows: %w", err)
	}
	if err := flush(); err != nil {
		return count, err
	}
	return count, nil
}

func (rt *RealtimeTransformer) transformSemanticContractFunctions(ctx context.Context, tx *sql.Tx, startLedger, endLedger int64) (int64, error) {
	query := `
		INSERT INTO semantic_contract_functions (
			contract_id, function_name,
			total_calls, successful_calls, failed_calls, unique_callers,
			first_called, last_called, updated_at
		)
		SELECT
			contract_id, function_name,
			COUNT(*),
			COUNT(*) FILTER (WHERE successful),
			COUNT(*) FILTER (WHERE NOT successful),
			COUNT(DISTINCT source_account),
			MIN(closed_at), MAX(closed_at), NOW()
		FROM contract_invocations_raw
		WHERE ledger_sequence BETWEEN $1 AND $2
		  AND function_name IS NOT NULL AND function_name != ''
		GROUP BY contract_id, function_name
		ON CONFLICT (contract_id, function_name) DO UPDATE SET
			total_calls = semantic_contract_functions.total_calls + EXCLUDED.total_calls,
			successful_calls = semantic_contract_functions.successful_calls + EXCLUDED.successful_calls,
			failed_calls = semantic_contract_functions.failed_calls + EXCLUDED.failed_calls,
			unique_callers = GREATEST(semantic_contract_functions.unique_callers, EXCLUDED.unique_callers),
			last_called = GREATEST(semantic_contract_functions.last_called, EXCLUDED.last_called),
			first_called = LEAST(semantic_contract_functions.first_called, EXCLUDED.first_called),
			updated_at = NOW()
	`

	result, err := tx.ExecContext(ctx, query, startLedger, endLedger)
	if err != nil {
		return 0, fmt.Errorf("failed to upsert semantic contract functions: %w", err)
	}

	count, _ := result.RowsAffected()
	return count, nil
}

// transformSemanticAssetStats upserts asset transfer stats from token_transfers_raw.
// Per-batch: increments transfer counts and volumes for assets seen in the batch.
// Time-windowed stats (24h/7d) and holder counts are handled by periodic refresh (not per-batch).
func (rt *RealtimeTransformer) transformSemanticAssetStats(ctx context.Context, tx *sql.Tx, startLedger, endLedger int64) (int64, error) {
	query := `
		INSERT INTO semantic_asset_stats (
			asset_key, asset_code, asset_issuer, asset_type,
			token_name, token_symbol, token_decimals, contract_id,
			transfer_count_24h, transfer_volume_24h,
			first_seen, last_transfer, updated_at
		)
		SELECT
			CASE
				WHEN t.source_type = 'soroban' THEN t.token_contract_id
				WHEN t.asset_code IS NULL OR t.asset_code = '' THEN 'native'
				ELSE t.asset_code || ':' || COALESCE(t.asset_issuer, '')
			END,
			t.asset_code,
			t.asset_issuer,
			CASE
				WHEN t.source_type = 'soroban' THEN 'soroban_token'
				WHEN t.asset_code IS NULL OR t.asset_code = '' THEN 'native'
				WHEN LENGTH(t.asset_code) <= 4 THEN 'credit_alphanum4'
				ELSE 'credit_alphanum12'
			END,
			tr.token_name,
			tr.token_symbol,
			tr.token_decimals,
			t.token_contract_id,
			COUNT(*),
			COALESCE(SUM(t.amount), 0),
			MIN(t.timestamp),
			MAX(t.timestamp),
			NOW()
		FROM token_transfers_raw t
		LEFT JOIN token_registry tr ON tr.contract_id = t.token_contract_id
		WHERE t.ledger_sequence BETWEEN $1 AND $2
		  AND NOT (t.source_type = 'soroban' AND t.token_contract_id IS NULL)
		GROUP BY
			CASE
				WHEN t.source_type = 'soroban' THEN t.token_contract_id
				WHEN t.asset_code IS NULL OR t.asset_code = '' THEN 'native'
				ELSE t.asset_code || ':' || COALESCE(t.asset_issuer, '')
			END,
			t.asset_code, t.asset_issuer, t.source_type, t.token_contract_id,
			tr.token_name, tr.token_symbol, tr.token_decimals
		ON CONFLICT (asset_key) DO UPDATE SET
			token_name = COALESCE(EXCLUDED.token_name, semantic_asset_stats.token_name),
			token_symbol = COALESCE(EXCLUDED.token_symbol, semantic_asset_stats.token_symbol),
			token_decimals = COALESCE(EXCLUDED.token_decimals, semantic_asset_stats.token_decimals),
			transfer_count_24h = semantic_asset_stats.transfer_count_24h + EXCLUDED.transfer_count_24h,
			transfer_volume_24h = semantic_asset_stats.transfer_volume_24h + EXCLUDED.transfer_volume_24h,
			last_transfer = GREATEST(semantic_asset_stats.last_transfer, EXCLUDED.last_transfer),
			first_seen = LEAST(semantic_asset_stats.first_seen, EXCLUDED.first_seen),
			updated_at = NOW()
	`

	result, err := tx.ExecContext(ctx, query, startLedger, endLedger)
	if err != nil {
		return 0, fmt.Errorf("failed to upsert semantic asset stats: %w", err)
	}

	count, _ := result.RowsAffected()
	return count, nil
}

// transformSemanticDexPairs upserts DEX trading pair stats from trades table.
func (rt *RealtimeTransformer) transformSemanticDexPairs(ctx context.Context, tx *sql.Tx, startLedger, endLedger int64) (int64, error) {
	query := `
		INSERT INTO semantic_dex_pairs (
			pair_key, selling_asset_code, selling_asset_issuer,
			buying_asset_code, buying_asset_issuer,
			trade_count, selling_volume, buying_volume,
			last_price, unique_sellers, unique_buyers,
			first_trade, last_trade, updated_at
		)
		SELECT
			COALESCE(selling_asset_code, 'XLM') || ':' || COALESCE(selling_asset_issuer, 'native')
			  || '/' || COALESCE(buying_asset_code, 'XLM') || ':' || COALESCE(buying_asset_issuer, 'native'),
			selling_asset_code, selling_asset_issuer,
			buying_asset_code, buying_asset_issuer,
			COUNT(*),
			SUM(selling_amount), SUM(buying_amount),
			(ARRAY_AGG(price ORDER BY trade_timestamp DESC))[1],
			COUNT(DISTINCT seller_account),
			COUNT(DISTINCT buyer_account),
			MIN(trade_timestamp), MAX(trade_timestamp), NOW()
		FROM trades
		WHERE ledger_sequence BETWEEN $1 AND $2
		GROUP BY selling_asset_code, selling_asset_issuer, buying_asset_code, buying_asset_issuer
		ON CONFLICT (pair_key) DO UPDATE SET
			trade_count = semantic_dex_pairs.trade_count + EXCLUDED.trade_count,
			selling_volume = semantic_dex_pairs.selling_volume + EXCLUDED.selling_volume,
			buying_volume = semantic_dex_pairs.buying_volume + EXCLUDED.buying_volume,
			last_price = EXCLUDED.last_price,
			unique_sellers = GREATEST(semantic_dex_pairs.unique_sellers, EXCLUDED.unique_sellers),
			unique_buyers = GREATEST(semantic_dex_pairs.unique_buyers, EXCLUDED.unique_buyers),
			last_trade = GREATEST(semantic_dex_pairs.last_trade, EXCLUDED.last_trade),
			first_trade = LEAST(semantic_dex_pairs.first_trade, EXCLUDED.first_trade),
			updated_at = NOW()
	`

	result, err := tx.ExecContext(ctx, query, startLedger, endLedger)
	if err != nil {
		return 0, fmt.Errorf("failed to upsert semantic dex pairs: %w", err)
	}

	count, _ := result.RowsAffected()
	return count, nil
}

// transformSemanticAccountSummary upserts account activity stats from enriched_history_operations
// and contract_invocations_raw for accounts active in the current batch.
func (rt *RealtimeTransformer) transformSemanticAccountSummary(ctx context.Context, tx *sql.Tx, startLedger, endLedger int64) (int64, error) {
	query := `
		INSERT INTO semantic_account_summary (
			account_id, total_operations,
			total_payments_sent, total_payments_received,
			total_contract_calls, unique_contracts_called,
			top_contract_id, top_contract_function,
			is_contract_deployer, contracts_deployed,
			first_activity, last_activity, updated_at
		)
		SELECT
			e.source_account,
			COUNT(*),
			COUNT(*) FILTER (WHERE e.is_payment_op),
			COUNT(*) FILTER (WHERE e.is_payment_op AND e.destination IS NOT NULL AND e.destination = e.source_account),
			COUNT(*) FILTER (WHERE e.type = 24),
			COUNT(DISTINCT e.contract_id) FILTER (WHERE e.type = 24),
			(ARRAY_AGG(e.contract_id ORDER BY e.ledger_sequence DESC) FILTER (WHERE e.type = 24))[1],
			(ARRAY_AGG(e.function_name ORDER BY e.ledger_sequence DESC) FILTER (WHERE e.type = 24))[1],
			BOOL_OR(e.host_function_type = 'CreateContract'),
			COUNT(*) FILTER (WHERE e.host_function_type = 'CreateContract'),
			MIN(COALESCE(e.ledger_closed_at, e.created_at)),
			MAX(COALESCE(e.ledger_closed_at, e.created_at)),
			NOW()
		FROM enriched_history_operations e
		WHERE e.ledger_sequence BETWEEN $1 AND $2
		  AND e.source_account IS NOT NULL AND e.source_account != ''
		GROUP BY e.source_account
		ON CONFLICT (account_id) DO UPDATE SET
			total_operations = semantic_account_summary.total_operations + EXCLUDED.total_operations,
			total_payments_sent = semantic_account_summary.total_payments_sent + EXCLUDED.total_payments_sent,
			total_payments_received = semantic_account_summary.total_payments_received + EXCLUDED.total_payments_received,
			total_contract_calls = semantic_account_summary.total_contract_calls + EXCLUDED.total_contract_calls,
			unique_contracts_called = GREATEST(semantic_account_summary.unique_contracts_called, EXCLUDED.unique_contracts_called),
			top_contract_id = COALESCE(EXCLUDED.top_contract_id, semantic_account_summary.top_contract_id),
			top_contract_function = COALESCE(EXCLUDED.top_contract_function, semantic_account_summary.top_contract_function),
			is_contract_deployer = semantic_account_summary.is_contract_deployer OR EXCLUDED.is_contract_deployer,
			contracts_deployed = semantic_account_summary.contracts_deployed + EXCLUDED.contracts_deployed,
			last_activity = GREATEST(semantic_account_summary.last_activity, EXCLUDED.last_activity),
			first_activity = LEAST(semantic_account_summary.first_activity, EXCLUDED.first_activity),
			updated_at = NOW()
	`

	result, err := tx.ExecContext(ctx, query, startLedger, endLedger)
	if err != nil {
		return 0, fmt.Errorf("failed to upsert semantic account summary: %w", err)
	}

	count, _ := result.RowsAffected()
	return count, nil
}
