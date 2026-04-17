package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"sync"
)

// SourceMode represents the current data source mode
type SourceMode string

const (
	// SourceModeHot reads from Bronze Hot (PostgreSQL) - normal operation
	SourceModeHot SourceMode = "hot"
	// SourceModeBackfill reads from Bronze Cold (DuckLake) - catching up
	SourceModeBackfill SourceMode = "backfill"
)

// SourceManager manages switching between hot and cold Bronze storage
// It implements a state machine that detects gaps and switches to backfill mode
type SourceManager struct {
	hotReader  *BronzeReader
	coldReader *BronzeColdReader

	mode           SourceMode
	hotMinLedger   int64 // Minimum ledger available in hot storage
	hotMaxLedger   int64 // Maximum ledger available in hot storage
	coldMaxLedger  int64 // Maximum ledger available in cold storage
	backfillTarget int64 // Ledger we're trying to reach before switching back to hot

	fallbackEnabled bool
	replayMode      bool // When true, suppress automatic mode switching in CheckAndSwitchMode
	mu              sync.RWMutex
}

// NewSourceManager creates a new source manager
// If coldReader is nil, fallback is disabled and only hot storage is used
func NewSourceManager(hotReader *BronzeReader, coldReader *BronzeColdReader, fallbackEnabled bool) *SourceManager {
	return &SourceManager{
		hotReader:       hotReader,
		coldReader:      coldReader,
		mode:            SourceModeHot,
		fallbackEnabled: fallbackEnabled && coldReader != nil,
	}
}

// GetMode returns the current source mode
func (sm *SourceManager) GetMode() SourceMode {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	return sm.mode
}

// GetBackfillProgress returns backfill progress information
func (sm *SourceManager) GetBackfillProgress(currentCheckpoint int64) (target int64, progress float64) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	if sm.mode != SourceModeBackfill {
		return 0, 0
	}

	target = sm.backfillTarget
	if target == 0 {
		return 0, 0
	}

	// Calculate progress as percentage
	// Progress = (currentCheckpoint - startOfBackfill) / (target - startOfBackfill)
	// We don't track startOfBackfill, so use a simpler calculation
	if currentCheckpoint >= target {
		return target, 100.0
	}

	// Use cold max as reference for total range
	if sm.coldMaxLedger > 0 && sm.coldMaxLedger > currentCheckpoint {
		totalRange := sm.coldMaxLedger - currentCheckpoint
		completed := target - currentCheckpoint
		if totalRange > 0 {
			progress = float64(completed) / float64(totalRange) * 100.0
		}
	}

	return target, progress
}

// GetHotMinLedger returns the minimum ledger in hot storage
func (sm *SourceManager) GetHotMinLedger() int64 {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	return sm.hotMinLedger
}

// RefreshLedgerRanges updates the known ledger ranges from both sources
func (sm *SourceManager) RefreshLedgerRanges(ctx context.Context) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	// Always refresh hot storage range
	hotMin, err := sm.hotReader.GetMinLedgerSequence(ctx)
	if err != nil {
		return fmt.Errorf("failed to get hot min ledger: %w", err)
	}
	sm.hotMinLedger = hotMin

	hotMax, err := sm.hotReader.GetMaxLedgerSequence(ctx)
	if err != nil {
		return fmt.Errorf("failed to get hot max ledger: %w", err)
	}
	sm.hotMaxLedger = hotMax

	// Refresh cold storage range if available
	if sm.coldReader != nil {
		coldMax, err := sm.coldReader.GetMaxLedgerSequence(ctx)
		if err != nil {
			log.Printf("⚠️  Failed to get cold max ledger (cold storage may be unavailable): %v", err)
			// Don't fail - cold storage is optional
		} else {
			sm.coldMaxLedger = coldMax
		}
	}

	return nil
}

// ForceBackfillMode forces the SourceManager into backfill mode with a fixed target.
// Used by cold replay to bypass gap detection. Also enables replayMode which
// suppresses automatic mode switching back to hot.
func (sm *SourceManager) ForceBackfillMode(target int64) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.mode = SourceModeBackfill
	sm.backfillTarget = target
	sm.replayMode = true
	sm.fallbackEnabled = true
}

// CheckAndSwitchMode evaluates if we need to switch modes based on current state
// Returns true if mode was switched
func (sm *SourceManager) CheckAndSwitchMode(ctx context.Context, currentCheckpoint int64, dataFound bool) (bool, error) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	// In replay mode, never auto-switch — caller controls the mode
	if sm.replayMode {
		return false, nil
	}

	nextLedger := currentCheckpoint + 1

	switch sm.mode {
	case SourceModeHot:
		// If data was found, we're good - stay in hot mode
		if dataFound {
			return false, nil
		}

		// No data found - check if there's a gap
		if !sm.fallbackEnabled {
			// Fallback disabled - can't switch to cold
			return false, nil
		}

		// Find the next available ledger at or after our checkpoint.
		// This handles sparse hot buffers where MIN(sequence) may be far below
		// the checkpoint but the actual data near the checkpoint has been flushed.
		nextAvailable, err := sm.hotReader.GetNextAvailableLedger(ctx, nextLedger)
		if err != nil {
			return false, fmt.Errorf("failed to find next available ledger: %w", err)
		}

		// Also refresh the global hot min for other callers
		hotMin, err := sm.hotReader.GetMinLedgerSequence(ctx)
		if err == nil {
			sm.hotMinLedger = hotMin
		}

		// Gap detected: next available ledger in hot is beyond what we need
		if nextAvailable == 0 || nextAvailable > nextLedger {
			// We have a gap - need to backfill from cold
			if nextAvailable > 0 {
				// Set backfill target to one less than next available
				sm.backfillTarget = nextAvailable - 1
			} else {
				// No data at all in hot after our checkpoint — backfill everything cold has
				sm.backfillTarget = 0 // will be set from cold max below
			}

			// Verify cold storage has the data we need
			if sm.coldReader == nil {
				log.Printf("🔴 GAP DETECTED but cold reader not available. Next available in hot: %d, Checkpoint: %d", nextAvailable, currentCheckpoint)
				return false, nil
			}

			coldMax, err := sm.coldReader.GetMaxLedgerSequence(ctx)
			if err != nil {
				log.Printf("⚠️  Failed to check cold storage: %v", err)
				return false, nil
			}
			sm.coldMaxLedger = coldMax

			if coldMax < nextLedger {
				log.Printf("🔴 GAP DETECTED but cold storage doesn't have required data. Cold MAX: %d, Need: %d", coldMax, nextLedger)
				return false, nil
			}

			// If no hot data exists after checkpoint, backfill up to cold max
			if sm.backfillTarget == 0 {
				sm.backfillTarget = coldMax
			}

			// Switch to backfill mode
			sm.mode = SourceModeBackfill
			log.Printf("🔄 MODE SWITCH: HOT → BACKFILL")
			log.Printf("   Gap detected: next available hot ledger (%d) > needed ledger (%d)", nextAvailable, nextLedger)
			log.Printf("   Backfill target: %d (will process ledgers %d-%d from cold storage)", sm.backfillTarget, nextLedger, sm.backfillTarget)
			log.Printf("   Cold MAX: %d", coldMax)
			return true, nil
		}

		// No gap - hot storage just doesn't have new data yet
		return false, nil

	case SourceModeBackfill:
		// Check if we've caught up to the backfill target
		if currentCheckpoint >= sm.backfillTarget {
			// We've finished backfilling - switch back to hot
			sm.mode = SourceModeHot
			log.Printf("✅ MODE SWITCH: BACKFILL → HOT")
			log.Printf("   Backfill complete: reached target ledger %d", sm.backfillTarget)
			log.Printf("   Resuming from Hot storage at ledger %d", currentCheckpoint+1)
			sm.backfillTarget = 0
			return true, nil
		}

		// Still backfilling
		return false, nil
	}

	return false, nil
}

// =============================================================================
// Query Method Delegation
// These methods delegate to the appropriate reader based on current mode
// =============================================================================

// GetMaxLedgerSequence returns the max ledger from the current source
func (sm *SourceManager) GetMaxLedgerSequence(ctx context.Context) (int64, error) {
	sm.mu.RLock()
	mode := sm.mode
	backfillTarget := sm.backfillTarget
	sm.mu.RUnlock()

	switch mode {
	case SourceModeHot:
		return sm.hotReader.GetMaxLedgerSequence(ctx)
	case SourceModeBackfill:
		// In backfill mode, return the backfill target as max
		// This prevents over-fetching beyond what cold storage has
		if backfillTarget > 0 {
			return backfillTarget, nil
		}
		return sm.coldReader.GetMaxLedgerSequence(ctx)
	}
	return 0, fmt.Errorf("unknown source mode: %s", mode)
}

// GetMinLedgerSequence returns the min ledger from the current source
func (sm *SourceManager) GetMinLedgerSequence(ctx context.Context) (int64, error) {
	sm.mu.RLock()
	mode := sm.mode
	sm.mu.RUnlock()

	switch mode {
	case SourceModeHot:
		return sm.hotReader.GetMinLedgerSequence(ctx)
	case SourceModeBackfill:
		return sm.coldReader.GetMinLedgerSequence(ctx)
	}
	return 0, fmt.Errorf("unknown source mode: %s", mode)
}

// CountLedgersInRange checks how many ledger rows exist in storage for the range.
// Used to distinguish empty ledgers (exist but no operations) from missing data.
func (sm *SourceManager) CountLedgersInRange(ctx context.Context, startLedger, endLedger int64) (int64, error) {
	sm.mu.RLock()
	mode := sm.mode
	sm.mu.RUnlock()

	switch mode {
	case SourceModeHot:
		return sm.hotReader.CountLedgersInRange(ctx, startLedger, endLedger)
	case SourceModeBackfill:
		if sm.coldReader != nil {
			return sm.coldReader.CountLedgersInRange(ctx, startLedger, endLedger)
		}
		return sm.hotReader.CountLedgersInRange(ctx, startLedger, endLedger)
	}
	return 0, fmt.Errorf("unknown source mode: %s", mode)
}

// QueryEnrichedOperations delegates to the appropriate reader
func (sm *SourceManager) QueryEnrichedOperations(ctx context.Context, startLedger, endLedger int64) (*sql.Rows, error) {
	sm.mu.RLock()
	mode := sm.mode
	sm.mu.RUnlock()

	switch mode {
	case SourceModeHot:
		return sm.hotReader.QueryEnrichedOperations(ctx, startLedger, endLedger)
	case SourceModeBackfill:
		return sm.coldReader.QueryEnrichedOperations(ctx, startLedger, endLedger)
	}
	return nil, fmt.Errorf("unknown source mode: %s", mode)
}

// QueryTokenTransfers delegates to the appropriate reader
func (sm *SourceManager) QueryTokenTransfers(ctx context.Context, startLedger, endLedger int64) (*sql.Rows, error) {
	sm.mu.RLock()
	mode := sm.mode
	sm.mu.RUnlock()

	switch mode {
	case SourceModeHot:
		return sm.hotReader.QueryTokenTransfers(ctx, startLedger, endLedger)
	case SourceModeBackfill:
		return sm.coldReader.QueryTokenTransfers(ctx, startLedger, endLedger)
	}
	return nil, fmt.Errorf("unknown source mode: %s", mode)
}

// QueryAccountsSnapshot delegates to the appropriate reader
func (sm *SourceManager) QueryAccountsSnapshot(ctx context.Context, startLedger, endLedger int64) (*sql.Rows, error) {
	sm.mu.RLock()
	mode := sm.mode
	sm.mu.RUnlock()

	switch mode {
	case SourceModeHot:
		return sm.hotReader.QueryAccountsSnapshot(ctx, startLedger, endLedger)
	case SourceModeBackfill:
		return sm.coldReader.QueryAccountsSnapshot(ctx, startLedger, endLedger)
	}
	return nil, fmt.Errorf("unknown source mode: %s", mode)
}

// QueryAccountsSnapshotAll delegates to the appropriate reader
func (sm *SourceManager) QueryAccountsSnapshotAll(ctx context.Context, startLedger, endLedger int64) (*sql.Rows, error) {
	sm.mu.RLock()
	mode := sm.mode
	sm.mu.RUnlock()

	switch mode {
	case SourceModeHot:
		return sm.hotReader.QueryAccountsSnapshotAll(ctx, startLedger, endLedger)
	case SourceModeBackfill:
		return sm.coldReader.QueryAccountsSnapshotAll(ctx, startLedger, endLedger)
	}
	return nil, fmt.Errorf("unknown source mode: %s", mode)
}

// QueryTrustlinesSnapshotAll delegates to the appropriate reader
func (sm *SourceManager) QueryTrustlinesSnapshotAll(ctx context.Context, startLedger, endLedger int64) (*sql.Rows, error) {
	sm.mu.RLock()
	mode := sm.mode
	sm.mu.RUnlock()

	switch mode {
	case SourceModeHot:
		return sm.hotReader.QueryTrustlinesSnapshotAll(ctx, startLedger, endLedger)
	case SourceModeBackfill:
		return sm.coldReader.QueryTrustlinesSnapshotAll(ctx, startLedger, endLedger)
	}
	return nil, fmt.Errorf("unknown source mode: %s", mode)
}

// QueryTrustlinesSnapshot delegates to the appropriate reader
func (sm *SourceManager) QueryTrustlinesSnapshot(ctx context.Context, startLedger, endLedger int64) (*sql.Rows, error) {
	sm.mu.RLock()
	mode := sm.mode
	sm.mu.RUnlock()

	switch mode {
	case SourceModeHot:
		return sm.hotReader.QueryTrustlinesSnapshot(ctx, startLedger, endLedger)
	case SourceModeBackfill:
		return sm.coldReader.QueryTrustlinesSnapshot(ctx, startLedger, endLedger)
	}
	return nil, fmt.Errorf("unknown source mode: %s", mode)
}

// QueryOffersSnapshot delegates to the appropriate reader
func (sm *SourceManager) QueryOffersSnapshot(ctx context.Context, startLedger, endLedger int64) (*sql.Rows, error) {
	sm.mu.RLock()
	mode := sm.mode
	sm.mu.RUnlock()

	switch mode {
	case SourceModeHot:
		return sm.hotReader.QueryOffersSnapshot(ctx, startLedger, endLedger)
	case SourceModeBackfill:
		return sm.coldReader.QueryOffersSnapshot(ctx, startLedger, endLedger)
	}
	return nil, fmt.Errorf("unknown source mode: %s", mode)
}

// QueryOffersSnapshotAll delegates to the appropriate reader
func (sm *SourceManager) QueryOffersSnapshotAll(ctx context.Context, startLedger, endLedger int64) (*sql.Rows, error) {
	sm.mu.RLock()
	mode := sm.mode
	sm.mu.RUnlock()

	switch mode {
	case SourceModeHot:
		return sm.hotReader.QueryOffersSnapshotAll(ctx, startLedger, endLedger)
	case SourceModeBackfill:
		return sm.coldReader.QueryOffersSnapshotAll(ctx, startLedger, endLedger)
	}
	return nil, fmt.Errorf("unknown source mode: %s", mode)
}

// QueryAccountSignersSnapshotAll delegates to the appropriate reader
func (sm *SourceManager) QueryAccountSignersSnapshotAll(ctx context.Context, startLedger, endLedger int64) (*sql.Rows, error) {
	sm.mu.RLock()
	mode := sm.mode
	sm.mu.RUnlock()

	switch mode {
	case SourceModeHot:
		return sm.hotReader.QueryAccountSignersSnapshotAll(ctx, startLedger, endLedger)
	case SourceModeBackfill:
		return sm.coldReader.QueryAccountSignersSnapshotAll(ctx, startLedger, endLedger)
	}
	return nil, fmt.Errorf("unknown source mode: %s", mode)
}

// QueryContractInvocations delegates to the appropriate reader
func (sm *SourceManager) QueryContractInvocations(ctx context.Context, startLedger, endLedger int64) (*sql.Rows, error) {
	sm.mu.RLock()
	mode := sm.mode
	sm.mu.RUnlock()

	switch mode {
	case SourceModeHot:
		return sm.hotReader.QueryContractInvocations(ctx, startLedger, endLedger)
	case SourceModeBackfill:
		return sm.coldReader.QueryContractInvocations(ctx, startLedger, endLedger)
	}
	return nil, fmt.Errorf("unknown source mode: %s", mode)
}

// QueryContractCallGraphs delegates to the appropriate reader
func (sm *SourceManager) QueryContractCallGraphs(ctx context.Context, startLedger, endLedger int64) (*sql.Rows, error) {
	sm.mu.RLock()
	mode := sm.mode
	sm.mu.RUnlock()

	switch mode {
	case SourceModeHot:
		return sm.hotReader.QueryContractCallGraphs(ctx, startLedger, endLedger)
	case SourceModeBackfill:
		return sm.coldReader.QueryContractCallGraphs(ctx, startLedger, endLedger)
	}
	return nil, fmt.Errorf("unknown source mode: %s", mode)
}

// QueryContractCreations delegates to the appropriate reader
func (sm *SourceManager) QueryContractCreations(ctx context.Context, startLedger, endLedger int64) (*sql.Rows, error) {
	sm.mu.RLock()
	mode := sm.mode
	sm.mu.RUnlock()

	switch mode {
	case SourceModeHot:
		return sm.hotReader.QueryContractCreations(ctx, startLedger, endLedger)
	case SourceModeBackfill:
		// contract_creations_v1 doesn't exist in cold storage yet, return empty result
		return sm.hotReader.QueryContractCreations(ctx, startLedger, endLedger)
	}
	return nil, fmt.Errorf("unknown source mode: %s", mode)
}

// QueryLiquidityPoolsSnapshot delegates to the appropriate reader
func (sm *SourceManager) QueryLiquidityPoolsSnapshot(ctx context.Context, startLedger, endLedger int64) (*sql.Rows, error) {
	sm.mu.RLock()
	mode := sm.mode
	sm.mu.RUnlock()

	switch mode {
	case SourceModeHot:
		return sm.hotReader.QueryLiquidityPoolsSnapshot(ctx, startLedger, endLedger)
	case SourceModeBackfill:
		return sm.coldReader.QueryLiquidityPoolsSnapshot(ctx, startLedger, endLedger)
	}
	return nil, fmt.Errorf("unknown source mode: %s", mode)
}

// QueryClaimableBalancesSnapshot delegates to the appropriate reader
func (sm *SourceManager) QueryClaimableBalancesSnapshot(ctx context.Context, startLedger, endLedger int64) (*sql.Rows, error) {
	sm.mu.RLock()
	mode := sm.mode
	sm.mu.RUnlock()

	switch mode {
	case SourceModeHot:
		return sm.hotReader.QueryClaimableBalancesSnapshot(ctx, startLedger, endLedger)
	case SourceModeBackfill:
		return sm.coldReader.QueryClaimableBalancesSnapshot(ctx, startLedger, endLedger)
	}
	return nil, fmt.Errorf("unknown source mode: %s", mode)
}

// QueryNativeBalancesSnapshot delegates to the appropriate reader
func (sm *SourceManager) QueryNativeBalancesSnapshot(ctx context.Context, startLedger, endLedger int64) (*sql.Rows, error) {
	sm.mu.RLock()
	mode := sm.mode
	sm.mu.RUnlock()

	switch mode {
	case SourceModeHot:
		return sm.hotReader.QueryNativeBalancesSnapshot(ctx, startLedger, endLedger)
	case SourceModeBackfill:
		return sm.coldReader.QueryNativeBalancesSnapshot(ctx, startLedger, endLedger)
	}
	return nil, fmt.Errorf("unknown source mode: %s", mode)
}

// QueryTrades delegates to the appropriate reader
func (sm *SourceManager) QueryTrades(ctx context.Context, startLedger, endLedger int64) (*sql.Rows, error) {
	sm.mu.RLock()
	mode := sm.mode
	sm.mu.RUnlock()

	switch mode {
	case SourceModeHot:
		return sm.hotReader.QueryTrades(ctx, startLedger, endLedger)
	case SourceModeBackfill:
		return sm.coldReader.QueryTrades(ctx, startLedger, endLedger)
	}
	return nil, fmt.Errorf("unknown source mode: %s", mode)
}

// QueryEffects delegates to the appropriate reader
func (sm *SourceManager) QueryEffects(ctx context.Context, startLedger, endLedger int64) (*sql.Rows, error) {
	sm.mu.RLock()
	mode := sm.mode
	sm.mu.RUnlock()

	switch mode {
	case SourceModeHot:
		return sm.hotReader.QueryEffects(ctx, startLedger, endLedger)
	case SourceModeBackfill:
		return sm.coldReader.QueryEffects(ctx, startLedger, endLedger)
	}
	return nil, fmt.Errorf("unknown source mode: %s", mode)
}

// QueryContractDataSnapshot delegates to the appropriate reader
func (sm *SourceManager) QueryContractDataSnapshot(ctx context.Context, startLedger, endLedger int64) (*sql.Rows, error) {
	sm.mu.RLock()
	mode := sm.mode
	sm.mu.RUnlock()

	switch mode {
	case SourceModeHot:
		return sm.hotReader.QueryContractDataSnapshot(ctx, startLedger, endLedger)
	case SourceModeBackfill:
		return sm.coldReader.QueryContractDataSnapshot(ctx, startLedger, endLedger)
	}
	return nil, fmt.Errorf("unknown source mode: %s", mode)
}

// QueryBalanceHolderSnapshots delegates to the appropriate reader.
func (sm *SourceManager) QueryBalanceHolderSnapshots(ctx context.Context, startLedger, endLedger int64) (*sql.Rows, error) {
	sm.mu.RLock()
	mode := sm.mode
	sm.mu.RUnlock()

	switch mode {
	case SourceModeHot:
		return sm.hotReader.QueryBalanceHolderSnapshots(ctx, startLedger, endLedger)
	case SourceModeBackfill:
		return sm.coldReader.QueryBalanceHolderSnapshots(ctx, startLedger, endLedger)
	}
	return nil, fmt.Errorf("unknown source mode: %s", mode)
}

// QueryContractCodeSnapshot delegates to the appropriate reader
func (sm *SourceManager) QueryContractCodeSnapshot(ctx context.Context, startLedger, endLedger int64) (*sql.Rows, error) {
	sm.mu.RLock()
	mode := sm.mode
	sm.mu.RUnlock()

	switch mode {
	case SourceModeHot:
		return sm.hotReader.QueryContractCodeSnapshot(ctx, startLedger, endLedger)
	case SourceModeBackfill:
		return sm.coldReader.QueryContractCodeSnapshot(ctx, startLedger, endLedger)
	}
	return nil, fmt.Errorf("unknown source mode: %s", mode)
}

// QueryTTLSnapshot delegates to the appropriate reader
func (sm *SourceManager) QueryTTLSnapshot(ctx context.Context, startLedger, endLedger int64) (*sql.Rows, error) {
	sm.mu.RLock()
	mode := sm.mode
	sm.mu.RUnlock()

	switch mode {
	case SourceModeHot:
		return sm.hotReader.QueryTTLSnapshot(ctx, startLedger, endLedger)
	case SourceModeBackfill:
		return sm.coldReader.QueryTTLSnapshot(ctx, startLedger, endLedger)
	}
	return nil, fmt.Errorf("unknown source mode: %s", mode)
}

// QueryEvictedKeys delegates to the appropriate reader
func (sm *SourceManager) QueryEvictedKeys(ctx context.Context, startLedger, endLedger int64) (*sql.Rows, error) {
	sm.mu.RLock()
	mode := sm.mode
	sm.mu.RUnlock()

	switch mode {
	case SourceModeHot:
		return sm.hotReader.QueryEvictedKeys(ctx, startLedger, endLedger)
	case SourceModeBackfill:
		return sm.coldReader.QueryEvictedKeys(ctx, startLedger, endLedger)
	}
	return nil, fmt.Errorf("unknown source mode: %s", mode)
}

// QueryRestoredKeys delegates to the appropriate reader
func (sm *SourceManager) QueryRestoredKeys(ctx context.Context, startLedger, endLedger int64) (*sql.Rows, error) {
	sm.mu.RLock()
	mode := sm.mode
	sm.mu.RUnlock()

	switch mode {
	case SourceModeHot:
		return sm.hotReader.QueryRestoredKeys(ctx, startLedger, endLedger)
	case SourceModeBackfill:
		return sm.coldReader.QueryRestoredKeys(ctx, startLedger, endLedger)
	}
	return nil, fmt.Errorf("unknown source mode: %s", mode)
}

// QueryConfigSettingsSnapshot delegates to the appropriate reader
func (sm *SourceManager) QueryConfigSettingsSnapshot(ctx context.Context, startLedger, endLedger int64) (*sql.Rows, error) {
	sm.mu.RLock()
	mode := sm.mode
	sm.mu.RUnlock()

	switch mode {
	case SourceModeHot:
		return sm.hotReader.QueryConfigSettingsSnapshot(ctx, startLedger, endLedger)
	case SourceModeBackfill:
		return sm.coldReader.QueryConfigSettingsSnapshot(ctx, startLedger, endLedger)
	}
	return nil, fmt.Errorf("unknown source mode: %s", mode)
}

// QueryTokenMetadataEntries delegates to the appropriate reader
func (sm *SourceManager) QueryTokenMetadataEntries(ctx context.Context, startLedger, endLedger int64) (*sql.Rows, error) {
	sm.mu.RLock()
	mode := sm.mode
	sm.mu.RUnlock()

	switch mode {
	case SourceModeHot:
		return sm.hotReader.QueryTokenMetadataEntries(ctx, startLedger, endLedger)
	case SourceModeBackfill:
		return sm.coldReader.QueryTokenMetadataEntries(ctx, startLedger, endLedger)
	}
	return nil, fmt.Errorf("unknown source mode: %s", mode)
}

// ForceHotMode forces the source manager back to hot mode, clearing any backfill state.
// Used when backfill mode is stuck (e.g., cold storage also has a gap).
func (sm *SourceManager) ForceHotMode() {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.mode = SourceModeHot
	sm.backfillTarget = 0
}

// Close closes the cold reader connection if it exists
func (sm *SourceManager) Close() error {
	if sm.coldReader != nil {
		return sm.coldReader.Close()
	}
	return nil
}
