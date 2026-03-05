package main

import (
	"context"
	"log"
	"sync"
	"time"
)

// Processor orchestrates token discovery
type Processor struct {
	config     *Config
	reader     *SilverReader
	writer     *TokenWriter
	checkpoint *CheckpointManager
	detector   *Detector
	health     *HealthServer

	stats ProcessorStats
	mu    sync.RWMutex

	stopCh chan struct{}
	wg     sync.WaitGroup
}

// NewProcessor creates a new token discovery processor
func NewProcessor(config *Config, reader *SilverReader, writer *TokenWriter, checkpoint *CheckpointManager, health *HealthServer) *Processor {
	return &Processor{
		config:     config,
		reader:     reader,
		writer:     writer,
		checkpoint: checkpoint,
		detector:   NewDetector(&config.Discovery),
		health:     health,
		stats: ProcessorStats{
			LastProcessedAt: time.Now(),
		},
		stopCh: make(chan struct{}),
	}
}

// Start begins the discovery processing loop
func (p *Processor) Start() error {
	log.Println("🚀 Starting token discovery processor...")

	// Start health server
	if err := p.health.Start(); err != nil {
		return err
	}

	p.wg.Add(1)
	go p.runLoop()

	return nil
}

// Stop gracefully stops the processor
func (p *Processor) Stop() error {
	log.Println("🛑 Stopping token discovery processor...")

	close(p.stopCh)
	p.wg.Wait()

	if err := p.health.Stop(); err != nil {
		log.Printf("Error stopping health server: %v", err)
	}

	log.Println("✅ Token discovery processor stopped")
	return nil
}

// runLoop is the main processing loop
func (p *Processor) runLoop() {
	defer p.wg.Done()

	ticker := time.NewTicker(p.config.GetPollInterval())
	defer ticker.Stop()

	// Run immediately on start
	p.runDiscoveryCycle()

	for {
		select {
		case <-p.stopCh:
			return
		case <-ticker.C:
			p.runDiscoveryCycle()
		}
	}
}

// runDiscoveryCycle performs one discovery cycle
func (p *Processor) runDiscoveryCycle() {
	ctx := context.Background()
	startTime := time.Now()

	// Load checkpoint
	lastLedger, err := p.checkpoint.Load(ctx)
	if err != nil {
		p.recordError("failed to load checkpoint: " + err.Error())
		return
	}

	// Get max ledger from source
	maxLedger, err := p.reader.GetMaxLedger(ctx)
	if err != nil {
		p.recordError("failed to get max ledger: " + err.Error())
		return
	}

	// Calculate end ledger (don't process too much at once)
	endLedger := lastLedger + int64(p.config.Discovery.MaxLedgersPerCycle)
	if endLedger > maxLedger {
		endLedger = maxLedger
	}

	// Nothing new to process
	if lastLedger >= maxLedger {
		log.Printf("📊 No new ledgers to process (at %d)", lastLedger)
		return
	}

	log.Printf("📊 Processing ledgers %d to %d (%d ledgers)", lastLedger+1, endLedger, endLedger-lastLedger)

	// Get contract activity
	activities, err := p.reader.GetContractActivity(ctx, lastLedger, endLedger, p.config.Discovery.BatchSize)
	if err != nil {
		p.recordError("failed to get contract activity: " + err.Error())
		return
	}

	if len(activities) == 0 {
		// No activity, just update checkpoint
		if err := p.checkpoint.Save(ctx, endLedger, 0, 0); err != nil {
			p.recordError("failed to save checkpoint: " + err.Error())
			return
		}
		log.Printf("✅ No contract activity found, checkpoint updated to %d", endLedger)
		p.updateStats(endLedger, 0, 0, startTime)
		return
	}

	log.Printf("📋 Found %d contracts with activity", len(activities))

	// Get transfer counts for these contracts
	contractIDs := make([]string, len(activities))
	for i, a := range activities {
		contractIDs[i] = a.ContractID
	}

	transferCounts, err := p.reader.GetTransferCounts(ctx, contractIDs)
	if err != nil {
		log.Printf("⚠️ Failed to get transfer counts: %v (continuing anyway)", err)
	}

	// Process each contract
	discovered := 0
	updated := 0

	for _, activity := range activities {
		// Detect token type
		result := p.detector.DetectTokenType(activity.Functions)

		// Skip unknown tokens with no SEP-41 functions
		if result.TokenType == "unknown" && result.Score == 0 {
			continue
		}

		// Check if token exists
		existing, err := p.writer.GetExistingToken(ctx, activity.ContractID)
		if err != nil {
			log.Printf("⚠️ Failed to check existing token %s: %v", activity.ContractID, err)
			continue
		}

		isNew := existing == nil

		// Build token record
		token := &DiscoveredToken{
			ContractID:         activity.ContractID,
			TokenType:          result.TokenType,
			DetectionMethod:    result.DetectionMethod,
			IsSAC:              result.IsSAC,
			LPPoolType:         result.LPPoolType,
			FirstSeenLedger:    activity.FirstSeen,
			LastActivityLedger: activity.LastSeen,
			ObservedFunctions:  activity.Functions,
			SEP41Score:         result.Score,
			TransferCount:      transferCounts[activity.ContractID],
		}

		// Preserve existing first_seen if updating
		if existing != nil && existing.FirstSeenLedger < token.FirstSeenLedger {
			token.FirstSeenLedger = existing.FirstSeenLedger
		}

		// Upsert token
		if err := p.writer.UpsertToken(ctx, token); err != nil {
			log.Printf("⚠️ Failed to upsert token %s: %v", activity.ContractID, err)
			continue
		}

		if isNew {
			discovered++
			log.Printf("🆕 Discovered %s token: %s (score: %d)", result.TokenType, activity.ContractID, result.Score)
		} else {
			updated++
		}
	}

	// Save checkpoint
	if err := p.checkpoint.Save(ctx, endLedger, discovered, updated); err != nil {
		p.recordError("failed to save checkpoint: " + err.Error())
		return
	}

	log.Printf("✅ Cycle complete: %d discovered, %d updated, checkpoint at %d (took %v)",
		discovered, updated, endLedger, time.Since(startTime).Round(time.Millisecond))

	p.updateStats(endLedger, discovered, updated, startTime)
	p.clearError()
}

// updateStats updates processor statistics
func (p *Processor) updateStats(ledger int64, discovered, updated int, startTime time.Time) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.stats.LastLedgerProcessed = ledger
	p.stats.LastProcessedAt = time.Now()
	p.stats.TokensDiscovered += int64(discovered)
	p.stats.TokensUpdated += int64(updated)
	p.stats.CyclesCompleted++
	p.stats.LastCycleDuration = time.Since(startTime)

	// Update health server
	p.health.UpdateStats(p.stats)

	// Get token counts by type
	ctx := context.Background()
	counts, err := p.writer.GetTokenCountByType(ctx)
	if err == nil {
		p.stats.SEP41TokenCount = counts["sep41"]
		p.stats.LPTokenCount = counts["lp"]
		p.stats.SACTokenCount = counts["sac"]
		p.stats.UnknownTokenCount = counts["unknown"]

		total, _ := p.writer.GetTokenCount(ctx)
		p.stats.TotalTokensInRegistry = total
	}
}

// recordError records an error and updates stats
func (p *Processor) recordError(msg string) {
	log.Printf("❌ Error: %s", msg)

	p.mu.Lock()
	defer p.mu.Unlock()

	p.stats.LastError = msg
	p.stats.ConsecutiveErrorCount++
	p.health.UpdateStats(p.stats)
}

// clearError clears the error state
func (p *Processor) clearError() {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.stats.LastError = ""
	p.stats.ConsecutiveErrorCount = 0
}

// GetStats returns current processor statistics
func (p *Processor) GetStats() ProcessorStats {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.stats
}
