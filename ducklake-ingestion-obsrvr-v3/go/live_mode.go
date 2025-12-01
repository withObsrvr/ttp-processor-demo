package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"time"
)

// liveModeIngestion handles continuous ingestion of new ledgers
// Polls for new ledgers every PollInterval seconds
// Processes 1 ledger at a time for low latency
func (ing *Ingester) liveModeIngestion(ctx context.Context, startSeq uint32) error {
	pollInterval := time.Duration(ing.config.Source.PollInterval) * time.Second
	if pollInterval <= 0 {
		pollInterval = 5 * time.Second // Default: 5 seconds
	}

	log.Printf("🔴 LIVE MODE: Starting continuous ingestion from ledger %d", startSeq)
	log.Printf("  Poll interval: %v", pollInterval)
	log.Printf("  Batch size: %d ledgers", ing.config.DuckLake.LiveBatchSize)
	log.Printf("  Commit interval: %ds", ing.config.DuckLake.LiveCommitIntervalSeconds)

	currentSeq := startSeq
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	processed := 0
	startTime := time.Now()

	for {
		select {
		case <-ctx.Done():
			log.Println("Live mode ingestion cancelled")
			return ctx.Err()

		case <-ticker.C:
			// Poll for next ledger(s)
			ledgersProcessed, nextSeq, err := ing.pollAndProcessLedgers(ctx, currentSeq)
			if err != nil {
				log.Printf("Error in live mode polling: %v", err)
				// Don't fail on transient errors, just log and continue
				continue
			}

			if ledgersProcessed > 0 {
				processed += ledgersProcessed
				currentSeq = nextSeq

				// Update metrics
				elapsed := time.Since(startTime)
				rate := float64(processed) / elapsed.Seconds()
				log.Printf("[LIVE] Processed %d ledgers (%.2f ledgers/sec), current: %d",
					processed, rate, currentSeq-1)

				if ing.metrics != nil {
					ing.metrics.UpdateMetrics(uint64(processed), 19, currentSeq-1, rate, 0)
				}
			}
		}
	}
}

// pollAndProcessLedgers polls for and processes new ledgers starting from currentSeq
// Returns: (ledgersProcessed, nextSeq, error)
func (ing *Ingester) pollAndProcessLedgers(ctx context.Context, currentSeq uint32) (int, uint32, error) {
	// Get batch size for live mode
	liveBatchSize := ing.config.DuckLake.LiveBatchSize
	if liveBatchSize <= 0 {
		liveBatchSize = 1 // Sanity check
	}

	// Request ledgers starting from currentSeq
	endSeq := currentSeq + uint32(liveBatchSize) - 1

	// Prepare range (might fail if ledger doesn't exist yet)
	if err := ing.ledgerSource.PrepareRange(ctx, currentSeq, endSeq); err != nil {
		// Ledger probably doesn't exist yet, not an error
		return 0, currentSeq, nil
	}

	// Get ledger stream
	ledgerChan, err := ing.ledgerSource.GetLedgerRange(ctx, currentSeq, endSeq)
	if err != nil {
		return 0, currentSeq, fmt.Errorf("failed to get ledger stream: %w", err)
	}

	processed := 0
	timeout := time.After(2 * time.Second) // Short timeout for live mode
	lastFlush := time.Now()
	commitInterval := time.Duration(ing.config.DuckLake.LiveCommitIntervalSeconds) * time.Second

	for {
		select {
		case <-ctx.Done():
			return processed, currentSeq + uint32(processed), ctx.Err()

		case result, ok := <-ledgerChan:
			if !ok {
				// Channel closed - flush if we have data
				if processed > 0 {
					if err := ing.flush(ctx); err != nil {
						return processed, currentSeq + uint32(processed), fmt.Errorf("failed to flush: %w", err)
					}
				}
				return processed, currentSeq + uint32(processed), nil
			}

			// Check for errors
			if result.Err != nil {
				if result.Err == io.EOF {
					if processed > 0 {
						if err := ing.flush(ctx); err != nil {
							return processed, currentSeq + uint32(processed), fmt.Errorf("failed to flush: %w", err)
						}
					}
					return processed, currentSeq + uint32(processed), nil
				}
				return processed, currentSeq + uint32(processed), fmt.Errorf("ledger stream error: %w", result.Err)
			}

			lcm := result.Ledger

			// Get sequence number
			var seq uint32
			switch lcm.V {
			case 0:
				seq = uint32(lcm.MustV0().LedgerHeader.Header.LedgerSeq)
			case 1:
				seq = uint32(lcm.MustV1().LedgerHeader.Header.LedgerSeq)
			case 2:
				seq = uint32(lcm.MustV2().LedgerHeader.Header.LedgerSeq)
			}

			// Process ledger
			if err := ing.processLedgerFromXDR(ctx, lcm); err != nil {
				log.Printf("Error processing ledger %d in live mode: %v", seq, err)
				return processed, currentSeq + uint32(processed), err
			}

			processed++

			// Flush based on batch size OR commit interval
			shouldFlush := false
			if processed >= liveBatchSize {
				shouldFlush = true
			} else if time.Since(lastFlush) >= commitInterval {
				shouldFlush = true
			}

			if shouldFlush {
				if err := ing.flush(ctx); err != nil {
					return processed, currentSeq + uint32(processed), fmt.Errorf("failed to flush ledger %d: %w", seq, err)
				}
				lastFlush = time.Now()
				log.Printf("✅ [LIVE] Flushed %d ledgers (latest: %d)", processed, seq)
			} else {
				log.Printf("✅ [LIVE] Processed ledger %d (buffered, %d/%d)", seq, processed, liveBatchSize)
			}

		case <-timeout:
			// Timeout - flush any pending data
			if processed > 0 {
				if err := ing.flush(ctx); err != nil {
					return processed, currentSeq + uint32(processed), fmt.Errorf("failed to flush on timeout: %w", err)
				}
				log.Printf("✅ [LIVE] Flushed %d ledgers (timeout)", processed)
			}
			return processed, currentSeq + uint32(processed), nil
		}
	}
}
