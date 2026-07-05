package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
)

type BackfillOptions struct {
	StartLedger  int64
	EndLedger    int64
	BatchLedgers int64
}

func RunBackfill(ctx context.Context, config *Config, catalogDB *sql.DB, opts BackfillOptions) error {
	if opts.BatchLedgers <= 0 {
		opts.BatchLedgers = config.IndexCold.PartitionSize
	}
	if opts.BatchLedgers <= 0 {
		opts.BatchLedgers = 100000
	}

	reader, err := NewSilverColdReader(&config.SilverCold)
	if err != nil {
		return err
	}
	defer reader.Close()

	indexConfig := config.IndexConfig()
	writer, err := NewIndexWriter(&indexConfig, catalogDB)
	if err != nil {
		return err
	}
	defer writer.Close()

	checkpoint, err := NewBackfillCheckpointManager(catalogDB, &config.Backfill)
	if err != nil {
		return err
	}

	checkpointLedger, checkpointStatus, err := checkpoint.Load(ctx)
	if err != nil {
		return err
	}

	startLedger := opts.StartLedger
	if startLedger <= 0 {
		startLedger = checkpointLedger + 1
	}
	if checkpointLedger >= startLedger {
		startLedger = checkpointLedger + 1
	}

	endLedger := opts.EndLedger
	if endLedger <= 0 {
		endLedger, err = reader.GetMaxLedger(ctx)
		if err != nil {
			return err
		}
	}
	if startLedger <= 0 {
		startLedger = 1
	}
	if endLedger < startLedger {
		log.Printf("✅ Backfill already complete or empty (checkpoint=%d status=%s start=%d end=%d)", checkpointLedger, checkpointStatus, startLedger, endLedger)
		return nil
	}

	log.Printf("🚚 Account index cold backfill: start=%d end=%d batch_ledgers=%d checkpoint=%d status=%s",
		startLedger, endLedger, opts.BatchLedgers, checkpointLedger, checkpointStatus)

	if err := checkpoint.Save(ctx, checkpointLedger, "running", ""); err != nil {
		return err
	}

	for batchStart := startLedger; batchStart <= endLedger; batchStart += opts.BatchLedgers {
		batchEnd := batchStart + opts.BatchLedgers - 1
		if batchEnd > endLedger {
			batchEnd = endLedger
		}

		rows, err := reader.ReadAccountLedgerRanges(ctx, batchStart, batchEnd, config.IndexCold.PartitionSize, config.AccountBucketCount())
		if err != nil {
			_ = checkpoint.Save(ctx, batchStart-1, "failed", err.Error())
			return err
		}
		written, err := writer.WriteAccountLedgerRanges(ctx, rows)
		if err != nil {
			_ = checkpoint.Save(ctx, batchStart-1, "failed", err.Error())
			return err
		}
		if err := checkpoint.Save(ctx, batchEnd, "running", ""); err != nil {
			return err
		}
		log.Printf("✅ Backfilled account index batch %d-%d rows_read=%d rows_written=%d", batchStart, batchEnd, len(rows), written)
	}

	if _, err := writer.FlushInlinedData(); err != nil {
		log.Printf("⚠️  Backfill flush inlined data failed (non-fatal): %v", err)
	}
	if config.Maintenance.Enabled {
		if err := writer.RunCheckpoint(ctx, config.Maintenance.MaxCompactedFiles); err != nil {
			log.Printf("⚠️  Backfill maintenance failed (non-fatal): %v", err)
		}
	}
	if err := checkpoint.Save(ctx, endLedger, "complete", ""); err != nil {
		return err
	}
	log.Printf("✅ Account index cold backfill complete through ledger %d", endLedger)
	return nil
}

func validateBackfillOptions(opts BackfillOptions) error {
	if opts.StartLedger < 0 {
		return fmt.Errorf("start ledger must be >= 0")
	}
	if opts.EndLedger < 0 {
		return fmt.Errorf("end ledger must be >= 0")
	}
	if opts.EndLedger > 0 && opts.StartLedger > opts.EndLedger {
		return fmt.Errorf("start ledger %d is greater than end ledger %d", opts.StartLedger, opts.EndLedger)
	}
	return nil
}
