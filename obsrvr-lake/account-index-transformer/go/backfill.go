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
	ForceRestart bool
	PostgresOnly bool
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
	var writer *IndexWriter
	if !opts.PostgresOnly {
		writer, err = NewIndexWriter(&indexConfig)
		if err != nil {
			return err
		}
		defer writer.Close()
	}

	var pgWriter *PostgresIndexWriter
	if config.IndexPostgres.Enabled {
		pgWriter = NewPostgresIndexWriter(catalogDB, config.IndexPostgres, config.IndexCold.PartitionSize)
	}
	if opts.PostgresOnly && pgWriter == nil {
		return fmt.Errorf("postgres-only backfill requires index_postgres.enabled=true")
	}

	var checkpoint *BackfillCheckpointManager
	var checkpointLedger int64
	var checkpointStatus string
	if opts.PostgresOnly {
		checkpointLedger, err = pgWriter.LoadCheckpoint(ctx)
		if err != nil {
			return err
		}
		checkpointStatus = "postgres_mirror"
	} else {
		checkpoint, err = NewBackfillCheckpointManager(catalogDB, &config.Backfill)
		if err != nil {
			return err
		}
		checkpointLedger, checkpointStatus, err = checkpoint.Load(ctx)
		if err != nil {
			return err
		}
	}

	startLedger := resolveBackfillStart(opts, checkpointLedger)
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

	mode := "ducklake"
	if pgWriter != nil {
		mode = "ducklake+postgres"
	}
	if opts.PostgresOnly {
		mode = "postgres_only"
	}
	log.Printf("🚚 Account index cold backfill: mode=%s start=%d end=%d batch_ledgers=%d checkpoint=%d status=%s",
		mode, startLedger, endLedger, opts.BatchLedgers, checkpointLedger, checkpointStatus)

	if !opts.PostgresOnly {
		if err := checkpoint.Save(ctx, checkpointLedger, "running", ""); err != nil {
			return err
		}
	}

	for batchStart := startLedger; batchStart <= endLedger; batchStart += opts.BatchLedgers {
		batchEnd := batchStart + opts.BatchLedgers - 1
		if batchEnd > endLedger {
			batchEnd = endLedger
		}

		rows, err := reader.ReadAccountLedgerRanges(ctx, batchStart, batchEnd, config.IndexCold.PartitionSize, config.AccountBucketCount())
		if err != nil {
			if checkpoint != nil {
				_ = checkpoint.Save(ctx, batchStart-1, "failed", err.Error())
			}
			return err
		}
		var written int64
		if writer != nil {
			written, err = writer.WriteAccountLedgerRanges(ctx, rows)
			if err != nil {
				_ = checkpoint.Save(ctx, batchStart-1, "failed", err.Error())
				return err
			}
		}
		var pgWritten int64
		if pgWriter != nil {
			pgWritten, err = pgWriter.WriteAccountLedgerRanges(ctx, rows, batchEnd)
			if err != nil {
				if checkpoint != nil {
					_ = checkpoint.Save(ctx, batchStart-1, "failed", err.Error())
				}
				return fmt.Errorf("postgres account index backfill batch %d-%d: %w", batchStart, batchEnd, err)
			}
		}
		if checkpoint != nil {
			if err := checkpoint.Save(ctx, batchEnd, "running", ""); err != nil {
				return err
			}
		}
		if pgWriter != nil {
			log.Printf("✅ Backfilled account index batch %d-%d rows_read=%d ducklake_rows=%d postgres_rows=%d", batchStart, batchEnd, len(rows), written, pgWritten)
		} else {
			log.Printf("✅ Backfilled account index batch %d-%d rows_read=%d rows_written=%d", batchStart, batchEnd, len(rows), written)
		}
	}

	if writer != nil {
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
	}
	log.Printf("✅ Account index cold backfill complete mode=%s through ledger %d", mode, endLedger)
	return nil
}

func resolveBackfillStart(opts BackfillOptions, checkpointLedger int64) int64 {
	startLedger := opts.StartLedger
	if startLedger <= 0 {
		return checkpointLedger + 1
	}
	if checkpointLedger >= startLedger && !opts.ForceRestart {
		log.Printf("⚠️  Requested start ledger %d is behind checkpoint %d; resuming at checkpoint+1. Use --force-restart to reprocess from the requested start.", opts.StartLedger, checkpointLedger)
		return checkpointLedger + 1
	}
	if checkpointLedger >= startLedger && opts.ForceRestart {
		log.Printf("⚠️  Force restart enabled: reprocessing from requested start ledger %d despite checkpoint %d", startLedger, checkpointLedger)
	}
	return startLedger
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
