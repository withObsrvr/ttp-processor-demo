package main

import (
	"context"
	"log"
	"time"

	pb "github.com/withObsrvr/obsrvr-lake-unified-processor/gen/bronze_ledger_service"
)

type Processor struct {
	cfg    *Config
	writer *DuckLakeWriter
}

func NewProcessor(cfg *Config, writer *DuckLakeWriter) *Processor {
	return &Processor{cfg: cfg, writer: writer}
}

// ProcessLedger runs all silver transforms for a single ledger's bronze data.
func (p *Processor) ProcessLedger(ctx context.Context, data *pb.BronzeLedgerData) error {
	return p.writer.ProcessBronzeToSilver(ctx, data)
}

// LoadCheckpoint reads the last processed ledger from silver tables.
func (p *Processor) LoadCheckpoint(ctx context.Context) (int64, error) {
	return p.writer.LoadCheckpoint(ctx)
}

// FlushLoop periodically consolidates inlined data to Parquet.
func (p *Processor) FlushLoop(ctx context.Context) {
	interval := time.Duration(p.cfg.DuckLake.FlushInterval) * time.Second
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	log.Printf("Flush loop started (interval: %s)", interval)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			startTime := time.Now()
			if err := p.writer.FlushInlinedData(ctx); err != nil {
				log.Printf("Warning: flush failed: %v", err)
			} else {
				log.Printf("Flushed inlined data in %s", time.Since(startTime).Round(time.Millisecond))
			}
		}
	}
}
