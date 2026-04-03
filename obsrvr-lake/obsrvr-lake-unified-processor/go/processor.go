package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/stellar/go-stellar-sdk/xdr"
	pb "github.com/withObsrvr/ttp-processor-demo/stellar-live-source-datalake/go/gen/raw_ledger_service"
)

// Processor handles the per-ledger pipeline: extract → bronze → silver → semantic → index
type Processor struct {
	cfg    *Config
	writer *DuckLakeWriter
}

func NewProcessor(cfg *Config, writer *DuckLakeWriter) *Processor {
	return &Processor{cfg: cfg, writer: writer}
}

// ProcessLedger handles a single ledger through the full pipeline
func (p *Processor) ProcessLedger(ctx context.Context, rawLedger *pb.RawLedger) error {
	// Step 0: Decode XDR
	var lcm xdr.LedgerCloseMeta
	if err := lcm.UnmarshalBinary(rawLedger.LedgerCloseMetaXdr); err != nil {
		return fmt.Errorf("unmarshal XDR for ledger %d: %w", rawLedger.Sequence, err)
	}

	ledgerSeq := rawLedger.Sequence
	closedAt := extractClosedAt(lcm)
	ledgerRange := uint32(ledgerSeq / 10000 * 10000)
	networkPassphrase := p.cfg.Source.NetworkPassphrase

	meta := LedgerMeta{
		LedgerSequence: ledgerSeq,
		ClosedAt:       closedAt,
		LedgerRange:    ledgerRange,
		LCM:            lcm,
	}

	// Step 1: Extract bronze data from XDR (pure functions, no DB)
	bronzeData, err := extractAllBronze(meta, networkPassphrase)
	if err != nil {
		return fmt.Errorf("bronze extraction for ledger %d: %w", ledgerSeq, err)
	}

	// Step 2: Write bronze to DuckLake + transform silver + semantic + index (single tx)
	if err := p.writer.ProcessLedgerData(ctx, bronzeData, ledgerSeq); err != nil {
		return fmt.Errorf("write ledger %d: %w", ledgerSeq, err)
	}

	return nil
}

// LoadCheckpoint reads the last processed ledger from DuckLake
func (p *Processor) LoadCheckpoint(ctx context.Context) (int64, error) {
	return p.writer.LoadCheckpoint(ctx)
}

// FlushLoop periodically calls ducklake_flush_inlined_data to consolidate to Parquet
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
				log.Printf("Warning: flush inlined data failed: %v", err)
			} else {
				log.Printf("Flushed inlined data to Parquet in %s", time.Since(startTime).Round(time.Millisecond))
			}
		}
	}
}

// extractClosedAt gets the ledger close time from LedgerCloseMeta
func extractClosedAt(lcm xdr.LedgerCloseMeta) time.Time {
	var closeTime int64
	switch lcm.V {
	case 0:
		closeTime = int64(lcm.MustV0().LedgerHeader.Header.ScpValue.CloseTime)
	case 1:
		closeTime = int64(lcm.MustV1().LedgerHeader.Header.ScpValue.CloseTime)
	case 2:
		closeTime = int64(lcm.MustV2().LedgerHeader.Header.ScpValue.CloseTime)
	}
	return time.Unix(closeTime, 0).UTC()
}
