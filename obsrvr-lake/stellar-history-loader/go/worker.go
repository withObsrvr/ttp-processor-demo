package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/stellar/go-stellar-sdk/xdr"
)

// ---------------------------------------------------------------------------
// Ledger Source Interface & Filesystem Stub
// ---------------------------------------------------------------------------

// LedgerSource provides access to raw ledger data.
type LedgerSource interface {
	GetLedger(ctx context.Context, sequence uint32) ([]byte, error) // returns raw XDR bytes
	Close() error
}

// FilesystemSource reads ledger XDR files from a local directory.
// File layout expected: <basePath>/<sequence>.xdr
type FilesystemSource struct {
	basePath string
}

// NewFilesystemSource creates a new FilesystemSource.
func NewFilesystemSource(basePath string) (*FilesystemSource, error) {
	info, err := os.Stat(basePath)
	if err != nil {
		return nil, fmt.Errorf("filesystem source: %w", err)
	}
	if !info.IsDir() {
		return nil, fmt.Errorf("filesystem source: %q is not a directory", basePath)
	}
	return &FilesystemSource{basePath: basePath}, nil
}

// GetLedger reads the XDR bytes for a given ledger sequence from disk.
func (fs *FilesystemSource) GetLedger(_ context.Context, sequence uint32) ([]byte, error) {
	path := filepath.Join(fs.basePath, fmt.Sprintf("%d.xdr", sequence))
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read ledger %d: %w", sequence, err)
	}
	return data, nil
}

// Close is a no-op for the filesystem source.
func (fs *FilesystemSource) Close() error {
	return nil
}

// XDRStreamSource reads a framed XDR stream file (output of `nebu fetch`)
// and indexes all ledgers into memory for random access by sequence.
type XDRStreamSource struct {
	ledgers map[uint32][]byte
}

// NewXDRStreamSource reads a nebu-style framed XDR file and indexes all ledgers.
func NewXDRStreamSource(filePath string) (*XDRStreamSource, error) {
	f, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("open xdr stream: %w", err)
	}
	defer f.Close()

	source := &XDRStreamSource{ledgers: make(map[uint32][]byte)}

	for {
		var lcm xdr.LedgerCloseMeta
		_, err := xdr.Unmarshal(f, &lcm)
		if err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break // clean end of stream
			}
			return nil, fmt.Errorf("XDR stream decode error: %w", err)
		}

		seq := uint32(lcm.LedgerSequence())
		raw, err := lcm.MarshalBinary()
		if err != nil {
			log.Printf("WARN: failed to re-marshal ledger %d: %v", seq, err)
			continue
		}
		source.ledgers[seq] = raw
	}

	if len(source.ledgers) == 0 {
		return nil, fmt.Errorf("no ledgers found in %s", filePath)
	}

	log.Printf("XDRStreamSource: loaded %d ledgers from %s", len(source.ledgers), filePath)
	return source, nil
}

func (s *XDRStreamSource) GetLedger(_ context.Context, sequence uint32) ([]byte, error) {
	data, ok := s.ledgers[sequence]
	if !ok {
		return nil, fmt.Errorf("ledger %d not found in stream", sequence)
	}
	return data, nil
}

func (s *XDRStreamSource) Close() error { return nil }

// NewLedgerSourceFromConfig creates the appropriate LedgerSource for the given config.
func NewLedgerSourceFromConfig(config OrchestratorConfig) (LedgerSource, error) {
	switch config.StorageType {
	case "FS":
		return NewFilesystemSource(config.Bucket)
	case "XDR":
		return NewXDRStreamSource(config.Bucket)
	case "RPC":
		return NewRPCSource(config.Bucket)
	case "GCS":
		return NewArchiveSource(ArchiveConfig{
			StorageType:       "GCS",
			BucketPath:        config.Bucket,
			LedgersPerFile:    config.LedgersPerFile,
			FilesPerPartition: config.FilesPerPartition,
		})
	case "S3":
		return NewArchiveSource(ArchiveConfig{
			StorageType:       "S3",
			BucketPath:        config.Bucket,
			LedgersPerFile:    config.LedgersPerFile,
			FilesPerPartition: config.FilesPerPartition,
		})
	default:
		return nil, fmt.Errorf("unknown storage type: %q", config.StorageType)
	}
}

// ---------------------------------------------------------------------------
// Batch: aggregated LedgerData across multiple ledgers
// ---------------------------------------------------------------------------

// BatchData holds all extracted data from multiple ledgers, ready for flushing
// to the ParquetWriter.
type BatchData struct {
	Transactions      []TransactionData
	Operations        []OperationData
	Effects           []EffectData
	Trades            []TradeData
	Accounts          []AccountData
	Offers            []OfferData
	Trustlines        []TrustlineData
	AccountSigners    []AccountSignerData
	ClaimableBalances []ClaimableBalanceData
	LiquidityPools    []LiquidityPoolData
	ConfigSettings    []ConfigSettingData
	TTLEntries        []TTLData
	EvictedKeys       []EvictedKeyData
	ContractEvents    []ContractEventData
	ContractData      []ContractDataData
	ContractCode      []ContractCodeData
	NativeBalances    []NativeBalanceData
	RestoredKeys      []RestoredKeyData
	ContractCreations []ContractCreationData
	Ledgers           []LedgerRowData
	TokenTransfers    []TokenTransferData
}

// mergeLedger appends all data from a single LedgerData into the batch.
func (b *BatchData) mergeLedger(ld *LedgerData) {
	b.Transactions = append(b.Transactions, ld.Transactions...)
	b.Operations = append(b.Operations, ld.Operations...)
	b.Effects = append(b.Effects, ld.Effects...)
	b.Trades = append(b.Trades, ld.Trades...)
	b.Accounts = append(b.Accounts, ld.Accounts...)
	b.Offers = append(b.Offers, ld.Offers...)
	b.Trustlines = append(b.Trustlines, ld.Trustlines...)
	b.AccountSigners = append(b.AccountSigners, ld.AccountSigners...)
	b.ClaimableBalances = append(b.ClaimableBalances, ld.ClaimableBalances...)
	b.LiquidityPools = append(b.LiquidityPools, ld.LiquidityPools...)
	b.ConfigSettings = append(b.ConfigSettings, ld.ConfigSettings...)
	b.TTLEntries = append(b.TTLEntries, ld.TTLEntries...)
	b.EvictedKeys = append(b.EvictedKeys, ld.EvictedKeys...)
	b.ContractEvents = append(b.ContractEvents, ld.ContractEvents...)
	b.ContractData = append(b.ContractData, ld.ContractData...)
	b.ContractCode = append(b.ContractCode, ld.ContractCode...)
	b.NativeBalances = append(b.NativeBalances, ld.NativeBalances...)
	b.RestoredKeys = append(b.RestoredKeys, ld.RestoredKeys...)
	b.ContractCreations = append(b.ContractCreations, ld.ContractCreations...)
	b.Ledgers = append(b.Ledgers, ld.Ledgers...)
	b.TokenTransfers = append(b.TokenTransfers, ld.TokenTransfers...)
}

// ---------------------------------------------------------------------------
// Prefetched ledger item sent through the read-ahead channel
// ---------------------------------------------------------------------------

type prefetchedLedger struct {
	sequence uint32
	data     []byte
	err      error
}

// ---------------------------------------------------------------------------
// Worker
// ---------------------------------------------------------------------------

const defaultPrefetchBuffer = 200

// Worker processes a shard of ledgers through the extraction pipeline.
type Worker struct {
	id         int
	shard      Shard
	config     OrchestratorConfig
	progress   *ProgressTracker
	writer     *ParquetWriter
	source     LedgerSource // shared across workers for XDR mode, per-worker for FS/GCS
	checkpoint *CheckpointManager // optional, for resume support
	lineage    *LineageWriter     // optional, for lineage tracking
}

// NewWorker creates a new Worker for the given shard.
func NewWorker(id int, shard Shard, config OrchestratorConfig, progress *ProgressTracker, writer *ParquetWriter, source LedgerSource, checkpoint *CheckpointManager, lineage *LineageWriter) *Worker {
	return &Worker{
		id:         id,
		shard:      shard,
		config:     config,
		progress:   progress,
		writer:     writer,
		source:     source,
		checkpoint: checkpoint,
		lineage:    lineage,
	}
}

// Run executes the full worker pipeline:
//
//  1. Create ledger source
//  2. Start prefetcher goroutine (reads ahead into a buffered channel)
//  3. Loop: receive from prefetch channel, decode XDR once, extract all
//     tables in parallel, accumulate batch
//  4. Every batchSize ledgers, flush to ParquetWriter
//  5. Final flush for remaining data
//  6. Report final progress
func (w *Worker) Run(ctx context.Context) error {
	// 1. Use provided source, or create per-worker source for archive backends
	source := w.source

	// Archive backends are sequential — each worker needs its own instance
	if w.config.StorageType == "GCS" || w.config.StorageType == "S3" {
		perWorkerSource, err := NewLedgerSourceFromConfig(w.config)
		if err != nil {
			return fmt.Errorf("[Worker %d] create per-worker source: %w", w.id, err)
		}
		defer perWorkerSource.Close()

		// Prepare this worker's shard range
		type rangePreparer interface {
			PrepareRange(ctx context.Context, start, end uint32) error
		}
		if p, ok := perWorkerSource.(rangePreparer); ok {
			if err := p.PrepareRange(ctx, w.shard.StartLedger, w.shard.EndLedger); err != nil {
				return fmt.Errorf("[Worker %d] prepare range: %w", w.id, err)
			}
		}
		source = perWorkerSource
	}

	// 2. Check for resume point
	effectiveShard := w.shard
	if w.checkpoint != nil {
		resumeFrom, skip := w.checkpoint.GetShardResumePoint(w.id, w.shard)
		if skip {
			log.Printf("[Worker %d] Shard already completed, skipping", w.id)
			return nil
		}
		if resumeFrom > effectiveShard.StartLedger {
			log.Printf("[Worker %d] Resuming from ledger %d (was %d)", w.id, resumeFrom, effectiveShard.StartLedger)
			effectiveShard.StartLedger = resumeFrom
		}
	}

	// 3. Start prefetcher goroutine
	prefetchCh := w.startPrefetcherRange(ctx, source, effectiveShard.StartLedger, effectiveShard.EndLedger)

	// 4-5. Process ledgers in batches
	batch := &BatchData{}
	batchCount := 0
	var lastSeq uint32
	var batchStartSeq uint32
	batchStartTime := time.Now()

	for item := range prefetchCh {
		// Check for context cancellation
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// Handle prefetch errors
		if item.err != nil {
			return fmt.Errorf("[Worker %d] prefetch ledger %d: %w", w.id, item.sequence, item.err)
		}

		// Decode XDR ONCE per ledger
		meta, err := w.decodeLedger(item.sequence, item.data)
		if err != nil {
			return fmt.Errorf("[Worker %d] decode ledger %d: %w", w.id, item.sequence, err)
		}

		// Extract all 21 data types in parallel
		ledgerData, err := w.extractLedger(meta)
		if err != nil {
			return fmt.Errorf("[Worker %d] extract ledger %d: %w", w.id, item.sequence, err)
		}

		// Accumulate into batch
		if batchCount == 0 {
			batchStartSeq = item.sequence
			batchStartTime = time.Now()
		}
		batch.mergeLedger(ledgerData)
		batchCount++
		lastSeq = item.sequence

		// Flush when batch is full
		if batchCount >= w.config.BatchSize {
			if err := w.writer.WriteBatch(batch); err != nil {
				return fmt.Errorf("[Worker %d] write batch: %w", w.id, err)
			}
			w.progress.ReportProgress(w.id, uint64(batchCount))
			w.recordLineage(batch, batchStartSeq, lastSeq, time.Since(batchStartTime))
			if w.checkpoint != nil {
				w.checkpoint.UpdateShard(w.id, lastSeq)
				if err := w.checkpoint.Save(); err != nil {
					log.Printf("[Worker %d] WARNING: checkpoint save failed: %v", w.id, err)
				}
			}
			batch = &BatchData{}
			batchCount = 0
		}
	}

	// 5. Final flush for remaining data
	if batchCount > 0 {
		if err := w.writer.WriteBatch(batch); err != nil {
			return fmt.Errorf("[Worker %d] write final batch: %w", w.id, err)
		}
		w.progress.ReportProgress(w.id, uint64(batchCount))
	}

	// 6. Mark shard complete in checkpoint
	if w.checkpoint != nil {
		w.checkpoint.CompleteShard(w.id, w.shard)
		w.checkpoint.Save()
	}

	shardSize := w.shard.EndLedger - w.shard.StartLedger + 1
	log.Printf("[Worker %d] Completed: %d ledgers processed", w.id, shardSize)

	return nil
}

// recordLineage records per-table row counts for a batch in the lineage writer.
func (w *Worker) recordLineage(batch *BatchData, startSeq, endSeq uint32, duration time.Duration) {
	if w.lineage == nil {
		return
	}
	tables := map[string]int{
		"transactions":              len(batch.Transactions),
		"operations":                len(batch.Operations),
		"effects":                   len(batch.Effects),
		"trades":                    len(batch.Trades),
		"accounts_snapshot":         len(batch.Accounts),
		"offers_snapshot":           len(batch.Offers),
		"trustlines_snapshot":       len(batch.Trustlines),
		"account_signers_snapshot":  len(batch.AccountSigners),
		"claimable_balances_snapshot": len(batch.ClaimableBalances),
		"liquidity_pools_snapshot":   len(batch.LiquidityPools),
		"config_settings":            len(batch.ConfigSettings),
		"ttl_snapshot":              len(batch.TTLEntries),
		"evicted_keys":              len(batch.EvictedKeys),
		"contract_events":           len(batch.ContractEvents),
		"contract_data_snapshot":    len(batch.ContractData),
		"contract_code_snapshot":    len(batch.ContractCode),
		"native_balances":           len(batch.NativeBalances),
		"restored_keys":             len(batch.RestoredKeys),
		"contract_creations":        len(batch.ContractCreations),
		"ledgers":                   len(batch.Ledgers),
		"token_transfers":           len(batch.TokenTransfers),
	}
	for table, count := range tables {
		if count > 0 {
			w.lineage.RecordBatch(w.id, table, startSeq, endSeq, count, duration)
		}
	}
}

// startPrefetcher launches a goroutine that reads the full shard range.
func (w *Worker) startPrefetcher(ctx context.Context, source LedgerSource) <-chan prefetchedLedger {
	return w.startPrefetcherRange(ctx, source, w.shard.StartLedger, w.shard.EndLedger)
}

// startPrefetcherRange launches a goroutine that reads ledgers in [start, end]
// ahead of processing and sends them into a buffered channel.
func (w *Worker) startPrefetcherRange(ctx context.Context, source LedgerSource, start, end uint32) <-chan prefetchedLedger {
	bufferSize := defaultPrefetchBuffer
	ch := make(chan prefetchedLedger, bufferSize)

	go func() {
		defer close(ch)

		for seq := start; seq <= end; seq++ {
			// Check for cancellation before each fetch
			select {
			case <-ctx.Done():
				ch <- prefetchedLedger{sequence: seq, err: ctx.Err()}
				return
			default:
			}

			data, err := source.GetLedger(ctx, seq)

			select {
			case ch <- prefetchedLedger{sequence: seq, data: data, err: err}:
			case <-ctx.Done():
				return
			}
		}
	}()

	return ch
}

// decodeLedger unmarshals raw XDR bytes into a LedgerCloseMeta and extracts
// header metadata. This is done ONCE per ledger (not per extractor).
func (w *Worker) decodeLedger(sequence uint32, rawXDR []byte) (LedgerMeta, error) {
	var lcm xdr.LedgerCloseMeta
	if err := lcm.UnmarshalBinary(rawXDR); err != nil {
		return LedgerMeta{}, fmt.Errorf("unmarshal XDR: %w", err)
	}

	// Extract header metadata
	ledgerSeq := uint32(lcm.LedgerSequence())
	closedAt := time.Unix(int64(lcm.LedgerHeaderHistoryEntry().Header.ScpValue.CloseTime), 0).UTC()
	ledgerRange := (ledgerSeq / 10000) * 10000

	// Sanity check
	if ledgerSeq != sequence {
		return LedgerMeta{}, fmt.Errorf("sequence mismatch: expected %d, got %d", sequence, ledgerSeq)
	}

	var eraID *string
	if w.config.EraID != "" {
		eraID = &w.config.EraID
	}

	return LedgerMeta{
		LCM:            lcm,
		LedgerSequence: ledgerSeq,
		ClosedAt:       closedAt,
		LedgerRange:    ledgerRange,
		EraID:          eraID,
	}, nil
}

// extractorResult holds the output of a single extractor goroutine.
type extractorResult struct {
	name string
	data *LedgerData
	err  error
}

// extractLedger fans out to all 21 extractors in parallel, each receiving the
// already-decoded LCM. Results are collected and merged into a single LedgerData.
func (w *Worker) extractLedger(meta LedgerMeta) (*LedgerData, error) {
	lcm := meta.LCM
	np := w.config.NetworkPassphrase
	seq := meta.LedgerSequence
	t := meta.ClosedAt
	lr := meta.LedgerRange

	resultCh := make(chan extractorResult, 21)
	var wg sync.WaitGroup

	// Helper to launch an extractor goroutine
	launch := func(name string, fn func() (*LedgerData, error)) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			data, err := fn()
			resultCh <- extractorResult{name: name, data: data, err: err}
		}()
	}

	// Fan out to all 21 extractors
	launch("transactions", func() (*LedgerData, error) {
		rows, err := extractTransactions(lcm, np, seq, t, lr)
		if err != nil {
			return nil, err
		}
		return &LedgerData{Transactions: rows}, nil
	})
	launch("operations", func() (*LedgerData, error) {
		rows, err := extractOperations(lcm, np, seq, t, lr)
		if err != nil {
			return nil, err
		}
		return &LedgerData{Operations: rows}, nil
	})
	launch("effects", func() (*LedgerData, error) {
		rows, err := extractEffects(lcm, np, seq, t, lr, meta.EraID)
		if err != nil {
			return nil, err
		}
		return &LedgerData{Effects: rows}, nil
	})
	launch("trades", func() (*LedgerData, error) {
		rows, err := extractTrades(lcm, np, seq, t, lr)
		if err != nil {
			return nil, err
		}
		return &LedgerData{Trades: rows}, nil
	})
	launch("accounts", func() (*LedgerData, error) {
		rows, err := extractAccounts(lcm, np, seq, t, lr)
		if err != nil {
			return nil, err
		}
		return &LedgerData{Accounts: rows}, nil
	})
	launch("offers", func() (*LedgerData, error) {
		rows, err := extractOffers(lcm, np, seq, t, lr)
		if err != nil {
			return nil, err
		}
		return &LedgerData{Offers: rows}, nil
	})
	launch("trustlines", func() (*LedgerData, error) {
		rows, err := extractTrustlines(lcm, np, seq, t, lr)
		if err != nil {
			return nil, err
		}
		return &LedgerData{Trustlines: rows}, nil
	})
	launch("account_signers", func() (*LedgerData, error) {
		rows, err := extractAccountSigners(lcm, np, seq, t, lr)
		if err != nil {
			return nil, err
		}
		return &LedgerData{AccountSigners: rows}, nil
	})
	launch("claimable_balances", func() (*LedgerData, error) {
		rows, err := extractClaimableBalances(lcm, np, seq, t, lr)
		if err != nil {
			return nil, err
		}
		return &LedgerData{ClaimableBalances: rows}, nil
	})
	launch("liquidity_pools", func() (*LedgerData, error) {
		rows, err := extractLiquidityPools(lcm, np, seq, t, lr)
		if err != nil {
			return nil, err
		}
		return &LedgerData{LiquidityPools: rows}, nil
	})
	launch("config_settings", func() (*LedgerData, error) {
		rows, err := extractConfigSettings(lcm, np, seq, t, lr)
		if err != nil {
			return nil, err
		}
		return &LedgerData{ConfigSettings: rows}, nil
	})
	launch("ttl", func() (*LedgerData, error) {
		rows, err := extractTTL(lcm, np, seq, t, lr)
		if err != nil {
			return nil, err
		}
		return &LedgerData{TTLEntries: rows}, nil
	})
	launch("evicted_keys", func() (*LedgerData, error) {
		rows, err := extractEvictedKeys(lcm, seq, t, lr)
		if err != nil {
			return nil, err
		}
		return &LedgerData{EvictedKeys: rows}, nil
	})
	launch("contract_events", func() (*LedgerData, error) {
		rows, err := extractContractEvents(lcm, np, seq, t, lr)
		if err != nil {
			return nil, err
		}
		return &LedgerData{ContractEvents: rows}, nil
	})
	launch("contract_data", func() (*LedgerData, error) {
		rows, err := extractContractData(lcm, np, seq, t, lr)
		if err != nil {
			return nil, err
		}
		return &LedgerData{ContractData: rows}, nil
	})
	launch("contract_code", func() (*LedgerData, error) {
		rows, err := extractContractCode(lcm, np, seq, t, lr)
		if err != nil {
			return nil, err
		}
		return &LedgerData{ContractCode: rows}, nil
	})
	launch("native_balances", func() (*LedgerData, error) {
		rows, err := extractNativeBalances(lcm, np, seq, t, lr)
		if err != nil {
			return nil, err
		}
		return &LedgerData{NativeBalances: rows}, nil
	})
	launch("restored_keys", func() (*LedgerData, error) {
		rows, err := extractRestoredKeys(lcm, np, seq, t, lr)
		if err != nil {
			return nil, err
		}
		return &LedgerData{RestoredKeys: rows}, nil
	})
	launch("contract_creations", func() (*LedgerData, error) {
		rows, err := extractContractCreations(lcm, np, seq, t, lr)
		if err != nil {
			return nil, err
		}
		return &LedgerData{ContractCreations: rows}, nil
	})
	launch("ledgers", func() (*LedgerData, error) {
		rows, err := extractLedgers(lcm, np, seq, t, lr)
		if err != nil {
			return nil, err
		}
		return &LedgerData{Ledgers: rows}, nil
	})
	launch("token_transfers", func() (*LedgerData, error) {
		rows, err := extractTokenTransfers(lcm, np, seq, t, lr)
		if err != nil {
			return nil, err
		}
		return &LedgerData{TokenTransfers: rows}, nil
	})

	// Wait for all extractors to complete, then close the results channel
	go func() {
		wg.Wait()
		close(resultCh)
	}()

	// Collect and merge results
	merged := &LedgerData{Meta: meta}
	var extractErrors []error

	for result := range resultCh {
		if result.err != nil {
			extractErrors = append(extractErrors, fmt.Errorf("%s: %w", result.name, result.err))
			continue
		}
		if result.data != nil {
			merged.Transactions = append(merged.Transactions, result.data.Transactions...)
			merged.Operations = append(merged.Operations, result.data.Operations...)
			merged.Effects = append(merged.Effects, result.data.Effects...)
			merged.Trades = append(merged.Trades, result.data.Trades...)
			merged.Accounts = append(merged.Accounts, result.data.Accounts...)
			merged.Offers = append(merged.Offers, result.data.Offers...)
			merged.Trustlines = append(merged.Trustlines, result.data.Trustlines...)
			merged.AccountSigners = append(merged.AccountSigners, result.data.AccountSigners...)
			merged.ClaimableBalances = append(merged.ClaimableBalances, result.data.ClaimableBalances...)
			merged.LiquidityPools = append(merged.LiquidityPools, result.data.LiquidityPools...)
			merged.ConfigSettings = append(merged.ConfigSettings, result.data.ConfigSettings...)
			merged.TTLEntries = append(merged.TTLEntries, result.data.TTLEntries...)
			merged.EvictedKeys = append(merged.EvictedKeys, result.data.EvictedKeys...)
			merged.ContractEvents = append(merged.ContractEvents, result.data.ContractEvents...)
			merged.ContractData = append(merged.ContractData, result.data.ContractData...)
			merged.ContractCode = append(merged.ContractCode, result.data.ContractCode...)
			merged.NativeBalances = append(merged.NativeBalances, result.data.NativeBalances...)
			merged.RestoredKeys = append(merged.RestoredKeys, result.data.RestoredKeys...)
			merged.ContractCreations = append(merged.ContractCreations, result.data.ContractCreations...)
			merged.Ledgers = append(merged.Ledgers, result.data.Ledgers...)
			merged.TokenTransfers = append(merged.TokenTransfers, result.data.TokenTransfers...)
		}
	}

	if len(extractErrors) > 0 {
		for _, err := range extractErrors {
			log.Printf("[Worker %d] extractor error on ledger %d: %v", w.id, meta.LedgerSequence, err)
		}
		return nil, fmt.Errorf("%d extractor(s) failed for ledger %d", len(extractErrors), meta.LedgerSequence)
	}

	// Stamp era_id on all rows from config (avoids passing through every extractor)
	stampEraID(merged, meta.EraID)

	return merged, nil
}

// stampEraID sets the EraID field on every row in every table slice.
func stampEraID(data *LedgerData, eraID *string) {
	if eraID == nil {
		return
	}
	for i := range data.Transactions {
		data.Transactions[i].EraID = eraID
	}
	for i := range data.Operations {
		data.Operations[i].EraID = eraID
	}
	for i := range data.Effects {
		data.Effects[i].EraID = eraID
	}
	for i := range data.Trades {
		data.Trades[i].EraID = eraID
	}
	for i := range data.Accounts {
		data.Accounts[i].EraID = eraID
	}
	for i := range data.Offers {
		data.Offers[i].EraID = eraID
	}
	for i := range data.Trustlines {
		data.Trustlines[i].EraID = eraID
	}
	for i := range data.AccountSigners {
		data.AccountSigners[i].EraID = eraID
	}
	for i := range data.ClaimableBalances {
		data.ClaimableBalances[i].EraID = eraID
	}
	for i := range data.LiquidityPools {
		data.LiquidityPools[i].EraID = eraID
	}
	for i := range data.ConfigSettings {
		data.ConfigSettings[i].EraID = eraID
	}
	for i := range data.TTLEntries {
		data.TTLEntries[i].EraID = eraID
	}
	for i := range data.EvictedKeys {
		data.EvictedKeys[i].EraID = eraID
	}
	for i := range data.ContractEvents {
		data.ContractEvents[i].EraID = eraID
	}
	for i := range data.ContractData {
		data.ContractData[i].EraID = eraID
	}
	for i := range data.ContractCode {
		data.ContractCode[i].EraID = eraID
	}
	for i := range data.NativeBalances {
		data.NativeBalances[i].EraID = eraID
	}
	for i := range data.RestoredKeys {
		data.RestoredKeys[i].EraID = eraID
	}
	for i := range data.ContractCreations {
		data.ContractCreations[i].EraID = eraID
	}
	for i := range data.Ledgers {
		data.Ledgers[i].EraID = eraID
	}
	for i := range data.TokenTransfers {
		data.TokenTransfers[i].EraID = eraID
	}
}

// ParquetWriter is an alias for ParquetWriterFull (see parquet_writer.go)
type ParquetWriter = ParquetWriterFull

// NewParquetWriter creates a new Parquet writer for a worker.
func NewParquetWriter(outputDir string, workerID int, pipelineVersion string) (*ParquetWriter, error) {
	return NewParquetWriterFull(outputDir, workerID, pipelineVersion), nil
}

// ===========================================================================
// STUB EXTRACTORS
//
// Each extractor takes the pre-decoded LedgerCloseMeta and returns typed data.
// These are stubs that return empty slices. They will be replaced with full
// implementations that mirror the existing stellar-postgres-ingester logic
// but output structs instead of SQL rows.
// ===========================================================================

// extractTransactions — see extractors_core.go
// extractOperations  — see extractors_core.go
// extractEffects     — see extractors_effects.go
// extractTrades      — see extractors_core.go

// extractAccounts         — see extractors_accounts.go
// extractTrustlines       — see extractors_accounts.go
// extractAccountSigners   — see extractors_accounts.go
// extractNativeBalances   — see extractors_accounts.go

// extractOffers            — see extractors_market.go
// extractClaimableBalances — see extractors_market.go
// extractLiquidityPools    — see extractors_market.go

// extractConfigSettings — see extractors_state.go
// extractTTL            — see extractors_state.go
// extractEvictedKeys    — see extractors_state.go

// extractContractEvents — see extractors_soroban.go
// extractContractData  — see extractors_soroban.go
// extractContractCode  — see extractors_soroban.go

// extractRestoredKeys     — see extractors_soroban.go
// extractContractCreations — see extractors_soroban.go
