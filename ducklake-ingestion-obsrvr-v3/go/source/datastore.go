package source

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/stellar/go/ingest/ledgerbackend"
	"github.com/stellar/go/support/datastore"
	"github.com/stellar/go/xdr"
)

// DatastoreSource uses the official Stellar datastore package to read ledgers
// directly from Galexie archives (GCS/S3) without a gRPC intermediary.
type DatastoreSource struct {
	backend           ledgerbackend.LedgerBackend
	store             datastore.DataStore
	networkPassphrase string
}

// NewDatastoreSource creates a new source using the official Stellar datastore.
// This provides direct access to Galexie archives with buffered reading.
func NewDatastoreSource(cfg DatastoreConfig, networkPassphrase string) (*DatastoreSource, error) {
	cfg.ApplyDefaults()

	ctx := context.Background()

	// Create schema - defines how ledgers are organized in the archive
	schema := datastore.DataStoreSchema{
		LedgersPerFile:    cfg.LedgersPerFile,
		FilesPerPartition: cfg.FilesPerPartition,
	}

	// Build datastore config based on storage type
	dsParams := make(map[string]string)
	var dsConfig datastore.DataStoreConfig

	switch cfg.Type {
	case "GCS":
		dsParams["destination_bucket_path"] = cfg.Path
		dsConfig = datastore.DataStoreConfig{
			Type:   "GCS",
			Schema: schema,
			Params: dsParams,
		}
		log.Printf("[source:datastore] configuring GCS backend: %s", cfg.Path)

	case "S3":
		dsParams["destination_bucket_path"] = cfg.Path
		if cfg.S3Region != "" {
			dsParams["region"] = cfg.S3Region
		}
		if cfg.S3Endpoint != "" {
			dsParams["endpoint"] = cfg.S3Endpoint
		}
		dsConfig = datastore.DataStoreConfig{
			Type:   "S3",
			Schema: schema,
			Params: dsParams,
		}
		log.Printf("[source:datastore] configuring S3 backend: %s (region=%s)", cfg.Path, cfg.S3Region)

	default:
		return nil, fmt.Errorf("unsupported storage type: %s (use GCS or S3)", cfg.Type)
	}

	// Create the datastore
	store, err := datastore.NewDataStore(ctx, dsConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create datastore: %w", err)
	}

	// Create BufferedStorageBackend configuration
	retryWait := time.Duration(cfg.RetryWaitMs) * time.Millisecond
	bufferedConfig := ledgerbackend.BufferedStorageBackendConfig{
		BufferSize: cfg.BufferSize,
		NumWorkers: cfg.NumWorkers,
		RetryLimit: cfg.RetryLimit,
		RetryWait:  retryWait,
	}

	// Create the BufferedStorageBackend
	backend, err := ledgerbackend.NewBufferedStorageBackend(bufferedConfig, store, schema)
	if err != nil {
		store.Close()
		return nil, fmt.Errorf("failed to create buffered storage backend: %w", err)
	}

	log.Printf("[source:datastore] created datastore backend (buffer=%d, workers=%d)",
		cfg.BufferSize, cfg.NumWorkers)

	return &DatastoreSource{
		backend:           backend,
		store:             store,
		networkPassphrase: networkPassphrase,
	}, nil
}

// GetLedger fetches a single ledger by sequence number.
func (s *DatastoreSource) GetLedger(ctx context.Context, seq uint32) (*xdr.LedgerCloseMeta, error) {
	lcm, err := s.backend.GetLedger(ctx, seq)
	if err != nil {
		return nil, fmt.Errorf("get ledger %d: %w", seq, err)
	}
	return &lcm, nil
}

// GetLedgerRange returns a channel that streams ledgers in the given range.
// If end is 0, the stream is unbounded (tail mode).
func (s *DatastoreSource) GetLedgerRange(ctx context.Context, start, end uint32) (<-chan LedgerResult, error) {
	ch := make(chan LedgerResult, 100)

	go func() {
		defer close(ch)

		for seq := start; end == 0 || seq <= end; seq++ {
			select {
			case <-ctx.Done():
				ch <- LedgerResult{Err: ctx.Err()}
				return
			default:
			}

			lcm, err := s.backend.GetLedger(ctx, seq)
			if err != nil {
				ch <- LedgerResult{Err: fmt.Errorf("get ledger %d: %w", seq, err)}
				return
			}

			ch <- LedgerResult{Ledger: &lcm}
		}

		log.Printf("[source:datastore] completed range %d-%d", start, end)
	}()

	return ch, nil
}

// PrepareRange prepares the source for reading the given range.
// This triggers prefetching in the BufferedStorageBackend.
func (s *DatastoreSource) PrepareRange(ctx context.Context, start, end uint32) error {
	var ledgerRange ledgerbackend.Range
	if end == 0 {
		ledgerRange = ledgerbackend.UnboundedRange(start)
		log.Printf("[source:datastore] preparing unbounded range from %d", start)
	} else {
		ledgerRange = ledgerbackend.BoundedRange(start, end)
		log.Printf("[source:datastore] preparing range %d-%d (%d ledgers)", start, end, end-start+1)
	}

	if err := s.backend.PrepareRange(ctx, ledgerRange); err != nil {
		return fmt.Errorf("prepare range: %w", err)
	}

	return nil
}

// Close releases resources associated with the source.
func (s *DatastoreSource) Close() error {
	if s.backend != nil {
		s.backend.Close()
	}
	if s.store != nil {
		return s.store.Close()
	}
	return nil
}
