package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"
	"github.com/stellar/go-stellar-sdk/support/datastore"
)

// ArchiveSource reads ledgers from GCS or S3 via the Stellar SDK's
// BufferedStorageBackend. This is the production backend for processing
// full Stellar history at scale.
type ArchiveSource struct {
	backend   ledgerbackend.LedgerBackend
	dataStore datastore.DataStore
}

// ArchiveConfig holds configuration for the archive backend.
type ArchiveConfig struct {
	StorageType       string // "GCS" or "S3"
	BucketPath        string // e.g. "my-bucket/ledgers/mainnet"
	Region            string // S3 region (default "us-east-1")
	LedgersPerFile    uint32 // default 64
	FilesPerPartition uint32 // default 10
	BufferSize        int    // default 100
	NumWorkers        int    // default 10
}

func NewArchiveSource(cfg ArchiveConfig) (*ArchiveSource, error) {
	if cfg.LedgersPerFile == 0 {
		cfg.LedgersPerFile = 64
	}
	if cfg.FilesPerPartition == 0 {
		cfg.FilesPerPartition = 10
	}
	if cfg.BufferSize == 0 {
		cfg.BufferSize = 100
	}
	if cfg.NumWorkers == 0 {
		cfg.NumWorkers = 10
	}
	if cfg.Region == "" {
		cfg.Region = "us-east-1"
	}

	schema := datastore.DataStoreSchema{
		LedgersPerFile:    cfg.LedgersPerFile,
		FilesPerPartition: cfg.FilesPerPartition,
	}

	dsParams := make(map[string]string)
	var dsConfig datastore.DataStoreConfig

	switch cfg.StorageType {
	case "GCS":
		dsParams["destination_bucket_path"] = cfg.BucketPath
		dsConfig = datastore.DataStoreConfig{Type: "GCS", Schema: schema, Params: dsParams}
	case "S3":
		dsParams["bucket_name"] = cfg.BucketPath
		dsParams["region"] = cfg.Region
		dsConfig = datastore.DataStoreConfig{Type: "S3", Schema: schema, Params: dsParams}
	default:
		return nil, fmt.Errorf("unsupported archive storage type: %s", cfg.StorageType)
	}

	log.Printf("Creating %s datastore: %s", cfg.StorageType, cfg.BucketPath)
	ds, err := datastore.NewDataStore(context.Background(), dsConfig)
	if err != nil {
		return nil, fmt.Errorf("create datastore: %w", err)
	}

	bufferedConfig := ledgerbackend.BufferedStorageBackendConfig{
		BufferSize: uint32(cfg.BufferSize),
		NumWorkers: uint32(cfg.NumWorkers),
		RetryLimit: 3,
		RetryWait:  time.Second,
	}

	backend, err := ledgerbackend.NewBufferedStorageBackend(bufferedConfig, ds, schema)
	if err != nil {
		ds.Close()
		return nil, fmt.Errorf("create buffered storage backend: %w", err)
	}

	log.Printf("Archive source ready: %s (%d ledgers/file, buffer=%d, workers=%d)",
		cfg.BucketPath, cfg.LedgersPerFile, cfg.BufferSize, cfg.NumWorkers)

	return &ArchiveSource{
		backend:   backend,
		dataStore: ds,
	}, nil
}

// PrepareRange initializes the archive backend for reading a ledger range.
func (s *ArchiveSource) PrepareRange(ctx context.Context, start, end uint32) error {
	r := ledgerbackend.BoundedRange(start, end)
	return s.backend.PrepareRange(ctx, r)
}

// GetLedger fetches a single ledger from the archive and returns raw XDR bytes.
func (s *ArchiveSource) GetLedger(ctx context.Context, sequence uint32) ([]byte, error) {
	lcm, err := s.backend.GetLedger(ctx, sequence)
	if err != nil {
		return nil, fmt.Errorf("get ledger %d: %w", sequence, err)
	}
	raw, err := lcm.MarshalBinary()
	if err != nil {
		return nil, fmt.Errorf("marshal ledger %d: %w", sequence, err)
	}
	return raw, nil
}

// Close shuts down the backend and datastore.
func (s *ArchiveSource) Close() error {
	if err := s.backend.Close(); err != nil {
		return err
	}
	return s.dataStore.Close()
}
