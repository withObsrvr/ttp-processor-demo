package server

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"path"
	"strings"

	"github.com/klauspost/compress/zstd"
	"github.com/stellar/go/support/storage"
	"github.com/stellar/go/xdr"
	"go.uber.org/zap"
)

// GalexieDatalakeReader handles the custom galexie datalake format
type GalexieDatalakeReader struct {
	storage    storage.Storage
	bucketName string
	archivePath string
	logger     *zap.Logger
}

// NewGalexieDatalakeReader creates a new galexie datalake reader
func NewGalexieDatalakeReader(storageType, bucketName, archivePath, region, endpoint string, logger *zap.Logger) (*GalexieDatalakeReader, error) {
	ctx := context.Background()

	logger.Info("Connecting to Galexie datalake",
		zap.String("storage_type", storageType),
		zap.String("bucket", bucketName),
		zap.String("archive_path", archivePath),
	)

	var storageBackend storage.Storage
	var err error

	switch strings.ToLower(storageType) {
	case "gcs":
		storageBackend, err = storage.NewGCSBackend(ctx, bucketName, archivePath, endpoint)
	case "s3":
		storageBackend, err = storage.NewS3Storage(ctx, bucketName, archivePath, region, endpoint, false, "")
	case "fs", "file":
		storageBackend = storage.NewFilesystemStorage(bucketName) // bucketName is the file path for filesystem
	default:
		return nil, fmt.Errorf("unsupported storage type: %s", storageType)
	}

	if err != nil {
		return nil, fmt.Errorf("failed to create storage backend: %w", err)
	}

	return &GalexieDatalakeReader{
		storage:     storageBackend,
		bucketName:  bucketName,
		archivePath: archivePath,
		logger:      logger,
	}, nil
}

// getLedgerPartition determines which partition directory contains a given ledger
func (gdr *GalexieDatalakeReader) getLedgerPartition(sequence uint32) string {
	// Galexie datalake uses 64k ledgers per partition
	// Partitions are named like: FFFA23FF--384000-447999
	partitionStart := (sequence / 64000) * 64000
	partitionEnd := partitionStart + 63999
	
	// Convert to hex for the partition directory name
	// The hex prefix seems to be calculated differently, but we can search for the pattern
	return fmt.Sprintf("%d-%d", partitionStart, partitionEnd)
}

// findPartitionDirectory finds the actual partition directory name containing the ledger
func (gdr *GalexieDatalakeReader) findPartitionDirectory(sequence uint32) (string, error) {
	partitionStart := (sequence / 64000) * 64000
	partitionEnd := partitionStart + 63999
	
	gdr.logger.Debug("Finding partition directory",
		zap.Uint32("sequence", sequence),
		zap.Uint32("partition_start", partitionStart),
		zap.Uint32("partition_end", partitionEnd))
	
	// List all directories and find the one with the matching range
	files, errors := gdr.storage.ListFiles("")
	
	expectedSuffix := fmt.Sprintf("--%d-%d", partitionStart, partitionEnd)
	gdr.logger.Debug("Looking for partition with suffix", zap.String("suffix", expectedSuffix))
	
	dirCount := 0
	for file := range files {
		select {
		case err := <-errors:
			if err != nil {
				gdr.logger.Error("Error listing root files", zap.Error(err))
				return "", fmt.Errorf("error listing files: %w", err)
			}
		default:
		}
		
		dirCount++
		gdr.logger.Debug("Checking file/directory",
			zap.String("file", file),
			zap.Bool("matches", strings.Contains(file, expectedSuffix)))
		
		// Look for directory pattern: XXXXXXXX--start-end
		if strings.Contains(file, expectedSuffix) {
			// Extract the partition directory name from the full path
			parts := strings.Split(file, "/")
			// Find the part that contains the expected suffix
			for _, part := range parts {
				if strings.Contains(part, expectedSuffix) {
					gdr.logger.Debug("Found matching partition directory",
						zap.String("directory", part),
						zap.String("full_path", file),
						zap.Uint32("sequence", sequence))
					return part, nil
				}
			}
		}
	}
	
	gdr.logger.Error("Partition directory not found",
		zap.Uint32("sequence", sequence),
		zap.String("expected_suffix", expectedSuffix),
		zap.Int("directories_checked", dirCount))
	
	return "", fmt.Errorf("partition directory not found for ledger %d (expected range %d-%d, checked %d items)", sequence, partitionStart, partitionEnd, dirCount)
}

// getLedgerFileName generates the expected file name for a ledger
func (gdr *GalexieDatalakeReader) getLedgerFileName(sequence uint32) string {
	// Files are named like: FFF9BECC--409907.xdr.zstd
	// The hex prefix appears to be some encoding, but the suffix is the decimal sequence
	return fmt.Sprintf("--%d.xdr.zstd", sequence)
}

// findLedgerFile finds the actual file name for a ledger sequence
func (gdr *GalexieDatalakeReader) findLedgerFile(partitionDir string, sequence uint32) (string, error) {
	// List files in the partition directory
	searchPath := partitionDir + "/"
	files, errors := gdr.storage.ListFiles(searchPath)
	
	sequenceStr := fmt.Sprintf("--%d.xdr.zstd", sequence)
	
	gdr.logger.Debug("Searching for ledger file",
		zap.String("partition", partitionDir),
		zap.String("search_path", searchPath),
		zap.String("sequence_suffix", sequenceStr),
		zap.Uint32("sequence", sequence))
	
	fileCount := 0
	for file := range files {
		select {
		case err := <-errors:
			if err != nil {
				gdr.logger.Error("Error listing files in partition",
					zap.String("partition", partitionDir),
					zap.Error(err))
				return "", fmt.Errorf("error listing files in partition: %w", err)
			}
		default:
		}
		
		fileCount++
		gdr.logger.Debug("Found file in partition",
			zap.String("file", file),
			zap.Bool("matches", strings.HasSuffix(file, sequenceStr)))
		
		if strings.HasSuffix(file, sequenceStr) {
			// Extract just the filename
			parts := strings.Split(file, "/")
			if len(parts) > 0 {
				fileName := parts[len(parts)-1]
				gdr.logger.Debug("Found matching ledger file",
					zap.String("file_name", fileName),
					zap.Uint32("sequence", sequence))
				return fileName, nil
			}
		}
	}
	
	gdr.logger.Error("Ledger file not found",
		zap.String("partition", partitionDir),
		zap.String("sequence_suffix", sequenceStr),
		zap.Int("files_checked", fileCount),
		zap.Uint32("sequence", sequence))
	
	return "", fmt.Errorf("ledger file not found for sequence %d (checked %d files in %s)", sequence, fileCount, partitionDir)
}

// GetLedgerCloseMeta retrieves a ledger close meta for a specific sequence from galexie datalake
func (gdr *GalexieDatalakeReader) GetLedgerCloseMeta(sequence uint32) (*xdr.LedgerCloseMeta, error) {
	gdr.logger.Debug("Getting ledger close meta from galexie datalake",
		zap.Uint32("sequence", sequence))

	// 1. Find the partition directory
	partitionDir, err := gdr.findPartitionDirectory(sequence)
	if err != nil {
		return nil, fmt.Errorf("failed to find partition for ledger %d: %w", sequence, err)
	}
	
	gdr.logger.Debug("Found partition directory",
		zap.String("partition", partitionDir),
		zap.Uint32("sequence", sequence))

	// 2. Find the specific ledger file
	fileName, err := gdr.findLedgerFile(partitionDir, sequence)
	if err != nil {
		return nil, fmt.Errorf("failed to find file for ledger %d: %w", sequence, err)
	}
	
	gdr.logger.Debug("Found ledger file",
		zap.String("file", fileName),
		zap.Uint32("sequence", sequence))

	// 3. Read the file
	filePath := path.Join(partitionDir, fileName)
	reader, err := gdr.storage.GetFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read file %s: %w", filePath, err)
	}
	defer reader.Close()

	// 4. Decompress with zstd
	decoder, err := zstd.NewReader(reader)
	if err != nil {
		return nil, fmt.Errorf("failed to create zstd decoder: %w", err)
	}
	defer decoder.Close()

	// 5. Read all data
	data, err := io.ReadAll(decoder)
	if err != nil {
		return nil, fmt.Errorf("failed to read decompressed data: %w", err)
	}

	// 6. Handle custom galexie format: skip 12 bytes (8-byte header + 4-byte version)
	if len(data) < 12 {
		return nil, fmt.Errorf("decompressed data too short: %d bytes", len(data))
	}
	xdrData := data[12:]

	// 7. Create LedgerCloseMeta with version 1 and unmarshal the V1 data
	var lcm xdr.LedgerCloseMeta
	lcm.V = 1
	dataReader := bytes.NewReader(xdrData)
	if _, err := xdr.Unmarshal(dataReader, &lcm.V1); err != nil {
		return nil, fmt.Errorf("failed to unmarshal LedgerCloseMetaV1: %w", err)
	}

	gdr.logger.Debug("Successfully retrieved ledger close meta",
		zap.Uint32("sequence", sequence),
		zap.Int("data_size", len(data)))

	return &lcm, nil
}

// Close closes the datalake reader
func (gdr *GalexieDatalakeReader) Close() error {
	if gdr.storage != nil {
		return gdr.storage.Close()
	}
	return nil
}