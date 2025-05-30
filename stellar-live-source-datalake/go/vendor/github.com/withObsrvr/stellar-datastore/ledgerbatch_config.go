package datastore

import (
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/stellar/go/support/compressxdr"
)

type DataStoreSchema struct {
	LedgersPerFile    uint32 `toml:"ledgers_per_file"`
	FilesPerPartition uint32 `toml:"files_per_partition"`
}

func (ec DataStoreSchema) GetSequenceNumberStartBoundary(ledgerSeq uint32) uint32 {
	if ec.LedgersPerFile == 0 {
		return 0
	}
	return (ledgerSeq / ec.LedgersPerFile) * ec.LedgersPerFile
}

func (ec DataStoreSchema) GetSequenceNumberEndBoundary(ledgerSeq uint32) uint32 {
	return ec.GetSequenceNumberStartBoundary(ledgerSeq) + ec.LedgersPerFile - 1
}

// GetObjectKeyFromSequenceNumber generates the object key name from the ledger sequence number based on configuration.
func (ec DataStoreSchema) GetObjectKeyFromSequenceNumber(ledgerSeq uint32) string {
	var objectKey string

	if ec.FilesPerPartition > 1 {
		partitionSize := ec.LedgersPerFile * ec.FilesPerPartition
		partitionStart := (ledgerSeq / partitionSize) * partitionSize
		partitionEnd := partitionStart + partitionSize - 1

		objectKey = fmt.Sprintf("%08X--%d-%d/", math.MaxUint32-partitionStart, partitionStart, partitionEnd)
	}

	fileStart := ec.GetSequenceNumberStartBoundary(ledgerSeq)
	fileEnd := ec.GetSequenceNumberEndBoundary(ledgerSeq)
	objectKey += fmt.Sprintf("%08X--%d", math.MaxUint32-fileStart, fileStart)

	// Multiple ledgers per file
	if fileStart != fileEnd {
		objectKey += fmt.Sprintf("-%d", fileEnd)
	}
	objectKey += fmt.Sprintf(".xdr.%s", compressxdr.DefaultCompressor.Name())

	return objectKey
}

// GetSequenceNumberFromObjectKey extracts the sequence number from an object key
func (ec DataStoreSchema) GetSequenceNumberFromObjectKey(key string) (uint32, error) {
	// Extract sequence number from format like "FFFFFFFF--0-63.xdr.zstd" or "FFFFFFFF--0-639/FFFFFFFF--0-63.xdr.zstd"
	parts := strings.Split(key, "--")
	if len(parts) != 2 {
		return 0, fmt.Errorf("invalid object key format: %s", key)
	}

	seqParts := strings.Split(parts[1], ".")
	rangeParts := strings.Split(seqParts[0], "-")
	seq, err := strconv.ParseUint(rangeParts[0], 10, 32)
	if err != nil {
		return 0, fmt.Errorf("invalid sequence number in key: %w", err)
	}

	return uint32(seq), nil
}
