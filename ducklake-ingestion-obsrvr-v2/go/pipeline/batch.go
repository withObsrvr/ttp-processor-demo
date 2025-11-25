// Package pipeline provides parallel processing infrastructure for Bronze Copier V2.
package pipeline

import (
	"time"

	"github.com/withObsrvr/ttp-processor-demo/ducklake-ingestion-obsrvr-v2/go/manifest"
)

// Batch represents a batch of ledgers to process.
type Batch struct {
	// Start is the first ledger sequence in the batch
	Start uint32

	// End is the last ledger sequence in the batch
	End uint32

	// ID is a unique identifier for tracking
	ID uint64

	// CreatedAt is when this batch was created
	CreatedAt time.Time
}

// Size returns the number of ledgers in the batch.
func (b *Batch) Size() int {
	return int(b.End - b.Start + 1)
}

// Contains returns true if the sequence is within this batch.
func (b *Batch) Contains(seq uint32) bool {
	return seq >= b.Start && seq <= b.End
}

// BatchResult holds the results of processing a batch.
type BatchResult struct {
	// Batch is the original batch that was processed
	Batch *Batch

	// Tables holds statistics for each table
	Tables map[string]manifest.TableStats

	// TotalRows is the sum of all table row counts
	TotalRows int64

	// ProcessingTime is how long processing took
	ProcessingTime time.Duration

	// Error holds any error that occurred
	Error error
}

// NewBatchResult creates a new batch result.
func NewBatchResult(batch *Batch) *BatchResult {
	return &BatchResult{
		Batch:  batch,
		Tables: make(map[string]manifest.TableStats),
	}
}

// AddTable adds table statistics to the result.
func (r *BatchResult) AddTable(name string, rowCount int64) {
	r.Tables[name] = manifest.TableStats{RowCount: rowCount}
	r.TotalRows += rowCount
}

// IsSuccess returns true if processing succeeded.
func (r *BatchResult) IsSuccess() bool {
	return r.Error == nil
}

// LedgerStart returns the start ledger for compatibility with BatchStats.
func (r *BatchResult) LedgerStart() uint32 {
	return r.Batch.Start
}

// LedgerEnd returns the end ledger for compatibility with BatchStats.
func (r *BatchResult) LedgerEnd() uint32 {
	return r.Batch.End
}

// LedgerCount returns the number of ledgers in the batch.
func (r *BatchResult) LedgerCount() int {
	return r.Batch.Size()
}
