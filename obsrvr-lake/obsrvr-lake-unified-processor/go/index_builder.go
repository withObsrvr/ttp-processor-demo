package main

import (
	"context"
	"database/sql"
)

// buildIndexes materializes index tables for fast lookups.
// Stub — to be implemented incrementally.
func (w *DuckLakeWriter) buildIndexes(ctx context.Context, tx *sql.Tx, cat, bronze, idx string, startSeq, endSeq int64) error {
	// No-op: index building will be ported later
	return nil
}
