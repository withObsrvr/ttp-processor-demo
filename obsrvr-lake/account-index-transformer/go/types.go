package main

import "time"

// AccountLedgerIndex represents one account-to-ledger-range presence entry.
// It is intentionally range-granular: the query-api needs ledger_range pruning,
// not per-operation precision.
type AccountLedgerIndex struct {
	AccountID     string
	AccountBucket int64
	LedgerRange   int64
}

// TransformerStats tracks statistics for the transformer
type TransformerStats struct {
	LastLedgerProcessed   int64
	LastProcessedAt       time.Time
	TransformationsTotal  int64
	TransformationErrors  int64
	LastTransformDuration time.Duration
	LagSeconds            int64
	SourceMinLedger       int64
	SourceMaxLedger       int64
	CheckpointGapLedgers  int64
	RetentionGapDetected  bool
	RetentionGapMessage   string
}
