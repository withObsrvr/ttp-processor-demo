package main

import "time"

// TransactionIndex represents a transaction hash index entry
type TransactionIndex struct {
	TxHash         string
	LedgerSequence int64
	OperationCount int32
	Successful     bool
	ClosedAt       time.Time
	LedgerRange    int64
	CreatedAt      time.Time
}

// TransformerStats tracks statistics for the transformer
type TransformerStats struct {
	LastLedgerProcessed   int64
	LastProcessedAt       time.Time
	TransformationsTotal  int64
	TransformationErrors  int64
	LastTransformDuration time.Duration
	LagSeconds            int64
}
