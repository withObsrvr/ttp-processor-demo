package main

import (
	"database/sql"
	"time"
)

// AccountLedgerIndex represents one account-to-ledger-range presence entry.
// It is intentionally range-granular: the query-api needs ledger_range pruning,
// not per-operation precision.
type AccountLedgerIndex struct {
	AccountID     string
	AccountBucket int64
	LedgerRange   int64
}

// AccountFeedRow is one account-scoped transaction/operation serving row.
type AccountFeedRow struct {
	AccountID          string
	SourceMask         int16
	TOID               int64
	OperationTOID      int64
	TxHash             string
	LedgerSequence     int64
	LedgerClosedAt     time.Time
	Successful         bool
	ActivityType       string
	SourceAccount      sql.NullString
	DestinationAccount sql.NullString
	PrimaryContractID  sql.NullString
	OperationCount     sql.NullInt64
	FeeCharged         sql.NullInt64
	MemoType           sql.NullString
	MemoValue          sql.NullString
	OperationIndex     int64
	OperationType      sql.NullInt64
	OperationTypeName  sql.NullString
	AssetKey           sql.NullString
	AmountStroops      sql.NullInt64
	ContractID         sql.NullString
	FunctionName       sql.NullString
	IsPaymentOp        bool
	IsSorobanOp        bool
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
