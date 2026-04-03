package main

import "time"

// HealthResponse is returned by the health endpoint.
type HealthResponse struct {
	Status  string `json:"status"`
	Service string `json:"service,omitempty"`
}

// DataBoundaries represents the available ledger range in the catalog.
type DataBoundaries struct {
	MinLedger int64  `json:"min_ledger"`
	MaxLedger int64  `json:"max_ledger"`
	UpdatedAt string `json:"updated_at,omitempty"`
}

// NetworkStats holds aggregate network statistics.
type NetworkStats struct {
	TotalTransactions  int64   `json:"total_transactions"`
	TotalOperations    int64   `json:"total_operations"`
	TotalLedgers       int64   `json:"total_ledgers"`
	AvgTxPerLedger     float64 `json:"avg_tx_per_ledger"`
	LatestLedger       int64   `json:"latest_ledger"`
	LatestLedgerClosed string  `json:"latest_ledger_closed,omitempty"`
}

// Ledger represents a row from bronze.ledgers_row_v2.
type Ledger struct {
	Sequence            int64     `json:"sequence"`
	LedgerHash          string    `json:"ledger_hash"`
	PreviousLedgerHash  string    `json:"previous_ledger_hash"`
	TransactionCount    int32     `json:"transaction_count"`
	OperationCount      int32     `json:"operation_count"`
	SuccessfulTxCount   int32     `json:"successful_tx_count"`
	FailedTxCount       int32     `json:"failed_tx_count"`
	ClosedAt            time.Time `json:"closed_at"`
	TotalCoins          int64     `json:"total_coins"`
	FeePool             int64     `json:"fee_pool"`
	BaseFee             int64     `json:"base_fee"`
	BaseReserve         int64     `json:"base_reserve"`
	MaxTxSetSize        int32     `json:"max_tx_set_size"`
	ProtocolVersion     int32     `json:"protocol_version"`
	TxSetOperationCount int32     `json:"tx_set_operation_count"`
}

// Transaction represents a row from bronze.transactions_row_v2.
type Transaction struct {
	LedgerSequence  int64     `json:"ledger_sequence"`
	TransactionHash string    `json:"transaction_hash"`
	SourceAccount   string    `json:"source_account"`
	AccountSequence int64     `json:"account_sequence"`
	MaxFee          int64     `json:"max_fee"`
	OperationCount  int32     `json:"operation_count"`
	Successful      bool      `json:"successful"`
	CreatedAt       time.Time `json:"created_at"`
}

// EnrichedOperation represents a row from silver.enriched_history_operations.
type EnrichedOperation struct {
	LedgerSequence  int64     `json:"ledger_sequence"`
	TransactionHash string    `json:"transaction_hash"`
	OperationIndex  int32     `json:"operation_index"`
	OpType          string    `json:"op_type"`
	SourceAccount   string    `json:"source_account"`
	CreatedAt       time.Time `json:"created_at"`
}

// AccountInfo represents account data.
type AccountInfo struct {
	AccountID      string `json:"account_id"`
	Balance        string `json:"balance"`
	SequenceNumber int64  `json:"sequence_number"`
	NumSubentries  int32  `json:"num_subentries"`
	HomeDomain     string `json:"home_domain,omitempty"`
	LedgerSequence int64  `json:"ledger_sequence"`
}

// TopAccount represents an account entry for the top accounts endpoint.
type TopAccount struct {
	AccountID string `json:"account_id"`
	Balance   string `json:"balance"`
}

// OperationFilters holds query parameters for filtering operations.
type OperationFilters struct {
	StartLedger int64
	EndLedger   int64
	OpType      string
	Account     string
	Limit       int
}

// listResponse creates a response map with a named key for the data array.
// This matches stellar-query-api's convention of using descriptive keys
// (e.g., "accounts", "operations") instead of generic "data".
func listResponse(key string, data interface{}, count int) map[string]interface{} {
	return map[string]interface{}{key: data, "count": count}
}

// ListResponse wraps paginated list results with a generic "data" key.
// Deprecated: prefer listResponse(key, data, count) for descriptive keys.
type ListResponse struct {
	Data  interface{} `json:"data"`
	Count int         `json:"count"`
}
