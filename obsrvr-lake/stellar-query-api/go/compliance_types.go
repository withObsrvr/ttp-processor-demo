package main

// ============================================
// COMPLIANCE ARCHIVE API - Response Types
// ============================================

// PeriodInfo represents a time period for archive queries
type PeriodInfo struct {
	Start       string `json:"start"`
	End         string `json:"end"`
	StartLedger int64  `json:"start_ledger,omitempty"`
	EndLedger   int64  `json:"end_ledger,omitempty"`
}

// ComplianceTransaction represents a single transaction in compliance archive
type ComplianceTransaction struct {
	LedgerSequence  int64  `json:"ledger_sequence"`
	ClosedAt        string `json:"closed_at"`
	TransactionHash string `json:"transaction_hash"`
	OperationIndex  int    `json:"operation_index"`
	OperationType   string `json:"operation_type"`
	FromAccount     string `json:"from_account"`
	ToAccount       string `json:"to_account,omitempty"`
	Amount          string `json:"amount"`
	Successful      bool   `json:"successful"`
}

// TransactionSummary provides aggregate info about transactions
type TransactionSummary struct {
	TotalTransactions      int    `json:"total_transactions"`
	TotalVolume            string `json:"total_volume"`
	UniqueAccounts         int    `json:"unique_accounts"`
	SuccessfulTransactions int    `json:"successful_transactions"`
	FailedTransactions     int    `json:"failed_transactions"`
}

// ComplianceTransactionsResponse for GET /gold/compliance/transactions
type ComplianceTransactionsResponse struct {
	ArchiveID          string                  `json:"archive_id"`
	Asset              AssetInfo               `json:"asset"`
	Period             PeriodInfo              `json:"period"`
	Summary            TransactionSummary      `json:"summary"`
	Transactions       []ComplianceTransaction `json:"transactions"`
	Checksum           string                  `json:"checksum"`
	MethodologyVersion string                  `json:"methodology_version"`
	GeneratedAt        string                  `json:"generated_at"`
}

// ComplianceHolder represents a holder in compliance balance archive
type ComplianceHolder struct {
	AccountID       string `json:"account_id"`
	Balance         string `json:"balance"`
	PercentOfSupply string `json:"percent_of_supply"`
}

// BalanceSummary provides aggregate info about balances
type BalanceSummary struct {
	TotalHolders      int    `json:"total_holders"`
	TotalSupply       string `json:"total_supply"`
	IssuerBalance     string `json:"issuer_balance"`
	CirculatingSupply string `json:"circulating_supply"`
}

// ComplianceBalancesResponse for GET /gold/compliance/balances
type ComplianceBalancesResponse struct {
	ArchiveID          string             `json:"archive_id"`
	Asset              AssetInfo          `json:"asset"`
	SnapshotAt         string             `json:"snapshot_at"`
	SnapshotLedger     int64              `json:"snapshot_ledger"`
	Summary            BalanceSummary     `json:"summary"`
	Holders            []ComplianceHolder `json:"holders"`
	Checksum           string             `json:"checksum"`
	MethodologyVersion string             `json:"methodology_version"`
	GeneratedAt        string             `json:"generated_at"`
}

// SupplyDataPoint represents a single point in supply timeline
type SupplyDataPoint struct {
	Timestamp           string  `json:"timestamp"`
	LedgerSequence      int64   `json:"ledger_sequence,omitempty"`
	TotalSupply         string  `json:"total_supply"`
	CirculatingSupply   string  `json:"circulating_supply"`
	IssuerBalance       string  `json:"issuer_balance"`
	HolderCount         int     `json:"holder_count"`
	SupplyChange        *string `json:"supply_change,omitempty"`
	SupplyChangePercent *string `json:"supply_change_percent,omitempty"`
}

// SupplySummary provides aggregate info about supply over a period
type SupplySummary struct {
	StartSupply   string `json:"start_supply"`
	EndSupply     string `json:"end_supply"`
	NetMinted     string `json:"net_minted"`
	PeakSupply    string `json:"peak_supply"`
	PeakDate      string `json:"peak_date"`
	LowestSupply  string `json:"lowest_supply"`
	LowestDate    string `json:"lowest_date"`
	DataPoints    int    `json:"data_points"`
}

// ComplianceSupplyResponse for GET /gold/compliance/supply
type ComplianceSupplyResponse struct {
	ArchiveID          string            `json:"archive_id"`
	Asset              AssetInfo         `json:"asset"`
	Period             PeriodInfo        `json:"period"`
	Interval           string            `json:"interval"`
	Timeline           []SupplyDataPoint `json:"timeline"`
	Summary            SupplySummary     `json:"summary"`
	Checksum           string            `json:"checksum"`
	MethodologyVersion string            `json:"methodology_version"`
	GeneratedAt        string            `json:"generated_at"`
}

// Methodology version constants
const (
	MethodologyTransactionsV1 = "asset_transactions_v1"
	MethodologyBalancesV1     = "balance_snapshot_v1"
	MethodologySupplyV1       = "supply_timeline_v1"
	MethodologyArchiveV1      = "compliance_archive_v1"
)

// ============================================
// FULL ARCHIVE TYPES (POST /gold/compliance/archive)
// ============================================

// ArchiveStatus represents the status of an archive job
type ArchiveStatus string

const (
	ArchiveStatusPending    ArchiveStatus = "pending"
	ArchiveStatusProcessing ArchiveStatus = "processing"
	ArchiveStatusComplete   ArchiveStatus = "complete"
	ArchiveStatusFailed     ArchiveStatus = "failed"
)

// FullArchiveRequest for POST /gold/compliance/archive
type FullArchiveRequest struct {
	AssetCode        string   `json:"asset_code"`
	AssetIssuer      string   `json:"asset_issuer"`
	StartDate        string   `json:"start_date"`
	EndDate          string   `json:"end_date"`
	Include          []string `json:"include"`           // ["transactions", "balances", "supply"]
	BalanceSnapshots []string `json:"balance_snapshots"` // specific dates for balance snapshots
	Format           string   `json:"format"`            // "json" or "csv"
}

// ArchiveArtifact represents a single file in an archive
type ArchiveArtifact struct {
	Name        string `json:"name"`
	Type        string `json:"type"` // transactions, balance_snapshot, supply_timeline, manifest
	SnapshotAt  string `json:"snapshot_at,omitempty"`
	RowCount    int    `json:"row_count,omitempty"`
	Checksum    string `json:"checksum"`
	GeneratedAt string `json:"generated_at"`
}

// FullArchiveResponse for POST /gold/compliance/archive (initial response)
type FullArchiveResponse struct {
	ArchiveID           string `json:"archive_id"`
	Status              string `json:"status"`
	EstimatedCompletion string `json:"estimated_completion,omitempty"`
	CallbackURL         string `json:"callback_url"`
	CreatedAt           string `json:"created_at"`
}

// FullArchiveStatusResponse for GET /gold/compliance/archive/{id}
type FullArchiveStatusResponse struct {
	ArchiveID          string            `json:"archive_id"`
	Status             string            `json:"status"`
	Asset              AssetInfo         `json:"asset"`
	Period             PeriodInfo        `json:"period"`
	Artifacts          []ArchiveArtifact `json:"artifacts,omitempty"`
	Error              string            `json:"error,omitempty"`
	MethodologyVersion string            `json:"methodology_version"`
	CreatedAt          string            `json:"created_at"`
	CompletedAt        string            `json:"completed_at,omitempty"`
}

// ============================================
// LINEAGE TYPES (GET /gold/compliance/lineage)
// ============================================

// ArchiveLineageEntry represents a single archive in the lineage
type ArchiveLineageEntry struct {
	ArchiveID          string            `json:"archive_id"`
	AssetCode          string            `json:"asset_code"`
	AssetIssuer        string            `json:"asset_issuer,omitempty"`
	PeriodStart        string            `json:"period_start"`
	PeriodEnd          string            `json:"period_end"`
	ArtifactsCount     int               `json:"artifacts_count"`
	MethodologyVersion string            `json:"methodology_version"`
	Status             string            `json:"status"`
	Checksums          map[string]string `json:"checksums"`
	GeneratedAt        string            `json:"generated_at"`
}

// LineageResponse for GET /gold/compliance/lineage
type LineageResponse struct {
	Archives    []ArchiveLineageEntry `json:"archives"`
	Count       int                   `json:"count"`
	GeneratedAt string                `json:"generated_at"`
}
