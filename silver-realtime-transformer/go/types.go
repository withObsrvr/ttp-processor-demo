package main

import (
	"database/sql"
	"time"
)

// EnrichedOperationRow represents a row in the enriched_history_operations table
type EnrichedOperationRow struct {
	// Operation fields
	TransactionHash      *string
	OperationIndex       *int
	LedgerSequence       *int64
	SourceAccount        *string
	Type                 *int
	TypeString           *string
	CreatedAt            *time.Time
	TransactionSuccessful *bool
	OperationResultCode  *string
	OperationTraceCode   *string
	LedgerRange          *int64

	// Asset fields
	SourceAccountMuxed *string
	Asset              *string
	AssetType          *string
	AssetCode          *string
	AssetIssuer        *string
	SourceAsset        *string
	SourceAssetType    *string
	SourceAssetCode    *string
	SourceAssetIssuer  *string

	// Operation-specific fields
	Destination      *string
	DestinationMuxed *string
	Amount           *int64
	SourceAmount     *int64
	FromAccount      *string
	FromMuxed        *string
	To               *string
	ToMuxed          *string

	// Trust line fields
	LimitAmount *int64

	// Offer fields
	OfferID            *int64
	SellingAsset       *string
	SellingAssetType   *string
	SellingAssetCode   *string
	SellingAssetIssuer *string
	BuyingAsset        *string
	BuyingAssetType    *string
	BuyingAssetCode    *string
	BuyingAssetIssuer  *string
	PriceN             *int
	PriceD             *int
	Price              *float64

	// Account management fields
	StartingBalance *int64
	HomeDomain      *string
	InflationDest   *string

	// Flags and thresholds
	SetFlags       *int
	SetFlagsS      interface{} // PostgreSQL array
	ClearFlags     *int
	ClearFlagsS    interface{} // PostgreSQL array
	MasterKeyWeight *int
	LowThreshold   *int
	MedThreshold   *int
	HighThreshold  *int

	// Signer fields
	SignerAccountID *string
	SignerKey       *string
	SignerWeight    *int

	// Data entry fields
	DataName  *string
	DataValue *string

	// Soroban fields
	HostFunctionType *string
	Parameters       *string
	Address          *string
	ContractID       *string
	FunctionName     *string

	// Claimable balance fields
	BalanceID      *string
	Claimant       *string
	ClaimantMuxed  *string
	Predicate      *string

	// Liquidity pool fields
	LiquidityPoolID *string
	ReserveAAsset   *string
	ReserveAAmount  *int64
	ReserveBAsset   *string
	ReserveBAmount  *int64
	Shares          *int64
	SharesReceived  *int64

	// Account merge
	Into      *string
	IntoMuxed *string

	// Sponsorship
	Sponsor      *string
	SponsoredID  *string
	BeginSponsor *string

	// Transaction fields (enriched)
	TxSuccessful     *bool
	TxFeeCharged     *int64
	TxMaxFee         *int64
	TxOperationCount *int
	TxMemoType       *string
	TxMemo           *string

	// Ledger fields (enriched)
	LedgerClosedAt           *time.Time
	LedgerTotalCoins         *int64
	LedgerFeePool            *int64
	LedgerBaseFee            *int
	LedgerBaseReserve        *int
	LedgerTransactionCount   *int
	LedgerOperationCount     *int
	LedgerSuccessfulTxCount  *int
	LedgerFailedTxCount      *int

	// Derived fields
	IsPaymentOp *bool
	IsSorobanOp *bool
}

// TokenTransferRow represents a row in the token_transfers_raw table
type TokenTransferRow struct {
	Timestamp             time.Time
	TransactionHash       string
	LedgerSequence        int64
	SourceType            string
	FromAccount           sql.NullString
	ToAccount             sql.NullString
	AssetCode             sql.NullString
	AssetIssuer           sql.NullString
	Amount                sql.NullInt64
	TokenContractID       sql.NullString
	OperationType         int
	TransactionSuccessful bool
}

// AccountCurrentRow represents a row in the accounts_current table
type AccountCurrentRow struct {
	AccountID           string
	Balance             string
	SequenceNumber      int64
	NumSubentries       int
	NumSponsoring       int
	NumSponsored        int
	HomeDomain          sql.NullString
	MasterWeight        int
	LowThreshold        int
	MedThreshold        int
	HighThreshold       int
	Flags               int
	AuthRequired        bool
	AuthRevocable       bool
	AuthImmutable       bool
	AuthClawbackEnabled bool
	Signers             sql.NullString
	SponsorAccount      sql.NullString
	CreatedAt           time.Time
	UpdatedAt           time.Time
	LastModifiedLedger  int64
	LedgerRange         int64
	EraID               sql.NullString
	VersionLabel        sql.NullString
}

// AccountSnapshotRow represents a row in the accounts_snapshot table (SCD Type 2)
type AccountSnapshotRow struct {
	AccountID           string
	LedgerSequence      int64
	ClosedAt            time.Time
	Balance             string
	SequenceNumber      int64
	NumSubentries       int
	NumSponsoring       int
	NumSponsored        int
	HomeDomain          sql.NullString
	MasterWeight        int
	LowThreshold        int
	MedThreshold        int
	HighThreshold       int
	Flags               int
	AuthRequired        bool
	AuthRevocable       bool
	AuthImmutable       bool
	AuthClawbackEnabled bool
	Signers             sql.NullString
	SponsorAccount      sql.NullString
	CreatedAt           time.Time
	UpdatedAt           time.Time
	LedgerRange         int64
	EraID               sql.NullString
	VersionLabel        sql.NullString
}

// TrustlineSnapshotRow represents a row in the trustlines_snapshot table (SCD Type 2)
type TrustlineSnapshotRow struct {
	AccountID                      string
	AssetCode                      string
	AssetIssuer                    string
	AssetType                      string
	Balance                        string
	TrustLimit                     string
	BuyingLiabilities              string
	SellingLiabilities             string
	Authorized                     bool
	AuthorizedToMaintainLiabilities bool
	ClawbackEnabled                bool
	LedgerSequence                 int64
	ClosedAt                       time.Time // from JOIN with ledgers
	CreatedAt                      time.Time
	LedgerRange                    int64
	EraID                          sql.NullString
	VersionLabel                   sql.NullString
}

// OfferSnapshotRow represents a row in the offers_snapshot table (SCD Type 2)
type OfferSnapshotRow struct {
	OfferID            int64
	SellerAccount      string
	LedgerSequence     int64
	ClosedAt           time.Time
	SellingAssetType   string
	SellingAssetCode   sql.NullString
	SellingAssetIssuer sql.NullString
	BuyingAssetType    string
	BuyingAssetCode    sql.NullString
	BuyingAssetIssuer  sql.NullString
	Amount             string
	Price              string
	Flags              int
	CreatedAt          time.Time
	LedgerRange        int64
	EraID              sql.NullString
	VersionLabel       sql.NullString
}

// AccountSignerSnapshotRow represents a row in the account_signers_snapshot table (SCD Type 2)
type AccountSignerSnapshotRow struct {
	AccountID      string
	Signer         string
	LedgerSequence int64
	ClosedAt       time.Time
	Weight         int
	Sponsor        sql.NullString
	LedgerRange    int64
	EraID          sql.NullString
	VersionLabel   sql.NullString
}

// ContractInvocationRow represents a row in the contract_invocations_raw table
// Extracted from Bronze operations_row_v2 where type = 24 (InvokeHostFunction)
type ContractInvocationRow struct {
	// TOID components
	LedgerSequence   int64
	TransactionIndex int
	OperationIndex   int

	// Transaction context
	TransactionHash string
	SourceAccount   string

	// Contract invocation details
	ContractID    string
	FunctionName  string
	ArgumentsJSON string

	// Execution context
	Successful bool
	ClosedAt   time.Time

	// Partitioning
	LedgerRange int64
}
