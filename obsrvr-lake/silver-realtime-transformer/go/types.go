package main

import (
	"database/sql"
	"fmt"
	"math"
	"strconv"
	"time"

	"github.com/lib/pq"
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
	Amount                sql.NullString
	TokenContractID       sql.NullString
	OperationType         int
	TransactionSuccessful bool
	EventIndex            sql.NullInt32
}

// UnmatchedContractEventRow preserves token-like Soroban contract events whose
// payload shape could not be confidently normalized into token_transfers_raw.
type UnmatchedContractEventRow struct {
	Timestamp       time.Time
	TransactionHash string
	LedgerSequence  int64
	ContractID      sql.NullString
	EventIndex      sql.NullInt32
	EventName       sql.NullString
	TopicsDecoded   sql.NullString
	DataDecoded     sql.NullString
	Successful      bool
	ParseReason     string
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
	AccountID                       string
	AssetCode                       string
	AssetIssuer                     string
	AssetType                       string
	Balance                         string
	TrustLimit                      string
	BuyingLiabilities               string
	SellingLiabilities              string
	Authorized                      bool
	AuthorizedToMaintainLiabilities bool
	ClawbackEnabled                 bool
	LedgerSequence                  int64
	ClosedAt                        time.Time // from JOIN with ledgers
	CreatedAt                       time.Time
	LedgerRange                     int64
	EraID                           sql.NullString
	VersionLabel                    sql.NullString
}

// TrustlineCurrentRow represents a row in the trustlines_current table
type TrustlineCurrentRow struct {
	AccountID                       string
	AssetType                       string
	AssetIssuer                     string
	AssetCode                       string
	LiquidityPoolID                 sql.NullString // NULL for classic trustlines
	Balance                         string         // stored as TEXT in bronze, converted to BIGINT for silver
	TrustLineLimit                  string
	BuyingLiabilities               string
	SellingLiabilities              string
	Flags                           int // Computed from: authorized(1) + auth_to_maintain(2) + clawback(4)
	LastModifiedLedger              int64
	LedgerSequence                  int64
	CreatedAt                       time.Time
	Sponsor                         sql.NullString
	LedgerRange                     int64

	// Temporary fields for bronze parsing (not written to silver)
	Authorized                      bool
	AuthorizedToMaintainLiabilities bool
	ClawbackEnabled                 bool
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

// OfferCurrentRow represents a row in the offers_current table
type OfferCurrentRow struct {
	OfferID            int64
	SellerID           string
	SellingAssetType   string
	SellingAssetCode   sql.NullString
	SellingAssetIssuer sql.NullString
	BuyingAssetType    string
	BuyingAssetCode    sql.NullString
	BuyingAssetIssuer  sql.NullString
	Amount             string // stored as TEXT in bronze, converted to BIGINT for silver
	PriceN             int    // Parsed from price string or defaulted
	PriceD             int    // Parsed from price string or defaulted
	Price              string // stored as TEXT in bronze, converted to DECIMAL for silver
	Flags              int
	LastModifiedLedger int64
	LedgerSequence     int64
	CreatedAt          time.Time
	Sponsor            sql.NullString
	LedgerRange        int64
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

	// Call graph (from Bronze layer)
	ContractCallsJSON sql.NullString
	ContractsInvolved []string
	MaxCallDepth      sql.NullInt32

	// Execution context
	Successful bool
	ClosedAt   time.Time

	// Partitioning
	LedgerRange int64
}

// ContractCallRow represents a row in the contract_invocation_calls table
// Flattened cross-contract call relationships extracted from call graph
type ContractCallRow struct {
	// Identity
	CallID int64 // Auto-generated

	// TOID components (for joining back to operations)
	LedgerSequence   int64
	TransactionIndex int
	OperationIndex   int
	TransactionHash  string

	// Call relationship
	FromContract string
	ToContract   string
	FunctionName string
	CallDepth    int
	ExecutionOrder int

	// Status
	Successful bool
	ClosedAt   time.Time

	// Partitioning
	LedgerRange int64
}

// ContractHierarchyRow represents a row in the contract_invocation_hierarchy table
// Pre-computed ancestry chains for efficient contract relationship queries
type ContractHierarchyRow struct {
	// Identity
	TransactionHash string
	RootContract    string
	ChildContract   string

	// Hierarchy
	PathDepth int
	FullPath  []string // All contracts in the path from root to child

	// Partitioning
	LedgerRange int64
}

// LiquidityPoolCurrentRow represents a row in the liquidity_pools_current table
type LiquidityPoolCurrentRow struct {
	LiquidityPoolID   string
	PoolType          string
	Fee               int
	TrustlineCount    int
	TotalPoolShares   int64
	AssetAType        string
	AssetACode        sql.NullString
	AssetAIssuer      sql.NullString
	AssetAAmount      int64
	AssetBType        string
	AssetBCode        sql.NullString
	AssetBIssuer      sql.NullString
	AssetBAmount      int64
	LastModifiedLedger int64
	LedgerSequence    int64
	ClosedAt          time.Time
	CreatedAt         time.Time
	LedgerRange       int64
}

// ClaimableBalanceCurrentRow represents a row in the claimable_balances_current table
type ClaimableBalanceCurrentRow struct {
	BalanceID          string
	Sponsor            string
	AssetType          string
	AssetCode          sql.NullString
	AssetIssuer        sql.NullString
	Amount             int64
	ClaimantsCount     int
	Flags              int
	LastModifiedLedger int64
	LedgerSequence     int64
	ClosedAt           time.Time
	CreatedAt          time.Time
	LedgerRange        int64
}

// NativeBalanceCurrentRow represents a row in the native_balances_current table
type NativeBalanceCurrentRow struct {
	AccountID          string
	Balance            int64
	BuyingLiabilities  int64
	SellingLiabilities int64
	NumSubentries      int
	NumSponsoring      int
	NumSponsored       int
	SequenceNumber     sql.NullInt64
	LastModifiedLedger int64
	LedgerSequence     int64
	LedgerRange        int64
}

// TradeRow represents a row in the trades table (event stream - append only)
// Extracted from Bronze trades_row_v1
type TradeRow struct {
	LedgerSequence     int64
	TransactionHash    string
	OperationIndex     int
	TradeIndex         int
	TradeType          string
	TradeTimestamp     time.Time
	SellerAccount      string
	SellingAssetCode   sql.NullString
	SellingAssetIssuer sql.NullString
	SellingAmount      string // Decimal string from bronze
	BuyerAccount       string
	BuyingAssetCode    sql.NullString
	BuyingAssetIssuer  sql.NullString
	BuyingAmount       string // Decimal string from bronze
	Price              string // Decimal string from bronze
	CreatedAt          time.Time
	LedgerRange        sql.NullInt64
}

// EffectRow represents a row in the effects table (event stream - append only)
// Extracted from Bronze effects_row_v1
type EffectRow struct {
	LedgerSequence   int64
	TransactionHash  string
	OperationIndex   int
	EffectIndex      int
	OperationID      sql.NullInt64
	EffectType       int
	EffectTypeString string
	AccountID        sql.NullString
	Amount           sql.NullString
	AssetCode        sql.NullString
	AssetIssuer      sql.NullString
	AssetType        sql.NullString
	DetailsJSON      sql.NullString
	TrustlineLimit   sql.NullString
	AuthorizeFlag    sql.NullBool
	ClawbackFlag     sql.NullBool
	SignerAccount    sql.NullString
	SignerWeight     sql.NullInt32
	OfferID          sql.NullInt64
	SellerAccount    sql.NullString
	CreatedAt        time.Time
	LedgerRange      sql.NullInt64
}

// =============================================================================
// Phase 3: Soroban Tables
// =============================================================================

// ContractDataCurrentRow represents a row in the contract_data_current table
// Extracted from Bronze contract_data_snapshot_v1 (UPSERT pattern)
type ContractDataCurrentRow struct {
	ContractID         string
	Key                sql.NullString // XDR encoded key
	KeyHash            string
	Durability         string // "temporary" or "persistent"
	AssetType          sql.NullString
	AssetCode          sql.NullString
	AssetIssuer        sql.NullString
	DataValue          sql.NullString // XDR encoded value (contract_data_xdr)
	LastModifiedLedger int64
	LedgerSequence     int64
	ClosedAt           time.Time
	CreatedAt          time.Time
	LedgerRange        int64
}

// ContractCodeCurrentRow represents a row in the contract_code_current table
// Extracted from Bronze contract_code_snapshot_v1 (UPSERT pattern)
type ContractCodeCurrentRow struct {
	ContractCodeHash   string
	ContractCodeExtV   sql.NullString
	NDataSegmentBytes  sql.NullInt32
	NDataSegments      sql.NullInt32
	NElemSegments      sql.NullInt32
	NExports           sql.NullInt32
	NFunctions         sql.NullInt32
	NGlobals           sql.NullInt32
	NImports           sql.NullInt32
	NInstructions      sql.NullInt32
	NTableEntries      sql.NullInt32
	NTypes             sql.NullInt32
	LastModifiedLedger int64
	LedgerSequence     int64
	ClosedAt           time.Time
	CreatedAt          time.Time
	LedgerRange        int64
}

// TTLCurrentRow represents a row in the ttl_current table
// Extracted from Bronze ttl_snapshot_v1 (UPSERT pattern)
type TTLCurrentRow struct {
	KeyHash             string
	LiveUntilLedgerSeq  int64
	TTLRemaining        sql.NullInt32 // Computed: live_until_ledger_seq - ledger_sequence
	Expired             bool
	LastModifiedLedger  int64
	LedgerSequence      int64
	ClosedAt            time.Time
	CreatedAt           time.Time
	LedgerRange         int64
}

// EvictedKeyRow represents a row in the evicted_keys table (event stream - append only)
// Extracted from Bronze evicted_keys_state_v1
type EvictedKeyRow struct {
	ContractID     string
	KeyHash        string
	LedgerSequence int64
	ClosedAt       time.Time
	CreatedAt      time.Time
	LedgerRange    int64
}

// RestoredKeyRow represents a row in the restored_keys table (event stream - append only)
// Extracted from Bronze restored_keys_state_v1
type RestoredKeyRow struct {
	ContractID     string
	KeyHash        string
	LedgerSequence int64
	ClosedAt       time.Time
	CreatedAt      time.Time
	LedgerRange    int64
}

// =============================================================================
// Phase 4: Config Settings
// =============================================================================

// TokenRegistryRow represents a row in the token_registry table
// Materialized from Bronze contract_data_snapshot_v1 instance entries with token metadata
type TokenRegistryRow struct {
	ContractID        string
	TokenName         sql.NullString
	TokenSymbol       sql.NullString
	TokenDecimals     int
	AssetCode         sql.NullString
	AssetIssuer       sql.NullString
	TokenType         string // "sac" or "custom_soroban"
	FirstSeenLedger   int64
	LastModifiedLedger int64
}

// ConfigSettingsCurrentRow represents a row in the config_settings_current table
// Extracted from Bronze config_settings_snapshot_v1 (UPSERT pattern)
// Contains Soroban network configuration parameters
type ConfigSettingsCurrentRow struct {
	ConfigSettingID int

	// Instruction limits
	LedgerMaxInstructions           sql.NullInt64
	TxMaxInstructions               sql.NullInt64
	FeeRatePerInstructionsIncrement sql.NullInt64
	TxMemoryLimit                   sql.NullInt64

	// Ledger read/write limits
	LedgerMaxReadLedgerEntries  sql.NullInt64
	LedgerMaxReadBytes          sql.NullInt64
	LedgerMaxWriteLedgerEntries sql.NullInt64
	LedgerMaxWriteBytes         sql.NullInt64

	// Transaction read/write limits
	TxMaxReadLedgerEntries  sql.NullInt64
	TxMaxReadBytes          sql.NullInt64
	TxMaxWriteLedgerEntries sql.NullInt64
	TxMaxWriteBytes         sql.NullInt64

	// Contract limits
	ContractMaxSizeBytes sql.NullInt64

	// Raw XDR for additional settings
	ConfigSettingXDR string

	// Metadata
	LastModifiedLedger int
	LedgerSequence     int64
	ClosedAt           time.Time
	CreatedAt          time.Time
	LedgerRange        int64
}

// =============================================================================
// Values() methods for batch inserting
// =============================================================================

// Values returns the ordered column values for batch insertion into enriched_history_operations
// (also used for enriched_history_operations_soroban since columns are identical).
func (row *EnrichedOperationRow) Values() []interface{} {
	return []interface{}{
		row.TransactionHash, row.OperationIndex, row.LedgerSequence, row.SourceAccount,
		row.Type, row.TypeString, row.CreatedAt, row.TransactionSuccessful,
		row.OperationResultCode, row.OperationTraceCode, row.LedgerRange,
		row.SourceAccountMuxed, row.Asset, row.AssetType, row.AssetCode, row.AssetIssuer,
		row.SourceAsset, row.SourceAssetType, row.SourceAssetCode, row.SourceAssetIssuer,
		row.Destination, row.DestinationMuxed, row.Amount, row.SourceAmount,
		row.FromAccount, row.FromMuxed, row.To, row.ToMuxed,
		row.LimitAmount, row.OfferID,
		row.SellingAsset, row.SellingAssetType, row.SellingAssetCode, row.SellingAssetIssuer,
		row.BuyingAsset, row.BuyingAssetType, row.BuyingAssetCode, row.BuyingAssetIssuer,
		row.PriceN, row.PriceD, row.Price,
		row.StartingBalance, row.HomeDomain, row.InflationDest,
		row.SetFlags, row.SetFlagsS, row.ClearFlags, row.ClearFlagsS,
		row.MasterKeyWeight, row.LowThreshold, row.MedThreshold, row.HighThreshold,
		row.SignerAccountID, row.SignerKey, row.SignerWeight,
		row.DataName, row.DataValue,
		row.HostFunctionType, row.Parameters, row.Address, row.ContractID, row.FunctionName,
		row.BalanceID, row.Claimant, row.ClaimantMuxed, row.Predicate,
		row.LiquidityPoolID, row.ReserveAAsset, row.ReserveAAmount,
		row.ReserveBAsset, row.ReserveBAmount, row.Shares, row.SharesReceived,
		row.Into, row.IntoMuxed,
		row.Sponsor, row.SponsoredID, row.BeginSponsor,
		row.TxSuccessful, row.TxFeeCharged, row.TxMaxFee, row.TxOperationCount,
		row.TxMemoType, row.TxMemo,
		row.LedgerClosedAt, row.LedgerTotalCoins, row.LedgerFeePool,
		row.LedgerBaseFee, row.LedgerBaseReserve,
		row.LedgerTransactionCount, row.LedgerOperationCount,
		row.LedgerSuccessfulTxCount, row.LedgerFailedTxCount,
		row.IsPaymentOp, row.IsSorobanOp,
	}
}

// Values returns the ordered column values for batch insertion into token_transfers_raw.
func (row *TokenTransferRow) Values() []interface{} {
	return []interface{}{
		row.Timestamp, row.TransactionHash, row.LedgerSequence, row.SourceType,
		row.FromAccount, row.ToAccount, row.AssetCode, row.AssetIssuer, row.Amount,
		row.TokenContractID, row.OperationType, row.TransactionSuccessful, row.EventIndex,
	}
}

// Values returns the ordered column values for batch insertion into contract_events_unmatched.
func (row *UnmatchedContractEventRow) Values() []interface{} {
	return []interface{}{
		row.Timestamp, row.TransactionHash, row.LedgerSequence, row.ContractID,
		row.EventIndex, row.EventName, row.TopicsDecoded, row.DataDecoded,
		row.Successful, row.ParseReason,
	}
}

// Values returns the ordered column values for batch insertion into accounts_current.
func (row *AccountCurrentRow) Values() []interface{} {
	return []interface{}{
		row.AccountID, row.Balance, row.SequenceNumber, row.NumSubentries,
		row.NumSponsoring, row.NumSponsored, row.HomeDomain,
		row.MasterWeight, row.LowThreshold, row.MedThreshold, row.HighThreshold,
		row.Flags, row.AuthRequired, row.AuthRevocable, row.AuthImmutable, row.AuthClawbackEnabled,
		row.Signers, row.SponsorAccount, row.CreatedAt, row.UpdatedAt,
		row.LastModifiedLedger, row.LedgerRange, row.EraID, row.VersionLabel,
	}
}

// Values returns the ordered column values for batch insertion into accounts_snapshot.
// Includes valid_to = nil (NULL) as the last column.
func (row *AccountSnapshotRow) Values() []interface{} {
	return []interface{}{
		row.AccountID, row.LedgerSequence, row.ClosedAt, row.Balance, row.SequenceNumber,
		row.NumSubentries, row.NumSponsoring, row.NumSponsored, row.HomeDomain,
		row.MasterWeight, row.LowThreshold, row.MedThreshold, row.HighThreshold,
		row.Flags, row.AuthRequired, row.AuthRevocable, row.AuthImmutable, row.AuthClawbackEnabled,
		row.Signers, row.SponsorAccount, row.CreatedAt, row.UpdatedAt,
		row.LedgerRange, row.EraID, row.VersionLabel, nil, // valid_to = NULL
	}
}

// Values returns the ordered column values for batch insertion into trustlines_snapshot.
// Includes valid_to = nil (NULL) as the last column.
func (row *TrustlineSnapshotRow) Values() []interface{} {
	return []interface{}{
		row.AccountID, row.AssetCode, row.AssetIssuer, row.AssetType, row.Balance, row.TrustLimit,
		row.BuyingLiabilities, row.SellingLiabilities, row.Authorized,
		row.AuthorizedToMaintainLiabilities, row.ClawbackEnabled,
		row.LedgerSequence, row.CreatedAt, row.LedgerRange, row.EraID, row.VersionLabel, nil, // valid_to
	}
}

// TrustlineCurrentValues returns the ordered column values for batch insertion into trustlines_current.
// Converts string balance/limit/liabilities to stroops (int64) in Go since the original SQL
// used ROUND($N::NUMERIC * 10000000)::BIGINT.
func (row *TrustlineCurrentRow) TrustlineCurrentValues() ([]interface{}, error) {
	balance, err := parseStroops(row.Balance)
	if err != nil {
		return nil, fmt.Errorf("trustline %s balance: %w", row.AccountID, err)
	}
	limit, err := parseStroops(row.TrustLineLimit)
	if err != nil {
		return nil, fmt.Errorf("trustline %s limit: %w", row.AccountID, err)
	}
	buyLiab, err := parseStroops(row.BuyingLiabilities)
	if err != nil {
		return nil, fmt.Errorf("trustline %s buying_liabilities: %w", row.AccountID, err)
	}
	sellLiab, err := parseStroops(row.SellingLiabilities)
	if err != nil {
		return nil, fmt.Errorf("trustline %s selling_liabilities: %w", row.AccountID, err)
	}
	return []interface{}{
		row.AccountID, row.AssetType, row.AssetIssuer, row.AssetCode, row.LiquidityPoolID,
		balance, limit, buyLiab, sellLiab,
		row.Flags, row.LastModifiedLedger, row.LedgerSequence, row.CreatedAt, row.Sponsor, row.LedgerRange,
	}, nil
}

// parseStroops converts a decimal string to stroops (multiply by 10^7 and round to int64).
// Returns an error on malformed input instead of silently returning 0.
func parseStroops(s string) (int64, error) {
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0, fmt.Errorf("parseStroops(%q): %w", s, err)
	}
	return int64(math.Round(f * 10000000)), nil
}

// Values returns the ordered column values for batch insertion into offers_snapshot.
// Includes valid_to = nil (NULL) as the last column.
func (row *OfferSnapshotRow) Values() []interface{} {
	return []interface{}{
		row.OfferID, row.SellerAccount, row.LedgerSequence, row.ClosedAt,
		row.SellingAssetType, row.SellingAssetCode, row.SellingAssetIssuer,
		row.BuyingAssetType, row.BuyingAssetCode, row.BuyingAssetIssuer,
		row.Amount, row.Price, row.Flags, row.CreatedAt, row.LedgerRange, row.EraID, row.VersionLabel, nil, // valid_to
	}
}

// OfferCurrentValues returns the ordered column values for batch insertion into offers_current.
// Amount is passed as int64 (parsed from string), price as string (already formatted).
func (row *OfferCurrentRow) OfferCurrentValues() ([]interface{}, error) {
	amount, err := strconv.ParseInt(row.Amount, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("offer %d amount %q: %w", row.OfferID, row.Amount, err)
	}
	return []interface{}{
		row.OfferID, row.SellerID, row.SellingAssetType, row.SellingAssetCode, row.SellingAssetIssuer,
		row.BuyingAssetType, row.BuyingAssetCode, row.BuyingAssetIssuer,
		amount, row.PriceN, row.PriceD, row.Price, row.Flags,
		row.LastModifiedLedger, row.LedgerSequence, row.CreatedAt, row.Sponsor, row.LedgerRange,
	}, nil
}

// Values returns the ordered column values for batch insertion into account_signers_snapshot.
// Includes valid_to = nil (NULL) as the last column.
func (row *AccountSignerSnapshotRow) Values() []interface{} {
	return []interface{}{
		row.AccountID, row.Signer, row.LedgerSequence, row.ClosedAt, row.Weight, row.Sponsor,
		row.LedgerRange, row.EraID, row.VersionLabel, nil, // valid_to
	}
}

// Values returns the ordered column values for batch insertion into contract_invocations_raw.
func (row *ContractInvocationRow) Values() []interface{} {
	return []interface{}{
		row.LedgerSequence, row.TransactionIndex, row.OperationIndex,
		row.TransactionHash, row.SourceAccount, row.ContractID, row.FunctionName,
		row.ArgumentsJSON, row.Successful, row.ClosedAt, row.LedgerRange,
	}
}

// Values returns the ordered column values for batch insertion into contract_invocation_calls.
func (row *ContractCallRow) Values() []interface{} {
	return []interface{}{
		row.LedgerSequence, row.TransactionIndex, row.OperationIndex, row.TransactionHash,
		row.FromContract, row.ToContract, row.FunctionName, row.CallDepth, row.ExecutionOrder,
		row.Successful, row.ClosedAt, row.LedgerRange,
	}
}

// Values returns the ordered column values for batch insertion into contract_invocation_hierarchy.
// Uses pq.Array for the full_path column.
func (row *ContractHierarchyRow) Values() []interface{} {
	return []interface{}{
		row.TransactionHash, row.RootContract, row.ChildContract, row.PathDepth,
		pq.Array(row.FullPath), row.LedgerRange,
	}
}

// Values returns the ordered column values for batch insertion into liquidity_pools_current.
func (row *LiquidityPoolCurrentRow) Values() []interface{} {
	return []interface{}{
		row.LiquidityPoolID, row.PoolType, row.Fee, row.TrustlineCount, row.TotalPoolShares,
		row.AssetAType, row.AssetACode, row.AssetAIssuer, row.AssetAAmount,
		row.AssetBType, row.AssetBCode, row.AssetBIssuer, row.AssetBAmount,
		row.LastModifiedLedger, row.LedgerSequence, row.ClosedAt, row.CreatedAt, row.LedgerRange,
	}
}

// Values returns the ordered column values for batch insertion into claimable_balances_current.
func (row *ClaimableBalanceCurrentRow) Values() []interface{} {
	return []interface{}{
		row.BalanceID, row.Sponsor, row.AssetType, row.AssetCode, row.AssetIssuer, row.Amount,
		row.ClaimantsCount, row.Flags, row.LastModifiedLedger, row.LedgerSequence,
		row.ClosedAt, row.CreatedAt, row.LedgerRange,
	}
}

// Values returns the ordered column values for batch insertion into native_balances_current.
func (row *NativeBalanceCurrentRow) Values() []interface{} {
	return []interface{}{
		row.AccountID, row.Balance, row.BuyingLiabilities, row.SellingLiabilities,
		row.NumSubentries, row.NumSponsoring, row.NumSponsored, row.SequenceNumber,
		row.LastModifiedLedger, row.LedgerSequence, row.LedgerRange,
	}
}

// TradeValues returns the ordered column values for batch insertion into trades.
// Converts selling_amount and buying_amount from strings to int64.
func (row *TradeRow) TradeValues() ([]interface{}, error) {
	sellingAmount, err := strconv.ParseInt(row.SellingAmount, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("trade selling_amount %q: %w", row.SellingAmount, err)
	}
	buyingAmount, err := strconv.ParseInt(row.BuyingAmount, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("trade buying_amount %q: %w", row.BuyingAmount, err)
	}
	return []interface{}{
		row.LedgerSequence, row.TransactionHash, row.OperationIndex, row.TradeIndex,
		row.TradeType, row.TradeTimestamp, row.SellerAccount,
		row.SellingAssetCode, row.SellingAssetIssuer, sellingAmount,
		row.BuyerAccount, row.BuyingAssetCode, row.BuyingAssetIssuer, buyingAmount,
		row.Price, row.CreatedAt, row.LedgerRange,
	}, nil
}

// Values returns the ordered column values for batch insertion into effects.
func (row *EffectRow) Values() []interface{} {
	return []interface{}{
		row.LedgerSequence, row.TransactionHash, row.OperationIndex, row.EffectIndex,
		row.OperationID, row.EffectType, row.EffectTypeString, row.AccountID,
		row.Amount, row.AssetCode, row.AssetIssuer, row.AssetType,
		row.DetailsJSON,
		row.TrustlineLimit, row.AuthorizeFlag, row.ClawbackFlag,
		row.SignerAccount, row.SignerWeight, row.OfferID, row.SellerAccount,
		row.CreatedAt, row.LedgerRange,
	}
}

// Values returns the ordered column values for batch insertion into contract_data_current.
func (row *ContractDataCurrentRow) Values() []interface{} {
	return []interface{}{
		row.ContractID, row.KeyHash, row.Durability, row.AssetType, row.AssetCode, row.AssetIssuer,
		row.DataValue, row.LastModifiedLedger, row.LedgerSequence, row.ClosedAt, row.CreatedAt, row.LedgerRange,
	}
}

// Values returns the ordered column values for batch insertion into contract_code_current.
func (row *ContractCodeCurrentRow) Values() []interface{} {
	return []interface{}{
		row.ContractCodeHash, row.ContractCodeExtV,
		row.NDataSegmentBytes, row.NDataSegments, row.NElemSegments, row.NExports,
		row.NFunctions, row.NGlobals, row.NImports, row.NInstructions, row.NTableEntries, row.NTypes,
		row.LastModifiedLedger, row.LedgerSequence, row.ClosedAt, row.CreatedAt, row.LedgerRange,
	}
}

// Values returns the ordered column values for batch insertion into ttl_current.
func (row *TTLCurrentRow) Values() []interface{} {
	return []interface{}{
		row.KeyHash, row.LiveUntilLedgerSeq, row.TTLRemaining, row.Expired,
		row.LastModifiedLedger, row.LedgerSequence, row.ClosedAt, row.CreatedAt, row.LedgerRange,
	}
}

// Values returns the ordered column values for batch insertion into evicted_keys.
func (row *EvictedKeyRow) Values() []interface{} {
	return []interface{}{
		row.ContractID, row.KeyHash, row.LedgerSequence, row.ClosedAt, row.CreatedAt, row.LedgerRange,
	}
}

// Values returns the ordered column values for batch insertion into restored_keys.
func (row *RestoredKeyRow) Values() []interface{} {
	return []interface{}{
		row.ContractID, row.KeyHash, row.LedgerSequence, row.ClosedAt, row.CreatedAt, row.LedgerRange,
	}
}

// Values returns the ordered column values for batch insertion into token_registry.
func (row *TokenRegistryRow) Values() []interface{} {
	return []interface{}{
		row.ContractID, row.TokenName, row.TokenSymbol, row.TokenDecimals,
		row.AssetCode, row.AssetIssuer, row.TokenType,
		row.FirstSeenLedger, row.LastModifiedLedger,
	}
}

// Values returns the ordered column values for batch insertion into config_settings_current.
func (row *ConfigSettingsCurrentRow) Values() []interface{} {
	return []interface{}{
		row.ConfigSettingID,
		row.LedgerMaxInstructions, row.TxMaxInstructions,
		row.FeeRatePerInstructionsIncrement, row.TxMemoryLimit,
		row.LedgerMaxReadLedgerEntries, row.LedgerMaxReadBytes,
		row.LedgerMaxWriteLedgerEntries, row.LedgerMaxWriteBytes,
		row.TxMaxReadLedgerEntries, row.TxMaxReadBytes,
		row.TxMaxWriteLedgerEntries, row.TxMaxWriteBytes,
		row.ContractMaxSizeBytes, row.ConfigSettingXDR,
		row.LastModifiedLedger, row.LedgerSequence, row.ClosedAt, row.CreatedAt, row.LedgerRange,
	}
}
