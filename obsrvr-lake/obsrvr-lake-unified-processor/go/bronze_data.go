package main

import (
	"time"

	"github.com/stellar/go-stellar-sdk/xdr"
)

// BronzeData holds all extracted bronze data for a single ledger.
// Each slice maps to a DuckLake bronze table.
type BronzeData struct {
	Ledger            *LedgerRow
	Transactions      []TransactionRow
	Operations        []OperationRow
	Effects           []EffectRow
	Trades            []TradeRow
	Accounts          []AccountRow
	Trustlines        []TrustlineRow
	Offers            []OfferRow
	AccountSigners    []AccountSignerRow
	ClaimableBalances []ClaimableBalanceRow
	LiquidityPools    []LiquidityPoolRow
	ConfigSettings    []ConfigSettingRow
	TTLEntries        []TTLRow
	EvictedKeys       []EvictedKeyRow
	RestoredKeys      []RestoredKeyRow
	ContractEvents    []ContractEventRow
	ContractData      []ContractDataRow
	ContractCode      []ContractCodeRow
	NativeBalances    []NativeBalanceRow
	ContractCreations []ContractCreationRow
}

// LedgerRow maps to bronze.ledgers_row_v2 (V3 schema — 29 columns)
type LedgerRow struct {
	Sequence             uint32
	LedgerHash           string
	PreviousLedgerHash   string
	ClosedAt             time.Time
	ProtocolVersion      int
	TotalCoins           int64
	FeePool              int64
	BaseFee              int
	BaseReserve          int
	MaxTxSetSize         int
	SuccessfulTxCount    int
	FailedTxCount        int
	IngestionTimestamp    time.Time
	LedgerRange          uint32
	TransactionCount     int
	OperationCount       int
	TxSetOperationCount  int
	// V3 fields
	SorobanFeeWrite1KB   *int64
	NodeID               *string
	Signature            *string
	LedgerHeader         *string
	BucketListSize       *int64
	LiveSorobanStateSize *int64
	EvictedKeysCount     *int
	SorobanOpCount       *int
	TotalFeeCharged      *int64
	ContractEventsCount  *int
	EraID                string
	VersionLabel         string
}

// TransactionRow maps to bronze.transactions_row_v2 (V3 schema — 49 columns)
type TransactionRow struct {
	LedgerSequence               uint32
	TransactionHash              string
	SourceAccount                string
	FeeCharged                   int64
	MaxFee                       int64
	Successful                   bool
	TransactionResultCode        string
	OperationCount               int
	MemoType                     *string
	Memo                         *string
	CreatedAt                    time.Time
	AccountSequence              int64
	LedgerRange                  uint32
	SourceAccountMuxed           *string
	FeeAccountMuxed              *string
	InnerTransactionHash         *string
	FeeBumpFee                   *int64
	MaxFeeBid                    *int64
	InnerSourceAccount           *string
	TimeboundsMinTime            *int64
	TimeboundsMaxTime            *int64
	LedgerboundsMin              *int64
	LedgerboundsMax              *int64
	MinSequenceNumber            *int64
	MinSequenceAge               *int64
	SorobanResourcesInstructions *int64
	SorobanResourcesReadBytes    *int64
	SorobanResourcesWriteBytes   *int64
	SorobanDataSizeBytes         *int
	SorobanDataResources         *string
	SorobanFeeBase               *int64
	SorobanFeeResources          *int64
	SorobanFeeRefund             *int64
	SorobanFeeCharged            *int64
	SorobanFeeWasted             *int64
	SorobanHostFunctionType      *string
	SorobanContractID            *string
	SorobanContractEventsCount   *int
	SignaturesCount              int
	NewAccount                   bool
	RentFeeCharged               *int64
	// Raw XDR (base64-encoded)
	TxEnvelope                   *string
	TxResult                     *string
	TxMeta                       *string
	TxFeeMeta                    *string
	TxSigners                    *string
	ExtraSigners                 *string
	EraID                        string
	VersionLabel                 string
}

// OperationRow maps to bronze.operations_row_v2 (V3 schema — 64 columns)
type OperationRow struct {
	TransactionHash       string
	OperationIndex        int
	LedgerSequence        uint32
	SourceAccount         string
	SourceAccountMuxed    *string
	OpType                int
	TypeString            string
	CreatedAt             time.Time
	TransactionSuccessful bool
	OperationResultCode   *string
	OperationTraceCode    *string
	LedgerRange           uint32
	Amount                *int64
	Asset                 *string
	AssetType             *string
	AssetCode             *string
	AssetIssuer           *string
	Destination           *string
	SourceAsset           *string
	SourceAssetType       *string
	SourceAssetCode       *string
	SourceAssetIssuer     *string
	SourceAmount          *int64
	DestinationMin        *int64
	StartingBalance       *int64
	TrustlineLimit        *int64
	Trustor               *string
	Authorize             *bool
	AuthorizeToMaintainLiabilities *bool
	TrustLineFlags        *int
	BalanceID             *string
	ClaimantsCount        *int
	SponsoredID           *string
	OfferID               *int64
	Price                 *string
	PriceR                *string
	BuyingAsset           *string
	BuyingAssetType       *string
	BuyingAssetCode       *string
	BuyingAssetIssuer     *string
	SellingAsset          *string
	SellingAssetType      *string
	SellingAssetCode      *string
	SellingAssetIssuer    *string
	SorobanOperation      *string
	SorobanFunction       *string
	SorobanContractID     *string
	SorobanAuthRequired   *bool
	BumpTo                *int64
	SetFlags              *int
	ClearFlags            *int
	HomeDomain            *string
	MasterWeight          *int
	LowThreshold          *int
	MediumThreshold       *int
	HighThreshold         *int
	DataName              *string
	DataValue             *string
	EraID                 string
	VersionLabel          string
	// Migration fields (contract invocation tracking)
	TransactionIndex      int
	SorobanArgumentsJSON  *string
	ContractCallsJSON     *string
	ContractsInvolved     *string
	MaxCallDepth          *int
}

// EffectRow maps to bronze.effects_row_v1 (V3 schema — 22 columns)
type EffectRow struct {
	LedgerSequence   uint32
	TransactionHash  string
	OperationIndex   int
	EffectIndex      int
	EffectType       int
	EffectTypeString string
	AccountID        *string
	Amount           *string
	AssetCode        *string
	AssetIssuer      *string
	AssetType        *string
	TrustlineLimit   *string
	AuthorizeFlag    *bool
	ClawbackFlag     *bool
	SignerAccount    *string
	SignerWeight     *int
	OfferID          *int64
	SellerAccount    *string
	CreatedAt        time.Time
	LedgerRange      uint32
	EraID            string
	VersionLabel     string
}

// TradeRow maps to bronze.trades_row_v1 (V3 schema — 19 columns)
type TradeRow struct {
	LedgerSequence     uint32
	TransactionHash    string
	OperationIndex     int
	TradeIndex         int
	TradeType          string
	TradeTimestamp     time.Time
	SellerAccount      string
	SellingAssetCode   *string
	SellingAssetIssuer *string
	SellingAmount      string
	BuyerAccount       string
	BuyingAssetCode    *string
	BuyingAssetIssuer  *string
	BuyingAmount       string
	Price              string
	CreatedAt          time.Time
	LedgerRange        uint32
	EraID              string
	VersionLabel       string
}

// AccountRow maps to bronze.accounts_snapshot_v1 (V3 schema — 25 columns)
type AccountRow struct {
	AccountID           string
	LedgerSequence      uint32
	ClosedAt            time.Time
	Balance             string
	SequenceNumber      uint64
	NumSubentries       uint32
	NumSponsoring       uint32
	NumSponsored        uint32
	HomeDomain          *string
	MasterWeight        uint32
	LowThreshold        uint32
	MedThreshold        uint32
	HighThreshold       uint32
	Flags               uint32
	AuthRequired        bool
	AuthRevocable       bool
	AuthImmutable       bool
	AuthClawbackEnabled bool
	Signers             *string
	SponsorAccount      *string
	CreatedAt           time.Time
	UpdatedAt           time.Time
	LedgerRange         uint32
	EraID               string
	VersionLabel        string
}

// TrustlineRow maps to bronze.trustlines_snapshot_v1 (V3 schema — 16 columns)
type TrustlineRow struct {
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
	LedgerSequence                  uint32
	CreatedAt                       time.Time
	LedgerRange                     uint32
	EraID                           string
	VersionLabel                    string
}

// OfferRow maps to bronze.offers_snapshot_v1 (V3 schema — 17 columns)
type OfferRow struct {
	OfferID            int64
	SellerAccount      string
	LedgerSequence     uint32
	ClosedAt           time.Time
	SellingAssetType   string
	SellingAssetCode   *string
	SellingAssetIssuer *string
	BuyingAssetType    string
	BuyingAssetCode    *string
	BuyingAssetIssuer  *string
	Amount             string
	Price              string
	Flags              uint32
	CreatedAt          time.Time
	LedgerRange        uint32
	EraID              string
	VersionLabel       string
}

// AccountSignerRow maps to bronze.account_signers_snapshot_v1 (V3 schema — 11 columns)
type AccountSignerRow struct {
	AccountID       string
	Signer          string
	LedgerSequence  uint32
	Weight          uint32
	Sponsor         string
	Deleted         bool
	ClosedAt        time.Time
	LedgerRange     uint32
	CreatedAt       time.Time
	EraID           string
	VersionLabel    string
}

type ClaimableBalanceRow struct {
	BalanceID       string
	Sponsor         string
	LedgerSequence  uint32
	ClosedAt        time.Time
	AssetType       string
	AssetCode       *string
	AssetIssuer     *string
	Amount          int64
	ClaimantsCount  int
	Flags           int
	CreatedAt       time.Time
	LedgerRange     uint32
	EraID           string
	VersionLabel    string
}

type LiquidityPoolRow struct {
	LiquidityPoolID string
	LedgerSequence  uint32
	ClosedAt        time.Time
	PoolType        string
	Fee             int
	TrustlineCount  int
	TotalPoolShares int64
	AssetAType      string
	AssetACode      *string
	AssetAIssuer    *string
	AssetAAmount    int64
	AssetBType      string
	AssetBCode      *string
	AssetBIssuer    *string
	AssetBAmount    int64
	CreatedAt       time.Time
	LedgerRange     uint32
	EraID           string
	VersionLabel    string
}

// ConfigSettingRow maps to bronze.config_settings_snapshot_v1 (V3 schema — 23 columns)
type ConfigSettingRow struct {
	ConfigSettingID                 int
	LedgerSequence                  uint32
	LastModifiedLedger              int
	Deleted                         bool
	ClosedAt                        time.Time
	// Soroban resource limits (V3 detail fields)
	LedgerMaxInstructions           *int64
	TxMaxInstructions               *int64
	FeeRatePerInstructionsIncrement *int64
	TxMemoryLimit                   *int64
	LedgerMaxReadLedgerEntries      *int64
	LedgerMaxReadBytes              *int64
	LedgerMaxWriteLedgerEntries     *int64
	LedgerMaxWriteBytes             *int64
	TxMaxReadLedgerEntries          *int64
	TxMaxReadBytes                  *int64
	TxMaxWriteLedgerEntries         *int64
	TxMaxWriteBytes                 *int64
	ContractMaxSizeBytes            *int64
	ConfigSettingXDR                string
	CreatedAt                       time.Time
	LedgerRange                     uint32
	EraID                           string
	VersionLabel                    string
}

type TTLRow struct {
	KeyHash            string
	LedgerSequence     uint32
	LiveUntilLedgerSeq int64
	TTLRemaining       int64
	Expired            bool
	LastModifiedLedger int
	Deleted            bool
	ClosedAt           time.Time
	CreatedAt          time.Time
	LedgerRange        uint32
	EraID              string
	VersionLabel       string
}

type EvictedKeyRow struct {
	KeyHash         string
	LedgerSequence  uint32
	ContractID      string
	KeyType         string
	Durability      string
	ClosedAt        time.Time
	LedgerRange     uint32
	CreatedAt       time.Time
	EraID           string
	VersionLabel    string
}

type RestoredKeyRow struct {
	KeyHash            string
	LedgerSequence     uint32
	ContractID         string
	KeyType            string
	Durability         string
	RestoredFromLedger int64
	ClosedAt           time.Time
	LedgerRange        uint32
	CreatedAt          time.Time
	EraID              string
	VersionLabel       string
}

type ContractEventRow struct {
	EventID                    string
	ContractID                 string
	LedgerSequence             uint32
	TransactionHash            string
	ClosedAt                   time.Time
	EventType                  string
	InSuccessfulContractCall   bool
	TopicsJSON                 string
	TopicsDecoded              string
	DataXDR                    string
	DataDecoded                string
	TopicCount                 int
	OperationIndex             int
	EventIndex                 int
	Topic0Decoded              *string
	Topic1Decoded              *string
	Topic2Decoded              *string
	Topic3Decoded              *string
	CreatedAt                  time.Time
	LedgerRange                uint32
	EraID                      string
	VersionLabel               string
}

type ContractDataRow struct {
	ContractID         string
	LedgerSequence     uint32
	LedgerKeyHash      string
	ContractKeyType    string
	ContractDurability string
	AssetCode          *string
	AssetIssuer        *string
	AssetType          *string
	BalanceHolder      *string
	Balance            *string
	LastModifiedLedger int
	LedgerEntryChange  int
	Deleted            bool
	ClosedAt           time.Time
	ContractDataXDR    string
	CreatedAt          time.Time
	LedgerRange        uint32
	TokenName          *string
	TokenSymbol        *string
	TokenDecimals      *int
	EraID              string
	VersionLabel       string
}

type ContractCodeRow struct {
	ContractCodeHash   string
	LedgerKeyHash      string
	ContractCodeExtV   int
	LastModifiedLedger int
	LedgerEntryChange  int
	Deleted            bool
	ClosedAt           time.Time
	LedgerSequence     uint32
	NInstructions      int64
	NFunctions         int64
	NGlobals           int64
	NTableEntries      int64
	NTypes             int64
	NDataSegments      int64
	NElemSegments      int64
	NImports           int64
	NExports           int64
	NDataSegmentBytes  int64
	CreatedAt          time.Time
	LedgerRange        uint32
	EraID              string
	VersionLabel       string
}

type NativeBalanceRow struct {
	AccountID          string
	Balance            int64
	BuyingLiabilities  int64
	SellingLiabilities int64
	NumSubentries      int
	NumSponsoring      int
	NumSponsored       int
	SequenceNumber     int64
	LastModifiedLedger int64
	LedgerSequence     uint32
	LedgerRange        uint32
	EraID              string
	VersionLabel       string
}

type ContractCreationRow struct {
	ContractID     string
	CreatorAddress string
	WasmHash       *string
	CreatedLedger  int64
	CreatedAt      time.Time
	LedgerRange    uint32
	EraID          string
	VersionLabel   string
}

// LedgerMeta holds pre-extracted metadata for a ledger
type LedgerMeta struct {
	LedgerSequence uint32
	ClosedAt       time.Time
	LedgerRange    uint32
	LCM            xdr.LedgerCloseMeta
}
