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

// LedgerRow maps to bronze.ledgers_row_v2
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
	PipelineVersion      string
}

// TransactionRow maps to bronze.transactions_row_v2
type TransactionRow struct {
	LedgerSequence               uint32
	TransactionHash              string
	SourceAccount                string
	SourceAccountMuxed           *string
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
	SignaturesCount              int
	NewAccount                   bool
	TimeboundsMinTime            *string
	TimeboundsMaxTime            *string
	SorobanHostFunctionType      *string
	SorobanContractID            *string
	RentFeeCharged               *int64
	SorobanResourcesInstructions *int64
	SorobanResourcesReadBytes    *int64
	SorobanResourcesWriteBytes   *int64
	// Raw XDR (base64-encoded) for decode endpoints
	TxEnvelope                   *string
	TxResult                     *string
	TxMeta                       *string
	TxFeeMeta                    *string
	PipelineVersion              string
}

// OperationRow maps to bronze.operations_row_v2
type OperationRow struct {
	TransactionHash       string
	TransactionIndex      int
	OperationIndex        int
	LedgerSequence        uint32
	SourceAccount         string
	SourceAccountMuxed    *string
	OpType                int
	TypeString            string
	CreatedAt             time.Time
	TransactionSuccessful bool
	OperationResultCode   *string
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
	SetFlags              *int
	ClearFlags            *int
	HomeDomain            *string
	MasterWeight          *int
	LowThreshold          *int
	MediumThreshold       *int
	HighThreshold         *int
	DataName              *string
	DataValue             *string
	BalanceID             *string
	SponsoredID           *string
	BumpTo                *int64
	SorobanAuthRequired   *bool
	SorobanOperation      *string
	SorobanContractID     *string
	SorobanFunction       *string
	SorobanArgumentsJSON  *string
	ContractCallsJSON     *string
	MaxCallDepth          *int
	PipelineVersion       string
}

// EffectRow maps to bronze.effects_row_v1
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
	PipelineVersion  string
}

// TradeRow maps to bronze.trades_row_v1
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
	PipelineVersion    string
}

// AccountRow maps to bronze.accounts_snapshot_v1
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
	LedgerRange         uint32
	PipelineVersion     string
}

// TrustlineRow maps to bronze.trustlines_snapshot_v1
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
	LedgerRange                     uint32
	PipelineVersion                 string
}

// OfferRow maps to bronze.offers_snapshot_v1
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
	LedgerRange        uint32
	PipelineVersion    string
}

// AccountSignerRow maps to bronze.account_signers_snapshot_v1
type AccountSignerRow struct {
	AccountID      string
	Signer         string
	LedgerSequence uint32
	Weight         uint32
	Sponsor        string
	Deleted        bool
	ClosedAt       time.Time
	LedgerRange    uint32
	PipelineVersion string
}

// Remaining types are stubs — will be populated as we port extractors

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
	LedgerRange     uint32
	PipelineVersion string
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
	LedgerRange     uint32
	PipelineVersion string
}

type ConfigSettingRow struct {
	ConfigSettingID    int
	LedgerSequence     uint32
	LastModifiedLedger int
	Deleted            bool
	ClosedAt           time.Time
	ConfigSettingXDR   string
	LedgerRange        uint32
	PipelineVersion    string
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
	LedgerRange        uint32
	PipelineVersion    string
}

type EvictedKeyRow struct {
	KeyHash        string
	LedgerSequence uint32
	ContractID     string
	KeyType        string
	Durability     string
	ClosedAt       time.Time
	LedgerRange    uint32
	PipelineVersion string
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
	PipelineVersion    string
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
	Topic0Decoded              *string
	Topic1Decoded              *string
	Topic2Decoded              *string
	Topic3Decoded              *string
	OperationIndex             int
	EventIndex                 int
	LedgerRange                uint32
	PipelineVersion            string
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
	TokenName          *string
	TokenSymbol        *string
	TokenDecimals      *int
	LedgerRange        uint32
	PipelineVersion    string
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
	LedgerRange        uint32
	PipelineVersion    string
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
	PipelineVersion    string
}

type ContractCreationRow struct {
	ContractID     string
	CreatorAddress string
	WasmHash       *string
	CreatedLedger  int64
	CreatedAt      time.Time
	LedgerRange    uint32
	PipelineVersion string
}

// LedgerMeta holds pre-extracted metadata for a ledger
type LedgerMeta struct {
	LedgerSequence uint32
	ClosedAt       time.Time
	LedgerRange    uint32
	LCM            xdr.LedgerCloseMeta
}
