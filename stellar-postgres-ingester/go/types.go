package main

import "time"

// TransactionData represents a single transaction (simplified - core fields only)
type TransactionData struct {
	LedgerSequence        uint32
	TransactionHash       string
	SourceAccount         string
	FeeCharged            int64
	MaxFee                int64
	Successful            bool
	TransactionResultCode string
	OperationCount        int
	MemoType              *string
	Memo                  *string
	CreatedAt             time.Time
	AccountSequence       int64
	LedgerRange           uint32
	SignaturesCount       int
	NewAccount            bool
}

// OperationData represents a single operation (simplified - core fields only)
type OperationData struct {
	TransactionHash      string
	OperationIndex       int
	LedgerSequence       uint32
	SourceAccount        string
	OpType               int
	TypeString           string
	CreatedAt            time.Time
	TransactionSuccessful bool
	OperationResultCode  *string
	LedgerRange          uint32
	// Core operation fields
	Amount               *int64
	Asset                *string
	Destination          *string
}

// EffectData represents a single effect (state changes from operations)
type EffectData struct {
	// Identity
	LedgerSequence   uint32
	TransactionHash  string
	OperationIndex   int
	EffectIndex      int

	// Effect type
	EffectType       int
	EffectTypeString string

	// Account affected
	AccountID *string

	// Amount changes
	Amount      *string
	AssetCode   *string
	AssetIssuer *string
	AssetType   *string

	// Trustline effects
	TrustlineLimit *string
	AuthorizeFlag  *bool
	ClawbackFlag   *bool

	// Signer effects
	SignerAccount *string
	SignerWeight  *int

	// Offer effects
	OfferID       *int64
	SellerAccount *string

	// Metadata
	CreatedAt   time.Time
	LedgerRange uint32
}

// TradeData represents a single trade execution (DEX trades)
type TradeData struct {
	// Identity
	LedgerSequence  uint32
	TransactionHash string
	OperationIndex  int
	TradeIndex      int

	// Trade details
	TradeType      string
	TradeTimestamp time.Time

	// Seller side
	SellerAccount      string
	SellingAssetCode   *string
	SellingAssetIssuer *string
	SellingAmount      string

	// Buyer side
	BuyerAccount      string
	BuyingAssetCode   *string
	BuyingAssetIssuer *string
	BuyingAmount      string

	// Price
	Price string

	// Metadata
	CreatedAt   time.Time
	LedgerRange uint32
}

// AccountData represents account snapshot state (accounts_snapshot_v1)
// 23 fields total - captures complete account state including flags, thresholds, signers
type AccountData struct {
	// Identity (3 fields)
	AccountID      string
	LedgerSequence uint32
	ClosedAt       time.Time

	// Balance (1 field)
	Balance string

	// Account settings (5 fields)
	SequenceNumber uint64
	NumSubentries  uint32
	NumSponsoring  uint32
	NumSponsored   uint32
	HomeDomain     *string

	// Thresholds (4 fields)
	MasterWeight  uint32
	LowThreshold  uint32
	MedThreshold  uint32
	HighThreshold uint32

	// Flags (5 fields)
	Flags               uint32
	AuthRequired        bool
	AuthRevocable       bool
	AuthImmutable       bool
	AuthClawbackEnabled bool

	// Signers (1 field) - JSON array
	Signers *string

	// Sponsorship (1 field)
	SponsorAccount *string

	// Metadata (3 fields)
	CreatedAt   time.Time
	UpdatedAt   time.Time
	LedgerRange uint32
}

// OfferData represents DEX offer snapshot state (offers_snapshot_v1)
// 15 fields total - captures orderbook state with selling/buying assets, amount, and price
type OfferData struct {
	// Identity (4 fields)
	OfferID        int64
	SellerAccount  string
	LedgerSequence uint32
	ClosedAt       time.Time

	// Selling asset (3 fields)
	SellingAssetType   string
	SellingAssetCode   *string
	SellingAssetIssuer *string

	// Buying asset (3 fields)
	BuyingAssetType   string
	BuyingAssetCode   *string
	BuyingAssetIssuer *string

	// Offer details (2 fields)
	Amount string
	Price  string

	// Flags (1 field)
	Flags uint32

	// Metadata (2 fields)
	CreatedAt   time.Time
	LedgerRange uint32
}

// TrustlineData represents trustline snapshot state (trustlines_snapshot_v1)
// 14 fields total - captures non-native asset holdings and trust relationships
type TrustlineData struct {
	// Identity (4 fields)
	AccountID   string
	AssetCode   string
	AssetIssuer string
	AssetType   string

	// Trust & Balance (4 fields)
	Balance            string
	TrustLimit         string
	BuyingLiabilities  string
	SellingLiabilities string

	// Authorization (3 fields)
	Authorized                      bool
	AuthorizedToMaintainLiabilities bool
	ClawbackEnabled                 bool

	// Metadata (3 fields)
	LedgerSequence uint32
	CreatedAt      time.Time
	LedgerRange    uint32
}

// AccountSignerData represents account signer snapshot state (account_signers_snapshot_v1)
// 9 fields total - tracks multi-sig configurations and signer changes
type AccountSignerData struct {
	// Identity (3 fields)
	AccountID      string
	Signer         string
	LedgerSequence uint32

	// Signer details (2 fields)
	Weight  uint32
	Sponsor string // Nullable - sponsor account ID

	// Status (1 field)
	Deleted bool

	// Metadata (3 fields)
	ClosedAt    time.Time
	LedgerRange uint32
	CreatedAt   time.Time
}

// ClaimableBalanceData represents claimable balance snapshot state (claimable_balances_snapshot_v1)
// 12 fields total - Protocol 14+ feature for deferred payments
type ClaimableBalanceData struct {
	// Identity (4 fields)
	BalanceID      string
	Sponsor        string // Nullable
	LedgerSequence uint32
	ClosedAt       time.Time

	// Asset & Amount (4 fields)
	AssetType   string
	AssetCode   *string // Nullable
	AssetIssuer *string // Nullable
	Amount      int64

	// Claimants (1 field)
	ClaimantsCount int32

	// Flags (1 field)
	Flags uint32

	// Metadata (2 fields)
	CreatedAt   time.Time
	LedgerRange uint32
}

// LiquidityPoolData represents liquidity pool snapshot state (liquidity_pools_snapshot_v1)
// 17 fields total - Protocol 18+ AMM pools
type LiquidityPoolData struct {
	// Identity (3 fields)
	LiquidityPoolID string
	LedgerSequence  uint32
	ClosedAt        time.Time

	// Pool Type (1 field)
	PoolType string // "constant_product"

	// Fee (1 field)
	Fee int32 // Basis points (30 = 0.3%)

	// Pool Shares (2 fields)
	TrustlineCount  int32
	TotalPoolShares int64

	// Asset A (4 fields)
	AssetAType   string
	AssetACode   *string // Nullable
	AssetAIssuer *string // Nullable
	AssetAAmount int64

	// Asset B (4 fields)
	AssetBType   string
	AssetBCode   *string // Nullable
	AssetBIssuer *string // Nullable
	AssetBAmount int64

	// Metadata (2 fields)
	CreatedAt   time.Time
	LedgerRange uint32
}

// ConfigSettingData represents network configuration settings snapshot (config_settings_snapshot_v1)
// 19 fields total - Protocol 20+ Soroban configuration parameters
type ConfigSettingData struct {
	// Identity (2 fields)
	ConfigSettingID int32
	LedgerSequence  uint32

	// Ledger metadata (3 fields)
	LastModifiedLedger int32
	Deleted            bool
	ClosedAt           time.Time

	// Soroban compute settings (4 fields, nullable)
	LedgerMaxInstructions           *int64
	TxMaxInstructions               *int64
	FeeRatePerInstructionsIncrement *int64
	TxMemoryLimit                   *uint32

	// Soroban ledger cost settings (8 fields, nullable)
	LedgerMaxReadLedgerEntries  *uint32
	LedgerMaxReadBytes          *uint32
	LedgerMaxWriteLedgerEntries *uint32
	LedgerMaxWriteBytes         *uint32
	TxMaxReadLedgerEntries      *uint32
	TxMaxReadBytes              *uint32
	TxMaxWriteLedgerEntries     *uint32
	TxMaxWriteBytes             *uint32

	// Contract size limit (1 field, nullable)
	ContractMaxSizeBytes *uint32

	// Raw XDR (1 field)
	ConfigSettingXDR string

	// Metadata (2 fields)
	CreatedAt   time.Time
	LedgerRange uint32
}

// TTLData represents time-to-live entries snapshot (ttl_snapshot_v1)
// 10 fields total - Protocol 20+ Soroban storage expiration tracking
type TTLData struct {
	// Identity (2 fields)
	KeyHash        string
	LedgerSequence uint32

	// TTL tracking (3 fields)
	LiveUntilLedgerSeq uint32
	TTLRemaining       int64
	Expired            bool

	// Ledger metadata (3 fields)
	LastModifiedLedger int32
	Deleted            bool
	ClosedAt           time.Time

	// Metadata (2 fields)
	CreatedAt   time.Time
	LedgerRange uint32
}

// EvictedKeyData represents evicted storage keys state (evicted_keys_state_v1)
// 8 fields total - Protocol 20+ Soroban archival tracking (V2-only)
type EvictedKeyData struct {
	// Identity (2 fields)
	KeyHash        string
	LedgerSequence uint32

	// Eviction details (3 fields)
	ContractID string
	KeyType    string
	Durability string

	// Metadata (3 fields)
	ClosedAt    time.Time
	LedgerRange uint32
	CreatedAt   time.Time
}

// ContractEventData represents Soroban contract events stream (contract_events_stream_v1)
// 16 fields total - Protocol 20+ Soroban smart contract events
type ContractEventData struct {
	// Identity (5 fields)
	EventID         string
	ContractID      *string
	LedgerSequence  uint32
	TransactionHash string
	ClosedAt        time.Time

	// Event Type (2 fields)
	EventType                string
	InSuccessfulContractCall bool

	// Event Data (5 fields - Hubble compatible with decoded versions)
	TopicsJSON    string
	TopicsDecoded string
	DataXDR       string
	DataDecoded   string
	TopicCount    int32

	// Context (2 fields)
	OperationIndex uint32
	EventIndex     uint32

	// Metadata (2 fields)
	CreatedAt   time.Time
	LedgerRange uint32
}

// ContractDataData represents Soroban contract data snapshot (contract_data_snapshot_v1)
// 17 fields total - Protocol 20+ Soroban smart contract storage with SAC detection
type ContractDataData struct {
	// Identity (3 fields)
	ContractId     string
	LedgerSequence uint32
	LedgerKeyHash  string

	// Contract metadata (2 fields)
	ContractKeyType    string
	ContractDurability string

	// Asset information (3 fields, nullable - SAC detection)
	AssetCode   *string
	AssetIssuer *string
	AssetType   *string

	// Balance information (2 fields, nullable - SAC detection)
	BalanceHolder *string
	Balance       *string

	// Ledger metadata (4 fields)
	LastModifiedLedger int32
	LedgerEntryChange  int32
	Deleted            bool
	ClosedAt           time.Time

	// XDR data (1 field)
	ContractDataXDR string

	// Metadata (2 fields)
	CreatedAt   time.Time
	LedgerRange uint32
}

// ContractCodeData represents Soroban contract code snapshot (contract_code_snapshot_v1)
// 18 fields total - Protocol 20+ Soroban WASM contract deployments
type ContractCodeData struct {
	// Identity (2 fields)
	ContractCodeHash string
	LedgerKeyHash    string

	// Extension (1 field)
	ContractCodeExtV int32

	// Ledger metadata (4 fields)
	LastModifiedLedger int32
	LedgerEntryChange  int32
	Deleted            bool
	ClosedAt           time.Time

	// Ledger tracking (1 field)
	LedgerSequence uint32

	// WASM metadata (10 fields, all nullable)
	NInstructions     *int64
	NFunctions        *int64
	NGlobals          *int64
	NTableEntries     *int64
	NTypes            *int64
	NDataSegments     *int64
	NElemSegments     *int64
	NImports          *int64
	NExports          *int64
	NDataSegmentBytes *int64

	// Metadata (2 fields)
	CreatedAt   time.Time
	LedgerRange uint32
}

// NativeBalanceData represents XLM-only balances snapshot (native_balances_snapshot_v1)
// 11 fields total - Native (XLM) balance tracking
type NativeBalanceData struct {
	// Identity (1 field)
	AccountID string

	// Balance details (7 fields)
	Balance            int64
	BuyingLiabilities  int64
	SellingLiabilities int64
	NumSubentries      int32
	NumSponsoring      int32
	NumSponsored       int32
	SequenceNumber     *int64 // Nullable

	// Ledger tracking (2 fields)
	LastModifiedLedger int64
	LedgerSequence     int64

	// Partition key (1 field)
	LedgerRange int64
}

// RestoredKeyData represents restored storage keys state (restored_keys_state_v1)
// 10 fields total - Protocol 20+ Soroban archival restoration tracking
type RestoredKeyData struct {
	// Identity (2 fields)
	KeyHash        string
	LedgerSequence uint32

	// Restoration details (4 fields)
	ContractID         string
	KeyType            string
	Durability         string
	RestoredFromLedger uint32

	// Metadata (3 fields)
	ClosedAt    time.Time
	LedgerRange uint32
	CreatedAt   time.Time
}

// Note: Cycle 2 MVP complete (5 of 19 Hubble tables): ledgers, transactions, operations, effects, trades
// Cycle 2 Extension COMPLETE (adding 14 more Hubble tables):
// Phase 1 (Days 1-3): accounts, offers, trustlines, account_signers - COMPLETE
// Phase 2 (Days 4-6): claimable_balances, liquidity_pools, config_settings, ttl - COMPLETE
// Phase 3 (Day 7): evicted_keys - COMPLETE
// Phase 4 (Days 8-10): contract_events, contract_data, contract_code - COMPLETE
// Phase 5 (Day 11): native_balances, restored_keys - IN PROGRESS
