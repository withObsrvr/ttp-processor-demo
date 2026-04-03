package main

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/parquet-go/parquet-go"
)

// ParquetTableWriter manages writing rows to a single Parquet file per ledger_range partition.
type ParquetTableWriter[T any] struct {
	mu        sync.Mutex
	outputDir string
	tableName string
	workerID  int
	writers   map[uint32]*parquet.GenericWriter[T] // keyed by ledger_range
	files     map[uint32]*os.File
}

func NewParquetTableWriter[T any](outputDir, tableName string, workerID int) *ParquetTableWriter[T] {
	return &ParquetTableWriter[T]{
		outputDir: outputDir,
		tableName: tableName,
		workerID:  workerID,
		writers:   make(map[uint32]*parquet.GenericWriter[T]),
		files:     make(map[uint32]*os.File),
	}
}

func (w *ParquetTableWriter[T]) getWriter(ledgerRange uint32) (*parquet.GenericWriter[T], error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if writer, ok := w.writers[ledgerRange]; ok {
		return writer, nil
	}

	dir := filepath.Join(w.outputDir, "bronze", w.tableName, fmt.Sprintf("range_%07d", ledgerRange))
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("create dir %s: %w", dir, err)
	}

	// Use a unique filename per run to avoid truncating existing files on resume.
	// The checkpoint tracks which ledgers have been processed, so duplicate files
	// for the same range are acceptable (DuckLake deduplicates on INSERT).
	path := filepath.Join(dir, fmt.Sprintf("shard_%04d_%d.parquet", w.workerID, time.Now().UnixNano()))
	f, err := os.Create(path)
	if err != nil {
		return nil, fmt.Errorf("create file %s: %w", path, err)
	}

	writer := parquet.NewGenericWriter[T](f,
		parquet.Compression(&parquet.Snappy),
	)

	w.writers[ledgerRange] = writer
	w.files[ledgerRange] = f
	return writer, nil
}

func (w *ParquetTableWriter[T]) Write(rows []T, getLedgerRange func(T) uint32) error {
	if len(rows) == 0 {
		return nil
	}

	// Group rows by ledger_range
	grouped := make(map[uint32][]T)
	for _, row := range rows {
		lr := getLedgerRange(row)
		grouped[lr] = append(grouped[lr], row)
	}

	for lr, batch := range grouped {
		writer, err := w.getWriter(lr)
		if err != nil {
			return err
		}
		if _, err := writer.Write(batch); err != nil {
			return fmt.Errorf("write %s range %d: %w", w.tableName, lr, err)
		}
	}
	return nil
}

func (w *ParquetTableWriter[T]) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	var firstErr error
	for lr, writer := range w.writers {
		if err := writer.Close(); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("close writer %s range %d: %w", w.tableName, lr, err)
		}
		if f, ok := w.files[lr]; ok {
			f.Close()
		}
	}
	return firstErr
}

// --- Parquet row types with struct tags ---
// These mirror the bronze data types but with parquet tags for schema definition.

type ParquetTransaction struct {
	LedgerSequence               uint32  `parquet:"ledger_sequence"`
	TransactionHash              string  `parquet:"transaction_hash"`
	SourceAccount                string  `parquet:"source_account"`
	SourceAccountMuxed           *string `parquet:"source_account_muxed,optional"`
	FeeCharged                   int64   `parquet:"fee_charged"`
	MaxFee                       int64   `parquet:"max_fee"`
	Successful                   bool    `parquet:"successful"`
	TransactionResultCode        string  `parquet:"transaction_result_code"`
	OperationCount               int32   `parquet:"operation_count"`
	MemoType                     *string `parquet:"memo_type,optional"`
	Memo                         *string `parquet:"memo,optional"`
	CreatedAt                    int64   `parquet:"created_at,timestamp(microsecond)"`
	AccountSequence              int64   `parquet:"account_sequence"`
	LedgerRange                  uint32  `parquet:"ledger_range"`
	SignaturesCount              int32   `parquet:"signatures_count"`
	NewAccount                   bool    `parquet:"new_account"`
	TimeboundsMinTime            *string `parquet:"timebounds_min_time,optional"`
	TimeboundsMaxTime            *string `parquet:"timebounds_max_time,optional"`
	SorobanHostFunctionType      *string `parquet:"soroban_host_function_type,optional"`
	SorobanContractID            *string `parquet:"soroban_contract_id,optional"`
	RentFeeCharged               *int64  `parquet:"rent_fee_charged,optional"`
	SorobanResourcesInstructions *int64  `parquet:"soroban_resources_instructions,optional"`
	SorobanResourcesReadBytes    *int64  `parquet:"soroban_resources_read_bytes,optional"`
	SorobanResourcesWriteBytes   *int64  `parquet:"soroban_resources_write_bytes,optional"`
	PipelineVersion              string  `parquet:"pipeline_version"`
}

type ParquetOperation struct {
	TransactionHash       string  `parquet:"transaction_hash"`
	TransactionIndex      int32   `parquet:"transaction_index"`
	OperationIndex        int32   `parquet:"operation_index"`
	LedgerSequence        uint32  `parquet:"ledger_sequence"`
	SourceAccount         string  `parquet:"source_account"`
	SourceAccountMuxed    *string `parquet:"source_account_muxed,optional"`
	OpType                int32   `parquet:"op_type"`
	TypeString            string  `parquet:"type_string"`
	CreatedAt             int64   `parquet:"created_at,timestamp(microsecond)"`
	TransactionSuccessful bool    `parquet:"transaction_successful"`
	OperationResultCode   *string `parquet:"operation_result_code,optional"`
	LedgerRange           uint32  `parquet:"ledger_range"`
	Amount                *int64  `parquet:"amount,optional"`
	Asset                 *string `parquet:"asset,optional"`
	AssetType             *string `parquet:"asset_type,optional"`
	AssetCode             *string `parquet:"asset_code,optional"`
	AssetIssuer           *string `parquet:"asset_issuer,optional"`
	Destination           *string `parquet:"destination,optional"`
	SourceAsset           *string `parquet:"source_asset,optional"`
	SourceAssetType       *string `parquet:"source_asset_type,optional"`
	SourceAssetCode       *string `parquet:"source_asset_code,optional"`
	SourceAssetIssuer     *string `parquet:"source_asset_issuer,optional"`
	SourceAmount          *int64  `parquet:"source_amount,optional"`
	DestinationMin        *int64  `parquet:"destination_min,optional"`
	StartingBalance       *int64  `parquet:"starting_balance,optional"`
	TrustlineLimit        *int64  `parquet:"trustline_limit,optional"`
	OfferID               *int64  `parquet:"offer_id,optional"`
	Price                 *string `parquet:"price,optional"`
	PriceR                *string `parquet:"price_r,optional"`
	BuyingAsset           *string `parquet:"buying_asset,optional"`
	BuyingAssetType       *string `parquet:"buying_asset_type,optional"`
	BuyingAssetCode       *string `parquet:"buying_asset_code,optional"`
	BuyingAssetIssuer     *string `parquet:"buying_asset_issuer,optional"`
	SellingAsset          *string `parquet:"selling_asset,optional"`
	SellingAssetType      *string `parquet:"selling_asset_type,optional"`
	SellingAssetCode      *string `parquet:"selling_asset_code,optional"`
	SellingAssetIssuer    *string `parquet:"selling_asset_issuer,optional"`
	SetFlags              *int32  `parquet:"set_flags,optional"`
	ClearFlags            *int32  `parquet:"clear_flags,optional"`
	HomeDomain            *string `parquet:"home_domain,optional"`
	MasterWeight          *int32  `parquet:"master_weight,optional"`
	LowThreshold          *int32  `parquet:"low_threshold,optional"`
	MediumThreshold       *int32  `parquet:"medium_threshold,optional"`
	HighThreshold         *int32  `parquet:"high_threshold,optional"`
	DataName              *string `parquet:"data_name,optional"`
	DataValue             *string `parquet:"data_value,optional"`
	BalanceID             *string `parquet:"balance_id,optional"`
	SponsoredID           *string `parquet:"sponsored_id,optional"`
	BumpTo                *int64  `parquet:"bump_to,optional"`
	SorobanAuthRequired   *bool   `parquet:"soroban_auth_required,optional"`
	SorobanOperation      *string `parquet:"soroban_operation,optional"`
	SorobanContractID     *string `parquet:"soroban_contract_id,optional"`
	SorobanFunction       *string `parquet:"soroban_function,optional"`
	SorobanArgumentsJSON  *string `parquet:"soroban_arguments_json,optional"`
	ContractCallsJSON     *string `parquet:"contract_calls_json,optional"`
	MaxCallDepth          *int32  `parquet:"max_call_depth,optional"`
	PipelineVersion       string  `parquet:"pipeline_version"`
}

type ParquetEffect struct {
	LedgerSequence   uint32  `parquet:"ledger_sequence"`
	TransactionHash  string  `parquet:"transaction_hash"`
	OperationIndex   int32   `parquet:"operation_index"`
	EffectIndex      int32   `parquet:"effect_index"`
	EffectType       int32   `parquet:"effect_type"`
	EffectTypeString string  `parquet:"effect_type_string"`
	AccountID        *string `parquet:"account_id,optional"`
	Amount           *string `parquet:"amount,optional"`
	AssetCode        *string `parquet:"asset_code,optional"`
	AssetIssuer      *string `parquet:"asset_issuer,optional"`
	AssetType        *string `parquet:"asset_type,optional"`
	TrustlineLimit   *string `parquet:"trustline_limit,optional"`
	AuthorizeFlag    *bool   `parquet:"authorize_flag,optional"`
	ClawbackFlag     *bool   `parquet:"clawback_flag,optional"`
	SignerAccount    *string `parquet:"signer_account,optional"`
	SignerWeight     *int32  `parquet:"signer_weight,optional"`
	OfferID          *int64  `parquet:"offer_id,optional"`
	SellerAccount    *string `parquet:"seller_account,optional"`
	CreatedAt        int64   `parquet:"created_at,timestamp(microsecond)"`
	LedgerRange      uint32  `parquet:"ledger_range"`
	PipelineVersion  string  `parquet:"pipeline_version"`
}

type ParquetTrade struct {
	LedgerSequence     uint32  `parquet:"ledger_sequence"`
	TransactionHash    string  `parquet:"transaction_hash"`
	OperationIndex     int32   `parquet:"operation_index"`
	TradeIndex         int32   `parquet:"trade_index"`
	TradeType          string  `parquet:"trade_type"`
	TradeTimestamp     int64   `parquet:"trade_timestamp,timestamp(microsecond)"`
	SellerAccount      string  `parquet:"seller_account"`
	SellingAssetCode   *string `parquet:"selling_asset_code,optional"`
	SellingAssetIssuer *string `parquet:"selling_asset_issuer,optional"`
	SellingAmount      string  `parquet:"selling_amount"`
	BuyerAccount       string  `parquet:"buyer_account"`
	BuyingAssetCode    *string `parquet:"buying_asset_code,optional"`
	BuyingAssetIssuer  *string `parquet:"buying_asset_issuer,optional"`
	BuyingAmount       string  `parquet:"buying_amount"`
	Price              string  `parquet:"price"`
	CreatedAt          int64   `parquet:"created_at,timestamp(microsecond)"`
	LedgerRange        uint32  `parquet:"ledger_range"`
	PipelineVersion    string  `parquet:"pipeline_version"`
}

type ParquetAccount struct {
	AccountID           string  `parquet:"account_id"`
	LedgerSequence      uint32  `parquet:"ledger_sequence"`
	ClosedAt            int64   `parquet:"closed_at,timestamp(microsecond)"`
	Balance             string  `parquet:"balance"`
	SequenceNumber      uint64  `parquet:"sequence_number"`
	NumSubentries       uint32  `parquet:"num_subentries"`
	NumSponsoring       uint32  `parquet:"num_sponsoring"`
	NumSponsored        uint32  `parquet:"num_sponsored"`
	HomeDomain          *string `parquet:"home_domain,optional"`
	MasterWeight        uint32  `parquet:"master_weight"`
	LowThreshold        uint32  `parquet:"low_threshold"`
	MedThreshold        uint32  `parquet:"med_threshold"`
	HighThreshold       uint32  `parquet:"high_threshold"`
	Flags               uint32  `parquet:"flags"`
	AuthRequired        bool    `parquet:"auth_required"`
	AuthRevocable       bool    `parquet:"auth_revocable"`
	AuthImmutable       bool    `parquet:"auth_immutable"`
	AuthClawbackEnabled bool    `parquet:"auth_clawback_enabled"`
	Signers             *string `parquet:"signers,optional"`
	SponsorAccount      *string `parquet:"sponsor_account,optional"`
	LedgerRange         uint32  `parquet:"ledger_range"`
	PipelineVersion     string  `parquet:"pipeline_version"`
}

type ParquetContractEvent struct {
	EventID                  string  `parquet:"event_id"`
	ContractID               *string `parquet:"contract_id,optional"`
	LedgerSequence           uint32  `parquet:"ledger_sequence"`
	TransactionHash          string  `parquet:"transaction_hash"`
	ClosedAt                 int64   `parquet:"closed_at,timestamp(microsecond)"`
	EventType                string  `parquet:"event_type"`
	InSuccessfulContractCall bool    `parquet:"in_successful_contract_call"`
	TopicsJSON               string  `parquet:"topics_json"`
	TopicsDecoded            string  `parquet:"topics_decoded"`
	DataXDR                  string  `parquet:"data_xdr"`
	DataDecoded              string  `parquet:"data_decoded"`
	TopicCount               int32   `parquet:"topic_count"`
	Topic0Decoded            *string `parquet:"topic0_decoded,optional"`
	Topic1Decoded            *string `parquet:"topic1_decoded,optional"`
	Topic2Decoded            *string `parquet:"topic2_decoded,optional"`
	Topic3Decoded            *string `parquet:"topic3_decoded,optional"`
	OperationIndex           uint32  `parquet:"operation_index"`
	EventIndex               uint32  `parquet:"event_index"`
	LedgerRange              uint32  `parquet:"ledger_range"`
	PipelineVersion          string  `parquet:"pipeline_version"`
}

type ParquetNativeBalance struct {
	AccountID          string `parquet:"account_id"`
	Balance            int64  `parquet:"balance"`
	BuyingLiabilities  int64  `parquet:"buying_liabilities"`
	SellingLiabilities int64  `parquet:"selling_liabilities"`
	NumSubentries      int32  `parquet:"num_subentries"`
	NumSponsoring      int32  `parquet:"num_sponsoring"`
	NumSponsored       int32  `parquet:"num_sponsored"`
	SequenceNumber     *int64 `parquet:"sequence_number,optional"`
	LastModifiedLedger int64  `parquet:"last_modified_ledger"`
	LedgerSequence     int64  `parquet:"ledger_sequence"`
	LedgerRange        int64  `parquet:"ledger_range"`
	PipelineVersion    string `parquet:"pipeline_version"`
}

type ParquetOffer struct {
	OfferID            int64   `parquet:"offer_id"`
	SellerAccount      string  `parquet:"seller_account"`
	LedgerSequence     uint32  `parquet:"ledger_sequence"`
	ClosedAt           int64   `parquet:"closed_at,timestamp(microsecond)"`
	SellingAssetType   string  `parquet:"selling_asset_type"`
	SellingAssetCode   *string `parquet:"selling_asset_code,optional"`
	SellingAssetIssuer *string `parquet:"selling_asset_issuer,optional"`
	BuyingAssetType    string  `parquet:"buying_asset_type"`
	BuyingAssetCode    *string `parquet:"buying_asset_code,optional"`
	BuyingAssetIssuer  *string `parquet:"buying_asset_issuer,optional"`
	Amount             string  `parquet:"amount"`
	Price              string  `parquet:"price"`
	Flags              uint32  `parquet:"flags"`
	LedgerRange        uint32  `parquet:"ledger_range"`
	PipelineVersion    string  `parquet:"pipeline_version"`
}

type ParquetTrustline struct {
	AccountID                       string `parquet:"account_id"`
	AssetCode                       string `parquet:"asset_code"`
	AssetIssuer                     string `parquet:"asset_issuer"`
	AssetType                       string `parquet:"asset_type"`
	Balance                         string `parquet:"balance"`
	TrustLimit                      string `parquet:"trust_limit"`
	BuyingLiabilities               string `parquet:"buying_liabilities"`
	SellingLiabilities              string `parquet:"selling_liabilities"`
	Authorized                      bool   `parquet:"authorized"`
	AuthorizedToMaintainLiabilities bool   `parquet:"authorized_to_maintain_liabilities"`
	ClawbackEnabled                 bool   `parquet:"clawback_enabled"`
	LedgerSequence                  uint32 `parquet:"ledger_sequence"`
	LedgerRange                     uint32 `parquet:"ledger_range"`
	PipelineVersion                 string `parquet:"pipeline_version"`
}

type ParquetAccountSigner struct {
	AccountID      string `parquet:"account_id"`
	Signer         string `parquet:"signer"`
	LedgerSequence uint32 `parquet:"ledger_sequence"`
	Weight         uint32 `parquet:"weight"`
	Sponsor        string `parquet:"sponsor"`
	Deleted        bool   `parquet:"deleted"`
	ClosedAt        int64  `parquet:"closed_at,timestamp(microsecond)"`
	LedgerRange     uint32 `parquet:"ledger_range"`
	PipelineVersion string `parquet:"pipeline_version"`
}

type ParquetClaimableBalance struct {
	BalanceID      string  `parquet:"balance_id"`
	Sponsor        string  `parquet:"sponsor"`
	LedgerSequence uint32  `parquet:"ledger_sequence"`
	ClosedAt       int64   `parquet:"closed_at,timestamp(microsecond)"`
	AssetType      string  `parquet:"asset_type"`
	AssetCode      *string `parquet:"asset_code,optional"`
	AssetIssuer    *string `parquet:"asset_issuer,optional"`
	Amount         int64   `parquet:"amount"`
	ClaimantsCount int32   `parquet:"claimants_count"`
	Flags           uint32  `parquet:"flags"`
	LedgerRange     uint32  `parquet:"ledger_range"`
	PipelineVersion string  `parquet:"pipeline_version"`
}

type ParquetLiquidityPool struct {
	LiquidityPoolID string  `parquet:"liquidity_pool_id"`
	LedgerSequence  uint32  `parquet:"ledger_sequence"`
	ClosedAt        int64   `parquet:"closed_at,timestamp(microsecond)"`
	PoolType        string  `parquet:"pool_type"`
	Fee             int32   `parquet:"fee"`
	TrustlineCount  int32   `parquet:"trustline_count"`
	TotalPoolShares int64   `parquet:"total_pool_shares"`
	AssetAType      string  `parquet:"asset_a_type"`
	AssetACode      *string `parquet:"asset_a_code,optional"`
	AssetAIssuer    *string `parquet:"asset_a_issuer,optional"`
	AssetAAmount    int64   `parquet:"asset_a_amount"`
	AssetBType      string  `parquet:"asset_b_type"`
	AssetBCode      *string `parquet:"asset_b_code,optional"`
	AssetBIssuer    *string `parquet:"asset_b_issuer,optional"`
	AssetBAmount    int64   `parquet:"asset_b_amount"`
	LedgerRange     uint32  `parquet:"ledger_range"`
	PipelineVersion string  `parquet:"pipeline_version"`
}

type ParquetTTL struct {
	KeyHash            string `parquet:"key_hash"`
	LedgerSequence     uint32 `parquet:"ledger_sequence"`
	LiveUntilLedgerSeq uint32 `parquet:"live_until_ledger_seq"`
	TTLRemaining       int64  `parquet:"ttl_remaining"`
	Expired            bool   `parquet:"expired"`
	LastModifiedLedger int32  `parquet:"last_modified_ledger"`
	Deleted            bool   `parquet:"deleted"`
	ClosedAt           int64  `parquet:"closed_at,timestamp(microsecond)"`
	LedgerRange        uint32 `parquet:"ledger_range"`
	PipelineVersion    string `parquet:"pipeline_version"`
}

type ParquetEvictedKey struct {
	KeyHash        string `parquet:"key_hash"`
	LedgerSequence uint32 `parquet:"ledger_sequence"`
	ContractID     string `parquet:"contract_id"`
	KeyType        string `parquet:"key_type"`
	Durability      string `parquet:"durability"`
	ClosedAt        int64  `parquet:"closed_at,timestamp(microsecond)"`
	LedgerRange     uint32 `parquet:"ledger_range"`
	PipelineVersion string `parquet:"pipeline_version"`
}

type ParquetContractData struct {
	ContractId         string  `parquet:"contract_id"`
	LedgerSequence     uint32  `parquet:"ledger_sequence"`
	LedgerKeyHash      string  `parquet:"ledger_key_hash"`
	ContractKeyType    string  `parquet:"contract_key_type"`
	ContractDurability string  `parquet:"contract_durability"`
	AssetCode          *string `parquet:"asset_code,optional"`
	AssetIssuer        *string `parquet:"asset_issuer,optional"`
	AssetType          *string `parquet:"asset_type,optional"`
	BalanceHolder      *string `parquet:"balance_holder,optional"`
	Balance            *string `parquet:"balance,optional"`
	LastModifiedLedger int32   `parquet:"last_modified_ledger"`
	LedgerEntryChange  int32   `parquet:"ledger_entry_change"`
	Deleted            bool    `parquet:"deleted"`
	ClosedAt           int64   `parquet:"closed_at,timestamp(microsecond)"`
	ContractDataXDR    string  `parquet:"contract_data_xdr"`
	TokenName          *string `parquet:"token_name,optional"`
	TokenSymbol        *string `parquet:"token_symbol,optional"`
	TokenDecimals      *int32  `parquet:"token_decimals,optional"`
	LedgerRange        uint32  `parquet:"ledger_range"`
	PipelineVersion    string  `parquet:"pipeline_version"`
}

type ParquetContractCode struct {
	ContractCodeHash   string `parquet:"contract_code_hash"`
	LedgerKeyHash      string `parquet:"ledger_key_hash"`
	ContractCodeExtV   int32  `parquet:"contract_code_ext_v"`
	LastModifiedLedger int32  `parquet:"last_modified_ledger"`
	LedgerEntryChange  int32  `parquet:"ledger_entry_change"`
	Deleted            bool   `parquet:"deleted"`
	ClosedAt           int64  `parquet:"closed_at,timestamp(microsecond)"`
	LedgerSequence     uint32 `parquet:"ledger_sequence"`
	NInstructions      *int64 `parquet:"n_instructions,optional"`
	NFunctions         *int64 `parquet:"n_functions,optional"`
	NGlobals           *int64 `parquet:"n_globals,optional"`
	NTableEntries      *int64 `parquet:"n_table_entries,optional"`
	NTypes             *int64 `parquet:"n_types,optional"`
	NDataSegments      *int64 `parquet:"n_data_segments,optional"`
	NElemSegments      *int64 `parquet:"n_elem_segments,optional"`
	NImports           *int64 `parquet:"n_imports,optional"`
	NExports           *int64 `parquet:"n_exports,optional"`
	NDataSegmentBytes  *int64 `parquet:"n_data_segment_bytes,optional"`
	LedgerRange        uint32 `parquet:"ledger_range"`
	PipelineVersion    string `parquet:"pipeline_version"`
}

type ParquetRestoredKey struct {
	KeyHash            string `parquet:"key_hash"`
	LedgerSequence     uint32 `parquet:"ledger_sequence"`
	ContractID         string `parquet:"contract_id"`
	KeyType            string `parquet:"key_type"`
	Durability         string `parquet:"durability"`
	RestoredFromLedger uint32 `parquet:"restored_from_ledger"`
	ClosedAt           int64  `parquet:"closed_at,timestamp(microsecond)"`
	LedgerRange        uint32 `parquet:"ledger_range"`
	PipelineVersion    string `parquet:"pipeline_version"`
}

type ParquetContractCreation struct {
	ContractID     string  `parquet:"contract_id"`
	CreatorAddress string  `parquet:"creator_address"`
	WasmHash       *string `parquet:"wasm_hash,optional"`
	CreatedLedger  uint32  `parquet:"created_ledger"`
	CreatedAt       int64   `parquet:"created_at,timestamp(microsecond)"`
	LedgerRange     uint32  `parquet:"ledger_range"`
	PipelineVersion string  `parquet:"pipeline_version"`
}

type ParquetConfigSetting struct {
	ConfigSettingID    int32  `parquet:"config_setting_id"`
	LedgerSequence     uint32 `parquet:"ledger_sequence"`
	LastModifiedLedger int32  `parquet:"last_modified_ledger"`
	Deleted            bool   `parquet:"deleted"`
	ClosedAt           int64  `parquet:"closed_at,timestamp(microsecond)"`
	ConfigSettingXDR   string `parquet:"config_setting_xdr"`
	LedgerRange        uint32 `parquet:"ledger_range"`
	PipelineVersion    string `parquet:"pipeline_version"`
}

// --- Full ParquetWriter implementation ---

// ParquetWriterFull replaces the stub ParquetWriter with real Parquet output.
type ParquetLedger struct {
	Sequence            uint32  `parquet:"sequence"`
	LedgerHash          string  `parquet:"ledger_hash"`
	PreviousLedgerHash  string  `parquet:"previous_ledger_hash"`
	ClosedAt            int64   `parquet:"closed_at,timestamp(microsecond)"`
	ProtocolVersion     uint32  `parquet:"protocol_version"`
	TotalCoins          int64   `parquet:"total_coins"`
	FeePool             int64   `parquet:"fee_pool"`
	BaseFee             uint32  `parquet:"base_fee"`
	BaseReserve         uint32  `parquet:"base_reserve"`
	MaxTxSetSize        uint32  `parquet:"max_tx_set_size"`
	TransactionCount    int32   `parquet:"transaction_count"`
	OperationCount      int32   `parquet:"operation_count"`
	SuccessfulTxCount   int32   `parquet:"successful_tx_count"`
	FailedTxCount       int32   `parquet:"failed_tx_count"`
	TxSetOperationCount int32   `parquet:"tx_set_operation_count"`
	IngestionTimestamp  int64   `parquet:"ingestion_timestamp,timestamp(microsecond)"`
	LedgerRange         uint32  `parquet:"ledger_range"`
	PipelineVersion     string  `parquet:"pipeline_version"`
}

type ParquetWriterFull struct {
	workerID           int
	pipelineVersion    string
	transactions       *ParquetTableWriter[ParquetTransaction]
	operations         *ParquetTableWriter[ParquetOperation]
	effects            *ParquetTableWriter[ParquetEffect]
	trades             *ParquetTableWriter[ParquetTrade]
	accounts           *ParquetTableWriter[ParquetAccount]
	offers             *ParquetTableWriter[ParquetOffer]
	trustlines         *ParquetTableWriter[ParquetTrustline]
	accountSigners     *ParquetTableWriter[ParquetAccountSigner]
	claimableBalances  *ParquetTableWriter[ParquetClaimableBalance]
	liquidityPools     *ParquetTableWriter[ParquetLiquidityPool]
	configSettings     *ParquetTableWriter[ParquetConfigSetting]
	ttl                *ParquetTableWriter[ParquetTTL]
	evictedKeys        *ParquetTableWriter[ParquetEvictedKey]
	contractEvents     *ParquetTableWriter[ParquetContractEvent]
	contractData       *ParquetTableWriter[ParquetContractData]
	contractCode       *ParquetTableWriter[ParquetContractCode]
	nativeBalances     *ParquetTableWriter[ParquetNativeBalance]
	restoredKeys       *ParquetTableWriter[ParquetRestoredKey]
	contractCreations  *ParquetTableWriter[ParquetContractCreation]
	ledgers            *ParquetTableWriter[ParquetLedger]
}

func NewParquetWriterFull(outputDir string, workerID int, pipelineVersion string) *ParquetWriterFull {
	return &ParquetWriterFull{
		workerID:          workerID,
		pipelineVersion:   pipelineVersion,
		transactions:      NewParquetTableWriter[ParquetTransaction](outputDir, "transactions", workerID),
		operations:        NewParquetTableWriter[ParquetOperation](outputDir, "operations", workerID),
		effects:           NewParquetTableWriter[ParquetEffect](outputDir, "effects", workerID),
		trades:            NewParquetTableWriter[ParquetTrade](outputDir, "trades", workerID),
		accounts:          NewParquetTableWriter[ParquetAccount](outputDir, "accounts_snapshot", workerID),
		offers:            NewParquetTableWriter[ParquetOffer](outputDir, "offers_snapshot", workerID),
		trustlines:        NewParquetTableWriter[ParquetTrustline](outputDir, "trustlines_snapshot", workerID),
		accountSigners:    NewParquetTableWriter[ParquetAccountSigner](outputDir, "account_signers_snapshot", workerID),
		claimableBalances: NewParquetTableWriter[ParquetClaimableBalance](outputDir, "claimable_balances_snapshot", workerID),
		liquidityPools:    NewParquetTableWriter[ParquetLiquidityPool](outputDir, "liquidity_pools_snapshot", workerID),
		configSettings:    NewParquetTableWriter[ParquetConfigSetting](outputDir, "config_settings", workerID),
		ttl:               NewParquetTableWriter[ParquetTTL](outputDir, "ttl_snapshot", workerID),
		evictedKeys:       NewParquetTableWriter[ParquetEvictedKey](outputDir, "evicted_keys", workerID),
		contractEvents:    NewParquetTableWriter[ParquetContractEvent](outputDir, "contract_events", workerID),
		contractData:      NewParquetTableWriter[ParquetContractData](outputDir, "contract_data_snapshot", workerID),
		contractCode:      NewParquetTableWriter[ParquetContractCode](outputDir, "contract_code_snapshot", workerID),
		nativeBalances:    NewParquetTableWriter[ParquetNativeBalance](outputDir, "native_balances", workerID),
		restoredKeys:      NewParquetTableWriter[ParquetRestoredKey](outputDir, "restored_keys", workerID),
		contractCreations: NewParquetTableWriter[ParquetContractCreation](outputDir, "contract_creations", workerID),
		ledgers:           NewParquetTableWriter[ParquetLedger](outputDir, "ledgers", workerID),
	}
}

func (pw *ParquetWriterFull) WriteBatch(batch *BatchData) error {
	// Transactions
	if len(batch.Transactions) > 0 {
		rows := make([]ParquetTransaction, len(batch.Transactions))
		for i, t := range batch.Transactions {
			rows[i] = ParquetTransaction{
				LedgerSequence:               t.LedgerSequence,
				TransactionHash:              t.TransactionHash,
				SourceAccount:                t.SourceAccount,
				SourceAccountMuxed:           t.SourceAccountMuxed,
				FeeCharged:                   t.FeeCharged,
				MaxFee:                       t.MaxFee,
				Successful:                   t.Successful,
				TransactionResultCode:        t.TransactionResultCode,
				OperationCount:               int32(t.OperationCount),
				MemoType:                     t.MemoType,
				Memo:                         t.Memo,
				CreatedAt:                    t.CreatedAt.UnixMicro(),
				AccountSequence:              t.AccountSequence,
				LedgerRange:                  t.LedgerRange,
				SignaturesCount:              int32(t.SignaturesCount),
				NewAccount:                   t.NewAccount,
				TimeboundsMinTime:            t.TimeboundsMinTime,
				TimeboundsMaxTime:            t.TimeboundsMaxTime,
				SorobanHostFunctionType:      t.SorobanHostFunctionType,
				SorobanContractID:            t.SorobanContractID,
				RentFeeCharged:               t.RentFeeCharged,
				SorobanResourcesInstructions: t.SorobanResourcesInstructions,
				SorobanResourcesReadBytes:    t.SorobanResourcesReadBytes,
				SorobanResourcesWriteBytes:   t.SorobanResourcesWriteBytes,
				PipelineVersion:              pw.pipelineVersion,
			}
		}
		if err := pw.transactions.Write(rows, func(r ParquetTransaction) uint32 { return r.LedgerRange }); err != nil {
			return fmt.Errorf("write transactions: %w", err)
		}
	}

	// Operations
	if len(batch.Operations) > 0 {
		rows := make([]ParquetOperation, len(batch.Operations))
		for i, o := range batch.Operations {
			var maxDepth *int32
			if o.MaxCallDepth != nil {
				d := int32(*o.MaxCallDepth)
				maxDepth = &d
			}
			// Convert *int to *int32 for parquet fields
			var setFlags, clearFlags, masterWeight, lowThreshold, mediumThreshold, highThreshold *int32
			if o.SetFlags != nil {
				v := int32(*o.SetFlags)
				setFlags = &v
			}
			if o.ClearFlags != nil {
				v := int32(*o.ClearFlags)
				clearFlags = &v
			}
			if o.MasterWeight != nil {
				v := int32(*o.MasterWeight)
				masterWeight = &v
			}
			if o.LowThreshold != nil {
				v := int32(*o.LowThreshold)
				lowThreshold = &v
			}
			if o.MediumThreshold != nil {
				v := int32(*o.MediumThreshold)
				mediumThreshold = &v
			}
			if o.HighThreshold != nil {
				v := int32(*o.HighThreshold)
				highThreshold = &v
			}
			rows[i] = ParquetOperation{
				TransactionHash:       o.TransactionHash,
				TransactionIndex:      int32(o.TransactionIndex),
				OperationIndex:        int32(o.OperationIndex),
				LedgerSequence:        o.LedgerSequence,
				SourceAccount:         o.SourceAccount,
				SourceAccountMuxed:    o.SourceAccountMuxed,
				OpType:                int32(o.OpType),
				TypeString:            o.TypeString,
				CreatedAt:             o.CreatedAt.UnixMicro(),
				TransactionSuccessful: o.TransactionSuccessful,
				OperationResultCode:   o.OperationResultCode,
				LedgerRange:           o.LedgerRange,
				Amount:                o.Amount,
				Asset:                 o.Asset,
				AssetType:             o.AssetType,
				AssetCode:             o.AssetCode,
				AssetIssuer:           o.AssetIssuer,
				Destination:           o.Destination,
				SourceAsset:           o.SourceAsset,
				SourceAssetType:       o.SourceAssetType,
				SourceAssetCode:       o.SourceAssetCode,
				SourceAssetIssuer:     o.SourceAssetIssuer,
				SourceAmount:          o.SourceAmount,
				DestinationMin:        o.DestinationMin,
				StartingBalance:       o.StartingBalance,
				TrustlineLimit:        o.TrustlineLimit,
				OfferID:               o.OfferID,
				Price:                 o.Price,
				PriceR:                o.PriceR,
				BuyingAsset:           o.BuyingAsset,
				BuyingAssetType:       o.BuyingAssetType,
				BuyingAssetCode:       o.BuyingAssetCode,
				BuyingAssetIssuer:     o.BuyingAssetIssuer,
				SellingAsset:          o.SellingAsset,
				SellingAssetType:      o.SellingAssetType,
				SellingAssetCode:      o.SellingAssetCode,
				SellingAssetIssuer:    o.SellingAssetIssuer,
				SetFlags:              setFlags,
				ClearFlags:            clearFlags,
				HomeDomain:            o.HomeDomain,
				MasterWeight:          masterWeight,
				LowThreshold:          lowThreshold,
				MediumThreshold:       mediumThreshold,
				HighThreshold:         highThreshold,
				DataName:              o.DataName,
				DataValue:             o.DataValue,
				BalanceID:             o.BalanceID,
				SponsoredID:           o.SponsoredID,
				BumpTo:                o.BumpTo,
				SorobanAuthRequired:   o.SorobanAuthRequired,
				SorobanOperation:      o.SorobanOperation,
				SorobanContractID:     o.SorobanContractID,
				SorobanFunction:       o.SorobanFunction,
				SorobanArgumentsJSON:  o.SorobanArgumentsJSON,
				ContractCallsJSON:     o.ContractCallsJSON,
				MaxCallDepth:          maxDepth,
				PipelineVersion:       pw.pipelineVersion,
			}
		}
		if err := pw.operations.Write(rows, func(r ParquetOperation) uint32 { return r.LedgerRange }); err != nil {
			return fmt.Errorf("write operations: %w", err)
		}
	}

	// Effects
	if len(batch.Effects) > 0 {
		rows := make([]ParquetEffect, len(batch.Effects))
		for i, e := range batch.Effects {
			var signerWeight *int32
			if e.SignerWeight != nil {
				w := int32(*e.SignerWeight)
				signerWeight = &w
			}
			rows[i] = ParquetEffect{
				LedgerSequence:   e.LedgerSequence,
				TransactionHash:  e.TransactionHash,
				OperationIndex:   int32(e.OperationIndex),
				EffectIndex:      int32(e.EffectIndex),
				EffectType:       int32(e.EffectType),
				EffectTypeString: e.EffectTypeString,
				AccountID:        e.AccountID,
				Amount:           e.Amount,
				AssetCode:        e.AssetCode,
				AssetIssuer:      e.AssetIssuer,
				AssetType:        e.AssetType,
				TrustlineLimit:   e.TrustlineLimit,
				AuthorizeFlag:    e.AuthorizeFlag,
				ClawbackFlag:     e.ClawbackFlag,
				SignerAccount:    e.SignerAccount,
				SignerWeight:     signerWeight,
				OfferID:          e.OfferID,
				SellerAccount:    e.SellerAccount,
				CreatedAt:        e.CreatedAt.UnixMicro(),
				LedgerRange:      e.LedgerRange,
				PipelineVersion:  pw.pipelineVersion,
			}
		}
		if err := pw.effects.Write(rows, func(r ParquetEffect) uint32 { return r.LedgerRange }); err != nil {
			return fmt.Errorf("write effects: %w", err)
		}
	}

	// Trades
	if len(batch.Trades) > 0 {
		rows := make([]ParquetTrade, len(batch.Trades))
		for i, t := range batch.Trades {
			rows[i] = ParquetTrade{
				LedgerSequence:     t.LedgerSequence,
				TransactionHash:    t.TransactionHash,
				OperationIndex:     int32(t.OperationIndex),
				TradeIndex:         int32(t.TradeIndex),
				TradeType:          t.TradeType,
				TradeTimestamp:     t.TradeTimestamp.UnixMicro(),
				SellerAccount:      t.SellerAccount,
				SellingAssetCode:   t.SellingAssetCode,
				SellingAssetIssuer: t.SellingAssetIssuer,
				SellingAmount:      t.SellingAmount,
				BuyerAccount:       t.BuyerAccount,
				BuyingAssetCode:    t.BuyingAssetCode,
				BuyingAssetIssuer:  t.BuyingAssetIssuer,
				BuyingAmount:       t.BuyingAmount,
				Price:              t.Price,
				CreatedAt:          t.CreatedAt.UnixMicro(),
				LedgerRange:        t.LedgerRange,
				PipelineVersion:    pw.pipelineVersion,
			}
		}
		if err := pw.trades.Write(rows, func(r ParquetTrade) uint32 { return r.LedgerRange }); err != nil {
			return fmt.Errorf("write trades: %w", err)
		}
	}

	// Accounts
	if len(batch.Accounts) > 0 {
		rows := make([]ParquetAccount, len(batch.Accounts))
		for i, a := range batch.Accounts {
			rows[i] = ParquetAccount{
				AccountID:           a.AccountID,
				LedgerSequence:      a.LedgerSequence,
				ClosedAt:            a.ClosedAt.UnixMicro(),
				Balance:             a.Balance,
				SequenceNumber:      a.SequenceNumber,
				NumSubentries:       a.NumSubentries,
				NumSponsoring:       a.NumSponsoring,
				NumSponsored:        a.NumSponsored,
				HomeDomain:          a.HomeDomain,
				MasterWeight:        a.MasterWeight,
				LowThreshold:        a.LowThreshold,
				MedThreshold:        a.MedThreshold,
				HighThreshold:       a.HighThreshold,
				Flags:               a.Flags,
				AuthRequired:        a.AuthRequired,
				AuthRevocable:       a.AuthRevocable,
				AuthImmutable:       a.AuthImmutable,
				AuthClawbackEnabled: a.AuthClawbackEnabled,
				Signers:             a.Signers,
				SponsorAccount:      a.SponsorAccount,
				LedgerRange:         a.LedgerRange,
				PipelineVersion:     pw.pipelineVersion,
			}
		}
		if err := pw.accounts.Write(rows, func(r ParquetAccount) uint32 { return r.LedgerRange }); err != nil {
			return fmt.Errorf("write accounts: %w", err)
		}
	}

	// Contract Events
	if len(batch.ContractEvents) > 0 {
		rows := make([]ParquetContractEvent, len(batch.ContractEvents))
		for i, e := range batch.ContractEvents {
			rows[i] = ParquetContractEvent{
				EventID:                  e.EventID,
				ContractID:               e.ContractID,
				LedgerSequence:           e.LedgerSequence,
				TransactionHash:          e.TransactionHash,
				ClosedAt:                 e.ClosedAt.UnixMicro(),
				EventType:                e.EventType,
				InSuccessfulContractCall: e.InSuccessfulContractCall,
				TopicsJSON:               e.TopicsJSON,
				TopicsDecoded:            e.TopicsDecoded,
				DataXDR:                  e.DataXDR,
				DataDecoded:              e.DataDecoded,
				TopicCount:               e.TopicCount,
				Topic0Decoded:            e.Topic0Decoded,
				Topic1Decoded:            e.Topic1Decoded,
				Topic2Decoded:            e.Topic2Decoded,
				Topic3Decoded:            e.Topic3Decoded,
				OperationIndex:           e.OperationIndex,
				EventIndex:               e.EventIndex,
				LedgerRange:              e.LedgerRange,
				PipelineVersion:          pw.pipelineVersion,
			}
		}
		if err := pw.contractEvents.Write(rows, func(r ParquetContractEvent) uint32 { return r.LedgerRange }); err != nil {
			return fmt.Errorf("write contract_events: %w", err)
		}
	}

	// Offers
	if len(batch.Offers) > 0 {
		rows := make([]ParquetOffer, len(batch.Offers))
		for i, o := range batch.Offers {
			rows[i] = ParquetOffer{
				OfferID: o.OfferID, SellerAccount: o.SellerAccount, LedgerSequence: o.LedgerSequence,
				ClosedAt: o.ClosedAt.UnixMicro(), SellingAssetType: o.SellingAssetType, SellingAssetCode: o.SellingAssetCode,
				SellingAssetIssuer: o.SellingAssetIssuer, BuyingAssetType: o.BuyingAssetType, BuyingAssetCode: o.BuyingAssetCode,
				BuyingAssetIssuer: o.BuyingAssetIssuer, Amount: o.Amount, Price: o.Price, Flags: o.Flags, LedgerRange: o.LedgerRange,
				PipelineVersion: pw.pipelineVersion,
			}
		}
		if err := pw.offers.Write(rows, func(r ParquetOffer) uint32 { return r.LedgerRange }); err != nil {
			return fmt.Errorf("write offers: %w", err)
		}
	}

	// Trustlines
	if len(batch.Trustlines) > 0 {
		rows := make([]ParquetTrustline, len(batch.Trustlines))
		for i, t := range batch.Trustlines {
			rows[i] = ParquetTrustline{
				AccountID: t.AccountID, AssetCode: t.AssetCode, AssetIssuer: t.AssetIssuer, AssetType: t.AssetType,
				Balance: t.Balance, TrustLimit: t.TrustLimit, BuyingLiabilities: t.BuyingLiabilities,
				SellingLiabilities: t.SellingLiabilities, Authorized: t.Authorized,
				AuthorizedToMaintainLiabilities: t.AuthorizedToMaintainLiabilities, ClawbackEnabled: t.ClawbackEnabled,
				LedgerSequence: t.LedgerSequence, LedgerRange: t.LedgerRange,
				PipelineVersion: pw.pipelineVersion,
			}
		}
		if err := pw.trustlines.Write(rows, func(r ParquetTrustline) uint32 { return r.LedgerRange }); err != nil {
			return fmt.Errorf("write trustlines: %w", err)
		}
	}

	// Account Signers
	if len(batch.AccountSigners) > 0 {
		rows := make([]ParquetAccountSigner, len(batch.AccountSigners))
		for i, s := range batch.AccountSigners {
			rows[i] = ParquetAccountSigner{
				AccountID: s.AccountID, Signer: s.Signer, LedgerSequence: s.LedgerSequence,
				Weight: s.Weight, Sponsor: s.Sponsor, Deleted: s.Deleted,
				ClosedAt: s.ClosedAt.UnixMicro(), LedgerRange: s.LedgerRange,
				PipelineVersion: pw.pipelineVersion,
			}
		}
		if err := pw.accountSigners.Write(rows, func(r ParquetAccountSigner) uint32 { return r.LedgerRange }); err != nil {
			return fmt.Errorf("write account_signers: %w", err)
		}
	}

	// Claimable Balances
	if len(batch.ClaimableBalances) > 0 {
		rows := make([]ParquetClaimableBalance, len(batch.ClaimableBalances))
		for i, b := range batch.ClaimableBalances {
			rows[i] = ParquetClaimableBalance{
				BalanceID: b.BalanceID, Sponsor: b.Sponsor, LedgerSequence: b.LedgerSequence,
				ClosedAt: b.ClosedAt.UnixMicro(), AssetType: b.AssetType, AssetCode: b.AssetCode,
				AssetIssuer: b.AssetIssuer, Amount: b.Amount, ClaimantsCount: b.ClaimantsCount,
				Flags: b.Flags, LedgerRange: b.LedgerRange,
				PipelineVersion: pw.pipelineVersion,
			}
		}
		if err := pw.claimableBalances.Write(rows, func(r ParquetClaimableBalance) uint32 { return r.LedgerRange }); err != nil {
			return fmt.Errorf("write claimable_balances: %w", err)
		}
	}

	// Liquidity Pools
	if len(batch.LiquidityPools) > 0 {
		rows := make([]ParquetLiquidityPool, len(batch.LiquidityPools))
		for i, p := range batch.LiquidityPools {
			rows[i] = ParquetLiquidityPool{
				LiquidityPoolID: p.LiquidityPoolID, LedgerSequence: p.LedgerSequence, ClosedAt: p.ClosedAt.UnixMicro(),
				PoolType: p.PoolType, Fee: p.Fee, TrustlineCount: p.TrustlineCount, TotalPoolShares: p.TotalPoolShares,
				AssetAType: p.AssetAType, AssetACode: p.AssetACode, AssetAIssuer: p.AssetAIssuer, AssetAAmount: p.AssetAAmount,
				AssetBType: p.AssetBType, AssetBCode: p.AssetBCode, AssetBIssuer: p.AssetBIssuer, AssetBAmount: p.AssetBAmount,
				LedgerRange: p.LedgerRange,
				PipelineVersion: pw.pipelineVersion,
			}
		}
		if err := pw.liquidityPools.Write(rows, func(r ParquetLiquidityPool) uint32 { return r.LedgerRange }); err != nil {
			return fmt.Errorf("write liquidity_pools: %w", err)
		}
	}

	// Config Settings
	if len(batch.ConfigSettings) > 0 {
		rows := make([]ParquetConfigSetting, len(batch.ConfigSettings))
		for i, c := range batch.ConfigSettings {
			rows[i] = ParquetConfigSetting{
				ConfigSettingID: c.ConfigSettingID, LedgerSequence: c.LedgerSequence,
				LastModifiedLedger: c.LastModifiedLedger, Deleted: c.Deleted, ClosedAt: c.ClosedAt.UnixMicro(),
				ConfigSettingXDR: c.ConfigSettingXDR, LedgerRange: c.LedgerRange,
				PipelineVersion: pw.pipelineVersion,
			}
		}
		if err := pw.configSettings.Write(rows, func(r ParquetConfigSetting) uint32 { return r.LedgerRange }); err != nil {
			return fmt.Errorf("write config_settings: %w", err)
		}
	}

	// TTL
	if len(batch.TTLEntries) > 0 {
		rows := make([]ParquetTTL, len(batch.TTLEntries))
		for i, t := range batch.TTLEntries {
			rows[i] = ParquetTTL{
				KeyHash: t.KeyHash, LedgerSequence: t.LedgerSequence, LiveUntilLedgerSeq: t.LiveUntilLedgerSeq,
				TTLRemaining: t.TTLRemaining, Expired: t.Expired, LastModifiedLedger: t.LastModifiedLedger,
				Deleted: t.Deleted, ClosedAt: t.ClosedAt.UnixMicro(), LedgerRange: t.LedgerRange,
				PipelineVersion: pw.pipelineVersion,
			}
		}
		if err := pw.ttl.Write(rows, func(r ParquetTTL) uint32 { return r.LedgerRange }); err != nil {
			return fmt.Errorf("write ttl: %w", err)
		}
	}

	// Evicted Keys
	if len(batch.EvictedKeys) > 0 {
		rows := make([]ParquetEvictedKey, len(batch.EvictedKeys))
		for i, e := range batch.EvictedKeys {
			rows[i] = ParquetEvictedKey{
				KeyHash: e.KeyHash, LedgerSequence: e.LedgerSequence, ContractID: e.ContractID,
				KeyType: e.KeyType, Durability: e.Durability, ClosedAt: e.ClosedAt.UnixMicro(), LedgerRange: e.LedgerRange,
				PipelineVersion: pw.pipelineVersion,
			}
		}
		if err := pw.evictedKeys.Write(rows, func(r ParquetEvictedKey) uint32 { return r.LedgerRange }); err != nil {
			return fmt.Errorf("write evicted_keys: %w", err)
		}
	}

	// Contract Data
	if len(batch.ContractData) > 0 {
		rows := make([]ParquetContractData, len(batch.ContractData))
		for i, d := range batch.ContractData {
			rows[i] = ParquetContractData{
				ContractId: d.ContractId, LedgerSequence: d.LedgerSequence, LedgerKeyHash: d.LedgerKeyHash,
				ContractKeyType: d.ContractKeyType, ContractDurability: d.ContractDurability,
				AssetCode: d.AssetCode, AssetIssuer: d.AssetIssuer, AssetType: d.AssetType,
				BalanceHolder: d.BalanceHolder, Balance: d.Balance,
				LastModifiedLedger: d.LastModifiedLedger, LedgerEntryChange: d.LedgerEntryChange,
				Deleted: d.Deleted, ClosedAt: d.ClosedAt.UnixMicro(), ContractDataXDR: d.ContractDataXDR,
				TokenName: d.TokenName, TokenSymbol: d.TokenSymbol, TokenDecimals: d.TokenDecimals,
				LedgerRange: d.LedgerRange,
				PipelineVersion: pw.pipelineVersion,
			}
		}
		if err := pw.contractData.Write(rows, func(r ParquetContractData) uint32 { return r.LedgerRange }); err != nil {
			return fmt.Errorf("write contract_data: %w", err)
		}
	}

	// Contract Code
	if len(batch.ContractCode) > 0 {
		rows := make([]ParquetContractCode, len(batch.ContractCode))
		for i, c := range batch.ContractCode {
			rows[i] = ParquetContractCode{
				ContractCodeHash: c.ContractCodeHash, LedgerKeyHash: c.LedgerKeyHash, ContractCodeExtV: c.ContractCodeExtV,
				LastModifiedLedger: c.LastModifiedLedger, LedgerEntryChange: c.LedgerEntryChange,
				Deleted: c.Deleted, ClosedAt: c.ClosedAt.UnixMicro(), LedgerSequence: c.LedgerSequence,
				NInstructions: c.NInstructions, NFunctions: c.NFunctions, NGlobals: c.NGlobals,
				NTableEntries: c.NTableEntries, NTypes: c.NTypes, NDataSegments: c.NDataSegments,
				NElemSegments: c.NElemSegments, NImports: c.NImports, NExports: c.NExports,
				NDataSegmentBytes: c.NDataSegmentBytes, LedgerRange: c.LedgerRange,
				PipelineVersion: pw.pipelineVersion,
			}
		}
		if err := pw.contractCode.Write(rows, func(r ParquetContractCode) uint32 { return r.LedgerRange }); err != nil {
			return fmt.Errorf("write contract_code: %w", err)
		}
	}

	// Restored Keys
	if len(batch.RestoredKeys) > 0 {
		rows := make([]ParquetRestoredKey, len(batch.RestoredKeys))
		for i, r := range batch.RestoredKeys {
			rows[i] = ParquetRestoredKey{
				KeyHash: r.KeyHash, LedgerSequence: r.LedgerSequence, ContractID: r.ContractID,
				KeyType: r.KeyType, Durability: r.Durability, RestoredFromLedger: r.RestoredFromLedger,
				ClosedAt: r.ClosedAt.UnixMicro(), LedgerRange: r.LedgerRange,
				PipelineVersion: pw.pipelineVersion,
			}
		}
		if err := pw.restoredKeys.Write(rows, func(r ParquetRestoredKey) uint32 { return r.LedgerRange }); err != nil {
			return fmt.Errorf("write restored_keys: %w", err)
		}
	}

	// Contract Creations
	if len(batch.ContractCreations) > 0 {
		rows := make([]ParquetContractCreation, len(batch.ContractCreations))
		for i, c := range batch.ContractCreations {
			rows[i] = ParquetContractCreation{
				ContractID: c.ContractID, CreatorAddress: c.CreatorAddress, WasmHash: c.WasmHash,
				CreatedLedger: c.CreatedLedger, CreatedAt: c.CreatedAt.UnixMicro(), LedgerRange: c.LedgerRange,
				PipelineVersion: pw.pipelineVersion,
			}
		}
		if err := pw.contractCreations.Write(rows, func(r ParquetContractCreation) uint32 { return r.LedgerRange }); err != nil {
			return fmt.Errorf("write contract_creations: %w", err)
		}
	}

	// Native Balances
	if len(batch.NativeBalances) > 0 {
		rows := make([]ParquetNativeBalance, len(batch.NativeBalances))
		for i, n := range batch.NativeBalances {
			rows[i] = ParquetNativeBalance{
				AccountID:          n.AccountID,
				Balance:            n.Balance,
				BuyingLiabilities:  n.BuyingLiabilities,
				SellingLiabilities: n.SellingLiabilities,
				NumSubentries:      n.NumSubentries,
				NumSponsoring:      n.NumSponsoring,
				NumSponsored:       n.NumSponsored,
				SequenceNumber:     n.SequenceNumber,
				LastModifiedLedger: n.LastModifiedLedger,
				LedgerSequence:     n.LedgerSequence,
				LedgerRange:        n.LedgerRange,
				PipelineVersion:    pw.pipelineVersion,
			}
		}
		if err := pw.nativeBalances.Write(rows, func(r ParquetNativeBalance) uint32 { return uint32(r.LedgerRange) }); err != nil {
			return fmt.Errorf("write native_balances: %w", err)
		}
	}

	// Ledgers
	if len(batch.Ledgers) > 0 {
		rows := make([]ParquetLedger, len(batch.Ledgers))
		for i, l := range batch.Ledgers {
			rows[i] = ParquetLedger{
				Sequence:            l.Sequence,
				LedgerHash:          l.LedgerHash,
				PreviousLedgerHash:  l.PreviousLedgerHash,
				ClosedAt:            l.ClosedAt.UnixMicro(),
				ProtocolVersion:     l.ProtocolVersion,
				TotalCoins:          l.TotalCoins,
				FeePool:             l.FeePool,
				BaseFee:             l.BaseFee,
				BaseReserve:         l.BaseReserve,
				MaxTxSetSize:        l.MaxTxSetSize,
				TransactionCount:    int32(l.TransactionCount),
				OperationCount:      int32(l.OperationCount),
				SuccessfulTxCount:   int32(l.SuccessfulTxCount),
				FailedTxCount:       int32(l.FailedTxCount),
				TxSetOperationCount: int32(l.TxSetOperationCount),
				IngestionTimestamp:  l.IngestionTimestamp.UnixMicro(),
				LedgerRange:         l.LedgerRange,
				PipelineVersion:     pw.pipelineVersion,
			}
		}
		if err := pw.ledgers.Write(rows, func(r ParquetLedger) uint32 { return r.LedgerRange }); err != nil {
			return fmt.Errorf("write ledgers: %w", err)
		}
	}

	return nil
}

func (pw *ParquetWriterFull) Close() error {
	var firstErr error
	for _, closer := range []interface{ Close() error }{
		pw.transactions, pw.operations, pw.effects, pw.trades,
		pw.accounts, pw.offers, pw.trustlines, pw.accountSigners,
		pw.claimableBalances, pw.liquidityPools, pw.configSettings,
		pw.ttl, pw.evictedKeys, pw.contractEvents, pw.contractData,
		pw.contractCode, pw.nativeBalances, pw.restoredKeys, pw.contractCreations,
		pw.ledgers,
	} {
		if err := closer.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}
