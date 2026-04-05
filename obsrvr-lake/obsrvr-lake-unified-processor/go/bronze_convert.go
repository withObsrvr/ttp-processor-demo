package main

import (
	"time"

	pb "github.com/withObsrvr/obsrvr-lake-unified-processor/gen/bronze_ledger_service"
)

// ConvertToProto converts internal BronzeData to the gRPC protobuf message.
func ConvertToProto(data *BronzeData, ledgerSeq uint32, closedAt time.Time, ledgerRange uint32) *pb.BronzeLedgerData {
	msg := &pb.BronzeLedgerData{
		LedgerSequence: ledgerSeq,
		ClosedAt:       closedAt.Format(time.RFC3339),
		LedgerRange:    ledgerRange,
	}

	if data.Ledger != nil {
		msg.Ledger = convertLedgerRow(data.Ledger)
	}

	for i := range data.Transactions {
		msg.Transactions = append(msg.Transactions, convertTransactionRow(&data.Transactions[i]))
	}

	for i := range data.Operations {
		msg.Operations = append(msg.Operations, convertOperationRow(&data.Operations[i]))
	}

	for i := range data.Effects {
		msg.Effects = append(msg.Effects, convertEffectRow(&data.Effects[i]))
	}

	for i := range data.Trades {
		msg.Trades = append(msg.Trades, convertTradeRow(&data.Trades[i]))
	}

	for i := range data.Accounts {
		msg.Accounts = append(msg.Accounts, convertAccountRow(&data.Accounts[i]))
	}

	for i := range data.Trustlines {
		msg.Trustlines = append(msg.Trustlines, convertTrustlineRow(&data.Trustlines[i]))
	}

	for i := range data.Offers {
		msg.Offers = append(msg.Offers, convertOfferRow(&data.Offers[i]))
	}

	for i := range data.AccountSigners {
		msg.AccountSigners = append(msg.AccountSigners, convertAccountSignerRow(&data.AccountSigners[i]))
	}

	for i := range data.ClaimableBalances {
		msg.ClaimableBalances = append(msg.ClaimableBalances, convertClaimableBalanceRow(&data.ClaimableBalances[i]))
	}

	for i := range data.LiquidityPools {
		msg.LiquidityPools = append(msg.LiquidityPools, convertLiquidityPoolRow(&data.LiquidityPools[i]))
	}

	for i := range data.ConfigSettings {
		msg.ConfigSettings = append(msg.ConfigSettings, convertConfigSettingRow(&data.ConfigSettings[i]))
	}

	for i := range data.TTLEntries {
		msg.TtlEntries = append(msg.TtlEntries, convertTTLRow(&data.TTLEntries[i]))
	}

	for i := range data.EvictedKeys {
		msg.EvictedKeys = append(msg.EvictedKeys, convertEvictedKeyRow(&data.EvictedKeys[i]))
	}

	for i := range data.RestoredKeys {
		msg.RestoredKeys = append(msg.RestoredKeys, convertRestoredKeyRow(&data.RestoredKeys[i]))
	}

	for i := range data.ContractEvents {
		msg.ContractEvents = append(msg.ContractEvents, convertContractEventRow(&data.ContractEvents[i]))
	}

	for i := range data.ContractData {
		msg.ContractData = append(msg.ContractData, convertContractDataRow(&data.ContractData[i]))
	}

	for i := range data.ContractCode {
		msg.ContractCode = append(msg.ContractCode, convertContractCodeRow(&data.ContractCode[i]))
	}

	for i := range data.NativeBalances {
		msg.NativeBalances = append(msg.NativeBalances, convertNativeBalanceRow(&data.NativeBalances[i]))
	}

	for i := range data.ContractCreations {
		msg.ContractCreations = append(msg.ContractCreations, convertContractCreationRow(&data.ContractCreations[i]))
	}

	return msg
}

// --- Conversion helpers ---

func int32Ptr(v *int) *int32 {
	if v == nil {
		return nil
	}
	i := int32(*v)
	return &i
}

func convertLedgerRow(r *LedgerRow) *pb.LedgerRow {
	return &pb.LedgerRow{
		Sequence:             r.Sequence,
		LedgerHash:           r.LedgerHash,
		PreviousLedgerHash:   r.PreviousLedgerHash,
		ClosedAt:             r.ClosedAt.Format(time.RFC3339),
		ProtocolVersion:      int32(r.ProtocolVersion),
		TotalCoins:           r.TotalCoins,
		FeePool:              r.FeePool,
		BaseFee:              int32(r.BaseFee),
		BaseReserve:          int32(r.BaseReserve),
		MaxTxSetSize:         int32(r.MaxTxSetSize),
		SuccessfulTxCount:    int32(r.SuccessfulTxCount),
		FailedTxCount:        int32(r.FailedTxCount),
		TransactionCount:     int32(r.TransactionCount),
		OperationCount:       int32(r.OperationCount),
		TxSetOperationCount:  int32(r.TxSetOperationCount),
		LedgerRange:          r.LedgerRange,
		SorobanFeeWrite1Kb:   r.SorobanFeeWrite1KB,
		NodeId:               r.NodeID,
		Signature:            r.Signature,
		LedgerHeader:         r.LedgerHeader,
		BucketListSize:       r.BucketListSize,
		LiveSorobanStateSize: r.LiveSorobanStateSize,
		EvictedKeysCount:     int32Ptr(r.EvictedKeysCount),
		SorobanOpCount:       int32Ptr(r.SorobanOpCount),
		TotalFeeCharged:      r.TotalFeeCharged,
		ContractEventsCount:  int32Ptr(r.ContractEventsCount),
	}
}

func convertTransactionRow(r *TransactionRow) *pb.TransactionRow {
	return &pb.TransactionRow{
		LedgerSequence:               r.LedgerSequence,
		TransactionHash:              r.TransactionHash,
		SourceAccount:                r.SourceAccount,
		FeeCharged:                   r.FeeCharged,
		MaxFee:                       r.MaxFee,
		Successful:                   r.Successful,
		TransactionResultCode:        r.TransactionResultCode,
		OperationCount:               int32(r.OperationCount),
		MemoType:                     r.MemoType,
		Memo:                         r.Memo,
		CreatedAt:                    r.CreatedAt.Format(time.RFC3339),
		AccountSequence:              r.AccountSequence,
		LedgerRange:                  r.LedgerRange,
		SourceAccountMuxed:           r.SourceAccountMuxed,
		FeeAccountMuxed:              r.FeeAccountMuxed,
		InnerTransactionHash:         r.InnerTransactionHash,
		FeeBumpFee:                   r.FeeBumpFee,
		MaxFeeBid:                    r.MaxFeeBid,
		InnerSourceAccount:           r.InnerSourceAccount,
		TimeboundsMinTime:            r.TimeboundsMinTime,
		TimeboundsMaxTime:            r.TimeboundsMaxTime,
		LedgerboundsMin:              r.LedgerboundsMin,
		LedgerboundsMax:              r.LedgerboundsMax,
		MinSequenceNumber:            r.MinSequenceNumber,
		MinSequenceAge:               r.MinSequenceAge,
		SorobanResourcesInstructions: r.SorobanResourcesInstructions,
		SorobanResourcesReadBytes:    r.SorobanResourcesReadBytes,
		SorobanResourcesWriteBytes:   r.SorobanResourcesWriteBytes,
		SorobanFeeBase:               r.SorobanFeeBase,
		SorobanFeeResources:          r.SorobanFeeResources,
		SorobanFeeRefund:             r.SorobanFeeRefund,
		SorobanFeeCharged:            r.SorobanFeeCharged,
		SorobanFeeWasted:             r.SorobanFeeWasted,
		SorobanHostFunctionType:      r.SorobanHostFunctionType,
		SorobanContractId:            r.SorobanContractID,
		SorobanContractEventsCount:   int32Ptr(r.SorobanContractEventsCount),
		SignaturesCount:              int32(r.SignaturesCount),
		NewAccount:                   r.NewAccount,
		RentFeeCharged:               r.RentFeeCharged,
		TxEnvelope:                   r.TxEnvelope,
		TxResult:                     r.TxResult,
		TxMeta:                       r.TxMeta,
		TxFeeMeta:                    r.TxFeeMeta,
		TxSigners:                    r.TxSigners,
		ExtraSigners:                 r.ExtraSigners,
	}
}

func convertOperationRow(r *OperationRow) *pb.OperationRow {
	return &pb.OperationRow{
		TransactionHash:       r.TransactionHash,
		OperationIndex:        int32(r.OperationIndex),
		LedgerSequence:        r.LedgerSequence,
		SourceAccount:         r.SourceAccount,
		Type:                  int32(r.OpType),
		TypeString:            r.TypeString,
		CreatedAt:             r.CreatedAt.Format(time.RFC3339),
		TransactionSuccessful: r.TransactionSuccessful,
		OperationResultCode:   r.OperationResultCode,
		OperationTraceCode:    r.OperationTraceCode,
		LedgerRange:           r.LedgerRange,
		SourceAccountMuxed:    r.SourceAccountMuxed,
		Amount:                r.Amount,
		Asset:                 r.Asset,
		AssetType:             r.AssetType,
		AssetCode:             r.AssetCode,
		AssetIssuer:           r.AssetIssuer,
		Destination:           r.Destination,
		SourceAsset:           r.SourceAsset,
		SourceAssetType:       r.SourceAssetType,
		SourceAssetCode:       r.SourceAssetCode,
		SourceAssetIssuer:     r.SourceAssetIssuer,
		SourceAmount:          r.SourceAmount,
		DestinationMin:        r.DestinationMin,
		StartingBalance:       r.StartingBalance,
		OfferId:               r.OfferID,
		Price:                 r.Price,
		BuyingAssetCode:       r.BuyingAssetCode,
		BuyingAssetIssuer:     r.BuyingAssetIssuer,
		SellingAssetCode:      r.SellingAssetCode,
		SellingAssetIssuer:    r.SellingAssetIssuer,
		SorobanOperation:      r.SorobanOperation,
		SorobanFunction:       r.SorobanFunction,
		SorobanContractId:     r.SorobanContractID,
		SorobanArgumentsJson:  r.SorobanArgumentsJSON,
		ContractCallsJson:     r.ContractCallsJSON,
		MaxCallDepth:          int32Ptr(r.MaxCallDepth),
		ContractsInvolved:     r.ContractsInvolved,
		TransactionIndex:      int32(r.TransactionIndex),
	}
}

func convertEffectRow(r *EffectRow) *pb.EffectRow {
	return &pb.EffectRow{
		LedgerSequence:   r.LedgerSequence,
		TransactionHash:  r.TransactionHash,
		OperationIndex:   int32(r.OperationIndex),
		EffectIndex:      int32(r.EffectIndex),
		EffectType:       int32(r.EffectType),
		EffectTypeString: r.EffectTypeString,
		AccountId:        r.AccountID,
		Amount:           r.Amount,
		AssetCode:        r.AssetCode,
		AssetIssuer:      r.AssetIssuer,
		AssetType:        r.AssetType,
		CreatedAt:        r.CreatedAt.Format(time.RFC3339),
		LedgerRange:      r.LedgerRange,
	}
}

func convertTradeRow(r *TradeRow) *pb.TradeRow {
	return &pb.TradeRow{
		LedgerSequence:     r.LedgerSequence,
		TransactionHash:    r.TransactionHash,
		OperationIndex:     int32(r.OperationIndex),
		TradeIndex:         int32(r.TradeIndex),
		TradeType:          r.TradeType,
		TradeTimestamp:     r.TradeTimestamp.Format(time.RFC3339),
		SellerAccount:      r.SellerAccount,
		SellingAssetCode:   r.SellingAssetCode,
		SellingAssetIssuer: r.SellingAssetIssuer,
		SellingAmount:      r.SellingAmount,
		BuyerAccount:       r.BuyerAccount,
		BuyingAssetCode:    r.BuyingAssetCode,
		BuyingAssetIssuer:  r.BuyingAssetIssuer,
		BuyingAmount:       r.BuyingAmount,
		Price:              r.Price,
		LedgerRange:        r.LedgerRange,
	}
}

func convertAccountRow(r *AccountRow) *pb.AccountRow {
	return &pb.AccountRow{
		AccountId:      r.AccountID,
		LedgerSequence: r.LedgerSequence,
		ClosedAt:       r.ClosedAt.Format(time.RFC3339),
		Balance:        r.Balance,
		SequenceNumber: r.SequenceNumber,
		Flags:          r.Flags,
		HomeDomain:     r.HomeDomain,
		Signers:        r.Signers,
		SponsorAccount: r.SponsorAccount,
		LedgerRange:    r.LedgerRange,
	}
}

func convertTrustlineRow(r *TrustlineRow) *pb.TrustlineRow {
	return &pb.TrustlineRow{
		AccountId:      r.AccountID,
		AssetCode:      r.AssetCode,
		AssetIssuer:    r.AssetIssuer,
		AssetType:      r.AssetType,
		Balance:        r.Balance,
		TrustLimit:     r.TrustLimit,
		Authorized:     r.Authorized,
		LedgerSequence: r.LedgerSequence,
		LedgerRange:    r.LedgerRange,
	}
}

func convertOfferRow(r *OfferRow) *pb.OfferRow {
	return &pb.OfferRow{
		OfferId:            r.OfferID,
		SellerAccount:      r.SellerAccount,
		LedgerSequence:     r.LedgerSequence,
		ClosedAt:           r.ClosedAt.Format(time.RFC3339),
		SellingAssetType:   r.SellingAssetType,
		SellingAssetCode:   r.SellingAssetCode,
		SellingAssetIssuer: r.SellingAssetIssuer,
		BuyingAssetType:    r.BuyingAssetType,
		BuyingAssetCode:    r.BuyingAssetCode,
		BuyingAssetIssuer:  r.BuyingAssetIssuer,
		Amount:             r.Amount,
		Price:              r.Price,
		Flags:              r.Flags,
		LedgerRange:        r.LedgerRange,
	}
}

func convertAccountSignerRow(r *AccountSignerRow) *pb.AccountSignerRow {
	return &pb.AccountSignerRow{
		AccountId:      r.AccountID,
		Signer:         r.Signer,
		LedgerSequence: r.LedgerSequence,
		Weight:         r.Weight,
		Sponsor:        r.Sponsor,
		Deleted:        r.Deleted,
		ClosedAt:       r.ClosedAt.Format(time.RFC3339),
		LedgerRange:    r.LedgerRange,
	}
}

func convertClaimableBalanceRow(r *ClaimableBalanceRow) *pb.ClaimableBalanceRow {
	return &pb.ClaimableBalanceRow{
		BalanceId:      r.BalanceID,
		Sponsor:        r.Sponsor,
		LedgerSequence: r.LedgerSequence,
		ClosedAt:       r.ClosedAt.Format(time.RFC3339),
		AssetType:      r.AssetType,
		AssetCode:      r.AssetCode,
		AssetIssuer:    r.AssetIssuer,
		Amount:         r.Amount,
		ClaimantsCount: int32(r.ClaimantsCount),
		LedgerRange:    r.LedgerRange,
	}
}

func convertLiquidityPoolRow(r *LiquidityPoolRow) *pb.LiquidityPoolRow {
	return &pb.LiquidityPoolRow{
		LiquidityPoolId: r.LiquidityPoolID,
		LedgerSequence:  r.LedgerSequence,
		ClosedAt:        r.ClosedAt.Format(time.RFC3339),
		PoolType:        r.PoolType,
		Fee:             int32(r.Fee),
		TotalPoolShares: r.TotalPoolShares,
		AssetAType:      r.AssetAType,
		AssetACode:      r.AssetACode,
		AssetAIssuer:    r.AssetAIssuer,
		AssetAAmount:    r.AssetAAmount,
		AssetBType:      r.AssetBType,
		AssetBCode:      r.AssetBCode,
		AssetBIssuer:    r.AssetBIssuer,
		AssetBAmount:    r.AssetBAmount,
		LedgerRange:     r.LedgerRange,
	}
}

func convertConfigSettingRow(r *ConfigSettingRow) *pb.ConfigSettingRow {
	return &pb.ConfigSettingRow{
		ConfigSettingId:    int32(r.ConfigSettingID),
		LedgerSequence:    r.LedgerSequence,
		LastModifiedLedger: int32(r.LastModifiedLedger),
		Deleted:            r.Deleted,
		ClosedAt:           r.ClosedAt.Format(time.RFC3339),
		ConfigSettingXdr:   r.ConfigSettingXDR,
		LedgerRange:        r.LedgerRange,
	}
}

func convertTTLRow(r *TTLRow) *pb.TTLRow {
	return &pb.TTLRow{
		KeyHash:            r.KeyHash,
		LedgerSequence:     r.LedgerSequence,
		LiveUntilLedgerSeq: r.LiveUntilLedgerSeq,
		TtlRemaining:       r.TTLRemaining,
		Expired:            r.Expired,
		LastModifiedLedger: int32(r.LastModifiedLedger),
		Deleted:            r.Deleted,
		ClosedAt:           r.ClosedAt.Format(time.RFC3339),
		LedgerRange:        r.LedgerRange,
	}
}

func convertEvictedKeyRow(r *EvictedKeyRow) *pb.EvictedKeyRow {
	return &pb.EvictedKeyRow{
		KeyHash:        r.KeyHash,
		LedgerSequence: r.LedgerSequence,
		ContractId:     r.ContractID,
		KeyType:        r.KeyType,
		Durability:     r.Durability,
		ClosedAt:       r.ClosedAt.Format(time.RFC3339),
		LedgerRange:    r.LedgerRange,
	}
}

func convertRestoredKeyRow(r *RestoredKeyRow) *pb.RestoredKeyRow {
	return &pb.RestoredKeyRow{
		KeyHash:            r.KeyHash,
		LedgerSequence:     r.LedgerSequence,
		ContractId:         r.ContractID,
		KeyType:            r.KeyType,
		Durability:         r.Durability,
		RestoredFromLedger: r.RestoredFromLedger,
		ClosedAt:           r.ClosedAt.Format(time.RFC3339),
		LedgerRange:        r.LedgerRange,
	}
}

func convertContractEventRow(r *ContractEventRow) *pb.ContractEventRow {
	return &pb.ContractEventRow{
		EventId:                  r.EventID,
		ContractId:               r.ContractID,
		LedgerSequence:           r.LedgerSequence,
		TransactionHash:          r.TransactionHash,
		ClosedAt:                 r.ClosedAt.Format(time.RFC3339),
		EventType:                r.EventType,
		InSuccessfulContractCall: r.InSuccessfulContractCall,
		TopicsJson:               r.TopicsJSON,
		TopicsDecoded:            r.TopicsDecoded,
		DataXdr:                  r.DataXDR,
		DataDecoded:              r.DataDecoded,
		TopicCount:               int32(r.TopicCount),
		OperationIndex:           int32(r.OperationIndex),
		EventIndex:               int32(r.EventIndex),
		Topic0Decoded:            r.Topic0Decoded,
		Topic1Decoded:            r.Topic1Decoded,
		Topic2Decoded:            r.Topic2Decoded,
		Topic3Decoded:            r.Topic3Decoded,
		LedgerRange:              r.LedgerRange,
	}
}

func convertContractDataRow(r *ContractDataRow) *pb.ContractDataRow {
	return &pb.ContractDataRow{
		ContractId:         r.ContractID,
		LedgerSequence:     r.LedgerSequence,
		LedgerKeyHash:      r.LedgerKeyHash,
		ContractKeyType:    r.ContractKeyType,
		ContractDurability: r.ContractDurability,
		AssetCode:          r.AssetCode,
		AssetIssuer:        r.AssetIssuer,
		AssetType:          r.AssetType,
		BalanceHolder:      r.BalanceHolder,
		Balance:            r.Balance,
		LastModifiedLedger: int32(r.LastModifiedLedger),
		LedgerEntryChange:  int32(r.LedgerEntryChange),
		Deleted:            r.Deleted,
		ClosedAt:           r.ClosedAt.Format(time.RFC3339),
		ContractDataXdr:    r.ContractDataXDR,
		TokenName:          r.TokenName,
		TokenSymbol:        r.TokenSymbol,
		TokenDecimals:      int32Ptr(r.TokenDecimals),
		LedgerRange:        r.LedgerRange,
	}
}

func convertContractCodeRow(r *ContractCodeRow) *pb.ContractCodeRow {
	return &pb.ContractCodeRow{
		ContractCodeHash:   r.ContractCodeHash,
		LedgerKeyHash:      r.LedgerKeyHash,
		ContractCodeExtV:   int32(r.ContractCodeExtV),
		LastModifiedLedger: int32(r.LastModifiedLedger),
		LedgerEntryChange:  int32(r.LedgerEntryChange),
		Deleted:            r.Deleted,
		ClosedAt:           r.ClosedAt.Format(time.RFC3339),
		LedgerSequence:     r.LedgerSequence,
		NInstructions:      r.NInstructions,
		NFunctions:         r.NFunctions,
		LedgerRange:        r.LedgerRange,
	}
}

func convertNativeBalanceRow(r *NativeBalanceRow) *pb.NativeBalanceRow {
	return &pb.NativeBalanceRow{
		AccountId:          r.AccountID,
		Balance:            r.Balance,
		BuyingLiabilities:  r.BuyingLiabilities,
		SellingLiabilities: r.SellingLiabilities,
		SequenceNumber:     r.SequenceNumber,
		LastModifiedLedger: r.LastModifiedLedger,
		LedgerSequence:     r.LedgerSequence,
		LedgerRange:        r.LedgerRange,
	}
}

func convertContractCreationRow(r *ContractCreationRow) *pb.ContractCreationRow {
	return &pb.ContractCreationRow{
		ContractId:     r.ContractID,
		CreatorAddress: r.CreatorAddress,
		WasmHash:       r.WasmHash,
		CreatedLedger:  r.CreatedLedger,
		CreatedAt:      r.CreatedAt.Format(time.RFC3339),
		LedgerRange:    r.LedgerRange,
	}
}
