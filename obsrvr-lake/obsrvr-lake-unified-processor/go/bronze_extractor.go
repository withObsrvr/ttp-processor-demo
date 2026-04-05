package main

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/big"
	"strconv"
	"strings"
	"time"

	"github.com/stellar/go-stellar-sdk/ingest"
	"github.com/stellar/go-stellar-sdk/ingest/sac"
	"github.com/stellar/go-stellar-sdk/strkey"
	"github.com/stellar/go-stellar-sdk/xdr"
)

// Ensure math/big is used (for SAC balance detection)
var _ = (*big.Int)(nil)

// EraID identifies this processor in era_id columns.
var EraID = "unified-processor"

// VersionLabel identifies this processor build in version_label columns.
var VersionLabel = "v1.0.0-dev"

// extractAllBronze extracts all bronze data from a single ledger's XDR.
func extractAllBronze(meta LedgerMeta, networkPassphrase string) (*BronzeData, error) {
	lcm := meta.LCM
	seq := meta.LedgerSequence
	closedAt := meta.ClosedAt
	ledgerRange := meta.LedgerRange

	data := &BronzeData{}

	// --- Ledger (always 1 row) ---
	ledgerRow, err := extractLedgerRow(lcm, seq, closedAt, ledgerRange)
	if err != nil {
		return nil, fmt.Errorf("extract ledger: %w", err)
	}
	data.Ledger = ledgerRow

	// --- Transactions ---
	txs, err := extractTransactionRows(lcm, networkPassphrase, seq, closedAt, ledgerRange)
	if err != nil {
		return nil, fmt.Errorf("extract transactions: %w", err)
	}
	data.Transactions = txs

	// --- Operations ---
	ops, err := extractOperationRows(lcm, networkPassphrase, seq, closedAt, ledgerRange)
	if err != nil {
		return nil, fmt.Errorf("extract operations: %w", err)
	}
	data.Operations = ops

	// --- Effects ---
	effs, err := extractEffectRows(lcm, networkPassphrase, seq, closedAt, ledgerRange)
	if err != nil {
		return nil, fmt.Errorf("extract effects: %w", err)
	}
	data.Effects = effs

	// --- Trades ---
	trades, err := extractTradeRows(lcm, networkPassphrase, seq, closedAt, ledgerRange)
	if err != nil {
		log.Printf("Warning: extract trades: %v", err)
	}
	data.Trades = trades

	// --- Accounts (state changes) ---
	accounts, err := extractAccountRows(lcm, networkPassphrase, seq, closedAt, ledgerRange)
	if err != nil {
		log.Printf("Warning: extract accounts: %v", err)
	}
	data.Accounts = accounts

	// --- Trustlines ---
	trustlines, err := extractTrustlineRows(lcm, networkPassphrase, seq, closedAt, ledgerRange)
	if err != nil {
		log.Printf("Warning: extract trustlines: %v", err)
	}
	data.Trustlines = trustlines

	// --- Offers ---
	offers, err := extractOfferRows(lcm, networkPassphrase, seq, closedAt, ledgerRange)
	if err != nil {
		log.Printf("Warning: extract offers: %v", err)
	}
	data.Offers = offers

	// --- Account Signers ---
	signers, err := extractAccountSignerRows(lcm, networkPassphrase, seq, closedAt, ledgerRange)
	if err != nil {
		log.Printf("Warning: extract account signers: %v", err)
	}
	data.AccountSigners = signers

	// --- Claimable Balances ---
	claimableBalances, err := extractClaimableBalanceRows(lcm, networkPassphrase, seq, closedAt, ledgerRange)
	if err != nil {
		log.Printf("Warning: extract claimable balances: %v", err)
	}
	data.ClaimableBalances = claimableBalances

	// --- Liquidity Pools ---
	liquidityPools, err := extractLiquidityPoolRows(lcm, networkPassphrase, seq, closedAt, ledgerRange)
	if err != nil {
		log.Printf("Warning: extract liquidity pools: %v", err)
	}
	data.LiquidityPools = liquidityPools

	// --- Config Settings ---
	configSettings, err := extractConfigSettingRows(lcm, networkPassphrase, seq, closedAt, ledgerRange)
	if err != nil {
		log.Printf("Warning: extract config settings: %v", err)
	}
	data.ConfigSettings = configSettings

	// --- TTL ---
	ttlEntries, err := extractTTLRows(lcm, networkPassphrase, seq, closedAt, ledgerRange)
	if err != nil {
		log.Printf("Warning: extract TTL: %v", err)
	}
	data.TTLEntries = ttlEntries

	// --- Evicted Keys ---
	evictedKeys, err := extractEvictedKeyRows(lcm, seq, closedAt, ledgerRange)
	if err != nil {
		log.Printf("Warning: extract evicted keys: %v", err)
	}
	data.EvictedKeys = evictedKeys

	// --- Restored Keys ---
	restoredKeys, err := extractRestoredKeyRows(lcm, networkPassphrase, seq, closedAt, ledgerRange)
	if err != nil {
		log.Printf("Warning: extract restored keys: %v", err)
	}
	data.RestoredKeys = restoredKeys

	// --- Contract Events ---
	contractEvents, err := extractContractEventRows(lcm, networkPassphrase, seq, closedAt, ledgerRange)
	if err != nil {
		log.Printf("Warning: extract contract events: %v", err)
	}
	data.ContractEvents = contractEvents

	// --- Contract Data ---
	contractData, err := extractContractDataRows(lcm, networkPassphrase, seq, closedAt, ledgerRange)
	if err != nil {
		log.Printf("Warning: extract contract data: %v", err)
	}
	data.ContractData = contractData

	// --- Contract Code ---
	contractCode, err := extractContractCodeRows(lcm, networkPassphrase, seq, closedAt, ledgerRange)
	if err != nil {
		log.Printf("Warning: extract contract code: %v", err)
	}
	data.ContractCode = contractCode

	// --- Native Balances ---
	nativeBalances, err := extractNativeBalanceRows(lcm, networkPassphrase, seq, closedAt, ledgerRange)
	if err != nil {
		log.Printf("Warning: extract native balances: %v", err)
	}
	data.NativeBalances = nativeBalances

	// --- Contract Creations ---
	contractCreations, err := extractContractCreationRows(lcm, networkPassphrase, seq, closedAt, ledgerRange)
	if err != nil {
		log.Printf("Warning: extract contract creations: %v", err)
	}
	data.ContractCreations = contractCreations

	return data, nil
}

// ---------------------------------------------------------------------------
// Ledger extraction
// ---------------------------------------------------------------------------

func extractLedgerRow(lcm xdr.LedgerCloseMeta, ledgerSeq uint32, closedAt time.Time, ledgerRange uint32) (*LedgerRow, error) {
	var header xdr.LedgerHeaderHistoryEntry
	switch lcm.V {
	case 0:
		header = lcm.MustV0().LedgerHeader
	case 1:
		header = lcm.MustV1().LedgerHeader
	case 2:
		header = lcm.MustV2().LedgerHeader
	default:
		return nil, fmt.Errorf("unknown LedgerCloseMeta version: %d", lcm.V)
	}

	row := &LedgerRow{
		Sequence:           ledgerSeq,
		LedgerHash:         hex.EncodeToString(header.Hash[:]),
		PreviousLedgerHash: hex.EncodeToString(header.Header.PreviousLedgerHash[:]),
		ClosedAt:           closedAt,
		ProtocolVersion:    int(header.Header.LedgerVersion),
		TotalCoins:         int64(header.Header.TotalCoins),
		FeePool:            int64(header.Header.FeePool),
		BaseFee:            int(header.Header.BaseFee),
		BaseReserve:        int(header.Header.BaseReserve),
		MaxTxSetSize:       int(header.Header.MaxTxSetSize),
		IngestionTimestamp:  time.Now().UTC(),
		LedgerRange:        ledgerRange,
		EraID:              EraID,
		VersionLabel:       VersionLabel,
	}

	// Count transactions and operations
	var txCount, failedCount, operationCount, txSetOperationCount int
	switch lcm.V {
	case 0:
		v0 := lcm.MustV0()
		txCount = len(v0.TxSet.Txs)
		for _, tx := range v0.TxSet.Txs {
			opCount := len(tx.Operations())
			txSetOperationCount += opCount
			operationCount += opCount
		}
	case 1:
		v1 := lcm.MustV1()
		txCount = len(v1.TxProcessing)
		for _, txApply := range v1.TxProcessing {
			if opResults, ok := txApply.Result.Result.OperationResults(); ok {
				opCount := len(opResults)
				txSetOperationCount += opCount
				if txApply.Result.Result.Successful() {
					operationCount += opCount
				} else {
					failedCount++
				}
			} else {
				failedCount++
			}
		}
	case 2:
		v2 := lcm.MustV2()
		txCount = len(v2.TxProcessing)
		for _, txApply := range v2.TxProcessing {
			if opResults, ok := txApply.Result.Result.OperationResults(); ok {
				opCount := len(opResults)
				txSetOperationCount += opCount
				if txApply.Result.Result.Successful() {
					operationCount += opCount
				} else {
					failedCount++
				}
			} else {
				failedCount++
			}
		}
	}

	row.TransactionCount = txCount
	row.SuccessfulTxCount = txCount - failedCount
	row.FailedTxCount = failedCount
	row.OperationCount = operationCount
	row.TxSetOperationCount = txSetOperationCount

	// --- V3 ledger aggregates ---

	// Compute per-ledger aggregates from TxProcessing
	var totalFeeCharged int64
	var contractEventsCount int
	var sorobanOpCount int

	switch lcm.V {
	case 1:
		for _, txApply := range lcm.MustV1().TxProcessing {
			totalFeeCharged += int64(txApply.Result.Result.FeeCharged)
			contractEventsCount += countContractEventsFromMeta(&txApply.TxApplyProcessing)
			// Count Soroban operations
			if opResults, ok := txApply.Result.Result.OperationResults(); ok {
				for _, opResult := range opResults {
					if opResult.Code == xdr.OperationResultCodeOpInner {
						tr := opResult.MustTr()
						switch tr.Type {
						case xdr.OperationTypeInvokeHostFunction, xdr.OperationTypeExtendFootprintTtl, xdr.OperationTypeRestoreFootprint:
							sorobanOpCount++
						}
					}
				}
			}
		}
	case 2:
		for _, txApply := range lcm.MustV2().TxProcessing {
			totalFeeCharged += int64(txApply.Result.Result.FeeCharged)
			contractEventsCount += countContractEventsFromMeta(&txApply.TxApplyProcessing)
			if opResults, ok := txApply.Result.Result.OperationResults(); ok {
				for _, opResult := range opResults {
					if opResult.Code == xdr.OperationResultCodeOpInner {
						tr := opResult.MustTr()
						switch tr.Type {
						case xdr.OperationTypeInvokeHostFunction, xdr.OperationTypeExtendFootprintTtl, xdr.OperationTypeRestoreFootprint:
							sorobanOpCount++
						}
					}
				}
			}
		}
	}

	row.TotalFeeCharged = &totalFeeCharged
	row.ContractEventsCount = &contractEventsCount
	row.SorobanOpCount = &sorobanOpCount

	// Protocol 20+ Soroban fields
	if lcmV1, ok := lcm.GetV1(); ok {
		if extV1, ok := lcmV1.Ext.GetV1(); ok {
			feeWrite := int64(extV1.SorobanFeeWrite1Kb)
			row.SorobanFeeWrite1KB = &feeWrite
		}
	} else if lcmV2, ok := lcm.GetV2(); ok {
		if extV1, ok := lcmV2.Ext.GetV1(); ok {
			feeWrite := int64(extV1.SorobanFeeWrite1Kb)
			row.SorobanFeeWrite1KB = &feeWrite
		}
	}

	// Node ID and signature (from SCP value)
	if lcValueSig, ok := header.Header.ScpValue.Ext.GetLcValueSignature(); ok {
		nodeIDStr := base64.StdEncoding.EncodeToString(lcValueSig.NodeId.Ed25519[:])
		row.NodeID = &nodeIDStr

		sigStr := base64.StdEncoding.EncodeToString(lcValueSig.Signature[:])
		row.Signature = &sigStr
	}

	// Ledger header XDR (base64 encoded)
	headerXDR, err := header.Header.MarshalBinary()
	if err == nil {
		headerStr := base64.StdEncoding.EncodeToString(headerXDR)
		row.LedgerHeader = &headerStr
	}

	// Bucket list size and live Soroban state size (Protocol 20+)
	if lcmV1, ok := lcm.GetV1(); ok {
		sorobanStateSize := int64(lcmV1.TotalByteSizeOfLiveSorobanState)
		row.BucketListSize = &sorobanStateSize
		row.LiveSorobanStateSize = &sorobanStateSize
	} else if lcmV2, ok := lcm.GetV2(); ok {
		sorobanStateSize := int64(lcmV2.TotalByteSizeOfLiveSorobanState)
		row.BucketListSize = &sorobanStateSize
		row.LiveSorobanStateSize = &sorobanStateSize
	}

	// Protocol 23+ Hot Archive fields (evicted keys count)
	if lcmV1, ok := lcm.GetV1(); ok {
		evicted := len(lcmV1.EvictedKeys)
		row.EvictedKeysCount = &evicted
	} else if lcmV2, ok := lcm.GetV2(); ok {
		evicted := len(lcmV2.EvictedKeys)
		row.EvictedKeysCount = &evicted
	}

	return row, nil
}

// countContractEventsFromMeta counts contract events from a TransactionMeta.
// Handles V3 (SorobanMeta.Events) and V4 (per-operation events from CAP-67).
func countContractEventsFromMeta(meta *xdr.TransactionMeta) int {
	switch meta.V {
	case 3:
		v3 := meta.MustV3()
		if v3.SorobanMeta != nil {
			return len(v3.SorobanMeta.Events)
		}
	case 4:
		v4 := meta.MustV4()
		count := 0
		for _, op := range v4.Operations {
			count += len(op.Events)
		}
		return count
	}
	return 0
}

// ---------------------------------------------------------------------------
// Transaction extraction
// ---------------------------------------------------------------------------

func extractTransactionRows(lcm xdr.LedgerCloseMeta, networkPassphrase string, ledgerSeq uint32, closedAt time.Time, ledgerRange uint32) ([]TransactionRow, error) {
	var transactions []TransactionRow

	reader, err := ingest.NewLedgerTransactionReaderFromLedgerCloseMeta(networkPassphrase, lcm)
	if err != nil {
		return nil, fmt.Errorf("create transaction reader: %w", err)
	}
	defer reader.Close()

	for {
		tx, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Printf("Error reading transaction in ledger %d: %v", ledgerSeq, err)
			continue
		}

		txRow := TransactionRow{
			LedgerSequence:        ledgerSeq,
			TransactionHash:       hex.EncodeToString(tx.Result.TransactionHash[:]),
			SourceAccount:         tx.Envelope.SourceAccount().ToAccountId().Address(),
			SourceAccountMuxed:    getMuxedAddr(tx.Envelope.SourceAccount()),
			FeeCharged:            int64(tx.Result.Result.FeeCharged),
			MaxFee:                int64(tx.Envelope.Fee()),
			Successful:            tx.Result.Successful(),
			TransactionResultCode: tx.Result.Result.Result.Code.String(),
			OperationCount:        len(tx.Envelope.Operations()),
			CreatedAt:             closedAt,
			AccountSequence:       int64(tx.Envelope.SeqNum()),
			LedgerRange:           ledgerRange,
			SignaturesCount:       len(tx.Envelope.Signatures()),
			NewAccount:            false,
			EraID:                 EraID,
			VersionLabel:          VersionLabel,
		}

		// Timebounds
		if tb := tx.Envelope.TimeBounds(); tb != nil {
			minTime := int64(tb.MinTime)
			txRow.TimeboundsMinTime = &minTime
			maxTime := int64(tb.MaxTime)
			txRow.TimeboundsMaxTime = &maxTime
		}

		// Preconditions (ledger bounds, min sequence)
		precond := tx.Envelope.Preconditions()
		if precond.Type == xdr.PreconditionTypePrecondV2 {
			v2 := precond.MustV2()
			if v2.LedgerBounds != nil {
				lbMin := int64(v2.LedgerBounds.MinLedger)
				txRow.LedgerboundsMin = &lbMin
				lbMax := int64(v2.LedgerBounds.MaxLedger)
				txRow.LedgerboundsMax = &lbMax
			}
			if v2.MinSeqNum != nil {
				msn := int64(*v2.MinSeqNum)
				txRow.MinSequenceNumber = &msn
			}
			minAge := int64(v2.MinSeqAge)
			txRow.MinSequenceAge = &minAge

			// Extra signers from preconditions
			if len(v2.ExtraSigners) > 0 {
				var signerStrs []string
				for _, signer := range v2.ExtraSigners {
					signerBytes, err := signer.MarshalBinary()
					if err == nil {
						signerStrs = append(signerStrs, base64.StdEncoding.EncodeToString(signerBytes))
					}
				}
				if len(signerStrs) > 0 {
					joined := strings.Join(signerStrs, ",")
					txRow.ExtraSigners = &joined
				}
			}
		}

		// Fee bump transaction fields
		if tx.Envelope.IsFeeBump() {
			feeBumpFee := int64(tx.Envelope.FeeBumpFee())
			txRow.FeeBumpFee = &feeBumpFee
			// MaxFeeBid = fee bump fee / (num ops + 1)
			opCount := int64(len(tx.Envelope.Operations()))
			if opCount > 0 {
				maxFeeBid := feeBumpFee / (opCount + 1)
				txRow.MaxFeeBid = &maxFeeBid
			}
			if innerHash, ok := tx.InnerTransactionHash(); ok {
				txRow.InnerTransactionHash = &innerHash
			}
			// Inner source account from fee bump inner transaction
			innerTx := tx.Envelope.FeeBump.Tx.InnerTx
			if innerTx.Type == xdr.EnvelopeTypeEnvelopeTypeTx {
				innerSourceAddr := innerTx.V1.Tx.SourceAccount.ToAccountId().Address()
				txRow.InnerSourceAccount = &innerSourceAddr
			}
			feeSource := tx.Envelope.FeeBumpAccount()
			feeAccountMuxed := getMuxedAddr(feeSource)
			txRow.FeeAccountMuxed = feeAccountMuxed
		}

		// Tx signers (base64 encoded signatures)
		sigs := tx.Envelope.Signatures()
		if len(sigs) > 0 {
			var sigStrs []string
			for _, sig := range sigs {
				sigStrs = append(sigStrs, base64.StdEncoding.EncodeToString(sig.Signature))
			}
			joined := strings.Join(sigStrs, ",")
			txRow.TxSigners = &joined
		}

		// Soroban host function type
		for _, op := range tx.Envelope.Operations() {
			if op.Body.Type == xdr.OperationTypeInvokeHostFunction {
				if invokeOp, ok := op.Body.GetInvokeHostFunctionOp(); ok {
					fnType := invokeOp.HostFunction.Type.String()
					txRow.SorobanHostFunctionType = &fnType
					if invokeOp.HostFunction.Type == xdr.HostFunctionTypeHostFunctionTypeInvokeContract && invokeOp.HostFunction.InvokeContract != nil {
						contractIDStr, cErr := invokeOp.HostFunction.InvokeContract.ContractAddress.String()
						if cErr == nil && contractIDStr != "" {
							txRow.SorobanContractID = &contractIDStr
						}
					}
				}
				break
			}
		}

		// Memo
		memo := tx.Envelope.Memo()
		switch memo.Type {
		case xdr.MemoTypeMemoNone:
			memoType := "none"
			txRow.MemoType = &memoType
		case xdr.MemoTypeMemoText:
			memoType := "text"
			txRow.MemoType = &memoType
			if text, ok := memo.GetText(); ok {
				txRow.Memo = &text
			}
		case xdr.MemoTypeMemoId:
			memoType := "id"
			txRow.MemoType = &memoType
			if id, ok := memo.GetId(); ok {
				memoStr := fmt.Sprintf("%d", id)
				txRow.Memo = &memoStr
			}
		case xdr.MemoTypeMemoHash:
			memoType := "hash"
			txRow.MemoType = &memoType
			if hash, ok := memo.GetHash(); ok {
				memoStr := hex.EncodeToString(hash[:])
				txRow.Memo = &memoStr
			}
		case xdr.MemoTypeMemoReturn:
			memoType := "return"
			txRow.MemoType = &memoType
			if ret, ok := memo.GetRetHash(); ok {
				memoStr := hex.EncodeToString(ret[:])
				txRow.Memo = &memoStr
			}
		}

		// Check for CREATE_ACCOUNT
		for _, op := range tx.Envelope.Operations() {
			if op.Body.Type == xdr.OperationTypeCreateAccount {
				txRow.NewAccount = true
				break
			}
		}

		// Soroban rent fee charged
		if tx.UnsafeMeta.V == 3 {
			v3 := tx.UnsafeMeta.MustV3()
			if v3.SorobanMeta != nil && v3.SorobanMeta.Ext.V == 1 && v3.SorobanMeta.Ext.V1 != nil {
				rentFee := int64(v3.SorobanMeta.Ext.V1.RentFeeCharged)
				txRow.RentFeeCharged = &rentFee
			}
		}

		// Soroban resources
		if instructions, ok := tx.SorobanResourcesInstructions(); ok {
			val := int64(instructions)
			txRow.SorobanResourcesInstructions = &val
		}
		if readBytes, ok := tx.SorobanResourcesDiskReadBytes(); ok {
			val := int64(readBytes)
			txRow.SorobanResourcesReadBytes = &val
		}
		if writeBytes, ok := tx.SorobanResourcesWriteBytes(); ok {
			val := int64(writeBytes)
			txRow.SorobanResourcesWriteBytes = &val
		}

		// Soroban data size and resources from envelope
		if sorobanData, ok := tx.GetSorobanData(); ok {
			if resBytes, err := sorobanData.Resources.MarshalBinary(); err == nil {
				size := len(resBytes)
				txRow.SorobanDataSizeBytes = &size
				resStr := base64.StdEncoding.EncodeToString(resBytes)
				txRow.SorobanDataResources = &resStr
			}
			// Soroban fee fields
			resourceFee := int64(sorobanData.ResourceFee)
			txRow.SorobanFeeResources = &resourceFee
		}

		// Soroban fee charged / wasted / refund from meta
		if tx.UnsafeMeta.V == 3 {
			v3 := tx.UnsafeMeta.MustV3()
			if v3.SorobanMeta != nil {
				// Count contract events for this transaction
				evtCount := len(v3.SorobanMeta.Events)
				txRow.SorobanContractEventsCount = &evtCount

				if v3.SorobanMeta.Ext.V == 1 && v3.SorobanMeta.Ext.V1 != nil {
					// Fee charged, refund, and wasted
					totalNonRefundable := int64(v3.SorobanMeta.Ext.V1.TotalNonRefundableResourceFeeCharged)
					totalRefundable := int64(v3.SorobanMeta.Ext.V1.TotalRefundableResourceFeeCharged)
					feeCharged := totalNonRefundable + totalRefundable
					txRow.SorobanFeeCharged = &feeCharged

					refund := int64(v3.SorobanMeta.Ext.V1.RentFeeCharged)
					_ = refund // rent fee already captured above

					// Base fee = max_fee - resource_fee
					if txRow.SorobanFeeResources != nil {
						baseFee := int64(tx.Envelope.Fee()) - *txRow.SorobanFeeResources
						txRow.SorobanFeeBase = &baseFee
					}

					// Fee refund = resource_fee - charged
					if txRow.SorobanFeeResources != nil {
						feeRefund := *txRow.SorobanFeeResources - feeCharged
						if feeRefund < 0 {
							feeRefund = 0
						}
						txRow.SorobanFeeRefund = &feeRefund
						// Wasted = refund (unused resource allocation)
						txRow.SorobanFeeWasted = &feeRefund
					}
				}
			}
		}

		// Raw XDR storage (base64-encoded)
		if envBytes, err := tx.Envelope.MarshalBinary(); err == nil {
			s := base64.StdEncoding.EncodeToString(envBytes)
			txRow.TxEnvelope = &s
		}
		if resBytes, err := tx.Result.MarshalBinary(); err == nil {
			s := base64.StdEncoding.EncodeToString(resBytes)
			txRow.TxResult = &s
		}
		if metaBytes, err := tx.UnsafeMeta.MarshalBinary(); err == nil {
			s := base64.StdEncoding.EncodeToString(metaBytes)
			txRow.TxMeta = &s
		}

		transactions = append(transactions, txRow)
	}

	return transactions, nil
}

// ---------------------------------------------------------------------------
// Operation extraction
// ---------------------------------------------------------------------------

func extractOperationRows(lcm xdr.LedgerCloseMeta, networkPassphrase string, ledgerSeq uint32, closedAt time.Time, ledgerRange uint32) ([]OperationRow, error) {
	var operations []OperationRow

	reader, err := ingest.NewLedgerTransactionReaderFromLedgerCloseMeta(networkPassphrase, lcm)
	if err != nil {
		return nil, fmt.Errorf("create transaction reader: %w", err)
	}
	defer reader.Close()

	txIndex := 0
	for {
		tx, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Printf("Error reading transaction for operations in ledger %d: %v", ledgerSeq, err)
			continue
		}

		txHash := hex.EncodeToString(tx.Result.TransactionHash[:])
		txSuccessful := tx.Result.Successful()

		for i, op := range tx.Envelope.Operations() {
			sourceAccount := tx.Envelope.SourceAccount().ToAccountId().Address()
			var sourceAccountMuxed *string
			if op.SourceAccount != nil {
				sourceAccount = op.SourceAccount.ToAccountId().Address()
				sourceAccountMuxed = getMuxedAddr(*op.SourceAccount)
			} else {
				sourceAccountMuxed = getMuxedAddr(tx.Envelope.SourceAccount())
			}

			opRow := OperationRow{
				TransactionHash:       txHash,
				TransactionIndex:      txIndex,
				OperationIndex:        i,
				LedgerSequence:        ledgerSeq,
				SourceAccount:         sourceAccount,
				SourceAccountMuxed:    sourceAccountMuxed,
				OpType:                int(op.Body.Type),
				TypeString:            op.Body.Type.String(),
				CreatedAt:             closedAt,
				TransactionSuccessful: txSuccessful,
				LedgerRange:           ledgerRange,
				EraID:                 EraID,
				VersionLabel:          VersionLabel,
			}

			// Operation-specific field extraction
			switch op.Body.Type {
			case xdr.OperationTypeCreateAccount:
				if createAcct, ok := op.Body.GetCreateAccountOp(); ok {
					startBal := int64(createAcct.StartingBalance)
					opRow.StartingBalance = &startBal
					opRow.Amount = &startBal
					dest := createAcct.Destination.Address()
					opRow.Destination = &dest
				}

			case xdr.OperationTypePayment:
				if payment, ok := op.Body.GetPaymentOp(); ok {
					amt := int64(payment.Amount)
					opRow.Amount = &amt
					setOpAssetFields(&opRow, payment.Asset)
					dest := payment.Destination.ToAccountId().Address()
					opRow.Destination = &dest
				}

			case xdr.OperationTypePathPaymentStrictReceive:
				if pp, ok := op.Body.GetPathPaymentStrictReceiveOp(); ok {
					amt := int64(pp.DestAmount)
					opRow.Amount = &amt
					setOpAssetFields(&opRow, pp.DestAsset)
					srcAmt := int64(pp.SendMax)
					opRow.SourceAmount = &srcAmt
					setOpSourceAssetFields(&opRow, pp.SendAsset)
					dest := pp.Destination.ToAccountId().Address()
					opRow.Destination = &dest
				}

			case xdr.OperationTypeManageSellOffer:
				if sellOffer, ok := op.Body.GetManageSellOfferOp(); ok {
					amt := int64(sellOffer.Amount)
					opRow.Amount = &amt
					setOpSellingAssetFields(&opRow, sellOffer.Selling)
					setOpBuyingAssetFields(&opRow, sellOffer.Buying)
					offerID := int64(sellOffer.OfferId)
					opRow.OfferID = &offerID
					price := fmt.Sprintf("%d/%d", sellOffer.Price.N, sellOffer.Price.D)
					opRow.Price = &price
					priceR := fmt.Sprintf("{\"n\":%d,\"d\":%d}", sellOffer.Price.N, sellOffer.Price.D)
					opRow.PriceR = &priceR
				}

			case xdr.OperationTypeCreatePassiveSellOffer:
				if passiveOffer, ok := op.Body.GetCreatePassiveSellOfferOp(); ok {
					amt := int64(passiveOffer.Amount)
					opRow.Amount = &amt
					setOpSellingAssetFields(&opRow, passiveOffer.Selling)
					setOpBuyingAssetFields(&opRow, passiveOffer.Buying)
					price := fmt.Sprintf("%d/%d", passiveOffer.Price.N, passiveOffer.Price.D)
					opRow.Price = &price
					priceR := fmt.Sprintf("{\"n\":%d,\"d\":%d}", passiveOffer.Price.N, passiveOffer.Price.D)
					opRow.PriceR = &priceR
				}

			case xdr.OperationTypeSetOptions:
				if setOpts, ok := op.Body.GetSetOptionsOp(); ok {
					if setOpts.HomeDomain != nil {
						hd := string(*setOpts.HomeDomain)
						opRow.HomeDomain = &hd
					}
					if setOpts.SetFlags != nil {
						sf := int(*setOpts.SetFlags)
						opRow.SetFlags = &sf
					}
					if setOpts.ClearFlags != nil {
						cf := int(*setOpts.ClearFlags)
						opRow.ClearFlags = &cf
					}
					if setOpts.MasterWeight != nil {
						mw := int(*setOpts.MasterWeight)
						opRow.MasterWeight = &mw
					}
					if setOpts.LowThreshold != nil {
						lt := int(*setOpts.LowThreshold)
						opRow.LowThreshold = &lt
					}
					if setOpts.MedThreshold != nil {
						mt := int(*setOpts.MedThreshold)
						opRow.MediumThreshold = &mt
					}
					if setOpts.HighThreshold != nil {
						ht := int(*setOpts.HighThreshold)
						opRow.HighThreshold = &ht
					}
				}

			case xdr.OperationTypeChangeTrust:
				if changeTrust, ok := op.Body.GetChangeTrustOp(); ok {
					limit := int64(changeTrust.Limit)
					opRow.TrustlineLimit = &limit
					if changeTrust.Line.Type == xdr.AssetTypeAssetTypePoolShare {
						assetStr := "liquidity_pool_shares"
						opRow.Asset = &assetStr
						aType := "liquidity_pool_shares"
						opRow.AssetType = &aType
					} else {
						asset := changeTrust.Line.ToAsset()
						setOpAssetFields(&opRow, asset)
					}
				}

			case xdr.OperationTypeAllowTrust:
				if allowTrust, ok := op.Body.GetAllowTrustOp(); ok {
					dest := allowTrust.Trustor.Address()
					opRow.Destination = &dest
					opRow.Trustor = &dest
					var code string
					if ac4, ok := allowTrust.Asset.GetAssetCode4(); ok {
						code = strings.TrimRight(string(ac4[:]), "\x00")
					} else if ac12, ok := allowTrust.Asset.GetAssetCode12(); ok {
						code = strings.TrimRight(string(ac12[:]), "\x00")
					}
					if code != "" {
						opRow.AssetCode = &code
					}
					flags := int(allowTrust.Authorize)
					opRow.SetFlags = &flags
					opRow.TrustLineFlags = &flags
					authorized := (flags & int(xdr.TrustLineFlagsAuthorizedFlag)) != 0
					opRow.Authorize = &authorized
					authMaintain := (flags & int(xdr.TrustLineFlagsAuthorizedToMaintainLiabilitiesFlag)) != 0
					opRow.AuthorizeToMaintainLiabilities = &authMaintain
				}

			case xdr.OperationTypeAccountMerge:
				if destAccount, ok := op.Body.GetDestination(); ok {
					dest := destAccount.ToAccountId().Address()
					opRow.Destination = &dest
				}

			case xdr.OperationTypeManageData:
				if manageData, ok := op.Body.GetManageDataOp(); ok {
					name := string(manageData.DataName)
					opRow.DataName = &name
					if manageData.DataValue != nil {
						val := base64.StdEncoding.EncodeToString(*manageData.DataValue)
						opRow.DataValue = &val
					}
				}

			case xdr.OperationTypeBumpSequence:
				if bumpSeq, ok := op.Body.GetBumpSequenceOp(); ok {
					bumpTo := int64(bumpSeq.BumpTo)
					opRow.BumpTo = &bumpTo
				}

			case xdr.OperationTypeManageBuyOffer:
				if buyOffer, ok := op.Body.GetManageBuyOfferOp(); ok {
					amt := int64(buyOffer.BuyAmount)
					opRow.Amount = &amt
					setOpSellingAssetFields(&opRow, buyOffer.Selling)
					setOpBuyingAssetFields(&opRow, buyOffer.Buying)
					offerID := int64(buyOffer.OfferId)
					opRow.OfferID = &offerID
					price := fmt.Sprintf("%d/%d", buyOffer.Price.N, buyOffer.Price.D)
					opRow.Price = &price
					priceR := fmt.Sprintf("{\"n\":%d,\"d\":%d}", buyOffer.Price.N, buyOffer.Price.D)
					opRow.PriceR = &priceR
				}

			case xdr.OperationTypePathPaymentStrictSend:
				if pp, ok := op.Body.GetPathPaymentStrictSendOp(); ok {
					amt := int64(pp.SendAmount)
					opRow.Amount = &amt
					setOpSourceAssetFields(&opRow, pp.SendAsset)
					setOpAssetFields(&opRow, pp.DestAsset)
					destMin := int64(pp.DestMin)
					opRow.DestinationMin = &destMin
					dest := pp.Destination.ToAccountId().Address()
					opRow.Destination = &dest
				}

			case xdr.OperationTypeCreateClaimableBalance:
				if createCB, ok := op.Body.GetCreateClaimableBalanceOp(); ok {
					amt := int64(createCB.Amount)
					opRow.Amount = &amt
					setOpAssetFields(&opRow, createCB.Asset)
					claimantsCount := len(createCB.Claimants)
					opRow.ClaimantsCount = &claimantsCount
				}

			case xdr.OperationTypeClaimClaimableBalance:
				if claimCB, ok := op.Body.GetClaimClaimableBalanceOp(); ok {
					balanceID, mErr := xdr.MarshalHex(claimCB.BalanceId)
					if mErr == nil {
						opRow.BalanceID = &balanceID
					}
				}

			case xdr.OperationTypeBeginSponsoringFutureReserves:
				if beginSponsoring, ok := op.Body.GetBeginSponsoringFutureReservesOp(); ok {
					sponsored := beginSponsoring.SponsoredId.Address()
					opRow.SponsoredID = &sponsored
				}

			case xdr.OperationTypeClawback:
				if clawback, ok := op.Body.GetClawbackOp(); ok {
					amt := int64(clawback.Amount)
					opRow.Amount = &amt
					setOpAssetFields(&opRow, clawback.Asset)
					dest := clawback.From.ToAccountId().Address()
					opRow.Destination = &dest
				}

			case xdr.OperationTypeSetTrustLineFlags:
				if setTLFlags, ok := op.Body.GetSetTrustLineFlagsOp(); ok {
					dest := setTLFlags.Trustor.Address()
					opRow.Destination = &dest
					opRow.Trustor = &dest
					setOpAssetFields(&opRow, setTLFlags.Asset)
					sf := int(setTLFlags.SetFlags)
					opRow.SetFlags = &sf
					cf := int(setTLFlags.ClearFlags)
					opRow.ClearFlags = &cf
					combinedFlags := sf
					opRow.TrustLineFlags = &combinedFlags
				}

			case xdr.OperationTypeLiquidityPoolDeposit:
				if lpDeposit, ok := op.Body.GetLiquidityPoolDepositOp(); ok {
					amt := int64(lpDeposit.MaxAmountA)
					opRow.Amount = &amt
				}

			case xdr.OperationTypeLiquidityPoolWithdraw:
				if lpWithdraw, ok := op.Body.GetLiquidityPoolWithdrawOp(); ok {
					amt := int64(lpWithdraw.Amount)
					opRow.Amount = &amt
				}

			case xdr.OperationTypeInvokeHostFunction:
				if invokeOp, ok := op.Body.GetInvokeHostFunctionOp(); ok {
					fnType := invokeOp.HostFunction.Type.String()
					opRow.SorobanOperation = &fnType
					authRequired := len(invokeOp.Auth) > 0
					opRow.SorobanAuthRequired = &authRequired
				}
				contractID, functionName, argsJSON := extractContractInvocation(op)
				opRow.SorobanContractID = contractID
				opRow.SorobanFunction = functionName
				opRow.SorobanArgumentsJSON = argsJSON
			}

			// Operation result code
			if txSuccessful {
				if opResults, ok := tx.Result.Result.OperationResults(); ok {
					if i < len(opResults) {
						resultCode := opResults[i].Code.String()
						opRow.OperationResultCode = &resultCode
					}
				}
			}

			operations = append(operations, opRow)
		}
		txIndex++
	}

	return operations, nil
}

// ---------------------------------------------------------------------------
// Effect extraction (basic credit/debit from balance changes)
// ---------------------------------------------------------------------------

func extractEffectRows(lcm xdr.LedgerCloseMeta, networkPassphrase string, ledgerSeq uint32, closedAt time.Time, ledgerRange uint32) ([]EffectRow, error) {
	var effects []EffectRow

	reader, err := ingest.NewLedgerTransactionReaderFromLedgerCloseMeta(networkPassphrase, lcm)
	if err != nil {
		return nil, fmt.Errorf("create transaction reader: %w", err)
	}
	defer reader.Close()

	for {
		tx, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Printf("Error reading transaction for effects in ledger %d: %v", ledgerSeq, err)
			continue
		}

		txHash := hex.EncodeToString(tx.Result.TransactionHash[:])

		for opIdx := uint32(0); opIdx < tx.OperationCount(); opIdx++ {
			changes, err := tx.GetOperationChanges(opIdx)
			if err != nil {
				log.Printf("Error getting operation changes: %v", err)
				continue
			}

			effectIdx := 0
			for _, change := range changes {
				if change.Type == xdr.LedgerEntryTypeAccount {
					if change.Pre != nil && change.Post != nil {
						preAccount := change.Pre.Data.MustAccount()
						postAccount := change.Post.Data.MustAccount()
						preBal := int64(preAccount.Balance)
						postBal := int64(postAccount.Balance)

						if postBal > preBal {
							amount := fmt.Sprintf("%d", postBal-preBal)
							accountID := postAccount.AccountId.Address()
							assetType := "native"
							effects = append(effects, EffectRow{
								LedgerSequence:   ledgerSeq,
								TransactionHash:  txHash,
								OperationIndex:   int(opIdx),
								EffectIndex:      effectIdx,
								EffectType:       2,
								EffectTypeString: "account_credited",
								AccountID:        &accountID,
								Amount:           &amount,
								AssetType:        &assetType,
								CreatedAt:        closedAt,
								LedgerRange:      ledgerRange,
								EraID:            EraID,
								VersionLabel:     VersionLabel,
							})
							effectIdx++
						} else if postBal < preBal {
							amount := fmt.Sprintf("%d", preBal-postBal)
							accountID := postAccount.AccountId.Address()
							assetType := "native"
							effects = append(effects, EffectRow{
								LedgerSequence:   ledgerSeq,
								TransactionHash:  txHash,
								OperationIndex:   int(opIdx),
								EffectIndex:      effectIdx,
								EffectType:       3,
								EffectTypeString: "account_debited",
								AccountID:        &accountID,
								Amount:           &amount,
								AssetType:        &assetType,
								CreatedAt:        closedAt,
								LedgerRange:      ledgerRange,
								EraID:            EraID,
								VersionLabel:     VersionLabel,
							})
							effectIdx++
						}
					}
				}
			}
		}
	}

	return effects, nil
}

// ---------------------------------------------------------------------------
// Asset decomposition helpers (adapted from history loader)
// ---------------------------------------------------------------------------

func decomposeAsset(asset xdr.Asset) (assetType, assetCode, assetIssuer string) {
	switch asset.Type {
	case xdr.AssetTypeAssetTypeNative:
		return "native", "", ""
	case xdr.AssetTypeAssetTypeCreditAlphanum4:
		if a4, ok := asset.GetAlphaNum4(); ok {
			code := strings.TrimRight(string(a4.AssetCode[:]), "\x00")
			return "credit_alphanum4", code, a4.Issuer.Address()
		}
	case xdr.AssetTypeAssetTypeCreditAlphanum12:
		if a12, ok := asset.GetAlphaNum12(); ok {
			code := strings.TrimRight(string(a12.AssetCode[:]), "\x00")
			return "credit_alphanum12", code, a12.Issuer.Address()
		}
	}
	return "", "", ""
}

func setOpAssetFields(opRow *OperationRow, asset xdr.Asset) {
	canonical := asset.StringCanonical()
	opRow.Asset = &canonical
	aType, aCode, aIssuer := decomposeAsset(asset)
	opRow.AssetType = &aType
	if aCode != "" {
		opRow.AssetCode = &aCode
	}
	if aIssuer != "" {
		opRow.AssetIssuer = &aIssuer
	}
}

func setOpSourceAssetFields(opRow *OperationRow, asset xdr.Asset) {
	canonical := asset.StringCanonical()
	opRow.SourceAsset = &canonical
	aType, aCode, aIssuer := decomposeAsset(asset)
	opRow.SourceAssetType = &aType
	if aCode != "" {
		opRow.SourceAssetCode = &aCode
	}
	if aIssuer != "" {
		opRow.SourceAssetIssuer = &aIssuer
	}
}

func setOpBuyingAssetFields(opRow *OperationRow, asset xdr.Asset) {
	canonical := asset.StringCanonical()
	opRow.BuyingAsset = &canonical
	aType, aCode, aIssuer := decomposeAsset(asset)
	opRow.BuyingAssetType = &aType
	if aCode != "" {
		opRow.BuyingAssetCode = &aCode
	}
	if aIssuer != "" {
		opRow.BuyingAssetIssuer = &aIssuer
	}
}

func setOpSellingAssetFields(opRow *OperationRow, asset xdr.Asset) {
	canonical := asset.StringCanonical()
	opRow.SellingAsset = &canonical
	aType, aCode, aIssuer := decomposeAsset(asset)
	opRow.SellingAssetType = &aType
	if aCode != "" {
		opRow.SellingAssetCode = &aCode
	}
	if aIssuer != "" {
		opRow.SellingAssetIssuer = &aIssuer
	}
}

func getMuxedAddr(account xdr.MuxedAccount) *string {
	if account.Type == xdr.CryptoKeyTypeKeyTypeMuxedEd25519 {
		addr := account.Address()
		return &addr
	}
	return nil
}

func extractContractInvocation(op xdr.Operation) (*string, *string, *string) {
	invokeOp, ok := op.Body.GetInvokeHostFunctionOp()
	if !ok {
		return nil, nil, nil
	}
	if invokeOp.HostFunction.Type != xdr.HostFunctionTypeHostFunctionTypeInvokeContract {
		return nil, nil, nil
	}
	if invokeOp.HostFunction.InvokeContract == nil {
		return nil, nil, nil
	}

	invokeContract := invokeOp.HostFunction.InvokeContract

	var contractID *string
	contractIDStr, err := invokeContract.ContractAddress.String()
	if err == nil && contractIDStr != "" {
		contractID = &contractIDStr
	}

	var functionName *string
	if invokeContract.FunctionName != "" {
		fnName := string(invokeContract.FunctionName)
		functionName = &fnName
	}

	var argsJSON *string
	if len(invokeContract.Args) > 0 {
		var argStrings []string
		for _, arg := range invokeContract.Args {
			if b, err := arg.MarshalBinary(); err == nil {
				argStrings = append(argStrings, base64.StdEncoding.EncodeToString(b))
			}
		}
		if j, err := json.Marshal(argStrings); err == nil {
			s := string(j)
			argsJSON = &s
		}
	}

	return contractID, functionName, argsJSON
}

// ---------------------------------------------------------------------------
// Trades extraction
// ---------------------------------------------------------------------------

func extractTradeRows(lcm xdr.LedgerCloseMeta, networkPassphrase string, ledgerSeq uint32, closedAt time.Time, ledgerRange uint32) ([]TradeRow, error) {
	var trades []TradeRow

	reader, err := ingest.NewLedgerTransactionReaderFromLedgerCloseMeta(networkPassphrase, lcm)
	if err != nil {
		return nil, fmt.Errorf("create transaction reader: %w", err)
	}
	defer reader.Close()

	for {
		tx, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Printf("Error reading transaction for trades in ledger %d: %v", ledgerSeq, err)
			continue
		}

		if !tx.Result.Successful() {
			continue
		}

		txHash := hex.EncodeToString(tx.Result.TransactionHash[:])

		for opIdx, op := range tx.Envelope.Operations() {
			if opResults, ok := tx.Result.Result.OperationResults(); ok {
				if opIdx >= len(opResults) {
					continue
				}
				opResult := opResults[opIdx]

				tradeIndex := 0
				switch op.Body.Type {
				case xdr.OperationTypeManageSellOffer, xdr.OperationTypeManageBuyOffer,
					xdr.OperationTypeCreatePassiveSellOffer:

					var offerResult *xdr.ManageOfferSuccessResult
					switch opResult.Code {
					case xdr.OperationResultCodeOpInner:
						tr := opResult.MustTr()
						switch tr.Type {
						case xdr.OperationTypeManageSellOffer:
							if r, ok := tr.GetManageSellOfferResult(); ok && r.Code == xdr.ManageSellOfferResultCodeManageSellOfferSuccess {
								result := r.MustSuccess()
								offerResult = &result
							}
						case xdr.OperationTypeManageBuyOffer:
							if r, ok := tr.GetManageBuyOfferResult(); ok && r.Code == xdr.ManageBuyOfferResultCodeManageBuyOfferSuccess {
								result := r.MustSuccess()
								offerResult = &result
							}
						case xdr.OperationTypeCreatePassiveSellOffer:
							if r, ok := tr.GetCreatePassiveSellOfferResult(); ok && r.Code == xdr.ManageSellOfferResultCodeManageSellOfferSuccess {
								result := r.MustSuccess()
								offerResult = &result
							}
						}
					}

					if offerResult != nil {
						for _, claimAtom := range offerResult.OffersClaimed {
							var sellerAccount, sellingAmount, buyingAmount string
							var sellingCode, sellingIssuer, buyingCode, buyingIssuer *string

							switch claimAtom.Type {
							case xdr.ClaimAtomTypeClaimAtomTypeOrderBook:
								ob := claimAtom.MustOrderBook()
								sellerAccount = ob.SellerId.Address()
								sellingAmount = fmt.Sprintf("%d", ob.AmountSold)
								buyingAmount = fmt.Sprintf("%d", ob.AmountBought)

								if ob.AssetSold.Type != xdr.AssetTypeAssetTypeNative {
									_, sc, si := decomposeAsset(ob.AssetSold)
									if sc != "" {
										sellingCode = &sc
									}
									if si != "" {
										sellingIssuer = &si
									}
								}

								if ob.AssetBought.Type != xdr.AssetTypeAssetTypeNative {
									_, bc, bi := decomposeAsset(ob.AssetBought)
									if bc != "" {
										buyingCode = &bc
									}
									if bi != "" {
										buyingIssuer = &bi
									}
								}

							case xdr.ClaimAtomTypeClaimAtomTypeV0:
								v0 := claimAtom.MustV0()
								sellerAccount = fmt.Sprintf("%x", v0.SellerEd25519)
								sellingAmount = fmt.Sprintf("%d", v0.AmountSold)
								buyingAmount = fmt.Sprintf("%d", v0.AmountBought)
							}

							buyerAccount := tx.Envelope.SourceAccount().ToAccountId().Address()

							trades = append(trades, TradeRow{
								LedgerSequence:     ledgerSeq,
								TransactionHash:    txHash,
								OperationIndex:     opIdx,
								TradeIndex:         tradeIndex,
								TradeType:          "orderbook",
								TradeTimestamp:     closedAt,
								SellerAccount:      sellerAccount,
								SellingAssetCode:   sellingCode,
								SellingAssetIssuer: sellingIssuer,
								SellingAmount:      sellingAmount,
								BuyerAccount:       buyerAccount,
								BuyingAssetCode:    buyingCode,
								BuyingAssetIssuer:  buyingIssuer,
								BuyingAmount:       buyingAmount,
								Price:              fmt.Sprintf("%s/%s", buyingAmount, sellingAmount),
								CreatedAt:          closedAt,
								LedgerRange:        ledgerRange,
								EraID:              EraID,
								VersionLabel:       VersionLabel,
							})
							tradeIndex++
						}
					}
				}
			}
		}
	}

	return trades, nil
}

// ---------------------------------------------------------------------------
// Account extraction
// ---------------------------------------------------------------------------

func extractAccountRows(lcm xdr.LedgerCloseMeta, networkPassphrase string, ledgerSeq uint32, closedAt time.Time, ledgerRange uint32) ([]AccountRow, error) {
	var accounts []AccountRow

	reader, err := ingest.NewLedgerChangeReaderFromLedgerCloseMeta(networkPassphrase, lcm)
	if err != nil {
		log.Printf("Failed to create change reader for accounts: %v", err)
		return accounts, nil
	}
	defer reader.Close()

	accountMap := make(map[string]*AccountRow)

	for {
		change, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Printf("Error reading change for accounts: %v", err)
			continue
		}

		if change.Type != xdr.LedgerEntryTypeAccount {
			continue
		}

		var accountEntry *xdr.AccountEntry
		if change.Post != nil {
			if ae, ok := change.Post.Data.GetAccount(); ok {
				accountEntry = &ae
			}
		}
		if accountEntry == nil {
			continue
		}

		accountID := accountEntry.AccountId.Address()
		balance := strconv.FormatInt(int64(accountEntry.Balance), 10)
		sequenceNumber := uint64(accountEntry.SeqNum)
		numSubentries := uint32(accountEntry.NumSubEntries)
		numSponsoring := uint32(0)
		numSponsored := uint32(0)

		var homeDomain *string
		if accountEntry.HomeDomain != "" {
			hd := string(accountEntry.HomeDomain)
			homeDomain = &hd
		}

		masterWeight := uint32(accountEntry.Thresholds[0])
		lowThreshold := uint32(accountEntry.Thresholds[1])
		medThreshold := uint32(accountEntry.Thresholds[2])
		highThreshold := uint32(accountEntry.Thresholds[3])

		flags := uint32(accountEntry.Flags)
		authRequired := (flags & uint32(xdr.AccountFlagsAuthRequiredFlag)) != 0
		authRevocable := (flags & uint32(xdr.AccountFlagsAuthRevocableFlag)) != 0
		authImmutable := (flags & uint32(xdr.AccountFlagsAuthImmutableFlag)) != 0
		authClawbackEnabled := (flags & uint32(xdr.AccountFlagsAuthClawbackEnabledFlag)) != 0

		var signersJSON *string
		if len(accountEntry.Signers) > 0 {
			type SignerData struct {
				Key    string `json:"key"`
				Weight uint32 `json:"weight"`
			}
			var signersList []SignerData
			for _, signer := range accountEntry.Signers {
				signersList = append(signersList, SignerData{
					Key:    signer.Key.Address(),
					Weight: uint32(signer.Weight),
				})
			}
			if jsonBytes, err := json.Marshal(signersList); err == nil {
				jsonStr := string(jsonBytes)
				signersJSON = &jsonStr
			}
		}

		if ext, ok := accountEntry.Ext.GetV1(); ok {
			if ext2, ok := ext.Ext.GetV2(); ok {
				numSponsoring = uint32(ext2.NumSponsoring)
				numSponsored = uint32(ext2.NumSponsored)
			}
		}

		row := AccountRow{
			AccountID:           accountID,
			LedgerSequence:      ledgerSeq,
			ClosedAt:            closedAt,
			Balance:             balance,
			SequenceNumber:      sequenceNumber,
			NumSubentries:       numSubentries,
			NumSponsoring:       numSponsoring,
			NumSponsored:        numSponsored,
			HomeDomain:          homeDomain,
			MasterWeight:        masterWeight,
			LowThreshold:        lowThreshold,
			MedThreshold:        medThreshold,
			HighThreshold:       highThreshold,
			Flags:               flags,
			AuthRequired:        authRequired,
			AuthRevocable:       authRevocable,
			AuthImmutable:       authImmutable,
			AuthClawbackEnabled: authClawbackEnabled,
			Signers:             signersJSON,
			CreatedAt:           closedAt,
			UpdatedAt:           closedAt,
			LedgerRange:         ledgerRange,
			EraID:               EraID,
			VersionLabel:        VersionLabel,
		}

		accountMap[accountID] = &row
	}

	for _, r := range accountMap {
		accounts = append(accounts, *r)
	}
	return accounts, nil
}

// ---------------------------------------------------------------------------
// Trustline extraction
// ---------------------------------------------------------------------------

func extractTrustlineRows(lcm xdr.LedgerCloseMeta, networkPassphrase string, ledgerSeq uint32, closedAt time.Time, ledgerRange uint32) ([]TrustlineRow, error) {
	var trustlines []TrustlineRow

	reader, err := ingest.NewLedgerChangeReaderFromLedgerCloseMeta(networkPassphrase, lcm)
	if err != nil {
		log.Printf("Failed to create change reader for trustlines: %v", err)
		return trustlines, nil
	}
	defer reader.Close()

	trustlineMap := make(map[string]*TrustlineRow)

	for {
		change, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Printf("Error reading change for trustlines: %v", err)
			continue
		}

		if change.Type != xdr.LedgerEntryTypeTrustline {
			continue
		}

		var tlEntry *xdr.TrustLineEntry
		if change.Post != nil {
			if tl, ok := change.Post.Data.GetTrustLine(); ok {
				tlEntry = &tl
			}
		}
		if tlEntry == nil {
			continue
		}

		accountID := tlEntry.AccountId.Address()

		var assetCode, assetIssuer, assetType string
		switch tlEntry.Asset.Type {
		case xdr.AssetTypeAssetTypeCreditAlphanum4:
			a4 := tlEntry.Asset.MustAlphaNum4()
			assetCode = strings.TrimRight(string(a4.AssetCode[:]), "\x00")
			assetIssuer = a4.Issuer.Address()
			assetType = "credit_alphanum4"
		case xdr.AssetTypeAssetTypeCreditAlphanum12:
			a12 := tlEntry.Asset.MustAlphaNum12()
			assetCode = strings.TrimRight(string(a12.AssetCode[:]), "\x00")
			assetIssuer = a12.Issuer.Address()
			assetType = "credit_alphanum12"
		case xdr.AssetTypeAssetTypePoolShare:
			assetCode = "POOL_SHARE"
			assetIssuer = "pool"
			assetType = "liquidity_pool_shares"
		}

		balance := strconv.FormatInt(int64(tlEntry.Balance), 10)
		trustLimit := strconv.FormatInt(int64(tlEntry.Limit), 10)
		buyingLiabilities := "0"
		sellingLiabilities := "0"

		if ext, ok := tlEntry.Ext.GetV1(); ok {
			buyingLiabilities = strconv.FormatInt(int64(ext.Liabilities.Buying), 10)
			sellingLiabilities = strconv.FormatInt(int64(ext.Liabilities.Selling), 10)
		}

		flags := uint32(tlEntry.Flags)
		authorized := (flags & uint32(xdr.TrustLineFlagsAuthorizedFlag)) != 0
		authorizedToMaintainLiabilities := (flags & uint32(xdr.TrustLineFlagsAuthorizedToMaintainLiabilitiesFlag)) != 0
		clawbackEnabled := (flags & uint32(xdr.TrustLineFlagsTrustlineClawbackEnabledFlag)) != 0

		row := TrustlineRow{
			AccountID:                       accountID,
			AssetCode:                       assetCode,
			AssetIssuer:                     assetIssuer,
			AssetType:                       assetType,
			Balance:                         balance,
			TrustLimit:                      trustLimit,
			BuyingLiabilities:               buyingLiabilities,
			SellingLiabilities:              sellingLiabilities,
			Authorized:                      authorized,
			AuthorizedToMaintainLiabilities: authorizedToMaintainLiabilities,
			ClawbackEnabled:                 clawbackEnabled,
			LedgerSequence:                  ledgerSeq,
			CreatedAt:                       closedAt,
			LedgerRange:                     ledgerRange,
			EraID:                           EraID,
			VersionLabel:                    VersionLabel,
		}

		key := fmt.Sprintf("%s:%s:%s", accountID, assetCode, assetIssuer)
		trustlineMap[key] = &row
	}

	for _, r := range trustlineMap {
		trustlines = append(trustlines, *r)
	}
	return trustlines, nil
}

// ---------------------------------------------------------------------------
// Offer extraction
// ---------------------------------------------------------------------------

func extractOfferRows(lcm xdr.LedgerCloseMeta, networkPassphrase string, ledgerSeq uint32, closedAt time.Time, ledgerRange uint32) ([]OfferRow, error) {
	var offers []OfferRow

	reader, err := ingest.NewLedgerChangeReaderFromLedgerCloseMeta(networkPassphrase, lcm)
	if err != nil {
		log.Printf("Failed to create change reader for offers: %v", err)
		return offers, nil
	}
	defer reader.Close()

	offerMap := make(map[int64]*OfferRow)

	for {
		change, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Printf("Error reading change for offers: %v", err)
			continue
		}

		if change.Type != xdr.LedgerEntryTypeOffer {
			continue
		}

		var offerEntry *xdr.OfferEntry
		if change.Post != nil {
			if oe, ok := change.Post.Data.GetOffer(); ok {
				offerEntry = &oe
			}
		}
		if offerEntry == nil {
			continue
		}

		offerID := int64(offerEntry.OfferId)
		sellerAccount := offerEntry.SellerId.Address()

		sellingType, sellingCode, sellingIssuer := parseOfferAsset(offerEntry.Selling)
		buyingType, buyingCode, buyingIssuer := parseOfferAsset(offerEntry.Buying)

		amount := strconv.FormatInt(int64(offerEntry.Amount), 10)
		priceStr := fmt.Sprintf("%d/%d", offerEntry.Price.N, offerEntry.Price.D)
		flags := uint32(offerEntry.Flags)

		row := OfferRow{
			OfferID:            offerID,
			SellerAccount:      sellerAccount,
			LedgerSequence:     ledgerSeq,
			ClosedAt:           closedAt,
			SellingAssetType:   sellingType,
			SellingAssetCode:   sellingCode,
			SellingAssetIssuer: sellingIssuer,
			BuyingAssetType:    buyingType,
			BuyingAssetCode:    buyingCode,
			BuyingAssetIssuer:  buyingIssuer,
			Amount:             amount,
			Price:              priceStr,
			Flags:              flags,
			CreatedAt:          closedAt,
			LedgerRange:        ledgerRange,
			EraID:              EraID,
			VersionLabel:       VersionLabel,
		}

		offerMap[offerID] = &row
	}

	for _, r := range offerMap {
		offers = append(offers, *r)
	}
	return offers, nil
}

// parseOfferAsset extracts asset type, code, and issuer from xdr.Asset for offers.
func parseOfferAsset(asset xdr.Asset) (assetType string, code *string, issuer *string) {
	switch asset.Type {
	case xdr.AssetTypeAssetTypeNative:
		return "native", nil, nil
	case xdr.AssetTypeAssetTypeCreditAlphanum4:
		a4 := asset.MustAlphaNum4()
		c := strings.TrimRight(string(a4.AssetCode[:]), "\x00")
		i := a4.Issuer.Address()
		return "credit_alphanum4", &c, &i
	case xdr.AssetTypeAssetTypeCreditAlphanum12:
		a12 := asset.MustAlphaNum12()
		c := strings.TrimRight(string(a12.AssetCode[:]), "\x00")
		i := a12.Issuer.Address()
		return "credit_alphanum12", &c, &i
	default:
		return "unknown", nil, nil
	}
}

// ---------------------------------------------------------------------------
// Account Signer extraction
// ---------------------------------------------------------------------------

func extractAccountSignerRows(lcm xdr.LedgerCloseMeta, networkPassphrase string, ledgerSeq uint32, closedAt time.Time, ledgerRange uint32) ([]AccountSignerRow, error) {
	var signersList []AccountSignerRow

	changeReader, err := ingest.NewLedgerChangeReaderFromLedgerCloseMeta(networkPassphrase, lcm)
	if err != nil {
		log.Printf("Failed to create change reader for account signers: %v", err)
		return signersList, nil
	}
	defer changeReader.Close()

	for {
		change, err := changeReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Printf("Error reading change for account signers: %v", err)
			continue
		}

		var accountEntry *xdr.AccountEntry
		var deleted bool

		switch change.Type {
		case xdr.LedgerEntryTypeAccount:
			if change.Post != nil {
				account := change.Post.Data.MustAccount()
				accountEntry = &account
				deleted = false
			} else if change.Pre != nil {
				account := change.Pre.Data.MustAccount()
				accountEntry = &account
				deleted = true
			}
		default:
			continue
		}

		if accountEntry == nil {
			continue
		}

		accountID := accountEntry.AccountId.Address()

		var sponsorIDs []xdr.SponsorshipDescriptor
		if accountEntry.Ext.V == 1 {
			v1 := accountEntry.Ext.MustV1()
			if v1.Ext.V == 2 {
				v2 := v1.Ext.MustV2()
				sponsorIDs = v2.SignerSponsoringIDs
			}
		}

		for i, signer := range accountEntry.Signers {
			var sponsor string
			if i < len(sponsorIDs) && sponsorIDs[i] != nil {
				sponsor = sponsorIDs[i].Address()
			}

			signersList = append(signersList, AccountSignerRow{
				AccountID:       accountID,
				Signer:          signer.Key.Address(),
				LedgerSequence:  ledgerSeq,
				Weight:          uint32(signer.Weight),
				Sponsor:         sponsor,
				Deleted:         deleted,
				ClosedAt:        closedAt,
				LedgerRange:     ledgerRange,
				CreatedAt:       closedAt,
				EraID:           EraID,
				VersionLabel:    VersionLabel,
			})
		}
	}

	return signersList, nil
}

// ---------------------------------------------------------------------------
// Claimable Balance extraction
// ---------------------------------------------------------------------------

func extractClaimableBalanceRows(lcm xdr.LedgerCloseMeta, networkPassphrase string, ledgerSeq uint32, closedAt time.Time, ledgerRange uint32) ([]ClaimableBalanceRow, error) {
	var balances []ClaimableBalanceRow

	reader, err := ingest.NewLedgerChangeReaderFromLedgerCloseMeta(networkPassphrase, lcm)
	if err != nil {
		log.Printf("Failed to create change reader for claimable balances: %v", err)
		return balances, nil
	}
	defer reader.Close()

	balanceMap := make(map[string]*ClaimableBalanceRow)

	for {
		change, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Printf("Error reading change for claimable balances: %v", err)
			continue
		}

		if change.Type != xdr.LedgerEntryTypeClaimableBalance {
			continue
		}

		var balanceEntry *xdr.ClaimableBalanceEntry
		var ledgerEntry *xdr.LedgerEntry
		if change.Post != nil {
			ledgerEntry = change.Post
			if cb, ok := change.Post.Data.GetClaimableBalance(); ok {
				balanceEntry = &cb
			}
		}
		if balanceEntry == nil {
			continue
		}

		var balanceID string
		switch balanceEntry.BalanceId.Type {
		case xdr.ClaimableBalanceIdTypeClaimableBalanceIdTypeV0:
			hashBytes := balanceEntry.BalanceId.MustV0()
			balanceID = hex.EncodeToString(hashBytes[:])
		default:
			continue
		}

		var sponsor string
		sponsorDesc := ledgerEntry.SponsoringID()
		if sponsorDesc != nil {
			sponsor = sponsorDesc.Address()
		}

		assetType, assetCode, assetIssuer := parseOfferAsset(balanceEntry.Asset)
		amount := int64(balanceEntry.Amount)
		claimantsCount := len(balanceEntry.Claimants)

		row := ClaimableBalanceRow{
			BalanceID:       balanceID,
			Sponsor:         sponsor,
			LedgerSequence:  ledgerSeq,
			ClosedAt:        closedAt,
			AssetType:       assetType,
			AssetCode:       assetCode,
			AssetIssuer:     assetIssuer,
			Amount:          amount,
			ClaimantsCount:  claimantsCount,
			Flags:           0,
			CreatedAt:       closedAt,
			LedgerRange:     ledgerRange,
			EraID:           EraID,
			VersionLabel:    VersionLabel,
		}

		balanceMap[balanceID] = &row
	}

	for _, r := range balanceMap {
		balances = append(balances, *r)
	}
	return balances, nil
}

// ---------------------------------------------------------------------------
// Liquidity Pool extraction
// ---------------------------------------------------------------------------

func extractLiquidityPoolRows(lcm xdr.LedgerCloseMeta, networkPassphrase string, ledgerSeq uint32, closedAt time.Time, ledgerRange uint32) ([]LiquidityPoolRow, error) {
	var pools []LiquidityPoolRow

	reader, err := ingest.NewLedgerChangeReaderFromLedgerCloseMeta(networkPassphrase, lcm)
	if err != nil {
		log.Printf("Failed to create change reader for liquidity pools: %v", err)
		return pools, nil
	}
	defer reader.Close()

	poolMap := make(map[string]*LiquidityPoolRow)

	for {
		change, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Printf("Error reading change for liquidity pools: %v", err)
			continue
		}

		if change.Type != xdr.LedgerEntryTypeLiquidityPool {
			continue
		}

		var poolEntry *xdr.LiquidityPoolEntry
		if change.Post != nil {
			if lp, ok := change.Post.Data.GetLiquidityPool(); ok {
				poolEntry = &lp
			}
		}
		if poolEntry == nil {
			continue
		}

		poolID := hex.EncodeToString(poolEntry.LiquidityPoolId[:])

		if poolEntry.Body.Type != xdr.LiquidityPoolTypeLiquidityPoolConstantProduct {
			continue
		}

		cp := poolEntry.Body.MustConstantProduct()

		assetAType, assetACode, assetAIssuer := parseOfferAsset(cp.Params.AssetA)
		assetBType, assetBCode, assetBIssuer := parseOfferAsset(cp.Params.AssetB)

		row := LiquidityPoolRow{
			LiquidityPoolID: poolID,
			LedgerSequence:  ledgerSeq,
			ClosedAt:        closedAt,
			PoolType:        "constant_product",
			Fee:             int(cp.Params.Fee),
			TrustlineCount:  int(cp.PoolSharesTrustLineCount),
			TotalPoolShares: int64(cp.TotalPoolShares),
			AssetAType:      assetAType,
			AssetACode:      assetACode,
			AssetAIssuer:    assetAIssuer,
			AssetAAmount:    int64(cp.ReserveA),
			AssetBType:      assetBType,
			AssetBCode:      assetBCode,
			AssetBIssuer:    assetBIssuer,
			AssetBAmount:    int64(cp.ReserveB),
			CreatedAt:       closedAt,
			LedgerRange:     ledgerRange,
			EraID:           EraID,
			VersionLabel:    VersionLabel,
		}

		poolMap[poolID] = &row
	}

	for _, r := range poolMap {
		pools = append(pools, *r)
	}
	return pools, nil
}

// ---------------------------------------------------------------------------
// Config Settings extraction
// ---------------------------------------------------------------------------

func extractConfigSettingRows(lcm xdr.LedgerCloseMeta, networkPassphrase string, ledgerSeq uint32, closedAt time.Time, ledgerRange uint32) ([]ConfigSettingRow, error) {
	var configSettings []ConfigSettingRow

	txReader, err := ingest.NewLedgerTransactionReaderFromLedgerCloseMeta(networkPassphrase, lcm)
	if err != nil {
		log.Printf("Failed to create transaction reader for config settings: %v", err)
		return configSettings, nil
	}
	defer txReader.Close()

	configMap := make(map[int]*ConfigSettingRow)

	for {
		tx, err := txReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Printf("Error reading transaction for config settings: %v", err)
			continue
		}

		changes, err := tx.GetChanges()
		if err != nil {
			continue
		}

		for _, change := range changes {
			if !isConfigSettingChangeType(change) {
				continue
			}

			var configEntry *xdr.ConfigSettingEntry
			var deleted bool
			var lastModifiedLedger uint32

			if change.Post != nil {
				entry, _ := change.Post.Data.GetConfigSetting()
				configEntry = &entry
				lastModifiedLedger = uint32(change.Post.LastModifiedLedgerSeq)
				deleted = false
			} else if change.Pre != nil {
				entry, _ := change.Pre.Data.GetConfigSetting()
				configEntry = &entry
				lastModifiedLedger = uint32(change.Pre.LastModifiedLedgerSeq)
				deleted = true
			}
			if configEntry == nil {
				continue
			}

			configID := int(configEntry.ConfigSettingId)

			var configXDR string
			if xdrBytes, err := configEntry.MarshalBinary(); err == nil {
				configXDR = base64.StdEncoding.EncodeToString(xdrBytes)
			}

			row := ConfigSettingRow{
				ConfigSettingID:    configID,
				LedgerSequence:     ledgerSeq,
				LastModifiedLedger: int(lastModifiedLedger),
				Deleted:            deleted,
				ClosedAt:           closedAt,
				ConfigSettingXDR:   configXDR,
				CreatedAt:          closedAt,
				LedgerRange:        ledgerRange,
				EraID:              EraID,
				VersionLabel:       VersionLabel,
			}

			// Parse Soroban resource limit fields from config setting
			parseConfigSettingResourceLimits(configEntry, &row)

			configMap[configID] = &row
		}
	}

	for _, r := range configMap {
		configSettings = append(configSettings, *r)
	}
	return configSettings, nil
}

func isConfigSettingChangeType(change ingest.Change) bool {
	if change.Pre != nil && change.Pre.Data.Type == xdr.LedgerEntryTypeConfigSetting {
		return true
	}
	if change.Post != nil && change.Post.Data.Type == xdr.LedgerEntryTypeConfigSetting {
		return true
	}
	return false
}

// parseConfigSettingResourceLimits extracts Soroban resource limit fields from a config setting entry.
func parseConfigSettingResourceLimits(entry *xdr.ConfigSettingEntry, row *ConfigSettingRow) {
	if entry == nil {
		return
	}

	switch entry.ConfigSettingId {
	case xdr.ConfigSettingIdConfigSettingContractMaxSizeBytes:
		if maxSize, ok := entry.GetContractMaxSizeBytes(); ok {
			val := int64(maxSize)
			row.ContractMaxSizeBytes = &val
		}

	case xdr.ConfigSettingIdConfigSettingContractComputeV0:
		if compute, ok := entry.GetContractCompute(); ok {
			ledgerMaxInstr := int64(compute.LedgerMaxInstructions)
			row.LedgerMaxInstructions = &ledgerMaxInstr
			txMaxInstr := int64(compute.TxMaxInstructions)
			row.TxMaxInstructions = &txMaxInstr
			feeRate := int64(compute.FeeRatePerInstructionsIncrement)
			row.FeeRatePerInstructionsIncrement = &feeRate
			txMemLimit := int64(compute.TxMemoryLimit)
			row.TxMemoryLimit = &txMemLimit
		}

	case xdr.ConfigSettingIdConfigSettingContractLedgerCostV0:
		if cost, ok := entry.GetContractLedgerCost(); ok {
			v := int64(cost.LedgerMaxDiskReadEntries)
			row.LedgerMaxReadLedgerEntries = &v
			v2 := int64(cost.LedgerMaxDiskReadBytes)
			row.LedgerMaxReadBytes = &v2
			v3 := int64(cost.LedgerMaxWriteLedgerEntries)
			row.LedgerMaxWriteLedgerEntries = &v3
			v4 := int64(cost.LedgerMaxWriteBytes)
			row.LedgerMaxWriteBytes = &v4
			v5 := int64(cost.TxMaxDiskReadEntries)
			row.TxMaxReadLedgerEntries = &v5
			v6 := int64(cost.TxMaxDiskReadBytes)
			row.TxMaxReadBytes = &v6
			v7 := int64(cost.TxMaxWriteLedgerEntries)
			row.TxMaxWriteLedgerEntries = &v7
			v8 := int64(cost.TxMaxWriteBytes)
			row.TxMaxWriteBytes = &v8
		}
	}
}

// ---------------------------------------------------------------------------
// TTL extraction
// ---------------------------------------------------------------------------

func extractTTLRows(lcm xdr.LedgerCloseMeta, networkPassphrase string, ledgerSeq uint32, closedAt time.Time, ledgerRange uint32) ([]TTLRow, error) {
	var ttlList []TTLRow

	txReader, err := ingest.NewLedgerTransactionReaderFromLedgerCloseMeta(networkPassphrase, lcm)
	if err != nil {
		log.Printf("Failed to create transaction reader for TTL: %v", err)
		return ttlList, nil
	}
	defer txReader.Close()

	ttlMap := make(map[string]*TTLRow)

	for {
		tx, err := txReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Printf("Error reading transaction for TTL: %v", err)
			continue
		}

		changes, err := tx.GetChanges()
		if err != nil {
			continue
		}

		for _, change := range changes {
			if !isTTLChangeType(change) {
				continue
			}

			var ttlEntry *xdr.TtlEntry
			var deleted bool
			var lastModifiedLedger uint32

			if change.Post != nil {
				entry, _ := change.Post.Data.GetTtl()
				ttlEntry = &entry
				lastModifiedLedger = uint32(change.Post.LastModifiedLedgerSeq)
				deleted = false
			} else if change.Pre != nil {
				entry, _ := change.Pre.Data.GetTtl()
				ttlEntry = &entry
				lastModifiedLedger = uint32(change.Pre.LastModifiedLedgerSeq)
				deleted = true
			}
			if ttlEntry == nil {
				continue
			}

			keyHashBytes, err := ttlEntry.KeyHash.MarshalBinary()
			if err != nil {
				continue
			}
			keyHash := hex.EncodeToString(keyHashBytes)

			liveUntilLedgerSeq := int64(ttlEntry.LiveUntilLedgerSeq)
			ttlRemaining := liveUntilLedgerSeq - int64(ledgerSeq)
			expired := ttlRemaining <= 0

			row := TTLRow{
				KeyHash:            keyHash,
				LedgerSequence:     ledgerSeq,
				LiveUntilLedgerSeq: liveUntilLedgerSeq,
				TTLRemaining:       ttlRemaining,
				Expired:            expired,
				LastModifiedLedger: int(lastModifiedLedger),
				Deleted:            deleted,
				ClosedAt:           closedAt,
				CreatedAt:          closedAt,
				LedgerRange:        ledgerRange,
				EraID:              EraID,
				VersionLabel:       VersionLabel,
			}

			ttlMap[keyHash] = &row
		}
	}

	for _, r := range ttlMap {
		ttlList = append(ttlList, *r)
	}
	return ttlList, nil
}

func isTTLChangeType(change ingest.Change) bool {
	if change.Pre != nil && change.Pre.Data.Type == xdr.LedgerEntryTypeTtl {
		return true
	}
	if change.Post != nil && change.Post.Data.Type == xdr.LedgerEntryTypeTtl {
		return true
	}
	return false
}

// ---------------------------------------------------------------------------
// Evicted Keys extraction
// ---------------------------------------------------------------------------

func extractEvictedKeyRows(lcm xdr.LedgerCloseMeta, ledgerSeq uint32, closedAt time.Time, ledgerRange uint32) ([]EvictedKeyRow, error) {
	var evictedKeys []EvictedKeyRow

	if lcm.V != 2 {
		return evictedKeys, nil
	}

	v2Meta := lcm.MustV2()
	evictedMap := make(map[string]*EvictedKeyRow)

	for _, evictedKey := range v2Meta.EvictedKeys {
		keyHashBytes, err := evictedKey.MarshalBinary()
		if err != nil {
			continue
		}
		hash := sha256.Sum256(keyHashBytes)
		keyHash := hex.EncodeToString(hash[:])

		var contractID, keyType string
		durability := "unknown"

		switch evictedKey.Type {
		case xdr.LedgerEntryTypeContractData:
			if evictedKey.ContractData != nil {
				contractBytes, err := evictedKey.ContractData.Contract.MarshalBinary()
				if err == nil && len(contractBytes) > 0 {
					contractHash := sha256.Sum256(contractBytes)
					contractID = hex.EncodeToString(contractHash[:])
				}
				keyType = evictedKey.ContractData.Key.Type.String()
			} else {
				keyType = "ContractData"
			}
		case xdr.LedgerEntryTypeContractCode:
			if evictedKey.ContractCode != nil {
				codeHashBytes, err := evictedKey.ContractCode.Hash.MarshalBinary()
				if err == nil {
					contractID = hex.EncodeToString(codeHashBytes)
				}
			}
			keyType = "ContractCode"
		default:
			keyType = evictedKey.Type.String()
		}

		row := EvictedKeyRow{
			KeyHash:         keyHash,
			LedgerSequence:  ledgerSeq,
			ContractID:      contractID,
			KeyType:         keyType,
			Durability:      durability,
			ClosedAt:        closedAt,
			LedgerRange:     ledgerRange,
			CreatedAt:       closedAt,
			EraID:           EraID,
			VersionLabel:    VersionLabel,
		}

		evictedMap[keyHash] = &row
	}

	for _, r := range evictedMap {
		evictedKeys = append(evictedKeys, *r)
	}
	return evictedKeys, nil
}

// ---------------------------------------------------------------------------
// Restored Keys extraction
// ---------------------------------------------------------------------------

func extractRestoredKeyRows(lcm xdr.LedgerCloseMeta, networkPassphrase string, ledgerSeq uint32, closedAt time.Time, ledgerRange uint32) ([]RestoredKeyRow, error) {
	var restoredKeys []RestoredKeyRow

	reader, err := ingest.NewLedgerTransactionReaderFromLedgerCloseMeta(networkPassphrase, lcm)
	if err != nil {
		log.Printf("Failed to create transaction reader for restored keys: %v", err)
		return restoredKeys, nil
	}
	defer reader.Close()

	restoredMap := make(map[string]*RestoredKeyRow)

	for {
		tx, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			continue
		}

		if !tx.Result.Successful() {
			continue
		}

		envelope := tx.Envelope
		var operations []xdr.Operation

		switch envelope.Type {
		case xdr.EnvelopeTypeEnvelopeTypeTx:
			operations = envelope.V1.Tx.Operations
		case xdr.EnvelopeTypeEnvelopeTypeTxV0:
			operations = envelope.V0.Tx.Operations
		case xdr.EnvelopeTypeEnvelopeTypeTxFeeBump:
			innerTx := envelope.FeeBump.Tx.InnerTx
			if innerTx.Type == xdr.EnvelopeTypeEnvelopeTypeTx {
				operations = innerTx.V1.Tx.Operations
			}
		default:
			continue
		}

		for _, op := range operations {
			if op.Body.Type != xdr.OperationTypeRestoreFootprint {
				continue
			}

			var footprint *xdr.LedgerFootprint
			switch envelope.Type {
			case xdr.EnvelopeTypeEnvelopeTypeTx:
				if envelope.V1.Tx.Ext.V == 1 && envelope.V1.Tx.Ext.SorobanData != nil {
					footprint = &envelope.V1.Tx.Ext.SorobanData.Resources.Footprint
				}
			}
			if footprint == nil {
				continue
			}

			for _, key := range footprint.ReadWrite {
				keyHashBytes, err := key.MarshalBinary()
				if err != nil {
					continue
				}
				hash := sha256.Sum256(keyHashBytes)
				keyHash := hex.EncodeToString(hash[:])

				var contractID, keyType string
				durability := "unknown"

				switch key.Type {
				case xdr.LedgerEntryTypeContractData:
					if key.ContractData != nil {
						if key.ContractData.Contract.ContractId != nil {
							contractIDBytes, err := key.ContractData.Contract.ContractId.MarshalBinary()
							if err == nil {
								contractID = hex.EncodeToString(contractIDBytes)
							}
						}
						keyType = key.ContractData.Key.Type.String()
					} else {
						keyType = "ContractData"
					}
				case xdr.LedgerEntryTypeContractCode:
					if key.ContractCode != nil {
						codeHashBytes, err := key.ContractCode.Hash.MarshalBinary()
						if err == nil {
							contractID = hex.EncodeToString(codeHashBytes)
						}
					}
					keyType = "ContractCode"
				default:
					keyType = key.Type.String()
				}

				row := RestoredKeyRow{
					KeyHash:            keyHash,
					LedgerSequence:     ledgerSeq,
					ContractID:         contractID,
					KeyType:            keyType,
					Durability:         durability,
					RestoredFromLedger: 0,
					ClosedAt:           closedAt,
					LedgerRange:        ledgerRange,
					CreatedAt:          closedAt,
					EraID:              EraID,
					VersionLabel:       VersionLabel,
				}

				restoredMap[keyHash] = &row
			}
		}
	}

	for _, r := range restoredMap {
		restoredKeys = append(restoredKeys, *r)
	}
	return restoredKeys, nil
}

// ---------------------------------------------------------------------------
// Contract Events extraction
// ---------------------------------------------------------------------------

func extractContractEventRows(lcm xdr.LedgerCloseMeta, networkPassphrase string, ledgerSeq uint32, closedAt time.Time, ledgerRange uint32) ([]ContractEventRow, error) {
	var events []ContractEventRow

	reader, err := ingest.NewLedgerTransactionReaderFromLedgerCloseMeta(networkPassphrase, lcm)
	if err != nil {
		log.Printf("Failed to create transaction reader for contract events: %v", err)
		return events, nil
	}
	defer reader.Close()

	for {
		tx, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Printf("Error reading transaction for contract events: %v", err)
			continue
		}

		txHash := hex.EncodeToString(tx.Result.TransactionHash[:])

		txEvents, err := tx.GetTransactionEvents()
		if err != nil {
			continue
		}

		// Diagnostic events
		for diagIdx, diagEvent := range txEvents.DiagnosticEvents {
			row := extractContractEventRowFromEvent(
				diagEvent.Event, txHash, ledgerSeq, closedAt, ledgerRange,
				uint32(diagIdx), 0, diagEvent.InSuccessfulContractCall,
			)
			row.EventType = "diagnostic"
			events = append(events, row)
		}

		// Operation-level events
		for opIdx, opEvents := range txEvents.OperationEvents {
			for eventIdx, contractEvent := range opEvents {
				row := extractContractEventRowFromEvent(
					contractEvent, txHash, ledgerSeq, closedAt, ledgerRange,
					uint32(opIdx), uint32(eventIdx), false,
				)
				events = append(events, row)
			}
		}
	}

	return events, nil
}

func extractContractEventRowFromEvent(
	event xdr.ContractEvent, txHash string, ledgerSeq uint32,
	closedAt time.Time, ledgerRange uint32, opIndex, eventIndex uint32,
	inSuccessfulCall bool,
) ContractEventRow {
	eventID := fmt.Sprintf("%s:%d:%d", txHash, opIndex, eventIndex)

	var contractID string
	if event.ContractId != nil {
		contractID = hex.EncodeToString((*event.ContractId)[:])
	}

	eventType := contractEventTypeStr(event.Type)
	topicsJSON, topicsDecoded, topicCount, dataXDR, dataDecoded, positionalTopics := extractContractEventBody(event.Body)

	return ContractEventRow{
		EventID:                  eventID,
		ContractID:               contractID,
		LedgerSequence:           ledgerSeq,
		TransactionHash:          txHash,
		ClosedAt:                 closedAt,
		EventType:                eventType,
		InSuccessfulContractCall: inSuccessfulCall,
		TopicsJSON:               topicsJSON,
		TopicsDecoded:            topicsDecoded,
		DataXDR:                  dataXDR,
		DataDecoded:              dataDecoded,
		TopicCount:               topicCount,
		Topic0Decoded:            positionalTopics[0],
		Topic1Decoded:            positionalTopics[1],
		Topic2Decoded:            positionalTopics[2],
		Topic3Decoded:            positionalTopics[3],
		OperationIndex:           int(opIndex),
		EventIndex:               int(eventIndex),
		CreatedAt:                closedAt,
		LedgerRange:              ledgerRange,
		EraID:                    EraID,
		VersionLabel:             VersionLabel,
	}
}

func contractEventTypeStr(t xdr.ContractEventType) string {
	switch t {
	case xdr.ContractEventTypeSystem:
		return "system"
	case xdr.ContractEventTypeContract:
		return "contract"
	case xdr.ContractEventTypeDiagnostic:
		return "diagnostic"
	default:
		return "unknown"
	}
}

func extractContractEventBody(body xdr.ContractEventBody) (string, string, int, string, string, [4]*string) {
	var positionalTopics [4]*string

	if body.V != 0 {
		return "[]", "[]", 0, "", "{}", positionalTopics
	}

	v0 := body.MustV0()

	topicsXDR := []string{}
	topicsDecodedArray := []interface{}{}
	for i, topic := range v0.Topics {
		topicBytes, err := topic.MarshalBinary()
		if err != nil {
			continue
		}
		topicsXDR = append(topicsXDR, base64.StdEncoding.EncodeToString(topicBytes))

		decodedTopic, err := ConvertScValToJSON(topic)
		if err != nil {
			topicsDecodedArray = append(topicsDecodedArray, map[string]interface{}{
				"error": err.Error(),
				"type":  topic.Type.String(),
			})
		} else {
			topicsDecodedArray = append(topicsDecodedArray, decodedTopic)
			if i < 4 {
				positionalTopics[i] = flattenTopicVal(decodedTopic)
			}
		}
	}

	topicsJSON, err := json.Marshal(topicsXDR)
	if err != nil {
		return "[]", "[]", 0, "", "{}", positionalTopics
	}

	topicsDecodedJSON, err := json.Marshal(topicsDecodedArray)
	if err != nil {
		topicsDecodedJSON = []byte("[]")
	}

	topicCount := len(v0.Topics)

	dataBytes, err := v0.Data.MarshalBinary()
	if err != nil {
		return string(topicsJSON), string(topicsDecodedJSON), topicCount, "", "{}", positionalTopics
	}

	dataXDR := base64.StdEncoding.EncodeToString(dataBytes)

	decodedData, err := ConvertScValToJSON(v0.Data)
	if err != nil {
		decodedData = map[string]interface{}{
			"error": err.Error(),
			"type":  v0.Data.Type.String(),
		}
	}

	dataDecodedJSON, err := json.Marshal(decodedData)
	if err != nil {
		dataDecodedJSON = []byte("{}")
	}

	return string(topicsJSON), string(topicsDecodedJSON), topicCount, dataXDR, string(dataDecodedJSON), positionalTopics
}

func flattenTopicVal(decoded interface{}) *string {
	switch v := decoded.(type) {
	case string:
		return &v
	case map[string]interface{}:
		if addr, ok := v["address"].(string); ok {
			return &addr
		}
		if val, ok := v["value"].(string); ok {
			return &val
		}
		b, err := json.Marshal(v)
		if err != nil {
			return nil
		}
		s := string(b)
		return &s
	default:
		s := fmt.Sprintf("%v", v)
		return &s
	}
}

// ---------------------------------------------------------------------------
// Contract Data extraction
// ---------------------------------------------------------------------------

func extractContractDataRows(lcm xdr.LedgerCloseMeta, networkPassphrase string, ledgerSeq uint32, closedAt time.Time, ledgerRange uint32) ([]ContractDataRow, error) {
	var contractDataList []ContractDataRow

	changeReader, err := ingest.NewLedgerChangeReaderFromLedgerCloseMeta(networkPassphrase, lcm)
	if err != nil {
		log.Printf("Failed to create change reader for contract data: %v", err)
		return contractDataList, nil
	}
	defer changeReader.Close()

	contractMap := make(map[string]*ContractDataRow)

	for {
		change, err := changeReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Printf("Error reading change for contract data: %v", err)
			continue
		}

		if !isContractDataChangeType(change) {
			continue
		}

		var entry *xdr.LedgerEntry
		var deleted bool

		if change.Post != nil {
			entry = change.Post
			deleted = false
		} else if change.Pre != nil {
			entry = change.Pre
			deleted = true
		} else {
			continue
		}

		contractData, ok := entry.Data.GetContractData()
		if !ok {
			continue
		}

		if contractData.Key.Type == xdr.ScValTypeScvLedgerKeyNonce {
			continue
		}

		contractIDHash, ok := contractData.Contract.GetContractId()
		if !ok {
			continue
		}
		contractIDBytes, err := contractIDHash.MarshalBinary()
		if err != nil {
			continue
		}
		contractIDStr, err := strkey.Encode(strkey.VersionByteContract, contractIDBytes)
		if err != nil {
			continue
		}

		var ledgerKeyHash string
		ledgerKey, err := entry.LedgerKey()
		if err == nil {
			keyBytes, err := ledgerKey.MarshalBinary()
			if err == nil {
				hash := sha256.Sum256(keyBytes)
				ledgerKeyHash = hex.EncodeToString(hash[:])
			}
		}

		keyType := contractData.Key.Type.String()
		durability := contractData.Durability.String()

		var contractDataXDR string
		if xdrBytes, err := contractData.MarshalBinary(); err == nil {
			contractDataXDR = base64.StdEncoding.EncodeToString(xdrBytes)
		}

		// SAC detection
		var assetCode, assetIssuer, assetType, balanceHolder, balance *string
		if asset, ok := sac.AssetFromContractData(*entry, networkPassphrase); ok {
			if asset.IsNative() {
				at := "native"
				assetType = &at
			} else {
				code := asset.GetCode()
				issuer := asset.GetIssuer()
				assetCode = &code
				assetIssuer = &issuer
				if len(code) <= 4 {
					at := "credit_alphanum4"
					assetType = &at
				} else {
					at := "credit_alphanum12"
					assetType = &at
				}
			}
		}

		if holder, amt, ok := sac.ContractBalanceFromContractData(*entry, networkPassphrase); ok {
			holderStr, err := strkey.Encode(strkey.VersionByteContract, holder[:])
			if err == nil {
				balanceHolder = &holderStr
			}
			amtStr := amt.String()
			balance = &amtStr
		}

		// Token metadata from instance storage
		var tokenName, tokenSymbol *string
		var tokenDecimals *int
		if contractData.Key.Type == xdr.ScValTypeScvLedgerKeyContractInstance {
			tn, ts, td := extractTokenMetadataFromInstance(contractData)
			tokenName = tn
			tokenSymbol = ts
			tokenDecimals = td
		}

		dedupeKey := fmt.Sprintf("%s_%s_%s", contractIDStr, keyType, ledgerKeyHash)

		row := ContractDataRow{
			ContractID:         contractIDStr,
			LedgerSequence:     ledgerSeq,
			LedgerKeyHash:      ledgerKeyHash,
			ContractKeyType:    keyType,
			ContractDurability: durability,
			AssetCode:          assetCode,
			AssetIssuer:        assetIssuer,
			AssetType:          assetType,
			BalanceHolder:      balanceHolder,
			Balance:            balance,
			LastModifiedLedger: int(entry.LastModifiedLedgerSeq),
			LedgerEntryChange:  int(change.Type),
			Deleted:            deleted,
			ClosedAt:           closedAt,
			ContractDataXDR:    contractDataXDR,
			TokenName:          tokenName,
			TokenSymbol:        tokenSymbol,
			TokenDecimals:      tokenDecimals,
			CreatedAt:          closedAt,
			LedgerRange:        ledgerRange,
			EraID:              EraID,
			VersionLabel:       VersionLabel,
		}

		contractMap[dedupeKey] = &row
	}

	for _, r := range contractMap {
		contractDataList = append(contractDataList, *r)
	}
	return contractDataList, nil
}

func isContractDataChangeType(change ingest.Change) bool {
	if change.Pre != nil && change.Pre.Data.Type == xdr.LedgerEntryTypeContractData {
		return true
	}
	if change.Post != nil && change.Post.Data.Type == xdr.LedgerEntryTypeContractData {
		return true
	}
	return false
}

// extractTokenMetadataFromInstance extracts token name, symbol, and decimals from instance storage.
func extractTokenMetadataFromInstance(contractData xdr.ContractDataEntry) (*string, *string, *int) {
	if contractData.Val.Type != xdr.ScValTypeScvContractInstance {
		return nil, nil, nil
	}

	instance, ok := contractData.Val.GetInstance()
	if !ok || instance.Storage == nil {
		return nil, nil, nil
	}

	var name, symbol *string
	var decimals *int

	for _, entry := range *instance.Storage {
		if entry.Key.Type == xdr.ScValTypeScvSymbol && entry.Key.Sym != nil {
			sym := string(*entry.Key.Sym)
			switch sym {
			case "METADATA":
				if entry.Val.Type == xdr.ScValTypeScvMap && entry.Val.Map != nil {
					for _, item := range **entry.Val.Map {
						if item.Key.Type == xdr.ScValTypeScvSymbol && item.Key.Sym != nil {
							key := string(*item.Key.Sym)
							switch key {
							case "name":
								if item.Val.Type == xdr.ScValTypeScvString && item.Val.Str != nil {
									s := string(*item.Val.Str)
									name = &s
								}
							case "symbol":
								if item.Val.Type == xdr.ScValTypeScvString && item.Val.Str != nil {
									s := string(*item.Val.Str)
									symbol = &s
								}
							case "decimal", "decimals":
								if item.Val.Type == xdr.ScValTypeScvU32 && item.Val.U32 != nil {
									d := int(*item.Val.U32)
									decimals = &d
								}
							}
						}
					}
				}
			}
		}
	}

	return name, symbol, decimals
}

// ---------------------------------------------------------------------------
// Contract Code extraction
// ---------------------------------------------------------------------------

func extractContractCodeRows(lcm xdr.LedgerCloseMeta, networkPassphrase string, ledgerSeq uint32, closedAt time.Time, ledgerRange uint32) ([]ContractCodeRow, error) {
	var contractCodeList []ContractCodeRow

	changeReader, err := ingest.NewLedgerChangeReaderFromLedgerCloseMeta(networkPassphrase, lcm)
	if err != nil {
		log.Printf("Failed to create change reader for contract code: %v", err)
		return contractCodeList, nil
	}
	defer changeReader.Close()

	contractCodeMap := make(map[string]*ContractCodeRow)

	for {
		change, err := changeReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Printf("Error reading change for contract code: %v", err)
			continue
		}

		if !isContractCodeChangeType(change) {
			continue
		}

		var contractCode *xdr.ContractCodeEntry
		var deleted bool
		var lastModifiedLedger uint32
		var ledgerKeyHash string

		if change.Post != nil {
			codeEntry, _ := change.Post.Data.GetContractCode()
			contractCode = &codeEntry
			lastModifiedLedger = uint32(change.Post.LastModifiedLedgerSeq)
			deleted = false
			if key, err := change.Post.LedgerKey(); err == nil {
				if keyBytes, err := key.MarshalBinary(); err == nil {
					ledgerKeyHash = hex.EncodeToString(keyBytes)
				}
			}
		} else if change.Pre != nil {
			codeEntry, _ := change.Pre.Data.GetContractCode()
			contractCode = &codeEntry
			lastModifiedLedger = uint32(change.Pre.LastModifiedLedgerSeq)
			deleted = true
			if key, err := change.Pre.LedgerKey(); err == nil {
				if keyBytes, err := key.MarshalBinary(); err == nil {
					ledgerKeyHash = hex.EncodeToString(keyBytes)
				}
			}
		}

		if contractCode == nil {
			continue
		}

		codeHashHex := hex.EncodeToString(contractCode.Hash[:])

		var extV int
		if contractCode.Ext.V == 1 && contractCode.Ext.V1 != nil {
			extV = 1
		}

		wasmMetadata := parseWASMMetadataBytes(contractCode.Code)

		var nInstructions, nFunctions, nGlobals, nTableEntries, nTypes int64
		var nDataSegments, nElemSegments, nImports, nExports, nDataSegmentBytes int64
		if wasmMetadata.NInstructions != nil {
			nInstructions = *wasmMetadata.NInstructions
		}
		if wasmMetadata.NFunctions != nil {
			nFunctions = *wasmMetadata.NFunctions
		}
		if wasmMetadata.NGlobals != nil {
			nGlobals = *wasmMetadata.NGlobals
		}
		if wasmMetadata.NTableEntries != nil {
			nTableEntries = *wasmMetadata.NTableEntries
		}
		if wasmMetadata.NTypes != nil {
			nTypes = *wasmMetadata.NTypes
		}
		if wasmMetadata.NDataSegments != nil {
			nDataSegments = *wasmMetadata.NDataSegments
		}
		if wasmMetadata.NElemSegments != nil {
			nElemSegments = *wasmMetadata.NElemSegments
		}
		if wasmMetadata.NImports != nil {
			nImports = *wasmMetadata.NImports
		}
		if wasmMetadata.NExports != nil {
			nExports = *wasmMetadata.NExports
		}
		if wasmMetadata.NDataSegmentBytes != nil {
			nDataSegmentBytes = *wasmMetadata.NDataSegmentBytes
		}

		row := ContractCodeRow{
			ContractCodeHash:   codeHashHex,
			LedgerKeyHash:      ledgerKeyHash,
			ContractCodeExtV:   extV,
			LastModifiedLedger: int(lastModifiedLedger),
			LedgerEntryChange:  int(change.Type),
			Deleted:            deleted,
			ClosedAt:           closedAt,
			LedgerSequence:     ledgerSeq,
			NInstructions:      nInstructions,
			NFunctions:         nFunctions,
			NGlobals:           nGlobals,
			NTableEntries:      nTableEntries,
			NTypes:             nTypes,
			NDataSegments:      nDataSegments,
			NElemSegments:      nElemSegments,
			NImports:           nImports,
			NExports:           nExports,
			NDataSegmentBytes:  nDataSegmentBytes,
			CreatedAt:          closedAt,
			LedgerRange:        ledgerRange,
			EraID:              EraID,
			VersionLabel:       VersionLabel,
		}

		contractCodeMap[codeHashHex] = &row
	}

	for _, r := range contractCodeMap {
		contractCodeList = append(contractCodeList, *r)
	}
	return contractCodeList, nil
}

func isContractCodeChangeType(change ingest.Change) bool {
	if change.Pre != nil && change.Pre.Data.Type == xdr.LedgerEntryTypeContractCode {
		return true
	}
	if change.Post != nil && change.Post.Data.Type == xdr.LedgerEntryTypeContractCode {
		return true
	}
	return false
}

// wasmMetadataResult holds parsed metadata from a WASM binary.
type wasmMetadataResult struct {
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
}

func parseWASMMetadataBytes(wasmCode []byte) wasmMetadataResult {
	metadata := wasmMetadataResult{}
	if len(wasmCode) < 8 {
		return metadata
	}
	if wasmCode[0] != 0x00 || wasmCode[1] != 0x61 || wasmCode[2] != 0x73 || wasmCode[3] != 0x6D {
		return metadata
	}

	offset := 8
	for offset < len(wasmCode) {
		if offset+1 >= len(wasmCode) {
			break
		}
		sectionType := wasmCode[offset]
		offset++
		sectionSize, bytesRead := decodeLEB128Bytes(wasmCode[offset:])
		offset += bytesRead
		if sectionSize < 0 || offset+int(sectionSize) > len(wasmCode) {
			break
		}
		sectionData := wasmCode[offset : offset+int(sectionSize)]
		offset += int(sectionSize)

		switch sectionType {
		case 1:
			count := countWASMSectionElements(sectionData)
			metadata.NTypes = &count
		case 2:
			count := countWASMSectionElements(sectionData)
			metadata.NImports = &count
		case 3:
			count := countWASMSectionElements(sectionData)
			metadata.NFunctions = &count
		case 4:
			count := countWASMSectionElements(sectionData)
			metadata.NTableEntries = &count
		case 6:
			count := countWASMSectionElements(sectionData)
			metadata.NGlobals = &count
		case 7:
			count := countWASMSectionElements(sectionData)
			metadata.NExports = &count
		case 9:
			count := countWASMSectionElements(sectionData)
			metadata.NElemSegments = &count
		case 10:
			count := countWASMSectionElements(sectionData)
			if count > 0 {
				instrCount := int64(len(sectionData))
				metadata.NInstructions = &instrCount
			}
		case 11:
			count := countWASMSectionElements(sectionData)
			metadata.NDataSegments = &count
			dataSize := int64(len(sectionData))
			metadata.NDataSegmentBytes = &dataSize
		}
	}

	return metadata
}

func countWASMSectionElements(data []byte) int64 {
	if len(data) == 0 {
		return 0
	}
	count, _ := decodeLEB128Bytes(data)
	return count
}

func decodeLEB128Bytes(data []byte) (int64, int) {
	var result int64
	var shift uint
	var bytesRead int

	for bytesRead = 0; bytesRead < len(data) && bytesRead < 10; bytesRead++ {
		b := data[bytesRead]
		result |= int64(b&0x7F) << shift
		if b&0x80 == 0 {
			if shift < 64 && (b&0x40) != 0 {
				result |= -(1 << (shift + 7))
			}
			return result, bytesRead + 1
		}
		shift += 7
	}

	return 0, bytesRead
}

// ---------------------------------------------------------------------------
// Native Balance extraction
// ---------------------------------------------------------------------------

func extractNativeBalanceRows(lcm xdr.LedgerCloseMeta, networkPassphrase string, ledgerSeq uint32, closedAt time.Time, ledgerRange uint32) ([]NativeBalanceRow, error) {
	_ = closedAt
	var nativeBalances []NativeBalanceRow

	changeReader, err := ingest.NewLedgerChangeReaderFromLedgerCloseMeta(networkPassphrase, lcm)
	if err != nil {
		log.Printf("Failed to create change reader for native balances: %v", err)
		return nativeBalances, nil
	}
	defer changeReader.Close()

	balanceMap := make(map[string]*NativeBalanceRow)

	for {
		change, err := changeReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Printf("Error reading change for native balances: %v", err)
			continue
		}

		if change.Type != xdr.LedgerEntryTypeAccount {
			continue
		}

		var accountEntry *xdr.AccountEntry
		var lastModifiedLedger uint32

		if change.Post != nil && change.Post.Data.Type == xdr.LedgerEntryTypeAccount {
			entry, _ := change.Post.Data.GetAccount()
			accountEntry = &entry
			lastModifiedLedger = uint32(change.Post.LastModifiedLedgerSeq)
		} else if change.Pre != nil && change.Pre.Data.Type == xdr.LedgerEntryTypeAccount {
			entry, _ := change.Pre.Data.GetAccount()
			accountEntry = &entry
			lastModifiedLedger = uint32(change.Pre.LastModifiedLedgerSeq)
		}
		if accountEntry == nil {
			continue
		}

		accountID := accountEntry.AccountId.Address()
		balance := int64(accountEntry.Balance)
		buyingLiabilities := int64(0)
		sellingLiabilities := int64(0)

		if ext, ok := accountEntry.Ext.GetV1(); ok {
			buyingLiabilities = int64(ext.Liabilities.Buying)
			sellingLiabilities = int64(ext.Liabilities.Selling)
		}

		numSubentries := int(accountEntry.NumSubEntries)
		numSponsoring := 0
		numSponsored := 0

		if ext, ok := accountEntry.Ext.GetV1(); ok {
			if ext2, ok := ext.Ext.GetV2(); ok {
				numSponsoring = int(ext2.NumSponsoring)
				numSponsored = int(ext2.NumSponsored)
			}
		}

		sequenceNumber := int64(accountEntry.SeqNum)

		row := NativeBalanceRow{
			AccountID:          accountID,
			Balance:            balance,
			BuyingLiabilities:  buyingLiabilities,
			SellingLiabilities: sellingLiabilities,
			NumSubentries:      numSubentries,
			NumSponsoring:      numSponsoring,
			NumSponsored:       numSponsored,
			SequenceNumber:     sequenceNumber,
			LastModifiedLedger: int64(lastModifiedLedger),
			LedgerSequence:     ledgerSeq,
			LedgerRange:        ledgerRange,
			EraID:              EraID,
			VersionLabel:       VersionLabel,
		}

		balanceMap[accountID] = &row
	}

	for _, r := range balanceMap {
		nativeBalances = append(nativeBalances, *r)
	}
	return nativeBalances, nil
}

// ---------------------------------------------------------------------------
// Contract Creation extraction
// ---------------------------------------------------------------------------

func extractContractCreationRows(lcm xdr.LedgerCloseMeta, networkPassphrase string, ledgerSeq uint32, closedAt time.Time, ledgerRange uint32) ([]ContractCreationRow, error) {
	var creations []ContractCreationRow

	reader, err := ingest.NewLedgerTransactionReaderFromLedgerCloseMeta(networkPassphrase, lcm)
	if err != nil {
		return creations, nil
	}
	defer reader.Close()

	for {
		tx, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			continue
		}

		if !tx.Result.Successful() {
			continue
		}

		txSourceAccount := tx.Envelope.SourceAccount().ToAccountId().Address()

		for opIdx, op := range tx.Envelope.Operations() {
			invokeHostFn, ok := op.Body.GetInvokeHostFunctionOp()
			if !ok {
				continue
			}

			fnType := invokeHostFn.HostFunction.Type
			if fnType != xdr.HostFunctionTypeHostFunctionTypeCreateContract &&
				fnType != xdr.HostFunctionTypeHostFunctionTypeCreateContractV2 {
				continue
			}

			creatorAddress := txSourceAccount
			if op.SourceAccount != nil {
				creatorAddress = op.SourceAccount.ToAccountId().Address()
			}

			var changes xdr.LedgerEntryChanges
			switch tx.UnsafeMeta.V {
			case 3:
				v3 := tx.UnsafeMeta.MustV3()
				if v3.SorobanMeta != nil && opIdx < len(v3.Operations) {
					changes = v3.Operations[opIdx].Changes
				}
			case 4:
				v4 := tx.UnsafeMeta.MustV4()
				if v4.SorobanMeta != nil && opIdx < len(v4.Operations) {
					changes = v4.Operations[opIdx].Changes
				}
			}

			if len(changes) > 0 {
				for _, change := range changes {
					created, ok := change.GetCreated()
					if !ok {
						continue
					}
					contractData, ok := created.Data.GetContractData()
					if !ok {
						continue
					}

					if contractData.Durability == xdr.ContractDataDurabilityPersistent {
						if scAddr, ok := contractData.Contract.GetContractId(); ok {
							contractIDStr, err := strkey.Encode(strkey.VersionByteContract, scAddr[:])
							if err != nil {
								continue
							}

							creation := ContractCreationRow{
								ContractID:      contractIDStr,
								CreatorAddress:  creatorAddress,
								CreatedLedger:   int64(ledgerSeq),
								CreatedAt:       closedAt,
								LedgerRange:     ledgerRange,
								EraID:           EraID,
								VersionLabel:    VersionLabel,
							}

							if fnType == xdr.HostFunctionTypeHostFunctionTypeCreateContract {
								if args, ok := invokeHostFn.HostFunction.GetCreateContract(); ok {
									if wasmHash, ok := args.Executable.GetWasmHash(); ok {
										wasmHashStr := hex.EncodeToString(wasmHash[:])
										creation.WasmHash = &wasmHashStr
									}
								}
							} else if fnType == xdr.HostFunctionTypeHostFunctionTypeCreateContractV2 {
								if args, ok := invokeHostFn.HostFunction.GetCreateContractV2(); ok {
									if wasmHash, ok := args.Executable.GetWasmHash(); ok {
										wasmHashStr := hex.EncodeToString(wasmHash[:])
										creation.WasmHash = &wasmHashStr
									}
								}
							}

							creations = append(creations, creation)
							break
						}
					}
				}
			}
		}
	}

	return creations, nil
}
