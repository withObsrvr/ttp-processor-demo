package main

import (
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"strings"
	"time"

	"github.com/stellar/go-stellar-sdk/ingest"
	"github.com/stellar/go-stellar-sdk/xdr"
)

// ---------------------------------------------------------------------------
// Core Extractors: Transactions, Operations, Effects, Trades
// Adapted from stellar-postgres-ingester/go/extractors.go
// ---------------------------------------------------------------------------

// extractTransactions extracts transaction data from a pre-decoded ledger.
func extractTransactions(lcm xdr.LedgerCloseMeta, networkPassphrase string, ledgerSeq uint32, closedAt time.Time, ledgerRange uint32) ([]TransactionData, error) {
	var transactions []TransactionData

	reader, err := ingest.NewLedgerTransactionReaderFromLedgerCloseMeta(networkPassphrase, lcm)
	if err != nil {
		return nil, fmt.Errorf("failed to create transaction reader: %w", err)
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

		txData := TransactionData{
			LedgerSequence:        ledgerSeq,
			TransactionHash:       hex.EncodeToString(tx.Result.TransactionHash[:]),
			SourceAccount:         tx.Envelope.SourceAccount().ToAccountId().Address(),
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
		}

		// Extract memo
		memo := tx.Envelope.Memo()
		switch memo.Type {
		case xdr.MemoTypeMemoNone:
			memoType := "none"
			txData.MemoType = &memoType
		case xdr.MemoTypeMemoText:
			memoType := "text"
			txData.MemoType = &memoType
			if text, ok := memo.GetText(); ok {
				txData.Memo = &text
			}
		case xdr.MemoTypeMemoId:
			memoType := "id"
			txData.MemoType = &memoType
			if id, ok := memo.GetId(); ok {
				memoStr := fmt.Sprintf("%d", id)
				txData.Memo = &memoStr
			}
		case xdr.MemoTypeMemoHash:
			memoType := "hash"
			txData.MemoType = &memoType
			if hash, ok := memo.GetHash(); ok {
				memoStr := hex.EncodeToString(hash[:])
				txData.Memo = &memoStr
			}
		case xdr.MemoTypeMemoReturn:
			memoType := "return"
			txData.MemoType = &memoType
			if ret, ok := memo.GetRetHash(); ok {
				memoStr := hex.EncodeToString(ret[:])
				txData.Memo = &memoStr
			}
		}

		// Check for CREATE_ACCOUNT operation
		for _, op := range tx.Envelope.Operations() {
			if op.Body.Type == xdr.OperationTypeCreateAccount {
				txData.NewAccount = true
				break
			}
		}

		// Extract Soroban rent fee charged (C13)
		if tx.UnsafeMeta.V == 3 {
			v3 := tx.UnsafeMeta.MustV3()
			if v3.SorobanMeta != nil && v3.SorobanMeta.Ext.V == 1 && v3.SorobanMeta.Ext.V1 != nil {
				rentFee := int64(v3.SorobanMeta.Ext.V1.RentFeeCharged)
				txData.RentFeeCharged = &rentFee
			}
		}

		// Extract Soroban resource fields from envelope
		if instructions, ok := tx.SorobanResourcesInstructions(); ok {
			val := int64(instructions)
			txData.SorobanResourcesInstructions = &val
		}
		if readBytes, ok := tx.SorobanResourcesDiskReadBytes(); ok {
			val := int64(readBytes)
			txData.SorobanResourcesReadBytes = &val
		}
		if writeBytes, ok := tx.SorobanResourcesWriteBytes(); ok {
			val := int64(writeBytes)
			txData.SorobanResourcesWriteBytes = &val
		}

		transactions = append(transactions, txData)
	}

	return transactions, nil
}

// extractOperations extracts operation data from a pre-decoded ledger.
func extractOperations(lcm xdr.LedgerCloseMeta, networkPassphrase string, ledgerSeq uint32, closedAt time.Time, ledgerRange uint32) ([]OperationData, error) {
	var operations []OperationData

	reader, err := ingest.NewLedgerTransactionReaderFromLedgerCloseMeta(networkPassphrase, lcm)
	if err != nil {
		return nil, fmt.Errorf("failed to create transaction reader: %w", err)
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
			// Get source account (defaults to transaction source if not specified)
			sourceAccount := tx.Envelope.SourceAccount().ToAccountId().Address()
			if op.SourceAccount != nil {
				sourceAccount = op.SourceAccount.ToAccountId().Address()
			}

			opData := OperationData{
				TransactionHash:       txHash,
				TransactionIndex:      txIndex,
				OperationIndex:        i,
				LedgerSequence:        ledgerSeq,
				SourceAccount:         sourceAccount,
				OpType:                int(op.Body.Type),
				TypeString:            op.Body.Type.String(),
				CreatedAt:             closedAt,
				TransactionSuccessful: txSuccessful,
				LedgerRange:           ledgerRange,
			}

			// Extract operation-specific fields
			switch op.Body.Type {
			case xdr.OperationTypePayment:
				if payment, ok := op.Body.GetPaymentOp(); ok {
					amount := int64(payment.Amount)
					opData.Amount = &amount

					asset := payment.Asset.StringCanonical()
					opData.Asset = &asset

					dest := payment.Destination.ToAccountId().Address()
					opData.Destination = &dest
				}

			case xdr.OperationTypeCreateAccount:
				if createAcct, ok := op.Body.GetCreateAccountOp(); ok {
					amount := int64(createAcct.StartingBalance)
					opData.Amount = &amount

					dest := createAcct.Destination.Address()
					opData.Destination = &dest
				}

			case xdr.OperationTypePathPaymentStrictReceive:
				if pathPayment, ok := op.Body.GetPathPaymentStrictReceiveOp(); ok {
					amount := int64(pathPayment.DestAmount)
					opData.Amount = &amount

					asset := pathPayment.DestAsset.StringCanonical()
					opData.Asset = &asset

					dest := pathPayment.Destination.ToAccountId().Address()
					opData.Destination = &dest
				}

			case xdr.OperationTypePathPaymentStrictSend:
				if pathPayment, ok := op.Body.GetPathPaymentStrictSendOp(); ok {
					amount := int64(pathPayment.SendAmount)
					opData.Amount = &amount

					asset := pathPayment.SendAsset.StringCanonical()
					opData.Asset = &asset

					dest := pathPayment.Destination.ToAccountId().Address()
					opData.Destination = &dest
				}
			}

			// Get operation result code if available
			if txSuccessful {
				if opResults, ok := tx.Result.Result.OperationResults(); ok {
					if i < len(opResults) {
						resultCode := opResults[i].Code.String()
						opData.OperationResultCode = &resultCode
					}
				}
			}

			// Extract contract invocation details for InvokeHostFunction operations (type 24)
			if op.Body.Type == xdr.OperationTypeInvokeHostFunction {
				if invokeOp, ok := op.Body.GetInvokeHostFunctionOp(); ok {
					fnType := invokeOp.HostFunction.Type.String()
					opData.SorobanOperation = &fnType
				}
				contractID, functionName, argsJSON, err := extractContractInvocationDetails(op)
				if err != nil {
					log.Printf("Warning: Failed to extract contract invocation details for op %s:%d: %v", txHash, i, err)
				}
				opData.SorobanContractID = contractID
				opData.SorobanFunction = functionName
				opData.SorobanArgumentsJSON = argsJSON

				// Call graph extraction (stubbed out to avoid call_graph.go dependency)
				if err := integrateCallGraph(tx, i, op, &opData); err != nil {
					log.Printf("Warning: Failed to integrate call graph for op %s:%d: %v", txHash, i, err)
				}
			}

			operations = append(operations, opData)
		}

		txIndex++
	}

	return operations, nil
}

// extractEffects extracts basic effects from a pre-decoded ledger.
// Simplified implementation covering account credited/debited effects from balance changes.
func extractEffects(lcm xdr.LedgerCloseMeta, networkPassphrase string, ledgerSeq uint32, closedAt time.Time, ledgerRange uint32) ([]EffectData, error) {
	var effects []EffectData

	reader, err := ingest.NewLedgerTransactionReaderFromLedgerCloseMeta(networkPassphrase, lcm)
	if err != nil {
		return nil, fmt.Errorf("failed to create transaction reader: %w", err)
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
				// Only track account balance changes (most common effect)
				if change.Type == xdr.LedgerEntryTypeAccount {
					if change.Pre != nil && change.Post != nil {
						preAccount := change.Pre.Data.MustAccount()
						postAccount := change.Post.Data.MustAccount()
						preBal := int64(preAccount.Balance)
						postBal := int64(postAccount.Balance)

						if postBal > preBal {
							// Account credited
							amount := fmt.Sprintf("%d", postBal-preBal)
							accountID := postAccount.AccountId.Address()
							assetType := "native"

							effects = append(effects, EffectData{
								LedgerSequence:   ledgerSeq,
								TransactionHash:  txHash,
								OperationIndex:   int(opIdx),
								EffectIndex:      effectIdx,
								EffectType:       2, // EffectAccountCredited
								EffectTypeString: "account_credited",
								AccountID:        &accountID,
								Amount:           &amount,
								AssetType:        &assetType,
								CreatedAt:        closedAt,
								LedgerRange:      ledgerRange,
							})
							effectIdx++
						} else if postBal < preBal {
							// Account debited
							amount := fmt.Sprintf("%d", preBal-postBal)
							accountID := postAccount.AccountId.Address()
							assetType := "native"

							effects = append(effects, EffectData{
								LedgerSequence:   ledgerSeq,
								TransactionHash:  txHash,
								OperationIndex:   int(opIdx),
								EffectIndex:      effectIdx,
								EffectType:       3, // EffectAccountDebited
								EffectTypeString: "account_debited",
								AccountID:        &accountID,
								Amount:           &amount,
								AssetType:        &assetType,
								CreatedAt:        closedAt,
								LedgerRange:      ledgerRange,
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

// extractTrades extracts trades from a pre-decoded ledger.
// Extracts trade data from MANAGE_SELL_OFFER, MANAGE_BUY_OFFER, and CREATE_PASSIVE_SELL_OFFER operations.
func extractTrades(lcm xdr.LedgerCloseMeta, networkPassphrase string, ledgerSeq uint32, closedAt time.Time, ledgerRange uint32) ([]TradeData, error) {
	var trades []TradeData

	reader, err := ingest.NewLedgerTransactionReaderFromLedgerCloseMeta(networkPassphrase, lcm)
	if err != nil {
		return nil, fmt.Errorf("failed to create transaction reader: %w", err)
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
			continue // Only process successful transactions
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

								// Parse selling asset
								if ob.AssetSold.Type != xdr.AssetTypeAssetTypeNative {
									switch ob.AssetSold.Type {
									case xdr.AssetTypeAssetTypeCreditAlphanum4:
										a4 := ob.AssetSold.MustAlphaNum4()
										code := strings.TrimRight(string(a4.AssetCode[:]), "\x00")
										issuer := a4.Issuer.Address()
										sellingCode = &code
										sellingIssuer = &issuer
									case xdr.AssetTypeAssetTypeCreditAlphanum12:
										a12 := ob.AssetSold.MustAlphaNum12()
										code := strings.TrimRight(string(a12.AssetCode[:]), "\x00")
										issuer := a12.Issuer.Address()
										sellingCode = &code
										sellingIssuer = &issuer
									}
								}

								// Parse buying asset
								if ob.AssetBought.Type != xdr.AssetTypeAssetTypeNative {
									switch ob.AssetBought.Type {
									case xdr.AssetTypeAssetTypeCreditAlphanum4:
										a4 := ob.AssetBought.MustAlphaNum4()
										code := strings.TrimRight(string(a4.AssetCode[:]), "\x00")
										issuer := a4.Issuer.Address()
										buyingCode = &code
										buyingIssuer = &issuer
									case xdr.AssetTypeAssetTypeCreditAlphanum12:
										a12 := ob.AssetBought.MustAlphaNum12()
										code := strings.TrimRight(string(a12.AssetCode[:]), "\x00")
										issuer := a12.Issuer.Address()
										buyingCode = &code
										buyingIssuer = &issuer
									}
								}

							case xdr.ClaimAtomTypeClaimAtomTypeV0:
								v0 := claimAtom.MustV0()
								sellerAccount = fmt.Sprintf("%x", v0.SellerEd25519)
								sellingAmount = fmt.Sprintf("%d", v0.AmountSold)
								buyingAmount = fmt.Sprintf("%d", v0.AmountBought)
							}

							buyerAccount := tx.Envelope.SourceAccount().ToAccountId().Address()

							trades = append(trades, TradeData{
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
// Helper functions
// ---------------------------------------------------------------------------

// getAssetString returns the canonical string representation of an XDR asset.
func getAssetString(asset xdr.Asset) string {
	switch asset.Type {
	case xdr.AssetTypeAssetTypeNative:
		return "native"
	case xdr.AssetTypeAssetTypeCreditAlphanum4:
		if a4, ok := asset.GetAlphaNum4(); ok {
			code := string(a4.AssetCode[:])
			issuer := a4.Issuer.Address()
			return fmt.Sprintf("%s:%s", code, issuer)
		}
	case xdr.AssetTypeAssetTypeCreditAlphanum12:
		if a12, ok := asset.GetAlphaNum12(); ok {
			code := string(a12.AssetCode[:])
			issuer := a12.Issuer.Address()
			return fmt.Sprintf("%s:%s", code, issuer)
		}
	}
	return ""
}

// marshalToBase64 encodes an XDR-marshallable value to a base64 string.
func marshalToBase64(v interface{ MarshalBinary() ([]byte, error) }) *string {
	if bytes, err := v.MarshalBinary(); err == nil {
		encoded := base64.StdEncoding.EncodeToString(bytes)
		return &encoded
	}
	return nil
}

// extractContractInvocationDetails extracts contract ID, function name, and arguments
// from an InvokeHostFunction operation.
func extractContractInvocationDetails(op xdr.Operation) (*string, *string, *string, error) {
	invokeOp, ok := op.Body.GetInvokeHostFunctionOp()
	if !ok {
		return nil, nil, nil, nil
	}

	if invokeOp.HostFunction.Type != xdr.HostFunctionTypeHostFunctionTypeInvokeContract {
		return nil, nil, nil, nil
	}

	if invokeOp.HostFunction.InvokeContract == nil {
		return nil, nil, nil, nil
	}

	invokeContract := invokeOp.HostFunction.InvokeContract

	// Extract contract address
	var contractID *string
	contractIDStr, err := invokeContract.ContractAddress.String()
	if err == nil && contractIDStr != "" {
		contractID = &contractIDStr
	}

	// Extract function name
	var functionName *string
	if invokeContract.FunctionName != "" {
		fnName := string(invokeContract.FunctionName)
		functionName = &fnName
	}

	// Extract arguments - encode each ScVal as base64 XDR for portability
	// (avoids dependency on the full ScVal-to-JSON converter)
	args := invokeContract.Args
	var argsJSON []interface{}
	for _, arg := range args {
		argBytes, marshalErr := arg.MarshalBinary()
		if marshalErr != nil {
			log.Printf("Warning: Failed to marshal ScVal arg: %v", marshalErr)
			argsJSON = append(argsJSON, map[string]interface{}{
				"error": marshalErr.Error(),
				"type":  arg.Type.String(),
			})
		} else {
			argsJSON = append(argsJSON, map[string]interface{}{
				"type":    arg.Type.String(),
				"xdr_b64": base64.StdEncoding.EncodeToString(argBytes),
			})
		}
	}

	var argsStr *string
	if len(argsJSON) > 0 {
		argsJSONBytes, marshalErr := json.Marshal(argsJSON)
		if marshalErr != nil {
			return contractID, functionName, nil, fmt.Errorf("failed to marshal arguments to JSON: %w", marshalErr)
		}
		s := string(argsJSONBytes)
		argsStr = &s
	}

	return contractID, functionName, argsStr, nil
}

// integrateCallGraph is a no-op stub to avoid pulling in the full call_graph.go dependency.
// Cross-contract call graph extraction can be added later if needed.
func integrateCallGraph(_ ingest.LedgerTransaction, _ int, _ xdr.Operation, _ *OperationData) error {
	return nil
}
